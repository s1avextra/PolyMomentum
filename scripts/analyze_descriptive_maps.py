#!/usr/bin/env python3
"""Descriptive maps over the sealed 20-hour / 704-row opportunity dataset.

Three cheap, outcome-safe passes that give the strategy generator a prior:

1. Price-bucket calibration (favorite-longshot map): realized win rate vs
   executable entry price, bucketed by price x decision time, with Wilson
   bounds and the fee-aware break-even overlaid. Uses ONLY the 571 labeled
   older/recent rows — the 133 fresh-holdout rows have no labels on disk and
   are excluded by the inner join (fresh outcomes stay sealed).
2. Ask-side complete-set violation scan: frequency of
   ask_up + fee(ask_up) + ask_dn + fee(ask_dn) < 1 over the paired-book
   cache. Book state is a causal feature, not an outcome, so all 704 rows
   participate; no label is read.
3. Decision-time x direction support summary for the labeled slice.

Output: JSON evidence to --output.
"""

from __future__ import annotations

import argparse
import json
import math

import pandas as pd

Z95 = 1.959_963_984_540_054
CRYPTO_TAKER_FEE = 0.072

SEAL_INDEX = "logs/strategy-research/opportunity-datasets/chronological_expanded_20260811.seal.json"
LABELS = "logs/strategy-research/opportunity-labels/chronological_expanded_20260811.labels.parquet"
PAIR_CACHE = "logs/strategy-research/opportunity-liquidity/paired_features_20260811.jsonl"


def wilson_bounds(wins: int, n: int, z: float = Z95) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    p = wins / n
    denom = 1.0 + z * z / n
    centre = p + z * z / (2 * n)
    radius = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (max(0.0, (centre - radius) / denom), min(1.0, (centre + radius) / denom))


def taker_fee(price: float) -> float:
    return CRYPTO_TAKER_FEE * price * (1.0 - price)


def load_sealed_tables() -> pd.DataFrame:
    """Load hour tables strictly through the sealed dataset index — the
    on-disk dirs also hold rejected hours (e.g. the zero-coverage June 26-27
    attempts) that are NOT part of the seal."""
    seal = json.load(open(SEAL_INDEX))
    frames = [pd.read_parquet(e["opportunity_table"]["path"]) for e in seal["entries"]]
    df = pd.concat(frames, ignore_index=True)
    assert len(df) == seal["total_rows"] == 704, \
        f"seal mismatch: {len(df)} vs {seal['total_rows']}"
    return df


def calibration_map(df: pd.DataFrame, labels: pd.DataFrame) -> dict:
    joined = df.merge(labels[["opportunity_id", "won"]], on="opportunity_id", how="inner")
    assert not (joined["chronological_window"] == "fresh_holdout").any(), \
        "fresh rows must not carry labels"
    usable = joined[joined["book_observable"] & joined["average_entry_price"].notna()].copy()

    price_edges = [0.0, 0.55, 0.65, 0.75, 0.85, 0.92, 1.0]
    usable["price_bucket"] = pd.cut(usable["average_entry_price"], price_edges)
    usable["decision_s"] = (300 - usable["remaining_seconds"]).round(-1).astype(int)

    buckets = []
    for (pb, ds), g in usable.groupby(["price_bucket", "decision_s"], observed=True):
        n = len(g)
        if n == 0:
            continue
        w = int(g["won"].sum())
        lo, hi = wilson_bounds(w, n)
        be = float(g["fee_aware_break_even_probability"].mean())
        buckets.append({
            "price_bucket": str(pb), "decision_seconds": int(ds), "n": n,
            "wins": w, "win_rate": round(w / n, 4),
            "wilson_lo": round(lo, 4), "wilson_hi": round(hi, 4),
            "avg_break_even": round(be, 4),
            "point_edge": round(w / n - be, 4),
            "edge_confirmed_at_95": lo > be,
            "mispriced_against_at_95": hi < be,
        })
    # Coarse price-only view for the favorite-longshot read.
    price_only = []
    for pb, g in usable.groupby("price_bucket", observed=True):
        n, w = len(g), int(g["won"].sum())
        lo, hi = wilson_bounds(w, n)
        be = float(g["fee_aware_break_even_probability"].mean())
        price_only.append({
            "price_bucket": str(pb), "n": n, "wins": w,
            "win_rate": round(w / n, 4), "wilson_lo": round(lo, 4),
            "wilson_hi": round(hi, 4), "avg_break_even": round(be, 4),
            "point_edge": round(w / n - be, 4),
        })
    return {"by_price_and_time": buckets, "by_price": price_only,
            "labeled_rows_used": int(len(usable))}


def complete_set_scan(path: str) -> dict:
    rows = [json.loads(l) for l in open(path)]
    total, both_books = len(rows), 0
    costs = []
    violations_raw, violations_fee, violations_fee_depth = [], [], []
    for r in rows:
        up, dn = r.get("up_now") or {}, r.get("down_now") or {}
        if not (up.get("observable") and dn.get("observable")):
            continue
        both_books += 1
        au, ad = up["best_ask"], dn["best_ask"]
        cost_raw = au + ad
        cost_fee = au + taker_fee(au) + ad + taker_fee(ad)
        costs.append(cost_fee)
        entry = {"condition_id": r["condition_id"][:10],
                 "window": r["chronological_window"],
                 "decision_seconds": r["decision_seconds"],
                 "ask_up": au, "ask_dn": ad,
                 "cost_fee_aware": round(cost_fee, 5)}
        if cost_raw < 1.0:
            violations_raw.append(entry)
        if cost_fee < 1.0:
            violations_fee.append(entry)
            if up.get("stake_fully_executable") and dn.get("stake_fully_executable"):
                violations_fee_depth.append(entry)
    costs_s = pd.Series(costs)
    return {
        "rows_total": total, "rows_both_books_observable": both_books,
        "fee_aware_cost_quantiles": {
            "p01": round(float(costs_s.quantile(0.01)), 5),
            "p05": round(float(costs_s.quantile(0.05)), 5),
            "p50": round(float(costs_s.quantile(0.50)), 5),
            "min": round(float(costs_s.min()), 5),
        },
        "violations_raw_ask_sum_lt_1": len(violations_raw),
        "violations_fee_aware_lt_1": len(violations_fee),
        "violations_fee_aware_lt_1_full_depth": len(violations_fee_depth),
        "violation_samples": violations_fee[:10],
    }


def support_summary(df: pd.DataFrame, labels: pd.DataFrame) -> dict:
    joined = df.merge(labels[["opportunity_id", "won"]], on="opportunity_id", how="inner")
    out = []
    joined["decision_s"] = (300 - joined["remaining_seconds"]).round(-1).astype(int)
    for (ds, dr), g in joined.groupby(["decision_s", "signal_direction"]):
        n, w = len(g), int(g["won"].sum())
        lo, hi = wilson_bounds(w, n)
        out.append({"decision_seconds": int(ds), "direction": dr, "n": n,
                    "win_rate": round(w / n, 4),
                    "wilson_lo": round(lo, 4), "wilson_hi": round(hi, 4)})
    return {"by_decision_time_and_direction": out}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    df = load_sealed_tables()
    labels = pd.read_parquet(LABELS)

    result = {
        "schema_version": 1,
        "registration": "descriptive_maps_20260817",
        "dataset": "chronological seal 20 hours / 704 rows (labels: 571, fresh excluded)",
        "outcome_safety": {
            "fresh_holdout_labels_read": False,
            "fresh_books_used_only_for_outcome_free_scan": True,
        },
        "calibration_map": calibration_map(df, labels),
        "complete_set_scan": complete_set_scan(PAIR_CACHE),
        "support_summary": support_summary(df, labels),
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=1)
        f.write("\n")
    print(f"written {args.output}")


if __name__ == "__main__":
    main()
