#!/usr/bin/env python3
"""Frozen 54-policy cheap screen for partial_twap_lock_v1.

Grid, gates, and budgets are EXACTLY those preregistered in
docs/partial_twap_lock_v1_preregistration_2026-08-17.md (with its
outcome-blind data-plan amendment). Discovery-only: reads the sealed table
rows and the discovery label file, which physically excludes every
fresh_holdout row. No fresh outcome exists on disk for this run.

Executability constraint declared at screen time: the sealed table carries
only the signal-selected token's book, so rows where the partial-TWAP lead
favours the OTHER side are not executable within this dataset and are
counted out per policy (reported as lead_side_mismatch). A future paired
cache can recover them.
"""

from __future__ import annotations

import argparse
import json
import math

import pandas as pd

Z95 = 1.959_963_984_540_054
SEAL = "logs/strategy-research/twap-era/twap_era_20260817.seal.json"
LABELS = "logs/strategy-research/twap-era/twap_labels.parquet"

DECISION_SECONDS = [120, 180, 240]
LOCK_STRENGTH_FLOORS = [1.0, 2.0, 3.0]
ASK_CAPS = [0.55, 0.75, 0.90]
MIN_LOCK_FRACTIONS = [0.6, 0.8]

GATE_OLDER_MIN = 30
GATE_RECENT_MIN = 100
GATE_RECENT_EDGE = 0.02


def wilson_lower(wins: int, n: int, z: float = Z95) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1.0 + z * z / n
    centre = p + z * z / (2 * n)
    radius = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return max(0.0, (centre - radius) / denom)


def load() -> pd.DataFrame:
    seal = json.load(open(SEAL))
    frames = [pd.read_parquet(e["opportunity_table"]["path"]) for e in seal["entries"]]
    df = pd.concat(frames, ignore_index=True)
    assert len(df) == seal["total_rows"]
    labels = pd.read_parquet(LABELS)
    joined = df.merge(labels[["opportunity_id", "won"]], on="opportunity_id", how="inner")
    assert not (joined["chronological_window"] == "fresh_holdout").any()
    return joined


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    rows = load()
    rows["decision_s"] = (rows["elapsed_seconds"]).round().astype(int)
    sigma_tail = (
        rows["btc_open"]
        * rows["causal_volatility"]
        * (rows["remaining_seconds"] / 31_536_000.0).pow(0.5)
    )
    rows["lock_strength"] = (rows["partial_twap_lead_usd"].abs() / sigma_tail).where(sigma_tail > 0)
    rows["lead_direction"] = rows["partial_twap_lead_usd"].apply(
        lambda v: "up" if v is not None and v > 0 else ("down" if v is not None and v < 0 else None)
    )
    rows["lead_matches_signal"] = rows["lead_direction"] == rows["signal_direction"]

    policies = []
    for t in DECISION_SECONDS:
        for ls in LOCK_STRENGTH_FLOORS:
            for cap in ASK_CAPS:
                for mlf in MIN_LOCK_FRACTIONS:
                    base = rows[
                        (rows["decision_s"] == t)
                        & (rows["twap_locked_fraction"] >= mlf - 1e-9)
                        & rows["partial_twap_lead_usd"].notna()
                        & (rows["lock_strength"] >= ls)
                    ]
                    mismatch = int((~base["lead_matches_signal"]).sum())
                    exec_rows = base[
                        base["lead_matches_signal"]
                        & base["book_observable"]
                        & base["stake_fully_executable"]
                        & (base["best_ask"] <= cap + 1e-9)
                    ]
                    rec = {"decision_seconds": t, "lock_strength_min": ls,
                           "ask_cap": cap, "min_lock_fraction": mlf,
                           "lead_side_mismatch_rows": mismatch}
                    for era, tag in (("older", "older"), ("recent", "recent_discovery")):
                        g = exec_rows[exec_rows["chronological_window"] == tag]
                        n, w = len(g), int(g["won"].sum())
                        be = float(g["fee_aware_break_even_probability"].mean()) if n else None
                        payoff = float(
                            (g["won"] * g["fee_aware_net_win_usd"]
                             - (1 - g["won"]) * g["fee_aware_max_loss_usd"]).sum()
                        ) if n else 0.0
                        rec[f"{era}_n"] = n
                        rec[f"{era}_wins"] = w
                        rec[f"{era}_win_rate"] = round(w / n, 4) if n else None
                        rec[f"{era}_avg_break_even"] = round(be, 4) if be is not None else None
                        rec[f"{era}_point_edge"] = round(w / n - be, 4) if n and be is not None else None
                        rec[f"{era}_wilson_lower"] = round(wilson_lower(w, n), 4) if n else None
                        rec[f"{era}_fee_aware_payoff_usd"] = round(payoff, 3)
                    rec["passes_older_gate"] = bool(
                        rec["older_n"] >= GATE_OLDER_MIN
                        and (rec["older_point_edge"] or -1) > 0
                    )
                    rec["passes_recent_gate"] = bool(
                        rec["recent_n"] >= GATE_RECENT_MIN
                        and (rec["recent_point_edge"] or -1) > GATE_RECENT_EDGE
                        and rec["recent_fee_aware_payoff_usd"] > 0
                    )
                    policies.append(rec)

    eligible = [p for p in policies if p["passes_older_gate"] and p["passes_recent_gate"]]
    supported = [p for p in policies if p["older_n"] + p["recent_n"] > 0]
    best_support = max((p["recent_n"] for p in policies), default=0)

    if eligible:
        verdict = "research_eligible_candidates_found"
    elif best_support < GATE_RECENT_MIN:
        verdict = "insufficient_support_data_blocked"
    else:
        verdict = "no_policy_survived_discovery"

    result = {
        "schema_version": 1,
        "registration": "partial_twap_lock_v1_screen_20260818",
        "preregistration": "docs/partial_twap_lock_v1_preregistration_2026-08-17.md",
        "dataset_seal": SEAL,
        "labels": LABELS,
        "grid_size": len(policies),
        "rows_labeled": int(len(rows)),
        "verdict": verdict,
        "max_recent_support_any_policy": best_support,
        "eligible_policies": eligible,
        "top_diagnostic_policies": sorted(
            supported,
            key=lambda p: (p["recent_point_edge"] or -9),
            reverse=True,
        )[:8],
        "fresh_outcomes_opened": False,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=1)
        f.write("\n")
    print(json.dumps({k: result[k] for k in
                      ["verdict", "grid_size", "rows_labeled",
                       "max_recent_support_any_policy",
                       "eligible_policies"]}, indent=1)[:400])


if __name__ == "__main__":
    main()
