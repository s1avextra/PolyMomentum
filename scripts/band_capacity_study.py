#!/usr/bin/env python3
"""Capacity/economics study for signal_favorite_band_official_v1.

For every band window of both discovery samples, measure the EXECUTED
liquidity available around the decision: the sum of third-party BUY print
sizes on the signal-side token within the entry window, at band prices.
That is a lower bound on takeable depth (someone actually took it), the
honest counterpart of the entry-price methodology.

Outputs a $/day model at several stakes with explicit haircuts, plus the
band-event frequency accounting (windows with no print at all are the
fill-rate risk, counted, not hidden).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics as st
import time
import urllib.parse
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent))
from adaptation_persistence_study import http_json  # noqa: E402

GAMMA = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
DECISION_OFFSET_S = 240
ENTRY_WINDOW_S = 30
BAND = (0.55, 0.92)
POINT_EDGE = 0.10  # discovery band edge (both samples ~+0.10..0.12)


def band_windows_from_studies() -> list[dict]:
    out = []
    primary = json.load(open("deploy/promotions/evidence/strategy_registry/20260819_adaptation_persistence_study.json"))
    for r in primary["rows"]:
        p = r.get("entry_executed_price")
        if p is not None and BAND[0] < p <= BAND[1]:
            out.append({"window_start": r["window_start"], "signal": r["signal"], "sample": "primary"})
    disjoint = json.load(open("deploy/promotions/evidence/strategy_registry/20260819_complement_and_band_disjoint_study.json"))
    for r in disjoint["rows"]:
        p = r.get("signal_entry")
        if p is not None and BAND[0] < p <= BAND[1]:
            out.append({"window_start": r["window_start"], "signal": r["signal"], "sample": "disjoint"})
    return out


def window_band_volume(ws: int, signal: str, pause_s: float) -> dict | None:
    slug = f"btc-updown-5m-{ws}"
    markets = http_json(f"{GAMMA}/markets?slug={slug}&closed=true")
    time.sleep(pause_s)
    if not markets:
        return None
    m = markets[0]
    outcomes = json.loads(m["outcomes"]) if isinstance(m.get("outcomes"), str) else m["outcomes"]
    tokens = json.loads(m["clobTokenIds"]) if isinstance(m.get("clobTokenIds"), str) else m["clobTokenIds"]
    token = {str(o).lower(): t for o, t in zip(outcomes, tokens)}.get(signal)
    condition_id = m.get("conditionId")
    if token is None or condition_id is None:
        return None
    params = urllib.parse.urlencode({"market": condition_id, "limit": 500})
    trades = http_json(f"{DATA_API}/trades?{params}")
    time.sleep(pause_s)
    t0 = ws + DECISION_OFFSET_S
    shares = 0.0
    notional = 0.0
    prints = 0
    for t in trades or []:
        if t.get("asset") != token or t.get("side") != "BUY":
            continue
        ts = int(t.get("timestamp", 0))
        price = float(t.get("price", 0))
        if t0 <= ts <= t0 + ENTRY_WINDOW_S and BAND[0] < price <= BAND[1]:
            size = float(t.get("size", 0))
            shares += size
            notional += size * price
            prints += 1
    return {"window_start": ws, "prints": prints, "shares": shares,
            "notional_usd": round(notional, 2)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pause-s", type=float, default=0.12)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    windows = band_windows_from_studies()
    vols = []
    for i, w in enumerate(windows):
        v = window_band_volume(w["window_start"], w["signal"], args.pause_s)
        if v is not None:
            v["sample"] = w["sample"]
            vols.append(v)
        if (i + 1) % 50 == 0:
            print(f"{i + 1}/{len(windows)}")

    notionals = [v["notional_usd"] for v in vols]
    days_covered = 10
    hours_per_day_sampled = 12  # 6 primary + 6 disjoint of 24
    events_per_day_full = len(vols) / days_covered * (24 / hours_per_day_sampled)

    def daily_model(stake: float) -> dict:
        fillable = [min(stake, n) for n in notionals]
        avg_fill = st.mean(fillable) if fillable else 0.0
        fill_full = sum(1 for n in notionals if n >= stake) / len(notionals) if notionals else 0
        return {
            "stake_usd": stake,
            "avg_fillable_usd_per_event": round(avg_fill, 2),
            "share_of_events_with_full_stake_depth": round(fill_full, 3),
            "gross_expected_usd_per_day": round(events_per_day_full * avg_fill * POINT_EDGE, 2),
        }

    result = {
        "schema_version": 1,
        "registration": "band_capacity_study_20260819",
        "band": list(BAND),
        "band_windows_measured": len(vols),
        "days_covered": days_covered,
        "entry_window_s": ENTRY_WINDOW_S,
        "executed_band_notional_per_event_usd": {
            "p25": round(st.quantiles(notionals, n=4)[0], 2) if len(notionals) >= 4 else None,
            "median": round(st.median(notionals), 2) if notionals else None,
            "p75": round(st.quantiles(notionals, n=4)[2], 2) if len(notionals) >= 4 else None,
            "mean": round(st.mean(notionals), 2) if notionals else None,
            "zero_volume_events": sum(1 for n in notionals if n == 0),
        },
        "events_per_day_at_full_coverage": round(events_per_day_full, 1),
        "daily_models_at_point_edge_0.10": [daily_model(s) for s in (5, 20, 50, 100)],
        "haircuts_not_applied": [
            "our order adds demand and competes with the measured prints (measured volume is a LOWER bound on depth but an UPPER bound on what we can take without moving price)",
            "fill rate for OUR taker order is unproven until live/exact-replay evidence",
            "edge decay/adaptation after our own participation",
            "VPS latency vs print timing",
        ],
        "rows": vols,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=1)
        f.write("\n")
    print(json.dumps({k: result[k] for k in
                      ["band_windows_measured", "executed_band_notional_per_event_usd",
                       "events_per_day_at_full_coverage", "daily_models_at_point_edge_0.10"]},
                     indent=1))


if __name__ == "__main__":
    main()
