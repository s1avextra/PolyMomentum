#!/usr/bin/env python3
"""Point rule vs first-crossing rule for the band's margin floor.

The floor (`min_decision_margin_usd`) was validated as a POINT rule: one
sample per window, traded iff |close(ws+240) - open| >= floor, with that
sample's sign. Before the latch (`BandLatch`, rust_engine/src/live/pipeline.rs)
the live loop re-read the margin every ~100 ms across the entry window and
fired at the FIRST second in [240, 270) whose |margin| cleared the floor,
with that second's sign - a multiple-looks rule this script scores against
the point rule on the same public data:

  - Binance BTCUSDT 1s closes from the day cache that
    scripts/margin_floor_study.py fills (a day missing from the cache is
    fetched with the same helper);
  - Gamma official resolutions from that script's outcome cache, read only
    (run margin_floor_study.py first to fill it; windows without a cached
    outcome are skipped and counted).

Usage:
  uv run python scripts/margin_latch_study.py --start 2026-08-19 --end 2026-09-02T19:00
"""

import argparse
import json
import statistics
from datetime import datetime, timezone

import margin_floor_study as mfs

ENTRY_WINDOW_S = 30


def parse_utc(s):
    fmt = "%Y-%m-%dT%H:%M" if "T" in s else "%Y-%m-%d"
    return int(datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp())


def window_rules(ws, prices, floor):
    """(point, first_crossing, margin_at_decision) for one window; each rule
    is None (no trade) or the traded direction."""
    p_open = prices.get(str(ws))
    if p_open is None:
        return None
    p_dec = prices.get(str(ws + mfs.DECISION_S))
    margin_dec = None if p_dec is None else p_dec - p_open
    point = None
    if margin_dec is not None and abs(margin_dec) >= floor:
        point = "up" if margin_dec > 0 else "down"
    first_crossing = None
    for s in range(mfs.DECISION_S, mfs.DECISION_S + ENTRY_WINDOW_S):
        p = prices.get(str(ws + s))
        if p is None:
            continue
        m = p - p_open
        if abs(m) >= floor:
            first_crossing = "up" if m > 0 else "down"
            break
    return point, first_crossing, margin_dec


def score(name, rows):
    n = len(rows)
    w = sum(1 for r in rows if r["correct"])
    lo = mfs.wilson_lo(w, n)
    acc = f"{w / n:.2%}" if n else "-"
    print(f"  {name:<32} traded {n:>5}  correct {w:>5}  accuracy {acc:>7}  wilson_lo {lo:.3f}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--start", required=True, help="first window open, UTC (YYYY-MM-DD[THH:MM])")
    ap.add_argument("--end", required=True, help="last window open, exclusive, UTC")
    ap.add_argument("--floor", type=float, default=50.0, help="|margin| floor in USD")
    args = ap.parse_args()
    start, end = parse_utc(args.start), parse_utc(args.end)

    outcomes_path = mfs.CACHE / "gamma_outcomes.json"
    outcomes = json.loads(outcomes_path.read_text()) if outcomes_path.is_file() else {}
    day_cache = {}
    point_rows, crossing_rows, extra_margins = [], [], []
    n_windows = n_no_outcome = n_no_open = 0
    for ws in range(start, end, 300):
        n_windows += 1
        official = outcomes.get(str(ws))
        if official not in ("up", "down"):
            n_no_outcome += 1
            continue
        day_key = ws - (ws % 86400)
        if day_key not in day_cache:
            day_cache[day_key] = mfs.binance_price_series(day_key * 1000, str(day_key))
        rules = window_rules(ws, day_cache[day_key], args.floor)
        if rules is None:
            n_no_open += 1
            continue
        point, crossing, margin_dec = rules
        if point is not None:
            point_rows.append({"ws": ws, "correct": point == official})
        if crossing is not None:
            crossing_rows.append({"ws": ws, "correct": crossing == official})
            if point is None:
                extra_margins.append(abs(margin_dec) if margin_dec is not None else None)

    print(
        f"windows {args.start} -> {args.end}: {n_windows} total, "
        f"{n_no_outcome} without a cached resolution, {n_no_open} without a Binance open; "
        f"floor ${args.floor:.0f}"
    )
    point_ws = {r["ws"] for r in point_rows}
    crossing_ws = {r["ws"] for r in crossing_rows}
    score("point (decision second)", point_rows)
    score("first-crossing [240, 270)", crossing_rows)
    score(
        "admitted ONLY by first-crossing",
        [r for r in crossing_rows if r["ws"] not in point_ws],
    )
    score("traded ONLY by point", [r for r in point_rows if r["ws"] not in crossing_ws])
    known = [m for m in extra_margins if m is not None]
    if known:
        print(
            f"  extra windows' |margin| at the decision second: median ${statistics.median(known):.0f}, "
            f"{sum(1 for m in known if m < args.floor)}/{len(known)} below the floor"
        )
    print("DONE")


if __name__ == "__main__":
    main()
