#!/usr/bin/env python3
"""Adaptation-persistence study over the PMXT-archive gap (Aug 9-18, 2026).

The TWAP-era calibration map showed large positive edge on days 0-2 after
the rule change. The decisive question: did the market adapt? This study
reconstructs the gap days from PUBLIC historical sources, no order books
required:

- market identity: Gamma (`closed=true`) per btc-updown-5m window slug;
- entry price: EXECUTED trades from data-api.polymarket.com — a taker BUY
  at price P at time T is proof an ask at P was executable at T. The entry
  proxy is the first trade on the signal-side token in the 30s AFTER the
  decision instant (strictly causal: decision at t, execution after t);
- signal: BTC direction vs window open at the 240s decision, from
  checksum-verified Binance 1s opens;
- outcome: window TWAP vs open (official rule, ties Up) from the same tape.

Outcome discipline: this consumes Aug 9-18 as DIAGNOSTIC material, exactly
like the descriptive maps. The live capture campaign (Aug 19+) remains the
untouched fresh source for any preregistered gate. No sealed fresh row is
touched here.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import math
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

GAMMA = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
Z95 = 1.959_963_984_540_054
DECISION_OFFSET_S = 240
ENTRY_WINDOW_S = 30
WINDOW_S = 300

# Fixed calendar rule: six hours per day, every fourth hour. Declared before
# any price or outcome is fetched.
STUDY_HOURS_UTC = [1, 5, 9, 13, 17, 21]


def http_json(url: str, retries: int = 3):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "polymomentum-research/1.0",
            "Accept": "application/json",
        },
    )
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(request, timeout=30) as resp:
                return json.load(resp)
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(1.5 * (attempt + 1))


def wilson_lo(w: int, n: int) -> float:
    if n == 0:
        return 0.0
    p = w / n
    den = 1 + Z95 * Z95 / n
    c = p + Z95 * Z95 / (2 * n)
    r = Z95 * math.sqrt((p * (1 - p) + Z95 * Z95 / (4 * n)) / n)
    return max(0.0, (c - r) / den)


def load_opens(zip_dir: Path, days: list[str]) -> dict[int, float]:
    opens: dict[int, float] = {}
    for day in days:
        zp = zip_dir / f"BTCUSDT-1s-{day}.zip"
        with zipfile.ZipFile(zp) as z:
            with z.open(z.namelist()[0]) as f:
                for row in csv.reader(io.TextIOWrapper(f, "utf-8")):
                    if not row or not row[0].isdigit():
                        continue
                    ts = int(row[0])
                    while ts > 10**11:
                        ts //= 1000
                    opens[ts] = float(row[1])
    return opens


def twap(opens: dict[int, float], start: int, end: int) -> float | None:
    prices = [opens.get(s) for s in range(start, end)]
    if any(p is None for p in prices):
        return None
    return sum(prices) / len(prices)


def taker_fee(price: float) -> float:
    return 0.072 * price * (1.0 - price)


def study_window(ws: int, opens: dict[int, float], pause_s: float) -> dict | None:
    """One 5m window: signal at 240s, executed-entry proxy, TWAP outcome."""
    p_open = opens.get(ws)
    p_decision = opens.get(ws + DECISION_OFFSET_S)
    if p_open is None or p_decision is None:
        return None
    if p_decision == p_open:
        return None  # no directional signal
    signal = "up" if p_decision > p_open else "down"

    slug = f"btc-updown-5m-{ws}"
    markets = http_json(f"{GAMMA}/markets?slug={slug}&closed=true")
    time.sleep(pause_s)
    if not markets:
        return {"window_start": ws, "status": "market_not_found"}
    market = markets[0]
    condition_id = market.get("conditionId")
    outcomes = json.loads(market["outcomes"]) if isinstance(market.get("outcomes"), str) else market.get("outcomes")
    token_ids = json.loads(market["clobTokenIds"]) if isinstance(market.get("clobTokenIds"), str) else market.get("clobTokenIds")
    if not condition_id or not token_ids or len(token_ids) != 2:
        return {"window_start": ws, "status": "identity_incomplete"}
    by_outcome = {str(o).lower(): t for o, t in zip(outcomes, token_ids)}
    signal_token = by_outcome.get(signal)
    if signal_token is None:
        return {"window_start": ws, "status": "outcome_names_unexpected"}

    params = urllib.parse.urlencode({"market": condition_id, "limit": 500})
    trades = http_json(f"{DATA_API}/trades?{params}")
    time.sleep(pause_s)
    decision_ts = ws + DECISION_OFFSET_S
    entry = None
    for t in sorted(trades or [], key=lambda x: x.get("timestamp", 0)):
        if t.get("asset") != signal_token or t.get("side") != "BUY":
            continue
        ts = int(t.get("timestamp", 0))
        if decision_ts <= ts <= decision_ts + ENTRY_WINDOW_S:
            entry = float(t["price"])
            break
    if entry is None:
        return {"window_start": ws, "status": "no_executable_trade_in_entry_window"}

    window_twap = twap(opens, ws, ws + WINDOW_S)
    if window_twap is None:
        return {"window_start": ws, "status": "tape_gap"}
    outcome = "up" if window_twap >= p_open else "down"
    won = outcome == signal
    break_even = entry + taker_fee(entry)
    return {
        "window_start": ws,
        "status": "ok",
        "signal": signal,
        "entry_executed_price": entry,
        "break_even": round(break_even, 5),
        "won": won,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip-dir", default="data/binance_1s_twap_era")
    ap.add_argument("--days", nargs="+", required=True, help="YYYY-MM-DD list")
    ap.add_argument("--pause-s", type=float, default=0.25)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    opens = load_opens(Path(args.zip_dir), args.days)
    rows, skipped = [], {}
    for day in args.days:
        base = dt.datetime.fromisoformat(day + "T00:00:00+00:00")
        for hour in STUDY_HOURS_UTC:
            hour_start = int((base + dt.timedelta(hours=hour)).timestamp())
            for ws in range(hour_start, hour_start + 3600, WINDOW_S):
                res = study_window(ws, opens, args.pause_s)
                if res is None:
                    continue
                if res["status"] != "ok":
                    skipped[res["status"]] = skipped.get(res["status"], 0) + 1
                    continue
                res["day"] = day
                rows.append(res)
        done = [r for r in rows if r["day"] == day]
        print(f"{day}: usable={len(done)}")

    per_day, buckets = [], [(0.0, 0.55), (0.55, 0.75), (0.75, 0.92), (0.92, 1.0)]
    for day in args.days:
        day_rows = [r for r in rows if r["day"] == day]
        entry = {"day": day, "n": len(day_rows)}
        for lo, hi in buckets:
            g = [r for r in day_rows if lo < r["entry_executed_price"] <= hi]
            n, w = len(g), sum(r["won"] for r in g)
            be = sum(r["break_even"] for r in g) / n if n else None
            entry[f"b{lo}_{hi}"] = {
                "n": n, "wins": w,
                "win_rate": round(w / n, 4) if n else None,
                "avg_break_even": round(be, 4) if be is not None else None,
                "point_edge": round(w / n - be, 4) if n and be is not None else None,
                "wilson_lo": round(wilson_lo(w, n), 4) if n else None,
            }
        per_day.append(entry)

    result = {
        "schema_version": 1,
        "registration": "adaptation_persistence_study_20260819",
        "method": "executed-trade entry proxy (data-api), Binance-1s TWAP labels, fixed calendar hours",
        "decision_offset_s": DECISION_OFFSET_S,
        "entry_window_s": ENTRY_WINDOW_S,
        "study_hours_utc": STUDY_HOURS_UTC,
        "days": args.days,
        "usable_rows": len(rows),
        "skipped": skipped,
        "per_day_buckets": per_day,
        "fresh_discipline": "Aug 19+ capture remains the untouched fresh source; this study is diagnostic",
        "rows": rows,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=1)
        f.write("\n")
    print(f"written {args.output}: usable={len(rows)} skipped={skipped}")


if __name__ == "__main__":
    main()
