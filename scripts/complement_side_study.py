#!/usr/bin/env python3
"""Complement-side study under OFFICIAL labels, on DISJOINT calendar hours.

The falsified proxy study showed the cheap SIGNAL side winning 17.6%
officially while priced ~31% — mirror arithmetic suggests the COMPLEMENT
side may be the underpriced one. This study measures it honestly:

- DISJOINT sample: hours 03/07/11/15/19/23 UTC (the prior study used
  01/05/09/13/17/21) — same era, zero window overlap, so this is
  discovery-grade, not a third reuse of the same windows;
- labels: OFFICIAL Gamma resolutions only (no proxy anywhere);
- entries: first executed BUY print per token in [decision, decision+30s]
  (third-party executions = proven executability), recorded for BOTH the
  signal side and the complement side;
- signal: sign(BTC@240s − open) from checksum-verified Binance 1s opens.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import time
import urllib.parse
import urllib.request
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent))
from adaptation_persistence_study import http_json, load_opens, taker_fee  # noqa: E402

GAMMA = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
DECISION_OFFSET_S = 240
ENTRY_WINDOW_S = 30
WINDOW_S = 300
STUDY_HOURS_UTC = [3, 7, 11, 15, 19, 23]  # disjoint from 01/05/09/13/17/21
Z95 = 1.959_963_984_540_054


def wilson_lo(w: int, n: int) -> float:
    if n == 0:
        return 0.0
    p = w / n
    den = 1 + Z95 * Z95 / n
    c = p + Z95 * Z95 / (2 * n)
    r = Z95 * math.sqrt((p * (1 - p) + Z95 * Z95 / (4 * n)) / n)
    return max(0.0, (c - r) / den)


def study_window(ws: int, opens: dict[int, float], pause_s: float) -> dict | None:
    p_open, p_dec = opens.get(ws), opens.get(ws + DECISION_OFFSET_S)
    if p_open is None or p_dec is None or p_dec == p_open:
        return None
    signal = "up" if p_dec > p_open else "down"
    complement = "down" if signal == "up" else "up"

    slug = f"btc-updown-5m-{ws}"
    markets = http_json(f"{GAMMA}/markets?slug={slug}&closed=true")
    time.sleep(pause_s)
    if not markets:
        return {"window_start": ws, "status": "market_not_found"}
    m = markets[0]
    if m.get("umaResolutionStatus") != "resolved":
        return {"window_start": ws, "status": "unresolved"}
    outcomes = json.loads(m["outcomes"]) if isinstance(m.get("outcomes"), str) else m["outcomes"]
    prices = json.loads(m["outcomePrices"]) if isinstance(m.get("outcomePrices"), str) else m["outcomePrices"]
    tokens = json.loads(m["clobTokenIds"]) if isinstance(m.get("clobTokenIds"), str) else m["clobTokenIds"]
    condition_id = m.get("conditionId")
    if not (outcomes and prices and tokens and condition_id):
        return {"window_start": ws, "status": "identity_incomplete"}
    by_name = {str(o).lower(): (t, float(p)) for o, t, p in zip(outcomes, tokens, prices)}
    winners = [name for name, (_, p) in by_name.items() if p > 0.5]
    if len(winners) != 1:
        return {"window_start": ws, "status": "no_unique_winner"}
    official = winners[0]

    params = urllib.parse.urlencode({"market": condition_id, "limit": 500})
    trades = http_json(f"{DATA_API}/trades?{params}")
    time.sleep(pause_s)
    decision_ts = ws + DECISION_OFFSET_S
    first_buy: dict[str, float] = {}
    for t in sorted(trades or [], key=lambda x: x.get("timestamp", 0)):
        if t.get("side") != "BUY":
            continue
        ts = int(t.get("timestamp", 0))
        if not (decision_ts <= ts <= decision_ts + ENTRY_WINDOW_S):
            continue
        asset = t.get("asset")
        for name, (token, _) in by_name.items():
            if asset == token and name not in first_buy:
                first_buy[name] = float(t["price"])
    return {
        "window_start": ws,
        "status": "ok",
        "signal": signal,
        "official": official,
        "signal_entry": first_buy.get(signal),
        "complement_entry": first_buy.get(complement),
        "won_signal": official == signal,
        "won_complement": official == complement,
    }


def bucketize(rows, price_key, won_key):
    out = {}
    for lo, hi in [(0.0, 0.55), (0.55, 0.75), (0.75, 0.92), (0.92, 1.0)]:
        g = [r for r in rows if r.get(price_key) is not None and lo < r[price_key] <= hi]
        n = len(g)
        w = sum(r[won_key] for r in g)
        be = sum(r[price_key] + taker_fee(r[price_key]) for r in g) / n if n else None
        out[f"({lo}, {hi}]"] = {
            "n": n, "wins": w,
            "win_rate": round(w / n, 4) if n else None,
            "avg_break_even": round(be, 4) if be is not None else None,
            "point_edge": round(w / n - be, 4) if n and be is not None else None,
            "wilson_lo": round(wilson_lo(w, n), 4) if n else None,
            "wilson_edge": round(wilson_lo(w, n) - be, 4) if n and be is not None else None,
        }
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip-dir", default="data/binance_1s_twap_era")
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--pause-s", type=float, default=0.12)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    opens = load_opens(Path(args.zip_dir), args.days)
    rows, skipped = [], {}
    for day in args.days:
        base = dt.datetime.fromisoformat(day + "T00:00:00+00:00")
        for hour in STUDY_HOURS_UTC:
            hs = int((base + dt.timedelta(hours=hour)).timestamp())
            for ws in range(hs, hs + 3600, WINDOW_S):
                res = study_window(ws, opens, args.pause_s)
                if res is None:
                    continue
                if res["status"] != "ok":
                    skipped[res["status"]] = skipped.get(res["status"], 0) + 1
                    continue
                res["day"] = day
                rows.append(res)
        print(f"{day}: rows={sum(1 for r in rows if r['day'] == day)}")

    result = {
        "schema_version": 1,
        "registration": "complement_side_official_study_20260819",
        "sample": "DISJOINT hours 03/07/11/15/19/23 UTC; labels = official Gamma resolutions only",
        "days": args.days,
        "usable_rows": len(rows),
        "skipped": skipped,
        "signal_side_by_entry_bucket": bucketize(rows, "signal_entry", "won_signal"),
        "complement_side_by_entry_bucket": bucketize(rows, "complement_entry", "won_complement"),
        "rows": rows,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=1)
        f.write("\n")
    print(json.dumps({k: result[k] for k in
                      ["usable_rows", "skipped",
                       "complement_side_by_entry_bucket"]}, indent=1)[:800])


if __name__ == "__main__":
    main()
