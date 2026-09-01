#!/usr/bin/env python3
"""Margin-floor study for the band mechanism (band_v2 candidate).

Question: does requiring |BTC_at_decision - window_open| >= X restore the
band's win rate - i.e., was the 2026-09-01 five-loss streak a chop regime
the mechanism has no defense against?

Three independent legs, all public/owned data:
  A. The 222 original gate rows (Aug 15-21): recompute decision margins
     from Binance 1s klines -> WR by |margin| bucket on the promotion
     evidence itself.
  B. All live trades (session evaluation records carry btc + open at the
     attempt) -> WR by |margin| on out-of-sample live data incl. the streak.
  C. Fresh windows (Aug 22 -> now): Binance margin at 240s + Gamma official
     resolution -> SIGNAL ACCURACY by |margin| (no entry prices needed;
     accuracy is the quantity the floor acts on).

Verdict logic: the floor is validated if high-margin buckets hold their WR
across ALL legs including the most recent days, while low-margin buckets
explain the losses.
"""

import json
import math
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "logs/strategy-research/margin_study_cache"
BINANCE = "https://api.binance.com/api/v3/klines"
GAMMA = "https://gamma-api.polymarket.com"
DECISION_S = 240
BUCKETS = [(0, 25), (25, 50), (50, 75), (75, 100), (100, 10**9)]


def http_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "pm-study/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def wilson_lo(w, n, z=1.959964):
    if n == 0:
        return 0.0
    p = w / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (c - m) / d


def binance_price_series(day_start_ms, cache_key):
    """1s close prices for one UTC day, cached."""
    path = CACHE / f"binance_{cache_key}.json"
    if path.is_file():
        return json.loads(path.read_text())
    prices = {}
    t = day_start_ms
    end = day_start_ms + 86_400_000
    while t < end:
        rows = http_json(
            f"{BINANCE}?symbol=BTCUSDT&interval=1s&startTime={t}&limit=1000"
        )
        if not rows:
            break
        for row in rows:
            prices[str(int(row[0] // 1000))] = float(row[4])
        t = int(rows[-1][0]) + 1000
        time.sleep(0.15)
    path.write_text(json.dumps(prices))
    return prices


def margin_for_window(window_start, day_cache):
    day_key = window_start - (window_start % 86400)
    if day_key not in day_cache:
        day_cache[day_key] = binance_price_series(day_key * 1000, str(day_key))
    prices = day_cache[day_key]
    p_open = prices.get(str(window_start))
    p_dec = prices.get(str(window_start + DECISION_S))
    if p_open is None or p_dec is None:
        return None, None
    return p_dec - p_open, p_open


def bucket_label(margin):
    a = abs(margin)
    for lo, hi in BUCKETS:
        if lo <= a < hi:
            return f"{lo}-{hi if hi < 10**9 else 'inf'}"
    return "?"


def tally(rows, key_won="won"):
    out = {}
    for row in rows:
        b = bucket_label(row["margin"])
        out.setdefault(b, [0, 0])
        out[b][1] += 1
        if row[key_won]:
            out[b][0] += 1
    return out


def print_tally(name, buckets):
    print(f"\n--- {name}")
    for lo, hi in BUCKETS:
        label = f"{lo}-{hi if hi < 10**9 else 'inf'}"
        w, n = buckets.get(label, (0, 0))
        if n:
            print(
                f"  |margin| ${label:>8}: {w:>3}/{n:<3} = {w/n:5.1%}  wilson_lo {wilson_lo(w,n):.3f}"
            )


def main():
    CACHE.mkdir(parents=True, exist_ok=True)
    day_cache = {}

    # Leg A: original gate rows
    gate = json.loads((ROOT / "logs/strategy-research/20260821_fresh_gate_public_v1.json").read_text())
    leg_a = []
    for row in gate["rows"]:
        margin, _ = margin_for_window(int(row["window_start"]), day_cache)
        if margin is None:
            continue
        leg_a.append({"margin": margin, "won": bool(row["won"])})
    print(f"leg A rows: {len(leg_a)}/{len(gate['rows'])}")
    print_tally("A: gate evidence (Aug 15-21), WR by |margin|", tally(leg_a))

    # Leg B: live trades passed in via a JSON file argument
    if len(sys.argv) > 1:
        live = json.loads(Path(sys.argv[1]).read_text())
        print(f"\nleg B rows: {len(live)}")
        print_tally("B: LIVE trades (new wallet), WR by |margin|", tally(live))

    # Leg C: fresh signal accuracy Aug 22 -> now
    start = 1787788800  # 2026-08-25 00:00 UTC (recent regime)
    end = int(time.time()) - 900
    leg_c = []
    outcomes_cache = CACHE / "gamma_outcomes.json"
    outcomes = json.loads(outcomes_cache.read_text()) if outcomes_cache.is_file() else {}
    fetched = 0
    for ws in range(start, end, 300):
        margin, _ = margin_for_window(ws, day_cache)
        if margin is None or abs(margin) < 1e-9:
            continue
        key = str(ws)
        if key not in outcomes:
            try:
                markets = http_json(f"{GAMMA}/markets?slug=btc-updown-5m-{ws}&closed=true")
            except Exception:
                continue
            fetched += 1
            if fetched % 40 == 0:
                time.sleep(2)
            m = markets[0] if markets else {}
            if m.get("umaResolutionStatus") != "resolved":
                outcomes[key] = None
            else:
                prices = m.get("outcomePrices")
                prices = json.loads(prices) if isinstance(prices, str) else prices
                names = m.get("outcomes")
                names = json.loads(names) if isinstance(names, str) else names
                winners = [str(o).lower() for o, p in zip(names, prices) if float(p) > 0.5]
                outcomes[key] = winners[0] if len(winners) == 1 else None
            if fetched % 200 == 0:
                outcomes_cache.write_text(json.dumps(outcomes))
        official = outcomes.get(key)
        if official not in ("up", "down"):
            continue
        signal = "up" if margin > 0 else "down"
        leg_c.append({"margin": margin, "correct": signal == official, "ws": ws})
    outcomes_cache.write_text(json.dumps(outcomes))
    print(f"\nleg C rows: {len(leg_c)} (windows since Aug 25)")
    print_tally("C: fresh SIGNAL ACCURACY by |margin|", tally(leg_c, key_won="correct"))
    # recent-days slice: the suspected chop regime
    recent = [r for r in leg_c if r["ws"] >= end - 2 * 86400]
    print(f"\nleg C recent-48h rows: {len(recent)}")
    print_tally("C': last 48h SIGNAL ACCURACY by |margin|", tally(recent, key_won="correct"))
    print("\nDONE")


if __name__ == "__main__":
    main()
