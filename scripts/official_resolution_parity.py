#!/usr/bin/env python3
"""Official-resolution parity audit — the free replacement for the paid
Chainlink historical API.

For every usable window of the adaptation study, fetch the market's OFFICIAL
resolution (Gamma outcomePrices of the closed market — the venue's own
settlement result) and compare it against the study's Binance-1s TWAP label.
This simultaneously:
  1. quantifies the Binance-proxy error on 500+ windows (the "official
     settlement parity" gate, previously assigned to a paid Chainlink
     backfill);
  2. recomputes the candidate's discovery metrics under OFFICIAL labels;
  3. proves the free label pipeline the fresh gate will use.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request

GAMMA = "https://gamma-api.polymarket.com"


def http_json(url: str, retries: int = 3):
    req = urllib.request.Request(
        url, headers={"User-Agent": "polymomentum-research/1.0", "Accept": "application/json"}
    )
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(1.5 * (attempt + 1))


def official_winner(window_start: int) -> str | None:
    slug = f"btc-updown-5m-{window_start}"
    markets = http_json(f"{GAMMA}/markets?slug={slug}&closed=true")
    if not markets:
        return None
    m = markets[0]
    if m.get("umaResolutionStatus") != "resolved":
        return None
    outcomes = json.loads(m["outcomes"]) if isinstance(m.get("outcomes"), str) else m.get("outcomes")
    prices = json.loads(m["outcomePrices"]) if isinstance(m.get("outcomePrices"), str) else m.get("outcomePrices")
    if not outcomes or not prices or len(outcomes) != len(prices):
        return None
    winners = [str(o).lower() for o, p in zip(outcomes, prices) if float(p) > 0.5]
    return winners[0] if len(winners) == 1 else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--study", default="deploy/promotions/evidence/strategy_registry/20260819_adaptation_persistence_study.json")
    ap.add_argument("--pause-s", type=float, default=0.12)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    study = json.load(open(args.study))
    rows = study["rows"]
    parity_rows, unresolved = [], 0
    agree = disagree = 0
    for i, r in enumerate(rows):
        ws = r["window_start"]
        # Binance-TWAP outcome implied by the study row.
        signal = r["signal"]
        binance_outcome = signal if r["won"] else ("down" if signal == "up" else "up")
        official = official_winner(ws)
        time.sleep(args.pause_s)
        if official is None:
            unresolved += 1
            continue
        match = official == binance_outcome
        agree += match
        disagree += not match
        parity_rows.append({
            "window_start": ws,
            "day": r["day"],
            "signal": signal,
            "entry_executed_price": r["entry_executed_price"],
            "break_even": r["break_even"],
            "binance_twap_outcome": binance_outcome,
            "official_outcome": official,
            "labels_agree": match,
            "won_official": official == signal,
        })
        if (i + 1) % 100 == 0:
            print(f"{i + 1}/{len(rows)} agree={agree} disagree={disagree}")

    # Candidate metrics under OFFICIAL labels.
    import math
    def wilson_lo(w: int, n: int) -> float:
        if n == 0:
            return 0.0
        z = 1.959963984540054
        p = w / n
        den = 1 + z * z / n
        c = p + z * z / (2 * n)
        rad = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
        return max(0.0, (c - rad) / den)

    cheap = [r for r in parity_rows if r["entry_executed_price"] <= 0.55]
    n, w = len(cheap), sum(r["won_official"] for r in cheap)
    be = sum(r["break_even"] for r in cheap) / n if n else None
    official_cheap = {
        "n": n,
        "wins": w,
        "win_rate": round(w / n, 4) if n else None,
        "avg_break_even": round(be, 4) if be is not None else None,
        "point_edge": round(w / n - be, 4) if n else None,
        "wilson_lower": round(wilson_lo(w, n), 4) if n else None,
        "wilson_edge": round(wilson_lo(w, n) - be, 4) if n else None,
    }

    total = agree + disagree
    result = {
        "schema_version": 1,
        "registration": "official_resolution_parity_20260819",
        "study": args.study,
        "windows_checked": total,
        "unresolved_or_missing": unresolved,
        "labels_agree": agree,
        "labels_disagree": disagree,
        "parity_rate": round(agree / total, 5) if total else None,
        "disagreements": [r for r in parity_rows if not r["labels_agree"]],
        "cheap_bucket_under_official_labels": official_cheap,
        "conclusion_free_label_pipeline": "official Gamma resolutions are the venue's own settlement result; no paid Chainlink history required for hold-to-expiry labels",
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=1)
        f.write("\n")
    print(json.dumps({k: result[k] for k in
                      ["windows_checked", "parity_rate", "labels_disagree",
                       "cheap_bucket_under_official_labels"]}, indent=1))


if __name__ == "__main__":
    main()
