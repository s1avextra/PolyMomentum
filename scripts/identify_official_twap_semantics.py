#!/usr/bin/env python3
"""Empirically identify the official TWAP resolution semantics.

The rule text is ambiguous ("the TWAP ... of the time range ... vs the
price at the beginning of that range", resolution source = the
btc-usd-twap-60s STREAM). Candidate formulas over Binance 1s opens are
parity-ranked against 510 OFFICIAL resolutions. The formula that agrees
~99%+ is the de-facto official semantics.

Candidates (s(t) = spot open at second t, T60(t) = mean s over [t-60, t)):
  A  mean s[0,300)        vs s(0)          — the original proxy (81.4%)
  B  mean s[0,300)        vs T60(0)        — stream baseline
  C  mean T60 over [0,300) vs T60(0)       — stream averaged over range
  D  T60(300)             vs T60(0)        — stream at end vs start
  E  mean s[0,300]        vs s(0), close-inclusive
  F  T60(300)             vs s(0)
  G  mean s[240,300)      vs s(0)          — last-minute avg vs open
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent))
from build_twap_era_inputs import load_1s_opens  # noqa: E402


def t60(opens: dict[int, float], t: int) -> float | None:
    xs = [opens.get(s) for s in range(t - 60, t)]
    if any(x is None for x in xs):
        return None
    return sum(xs) / len(xs)


def mean_spot(opens, a, b):
    xs = [opens.get(s) for s in range(a, b)]
    if any(x is None for x in xs):
        return None
    return sum(xs) / len(xs)


def mean_t60(opens, a, b, step=1):
    xs = []
    for t in range(a, b, step):
        v = t60(opens, t)
        if v is None:
            return None
        xs.append(v)
    return sum(xs) / len(xs)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parity", default="deploy/promotions/evidence/strategy_registry/20260819_official_resolution_parity.json")
    ap.add_argument("--zip-dir", default="data/binance_1s_twap_era")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    parity = json.load(open(args.parity))
    # Reconstruct official outcomes per window from the audit (agree rows =
    # binance outcome, disagree rows recorded both sides).
    officials: dict[int, str] = {}
    study = json.load(open("deploy/promotions/evidence/strategy_registry/20260819_adaptation_persistence_study.json"))
    disagreements = {d["window_start"]: d["official_outcome"] for d in parity["disagreements"]}
    for r in study["rows"]:
        ws = r["window_start"]
        signal = r["signal"]
        binance_outcome = signal if r["won"] else ("down" if signal == "up" else "up")
        officials[ws] = disagreements.get(ws, binance_outcome)

    days = sorted({r["day"] for r in study["rows"]})
    opens = load_1s_opens([Path(args.zip_dir) / f"BTCUSDT-1s-{d}.zip" for d in days])

    def outcome(value: float | None, baseline: float | None) -> str | None:
        if value is None or baseline is None:
            return None
        return "up" if value >= baseline else "down"

    formulas = {
        "A_meanspot_vs_spot0": lambda ws: outcome(mean_spot(opens, ws, ws + 300), opens.get(ws)),
        "B_meanspot_vs_t60_0": lambda ws: outcome(mean_spot(opens, ws, ws + 300), t60(opens, ws)),
        "C_meant60_vs_t60_0": lambda ws: outcome(mean_t60(opens, ws, ws + 300, 5), t60(opens, ws)),
        "D_t60end_vs_t60_0": lambda ws: outcome(t60(opens, ws + 300), t60(opens, ws)),
        "E_meanspot_incl_vs_spot0": lambda ws: outcome(mean_spot(opens, ws, ws + 301), opens.get(ws)),
        "F_t60end_vs_spot0": lambda ws: outcome(t60(opens, ws + 300), opens.get(ws)),
        "G_lastmin_vs_spot0": lambda ws: outcome(mean_spot(opens, ws + 240, ws + 300), opens.get(ws)),
    }

    scores = {}
    for name, f in formulas.items():
        agree = total = 0
        for ws, official in officials.items():
            predicted = f(ws)
            if predicted is None:
                continue
            total += 1
            agree += predicted == official
        scores[name] = {"n": total, "agree": agree,
                        "parity": round(agree / total, 5) if total else None}

    ranked = sorted(scores.items(), key=lambda kv: -(kv[1]["parity"] or 0))
    result = {
        "schema_version": 1,
        "registration": "official_twap_semantics_identification_20260819",
        "windows": len(officials),
        "scores": dict(ranked),
        "best": ranked[0][0],
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=1)
        f.write("\n")
    for name, s in ranked:
        print(f"{name:28s} parity={s['parity']} (n={s['n']})")


if __name__ == "__main__":
    main()
