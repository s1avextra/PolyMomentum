#!/usr/bin/env python3
"""Power analysis of the +0.02 Wilson-edge advancement gate.

The v3 funnel advances a replayed trace only when
    wilson_lower(wins, fills; z=1.959964) - avg_break_even > 0.02  and  PnL > 0
(rust_engine/src/strategy_builder/opportunity_replay.rs:781-789).

This script answers, without touching any outcome data:
  1. the minimal win count W*(n, b, m) required to pass at support n,
     average break-even b, margin m;
  2. statistical power P[pass | true win prob p = b + e] over a grid of
     support n and true edge e;
  3. the support required for 80% power at each (b, e);
  4. a research-only re-read of the eight August 2026 exact-replay traces
     under alternative margins/confidences (their terminal decisions stand;
     tombstones are NOT reopened by this script).

Formula parity with the Rust implementation is asserted against the recorded
wilson_edge values of the eight traces before anything else is computed.

Output: JSON evidence to stdout or --output.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass

Z_9750 = 1.959_963_984_540_054  # two-sided 95% — the production gate
Z_9000 = 1.281_551_565_544_8    # one-sided 90% — alternative examined


def wilson_lower(wins: int, support: int, z: float = Z_9750) -> float:
    if support == 0:
        return 0.0
    n = float(support)
    p = wins / n
    denom = 1.0 + z * z / n
    centre = p + z * z / (2.0 * n)
    radius = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * n)) / n)
    return max(0.0, min(1.0, (centre - radius) / denom))


def min_wins_to_pass(n: int, b: float, margin: float, z: float = Z_9750) -> int | None:
    """Smallest W with wilson_lower(W, n, z) - b > margin, or None."""
    for w in range(n + 1):
        if wilson_lower(w, n, z) - b > margin:
            return w
    return None


def log_binom_pmf(n: int, k: int, p: float) -> float:
    if p <= 0.0:
        return 0.0 if k == 0 else -math.inf
    if p >= 1.0:
        return 0.0 if k == n else -math.inf
    return (
        math.lgamma(n + 1)
        - math.lgamma(k + 1)
        - math.lgamma(n - k + 1)
        + k * math.log(p)
        + (n - k) * math.log1p(-p)
    )


def binom_sf(n: int, k_min: int, p: float) -> float:
    """P[X >= k_min] for X ~ Bin(n, p)."""
    if k_min <= 0:
        return 1.0
    if k_min > n:
        return 0.0
    return sum(math.exp(log_binom_pmf(n, k, p)) for k in range(k_min, n + 1))


def power(n: int, b: float, e: float, margin: float, z: float = Z_9750) -> float:
    w_star = min_wins_to_pass(n, b, margin, z)
    if w_star is None:
        return 0.0
    return binom_sf(n, w_star, min(b + e, 1.0))


def support_for_power(b: float, e: float, margin: float, target: float,
                      z: float = Z_9750, n_max: int = 5000) -> int | None:
    n = 10
    while n <= n_max:
        if power(n, b, e, margin, z) >= target:
            return n
        n += 10 if n < 200 else 50
    return None


@dataclass
class Trace:
    family: str
    label: str
    fills: int
    wins: int
    point_edge: float
    recorded_wilson_edge: float

    @property
    def break_even(self) -> float:
        return self.wins / self.fills - self.point_edge


# The eight August 2026 exact-replay traces
# (docs/strategy_finding_architecture_v3_2026-08-08.md, sections dated 08-11).
TRACES = [
    Trace("late_window_path_min", "120s ask<=0.85", 20, 16, 0.14350, -0.07252),
    Trace("late_window_path_min", "240s ask<=1.00 down", 23, 22, 0.12545, -0.04099),
    Trace("late_window_path_exp", "180s ask<=0.90", 28, 23, 0.10544, -0.07190),
    Trace("late_window_path_exp", "120s ask<=0.90", 28, 20, 0.08390, -0.10098),
    Trace("causal_probability", "180s ask<=0.85", 32, 25, 0.17152, 0.00272),
    Trace("causal_probability", "120s ask<=0.90", 37, 26, 0.07720, -0.08333),
    Trace("paired_liquidity", "gap>=0.5 180s", 65, 37, 0.04192, -0.07899),
    Trace("paired_liquidity", "gap>=1.0 180s", 61, 34, 0.03389, -0.09046),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default=None)
    args = ap.parse_args()

    # 0. Formula parity with the Rust gate, asserted on all eight traces.
    parity = []
    for t in TRACES:
        we = wilson_lower(t.wins, t.fills) - t.break_even
        drift = abs(we - t.recorded_wilson_edge)
        parity.append({"family": t.family, "label": t.label,
                       "recomputed_wilson_edge": round(we, 5),
                       "recorded_wilson_edge": t.recorded_wilson_edge,
                       "abs_drift": round(drift, 6)})
        assert drift < 5e-4, f"formula parity broken on {t.label}: {we} vs {t.recorded_wilson_edge}"

    break_evens = [0.75, 0.80, 0.85, 0.90]
    edges = [0.02, 0.03, 0.05, 0.08, 0.10, 0.15]
    supports = [20, 30, 40, 65, 100, 150, 250, 500]

    # 1-2. Power of the production gate over the grid.
    power_grid = []
    for b in break_evens:
        for e in edges:
            row = {"break_even": b, "true_edge": e,
                   "power_by_support": {
                       str(n): round(power(n, b, e, 0.02), 4) for n in supports}}
            power_grid.append(row)

    # 3. Support needed for 80% power, production gate vs alternatives.
    required = []
    for b in break_evens:
        for e in edges:
            required.append({
                "break_even": b, "true_edge": e,
                "n_for_80pct_margin_0.02_z95": support_for_power(b, e, 0.02, 0.80),
                "n_for_80pct_margin_0.00_z95": support_for_power(b, e, 0.00, 0.80),
                "n_for_80pct_margin_0.00_z90": support_for_power(b, e, 0.00, 0.80, Z_9000),
            })

    # 4. Research-only re-read of the August traces under alternative gates.
    reread = []
    for t in TRACES:
        b = t.break_even
        reread.append({
            "family": t.family, "label": t.label,
            "fills": t.fills, "wins": t.wins,
            "break_even": round(b, 5),
            "point_edge": t.point_edge,
            "passes_margin_0.02_z95": wilson_lower(t.wins, t.fills) - b > 0.02,
            "passes_margin_0.00_z95": wilson_lower(t.wins, t.fills) - b > 0.0,
            "passes_margin_0.00_z90": wilson_lower(t.wins, t.fills, Z_9000) - b > 0.0,
            "fills_for_80pct_power_at_observed_point_edge":
                support_for_power(b, t.point_edge, 0.02, 0.80),
        })

    # Aggregate sign test across the eight traces (correlated: shared hours,
    # post-screen selection — stated in caveats, not corrected here).
    all_positive_p = 0.5 ** len(TRACES)

    result = {
        "schema_version": 1,
        "registration": "wilson_gate_power_analysis_20260817",
        "gate_definition": "wilson_lower(z=1.959964) - avg_break_even > 0.02 and pnl > 0",
        "formula_parity": parity,
        "power_grid": power_grid,
        "support_required_for_80pct_power": required,
        "august_traces_reread_research_only": reread,
        "aggregate_sign_test": {
            "positive_point_edges": len(TRACES),
            "of": len(TRACES),
            "p_all_positive_under_null_if_independent": all_positive_p,
            "caveat": "traces share discovery hours and are post-screen selected; "
                      "this is NOT a valid independent p-value, only a direction indicator",
        },
        "terminal_decisions_unchanged": True,
    }
    payload = json.dumps(result, indent=1)
    if args.output:
        with open(args.output, "w") as f:
            f.write(payload + "\n")
        print(f"written {args.output}")
    else:
        print(payload)


if __name__ == "__main__":
    main()
