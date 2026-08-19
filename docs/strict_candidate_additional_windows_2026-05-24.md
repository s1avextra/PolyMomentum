# Strict Candidate Additional Windows - 2026-05-24

Scope: follow-up validation for the strict early-guard candidate created on
2026-05-24:

`deploy/promotions/promotion_candidate_early_guard_c040_z070_e003_cap15_20260523_24.json`

Candidate hash:

`615603bd20242aa4c58332d3d9bb7c10c871f7215f5cac6455188e6c8ab73cfb`

Shape:

`early_c0.40_z0.70_e0.03_ev-1.00_p0.10-0.75_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk`

The work ran on the local dev box using temporary PMXT caches under
`/private/tmp/polymomentum_gate_20260524T0539Z` and
`/private/tmp/polymomentum_gate2_20260524T0545Z`. The VPS and other bots were
not used for CPU-heavy validation. Temporary raw PMXT/eval caches were deleted
after preserving the small L2 report evidence.

## Window 1

UTC window: 2026-05-21T03:00:00Z..2026-05-22T02:00:00Z

Feed-forward eval-cache:

- PMXT hours: 24
- Contracts: 288
- Events scanned: 24,465,801
- Evaluations: 749,066
- Resolutions: 288

Fast sweep comparison:

- Active-style `c0.30/z0.50/max_price=0.90`: 96 trades, 74 wins, 22 losses, +74.57
- Strict candidate `c0.40/z0.70/max_price=0.75`: 44 trades, 34 wins, 10 losses, +32.43

Full L2 harness-sweep for strict candidate:

- Fills: 53
- Attempts: 55
- Fill rate: 96.4%
- Failed maker attempts: 2
- Wins/losses: 41 / 12
- Win rate: 77.4%
- PnL: +59.59
- PnL/trade: +1.124
- Breaker: no

Window 1 passes as a positive, sufficiently sampled holdout. It also shows the
strict candidate is not always the highest-PnL shape; it is a risk filter, not
a universal improvement.

## Window 2

UTC window: 2026-05-22T03:00:00Z..2026-05-23T02:00:00Z

Feed-forward eval-cache:

- PMXT hours: 24
- Contracts: 288
- Events scanned: 9,554,044
- Evaluations: 176,934
- Resolutions: 288

Fast sweep comparison:

- Active-style `c0.30/z0.50/max_price=0.90`: 18 trades, 16 wins, 2 losses, +16.87
- Strict candidate `c0.40/z0.70/max_price=0.75`: 11 trades, 10 wins, 1 loss, +13.45

Full L2 harness-sweep for strict candidate:

- Fills: 20
- Attempts: 21
- Fill rate: 95.2%
- Failed maker attempts: 1
- Wins/losses: 16 / 4
- Win rate: 80.0%
- PnL: +18.61
- PnL/trade: +0.930
- Breaker: no

Window 2 is profitable and mechanically clean, but it misses the 30-trade
statistical floor. Treat it as directional evidence only.

## Window 3

UTC window: 2026-05-20T03:00:00Z..2026-05-21T02:00:00Z

Feed-forward eval-cache:

- PMXT hours: 24
- Contracts: 288
- Events scanned: 49,866,720
- Evaluations: 1,460,672
- Resolutions: 288

Fast sweep comparison:

- Active-style `c0.30/z0.50/max_price=0.90`: 184 trades, 130 wins, 54 losses, +37.15
- Strict candidate `c0.40/z0.70/max_price=0.75`: 111 trades, 80 wins, 31 losses, +13.62

Full L2 harness-sweep for strict candidate:

- Evidence JSON: `deploy/promotions/evidence/harness_sweep_strict_candidate_window3_20260520_21.json`
- Fills: 101
- Attempts: 112
- Fill rate: 90.2%
- Failed maker attempts: 11
- Wins/losses: 68 / 33
- Win rate: 67.3%
- PnL: +4.71
- PnL/trade: +0.047
- Breaker: yes
- Breaker reason: `open_exposure_stress`
- Breaker time: 2026-05-21T00:45:42Z
- Realized/stressed drawdown at breaker: 26.3%

Window 3 is the veto fold. It is barely profitable before/at breaker, but the
breaker trip means this candidate is not A+ production-ready.

## Decision

Do not aggregate-promote the strict candidate yet.

The candidate is positive across the original current day plus three additional
non-overlapping windows, but the evidence does not meet A+ promotion quality:

- Window 2 had too few strict-candidate fills for the 30-trade statistical
  floor.
- Window 3 tripped the circuit breaker on open-exposure stress.

The blocker is now regime robustness and drawdown control, not CLOB plumbing.
The order lifecycle remains mechanically clean: the failures observed in these
folds are passive maker misses or post-only crosses, not critical execution
errors.

Next production-grade gate:

- Add a drawdown-aware filter or exposure throttle that avoids the Window 3
  stress regime without using future data for entries.
- Re-run the full feed-forward/L2 sequence over Window 3 plus the current-day
  and Window 1 holdouts.
- Only run aggregate promotion after every retained fold has enough fills, no
  breaker trip, and positive post-fee PnL.
- Keep paper mode only for venue plumbing; offline backtest/live-replay remains
  the validation path.

## Follow-up Completed

Implemented and validated a feed-forward projected stressed-drawdown cap. See
`docs/stress_drawdown_cap_validation_2026-05-24.md`.

At `max_projected_stressed_drawdown_pct=0.24`, the former Window 3 veto fold is
breaker-free and positive:

- Fills: 98
- Wins/losses: 67 / 31
- PnL: +7.92
- Breaker: no

Aggregate promotion across Window 3, Window 1, and current-day passed with:

- Fills: 208
- Wins/losses: 154 / 54
- PnL: +121.36
- Breakers: 0 / 3
- Artifact: `deploy/promotions/promotion_candidate_stresscap024_aggregate_20260520_24.json`
