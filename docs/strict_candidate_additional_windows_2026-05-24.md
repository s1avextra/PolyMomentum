# Strict Candidate Additional Windows - 2026-05-24

Scope: follow-up validation for the strict early-guard candidate created on
2026-05-24:

`deploy/promotions/promotion_candidate_early_guard_c040_z070_e003_cap15_20260523_24.json`

Candidate hash:

`615603bd20242aa4c58332d3d9bb7c10c871f7215f5cac6455188e6c8ab73cfb`

Shape:

`early_c0.40_z0.70_e0.03_ev-1.00_p0.10-0.75_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk`

The work ran on the local dev box using temporary PMXT caches under
`/private/tmp/polymomentum_gate_20260524T0539Z`. The VPS and other bots were
not used for CPU-heavy validation.

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

## Decision

Do not aggregate-promote the strict candidate yet.

The candidate is positive across the original current day plus two additional
non-overlapping windows, and no breaker or order-lifecycle issue appeared in
the exact L2 harness. The blocker is evidence quality, not mechanics: one of
the added 24h windows had too few strict-candidate trades for A+ promotion.

Next production-grade gate:

- Collect one or two more non-overlapping 24h windows with at least 30 strict
  candidate fills each, or widen the evaluation horizon until each fold reaches
  the statistical floor without using future data for entries.
- Only then run aggregate promotion across the retained L2 harness reports.
- Keep paper mode only for venue plumbing; offline backtest/live-replay remains
  the validation path.
