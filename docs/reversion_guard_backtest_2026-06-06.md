# Reversion Guard Backtest - 2026-06-06

## Scope

This pass was a backtest-first validation loop for the 5 minute BTC candle
strategy after the selected maker cell failed fresh May data. The run used
local dev CPU, atomic PMXT fetch/replay/delete, cached Gamma metadata per fold,
and Polymarket terminal outcomes for PnL where available.

Raw PMXT parquets were deleted after each hour. Temporary sidecar caches lived
under `/private/tmp/polymomentum_a_plus_loop_20260606/` and are session-owned.

## Code Changes

- Added `--maker-only` to `harness-sweep` so maker-only sweeps do not waste work
  on taker twins.
- Added a feed-forward `min_reversion_count` strategy guard and sweep dimension.
- Added rolling-history profile `a_plus5m_reversion_guard`.
- Included `min_reversion_count` and `max_reversion_count` in live/replay zone
  parity checks so runtime artifacts cannot silently ignore the guard.

## Fresh Failure

The prior selected maker cell was rerun over the largest complete post-May-25
block available before the missing `2026-05-30T19:00:00Z` PMXT hour:

`2026-05-26T00:00:00Z` through `2026-05-30T15:00:00Z`, 14 x 8h folds.

Selected maker result:

| Metric | Value |
| --- | ---: |
| Trades | 265 |
| Wins / losses | 209 / 56 |
| Win rate | 78.87% |
| Wilson 95 lower | 73.56% |
| PnL | -3.21784 |
| Avg PnL / trade | -0.01214 |
| Losing folds | 7 / 14 |
| Resolution source | 265 Polymarket terminal |
| Unresolved fills | 0 |
| Circuit breakers | 0 |

Verdict: reject the selected maker cell. The fresh block exposed weak
no-reversion and mid-price entries.

## Guarded Candidate

Implemented guard:

- `min_reversion_count = 1`
- `max_reversion_count = 2`
- `min_price = 0.75`
- `max_price = 0.85`
- `min_confidence = 0.50`
- maker-only with degraded taker fallback after two losses

Fresh May26-May30 result for this guarded candidate:

| Metric | Value |
| --- | ---: |
| Trades | 55 |
| Attempts | 80 |
| Fill rate | 68.75% |
| Wins / losses | 50 / 5 |
| Win rate | 90.91% |
| Wilson 95 lower | 80.42% |
| PnL | +36.39873 |
| Avg PnL / trade | +0.66180 |
| Fees | 0.49537 |
| Losing folds | 4 / 14 |
| Dense folds >= 5 trades | 5 |
| Losing dense folds >= 5 trades | 3 |
| Resolution source | 55 Polymarket terminal |
| Unresolved fills | 0 |
| Circuit breakers | 0 |

This is a real repair versus the failed selected cell, but the sample is still
too sparse for A+ promotion.

## Neighbor Sweep

Neighbor grid:

- `min_confidence in {0.45, 0.50}`
- `min_price in {0.70, 0.75}`
- fixed `z = 0.90`, `edge = 0.07`, `min_reversion_count = 1`,
  `max_reversion_count = 2`

Fresh May26-May30:

| Cell | Trades | Wins / losses | PnL | Wilson 95 | Worst fold |
| --- | ---: | ---: | ---: | ---: | ---: |
| c0.45 p0.75 | 60 | 55 / 5 | +44.77809 | 81.93% | -4.09460 |
| c0.45 p0.70 | 54 | 49 / 5 | +41.63900 | 80.09% | -4.09460 |
| c0.50 p0.70 | 48 | 44 / 4 | +38.62110 | 80.45% | -3.88640 |
| c0.50 p0.75 | 55 | 50 / 5 | +36.39873 | 80.42% | -3.88640 |

Historical May20-May25:

| Cell | Trades | Wins / losses | PnL | Wilson 95 | Worst fold |
| --- | ---: | ---: | ---: | ---: | ---: |
| c0.45 p0.75 | 40 | 35 / 5 | +25.13544 | 73.89% | -14.79856 |
| c0.50 p0.75 | 36 | 32 / 4 | +25.09630 | 74.68% | -5.00000 |
| c0.45 p0.70 | 41 | 35 / 6 | +23.81077 | 71.56% | -11.71663 |
| c0.50 p0.70 | 44 | 37 / 7 | +19.23259 | 70.63% | -8.40191 |

Combined May20-May30:

| Cell | Trades | Wins / losses | PnL | Wilson 95 | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| c0.45 p0.75 | 100 | 90 / 10 | +69.91353 | 82.56% | Highest PnL, but -14.80 tail fold |
| c0.45 p0.70 | 95 | 84 / 11 | +65.44977 | 80.45% | Lower price floor adds tail risk |
| c0.50 p0.75 | 91 | 82 / 9 | +61.49503 | 82.26% | Lower PnL, materially better historical tail |
| c0.50 p0.70 | 92 | 81 / 11 | +57.85369 | 79.85% | Rejected on lower Wilson and lower PnL |

All combined trades above used `polymarket_terminal` resolution, had zero
unresolved fills, and tripped zero circuit breakers.

## Tail Inspection

The `c0.45 p0.75` cell won the fresh block but lost `-14.79856` on historical
fold `2026-05-25T08:00:00Z` to `2026-05-25T15:00:00Z`.

That tail was caused by a cluster of low-confidence early fills around
`confidence = 0.45..0.50` with terminal losses. The stricter `c0.50 p0.75`
cell reduced that fold to `-2.53`.

An initially suspicious fold (`2026-05-24T00:00:00Z` to
`2026-05-24T07:00:00Z`) showed `c0.45` taking no fills while `c0.50` took a
loss. Inspection showed the difference came from stateful prior maker/post-only
attempts, not a timestamp or resolution bug. The maker simulator already uses a
deterministic fill key for identical simulated orders.

## Verdict

Current grade: **B+ research candidate / not A+ production**.

The reversion guard is a real improvement and should remain in the candidate
profile. The lower confidence neighbor is rejected for production because it
improves headline PnL while worsening the historical tail. The safer current
candidate is:

- `a_plus5m_reversion_guard`
- `min_confidence = 0.50`
- `min_price = 0.75`
- `max_price = 0.85`
- `min_reversion_count = 1`
- `max_reversion_count = 2`

Promotion is still blocked by sample size and fold density. Required next gate:

1. Run the guarded profile across more recent complete PMXT blocks using atomic
   fetch/replay/delete.
2. Require at least 626 terminal-resolved trades for Wilson confidence.
3. Require at least 20 dense folds, preferably 30+, with no single fold capable
   of erasing the edge.
4. Compare maker fill sensitivity with deterministic fill keys and a harsher
   maker probability grid before canary.
5. Keep validation backtest/live-replay only until a behavior cannot be proven
   offline.
