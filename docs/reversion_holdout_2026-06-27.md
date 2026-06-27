# Reversion 1_2 Holdout Gate - 2026-06-27

## Scope

Fresh atomic PMXT rolling-history holdout for the `a_plus5m_causal_guard_selected`
profile with `require-causal-tag reversion=1_2`.

- Window: 2026-05-28T00:00:00Z through 2026-06-06T23:00:00Z.
- Fold size: 8 hours, 30 full folds.
- Bankroll: 100 USD.
- Latency model: 50 ms.
- Mode: feed-forward backtest/live-replay style harness, no paper validation.
- Storage mode: `--atomic-parquet --delete-after-process --max-cache-gb 8`.
- Artifacts: `/private/tmp/polymomentum_reversion_holdout_20260627_may28_jun06/`.
- Temporary cache: `/private/tmp/polymomentum_reversion_holdout_20260627_cache/`.

## Result

Promotion failed. The candidate is profitable but not A+ production-grade.

Final zone-audit totals:

- Trades: 178, below the 200-trade gate.
- Wins/losses: 152 / 26.
- Win rate: 85.39%.
- Wilson lower win-rate bound: 79.46%.
- Net PnL: +73.80652 USD.
- Fees: 13.91578 USD.
- Zone split:
  - Early: 108 trades, +64.56972 PnL, 60.67% trade share.
  - Primary: 70 trades, +9.23680 PnL, 39.33% trade share.

Robust promotion rejection reasons:

- Trades 178 below minimum 200.
- Worst fold/window PnL -8.51781 below required 0.
- Neighbor count 0 below required 2.
- Neighbor positive rate 0.0 below required 0.6.
- Robust score -0.0745 below required 0.
- Offending causal bucket: `regime=zone=early|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=lt_0.50|vol=lt_0.40|rev=1_2|min=2_4`, 12 trades, -6.47638 PnL.

## Losing Folds

| Start UTC | End UTC | Trades | W/L | PnL | Dominant zone |
|---|---:|---:|---:|---:|---|
| 2026-05-28T16:00 | 2026-05-28T23:00 | 8 | 6/2 | -3.74350 | primary |
| 2026-05-31T08:00 | 2026-05-31T15:00 | 9 | 6/3 | -8.51781 | primary |
| 2026-06-01T00:00 | 2026-06-01T07:00 | 3 | 2/1 | -3.54842 | early |
| 2026-06-01T08:00 | 2026-06-01T15:00 | 8 | 6/2 | -2.24524 | early |
| 2026-06-01T16:00 | 2026-06-01T23:00 | 3 | 2/1 | -2.18375 | early |
| 2026-06-03T16:00 | 2026-06-03T23:00 | 8 | 6/2 | -3.70303 | primary |
| 2026-06-06T00:00 | 2026-06-06T07:00 | 5 | 3/2 | -5.67420 | primary |
| 2026-06-06T16:00 | 2026-06-06T23:00 | 4 | 2/2 | -7.67995 | early |

## Selectivity Search

Ran feed-forward `strategy-builder selectivity-search` over the 30 reports with:

- `--min-train-reports 6`
- `--min-train-trades 40`
- `--min-oos-trades 40`
- `--min-oos-wilson-win-rate-lower 0.60`
- `--min-oos-profitable-reports 10`
- `--min-worst-oos-pnl 0`

Result: no candidate passed. The best single deny rule improved aggregate PnL but
still had negative worst OOS fold PnL.

Aggregate worst compound buckets with at least 5 trades:

- `primary/down/price=0.75_0.90/edge=0.07_0.15/z=1.1_1.5/conf=0.50_0.70/vol=lt_0.40`: 5 trades, -7.57015 PnL.
- `early/down/price=0.75_0.90/edge=0.07_0.15/z=0.7_1.1/conf=lt_0.50/vol=lt_0.40`: 12 trades, -6.47638 PnL.
- `primary/up/price=0.75_0.90/edge=0.07_0.15/z=0.7_1.1/conf=0.50_0.70/vol=lt_0.40`: 11 trades, -1.58744 PnL.

A combined deny approximation over the four negative regime buckets produced:

- Trades: 145.
- Wins/losses: 127 / 18.
- PnL: +90.44008.
- Losing folds: 8.
- Worst fold: -5.67420.

This improves aggregate PnL but fails the same A+ robustness goal and worsens
sample sufficiency.

## Verdict

The `reversion=1_2` candidate is a strong research signal, not a production
strategy. Execution/replay mechanics were stable: no breaker trips, no fill
failures in the reported fold summaries, and raw PMXT parquets were deleted
after each hour. The blocker is not order mechanics; it is strategy robustness
and sample sufficiency.

Current grade for this candidate:

- Execution harness: A-
- Storage discipline: A
- Strategy edge: B+
- Production readiness: C+

## Next Steps

1. Extend the same atomic holdout until at least 250 trades, because 30 folds
   only yielded 178 trades.
2. Use the completed reports as the baseline for a multi-rule feed-forward
   selectivity pass; single-rule selectivity did not pass.
3. Test an explicit loss-context guard in full harness, not from aggregate math
   alone. Candidate guards should focus on low-volatility, high-price, near-
   expiry `down` buckets and should be rejected if worst fold stays negative.
4. Add a robust-gate mode for combined deny candidates if the current
   selectivity-search remains single-rule only.
5. Only after a 200+ trade feed-forward gate passes with nonnegative worst fold,
   run live-replay parity. Do not use paper mode for this validation.
