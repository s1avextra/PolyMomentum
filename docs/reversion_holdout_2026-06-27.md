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

## Extension: 2026-06-07 Through 2026-06-10

Extended the same `reversion=1_2` profile with 12 more full 8-hour folds:

- Window: 2026-06-07T00:00:00Z through 2026-06-10T23:00:00Z.
- Artifacts: `/private/tmp/polymomentum_reversion_extension_20260627_jun07_10/`.
- Temporary cache: `/private/tmp/polymomentum_reversion_extension_20260627_cache/`.
- Storage mode: `--atomic-parquet --delete-after-process --max-cache-gb 8`.
- Preflight: 96/96 PMXT hours available, 12/12 full folds.

Extension totals:

- Folds: 12.
- Trades: 91.
- Wins/losses: 74 / 17.
- Win rate: 81.32%.
- Net PnL: +11.29482 USD.
- Fees: 6.94988 USD.
- Profitable/losing folds: 7 / 5.
- Worst fold: -12.05971 USD.
- Breaker trips: none.

The extension met the basic aggregate direction, but failed robust promotion:

- Profitable reports 7 below the 8-report extension gate.
- Worst window PnL -12.05971 below required 0.
- Neighbor count 0 below required 2.
- Neighbor positive rate 0.0 below required 0.6.
- Robust score -0.1761 below required 0.
- Profit factor 1.1277 below required 1.20.
- Five causal buckets were negative at the configured minimum support.

The worst extension fold was
`fold_011_20260610T080000Z_20260610T150000Z_sweep.json`: 9 trades, 5 wins,
4 losses, -12.05971 PnL, 100% fill rate, no breaker. Its primary zone was
profitable (+1.61857 on 2/2), while early-zone trades lost -13.67828 on 3/7.
The loss was payoff-asymmetry, not an execution artifact: average win was
+1.74579 and average loss was -5.19716.

## Combined 42-Fold Result

Combined the original 30 folds plus the 12-fold extension:

- Window: 2026-05-28T00:00:00Z through 2026-06-10T23:00:00Z.
- Folds: 42.
- Trades: 269.
- Wins/losses: 226 / 43.
- Win rate: 84.01%.
- Net PnL: +85.10134 USD.
- Fees: 20.86566 USD.
- Profitable/losing folds: 29 / 13.
- Worst fold: -12.05971 USD.
- Breaker trips: none.

The combined run now clears the prior sample-size blocker, but it still fails
A+ promotion. Robust promotion rejected the candidate for:

- Worst window PnL -12.05971 below required 0.
- Neighbor count 0 below required 2.
- Neighbor positive rate 0.0 below required 0.6.
- Robust score -0.1268 below required 0.
- Negative causal bucket:
  `regime=zone=early|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=lt_0.50|vol=lt_0.40|rev=1_2|min=2_4`,
  22 trades, 16 wins, 6 losses, -13.26433 PnL, 0.5791 profit factor.

Zone audit passed on the combined reports:

- Early: 169 trades, 142 wins, 27 losses, +75.43883 PnL, 62.83% trade share.
- Primary: 100 trades, 84 wins, 16 losses, +9.66251 PnL, 37.17% trade share.

So the blocker is not early-zone concentration by itself. It is a narrower
interaction: high-price, low-edge, low-z, low-confidence, low-volatility
near-expiry contexts, especially on early down trades. Similar-looking broader
buckets can be profitable, so a blunt direction or zone ban is too crude.

Feed-forward selectivity-search over all 42 reports generated 201 single-rule
candidates and none passed. The best deny rule improved aggregate PnL to
+95.89509 with 267 trades, but its fold-forward worst report remained
-12.05971 and only 24/36 eligible reports were profitable. This means the
current single-rule search cannot explain away the bad window without
post-hoc leakage.

## Verdict

The `reversion=1_2` candidate is still a strong research signal, not a
production strategy. Execution/replay mechanics were stable: no breaker trips,
no fill failures in the reported fold summaries, and raw PMXT parquets were
deleted after each downloaded hour. The blocker is not order mechanics or
sample count anymore; it is strategy robustness under bad payoff-asymmetry
windows.

Current grade for this candidate:

- Execution harness: A
- Storage discipline: A
- Strategy edge: B
- Production readiness: C+

## Next Steps

1. Add multi-rule feed-forward guard search. The search must learn guards only
   from prior folds, then evaluate each later fold without future information.
2. Add explicit payoff-asymmetry guard features to the strategy lab: rolling
   average win, rolling average loss, loss-to-win ratio, recent resolved loss
   burst, and per-regime profit factor. These must be feed-forward only.
3. Add neighbor evidence for the selected parameter point. Current promotion
   still has `neighbor_count=0`, so we cannot know if the result is a stable
   plateau or a narrow parameter spike.
4. Rerun the full harness on the selected multi-rule candidate. Promotion
   should require at least 250 trades, positive PnL, Wilson lower bound above
   0.60, nonnegative worst fold, positive worst supported causal bucket, and
   passing neighbor robustness.
5. Only after that passes, run live-replay parity. Do not use paper mode for
   strategy validation when backtest/live-replay can prove the same behavior.
