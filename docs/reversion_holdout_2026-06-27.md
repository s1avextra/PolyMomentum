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

## Multi-Guard Search: 2026-06-28

Added `strategy-builder multi-guard-search` to test composed feed-forward
guards without using paper mode. The implementation:

- Learns denied full-regime buckets only from strictly prior reports.
- Scores each OOS fold only after that fold's guard is fixed.
- Uses disjoint full-regime buckets for arithmetic, avoiding overlap between
  causal dimensions.
- Exposes payoff-asymmetry fields in search artifacts: gross win/loss, average
  win/loss, max win/loss, profit factor, payoff ratio, and worst loss to
  average win.
- Supports optional broader pattern guards with `--pattern-guards`; these
  still apply by matching full-regime buckets rather than summing overlapping
  dimension buckets.

Validation:

- `cargo test --manifest-path rust_engine/Cargo.toml strategy_builder::tests:: -- --nocapture`
  passed 19/19 tests.
- Tests cover multi-rule composition, future-only loss rejection, and
  feed-forward pattern generalization.

Strict exact-regime multi-guard search:

- Artifact: `/private/tmp/polymomentum_reversion_combined_20260628_multi_guard_search.json`.
- Gates: 42 reports, min 6 train reports, min 40 train trades, min 60 OOS
  trades, min 18 profitable OOS reports, nonnegative worst OOS fold.
- Result: failed.
- Feed-forward OOS: 211 trades, 176 wins, 35 losses, +62.95658 PnL, Wilson
  lower 0.77805, 23 profitable / 13 losing eligible reports, worst -12.05971.
- Final static exact guard improved aggregate to +99.04789, but that is not
  promotable because feed-forward worst fold stayed negative.

Loose exact-regime search:

- Artifact: `/private/tmp/polymomentum_reversion_combined_20260628_multi_guard_loose_search.json`.
- Loosened to min 1 guard trade/report and max 8 rules.
- Result: failed.
- Feed-forward OOS: 201 trades, 169 wins, 32 losses, +69.23991 PnL, Wilson
  lower 0.78390, 22 profitable / 14 losing eligible reports, worst -12.05971.
- Static aggregate looked excellent (+141.93770), but this was not
  feed-forward robust.

Pattern-guard search:

- Artifact: `/private/tmp/polymomentum_reversion_combined_20260628_multi_guard_pattern_search.json`.
- Enabled `--pattern-guards`, max 6 rules, min 10 guard trades, min 2 guard
  reports.
- Result: failed.
- Feed-forward OOS: 165 trades, 140 wins, 25 losses, +72.57082 PnL, Wilson
  lower 0.78589, 25 profitable / 11 losing eligible reports, worst -12.88359.
- Final static pattern guard improved aggregate to +142.79914 with 185 trades
  and 2.4244 profit factor, but the feed-forward worst fold worsened. This is
  a classic static-overfit warning, not a promotion signal.

Adaptive direction comparison:

- Artifact: `/private/tmp/polymomentum_reversion_combined_20260628_adaptive_direction_search.json`.
- Result: failed.
- Feed-forward OOS: 95 trades, 76 wins, 19 losses, +6.49206 PnL, Wilson lower
  0.70862, 17 profitable / 12 losing eligible reports, worst -10.27148.
- Direction selection reduced the Jun 10 08:00-15:00 UTC fold from -12.05971
  to -1.78366, but introduced or left other bad folds, so it is not sufficient.

## Verdict

The `reversion=1_2` candidate is still a strong research signal, not a
production strategy. Execution/replay mechanics were stable: no breaker trips,
no fill failures in the reported fold summaries, and raw PMXT parquets were
deleted after each downloaded hour. The blocker is not order mechanics or
sample count anymore; it is strategy robustness under bad payoff-asymmetry
windows. The new multi-guard tooling makes this clearer: static aggregate
guards can look excellent while feed-forward OOS still fails.

Current grade for this candidate:

- Execution harness: A
- Storage discipline: A
- Strategy-lab tooling: A-
- Strategy edge: B-
- Production readiness: C+

## Next Steps

1. Do not promote `reversion=1_2` as-is. The exact, loose, pattern, and
   adaptive-direction searches all failed feed-forward worst-fold gates.
2. Move the search objective away from maximizing static aggregate PnL and
   toward minimizing tail loss: worst-fold PnL, average loss size, and loss
   burst frequency must be first-class objectives.
3. Add a combined adaptive policy search that can choose among flat, direction,
   and guarded modes per fold from prior evidence only. The current individual
   pieces help different bad folds but do not solve all of them.
4. Add neighbor evidence for any selected parameter point. Current promotion
   still has `neighbor_count=0`, so we cannot know if the result is a stable
   plateau or a narrow parameter spike.
5. Only after a candidate passes 250+ trades, positive PnL, Wilson lower bound
   above 0.60, nonnegative worst fold, positive worst supported causal bucket,
   and neighbor robustness, run live-replay parity. Do not use paper mode for
   strategy validation when backtest/live-replay can prove the same behavior.
