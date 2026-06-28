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
  passed 21/21 tests after the adaptive-mode extension.
- Tests cover multi-rule composition, future-only loss rejection, and
  feed-forward pattern generalization. The adaptive-mode tests also prove the
  selector cannot flat a future-only loss and only chooses flat from prior-tail
  evidence.

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

## Adaptive Mode Search: 2026-06-28

Added `strategy-builder adaptive-mode-search` to choose among flat, direction,
and guarded mode per fold using only prior evidence. The selector:

- Builds direction and guarded options from strictly prior reports.
- Ranks active modes by prior worst-fold PnL first, then prior aggregate PnL,
  Wilson lower bound, profit factor, and trade count.
- Can choose flat when no active mode passes prior gates, or when the best
  active mode's prior worst-fold PnL is below `--flat-if-worst-train-below`.
- Scores the current fold only after the mode is fixed.

Pattern-guard adaptive mode, no flat threshold:

- Artifact: `/private/tmp/polymomentum_reversion_combined_20260628_adaptive_mode_no_flat.json`.
- Result: failed.
- Feed-forward OOS: 126 trades, 103 wins, 23 losses, +34.86972 PnL, Wilson
  lower 0.74096, 20 profitable / 12 losing eligible reports, worst -12.88359.
- Mode counts: direction 6, guarded 26, flat 10.

Pattern-guard adaptive mode, flat threshold -8 and -5:

- Artifacts:
  `/private/tmp/polymomentum_reversion_combined_20260628_adaptive_mode_flat_m8.json`
  and
  `/private/tmp/polymomentum_reversion_combined_20260628_adaptive_mode_flat_m5.json`.
- Result: both failed with the same summary.
- Feed-forward OOS: 124 trades, 101 wins, 23 losses, +33.18294 PnL, Wilson
  lower 0.73703, 19 profitable / 12 losing eligible reports, worst -12.88359.
- Mode counts: direction 5, guarded 26, flat 11.

Pattern-guard adaptive mode, flat threshold -3 and -2:

- Artifacts:
  `/private/tmp/polymomentum_reversion_combined_20260628_adaptive_mode_flat_m3.json`
  and
  `/private/tmp/polymomentum_reversion_combined_20260628_adaptive_mode_flat_m2.json`.
- Result: both failed.
- At -3: only 16 OOS trades, -8.02990 PnL, worst -5.28118, 38 flat reports.
- At -2: only 10 OOS trades, -7.70561 PnL, worst -4.15719, 40 flat reports.
- These thresholds reduce exposure but collapse sample size and still lose.

Exact-guard adaptive mode:

- Artifact: `/private/tmp/polymomentum_reversion_combined_20260628_adaptive_mode_exact_no_flat.json`.
- Result: failed.
- Feed-forward OOS: 101 trades, 79 wins, 22 losses, -3.78399 PnL, Wilson
  lower 0.69215, 17 profitable / 12 losing eligible reports, worst -12.05971.

The combined selector is useful as a control/diagnostic primitive, but it does
not rescue this candidate. The bad folds are not predictable enough from the
current prior-fold regime summaries: a prior-tail-ranked guarded mode still
chooses the Jun 10 08:00-15:00 UTC window and loses -12.88359.

## Causal Policy Search

Implementation added on 2026-06-28:

- New CLI: `strategy-builder causal-policy-search`.
- Generates causal require-policy conjunctions from observed decision-time
  regime tags, then evaluates each OOS fold using only earlier reports.
- Learns optional prior-toxic single-tag deny rules inside each require policy.
  The default single-tag deny shape maps directly to existing
  `--require-causal-tag` and `--deny-causal-tag` harness/runtime controls.
- Caches parsed causal tags per regime row, so the 42-report search runs in
  about 14 seconds on the dev box instead of timing out from repeated string
  parsing.
- Tests cover feed-forward interaction selection, future-only luck rejection,
  and prior-only deny learning.

Strict A+ tail run:

- Artifact:
  `/private/tmp/polymomentum_reversion_combined_20260628_causal_policy_search.json`.
- Gates: 80+ OOS trades, Wilson lower >= 0.70, 20+ profitable OOS reports,
  positive PnL, worst OOS fold >= 0.
- Result: failed.
- Candidate count: 925.
- Best ranked coverage policy:
  - require: `zone=early`
  - learned final deny: `z=gte_1.5`
  - feed-forward OOS: 129 trades, +71.70188 PnL, Wilson lower 0.77263,
    28 profitable / 8 losing reports
  - blocker: worst OOS fold -16.40518
- The worst fold was report index 40. The policy had strong prior train stats
  before that fold, then the current fold produced 6 OOS trades, 2 wins, 4
  losses, and -16.40518 PnL. This is a real tail cluster, not a selector
  timestamp leak.

Relaxed bounded-loss diagnostic:

- Artifact:
  `/private/tmp/polymomentum_reversion_combined_20260628_causal_policy_search_relaxed_tail.json`.
- Same gates except worst OOS fold >= -13.
- Result: 17 candidates passed the relaxed diagnostic.
- Top policy:
  - require: `reversion=1_2`
  - learned final deny: `z=gte_1.5`
  - feed-forward OOS: 186 trades, +68.28332 PnL, Wilson lower 0.77315,
    26 profitable / 13 losing reports, worst -12.85440
  - aggregate static final policy: 259 trades, +85.48637 PnL, Wilson lower
    0.79229
- This is useful as a research candidate, but it is not A+ because strict
  zero-worst-fold robustness still fails.

## Verdict

The `reversion=1_2` candidate is still a strong research signal, not a
production strategy. Execution/replay mechanics were stable: no breaker trips,
no fill failures in the reported fold summaries, and raw PMXT parquets were
deleted after each downloaded hour. The blocker is not order mechanics or
sample count anymore; it is strategy robustness under bad payoff-asymmetry
windows. The new multi-guard and adaptive-mode tooling makes this clearer:
static aggregate guards can look excellent while feed-forward OOS still fails,
and flat thresholds that avoid cliffs also erase sample size and profitability.

Current grade for this candidate:

- Execution harness: A
- Storage discipline: A
- Strategy-lab tooling: A
- Strategy edge: B-
- Production readiness: C+

## Next Steps

1. Do not promote `reversion=1_2` as-is. The exact, loose, pattern,
   adaptive-direction, and adaptive-mode searches all failed feed-forward
   worst-fold gates.
2. Move the search objective away from maximizing static aggregate PnL and
   toward minimizing tail loss: worst-fold PnL, average loss size, and loss
   burst frequency must be first-class objectives.
3. Add richer pre-trade state features for the search lab. Current fold-level
   causal summaries are better now but still too blunt for the report-40 tail.
   Next candidates should include distance to BTC candle close, intra-candle
   path shape, local book imbalance, spread stability, and loss-burst state
   computed strictly from already-resolved prior positions.
4. Add neighbor evidence for any selected parameter point. Current promotion
   still has `neighbor_count=0`, so we cannot know if the result is a stable
   plateau or a narrow parameter spike.
5. Only after a candidate passes 250+ trades, positive PnL, Wilson lower bound
   above 0.60, nonnegative worst fold, positive worst supported causal bucket,
   and neighbor robustness, run live-replay parity. Do not use paper mode for
   strategy validation when backtest/live-replay can prove the same behavior.
