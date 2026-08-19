# Overfitting And Regime Rules - 2026-05-26

Scope: research how to avoid fitting the strategy search to one May sample, then
inspect the current May backtest artifacts for why nearby strategies perform
differently across frames.

Fresh artifacts inspected:

- `/private/tmp/polymomentum_fresh_may_backtest_20260526T0303Z/harness_sweep_early_grid_continuous_20260524.json`
- `/private/tmp/polymomentum_fresh_may_backtest_20260526T0303Z/harness_sweep_early_grid_continuous_20260525T00_08.json`

PMXT had a full 2026-05-24 archive and only 2026-05-25T00..08 available at
inspection time. The 2026-05-25T09 archive returned 404, so May 25 is a fresh
partial holdout, not a full-day gate.

## Research Takeaways

Backtest overfitting is primarily a multiple-testing problem. Bailey and Lopez
de Prado's Deflated Sharpe Ratio work warns that strategy optimizers can inflate
performance when many variants are tried and only the winner is reported:
https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf

Bailey, Borwein, Lopez de Prado, and Zhu define the Probability of Backtest
Overfitting (PBO) as the probability that the in-sample winner underperforms the
median out-of-sample result. Their CSCV framework is useful for us because it
works from the matrix of strategy returns across folds, independent of strategy
internals: https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf

Hansen's Superior Predictive Ability test is relevant for data-snooping control:
when many rules are tested, the selected rule should be compared against the
family of alternatives, not treated as a single hypothesis:
https://papers.ssrn.com/sol3/papers.cfm?abstract_id=264569

For time series, conventional random cross-validation is not acceptable. Our
current feed-forward rule is directionally right. The next step is a combinatorial
set of chronological folds with an embargo around fold boundaries, because 5m
candles and L2 state are serially dependent.

For adaptation, change-point detection should be an alert and re-scout trigger,
not an automatic live parameter mutator. Page/CUSUM-style sequential tests are
appropriate for online structural-break monitoring, while Bayesian online
change-point detection is a richer later upgrade:

- https://arxiv.org/abs/1308.1237
- https://sciendo.com/article/10.2478/fiqf-2021-0025

## Local Evidence

The current strongest aggregate fresh-May candidate is:

```text
early_c0.40_z0.70_e0.03_ev-1.00_p0.10-0.90_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk
```

Aggregate over 2026-05-24 full day plus 2026-05-25T00..08:

| Variant | PnL 2026-05-24 | PnL 2026-05-25T00..08 | Aggregate PnL | Trades | W/L | Avg fill rate | Breaker |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `c0.40 z0.70 p0.10-0.90 maker` | +41.63 | +14.98 | +56.60 | 96 | 80/16 | 62.7% | no |
| `c0.30 z0.70 p0.10-0.90 maker` | +39.40 | +13.48 | +52.88 | 98 | 81/17 | 61.5% | no |
| `c0.35 z0.70 p0.10-0.90 maker` | +36.46 | +13.48 | +49.95 | 97 | 80/17 | 61.3% | no |
| `c0.40 z0.50 p0.10-0.90 maker` | +55.55 | -17.93 | +37.63 | 112 | 86/26 | 60.8% | no |
| `c0.30 z0.50 p0.10-0.90 maker` | +49.07 | -14.03 | +35.04 | 111 | 84/27 | 59.7% | no |

Key observation: the May 24 winner (`z0.50`) is not the robust rule. It wins one
full day by entering more weak early signals, then fails the next fresh block.
The `z0.70` family gives up some May 24 PnL but keeps May 25 positive.

This is not just "higher threshold means fewer trades." In our mechanics,
thresholds change entry timing. The backtest and live-replay paths allow one
trade per candle condition:

- `rust_engine/src/backtest/harness.rs`: returns early when `traded` already
  contains the condition id, then inserts the id when a decision becomes an
  order.
- `rust_engine/src/live/replay.rs`: mirrors the same one-trade-per-condition
  behavior.

Therefore a loose `z0.50` setting can fire earlier on a weak signal and consume
the candle. A stricter `z0.70` rule may wait for a more decisive state in the
same 5m frame. That makes `z` an entry-timing control, not merely a sample-size
control.

## Why Some Frames Differ

The fragile variants show three symptoms:

1. They are profitable when weak early momentum keeps trending.
2. They lose when early movement reverts or chops after the first weak signal.
3. Their fill rate does not explain the failure. The bad May 25 `z0.50` maker
   variants still filled around 62%, but their win rate fell to 55.6% and their
   average PnL per fill was strongly negative.

That points to signal quality and entry timing, not venue plumbing.

The maker edge remains important. Maker variants avoid taker fees and often
enter at a better limit price, but maker does not rescue bad signal timing. The
universal rule should be:

```text
maker first, but only after signal quality and entry timing pass.
```

## Anti-Overfit Strategy Selection Rules

The strategy finder should stop selecting by raw aggregate PnL. Promotion should
use a robustness score and hard vetoes:

Hard vetoes:

- No look-ahead: train windows must end before holdout windows start.
- Embargo fold boundaries by at least one candle window, preferably one full
  5m market plus order latency.
- No incomplete-data promotion unless explicitly labelled exploratory.
- No breaker trips.
- No unresolved fills.
- No non-passive execution failures.
- Worst-window PnL must be positive for production candidates.
- Candidate must be profitable in at least 3 independent chronological windows.
- Candidate must not be an isolated parameter spike.

Robustness score:

```text
score =
  0.30 * worst_window_expectancy
+ 0.20 * median_window_expectancy
+ 0.15 * Wilson_lower_win_rate
+ 0.15 * neighbor_stability
+ 0.10 * maker_fill_reliability
+ 0.05 * low_drawdown_pressure
+ 0.05 * simplicity_penalty_inverse
```

Neighbor stability means the selected rule's nearby parameter family also works:

- `confidence +/- 0.05`
- `z +/- 0.20`
- `max_price` neighboring bucket
- maker/taker comparison, where maker should dominate but taker should not be
  catastrophically opposite unless explicitly disallowed

The selected point should be the center of a plateau, not the single tallest
needle.

## Regime-Aware Rules

The next strategy system should tag every evaluation and every resolved trade
with regime features:

- Time zone in candle: early, primary, late, terminal.
- Elapsed percentage in the 5m frame.
- Signal strength: z-score, confidence, consistency.
- Move quality: move-from-open, move speed, reversion count.
- Volatility regime: rolling realized volatility percentile.
- Price bucket: entry token price, payout asymmetry, distance to max price.
- Microstructure: spread, bid depth, ask depth, pressure, book age.
- Execution mode: maker/taker, fill latency, passive miss/cross reason.

Promotion should require the candidate to explain where it works. A good rule is
not "z0.70 is best"; it is closer to:

```text
In early 5m BTC candles, use maker-first entry only after momentum has crossed a
strong z threshold, with positive fair-value edge, acceptable price/payout
asymmetry, and non-hostile book pressure. Do not enter merely because a weak
early signal is available.
```

## Concrete Next Implementation

1. Add a trial ledger to every search report.
   Record total variants tested, parameter ranges, data windows, cache hashes,
   and selection objective. This is required for DSR/PBO-style correction.

2. Add `robust-promote`.
   Input: multiple report JSON files. Output: a promotion artifact only if the
   selected candidate passes hard vetoes, neighbor stability, and worst-window
   gates.

3. Add fold-level PBO estimation.
   Build a variant x window matrix of returns. For each combinatorial split,
   find the in-sample winner and score its out-of-sample percentile. Reject
   families whose IS winners repeatedly fall below OOS median.

4. Add a plateau selector.
   Select from stable neighborhoods, not raw top PnL. Prefer the simplest
   threshold that remains positive across windows and neighbors.

5. Add regime attribution to reports.
   Aggregate PnL, win rate, loss severity, fill rate, and skip reasons by
   regime bucket. The report should explain whether a candidate works because of
   true signal quality, price/payout asymmetry, maker spread capture, or one
   lucky time slice.

6. Add adaptive monitoring as a re-scout trigger.
   In live/paper diagnostics, run CUSUM/Page-style detectors over per-trade PnL,
   win/loss outcomes, fill rate, and book-age/latency. The detector should never
   mutate live parameters automatically; it should freeze promotion and trigger
   a new offline search.

## Current Decision

Do not promote the May 24 `z0.50` winner. It is a likely frame-fit artifact.

The `z0.70` maker family is the current better hypothesis because it stayed
positive on the next fresh May block and its behavior matches a causal story:
wait for stronger early momentum before consuming the one allowed trade in a 5m
candle.

The next A+ move is not a larger grid. It is a robustness layer that proves the
rule is a stable plateau across folds and regimes.
