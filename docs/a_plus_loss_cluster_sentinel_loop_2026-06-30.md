# A+ Loss-Cluster Sentinel Loop - 2026-06-30

## Scope

This loop implemented the next backtest-first control after the tail-first
causal-policy search: make loss-cluster state causal inside fold selection, not
only a final reject metric.

No paper mode was used. No VPS CPU-heavy work was run. No shared PMXT/parquet
cache was touched.

Input reports:

- 30 folds from `/private/tmp/polymomentum_reversion_holdout_20260627_may28_jun06/reports`
- 12 folds from `/private/tmp/polymomentum_reversion_extension_20260627_jun07_10/reports`

## Implementation

Changed:

- `rust_engine/src/strategy_builder.rs`
- `rust_engine/src/main.rs`

New causal-policy search controls:

- `--prior-loss-cluster-lookback`
- `--max-prior-loss-burst-reports`
- `--min-prior-payoff-ratio`
- `--max-prior-worst-loss-to-avg-win`

The defaults are disabled, so existing search behavior stays reproducible.

The loss-cluster sentinel is feed-forward:

- Each fold computes the selected policy's prior fold-level PnL only from
  earlier reports.
- The final promotion gate still uses `--loss-burst-lookback`.
- The sentinel can use a separate shorter prior window via
  `--prior-loss-cluster-lookback`.
- If recent prior losses hit the budget, the current fold is flat and records
  `prior_loss_cluster_sentinel_flat`.
- Prior payoff/worst-loss budget failures also flatten the current fold before
  any OOS PnL is counted.

Regression tests added:

- `causal_policy_prior_loss_cluster_sentinel_flattens_after_warning`
- `causal_policy_prior_payoff_budget_flattens_bad_asymmetry`
- `causal_policy_prior_worst_loss_budget_flattens_bad_asymmetry`

Verification:

```text
cargo test --manifest-path rust_engine/Cargo.toml causal_policy
cargo test --manifest-path rust_engine/Cargo.toml strategy_builder
cargo build --manifest-path rust_engine/Cargo.toml
```

## Results

All runs used the same 42-fold strict gate:

- minimum OOS trades: 80
- Wilson lower bound >= 0.70
- profitable reports >= 20
- worst fold >= -13.0
- 20% fold CVaR >= -8.0
- max burst <= 2 losing reports in any 5 eligible reports
- payoff ratio >= 0.30
- worst-loss-to-average-win <= 3.50

Artifacts:

- `deploy/promotions/evidence/strategy_registry/20260630_causal_policy_cluster_sentinel.json`
- `deploy/promotions/evidence/strategy_registry/20260630_causal_policy_loss_cooldown2.json`
- `deploy/promotions/evidence/strategy_registry/20260630_causal_policy_loss_cooldown2_budget028.json`
- `deploy/promotions/evidence/strategy_registry/20260630_causal_policy_loss_cooldown1.json`
- `deploy/promotions/evidence/strategy_registry/20260630_causal_policy_loss_cooldown3.json`
- `deploy/promotions/evidence/strategy_registry/20260630_causal_policy_cluster_budget.json`

| Run | OK | Trades | PnL | Wilson | Profitable reports | Worst fold | CVaR | Burst | Payoff | Worst loss / avg win | Main blocker |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 5-fold sentinel, threshold 2 | false | 80 | 41.11037 | 0.74157 | 20 | -16.40518 | -7.20548 | 3 | 0.31242 | 3.31651 | burst and worst fold |
| 2-fold cooldown, threshold 1 | false | 96 | 34.42104 | 0.74629 | 15 | -12.85440 | -8.18321 | 2 | 0.28270 | 3.82191 | CVaR, payoff, profitable reports |
| 2-fold cooldown plus prior payoff budget | false | 18 | 35.82605 | 0.74242 | 8 | -0.98667 | 0.15539 | 1 | 0.45126 | 2.21604 | too sparse |
| 1-fold cooldown, threshold 1 | false | 12 | 15.76544 | 0.75750 | 8 | 0.79469 | 0.87581 | 0 | 999.0 | 0.0 | too sparse |
| 3-fold cooldown, threshold 1 | false | 12 | 15.76544 | 0.75750 | 8 | 0.79469 | 0.87581 | 0 | 999.0 | 0.0 | too sparse |
| 5-fold sentinel plus prior payoff budget | false | 18 | 35.82605 | 0.74242 | 8 | -0.98667 | 0.15539 | 1 | 0.45126 | 2.21604 | too sparse |

## Interpretation

The new control is useful, but it does not produce a live-ready strategy by
itself.

The key result is the 2-fold cooldown:

- It keeps the trade count above the 80-trade floor.
- It reduces the burst to the required `2`.
- It keeps worst fold just inside the configured `-13.0` limit.
- It still fails CVaR, payoff geometry, and profitable-report count.

That means the current problem is no longer only "detect a cluster and stop."
The remaining problem is that the trades still active outside cooldown have bad
payoff geometry. More cooldown makes the curve clean but too sparse. Less
cooldown preserves sample size but lets the left tail through.

## Verdict

Mark this branch of the search as a useful dead end, not a candidate.

Live trading remains blocked.

Current grade remains:

- Validation/infrastructure: `A-`
- Strategy search controls: `A`
- Strategy readiness: `C+`
- Overall production readiness: `B+`

## Next Loop

Stay backtest-first.

1. Add a fold-level loss classifier rather than another fixed cooldown. The
   classifier should learn which active folds are still exposed after cooldown.
2. Add bankroll-aware position sizing in the backtest artifact: keep capital
   locked until resolution and score the same policy under fixed $100 bankroll.
3. Search over position-size caps by regime only after the bankroll simulator is
   wired, because the current aggregate PnL cannot prove deployable exposure.
4. Rerun the 2-fold cooldown candidate through the bankroll simulator as the
   baseline challenger.
5. Only if CVaR, payoff, burst, and profitable-report count pass on the 42-fold
   window, rerun the freshest fully resolved PMXT windows.
