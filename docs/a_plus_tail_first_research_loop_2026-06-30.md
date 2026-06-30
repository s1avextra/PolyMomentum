# A+ Tail-First Research Loop - 2026-06-30

## Scope

This loop implemented the next backtest-first step after the strict-tail audit:
make causal-policy search prefer downside stability before aggregate PnL, then
rerun the full May28-Jun10 feed-forward evidence set.

No paper mode was used. No VPS CPU-heavy work was run. No shared PMXT/parquet
cache was modified.

Input reports:

- 30 folds from `/private/tmp/polymomentum_reversion_holdout_20260627_may28_jun06/reports`
- 12 folds from `/private/tmp/polymomentum_reversion_extension_20260627_jun07_10/reports`

## Research Principle

The previous best candidates were not failing because they lacked aggregate edge.
They were failing because small frequent wins were exposed to clustered large
losses. For production selection, ranking must therefore optimize the left tail
first.

Implemented selection principles:

- Feed-forward only: each fold is selected from earlier folds, never from future
  observations.
- Tail-first ranking: loss-burst count, worst fold, CVaR/expected shortfall,
  worst-loss-to-average-win ratio, and payoff ratio rank before total PnL.
- Payoff asymmetry gate: reject high win-rate candidates when average wins are
  too small relative to losses.
- Fail-closed registry: every tested dead end is recorded so future searches do
  not repeat the same idea without a materially different hypothesis.

Reference concept: CVaR/expected shortfall is the correct risk object for this
problem because it measures the mean of the left-tail losses, not just the single
worst observation. See Rockafellar and Uryasev's CVaR formulation:
https://www.ise.ufl.edu/uryasev/files/2011/11/CVaR1_JOR.pdf

## Implementation

Changed:

- `rust_engine/src/strategy_builder.rs`
- `rust_engine/src/main.rs`

New `strategy-builder causal-policy-search` flags:

- `--tail-first-ranking`
- `--min-oos-payoff-ratio`
- `--max-oos-worst-loss-to-avg-win`

The defaults keep existing behavior unchanged. The new behavior is opt-in for
causal-policy search only.

Regression tests added:

- `causal_policy_tail_first_ranking_prefers_cleaner_tail_over_higher_pnl`
- `causal_policy_payoff_asymmetry_gate_rejects_high_win_rate_candidate`

Verification:

```text
cargo fmt --manifest-path rust_engine/Cargo.toml
cargo test --manifest-path rust_engine/Cargo.toml causal_policy
cargo test --manifest-path rust_engine/Cargo.toml strategy_builder
cargo build --manifest-path rust_engine/Cargo.toml
```

## Search Results

Artifacts:

- `deploy/promotions/evidence/strategy_registry/20260630_tail_first_causal_policy_diagnostic.json`
- `deploy/promotions/evidence/strategy_registry/20260630_tail_first_causal_policy_payoff_gated.json`

Strict gates for the payoff-gated run:

- 42 chronological folds.
- Minimum OOS trades: 80.
- Wilson lower bound >= 0.70.
- Profitable reports >= 20.
- Worst OOS fold >= -13.0.
- 20% left-tail CVaR >= -8.0.
- Loss burst <= 2 losing reports in any 5 eligible reports.
- Payoff ratio >= 0.30.
- Worst-loss-to-average-win <= 3.50.

Best top candidate:

| Policy | Passed | Trades | PnL | Wilson lower | Profitable reports | Worst fold | CVaR | Max burst | Payoff ratio | Worst loss / avg win |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| require `direction=up`, deny `zone=primary` | false | 85 | 30.61314 | 0.74232 | 23 | -11.09993 | -5.83126 | 3 | 0.27932 | 3.97036 |

Interpretation:

- Tail-first ranking improved the top candidate shape versus pure aggregate-PnL
  ranking.
- It still fails the production gate because clustered losses remain above the
  allowed burst threshold.
- It also fails the new payoff-asymmetry gates: the candidate's payoff ratio is
  below 0.30 and worst-loss-to-average-win is above 3.50.

## Registry Result

Marked:

- `tail_first_causal_policy_payoff_20260630`: `dead_end`

Reason:

- Tail-first causal-policy search improved top tail shape but found no promotable
  candidate.
- The best candidate still has burst `3/5`, payoff ratio `0.279`, and
  worst-loss-to-average-win `3.970`.

Registry audit artifact:

- `deploy/promotions/evidence/strategy_registry/20260630_registry_audit_after_tail_first.json`

Audit result:

- `ok=true`
- `live_ready=false`
- `grade=A-`
- `live_candidate_count=0`
- `missing_paths=0`
- `non_durable_paths=0`

## Verdict

This was a useful implementation step, but not a promotable strategy.

Current state:

- Infrastructure and validation hygiene: `A-`
- Strategy search discipline: `A-`
- Strategy readiness: `C+`
- Production verdict: live trading remains blocked

The project is stronger because it now refuses another class of attractive but
fragile strategies. It is not A+ yet because no candidate clears the clustered
loss and payoff-asymmetry gates on the full chronological window.

## Next Loop

Stay backtest-first. Do not use paper mode for this.

1. Add a loss-cluster sentinel to candidate generation, not only post-selection.
   A candidate should be rejected as soon as prior folds show the same loss
   cluster shape that blocks promotion.
2. Add abstention/risk-budget policies by regime: when the policy enters a
   known bad cluster state, it should flatten or shrink rather than keep firing.
3. Re-score candidates with active bankroll exposure, including locked capital
   until resolution, so payoff gates reflect deployable position sizing.
4. Run the four known tail clusters as a fail-fast suite before the full 42-fold
   gate.
5. Only if a candidate passes those fail-fast clusters, rerun the full gate and
   then the freshest fully resolved PMXT windows.
