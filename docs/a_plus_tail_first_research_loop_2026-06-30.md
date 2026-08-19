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

## Measured-Latency Cluster Retest - 2026-07-05

Artifact:
`deploy/promotions/evidence/strategy_registry/20260705_latency128_tail_cluster1/`.

Run shape:

- Window: `2026-05-31T08:00:00Z` through `2026-06-01T23:00:00Z`, the first
  known loss cluster.
- Folds: five full 8-hour folds.
- Profile: `a_plus5m_down_reversion_guard_confidence`.
- Causal filter: `direction=down`.
- Latency: requested `128 ms`, with the July 5 VPS latency audit attached.
  Effective latency stayed `128 ms` because the audit p99 recommendation was
  `62 ms`.
- Storage: `--atomic-parquet --delete-after-process`; per-fold caches were
  deleted after report writing.

Result:

- Promotion status: `promotion_failed`.
- Aggregate best audited variant: `6` trades, `6` wins, `0` losses, `+7.13485`
  PnL, Wilson lower `0.60966`.
- Fold trades: `3`, `2`, `0`, `1`, `0`.
- Rejections: trades below the `50` minimum, and primary-zone trade share
  `0.8333` above the `0.7000` maximum.

Interpretation: this profile avoids the cluster by becoming too sparse and too
zone-concentrated. That is safer than the previous clustered-loss behavior, but
it is not a tradable candidate and should not be promoted. The next challenger
should widen participation under low exposure rather than simply flattening the
known bad windows.

## Low-Exposure Remap Diagnostics - 2026-07-05

Artifact:
`deploy/promotions/evidence/strategy_registry/20260705_low_exposure_remap_diagnostics/`.

Run shape:

- Window: the first 8-hour fold of the first known loss cluster,
  `2026-05-31T08:00:00Z` through `2026-05-31T15:00:00Z`.
- Profile: `a_plus5m_tail_low_exposure`.
- Latency: requested `128 ms`, with the July 5 VPS latency audit attached.
  Effective latency stayed `128 ms`.
- Storage: `--atomic-parquet --delete-after-process`; only compact reports and
  manifests were archived.

Result:

- Baseline fold: `2` trades, `1` win, `1` loss, `-4.06889` PnL, `100%` fill
  rate.
- Exact losing-regime deny: still `2` trades, `1` win, `1` loss, `-4.43706`
  PnL; the loss moved to adjacent `book_min_depth=100_250`.
- `book_pressure=strong_positive` deny: still `2` trades, `1` win, `1` loss,
  `-4.18424` PnL; the loss moved to a `book_pressure=negative` regime.

Interpretation: manual micro-regime remapping is not enough. The stable toxic
shape is low-price, high-edge, primary-zone down entries:
`price=0.50_0.75` with `edge=gte_0.15`. That explains why the stricter
down-reversion guard avoided losses with `min_price=0.75` but became too sparse.
The next A+ step should use learned causal policy search across chronological
folds, or a new signal family, rather than widening low-price entries by hand.

## Low-Exposure Policy Search Diagnostics - 2026-07-05

Artifact:
`deploy/promotions/evidence/strategy_registry/20260705_low_exposure_policy_search_diagnostics/`.

Run shape:

- Fold 1 baseline from the remap diagnostics.
- Added chronological folds 2-3:
  `2026-05-31T16:00:00Z` through `2026-06-01T07:00:00Z`.
- Profile: `a_plus5m_tail_low_exposure`.
- Latency: requested `128 ms`, with the July 5 VPS latency audit attached.
  Effective latency stayed `128 ms`.

Result:

- Fold 2 top variant: `6` trades, `5` wins, `1` loss, `+1.00454` PnL.
- Fold 3 top variant: `1` trade, `0` wins, `1` loss, `-5.13834` PnL.
- Three-report causal-policy search produced a thin top hypothesis: require
  `book_age=lte_100ms`, deny `book_imbalance=strong_positive`.
- Static filtered view looked clean: `6` trades, `6` wins, `0` losses,
  `+7.19137` PnL.
- Direct replay rejected the hypothesis immediately on fold 1: `2` trades,
  `1` win, `1` loss, `-4.18424` PnL. The replacement losing regime had
  `book_imbalance=negative`.

Interpretation: static causal filtering is not strong enough for promotion or
even candidate credit. Candidate filters must be generated and then rerun
through full rolling-history replay before they count. The low-exposure family
remains rejected until a replay-integrated policy, or a different signal
family, clears the fail-fast tail clusters.

## Eligible-Report Policy Credit Gate - 2026-07-06

Artifact:
`deploy/promotions/evidence/strategy_registry/20260706_policy_search_min_eligible_gate/`.

Implementation:

- Added `--min-oos-eligible-reports` to
  `strategy-builder causal-policy-search`.
- Default `0` preserves existing research behavior.
- A+ causal-policy runs can now require selected trades in multiple
  chronological OOS reports before a candidate is allowed to pass.

Result:

- Reran the same three low-exposure reports with
  `--min-oos-eligible-reports 2`.
- Previous July 5 top hypothesis had `ok=true` but only `1` eligible OOS report
  and `2` abstentions.
- New result is `ok=false` across `9046` candidates.
- The best broader-coverage ranked policy had `2` eligible reports, but failed
  on realized tail/PnL: `6` trades, `4` wins, `2` losses, `-4.60148` PnL, worst
  report `-5.13834`, CVaR `-5.13834`.

Interpretation: current A+ work should continue down replay-integrated policy
credit before spending engineering effort on a new signal family. The available
causal/orderbook buckets already expose the replacement-loss shape; the missing
piece is a selector that refuses thin static credit and then survives full
harness replay.

## Replay-Integrated Policy Bridge - 2026-07-06

Artifact:
`deploy/promotions/evidence/strategy_registry/20260706_replay_integrated_policy_bridge/`.

Implementation:

- Added `strategy-builder causal-policy-replay-plan`.
- The command reads a causal-policy-search artifact, extracts
  `harness_require_args` and `harness_deny_args`, and emits per-candidate
  `rolling-history` manifests.
- By default it selects only candidates that passed search. `--include-failed`
  is diagnostic-only.
- Dry-run mode is the default; `--execute` is required before any heavy replay
  runs.

Evidence:

- The old July 5 static pass now emits an exact three-fold rolling-history plan
  for `require book_age=lte_100ms` and
  `deny book_imbalance=strong_positive`.
- The stricter July 6 min-eligible artifact emits `selected_count=0` by default,
  because no candidate passed the causal-policy gates.
- A diagnostic `--include-failed` plan exists for the top failed
  broader-coverage candidate, but it remains research-only: static fold-forward
  PnL was `-4.60148` with worst report `-5.13834`.

Interpretation: the project now has the missing mechanical bridge from policy
search to full replay verification. The next A+ candidate must pass causal
policy search, pass this replay-plan bridge, and then survive executed
rolling-history replay before promotion can be discussed.
