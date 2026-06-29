# A+ Strict Tail Loop - 2026-06-30

## Scope

This loop reran the May28-Jun10 chronological backtest/search evidence with
strict feed-forward gates and no paper-mode validation. The VPS was not used for
CPU-heavy work, and no shared PMXT/parquet cache was modified.

Input reports:

- 30 folds from `/private/tmp/polymomentum_reversion_holdout_20260627_may28_jun06/reports`
- 12 folds from `/private/tmp/polymomentum_reversion_extension_20260627_jun07_10/reports`

Strict gate shape:

- Feed-forward only; every decision is trained from prior folds.
- Wilson lower bound >= 0.70.
- Positive OOS PnL and sufficient OOS trades.
- Worst OOS fold within configured tolerance.
- OOS fold CVaR >= -8.0 at 20% left tail.
- Loss-burst <= 2 losing folds in any rolling 5 eligible-fold window.

## Implementation Fix

The audit found a validation inconsistency: `adaptive-direction-search` and
`adaptive-mode-search` could report `ok=true` without the same fold-tail CVaR
and loss-burst gates already present in causal-policy and multi-guard search.

Fixed in:

- `rust_engine/src/strategy_builder.rs`
- `rust_engine/src/main.rs`

Both adaptive selectors now:

- Accept `--tail-alpha`, `--min-oos-cvar-pnl`, `--loss-burst-lookback`, and
  `--max-loss-burst-reports`.
- Emit `fold_forward.tail`.
- Include CVaR and loss-burst in `passed`.
- Rank candidates with tail-gate completion before aggregate polish.

Regression tests added:

- `adaptive_direction_search_reports_tail_cvar_and_loss_burst`
- `adaptive_mode_search_reports_tail_cvar_and_loss_burst`

Verification:

```text
cargo test --manifest-path rust_engine/Cargo.toml reports_tail_cvar_and_loss_burst
cargo test --manifest-path rust_engine/Cargo.toml strategy_builder
cargo build --manifest-path rust_engine/Cargo.toml
```

## Selector Results

Artifacts:

- `deploy/promotions/evidence/strategy_registry/20260630_full_may28_jun10_multi_guard_tail.json`
- `deploy/promotions/evidence/strategy_registry/20260630_full_may28_jun10_causal_policy_tail.json`
- `deploy/promotions/evidence/strategy_registry/20260630_full_may28_jun10_adaptive_direction_tail.json`
- `deploy/promotions/evidence/strategy_registry/20260630_full_may28_jun10_adaptive_mode_tail.json`

| Selector | OK | Trades | PnL | Wilson lower | Worst fold | CVaR | Max burst |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Multi-guard | false | 200 | 69.58068 | 0.78286 | -12.05971 | -7.17578 | 4 |
| Causal policy | false | 186 | 68.28332 | 0.77315 | -12.85440 | -7.97881 | 5 |
| Adaptive direction | false | 102 | 25.16000 | 0.74921 | -10.27148 | -5.52738 | 3 |
| Adaptive mode | false | 144 | 51.23071 | 0.76398 | -12.05971 | -6.26802 | 4 |

Interpretation: the edge is not gone, but it is not production-sound. All four
selector families can produce positive aggregate OOS PnL, but every family fails
the clustered-loss gate. That means the current strategy is still a research
candidate, not a live or canary candidate.

## Tail Blockers

The recurring loss clusters are concentrated around these UTC fold windows:

- 2026-05-31 08:00 to 2026-06-01 23:00
- 2026-06-06 16:00 to 2026-06-07 23:00
- 2026-06-08 16:00 to 2026-06-09 07:00
- 2026-06-10 08:00 to 2026-06-10 15:00

The biggest practical pattern is payoff asymmetry: many losing folds still have
high nominal win rates, but each resolved loss is roughly 3-5x a normal win. A
strategy can therefore look good on win rate and aggregate PnL while being
fragile to clustered wrong-side resolutions.

## Registry Result

Marked:

- `full_may28_jun10_strict_tail_loop_20260630`: `dead_end`

Registry audit:

- `ok=true`
- `live_ready=false`
- `grade=A-`
- `live_candidate_count=0`
- `missing_paths=0`
- `non_durable_paths=0`

Audit artifact:

- `deploy/promotions/evidence/strategy_registry/20260630_registry_audit_after_full_tail_loop.json`

## Verdict

Current project grade remains `A-` for infrastructure/evidence hygiene and `C+`
for strategy readiness. Overall production grade remains `B+` because the system
is now better at refusing weak candidates, but it still has no promotable edge.

Live trading remains blocked.

## Next A+ Loop

Do not use paper mode for this. The next loop should be backtest/live-replay
only:

1. Build a tail-first candidate generator, not another aggregate-PnL selector.
   Candidate objective should minimize burst count, worst-fold loss, and
   loss-to-win payoff ratio before maximizing PnL.
2. Add explicit loss-size controls to strategy search:
   price distance to resolution boundary, confidence bucket, zone, direction,
   reversion count, minutes remaining, and BTC regime must be jointly tested as
   causal tags.
3. Require a minimum expected payoff ratio or bounded downside per trade.
   High win-rate candidates with large average losses should be rejected early.
4. Run fail-fast on the four known tail clusters above.
5. Only if a candidate passes those tail clusters, run the full May28-Jun10
   chronological gate.
6. Only if that passes, fetch the freshest fully resolved PMXT windows and run
   the same strict feed-forward gate.
7. Only after a fresh-window pass should VPS read-only checks or paper plumbing
   be considered.

