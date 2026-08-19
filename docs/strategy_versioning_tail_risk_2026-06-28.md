# Strategy Versioning And Tail-Risk Gate - 2026-06-28

## Implementation

- Added tail-aware causal-policy search fields to `strategy-builder causal-policy-search`:
  - `--tail-alpha`
  - `--min-oos-cvar-pnl`
  - `--loss-burst-lookback`
  - `--max-loss-burst-reports`
- Added `fold_forward.tail` to causal-policy search artifacts:
  - left-tail CVaR over feed-forward OOS report PnL
  - worst fold PnL
  - losing report count
  - max clustered-loss count inside a rolling report window
- Added `strategy-builder registry-mark` and `docs/strategy_registry.json`:
  - statuses: `candidate`, `active`, `questionable`, `dead_end`, `promoted`, `rejected`
  - each entry keeps version id, parent id, artifact paths, evidence paths, notes, and an event history
  - writes are atomic via temp-file plus rename
- Added `a_plus5m_tail_guard` to both planning and rolling-history profile tables:
  - `position_pct=0.025`
  - `max_total_exposure_usd=8`
  - `max_per_market_usd=10`
  - `max_projected_stressed_drawdown_pct=0.12`
  - degraded after one loss
  - degraded mode actually tightens to `degraded_min_z=1.10` and `degraded_max_price=0.75`

## Research Principles Applied

- Strategy versions should be treated like model registry entries: immutable evidence, mutable lifecycle status, and explicit lineage.
- Mean PnL is not enough for promotion. The gate now measures expected shortfall/CVaR over report PnL so the left tail is first-class.
- Feed-forward remains mandatory. A policy can only use prior reports to select current-fold require/deny tags.
- Loss clustering is separate from worst-fold loss. A strategy with acceptable single-fold loss but clustered drawdown still fails the burst gate.

## Current Evidence

Tail-gated command:

```bash
rust_engine/target/debug/polymomentum-engine strategy-builder causal-policy-search \
  --report /private/tmp/polymomentum_reversion_holdout_20260627_may28_jun06/reports/*.json \
           /private/tmp/polymomentum_reversion_extension_20260627_jun07_10/reports/*.json \
  --min-train-reports 3 \
  --min-train-trades 10 \
  --min-oos-trades 80 \
  --min-oos-wilson-win-rate-lower 0.70 \
  --min-oos-total-pnl 0.0 \
  --min-oos-profitable-reports 20 \
  --min-worst-oos-pnl -13.0 \
  --max-require-terms 3 \
  --max-deny-rules 1 \
  --max-deny-terms 1 \
  --min-deny-trades 5 \
  --min-deny-loss-pnl 0.0 \
  --min-deny-loss-reports 2 \
  --tail-alpha 0.20 \
  --min-oos-cvar-pnl -8.0 \
  --loss-burst-lookback 5 \
  --max-loss-burst-reports 2 \
  --top 25 \
  --output /private/tmp/polymomentum_reversion_combined_20260628_tail_gated_causal_policy_search.json
```

Result: `ok=false`, `0` passed candidates.

Best candidate:

- require: `reversion=1_2`
- variant: `all_c0.40_z0.90_e0.07_ev-1.00_p0.10-0.85_sc2.0_rv2_sf10_sg2.0_ss0.00_ms1.00_md0_mp-1.00_fbL2d0.00z0.90tk_selreqreversion-1_2_tk`
- feed-forward OOS trades: `186`
- feed-forward OOS PnL: `68.28332`
- Wilson lower: `0.773146`
- profitable reports: `26`
- losing reports: `13`
- worst report PnL: `-12.8544`
- CVaR at 20 percent report tail: `-7.9788075`
- max losing reports inside 5-report window: `5`

Verdict: aggregate edge still exists, but the current strategy is not production-sound. The loss burst violates the configured tail gate and the left-tail value is too close to the threshold to promote.

## Registry State

- `reversion_1_2_causal_policy_tail_20260628`: `questionable`
- `zero_worst_fold_causal_policy_20260628`: `dead_end`

See `docs/strategy_registry.json`.

## Targeted Tail Probes

Before paying the cost of a full 42-fold rerun, the new `a_plus5m_tail_guard`
profile was tested on the two known tail clusters from the May28-Jun10 evidence.

Jun10 08:00-15:00 UTC:

- previous reversion candidate:
  - trades: `9`
  - wins/losses: `5/4`
  - PnL: `-12.05971`
  - early zone: `7` trades, `-13.67828` PnL
  - primary zone: `2` trades, `+1.61857` PnL
- `a_plus5m_tail_guard`:
  - trades: `1`
  - wins/losses: `1/0`
  - PnL: `+2.36253`
  - zone: primary only
  - execution failures: `0`
  - unresolved fills: `0`
  - breaker tripped: `false`
  - robustness status: rejected because one trade means zone concentration and Wilson evidence are too weak

Jun7 00:00-07:00 UTC:

- previous reversion candidate:
  - trades: `7`
  - wins/losses: `4/3`
  - PnL: `-10.49522`
  - early zone: `5` trades, `-12.42583` PnL
  - primary zone: `2` trades, `+1.93061` PnL
- `a_plus5m_tail_guard`:
  - trades: `0`
  - PnL: `0`
  - execution failures: `0`
  - unresolved fills: `0`
  - breaker tripped: `false`
  - coverage status: rejected because the fold has no trades

Interpretation: the tail guard is doing the right protective thing on the
sampled loss clusters, but it is not yet a production strategy. It mainly avoids
early-zone tail losses by starving the fold. The next search should either build
a primary-only challenger with explicit minimum trade-rate gates, or re-admit
early-zone trades only under stricter confidence, price, timing, and reversal
constraints.

## Min-Order Primary Challenger - 2026-06-29

The first primary-only challenger exposed a sizing bug in the search design:
with a `$100` bankroll and `position_pct=0.025`, many otherwise eligible
primary-zone decisions were rejected by `min_order_size_primary`. The challenger
profiles were updated so candidate per-order notional is actionable while total
exposure remains capped.

Implementation changes:

- Added targeted profiles:
  - `a_plus5m_tail_primary`
  - `a_plus5m_tail_early_reentry`
  - `a_plus5m_tail_low_exposure`
- Kept the original `a_plus5m_tail_guard` unchanged for historical
  comparability.
- Updated continuous harness loading to check shared distilled/per-tenant
  sidecars before downloading raw PMXT parquet. This made reruns load cached
  hours in about `22-26s` instead of redownloading each raw hour.
- Added `AGENTS.md` rule: promotion evidence must include the freshest fully
  resolved windows available. Old tail windows are regression/debug windows, not
  promotion proof.

Targeted results for `a_plus5m_tail_primary`, `--zone-mode primary`,
`bankroll=100`, `position_pct=0.05`, `max_total_exposure_usd=8`:

Jun7 00:00-07:00 UTC:

- trades: `7`
- wins/losses: `7/0`
- PnL: `+7.73694`
- fees: `0.46576`
- fill rate: `100%`
- execution failures: `0`
- unresolved fills: `0`
- breaker tripped: `false`
- zone audit: passed on this one fold

Jun10 08:00-15:00 UTC:

- trades: `3`
- wins/losses: `2/1`
- PnL: `-2.10983`
- fees: `0.23003`
- fill rate: `100%`
- execution failures: `0`
- unresolved fills: `0`
- breaker tripped: `false`
- zone audit: failed because primary-zone PnL was negative

Verdict: `a_plus5m_tail_primary` is useful but not sound. It proves that
min-order-aware sizing can rescue one known tail cluster, but it fails the second
known tail cluster and therefore remains `questionable` in
`docs/strategy_registry.json`.

## Remaining Challenger Fail-Fast - 2026-06-29

The remaining challenger families were run against the first required tail
cluster, Jun7 00:00-07:00 UTC. Neither qualified for Jun10, full-window, or
freshest-window promotion runs.

`a_plus5m_tail_early_reentry`, `--zone-mode early`:

- variants tested: `8`
- trades: `0` on every variant
- execution attempts: `0`
- execution failures: `0`
- unresolved fills: `0`
- breaker tripped: `false`
- main skip reasons: `low_reversion_count_early`, `settlement_cutoff_*`,
  `low_confidence_*`, and `high_reversion_count_*`
- verdict: rejected as coverage-limited. The current gates are too strict to
  prove a tradable edge.

`a_plus5m_tail_low_exposure`, `--zone-mode all`:

- best variant trades: `5`
- wins/losses: `4/1`
- PnL: `-1.42235`
- fees: `0.29385`
- fill rate: `100%`
- execution failures: `0`
- unresolved fills: `0`
- breaker tripped: `false`
- early zone: `2` trades, `-4.28375` PnL
- primary zone: `3` trades, `+2.86140` PnL
- profit factor: `0.72237`
- zone audit: failed because early-zone PnL was below zero
- verdict: rejected. Lower exposure reduced damage but did not remove the toxic
  early bucket.

Fail-fast result: no challenger family passed the known tail clusters. The only
surviving variant remains `a_plus5m_tail_primary_minorder_20260629` as
`questionable`, not promotable, because it passed Jun7 but failed Jun10. The next
search should learn or explicitly deny toxic early buckets before any paper/live
step; old tail clusters remain regression/debug gates, and freshest fully
resolved windows remain mandatory for promotion evidence.

## Next Loop

1. Build the next search around explicit toxic-early-bucket denial or
   feed-forward learned deny rules from Jun7 and Jun10.
2. Reject any family that cannot pass both known tail clusters.
3. For any family that passes both tail clusters, run the full May28-Jun10
   window.
4. Before any promotion step, run the same family on the freshest available
   fully resolved windows.
5. Re-run `strategy-builder causal-policy-search` with the same tail gates.
6. Promote only if all are true:
   - `ok=true`
   - freshest-window result passes
   - no loss burst above 2 in a 5-report window
   - CVaR clears the configured floor with margin
   - worst report is acceptable
   - no unresolved fills, execution failures, or breaker artifacts
7. If still failing, mark that profile version `dead_end` and branch search
   toward primary-only selectivity or lower exposure, not paper mode.
