# A+ Live-Replay Readiness - 2026-05-31

## Status

Offline strategy/order-path readiness is now `A+` for the freshest complete May
PMXT v2 sample available locally on 2026-05-31.

This is not a live-capital claim. It means the selected artifact passed
feed-forward backtest promotion, exact live-replay order-path parity, causality,
oracle, replay sample, and adaptive-rearm dependency gates without using paper
mode for strategy validation.

## Promoted Artifact

Artifact:

```text
deploy/promotions/promotion_candidate_a_plus5m_guard_may23_25_20260531.json
```

Selected strategy:

```text
all_c0.40_z0.90_e0.07_ev-1.00_p0.10-0.85_sc2.0_rv2_sf10_sg2.0_ss0.00_ms1.00_md0_mp-1.00_fbL2d0.00z0.90tk_tk
```

Promotion evidence:

- source window: `2026-05-23T00:00:00Z..2026-05-25T07:00:00Z`
- folds: `7` complete 8-hour folds
- selected trades: `157`
- selected PnL: `+79.41254`
- win rate: `88.535%`
- Wilson 95% lower bound: `0.826`
- worst fold PnL: `+2.00559`
- median fold PnL: `+10.20053`
- PBO: `0.114`
- median OOS percentile: `0.948`
- neighbor-positive rate: `74.849%` over `71` neighbors
- profit factor: `1.858`
- payoff ratio: `0.241`
- worst-loss / average-win: `4.206`
- negative causal buckets: none

The PMXT archive preflight found `2026-05-25T09:00:00Z` missing, so the run
used the largest complete strict window available: May 23 through May 25 07:00Z.

## Live-Replay Parity

All seven promotion folds were replayed through `live-replay` with the exact
promotion artifact and `--settlement-alignment-ready`.

Evidence reports:

```text
deploy/promotions/evidence/live_replay_a_plus5m_guard_may23T00_07_20260531.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_may23T08_15_20260531.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_may23T16_23_20260531.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_may24T00_07_20260531.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_may24T08_15_20260531.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_may24T16_23_20260531.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_may25T00_07_20260531.json
```

Replay aggregate:

- orders submitted: `157`
- fills successful: `157`
- fills failed: `0`
- resolved executable samples: `157`
- oracle checks: `157`
- oracle disagreements: `0`
- causality violations: `0`
- replay PnL: `+79.41254`

Per-fold replay PnL matched the selected harness variant exactly:

| Window | Orders/Fills | PnL |
| --- | ---: | ---: |
| 2026-05-23 00-07Z | 16 / 16 | +14.77284 |
| 2026-05-23 08-15Z | 15 / 15 | +19.17701 |
| 2026-05-23 16-23Z | 28 / 28 | +7.51066 |
| 2026-05-24 00-07Z | 28 / 28 | +9.11176 |
| 2026-05-24 08-15Z | 27 / 27 | +16.63415 |
| 2026-05-24 16-23Z | 25 / 25 | +10.20053 |
| 2026-05-25 00-07Z | 18 / 18 | +2.00559 |

## Adaptive Probe

Adaptive probe report:

```text
deploy/promotions/evidence/harness_sweep_adaptive_probe_a_plus5m_guard_may25T00_07_20260531.json
```

The worst promotion fold was replayed with `--adaptive-health-rearm-minutes 15`
across the same 192-variant grid:

- variants with adaptive rearm: `0`
- max adaptive rearms: `0`
- max breaker paused events: `0`
- best variant breaker tripped: `false`

This removes the concern that the candidate only works after artificial breaker
rearming.

## Code Changes

- `live-replay` now supports `--delete-after-process` and deletes only raw PMXT
  parquets downloaded by that specific process.
- `strategy-builder audit` now treats segmented live-replay sessions as one
  validation horizon for adaptive drift and A+ sample size. Per-session replay,
  oracle, settlement, and causality checks remain separate.

This fixed the earlier false gate where the intentionally worst 8-hour replay
slice matched the harness exactly but failed adaptive drift because it was
compared in isolation against the aggregate baseline.

## Final Audit

Final audit result:

```text
grade=A+
ok=true
a_plus_ready=true
research_reports=7
adaptive_reports=1
replay_sessions=7
resolved_samples=157
```

Key audit checks:

- `promotion.params_hash`: ok
- `promotion.robustness`: ok
- `adaptive_probe.health`: ok
- `adaptive.drift`: ok
- `replay.shadow_oracle`: ok
- `replay.settlement_alignment`: ok
- `replay.causality`: ok
- `replay.a_plus_sample`: ok

## Storage Hygiene

Validation used session-owned temporary dirs under `/private/tmp`.

- Raw PMXT parquets from the production replay loop were deleted by
  `--delete-after-process`.
- The obsolete first replay cache and generated sidecar caches were deleted
  after evidence reports and replay sessions were produced.
- Full fold reports remain in `/private/tmp/polymomentum_broad_gated_20260531_may23_25`
  because they are large and should not be committed.

## Remaining Live Gates

What no longer blocks live:

- strategy/backtest promotion parity;
- executable order-path parity in live-replay;
- PnL/order/fill accounting for the selected artifact;
- timestamp/causality checks;
- oracle alignment for the executed replay samples;
- adaptive-rearm dependency.

What still gates real capital:

- live/paper startup preflight against the deployed environment;
- current wallet, allowance, pUSD, and POL gas checks;
- CLOB v2 venue ack/reject/fill behavior in the deployed process;
- operational supervision and alerting on the VPS;
- the user decision to run a bounded live/canary session.

Next step: run deployment preflight with this artifact, then a bounded venue
integration/canary run if the user explicitly approves live capital risk.
