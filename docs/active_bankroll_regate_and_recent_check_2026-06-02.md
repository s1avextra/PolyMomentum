# Active-Bankroll Regate And Recent Check - 2026-06-02

## Scope

Local dev-box validation only. No VPS process, paper mode, live venue
connection, wallet, or peer bot cache was touched.

The gate reused the May 31 A+ data horizon and profile, but with the corrected
active-bankroll sizing model:

- window: `2026-05-23T00:00:00Z..2026-05-25T07:00:00Z`
- folds: `7` complete 8-hour folds
- profile: `a_plus5m_causal_guard`
- zone mode: `all`
- atomic parquet: enabled
- fold cache deletion: enabled

Regenerated artifact:

```text
deploy/promotions/promotion_candidate_a_plus5m_guard_active_bankroll_may23_25_20260602.json
```

Compact evidence:

```text
deploy/promotions/evidence/rolling_history_a_plus5m_guard_active_bankroll_may23_25_20260602_manifest.json
deploy/promotions/evidence/rolling_history_recent_may25T08_20260602_manifest.json
```

## Regenerated Gate

The regenerated robust gate passed and selected the same strategy hash as the
May 31 artifact:

```text
all_c0.40_z0.90_e0.07_ev-1.00_p0.10-0.85_sc2.0_rv2_sf10_sg2.0_ss0.00_ms1.00_md0_mp-1.00_fbL2d0.00z0.90tk_tk
```

After adding the median-OOS hard gate, the same seven reports were re-promoted
with:

```text
--min-median-oos-percentile 0.80
```

The selected artifact passed with median OOS percentile `0.823`.

Old vs regenerated:

| Metric | May 31 Artifact | Active-Bankroll Regate |
| --- | ---: | ---: |
| Trades | 157 | 157 |
| Total PnL | +79.41254 | +81.20911 |
| Win rate | 88.535% | 88.535% |
| Worst fold PnL | +2.00559 | +1.48421 |
| Median fold PnL | +10.20053 | +10.10251 |
| Robust score | 0.5452 | 0.5333 |
| PBO | 0.114 | 0.114 |
| Median OOS percentile | 0.948 | 0.823 |
| Neighbor positive rate | 74.849% | 73.843% |
| Profit factor | 1.858 | 1.796 |
| Payoff ratio | 0.241 | 0.233 |
| Worst-loss / avg-win | 4.206 | 4.847 |
| Max stressed drawdown pct | 10.33% | 12.20% |

Interpretation:

- Corrected active-bankroll sizing increased aggregate PnL by `+1.79657`.
- It also made robustness slightly worse: lower median OOS percentile, lower
  robust score, lower worst-fold PnL, and worse loss asymmetry.
- The candidate still passes the strict gate, but the May 31 artifact's exact
  PnL numbers are superseded by this active-bankroll artifact.

Selected-strategy fold PnL after active-bankroll sizing:

| Fold | Window | Trades | Wins | Losses | PnL |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 2026-05-23 00-07Z | 16 | 15 | 1 | +15.58997 |
| 2 | 2026-05-23 08-15Z | 15 | 15 | 0 | +20.97455 |
| 3 | 2026-05-23 16-23Z | 28 | 24 | 4 | +7.01139 |
| 4 | 2026-05-24 00-07Z | 28 | 24 | 4 | +8.71370 |
| 5 | 2026-05-24 08-15Z | 27 | 24 | 3 | +17.33278 |
| 6 | 2026-05-24 16-23Z | 25 | 22 | 3 | +10.10251 |
| 7 | 2026-05-25 00-07Z | 18 | 15 | 3 | +1.48421 |

## Improvement Implemented

Robust promotion now supports a hard median-OOS-percentile gate:

```text
--min-median-oos-percentile <float>
```

`strategy-builder rolling-history` now passes this gate to robust promotion and
defaults it to `0.80`.

Why this matters:

- PBO alone catches how often the selected family underperforms OOS.
- Median OOS percentile catches whether the selected family is consistently
  high-ranked out of sample.
- The active-bankroll regate passed at `0.823`, but the drop from the old
  `0.948` is now visible and enforceable.

## Freshest Available Data

Archive preflight showed no PMXT hours available from `2026-05-26T00:00:00Z`.
The freshest available post-window hour was only:

```text
2026-05-25T08:00:00Z
```

That one-hour freshness gate failed promotion, correctly:

- target events: `2,661,237`
- variants tested: `192`
- positive variants: `0`
- best variant: `-1.35190` PnL on `3` trades
- selected active-bankroll candidate: `-7.00492` PnL on `4` trades
- top coverage variant: `-5.16314` PnL on `5` trades
- cache deleted: `true`

This is not enough data to replace the multi-fold artifact. It is a useful
freshness warning: the first available hour after the promotion horizon was a
losing hour across the whole searched family.

## Storage Hygiene

Raw PMXT parquets downloaded by both runs were deleted by the atomic replay
loop. The retained scratch roots contain compact reports/manifests only:

```text
/private/tmp/polymomentum_regate_active_20260602_may23_25
/private/tmp/polymomentum_recent_gate_20260602_may25T08
```

## Live-Replay Parity Gate

Completed after the active-bankroll regate. All seven promotion folds were
replayed through `live-replay` with the regenerated promotion artifact and
`--settlement-alignment-ready`.

Persisted replay evidence:

```text
deploy/promotions/evidence/live_replay_a_plus5m_guard_active_bankroll_may23T00_07_20260602.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_active_bankroll_may23T08_15_20260602.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_active_bankroll_may23T16_23_20260602.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_active_bankroll_may24T00_07_20260602.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_active_bankroll_may24T08_15_20260602.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_active_bankroll_may24T16_23_20260602.json
deploy/promotions/evidence/live_replay_a_plus5m_guard_active_bankroll_may25T00_07_20260602.json
```

Aggregate replay result:

- events processed: `172,460,260`
- orders submitted: `157`
- fills successful: `157`
- fills failed: `0`
- oracle checks: `157`
- oracle disagreements: `0`
- total cost: `851.46210`
- total fees: `12.21879`
- total PnL: `+81.20911`

The aggregate replay totals match the regenerated harness reports exactly:

| Metric | Regenerated Harness | Live-Replay | Delta |
| --- | ---: | ---: | ---: |
| Trades / orders | 157 | 157 | 0 |
| PnL | +81.20911 | +81.20911 | 0.00000 |
| Fees | 12.21879 | 12.21879 | 0.00000 |

Per-fold replay PnL also matched the selected harness variant exactly:

| Fold | Window | Orders/Fills | Replay PnL | Delta vs Harness |
| ---: | --- | ---: | ---: | ---: |
| 1 | 2026-05-23 00-07Z | 16 / 16 | +15.58997 | 0.00000 |
| 2 | 2026-05-23 08-15Z | 15 / 15 | +20.97455 | 0.00000 |
| 3 | 2026-05-23 16-23Z | 28 / 28 | +7.01139 | 0.00000 |
| 4 | 2026-05-24 00-07Z | 28 / 28 | +8.71370 | 0.00000 |
| 5 | 2026-05-24 08-15Z | 27 / 27 | +17.33278 | 0.00000 |
| 6 | 2026-05-24 16-23Z | 25 / 25 | +10.10251 | 0.00000 |
| 7 | 2026-05-25 00-07Z | 18 / 18 | +1.48421 | 0.00000 |

Diagnostics:

- session diagnostics: `ok=true` on all seven sessions
- malformed lines: `0`
- system errors / fatal errors: `0 / 0`
- order rejects: `0`
- passive rejects: `0`
- breaker trips: `0`
- causality checks: `ok=true` on all seven sessions
- causality timing violations: `0`
- missing timing records for fills: `0`

Settlement-basis warning:

- four folds had near-threshold resolutions
- total near-threshold resolutions: `10`
- minimum observed absolute BTC move: `$0.34`
- oracle disagreements still remained `0 / 157`

Interpretation: offline order-path parity is restored for the regenerated
active-bankroll artifact. The exact harness/live-replay match means the
strategy, sizing, fees, fills, realized PnL, and replayed order lifecycle are
now aligned across the backtest and live-replay surfaces. The near-threshold
settlement warnings are not a parity failure, but they remain a production risk
control input for bounded canary sizing.

## Updated Gate Status

Backtest/live-replay strategy validation is complete for this artifact. Paper
mode is still not needed for strategy proof; it should only be used for
deployment plumbing that offline replay cannot validate: credentials, wallet
state, allowances, CLOB v2 ack/reject/fill behavior, process supervision, and
VPS alerts.
