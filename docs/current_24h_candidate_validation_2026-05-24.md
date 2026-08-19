# Current 24h Candidate Validation - 2026-05-24

Scope: local dev-box validation using freshly downloaded PMXT v2 order-book
archives for 2026-05-23T03:00:00Z through 2026-05-24T02:00:00Z. The VPS was
not used for CPU-heavy sweeps. The running VPS paper process remained on the
previous production artifact:

`promotion_early_scout_wr_c030_z050_e003_cap15_20260423_25.json`.

## Data

- PMXT hours: 24
- 5-minute BTC candle contracts: 288
- Full eval-cache events scanned: 65,528,876
- Feed-forward evaluations emitted: 1,166,795
- Resolutions emitted: 288
- Temporary local cache root: `/private/tmp/polymomentum_current_24h_20260524T0322Z`

Per the time-limited cache rule, the temporary local cache was deleted after
this validation summary was captured.

## Active Artifact Check

Promoted active shape:

`early_c0.30_z0.50_e0.03_ev-1.00_p0.10-0.90_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk`

Fast feed-forward sweep over the current 24h:

- Trades: 133
- Wins/losses: 100 / 33
- Win rate: 75.2%
- PnL at 5% sizing: +32.33

Train/holdout split showed instability:

- Train 2026-05-23T03..14: active-nearby variants were strongly positive.
- Holdout 2026-05-23T15..2026-05-24T02: the active shape was negative
  despite high win rate because the bad half had worse loss severity.

Conclusion: the active artifact is operationally valid but should not be
promoted to live based only on this current-regime evidence.

## Candidate

Created candidate artifact:

`deploy/promotions/promotion_candidate_early_guard_c040_z070_e003_cap15_20260523_24.json`

Strategy hash:

`615603bd20242aa4c58332d3d9bb7c10c871f7215f5cac6455188e6c8ab73cfb`

Shape:

`early_c0.40_z0.70_e0.03_ev-1.00_p0.10-0.75_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk`

Feed-forward split checks:

- Train 12h: 27 trades, 24 wins, 3 losses, +37.93
- Holdout 12h: 35 trades, 27 wins, 8 losses, +11.93
- Full 24h fast sweep: 59 trades, 48 wins, 11 losses, +42.81

Full L2 harness-sweep for the exact candidate:

- Fills: 57
- Attempts: 69
- Fill rate: 82.6%
- Failed maker attempts: 12
- Wins/losses: 46 / 11
- PnL: +53.85
- PnL/trade: +0.945
- Breaker: no

Bounded exact live-replay over 2026-05-23T15:00:00Z..20:00:00Z:

- Contracts: 72
- Events processed: 20,032,997
- Orders submitted: 19
- Fills: 12 success, 7 passive maker misses
- Average book age: 1.42 ms
- Resolutions: 12
- Wins/losses: 10 / 2
- PnL: +10.15
- Oracle disagreements: 0
- Replay validator: 12,976 evaluations, 0 mismatches
- Diagnostics: ok=true

## Decision

The strict candidate is better than the active artifact on this current 24h
sample and survives a train/holdout split, but it is still a single-day
candidate. Do not deploy it live yet. Next gate is the same feed-forward
procedure on at least two additional fresh, non-overlapping 24h windows, then
aggregate promotion.

The Dublin VPS paper process was checked after local validation and was still
running on the previous artifact. The strict candidate was not deployed.

## Follow-up Improvements

- Make `eval-cache` stream rows as it scans instead of buffering all rows until
  the final atomic write.
- Keep `sweep --position-pct` and `--max-per-market-usd` aligned with promotion
  artifacts so strategy-lab PnL does not hide sizing assumptions.
- Use exact `live-replay` only for order lifecycle and runtime parity; use
  feed-forward eval-cache and harness-sweep for validation that can be proven
  offline.
