# Continuous Promotion Gate - 2026-05-24

Scope: rerun the multi-window promotion gate after adding continuous harness
state and deterministic order-key maker fills. Validation stayed local; the VPS
and peer bots were not used for CPU-heavy work.

## Inputs

Original candidate:

```text
deploy/promotions/promotion_candidate_stresscap024_aggregate_20260520_24.json
```

Original candidate hash:

```text
d6f02682adf2c22a20a32cbaa9212657daebda48c61cde59e5622f9be8553e74
```

Replayed windows:

```text
2026-05-20T03:00:00Z..2026-05-21T02:00:00Z
2026-05-21T03:00:00Z..2026-05-22T02:00:00Z
2026-05-23T03:00:00Z..2026-05-24T02:00:00Z
```

## Exact Candidate Result

The exact old stress-cap candidate no longer passed strict aggregate gates
under continuous mechanics.

| Window | Variant | Attempts | Fills | Passive non-fills | W/L | PnL | Breaker |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 2026-05-20..21 | maker | 90 | 52 | 38 | 40/12 | +30.46 | no |
| 2026-05-21..22 | maker | 31 | 18 | 13 | 15/3 | +23.82 | no |
| 2026-05-23..24 | maker | 56 | 30 | 26 | 25/5 | +27.01 | no |

Aggregate rejection with old gates:

```text
trades 100 below minimum 180
total_pnl 81.29 below minimum 100
fills_failed 77 above maximum 30
daily trades 18 below minimum 30
```

Decision: do not promote the exact old candidate.

## Gate Improvement

The old `max_failed_fills` gate treated passive maker outcomes as equivalent
to true execution failures. That was too blunt once maker replay became more
life-like.

Implemented promotion-gate separation:

- `max_failed_fills`: non-passive execution failures only.
- `max_passive_failed_fills`: maker non-fills and post-only crosses.
- `min_fill_rate`: minimum fill rate across all execution attempts.

Passive reasons are:

```text
maker_unfilled
post_only_cross
```

True non-passive failures still fail closed by default.

## Continuous Grid Search

A bounded early-zone grid was run over the same three windows:

```text
conf=0.30,0.35,0.40
z=0.50,0.70
edge=0.03
max_price=0.75,0.90
position_pct=0.05
max_per_market_usd=20
max_projected_stressed_drawdown_pct=0.24
maker+taker
continuous=true
```

Persisted evidence:

```text
deploy/promotions/evidence/harness_sweep_continuous_grid_early_stresscap024_window3_20260520_21.json
deploy/promotions/evidence/harness_sweep_continuous_grid_early_stresscap024_window1_20260521_22.json
deploy/promotions/evidence/harness_sweep_continuous_grid_early_stresscap024_current_20260523_24.json
```

Selected variant:

```text
early_c0.30_z0.50_e0.03_ev-1.00_p0.10-0.90_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk
```

Strategy hash:

```text
a40d2eb7bc3f1747636f9dfbf4790f3ad4d2c745483d5a6e07da2284b6789df9
```

Per-window selected metrics:

| Window | Attempts | Fills | Fill rate | Passive non-fills | W/L | PnL | Breaker |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2026-05-20..21 | 173 | 105 | 60.69% | 68 | 77/28 | +55.61 | no |
| 2026-05-21..22 | 77 | 49 | 63.64% | 28 | 39/10 | +47.55 | no |
| 2026-05-23..24 | 126 | 70 | 55.56% | 56 | 57/13 | +51.43 | no |

Aggregate metrics:

| Metric | Value |
| --- | ---: |
| Fills | 224 |
| Attempts | 376 |
| Fill rate | 59.57% |
| Passive non-fills | 152 |
| Non-passive execution failures | 0 |
| Wins / losses | 173 / 51 |
| Win rate | 77.23% |
| Wilson lower bound | 71.3% |
| PnL | +154.59 |
| Worst daily PnL | +47.55 |
| Breaker trips | 0 / 3 |

Promotion artifact:

```text
deploy/promotions/promotion_candidate_continuous_grid_early_stresscap024_aggregate_20260520_24.json
```

Promotion gates used:

```text
min_trades=180
min_win_rate=0.70
min_wilson_win_rate_lower=0.65
min_total_pnl=100
max_unresolved_fills=0
max_failed_fills=0
max_passive_failed_fills=200
min_fill_rate=0.55
min_reports=3
min_profitable_reports=3
min_daily_trades=30
min_daily_pnl=0
```

## Live-Replay Parity

A first live-replay pass found a small drift: live-replay loaded every selected
condition ID for every replay hour, while the continuous harness loaded only
condition IDs whose 5-minute windows overlapped each hour. That allowed stale
out-of-window book updates to affect starting book state.

Fix: live-replay now uses the same per-hour `condition_id_set_for_hour` filter
as the continuous harness.

Verified current-day parity after the fix:

| Metric | Harness | Live-replay |
| --- | ---: | ---: |
| Events | 65,528,876 | 65,528,876 |
| Attempts | 126 | 126 |
| Fills | 70 | 70 |
| Passive non-fills | 56 | 56 |
| PnL | +51.43 | +51.43 |
| Oracle disagreements | n/a | 0 |
| Breaker | no | no |

Persisted live-replay evidence:

```text
deploy/promotions/evidence/live_replay_continuous_grid_early_stresscap024_current_20260523_24.json
```

Diagnostics on the filtered replay session:

```text
ok=true
critical_rejections=0
oracle_disagreements=0
breaker_tripped=false
warnings=56 passive non-fills; 4 near-threshold resolutions
```

## Preflight

Paper preflight passed:

```text
mode=paper
venue=paper_only
promotion_status=ok
trades=224
strategy_hash=a40d2eb7bc3f1747636f9dfbf4790f3ad4d2c745483d5a6e07da2284b6789df9
```

Live preflight correctly failed closed locally. Expected blockers:

```text
VENUE=paper_only
CLOB_V2_READY=0
POLYMOMENTUM_LIVE_RECONCILIATION_READY=0
ALERT_REQUIRED=0
LIVE_ALLOW_MAKER_ORDERS=0
wallet fetch failed
local /opt/polymomentum dirs missing
```

## Current Decision

This is a materially stronger promotion candidate than the exact old stress-cap
candidate under corrected mechanics. It is production-candidate quality for the
offline gate, but not yet permission to trade real money.

Required next steps before live canary:

1. Deploy the exact binary and artifact to paper on the VPS.
2. Run bounded paper only for venue/feed/process wiring, not strategy edge.
3. Confirm live preflight on the VPS with CLOB V2, reconciliation, alerting,
   maker-order permission, wallet budget, and pUSD readiness explicitly armed.
4. Start with a tiny maker canary only after those live gates pass.

Temporary PMXT, Gamma, replay, and session caches were created under:

```text
/private/tmp/polymomentum_continuous_promotion_gate_20260524T1210Z/
```

They were deleted after the evidence above was copied into the repo.
