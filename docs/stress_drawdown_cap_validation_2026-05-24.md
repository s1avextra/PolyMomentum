# Stress Drawdown Cap Validation - 2026-05-24

## Change

Implemented a feed-forward projected stressed-drawdown sizing cap:

- Strategy param: `max_projected_stressed_drawdown_pct`
- Runtime setting: `CANDLE_MAX_PROJECTED_STRESSED_DRAWDOWN_PCT`
- CLI sweep flag: `--max-projected-stressed-drawdown-pct`
- Default: `0.0` disabled, so existing promotion artifacts remain hash-compatible.

The cap uses only realized breaker state plus currently open/submitted exposure.
It caps or skips a new order if that order would push projected stressed
drawdown above the configured fraction. It is enforced in the L2 harness, live
runtime, and cached live-replay path.

## Validation

Candidate:

- Variant: `early_c0.40_z0.70_e0.03_ev-1.00_p0.10-0.75_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk`
- Position: `5%` bankroll, `20 USD` max per market
- New cap: `0.24`
- Fill model: maker-first L2 with 50 ms latency

| Window | Fills | W/L | Win rate | PnL | Breaker | Stress-cap skips |
| --- | ---: | ---: | ---: | ---: | --- | ---: |
| `2026-05-20T03:00Z..2026-05-21T02:00Z` | 98 | 67/31 | 68.37% | +7.92 | no | 8,216 |
| `2026-05-21T03:00Z..2026-05-22T02:00Z` | 53 | 41/12 | 77.36% | +59.59 | no | 0 |
| `2026-05-23T03:00Z..2026-05-24T02:00Z` | 57 | 46/11 | 80.70% | +53.85 | no | 0 |

Aggregate promotion:

- Artifact: `deploy/promotions/promotion_candidate_stresscap024_aggregate_20260520_24.json`
- Params hash: `d6f02682adf2c22a20a32cbaa9212657daebda48c61cde59e5622f9be8553e74`
- Aggregate fills: `208`
- Aggregate W/L: `154/54`
- Aggregate win rate: `74.04%`
- Wilson lower bound: `67.7%`
- Aggregate PnL: `+121.36`
- Worst daily PnL: `+7.92`
- Breakers: `0/3`

## Notes

The previously failing `2026-05-20..2026-05-21` fold is no longer a veto:
the cap reduced maker fills from `101` to `98` and turned the fold from
`open_exposure_stress` breaker to breaker-free positive PnL.

The remaining production caveats are explicit in the promotion risk notes:
passive maker had `25` failed attempts across the three reports
(`14` maker unfilled, `11` post-only crossed), and the strategy is still
100% concentrated in the early zone.

Temporary PMXT/eval caches were staged under
`/private/tmp/polymomentum_stresscap_gate_20260524T0800Z/` and should be
deleted after evidence is copied into the repo.
