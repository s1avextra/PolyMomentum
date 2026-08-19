# Adaptive and EV-Guard Validation - 2026-05-28

Scope: 5-minute BTC candle strategy research using PMXT v2 L2 order flow,
continuous live-mirrored backtest state, and atomic parquet
download/replay/delete. No paper or live orders were used.

## Code changes

- Added stress-drawdown caps as a sweep dimension:
  `--max-projected-stressed-drawdown-pct` now accepts CSV values.
- Added a feed-forward degraded execution fallback:
  - activate after realized loss count and drawdown thresholds;
  - tighten z-score floors;
  - optionally cap max executable price;
  - optionally force taker execution.
- Mirrored the degraded execution fields in backtest, live replay, and live
  pipeline runtime strategy loading so promotion artifacts can carry the same
  behavior to paper/live.
- Added strategy profiles:
  - `a_plus5m_regime`
  - `a_plus5m_adaptive`
  - `a_plus5m_adaptive_price`
  - `a_plus5m_ev_guard`

## Validation Runs

### Stress drawdown cap probe

Path:
`/private/tmp/polymomentum_regime_veto_20260528_apr25_late_probe`

Result: rejected. Tight caps stopped losses sooner but did not create a
robustly profitable fold. This is not sufficient as a production fix.

### Adaptive fallback fold-009 probe

Path:
`/private/tmp/polymomentum_adaptive_veto_20260528_apr25_late_probe`

Best targeted result:

- variant:
  `early_c0.40_z0.50_e0.03_ev-1.00_p0.10-0.90_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_fbL2d0.00z0.90tk_mk`
- trades: `17`
- wins/losses: `13/4`
- PnL: `+1.01909`
- breaker: no

Conclusion: adaptive fallback fixes the original Apr25 16-23 UTC failure in
isolation, but that alone is not promotion evidence.

### Adaptive early-only 72h run

Path:
`/private/tmp/polymomentum_adaptive_history_20260528_apr23_25`

Result: strict robust promotion rejected. Best family was positive in `7/9`
folds, total PnL about `+43.55`, but failed Apr23 00-07 UTC and Apr25
00-07 UTC. Early-only is not A+.

### Adaptive all-zone 72h run

Path:
`/private/tmp/polymomentum_adaptive_allzones_history_20260528_apr23_25`

Result: strict robust promotion rejected. Best aggregate candidate:

- variant:
  `all_c0.40_z1.10_e0.03_ev-1.00_p0.10-0.90_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_fbL2d0.00z0.90tk_mk`
- profitable folds: `8/9`
- total PnL: `+52.28231`
- total trades: `212`
- wins/losses: `182/30`
- worst fold: fold 8, PnL `-10.00735`
- neighbor-positive rate: about `0.5217`
- breaker: no

Conclusion: all-zone adaptive is the best research candidate so far, but it is
not A+. The failure is not order mechanics; it is strategy robustness and
loss-size asymmetry.

### Fold-008 stricter edge probe

Path:
`/private/tmp/polymomentum_edge_probe_20260528_fold008`

Result: stricter z/edge/max-price gates can make fold 8 positive, but only by
starving the strategy:

- positive top variants: `3-7` trades
- every variant with at least `15` trades remained negative

Conclusion: simply raising z/edge is not a production fix.

### Fold-008 EV-buffer probe

Path:
`/private/tmp/polymomentum_ev_probe_20260528_fold008`

Result: `ev_buffer=0.05` made fold 8 slightly positive, but with only `5-6`
trades.

Conclusion: the EV buffer behaves as an abstention gate. It is useful as a
research signal, not yet a deployable policy.

### EV-guard 72h run

Path:
`/private/tmp/polymomentum_ev_guard_history_20260528_apr23_25`

Result: robust promotion rejected with PBO `0.6111` above the `0.50` gate.

Aggregate view:

- best taker family: profitable `6/9`, total PnL about `+9.08`, total trades
  `28`, min trades `0`, worst PnL `-3.47`
- best maker family: profitable `5/9`, total PnL about `+12.26`, total trades
  `15`, min trades `0`, worst PnL `0.00`

Conclusion: EV guard avoids the large losing fold but over-abstains and becomes
a tiny-sample strategy. This is not A+.

## Storage and VPS Safety

- All heavy runs were local under `/private/tmp`.
- No paper/live orders were sent.
- No VPS-heavy sweep was run.
- Atomic PMXT parquet handling worked: completed run dirs retained no
  `*.parquet` or `*.tmp.*` files.
- The two direct fold probes left local sidecar caches; those temporary cache
  dirs were deleted after parsing. Their JSON reports were retained.

## Current Grade

Strategy readiness grade: `B+ research / not production A+`.

Reasons:

- backtest/live-replay mechanics are much stronger now;
- order flow is processed in the correct continuous path;
- PnL math uses binary payoff after fees and is not the blocker;
- adaptive fallback improves weak windows;
- no tested strategy family passes robust 9-fold, neighbor-stability, and PBO
  gates simultaneously.

## Next A+ Steps

1. Add per-trade loss-asymmetry diagnostics to experiment reports:
   average win, average loss, max loss, profit factor, and payoff by zone.
2. Add causal regime tags at decision time:
   volatility percentile, trend/chop/reversion count, price bucket, time zone,
   and market-price/fair-value edge bucket.
3. Promote only feed-forward rules:
   regime selection must use information available before each trade or from
   prior resolved trades, never fold timestamps or future fold PnL.
4. Test a two-layer policy:
   opportunity detector first, execution strategy second. Abstention can be
   valid, but only if the opportunity detector itself has enough historical
   support and low PBO.
5. Re-run atomic rolling history across more days, one parquet hour at a time,
   deleting every downloaded parquet after processing.
