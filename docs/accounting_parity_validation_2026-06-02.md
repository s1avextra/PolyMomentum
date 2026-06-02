# Accounting Parity Validation - 2026-06-02

## Scope

Local dev-box validation of the runtime bankroll and realized-PnL accounting
pipeline after refactoring. No VPS runtime, paper mode, live venue connection,
or peer bot cache was touched.

Validation artifact:

```text
deploy/promotions/promotion_candidate_a_plus5m_guard_may23_25_20260531.json
```

Selected strategy:

```text
all_c0.40_z0.90_e0.07_ev-1.00_p0.10-0.85_sc2.0_rv2_sf10_sg2.0_ss0.00_ms1.00_md0_mp-1.00_fbL2d0.00z0.90tk_tk
```

## Finding

The first exact one-hour replay found a real accounting mismatch:

- live-replay PnL: `-4.15462`
- harness PnL before fix: `-4.09727`
- cause: live-replay sized the second trade from active bankroll after the
  first realized loss; harness still sized from the static starting bankroll.

## Fix

Backtest harness sizing now mirrors live-replay sizing:

- position notional uses `starting_bankroll + realized_pnl`, floored at zero;
- exposure cap uses `min(active_bankroll * 0.80, max_total_exposure_usd)`;
- depleted bankroll, exposure cap, and stress-drawdown cap produce distinct
  skip reasons;
- live-replay receives `settings.max_total_exposure_usd` so the same global
  exposure cap applies in replay and live-shaped execution.

## Evidence

Focused one-hour parity after the fix:

- window: `2026-05-23T00:00:00Z..2026-05-23T00:59:59Z`
- trades: `2`
- harness PnL: `-4.15462`
- live-replay PnL: `-4.15462`
- PnL delta: `0`
- fee delta: `0`

Full May 23 24h parity, split into three 8-hour folds:

| Window | Trades | Wins | Losses | Harness PnL | Live-replay PnL | Fee Delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-05-23 00-07Z | 16 | 15 | 1 | +15.58997 | +15.58997 | 0 |
| 2026-05-23 08-15Z | 15 | 15 | 0 | +20.97455 | +20.97455 | 0 |
| 2026-05-23 16-23Z | 28 | 24 | 4 | +7.01139 | +7.01139 | 0 |

Aggregate:

- trades: `59`
- wins/losses: `54 / 5`
- harness PnL: `+43.57591`
- live-replay PnL: `+43.57591`
- fees: `4.54119`
- oracle checks: `59`
- replay fills failed: `0`
- causality violations: `0`

Diagnostics:

- all three replay sessions: `ok=true`
- all three causality checks: `ok=true`
- oracle disagreements: `0`
- warning only: two May 23 16-23Z resolutions and two May 23 00-07Z
  resolutions were within `$5` BTC of the candle threshold, so settlement-basis
  risk remains elevated for those samples.

## Storage Hygiene

Validation used session-owned scratch data under:

```text
/private/tmp/polymomentum_accounting_validation_20260602
```

Raw PMXT parquets downloaded by the validation run were deleted. Reports and
session logs were left in place as temporary evidence.

## Production Impact

The previous 2026-05-31 A+ evidence remains directionally useful, but its PnL
numbers are superseded because the sizing model changed. Before live/canary,
regenerate the promotion gate across the full May 23-25 fold set with the
active-bankroll harness, then replay the selected artifact through live-replay
again.
