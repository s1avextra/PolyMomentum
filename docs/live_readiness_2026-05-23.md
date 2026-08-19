# Live Readiness - 2026-05-23

Scope: PolyMomentum 5-minute BTC candle strategy, Dublin VPS, international
CLOB. This note records the A+ offline gate and the exact production binding.

## Current Grade

Grade: `A+` for backtest/live-replay readiness.

The promoted strategy is tracked at:

```text
deploy/promotions/promotion_early_scout_wr_c030_z050_e003_cap15_20260423_25.json
```

Remote deploy path:

```text
/opt/polymomentum/config/promotion_early_scout_wr_c030_z050_e003_cap15_20260423_25.json
```

## Evidence

- Aggregate promotion, 2026-04-23 through 2026-04-25:
  - trades: `416`
  - win rate: `71.39%`
  - Wilson lower bound: `0.669`
  - total PnL: `+157.12`
  - profitable days: `3/3`
  - unresolved fills: `0`
  - passive failed execution attempts: `108`
- Per-day maker harness:
  - 2026-04-23: `160` trades, `+66.43`, win rate `70.00%`
  - 2026-04-24: `144` trades, `+38.66`, win rate `72.22%`
  - 2026-04-25: `112` trades, `+52.03`, win rate `72.32%`
- 2026-04-25 live-replay through the live path:
  - PMXT events processed: `75,547,855`
  - orders submitted: `130`
  - filled: `84`
  - passive failed: `46`
  - average book age: `4.68 ms`
  - oracle checks: `84`
  - total PnL: `+51.17`
  - system errors: `0`
- Strategy-builder audit:
  - `grade=A+`
  - `ok=true`
  - `a_plus_ready=true`

## Required Runtime Binding

The A+ result is strategy-identical only with these production constraints:

```text
POLYMOMENTUM_PROMOTION_ARTIFACT=/opt/polymomentum/config/promotion_early_scout_wr_c030_z050_e003_cap15_20260423_25.json
POLYMOMENTUM_REQUIRE_PROMOTION=1
VENUE=polymarket_international
OPERATOR_COUNTRY=IE
POLYMOMENTUM_VENUE_COMPLIANCE_OK=1
POLYMARKET_US_API_ENABLED=0
CLOB_V2_READY=1
POLYMOMENTUM_LIVE_RECONCILIATION_READY=1
CANDLE_SETTLEMENT_ALIGNMENT_READY=true
CANDLE_WINDOW_MINUTES=5
BANKROLL_USD=100
CANDLE_POSITION_PCT=0.05
MAX_TOTAL_EXPOSURE_USD=15.0
MAX_POSITION_PER_MARKET_USD=20.0
LIVE_ALLOW_MAKER_ORDERS=1
LIVE_MIN_ORDER_SIZE_SHARES=5.0
LIVE_ORDER_BUDGET_BUFFER=1.10
ALERT_REQUIRED=1
```

Do not run this promotion as taker-only. The selected variant is a maker
strategy, and prior taker validation was negative or breaker-tripped.

## Wallet Gate

Live preflight must show pUSD, both CTF Exchange V2 pUSD allowances, and POL
cover the configured first-order budget. With the binding above, the wallet
needs at least `$11.00` pUSD and both pUSD allowances after the default `1.10`
buffer. More is better; the exposure cap still limits open risk to `$15.00`.

## Deploy Commands

Paper service with the promoted artifact:

```bash
bash deploy/deploy.sh vps --enable-service --mode paper --binary /private/tmp/polymomentum-release/polymomentum-engine-linux-x86_64 --promotion-artifact deploy/promotions/promotion_early_scout_wr_c030_z050_e003_cap15_20260423_25.json
```

Live service after paper/deploy wiring and wallet preflight are green:

```bash
bash deploy/deploy.sh vps --enable-service --mode live --i-understand-live --binary /private/tmp/polymomentum-release/polymomentum-engine-linux-x86_64 --promotion-artifact deploy/promotions/promotion_early_scout_wr_c030_z050_e003_cap15_20260423_25.json
```

## VPS Coexistence

- Heavy sweeps and harness runs stay on the dev box.
- The VPS only runs the service, read-only diagnostics, deployment preflight,
  and bounded paper/live integration.
- PMXT scratch data for future tests must use the shared testing-session cache
  protocol and may be deleted only under that protocol.
