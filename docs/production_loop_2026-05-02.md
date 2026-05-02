# Production loop - 2026-05-02

Scope: local diagnostics and local PMXT cache only. No VPS service, peer bot
directory, peer systemd unit, or shared parquet cache was modified.

## Code changes

- Migrated the raw CLOB order signer and order wire body to the documented CLOB
  V2 shape in the previous commit on this branch.
- Corrected authenticated CLOB header names to the current `POLY_*` form.
- Added read-only authenticated CLOB diagnostics:
  `clob orders`, `clob order <id>`, and `clob trades`.
- Added authenticated `clob heartbeat` support and a live-mode heartbeat loop
  so automated/live maker orders do not miss CLOB heartbeat safety.
- Added a `wallet` ready/not-ready line based on pUSD, both V2 pUSD allowances,
  and POL gas.
- Added visible PMXT/Gamma/harness progress output for long archive backtests.

Official references checked:

- https://docs.polymarket.com/v2-migration
- https://docs.polymarket.com/trading/orders/create
- https://docs.polymarket.com/api-reference/authentication
- https://docs.polymarket.com/api-reference/orders/get-active-orders
- https://docs.polymarket.com/api-reference/orders/get-order-by-id
- https://docs.polymarket.com/api-reference/trades/get-trades
- https://docs.polymarket.com/api-reference/orders/heartbeat
- https://docs.polymarket.com/developers/CLOB/websocket/wss-user

## Paper loop

Latest post-change run:

- Session: `/private/tmp/polymomentum-prodloop3/logs/sessions/session_20260502_073912.jsonl`
- Duration: about 1.1 minutes.
- Feeds connected: Binance, Bybit, OKX, Binance alt, Bybit alt.
- Gamma markets fetched: 3,146.
- Active candle contracts scanned: 124.
- Session diagnostics: `ok=true`.
- Events: 787 total, 0 malformed.
- Signal evaluations: 390.
- Decision trades: 0.
- Execution attempts: 0.
- Orders placed/filled/rejected: 0/0/0.
- System errors/fatal errors: 0/0.
- Replay validation: `total=390 mismatches=0 (0.00%)`.

Interpretation: current paper runtime plumbing is healthy, but the short sample
remains inert. It proves schema/replay/runtime liveness, not alpha.

## Backtest loop

Session sweep:

```bash
polymomentum-engine sweep --session session_20260502_073912.jsonl --min-trades 0
```

Result:

- All default strategy variants produced 0 trades because the paper session had
  no decision trades and no resolutions.
- Parser/replay compatibility is confirmed for the latest session schema.
- No statistical strategy conclusion can be drawn from this run.

Archive harness attempt:

- Command used one cached local PMXT hour:
  `2026-04-25T10:00:00Z`, cache
  `/Users/ttoomm/Documents/PolyMomentum/data/pmxt_cache`, BTC tape
  `/private/tmp/pm_btc_ticks_20260425.csv`, `--threads 1`.
- Sandbox run failed on Gamma network access after PMXT condition-id discovery.
- Network-enabled run was stopped after more than 20 minutes with no user-visible
  progress.

Fix from that attempt:

- Added explicit PMXT/Gamma/harness progress output so long archive runs show
  where they are spending time.

Remaining archive issue:

- One-hour parquet harness latency is still too slow for an inner production
  loop unless the hour is pre-distilled or the sidecar/shared candle cache is
  reused.

## Preflight

Paper preflight:

- `ok=true`.
- Runtime paths are under `/private/tmp/polymomentum-prodloop2`.
- Peer private path guard passes.
- Paper mode does not initialize live CLOB order placement.

Live-shaped local preflight:

- Venue/compliance and CLOB V2 flag can pass with explicit env.
- It still fails safely without live credentials and `ALERT_REQUIRED=1`.
- This was only a local dry preflight; no order endpoint was posted.

## Current grade

Current state: C.

Paper runtime and V2 order-shape readiness improved, but production capital
still needs authenticated user-channel reconciliation evidence, funded canary
evidence, promotion artifact evidence, and a practical archive harness loop.

## Next steps

1. Add authenticated user WebSocket reconciliation and feed order/trade events
   into `OrderManager`.
2. Add REST fallback reconciliation using `clob orders`, `clob order`, and
   `clob trades`; never record a live fill until exchange state confirms it.
3. Run `wallet` on the VPS and require `live_ready yes` before any live canary.
4. Run 24-48h current binary in VPS paper mode with daily diagnostics:
   session ok, replay mismatches, order lifecycle counts, unresolved positions,
   feed gaps, Gamma errors, and CLOB REST health.
5. Pre-distill target PMXT hours or use the shared distilled candle cache before
   broad harness/sweep work.
6. Promote only a backtest variant with sufficient out-of-sample trades, positive
   net PnL after fees/slippage, and no single-window concentration.
7. Only after steps 1-6, run a tiny live canary with manual supervision,
   alerting enabled, and reconciliation logs checked order by order.
