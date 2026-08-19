# Production loop - 2026-04-30

Scope: local diagnostics only. No VPS services were modified.

## Paper loop

Runtime env wrote all artifacts under `/private/tmp/polymomentum-prodloop`.

First sandbox run:

- Session: `session_20260430_091444.jsonl`
- Result: DNS/network resolution failed inside the sandbox.
- Problem found: startup wait for first BTC price ignored SIGINT and could hang
  forever when feeds never produced a tick.

Fix:

- Startup wait now records `startup_price_wait` errors after 30 seconds without
  BTC price.
- Startup wait now exits cleanly on SIGINT/SIGTERM and saves a summary.

Network-enabled paper run:

- Session: `session_20260430_091629.jsonl`
- Duration: about 1 minute.
- Feeds connected: Binance, Bybit, OKX, Binance alt, Bybit alt.
- Gamma markets fetched: 4,146.
- Active candle contracts scanned: 119.
- Signal evaluations: 208.
- Replay validation: `0` mismatches.
- Orders/fills: 0.
- State DB: 0 trades, 0 paper positions, 0 oracle pending, 0 meta.

Post-fix network-enabled paper run:

- Session: `session_20260430_092314.jsonl`
- Active candle contracts scanned: 120.
- Signal evaluations: 180.
- Replay validation: `0` mismatches.
- Orders/fills: 0.
- New evaluation fields present:
  `decision_trade=false`, `execution_attempted=false`, `traded=false`.

## Paper/live parity fixes

Problems found by code inspection during the loop:

- Paper fills did not emit the same order lifecycle events as live.
- `signal.evaluation.traded=true` meant the strategy wanted to trade, not that
  execution happened.
- Live treated a CLOB accepted order ID as a confirmed fill.

Fixes:

- `signal.evaluation` now separates `decision_trade`, `execution_attempted`,
  and `traded`.
- `validate-replay` uses `decision_trade` when available and falls back to old
  `traded` logs for backward compatibility.
- Paper fills now emit `order.placed` and `order.filled` events.
- Live now records CLOB acceptance as `order.placed` only and logs
  `candle.trade.live.accepted_unconfirmed`; it no longer records a fill, trade,
  position, or `traded` state without reconciliation.

## Backtest loop

Command:

```bash
polymomentum-engine sweep --session session_20260430_092314.jsonl --min-trades 0
```

Result:

- Sweep completed cleanly.
- All strategy variants reported 0 trades because the short session had no
  trade decisions and no resolutions.
- This confirms parser/replay compatibility for the new evaluation fields, but
  does not prove PnL parity yet.

## Remaining parity gap

Paper and live are safer and closer, but not yet identical. The remaining
blocker is the planned `OrderManager`/reconciliation layer:

- user WebSocket order updates,
- REST `getOrder`/`getTrades` fallback,
- explicit accepted/open/partial/matched/confirmed/cancelled states,
- replayable paper adapter using the same state machine.
