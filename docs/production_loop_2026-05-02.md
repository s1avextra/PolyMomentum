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

## Cached live replay loop

Reason for this loop: paper/live parity can be tested much faster and more
causally by replaying cached PMXT L2 events through the same live decision and
diagnostics path, with a cached BTC tape as the exchange-price feed. This is
the missing bridge between research backtests and production paper/live runs.

New command:

```bash
polymomentum-engine live-replay \
  --start 2026-04-25T10:00:00Z \
  --end 2026-04-25T10:00:00Z \
  --cache-dir /Users/ttoomm/Documents/PolyMomentum/data/pmxt_cache \
  --btc-csv /private/tmp/pm_btc_ticks_20260425.csv \
  --session-log-dir /private/tmp/polymomentum-live-replay/sessions \
  --max-contracts 1
```

Resource controls:

- Cache-only by default; missing PMXT hours fail unless `--allow-download` is
  explicit.
- Cached Gamma metadata is used by default; the expensive archive condition-id
  scan plus network Gamma fill is only enabled by `--allow-gamma-fetch`.
- `--max-contracts` caps the replay universe for short diagnostics, so a live
  replay smoke does not contend with local or VPS peer bot workloads.
- The command now fails fast if the BTC CSV range does not overlap the replay
  hour. The failed guard test caught the invalid pairing of
  `2026-04-23T00:00:00Z` with `/private/tmp/pm_btc_ticks_20260425.csv`.

Latest capped replay:

- Session: `/private/tmp/polymomentum-live-replay/sessions/session_20260502_081034.jsonl`
- Hour: `2026-04-25T10:00:00Z`.
- Contracts: 1.
- PMXT events loaded/processed: 219,023 / 219,023.
- Orders placed/filled/rejected: 1/1/0.
- Diagnostics: `ok=true`, 0 malformed, 0 system errors, 0 fatal errors.
- Replay validation: `total=119748 mismatches=0 (0.00%)`.

Current limitation: this proves deterministic decision/diagnostics/fill-model
parity on cached public data. It cannot prove authenticated live exchange
behavior, user WebSocket reconciliation, allowance failures, or live CLOB
reject semantics; those still require paper/live canary loops with credentials.

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

Current state: C+.

Paper runtime, V2 order-shape readiness, and cached live replay parity improved,
but production capital still needs authenticated user-channel reconciliation
evidence, funded canary evidence, promotion artifact evidence, and broader
archive replay coverage.

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

## A+ readiness iteration - 2026-05-02 08:40 UTC

Target A+ gates:

1. Fail closed before live unless venue/compliance, CLOB V2 signing, valid
   credentials, alerting, and live reconciliation are explicitly ready.
2. Consume authenticated CLOB user-channel `order` and `trade` events and
   reconcile them into the local order state machine by venue order ID.
3. Keep local production loops resource-bounded: cache-only metadata by default,
   no archive-wide scans unless explicitly requested, and short `--max-contracts`
   diagnostics for replay/backtest.
4. Prove the current code with tests, a short real paper run, cached live replay,
   and bounded archive harness.

Implemented in this iteration:

- Added authenticated CLOB user-channel parser/feed for the documented
  `wss://ws-subscriptions-clob.polymarket.com/ws/user` subscription shape.
- Added order-manager reconciliation helpers keyed by venue order ID.
- Live runtime now subscribes to active condition IDs in live mode and reconciles
  user-channel order/trade events into `OrderManager` plus session JSONL
  `order.reconciled` / `order.filled` / `order.rejected` evidence.
- Live preflight now fails unless
  `POLYMOMENTUM_LIVE_RECONCILIATION_READY=1` is set.
- Live preflight now rejects malformed `PRIVATE_KEY` values instead of treating
  any non-empty string as credential-ready.
- `harness` now supports `--max-contracts` and uses cached Gamma metadata by
  default; archive-wide condition-id scans/Gamma fetches require
  `--allow-gamma-fetch`.

Official references checked for this iteration:

- https://docs.polymarket.com/market-data/websocket/user-channel
- https://docs.polymarket.com/api-reference/wss/user
- https://docs.polymarket.com/api-reference/authentication
- https://docs.polymarket.com/api-reference/trade/get-single-order-by-id
- https://docs.polymarket.com/api-reference/trade/get-trades

Fresh verification:

- Unit/integration tests: `cargo test` passed, 125 lib tests + 125 binary tests.
- Paper run: `/private/tmp/polymomentum-a-plus/logs/sessions/session_20260502_083808.jsonl`
  - Duration: about 2 minutes.
  - Feeds connected: Binance, Bybit, OKX, Binance alt, Bybit alt.
  - Active candle contracts scanned: 119.
  - Diagnostics: `ok=true`, 921 events, 453 signal evaluations, 0 malformed,
    0 system/fatal errors.
  - Replay validation: `total=453 mismatches=0 (0.00%)`.
- Cached live replay:
  `/private/tmp/polymomentum-live-replay/sessions/session_20260502_085542.jsonl`
  - 1 BTC candle contract, 219,023 PMXT events processed.
  - Orders placed/filled/rejected: 1/1/0.
  - Diagnostics: `ok=true`, 0 malformed, 0 system/fatal errors.
  - Replay validation: `total=119748 mismatches=0 (0.00%)`.
- Bounded archive harness:
  `/private/tmp/polymomentum-a-plus/harness_20260425T10_max1.json`
  - 1 BTC candle contract, 9 variants, 1 hour, `--threads 1`.
  - Best variant in this tiny smoke: `loose_maker`, 1 trade, +$6.72.
  - This validates harness plumbing only; one trade is not promotion evidence.
- Live-shaped preflight:
  - Fails closed when `POLYMOMENTUM_LIVE_RECONCILIATION_READY` is absent.
  - Passes structurally with valid hex key, explicit international venue,
    compliance acknowledgement, CLOB V2 flag, reconciliation-ready flag, and
    alerting. This used test credentials only and did not place orders.

Current grade after this iteration: B.

Why not A+ yet:

- No real authenticated user-channel session has been observed with production
  credentials.
- No wallet run has proven pUSD balance, both V2 pUSD allowances, and POL gas
  on the Dublin VPS.
- No 24-48h VPS paper soak has produced daily diagnostics under shared-resource
  conditions.
- No promotion artifact has enough out-of-sample trades and positive net PnL.
- No funded $1 live canary has been reconciled order by order.

A+ promotion checklist:

1. Run `wallet` on the VPS and require `live_ready yes`.
2. Run live-shaped preflight on VPS with real credentials, real alerting, and
   `POLYMOMENTUM_LIVE_RECONCILIATION_READY=1`.
3. Run 24-48h VPS paper mode with diagnostics every 6h and no peer bot resource
   degradation.
4. Run bounded archive harness over many cached/distilled hours, then expand
   only on a dev box; promote only with sufficient OOS trades and positive net
   fees/slippage-adjusted PnL.
5. Run a supervised $1 live canary and require CLOB user-channel/REST evidence
   for every accepted, filled, canceled, or rejected order.

## A+ readiness iteration - 2026-05-02 12:20 UTC

Target A+ gates tightened:

1. Live startup must verify wallet readiness from chain state, not merely
   private-key shape.
2. Wallet diagnostics must distinguish "wallet empty/not approved" from "RPC
   read failed".
3. VPS work must not start a second PolyMomentum runtime while an existing
   orphan process is still running.

Implemented locally:

- Added `polymomentum-engine wallet --json` with `live_ready` and readiness
  detail.
- Live `preflight` and `live` startup now append a fail-closed `live_wallet`
  check in live mode.
- Wallet balance/allowance reads now propagate RPC failures instead of silently
  converting failed calls into zero balances.
- VPS setup template now exposes `CLOB_V2_READY` and
  `POLYMOMENTUM_LIVE_RECONCILIATION_READY` explicitly.

Fresh local verification:

- Unit/integration tests: `cargo test` passed, 126 lib tests + 126 binary tests.
- Live-shaped preflight with valid test key and unreachable RPC:
  `ok=false`, with `live_wallet` failing on `wallet fetch failed: fetch pUSD
  balance`.

VPS inspection before deployment:

- Host time checked at `2026-05-02T12:15:16Z`.
- `polymomentum-engine.service` is inactive, but an orphan
  `/opt/polymomentum/polymomentum-engine` process owned by `polymomentum` has
  been running for about 7 days.
- `adgts` is active; `polyarbitrage` service is inactive, while its collector
  process is active. No peer private directories were read.
- Because an orphan PolyMomentum runtime exists, do not enable/restart the
  systemd service until that process is intentionally drained or stopped.
