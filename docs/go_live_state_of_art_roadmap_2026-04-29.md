# State-of-the-art go-live roadmap - 2026-04-29

This is the implementation roadmap for taking PolyMomentum from the current
audited state to a controlled live launch. It is intentionally stricter than a
"make it run" checklist: real-money trading needs venue compliance, execution
truth, state reconciliation, risk persistence, and out-of-sample alpha evidence.

## Current research sources

Primary and near-primary sources used:

- Polymarket official CLOB order docs:
  <https://docs.polymarket.com/developers/CLOB/orders/create-order>
- Polymarket official order/cancel/query/trade docs:
  <https://docs.polymarket.com/developers/CLOB/orders/cancel-orders>
- Polymarket official WebSocket overview:
  <https://docs.polymarket.com/developers/CLOB/websocket/wss-overview>
- Polymarket official market channel docs:
  <https://docs.polymarket.com/developers/CLOB/websocket/market-channel>
- Polymarket official user channel docs:
  <https://docs.polymarket.com/market-data/websocket/user-channel>
- Polymarket official fees docs:
  <https://docs.polymarket.com/trading/fees>
- Polymarket official authentication docs:
  <https://docs.polymarket.com/api-reference/authentication>
- Polymarket official SDK docs:
  <https://docs.polymarket.com/developers/CLOB/clients>
- Polymarket official Rust client repository:
  <https://github.com/Polymarket/rs-clob-client>
- Polymarket official terms page:
  <https://polymarket.com/tos>
- Polymarket official geographic restrictions help page:
  <https://help.polymarket.com/en/articles/13364163-geographic-restrictions>
- CFTC 2022 Polymarket enforcement release:
  <https://www.cftc.gov/PressRoom/PressReleases/8478-22>
- SQLite WAL documentation:
  <https://www.sqlite.org/wal.html>
- SQLite atomic commit documentation:
  <https://sqlite.org/atomiccommit.html>
- systemd execution sandboxing:
  <https://www.freedesktop.org/software/systemd/man/systemd.exec.html>
- systemd resource controls:
  <https://www.freedesktop.org/software/systemd/man/systemd.resource-control.html>
- OpenTelemetry observability documentation:
  <https://opentelemetry.io/docs/>
- OpenTelemetry logging/correlation spec:
  <https://opentelemetry.io/docs/specs/otel/logs/>
- Prometheus alerting rules:
  <https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/>
- QuantConnect LEAN reality/live-order modeling docs:
  <https://www.quantconnect.com/docs/v2/writing-algorithms/live-trading/trading-and-orders>
  and
  <https://www.quantconnect.com/docs/v2/writing-algorithms/reality-modeling/slippage/supported-models>
- Bailey and Lopez de Prado, Deflated Sharpe Ratio:
  <https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551>
- Bailey et al., Backtest Overfitting:
  <https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2308659>
- scikit-learn probability calibration docs:
  <https://scikit-learn.org/stable/modules/calibration.html>

## Non-negotiable compliance gate

As of this audit date, Polymarket's official geographic restrictions page lists
the United States as a blocked country for the international Polymarket surface
and prohibits VPN-style bypassing of restrictions. Polymarket's official terms
page also says Polymarket US is a separate CFTC-regulated Designated Contract
Market, while the international platform is not CFTC-regulated.

Therefore, because the operator is in the United States, the live roadmap must
start with one of these compliant choices:

1. Use an available, properly onboarded Polymarket US route/API/account if and
   only if it supports the target market class and automated trading.
2. Do not go live from the United States on the international platform.
3. Get legal/compliance sign-off before any real-money order path is enabled.

No engineering milestone below overrides this gate.

## Target architecture

The state-of-the-art shape for this bot is:

```text
market data adapters
    -> normalized event bus
    -> strategy decision engine
    -> pre-trade risk gate
    -> order manager / execution adapter
    -> reconciliation engine
    -> append-only journal + SQLite state
    -> metrics/logs/alerts
```

Core rule: every external effect must have an immutable intent, an execution
event, and a reconciliation result. A strategy signal is not a trade. A CLOB
order response is not a fill. A matched trade is not the same as a final
confirmed settlement.

## Phase 0 - freeze target and venue

State-of-the-art implementation:

- Treat release identity as an immutable input: git SHA, binary checksum,
  service file checksum, config hash with secrets redacted.
- Separate `paper`, `dry_run`, and `live` by type/enum, not by loose strings
  scattered through runtime code.
- Add a startup compliance gate before networking or signing:
  `VENUE=polymarket_us|polymarket_international|paper_only`.
- Default to `paper_only`.
- Refuse live if `VENUE=polymarket_international` and the operator/account is
  not explicitly cleared for that venue.

Implementation tasks:

1. Add `ReleaseManifest` logged at startup: git SHA, build timestamp, binary
   path, mode, venue, config hash.
2. Add `VenueMode` config and fail-closed live validation.
3. Add `--print-release-manifest` CLI for deploy checks.
4. Add a preflight command:

   ```bash
   polymomentum-engine preflight --mode paper
   polymomentum-engine preflight --mode live
   ```

Acceptance:

- A live process cannot start without explicit venue, live flag, alerting, clean
  DB schema, and wallet/balance/allowance checks.
- The first journal event of every session contains the release manifest.

## Phase 1 - VPS deployment alignment

Current issue: the VPS has an active legacy `polymomentum-rust.service` running
an old IPC/latency binary, while the current repo expects
`polymomentum-engine live --mode paper`.

State-of-the-art implementation:

- Use one canonical systemd unit for the current binary.
- Make deploy idempotent and atomic: upload `*.new`, checksum, chmod, rename,
  daemon-reload, restart.
- Use systemd sandboxing:
  `NoNewPrivileges=true`, `ProtectSystem=strict`, explicit `ReadWritePaths`.
- Use cgroup controls:
  `CPUQuota`, `MemoryMax`, `TasksMax`.
- Run `systemd-analyze security polymomentum-engine.service` and track the
  result in deploy output.

Implementation tasks:

1. Install `polymomentum-engine.service` from `deploy/`.
2. Disable old `polymomentum-rust.service` after the new unit is healthy.
3. Keep failed `polymomentum-candle.service` disabled unless intentionally
   revived.
4. Enable `polymomentum-healthcheck.timer`.
5. Add `ExecStartPre=/opt/polymomentum/polymomentum-engine preflight --mode paper`.
6. Include `/opt/shared/pmxt_v2_distilled_candles` only as read/write if this
   service is actively writing distilled files; otherwise read-only or absent.

Acceptance commands:

```bash
systemctl is-active polymomentum-engine polymomentum-healthcheck.timer
systemctl show polymomentum-engine -p ActiveState -p SubState -p ExecMainStartTimestamp -p NRestarts
journalctl -u polymomentum-engine -n 100 --no-pager
```

Expected result:

- No legacy IPC-only engine process.
- Logs show `candle.start`, `candle.scan`, `candle.cycle`, and session JSONL
  creation from the current binary.

## Phase 2 - event journal and durable state

State-of-the-art implementation:

- Use an append-only event journal for every important state transition.
- Use SQLite WAL for current state and queryable history. SQLite WAL allows
  readers and a single writer to run concurrently, but it is still a single
  writer design; write paths must be short and bounded.
- Use idempotency keys:
  `session_id`, `decision_id`, `intent_id`, `order_id`, `trade_id`,
  `condition_id`.
- Store schema version and migration history in DB.
- Use atomic rename for file artifacts and never modify shared parquets.

Minimum tables:

```text
sessions
market_data_health
signal_evaluations
execution_intents
orders
order_events
trades
trade_events
positions
resolutions
oracle_checks
risk_snapshots
breaker_state
meta
```

Important semantics:

- `decision_trade=true`: strategy wanted to trade.
- `intent_created=true`: risk gate accepted and order manager received intent.
- `order_accepted=true`: CLOB accepted order.
- `matched_size > 0`: economic exposure exists.
- `confirmed_size > 0`: onchain/final status confirmed.
- `traded=true` should be removed or reserved only for confirmed economic
  exposure.

Acceptance:

- Replaying the journal reconstructs the same open positions, realized PnL,
  breaker state, and pending oracle checks as SQLite current-state tables.
- A crash after any step can restart without duplicating an order.

## Phase 3 - market data plane

State-of-the-art implementation:

- Use Polymarket market WebSocket for L2 book, price changes, last trades, and
  best bid/ask. The docs indicate `custom_feature_enabled: true` is needed for
  `best_bid_ask`, `new_market`, and `market_resolved`.
- Subscribe by asset IDs for market data and by condition IDs for the
  authenticated user channel.
- Use dynamic subscription updates instead of reconnect-only subscription
  changes.
- Track staleness and sequence health per source.
- Persist periodic book snapshots around trade decisions for replay.

Implementation tasks:

1. Update `polymarket_ws` to support documented `event_type` payloads and
   `custom_feature_enabled`.
2. Add `best_bid_ask` handling for cheap top-of-book updates.
3. Add market-resolution event handling for faster closeout/oracle checks.
4. Add source-health metrics:
   `last_frame_age_ms`, `book_age_ms`, `cross_exchange_spread`, `n_sources`,
   `ws_reconnects_total`.
5. Gate trading if market book age exceeds a strict threshold, e.g. 1-2 seconds
   for terminal entries.

Acceptance:

- For every execution intent, the journal stores the exact book top and age
  used by the strategy and order manager.
- Terminal entries are impossible on stale Polymarket books or stale BTC prices.

## Phase 4 - order management and execution

State-of-the-art implementation:

- Prefer the official Rust SDK (`polymarket-client-sdk`) unless a measured
  latency need justifies custom signing. Polymarket's docs describe official
  TypeScript, Python, and Rust clients, with the Rust repository providing typed
  request builders and authentication-state enforcement.
- Use the documented order types:
  FOK for all-or-nothing immediate execution, FAK for partial immediate
  execution, GTC/GTD for maker resting orders, post-only for maker-only orders.
- Fetch or use SDK-provided market-specific tick size and neg-risk.
- Do not hardcode fees. Polymarket docs say fees are market/category dependent
  and makers are not charged; fee handling docs have changed over time, so the
  runtime should either use the SDK default or query market/fee info.
- Use heartbeat for maker/open-order protection. Current docs state a missing
  heartbeat cancels open orders after the heartbeat window.

Order state machine:

```text
IntentCreated
  -> RiskAccepted | RiskRejected
  -> OrderSigned
  -> SubmitStarted
  -> SubmitRejected | AcceptedLive | AcceptedMatched | AcceptedDelayed | AcceptedUnmatched
  -> Open | PartiallyMatched | MatchedPendingSettlement
  -> Confirmed | Failed | CancelRequested | Cancelled | CancelFailed | Expired
```

Execution truth rules:

- `/order` response with `orderID` means accepted, not necessarily filled.
- Response `status=matched` plus trade IDs means matched exposure; still track
  trade status to `CONFIRMED` or `FAILED`.
- `getOrder` and `getTrades` are reconciliation sources.
- Authenticated user WebSocket is the preferred low-latency update path.
- Polling is the fallback when the user WebSocket disconnects.

Implementation tasks:

1. Add `OrderManager` module with the state machine above.
2. Add authenticated user WebSocket subscription by condition ID.
3. Add `getOrder`, `getOpenOrders`, and `getTrades` reconciliation calls.
4. Implement FOK first; defer maker/GTD until reconciliation is proven.
5. Implement maker later with:
   - post-only GTC/GTD,
   - heartbeat loop,
   - timeout,
   - cancel,
   - post-cancel reconciliation.
6. Pass `neg_risk` from market metadata.
7. Use dynamic tick size from market metadata or SDK.
8. Implement idempotent retry: same `intent_id`, never duplicate intent.

Acceptance:

- No order is recorded as filled unless user WSS or REST trade history confirms
  matched size.
- No exposure remains unknown after restart.
- Partial fills produce partial positions and partial residual cancel/reject
  handling.

## Phase 5 - paper mode as a shadow OMS

State-of-the-art implementation:

- Paper mode should run through the same `OrderManager` interface as live mode.
- The only difference is the execution adapter:
  `PaperExecutionAdapter` vs `ClobExecutionAdapter`.
- Paper fills must be generated from contemporaneous L2 book state, not from
  post-decision data.
- Paper should emit the same order and trade events as live, including
  accepted, matched, partial, cancelled, failed.

Implementation tasks:

1. Replace direct paper position insertion in the pipeline with `OrderManager`.
2. Generate paper order events using the same state machine.
3. Add replay parity test:

   ```bash
   polymomentum-engine validate-replay session.jsonl
   polymomentum-engine replay-orders session.jsonl --state-db /tmp/replay.db
   ```

4. Add "decision but no execution" counters.

Acceptance:

- 48 continuous hours current-Rust paper mode.
- 0 replay mismatches.
- 0 decisions marked as executed without an order/position event.
- 0 stale unresolved paper positions beyond configured oracle delay.

## Phase 6 - risk engine

State-of-the-art implementation:

- Risk is a pre-trade service, not just post-trade accounting.
- Every order intent must pass an atomic pre-trade check against current
  positions, open orders, daily loss, max exposure, instrument eligibility,
  source health, and kill switch.
- Breaker state must survive restart.
- Risk snapshots should be journaled so a postmortem can explain every allow or
  reject.

Pre-trade checks:

```text
venue_allowed
mode_allowed
market_active
asset_allowed
book_fresh
price_fresh
not_already_traded_condition
not_in_cooldown
position_size >= venue_min
position_size <= max_per_market
total_exposure + order <= max_total_exposure
open_order_reserve + order <= available_balance
daily_loss <= limit
breaker_not_tripped
kill_switch_absent
alerts_available_if_live
```

Implementation tasks:

1. Enforce `max_total_exposure_usd`; it is currently config-only.
2. Enforce cooldowns or remove the unused setting.
3. Persist breaker wins/losses/PnL/peak state.
4. Reconstruct breaker state from DB on startup.
5. Add balance/allowance/open-order reserve checks.
6. Add manual `risk status` CLI.

Acceptance:

- Unit tests cover every reject reason.
- Integration tests prove restart preserves breaker and open exposure.
- Live mode refuses to start if alerts are required but unavailable.

## Phase 7 - alpha validation and calibration

State-of-the-art implementation:

- Use event-driven backtesting with the same decision and fill interfaces as
  live, point-in-time data, and realistic transaction costs.
- Treat parameter sweeps as multiple hypothesis tests. The Bailey/Lopez de
  Prado work on deflated Sharpe and backtest overfitting is directly relevant:
  do not report the best sweep cell without accounting for how many cells were
  tried.
- Calibrate the strategy confidence before using it as an EV probability.
  scikit-learn's calibration docs describe the desired property: predictions
  near 0.8 should resolve positive about 80% of the time. Sigmoid/Platt
  calibration is appropriate for small samples; isotonic needs more data and can
  overfit when samples are too few.

Validation protocol:

1. Freeze one candidate strategy before the final test.
2. Split data by time:
   - development/train,
   - calibration,
   - validation,
   - final holdout.
3. Report:
   - net PnL,
   - win rate,
   - average PnL/trade,
   - max drawdown,
   - fees/slippage,
   - Brier score,
   - log loss,
   - calibration curve,
   - by-zone results,
   - by-price-bucket results,
   - by-vol-regime results,
   - by-day contribution.
4. Bootstrap confidence intervals over trade days, not only trades.
5. Penalize or reject if one day/window explains most profit.

Implementation tasks:

1. Add a calibration report command:

   ```bash
   polymomentum-engine calibration-report --sessions ... --out report.json
   ```

2. Add `--holdout-start` to harness/sweep commands.
3. Add trade-day bootstrap summary.
4. Add deflated/penalized Sharpe or at least report number of variants tried
   and holdout performance separately.
5. Make BTC-only terminal strategy the first candidate; do not widen assets
   until BTC passes.

Acceptance:

- At least 100 out-of-sample resolved candidate trades before live.
- Positive net PnL after fees/slippage in final holdout.
- Calibration curve is not obviously overconfident in the traded bucket.
- Candidate remains acceptable under stricter slippage/latency stress.

## Phase 8 - observability and alerting

State-of-the-art implementation:

- Use structured JSON logs for the durable audit trail.
- Export metrics for operational alerting.
- Use OpenTelemetry-compatible fields where practical so traces/logs/metrics
  can correlate by `session_id`, `decision_id`, `intent_id`, and `order_id`.
- Use Prometheus-style alert rules with `for` and `keep_firing_for` to avoid
  alert flapping.

Minimum metrics:

```text
polymomentum_service_up
polymomentum_cycle_latency_ms
polymomentum_market_ws_age_ms
polymomentum_user_ws_age_ms
polymomentum_price_source_count
polymomentum_cross_exchange_spread
polymomentum_decisions_total{result=...}
polymomentum_order_intents_total{result=...}
polymomentum_orders_total{state=...}
polymomentum_trades_total{status=...}
polymomentum_open_exposure_usd
polymomentum_realized_pnl_usd
polymomentum_breaker_tripped
polymomentum_replay_mismatches_total
```

Minimum alerts:

- Service down for 60 seconds.
- Market WS stale for 5 seconds during an active window.
- User WS stale while live and there are open orders.
- Any order in unknown state for more than 10 seconds.
- Any live position unresolved after market close plus grace period.
- Breaker tripped.
- Replay mismatch.
- Alert sink unavailable in live mode.

Implementation tasks:

1. Add `/metrics` endpoint or textfile exporter.
2. Add a daily summary artifact in `logs/reports/YYYY-MM-DD.json`.
3. Extend `healthcheck.sh` to validate current Rust service, not legacy units.
4. Add runbook links to alert annotations.

Acceptance:

- A synthetic test can force every critical alert.
- Live preflight fails if alerting is unavailable.

## Phase 9 - shared cache and historical data ops

State-of-the-art implementation:

- Heavy work runs on the dev box.
- The VPS hosts live runtime and light one-off distill only.
- Shared artifacts use atomic tmp-plus-rename writes.
- The reader fallback chain is:
  shared distilled cache -> private sidecar -> parquet RowFilter.

Implementation tasks:

1. Create `/opt/shared/pmxt_v2_distilled_candles` with `pmxt-data` group.
2. Backfill v1 distilled files from dev box.
3. Add a cache verifier:

   ```bash
   polymomentum-engine cache-verify --start ... --end ...
   ```

4. Add a metadata cache export so harness does not spend minutes hydrating Gamma
   condition IDs during every audit.

Acceptance:

- Harness for a six-hour recent slice starts from cached metadata and reaches
  replay quickly.
- Distilled-cache corrupt/missing cases fall back cleanly.
- No concurrent parquet scans for the same hour on the VPS.

## Phase 10 - live ramp

State-of-the-art implementation:

- First live launch is an experiment with a kill switch, capped exposure, and
  manual supervision.
- Use the simplest execution path first: BTC-only, FOK or FAK taker with strict
  worst-price protection.
- Maker mode only after order reconciliation and cancel/heartbeat are proven.
- Run one controlled window, then stop and reconcile.

Initial live config:

```env
VENUE=polymarket_us
MAX_POSITION_PER_MARKET_USD=1
MAX_TOTAL_EXPOSURE_USD=1
CANDLE_POSITION_PCT=0.01
CANDLE_CROSS_ASSET_ENABLED=false
CANDLE_PREFER_MAKER=false
ALERT_REQUIRED=true
```

Live gate checklist:

1. Compliance/venue sign-off complete.
2. Current Rust paper service ran 48h clean.
3. Reconciliation implemented and tested.
4. Risk preflight passes.
5. Backtest holdout passes.
6. Alerts tested.
7. Kill switch tested.
8. Manual operator watching logs.

First live procedure:

1. Start live for one predefined BTC terminal window.
2. Place at most one order.
3. Reconcile order/trade status to terminal state.
4. Switch back to paper.
5. Write postmortem even if nothing bad happened.

Ramp rules:

- 20 clean live orders before raising per-market cap above $1.
- 50 clean live orders before maker mode.
- 100 clean live orders before any non-BTC asset.
- Any unknown order state, unreconciled exposure, or compliance uncertainty
  returns the bot to paper-only.

## Implementation order for the next coding sessions

1. Add preflight/release manifest and venue gate.
2. Replace VPS service with current paper runtime.
3. Fix decision/execution logging semantics.
4. Add order manager state machine and paper adapter.
5. Add user WebSocket and REST reconciliation.
6. Enforce risk settings and persisted breaker state.
7. Add observability metrics and alert tests.
8. Run 48h current Rust paper.
9. Add calibration/holdout reports.
10. Run out-of-sample harness.
11. Only then consider a one-window live trial.

