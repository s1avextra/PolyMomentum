# Polymarket CLI + poly_data strategy-platform research - 2026-05-01

## Scope

This note maps two external repositories into PolyMomentum's backtest, paper,
and live trading architecture:

- Polymarket CLI: <https://github.com/Polymarket/polymarket-cli>
- poly_data: <https://github.com/warproxxx/poly_data>

The intended result is a system that discovers and validates strategies in
backtest, promotes them into paper mode without semantic drift, and then moves
to live trading only after diagnostics prove that execution behavior is the
same apart from the venue side effects.

Assumptions:

- We keep PolyMomentum as the primary Rust engine.
- We do not copy GPL-covered `poly_data` code. GitHub currently shows the repo
  as GPL-3.0 even though `pyproject.toml` says MIT, so treat GPL as controlling
  and reimplement only clean-room concepts.
- No CPU-heavy research job should run on the multibot VPS. Backtests, Goldsky
  ingestion, and parameter searches run on the dev box unless bounded to a
  short, one-off diagnostic.

## Sources reviewed

- `polymarket-cli` README and repo structure:
  <https://github.com/Polymarket/polymarket-cli>
- `polymarket-cli` command modules:
  <https://github.com/Polymarket/polymarket-cli/tree/main/src/commands>
- `polymarket-cli` auth/config modules:
  <https://github.com/Polymarket/polymarket-cli/blob/main/src/auth.rs>,
  <https://github.com/Polymarket/polymarket-cli/blob/main/src/config.rs>
- `poly_data` README and project structure:
  <https://github.com/warproxxx/poly_data>
- `poly_data` orchestrator:
  <https://github.com/warproxxx/poly_data/blob/main/update_all.py>
- `poly_data` processing module:
  <https://github.com/warproxxx/poly_data/blob/main/update_utils/process_live.py>
- Existing PolyMomentum roadmap:
  `docs/go_live_state_of_art_roadmap_2026-04-29.md`
- Existing PolyMomentum production-loop report:
  `docs/production_loop_2026-04-30.md`

## Main takeaway

`polymarket-cli` is most useful as an operational pattern: clear command groups,
read-only market/CLOB diagnostics without wallet risk, wallet/approval checks,
JSON/table output, and explicit auth handling. It should shape our CLI and
preflight surface, not replace the hot trading loop immediately.

`poly_data` is most useful as a data-platform pattern: separate market metadata,
raw order-filled events, and processed trades; make ingestion resumable; map
token IDs to markets once; and keep processed research data separate from raw
evidence. It should shape our data catalog, trade-tape calibration, and research
workflow, not replace the PMXT v2 L2 replay harness.

## What to import from polymarket-cli

Principles to implement:

1. Command groups should mirror operational domains:
   `markets`, `events`, `clob`, `wallet`, `approve`, `ctf`, `data`, `config`,
   and `setup`.
2. Most diagnostics should be read-only and runnable without a wallet:
   order book, mid, spread, tick size, fee rate, negative-risk status, market
   metadata, geoblock/status, and time.
3. Wallet-dependent commands should be isolated:
   balances, open orders, trade history, approvals, CLOB API keys, and order
   cancellation.
4. Every diagnostic command should support machine-readable JSON and concise
   human-readable table output.
5. Signing, API credentials, and config should be explicit and redacted in
   output. Strategy runtime should not be the first place credentials are
   discovered to be broken.
6. Treat the upstream CLI as experimental. Use it for taxonomy and compatibility
   tests, then wrap any official SDK/client usage behind our own adapter.

Where to implement in PolyMomentum:

| Need | Target location | Notes |
| --- | --- | --- |
| JSON/table output | `rust_engine/src/main.rs`, new `cli/output.rs` | Add `--output json|table` once, not per command. |
| Config diagnostics | `rust_engine/src/config.rs`, new `Command::Config` | Add `show`, `doctor`, and redacted hashes; keep env-file compatibility. |
| Auth diagnostics | new `rust_engine/src/auth_diag.rs` | Verify signer address, API-key presence, CLOB auth headers, and clock skew without placing orders. |
| CLOB diagnostics | extend `rust_engine/src/clob.rs`, new `Command::Clob` | Read-only book/mid/spread/tick/fee/open-orders/trades endpoints. |
| Wallet/approval checks | `rust_engine/src/data/wallet.rs`, new `Command::Approve` | Move live preflight balance/allowance checks into reusable commands. |
| Live adapter boundary | new `rust_engine/src/execution/adapters/live.rs` | Keep external client choice hidden behind `ExecutionAdapter`. |

Do not implement upstream-style interactive shell until the core promotion path
is stable. It is convenient but not necessary for safe launch.

## What to import from poly_data

Principles to implement:

1. Separate raw evidence from processed research data.
2. Make every data pipeline resumable from an explicit cursor:
   market offset, timestamp, block number, transaction hash, or PMXT hour.
3. Persist a canonical market/token map:
   `condition_id`, yes/no token IDs, slug, question, close time, negative-risk
   flag, liquidity, tick size, and fee rate.
4. Process raw order-filled events into a trade tape:
   identify USDC vs outcome token, infer buy/sell direction, normalize amount
   units, compute price, and preserve transaction hash.
5. Discover missing markets during processing, but make the discovery visible
   through a manifest instead of silently mutating backtest inputs.
6. Use processed trades to calibrate execution assumptions, not as a replacement
   for order-book replay.

Where to implement in PolyMomentum:

| Need | Target location | Notes |
| --- | --- | --- |
| Market catalog | new `rust_engine/src/data/catalog.rs` | SQLite or JSONL catalog with token-to-market lookup and ingest manifest. |
| Raw trade-event store | new `rust_engine/src/data/trades_raw.rs` | Dev-box ingestion from Goldsky or other sources; never in peer-private dirs. |
| Processed trade tape | new `rust_engine/src/data/trades_processed.rs` | Clean-room USDC/outcome mapping and direction inference. |
| Data manifests | new `rust_engine/src/data/manifest.rs` | Track source, cursor, checksum, row count, and window coverage. |
| Fill calibration | extend `rust_engine/src/backtest/fill_model.rs` | Estimate maker fill probability, taker slippage, and fee realization from processed trades plus paper/live diagnostics. |
| Research CLI | new `Command::Research` | `update-markets`, `process-trades`, `catalog-check`, `calibrate-fill`, all dev-box first. |

`poly_data` trade history is fill-level/on-chain evidence. It cannot simulate
what our order would have seen in the book at decision time. The PMXT v2 L2
harness remains the source of truth for backtest execution, while the processed
trade tape calibrates and audits that harness.

## Target architecture

The missing abstraction is a stage-neutral order and strategy core:

```text
DataCatalog + MarketDataSource
    -> FeatureStore
    -> StrategySpec
    -> Signal
    -> OrderIntent
    -> RiskDecision
    -> OrderManager
    -> ExecutionAdapter
    -> Reconciler
    -> EventJournal + RunManifest + ExperimentReport
```

The same `StrategySpec` and `OrderIntent` types must flow through backtest,
paper, and live. The only part that changes by stage is the `ExecutionAdapter`:

| Stage | Market data source | Execution adapter | Required output |
| --- | --- | --- | --- |
| Backtest | PMXT v2 L2 replay + BTC tape + catalog snapshot | deterministic fill model | `ExperimentReport` plus replay-grade order events |
| Paper | live WSS/books + live BTC tape | paper adapter using the same order state machine | session JSONL identical to live schema |
| Live | live WSS/books + user channel + REST reconciliation | CLOB adapter | session JSONL plus reconciled venue order/trade IDs |

## Current local fit

Already strong:

- `rust_engine/src/backtest/harness.rs` runs variants over PMXT v2 hours.
- `rust_engine/src/backtest/l2_replay.rs` is event-driven and avoids
  same-event lookahead when applying latency.
- `rust_engine/src/backtest/fill_model.rs` already separates taker, maker, and
  perfect-fill assumptions.
- `rust_engine/src/sweep` replays captured paper sessions for fast parameter
  checks.
- `rust_engine/src/release.rs` and `preflight` already enforce paper/live
  venue gates and peer-private path checks.

Still missing:

- A canonical market/token catalog with explicit data completeness manifests.
- A processed trade tape used for fill/slippage calibration.
- A common order state machine used by backtest, paper, and live.
- User-channel/REST reconciliation for live orders and fills.
- CLI diagnostics equivalent to the read-only parts of `polymarket-cli`.
- A promotion artifact that locks code SHA, strategy parameters, data manifest,
  fill model, and risk limits together.

## Algorithmic structure

Keep the existing candle momentum math as the first strategy implementation,
but package it behind a stage-neutral interface:

```rust
struct StrategySpec {
    name: String,
    version: String,
    params_hash: String,
    risk_profile: String,
}

struct Signal {
    market_id: String,
    token_id: String,
    direction: String,
    fair_price: f64,
    edge: f64,
    confidence: f64,
    diagnostics: serde_json::Value,
}

struct OrderIntent {
    intent_id: String,
    strategy: StrategySpec,
    token_id: String,
    side: String,
    order_type: String,
    limit_price: Option<f64>,
    size: f64,
    reason: String,
}
```

For backtest quality, every report should include:

- sample count and market count;
- in-sample, out-of-sample, and walk-forward windows;
- win rate, net PnL, PnL/trade, drawdown, exposure time, turnover, fees;
- calibration diagnostics such as Brier score or reliability bins if the
  strategy emits probabilities;
- fill model assumptions, maker fill probability, slippage, latency, and fee
  schedule;
- a null strategy comparison and a perfect-fill upper bound.

The promotion question should be: "Does this strategy survive after replacing
optimistic assumptions with observed paper/live execution?" If not, it stays in
research.

## Implementation roadmap

### Phase 1 - Stage-neutral strategy and order types

Goal: make backtest, paper, and live consume the same strategy outputs.

Implementation:

- Add `rust_engine/src/strategy/spec.rs` with `StrategySpec`, `Signal`, and
  `OrderIntent`.
- Wrap current `decide_candle_trade` output into `Signal` and `OrderIntent`.
- Give every intent a deterministic ID in backtest and a UUID-like ID in
  paper/live.
- Add serialization tests so session JSONL, backtest reports, and live logs
  use the same field names.

Verify:

- `cargo test strategy::`
- existing `validate-replay` continues to pass on recent paper sessions.

### Phase 2 - Data catalog and manifests

Goal: know exactly what data a backtest used and whether it was complete.

Implementation:

- Add `data/catalog.rs` for canonical market/token lookup.
- Add `data/manifest.rs` with source, cursor, row count, checksum, start/end,
  and completeness status.
- Extend `scan`/harness loading to write a catalog snapshot for the tested
  window.
- Add `research catalog-check --start ... --end ...`.

Verify:

- A harness run refuses to promote if any token lacks catalog metadata.
- Manifests are deterministic for the same data window.
- VPS rule remains intact: shared PMXT/distilled files are read through existing
  cache rules; no deletion of peer-owned parquets.

### Phase 3 - Clean-room processed trade tape

Goal: use `poly_data`'s data idea for calibration without importing its code.

Implementation:

- Add a raw trade-event store for order-filled events.
- Add a processed trade model:
  `timestamp`, `condition_id`, `token_id`, `side`, `price`, `usd_amount`,
  `token_amount`, `maker`, `taker`, `tx_hash`.
- Add idempotent processing from raw cursor to processed cursor.
- Record missing-market discoveries in the manifest.

Verify:

- Reprocessing the same raw input produces byte-identical processed output.
- Unit tests cover USDC-paid buy, USDC-received sell, missing market, duplicate
  transaction, and negative-risk metadata.
- Heavy collection runs only on the dev box or under an explicit low-resource
  diagnostic mode.

### Phase 4 - Experiment registry

Goal: make strategy discovery reproducible instead of ad hoc tables.

Implementation:

- Add `backtest/experiment.rs` with `ExperimentRun`, `ExperimentReport`, and
  `PromotionCandidate`.
- Extend `harness` and `harness-sweep` to emit JSON reports, not just tables.
- Store code SHA, config hash, data manifest hash, strategy params, fill model,
  and risk config.
- Add `experiment report` and `experiment compare`.

Verify:

- Re-running the same manifest and strategy produces the same report.
- Report rejects promotion when trades are below minimum sample size or when
  out-of-sample performance fails.

### Phase 5 - Common order manager

Goal: remove paper/live divergence.

Implementation:

- Add `execution/order_manager.rs` with states:
  `IntentCreated`, `RiskAccepted`, `Submitted`, `Acked`, `PartiallyFilled`,
  `Filled`, `Canceled`, `Rejected`, `Expired`, `Settled`.
- Move direct paper position insertion in `live/pipeline.rs` behind a
  `PaperExecutionAdapter`.
- Make backtest emit the same order lifecycle schema with `mode=backtest`.
- Make live treat CLOB order acceptance as `Acked`, never as `Filled`.

Verify:

- Paper and live session schemas diff equal except for venue-only fields.
- `validate-replay` verifies decisions and order-intent creation.
- Existing no-fill paper sessions remain valid.

### Phase 6 - CLOB, wallet, and approval diagnostics

Goal: make operational readiness visible before trading.

Implementation:

- Add `clob ok`, `clob book`, `clob midpoint`, `clob spread`, `clob tick-size`,
  `clob fee-rate`, `clob orders`, and `clob trades`.
- Add `wallet show`, `wallet balances`, and `approve check`.
- Add `config show --redacted` and `config doctor`.
- Add `--output json|table`.

Verify:

- All read-only commands run without private key.
- Wallet commands never print secrets.
- Live preflight calls these diagnostics internally and fails closed.

### Phase 7 - Live reconciliation

Goal: prove venue truth.

Implementation:

- Add user-channel listener for authenticated order/trade events.
- Add REST reconciliation fallback for open orders and trade history.
- Reconcile every `Acked` order to terminal state or alert.
- Persist venue order IDs and trade IDs in the journal.

Verify:

- Paper diagnostics and live diagnostics have identical order lifecycle counts
  on shadow runs except for venue IDs.
- Any order stuck in `Acked` beyond timeout triggers alert and cancel logic.

### Phase 8 - Promotion and staged launch

Goal: one controlled path from research to paper to live.

Implementation:

- Add `experiment promote <report.json>` that creates a locked promotion file.
- Add `paper --promotion <file>` so paper runs exactly the backtested params.
- Add live preflight validation against the promotion file.
- Start with micro-size live only after compliance gate, paper/live parity, and
  reconciliation are green.

Verify:

- A promotion file cannot be edited silently; hash mismatch fails startup.
- Paper must run for the defined observation window with zero schema drift,
  zero replay mismatches, no stale positions, and no reconciliation gaps.
- Live starts only with explicit venue clearance and the existing
  `--i-understand-live` gate.

## Smooth transition gates

Backtest to paper:

- Data manifest complete for the tested window.
- Strategy report passes out-of-sample and null-comparison gates.
- Fill model calibrated from processed trade tape and recent paper diagnostics.
- Promotion file generated and committed.

Paper to live:

- Paper emits the same order lifecycle as live would.
- Replay validator reports zero mismatches.
- Wallet, allowance, CLOB status, market metadata, tick size, fee rate, and
  geoblock/status diagnostics pass.
- Alerting works and kill switch path is writable by PolyMomentum only.
- VPS resource plan is confirmed: no release build overlap, no CPU-heavy sweep,
  no peer-private path access, and no shared parquet deletion.

Live ramp:

- First run is micro-size with strict exposure caps.
- Every live order must reconcile through user stream or REST.
- Ramp only after a written report shows live fills match paper assumptions
  within predefined slippage, latency, and fee tolerances.

## VPS and multibot safety

The new data and research layers must respect the shared-host rules:

- Do not read `/opt/polyarbitrage/*` or other peer-private paths.
- Do not delete shared PMXT parquets unless this process downloaded them in the
  same one-shot command and the caller opted into deletion.
- Use `/opt/shared/pmxt_v2_distilled_candles/` only through the existing v1
  contract and atomic rename semantics.
- Put any new peer-visible convention into `/opt/shared/cross_bot_notes/` and
  mirror it under `docs/`.
- Run full sweeps and Goldsky-style collection on the dev box, not the VPS.
- If a backtest must run on the VPS for a diagnostic, force `--threads 1`,
  small windows, and no concurrent parquet scan of the same hour.

## Immediate next coding sequence

1. Add `StrategySpec`, `Signal`, and `OrderIntent`.
2. Add `ExperimentReport` JSON output to the existing harness.
3. Add `DataManifest` and catalog snapshot support for harness runs.
4. Add `OrderManager` and paper adapter, then route paper mode through it.
5. Add read-only `clob` and `wallet` diagnostics with JSON/table output.
6. Add clean-room processed trade tape only after the catalog and reports exist.
7. Add live user-channel/REST reconciliation.

This sequence keeps the hot path stable while making each stage more identical.
It also avoids spending VPS resources on research infrastructure before the
promotion and order-state contracts are in place.
