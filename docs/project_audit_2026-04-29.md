# PolyMomentum project audit - 2026-04-29

## Scope and assumptions

This audit covers the local repo at `/Users/ttoomm/Documents/PolyMomentum`, local data/log artifacts, and VPS-visible PolyMomentum/shared-cache state. I did not inspect peer-private paths such as `/opt/polyarbitrage` or `/etc/polyarbitrage`; VPS reads stayed inside `/opt/polymomentum` and `/opt/shared/{pmxt_v2_cache,pmxt_v2_distilled_candles,cross_bot_notes}`.

The requested go-live standard is interpreted as real-money Polymarket candle trading with the current Rust repo, not the legacy Python/IPC runtime still present on the VPS.

## Executive grade

Overall grade: C-.

The engineering base is meaningfully better than the alpha/readiness evidence. The current Rust code has a good causal backtest spine, shared pure decision function, parquet predicate filtering, and a strong test suite after the doctest fix in this audit. But the project should not go live with capital now. The deployment on the VPS is not running the current repo's `live --mode paper` runtime, the paper/live evidence is internally inconsistent, live execution assumes fills too eagerly, and the strategy edge is not proven out-of-sample.

Subgrades:

- Code/test structure: B.
- Backtest and data plumbing: B-.
- Strategy/math confidence: C-.
- Paper/live evidence: D+.
- VPS deployment readiness: D.
- Real-money execution readiness: D.
- Operational safety posture: C+.

## What I changed

- Fixed the Rust doctest failure in `rust_engine/src/execution/fees.rs` by marking the fee formula as text and replacing Unicode math symbols with ASCII operators.
- Verified `cargo test` passes after the fix: 88 lib tests, 88 bin tests, and doc-tests all pass.

## Data inventory

Local machine:

- `data/`: 30G.
- `logs/`: 8.9G.
- PMXT local archive: 72 hourly parquet files in `data/pmxt_cache`, covering 2026-04-23 through 2026-04-25.
- Local sidecar cache: 8 `*.events.bin.gz` files for 2026-04-23T00 through 2026-04-23T07.
- Local Gamma metadata cache: `data/pmxt_cache/gamma_market_cache.json`, about 60M.
- Local session summaries: 3 non-empty summary JSONs from 2026-04-05 and 2026-04-07.
- Local non-empty session JSONLs: 6.
- Local historical harness evidence is mostly earlier Python-era or transition-era, not current Rust live evidence.

VPS:

- `/opt/shared/pmxt_v2_cache`: 30G, 74 parquet files.
- `/opt/shared/pmxt_v2_distilled_candles`: missing at audit time.
- `/opt/shared/cross_bot_notes`: populated with v1 protocol notes from both bots.
- `/opt/polymomentum/logs`: 189M.
- `/opt/polymomentum/data`: 97M.
- VPS paper/session JSONLs from 2026-04-25 and 2026-04-26 total roughly 197M.
- VPS live BTC tick CSVs exist for 2026-04-25 and 2026-04-26.

## Local evidence

The local `cargo test` run initially failed only because a doc comment formula in `execution/fees.rs` was treated as Rust code. After the fix, all tests passed. That is a good sign for code hygiene, especially because the suite covers:

- Black-Scholes binary fair value and normal CDF known values.
- Momentum signal construction.
- Decision gates and terminal trade behavior.
- Paper fill slippage/fees.
- L2 replay lookahead guard.
- BTC history causality.
- Resolver PnL.
- Distilled-cache read/write behavior.
- Risk persistence.
- Signing helpers.

The local historical paper summaries are weak as alpha evidence:

- 2026-04-05 summary: 15.7 minutes, 0 orders, 1 detected signal, 4,620 skips.
- 2026-04-07 summary: 121.5 minutes, 0 orders, 3 detected signals, 49,578 skips, 4 Gamma 429 errors.
- 2026-04-07 later summary: 15.5 minutes, 0 orders, 12,081 skips.

The older local SQLite candle DB has only 3 paper trade rows with `pnl=0` in the trade table. The old session log reports 3 wins and +$2.98 realized PnL, but that is pre-Rust-port evidence and too small to matter.

The local historical harness result `logs/harness_apr12_multi.log` is mixed:

- `baseline`: 42 trades, 57.1% win rate, +$0.32 total PnL.
- `terminal` slice inside baseline: 13 trades, +$7.66 PnL.
- `terminal_only`: 27 trades, 44.4% win rate, -$3.02 PnL.
- `ewma_terminal_only`: 46 trades, 63.0% win rate, -$3.04 PnL.
- `ewma_15min`: 177 trades, 76.3% win rate, -$11.17 PnL.

Interpretation: there may be a terminal-window phenomenon, but it is not stable enough to trade. The positive terminal slice is tiny and contradicted by terminal-only variants.

I attempted a fresh six-hour Rust harness run for 2026-04-25T10 through 2026-04-25T15 using local PMXT parquets and VPS BTC ticks. Without network permission it failed on missing Gamma metadata; with permission it ran too long without output and was stopped. That is itself useful: fresh harness validation is still operationally awkward and should be streamlined before go-live.

## VPS evidence

The VPS deployment does not match the current local repo.

Observed systemd state:

- `polymomentum-rust.service`: active/running since 2026-04-25 08:38 UTC.
- `polymomentum-candle.service`: loaded/failed.
- `polymomentum-healthcheck.timer`: disabled.
- `polymomentum-engine.service`: not installed.

The active service runs:

```text
/opt/polymomentum/polymomentum-engine
```

with no `live --mode paper` subcommand. The binary is an older latency/IPC engine that prints:

```text
PolyMomentum Latency Engine v0.2.0
Strategy: detect stale MM prices, accumulate edge, scale in
Listening for contracts on stdin + IPC (/tmp/polymomentum/engine.sock)...
```

Recent journal output is almost entirely Deribit IV lines. That is not equivalent to the current repo's Rust candle runtime, whose service file expects:

```text
/opt/polymomentum/polymomentum-engine live --mode paper
```

VPS session data is also inconsistent:

- `summary_20260425_110813.json`: 283.1 minutes, 0 orders placed/filled, 40 signal detections, 91,516 skips.
- `summary_20260426_045141.json`: 2.1 minutes, 0 orders, 1,413 skips.
- `session_20260426_050632.jsonl`: 302.6 minutes, 122,291 signal evaluations, 2,174 `traded=true` evaluations, but 0 order events and 0 resolutions.
- VPS SQLite `/opt/polymomentum/logs/candle/state.db`: 0 `trades`, 4 legacy `positions`, 0 `paper_positions`, 0 `oracle_pending`, 0 `meta`, 0 `state`.

Interpretation: paper trading has not been validated on the current Rust runtime. Some sessions log a decision as `traded=true` before execution actually creates a paper/live position, so a zero-bankroll or failed execution path can produce thousands of apparent "trades" without any positions or PnL.

## Algorithm and math audit

Current signal path:

1. Aggregate spot prices into a BTC/alt mid price.
2. Maintain a momentum detector per asset.
3. Compute window-open move, EWMA realized volatility, z-score, consistency, and reversion count.
4. Convert those heuristic components into `confidence`.
5. Compute binary fair value using Black-Scholes probability `P(S_T > open_price) = N(d2)`.
6. Buy the `up` or `down` outcome if confidence, z-score, price, EV, and edge gates pass.

Strengths:

- The decision function is pure and shared by live/backtest.
- The BTC history reader shifts kline timestamps to close time, which avoids a common lookahead bug.
- The L2 replay engine flushes pending orders before applying same-event book updates.
- The PMXT loader projects only needed columns and supports row-filter predicate pushdown.
- The fee model is covered by tests and now no longer breaks doctests.

Main math/alpha risks:

- `confidence` is not calibrated as a probability, but the EV gate treats it like one: `confidence >= market_price + buffer`.
- The Black-Scholes binary fair value is used near expiry where tiny spot moves and IV assumptions drive probabilities toward 0.01/0.99. That can create huge apparent edges, especially in terminal windows.
- Terminal-zone entries bypass the stale-edge cap. This is exactly where the fair value can become most extreme.
- Deribit BTC IV is used as the implied vol source. For non-BTC candles, the current live path still uses the same `ps.implied_vol`, which is not asset-specific.
- Cross-asset config exists, and ETH/SOL detectors are ticked, but the live decision call passes `cross_asset_boost = 0.0`; the feature is not actually wired.
- Scanner supports 11 assets, while live exchange feeds only cover BTC plus ETH/SOL. Unsupported assets silently lack prices.
- The strategy buys direction only; there is no explicit market-making, exit, hedge, or sell-side unwind logic before resolution.
- The strategy relies on Polymarket order book prices as if best ask is executable; real fill/cancel/queue position is not yet measured well enough.

Main backtest risks:

- `OneTickTaker` and probabilistic `Maker` are useful approximations, but they are still not live fill evidence.
- Maker fill probability is hard-coded/calibrated from prior notes, not continuously measured from this live deployment.
- A meaningful edge needs out-of-sample validation across more dates and regimes. Current positive evidence is too sparse.

## Execution and risk audit

Live execution is not ready for money in its current form.

Critical issues:

- `execute_trade` logs `traded=true` before it knows an order/paper position was created. If position sizing returns `< $1`, evaluations can look like trades without any execution.
- Live CLOB order placement treats a successful `/order` response as a fill. It records `order_filled` immediately, with `fill_pct=1.0`, instead of polling/reconciling actual fills, partials, cancels, and rejects.
- Maker mode has no implemented timeout/cancel path even though `CANDLE_MAKER_TIMEOUT_S` exists.
- `neg_risk` is passed as `false` into CLOB placement. The scanner carries neg-risk metadata, but live order signing does not use it.
- Breaker state starts from `BreakerState::default()` on process startup. The tripped flag is persisted, but wins/losses/drawdown state is not reconstructed from history.
- `max_total_exposure_usd`, cooldown settings, and several sizing knobs exist in settings but are not enforced by the current live path.
- Healthcheck timer is disabled on the VPS.

Positive controls:

- Live mode requires `--i-understand-live`.
- Kill-switch path exists.
- Systemd resource caps exist on the old VPS units.
- Paper positions and oracle pending are designed to persist in the current Rust DB schema.

## Data/cache protocol audit

Shared-cache rules in `AGENTS.md`/`CLAUDE.md` are sound and reflected in code:

- Parquet archives are treated as read-only.
- Sidecars use distinct names.
- Checkpoints use tmp-plus-rename atomic writes.
- Shared distilled v1 schema is documented.

Gaps:

- `/opt/shared/pmxt_v2_distilled_candles` is missing on the VPS, so the shared distilled-cache reader path is not actually being exercised there.
- The cache contains parquets but not the finalized distilled candles export. Both bots still pay parquet/sidecar costs until this is backfilled from a dev box.
- Fresh Rust harnesses still need easier offline metadata hydration. A six-hour local harness attempted during this audit stalled long enough to be stopped after Gamma metadata fetch/setup.

## Go-live decision

Do not go live with real capital now.

Minimum condition for flipping live should be: current Rust repo deployed to the VPS in paper mode, continuous paper evidence from that exact binary, clean replay parity, real order/fill reconciliation implemented and tested, and a statistically meaningful positive out-of-sample backtest after realistic fees/slippage.

## Sufficient go-live plan

Phase 0 - freeze and align deployment:

- Deploy the current repo's `polymomentum-engine` binary, not the legacy latency engine.
- Install the current `deploy/polymomentum-engine.service` as `polymomentum-engine.service`.
- Disable or archive old `polymomentum-rust.service` and failed `polymomentum-candle.service` once the new unit is healthy.
- Enable `polymomentum-healthcheck.timer`.
- Archive the old candle SQLite DB and start a clean current-schema paper DB, unless a migration is explicitly written.
- Verify service command is exactly `polymomentum-engine live --mode paper`.

Acceptance:

- `systemctl is-active polymomentum-engine polymomentum-healthcheck.timer` returns active.
- Journals show `candle.start`, `candle.scan`, `candle.cycle`, and session JSONL creation.
- No legacy IPC-only engine is running.

Phase 1 - paper correctness:

- Run current Rust paper mode for at least 48 continuous hours.
- Fix logging so `traded=true` means an order/position was actually created, or add a separate `decision_trade=true` field.
- Add a guard/metric for `decision_trade=true` with no paper/live position created.
- Validate every completed session with `polymomentum-engine validate-replay`.
- Confirm paper `trades`, `paper_positions`, `oracle_pending`, and resolution events are consistent.

Acceptance:

- 0 replay mismatches.
- 0 repeated execution attempts on the same condition after a successful paper position.
- 0 unresolved positions older than 2 resolution cycles unless oracle explicitly unavailable.
- Source dropout and cross-exchange spread metrics within defined thresholds.

Phase 2 - execution hardening:

- Implement CLOB order status polling/reconciliation before recording a live fill.
- Handle partial fills, cancels, rejects, and expired maker orders.
- Implement maker timeout/cancel using `CANDLE_MAKER_TIMEOUT_S`.
- Pass `contract.market.neg_risk` into order building/signing.
- Add live dry-run tests using mocked CLOB responses for accepted/unfilled/partial/rejected orders.
- Verify wallet balance, allowance, and minimum order sizing before entering live mode.

Acceptance:

- No live order is recorded as filled until exchange state confirms it.
- Partial and rejected orders update risk/session logs correctly.
- Live mode refuses to start without Slack/webhook or equivalent critical alerting.

Phase 3 - alpha validation:

- Backfill or hydrate a clean 7-30 day harness set locally, using shared PMXT parquets and VPS/local BTC ticks where available.
- Run terminal-only, baseline, maker-first, and a small predeclared grid. Do not tune on the test slice and then report that same slice as proof.
- Use realistic taker/maker fills, fees, and latency.
- Report by zone, confidence bucket, price bucket, window length, volatility regime, and asset.
- Bootstrap trade-level PnL confidence intervals.

Acceptance:

- Candidate strategy has at least 100 resolved trades out-of-sample, positive net PnL after fees/slippage, and no single day/window explains the whole profit.
- Terminal-only remains positive when isolated, not just as a lucky slice of baseline.
- Strategy passes a no-trade/null-signal comparison and a stale-price stress test.

Phase 4 - shared cache and ops cleanup:

- Create `/opt/shared/pmxt_v2_distilled_candles` with `pmxt-data` ownership/group permissions.
- Backfill v1 distilled candles from the dev box, not the VPS.
- Leave a cross-bot note if any CLI/path/protocol behavior changes.
- Add a lightweight daily report: service status, paper PnL, unresolved positions, replay mismatches, source gaps, Gamma errors.

Acceptance:

- Harness uses shared distilled cache first, then sidecar, then parquet.
- Missing/corrupt distilled files fall back cleanly.
- No CPU-heavy sweep runs on the VPS.

Phase 5 - limited live ramp:

- Start with terminal-only, BTC-only, $1 max position, and a tiny bankroll.
- Run live for one short window only after paper mode is clean.
- Reconcile every order manually after the first run.
- Increase only after 20+ live orders have clean fill/reconcile logs and paper-vs-live slippage is acceptable.

Initial live caps:

- `MAX_POSITION_PER_MARKET_USD=1`.
- Disable cross-asset boost.
- Disable non-BTC assets until asset-specific IV/history is implemented.
- Keep `--i-understand-live` manual and never bake it into unattended deploys until the first live checklist passes.

## Recommended immediate tickets

1. Replace VPS legacy service with current paper service.
2. Fix `traded=true` semantics and add "decision vs executed" counters.
3. Implement CLOB fill reconciliation before live.
4. Enforce `max_total_exposure_usd` and cooldown settings or remove them from config.
5. Pass neg-risk metadata into signing.
6. Restore breaker state from persisted trade/resolution history.
7. Add asset gating: BTC only by default; ETH/SOL only with asset-specific IV/history.
8. Create shared distilled-cache directory and backfill from the dev box.
9. Add a reproducible offline harness command for Apr 25-26 using cached Gamma metadata and tick CSVs.
10. Require 48h current-Rust paper evidence before any live attempt.

