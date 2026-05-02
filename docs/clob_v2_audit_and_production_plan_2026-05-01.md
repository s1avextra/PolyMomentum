# CLOB V2 Audit And Production Plan - 2026-05-01

## Scope

VPS context: Dublin host, international Polymarket CLOB, shared with adgts and
polyarbitrage. This note checks whether PolyMomentum is production-ready for
the newly live CLOB V2 API and lays out the remaining path from backtest to
paper to live without disturbing peer bots.

Official sources reviewed:

- Polymarket CLOB V2 migration guide: https://docs.polymarket.com/v2-migration
- Polymarket API overview: https://docs.polymarket.com/api-reference/introduction
- Polymarket auth docs: https://docs.polymarket.com/api-reference/authentication
- Polymarket clients and SDKs: https://docs.polymarket.com/api-reference/clients-sdks
- Polymarket create-order docs: https://docs.polymarket.com/trading/orders/create
- Polymarket WebSocket overview: https://docs.polymarket.com/market-data/websocket/overview
- Polymarket contracts: https://docs.polymarket.com/resources/contracts

## Verdict

PolyMomentum is pointed at the correct international production hosts, and the
raw order signer has been migrated to the documented CLOB V2 EIP-712 shape.
The system is still not production-ready for capital because authenticated
user-channel/REST reconciliation and live canary evidence are missing.

Current state grade for production trading: C-.

Why:

- Backtest, paper, diagnostics, and market-data intake are now structurally much
  stronger than the initial audit state.
- The CLOB read path targets the current international CLOB REST host and market
  WebSocket host.
- Live order submission now builds a V2-shaped raw order, but it has not yet
  been proven with a funded canary and reconciliation evidence.
- Wallet diagnostics are pUSD-first and check V2 allowances, but wrapping and
  a funded canary still have to be verified operationally.
- The live pipeline now uses market neg-risk and Gamma-provided tick size, but
  still lacks CLOB V2 market-info fee parity and user-channel reconciliation.

## What Is Correct

International host selection is correct:

- `rust_engine/src/config.rs` defaults `POLY_BASE_URL` to
  `https://clob.polymarket.com`.
- `rust_engine/src/config.rs` defaults Gamma discovery to
  `https://gamma-api.polymarket.com`.
- `rust_engine/src/polymarket_ws.rs` uses
  `wss://ws-subscriptions-clob.polymarket.com/ws/market`.

Those are the international docs endpoints, and the CLOB V2 migration guide says
production CLOB V2 now runs at `https://clob.polymarket.com`.

Public diagnostics/read-only CLOB calls are mostly compatible:

- `rust_engine/src/clob.rs` supports `/ok`, `/time`, `/book`, `/price`,
  `/midpoint`, `/spread`, `/tick-size`, `/fee-rate`, `/neg-risk`, and
  `/markets/{condition_id}` style read calls.
- Auth headers for L2 REST still use the documented `POLY_*` header model.
  CLOB V2 keeps L1/L2 API auth compatible; the breaking change is order signing.

## CLOB V2 Blockers

### 1. Manual order signing is now V2-shaped, but unproven live

`rust_engine/src/signing.rs` now uses:

- CLOB V2 exchange address: `0xE111180000d2663C0091e4f400237545B87B996B`
- CLOB V2 neg-risk exchange address: `0xe2222d279d744050d28e00520010520000310F59`
- EIP-712 exchange domain version `"2"`
- V2 signed order fields: `timestamp`, `metadata`, `builder`
- no signed `taker`, `expiration`, `nonce`, or `feeRateBps`

Remaining risk: this is a raw local implementation, not the official SDK, and
still needs a funded canary against production CLOB V2 before capital ramp.

### 2. Order wire body is V2-shaped

`rust_engine/src/clob.rs` now posts the signature inside the order object,
includes V2 `timestamp`, `metadata`, and `builder` fields, includes `deferExec`,
and omits removed V1 fields `nonce`, `feeRateBps`, and `taker`.

### 3. Fees are hardcoded and embedded in signing

`rust_engine/src/signing.rs` no longer signs `feeRateBps`. CLOB V2 fees are
applied by the protocol at match time, not embedded in the signed order. Paper
and backtest still need CLOB V2 market-info fee details for closer parity.

### 4. Fees are not carried into live orders dynamically

`rust_engine/src/live/pipeline.rs` now passes market `neg_risk` metadata into
the blocked live order path and rounds prices using market `minimum_tick_size`
when Gamma supplies it. Dynamic fee handling is still missing: the V1 signer
embeds `feeRateBps`, while CLOB V2 applies operator-set fees at match time.

### 5. Collateral docs and wallet checks are partially migrated

`docs/SETUP_API_KEYS.md`, `rust_engine/src/data/wallet.rs`, and related CLI copy
now emphasize pUSD and V2 exchange allowances. Remaining work is an explicit
wrap/allowance doctor with a single ready/not-ready result.

### 6. User-channel reconciliation is still incomplete

The market WebSocket path is present. The authenticated user WebSocket path is
not yet integrated into the live order manager. CLOB V2 docs expose user-channel
`trade` and `order` events and heartbeat expectations. Live cannot be considered
paper-identical until accepted, live, matched, confirmed, cancelled, delayed,
and failed states are reconciled from user-channel/REST evidence.

## Production Plan

### Phase 0 - Safety freeze

Goal: keep paper/backtest research moving while blocking unsafe capital flow.

Steps:

1. Keep `LIVE_MODE=false` by default and require an explicit `CLOB_V2_READY=true`
   guard before live order posting. Implemented in code as `CLOB_V2_READY=1`,
   with an additional compiled-signer-version check so the flag cannot unlock
   the current V1 signing path.
2. On the VPS, do not run CPU-heavy sweeps. Use the dev machine for sweeps and
   export artifacts to the VPS.
3. Keep shared parquet rules intact: no deletion of peer-owned parquet, no
   concurrent scan of the same parquet hour, atomic writes for shared artifacts.
4. Confirm `VENUE=polymarket_international`, `OPERATOR_COUNTRY` reflects Dublin
   operation, and `POLY_BASE_URL=https://clob.polymarket.com`.

Verification:

- `cargo test`
- read-only `clob ok`, `clob time`, `clob tick-size`, `clob fee-rate`,
  `clob neg-risk` against known active tokens
- no live POST `/order` from production service

### Phase 1 - Replace raw CLOB V1 signing

Goal: make the order path CLOB V2-correct before any live trading.

Preferred implementation:

1. Prefer the official Rust V2 SDK, `polymarket_client_sdk_v2`, as the source of
   truth for order creation, signing, auth, and posting. Current state uses a
   raw V2-compatible signer, so SDK parity/golden tests remain desirable.
2. Wrap it behind our own narrow `ClobExecutionAdapter` so strategy code does
   not depend directly on SDK internals.
3. Keep the existing read-only `ClobClient` for diagnostics if it remains useful.
4. If SDK integration is blocked, port the exact V2 signing shape from official
   references and add golden-vector tests before enabling live.

Required behavior:

- EIP-712 exchange domain version `"2"`
- V2 exchange and neg-risk exchange addresses from Polymarket Contracts docs
- V2 signed order type with `timestamp`, `metadata`, `builder`
- no signed `nonce`, `feeRateBps`, `taker`, or `expiration`
- support optional `POLY_BUILDER_CODE`
- support EOA, proxy, and Gnosis Safe signature types through config

Verification:

- Unit tests for V2 order type hash/domain hash against SDK/reference fixtures
- Integration test that builds but does not post a signed order for both normal
  and neg-risk markets
- Read-only auth doctor validates API credentials, signer address, server time,
  and market metadata

### Phase 2 - Market parameter truth source

Goal: make backtest, paper, and live use the same market facts.

Steps:

1. Add a CLOB market info cache keyed by condition ID/token ID.
2. Populate minimum tick size, minimum order size, fee details, token mapping,
   RFQ flag, and neg-risk from current CLOB/Gamma metadata. Partial:
   Gamma `minimum_tick_size` is now parsed and live rounding uses it.
3. Feed these values into live order creation and into paper/backtest fill
   models.
4. Remove hardcoded `0.01` live tick size. Done for Gamma-provided
   `minimum_tick_size`; CLOB endpoint fallback cache is still pending.
5. Replace hardcoded `0.072`/`200 bps` fee assumptions with dynamic fee details
   in diagnostics and strategy evaluation.

Verification:

- For every tradable token selected by scanner, diagnostics record:
  `tick_size`, `min_order_size`, `neg_risk`, fee details, book spread, and depth.
- Paper fill math uses the same fee-rate source as live order preparation.
- Backtest report includes the market parameter snapshot used for each run.

### Phase 3 - Collateral and allowance migration

Goal: prove the wallet can trade V2 without discovering allowance failures live.

Steps:

1. Update wallet balance reader to include pUSD. Done for balance display and
   bankroll auto-detection.
2. Keep USDC.e/native USDC/POL readings as diagnostics only.
3. Add allowance checks for pUSD against V2 CTF Exchange and V2 Neg Risk CTF
   Exchange. Done in the read-only `wallet` command.
4. Update `docs/SETUP_API_KEYS.md` from USDC.e-first to pUSD-first setup.
5. Add a `wallet doctor` command that prints balances, allowances, signature
   type, funder, and whether the account is ready for BUY/SELL orders. Partial:
   the existing `wallet` command now prints pUSD and allowance diagnostics.

Verification:

- `wallet doctor` returns a single ready/not-ready summary.
- Live startup refuses trading when pUSD balance or allowance is insufficient.

### Phase 4 - Authenticated order reconciliation

Goal: make live fills exchange-evidenced and auditable.

Steps:

1. Add authenticated user WebSocket client for `ws/user`.
2. Subscribe by condition ID for active markets.
3. Merge user-channel `order` and `trade` events into
   `SharedOrderManager`.
4. Add REST fallback polling for open orders/trades when the user channel is
   stale.
5. Implement heartbeat handling according to docs. If heartbeat is active and
   missed, mark local orders as potentially cancelled and reconcile.
6. Add cancel-by-id and cancel-all scoped to PolyMomentum-owned order IDs only.

Verification:

- Every live order intent transitions through exchange-evidenced states.
- No fill is recorded from POST acceptance alone.
- Restart/recovery can rebuild state from REST open orders and recent trades.

### Phase 5 - Paper/live parity loop

Goal: paper and live produce identical decisions, with differences limited to
exchange response states.

Loop:

1. Run short paper session with diagnostics.
2. Replay the session with the frozen strategy artifact.
3. Compare signal decisions, selected markets, order intents, sizing, tick/fee
   parameters, and simulated fills.
4. Fix mismatches.
5. Repeat until decision mismatch count is zero over multiple active windows.
6. Run one minimum-size live canary order on an actively traded, low-risk market.
7. Compare paper shadow vs live actual through user-channel/REST evidence.

Promotion criteria:

- zero replay mismatches for decisions and intended orders
- no malformed diagnostics
- no unbounded queue growth
- no stale WebSocket during active subscription window
- live canary has complete order lifecycle evidence

### Phase 6 - Backtest research loop

Goal: improve strategy quality without overfitting to one short window.

Steps:

1. Use PMXT v2/orderbook data with row-filtered scans only.
2. Run sweeps on the dev machine, not the VPS.
3. Split data into train, validation, and forward-test windows by date and
   market regime.
4. Evaluate baseline, maker-first, microstructure-confirmed, and terminal
   microstructure variants.
5. Add explicit cost models for spread, adverse selection, dynamic taker fees,
   maker probability, partial fill risk, and latency.
6. Require enough trades before promotion; no strategy graduates on a tiny sample
   just because PnL is positive.
7. Store promoted strategy artifacts with parameter hash, data manifest,
   backtest report, and diagnostics replay report.

Verification:

- Candidate has positive net EV after fees/slippage across validation and
  forward-test windows.
- Max drawdown and per-market exposure remain within configured risk limits.
- Promotion artifact is the only source loaded by paper/live.

### Phase 7 - VPS deployment without affecting peer bots

Goal: deploy safely on the 2-core Dublin VPS.

Steps:

1. Build release on the dev machine when possible.
2. If building on VPS, run only one build and use
   `nice -n 10 cargo build --release`.
3. Run PolyMomentum as its own systemd service with conservative CPU and memory
   limits.
4. Keep logs under PolyMomentum-owned dirs.
5. Use shared dirs only for finalized cross-bot artifacts and
   `/opt/shared/cross_bot_notes/`.
6. Write peer-visible notes for any shared-cache format or operational
   convention change, and mirror them into `docs/`.
7. Never read peer private dirs or modify peer services.

Verification:

- adgts and polyarbitrage service health unchanged during deploy.
- CPU/memory stay within resource budget during paper and live canaries.
- No concurrent parquet hour scan with peer jobs.

### Phase 8 - Capital ramp

Goal: move from canary to controlled production.

Stages:

1. Shadow-only: decisions and would-trade orders, no live POST.
2. Single minimum-size live order with manual observation.
3. Ten minimum-size live orders with automatic reconciliation.
4. Small bankroll cap with kill switch and daily loss limit.
5. Gradual exposure increases only after diagnostics show stable behavior.

Stop conditions:

- any V2 signing/auth/order schema mismatch
- stale market or user WebSocket beyond threshold
- REST/user-channel disagreement not resolved by polling
- paper/live decision mismatch after code freeze
- unexplained PnL divergence from expected fee/slippage model
- peer bot resource impact on the shared VPS

## Immediate Next Actions

1. Add an explicit live-order guard until Phase 1 is complete. Done:
   live preflight and pipeline initialization now fail closed unless
   `CLOB_V2_READY=1` and the compiled order signer reports CLOB V2.
2. Integrate or wrap `polymarket_client_sdk_v2` for CLOB V2 order signing, or
   add golden tests proving the current raw signer matches the SDK.
3. Add CLOB V2 golden tests and an auth/order doctor command.
4. Replace hardcoded tick/fee/neg-risk handling in the live pipeline. Partial:
   neg-risk and Gamma-provided tick size are now passed from market metadata;
   fee handling remains a blocker.
5. Add pUSD wallet/allowance checks.
6. Add authenticated user-channel reconciliation.
7. Re-run the production loop: paper diagnostics, replay comparison, fixes, then
   one minimum-size live canary.
