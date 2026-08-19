# PolyMomentum Project Summary And Findings - 2026-05-24

Generated on: `2026-05-24T09:35:41Z`

Branch: `codex/audit1`

## Executive Summary

PolyMomentum is a Rust trading system for Polymarket crypto candle markets,
currently focused on 5-minute BTC `Up or Down` markets on the international
CLOB from a Dublin VPS. The project has moved from broad paper-mode plumbing
and strategy exploration into a backtest-first promotion workflow with
replay-grade PMXT v2 order-book data, deterministic strategy artifacts, and
explicit live-mode guards.

The latest validated candidate is:

```text
deploy/promotions/promotion_candidate_stresscap024_aggregate_20260520_24.json
```

Latest strategy hash:

```text
d6f02682adf2c22a20a32cbaa9212657daebda48c61cde59e5622f9be8553e74
```

Latest aggregate offline result:

| Metric | Value |
| --- | ---: |
| Fills | 208 |
| Wins / losses | 154 / 54 |
| Win rate | 74.04% |
| Wilson 95% lower bound | 67.7% |
| PnL | +121.36 |
| Average PnL / fill | +0.583 |
| Breaker trips | 0 / 3 windows |
| Worst daily PnL | +7.92 |

Current grade:

| Category | Grade | Reason |
| --- | --- | --- |
| Rust code and test health | A- | Core flow is Rust-only, tested, and modular; some live reconciliation work remains. |
| Offline backtest/live-replay platform | A- | L2 replay, promotion artifacts, diagnostics, and feed-forward gates are strong. |
| Latest strategy candidate | B+ / A- offline | Profitable across latest 3 folds after stress cap, but still early-zone concentrated. |
| Paper/live venue readiness | B | Live mode is guarded and CLOB V2-aware, but funded canary/user-channel reconciliation evidence is still required. |
| Overall production readiness | B+ | Ready for bounded venue-plumbing validation, not yet ready for unconstrained live capital. |

## What The Bot Does

PolyMomentum trades binary candle markets where the outcome is whether an asset
finishes above or below a strike at candle close. The current production focus
is BTC 5-minute markets.

The runtime loop:

1. Discovers active BTC candle markets from Gamma.
2. Subscribes to external BTC price feeds and Polymarket CLOB market data.
3. Maintains full L2 order books for YES/NO tokens.
4. Builds a momentum signal from move-from-open, z-score, consistency, and
   realized volatility.
5. Estimates binary fair value using option-style/fair-value logic.
6. Applies decision gates by timing zone, confidence, z-score, edge, price
   range, EV buffer, settlement guard, and microstructure.
7. Creates a deterministic order intent with a stable strategy identity.
8. In paper/backtest, simulates fills and resolves against BTC tape and/or
   oracle evidence.
9. In live mode, submits signed CLOB V2 orders only if explicit live safeguards
   pass.

High-level flow:

```text
Gamma markets + PMXT/CLOB L2 + BTC tape
        |
        v
Market scanner -> candle universe -> strategy loop
        |
        v
Momentum signal -> fair value -> decision gates
        |
        v
Risk sizing -> order intent -> maker/taker fill model or live CLOB
        |
        v
Resolution -> PnL -> breaker state -> diagnostics/promotion gate
```

## Repository Structure

Important paths:

| Path | Purpose |
| --- | --- |
| `rust_engine/` | Main Rust engine and all production logic. |
| `rust_engine/src/main.rs` | CLI command dispatch: live, live-replay, harness, sweep, diagnostics, wallet, CLOB checks. |
| `rust_engine/src/live/` | Paper/live runtime, breaker, replay bridge, paper fill model, window parsing. |
| `rust_engine/src/backtest/` | PMXT loader, L2 replay, harness, resolver, experiment/promotion reports. |
| `rust_engine/src/strategy/` | Momentum detector, decision gates, microstructure checks, strategy specs. |
| `rust_engine/src/clob.rs` and `rust_engine/src/signing.rs` | CLOB REST diagnostics and V2 order signing. |
| `deploy/` | Deployment scripts, systemd-related runtime material, promotion artifacts. |
| `deploy/promotions/` | Promoted strategy artifacts and evidence reports. |
| `docs/` | Audit notes, production plans, cross-bot coordination notes, validation results. |

## Current Strategy Candidate

Latest selected aggregate candidate:

```text
early_c0.40_z0.70_e0.03_ev-1.00_p0.10-0.75_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk
```

Meaning:

| Parameter | Value |
| --- | --- |
| Zone | early only |
| Confidence threshold | 0.40 |
| Z-score threshold | 0.70 |
| Edge threshold | 0.03 |
| EV buffer | -1.00, effectively disabled |
| Price range | 0.10 to 0.75 |
| Settlement floor | 10 USD |
| Settlement guard | 1.0 minute |
| Micro spread | <= 1.00 |
| Micro depth | >= 0 |
| Micro pressure | >= -1.00 |
| Execution style | maker-first |
| Position sizing | 5% bankroll, capped at 20 USD per market |
| New risk cap | projected stressed drawdown <= 24% |

Risk profile:

```text
position_pct=0.0500;max_per_market_usd=20.00;stress_dd_cap=0.2400
```

## Key Findings

### 1. Backtest-first validation is the right default

Paper mode is not necessary for validating strategy logic when cached PMXT L2
data and BTC tape can reproduce the same feed mechanism. The current rule is:

- Use feed-forward backtest, cached live-replay, diagnostics, and parity checks
  for strategy, fills, risk, and PnL validation.
- Use paper mode only for irreducible venue plumbing: credentials, websocket
  behavior, REST/user-channel reconciliation, real rejects/acks/fills, and VPS
  service wiring.

This avoids waiting days for paper-mode samples when the same behavior can be
proved offline on 5-minute frames.

### 2. The strategy is maker-dependent

Recent validations repeatedly show that maker-first variants are materially
better than taker variants. Taker variants often lose money or trip the breaker.

Production implication:

- Do not deploy the selected strategy as taker-only.
- `LIVE_ALLOW_MAKER_ORDERS=1` is required for live deployment of maker
  promotions.
- Passive maker non-fills and post-only crosses are expected and must be
  monitored, not treated as fatal by themselves.

### 3. The previous strict candidate was profitable but not safe enough

The strict candidate before the stress cap had a veto fold:

```text
2026-05-20T03:00Z..2026-05-21T02:00Z
```

Without the new stress cap:

| Metric | Value |
| --- | ---: |
| Fills | 101 |
| Wins / losses | 68 / 33 |
| PnL | +4.71 |
| Breaker | yes |
| Breaker reason | open_exposure_stress |
| Breaker time | 2026-05-21T00:45:42Z |
| Stressed drawdown | 26.3% |

Conclusion: the issue was not CLOB plumbing. The issue was exposure stress in a
bad regime.

### 4. The projected stressed-drawdown cap fixed the veto fold

The latest implementation adds a feed-forward cap:

```text
max_projected_stressed_drawdown_pct = 0.24
```

It uses only realized breaker state plus current open/submitted exposure. It
does not look at future outcomes.

With the cap:

| Window | Fills | W/L | Win rate | PnL | Breaker | Stress-cap skips |
| --- | ---: | ---: | ---: | ---: | --- | ---: |
| 2026-05-20T03..2026-05-21T02 | 98 | 67/31 | 68.37% | +7.92 | no | 8,216 |
| 2026-05-21T03..2026-05-22T02 | 53 | 41/12 | 77.36% | +59.59 | no | 0 |
| 2026-05-23T03..2026-05-24T02 | 57 | 46/11 | 80.70% | +53.85 | no | 0 |

Conclusion: the cap corrected the known bad exposure regime while leaving the
good folds essentially intact.

### 5. The latest aggregate candidate is promotable offline, but not final live A+

The aggregate promotion passed strict offline gates:

- 3 reports
- 3 profitable reports
- >= 180 total fills
- >= 30 fills per daily report
- win rate >= 70%
- Wilson lower bound >= 65%
- total PnL >= 100
- no unresolved fills
- failed passive maker attempts allowed up to 30

Remaining caveats:

- 25 failed passive execution attempts across aggregate reports:
  - 14 maker unfilled
  - 11 post-only crossed
- 100% of selected fills are early-zone trades.
- Live venue reconciliation still needs funded canary evidence.

## PnL Calculation

The backtest/paper PnL model resolves a filled binary token position at candle
close:

```text
if predicted direction wins:
    pnl = (1.0 - entry_price) * shares - fee
else:
    pnl = -entry_price * shares - fee
```

Position sizing:

```text
base_position_usd = min(bankroll * position_pct, max_per_market_usd)
shares = round(position_usd / sizing_price).max(1)
```

For maker fills, the sizing price is the resting maker limit price. For taker
fills, it is the executable market price under the taker fill model.

The latest maker reports have zero maker fees in the model. Taker variants
include taker fees and slippage assumptions.

## Backtesting And Replay Principles

The project now follows these validation rules:

1. Feed-forward only. Strategy decisions must be made from information
   available at the event timestamp.
2. No paper validation when backtest/live-replay can prove the same behavior.
3. PMXT L2 replay is the primary strategy validator.
4. Live-replay bridges the research harness and production runtime by using the
   live decision/order diagnostics path on cached data.
5. Promotion artifacts are immutable strategy contracts with stable hashes.
6. Aggregate promotion requires multiple non-overlapping windows.
7. Circuit breaker trips are vetoes unless the strategy change proves the
   breaker is avoided feed-forward.
8. Passive maker non-fills are expected, but unresolved fills are not allowed in
   promotion gates.

## Data And Cache Findings

Data sources:

- PMXT v2 archives for Polymarket order-book events.
- Gamma historical market metadata.
- BTC price tape from Binance klines or cached CSV.
- Live CLOB/Gamma/websocket data for paper/live runtime.
- Optional on-chain CTF resolution checks.

Shared VPS cache rules:

- PMXT v2 archive cache: `/opt/shared/pmxt_v2_cache/`
- Distilled candles cache: `/opt/shared/pmxt_v2_distilled_candles/`
- Cross-bot notes: `/opt/shared/cross_bot_notes/`

Important operational rules:

- Never delete parquet files downloaded by another bot.
- Heavy sweeps and parameter searches run on the dev box, not the VPS.
- The VPS is for live runtime, bounded diagnostics, deployment, and one-off
  distill jobs only.
- Any short-lived test cache must have a session-specific root and be deleted
  after reports are copied.

Recent local temp cache handling:

```text
/private/tmp/polymomentum_stresscap_gate_20260524T0800Z/
```

This root was deleted after evidence reports were copied into the repo.

## CLOB V2 And Live Path Findings

The project is aligned with the international Polymarket CLOB host:

```text
https://clob.polymarket.com
```

Important live-mode guards:

```text
VENUE=polymarket_international
OPERATOR_COUNTRY=IE
POLYMOMENTUM_VENUE_COMPLIANCE_OK=1
POLYMARKET_US_API_ENABLED=0
CLOB_V2_READY=1
POLYMOMENTUM_LIVE_RECONCILIATION_READY=1
POLYMOMENTUM_REQUIRE_PROMOTION=1
CANDLE_SETTLEMENT_ALIGNMENT_READY=true
LIVE_ALLOW_MAKER_ORDERS=1
ALERT_REQUIRED=1
```

Known live/CLOB findings:

- The raw order signer was migrated toward the CLOB V2 EIP-712 shape.
- pUSD and CTF Exchange V2 allowances are the live collateral focus.
- Read-only CLOB diagnostics exist for health, time, market metadata, orders,
  and trades.
- Live mode is intentionally fail-closed behind explicit flags.
- Funded live canary and user-channel/REST reconciliation are still required
  before real capital ramp.

## Wallet And Collateral Findings

Live trading requires:

- pUSD balance sufficient for configured order budget.
- pUSD allowances for CTF Exchange V2 and Neg Risk CTF Exchange V2 where
  applicable.
- POL for gas.
- API credentials and signer configuration passing live preflight.

The docs currently recommend at least the configured first-order budget plus
buffer. For earlier `BANKROLL_USD=100`, `CANDLE_POSITION_PCT=0.05`, and
`LIVE_ORDER_BUDGET_BUFFER=1.10`, that first-order requirement was about
`$11.00` pUSD, but the exact live requirement should always be read from the
current preflight output.

Do not commit credentials or wallet secrets into docs.

## VPS And Multi-Bot Coexistence

The VPS is shared with:

- `adgts`
- `polyarbitrage`
- `polyarbitrage-collector`

Rules followed during this work:

- No peer private directories were read.
- No peer systemd units were modified.
- Heavy backtests ran locally, not on the VPS.
- Disk cleanup is coordinated through cross-bot notes.
- Shared cache ownership rules are respected.

Production implication:

- Live deployments should check peer service state before restart.
- CPU-heavy sweeps should remain local.
- Shared-disk cleanup must be coordinated before deleting shared artifacts.

## Most Important Artifacts

Latest aggregate promotion:

```text
deploy/promotions/promotion_candidate_stresscap024_aggregate_20260520_24.json
```

Latest evidence:

```text
deploy/promotions/evidence/harness_sweep_strict_candidate_stresscap024_window3_20260520_21.json
deploy/promotions/evidence/harness_sweep_strict_candidate_stresscap024_window1_20260521_22.json
deploy/promotions/evidence/harness_sweep_strict_candidate_stresscap024_current_20260523_24.json
```

Important docs:

```text
docs/stress_drawdown_cap_validation_2026-05-24.md
docs/strict_candidate_additional_windows_2026-05-24.md
docs/current_24h_candidate_validation_2026-05-24.md
docs/live_readiness_2026-05-23.md
docs/clob_v2_audit_and_production_plan_2026-05-01.md
docs/project_audit_2026-05-15.md
docs/shared_testing_cache_protocol_v1.md
docs/cross_bot_protocol_v1_finalized.md
```

Prior notable promotion:

```text
deploy/promotions/promotion_early_scout_wr_c030_z050_e003_cap15_20260423_25.json
```

That prior artifact had strong earlier offline/live-replay evidence. The latest
stress-cap artifact is the current candidate from the newest validation loop.

## What Is Strong Now

- Rust-only engine with production subcommands.
- Backtest/live-replay/promotion pipeline exists and is usable.
- PMXT v2 data can replay the live feed mechanism closely enough for strategy
  validation.
- Strategy identity is artifact-bound and hash-checked.
- Breaker state is shared conceptually between backtest and live.
- Projected stressed-drawdown cap prevents the known open-exposure breaker
  failure feed-forward.
- Latest aggregate candidate passes offline promotion gates.
- Temporary test caches were cleaned after evidence extraction.
- VPS coexistence rules are documented and followed.

## What Is Still Weak Or Risky

- Latest selected strategy is 100% early-zone concentrated.
- Passive maker fill behavior remains a live-sensitive assumption.
- Dynamic CLOB V2 fee parity is not fully proven.
- Live user-channel reconciliation still needs funded canary proof.
- Wallet pUSD/allowance/POL readiness must be checked immediately before live.
- Paper mode should not be used as a substitute for backtest validation, but it
  is still needed for venue plumbing.
- Latest aggregate covers 3 windows, not weeks of market regimes.
- Old docs may still describe earlier strategy states; use dated docs and
  promotion artifact paths carefully.

## Recommended Production Roadmap

### Phase 1 - Freeze the latest candidate

Use:

```text
deploy/promotions/promotion_candidate_stresscap024_aggregate_20260520_24.json
```

Do not tune it further until the next validation phase is complete.

Verification:

```bash
POLYMOMENTUM_PROMOTION_ARTIFACT=deploy/promotions/promotion_candidate_stresscap024_aggregate_20260520_24.json \
./rust_engine/target/release/polymomentum-engine release-manifest --mode paper
```

Expected:

- promotion status `ok`
- params hash `d6f02682adf2c22a20a32cbaa9212657daebda48c61cde59e5622f9be8553e74`
- risk profile includes `stress_dd_cap=0.2400`

### Phase 2 - Live-replay parity on the latest artifact

Run cached live-replay on the same windows used for promotion and compare
diagnostics against harness results. This validates the production decision
path, order lifecycle logging, and session diagnostics without touching live
capital.

Success criteria:

- diagnostics `ok=true`
- no critical order lifecycle errors
- oracle/resolution checks present
- zero strategy identity drift
- no breaker trip

### Phase 3 - Paper only for venue plumbing

Run bounded paper mode only to prove:

- VPS deployment wiring
- credentials are loadable
- websocket subscriptions work
- session diagnostics are written
- CLOB/Gamma endpoints are reachable
- peer services are unaffected

Success criteria:

- no peer service degradation
- no resource contention
- diagnostics `ok=true`
- no unexpected breaker state restored from old sessions

### Phase 4 - Live preflight and wallet gate

Before live:

- Confirm pUSD balance.
- Confirm CTF Exchange V2 allowances.
- Confirm POL gas.
- Confirm CLOB V2 flags.
- Confirm `LIVE_ALLOW_MAKER_ORDERS=1`.
- Confirm alerts are enabled.
- Confirm kill switch path.

Success criteria:

- live preflight passes
- no warning about missing promotion
- wallet budget covers first order plus buffer
- reconciliation readiness flag is intentional, not cargo-culted

### Phase 5 - Funded canary

Run a minimal live canary with tiny bankroll and hard caps.

Success criteria:

- order created with expected deterministic intent
- CLOB ack received
- user-channel or REST reconciliation confirms state
- fill/cancel/reject is recorded correctly
- PnL/resolution path matches expectation
- no orphan orders
- no unintended repeat entries

### Phase 6 - Controlled ramp

Only after canary:

- increase bankroll slowly
- keep stress cap enabled
- keep max total exposure conservative
- monitor passive maker failure rate, fill rate, latency, and breaker metrics
- stop if live deviates from cached replay assumptions

## Current Bottom Line

PolyMomentum is no longer a loose experiment. It has a credible, reproducible
offline promotion workflow and a latest candidate that fixed the known
open-exposure stress failure without lookahead.

It is not yet safe to treat as fully live A+ because the remaining unknowns are
venue-real:

- maker execution realism
- live CLOB V2 user-channel reconciliation
- funded wallet/allowance behavior
- canary order lifecycle

The correct next step is not more paper strategy validation. The correct next
step is bounded live-replay parity for the latest artifact, followed by a very
small venue canary once wallet/preflight gates are green.
