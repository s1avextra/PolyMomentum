# PolyMomentum A+ Project Audit - 2026-06-30

## Scope

This audit reviewed the local repository, module structure, current strategy
registry, archived promotion evidence, deployment scripts, CI, and prior
production notes. It did not change VPS runtime state and did not inspect peer
bot private directories.

Current branch: `codex/audit1`.

Current verification:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Result: formatting clean, Clippy clean, `314` tests passed.

After adding the registry audit gate in this loop, the current full test count is
`316` passed.

## Research Baseline

State-of-the-art bar used for this audit:

- Polymarket CLOB orders must be treated as an explicit lifecycle: signed
  intent, venue acknowledgement, fill/cancel/reject reconciliation, inventory
  lock, and terminal settlement. Accepted order placement is not PnL.
- Polymarket fees are outcome-price dependent, so win rate alone is not a
  useful profitability metric.
- NautilusTrader-style separation remains the right architecture pattern:
  strategy, risk, execution manager, venue adapter, reconciliation, inventory,
  and realized PnL should be independent enough to audit.
- Financial strategy search must be feed-forward, with trial ledgers,
  out-of-sample folds, left-tail risk, loss clustering, and multiple-testing
  awareness. Paper mode is not a substitute for backtest/live-replay when those
  can prove the same behavior.

Primary references:

- https://docs.polymarket.com/developers/CLOB/introduction
- https://docs.polymarket.com/trading/orders/overview
- https://docs.polymarket.com/trading/fees
- https://nautilustrader.io/docs/latest/concepts/execution
- https://nautilustrader.io/docs/latest/concepts/backtesting/

## Module Grades

| Area | Grade | Evidence | A+ gap |
| --- | --- | --- | --- |
| Rust build/test hygiene | A | `fmt`, `clippy -D warnings`, and `cargo test` pass locally | CI was stale; fixed in this loop |
| CI/release workflow | A- | Linux release artifact exists; CI now Rust-only with fmt/clippy/test/build | Needs one green remote run after push |
| Strategy discovery | A- | Feed-forward searches, registry, tail CVaR, loss-burst, robust promotion, PBO/neighbor gates | No passing current candidate |
| Strategy evidence registry | A | Durable archive with 38 copied files, 0 missing | Needs every future sweep exported by default |
| Current strategy candidate | C+ | Registry has rejected/questionable/dead-end entries only | Needs fresh resolved full-window candidate |
| Backtest/live-replay mechanics | A | Locked inventory, active bankroll, fill model, causality, resolver, and replay tests | Needs larger fresh-window replay matrix |
| Execution/order lifecycle | A | Strict order manager, CLOB v2 guards, user event parser, fail-closed rejects | Real venue canary still unproven |
| Inventory/PnL accounting | A | Active bankroll, locked exposure, tie payout, restart actualization tests | Needs current live wallet startup proof |
| Market data/PMXT cache | A- | Row filters, distilled cache protocol, shared-cache rules | Needs ongoing freshness and corrupt-cache monitoring |
| Risk/circuit breaker | A- | Drawdown, win-rate, open-exposure stress, permanent reject stops | Breaker policy still needs long-running live evidence |
| Monitoring/Telegram | A- | Status, stale, wallet, preflight, terminate controls and token redaction tests | Needs production UX soak after next deploy |
| Deployment/VPS coexistence | A- local, unknown current VPS | Deploy checks peer state, resource caps, disk preflight, start limits | VPS runtime freshness must be rechecked before promotion |
| Documentation | B+ -> A- | README and this audit now updated | Docs still contain many historic candidate notes that can confuse readers |

Overall current grade: **A- infrastructure, C+ strategy, B+ project**.

The important distinction: the engineering shell is now close to A, but the
trading system cannot be A+ until a strategy passes fresh, live-equivalent,
feed-forward evidence.

## Findings

1. **CI was stale.**
   The old CI still referenced a deleted Python package layout. That could make
   remote validation fail even though the project is now Rust-only.

2. **README was stale.**
   It still claimed the April paper-mode state and `51` tests. This obscured the
   current fail-closed registry verdict and current 314-test Rust suite.

3. **Current strategy remains research-only.**
   The registry has no `promoted` or `active` live-ready entry. The best primary
   challenger fixed one tail window but failed another.

4. **Execution and inventory are much stronger than strategy evidence.**
   Order lifecycle, active bankroll, locked exposure, and settlement accounting
   have tests and parity artifacts. The weak point is not basic mechanics; it is
   stable edge.

5. **Historical evidence is now durable, but future evidence needs discipline.**
   The new evidence export solved scratch-path registry pointers. Future sweeps
   should treat export as mandatory.

## Implementation Completed In This Loop

1. Replaced stale Python CI with Rust-only CI:
   - `cargo fmt --check`
   - `cargo clippy --all-targets --locked -- -D warnings`
   - `cargo test --locked`
   - release build

2. Hardened Linux release workflow:
   - added `rustfmt` and `clippy`
   - added format and Clippy gates before test/build

3. Updated `README.md`:
   - current fail-closed status
   - current test commands/count
   - strategy reality now points to registry evidence instead of April notes

4. Added `strategy-builder registry-audit`:
   - checks strategy registry status counts
   - verifies evidence files exist
   - verifies evidence paths live under the durable promotion archive
   - reports `live_ready=true` only when exactly one active/promoted entry has
     durable evidence

Current registry audit artifact:

```text
deploy/promotions/evidence/strategy_registry/20260630_registry_audit.json
```

Verdict:

- `ok=true`
- `live_ready=false`
- `grade=A-`
- entries: `5`
- live candidates: `0`
- missing paths: `0`
- non-durable paths: `0`

## A+ Implementation Plan

### Phase 1 - Keep Project Gates Honest

Status: implemented locally.

Gate:

- GitHub CI must pass on `codex/audit1`.
- No Python jobs until a Python package actually exists again.

### Phase 2 - Freeze Promotion Truth

Status: mostly implemented.

Next:

- Treat `docs/strategy_registry.json` as the promotion source of truth.
- Refuse promotion if registry status is not explicitly `promoted` and evidence
  is not archived.
- Keep stale promotion artifacts paper-only unless explicitly overridden.

### Phase 3 - Fresh Strategy Search

Status: pending.

Next:

- Run `multi-guard-search` over the full chronological May28-Jun10 reports with
  strict tail gates.
- Re-run the same gate on the freshest fully resolved windows.
- If no candidate passes, expand causal tags around the failure mechanism:
  direction, zone, reversion count, settlement distance, price bucket,
  microstructure, and execution mode.

Required A+ gate:

- Feed-forward OOS only.
- No lookahead or timestamp shortcuts.
- Positive or bounded worst fold.
- Tail CVaR and loss-burst pass.
- Profit factor after fees passes.
- Freshest resolved window passes.

### Phase 4 - Runtime/VPS Recheck

Status: pending and must be read-only first.

Next:

- Verify deployed git SHA, artifact identity, resource caps, disk headroom,
  health timers, Telegram monitor, and peer bot states.
- Do not run CPU-heavy sweeps on the VPS.
- Do not restart or deploy until peer state and disk state are safe.

### Phase 5 - Live-Only Mechanics

Status: blocked until strategy passes.

Next:

- Wallet doctor immediately before canary.
- Live preflight with CLOB v2 and reconciliation flags.
- Minimum-size canary only for venue mechanics, not strategy validation.

## Current Verdict

Do not go live.

The project is now in a better engineering state than the current strategy
state. The next productive loop is a fresh strict backtest/live-replay search
using the newly archived evidence and tail gates, then a VPS freshness check
only after a candidate clears offline promotion.
