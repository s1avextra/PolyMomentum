# Order Execution A+ Audit - 2026-06-02

## Scope

Local/offline execution-layer audit only. No VPS process, paper process, live
venue connection, wallet action, or peer-bot private path was touched.

Goal: compare PolyMomentum's order-flow model against NautilusTrader-style
execution architecture, Polymarket CLOB v2 mechanics, and the prior canary
failure mode; then harden the parts that can be proven offline before any live
capital is used.

## References Compared

- NautilusTrader execution architecture:
  https://nautilustrader.io/docs/latest/concepts/execution
- NautilusTrader order lifecycle:
  https://nautilustrader.io/docs/latest/concepts/orders
- Polymarket CLOB v2 trading overview:
  https://docs.polymarket.com/developers/CLOB/trades/trades-overview
- Polymarket CLOB v2 order creation and options:
  https://docs.polymarket.com/developers/CLOB/orders/create-order
- Polymarket CLOB v2 order details, tick sizes, neg-risk, and validity:
  https://docs.polymarket.com/developers/CLOB/orders/onchain-order-info
- Polymarket websocket user channel:
  https://docs.polymarket.com/developers/CLOB/websocket/wss-auth
- Polymarket Rust v2 client:
  https://github.com/Polymarket/rs-clob-client-v2
- Prior canary failure handoff:
  `docs/canary_orderflow_handoff_2026-05-17.md`

## Reference Model

NautilusTrader's useful principle for us is not its full generic framework; it
is the separation of responsibilities:

```text
Strategy -> risk gate -> execution manager -> venue client -> venue reports
        -> order state -> inventory/positions -> realized PnL
```

Polymarket CLOB v2 adds venue-specific constraints:

- every trade is a signed limit-order primitive;
- market/FOK orders are still limit orders with a worst-price bound;
- maker post-only orders must not cross or they are rejected;
- prices must match the market tick size;
- neg-risk markets require the correct exchange path;
- pUSD/allowance and open-order reservations constrain order size;
- accepted REST order IDs are not fills;
- user websocket order/trade events are the live reconciliation source;
- heartbeat failure cancels open orders.

The May 17 canary showed the exact hazards to guard:

- duplicate order slots;
- post-only cross reject storms;
- balance/allowance reject storms;
- one-sided/unattributed fills;
- treating live acceptance too much like paper fill.

## PolyMomentum Mapping

| Reference requirement | Current implementation | Grade |
| --- | --- | --- |
| Deterministic strategy intent | `strategy::spec::OrderIntent` with deterministic IDs | A+ |
| Risk before submission | `RiskManager`, active bankroll sizing, exposure cap, stress cap | A+ offline |
| Explicit order state | `execution::order_manager::OrderManager` | A+ after this patch |
| Venue order placement | `clob.rs` signs CLOB v2 EIP-712 and posts `/order` | A offline, live-gated |
| CLOB v2 tick/neg-risk | `clob.rs`, `signing.rs`, market metadata | A offline |
| Maker post-only shape | `resting_limit_price`, tick rounding, permanent reject halt | A+ offline |
| User event reconciliation | `clob_user_ws.rs` order/trade parser and `Pipeline::handle_user_event` | A offline, live-gated |
| Accepted order is not fill | live logs accepted orders as unconfirmed; fills only from user trade events | A+ |
| Pending/open exposure reserve | `live_pending_positions`, `paper_positions`, `oracle_pending` included in open exposure | A+ |
| Inventory/PnL persistence | `RiskManager`, paper/live fill attachment, oracle realization | A+ offline |
| Canary failure handling | duplicate slot blocked by `traded`, permanent rejects halt, failed trade releases pending exposure | A+ offline |
| Deployment fail-closed | live preflight requires CLOB v2 and reconciliation readiness flags | A+ |

## Changes Made

### Stricter order lifecycle

`OrderManager` now rejects invalid execution transitions:

- no `Submitted` without `RiskAccepted`;
- no second risk acceptance after submission;
- no fill before venue ack;
- no silent overfill capping;
- no duplicate fill after full fill.

This is closer to NautilusTrader's order-event discipline: state transitions are
explicit and invalid venue reports cannot silently mutate inventory.

### Permanent CLOB reject halt

Live placement failures now classify known permanent Polymarket CLOB rejects:

- balance / allowance exhausted;
- post-only order crosses;
- marketable BUY min-size violation;
- invalid tick / order shape.

Those reasons trip the circuit breaker and request process stop. This is
intentionally conservative for first live capital. A permanent venue reject is
not a strategy skip; it means the execution model no longer matches the venue.

### Failed trade reconciliation releases pending exposure

If a user-channel trade event is classified as failed, the pipeline now:

- rejects the managed order;
- removes its live pending position reserve;
- records an order rejection;
- trips the breaker with `live_trade_failed`;
- stops the process.

That keeps inventory state internally consistent and avoids stranded pending
exposure after a terminal venue failure.

## Inventory Machine Check

Inventory is still working after the execution hardening:

- active bankroll actualization on restart remains covered by
  `risk::manager::tests::actualize_on_open_promotes_restored_pnl_into_new_baseline`;
- open exposure still sums paper positions, pending oracle realization, and
  live pending orders;
- partial live fills attach actual fill size, price, and fee to the settlement
  lifecycle;
- active-bankroll backtest/live-replay parity remains supported by the June 2
  parity evidence:
  `deploy/promotions/evidence/live_replay_a_plus5m_guard_active_bankroll_parity_20260602.json`.

## Test Evidence

Targeted:

```text
cargo test -q order_manager
cargo test -q permanent_live_order_rejects_are_fail_closed
cargo test -q actualize_on_open_promotes_restored_pnl_into_new_baseline
cargo test -q live_position_from_fill_uses_actual_fill_economics
```

Full suite:

```text
cargo test
```

Result:

- library tests: `243 passed`
- binary tests: `262 passed`
- doc tests: `0 passed`

## Current Module Grades Before Live

| Module | Grade | Evidence | Remaining live-only gate |
| --- | --- | --- | --- |
| Strategy selection / backtest | A+ offline | feed-forward robust gate and active-bankroll parity | none for strategy proof |
| Live-replay order path | A+ offline | exact 157/157 harness parity, zero fill failures | none for offline path |
| Order manager lifecycle | A+ offline | strict transition tests, full suite | none |
| Inventory / PnL accounting | A+ offline | active-bankroll tests and parity artifact | live wallet actualization check at startup |
| CLOB v2 signing / order shape | A offline | signer/version/tick/neg-risk tests | real venue ack/reject smoke |
| User websocket reconciliation | A offline | parser tests and fail-closed handling | real user-channel order/fill/cancel stream |
| Wallet / allowance readiness | A offline | wallet tests and fail-closed preflight | current pUSD, allowances, POL immediately before live |
| Operational live readiness | A- | preflight gates exist | bounded canary with live venue diagnostics |

## Go/No-Go Interpretation

We can call the local execution model A+ for everything that can be proven
offline. We should not call the full live venue module A+ until the irreducible
venue checks pass:

1. current wallet doctor shows pUSD, both CTF Exchange v2 allowances, and POL;
2. live preflight passes with `VENUE=clob`, `CLOB_V2_READY=1`, and
   `POLYMOMENTUM_LIVE_RECONCILIATION_READY=1`;
3. a bounded live/canary session proves:
   - permanent CLOB errors: `0`;
   - duplicate order slots: `0`;
   - user-channel fills are attributed to managed venue order IDs;
   - order acceptance and fill/cancel/reject lifecycle matches diagnostics;
   - no breaker trip except deliberate fail-closed rejects.

Until those live-only checks pass, the safe state remains fail-closed.
