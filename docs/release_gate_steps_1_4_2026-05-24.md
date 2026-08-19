# Release Gate Steps 1-4 - 2026-05-24

Scope: execute the first four production-gate steps for the current
PolyMomentum candle candidate without enabling real-money trading.

Branch: `codex/audit1`

## 1. Candidate Freeze

Frozen candidate:

```text
deploy/promotions/promotion_candidate_stresscap024_aggregate_20260520_24.json
```

Strategy hash:

```text
d6f02682adf2c22a20a32cbaa9212657daebda48c61cde59e5622f9be8553e74
```

Runtime profile:

```text
position_pct=0.0500;max_per_market_usd=20.00;stress_dd_cap=0.2400
```

No strategy parameters were tuned during this gate. The code changed only to
make replay/backtest mechanics more faithful and deterministic.

## 2. Fresh 24h Replay Gate

Window:

```text
2026-05-23T09:00:00Z..2026-05-24T08:00:00Z
```

Fresh PMXT cache:

```text
/private/tmp/polymomentum_release_gate_20260524T0947Z/pmxt
```

The cache was created for this bounded test session.
It was deleted after the replay reports and diagnostics evidence were copied
into git-tracked paths.

### Problems Found And Fixed

1. `live-replay` emitted `risk.state` on every L2 event even when no position
   changed. A first attempt generated a multi-GB session log. Fixed by making
   no-op settlement passes silent.
2. `live-replay` requested unused L2 history from the replay engine. Fixed by
   opting out of L2 history for the live replay strategy.
3. `live-replay` evaluated every L2 update while the harness used a 100ms
   live-like cadence bucket. Fixed by adding the same per-token cadence throttle.
4. Seeded maker fills depended on RNG call order, so chunking by hour changed
   fills. Fixed by making seeded maker fills deterministic from order economics,
   not from mutable RNG sequence or stage-specific intent IDs.
5. The historical harness reset strategy state each hour, which can double-enter
   hour-boundary markets and does not mirror live. Added `--continuous` mode to
   preserve strategy, fill-model, order-book, breaker, and traded-market state
   across the whole window.

### Final Parity Result

Persisted evidence:

```text
deploy/promotions/evidence/live_replay_orderkey_20260523T09_20260524T08.json
deploy/promotions/evidence/harness_sweep_continuous_orderkey_20260523T09_20260524T08.json
```

Live-replay final:

| Metric | Value |
| --- | ---: |
| Contracts | 288 |
| L2 events processed | 76,072,417 |
| Orders submitted | 67 |
| Successful fills | 36 |
| Passive non-fills | 31 |
| Wins / losses | 30 / 6 |
| PnL | +34.20 |
| Fees | 0.00 |
| Breaker trips | 0 |
| Oracle disagreements | 0 |
| Avg book age at fill | 2.31 ms |

Continuous backtest final, maker variant:

| Metric | Value |
| --- | ---: |
| Attempts | 67 |
| Successful fills | 36 |
| Passive non-fills | 31 |
| Wins / losses | 30 / 6 |
| PnL | +34.13 |
| Fees | 0.00 |
| Breaker trips | 0 |

The remaining PnL delta is `0.07` on `34+` total PnL, with matched attempts,
fills, non-fills, wins, losses, and breaker state.

Continuous backtest also showed the taker variant was profitable in this fresh
window:

| Variant | Attempts | Fills | Wins / losses | PnL | Fees |
| --- | ---: | ---: | ---: | ---: | ---: |
| Maker | 67 | 36 | 30 / 6 | +34.13 | 0.00 |
| Taker | 67 | 67 | 52 / 15 | +16.03 | 6.73 |

Maker remains better on this gate, but the taker result should be rechecked
across the older folds before making a new promotion decision.

## 3. Execution Mechanics Audit

Paper preflight with the frozen promotion passed:

```text
BANKROLL_USD=100
CANDLE_SETTLEMENT_ALIGNMENT_READY=true
mode=paper
venue=paper_only
promotion_status=ok
```

Local live preflight correctly refused live trading. The blockers were expected:

- `VENUE=paper_only`
- `CLOB_V2_READY=0`
- `POLYMOMENTUM_LIVE_RECONCILIATION_READY=0`
- `ALERT_REQUIRED=0`
- `LIVE_ALLOW_MAKER_ORDERS=0`
- wallet live-readiness was not established in this local run

This is the desired fail-closed posture. The code and tests account for:

- CLOB V2 signed order serialization and post-only flag handling
- authenticated open-order/trade diagnostics for reconciliation
- user websocket parsing for order/trade events
- order-manager ack, reject, partial-fill, full-fill, and venue-ID reconciliation
- live preflight guards for CLOB V2 readiness, reconciliation readiness, maker
  permission, wallet budget, venue compliance, and alerting

## 4. Bounded Paper Plumbing Run

A short local paper run was executed with `VENUE=paper_only` and temp runtime
dirs. It was not used to validate strategy edge; it only checked venue/feed,
session logging, and process plumbing.

Duration:

```text
2.2 minutes
```

Paper diagnostics:

| Metric | Value |
| --- | ---: |
| Diagnostics OK | true |
| Runtime errors | 0 |
| Fatal errors | 0 |
| Price snapshots | 8 |
| Signal evaluations | 156 |
| Orders placed | 0 |
| Avg cycle time | 0.17 ms |
| Max cycle time | 0.22 ms |
| Avg price staleness | 85.8 ms |
| Max price staleness | 231.0 ms |

No orders were expected in this short run because every signal was skipped by
the frozen strategy gates.

## Verification

Commands completed:

```text
cargo test
cargo build --release
polymomentum-engine pmxt-download
polymomentum-engine live-replay
polymomentum-engine harness-sweep --continuous
polymomentum-engine diagnostics session
polymomentum-engine preflight --mode paper
polymomentum-engine preflight --mode live --i-understand-live
polymomentum-engine live --mode paper
```

## Current Decision

The project is materially stronger after this pass:

- Backtest/live-replay mechanics are now feed-forward and nearly identical.
- Paper feed/session plumbing is healthy in a bounded run.
- Live mode remains fail-closed until explicit CLOB V2, reconciliation, alert,
  maker-order, venue, and wallet gates are satisfied.

Do not promote to real capital yet. The next required step is to rerun the
multi-window promotion gate with `harness-sweep --continuous` and the
order-keyed maker fill model, then create a new promotion artifact whose source
metrics were generated under the corrected mechanics.
