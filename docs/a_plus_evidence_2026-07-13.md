# PolyMomentum A+ Evidence And Readiness Verdict - 2026-07-13

## Executive Verdict

"A+" is interpreted as engineering and validation quality, not a guarantee of
profit. Under that standard, the strategy lab is materially stronger but the
trading strategy is not A+ and is not ready for live capital.

| Layer | Grade | Current result |
| --- | --- | --- |
| Offline evolution and promotion safety | A | Deterministic, replay-first, artifact-only, fail-closed |
| Replay and execution correctness | A- | Visible L2/FOK semantics, causal source clocks, fees, settlement, and risk aligned |
| Strategy proof | D / insufficient | One current-semantic tail winner and two silent folds are not statistical evidence |
| Live readiness | BLOCKED | No candidate passed promotion; measured latency and fresh OOS proof are incomplete |

The operational verdict remains `live_ready=false`. No live configuration,
promotion registry, or deployed strategy was changed.

## Assumptions And Scope

- Binance is a causal momentum/reference proxy, not the official settlement
  source.
- Chainlink RTDS is captured separately and is required for settlement-aligned
  evidence.
- Evolution is offline research evolution. It never mutates live parameters.
- Paper mode is not strategy validation. All strategy proof must come from
  feed-forward replay, current semantics, and fully resolved outcomes.
- The full historical cascade stops when an earlier tail or data gate fails.
  This prevents a large search from laundering a known blocker through total
  PnL.

## Weaknesses Eliminated

### Causality And Data Streams

- BTC sampling now advances globally at one Hz before per-market gates, so a
  traded/throttled market cannot alter future momentum history.
- Binance and Chainlink are isolated reference streams with observation-time
  freshness, internal-gap checks, provenance, and out-of-order rejection.
- Live volatility now uses the same causal one-hour realized-volatility
  estimator as replay; the live-only Deribit multiplier was removed.
- Historical Gamma prices are no longer accepted as an L2 or terminal-price
  fallback. Resolution requires terminal outcomes or explicit settlement
  evidence.
- The RTDS recorder independently watches Chainlink and Binance and reconnects
  when either relevant stream stops, even if the socket remains open.
- Live CLOB state now uses the exchange event timestamp rather than local
  receipt time. Missing, invalid, older, and future-freshness frames fail
  closed, so a stale reconnect snapshot cannot overwrite or refresh a newer
  book.

### Math, Execution, And Risk

- Taker fills walk visible asks only, enforce a rounded FOK limit, and reject
  insufficient depth. Perfect fills and invented residual liquidity cannot be
  promoted.
- Budget sizing maximizes executable shares while reserving worst-case FOK
  cost. High-price dust cannot raise the limit on an otherwise low-price order.
- A taker decision is repriced at executable VWAP and the effective entry fee,
  then reruns max-price, EV, stale-edge, and zone-edge gates.
- Maker/taker selection is consistent between decision fees, order intent, and
  actual execution. The old invented live slippage adjustment was removed.
- Decision edge is `fair_value - executable_vwap - effective_entry_fee`.
  Non-finite inputs fail closed.
- Position sizing uses promoted fractions, stressed-drawdown headroom, visible
  depth, and worst FOK cost.

### Evolution And Promotion

- `strategy-builder evolve-search` writes deterministic genomes, candidates,
  generations, exact variants, ledgers, and dry replay manifests.
- Strategy-knob or selectivity mutations receive no report-native promotion
  credit. They are marked as counterfactual hypotheses until exact replay.
- Fitness is tail-first: gates, loss bursts, worst fold, CVaR, loss asymmetry,
  payoff, profit factor, Wilson lower bound, support, expectancy, then PnL.
- Promotion rejects legacy replay semantics, synthetic/perfect execution,
  unresolved fills, thin samples, bad tails, unstable parameter neighborhoods,
  and incomplete data.
- Current replay semantics are v6:
  `max_share_budget_optimized_visible_l2_bookwalk_with_fok_limit`.

## Current-Semantic Tail Evidence

The June 9 08:00-15:00 UTC tail replay covers 96 markets and 192 tokens at the
previous 1,030 ms latency assumption.

| `max_price` | Trades | Wins | Losses | PnL | Fees |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0.80 | 0 | 0 | 0 | 0.00000 | 0.00000 |
| 0.85 | 1 | 1 | 0 | 0.89602 | 0.05598 |
| 0.90 | 1 | 1 | 0 | 0.89602 | 0.05598 |

Artifacts:

- `/private/tmp/polymomentum_a_plus_20260713/tail_v6_1030ms_report.json`
- `/private/tmp/polymomentum_a_plus_20260713/tail_v6_1030ms_mp080_report.json`
- `/private/tmp/polymomentum_a_plus_20260713/tail_v6_1030ms_mp090_report.json`
- `/private/tmp/polymomentum_a_plus_20260713/tail_v6_1030ms_max_price_neighbors_report.json`

The current binary's robust gate rejects all three points. Caps 0.85 and 0.90
have exact `max_price` neighbor positive rate `0.5000`, below the required
`0.6000`; cap 0.80 fails for zero trades. Therefore 0.85 is not proven to be a
stable plateau and cannot be declared non-overfit. A single lossless trade also
cannot estimate CVaR, payoff asymmetry, or loss-burst behavior.

This replay is research context only because the fresh latency audit below does
not accept 1,030 ms as the current promotion assumption.

## Evolution Smoke

Run `evo_59da5b9398ffe9c2` used three chronological v6 reports:

| Fold | Window UTC | Trades | Result |
| --- | --- | ---: | ---: |
| 1 | 08:00-09:00 | 0 | 0.00000 |
| 2 | 10:00-11:00 | 0 | 0.00000 |
| 3 | 12:00-15:00 | 1 | 0.89602 |

The run evaluated 24 candidates and passed zero. Every candidate failed OOS
trade count, profitable-report support, eligible-report support, Wilson lower
bound, and prior-train availability. Mutated counterfactuals additionally
require exact replay.

The candidate manifests are dry-run only (`execute=false`), reference exact
`variant.json` files, include `--atomic-parquet`, and preserve the requested
latency. Static fitness did not alter any registry or runtime state.

Artifact:
`/private/tmp/polymomentum_a_plus_20260713/evolution_v6_tail_smoke/evolution_summary.json`

## Fresh Feed And Latency Evidence

An isolated 780-second dev-host capture produced:

- Chainlink: 764 ticks, no reconnect, maximum observation gap 8,000 ms.
- Binance: 775 ticks, no reconnect, maximum observation gap 2,000 ms.
- RTDS source provenance: ready and fresh at capture end.
- CLOB: 312,799 frames, 294,203 timestamped book/change events, one read-error
  reconnect, and two connected sessions.
- Overall CLOB receive gap: 3,126 ms.
- Largest active-token continuity gap: 52,309 ms, above the 2,000 ms gate.

Observed CLOB source-to-receipt delay:

| Percentile | Delay ms |
| ---: | ---: |
| p50 | 55 |
| p90 | 630 |
| p95 | 3,586 |
| p99 | 13,378 |
| max | 48,462 |

The audit verdict is `CLOB_P99_DELAY_TOO_HIGH`, `ready=false`, with recommended
retest latency 13,378 ms. This is a dev-host diagnostic, not an accepted VPS
latency profile, but it is enough to block using 1,030 ms as current promotion
proof.

`record_overhead_ms.p99=0` measures processing only up to the pre-write stamp.
It does not include JSON serialization, buffered write, flush, or disk latency,
so it must not be presented as total recorder overhead.

The separate concurrent-load capture reported p99 4,451 ms and is discarded as
promotion evidence because backtests were running during measurement.

Artifacts:

- `/private/tmp/polymomentum_a_plus_20260713/postfix_rtds_health_13m_clean/summary.json`
- `/private/tmp/polymomentum_a_plus_20260713/postfix_rtds_health_13m_clean/forward_latency_audit.json`

## Remaining Promotion Blockers

1. Capture at least 75-90 isolated minutes on the actual VPS with Chainlink,
   Binance, and CLOB health checks active and no CPU-intensive research load.
2. Accept a measured VPS latency profile. Do not substitute the dev capture or
   the old 1,030 ms assumption.
3. Replay the known tail clusters under v6 at that accepted latency. Stop if
   trade support, payoff, CVaR, or loss-burst gates fail.
4. Only after tail survival, regenerate the complete 42-fold May 28-June 10
   history under v6 and the same latency policy.
5. Validate on the freshest fully resolved PMXT/forward windows with Chainlink
   settlement alignment and complete internal coverage.
6. Feed only current-semantic chronological reports back into evolution and
   replay every runtime counterfactual.
7. Require robust promotion, zone audit, durable evidence export, registry
   mark, and registry audit before `live_ready` can become true.

## Verification

- `cargo test --manifest-path rust_engine/Cargo.toml`: 308 library tests and
  420 binary tests passed.
- `cargo clippy --manifest-path rust_engine/Cargo.toml --all-targets -- -D warnings`:
  passed.
- `cargo build --release --manifest-path rust_engine/Cargo.toml --bin polymomentum-engine`:
  passed.
- Current binary robust-promotion diagnostic: rejected all `max_price`
  neighbors and wrote no promotion artifact.

## Final Acceptance Result

The self-evolution mechanism is connected and safe enough to generate and
replay hypotheses without touching live state. The current candidate is not
proven profitable, `max_price=0.85` is not proven robust, latency is not
promotion-ready, and the full current-semantic proof cascade has not passed.

Correct action: keep live trading off and continue from the isolated VPS
latency capture, not from another parameter search.
