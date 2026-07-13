# PolyMomentum Algorithm And Evolution Audit - 2026-07-13

> Superseded by `docs/a_plus_evidence_2026-07-13.md`. The later report includes
> replay semantics v6, executable L2/FOK pricing, source-time CLOB freshness,
> the current evolution smoke, and the isolated forward-latency evidence.

## Executive Verdict

PolyMomentum has a strong offline research and execution shell, but it does not
currently have a strategy proven profitable enough for live trading.

- Infrastructure and fail-closed controls: **A-**
- Strategy evidence: **C+**
- Current state: **research-only, `live_ready=false`**

The historical candidate is profitable in aggregate, but its payoff geometry
is fragile: one full-loss outcome erases roughly six ordinary wins. Neither a
lower `max_price`, a contract-price anti-chase guard, nor the first evolved
selectivity policy survives both executable tail replay and the fresh window.

No live parameters were changed. Evolution remains offline and artifact-only.

## Scope And Evidence

This audit traced the strategy from exchange-price ingestion through momentum,
fair value, CLOB decisions, selectivity, simulated execution, settlement, fold
fitness, evolution, and promotion controls.

Evidence used:

- Historical 42-fold May 28 through June 10 replay reports.
- The known June 9 08:00-15:00 UTC loss cluster.
- The freshest available July 5 06:00-07:00 UTC forward capture.
- Binance one-second tape, PMXT L2 books, Gamma terminal outcomes, and the
  current live/replay code paths.
- Current Polymarket fee, CLOB V2, and RTDS documentation.

Assumption: Binance remains a causal research proxy. It is not accepted as
exact settlement evidence until the market's official settlement source and
timestamp have been captured and reconciled.

## Data Flow Audit

| Stage | Research/replay | Live | Verdict |
| --- | --- | --- | --- |
| Underlying price | Binance CSV | Multi-venue aggregate | Causal, but source parity is not exact |
| Momentum cadence | 1 Hz | 1 Hz after this audit | Fixed |
| Contract market data | PMXT L2 replay | Polymarket CLOB websocket | Same book semantics; live quality tags fixed |
| Volatility for fair value | One-hour Binance realized volatility | Deribit IV | Material parity gap remains |
| Outcome resolution | Gamma terminal plus BTC proxy | Venue/oracle lifecycle | Official settlement alignment remains unproven |
| Fees | Price-dependent crypto taker fee, maker zero | Dynamic CLOB market fee path | Formula matches current Polymarket docs |
| Execution | 128 ms latency, one-tick taker or maker model | Order manager and CLOB V2 | Taker L2 walk is diagnostic, not yet the fill model |

Current Polymarket references:

- https://docs.polymarket.com/trading/fees
- https://docs.polymarket.com/v2-migration
- https://docs.polymarket.com/market-data/websocket/rtds

## Critical Defects Fixed

### 1. Live Selectivity Did Not Match Replay

The harness populated `book_age` and `bookwalk_slippage` before selectivity,
but live and live-replay did not. The current candidate requires
`book_age=lte_100ms`, so the serialized policy would have rejected every live
decision as a missing tag.

Live, replay, and harness now attach the same book age, L2 walk, and causal path
inputs before applying selectivity.

### 2. Shadow Trades Bypassed Execution Gates

Settlement-alignment shadow handling ran before microstructure and maker
preflight. Shadow evidence could therefore credit trades that executable logic
would reject. Execution-quality gates now run first in live and live-replay.

### 3. EV Compared Price To An Uncalibrated Confidence Score

`min_ev_buffer` compared heuristic momentum confidence with contract price as
if confidence were a calibrated probability. It now compares Black-Scholes
binary fair value minus market price with the configured EV buffer.

The current candidate sets `min_ev_buffer=-1`, so this correction does not
improve its historical result by construction.

### 4. Binance Coverage Allowed Internal Outages

Coverage validation checked only the first and last timestamp. A tape with a
large internal hole could silently reuse a stale last price. Coverage now
measures median cadence and maximum internal gap and fails when the gap exceeds
the larger of five seconds or three normal intervals.

### 5. L2 Path History Was Slow And Inconsistent

Enabling path features initially scanned and shifted a five-minute vector on
every event. History now uses `VecDeque`, ten-Hz sampling matching live books,
constant-time eviction, and a reverse causal lookback. Replay returned to its
baseline speed.

### 6. Report-Native Evolution Misclassified Counterfactual Filters

The first evolution run labeled an added pressure filter as exact and scored it
32 wins, zero losses. Executable replay still entered the losing market one
second later after pressure changed, producing three wins, one loss, and
`-2.78` PnL in the tail fold.

This is a fundamental counterfactual issue: removing a recorded decision does
not prove that runtime will abstain for the rest of the market.

Evolution now marks any strategy-knob or runtime-selectivity change as
`report_counterfactual_requires_replay`. Such candidates cannot pass static
fitness or promotion gates.

## Math Audit

The binary fair-value formula is structurally correct:

```text
P(S_T > K) = N(d2)
d2 = [ln(S/K) + (r - sigma^2/2)T] / (sigma * sqrt(T))
edge = fair_value - executable_market_price
```

The current Polymarket crypto fee implementation also matches the documented
shape:

```text
fee = shares * fee_rate * p * (1 - p)
```

The dominant mathematical weakness is payoff asymmetry, not fee arithmetic.
At entry prices around `0.83-0.85`, a normal win earns about `0.74-0.90` after
fees while a wrong terminal outcome loses about `5.1`. A 98% historical win
rate can therefore still hide an unstable left tail. Tail CVaR, worst loss to
average win, payoff ratio, and loss bursts must stay ahead of total PnL in
fitness and promotion.

The volatility input is still model risk. Historical fair value uses causal
realized volatility while live uses Deribit IV. One source must be selected and
captured identically before historical fair-value evidence can represent live
decisions.

## Hypotheses Tested

### `max_price`

- Caps `0.81` through `0.85` retained the same known tail loss.
- Cap `0.80` produced no tail trades and removed the fresh winner.
- Verdict: rejected as an overfit tail fix.

### Contract Midpoint Anti-Chase

- Ten- and fifteen-second guards retained the known loss and removed a winner.
- A thirty-second guard removed the loss but retained only one tail winner.
- The thirty-second guard produced zero trades in the fresh July window.
- Verdict: useful causal search vocabulary, rejected as a current policy.

### Moderate Positive Book Pressure

- Report-native fitness showed 32 wins, zero losses.
- Exact tail replay re-entered later and produced 3 wins, 1 loss, `-2.78` PnL.
- Verdict: rejected; this replay exposed the counterfactual-fitness defect.

### Ten-Second Directional Binance Impulse

The fresh winner followed an opposite-direction impulse, while the known loss
followed a strong same-direction impulse. Exact harness labels also show winning
trades in the same `5_8` and `8_12` bps buckets as the loss.

- Verdict: retained as a causal feature for combinations, not a standalone
  gate.

Binary outcome overround is also logged as a causal data-quality feature. No
hard threshold was introduced.

## Evolution Mechanism Now Connected

`strategy-builder evolve-search` now provides a deterministic offline loop:

1. Seed from source variants, historical policies, and bounded path hypotheses.
2. Mutate executable strategy knobs and supported causal tags.
3. Rank with tail-first gates and non-dominated fronts.
4. Write stable generation, candidate, variant, ledger, and replay-manifest
   artifacts.
5. Treat all runtime counterfactuals as replay-only hypotheses.
6. Replay known tail clusters first, then full history, then freshest fully
   resolved captures.
7. Allow promotion only through robust promotion, zone audit, evidence export,
   registry mark, and registry audit.

The corrected 42-report dry run produced:

- Run ID: `evo_cf7f84d3cea40937`
- Candidates evaluated: `101`
- Passed candidates: `0`
- Result: `ok=false`
- Primary failure: `report_counterfactual_requires_replay`

The top replay hypothesis also produced zero trades on the fresh July window,
so the fail-fast cascade stopped before an expensive full-history replay.

## Remaining Blockers

1. **Official settlement parity:** capture the exact Chainlink or market-defined
   settlement observation and compare it with Gamma and the research proxy.
2. **Volatility parity:** use one causal volatility stream and estimator in
   harness, live-replay, and live.
3. **Executable taker price:** make L2 book walking the actual taker fill model,
   including insufficient-depth failure, rather than only a selectivity tag.
4. **Payoff geometry:** search lower-cost entries, maker execution, or a tested
   pre-settlement exit lifecycle. More high-price filters do not repair the
   underlying loss-to-win ratio.
5. **Fresh resolved sample size:** the July capture has one winning trade, which
   is useful but not promotion evidence, and its official settlement source is
   not aligned.
6. **Feature regeneration:** the 42-fold reports predate the new Binance impulse
   and outcome-overround tags. They must be regenerated before those dimensions
   can receive feed-forward fitness.

## Next Evidence Loop

1. Capture the freshest fully resolved PMXT window with Binance tape and the
   official settlement source.
2. Regenerate trade-feature reports with `btc_impulse_10s`,
   `outcome_overround`, book quality, and exact execution diagnostics.
3. Run deterministic evolution and export only replay manifests.
4. Replay top hypotheses against known tail clusters.
5. Run all 42 historical folds only for hypotheses that survive tails and the
   fresh window without collapsing trade count.
6. Keep `live_ready=false` until volatility parity, settlement alignment,
   shadow parity, and registry audit all pass.

## Acceptance Result

The system now fails honestly: no current candidate is promoted, rejected
thresholds remain rejected, runtime counterfactuals require replay, and the
evolution layer cannot mutate live parameters. That is the correct foundation
for finding a profitable strategy, but it is not proof that one exists yet.
