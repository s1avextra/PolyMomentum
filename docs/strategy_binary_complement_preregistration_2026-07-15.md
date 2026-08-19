# Binary-complement coherence pre-registration

Status: `PRE_REGISTERED_FORWARD_ONLY_NOT_SCORED_POWER_AND_REALIZED_SUPPORT_AMENDED`  
Strategy: `primary_v6_volfloor_300`  
Decision behavior changed: no  
Live behavior changed: no

## Blinded power amendment

At `2026-07-18T16:14:37Z`, before any forward opportunity or outcome metric
was generated, the block disclosure floor was strengthened from `100` to
`750` terminal settlement-aligned conditions. The strategy rule, latency,
baseline identity, and every rate threshold remain unchanged.

The historical nuisance rate is `102 / 631 = 16.16%` baseline candidates per
terminal condition, including `23 / 631 = 3.65%` baseline losses. A
`100`-condition block would therefore contain only about `16.16` candidates
and `3.65` losses. At that support, the Wilson gate generally requires a
nearly perfect selected record and the loss-removal rate moves in very large
single-observation steps. The executed-trade rate is a conservative proxy for
reconstructed screen eligibility because exposure and state gates can suppress
executions; the screen may therefore contain more candidates, not fewer.

At `750` conditions, the preregistered effect implies about `121.24` baseline
candidates, `27.34` baseline losses, and `103.65` selected candidates. Under
the historical nuisance rates, the chance of observing at least `100`
baseline candidates is `98.61%`, and the chance of at least `15` baseline
losses is `99.66%`. This is a fixed, outcome-blind sample-size strengthening,
not a threshold search or a response to forward results.

## Decision

The next genuinely new causal mechanism is a paired-outcome coherence gate.
It is now instrumented in the pre-edge opportunity report, but it is not a
runtime filter and has not been scored on the exhausted 42-fold history.

This boundary is deliberate. Chosen-token pressure, temporal OFI, static book
filters, midpoint runup, entry persistence, cross-venue paths, and probability
calibration have already failed. Retuning one of them would be another
in-sample search. The new feature instead uses information that those screens
did not contain: the simultaneous causal state of both outcome books.

## Structural basis

Polymarket documents that every binary Yes/No pair is fully backed by exactly
one dollar and can be split from or merged into one dollar of collateral.
Therefore the two outcome probabilities are complements. Polymarket also
documents that midpoint is normally the displayed probability, while actual
execution occurs at the bid or ask. See [Positions &
Tokens](https://docs.polymarket.com/concepts/positions-tokens) and [Prices &
Orderbook](https://docs.polymarket.com/concepts/prices-orderbook).

Queue imbalance can contain short-horizon price information, especially in
large-tick books, which supports comparing depth-weighted microprices rather
than midpoint alone ([Gould and Bonart,
2015](https://arxiv.org/abs/1512.03492)). Cross-impact evidence also cautions
against assuming that a high-dimensional contemporaneous cross-book model adds
value once within-book depth information is present; short-horizon lagged
effects are more defensible ([Cont, Cucuringu, and Zhang,
2021](https://arxiv.org/abs/2112.13213)). Finally, prediction-market liquidity
does not itself guarantee price efficiency ([Tetlock,
2008](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=929916)).

Those findings motivate one sparse uncertainty gate. They do not establish
that the gate is profitable on PolyMomentum; only unseen forward replay can do
that.

## Frozen feature contract

At each existing one-Hz pre-edge opportunity sample, use the current causal
three-level books for the chosen and opposite tokens:

```text
mid_chosen    = (best_bid_chosen + best_ask_chosen) / 2
mid_opposite  = (best_bid_opposite + best_ask_opposite) / 2

microprice = (best_ask * bid_depth + best_bid * ask_depth)
             / (bid_depth + ask_depth)

mid_residual   = mid_chosen + mid_opposite - 1
micro_residual = microprice_chosen + microprice_opposite - 1
```

Both books must be valid and present at the decision timestamp. Missing or
invalid state yields null features and fails closed. Both recorded book ages
must also be finite and no greater than the replay engine's existing `30 s`
freshness limit. No imputation is allowed.

The fixed gate is:

```text
max(abs(mid_residual), abs(micro_residual)) <= 2 * market_tick_size
```

The two-tick tolerance comes from venue price discretization. It was not
selected from historical winners or losses. No neighboring threshold will be
searched after seeing the forward result.

This differs from the existing `outcome_overround`, which is
`ask_up + ask_down - 1` and measures executable round-trip cost. The new
residual compares midpoint and depth-weighted probability consistency across
both books.

## Frozen forward screen

For each block, order rows chronologically by decision timestamp:

1. Require at least `750` terminal, official-source settlement-aligned
   conditions.
2. Require valid paired-book fields for at least `95%` of baseline edge-pass
   conditions.
3. Baseline selects the first capture row per condition that reconstructs the
   registered baseline's final edge pass: `evaluation_result=low_edge`,
   `decision.zone=primary`, and fee-adjusted `decision.edge >= 0.07`.
4. The mechanism selects the first such baseline-eligible row per condition
   that also passes the two-tick rule. Missing paired features reject that row.
5. Retain at least `70%` of baseline candidate conditions.
6. Retain at least `90%` of baseline terminal winners.
7. Remove at least `30%` of baseline terminal losses.
8. Achieve a condition-level 95% Wilson lower bound of at least `0.70`.
9. Normalize every selected condition to `$1` of entry cost at its recorded
   decision ask, apply its recorded Polymarket entry fee, and aggregate terminal
   unit PnL in chronological order.
10. Require fee-aware unit profit factor `>= 1.20` and payoff ratio `>= 0.20`.
    The scorer evaluates the equivalent finite margins `gross wins - 1.20 ×
    |gross losses| >= 0` and `average win - 0.20 × |average loss| >= 0`.

Failure of any criterion rejects the mechanism family. The rule must then be
repeated unchanged on a second disjoint block with at least `750` resolved
conditions. Passing the screen is not promotion: it only earns one exact
runtime variant and measured-latency replay.

That exact replay must still pass the full A+ contract, including positive
fee-inclusive PnL in both chronological halves, Wilson `>= 0.70`, at least
`20` profitable eligible reports, five-report loss burst `<= 2`, tail CVaR,
profit factor `>= 1.20`, payoff ratio `>= 0.20`, neighbor stability, and a fresh
registry audit with `live_ready=true`.

## Instrumentation

The capture adds these fields without changing decisions:

- `opposite_token_id`
- `market_tick_size`
- `chosen_best_bid`, `chosen_best_ask`
- `chosen_bid_depth`, `chosen_ask_depth`
- `chosen_book_age_ms`
- `opposite_best_bid`, `opposite_best_ask`
- `opposite_bid_depth`, `opposite_ask_depth`
- `opposite_book_age_ms`
- `chosen_microprice`
- `opposite_mid`
- `opposite_microprice`
- `complement_mid_sum_residual`
- `complement_microprice_sum_residual`

The exact paired quotes, visible depths, and per-token book ages are retained
for execution-path and data-integrity diagnostics. They are not inputs to the
frozen two-tick selection rule and do not change its pre-registered gates.

The frozen capture variant is
[`20260715_binary_complement_capture_variant.json`](../deploy/promotions/evidence/strategy_registry/20260715_binary_complement_capture_variant.json).
It preserves the baseline's non-edge gates and changes only its name and final
edge thresholds to an impossible `1.0`, which guarantees that the continuous
capture submits no trade and therefore does not truncate later opportunity
rows. The scorer requires its exact name, parameter hash, risk profile,
five-minute window, and a latency of at least the registered `202 ms` floor.

The report label `low_edge` is intentional: it describes the impossible
capture variant's final gate, not the registered baseline's `0.07` edge gate.
Rows labeled `negative_ev` or `edge_too_high_stale` remain ineligible.

Recorded segments are replayed with
[`deploy/replay-binary-complement-block.sh`](../deploy/replay-binary-complement-block.sh).
Before starting any CPU-heavy replay or writing any labeled opportunity file,
the wrapper independently counts unique post-registration terminal
official-source-aligned conditions and refuses the block below `750`.
It uses Binance RTDS as the causal signal tape, Chainlink as the settlement
tape, and the segment's measured latency with a `202 ms` floor. The converted
capture is supplied through `PMXT_DISTILLED_DIR` together with
`--require-shared-distilled`; missing, corrupt, or empty captured hours cannot
fall back to a sidecar, parquet, or network download.

Block 1 support is extended with
[`deploy/collect-binary-complement-floor.sh`](../deploy/collect-binary-complement-floor.sh).
After the 120-condition stability canary, it may collect at most 96 additional
24-window segments and stops only when `750` unique post-registration,
official-source-aligned terminal conditions are ready. The counter never reads
strategy wins, losses, paired-book residuals, retention, Wilson, opportunity,
or PnL fields. A condition contributes only from a fully sealed segment whose
status and resolution-manifest cardinality prove complete ready resolution and
positive distilled replay data; partial or rejected segments cannot inflate the
floor. Raw capture and the all-condition causal audit remain unchanged;
measurement v4 only restricts the converted replay cache to condition IDs that
the audit already admitted. This is storage and sample-support hardening, not a
strategy or threshold amendment.

## Immutable scorer

### Pre-score descriptive economic diagnostics

At `2026-07-18T18:05:51Z`, while support was still `12 / 750` and before any
forward opportunity report, screen artifact, or strategy metric existed, the
screen contract advanced from schema 2 to schema 3. The chronological strategy
rule, all six gates, both `750`-condition floors, and the exact-replay boundary
remain unchanged.

The reason is a measurement risk in the already-registered chronology: the
first row passing paired-book coherence may occur after the first baseline row
and may choose the opposite direction. That is causal behavior, but a terminal
classification screen can benefit from being closer to resolution while the
later top ask leaves little fee-inclusive payoff. Schema 3 therefore reports:

- baseline and selected decision-time top-ask distributions;
- selection-delay and selected time-to-close distributions;
- same-direction selected-minus-baseline ask changes;
- counts and rates for selections in the final 60 seconds;
- counts and rates for selected direction changes.

These fields are explicitly `descriptive_only_non_gating`. They cannot change
a block verdict, authorize or cancel the second registered block, replace exact
measured-latency replay, or justify a threshold change. Decision-time asks are
only executable-price proxies before latency, depth traversal, fills, and fees.
The repeat audit checks their count, rate, range, and ordering invariants.

Machine-readable amendment:
[`20260718_binary_complement_prescore_economic_diagnostics_amendment.json`](../deploy/promotions/evidence/strategy_registry/20260718_binary_complement_prescore_economic_diagnostics_amendment.json).

### Pre-score A+ economic alignment

At `2026-07-18T18:37:07Z`, support was still sealed at `12 / 750`: no forward
opportunity report, screen artifact, or strategy metric existed. A deeper audit
showed that the six schema-3 classification gates could all pass even when the
selected decision asks implied inadequate payoff. Counts do not preserve PnL
dollars.

Schema 4 therefore retains the two-tick rule and every classification threshold
unchanged but adds two fail-closed gates that mirror the already-existing A+
economic floors. For each selected row it prices exactly `$1` of entry cost at
the recorded ask, applies the row's recorded entry fee, and computes terminal
unit PnL. The block must have unit profit factor `>= 1.20` and payoff `>= 0.20`.
It also publishes chronological-half unit PnL for diagnosis.

This is an optimistic decision-time economic screen, not exact replay. It does
not model the registered latency, depth traversal, fill failure, stateful sizing,
breadth, or tail clustering, so a pass cannot establish profitability or A+.
Failure rejects the mechanism without retuning. The second disjoint block and
exact measured-latency replay remain mandatory.

Machine-readable amendment:
[`20260718_binary_complement_prescore_unit_economics_amendment.json`](../deploy/promotions/evidence/strategy_registry/20260718_binary_complement_prescore_unit_economics_amendment.json).

The forward block is scored once with no threshold arguments:

```bash
./rust_engine/target/release/polymomentum-engine strategy-builder \
  binary-complement-screen \
  --opportunity local-forward/block-1/opportunities.json \
  --resolution-manifest local-forward/block-1/resolution_manifest.json \
  --block-id binary-complement-block-1 \
  --output local-forward/block-1/binary_complement_screen.json
```

Both file flags may be repeated for segmented evidence. The scorer filters
terminal manifests to the opportunity-report time windows and refuses to
reveal partial metrics before at least `750` post-registration aligned
conditions exist; this prevents sequential peeking. It recomputes row wins
from terminal manifest directions and rejects any capture identity, timestamp,
latency, or label mismatch. Exit `1` means the frozen mechanism failed; exit
`2` means the inputs or provenance contract were invalid.

If and only if both blocks pass independently, verify the repeat contract:

```bash
./rust_engine/target/release/polymomentum-engine strategy-builder \
  binary-complement-repeat-audit \
  --screen local-forward/block-1/binary_complement_screen.json \
  --screen local-forward/block-2/binary_complement_screen.json \
  --output local-forward/binary_complement_repeat_audit.json
```

The two screens must be supplied in chronological order. The audit rederives
every rate and gate from the recorded counts, validates the frozen capture and
baseline identities, condition-set hashes, rule, and verdicts, then requires
non-overlapping report windows and zero shared condition IDs. Every opportunity
report and resolution manifest is pinned by SHA-256; the repeat audit rereads
the sources and rejects mutable-input drift. Only
`TWO_BLOCK_SCREEN_PASS_EXACT_REPLAY_ALLOWED` permits materializing one exact
runtime gate. That status still does not authorize live or paper trading.

Machine-readable contract:
[`20260715_binary_complement_coherence_preregistration.json`](../deploy/promotions/evidence/strategy_registry/20260715_binary_complement_coherence_preregistration.json).

Until two unseen blocks and exact replay pass, the verdict remains
`KEEP_REPLAY_RESEARCH`, grade A-, and `LIVE_TRADING_OFF`.

## Pre-score V2 live-reconciliation parity amendment

At `2026-07-21T04:18:00Z`, the fixed first block was still sealed at
`290 / 750`; no opportunity report, strategy score, or forward outcome metric
had been generated. A protocol audit found that the future live path still
trusted a legacy WebSocket fee field, ignored current nested Gamma fee/tick
fields, sent the wrong authenticated heartbeat, applied aggregate taker
economics to managed maker legs, and booked nonterminal matched trades before
later status updates.

Measurement v9 retains the unchanged schema-4 scorer and frozen strategy rule.
The future live path now snapshots the current effective entry fee with each
pending order, computes the official five-decimal fee only from a terminal
confirmed fill, uses each maker order's own matched leg, sends literal `PING`,
and fails closed on invalid fee or fill inputs. The production binary and active
measurement-v5 capture were not replaced or restarted.

This amendment does not authorize live trading. Automated authenticated REST
startup/reconnect recovery remains unimplemented, so live promotion is blocked
even if both forward blocks and exact replay eventually pass.

Machine-readable amendment:
[`20260721_binary_complement_live_reconciliation_parity_amendment.json`](../deploy/promotions/evidence/strategy_registry/20260721_binary_complement_live_reconciliation_parity_amendment.json).

## Pre-score authenticated REST-recovery parity amendment

At `2026-07-21T04:46:59Z`, the first block remained sealed at `290 / 750` and
no strategy score had been disclosed. Measurement v10 closes the remaining
live-reconciliation boundary without changing the frozen rule, schema-4 scorer,
capture format, or any gate.

The runtime now computes and journals the V2 EIP-712 order hash before sending
the HTTP order request. Nonterminal lifecycle state, remaining position
economics, fee schedule, recovery miss count, and processed trade ids survive a
restart in SQLite. Live startup restores that exposure and blocks new orders
until authenticated trade and order recovery succeeds. Confirmed trades remain
the only financial source; terminal order `size_matched` may prove missing
evidence but can never supply fill price or fees. Ambiguous submissions remain
locked for recovery, while only a submitted zero-fill hash with three REST 404s
after 30 seconds may be cleared as never accepted.

Measurement v10 is installed separately at
`/opt/polymomentum/tools/polymomentum-engine-measurement-v10` with SHA-256
`03073aefcad8f5a403e55e5a472d81a915632c131db3c3a241d326845e08585f`.
Production and the active measurement-v5 collector were not restarted or
replaced. This closes the automated recovery implementation blocker, but it
does not authorize live trading: two disjoint 750-condition screen passes and
exact measured-latency replay must still pass every registered A+ gate.

Machine-readable amendment:
[`20260721_binary_complement_rest_recovery_parity_amendment.json`](../deploy/promotions/evidence/strategy_registry/20260721_binary_complement_rest_recovery_parity_amendment.json).

## Pre-score realized-support amendment

At `2026-07-21T05:44:36Z`, the first block remained sealed at disclosed support
`290 / 750`; no opportunity report, forward strategy metric, or screen artifact
had been generated. A scorer audit found that schema 4 treated the blinded power
analysis's realized support as an expectation rather than a hard verdict gate.
An unusually sparse but nearly perfect sample could therefore pass the ratio,
Wilson, and normalized-economic gates without the decision-useful evidence that
motivated the 750-condition floor.

Schema 5 retains the frozen two-tick rule and every rate and economic threshold.
It adds three fail-closed realized-support gates derived only from evidence fixed
before forward scoring:

- at least `100` baseline candidates;
- at least `15` baseline losses;
- at least `80` selected candidates.

The first two are the exact support levels used by the blinded power amendment;
the third applies the standing A+ minimum-80-trades policy to the selected rows
that feed Wilson and unit economics. A sparse block is rejected at the fixed
score. It cannot authorize adaptive collection or threshold changes.

Measurement v11 is installed separately at
`/opt/polymomentum/tools/polymomentum-engine-measurement-v11` with SHA-256
`07a386c748d3756462cb3a654c5999936b929f9be93f8190ebbfb80c34f00b89`.
The active measurement-v5 collector remained on PID `289160` with zero restarts,
and production continued to execute `/opt/polymomentum/polymomentum-engine`.

Machine-readable amendment:
[`20260721_binary_complement_realized_support_amendment.json`](../deploy/promotions/evidence/strategy_registry/20260721_binary_complement_realized_support_amendment.json).
