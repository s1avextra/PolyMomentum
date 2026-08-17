# Strategy-finding architecture v3: opportunity table first

## Decision

Stop expanding the late-window parameter grid through repeated engine replays.
The next research system should extract each causal, executable market
opportunity once, evaluate many simple policies over that immutable table, and
reserve exact L2 replay for one representative of each unique decision trace.

This does not weaken fresh validation. It moves expensive execution simulation
later, after a candidate has shown support and fee-aware value on data that can be
reused safely.

## Step 1 implementation status — 2026-08-09

The bounded exporter is implemented as `strategy-builder opportunity-table`.
It accepts one UTC hour, one strict causal JSONL signal file, and an already
cached PMXT directory. It refuses to download data, rejects unknown signal
fields (including outcome/label fields), applies the PMXT condition filter in
the parquet reader, walks the resulting events once, and writes an atomic
PolyMomentum-owned Parquet table plus a JSON provenance manifest.

The input contract is one JSON object per line with exactly these fields:

```json
{
  "condition_id": "0x...",
  "token_id": "...",
  "chronological_window": "recent_discovery",
  "window_start": "2026-08-01T12:00:00Z",
  "market_close": "2026-08-01T12:05:00Z",
  "observed_at": "2026-08-01T12:03:00Z",
  "signal_direction": "up",
  "strike_price": 115000.0,
  "btc_open": 114800.0,
  "btc_60s": 114900.0,
  "btc_120s": 115050.0,
  "btc_180s": 115150.0,
  "btc_240s": null,
  "btc_observed": 115150.0,
  "causal_volatility": 0.0012
}
```

A checkpoint is rejected if its offset is later than `observed_at`. The output
contains causal 2/3/4-minute features, distance to strike, exact causal
`TokenBook` top/depth state, stake book-walk capacity, taker fees, break-even
probability, and loss-recovery economics. It contains no resolution, candidate
score, retention, realized payoff, or PnL column.

Example:

```bash
rust_engine/target/release/polymomentum-engine strategy-builder opportunity-table \
  --hour 2026-08-01T12:00:00Z \
  --signals logs/strategy-research/causal-signals/2026-08-01T12.jsonl \
  --cache-dir data/pmxt_v2_cache \
  --output logs/strategy-research/opportunities/opportunity_table_v1/2026-08-01T12.parquet \
  --manifest logs/strategy-research/opportunities/opportunity_table_v1/2026-08-01T12.manifest.json
```

The manifest pins the signal, source parquet, and output hashes and explicitly
records `outcome_columns_present=false` and
`single_pmxt_rowfiltered_scan=true`. Unit tests cover future-checkpoint and
outcome-field rejection, exact causal book/fee parity, deterministic opportunity
IDs, deterministic Parquet bytes, and CLI bounds.

## Evidence for the shift

The bounded 2026-08-08 tests separated historical signal quality from fresh
executability:

| Entry cap | Historical exact replay | Fresh frozen replay | Decision |
|---|---:|---:|---|
| 0.95 | 8 windows, 34 signals, 9 fills, +$4.28441 | 4 windows, 4 signals, 0 attempts, 0 fills | insufficient fresh support |
| 0.90 | 8 windows, 34 signals, 6 fills, +$3.16499 | 4 windows, 4 signals, 1 failed attempt, 0 fills | insufficient fresh support |

Both fresh jobs stopped outcome-blind when the five-fill floor became
mathematically unreachable in the remaining fixed budget. Neither candidate is
eligible for fixed-forward, paper, or live execution.

The failure is informative: the current public rule can predict direction on
selected historical events, but it does not reliably produce a buyable contract
at the requested late-window price on the freshest resolved data. Searching more
minor variants of the same rule with full replay is therefore the wrong unit of
work.

## Target architecture

### 1. Immutable causal opportunity table

Build one row per signal-time contract opportunity. PMXT must be filtered at the
parquet layer and scanned once per hour. Each row should include:

- stable `opportunity_id` derived from source hashes, condition/token IDs, and
  causal observation time;
- condition, token, direction, window start, observation time, and remaining
  seconds;
- BTC path and displacement at 2, 3, and 4 minutes;
- distance to strike, causal volatility, and book-pressure features;
- executable best ask, spread, depth at stake, fee estimate, and latency-adjusted
  price;
- source/semantics hashes and observability flags.

Resolution and realized payoff belong in a separately joined label table. Fresh
window selection sees causal fields and support counts only; it must not read the
joined outcome columns.

### 2. Vectorized policy evaluator

A strategy becomes a small predicate plus an economic rule over the opportunity
table, for example:

```text
path_minutes >= 3
and abs(move_2m_usd) >= 100
and direction == path_direction
and executable_ask <= 0.90
and conservative_probability > fee_aware_break_even + safety_margin
```

All allowed path, move, price, direction, distance-to-strike, time-remaining, and
volatility combinations are evaluated in one pass. This is the cheap discovery
layer; it reports support, calibration, executable opportunity count, and
fee-aware expected value without pretending to prove fills.

Probability should be estimated only from chronological training rows using a
small calibrated table such as:

```text
P(win | move bucket, distance-to-strike bucket, remaining-time bucket,
        direction, volatility bucket)
```

Use a conservative lower bound. Trade only when that lower bound exceeds the
actual executable break-even probability plus a fixed margin. Prefer monotone,
coarsely bucketed models over a large optimizer.

### 3. Decision-trace equivalence

For each candidate and partition, hash the ordered accepted `opportunity_id` list
together with execution-relevant parameters (side, price cap, stake, latency,
fee model, and semantics version). Candidates with the same hash are execution
equivalent for that partition.

Only one representative of an equivalence group receives exact L2 replay. The
remaining variants link to the representative result. A summary-statistics hash
is not sufficient: reuse is allowed only when the accepted opportunity IDs and
execution parameters are identical.

### 4. Exact replay and fresh validation

The expensive stages remain strict:

1. exact historical L2 replay for unique decision traces only;
2. newest fully resolved frozen holdout selected without outcomes;
3. fee-aware exact economics;
4. future-only fixed-forward confirmation;
5. official-resolution parity;
6. bounded zero-order VPS shadow for production wiring only.

The `signal_to_attempt_rate` remains visible throughout. In historical and fresh
replay it is diagnostic because minimum fills, active-window coverage, fill rate,
and economics already determine whether the strategy is useful. It remains a
hard operational gate at the fixed-forward checkpoint.

## Storage contract

Suggested paths:

```text
logs/strategy-research/opportunities/<semantics>/<window>.parquet
logs/strategy-research/opportunity-labels/<resolution-source>/<window>.parquet
logs/strategy-research/policy-screens/<snapshot-hash>/<policy-hash>.json
logs/strategy-research/decision-traces/<partition>/<trace-hash>.json
```

Writes must be atomic. Source and feature-schema hashes are mandatory. Shared
PMXT parquet ownership rules remain unchanged; the opportunity table is a
PolyMomentum-owned derived cache.

## Migration plan

1. **Complete:** add a measurement-only `strategy-builder opportunity-table` command that
   emits causal rows plus a manifest for one bounded window. Verify row counts,
   hashes, causal timestamps, and parity with the existing public evaluator.
2. Build the newest 30 fully resolved days on the dev Mac, one parquet hour per
   pass. Add the outcome table only after the causal table is sealed.
3. Replace the current candidate-by-candidate cached screen with one vectorized
   late-window family evaluation. Emit decision-trace hashes and collapse
   equivalent variants before exact replay.
4. Add the conservative calibration table and compare it with the fixed rule on
   chronological train/holdout splits.
5. Retire repeated deterministic evolution when its input hash is unchanged;
   later retire it entirely if opportunity-table search dominates it on fresh
   support and compute cost.

## Acceptance criteria for v3

- one source scan evaluates the complete bounded policy grid;
- at least 80% of execution-equivalent variants are removed before exact replay;
- no outcome field is accessible during fresh-window selection;
- exact replay count is proportional to unique decision traces, not parameter
  combinations;
- every promoted candidate still passes fresh resolved and future-only stages;
- the VPS performs capture/runtime work only; search stays on the dev Mac.

## Implementation status — 2026-08-10

The v3 discovery path is now implemented end to end:

1. `opportunity-signals` compiles strict outcome-free 120/180/240-second signals
   from the physical causal snapshot. Gamma is projected to condition/token
   identity only; changing terminal Gamma prices does not change the signal
   output or identity hash.
2. `opportunity-table` scans one cached PMXT hour once with a condition-level
   parquet filter and writes causal book, depth, fee, path, displacement,
   distance, pressure, and volatility features. It contains no resolution or
   realized-PnL field.
3. `opportunity-dataset-seal` hash-pins any number of hourly manifests and
   tables, verifies stable causal semantics/stake/fee settings, unique
   opportunity IDs, and outcome-free schemas, and writes an immutable index.
4. `opportunity-labels` joins labels only after the seal. It physically excludes
   every `fresh_holdout` row and records the exclusion count and
   `fresh_holdout_labels_present=false` in its manifest.
5. `opportunity-policy-search` builds the fixed 7,290-policy grid once, calibrates
   only on `older`, evaluates only on `recent_discovery`, and reads only causal
   support for `fresh_holdout`. Minimum support, positive point edge, and positive
   fee-aware payoff can nominate research-only replay; Wilson confidence remains
   a later advancement gate.
6. The same pass hashes ordered accepted opportunity IDs and the execution cap,
   stake, latency, fee rate, side, and feature semantics. It emits one exact-L2
   plan entry per unique discovery trace. A synthetic 100-policy equivalence test
   collapses 100 candidates to one replay (99% reduction).

The local coordinator now routes its default cycle to this content-addressed
policy-search lane. It does not refresh the legacy public/LLM lane, does not run
legacy candidate replay, and skips evaluation entirely when the seal, label
manifest, and settings hash are unchanged. The installed LaunchAgent remains a
one-shot 30-minute research timer and has no paper/live/deploy command in its
allowlist.

### Real bounded pilot

The real 2026-05-25 15:00 UTC pilot contains 36 causal opportunities from 12
markets at the three decision times. Thirty-five rows have an observable and
fully executable $5 book; one is fail-closed as `invalid_top_of_book`. The
separate label table contains 36 historical proxy labels and zero fresh labels.

The policy pass reads the one causal table once, evaluates 7,290 policies, and
returns `no_candidate_survived_discovery`: the pilot has 36 `older` calibration
rows but intentionally has no `recent_discovery` or `fresh_holdout` partition.
It therefore validates mechanics and leakage boundaries, not strategy quality,
and correctly schedules zero exact replays.

Durable pilot artifacts live under
`deploy/promotions/evidence/strategy_registry/source_snapshots/opportunity_table_v1_pilot/`;
the search report is
`deploy/promotions/evidence/strategy_registry/20260810_opportunity_policy_search_pilot.json`.

## Chronological minimum and exact replay — 2026-08-11

The first non-pilot dataset is sealed from twelve causally selected PMXT hours:
four older hours on 2026-04-16, four recent-discovery hours on 2026-07-25, and
four freshest fully resolved holdout hours on 2026-08-08. It contains 420 unique
opportunities. The separate label table contains 287 older/discovery labels and
physically excludes all 133 fresh rows. Fresh outcomes were not accessed.

Four attempted June 26–27 hours were rejected before strategy scoring because
PMXT contained zero events for both the exact twelve target conditions and a
broader 348-market Gamma catalog. Coverage checks recovered on July 25. The
opportunity-table command now fails fast on such zero-target-event hours instead
of silently converting an upstream archive gap into strategy evidence.

The discovery architecture is now staged:

1. older rows establish minimum coarse-cell support;
2. recent-discovery point edge and fee-aware payoff can nominate research-only
   exact replay before Wilson confidence is available;
3. execution-equivalent variants collapse by ordered opportunity trace;
4. at most two traces, preferably at different decision times, receive replay;
5. Wilson edge above fee-aware break-even remains required before any fresh
   outcome gate can open.

The 7,290-policy pass found 132 research-eligible variants but only 44 unique
traces. The hard cap selected two and avoided 130 exact replays (98.48% of
eligible variants):

- 120 seconds, maximum ask 0.85: 20/20 fills at 128 ms, 16 wins, 4 losses,
  +$31.72984, point edge +0.14350, Wilson edge -0.07252;
- 240 seconds, maximum ask 1.00, down only: 23/23 fills, 22 wins, 1 loss,
  +$24.19572, point edge +0.12545, Wilson edge -0.04099.

Both traces retain a research signal and neither is promotion-ready. All four
discovery PMXT hours were scanned once for the union of both traces, avoiding four
duplicate hour scans. The next step is to add preselected discovery hours from
multiple days until the replayed Wilson edge can make a real decision. If it
remains non-positive, reject the trace without opening fresh outcomes. If it
clears the fixed margin, freeze the policy and run the already sealed fresh gate
exactly once.

## Bounded discovery expansion and final decision — 2026-08-11

Before reading any additional Gamma identity, PMXT content, labels, or outcomes,
the expansion preregistered eight distinct recent-discovery hours from July
17–24 using a fixed day/hour calendar rule. The resulting 20-hour seal contains
704 unique causal opportunities: 144 older, 427 recent-discovery, and 133 fresh.
The separate label table contains 571 labels and still physically excludes all
133 fresh rows.

The fixed 7,290-policy family produced 120 eligible variants and 40 unique
decision traces. Equivalence collapse plus the hard two-trace cap avoided 118
replays (98.33%). Exact replay scanned each of the 12 discovery PMXT hours once:

- 180 seconds, maximum ask 0.90: 28/28 fills, 23 wins, 5 losses, +$27.53992,
  point edge +0.10544, Wilson edge -0.07190;
- 120 seconds, maximum ask 0.90: 28/28 fills, 20 wins, 8 losses, +$28.13791,
  point edge +0.08390, Wilson edge -0.10098.

The positive point estimates show that the hypothesis is worth having tested,
but neither trace crossed the preregistered Wilson edge margin of +0.02. The
bounded reject condition therefore applies: both traces are retired from
promotion work, no more hours or variants may be added for them, and fresh
outcomes remain unopened. The durable decision is
`deploy/promotions/evidence/strategy_registry/20260811_opportunity_discovery_expansion_bounded_decision.json`.

## Next architecture shift: hypothesis portfolio with explicit budgets

The opportunity-table pipeline is now fast enough; the remaining bottleneck is
hypothesis selection and stopping discipline. The next version should organize
research as a small portfolio of independent, preregistered mechanism families:

1. Each family declares its causal thesis, fixed feature subset, maximum policy
   count, maximum discovery hours, maximum exact traces, and advancement/reject
   gates before labels are joined.
2. A cheap causal screen evaluates every family on the same sealed table and
   allocates exact replay only to distinct decision traces with positive
   fee-aware point economics.
3. One bounded exact replay produces a terminal family decision: reject, freeze
   for one fresh test, or mark data-quality-blocked. `more_evidence_required`
   cannot silently expand the budget.
4. Rejected families stay in a tombstone registry keyed by causal thesis and
   trace hashes, preventing cosmetic variants from restarting the same search.
5. Only a frozen winner may open the newest resolved holdout once; official
   settlement parity and future-only confirmation remain later gates.

The first new family should test a genuinely different mechanism rather than a
minor threshold change: market-implied mispricing versus an external causal
probability estimate, cross-asset lead/lag, or liquidity/pressure dislocation.
Each starts with a cheap support/calibration pass and earns exact L2 replay only
after passing its declared economics gate.

## First budgeted portfolio family — 2026-08-11

The first independent family is implemented as
`opportunity-probability-search`. It deliberately excludes the prior path,
move, and pressure predicates. For each UP or DOWN token it computes a causal
Black-Scholes binary terminal probability from observed BTC, strike, remaining
seconds, and annualized realized volatility, then selects only rows where that
probability exceeds the executable fee-aware break-even price by a fixed model
margin.

The preregistered grid contains 54 policies: three decision times, three fixed
volatility scales, three model-edge floors, and two price caps. Older selected
rows must provide at least 20 observations with Brier score at most 0.25;
recent discovery must provide at least 20 observations, point edge above 0.02,
and positive fee-aware payoff. At most two distinct traces receive 128 ms exact
replay.

The cheap screen produced 27 eligible distinct traces and selected one at 180
seconds and one at 120 seconds. Exact results were:

- 180 seconds, maximum ask 0.85: 32/32 fills, 25 wins, 7 losses, +$48.00168,
  point edge +0.17152, Wilson edge +0.00272;
- 120 seconds, maximum ask 0.90: 37/37 fills, 26 wins, 11 losses, +$33.52831,
  point edge +0.07720, Wilson edge -0.08333.

The first trace is materially closer to confidence than the retired directional
family, but it still misses the fixed +0.02 Wilson advancement margin. The new
`opportunity-probability-decision` command validates preregistration, provenance,
budget, replay count, and outcome isolation, then emits a terminal decision. It
returned `reject_family_keep_fresh_sealed`, `more_evidence_allowed=false`, and
`fresh_gate_opened=false`; positive point PnL cannot extend the experiment.

The next portfolio family should therefore use a different source of
information, not another probability-model scale. The lowest-data-cost option
is a pure liquidity dislocation family based on causal spread/depth imbalance
and short-lived cross-token book inconsistency, with the same 54-or-smaller
cheap grid, two-trace replay cap, and terminal decision contract.

## Second budgeted portfolio family: paired-book liquidity — 2026-08-11

The second independent family is now implemented end to end. The old causal
opportunity tables contain only the token selected by BTC direction, so they
could not honestly supply the second book. The new `opportunity-pair-features`
command replays both complementary books at each sealed condition/timestamp
coordinate and at a causal 15-second lookback. It hash-verifies and scans each
of the 20 source PMXT hours once and writes a compact outcome-free cache. Gamma
outcome prices, BTC features, model probabilities, labels, and PnL do not enter
the paired feature values or policy selection. The cache contains 704 rows;
both current books are observable on 685 rows and both lookback books on 690.

The outcome-free coverage pass showed that midpoint parity is almost always
exactly zero, so midpoint residual was discarded before labels were read. The
preregistered 54-policy grid instead uses three decision times, pressure-gap
floors 0.5/1.0/1.5, 15-second pressure-gap widening floors 0/0.25/0.5, and
pair-spread caps 0.01/0.02. Direction is only the sign of the Up/Down pressure
gap. The lightweight gates require 12 older observations with positive point
edge, then 20 recent observations, point edge above 0.02, and positive
fee-aware payoff. Wilson confidence is deferred to exact execution.

The cheap pass found six eligible policies but only four unique token traces.
The two-trace cap selected two 180-second traces and avoided four redundant
exact replays. One union pass scanned each of 12 discovery PMXT hours once and
avoided 12 duplicate scans:

- pressure gap at least 0.5: 65/65 fills, 37 wins, 28 losses, +$7.28865,
  point edge +0.04192, Wilson edge -0.07899;
- pressure gap at least 1.0: 61/61 fills, 34 wins, 27 losses, +$0.81407,
  point edge +0.03389, Wilson edge -0.09046.

The token-override replay contract lets a paired-book policy buy either catalog
token while retaining the original sealed coordinate. It correctly inverts the
separately keyed binary label when the selected token is the complement, and
rejects any override outside the pinned Up/Down identity.

Both traces retain positive point economics but fail the fixed +0.02 Wilson
advancement gate. `opportunity-liquidity-decision` therefore returns
`reject_family_keep_fresh_sealed`, exhausts the family budget, forbids more
evidence, and leaves all 133 fresh outcomes unopened.

The next architecture shift should make the paired cache the first member of a
general outcome-free feature store rather than add thresholds to this rejected
family. Each family plugin should declare an input-column allowlist, fixed grid,
cheap gates, exact budget, and terminal rule; the orchestrator can then run
coverage, label scoring, exact replay, and tombstone registration uniformly.
The next genuinely independent thesis should use external cross-venue lead/lag
or trade-arrival/order-flow dynamics, not another static pressure or price cap.

## Third budgeted portfolio family: trade-tape directional flow — 2026-08-11

The general feature-store boundary and its first plugin are now implemented.
`opportunity-flow-features` writes generic coordinate/token envelopes with a
plugin-owned payload and a hash-pinned manifest declaring causal windows,
payload fields, source parquets, and outcome-safety flags. The first plugin
streams PMXT book-top, quote-change, and `last_trade_price` rows without changing
the execution replay loader. Trade prints are deduplicated by transaction hash;
the event tuple is the deterministic fallback when a hash is absent. One
RowFilter scan is performed per source hour and no multi-million-row event vector
is retained.

The cold pass covered all 704 sealed coordinates and both token quotes were
observable on every row. Outcome-free tape inspection found median 15-second
pair trade counts of 126 on older, 73 on recent-discovery, and 49 on fresh rows.
It therefore replaced the initially considered non-discriminating 1/2/4 count
floors with 25/75/150 before labels were read. The fixed 54-rule preregistration
combines three decision times, two windows, three count floors, and three
absolute normalized trade-imbalance floors. Direction uses only complementary
aggressor flow: Up buys plus Down sells against Up sells plus Down buys. Book
quotes enforce a fixed 0.02 pair-spread and 0.99 selected-ask ceiling; depth
pressure, BTC path, volatility, model probability, scores, and PnL are forbidden
selection inputs.

Zero policies cleared both chronological cheap gates. The strongest recent
diagnostic (240 seconds, 15-second window, at least 25 prints, imbalance at least
0.50) had 71 recent observations, +0.03234 point edge, and +$22.04 top-quote
proxy PnL, but its older edge was -0.04331. The most stable older-positive rule
had 33 older and 83 recent observations, but recent edge was only +0.00076.
This is a temporal-instability failure, not missing tape coverage. No exact
replay was authorized, no fresh outcome was opened, and the terminal decision
sets `more_evidence_allowed=false`.

The next architecture work is not another PMXT tape threshold. Add a source
adapter registry to the feature store and prove timestamp/provenance parity for
an external Binance spot/perpetual tape. The first cross-venue plugin should
measure 1/5/15-second exchange return and signed taker flow, subtract the causal
move already reflected in the Up/Down midpoint, and test a fixed lead/lag
residual grid. If causally aligned exchange data is unavailable for every sealed
hour, record a data-quality block before labels rather than substituting a
different historical slice. After that plugin, replace family-specific search
commands with one manifest-driven screen/decision runner; exact replay and the
tombstone registry already have the required common contracts.

## Fourth budgeted portfolio family: cross-venue lead/lag — 2026-08-12

The external adapter is now implemented with actual causal parity rather than
minute-data substitution. Binance spot supplies official one-second klines;
USD-M perpetual one-second klines do not exist in the archive, so the adapter
groups official perpetual `aggTrades` by transaction-time second. Every archive
is verified against its adjacent SHA-256 checksum. Spot microseconds and
perpetual milliseconds are normalized to milliseconds, and a bucket is hidden
until `second_start_ms + 1000`.

The resulting 11 daily partitions have zero missing spot seconds and fully
align all 704 sealed coordinates. Perpetual no-trade seconds carry only the last
price while volume and counts remain zero; the longest trade gap is nine
seconds, and observed price age is at most 3.413 seconds. The plugin emits fixed
1/5/15-second returns, signed taker flow, volumes, counts, return gap, and the
existing outcome-free paired-book now/lookback state. All 704 external rows are
complete and 681 have complete paired state.

Before discovery labels were opened, the engine sealed a 36-policy grid: three
decision times; two fixed magnitude levels for each of the 1/5/15-second
horizons; and two maximum already-reflected PM midpoint moves. Direction
requires spot and perpetual sign agreement. The screen also required a maximum
2 bps source divergence, 0.02 pair spread, and full $5 top-book execution.

Zero policies passed. The conjunction was too sparse: displayed recent support
peaked at six and older support at two, far below the fixed 20/12 floors. No
exact replay or fresh outcome read occurred. This is a terminal family failure,
not permission to lower thresholds.

The architectural lesson is to separate hypothesis validity from marketability.
The next generic runner has five explicit stages: source/causality, raw
predictive discovery, top-quote marketability, bounded exact L2 replay, and only
then fresh resolved holdout. Execution filters may reject a directionally valid
trace, but they must not make the first predictive test too sparse to say
anything. The immutable v3 funnel is recorded in
`deploy/promotions/evidence/strategy_registry/20260812_strategy_finding_funnel_v3.json`.
