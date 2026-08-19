# PolyMomentum autonomous strategy research loop

## Outcome

The research coordinator runs on the 10-core dev Mac, not on the shared two-core
VPS. It wakes every 30 minutes, acquires a single-process lock, checks disk and
load, and audits the registry. During the v3 migration its active default lane is
the sealed opportunity-table policy search; the two legacy proposal lanes remain
fail-closed.

The coordinator is deliberately **research-only**. Its command allowlist contains
only:

- `strategy-builder registry-audit`
- `strategy-builder evolve-search`
- `strategy-builder rolling-history` with one frozen variant and one signal-hour fold
- `strategy-builder opportunity-policy-search` over hash-pinned local artifacts
- `strategy-builder opportunity-probability-search` for the bounded external
  causal-probability-versus-price family
- `strategy-builder opportunity-exact-replay` for at most two policy-search traces
- `strategy-builder opportunity-probability-decision` for a terminal,
  budget-enforced family verdict

It cannot call paper, live, materialization, registry promotion, deployment, SSH,
or VPS commands. The active exact replay reads each required PMXT hour once for
the union of at most two discovery traces and applies the pinned latency before
walking visible ask depth. Fresh outcomes remain physically absent. Promotion
remains a separate human-reviewed workflow.

## Why the 750-condition block is not an exploration gate

The active validation funnel is:

1. causal mechanism definition and cheap public directional screen;
2. cached, fee-aware top-of-book economic fail-fast;
3. bounded historical exact-L2 replay with fills, fees, latency, sizing, and risk
   controls;
4. newest resolved frozen exact-L2 holdout of at most eight observable windows;
5. a future-only fixed-forward confirmation sealed before any forward outcomes
   exist;
6. official Chainlink settlement-source parity;
7. a bounded, zero-order `paper_only` VPS shadow that validates production wiring.

The historical and fresh exact screens are deliberately bounded feasibility
gates. They require support, successful fills, directional coverage, and clean
risk/runtime diagnostics. A pass means only `research_eligible`; it does not mean
economically attractive, paper-ready, or live-ready. The economic gate separately
checks net payoff, the mean winning payoff, recovery cost after one maximum-stake
loss, and whether the public Wilson lower bound clears the payoff-derived
break-even rate. A failed economic gate terminates the candidate before forward
measurement.

There is no fixed 750-condition exploration gate. The fixed-forward target is
derived from each frozen candidate's realized payoff asymmetry: the one-sided 95%
Wilson lower bound must clear its frozen break-even accuracy. The target is
bounded by configuration (currently 1,000 fills), with a 25-fill operational
checkpoint that cannot promote anything. The current candidate's math produces a
765-fill target; that number is evidence about its weak payoff, not a universal
barrier applied to every new idea.

## Lanes

### `opportunity_policy_search` (active default)

This lane consumes an immutable causal dataset seal and a separately hashed
discovery-label manifest. It calibrates on `older`, evaluates policies on
`recent_discovery`, and reports only causal support for `fresh_holdout`. Its
inputs and settings are content-addressed, so the 30-minute timer does no policy
work when the hash is unchanged. The cheap discovery gate uses support, positive
fee-aware payoff, and point-estimate edge only to choose research traces; Wilson
confidence remains mandatory for later advancement. The lane collapses execution-
equivalent policies, keeps at most two traces at distinct decision times when
possible, and runs one content-addressed 128 ms exact-L2 replay. It never schedules
paper or live execution.

### `baseline_evolution` (paused)

This preserves the original `primary_v6` research family. Once per day it runs a
small deterministic `evolve-search` over at least three distinct chronological
reports using current replay semantics v6. The run is intentionally a historical
mechanism screen. Its output is recorded but is not placed in the executable
holdout queue until it has a compiled frozen variant and preregistered fresh
windows. Old-semantics reports can reject or diagnose a historical failure but
are never silently fed to current evolution and cannot promote anything.
The coordinator hashes the report contents and search settings and skips the run
when those inputs are unchanged.

### `late_window_mechanisms` (paused)

Once per six hours this lane asks the already-loaded Gemma 12B for one bounded
public-data rule and then asks the same model, sequentially, to criticize the
proposal. The model can choose only a finite DSL:

- path persistence through minute 3 or 4;
- two-minute BTC displacement from the executable `$100–200` / `≥$200` buckets;
- path-only, move-only, AND, or OR composition;
- maximum entry price from the regular 0.75 through 1.00 grid, plus targeted
  payoff-derived 0.97 proposals;
- a `$0`, `$100`, or `$200` decision margin;
- a `0.0`, `0.1`, or `0.2` settlement-volatility buffer;
- both directions or one explicit causal direction (`up` / `down`).

The local validator rejects unknown fields, out-of-grid values, invalid causal
timing, and duplicates. The public screen independently recomputes support,
directional stability, chronological stability, and a Wilson lower bound from the
durable public snapshot. A survivor first receives a cached top-of-book economic
screen. Only a passing survivor may occupy one of the two exact-L2 shortlist slots.
Public accuracy never establishes executable edge.

The public snapshot is rebuilt daily from checksum-verified official Binance
one-minute archives. The newest 14 fully resolved days are tagged as holdout
before any candidate is proposed. Their terminal outcomes are not read during
candidate selection. Historical and fresh reserves may use causal signal density
to spend the bounded replay budget efficiently. Fixed-forward windows do not:
they are every causal signal-hour strictly after `sealed_at`, in chronological
order, without terminal-outcome scoring or density ranking.

Binance remains the causal BTC market-data input. Promotion evidence uses a
separate settlement tape whose manifest must be complete, nonempty, hash-pinned,
and identified as `chainlink_btc_usd_data_stream`. Missing official settlement
data defers the forward window; it is never silently replaced by Binance close.

A compact report that proves PMXT contained zero target events is retained as
support-only evidence and does not consume the eight-window measurement budget.
Its replacement can come only from the already frozen reserve order. A zero-trade
result on otherwise complete L2 data remains a measured strategy result and is
never excluded.

Historical and fresh exact replay always report the signal-to-attempt rate, but
v6 treats it as diagnostic at those stages because minimum fills, active-window
coverage, fill rate, and net economics already gate advancement. The rate remains
a hard requirement at the fixed-forward operational checkpoint. Policy migration
is fingerprint-allowlisted so a rule change cannot silently reopen the historical
candidate archive.

If LM Studio is stopped, busy, or returns invalid JSON, the deterministic finite
grid supplies the next unseen rule. The research loop therefore keeps advancing
without treating model availability as evidence.

## LLM isolation and privacy

The exact model ID is pinned to
`gemma4-12b-qat-uncensored-hauhaucs-balanced`. The coordinator probes that exact
ID and never requests model loading, so it cannot JIT-load a 27B model or evict a
model used by another process. A separate advisory lock permits one PolyMomentum
LLM request at a time. Timeouts, connection failures, and busy responses defer to
the next scheduled cycle.

LM Studio currently has `logSensitiveData=true`. Consequently the checked-in
configuration permits only the public late-window prompt: no strategy results,
outcomes, scores, economics, active forward rows, secrets, wallets, private paths,
or commands are sent to the model. Keep proprietary LLM prompts disabled until
that LM Studio setting is explicitly turned off.

The model is a proposer and critic, not a validator. All metrics, gates, hashes,
deduplication, queue state, and progression decisions are deterministic.

## State and evidence

Runtime state is ignored by Git under `logs/strategy-research/`:

- `research.sqlite3`: durable hypothesis/job ledger;
- `status.json`: atomic latest status snapshot;
- `evidence/`: immutable public screens and registry audits;
- `runs/`: deterministic evolution output;
- `locks/`: cycle and LLM advisory locks;
- `launchd.*.log`: scheduler output.

Late-window hypotheses are content-addressed by the normalized executable rule
and evaluator version. Titles, LLM wording, and a refreshed snapshot cannot create
a duplicate semantic candidate. Source hashes are retained in evidence so every
screen remains reproducible.

## Resource policy

The coordinator refuses heavy work unless the host has at least eight CPUs, 20
GiB free, and one-minute load at or below 1.00 per CPU. Exact replay uses two
worker threads. Engine subprocesses run at
nice 10, one lane per cycle, with a 30-minute wall-clock limit. Exact L2 remains
single-worker work and uses atomic parquet staging. Downloaded raw parquet is
removed after processing, while candidate-independent compact sidecars are kept
under a shared per-window cache and reused by later variants.

Do not install this timer on the PolyMomentum VPS. That host is capture/runtime
only under the repository's multi-tenant rules.

## Commands

Validate and inspect without running the engine:

```bash
PYTHONPYCACHEPREFIX=/private/tmp/polymomentum-pycache \
  python3 -m unittest -v tests/test_strategy_research_loop.py

python3 scripts/strategy_research_loop.py --once --dry-run \
  --config deploy/strategy-research-loop.json

python3 scripts/strategy_research_loop.py --status \
  --config deploy/strategy-research-loop.json
```

Run one explicit lane:

```bash
python3 scripts/strategy_research_loop.py --once \
  --lane opportunity_policy_search \
  --config deploy/strategy-research-loop.json

# Legacy diagnostics remain fail-closed during migration:
python3 scripts/strategy_research_loop.py --once \
  --lane baseline_evolution \
  --config deploy/strategy-research-loop.json

python3 scripts/strategy_research_loop.py --once \
  --lane late_window_mechanisms \
  --config deploy/strategy-research-loop.json
```

Install on macOS:

```bash
python3 scripts/install_strategy_research_launchd.py
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.polymomentum.strategy-research.plist 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.polymomentum.strategy-research.plist
launchctl kickstart -k gui/$(id -u)/com.polymomentum.strategy-research
```

The installer copies a self-contained, secret-free runtime bundle to
`~/Library/Application Support/PolyMomentumStrategyResearch`. macOS does not grant
background `launchd` Python processes access to `~/Documents`; placing the small
bundle outside that TCC-protected directory avoids giving Python broad Files and
Folders permission. The bundle contains the research script, measurement binary,
config, pinned public snapshot, four current-semantics input reports, base variant, and registry
evidence referenced by the audit. It never copies `.env`, API credentials, wallet
material, SSH state, raw active-forward captures, or the Git worktree. PMXT files
pinned by the active dataset seal are exposed inside the bundle through atomic
same-filesystem hard links, so the LaunchAgent can read them without duplicating
the multi-gigabyte cache or crossing the macOS Documents privacy boundary. Re-run
the installer after changing checked-in research code/config or the dataset seal.

The Linux service/timer templates are provided for a dedicated 8+ CPU dev box as
`deploy/polymomentum-strategy-research.{service,timer}`. They must not be copied to
the shared VPS.

## Resolved late-window candidates

The first historically eligible late-window hypothesis is the executable conjunction
`article_path_3m=aligned AND article_move_2m=aligned_ge_200`: all first three
one-minute BTC moves point in the same direction and the absolute two-minute move
is at least $200. Its cheap public screen found 31 signals, 31 directionally
correct outcomes, both directions represented (16 up and 15 down), and a 0.8897
Wilson 95% lower bound. Only one signal belongs to the fresh pre-forward subgroup,
so this is a screen survivor rather than fresh confirmation.

The autonomous worker then exhausted its preregistered two-window exact-L2 budget:

- 2026-05-04 08:00–15:00 UTC: 5/5 wins, 5/5 fills, +$28.02107 net after
  $0.84923 fees;
- 2026-04-21 08:00–15:00 UTC: 3/3 wins, 3/3 fills, +$4.68243 net after
  $0.17530 fees.

The combined result is 8/8 wins and fills, +$32.70350 net after $1.02453 fees,
with no unresolved fills or breaker trip. Each engine report returns code 2 because
the individual fold has fewer than the engine's 20-trade coverage floor; the
coordinator records that as a completed coverage-limited window, not as missing
evidence. The ledger job is closed as `completed`, so it cannot opportunistically
add more windows.

This remains historical research, not a live or paper candidate. Its frozen
current holdout contains only two causal opportunities, below the five-fill
holdout floor, so that holdout cannot make it eligible.
Durable hashes and metrics are recorded in
`deploy/promotions/evidence/strategy_registry/20260728_late_window_path3_move200_bounded_exact_replay.json`.

The later four-minute-path OR $200-two-minute-move candidate passed the thin
historical and fresh exact gates, but the economic stage rejected it. Five fresh
wins returned only $0.09817 total on a $5 maximum stake. One full loss would
require 255 average wins to recover, and its public Wilson lower bound of
0.991269 is below the 0.996089 break-even accuracy. Its forward job was therefore
sealed and immediately blocked without consuming future outcomes. The durable
registry records it as `rejected`, with the public, fresh, economic, and blocked
forward evidence hash-pinned.

## Current operational boundary

As of 2026-08-11 the legacy candidate generators and legacy exact replay worker
remain paused fail-closed, while the opportunity-table policy-search lane is
active. The scheduler continues its resource/status and registry-audit duties,
but its default cycle skips the old public refresh and LLM lanes and runs only
the content-addressed sealed-dataset evaluator and its bounded exact replay.
`baseline_evolution` and
`late_window_mechanisms` still return `paused_architecture_migration` before
proposing work. Any queued legacy
exact job is durably blocked while preserving its partial evidence; the known
execution-equivalent duplicate is classified
`superseded_execution_equivalent` with its reference fingerprint recorded.

This migration does not relax or alter the independently sealed binary-complement
forward canary. It authorizes only the local measurement commands
`opportunity-signals`, `opportunity-table`, `opportunity-dataset-seal`,
`opportunity-labels`, `opportunity-policy-search`,
`opportunity-probability-search`, `opportunity-pair-features`,
`opportunity-liquidity-search`, `opportunity-flow-features`,
`opportunity-flow-search`, `opportunity-exact-replay`,
`opportunity-probability-decision`, `opportunity-liquidity-decision`, and
`opportunity-flow-decision`; it does not authorize paper orders or live
execution.
Discovery labels physically exclude fresh rows.

The active immutable dataset now contains 704 causal rows: 144 older, 427
recent-discovery, and 133 fresh-holdout. The label table contains 571 older and
discovery labels and physically excludes all 133 fresh rows. The eight additional
discovery hours were selected by a calendar rule before Gamma identity lookup,
PMXT inspection, label access, or outcome scoring. An unchanged seal and replay
input are skipped.

The bounded expansion selected two exact-replay traces at 180 and 120 seconds.
Both filled 28/28 orders at the pinned 128 ms latency and remained profitable,
but their replayed Wilson edges were -0.07190 and -0.10098. Neither exceeded the
preregistered +0.02 advancement margin, so both traces are rejected, the search
budget for this family is exhausted, and the fresh holdout remains sealed. The
next research unit must be a new hypothesis family, not more hours or parameter
variants for this one.

The first independent replacement family, external causal probability versus
executable price, has also completed its fixed budget. Its best 180-second trace
filled 32/32, returned +$48.00168, and reached Wilson edge +0.00272, below the
fixed +0.02 advancement margin. The terminal decision rejects the family,
forbids more evidence, and keeps fresh sealed. The next family must use a new
information source such as liquidity/cross-token dislocation rather than another
volatility scale or price threshold.

The independent paired-book liquidity family has now also exhausted its fixed
budget. Its outcome-free cache reconstructed both complementary books at the
decision time and 15 seconds earlier for all 704 sealed coordinates. The fixed
54-policy screen used only pair spread and cross-token depth-pressure gap; it
selected six eligible policies and four unique traces. The two-trace exact cap
tested two 180-second traces in one 12-hour union scan. They filled 65/65 and
61/61 and returned +$7.28865 and +$0.81407, but Wilson edges were -0.07899 and
-0.09046. The terminal decision rejects the family, forbids added thresholds or
hours, and leaves fresh sealed. The next family must use a genuinely independent
source such as cross-venue lead/lag or trade-arrival dynamics.

The independent trade-tape family and the general outcome-free feature-store
contract are now complete as well. A single streamed pass over each of the 20
sealed PMXT hours produced 704/704 paired rows without reading labels. The fixed
54-rule trade-flow screen found no rule that reproduced fee-aware edge across
both older and recent-discovery partitions. Its best recent point-edge rule was
positive by +0.03234 but negative by -0.04331 on older data; no exact replay was
authorized. The family is terminally rejected, fresh remains sealed, and its
threshold budget cannot be reopened.

The cross-venue source-feasibility and timestamp-parity pass is now complete.
Official checksum-verified Binance spot 1-second klines and USD-M perpetual
aggregate trades cover all 11 source dates behind the 20 sealed PMXT hours.
Closed-second normalization produced complete external features for 704/704
coordinates; 681 also have both current and 15-second-lookback complementary
books. No labels were read during source or feature construction.

The fixed 36-policy lead/lag family then failed its cheap screen. No policy
cleared the fixed older/recent support gates, no exact replay was authorized,
and fresh stayed sealed. The largest recent support among the displayed
diagnostics was only six and older support never exceeded two. The family is
terminal and its thresholds cannot be reopened.

This failure exposed a funnel problem: the cheap screen simultaneously required
predictive external consensus, limited PM chase, a 0.02 complementary spread,
and full top-book executability. The next architecture revision separates a
source-only raw predictive gate from marketability and exact execution. A
family must first reproduce direction across older and recent partitions; only
that unchanged trace may then acquire fee/quote gates and at most two exact L2
replays. The staged contract is pinned in
`20260812_strategy_finding_funnel_v3.json`. Re-scoring this cross-venue family,
adding hours, or relaxing its gates after outcomes is forbidden.

This coordinator never starts, stops, inspects, or deploys VPS trading services.
The VPS remains capture/runtime infrastructure; strategy generation and replay
stay on the 10-core dev Mac. If a candidate eventually passes fixed-forward and
official settlement parity, the only automatic next artifact is a bounded shadow
verdict: `paper_only`, at most 9,000 seconds, at least 24 resolved shadows, and
exactly zero live order submissions. It still cannot authorize paper orders or
live trading.
