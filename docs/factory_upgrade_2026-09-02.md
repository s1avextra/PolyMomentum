# Factory upgrade (2026-09-02)

Six changes to the hypothesis factory (`scripts/strategy_research_loop.py`
and satellites). Every new behaviour is off in the checked-in
`deploy/strategy-research-loop.json` (kept fail-closed by
`tests/test_strategy_research_loop.py::test_config_is_fail_closed`); the
gitignored overlay `logs/strategy-research/loop-config.local.json` mirrors it
and is the only place lanes are enabled. Runtime state lives under
`logs/strategy-research/` (`research.sqlite3`, `trial_ledger.jsonl`,
`evidence/<lane>/<fingerprint>.json`).

## 1. Band lane (`scripts/band_lane.py`)

Searches the family that actually trades: at `decision_second` into a
btc-updown-5m window, compare the Binance 1s close with the open; when
|margin| clears `margin_floor_usd` (or `margin_floor_sigma` recent-sigma
units) buy the momentum side as a taker if its ask is inside
`(favorite_price_floor, favorite_price_cap]`. Every rule field is an enum
(`BAND_GRID`), so the constrained JSON schema makes every LLM sample valid.

Stages, all feed-forward on public cached data (Binance 1s closes and Gamma
outcomes from `scripts/margin_floor_study.py`'s cache; first public BUY print
within 30 s of the decision under
`logs/strategy-research/band_lane_cache/prints/`):

1. `band_signal_screen` - momentum sign vs official outcome.
2. `band_entry_economics` - realized win rate vs fee-aware break-even at the
   entry print.
3. `fresh_public_accrual` - e-process over strictly newer windows
   (`Ledger.accrue`, seeded at the newest window the screens used).

Proposer order: three deterministic priors (the live rule first), then strict
alternation LLM burst / seeded uniform control, grid fallback when the model
is not ready. Config (`lanes.band_mechanisms`): `enabled` (false in deploy,
true in the overlay), `minimum_interval_seconds` 900, `start_ts` 1787788800,
`maximum_new_windows_per_cycle` 400, `gates.minimum_signals` 100,
`gates.minimum_recent_signals` 20, `gates.minimum_entries` 50. Dispatch:
`--lane band_mechanisms` (`run_cycle` skips the public refresh and the
late-lane chain in that mode). Tests: `tests/test_band_lane.py`.

## 2. Fresh public accrual with e-values

`scripts/evidence_accrual.py` is the Python mirror of
`rust_engine/src/backtest/evalue.rs` (lambda grid 0.05..1.0 step 0.05,
per-lambda log-wealth, mixture = mean of exp(log-wealth), `PROMOTE_E` 20,
`FUTILITY_E` 0.1; the Rust unit tests are the reference vectors). The ledger
gained an `evidence_accrual` table and `Ledger.accrue(fingerprint, lane,
outcomes, seed_last_window_start)`, which only folds windows newer than the
stored `last_window_start`, so a replay is a no-op.

`run_fresh_public_accrual` (called from `run_cycle` after job reconciliation,
reported as `fresh_public_accrual`) scores every late-lane hypothesis whose
status is not terminal (`factory_generator.KILL_STATUS_STAGES`) on public
windows strictly after `stage_1_screen_cut` (the end of the newest signal
hour the stage-1 screen enumerated, replay and outcome-blind fresh buckets
alike), with break-even = `maximum_entry_price + taker_fee(price)` for every
signal. Rules with an L2-only filter (`settlement_sigma_buffer` > 0 or
`minimum_book_pressure` > -1) are skipped: public windows cannot tell which
signals those rules would actually trade.

The null is therefore the worst-case break-even (entry cap plus fee) over all
public directional signals, not the executable strategy's per-fill null, so
the verdict is not a promotion: `promote` and `continue` leave
`hypotheses.status` (the pipeline stage) untouched and are visible only in
`evidence_accrual.verdict`, the `stage: fresh_public_accrual` rows of
`trial_ledger.jsonl` and the cycle summary buckets; only `kill` writes
`killed_futility` (terminal, feeds kill feedback and the negative prompt, and
is never an EoH parent).
Executable per-fill evidence remains the job of the exact-L2 / fresh-holdout
stages. Tests: `tests/test_evidence_accrual.py`,
`test_evidence_accrual_is_idempotent_on_replay`, `test_fresh_public_accrual_*`,
`test_cycle_accrues_fresh_public_evidence_after_reconciliation`.

## 3. Economic-screen fix

`run_queued_economic_screen` now checks the exact-L2 shortlist
(`maximum_exact_l2_shortlist`, 2) right after the cached-family verdict and,
when the candidate passes but the shortlist is full, returns
`shortlist_saturated` before the trial-ledger row, the evidence artifact or
the job payload are written. Previously the saturation check ran after all of
those, so a saturated shortlist re-logged and rewrote the evidence artifact
for the same job every cycle. A rejected head is still rejected (the queue
keeps draining) and `fresh_exact_replay` jobs are exempt as before.
`run_cycle` treats `shortlist_saturated` like the other terminal screen
statuses, so the fixed-forward / fresh-holdout / exact dispatchers still run
in that cycle and the exact jobs that free the shortlist keep progressing.
Tests: `test_saturated_shortlist_defers_passing_screen_without_side_effects`,
`test_saturated_shortlist_still_rejects_head_and_reaches_fresh_job`,
`test_cycle_reaches_exact_dispatcher_while_shortlist_is_saturated`.

## 4. Sampler KPI, uniform control, prefix cut

- Prefix cut: `DIAGNOSTIC_PROPOSAL_PREFIX = 6`. The late lane walks
  `fallback_late_proposals()` only until the ledger holds six late
  hypotheses; from then on a ready LLM strictly alternates with the uniform
  control (the arm with fewer ledger rows goes next, LLM on ties; burst-queue
  replays count as LLM rows via `LATE_LLM_ARM_SOURCES`) and the grid is only
  the not-ready fallback.
- Uniform control: `uniform_late_proposal(rng)` draws a seeded uniform rule
  from the executable late grid (7128 points); the band lane has
  `uniform_control_rule`.
- Symmetric arms: both arms pass the same pre-screen filters, so the verdict
  compares sampler against sampler. The novelty gate screens uniform draws
  exactly as it screens LLM samples (provenance `control` = draws /
  duplicate / novelty_rejected); the reviewer verdict is advisory (persisted
  in `review_json`, never swapped for a grid rule); and an LLM turn whose
  burst leaves no survivor falls through to a control draw, so every ready
  cycle after the prefix lands in one arm and a failing model cannot hold
  the turn.
- Provenance: `hypotheses.source` column (migrated in place) and
  `proposal_source` in the lane result, one of `LATE_PROPOSAL_SOURCES`
  (`diagnostic`, `llm`, `uniform_control`, `fallback_grid`, `burst_queue`) or
  band `PROPOSAL_SOURCES` (`prior`, `llm`, `uniform_control`, `fallback_grid`).
- KPI: `uv run python scripts/factory_kpi.py` (`--state-dir`, `--json`)
  reports the funnel per lane x source, burst throughput and the
  LLM-versus-uniform verdict (`insufficient` below `VERDICT_MINIMUM_N` = 25),
  with `reviewer_rejected_s1=k/n` beside it: the reviewer-rejected subset of
  the LLM arm and how many of those still survived stage 1. The verdict
  scores each arm over distinct stage-1 projections
  (`factory_kpi.LATE_STAGE_1_FIELDS`: operator, path, move, buffer,
  direction), not rows: the late screen ignores the cap, sigma buffer and
  book pressure, and the LLM arm is deduplicated on the full rule, so the 54
  execution-only variants of one known survivor count as one projection
  instead of 54 banked survivors.
  Tests: `tests/test_factory_kpi.py`,
  `test_uniform_late_proposal_always_validates_and_replays`,
  `test_late_lane_alternates_llm_and_uniform_control_after_prefix`,
  `test_reviewer_reject_is_advisory_and_stays_in_llm_arm`,
  `test_llm_turn_without_survivor_falls_through_to_uniform_control`,
  `test_novelty_gate_applies_to_uniform_control_draws`.

## 5. Ensemble / reviewer plumbing (off by default)

Config keys under `llm`, present in both deploy and overlay as
`"sampler_models": []` and `"reviewer_model": null`:

- `sampler_models`: list of LM Studio model ids. When non-empty each LLM
  burst (late lane and band lane) uses the next model round-robin via
  `factory_generator.next_sampler_model`; the cursor is ledger meta
  `sampler_model_index.<lane>` (per lane: a cursor shared by both lanes pins
  each lane to one model whenever they burst at the same cadence) and the
  provenance records `sampler_model`. Queued burst survivors carry the
  model too (`sampler_model` in the queue entry), so a `burst_queue` replay
  is attributed to the model whose burst produced it.
- Constrained-schema fallback: the late burst drops to the legacy schema for
  the rest of a burst only when sample 0 is an HTTP 4xx whose body mentions
  the schema/grammar (a backend that cannot compile the anyOf branches). A
  timeout, connection error or model-load failure (a cold sampler) is a
  transport failure: the burst keeps the constrained schema and the sample
  is charged as generated, not mis-attributed to `constrained_schema_fallback`.
- `reviewer_model`: when set, the review call in `propose_late_rule` uses it
  (the band lane has no reviewer). The verdict is advisory: stored in
  `review_json` and counted by the KPI, it never replaces the proposal.
- `LmStudioClient.complete(..., model=None)` takes the override; the payload
  and the result's `model` carry the model actually used. `readiness()` still
  checks `default_model` only, so a roster or reviewer model that is not
  loaded fails its calls (60 s timeout or a load error per sample) while the
  lane reports ready: the runner keeps every configured model warm (section
  6) and the roster models should be pinned in LM Studio.

Recommended values (documented in `deploy/factory-runner.sh`, not in the
JSON): `sampler_models` `["openai/gpt-oss-20b", "deepseek-v4-flash-0731"]`,
`reviewer_model` `"qwen/qwen3.8-27b"`. Tests:
`test_sampler_models_round_robin_advances_and_persists`,
`test_model_override_reaches_request_payload`.

## 6. Operations (Mac only; VPS untouched)

`deploy/factory-runner.sh` rotates opportunity funnel / late lane / band lane
every 7.5 min against the overlay config, logging to
`logs/strategy-research/runner.log`. Each tick first POSTs a one-token
completion for every model the overlay can route to (`llm.default_model`,
each `llm.sampler_models` entry and `llm.reviewer_model`, deduplicated) to
`http://127.0.0.1:1234/v1/chat/completions` (25 s timeout each, output
discarded, failure ignored) so the remote models survive LM Link's 1-hour
idle TTL. With LM Studio's JIT "unload previous model on load" setting on,
every model switch is still a cold load, so pin the roster models in LM
Studio when enabling the ensemble. A `pgrep` guard on the script name (own
pid excluded) makes a second instance log one line and exit 0.

`deploy/com.polymomentum.factory-runner.plist` is a launchd user agent
(`RunAtLoad`, `KeepAlive` only on a non-zero exit - `SuccessfulExit` false -
so the guard's exit 0 is not respawned every ~10 s while a manual copy runs,
`ProcessType` Background, `Nice` 10, `PATH` including `~/.local/bin` for
`uv`; stdout/stderr to
`logs/strategy-research/factory-runner.launchd.{out,err}.log`). After a
manual run ends the agent does not come back on its own: kickstart it.

```
cp deploy/com.polymomentum.factory-runner.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.polymomentum.factory-runner.plist
launchctl kickstart gui/$(id -u)/com.polymomentum.factory-runner      # after a manual run
launchctl bootout gui/$(id -u)/com.polymomentum.factory-runner        # uninstall
```

Retire the stale Aug-12 copy of the loop, `com.polymomentum.strategy-research`
(every 30 min from `~/Library/Application Support/PolyMomentumStrategyResearch`
against the same LM Studio):

```
launchctl bootout gui/$(id -u)/com.polymomentum.strategy-research
mv ~/Library/LaunchAgents/com.polymomentum.strategy-research.plist \
   ~/Library/LaunchAgents/com.polymomentum.strategy-research.plist.disabled
```

## Addendum (2026-09-02 12:10 UTC): structural novelty gate

Distinct enum-grid rules embed at cosine 0.974-0.984 with nomic-embed, above
the 0.97 `novelty_max_cosine`, so the cosine gate rejected 64 of 64 uniform
control draws once a handful of rules existed. Structured proposals (any
proposal carrying a `rule` dict) now use `factory_generator.structural_novelty`:
rejected when fewer than `generator.novelty_min_hamming` (default 2) fields
differ from a killed rule (`killed_negative_items` / `band_lane.killed_items`
carry `rule_fields`). The cosine path remains for free-text proposals.

## Addendum: shadow race

Challengers run "in parallel" with the champion without money: the canary
records what the venue actually offered at fixed decision seconds, and the
race scores champion and challenger rules offline on those same windows.

### `band_anchor` records

The live band engine (`rust_engine` with a band strategy, i.e.
`deploy/band-canary.env`; a legacy candle deployment never captures, since
its shared open cache is on the chainlink basis and the anchor margin would
mix bases) appends one `band_anchor` record per btc-updown-5m window and
anchor second to its session log (`SESSION_LOG_DIR`, `session_*.jsonl`;
anchor seconds from `BAND_ANCHOR_SECONDS`, default `180,210,240,270`, the
band grid). The write is side-effect free for trading (no order, no
state), costs microseconds and swallows its own errors. Shape:

```
{"type":"band_anchor","ts":...,"cid":"<16 hex>","anchor_s":210,"elapsed_s":210.31,
 "btc":...,"open":...,"margin":...,"direction":"up"|"down"|null,
 "stake_usd":...|null,"quote_budget_usd":...,
 "up":{"best_ask":..,"book_age_s":..,"vwap":..,"worst":..,"shares":..},"down":{...},
 "pair_sum":...}
```

`margin` is the exchange mid minus the window open at the anchor. The
window start is `ts - elapsed_s` rounded to the 300 s grid. Anchors are
written only once the window is open (the contract list runs up to an hour
ahead; a `0` anchor is the first cycle after the open).

- `stake_usd` is the live sizing target for this cycle (`kelly_lo_stake` or
  `target_stake` per `BAND_SIZING`), null exactly when the champion could
  not trade this cycle for reasons that are not the rule's: no fresh best
  ask on both sides (`pick_book_prices`, the `fresh_outcome_book_unavailable`
  skip; `book_age_s` / `best_ask` are then null on the stale side) or the
  sizing policy declining the bucket (kelly_lo's <= 0.70 favorite, the
  `kelly_no_edge_bucket` skip). The scorer treats null as "no trade" for
  every rule, champion and challengers alike.
- `quote_budget_usd` is what `up` / `down` were sized at: `stake_usd`, or
  the frozen `target_stake` when `stake_usd` is null, so the venue's offer
  is still recorded. `up` / `down` are the budget-aware FOK quotes (`vwap`,
  `worst`, `shares`), null when the book cannot fill that budget.
- The budget is the sizing target BEFORE the per-market, available-capital
  and stress caps `evaluate_band_opportunity` applies. When a cap binds the
  live champion quotes a smaller budget (or skips with `band_no_capital`)
  and the anchor does not show it: the race is a rule-vs-rule comparison on
  a common budget, not a replica of the live ledger, and capital-cap skips
  are not reproducible from anchors (cross-reference `band_skip_detail` or
  the `risk_v2` sizing shadow for that).

### Race CLI: `scripts/band_shadow_race.py`

```
uv run python scripts/band_shadow_race.py --pull \
    --challengers '{"margin_floor_usd":50,"margin_floor_sigma":0,"decision_second":210,"direction":"both","favorite_price_floor":0.55,"favorite_price_cap":0.92}' \
    <band-lane fingerprint> --json
```

- `--sessions-dir` (default `logs/band-canary-mirror/sessions`); `--pull`
  runs `rsync -az vps:/opt/polymomentum/logs/band-canary/sessions/
  logs/band-canary-mirror/sessions/` (read-only on the VPS).
- `--champion` (default the live band rule: floor $50, sigma 0, 240 s,
  both, ask (0.55, 0.92]) and `--challengers` take JSON band rules or
  band-lane fingerprints (`hypotheses.proposal_json` in
  `logs/strategy-research/research.sqlite3`, read-only). The champion is
  replayed exactly like every challenger, on the anchor quote at the
  pre-cap budget: the engine's fresh-book, sizing-policy and
  pair-coherence gates are replayed, its capital, wallet and breaker gates
  are not, so the champion row need not match the canary's order log (the
  markdown footer says so).
- Outcomes come from band_lane's Gamma cache
  (`logs/strategy-research/margin_study_cache/gamma_outcomes.json`); missing
  windows are fetched through `band_lane`'s fetcher with the same 15-minute
  eligibility and recorded as final null after two hours, like
  `BandCache.refresh`. The file is shared with the research loop's band
  lane, so the race re-reads it before saving and adds only its own
  fetches (neither writer loses the other's). The sigma floor uses band_lane's sigma (population
  stdev of |Binance margin| over the 12 preceding windows) on the Binance
  1s closes from the same cache.
- Per window and rule: the anchor at the rule's `decision_second`. The
  engine's cycle gates first, for every rule (they are not the rule's):
  `book_age_s` and `best_ask` non-null on both sides, `stake_usd` non-null.
  Then direction from the anchor margin sign (skip when null, below
  `margin_floor_usd` or below `margin_floor_sigma * sigma`); the direction
  must be allowed; the momentum side's VWAP plus the complement's best ask
  must lie in [0.90, 1.10] (the engine's `band_pair_incoherent` skip, the
  2026-08-26 frozen-book incident: a fresh best ask on both sides does not
  imply a coherent pair); the momentum side's quote must clear the band
  exactly as `BandPolicyParams::quote_clears_band` does (`vwap > floor`
  and FOK `worst <= cap`). No executable quote, incoherent pair or out of
  band: the rule does not trade (score 0). Score when it trades, net per 1 USD staked with the fee
  model of `band_lane.break_even` (`taker_fee` from
  `scripts/adaptation_persistence_study.py`, 0.072 p (1 - p) per share):
  win `1/(vwap + fee) - 1`, i.e. `(1/vwap - 1)` minus the fee per USD;
  loss `-1`.
- Output: per rule windows seen, trades, wins, net per USD and total net at
  the anchor's stake (the same trade set: null-stake windows are not
  trades); per challenger the paired n (windows with both anchors where at
  least one rule traded), mean d (after the divisor below), d > 0 / d < 0
  counts, overlap (both traded the same side), clipped count (0 by
  construction; anything else is printed as a validity warning), e-value
  and verdict.
  Markdown to stdout; `--json` also writes
  `logs/strategy-research/band_race/<utc>.json` and appends a trial-ledger
  row (stage `champion_challenger_race`, candidate = challenger
  fingerprint, n = paired windows, wins = windows with d > 0) via
  `factory_generator.append_trial_entry` with
  `deploy/strategy-research-loop.json`.

### Paired e-process and its null

`EProcess.update_signed(d)` (identical in `scripts/evidence_accrual.py` and
`rust_engine/src/backtest/evalue.rs`, reference vectors asserted on both
sides at relative error 1e-9; both reject a NaN d as an error, a bug,
never evidence): d clipped to [-1, 1], each lambda's factor
`max(1 + lambda_j d, 1e-12)` on the existing grid lambda_j = (j + 1) 0.05,
mixture = mean of the per-lambda wealths. Under the null "E[clip(d)] <= 0"
every factor has expectation <= 1, so the wealth is a supermartingale and
Ville's inequality bounds P(e ever >= 1/alpha) by alpha at any stopping
time (Waudby-Smith & Ramdas betting; Choe & Ramdas sequential forecaster
comparison). Verdict: `promote` at e >= K/alpha for the K challengers of
the run (Bonferroni: each challenger's e-process is bounded by Ville at
level alpha/K, so the union bound keeps the family-wise false-promote rate
of the run at alpha; 20 for one challenger at the default 0.05, 40 for
two), `kill` at e <= 0.1 (futility, not a type-I bound), else `continue`.
The family is the run's challengers only: a challenger raced against the
same champion history in a separate run is not counted, so race the whole
set in one run, or count every challenger ever raced against this
champion history and lower `--alpha` accordingly.

The clip is a guard, not a transform: E[clip(d)] <= 0 equals the null the
race documents, E[d] <= 0 (the challenger is no better than the champion
per USD per window), only when |d| <= 1 almost surely. Clipping the raw
score difference is NOT merely conservative: a win pays 1/break_even(vwap)
- 1 (0.08 at 0.92 .. 0.76 at 0.55) and a loss -1, so every disagreement
window has |d| > 1, and a tighter-band challenger that wins small (0.85)
and loses big against a champion at 0.60 can have E[d] < 0 while
E[clip(d)] > 0 (champion winning 42-50% of disagreements): the clipped test
would promote a challenger that loses money per USD. The race therefore
divides d by a constant fixed for the race, the largest
1/break_even(favorite_price_floor) among the rules (1.761 at the 0.55
floor), which preserves the sign of E[d] and keeps |d| <= 1 so the clamp
never fires; the report prints the divisor, and a clipped count above 0
is a validity warning (the e-value then tests E[clip(d)] <= 0).

Post-fill book bias: the capture runs before the cycle's traded-set skip
on purpose (a window the champion entered at 240 s still yields its 270 s
anchor), so a later-second anchor in a window the champion traded quotes
the book after the champion's own FOK fill lifted that side's asks. The
champion's own anchor was walked over the undepleted book, so a
later-second same-side challenger is scored on equal-or-worse VWAPs in
exactly the windows where the champion traded: a one-sided bias against
such challengers of roughly the canary's own market impact (stake-sized,
small). The paired test cannot see it; the anchor carries no
champion-traded flag, so affected pairs are not counted in the report.

### Operational sequence

1. Ship the anchor build to the canary (operator step; the money path is
   unchanged). The canary runs the pinned binary
   `/opt/polymomentum/tools/polymomentum-engine-band-v1`
   (`deploy/polymomentum-band-canary.service`), NOT the
   `/opt/polymomentum/polymomentum-engine` that `deploy/deploy.sh`
   installs, so:
   - build the release binary off the VPS (CI `linux-release` artifact or
     a dev box); if it must be built on the VPS, `nice -n 10 cargo build
     --release --locked -j 1 --bin polymomentum-engine` and never
     concurrently with a peer bot's build (CLAUDE.md rules 3 and 5);
   - copy it to `/opt/polymomentum/tools/polymomentum-engine-band-v1` and
     sync `deploy/band-canary.env` to `/etc/polymomentum/band-canary.env`
     (`BAND_ANCHOR_SECONDS` defaults to `180,210,240,270` in the binary,
     so the env sync only matters if the operator changes the anchors);
   - `systemctl restart polymomentum-band-canary` between windows, with
     no open band position;
   - verify after one full window, before starting the accrual clock:
     `grep -c '"type":"band_anchor"' <newest session_*.jsonl under
     /opt/polymomentum/logs/band-canary/sessions/>` must be > 0 (one per
     anchor second per BTC 5m window). Zero means the old binary is still
     running; `scripts/band_shadow_race.py` would only report "no
     band_anchor records" after the wait.
2. Wait N days while `band_anchor` records accumulate (about 288 windows a
   day per anchor second).
3. `--pull` and run the race against the live rule, every challenger in
   one run; re-run as windows resolve. The e-process is anytime-valid, so
   peeking is free.
4. Only a `promote` verdict on paired windows (fresh, official outcomes,
   executable quotes at the anchor) earns a promotion artifact for the
   challenger; that artifact still goes through the usual operator review.
   `kill` retires the challenger; `continue` keeps waiting.

Tests: `tests/test_evidence_accrual.py` (`SignedUpdateTest`),
`tests/test_band_shadow_race.py` (synthetic anchors, injected outcomes and
closes, no network; `--pull` is mocked and asserted absent on the default
path).
