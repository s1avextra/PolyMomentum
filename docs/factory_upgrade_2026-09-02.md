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
