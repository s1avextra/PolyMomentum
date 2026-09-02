# Autonomous factory topology: PC generator, VPS collector+trader, Mac orchestrator (2026-09-02)

Status: design synthesis, nothing installed. The "ops" design won the three-way review; its topology, authority table, data plane, retention math and phase order are kept. The statistical spine is grafted from "rigor", the budget/demotion rules from "automation", and the points the judges said all three missed are tagged "(missed)".

## 1. Goal and principles

Goal: the hypothesis factory (`scripts/strategy_research_loop.py`, `scripts/band_lane.py`, `scripts/factory_generator.py`, `scripts/evidence_accrual.py`) runs unattended on MainPC with local LLMs; the VPS keeps trading the band mechanism (`deploy/promotions/band_policy_margin50.json`) and collects only what it alone can see; the Mac makes every code change, verifies every promotion offline, and deploys.

Verified facts (probed 2026-09-02) that override the brief: the Mac<->PC mesh already exists (Tailscale on both, `ssh mainpc` works, so LM Link is redundant); MainPC is a Ryzen 9 7900X, 31.6 GB RAM, **RTX 4070 Ti 12 GB VRAM** (not 24-32), ~337 GB free on C:, Windows 11 **Enterprise** (RU locale, corporate VPN client), no WSL, LM Studio GUI living in an RDP session with gpt-oss-20b JIT-loaded on a 1 h TTL, and no `deepseek-v4-flash-0731` on disk; PMXT v2 stalled upstream at ~2026-08-10T0x and both `/opt/shared` caches are empty or gone, so the VPS is the only fresh exact-L2 source; the VPS has 23 GB free, sessions of 3-6 MB/day, 1.5 GB of our stale `logs/latency`, and a `failed` `polymomentum-soak-report.service`; `band_promotion_margin50.json`, the artifact the canary runs under, exists only at `vps:/opt/polymomentum/promotions/`.

Principles: one writer per data class; pull-only transfers ("downloader owns it", CLAUDE.md section 7); no broker; every shared file `*.tmp.<pid>` + `rename(2)`. The spine is structural: fresh-window-first (CLAUDE.md section 6), the e-process of `rust_engine/src/backtest/evalue.rs` = `scripts/evidence_accrual.py` (lambda grid 0.05..1.0, `PROMOTE_E` 20, `FUTILITY_E` 0.1), one ledger row per look, e-BH across the family, tripwires, a proposer that never sees outcomes, no LLM-authored code executed anywhere. Live is never gated by research; the Mac recomputes every verdict number; promotion stays a human commit plus restart. One Telegram voice (the VPS), state-change-only alerts. A candidate without fresh L2 is "blocked", never "stale but running".

## 2. Node responsibilities

| | MainPC (generator) | VPS (collector + trader) | Mac (orchestrator) |
|---|---|---|---|
| Runs | WSL2 Ubuntu 24.04 + systemd; LM Studio/llmster on the Windows host at `127.0.0.1:1234` | `polymomentum-band-canary.service` (unchanged) + new `polymomentum-collector.service`, `-trim.timer`, `-nodecheck.timer` | Claude Code, git, tests, `deploy/pc-deploy.sh`, `deploy/pc-pull.sh`, the VPS deploy recipe |
| Sole writer of | factory state `logs/strategy-research/`, PMXT + distilled caches, `export/` bundle | session JSONL, `candle/state.db`, L2 mirror, RTDS, venue status, resolutions, secrets, Telegram | code, `docs/strategy_registry.json`, `deploy/promotions/*`, `deploy/campaigns/*` |
| Reads | `vps_mirror/` (evaluators only), public APIs | nothing from PC or Mac | PC `export/`, VPS via ssh |
| Never | edits code; writes to the VPS outside `export/ack`; holds secrets | CPU work > 30 s; parquet reads by us; peer dirs | sits on an always-on path; trusts evidence it did not recompute |
| Compute | LLM sampling, screens, exact-L2 replays (`exact_replay.threads: 8`), sweeps | ws parse + gzip under `CPUQuota=15%` | reviews, `verify_gate.py`, builds |

## 3. Data plane

| # | Stream | Path | Cadence | Size | Retention (owner deletes) |
|---|---|---|---|---|---|
| A1 | Session JSONL + `summary_*.json` (`rust_engine/src/monitoring/session.rs`) | canary -> PC pull via `export/sessions` (ro bind) | hourly; sealed when `summary_` exists | 3-6 MB/day | VPS 21 d if acked, hard 45 d; PC forever |
| A2 | `book_anchor`: the REST `/book` ladder the trader saw, top-10 both tokens, every 4 s while `remaining <= 120 s` (`rust_engine/src/live/pipeline.rs:1934`) | canary, logging-only change at the next planned restart | in A1 | ~10 MB/day | as A1 |
| A3-6 | RTDS tapes, venue status transitions (60 s poll), Gamma resolutions (<= 1 call / 5 min, fixed in the unit), daily `candle/state.db` `.backup` (missed) | collector/trim -> `export/collector/YYYY-MM-DD/HH.*`, `export/state/` | hourly / daily | ~1 MB/day | VPS 21 d; PC forever |
| B1 | **L2 mirror, btc-updown-5m only**: ws `book/price_change/last_trade_price` applied on the fly, emitted as frozen schema-v1 `book/chg/trade` (`docs/cross_bot_protocol_v1_finalized.md`); raw frames never written | collector -> `export/collector/l2/<hour>.v1.candles.jsonl.gz` + `.sha256` | hourly, `.tmp.<pid>` -> rename | 1.7-2.1 GB/day (measured 2026-08-25) | VPS 72 h cap (<= 6.5 GB) + df guard; PC >= 90 d |
| C | PMXT tail 2026-08-08T13 -> 08-10T0x (~45 h, ~20 GB), then one HEAD probe/day | r2v2 -> PC `data/pmxt_v2_cache/` -> `distill` -> `data/pmxt_v2_distilled_candles/` | once | 20 GB transient | parquet set <= 60 GB; distilled forever |
| D | Binance 1s/1m, Gamma, Data-API prints (existing refreshers, now on the PC) | public -> PC | 15 min | ~30 MB/day | re-derivable |
| E | Export bundle: `status.json`, ledger, `evidence/`, `candidates/`, `kpi.json`, `health.json`, `DEPLOYED_SHA`, daily `research.sqlite3.bak` x7 | PC -> Mac `~/PolyMomentum_pc_mirror/` | 30 min while the Mac is awake | 5-10 MB | Mac copy is the off-PC backup |

VPS disk budget for us: ~7.5 GB against ~24 GB free after the latency cleanup; free < 20 % pauses B1, < 10 % stops the collector, the trader is never gated. The collector makes **no CLOB REST calls** (A2 comes from the trader) and adds one ws subscription from the trader's IP (missed): the 24 h Tier-A canary watches the trader's `ws_health` reconnect/lag counters before B1 is enabled, and the collector backs off first.

Provenance (missed): every replay report carries `l2_source: pmxt|vps_l2`; a `fresh_range` never mixes sources; B1-derived evidence counts toward promotion only after the parity monitor (5.4) has passed on >= 7 days of B1 hours. Ingested session records pass a schema/range check: the newest VPS `summary_*.json` shows `avg_fill_time_s` ~1.79e12, an epoch-ms leak to fix in the engine and reject on the PC. Prints attributable to our own wallet/orders (session `placed`/`filled`) are excluded from `band_entry_economics` and every fill model, so the public-print null is not contaminated by the canary's own fills.

PC layout (WSL2 ext4, sparse VHDX): `/srv/pm/repo/` is a git checkout at a pinned SHA with state inside it (`resolve_repo_path()`, `scripts/strategy_research_loop.py:158`, refuses out-of-tree paths; `fcntl` locks need ext4); `/srv/pm/vps_mirror/` is 0700, evaluators only, never prompt builders; `/srv/pm/export/` is the Mac-facing bundle. Every disk gate on the PC reads `df /mnt/c`, not the VHDX, which reports its virtual size (missed).

## 4. Control plane

Transport is git plus rsync over the existing Tailscale ssh. No hooks, no broker, no automatic deploy anywhere.

- **Code to the PC**: `deploy/pc-deploy.sh <sha>` = `git push`, then via `ssh mainpc wsl.exe -d Ubuntu-24.04 --exec`: `git checkout --detach <sha>`, `uv sync --frozen`, `uv run --group dev pytest -q`, `cargo build --release --locked -j 8`, `strategy-builder registry-audit`, `systemctl restart pm-factory.timer`. Previous binary kept as `polymomentum-engine.prev`; ledger rows gain `host` and `git_sha`.
- **Config**: `deploy/strategy-research-loop.json` stays fail-closed (`tests/test_strategy_research_loop.py::test_config_is_fail_closed`). The PC overlay is generated from `deploy/pc/loop-config.pc.json`: lanes on, `exact_replay.threads: 8`, `minimum_free_disk_gib: 100` on `/mnt/c`, `sampler_models: []`, `reviewer_model: null`, a `budget` block (5.5), `PMXT_DISTILLED_DIR`.
- **Campaigns**: `deploy/campaigns/<id>.json` in git: `{campaign_id, discovery_cut, lanes, budgets:{proposals, evaluations, replay_hours, wall_clock_days}, alpha:0.05}`. N is bounded before any outcome is read.
- **Grammars and evaluators** change only as Mac-authored code with tests, registered with `grammar_version`/`evaluator_version` and `registered_at`; a candidate accrues fresh evidence only on windows after its version's `registered_at` (registration cut), closing the Claude-sees-results-then-edits-the-grammar channel. The LLM never edits a grammar; the growth signal is the mechanical `grid_edge` note (an elite on an enum boundary) in `kpi.json`.
- **Candidates back to the Mac**: `export/candidates/<fingerprint>/{gate.json, fill_realism.json, ledger_excerpt.jsonl, parity.json}`. `gate.json` keeps the existing fresh-gate shape (`rows[{window_start, signal_entry, won}]`, `verdict`) plus `e_value`, `ebh:{N,k,threshold}`, `l2_source`, `evaluator_version`, `registered_cut`, tripwire flags.
- **Mac verification before deploy** (`scripts/verify_gate.py`, stdlib + `evidence_accrual.py`, no network, inputs untrusted): e-value recomputed from `rows` to 1e-9; `registered_cut < fresh_range[0]`; `fresh_range[1]` <= 72 h old at verify time; `n >= 60` fills over >= 3 UTC days; N recomputed from the ledger excerpt and `e >= N/(alpha*k)`; one `l2_source`; `fill_realism.verdict == FILL_REALISM_CONFIRMED` on VPS-captured hours; `parity.state != drift`; evaluator version cites a passing planted-oracle result. Then the unchanged path: `polymomentum-engine band-promotion-artifact --params ... --gate-artifact gate.json --fill-artifact fill_realism.json --output deploy/promotions/band_promotion_<x>.json`, git commit, rsync `rust_engine/` to `vps:/opt/polymomentum/build_band/rust_engine/`, `nice -n 10 cargo build --release --locked -j 1`, operator install and restart. `release_manifest` gains `promotion_artifact_sha256`; runtime preflight (`rust_engine/src/release.rs:885`) stays a self-consistency check.
- **Acks and heartbeat**: PC -> VPS `export/ack/{pulled.json, heartbeat.json}` over a write-only `rrsync -wo` key; the trim deletes only acked B1 hours (< 7 d) and acked sessions (< 45 d).

## 5. The generator brain on the PC

### 5.1 Model roles on 12 GB VRAM

| Role | Model | Residency | Sees | Produces |
|---|---|---|---|---|
| Proposer | `openai/gpt-oss-20b` (~9.4 GB at ctx 8192) | always; `lms load ... --identifier proposer`, no TTL | grammar enums, EoH operator, structural parents (rank + public aggregates), killed-structure list, gate-name kill taxonomy; **never dates, outcomes, PnL, e-values** | strict-schema JSON re-validated by `normalized_band_rule` / `validate_late_proposal` |
| Novelty | `text-embedding-nomic-embed-text-v1.5` | always | rule text | cosine < 0.97 (`scripts/factory_generator.py:32`) + canonical duplicate + Hamming >= 2, before any evaluation |
| Reviewer (Phase 4, advisory) | `qwen/qwen3.8-27b` (CPU spill, ~84 s/sample) | hourly swap phase, minutes 50-58 | one proposal + killed list | `{aligned, duplicate_of_killed, sanity[]}` into `hypotheses.review_json`; never a gate; off if `reviewer_rejected_s1` is not predictive at n >= 50 |
| Uniform control | none | - | - | strict 50/50 alternation stays; the KPI needs it |

`sampler_models` stays `[]`: per-burst switching is a 12-18 GB cold load every 15 min on this card. Ensembles, bandits, MAP-Elites, a critic and grammar-request channels wait until the demotion rule (5.5) shows the LLM earns the slot and a GPU can hold two models. Residency is kept by `pm-lms-keeper.timer` (2 min) through the idempotent `POST /api/v1/models/load`, `lms.exe` interop as fallback.

### 5.2 Lanes and cascade

`pm-factory.timer` (5 min, oneshot, `Nice=10 MemoryMax=6G TimeoutStartSec=25min`) rotates opportunity -> `late_window_mechanisms` -> `band_mechanisms` as `deploy/factory-runner.sh` does today, minus the LM Link keepalive curls. Band cascade: `registered` -> `band_signal_screen` -> `band_entry_economics` (own prints excluded) -> `band_exact_l2` (new; distilled hours, worst-7 plus best-7 discovery days by realized-vol rank) -> `fresh_accrual`. Late lane unchanged. Stage 0 writes `{look_id = sha256(candidate|stage|fresh_range), campaign_id, information_set, registered_cut, prompt_sha256, source, host, git_sha}` before any outcome is read and skips an existing `look_id`. Today's 278 ledger rows are 60/52/52/52/52 re-logs of four diagnostic candidates across five stage pairs; that stops.

### 5.3 Blindness made structural

`band_signal_records()` receives `{window_start, open, closes}` only; `official` is joined afterwards in `_labelled` (today it rides in the same dict, `scripts/band_lane.py:527`). `tests/test_prompt_information_set.py` builds both prompts from a fixture ledger and asserts only allowed keys appear. `scripts/planted_oracle_test.py`: a rule reading `official` must `KeyError` at the projection layer; a synthetic +10 pp edge must be discovered; 200 label-permutation nulls promote <= 5 %. Its result hash is cited by every `gate.json` (deflation cannot catch leakage, arXiv 2608.27734).

### 5.4 Evidence, e-BH, parity, race

- Per-candidate e-process on windows after `registered_cut`, folded once per fully resolved hour (Gamma closed for every window and the B1 or PMXT hour present on the PC); optional continuation is licensed (Grunwald et al. 2024).
- e-BH (Wang and Ramdas 2022) over the campaign family: rank running e-values, discover the largest k with `e_(k) >= N/(alpha*k)`; replaces the Sidak `family_k` rule of `docs/factory_phase1_spine_2026-09-01.md`. N=1 gives `e >= 20`; N=50 makes a lone survivor need `e ~ 1000`, about 140 fills or three days at ~45 band fills/day. `factory_kpi.py` reports the running threshold. The hand studies behind the champion (`docs/risk_book_v2/margin_floor_study_2026-09-01.md`, `sizing_study_2026-09-01.md`) enter as `stage:"manual_look"` rows (missed).
- Tripwires, failing closed to `MANUAL_AUDIT`: win rate > 0.97 at n >= 50 (existing); fresh minus discovery win rate > +5 pp; entry-VWAP drift vs live > 0.03 for the same `params_hash`; fresh coverage < 0.9.
- Champion parity monitor (global gate): replay the live params on B1-distilled hours against session `filled`/`resolution` rows, rolling 7 d; fill-rate gap <= 10 pp, entry-price gap <= 0.02, outcome agreement 100 %; `parity_drift` blocks every candidate and is how the new L2 source is validated. Later, wire the empirical placed-to-ack latency distribution from `order_timing` + `book_anchor` into `live-replay` instead of the constant 202 ms (missed).
- Champion-challenger (after >= 2 weeks of B1): Choe-Ramdas e-process on the clipped score differential (`EProcess::update_signed` in `evalue.rs` plus the Python mirror) on B1 full-window hours, which cover all four `decision_second` values. Replacement needs `e_diff >= 20` over >= 100 windows plus the challenger's own e-BH discovery, using the fill model that reproduces the champion's fills.

### 5.5 Budgets and demotion

Overlay `budget` per UTC day: `ledger_new_candidates_per_day {band: 24, late_window: 12}`, `llm_samples_per_day 600`, `exact_l2_jobs_per_day 4`, `band_exact_l2_windows_per_day 2000`, `disk_free_gib_floor 100`; exhausted budgets degrade ticks to accrual-only. Demotion: if after n >= 50 per arm the LLM's stage-1 pass rate is not above the uniform arm's Wilson upper bound (`scripts/factory_kpi.py`), the lane runs priors plus uniform only. The band lane has 0 LLM cycles today; this is the first question the PC answers.

## 6. Supervision, security, failure modes

`pm-heartbeat.timer` (5 min) pushes `{ts, last_cycle_ts, lanes, budget_exhausted, disk_free_gib, gpu_resident, newest_l2_hour, clock_skew_s}` to `vps:export/ack/heartbeat.json`. On the VPS `polymomentum-nodecheck.timer` (5 min, sibling of the untouched `deploy/healthcheck.sh`) owns three state files (`pc`, `disk`, `collector`) and is the only new Telegram sender, one line per transition. WSL units `Restart=on-failure`, timers `Persistent=true`; the Mac runs no daemon. Quarterly drill (missed): `sqlite3 .restore` of the Mac backup plus a `factory_kpi.py` comparison; a runbook "rebuild the PC from git + export/ + vps_mirror".

| Failure | Detection | Response | Alert |
|---|---|---|---|
| PC off, reboot, WSL down | heartbeat age > 20 min | VPS keeps 7 d L2, 45 d sessions; WSL task + llmster autostart | `pc: stale` / `pc: ok` |
| Proposer unloaded, LM Studio dead | keeper | reload, `lms server start`; lanes fall back to uniform/grid | `lms_down` via heartbeat |
| Factory stalled | no cycle in `status.json` for 30 min | oneshot timer skips overlap; `cycle.lock` second layer | `pc: factory_stalled` |
| VPS disk | df with hysteresis 80/75, 90/85 | pause B1, then stop collector; trader unaffected | `disk: warn80` / `crit90` |
| Collector crash, ws lag | `Restart=always`; no records 10 min; lag counter | partial hour sealed as `<hour>.partial.v1...` | `collector: down` / `lagging` |
| Two writers of factory state | `logs/strategy-research/OWNER` checked by both runners | second writer exits 0 | none |
| Clock drift, unacked deletion | heartbeat `clock_skew_s` > 2; trim finds B1 > 7 d unacked | chrony; delete and alert | `pc: clock_skew`, `collector: unacked_delete` |

Security: secrets stay in `/etc/polymomentum/*`. The PC holds two forced-command keys for user `pmsync` (nologin, `AllowTcpForwarding no`): `restrict,command="/usr/bin/rrsync -ro /opt/polymomentum/export"` and `... -wo /opt/polymomentum/export/ack"`; `export/{sessions,promotions,soak}` are read-only bind mounts. VPS -> PC, VPS -> Mac and PC -> VPS outside `export/` are unreachable by design; the VPS joins no tailnet (shared box). Collector unit: no `EnvironmentFile`, `ProtectSystem=strict`, `ReadWritePaths=/opt/polymomentum/export/collector`, `CPUQuota=15% MemoryMax=192M Nice=10`. LM Studio stays bound to `127.0.0.1`; LM Link goes off once the tunnel exists. A test asserts prompt builders reference no `vps_mirror` path. Windows sshd: `PasswordAuthentication no`, firewall scoped to `100.64.0.0/10`. VPS binaries built in WSL must pin `-C target-cpu=x86-64-v2`, never `native` (missed).

## 7. Migration plan

### Phase 0: Mac only, zero live risk (this week)

1. `brew install rsync` (macOS ships openrsync); `ssh -f -N -L 1235:127.0.0.1:1234 mainpc`; `POLYMOMENTUM_LLM_BASE_URL=http://127.0.0.1:1235/v1 uv run python scripts/llm_bench.py` must return one completion. Add `Host mainpc-llm` with `LocalForward` to `~/.ssh/config`. LM Link leaves the execution path.
2. PC, admin PowerShell, no reboot: `powercfg /x standby-timeout-ac 0`; `lms load openai/gpt-oss-20b --gpu max --context-length 8192 --identifier proposer`; `lms load text-embedding-nomic-embed-text-v1.5`; disable Ollama autostart. Confirm Intune/GPO cannot override `powercfg`, force reboots, or block Tailscale/OpenSSH (missed).
3. Rigor patches, each a small PR with tests on `codex/audit1`: ledger `look_id` idempotency in `factory_generator.append_trial_entry`; label projection in `band_signal_records()` plus `scripts/planted_oracle_test.py`; `tests/test_prompt_information_set.py`; `budget` block and demotion rule; `scripts/verify_gate.py`, run once against `logs/strategy-research/20260821_fresh_gate_public_v1.json` to reproduce its e-value; `release_manifest.promotion_artifact_sha256`; the `avg_fill_time_s` fix; `#[cfg(unix)]` around `tokio::signal::unix` at `rust_engine/src/main.rs:6890, 13381, 14058`. Verify: `uv run --group dev pytest`, `cargo test`.
4. `scp vps:/opt/polymomentum/promotions/band_promotion_margin50.json deploy/promotions/` and commit.
5. Repo skeleton, uninstalled: `deploy/pc/` (six `pm-*.{service,timer}` pairs, `factory-tick.sh`, `keeper.sh`, `pull.sh`, `export.sh`, `heartbeat.sh`, `wsl.conf`, `wslconfig`, `loop-config.pc.json`, `probe.ps1`), `deploy/pc-deploy.sh`, `deploy/pc-pull.sh`, `deploy/polymomentum-{collector,trim,nodecheck}.{service,timer}`, `deploy/polymomentum-nodecheck.sh`, `deploy/campaigns/2026-09_band_v2.json`. `OWNER` guard and a repo-relative `cd` in `deploy/factory-runner.sh`.
6. VPS housekeeping (user OK first): archive then `rm -rf /opt/polymomentum/logs/latency` (+1.5 GB); fix or mask the failed `polymomentum-soak-report.service` (missed). Drop `deepseek-v4-flash-0731` from `scripts/sampler_sweep.py` and the runner comment until located.

### Phase 1: PC becomes the single factory writer (one evening, one reboot)

1. PC: `wsl --install -d Ubuntu-24.04`; reboot; `.wslconfig` (`memory=10GB processors=8 swap=0 networkingMode=mirrored sparseVhd=true`); `/etc/wsl.conf` (`[boot] systemd=true`); scheduled task `PolyMomentum-WSL` running `wsl.exe -d Ubuntu-24.04 -- sleep infinity` at startup and logon, "run whether user is logged on or not". Install `llmster` (`lms daemon up`) under NSSM now, not in Phase 4, so models survive a reboot without RDP. **Acceptance test** (missed): reboot without logging in; from WSL `curl -s localhost:1234/api/v0/models` lists `proposer`; `pm-factory.timer` fires. NAT fallback if the Red Shield VPN breaks mirrored networking.
2. WSL: `apt install rsync git build-essential pkg-config chrony jq sqlite3`, rustup, `uv`, `useradd pm`; clone to `/srv/pm/repo`, `git checkout --detach <sha>`, `uv sync --frozen`, `cargo build --release --locked -j 8`; `registry-audit` passes.
3. Handover: Mac `launchctl bootout gui/$(id -u)/com.polymomentum.factory-runner`; wait for `locks/cycle.lock`; `rsync -az logs/strategy-research/` and `data/pmxt_v2_cache/` (9.9 GB) to the PC; `echo mainpc > logs/strategy-research/OWNER` there; on the Mac `mv logs/strategy-research logs/strategy-research.retired-<date>`; apply `loop-config.pc.json`.
4. `systemctl enable --now pm-lms-keeper.timer pm-factory.timer pm-export.timer pm-heartbeat.timer`; verify three cycles advance, ledger rows carry `host: mainpc`, zero duplicate `look_id`s, `deploy/pc-pull.sh` mirrors `export/`, `factory_kpi.py` on the mirror matches. Rollback: disable the timers, rsync state back, `OWNER` = Mac, `launchctl kickstart`.

### Phase 2: data on the PC

`pm-l2-ingest.timer`: PMXT tail backfill (`pmxt-download --start 2026-08-08T13 --end 2026-08-10T06 --cache-dir data/pmxt_v2_cache`), `distill` each hour, `sources.jsonl` manifest (`pmxt|vps_l2`, sha256), parquet pruned to <= 60 GB, daily HEAD probe. Seal the 08-08 to 08-10 opportunity block and reproduce one stored Mac `opportunity-exact-replay` report on the PC. Optionally move `~/PolyMomentum_capture_archive` (18 GB) to `/srv/pm/archive/`, keeping the 2026-08-25 raw-frame segment as the Phase 3 fixture.

### Phase 3: VPS export, collector, book_anchor (trader untouched until 3c)

- 3a: `useradd -r -s /usr/sbin/nologin -d /opt/polymomentum/export pmsync`; the two rrsync keys; `Match User pmsync`; ro bind mounts; `export/{collector,ack,state}`; install trim and nodecheck; PC pull and heartbeat timers. Cross-bot note `/opt/shared/cross_bot_notes/<date>_pmsync_and_collector_from_polymomentum.md`, mirrored in `docs/`.
- 3b: `polymomentum-engine collect --out-dir /opt/polymomentum/export/collector --assets btc --tiers a[,b]`, reusing `polymarket_ws`, the RTDS recorder, the Gamma client and the v1 writer. Offline acceptance first: replay the archived 2026-08-25 `market_ws_frames.jsonl` through the streaming converter and match that segment's `converted/<hour>.v1.candles.jsonl.gz` (byte-diff, else identical per-token event sequence). Then a 24 h Tier-A canary at `CPUQuota=15%` watching the trader's `cycle` latency and `ws_health`; then B1 with the df guard; PC ingest verifies sha256 and runs `harness` on one fresh hour.
- 3c: `book_anchor` logging plus a unit test, shipped by the standard recipe at the next planned restart; verify a new `session_*.jsonl` contains `"type": "book_anchor"`. Parity starts at the first pulled B1 hour; no B1-based evidence counts until seven parity-clean days.

### Phase 4: hardening, in order of value

Async reviewer phase and measure it; Windows Update gpedit policy; champion-challenger race; VPS binaries built in WSL (glibc 2.39 both sides, `target-cpu=x86-64-v2`, `preflight` plus manifest sha before install); B2 (ETH/SOL) only with a measured disk budget; Tailscale on the VPS only after a peer note.

### What leaves the Mac

Retired: the `com.polymomentum.factory-runner` agent (booted out; `deploy/factory-runner.sh` stays as the fallback), the LM Link keepalive curls, `deploy/local/com.polymomentum.strategy-research.plist.in`, `scripts/install_strategy_research_launchd.py`, `deploy/polymomentum-strategy-research.{service,timer}`. Moved after verified copies: `logs/strategy-research/` (authority to the PC; the Mac keeps `~/PolyMomentum_pc_mirror/` outside the repo), `data/pmxt_v2_cache/` (9.9 GB), the 18 GB capture archive, the Mac-side `margin_study_cache`, `band_lane_cache`, `fresh-gate-public`. Stays: code authority, evidence freezing, registry marks, promotion artifacts, VPS deploys.

## 8. Open questions for the user

1. WSL2 on the PC (install plus one reboot, 10 GB RAM cap, VHDX on C:), or native Windows (signal patches, MSVC toolchain, `fcntl` shim, Task Scheduler, permanent divergence from the VPS)?
2. Is the PC managed by Intune/GPO (Enterprise edition, corporate VPN)? Can it run 24/7 with sleep off, is it used interactively (games, other GPU work), and is the Red Shield VPN ever active while the factory runs?
3. PC disk: OK to allocate ~200 GB of C: (60 GB parquet working set, 90 d of L2 at ~2 GB/day, archive)? Second drive? Residential bandwidth for a 20 GB backfill plus ~2 GB/day, and any VPS traffic cap?
4. Approve the host-level VPS changes (`pmsync` user, two forced-command keys, ro bind mounts, three new units, cross-bot note) and the logging-only `book_anchor` trader change at the next planned restart?
5. Enable B1 (btc L2 mirror, ~2 GB/day) at all; later ETH/SOL (x3)? Retention 72 h L2 on the VPS, 21/45 d sessions, 90 d L2 on the PC?
6. Reviewer on 12 GB: hourly swapped qwen3.8-27b (advisory), gemma4-12b, or none until a bigger GPU? Where is `deepseek-v4-flash-0731`?
7. e-BH family: all registered candidates in a campaign (default) or only those reaching fresh accrual? Register the hand studies behind margin50 as `manual_look` rows now?
8. Prompt policy: keep parents' public aggregates (`accuracy`, `wilson_lower`, `mean_net`) in the proposer prompt, or enforce rank-only from day one?
9. Turn LM Link off once the tunnel works? Delete the Mac's PMXT cache (9.9 GB) and capture archive (18 GB) after verified copies on the PC? Budget defaults (24 band / 12 late candidates per day, 600 samples per day): keep, or grow the e-BH pool more slowly?

## 9. References

- Repo: `CLAUDE.md` sections 5-7; `docs/strategy_research_loop.md`; `docs/factory_phase1_spine_2026-09-01.md`; `docs/factory_upgrade_2026-09-02.md`; `docs/hypothesis_factory_research_2026-09-01/`; `docs/risk_book_v2/`; `docs/cross_bot_protocol_v1_finalized.md`; `docs/candle_universe.md`; `rust_engine/src/backtest/{evalue,distill,pmxt}.rs`; `rust_engine/src/live/pipeline.rs`; `rust_engine/src/release.rs`; `rust_engine/src/monitoring/session.rs`.
- Ramdas, Grunwald, Vovk, Shafer, "Game-theoretic statistics and safe anytime-valid inference", Statistical Science 2023. Wang, Ramdas, "False discovery rate control with e-values", JRSS-B 2022. Grunwald, de Heide, Koolen, "Safe Testing", JRSS-B 2024. Waudby-Smith, Ramdas, "Estimating means of bounded random variables by betting", JRSS-B 2024. Choe, Ramdas, "Comparing Sequential Forecasters", Operations Research 2023.
- "What survives honest evaluation?", arXiv 2608.27734; "Mutation Without Variation", arXiv 2606.05408; "Why LLMs Aren't Scientists Yet", arXiv 2601.03315; EoH (ICML 2024); ShinkaEvolve (arXiv 2509.19349); CodeEvolve (arXiv 2510.14150).
- LM Studio docs (headless `llmster`, `lms load` without TTL, LM Link bug #2184); Microsoft WSL docs (`wsl.conf` systemd, mirrored networking); Tailscale unattended mode; rrsync(1).
