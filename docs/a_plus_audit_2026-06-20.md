# PolyMomentum A+ Audit - 2026-06-20

## Scope

Evidence-first audit of local repository state, local artifacts, VPS runtime, VPS data/storage, and the current strategy/inventory/execution architecture.

No peer private directories were modified or directly inspected. VPS checks were read-only. Peer-bot coordination remains through `/opt/shared/cross_bot_notes/`.

## Current Verdict

PolyMomentum is not A+ yet.

Current grade:

- Core Rust code and tests: A-
- Offline execution/inventory model: A-
- Strategy discovery loop: A- as a research tool
- Current live/paper strategy evidence: B / fail-closed
- VPS deployment freshness: C+
- VPS operational resilience: C+ after the June 17 disk-full incident
- Overall project state: B+

The most important finding is that local code has moved past the VPS deployment. Local branch `codex/audit1` is at `f78b32a`, while the VPS paper service is running binary git SHA `29c739a` built on 2026-06-04 with the older May 31 promotion artifact.

## Local State

Branch:

- `codex/audit1`
- synced to `origin/codex/audit1`
- HEAD: `f78b32a Fix simulated bankroll and locked inventory accounting`

Worktree:

- Dirty files remain, mostly formatting drift in existing Rust files plus `.DS_Store`.
- These were not touched by this audit.
- Untracked `docs/canary_orderflow_handoff_2026-05-17.md` exists.

Tests:

- `cargo test --manifest-path rust_engine/Cargo.toml`
- Result: 288 passed, 0 failed.

Local storage:

- Repo total: 18G
- `rust_engine/target`: 15G
- `logs`: 2.5G
- `logs/experiments`: 1.3G
- `logs/strategy_builder`: 760M
- `logs/soak_evidence`: 262M

Local storage is not dangerous, but it is untidy. The 15G Rust target directory is cleanable on the dev box when needed. Do not mirror this build cache to the VPS.

## Strategy Evidence

The older May 23-25 candidate looks attractive in isolation:

- 157 trades
- 139 wins / 18 losses
- PnL about +81.21 after active-bankroll parity
- live-replay parity passed exactly: trades delta 0, PnL delta 0, fees delta 0
- 0 fill failures
- 0 oracle disagreements
- 0 causality violations

But the newer extended selected-profile replay from June 8 is the stronger evidence:

- 30 folds
- 726 trades
- 589 wins / 137 losses
- PnL +47.31
- fees +54.47
- profitable folds: 16 / 30
- losing folds: 14 / 30
- breaker folds: 2
- first 9 folds: 221 trades, +63.40
- remaining 21 folds: 505 trades, -16.10
- robust promotion rejected: profit factor 1.0668 below 1.20, worst fold -22.32, negative causal buckets remained.

This means the candidate is real enough to study but not stable enough to trade. The system must learn when to abstain, not just how to find a high win-rate direction.

## Algorithm / Math Structure

Current strategy path:

1. Multi-exchange BTC/ETH/SOL feeds update `PriceState`.
2. `MomentumDetector` computes z-score, confidence, consistency, and reversion count.
3. `decide_candle_trade` gates by zone, confidence, z, EV/edge, price, settlement margin, and microstructure.
4. Causal tags become `DecisionRegime`.
5. Harness/live-replay/strategy-builder can apply selectivity filters to the causal tags.
6. Risk uses active bankroll, per-market caps, total exposure caps, and stressed drawdown headroom.
7. Positions settle at Polymarket/CTF resolution, with tie payout handled as half redemption in the newer local code.

Strengths:

- Feed-forward selectivity exists and has tests against future-luck promotion.
- Decision regime tags are causal and reusable across backtest/live-replay.
- Robust promotion gates include Wilson lower bound, PBO, neighbor stability, worst-window PnL, profit factor, and causal bucket diagnostics.
- Backtest/live-replay parity exists for the May 23-25 active-bankroll slice.
- Local inventory/accounting now models locked exposure and fresh simulated bankroll.

Weaknesses:

- Current candidate over-earns in some regimes and bleeds in others.
- The selector is still mostly one-dimensional or manually chosen; it is not yet a robust meta-policy.
- Promotion evidence is split between older passing windows and newer failing extended windows.
- A+ should not accept labels like `promotion_passed` from older artifacts unless they pass the latest locked-inventory and tail-stability gates.

## VPS Runtime State

VPS:

- hostname: `vps`
- checked at: 2026-06-20T09:45 UTC
- root disk: 72G total, 57G used, 13G free, 82 percent used

Services:

- `polymomentum-engine`: active
- `polymomentum-telegram-monitor`: active
- `polymomentum-healthcheck`: inactive
- `polymomentum-soak-report`: inactive
- `adgts`: active
- `polyarbitrage`: active

PolyMomentum process:

- `/opt/polymomentum/polymomentum-engine live --mode paper --promotion-artifact /opt/polymomentum/config/promotion_candidate_a_plus5m_guard_may23_25_20260531.json`
- binary git SHA: `29c739a`
- build timestamp: 2026-06-04T10:38:00Z
- mode: paper
- venue: paper_only
- resource controls: Nice=5, CPUQuota=80%, MemoryMax=512M
- current CPU about 6-7 percent during audit
- memory about 32M RSS

The process is currently stable and emits session events, but it is not running the latest local branch.

## VPS Inventory / Accounting State

Current SQLite state on VPS:

- `bankroll_baseline`: 2345.949385642335
- `total_pnl`: 10.343652800000003
- `total_fees_paid`: 161.4026
- positions: 0
- paper_positions: 0
- oracle_pending: 0
- trades: 1739

This is not the intended fresh `$100` simulated model from local `f78b32a`. The VPS is still using the older accounting startup behavior, so VPS paper data after June 4 should be treated as operational telemetry, not as A+ strategy validation.

## VPS Data / Disk State

PolyMomentum:

- `/opt/polymomentum`: 3.0G
- `/opt/polymomentum/logs`: 2.2G
- `/opt/polymomentum/logs/sessions`: 2.2G
- `/opt/polymomentum/build_src_982be12`: 608M
- `/opt/polymomentum/data`: 97M
- `/opt/polymomentum/polymomentum-engine`: 20M
- previous binary: 20M

Shared:

- `/opt/shared`: 224K
- PMXT shared cache is effectively empty now.

System:

- `/var/log`: 32G
- `/var/log/journal`: 510M

PolyMomentum session logs contain several very large uncompressed files:

- `session_20260604_104510.jsonl`: 190M
- `session_20260606_040904.jsonl`: 741M
- `session_20260610_064109.jsonl`: 682M

There are also many zero-byte session and summary files from June 15-17, caused by restart churn during the disk-full incident.

## VPS Outage Root Cause

On June 17 the box hit `No space left on device`.

Observed effects:

- PolyMomentum repeatedly failed startup with `pipeline init failed: disk I/O error`.
- Restart counter reached at least 160.
- PolyArbitrage collector failed with `No space left on device`.
- ADGTS failed to persist state due to `No space left on device`.

This was not a PolyMomentum-only bug. It was a VPS-wide storage failure that affected all bots. A+ requires global disk watermarks and cross-bot log retention, not only PolyMomentum cleanup.

## Critical Gaps

1. VPS is stale.
   - It runs `29c739a`, not `f78b32a`.
   - It lacks the fresh `$100` simulated reset and locked-inventory fixes.

2. VPS is using an old strategy artifact.
   - Current service artifact is May 31.
   - Newer June 8 extended evidence rejected promotion.

3. Disk resilience is insufficient.
   - Systemd restarted into a disk-full storm.
   - Session logging can create large uncompressed JSONL files.
   - There is no visible start-time disk watermark gate that stops safely before SQLite/session open.

4. Strategy is not robust enough.
   - Aggregate positive PnL hides poor fold stability.
   - High win rate can still lose money after fees, price asymmetry, and tail losses.

5. Promotion evidence is fragmented.
   - Some artifacts say A+ on older slices.
   - Latest stricter evidence says fail-closed.

## A+ Roadmap

### Phase 1 - Freeze Promotion Truth

Goal: prevent stale or contradictory artifacts from being treated as live candidates.

Actions:

- Mark the May 31 VPS promotion artifact as `research_only` until it re-passes latest gates.
- Add a promotion artifact freshness check: deployed artifact must cite latest accounting model version and latest robust gate run.
- Add a release manifest field for `inventory_model_version`.
- Gate paper/live startup if the artifact predates the current inventory model unless `--allow-stale-research-artifact` is explicitly set for paper-only diagnostics.

A+ gate:

- Runtime refuses stale promotion artifacts by default.
- Paper diagnostics can run stale artifacts only when explicitly marked research-only.

### Phase 2 - Deploy Current Binary Safely

Goal: get VPS paper onto the same code that passed local tests.

Actions:

- Deploy `f78b32a` or newer during a low-load window.
- Keep mode `paper`.
- Do not change peer bot services.
- Before restart, verify peers are not deactivating.
- After restart, verify release manifest SHA, `$100` simulated baseline, empty simulated positions, and no stale oracle pending state.

A+ gate:

- VPS paper state starts from `$100` baseline.
- Session state shows no restored old simulated inventory.
- Resource controls remain active.

### Phase 3 - Disk Guard and Log Retention

Goal: no bot should enter a restart storm because the shared VPS fills disk.

Actions:

- Add PolyMomentum preflight disk checks:
  - fail if free disk below 10G or below 15 percent
  - fail before opening SQLite/session writers
  - return a distinct non-restart exit code if possible
- Add systemd start-limit protection:
  - `StartLimitBurst=3`
  - `StartLimitIntervalSec=10min`
  - consider `RestartPreventExitStatus` for disk-watermark failures
- Add session log rotation:
  - compress closed JSONL sessions
  - cap active session size
  - keep summaries separate from raw event logs
- Coordinate cross-bot retention via `/opt/shared/cross_bot_notes/`.

A+ gate:

- Disk cannot fall below warning threshold without Telegram alert.
- Disk cannot fall below critical threshold without stopping new diagnostics.
- Restart storm cannot exceed a small bounded count.

### Phase 4 - Rebuild Strategy Gate on Latest Accounting

Goal: find a strategy that survives the newer locked-inventory model and extended tail.

Actions:

- Use only backtest/live-replay for validation unless a venue-only behavior is being tested.
- Generate per-trade `trade-features-json` for recent folds.
- Train a feed-forward meta-selector on prior folds only.
- Candidate feature families:
  - direction
  - z bucket
  - confidence bucket
  - price bucket
  - edge bucket
  - reversion bucket
  - book spread/depth/pressure
  - minutes remaining
  - recent BTC trend state
  - recent fold-level realized expectancy
- Start with interpretable rules, then add online models only after enough sample:
  - causal interaction rules
  - monotone logistic/meta-label filter
  - BOCPD-style drift alarm for monitoring, not direct promotion

A+ gate:

- At least 14 days of feed-forward folds, preferably 30.
- At least 500 resolved OOS trades, or 100+ if the strategy is intentionally sparse and worst-window is non-negative.
- Profit factor at least 1.20 after fees.
- No negative causal bucket with at least 50 trades.
- Worst active 8h fold non-negative, or a documented bounded-loss rule with strict daily drawdown.
- PBO <= 0.50.
- Median OOS percentile >= 0.80.
- Neighbor positive rate >= 0.60.
- No breaker trips.

### Phase 5 - Replay/Paper/Canary Boundary

Goal: keep validation cheap and realistic.

Actions:

- Backtest first for strategy, sizing, PnL, resolution, fill model, and stale-strategy checks.
- Live-replay for exact feed/order/session equivalence.
- Paper only for irreducible live behavior:
  - websocket health
  - Gamma/market discovery freshness
  - process supervision
  - Telegram monitoring
  - real venue acks/rejects in canary
- No canary until latest artifact passes the full locked-inventory promotion gate.

A+ gate:

- Backtest and live-replay match on trade count, fees, PnL, and resolutions.
- Paper produces the same decision/no-decision behavior on a bounded diagnostics run.
- Canary is only for venue mechanics, not strategy discovery.

## Immediate Next Moves

1. Do not go live.
2. Deploy latest local binary to VPS paper only after a peer-safe check.
3. Add disk watermark preflight and systemd restart limits.
4. Compress or archive old PolyMomentum session logs; coordinate peer `/var/log` cleanup via cross-bot notes.
5. Re-run a fresh 14-30 day atomic rolling-history gate with locked inventory.
6. Build the feed-forward meta-selector over `trade-features-json`.
7. Promote only if the new selector passes latest A+ gates on fresh data.

