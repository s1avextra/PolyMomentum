# CLAUDE.md

PolyMomentum: a Rust engine (`rust_engine/`) trading Polymarket btc-updown-5m windows on a 2-core VPS
(`polymomentum-band-canary.service`), plus a Python evaluator and factory (`scripts/`) on the Mac.
Current truth and roadmap: `docs/profitability_basement_2026-09-18.md`. Older design docs were deleted from
the working tree; read them with `git show 0cd8b0d:docs/<path>`.

## 1. How to work

- Think first: state assumptions, surface tradeoffs, ask when unclear. Present alternatives, do not pick silently.
- Surgical minimal changes: every changed line traces to the request; no speculative abstractions; match
  existing style; mention unrelated dead code, delete it only when asked.
- Goal-driven: turn each task into a verifiable check (test, replay, byte-diff) and loop until it passes.
- Python dependency management is `uv` only (`uv run`, `uv sync`); never pip.
- Commit at phase boundaries with a clear message and push to `codex/audit1`; never force-push; never commit
  operator-made deletions or state changes without confirmation. Legacy docs were removed on purpose (7b4a39e).
- Never `cargo build --release` from an agent session. Verify on the Mac with `cargo test` and the debug
  binary; the operator builds on the VPS with `nice -n 10 cargo build --release -j 1`, one build at a time.

## 2. Money and safety (non-negotiable)

- Never touch wallet keys, `/etc/polymomentum/*secrets*`, `KILL_BAND`/`KILL_OBSERVER`, `state.db` breaker
  keys, `BANKROLL_USD` or breaker floors. Those are operator actions with a written post-mortem.
- No funding before a promoted artifact exists whose entry-level Wilson lower bound clears break-even at its
  cap on executable (ladder) rows. The $19 -> $2.78 outcome repeats at any size without that.
- Sizing never clamps up to the venue minimum: skip when 0.5 x f_lo x equity < $5. Equity is the RiskBook v2
  wallet-anchored book, never a pinned number.
- Breaker trips HOLD across restarts; the kill switch is checked ahead of the feed gate; the breaker
  consumes Gamma-final outcomes only.
- Paper mode (`live --mode paper`) is the observer: no orders, no CLOB client, own state/session dirs.
- Any change to `execute_trade`, `record_live_fill_position`, `handle_user_event_from`,
  `handle_missing_rest_order` or `kelly_lo_stake` ships with a test.
- Live artifacts are hash-bound through `POLYMOMENTUM_PROMOTION_ARTIFACT`; never pin one on the command line.

## 3. VPS coexistence

- The VPS (ssh alias `vps`) may be unreachable at times; when it is, say so and write every VPS step as an
  operator recipe. Read-only ssh (journal, session files, sqlite) is allowed.
- Never write to the VPS from an agent session. Transfers are pull-only (rsync into
  `logs/band-canary-mirror/`).
- The box is shared with adgts and polyarbitrage: never read their dirs (`/opt/polyarbitrage/*`), never
  delete a parquet you did not download, never run CPU work > 30 s there, never two release builds.
  Coordination goes through `/opt/shared/cross_bot_notes/` only, mirrored into `docs/`.
- One process at a time on the trading wallet: the observer while halted, the trader after promotion.
- Shared files are written atomically: `*.tmp.<pid>` then `rename(2)`; no lockfiles.

## 4. Validation

- Backtest-first: replay, parity and diagnostics before paper; paper only for what offline cannot prove
  (venue acks, websocket behaviour, unit wiring), bounded, with diagnostics on.
- Fresh windows: promotion evidence must include the newest fully resolved windows; a candidate that works on
  one slice is not sound.
- Executability: entry price is the worst (FOK limit) price at the budget. The engine's `band_ladder` record
  is the truth, public prints are an upper bound, signal accuracy is a ceiling. Never gate signal accuracy
  against entry break-even.
- Fee: pinned from realized fills; one constant, one place.
- Every cell reports its excluded populations (no-print, out-of-band, never-cleared); a cell whose excluded
  population wins more is adverse-selected.
- Oracle noise per final-margin bucket bounds every cell and is reported next to it.

## 5. Evidence discipline

- Public tape: paginated Data-API only (`fetch_data_api_trades`), writer v2 ordering, decision-second
  prints excluded. A single `/trades?limit=500` page is a known, outcome-correlated truncation artifact.
- Pre-register the family (`deploy/campaigns/*.json`, N fixed) before any outcome is read; accrue only on
  windows after `registered_at`; e-BH at alpha 0.05; one ledger row per (fingerprint, look_id); tripwires
  fail closed to `manual_audit`.
- Grammar and evaluator changes are versioned code with tests and reset `registered_at`.
- Label projection stays blind: proposers and signal records never see `official`.
- Be skeptical of too-good numbers: WR > 0.995 at n >= 100, a fresh-vs-discovery jump, or a survivor
  without an excluded-population report is a defect until audited.

## 6. Tests and the green-tree rule

- The factory runner re-imports `scripts/*.py` every 5 min: every Python step lands importable with the
  suite green.
- Python: `uv run python -m unittest tests.test_strategy_research_loop tests.test_band_lane
  tests.test_evidence_accrual tests.test_factory_kpi tests.test_band_shadow_race tests.test_executable_truth`
  (174 OK; the runner never imports `executable_truth.py`, so only this command catches its regressions).
- Rust: `cd rust_engine && cargo test -p polymomentum-engine` (677 on the WIP tree, 674 at HEAD);
  `cargo clippy --no-deps` real warnings <= 9 and never increasing.
- Engine deletions only after their replacement runs, one step per change, `git tag pre-basement` first.

## 7. Where the truth lives

- `docs/profitability_basement_2026-09-18.md`: audit, design, roadmap, first implementation slice.
- Memory: `~/.claude/projects/-Users-ttoomm-Documents-PolyMomentum/memory/` (read `project_state.md` first).
- Ground truth (verified 2026-09-07/08): wallet $19 -> $2.78 (35W/12L at ~0.80 plus an untracked $5.18
  fill); the band (0.55,0.92] has no executable edge; signal accuracy is 98.5% at |margin| >= $50; the only
  executable edge is at asks >= 0.92 with |margin| >= 75, +1 to +3.5%/USD, 5-25 trades/day.
