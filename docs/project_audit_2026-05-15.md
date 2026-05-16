# Project audit - 2026-05-15

Branch: `codex/audit1`

## Scope

This pass audited the Rust engine, local generated data, and read-only VPS
runtime evidence. It did not read peer private directories and did not delete
shared PMXT parquet/cache files.

## Findings and fixes

- Static quality: `cargo clippy --all-targets -- -D warnings` had accumulated
  dead-code and maintainability warnings. The pass removed or test-gated unused
  helpers, simplified repeated config mutation, avoided a large enum variant,
  and fixed smaller clippy issues without changing strategy economics.
- Shared cache default: PMXT v2 cache selection is now centralized. Commands use
  `PMXT_V2_CACHE_DIR`, then the VPS shared cache at `/opt/shared/pmxt_v2_cache`
  when present, then the local `data/pmxt_v2_cache` fallback.
- Replay diagnostics: cached live-replay reports now surface fill totals, fees,
  cost, slippage, book age, and tracked-token counts that were already computed
  but not exported.
- Runtime strategy provenance: session logs now include the full runtime
  `zone_config`, `min_confidence`, `min_edge`, and `skip_dead_zone` values.
- Replay parity: `validate-replay` now reconstructs the actual runtime strategy
  from the session's `system.runtime_strategy` event. For existing promoted VPS
  sessions it can load the referenced promotion artifact, apply settlement
  floor fields, and account for the settlement-shadow gate when
  `CANDLE_SETTLEMENT_ALIGNMENT_READY=false`.
- Current VPS replay proof: the copied VPS session
  `session_20260515_145845.jsonl` validates locally with the new code at
  `total=71573 mismatches=0 (0.00%)` after mapping the promotion artifact path
  to the copied local artifact. The previous soak failure was validator drift,
  not evidence of live decision nondeterminism.

## Data cleanup

Removed only local, project-owned generated artifacts:

- `rust_engine/target/` via `cargo clean`
- stale local `logs/paper_session.log` from 2026-04-06
- one orphan PMXT sidecar temp file under `data/pmxt_cache`
- `data/.DS_Store`

Retained `data/pmxt_cache` parquet and sidecar files because they are reusable
research/cache inputs, and there is no ownership-safe proof that they are
disposable.

## VPS state

Read-only check on the VPS showed:

- `polymomentum-engine`, `adgts`, `polyarbitrage`, and
  `polyarbitrage-collector` active.
- Current root filesystem around 88% used after prior cleanup, still high but
  not an emergency. Further cleanup should stay coordinated through
  `/opt/shared/cross_bot_notes/`.
- Latest deployed binary is still `ac85da9`; the replay-validator fix in this
  audit needs a Linux artifact deploy before the VPS soak report can turn green
  on the same session.
- Wallet remains not live-ready: observed pUSD, allowances, and POL were zero
  in the latest soak report.

## Grade

Current state after this audit: **A-** locally, **B+/A-** on the VPS until the
new validator/runtime-strategy logging is deployed and a fresh soak report is
generated.

A+ requires all of these at the same time:

1. New Linux artifact deployed in paper mode.
2. Fresh soak report with diagnostics `ok=true`, replay exit `0`, peer services
   active, and no unexpected resource contention.
3. Settlement-shadow sample large enough to flip
   `CANDLE_SETTLEMENT_ALIGNMENT_READY=true` with zero actionable oracle drift.
4. Wallet live preflight satisfied with pUSD, CTF Exchange V2 allowances, and
   POL gas.
5. Disk pressure reduced or explicitly accepted through cross-bot orchestration.

## Next production loop

1. Commit and push this branch so CI can produce the Linux artifact.
2. Deploy that artifact to the VPS in paper mode only.
3. Run `soak-report.sh` immediately and confirm replay parity is `0` on the
   existing session, then again after fresh data accumulates.
4. Continue paper until enough resolved shadow samples exist for the settlement
   alignment gate.
5. Run CPU-heavy backtest/sweep work on the dev machine, not the VPS, exporting
   only reports/artifacts needed for promotion.
6. Convert/fund to pUSD and set allowances before any live preflight.
