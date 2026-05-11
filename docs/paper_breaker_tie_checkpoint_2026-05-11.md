# Paper breaker/tie checkpoint

**Date:** 2026-05-11
**Branch:** `codex/audit1`
**Commit:** `dacdd21` (`Expose breaker and tie diagnostics`)

## What changed

- Added `risk.breaker` session events so restored/tripped circuit-breaker state
  appears in JSONL diagnostics instead of looking like silent starvation.
- Added diagnostics fields/warnings for breaker state and Polymarket tie
  resolutions.
- Made CTF `tie` outcomes explicit in oracle PnL handling: ties are treated as
  losses for a directional paper fill, and an observed tie trips/stops paper/live
  for investigation.
- Added cross-bot VPS disk cleanup orchestration/result notes and mirrored them
  into `docs/`.

## Verification

Local:

- `cargo test` in `rust_engine`: `140` tests passed in both lib and main test
  binaries, plus doc-tests.
- GitHub Linux release workflow for `dacdd21`: completed successfully.
- Downloaded artifact SHA256:
  `272f0f7c499bc4c612b2e0c72dfe6e9dcdd12a39cfd6ff0d009d962b89cc0320`.

VPS deployment:

- Deployed `/opt/polymomentum/polymomentum-engine` in paper mode.
- Release manifest on VPS reports git SHA
  `dacdd2113f9592dd19c4e70c0e43b1071310df71`.
- Remote preflight: `ok=true`, paper mode, promotion artifact valid.

Clean paper reset:

- Stopped only `polymomentum-engine`.
- Backed up state DB to:
  `/opt/polymomentum/logs/candle/state.db.bak.paper_breaker_tie_clean.20260511T021702Z`
- Cleared PolyMomentum paper tables only:
  `trades`, `positions`, `cooldowns`, `paper_positions`, `oracle_pending`,
  and the `candle_breaker_tripped` meta key.
- Reset `state.total_pnl=0.0` and `state.total_fees_paid=0.0`.
- Restarted `polymomentum-engine`.

Peer/service checks after reset:

- `adgts`: active
- `polyarbitrage`: active
- `polyarbitrage-collector`: active
- `polymomentum-engine`: active
- PolyMomentum systemd limits unchanged:
  `CPUQuotaPerSecUSec=800ms`, `MemoryMax=536870912`, `TasksMax=256`

Fresh paper diagnostics:

- Session:
  `/opt/polymomentum/logs/sessions/session_20260511_021702.jsonl`
- Diagnostics after ~7 minutes:
  `ok=true`, `malformed_lines=0`, `warnings=[]`
- Events:
  `signal.evaluation=2335`, `signal.skip=2335`, `price.snapshot=29`,
  `risk.state=29`, `system.release_manifest=1`
- Risk:
  first/last bankroll `100.0`, first/last realized PnL `0.0`, first/last
  wins/losses `0/0`, `breaker_tripped=false`
- Oracle:
  checks/disagreements/ties/corrections all `0`
- Orders/resolutions:
  `0`; skips were dominated by `low_confidence_*`, not by breaker starvation.

## Disk cleanup coordination

Before deploy, root disk was full (`327M` free). Only old PolyMomentum-owned
session logs were compressed with `nice -n 10 gzip -1`; no peer private paths or
shared PMXT parquets were modified. After cleanup/reset, root had about `12G`
free. Shared PMXT cache cleanup remains blocked on downloader-owner
coordination through `/opt/shared/cross_bot_notes/`.

## Wallet / pUSD state

Read-only wallet check on the VPS:

- Address: `0xe0ab9972e6ac14c29c06699fb0096a83f2a931ba`
- pUSD: `0.00`
- USDC.e: `6.03363`
- POL: `5.3759`
- pUSD allowance to CTF Exchange V2: `0.00`
- pUSD allowance to Neg Risk CTF Exchange V2: `0.00`
- USDC.e allowance to Collateral Onramp: `0.00`
- `live_ready=false`

Live remains blocked until pUSD and both CLOB V2 allowances are prepared.
