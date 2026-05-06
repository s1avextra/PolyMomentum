# Paper Parity Checkpoint - 2026-05-06

Scope: moved forward from the first 16h paper soak into local parity analysis.
No peer bot private paths were read. No VPS sweeps, release builds, or parquet
scans were run.

## Evidence Pulled Locally

- Contaminated paper snapshot:
  `logs/soak_evidence/20260505_paper/current_session/session_20260505_133514_snapshot.jsonl`
- Snapshot SHA256:
  `6c5006ef4103828fa8507c63a20f3c31402d145525190a3970a46a93ec396558`
- Local diagnostics:
  `logs/soak_evidence/20260505_paper/current_session/diagnostics_snapshot_strict.json`
- Local replay:
  `logs/soak_evidence/20260505_paper/current_session/validate_replay_snapshot.txt`
- Local sweep:
  `logs/soak_evidence/20260505_paper/current_session/sweep_snapshot.txt`

The original diagnostics/replay gate was clean:

- Events: `538513`
- Signal evaluations: `265141`
- Orders: `180 placed / 180 filled / 0 rejected`
- Replay: `total=265141 mismatches=0 (0.00%)`

The local sweep over the captured session favored the promoted `maker_first`
variant:

- `maker_first`: `180` trades, `168` wins, `12` losses, `93.3%` win rate,
  `+1646.54` synthetic PnL, `+9.147` PnL/trade.
- Zone concentration: `132 / 180` trades in terminal zone (`73.3%`), above the
  original aggregate promotion concentration gate of `70%`.

## Critical Finding

The first paper session was operationally healthy but not a clean bankroll
baseline.

Journal evidence at `2026-05-05T13:35:15Z` showed:

- `restored paper positions n=1`
- restored state with large old PnL/fees
- immediate resolution of a stale paper position before the first new order

The tightened local diagnostics now marks that snapshot non-green:

- `ok=false`
- Resolutions: `181`
- Filled orders: `180`
- Oracle disagreements: `57`
- First risk bankroll: `12354.62`
- First realized PnL: `8.81`

This means the 16h run is valid as a runtime stability and strategy-behavior
sample, but not valid as clean paper-to-live acceptance evidence.

## Fix Applied On VPS

At `2026-05-06T05:40:27Z`, backed up and reset only the PolyMomentum state DB:

- Backup:
  `/opt/polymomentum/logs/candle/state.db.bak.paper_clean2.20260506T054027Z`
- Cleared tables:
  `trades`, `paper_positions`, `positions`, `cooldowns`, `oracle_pending`,
  `state`, `meta`
- Restarted only `polymomentum-engine.service`
- Peer services/timers remained active:
  `adgts`, `polyarbitrage`, `polyarbitrage-collector`,
  `polymomentum-engine`, `polymomentum-soak-report.timer`,
  `polymomentum-healthcheck.timer`

Fresh clean session:

- Session: `/opt/polymomentum/logs/sessions/session_20260506_054027.jsonl`
- Local snapshot:
  `logs/soak_evidence/20260505_paper/current_session/session_20260506_054027_clean_snapshot.jsonl`
- Strict diagnostics:
  `logs/soak_evidence/20260505_paper/current_session/diagnostics_clean_snapshot_strict.json`

Fresh clean strict diagnostics:

- `ok=true`
- Events: `3260`
- Orders: `1 placed / 1 filled / 0 rejected`
- Resolutions: `1`
- Oracle disagreements: `0`
- First risk bankroll: `100.0`
- First realized PnL: `0.0`
- Warnings: `[]`

## Hardened Binary Deployed

After CI built commit `412a04184ae150b343e570c2cefd2c8bcfb91a2f`, deployed
the Linux x86_64 artifact to the VPS:

- Artifact SHA256:
  `a9770281ee2f3db181ac4dd646b77d136cd1de1827006b81b9ec5b4d9e8a8cf6`
- Build timestamp: `2026-05-06T05:48:05Z`
- Deployment mode: `paper`
- Promotion artifact unchanged:
  `3691ff1d390821e73b44e951470420924e95a43f6d6c554f76c5e748a6c82b26`

To make the next soak window strict and unambiguous, reset the PolyMomentum DB
again after deployment:

- Backup:
  `/opt/polymomentum/logs/candle/state.db.bak.paper_clean3_after_diag_deploy.20260506T071711Z`
- Fresh deployed session:
  `/opt/polymomentum/logs/sessions/session_20260506_071712.jsonl`
- First strict diagnostics after risk snapshots:
  - `ok=true`
  - first bankroll: `100.0`
  - first realized PnL: `0.0`
  - orders: `0 placed / 0 filled / 0 rejected`
  - resolutions: `0`
  - oracle disagreements: `0`
  - warnings: `[]`

## First Hardened Soak Evidence

Manual low-impact soak report after the hardened deployment:

- Report: `/opt/polymomentum/logs/soak/soak_20260506T072631Z.json`
- Local copy:
  `logs/soak_evidence/20260506_hardened_clean/soak_20260506T072631Z.json`
- Session snapshot:
  `logs/soak_evidence/20260506_hardened_clean/session_20260506_071712_snapshot.jsonl`
- Session SHA256:
  `9a9f883f85c2ab2109d53511b9e656c81b66188b58b24f0c741deafe7ec93ad4`
- Soak `ok=true`
- Replay exit: `0`
- Local replay: `total=1543 mismatches=0 (0.00%)`
- Peer services active: `adgts`, `polyarbitrage`,
  `polyarbitrage-collector`

Strict diagnostics on the local snapshot:

- `ok=true`
- Events: `3169`
- Orders: `3 placed / 3 filled / 0 rejected`
- Resolved trades: `2`
- Oracle disagreements: `0`
- First bankroll: `100.0`
- Last realized PnL: `20.61`
- Warnings: `[]`

Small-sample strategy sweep over the two resolved trades:

- `maker_first`: `2` trades, `2` wins, `+19.64` synthetic PnL.
- Zones: `1` late, `1` primary.
- Terminal-only variants had `0` trades in this tiny clean snapshot.

This is not statistically sufficient, but it confirms that the post-deploy
clean baseline is instrumented correctly and that the promoted strategy is
producing replayable paper orders without terminal-zone concentration so far.

## Second Hardened Soak Evidence

Manual low-impact soak at `2026-05-06T07:48:51Z`, before the scheduled timer
report:

- Report: `/opt/polymomentum/logs/soak/soak_20260506T074851Z.json`
- Local copy:
  `logs/soak_evidence/20260506_hardened_clean/soak_20260506T074851Z.json`
- Session snapshot:
  `logs/soak_evidence/20260506_hardened_clean/session_20260506_071712_074851_snapshot.jsonl`
- Session SHA256:
  `4493fdee58a5ef11761e51011fdd2b94173c4079b13569a3cac8f0d91a9f118a`
- Soak `ok=true`
- Replay exit: `0`
- Local replay: `total=8585 mismatches=0 (0.00%)`
- Peer services active: `adgts`, `polyarbitrage`,
  `polyarbitrage-collector`

Strict diagnostics on the local snapshot:

- `ok=true`
- Events: `17450`
- Orders: `9 placed / 9 filled / 0 rejected`
- Resolved trades: `8`
- Wins / losses: `7 / 1`
- Oracle disagreements: `0`
- First bankroll: `100.0`
- Last realized PnL: `63.34`
- Warnings: `[]`

Small-sample strategy sweep over the eight resolved trades:

- `maker_first`: `8` trades, `7` wins, `1` loss, `+52.68` synthetic PnL,
  `+6.585` PnL/trade.
- Zones: `1` early, `1` late, `6` primary.
- Terminal-only variants still had `0` trades in the clean sample.

The sample is still too small for promotion, but it is useful because it has a
loss, open-position overlap, oracle verification, replay validation, and no
terminal-zone concentration.

## Oracle Disagreement Found

After the second manual soak, strict diagnostics correctly flipped non-green on
the running session:

- Disagreement CID: `0x1dd0bd3ab835e0`
- Local paper actual: `down`
- Polymarket actual: `up`
- Local open BTC: `81428.50`
- Local close BTC: `81425.12`
- Difference: `-3.39`

The edge case is a tight market where the local exchange-mid close disagreed
with Polymarket's official settlement. This is exactly why paper cannot only
log oracle disagreements. Paper PnL must reconcile to oracle truth because live
PnL is determined by Polymarket settlement.

Code fix:

- `OraclePending` now stores direction, entry price, size, fee, provisional PnL,
  and provisional win/loss.
- On oracle disagreement, paper computes the final Polymarket-settled PnL and
  applies the delta to `RiskManager`.
- The circuit breaker moves the provisional win/loss bucket to the final
  oracle bucket and adjusts realized PnL.
- Session logs now emit `oracle.correction` with provisional/final outcome and
  PnL delta.

Verification:

```bash
cargo test --manifest-path rust_engine/Cargo.toml --locked --lib
```

Result: `134 passed`.

The current running session must be treated as invalid for live-readiness
because it was started before oracle correction landed. Restart the clean clock
after deploying the correction binary and resetting paper state.

## Code Hardening

Diagnostics now reports and gates:

- `resolutions`: resolved, wins, losses, total PnL
- `oracle`: checks and disagreements
- `risk`: first/last bankroll, realized PnL, wins/losses, max positions
- non-green if rejected orders are present
- non-green if resolution count exceeds filled orders
- non-green if oracle disagreements are present
- non-green if the first risk snapshot starts with already-realized paper PnL
  or pre-existing paper wins/losses
- oracle correction events with provisional/final PnL deltas

Verification:

```bash
cargo test --manifest-path rust_engine/Cargo.toml --locked monitoring::diagnostics --lib
```

Result: `4 passed`.

## Decision

We can move on from the original 16h runtime soak into strategy/parity work, but
the live-readiness clock must restart from the clean session at
`2026-05-06T05:40:27Z`.

Next steps:

1. Let the clean paper session produce its next soak report.
2. Run strict diagnostics locally on that clean report/session until the VPS
   binary with the hardened diagnostics is deployed.
3. Treat the terminal-zone concentration as a strategy research issue before
   live: paper holdout showed stronger performance than the original backtest,
   but with slightly higher terminal concentration.
4. Do exact same-window archive replay only after May 5/6 PMXT archive data is
   available locally or in the shared cache. As of this checkpoint, neither
   local nor shared cache had May 5/6 archive files.
