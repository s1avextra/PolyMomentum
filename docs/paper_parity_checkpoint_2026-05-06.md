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

## Code Hardening

Diagnostics now reports and gates:

- `resolutions`: resolved, wins, losses, total PnL
- `oracle`: checks and disagreements
- `risk`: first/last bankroll, realized PnL, wins/losses, max positions
- non-green if rejected orders are present
- non-green if resolution count exceeds filled orders
- non-green if oracle disagreements are present

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
