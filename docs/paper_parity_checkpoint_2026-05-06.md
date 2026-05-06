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

## Oracle-Correction Deployment

CI artifact commit `97483cbfc80aa390b0a282c537e402b55fb1dc36` was deployed to
the VPS in paper mode after the oracle-PnL reconciliation fix:

- Artifact SHA256:
  `2ea0a253794a0123228f7829b21fd3ecfc5a1b40006e6c1b129ddcfad3715e20`
- Build timestamp: `2026-05-06T08:02:11Z`
- Promotion artifact hash unchanged:
  `3691ff1d390821e73b44e951470420924e95a43f6d6c554f76c5e748a6c82b26`

The first post-deploy restart produced a non-trustworthy baseline because the
service was active around the DB reset. A hard stop/reset/start was then run
against PolyMomentum only:

- Backup:
  `/opt/polymomentum/logs/candle/state.db.bak.paper_oracle_correction_clean_retry.20260506T081315Z`
- Fresh session:
  `/opt/polymomentum/logs/sessions/session_20260506_081315.jsonl`
- State DB counts before start and after the initial check:
  `trades=0`, `paper_positions=0`, `oracle_pending=0`, `state=0`
- First strict diagnostics:
  - `ok=true`
  - first bankroll: `100.0`
  - first realized PnL: `0.0`
  - orders: `0 placed / 0 filled / 0 rejected`
  - oracle disagreements/corrections: `0 / 0`
  - warnings: `[]`

Manual low-impact soak after the clean restart:

- Report: `/opt/polymomentum/logs/soak/soak_20260506T081445Z.json`
- Local copy:
  `logs/soak_evidence/20260506_oracle_correction_clean/soak_20260506T081445Z.json`
- Session snapshot:
  `logs/soak_evidence/20260506_oracle_correction_clean/session_20260506_081315_snapshot.jsonl`
- Session SHA256:
  `789ef386658f321236a492689cda354ac74ec6b326052e11f62cbc759bcf7b05`
- Soak `ok=true`
- Replay exit: `0`
- Current-code local replay: `total=366 mismatches=0 (0.00%)`
- Current-code strict diagnostics:
  - `ok=true`
  - events: `753`
  - signal evaluations/skips: `366 / 366`
  - orders/resolutions/oracle checks: `0 / 0 / 0`
  - first/last bankroll: `100.0 / 100.0`
  - warnings: `[]`

Second manual low-impact soak after the same clean restart captured the first
oracle-correction-build trade:

- Report: `/opt/polymomentum/logs/soak/soak_20260506T082735Z.json`
- Local copy:
  `logs/soak_evidence/20260506_oracle_correction_clean/soak_20260506T082735Z.json`
- Session snapshot:
  `logs/soak_evidence/20260506_oracle_correction_clean/session_20260506_081315_082735_snapshot.jsonl`
- Session SHA256:
  `eb4809142f04611423f94b5451752bdb81888e7cfb18579cbae64ce69089621e`
- Soak `ok=true`
- Soak replay exit: `0`
- Soak replay: `total=2501 mismatches=0 (0.00%)`
- Soak diagnostics:
  - orders: `1 placed / 1 filled / 0 rejected`
  - resolutions: `1`, wins/losses `1 / 0`, PnL `+7.4087`
  - oracle checks/disagreements/corrections: `1 / 0 / 0`
  - first/last bankroll: `100.0 / 107.41`
  - warnings: `[]`
- Current-code local replay on the later copied snapshot:
  `total=2577 mismatches=0 (0.00%)`
- Current-code local diagnostics on the later copied snapshot:
  - `ok=true`
  - events: `5274`
  - orders/resolutions/oracle checks: `1 / 1 / 1`
  - last realized PnL: `+7.41`
  - warnings: `[]`

Resource coexistence check:

- Peer services remained active: `adgts`, `polyarbitrage`,
  `polyarbitrage-collector`.
- PolyMomentum service resource controls:
  `CPUQuotaPerSecUSec=800ms`, `MemoryMax=512M`, `TasksMax=256`, `Nice=5`.
- A peer `strategy-finder` process was visible from process metadata using about
  one CPU core. It was not inspected beyond process metadata and was not
  touched.

The live-readiness paper clock restarts from the clean oracle-correction
session at `2026-05-06T08:13:15Z`.

## Promoted Cached Replay Parity

Cached live-replay exposed a transition gap: paper/live loaded the promoted
`maker_first` artifact, while `live-replay` rebuilt a strategy only from
environment settings. That made cached replay useful for plumbing, but not a
true proof of the promoted strategy handoff.

Code fix:

- `live-replay` now accepts `--promotion-artifact`.
- `ReplayStrategy::load` validates and loads the same promoted
  `StrategyVariant` hash used by paper/live.
- The release manifest in replay now carries the promoted hash/source.

Verification:

```bash
cargo test --manifest-path rust_engine/Cargo.toml --locked --lib
```

Result: `135 passed`.

Promoted cached live-replay, local dev box only:

- Command window: `2026-04-25T10:00:00Z`
- Cache: `data/pmxt_cache`
- BTC tape: `/private/tmp/pm_btc_ticks_20260425.csv`
- Contracts cap: `5`
- Promotion artifact:
  `logs/experiments/promotion_20260425T00_23_livecadence.json`
- Report:
  `logs/experiments/live_replay_20260506_promoted/live_replay_report_20260425T10_max5_maker_first.json`
- Session:
  `logs/experiments/live_replay_20260506_promoted/session_20260506_082302.jsonl`
- Report SHA256:
  `489791fb5df0908ead618ae6d984f4f39e9e5030fa1348dd67c095295ae7a282`
- Diagnostics SHA256:
  `bfdeed899dd7f9eef956764315ab790e536775223f1abf46be5b663df05379c2`
- Strategy/source:
  `maker_first`,
  `promotion:logs/experiments/promotion_20260425T00_23_livecadence.json`
- PMXT events loaded/processed: `911400 / 911400`
- Session diagnostics:
  - `ok=true`
  - signal evaluations: `715788`
  - orders: `1 placed / 1 filled / 0 rejected`
  - replay: `total=715788 mismatches=0 (0.00%)`
  - fill price: `0.87`, fees: `0.0`

Matching capped L2 harness, same hour/contracts/cache:

- Report:
  `logs/experiments/backtest_parity_20260506/harness_20260425T10_max5_oracle_correction.json`
- Report SHA256:
  `2f84066dac8ddeca660ff43865b6d282e7d800d7d7c9017c9ffa67203c1c5152`
- `maker_first`: `1` trade, `1` win, `0` losses, `+1.43` PnL, `0.0` fees,
  zone `primary`.

Interpretation: the promoted cached-feed replay and the backtest harness now
agree on the order/fill economics for this capped sample. This is a parity
smoke, not statistical proof; the same promoted bridge should be rerun over
larger cached windows on the dev box.

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
the live-readiness clock must restart from the clean oracle-correction session
at `2026-05-06T08:13:15Z`.

Next steps:

1. Let the clean oracle-correction paper session produce its next scheduled
   soak report.
2. Run strict diagnostics locally on that clean report/session and confirm that
   oracle corrections, if any, adjust realized PnL instead of leaving
   unresolved disagreements.
3. Treat the terminal-zone concentration as a strategy research issue before
   live: paper holdout showed stronger performance than the original backtest,
   but with slightly higher terminal concentration.
4. Expand the promoted cached-replay/backtest parity from the capped
   `2026-04-25T10` smoke to multi-hour cached windows on the dev box.
5. Do exact same-window archive replay only after May 5/6 PMXT archive data is
   available locally or in the shared cache. As of this checkpoint, neither
   local nor shared cache had May 5/6 archive files.
