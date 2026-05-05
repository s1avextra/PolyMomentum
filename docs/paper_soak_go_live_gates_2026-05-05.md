# Paper Soak Go-Live Gates - 2026-05-05

Scope: Dublin VPS, PolyMomentum only. The running service stays in paper mode
while diagnostics collect enough evidence for the next paper-to-live decision.
Peer bots must remain untouched: no peer-private paths, no VPS sweeps, no release
builds, and no shared parquet scans while the soak is running.

## Current Run

- Service: `polymomentum-engine.service`
- Mode: `paper`
- Venue: `paper_only`
- Session: `/opt/polymomentum/logs/sessions/session_20260505_133514.jsonl`
- Promotion artifact:
  `/opt/polymomentum/config/promotion_20260423_25_livecadence_aggregate_strict.json`
- Strategy hash:
  `3691ff1d390821e73b44e951470420924e95a43f6d6c554f76c5e748a6c82b26`
- State reset backup:
  `/opt/polymomentum/logs/candle/state.db.bak.paper_clean_retry.20260505T133522Z`

First managed soak report:

- Report: `/opt/polymomentum/logs/soak/soak_20260505T141348Z.json`
- Generated: `2026-05-05T14:13:48Z`
- Overall `ok`: `true`
- Session events: `28344`
- Orders placed / filled / rejected: `7 / 7 / 0`
- Malformed lines: `0`
- System errors / fatal errors: `0 / 0`
- Replay exit: `0`
- Peers active: `adgts`, `polyarbitrage`, `polyarbitrage-collector`

## Acceptance Gates

The run is not live-ready until every gate below is green over a full 24h
window. Prefer 48h before any funded canary.

| Gate | Pass rule | Evidence |
| --- | --- | --- |
| Service health | `polymomentum-engine`, soak timer, healthcheck timer, `adgts`, `polyarbitrage`, and `polyarbitrage-collector` stay active. | `systemctl is-active ...`; soak `peers` block. |
| Promotion identity | Promotion status is `ok`; strategy, source-report, and data-manifest hashes stay constant. | `diagnostics session`; soak `preflight` and `diagnostics`. |
| Session schema | Release manifest seen, `malformed_lines=0`, `warnings=[]`, no missing replay/order fields. | `polymomentum-engine diagnostics session`. |
| Replay determinism | `validate-replay` exits `0` on the latest session. | Soak `replay.exit_code`; manual validator if needed. |
| Order lifecycle | `order.rejected=0`, missing intent IDs `0`, missing placed state `0`; filled/open counts are explainable from paper positions. | Diagnostics `orders`; SQLite paper tables. |
| Runtime errors | `system.errors=0` and `fatal_errors=0` for the acceptance window. | Diagnostics `system`; service journal if diagnostics flags anything. |
| Resource coexistence | No peer degradation; PolyMomentum stays within systemd `CPUQuota` and `MemoryMax`; no CPU-heavy jobs on the VPS. | `systemctl show`; `ps`; peer states in soak report. |
| Observation length | At least four consecutive 6h soak reports for 24h, all green. Prefer eight reports for 48h. | `/opt/polymomentum/logs/soak/soak_*.json`. |
| Paper/backtest parity | Same promotion hash and no unexplained signal/order drift when the paper window is replayed or compared to cached-feed/backtest evidence. | `diagnostics compare`; local cached replay/harness reports. |

## Safe Monitoring Commands

These commands are intentionally light and safe to run while paper is active.
They do not touch peer-private directories or shared parquet data.

```bash
ssh vps 'systemctl is-active adgts polyarbitrage polyarbitrage-collector polymomentum-engine polymomentum-soak-report.timer polymomentum-healthcheck.timer'
```

```bash
ssh vps 'latest=$(ls -1t /opt/polymomentum/logs/sessions/session_*.jsonl | head -1); /opt/polymomentum/polymomentum-engine diagnostics session "$latest"'
```

```bash
ssh vps 'latest=$(ls -1t /opt/polymomentum/logs/soak/soak_*.json | head -1); jq "{generated_at, ok, mode, latest_session, events:.diagnostics.total_events, orders:.diagnostics.orders, system:.diagnostics.system, replay_exit:.replay.exit_code, peers}" "$latest"'
```

```bash
ssh vps 'systemctl show polymomentum-engine.service -p ActiveState -p SubState -p MainPID -p NRestarts -p MemoryCurrent -p CPUUsageNSec --no-pager'
```

## Next Analysis Pass

Run this sequence after the next soak report, then again after the 24h mark.

1. Summarize all soak reports since `2026-05-05T13:35:14Z`.
   Verify: all `ok=true`, no peer inactive states, no replay failures.
2. Run diagnostics on the current paper session.
   Verify: no malformed lines, no warnings, no rejects, no fatal errors.
3. Validate replay on the same session.
   Verify: exit `0`; if nonzero, stop and inspect the first mismatch before
   considering live.
4. Snapshot paper DB counts.
   Verify: trades, paper positions, cooldowns, and risk state explain the
   order lifecycle counts in the session log.
5. Pull only the soak JSON reports and compact diagnostics locally.
   Verify: evidence is reproducible without copying large session files unless
   a mismatch needs detailed replay.
6. Run cached-feed/backtest parity locally on the dev box only.
   Verify: same promotion identity, same strategy parameters, and any event
   deltas are caused by expected paper/live environment differences.

## Hold Conditions

Do not advance toward live if any of these occurs:

- Any peer service becomes inactive during the paper window.
- `promotion_status` is missing, invalid, or changes hash.
- Any paper order is rejected without a fully understood cause.
- `validate-replay` reports mismatches.
- System errors appear repeatedly, even if marked recoverable.
- The VPS needs a CPU-heavy harness, sweep, release build, or parquet scan to
  explain the result. Move that work to the dev box.

## Current Grade

The paper run is healthy and correctly instrumented, but still in progress.
Current operational grade: `A-` for paper collection setup. It can move to `A`
after one clean 24h window, and toward `A+` only after paper/backtest parity and
live-readiness checks are also green.
