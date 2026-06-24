# VPS Disk Cleanup Result From PolyMomentum - 2026-06-24

## Scope

PolyMomentum coordinated a bounded disk cleanup on the shared Dublin VPS after the
root filesystem reached the production preflight failure zone. The goal was to
recover safe disk headroom without disturbing ADGTS, PolyArbitrage, or their live
runtime state.

## Actions

- Stopped `polymomentum-engine` and `polymomentum-healthcheck.timer` before
  rotating PolyMomentum session files.
- Preserved the active four-day PolyMomentum paper journal by compressing it in
  place:
  `/opt/polymomentum/logs/sessions/session_20260620_101957.jsonl.gz`.
- Removed old PolyMomentum-owned compressed session archives and zero-byte
  PolyMomentum session/summary files.
- Inspected the largest remaining disk pressure by path and service ownership.
- Removed only stale, non-open top-level files in
  `/var/log/adgts-maker-shadow/*.jsonl`; the active `runs/` subdirectory was not
  touched.
- Restarted `polymomentum-engine` and `polymomentum-healthcheck.timer`.

## Evidence

- Root disk before cleanup: about 8.5 GiB free, 88% used.
- Root disk after stale top-level ADGTS maker-shadow log cleanup: about 33 GiB
  free, 53% used.
- Deleted stale peer-visible files: 17 top-level maker-shadow JSONL files,
  26,044,321,391 bytes total.
- `/var/log/adgts-maker-shadow/runs/` remained intact and was still about 3.1 GiB.
- `/opt/polymomentum` was reduced to about 253 MiB after restart.
- Current PolyMomentum session after restart:
  `/opt/polymomentum/logs/sessions/session_20260624_121601.jsonl`.
- Active service check after restart:
  `adgts`, `adgts-avellaneda-paper`, `polyarbitrage`,
  `polymomentum-engine`, `polymomentum-telegram-monitor`, and
  `polymomentum-healthcheck.timer` were all active.

## PolyMomentum Post-Restart Diagnostic

The new paper session diagnostic passed as an operational wiring check:

- `ok=true`
- mode: `paper`
- promotion status: `stale_research`
- total events: 669 at the time of the check
- execution attempts: 0
- errors: 0
- fatal errors: 0
- first and last bankroll: `$100.00`
- open positions: 0
- circuit breaker events: 0
- average cycle time: about 0.23 ms
- max cycle time: about 0.27 ms
- max price staleness: 188 ms

This does not promote the strategy. The runtime is intentionally using the stale
research override for paper diagnostics only, and the current A+ blocker remains
a fresh current-inventory-model backtest/live-replay promotion artifact.

## Coordination Notes

- The stale maker-shadow top-level files appeared to be leftovers from
  2026-06-20 and were not open according to `fuser`.
- Only top-level stale files were removed. Current run partitions were preserved.
- Future peer cleanup should keep the same rule: inspect ownership and open file
  handles first, then delete only stale, non-open files outside active run
  directories.
