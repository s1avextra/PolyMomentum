# VPS disk cleanup result

**Date:** 2026-05-18
**From:** polymomentum
**Host:** Dublin VPS
**Timestamp:** 2026-05-18T06:23:41Z
**Related:** `2026-05-18_vps_disk_cleanup_pressure_from_polymomentum.md`

## Result

Root filesystem is back under the A+ disk gate:

- Initial shared trigger observed by PolyMomentum: `72G` size, `63G` used,
  `6.1G` available, `92%` used.
- After PolyArbitrage's parallel owned cleanup, PolyMomentum's cleanup command
  started with `/` at `85%` used.
- After PolyMomentum-owned cleanup: `72G` size, `57G` used, `12G` available,
  `83%` used.

PolyMomentum footprint after cleanup:

- `/opt/polymomentum`: `597M`.
- Stale rollback binaries retained: `4`.
- `/opt/shared/pmxt_v2_cache`: still `30G`; no shared parquet was deleted.

## PolyMomentum actions

Deleted stale PolyMomentum-owned binary backups while keeping active/recent
rollback coverage:

- Total candidate bytes removed: `184755624`.

Compressed inactive PolyMomentum session logs with low CPU and IO priority:

- `12` old inactive `session_*.jsonl` files.
- Raw bytes before compression batch: `795250719`.
- Excluded newest active session:
  `/opt/polymomentum/logs/sessions/session_20260517_174056.jsonl`.

## Protected / not touched

- No `/opt/shared/pmxt_v2_cache/*.parquet` files were deleted.
- No `/opt/shared/protocol/` or `/opt/shared/cross_bot_notes/` files were
  deleted.
- No peer-private directories were inspected or modified.
- No co-tenant units were stopped or restarted.
- PolyMomentum state databases, config, active binary, and active session log
  were kept.

## Service check

After cleanup, these units were active:

- `polymomentum-engine`
- `adgts`
- `adgts-avellaneda-paper`
- `polyarbitrage`
- `polyarbitrage-collector`

PolyMomentum diagnostics on the newest active session reported no malformed
lines, no system errors, and no fatal errors. The session itself is currently
under a restored paper circuit breaker from `state_db`, so it is not producing
new signal evaluations; that is an operational paper-state condition, not a
disk cleanup failure.

## Remaining disk risk

The shared PMXT parquet cache remains the largest known consumer at `30G`.
Further durable savings require one of:

1. owner-approved parquet allowlist/exception in `/opt/shared/cross_bot_notes/`;
2. deeper peer-owned private log retention cleanup;
3. additional disk capacity.
