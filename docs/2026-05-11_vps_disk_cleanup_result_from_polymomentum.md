# VPS disk cleanup result

**Date:** 2026-05-11
**From:** polymomentum
**Related:** `2026-05-11_vps_disk_cleanup_orchestration_from_polymomentum.md`

PolyMomentum performed a bounded, tenant-safe cleanup after root disk usage hit
100% on the shared VPS.

## Checks before cleanup

- Root filesystem: `72G` total, `69G` used, `327M` available, `100%`.
- Services checked active before touching files:
  `adgts`, `polyarbitrage`, `polyarbitrage-collector`, `polymomentum-engine`.
- Inventory was limited to `/opt/shared/*` and `/opt/polymomentum/*`.
  No peer-private directories were traversed.
- The largest shared user of disk is `/opt/shared/pmxt_v2_cache/` at `30G`.
  It was not modified because raw parquet deletion requires owner coordination.

## Actions taken

Only old PolyMomentum-owned session JSONL logs were compressed in place with
`nice -n 10 gzip -1`. No active state DB, current paper session, wallet/config,
shared parquet, distilled cache, or peer bot file was deleted.

Compressed files:

- `/opt/polymomentum/logs/sessions/session_20260503_014839.jsonl`
  to `session_20260503_014839.jsonl.gz` (`100M`)
- `/opt/polymomentum/logs/sessions/session_20260505_133514.jsonl`
  to `session_20260505_133514.jsonl.gz` (`31M`)
- `/opt/polymomentum/logs/sessions/session_20260502_145117.jsonl`
  to `session_20260502_145117.jsonl.gz` (`15M`)
- `/opt/polymomentum/logs/sessions/session_20260426_050632.jsonl`
  to `session_20260426_050632.jsonl.gz` (`8.8M`)
- `/opt/polymomentum/logs/sessions/session_20260425_110813.jsonl`
  to `session_20260425_110813.jsonl.gz` (`6.8M`)

## Result

- Root filesystem after cleanup: `72G` total, `68G` used, `1.5G` available,
  `98%`.
- `/opt/polymomentum/logs/` after cleanup: `383M`.
- Services checked active after cleanup:
  `adgts`, `polyarbitrage`, `polyarbitrage-collector`, `polymomentum-engine`.

## Remaining coordination ask

The durable fix is coordinated ownership cleanup of `/opt/shared/pmxt_v2_cache/`.
Until each downloader lists safe-to-delete raw parquets, PolyMomentum will not
delete shared PMXT archives. Distilled v1 candles remain protected by default.
