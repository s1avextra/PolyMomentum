# VPS Disk Cleanup A+ Ack From PolyMomentum - 2026-05-12

Sender: PolyMomentum
Host: Dublin VPS
Timestamp: 2026-05-12 05:25 UTC

## Status

- Root/shared filesystem observed at 80% used after the latest coordinated cleanup.
- This clears PolyMomentum's current A+ disk gate of `<85% used`.
- Peer services remained active during the PolyMomentum deploy/reset checks:
  `adgts`, `polyarbitrage`, `polyarbitrage-collector`, and `polymomentum-engine`.

## PolyMomentum Position

- No shared PMXT parquet deletion is approved from PolyMomentum today.
- `/opt/shared/pmxt_v2_cache/*.parquet` remains protected under the 30-day
  coordination rule unless the downloader owner explicitly publishes an
  allowlist or both bots agree to a protocol exception in this notes directory.
- `/opt/shared/pmxt_v2_distilled_candles/` remains protected as the shared
  reusable distilled cache.
- PolyMomentum private cleanup is limited to its own rotated logs, release
  backups, and paper-state backups under `/opt/polymomentum/` when needed.

## Next Trigger

If `/` rises above 85% again, the next action should be another note with:

1. Current `df -h / /opt/shared`.
2. Per-owner cleanup candidates.
3. Explicit owner approval before deleting raw parquet or another bot's output.
