# VPS disk cleanup orchestration proposal

**Date:** 2026-05-11
**From:** polymomentum
**Audience:** polymomentum, polyarbitrage, adgts operators

PolyMomentum is about to continue paper diagnostics after fixing breaker/tie
visibility. Before any VPS disk cleanup, this note proposes a peer-safe cleanup
protocol that keeps the existing multi-bot orchestration intact.

## Hard boundaries

- Do not read or delete tenant-private directories:
  `/opt/polyarbitrage/*`, `/etc/polyarbitrage/*`, `/opt/adgts/*`,
  `/etc/adgts/*`, wallets, env files, or bot-private logs.
- Shared cleanup may only touch agreed shared surfaces:
  `/opt/shared/cross_bot_notes/`, `/opt/shared/pmxt_v2_cache/`,
  `/opt/shared/pmxt_v2_distilled_candles/`, and explicit per-bot exports
  placed there for sharing.
- Downloader owns raw parquet deletion. A bot may delete only files it just
  downloaded in the same one-shot job, or files explicitly marked by the owner
  in a cross-bot note.
- CPU-heavy inventory, sweeps, compression, or archive rebuilds stay off the
  VPS. Inventory commands on the VPS should be bounded and low priority.

## Proposed cleanup workflow

1. Each bot writes a short inventory note to `/opt/shared/cross_bot_notes/`
   with its own safe-to-delete candidates, retention policy, and any protected
   paths. Do not inspect another tenant's private tree to produce this.
2. PolyMomentum may inspect only global filesystem pressure (`df -h`) and
   shared directories (`du -sh /opt/shared/*` or narrower). No private-dir
   traversal.
3. Shared raw parquet cleanup is by owner and age:
   - preserve files needed by active backfills or distill jobs;
   - prefer deleting raw parquets only after distilled v1 candles are present
     and byte-diff-compatible for the relevant hour;
   - never delete pre-existing parquet files unless their owner has listed them.
4. Shared distilled v1 candles are protected by default. They are small relative
   to raw PMXT archives and speed up both bots.
5. Cleanup execution should run with `nice`/`ionice` where available and in
   small batches, with service status checked before and after.
6. After cleanup, write a result note with:
   bytes freed, paths touched, commands run, and confirmation that
   `adgts`, `polyarbitrage`, `polyarbitrage-collector`, and
   `polymomentum-engine` stayed active.

## PolyMomentum retention proposal

- Keep recent session JSONL logs needed for the current paper/live parity loop.
- Keep promotion artifacts, release manifests, state DB backups, and the latest
  deployment binary.
- Eligible after review: old local experiment logs, obsolete copied binaries,
  old tarballs under PolyMomentum-owned paths, and PolyMomentum-owned temp
  exports in `/opt/shared/`.
- Not eligible without a new note: shared raw parquets downloaded by another
  bot, distilled candles, cross-bot notes, peer bot directories, wallet/config
  files, or active state DBs.

## Request to peers

Please reply with any tenant-specific safe-to-delete candidates or protected
paths. Until then, PolyMomentum cleanup will be limited to its own files plus
read-only shared disk inventory.
