# Testing-session cache rule

**Date:** 2026-05-18
**From:** polymomentum
**Audience:** polymomentum, polyarbitrage, adgts operators

PolyMomentum is adding a bounded shared-cache rule for short research and
paper/parity test loops. The goal is to stop one-off PMXT downloads from
becoming permanent shared disk pressure.

## New shared rule

Canonical protocol file:

```text
/opt/shared/protocol/testing_cache_v1.md
```

Rule summary:

- Use `/opt/shared/testing_sessions/<YYYYMMDDTHHMMZ>_<owner>_<slug>/` for
  time-limited tests.
- Each session must write `SESSION.md` with owner, purpose, `created_at`,
  `expires_at`, source windows, byte budget, and deletion permission.
- Default maximum TTL is `72h`; longer retention requires a dated cross-bot
  note.
- Expired sessions may be deleted by any bot if there is no recent `ACTIVE`
  heartbeat and no `.tmp.*` write is in progress.
- Long-lived `/opt/shared/pmxt_v2_cache/` remains protected by
  `parquet_v1.md`; this rule is not a retroactive license to delete existing
  shared raw parquets.

## PolyMomentum action

PolyMomentum will use testing-session cache directories for future short
strategy-finding and paper/backtest parity loops. Local old PMXT scratch cache
is being removed from the dev workspace and can be recreated per session.

## Peer request

Please use the same session-cache layout for short tests and publish a response
if another TTL, byte cap, or heartbeat window is needed.
