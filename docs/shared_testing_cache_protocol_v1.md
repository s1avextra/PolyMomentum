# Shared testing-session cache protocol v1

**Canonical shared path:** `/opt/shared/protocol/testing_cache_v1.md`
**Date:** 2026-05-18
**From:** polymomentum

This rule exists because the long-lived PMXT raw parquet cache is too expensive
to use as a scratchpad for short strategy-finding loops. Time-limited tests
must create explicit expiring session caches instead of silently growing
`/opt/shared/pmxt_v2_cache/`.

## Directory layout

Use one directory per bounded test:

```text
/opt/shared/testing_sessions/<YYYYMMDDTHHMMZ>_<owner>_<slug>/
|-- SESSION.md
|-- pmxt_v2_cache/
|-- pmxt_v2_distilled_candles/
`-- artifacts/
```

Owners are `polymomentum`, `polyarbitrage`, or `adgts`. Slugs are lowercase
letters, numbers, and dashes only.

## Required manifest

Each session must write `SESSION.md` before downloading data:

```text
owner: polymomentum
created_at: 2026-05-18T06:00:00Z
expires_at: 2026-05-20T06:00:00Z
purpose: 5m-candidate-sweep
source_windows: 2026-04-25T00..2026-04-26T00
max_bytes: 10G
may_delete_after_expiry: yes
```

The default maximum TTL is `72h`. Longer retention needs a cross-bot note that
names the owner, purpose, expiry, and byte budget.

## Use rules

- One-off backtests, sweeps, and parity experiments use the session cache path
  via `--cache-dir` or `PMXT_V2_CACHE_DIR`.
- Reusable production evidence can still use `/opt/shared/pmxt_v2_cache/`, but
  only when it is expected to survive beyond a single test session.
- Distilled files generated only for the test go under the session's
  `pmxt_v2_distilled_candles/` directory.
- Shared long-lived caches remain governed by `parquet_v1.md`.

## Cleanup rules

Any bot may delete an expired testing-session directory if all are true:

- `expires_at` in `SESSION.md` is in the past;
- no `.tmp.*` files exist under the session directory;
- no `ACTIVE` heartbeat file has been updated in the last `30m`;
- the directory is under `/opt/shared/testing_sessions/`.

If `/` is above `85%` used, expired testing sessions are the first shared
cleanup target. If `/` is above `95%`, expired testing sessions may be pruned
immediately before deeper owner-specific cleanup.

## Existing raw PMXT cache

This protocol does not retroactively authorize deleting existing files in
`/opt/shared/pmxt_v2_cache/`. Legacy shared parquets still require downloader
ownership, the `parquet_v1.md` age/size rule, or an explicit owner allowlist in
`/opt/shared/cross_bot_notes/`.
