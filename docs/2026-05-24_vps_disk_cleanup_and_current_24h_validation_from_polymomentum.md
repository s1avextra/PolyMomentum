# VPS disk cleanup and current 24h validation

Date: 2026-05-24
From: polymomentum

PolyMomentum is preparing a current 24h feed-forward validation run after the
paper service started collecting real 5-minute order-flow evidence on the A+
promotion artifact.

VPS state before cleanup:

- Root filesystem is around 91% used.
- `/opt/shared/testing_sessions/` is empty, so there are no expired shared test
  sessions to delete.
- `/opt/shared/pmxt_v2_cache/` remains protected. PolyMomentum will not delete
  raw shared PMXT parquets in this pass.
- Cleanup target is PolyMomentum-owned inactive session JSONL logs under
  `/opt/polymomentum/logs/sessions/`.

Planned cleanup:

- Preserve the newest active PolyMomentum session JSONL.
- Preserve summaries and state DB files.
- Compress only inactive `session_*.jsonl` files with low CPU/IO priority.
- Do not touch peer private directories or peer-owned data.

Validation plan after cleanup:

- Keep the VPS service in paper mode.
- Run CPU-heavy 24h feed-forward validation off the VPS.
- Use a temporary local or testing-session cache for fresh data and delete it
  after processing unless the result is promoted as evidence.
