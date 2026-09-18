#!/bin/bash
# Pull-only mirror of the VPS session logs (band observer + band canary)
# into logs/band-canary-mirror/sessions/ for the Mac evaluator.
# Never writes to the VPS: the rsync source is remote and rsync writes to
# the receiver only. Plain -az, never --ignore-existing: the engine appends
# to one session_<start>.jsonl for its whole process lifetime (days for
# the observer) and rewrites summary_<id>.json in place, so a file the
# mirror already holds must keep being refreshed or the mirror freezes at
# whatever the first pull saw (rsync transfers the changed tail only).
# Exits 0 when the VPS is unreachable (ssh/rsync timeout) or a source tree
# does not exist yet, so a launchd or cron caller sees a clean no-op rather
# than a failure.
set -uo pipefail
cd /Users/ttoomm/Documents/PolyMomentum
DEST=logs/band-canary-mirror/sessions
mkdir -p "$DEST"
RSH="ssh -o BatchMode=yes -o ConnectTimeout=15"

status=0
for src in \
  vps:/opt/polymomentum/logs/band-observer/sessions/ \
  vps:/opt/polymomentum/logs/band-canary/sessions/; do
  rsync -az --timeout=60 -e "$RSH" "$src" "$DEST/"
  rc=$?
  case "$rc" in
    0) ;;
    # 255 ssh failed (host down, timeout), 30/35 rsync I/O or connection
    # timeout, 23/24 source tree missing or files vanished mid-transfer.
    255|30|35|23|24) echo "pull_vps_sessions: $src skipped (rsync exit $rc)" >&2 ;;
    *) echo "pull_vps_sessions: rsync $src failed (exit $rc)" >&2; status=$rc ;;
  esac
done
exit $status
