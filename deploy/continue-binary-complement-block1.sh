#!/bin/bash
# Continue the frozen binary-complement block only after segment 003 seals.
set -euo pipefail

WAIT_UNIT="polymomentum-binary-complement-block1-seg003-20260718.service"
WAIT_STATUS="/opt/polymomentum/logs/forward-captures/binary-complement-block1-seg003-20260718/segment_001/status.json"
CAPTURE_RUNNER="/opt/polymomentum/tools/capture-forward-segments.sh"
NEXT_SESSION="binary-complement-block1-seg004-007-20260718"
MAX_WAIT_SECONDS=7200
POLL_SECONDS=30

log() {
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

deadline=$(( $(date -u +%s) + MAX_WAIT_SECONDS ))
while systemctl is-active --quiet "$WAIT_UNIT"; do
    if [ "$(date -u +%s)" -ge "$deadline" ]; then
        log "timed out waiting for $WAIT_UNIT"
        exit 1
    fi
    sleep "$POLL_SECONDS"
done

jq -e '
    .capture_verified == true
    and .captured_conditions == 24
    and .admissible_conditions > 0
    and .admissible_groups > 0
' "$WAIT_STATUS" >/dev/null || {
    log "segment 003 did not seal at least one admissible condition; continuation stopped"
    exit 1
}

if [ -e "/opt/polymomentum/logs/forward-captures/$NEXT_SESSION" ]; then
    log "refusing to reuse existing session $NEXT_SESSION"
    exit 1
fi

log "segment 003 sealed; starting four unchanged bounded continuation segments"
exec "$CAPTURE_RUNNER" \
    --session-id "$NEXT_SESSION" \
    --segments 4 \
    --windows-per-segment 24 \
    --signal-preroll-seconds 3600 \
    --delete-session-owned-frames-after-verify
