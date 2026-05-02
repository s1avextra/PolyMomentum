#!/bin/bash
# Capture low-impact production-loop evidence from the running VPS service.
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/polymomentum}"
ENGINE="${ENGINE:-$APP_DIR/polymomentum-engine}"
ENV_FILE="${ENV_FILE:-/etc/polymomentum/env}"

if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
fi

LOG_DIR="${POLYMOMENTUM_LOGS_DIR:-$APP_DIR/logs}"
SESSION_DIR="${SESSION_LOG_DIR:-$LOG_DIR/sessions}"
SOAK_DIR="${POLYMOMENTUM_SOAK_DIR:-$LOG_DIR/soak}"
mkdir -p "$SOAK_DIR"

if [ ! -x "$ENGINE" ]; then
    echo "engine binary is not executable: $ENGINE" >&2
    exit 1
fi

MODE="${POLYMOMENTUM_SOAK_MODE:-}"
if [ -z "$MODE" ]; then
    EXEC_START="$(systemctl show polymomentum-engine.service -p ExecStart --value 2>/dev/null || true)"
    if printf '%s\n' "$EXEC_START" | grep -q -- "--mode live"; then
        MODE="live"
    else
        MODE="paper"
    fi
fi

TMPDIR="$(mktemp -d)"
OUT_TMP=""
cleanup() {
    rm -rf "$TMPDIR"
    if [ -n "$OUT_TMP" ]; then
        rm -f "$OUT_TMP"
    fi
}
trap cleanup EXIT

capture_json() {
    local out="$1"
    shift
    local raw="$out.raw"
    local rc=0
    if "$@" >"$raw" 2>&1; then
        rc=0
    else
        rc=$?
    fi
    if [ "$rc" -eq 0 ] && jq -e . "$raw" >/dev/null 2>&1; then
        cp "$raw" "$out"
    else
        jq -Rs --argjson exit_code "$rc" \
            '{ok:false, exit_code:$exit_code, output:.}' <"$raw" >"$out"
    fi
}

capture_text() {
    local out="$1"
    shift
    local raw="$out.raw"
    local rc=0
    if "$@" >"$raw" 2>&1; then
        rc=0
    else
        rc=$?
    fi
    jq -Rs --argjson exit_code "$rc" \
        '{exit_code:$exit_code, output:.}' <"$raw" >"$out"
}

LATEST_SESSION="$(find "$SESSION_DIR" -maxdepth 1 -type f -name 'session_*.jsonl' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2- || true)"

PREFLIGHT_ARGS=(preflight --mode "$MODE")
if [ "$MODE" = "live" ]; then
    PREFLIGHT_ARGS+=(--i-understand-live)
fi
capture_json "$TMPDIR/preflight.json" "$ENGINE" "${PREFLIGHT_ARGS[@]}"
capture_json "$TMPDIR/release.json" "$ENGINE" release-manifest --mode "$MODE"
capture_json "$TMPDIR/wallet.json" "$ENGINE" wallet --json

if [ -n "$LATEST_SESSION" ]; then
    capture_json "$TMPDIR/diagnostics.json" "$ENGINE" diagnostics session "$LATEST_SESSION"
    capture_text "$TMPDIR/replay.json" "$ENGINE" validate-replay "$LATEST_SESSION"
else
    jq -n '{ok:false, error:"no session_*.jsonl found"}' >"$TMPDIR/diagnostics.json"
    jq -n '{exit_code:1, output:"no session_*.jsonl found"}' >"$TMPDIR/replay.json"
fi

capture_text "$TMPDIR/systemd.json" systemctl show polymomentum-engine.service \
    -p ActiveState -p SubState -p MainPID -p NRestarts -p ExecStart -p MemoryCurrent -p CPUUsageNSec --no-pager
capture_text "$TMPDIR/disk.json" df -P / "$APP_DIR"
capture_text "$TMPDIR/resources.json" ps -Ao pid,pcpu,pmem,rss,etime,command

ADGTS_STATE="$(systemctl is-active adgts.service 2>/dev/null || true)"
POLYARB_STATE="$(systemctl is-active polyarbitrage.service 2>/dev/null || true)"
POLYARB_COLLECTOR_STATE="$(systemctl is-active polyarbitrage-collector.service 2>/dev/null || true)"
jq -n \
    --arg adgts "$ADGTS_STATE" \
    --arg polyarbitrage "$POLYARB_STATE" \
    --arg polyarbitrage_collector "$POLYARB_COLLECTOR_STATE" \
    '{adgts:$adgts, polyarbitrage:$polyarbitrage, polyarbitrage_collector:$polyarbitrage_collector}' \
    >"$TMPDIR/peers.json"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$SOAK_DIR/soak_$STAMP.json"
OUT_TMP="$OUT.tmp.$$"

jq -n \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg mode "$MODE" \
    --arg latest_session "$LATEST_SESSION" \
    --slurpfile preflight "$TMPDIR/preflight.json" \
    --slurpfile release "$TMPDIR/release.json" \
    --slurpfile wallet "$TMPDIR/wallet.json" \
    --slurpfile diagnostics "$TMPDIR/diagnostics.json" \
    --slurpfile replay "$TMPDIR/replay.json" \
    --slurpfile systemd "$TMPDIR/systemd.json" \
    --slurpfile disk "$TMPDIR/disk.json" \
    --slurpfile resources "$TMPDIR/resources.json" \
    --slurpfile peers "$TMPDIR/peers.json" \
    '{
        schema_version: 1,
        generated_at: $generated_at,
        mode: $mode,
        latest_session: $latest_session,
        ok: (
            (($preflight[0].ok // false) == true) and
            (($diagnostics[0].ok // false) == true) and
            (($replay[0].exit_code // 1) == 0)
        ),
        preflight: $preflight[0],
        release: $release[0],
        wallet: $wallet[0],
        diagnostics: $diagnostics[0],
        replay: $replay[0],
        systemd: $systemd[0],
        disk: $disk[0],
        peers: $peers[0],
        resources: $resources[0]
    }' >"$OUT_TMP"

mv "$OUT_TMP" "$OUT"
OUT_TMP=""
printf '%s\n' "$OUT"
