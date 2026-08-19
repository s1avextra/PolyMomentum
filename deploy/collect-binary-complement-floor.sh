#!/bin/bash
# Collect a fixed-support binary-complement block without inspecting strategy outcomes.
set -euo pipefail

TARGET_CONDITIONS=750
MAX_NEW_SEGMENTS=96
PREREGISTERED_EPOCH=1784090954
WAIT_UNIT="polymomentum-binary-complement-block1-continuation-20260718.service"
WAIT_MAX_SECONDS=57600
POLL_SECONDS=30
TERMINAL_WAIT_ATTEMPTS=31
TERMINAL_WAIT_SECONDS=30
BINARY="${POLYMOMENTUM_MEASUREMENT_BINARY:-/opt/polymomentum/tools/polymomentum-engine-measurement-v4}"
CAPTURE_RUNNER="${POLYMOMENTUM_CAPTURE_RUNNER:-/opt/polymomentum/tools/capture-forward-segments-v4.sh}"
BASE_DIR="/opt/polymomentum/logs/forward-captures/binary-complement-block1-floor"
SESSION_PREFIX="binary-complement-block1-floor"
STATUS_PATH="$BASE_DIR/floor_collection_status.json"
SOURCE_ROOTS=(
    "/opt/polymomentum/logs/forward-captures/binary-complement-block1-seg003-20260718"
    "/opt/polymomentum/logs/forward-captures/binary-complement-block1-seg004-007-20260718"
    "$BASE_DIR"
)

log() {
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

sealed_segment_dirs() {
    local root status segment_dir expected_groups actual_groups
    for root in "$@"; do
        [ -d "$root" ] || continue
        while IFS= read -r status; do
            jq -e '
                .capture_verified == true
                and .resolution_ready == true
                and .admissible_conditions > 0
                and .admissible_groups > 0
                and .resolution_total_groups == .admissible_groups
                and .resolution_ready_groups == .admissible_groups
                and .distilled_events > 0
            ' "$status" >/dev/null 2>&1 || continue
            segment_dir="$(dirname "$status")"
            expected_groups="$(jq -r '.admissible_groups' "$status")"
            actual_groups="$(find "$segment_dir" -maxdepth 1 -type f -name 'resolution_group_*.json' -print | wc -l | tr -d ' ')"
            [ "$actual_groups" -eq "$expected_groups" ] || continue
            echo "$segment_dir"
        done < <(find "$root" -type f -name status.json -print | LC_ALL=C sort)
    done | LC_ALL=C sort -u
}

resolution_manifests() {
    local segment_dir
    while IFS= read -r segment_dir; do
        find "$segment_dir" -maxdepth 1 -type f -name 'resolution_group_*.json' -print
    done < <(sealed_segment_dirs "$@") | LC_ALL=C sort -u
}

count_unique_ready_conditions() {
    local roots=("$@")
    local manifests=()
    local manifest
    while IFS= read -r manifest; do
        manifests+=("$manifest")
    done < <(resolution_manifests "${roots[@]}")
    if [ "${#manifests[@]}" -eq 0 ]; then
        echo 0
        return 0
    fi
    jq -s --argjson not_before "$PREREGISTERED_EPOCH" '
        [ .[]
          | select(.a_plus_gate.settlement_alignment_ready == true)
          | .markets[]
          | select(.settlement_aligned == true)
          | select(.official_source_matches_btc_tape == true)
          | select(.open_ts_s >= $not_before)
          | select(.terminal_direction == "up" or .terminal_direction == "down")
          | .condition_id ]
        | unique
        | length
    ' "${manifests[@]}"
}

completed_new_segments() {
    if [ ! -d "$BASE_DIR" ]; then
        echo 0
        return 0
    fi
    find "$BASE_DIR" -type f -path '*/segment_001/status.json' -print | wc -l | tr -d ' '
}

write_floor_status() {
    local state="$1"
    local ready_conditions="$2"
    local attempted_segments="$3"
    local tmp="$STATUS_PATH.tmp.$$"
    mkdir -p "$BASE_DIR"
    jq -n \
        --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --arg state "$state" \
        --argjson ready_conditions "$ready_conditions" \
        --argjson attempted_segments "$attempted_segments" \
        --argjson target "$TARGET_CONDITIONS" \
        --argjson maximum "$MAX_NEW_SEGMENTS" \
        '{schema_version:1, generated_at:$generated_at, mechanism_id:"binary_complement_coherence_v1",
          state:$state, target_terminal_conditions:$target,
          unique_ready_terminal_conditions:$ready_conditions,
          new_segments_completed:$attempted_segments,
          maximum_new_segments:$maximum,
          strategy_metrics_disclosed:false,
          stopping_rule:"condition support only; no strategy outcome or rate is inspected"}' >"$tmp"
    mv "$tmp" "$STATUS_PATH"
}

next_session_index() {
    local index=1
    while [ -e "$BASE_DIR/${SESSION_PREFIX}-$(printf '%03d' "$index")" ]; do
        index=$(( index + 1 ))
    done
    echo "$index"
}

refresh_pending_segments() {
    local status segment_dir root
    while IFS= read -r status; do
        jq -e '
            .capture_verified == true
            and .resolution_ready == false
            and .admissible_conditions > 0
            and (.resolution_verdict | contains("WAIT_FOR_TERMINAL_MARKETS"))
        ' "$status" >/dev/null 2>&1 || continue
        segment_dir="$(dirname "$status")"
        log "refreshing terminal-only pending segment: $segment_dir"
        "$CAPTURE_RUNNER" \
            --binary "$BINARY" \
            --refresh-segment "$segment_dir" \
            --terminal-wait-attempts 1 \
            --terminal-wait-seconds 0
    done < <(
        for root in "${SOURCE_ROOTS[@]}"; do
            [ -d "$root" ] || continue
            find "$root" -type f -path '*/segment_*/status.json' -print
        done | LC_ALL=C sort -u
    )
}

main() {
    local refresh_only=false
    while [ $# -gt 0 ]; do
        case "$1" in
            --refresh-only) refresh_only=true; shift ;;
            -h|--help)
                echo "Usage: collect-binary-complement-floor.sh [--refresh-only]"
                return 0
                ;;
            *) echo "collect-binary-complement-floor: unknown argument: $1" >&2; return 2 ;;
        esac
    done
    [ "$(id -un)" = "polymomentum" ] || {
        echo "collect-binary-complement-floor: run as polymomentum" >&2
        return 2
    }
    cd / || return 1
    [ -x "$BINARY" ] || {
        echo "collect-binary-complement-floor: binary is not executable: $BINARY" >&2
        return 1
    }
    [ -x "$CAPTURE_RUNNER" ] || {
        echo "collect-binary-complement-floor: runner is not executable: $CAPTURE_RUNNER" >&2
        return 1
    }

    if [ "$refresh_only" = true ]; then
        refresh_pending_segments
        local refreshed_ready refreshed_attempted refreshed_state="COLLECTING"
        refreshed_ready="$(count_unique_ready_conditions "${SOURCE_ROOTS[@]}")"
        refreshed_attempted="$(completed_new_segments)"
        if [ "$refreshed_ready" -ge "$TARGET_CONDITIONS" ]; then
            refreshed_state="TARGET_REACHED_METRICS_STILL_SEALED"
        fi
        write_floor_status "$refreshed_state" "$refreshed_ready" "$refreshed_attempted"
        log "refresh complete; support is ${refreshed_ready}/${TARGET_CONDITIONS}"
        return 0
    fi

    local deadline=$(( $(date -u +%s) + WAIT_MAX_SECONDS ))
    while systemctl is-active --quiet "$WAIT_UNIT"; do
        if [ "$(date -u +%s)" -ge "$deadline" ]; then
            log "timed out waiting for $WAIT_UNIT"
            return 1
        fi
        sleep "$POLL_SECONDS"
    done

    mkdir -p "$BASE_DIR"
    local ready_conditions attempted_segments previous_ready session_index session_id
    refresh_pending_segments
    ready_conditions="$(count_unique_ready_conditions "${SOURCE_ROOTS[@]}")"
    attempted_segments="$(completed_new_segments)"
    write_floor_status "COLLECTING" "$ready_conditions" "$attempted_segments"
    log "starting sealed support collection at ${ready_conditions}/${TARGET_CONDITIONS} ready conditions"

    while [ "$ready_conditions" -lt "$TARGET_CONDITIONS" ] \
        && [ "$attempted_segments" -lt "$MAX_NEW_SEGMENTS" ]; do
        session_index="$(next_session_index)"
        session_id="${SESSION_PREFIX}-$(printf '%03d' "$session_index")"
        previous_ready="$ready_conditions"
        "$CAPTURE_RUNNER" \
            --binary "$BINARY" \
            --base-dir "$BASE_DIR" \
            --session-id "$session_id" \
            --segments 1 \
            --windows-per-segment 24 \
            --signal-preroll-seconds 3600 \
            --estimated-bytes-per-second 350000 \
            --reserve-gb 5 \
            --terminal-wait-attempts "$TERMINAL_WAIT_ATTEMPTS" \
            --terminal-wait-seconds "$TERMINAL_WAIT_SECONDS" \
            --delete-session-owned-frames-after-verify \
            --continue-after-zero-admissible \
            --delete-session-owned-frames-after-zero-admissible-audit

        attempted_segments="$(completed_new_segments)"
        refresh_pending_segments
        ready_conditions="$(count_unique_ready_conditions "${SOURCE_ROOTS[@]}")"
        [ "$ready_conditions" -ge "$previous_ready" ] || {
            log "ready-condition count regressed from $previous_ready to $ready_conditions"
            return 1
        }
        write_floor_status "COLLECTING" "$ready_conditions" "$attempted_segments"
        log "$session_id sealed; support is ${ready_conditions}/${TARGET_CONDITIONS}"
    done

    if [ "$ready_conditions" -ge "$TARGET_CONDITIONS" ]; then
        write_floor_status "TARGET_REACHED_METRICS_STILL_SEALED" "$ready_conditions" "$attempted_segments"
        log "fixed support target reached; strategy metrics remain sealed for one explicit score"
        return 0
    fi

    write_floor_status "MAX_SEGMENTS_EXHAUSTED_TARGET_NOT_REACHED" "$ready_conditions" "$attempted_segments"
    log "maximum segments exhausted at ${ready_conditions}/${TARGET_CONDITIONS}"
    return 1
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    main "$@"
fi
