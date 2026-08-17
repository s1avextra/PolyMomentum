#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=collect-binary-complement-floor.sh
source "$ROOT_DIR/deploy/collect-binary-complement-floor.sh"

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/polymomentum-floor-test.XXXXXX")"
trap 'rm -rf "$TMP_ROOT"' EXIT
BASE_DIR="$TMP_ROOT/new"
STATUS_PATH="$BASE_DIR/floor_collection_status.json"
SOURCE_ROOTS=("$TMP_ROOT/source-a" "$TMP_ROOT/source-b" "$BASE_DIR")
mkdir -p "$TMP_ROOT/source-a/segment_001" "$TMP_ROOT/source-b/segment_001"

sealed_status() {
    local groups="$1"
    jq -n --argjson groups "$groups" '{capture_verified:true,resolution_ready:true,
      admissible_conditions:$groups,admissible_groups:$groups,
      resolution_total_groups:$groups,resolution_ready_groups:$groups,
      distilled_events:100}'
}

resolution() {
    local ready="$1"
    local condition_id="$2"
    local open_ts="$3"
    jq -n \
        --argjson ready "$ready" \
        --arg condition_id "$condition_id" \
        --argjson open_ts "$open_ts" \
        '{a_plus_gate:{settlement_alignment_ready:$ready},markets:[{
          condition_id:$condition_id,open_ts_s:$open_ts,settlement_aligned:true,
          official_source_matches_btc_tape:true,terminal_direction:"up"}]}'
}

resolution true c1 1784092200 >"$TMP_ROOT/source-a/segment_001/resolution_group_001.json"
resolution true c1 1784092200 >"$TMP_ROOT/source-b/segment_001/resolution_group_001.json"
resolution true c2 1784092500 >"$TMP_ROOT/source-b/segment_001/resolution_group_002.json"
resolution false c3 1784092800 >"$TMP_ROOT/source-b/segment_001/resolution_group_003.json"
resolution true old 1784090000 >"$TMP_ROOT/source-b/segment_001/resolution_group_004.json"
sealed_status 1 >"$TMP_ROOT/source-a/segment_001/status.json"
sealed_status 4 >"$TMP_ROOT/source-b/segment_001/status.json"

[ "$(count_unique_ready_conditions "${SOURCE_ROOTS[@]}")" -eq 2 ]
mkdir -p "$TMP_ROOT/unsealed/segment_001" "$TMP_ROOT/rejected/segment_001"
resolution true ignored-unsealed 1784093000 >"$TMP_ROOT/unsealed/segment_001/resolution_group_001.json"
resolution true ignored-rejected 1784093000 >"$TMP_ROOT/rejected/segment_001/resolution_group_001.json"
jq -n '{capture_verified:true,resolution_ready:false,admissible_conditions:0,
  admissible_groups:0,resolution_total_groups:0,resolution_ready_groups:0,
  distilled_events:0}' >"$TMP_ROOT/rejected/segment_001/status.json"
[ "$(count_unique_ready_conditions "${SOURCE_ROOTS[@]}" "$TMP_ROOT/unsealed" "$TMP_ROOT/rejected")" -eq 2 ]
mkdir -p "$BASE_DIR/binary-complement-block1-floor-001/segment_001"
jq -n '{capture_verified:true}' >"$BASE_DIR/binary-complement-block1-floor-001/segment_001/status.json"
[ "$(completed_new_segments)" -eq 1 ]
[ "$(next_session_index)" -eq 2 ]
write_floor_status "COLLECTING" 2 1
jq -e '.state == "COLLECTING"
       and .unique_ready_terminal_conditions == 2
       and .strategy_metrics_disclosed == false
       and .target_terminal_conditions == 750' "$STATUS_PATH" >/dev/null

PENDING_DIR="$BASE_DIR/binary-complement-block1-floor-002/segment_001"
mkdir -p "$PENDING_DIR"
jq -n '{capture_verified:true,resolution_ready:false,admissible_conditions:1,
  resolution_verdict:"group_001:WAIT_FOR_TERMINAL_MARKETS"}' >"$PENDING_DIR/status.json"
SOURCE_PENDING_DIR="$TMP_ROOT/source-a/pending-session/segment_001"
mkdir -p "$SOURCE_PENDING_DIR"
jq -n '{capture_verified:true,resolution_ready:false,admissible_conditions:1,
  resolution_verdict:"group_001:WAIT_FOR_TERMINAL_MARKETS"}' >"$SOURCE_PENDING_DIR/status.json"
REFRESH_CALLS="$TMP_ROOT/refresh-calls"
export REFRESH_CALLS
CAPTURE_RUNNER="$TMP_ROOT/fake-capture-runner"
printf '%s\n' \
    '#!/bin/bash' \
    'set -euo pipefail' \
    'printf '\''%s\n'\'' "$*" >>"$REFRESH_CALLS"' \
    >"$CAPTURE_RUNNER"
chmod +x "$CAPTURE_RUNNER"
refresh_pending_segments
[ "$(wc -l <"$REFRESH_CALLS" | tr -d ' ')" -eq 2 ]
grep -q -- "--refresh-segment $PENDING_DIR" "$REFRESH_CALLS"
grep -q -- "--refresh-segment $SOURCE_PENDING_DIR" "$REFRESH_CALLS"
grep -q -- '--terminal-wait-attempts 1 --terminal-wait-seconds 0' "$REFRESH_CALLS"

echo "collect-binary-complement-floor tests passed"
