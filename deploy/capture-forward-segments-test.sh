#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=capture-forward-segments.sh
source "$ROOT_DIR/deploy/capture-forward-segments.sh"

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/polymomentum-capture-test.XXXXXX")"
REFRESH_SESSION="/tmp/polymomentum-capture-refresh-test-$$"
trap 'rm -rf "$TMP_ROOT" "$REFRESH_SESSION"' EXIT

SESSION="$TMP_ROOT/session-test"
RAW="$SESSION/segment_001/raw"
CONVERTED="$SESSION/segment_001/converted"
mkdir -p "$RAW" "$CONVERTED"
printf 'owned\n' >"$SESSION/.polymomentum-forward-capture-owned"
printf '{}\n' >"$RAW/gamma_market_cache.json"
printf '{"raw":"frame"}\n' >"$RAW/market_ws_frames.jsonl"
printf 'timestamp_ms,source,price,received_at_ms\n1,chainlink_btc_usd_data_stream,1,1\n' >"$RAW/chainlink_btcusd.csv"
printf 'timestamp_ms,source,price,received_at_ms\n1,binance_btcusdt_rtds,1,1\n' >"$RAW/binance_btcusdt_rtds.csv"

jq -n '{
  schema_version:2,
  duration_seconds:330,
  slugs:["a"],
  condition_ids:["c"],
  token_ids:["up","down"],
  stats:{frames:1,bytes:1},
  reference_tapes:{
    source_provenance_ready:true,
    official_chainlink_ready:true,
    binance_proxy_ready:true,
    stats:{chainlink:{ticks:1},binance:{ticks:1}}
  }
}' >"$RAW/summary.json"

jq -n '{
  stats:{clob_events:1,delay_samples:1,negative_delay_samples:0,missing_event_timestamp:0},
  a_plus_latency_gate:{
    stream_latency_ready:true,
    timestamp_ready:true,
    coverage_ready:true,
    token_gap_ready:true,
    backtest_latency_assumption_ready:false,
    recommended_retest_latency_ms:202
  },
  window_continuity:{conditions:1},
  window_admissibility:{
    conditions:1,
    admissible_conditions:1,
    excluded_conditions:0,
    has_admissible_conditions:true,
    groups:[{
      group:"group_001",
      conditions:1,
      first_open_ms:1784092200000,
      last_close_ms:1784092500000,
      condition_ids:["c"]
    }]
  }
}' >"$SESSION/segment_001/forward_latency_audit.json"

jq -n '{
  schema_version:1,
  stats:{
    book_events:1,
    change_events:1,
    skipped_malformed_lines:0,
    skipped_malformed_raw:0,
    skipped_missing_fields:0,
    skipped_unknown_market:0,
    skipped_unknown_token:0
  },
  tick_integrity:{
    schema_version:1,
    raw_tick_size_change_rows:0,
    markets_with_tick_size_change:0,
    malformed_selected_tick_size_change_rows:0,
    transitions_match_documented_contract:true,
    all_observed_transitions_reconstructable:true,
    distilled_schema_changed:false,
    tick_size_change_events_preserved_in_distilled_stream:false,
    markets:[]
  },
  hours:[{hour:"test"}],
  markets:{c:{slug:"test"}},
  selection:{
    filtered_to_condition_ids:true,
    source_market_count:1,
    selected_market_count:1,
    selected_condition_ids:["c"]
  },
  output:{
    exact_replay_flag:"--require-shared-distilled",
    harness_env:{PMXT_DISTILLED_DIR:"converted"}
  }
}' >"$CONVERTED/manifest.json"

jq -n '{
  schema_version:1,
  stats:{markets:1},
  btc_tape:{source:{provenance:{official_chainlink_provenance_ready:true}}},
  a_plus_gate:{verdict:"WAIT_FOR_TERMINAL_MARKETS",settlement_alignment_ready:false}
}' >"$SESSION/segment_001/resolution_manifest.json"

verify_record_capture "$RAW" 1
verify_latency_audit "$SESSION/segment_001/forward_latency_audit.json"
ZERO_AUDIT="$TMP_ROOT/zero-admissible-audit.json"
jq '.window_admissibility.admissible_conditions = 0
    | .window_admissibility.excluded_conditions = 1
    | .window_admissibility.has_admissible_conditions = false
    | .window_admissibility.groups = []' \
    "$SESSION/segment_001/forward_latency_audit.json" >"$ZERO_AUDIT"
verify_zero_admissible_audit "$ZERO_AUDIT"
if verify_latency_audit "$ZERO_AUDIT"; then
    echo "capture accepted a segment with zero admissible conditions" >&2
    exit 1
fi
verify_conversion "$CONVERTED/manifest.json" 1
verify_refreshable_conversion "$CONVERTED/manifest.json" "$SESSION/segment_001/forward_latency_audit.json" 1
INVALID_TICK_MANIFEST="$TMP_ROOT/invalid-tick-manifest.json"
jq '.tick_integrity.all_observed_transitions_reconstructable = false' \
    "$CONVERTED/manifest.json" >"$INVALID_TICK_MANIFEST"
if verify_conversion "$INVALID_TICK_MANIFEST" 1; then
    echo "conversion accepted an unreconstructable tick transition" >&2
    exit 1
fi
PRE_V5_MANIFEST="$TMP_ROOT/pre-v5-manifest.json"
jq 'del(.tick_integrity)' "$CONVERTED/manifest.json" >"$PRE_V5_MANIFEST"
verify_refreshable_conversion \
    "$PRE_V5_MANIFEST" "$SESSION/segment_001/forward_latency_audit.json" 1
PRE_V5_WRONG_SELECTION="$TMP_ROOT/pre-v5-wrong-selection.json"
jq '.selection.selected_condition_ids = ["wrong"] | del(.tick_integrity)' \
    "$CONVERTED/manifest.json" >"$PRE_V5_WRONG_SELECTION"
if verify_refreshable_conversion \
    "$PRE_V5_WRONG_SELECTION" "$SESSION/segment_001/forward_latency_audit.json" 1; then
    echo "pre-v5 conversion accepted the wrong sealed condition set" >&2
    exit 1
fi
LEGACY_MANIFEST="$TMP_ROOT/legacy-manifest.json"
jq '.selection = null | .markets = {c:{slug:"test"}} | del(.tick_integrity)' \
    "$CONVERTED/manifest.json" >"$LEGACY_MANIFEST"
verify_refreshable_conversion "$LEGACY_MANIFEST" "$SESSION/segment_001/forward_latency_audit.json" 1
jq '.markets = {}' "$LEGACY_MANIFEST" >"$TMP_ROOT/legacy-missing.json"
if verify_refreshable_conversion "$TMP_ROOT/legacy-missing.json" "$SESSION/segment_001/forward_latency_audit.json" 1; then
    echo "legacy conversion accepted without the audited condition" >&2
    exit 1
fi
verify_resolution_manifest "$SESSION/segment_001/resolution_manifest.json"

FAKE_BINARY="$TMP_ROOT/fake-measurement"
printf '%s\n' \
    '#!/bin/bash' \
    'set -euo pipefail' \
    'output=""' \
    'condition_ids=()' \
    'while [ $# -gt 0 ]; do' \
    '  case "$1" in' \
    '    --output) output="$2"; shift 2 ;;' \
    '    --condition-id) condition_ids+=("$2"); shift 2 ;;' \
    '    *) shift ;;' \
    '  esac' \
    'done' \
    'ready=true' \
    'verdict="FORWARD_GROUND_TRUTH_READY_NEEDS_SAMPLE_SIZE"' \
    'if [ -n "${FAKE_FINALIZE_COUNTER:-}" ]; then' \
    '  count=0' \
    '  [ ! -f "$FAKE_FINALIZE_COUNTER" ] || read -r count <"$FAKE_FINALIZE_COUNTER"' \
    '  count=$(( count + 1 ))' \
    '  printf '\''%s\n'\'' "$count" >"$FAKE_FINALIZE_COUNTER"' \
    '  if [ "$count" -lt "${FAKE_FINALIZE_READY_AFTER:-1}" ]; then' \
    '    ready=false' \
    '    verdict="WAIT_FOR_TERMINAL_MARKETS"' \
    '  fi' \
    'fi' \
    'jq -n --argjson markets "${#condition_ids[@]}" --arg verdict "$verdict" --argjson ready "$ready" '\''{schema_version:1,stats:{markets:$markets},btc_tape:{source:{provenance:{official_chainlink_provenance_ready:true}}},a_plus_gate:{verdict:$verdict,settlement_alignment_ready:$ready}}'\'' >"$output"' \
    >"$FAKE_BINARY"
chmod +x "$FAKE_BINARY"
finalize_admissible_groups \
    "$FAKE_BINARY" \
    "$SESSION/segment_001" \
    "$CONVERTED" \
    "$RAW/chainlink_btcusd.csv"
jq -e '.all_ready == true and .total_groups == 1 and .selected_conditions == 1' \
    "$SESSION/segment_001/resolution_summary.json" >/dev/null
write_segment_status "$SESSION/segment_001" 1 "2026-07-18T06:50:00Z" false true "ALL_ADMISSIBLE_GROUPS_READY" false
jq -e '.admissible_conditions == 1
       and .resolution_ready_groups == 1
       and .resolution_total_groups == 1
       and .full_segment_signal_coverage == false' \
    "$SESSION/segment_001/status.json" >/dev/null

export FAKE_FINALIZE_COUNTER="$TMP_ROOT/finalize-counter"
export FAKE_FINALIZE_READY_AFTER=3
finalize_admissible_groups \
    "$FAKE_BINARY" \
    "$SESSION/segment_001" \
    "$CONVERTED" \
    "$RAW/chainlink_btcusd.csv" \
    3 \
    0
[ "$(cat "$FAKE_FINALIZE_COUNTER")" -eq 3 ]
jq -e '.all_ready == true and .groups[0].verdict == "FORWARD_GROUND_TRUTH_READY_NEEDS_SAMPLE_SIZE"' \
    "$SESSION/segment_001/resolution_summary.json" >/dev/null
unset FAKE_FINALIZE_COUNTER FAKE_FINALIZE_READY_AFTER

mkdir -p "$REFRESH_SESSION"
printf 'owned\n' >"$REFRESH_SESSION/.polymomentum-forward-capture-owned"
cp -R "$SESSION/segment_001" "$REFRESH_SESSION/segment_001"
jq '.all_ready = false
    | .ready_groups = 0
    | .groups[0].ready = false
    | .groups[0].verdict = "WAIT_FOR_TERMINAL_MARKETS"' \
    "$REFRESH_SESSION/segment_001/resolution_summary.json" >"$TMP_ROOT/pending-summary.json"
mv "$TMP_ROOT/pending-summary.json" "$REFRESH_SESSION/segment_001/resolution_summary.json"
write_segment_status \
    "$REFRESH_SESSION/segment_001" 1 "2026-07-18T06:50:00Z" false false \
    "group_001:WAIT_FOR_TERMINAL_MARKETS" false
refresh_segment_resolution "$FAKE_BINARY" "$REFRESH_SESSION/segment_001" 1 0
jq -e '.resolution_ready == true and .resolution_verdict == "ALL_ADMISSIBLE_GROUPS_READY"' \
    "$REFRESH_SESSION/segment_001/status.json" >/dev/null
jq -e '.resolution_ready_segments == 1' "$REFRESH_SESSION/session_summary.json" >/dev/null

ZERO_SEGMENT="$SESSION/segment_002"
mkdir -p "$ZERO_SEGMENT"
cp "$ZERO_AUDIT" "$ZERO_SEGMENT/forward_latency_audit.json"
write_zero_admissible_segment_status \
    "$ZERO_SEGMENT" 2 "2026-07-18T08:50:00Z" true false
jq -e '.capture_verified == true
       and .admissible_conditions == 0
       and .excluded_conditions == 1
       and .session_owned_frames_deleted == true
       and .resolution_ready == false
       and .resolution_verdict == "NO_ADMISSIBLE_CONDITIONS"' \
    "$ZERO_SEGMENT/status.json" >/dev/null

printf 'timestamp_ms,source,price,received_at_ms\n1784088600000,binance_btcusdt_rtds,1,1784088600000\n1784092500000,binance_btcusdt_rtds,1,1784092500000\n' >"$RAW/binance_btcusdt_rtds.csv"
verify_replay_signal_coverage "$RAW/binance_btcusdt_rtds.csv" 1784092200 1
printf 'timestamp_ms,source,price,received_at_ms\n' >"$RAW/binance_btcusdt_rtds.csv"
timestamp_ms=1784088600000
while [ "$timestamp_ms" -le 1784092500000 ]; do
    if [ "$timestamp_ms" -lt 1784090486000 ] || [ "$timestamp_ms" -gt 1784090505000 ]; then
        printf '%s,binance_btcusdt_rtds,1,%s\n' "$timestamp_ms" "$timestamp_ms" >>"$RAW/binance_btcusdt_rtds.csv"
    fi
    timestamp_ms=$(( timestamp_ms + 1000 ))
done
if verify_replay_signal_coverage "$RAW/binance_btcusdt_rtds.csv" 1784092200 1; then
    echo "capture accepted a Binance tape with an internal 21-second gap" >&2
    exit 1
fi
printf 'timestamp_ms,source,price,received_at_ms\n1784092170000,binance_btcusdt_rtds,1,1784092170000\n1784092500000,binance_btcusdt_rtds,1,1784092500000\n' >"$RAW/binance_btcusdt_rtds.csv"
if verify_replay_signal_coverage "$RAW/binance_btcusdt_rtds.csv" 1784092200 1; then
    echo "capture accepted a Binance tape without a causal hour" >&2
    exit 1
fi

DRY_RUN="$TMP_ROOT/dry-run.txt"
main --base-dir "/tmp/polymomentum-capture-test-dry-$$" --session-id future-plan --segments 1 --windows-per-segment 1 --dry-run >"$DRY_RUN"
grep -q 'signal_preroll_seconds=3600' "$DRY_RUN"
grep -q 'capture_seconds=3960' "$DRY_RUN"
grep -q 'terminal_wait_attempts=31 terminal_wait_seconds=30' "$DRY_RUN"
if main --base-dir "/tmp/polymomentum-capture-test-dry-$$" \
    --session-id invalid-delete-plan \
    --delete-session-owned-frames-after-zero-admissible-audit \
    --dry-run >/dev/null 2>&1; then
    echo "capture accepted zero-admissible deletion without continuation mode" >&2
    exit 1
fi
main --base-dir "/tmp/polymomentum-capture-test-dry-$$" \
    --session-id robust-plan \
    --continue-after-zero-admissible \
    --delete-session-owned-frames-after-zero-admissible-audit \
    --dry-run >"$DRY_RUN"
grep -q 'continue_after_zero_admissible=true' "$DRY_RUN"
grep -q 'delete_zero_admissible_frames=true' "$DRY_RUN"
if main --base-dir "/tmp/polymomentum-capture-test-dry-$$" --session-id invalid-plan --signal-preroll-seconds 3599 --dry-run >/dev/null 2>&1; then
    echo "capture accepted less than one causal hour of signal pre-roll" >&2
    exit 1
fi

jq '.reference_tapes.source_provenance_ready = false' "$RAW/summary.json" >"$RAW/summary.bad.json"
mv "$RAW/summary.json" "$RAW/summary.good.json"
mv "$RAW/summary.bad.json" "$RAW/summary.json"
if verify_record_capture "$RAW" 1 >/dev/null 2>&1; then
    echo "capture with failed source provenance unexpectedly passed" >&2
    exit 1
fi
mv "$RAW/summary.good.json" "$RAW/summary.json"

if validate_private_base_path "/opt/shared/pmxt_v2_cache" >/dev/null 2>&1; then
    echo "shared path unexpectedly accepted" >&2
    exit 1
fi

BAD_SESSION="$TMP_ROOT/bad-session"
BAD_RAW="$BAD_SESSION/segment_001/raw"
mkdir -p "$BAD_RAW"
printf 'frame\n' >"$BAD_RAW/market_ws_frames.jsonl"
if delete_session_owned_frames "$BAD_SESSION" "$BAD_RAW" >/dev/null 2>&1; then
    echo "frame deletion unexpectedly succeeded without ownership marker" >&2
    exit 1
fi
test -f "$BAD_RAW/market_ws_frames.jsonl"

delete_session_owned_frames "$SESSION" "$RAW"
test ! -e "$RAW/market_ws_frames.jsonl"
test -s "$RAW/chainlink_btcusd.csv"
test -s "$CONVERTED/manifest.json"

echo "capture-forward-segments tests passed"
