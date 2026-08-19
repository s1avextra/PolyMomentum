#!/bin/bash
# Capture fresh BTC 5-minute CLOB and reference data in disk-bounded segments.
# This script never places orders and only removes its own market_ws_frames.jsonl
# when the caller explicitly opts in and capture/audit/conversion checks pass.
set -euo pipefail

umask 027

DEFAULT_BINARY="/opt/polymomentum/tools/polymomentum-engine-measurement"
DEFAULT_BASE_DIR="/opt/polymomentum/logs/forward-captures"
DEFAULT_SEGMENTS=1
DEFAULT_WINDOWS_PER_SEGMENT=24
DEFAULT_PADDING_SECONDS=30
DEFAULT_SIGNAL_PREROLL_SECONDS=3600
DEFAULT_ESTIMATED_BYTES_PER_SECOND=600000
DEFAULT_RESERVE_GB=8
DEFAULT_TERMINAL_WAIT_ATTEMPTS=31
DEFAULT_TERMINAL_WAIT_SECONDS=30
WINDOW_SECONDS=300

usage() {
    cat <<'EOF'
Usage: capture-forward-segments.sh [options]

Options:
  --binary PATH                 Measurement-capable polymomentum-engine.
  --base-dir PATH               Private capture parent directory.
  --session-id ID               New session directory name (default: UTC timestamp).
  --segments N                  Number of independent segments (default: 1).
  --windows-per-segment N       Complete 5-minute windows per segment (default: 24).
  --padding-seconds N           Tape coverage before/after window boundaries (default: 30).
  --signal-preroll-seconds N    Causal Binance history before the first window (minimum/default: 3600).
  --estimated-bytes-per-second N
                                Conservative raw-frame disk estimate (default: 600000).
  --reserve-gb N                Free disk preserved after estimated raw capture (default: 8).
  --terminal-wait-attempts N    Terminal-finalization attempts (default: 31).
  --terminal-wait-seconds N     Seconds between terminal attempts (default: 30).
  --refresh-segment DIR         Recheck one verified segment that is waiting only
                                for terminal Gamma outcomes; do not capture data.
  --delete-session-owned-frames-after-verify
                                Delete only each verified segment's market_ws_frames.jsonl.
  --continue-after-zero-admissible
                                Record a rejected status and continue when capture and
                                audit are valid but no condition is admissible.
  --delete-session-owned-frames-after-zero-admissible-audit
                                With the preceding flag, delete only the rejected
                                segment's owned frame log after its audit is sealed.
  --dry-run                     Print the bounded plan without writing or waiting.
  -h, --help                    Show this help.

The runner aligns each segment to the next full 5-minute boundary, records the
official Chainlink and Binance RTDS tapes with the CLOB stream, audits latency,
converts to distilled replay files, and then attempts terminal finalization.
Resolution failures remain visible and never become promotion evidence.
EOF
}

fail() {
    echo "capture-forward-segments: $*" >&2
    return 1
}

log() {
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

is_positive_integer() {
    case "$1" in
        ''|*[!0-9]*|0) return 1 ;;
        *) return 0 ;;
    esac
}

is_nonnegative_integer() {
    case "$1" in
        ''|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

validate_private_base_path() {
    local path="$1"
    case "$path" in
        *'/../'*|*/..|*'/./'*|*/.|*'//'*|.|..)
            fail "base path must not contain traversal or ambiguous components: $path"
            return 1
            ;;
    esac
    case "$path" in
        /opt/polymomentum/*|/private/tmp/polymomentum-*|/private/tmp/polymomentum-*/*|/tmp/polymomentum-*|/tmp/polymomentum-*/*)
            return 0
            ;;
        *)
            fail "base path must stay under /opt/polymomentum or a polymomentum-named local temp directory: $path"
            return 1
            ;;
    esac
}

format_epoch_utc() {
    local epoch="$1"
    if date -u -d "@$epoch" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null; then
        return 0
    fi
    date -u -r "$epoch" +%Y-%m-%dT%H:%M:%SZ
}

print_command() {
    printf '  '
    printf '%q ' "$@"
    printf '\n'
}

run_logged() {
    local stdout_path="$1"
    local stderr_path="$2"
    shift 2
    if "$@" >"$stdout_path" 2>"$stderr_path"; then
        return 0
    else
        local status=$?
        echo "command failed (exit $status): $*" >&2
        tail -40 "$stderr_path" >&2 || true
        return "$status"
    fi
}

verify_record_capture() {
    local raw_dir="$1"
    local expected_windows="$2"
    local summary="$raw_dir/summary.json"
    jq -e --argjson windows "$expected_windows" '
        (.schema_version >= 2)
        and (.duration_seconds > 0)
        and ((.slugs | length) == $windows)
        and ((.condition_ids | length) == $windows)
        and ((.token_ids | length) == ($windows * 2))
        and (.stats.frames > 0)
        and (.stats.bytes > 0)
        and (.reference_tapes.source_provenance_ready == true)
        and (.reference_tapes.official_chainlink_ready == true)
        and (.reference_tapes.binance_proxy_ready == true)
        and (.reference_tapes.stats.chainlink.ticks > 0)
        and (.reference_tapes.stats.binance.ticks > 0)
    ' "$summary" >/dev/null || return 1
    test -s "$raw_dir/market_ws_frames.jsonl" || return 1
    test -s "$raw_dir/gamma_market_cache.json" || return 1
    test -s "$raw_dir/chainlink_btcusd.csv" || return 1
    test -s "$raw_dir/binance_btcusdt_rtds.csv" || return 1
}

verify_latency_audit() {
    local audit="$1"
    jq -e '
        (.stats.clob_events > 0)
        and (.stats.delay_samples > 0)
        and (.stats.negative_delay_samples == 0)
        and (.stats.missing_event_timestamp == 0)
        and (.a_plus_latency_gate.stream_latency_ready == true)
        and (.a_plus_latency_gate.timestamp_ready == true)
        and (.a_plus_latency_gate.recommended_retest_latency_ms >= 50)
        and (.window_admissibility.conditions > 0)
        and (.window_admissibility.conditions == .window_continuity.conditions)
        and (.window_admissibility.has_admissible_conditions == true)
    ' "$audit" >/dev/null
}

verify_zero_admissible_audit() {
    local audit="$1"
    jq -e '
        (.stats.clob_events > 0)
        and (.stats.delay_samples > 0)
        and (.stats.negative_delay_samples == 0)
        and (.stats.missing_event_timestamp == 0)
        and (.a_plus_latency_gate.stream_latency_ready == true)
        and (.a_plus_latency_gate.timestamp_ready == true)
        and (.a_plus_latency_gate.recommended_retest_latency_ms >= 50)
        and (.window_admissibility.conditions > 0)
        and (.window_admissibility.conditions == .window_continuity.conditions)
        and (.window_admissibility.admissible_conditions == 0)
        and (.window_admissibility.excluded_conditions == .window_admissibility.conditions)
        and (.window_admissibility.has_admissible_conditions == false)
        and ((.window_admissibility.groups | length) == 0)
    ' "$audit" >/dev/null
}

verify_conversion() {
    local manifest="$1"
    local expected_conditions="$2"
    jq -e --argjson expected "$expected_conditions" '
        (.schema_version == 1)
        and (((.stats.book_events // 0) + (.stats.change_events // 0)) > 0)
        and (.stats.skipped_malformed_lines == 0)
        and (.stats.skipped_malformed_raw == 0)
        and (.stats.skipped_missing_fields == 0)
        and (.stats.skipped_unknown_market == 0)
        and (.stats.skipped_unknown_token == 0)
        and (.tick_integrity.schema_version == 1)
        and (.tick_integrity.malformed_selected_tick_size_change_rows == 0)
        and (.tick_integrity.transitions_match_documented_contract == true)
        and (.tick_integrity.all_observed_transitions_reconstructable == true)
        and (.tick_integrity.distilled_schema_changed == false)
        and (.tick_integrity.tick_size_change_events_preserved_in_distilled_stream == false)
        and ((.hours | length) > 0)
        and ((.markets | length) == $expected)
        and (.selection.filtered_to_condition_ids == true)
        and (.selection.selected_market_count == $expected)
        and ((.selection.selected_condition_ids | length) == $expected)
        and (.output.exact_replay_flag == "--require-shared-distilled")
        and (.output.harness_env.PMXT_DISTILLED_DIR | type == "string")
    ' "$manifest" >/dev/null
}

verify_refreshable_conversion() {
    local manifest="$1"
    local audit="$2"
    local expected_conditions="$3"
    if verify_conversion "$manifest" "$expected_conditions"; then
        return 0
    fi

    # Pre-v5 segments lack tick_integrity. Accept them only when their selected
    # IDs exactly match the sealed admissibility audit, or—before condition
    # filtering existed—when the converted universe contains every audited ID.
    jq -e --argjson expected "$expected_conditions" --slurpfile audit "$audit" '
        ($audit[0].window_admissibility.groups | map(.condition_ids[]) | unique) as $required
        | (.markets | keys) as $converted
        | (.schema_version == 1)
        and (((.stats.book_events // 0) + (.stats.change_events // 0)) > 0)
        and (.stats.skipped_malformed_lines == 0)
        and (.stats.skipped_malformed_raw == 0)
        and (.stats.skipped_missing_fields == 0)
        and (.stats.skipped_unknown_market == 0)
        and (.stats.skipped_unknown_token == 0)
        and ((.hours | length) > 0)
        and (($required | length) == $expected)
        and (
            ((.selection == null)
             and (($converted | length) >= $expected)
             and (([$required[] | select(. as $id | $converted | index($id) == null)] | length) == 0))
            or
            ((.tick_integrity == null)
             and (.selection.filtered_to_condition_ids == true)
             and (.selection.selected_market_count == $expected)
             and ((.selection.selected_condition_ids | sort) == ($required | sort))
             and (($converted | sort) == ($required | sort)))
        )
        and (.output.exact_replay_flag == "--require-shared-distilled")
        and (.output.harness_env.PMXT_DISTILLED_DIR | type == "string")
    ' "$manifest" >/dev/null
}

verify_resolution_manifest() {
    local manifest="$1"
    jq -e '
        (.schema_version == 1)
        and (.stats.markets > 0)
        and (.btc_tape.source.provenance.official_chainlink_provenance_ready == true)
        and (.a_plus_gate.verdict | type == "string")
    ' "$manifest" >/dev/null
}

csv_timestamp_bounds_ms() {
    local csv="$1"
    awk -F, '
        NR > 1 && $1 ~ /^[0-9]+$/ {
            value = $1 + 0
            if (!seen || value < min) min = value
            if (!seen || value > max) max = value
            seen = 1
        }
        END {
            if (!seen) exit 1
            printf "%.0f %.0f\n", min, max
        }
    ' "$csv"
}

csv_internal_gap_stats_ms() {
    local csv="$1"
    local required_start_ms="$2"
    local required_end_ms="$3"
    awk -F, '
        NR > 1 && $1 ~ /^[0-9]+$/ { printf "%.0f\n", $1 + 0 }
    ' "$csv" \
        | LC_ALL=C sort -n -u \
        | awk -v start="$required_start_ms" -v end="$required_end_ms" '
            $1 <= end {
                current = $1 + 0
                if (current >= start && seen_previous) {
                    gap = current - previous
                    if (gap > 0) {
                        gaps[gap]++
                        interval_count++
                        if (gap > max_gap) max_gap = gap
                    }
                }
                previous = current
                seen_previous = 1
            }
            END {
                if (interval_count == 0) exit 1
                target_rank = int(interval_count / 2) + 1
                rank = 0
                for (gap = 1; gap <= max_gap; gap++) {
                    rank += gaps[gap]
                    if (rank >= target_rank) {
                        median_gap = gap
                        break
                    }
                }
                allowed_gap = median_gap * 3
                if (allowed_gap < 5000) allowed_gap = 5000
                printf "%d %d %d\n", median_gap, max_gap, allowed_gap
            }
        '
}

verify_replay_signal_coverage() {
    local binance_csv="$1"
    local first_window_epoch="$2"
    local windows="$3"
    local required_start_ms required_end_ms first_ms last_ms
    local median_gap_ms max_gap_ms allowed_gap_ms
    required_start_ms=$(( first_window_epoch * 1000 - DEFAULT_SIGNAL_PREROLL_SECONDS * 1000 ))
    required_end_ms=$(( (first_window_epoch + windows * WINDOW_SECONDS) * 1000 ))
    read -r first_ms last_ms < <(csv_timestamp_bounds_ms "$binance_csv") || return 1
    [ "$first_ms" -le $(( required_start_ms + 1000 )) ] || return 1
    [ "$last_ms" -ge "$required_end_ms" ] || return 1
    read -r median_gap_ms max_gap_ms allowed_gap_ms < <(
        csv_internal_gap_stats_ms "$binance_csv" "$required_start_ms" "$required_end_ms"
    ) || return 1
    [ "$max_gap_ms" -le "$allowed_gap_ms" ]
}

delete_session_owned_frames() {
    local session_dir="$1"
    local raw_dir="$2"
    local session_real raw_real frames
    test -f "$session_dir/.polymomentum-forward-capture-owned" || {
        fail "session ownership marker missing; refusing frame deletion"
        return 1
    }
    session_real="$(cd "$session_dir" && pwd -P)"
    raw_real="$(cd "$raw_dir" && pwd -P)"
    case "$raw_real" in
        "$session_real"/segment_[0-9][0-9][0-9]/raw) ;;
        *)
            fail "raw directory is not an owned segment path: $raw_real"
            return 1
            ;;
    esac
    frames="$raw_real/market_ws_frames.jsonl"
    test -f "$frames" || {
        fail "owned frame log is missing: $frames"
        return 1
    }
    rm -- "$frames"
}

check_disk_capacity() {
    local base_dir="$1"
    local capture_seconds="$2"
    local bytes_per_second="$3"
    local reserve_gb="$4"
    local available_kb capture_kb required_kb
    available_kb="$(df -Pk "$base_dir" | awk 'NR == 2 {print $4}')"
    is_positive_integer "$available_kb" || {
        fail "could not determine available disk for $base_dir"
        return 1
    }
    capture_kb=$(( (capture_seconds * bytes_per_second + 1023) / 1024 ))
    required_kb=$(( capture_kb + reserve_gb * 1024 * 1024 ))
    if [ "$available_kb" -lt "$required_kb" ]; then
        fail "insufficient disk: available=${available_kb}KiB required=${required_kb}KiB (includes ${reserve_gb}GiB reserve)"
        return 1
    fi
}

finalize_admissible_groups() {
    local binary="$1"
    local segment_dir="$2"
    local converted_dir="$3"
    local chainlink_csv="$4"
    local max_attempts="${5:-$DEFAULT_TERMINAL_WAIT_ATTEMPTS}"
    local wait_seconds="${6:-$DEFAULT_TERMINAL_WAIT_SECONDS}"
    local audit="$segment_dir/forward_latency_audit.json"
    local rows="$segment_dir/resolution_groups.jsonl.tmp.$$"
    local summary="$segment_dir/resolution_summary.json"
    local summary_tmp="$summary.tmp.$$"
    : >"$rows"

    local group_json group_id output attempt verdict ready
    while IFS= read -r group_json; do
        group_id="$(jq -r '.group' <<<"$group_json")"
        output="$segment_dir/resolution_${group_id}.json"
        verdict="FINALIZE_NOT_RUN"
        ready=false
        local condition_args=()
        local condition_id
        while IFS= read -r condition_id; do
            condition_args+=(--condition-id "$condition_id")
        done < <(jq -r '.condition_ids[]' <<<"$group_json")
        [ "${#condition_args[@]}" -gt 0 ] || {
            fail "$group_id contains no condition IDs"
            return 1
        }

        attempt=1
        while [ "$attempt" -le "$max_attempts" ]; do
            if run_logged "$segment_dir/finalize_${group_id}_attempt_${attempt}.stdout.json" "$segment_dir/finalize_${group_id}_attempt_${attempt}.stderr.log" \
                "$binary" finalize-recorded-btc-books \
                --input-dir "$converted_dir" \
                "${condition_args[@]}" \
                --btc-csv "$chainlink_csv" \
                --settlement-source-kind chainlink_btc_usd_data_stream \
                --output "$output"; then
                if ! verify_resolution_manifest "$output"; then
                    verdict="INVALID_RESOLUTION_MANIFEST"
                    break
                fi
                verdict="$(jq -r '.a_plus_gate.verdict' "$output")"
                if jq -e '.a_plus_gate.settlement_alignment_ready == true' "$output" >/dev/null; then
                    ready=true
                    break
                fi
                if [ "$verdict" != "WAIT_FOR_TERMINAL_MARKETS" ]; then
                    break
                fi
            else
                verdict="FINALIZE_COMMAND_FAILED"
                break
            fi
            if [ "$attempt" -lt "$max_attempts" ]; then
                log "$group_id waiting ${wait_seconds}s for terminal Gamma outcomes"
                sleep "$wait_seconds"
            fi
            attempt=$(( attempt + 1 ))
        done

        jq -n \
            --arg group "$group_id" \
            --arg manifest "$(basename "$output")" \
            --arg verdict "$verdict" \
            --argjson ready "$ready" \
            --argjson conditions "$(jq '.conditions' <<<"$group_json")" \
            --argjson first_open_ms "$(jq '.first_open_ms' <<<"$group_json")" \
            --argjson last_close_ms "$(jq '.last_close_ms' <<<"$group_json")" \
            --argjson condition_ids "$(jq '.condition_ids' <<<"$group_json")" \
            '{group:$group, manifest:$manifest, conditions:$conditions,
              first_open_ms:$first_open_ms, last_close_ms:$last_close_ms,
              condition_ids:$condition_ids, ready:$ready, verdict:$verdict}' >>"$rows"
    done < <(jq -c '.window_admissibility.groups[]' "$audit")

    jq -s \
        --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        '{schema_version:1, generated_at:$generated_at, groups:.,
          total_groups:length,
          ready_groups:(map(select(.ready == true)) | length),
          selected_conditions:(map(.conditions) | add // 0),
          all_ready:((length > 0) and all(.ready == true))}' \
        "$rows" >"$summary_tmp"
    mv "$summary_tmp" "$summary"
    rm -- "$rows"
}

write_session_summary() {
    local session_dir="$1"
    local summary="$session_dir/session_summary.json"
    local summary_tmp="$summary.tmp.$$"
    jq -s \
        --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        '{schema_version:1, generated_at:$generated_at, segments:.,
          capture_verified:(all(.capture_verified == true)),
          resolution_ready_segments:(map(select(.resolution_ready == true)) | length)}' \
        "$session_dir"/segment_*/status.json >"$summary_tmp"
    mv "$summary_tmp" "$summary"
}

refresh_segment_resolution() {
    local binary="$1"
    local requested_segment_dir="$2"
    local max_attempts="$3"
    local wait_seconds="$4"
    local segment_dir session_dir status summary audit converted chainlink_csv

    segment_dir="$(cd "$requested_segment_dir" && pwd -P)"
    session_dir="$(dirname "$segment_dir")"
    validate_private_base_path "$segment_dir" || return 2
    case "$(basename "$segment_dir")" in
        segment_[0-9][0-9][0-9]) ;;
        *) fail "refresh path is not a segment directory: $segment_dir"; return 2 ;;
    esac
    [ -f "$session_dir/.polymomentum-forward-capture-owned" ] || {
        fail "session ownership marker missing; refusing refresh: $session_dir"
        return 1
    }

    status="$segment_dir/status.json"
    summary="$segment_dir/resolution_summary.json"
    audit="$segment_dir/forward_latency_audit.json"
    converted="$segment_dir/converted"
    chainlink_csv="$segment_dir/raw/chainlink_btcusd.csv"
    jq -e '
        .capture_verified == true
        and .resolution_ready == false
        and .admissible_conditions > 0
        and (.resolution_verdict | contains("WAIT_FOR_TERMINAL_MARKETS"))
    ' "$status" >/dev/null || {
        fail "segment is not a verified terminal-only pending segment: $segment_dir"
        return 1
    }
    jq -e '
        (.groups | length) > 0
        and any(.groups[]; .ready != true)
        and all(.groups[]; .ready == true or .verdict == "WAIT_FOR_TERMINAL_MARKETS")
    ' "$summary" >/dev/null || {
        fail "resolution summary contains a non-terminal blocker: $summary"
        return 1
    }
    verify_latency_audit "$audit" || { fail "latency audit is not refreshable: $audit"; return 1; }
    verify_refreshable_conversion \
        "$converted/manifest.json" "$audit" "$(jq '.admissible_conditions' "$status")" || {
        fail "conversion manifest is not refreshable: $converted/manifest.json"
        return 1
    }
    test -s "$chainlink_csv" || { fail "Chainlink tape is missing: $chainlink_csv"; return 1; }

    finalize_admissible_groups \
        "$binary" "$segment_dir" "$converted" "$chainlink_csv" \
        "$max_attempts" "$wait_seconds"

    local resolution_ready=false
    local resolution_verdict
    if jq -e '.all_ready == true' "$summary" >/dev/null; then
        resolution_ready=true
        resolution_verdict="ALL_ADMISSIBLE_GROUPS_READY"
    else
        resolution_verdict="$(jq -r '[.groups[] | select(.ready != true) | (.group + ":" + .verdict)] | join(",")' "$summary")"
    fi
    write_segment_status \
        "$segment_dir" \
        "$(jq '.segment' "$status")" \
        "$(jq -r '.first_window_start' "$status")" \
        "$(jq '.session_owned_frames_deleted' "$status")" \
        "$resolution_ready" \
        "$resolution_verdict" \
        "$(jq '.full_segment_signal_coverage' "$status")"
    write_session_summary "$session_dir"
    log "$(basename "$session_dir")/$(basename "$segment_dir") refreshed resolution_ready=$resolution_ready verdict=$resolution_verdict"
}

write_segment_status() {
    local segment_dir="$1"
    local segment_number="$2"
    local start_iso="$3"
    local frames_deleted="$4"
    local resolution_ready="$5"
    local resolution_verdict="$6"
    local full_segment_signal_coverage="$7"
    local audit="$segment_dir/forward_latency_audit.json"
    local conversion="$segment_dir/converted/manifest.json"
    local resolution="$segment_dir/resolution_summary.json"
    local output="$segment_dir/status.json"
    local tmp="$output.tmp.$$"
    jq -n \
        --argjson schema_version 1 \
        --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --argjson segment "$segment_number" \
        --arg start "$start_iso" \
        --argjson capture_verified true \
        --argjson frames_deleted "$frames_deleted" \
        --argjson full_segment_signal_coverage "$full_segment_signal_coverage" \
        --argjson resolution_ready "$resolution_ready" \
        --arg resolution_verdict "$resolution_verdict" \
        --argjson resolution_total_groups "$(jq '.total_groups' "$resolution")" \
        --argjson resolution_ready_groups "$(jq '.ready_groups' "$resolution")" \
        --argjson replay_latency_ms "$(jq '.a_plus_latency_gate.recommended_retest_latency_ms' "$audit")" \
        --argjson captured_conditions "$(jq '.window_admissibility.conditions' "$audit")" \
        --argjson admissible_conditions "$(jq '.window_admissibility.admissible_conditions' "$audit")" \
        --argjson excluded_conditions "$(jq '.window_admissibility.excluded_conditions' "$audit")" \
        --argjson admissible_groups "$(jq '.window_admissibility.groups | length' "$audit")" \
        --argjson distilled_events "$(jq '(.stats.book_events // 0) + (.stats.change_events // 0)' "$conversion")" \
        '{schema_version:$schema_version, generated_at:$generated_at, segment:$segment,
          first_window_start:$start, capture_verified:$capture_verified,
          full_segment_signal_coverage:$full_segment_signal_coverage,
          session_owned_frames_deleted:$frames_deleted,
          recommended_replay_latency_ms:$replay_latency_ms,
          captured_conditions:$captured_conditions,
          admissible_conditions:$admissible_conditions,
          excluded_conditions:$excluded_conditions,
          admissible_groups:$admissible_groups,
          distilled_events:$distilled_events,
          resolution_ready:$resolution_ready,
          resolution_total_groups:$resolution_total_groups,
          resolution_ready_groups:$resolution_ready_groups,
          resolution_verdict:$resolution_verdict}' >"$tmp"
    mv "$tmp" "$output"
}

write_zero_admissible_segment_status() {
    local segment_dir="$1"
    local segment_number="$2"
    local start_iso="$3"
    local frames_deleted="$4"
    local full_segment_signal_coverage="$5"
    local audit="$segment_dir/forward_latency_audit.json"
    local output="$segment_dir/status.json"
    local tmp="$output.tmp.$$"
    jq -n \
        --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --argjson segment "$segment_number" \
        --arg start "$start_iso" \
        --argjson frames_deleted "$frames_deleted" \
        --argjson full_segment_signal_coverage "$full_segment_signal_coverage" \
        --argjson replay_latency_ms "$(jq '.a_plus_latency_gate.recommended_retest_latency_ms' "$audit")" \
        --argjson captured_conditions "$(jq '.window_admissibility.conditions' "$audit")" \
        '{schema_version:1, generated_at:$generated_at, segment:$segment,
          first_window_start:$start, capture_verified:true,
          full_segment_signal_coverage:$full_segment_signal_coverage,
          session_owned_frames_deleted:$frames_deleted,
          recommended_replay_latency_ms:$replay_latency_ms,
          captured_conditions:$captured_conditions,
          admissible_conditions:0,
          excluded_conditions:$captured_conditions,
          admissible_groups:0,
          distilled_events:0,
          resolution_ready:false,
          resolution_total_groups:0,
          resolution_ready_groups:0,
          resolution_verdict:"NO_ADMISSIBLE_CONDITIONS",
          rejection_reason:"REFERENCE_TAPE_ADMISSIBILITY_FAILED_ALL_CONDITIONS"}' >"$tmp"
    mv "$tmp" "$output"
}

main() {
    local binary="$DEFAULT_BINARY"
    local base_dir="$DEFAULT_BASE_DIR"
    local session_id=""
    local segments="$DEFAULT_SEGMENTS"
    local windows="$DEFAULT_WINDOWS_PER_SEGMENT"
    local padding="$DEFAULT_PADDING_SECONDS"
    local signal_preroll="$DEFAULT_SIGNAL_PREROLL_SECONDS"
    local bytes_per_second="$DEFAULT_ESTIMATED_BYTES_PER_SECOND"
    local reserve_gb="$DEFAULT_RESERVE_GB"
    local terminal_wait_attempts="$DEFAULT_TERMINAL_WAIT_ATTEMPTS"
    local terminal_wait_seconds="$DEFAULT_TERMINAL_WAIT_SECONDS"
    local refresh_segment=""
    local delete_frames=false
    local continue_after_zero_admissible=false
    local delete_zero_admissible_frames=false
    local dry_run=false

    while [ $# -gt 0 ]; do
        case "$1" in
            --binary) binary="$2"; shift 2 ;;
            --base-dir) base_dir="$2"; shift 2 ;;
            --session-id) session_id="$2"; shift 2 ;;
            --segments) segments="$2"; shift 2 ;;
            --windows-per-segment) windows="$2"; shift 2 ;;
            --padding-seconds) padding="$2"; shift 2 ;;
            --signal-preroll-seconds) signal_preroll="$2"; shift 2 ;;
            --estimated-bytes-per-second) bytes_per_second="$2"; shift 2 ;;
            --reserve-gb) reserve_gb="$2"; shift 2 ;;
            --terminal-wait-attempts) terminal_wait_attempts="$2"; shift 2 ;;
            --terminal-wait-seconds) terminal_wait_seconds="$2"; shift 2 ;;
            --refresh-segment) refresh_segment="$2"; shift 2 ;;
            --delete-session-owned-frames-after-verify) delete_frames=true; shift ;;
            --continue-after-zero-admissible) continue_after_zero_admissible=true; shift ;;
            --delete-session-owned-frames-after-zero-admissible-audit) delete_zero_admissible_frames=true; shift ;;
            --dry-run) dry_run=true; shift ;;
            -h|--help) usage; return 0 ;;
            *) fail "unknown argument: $1"; usage >&2; return 2 ;;
        esac
    done

    is_positive_integer "$segments" || { fail "--segments must be a positive integer"; return 2; }
    is_positive_integer "$windows" || { fail "--windows-per-segment must be a positive integer"; return 2; }
    is_nonnegative_integer "$padding" || { fail "--padding-seconds must be a non-negative integer"; return 2; }
    is_nonnegative_integer "$signal_preroll" || { fail "--signal-preroll-seconds must be a non-negative integer"; return 2; }
    [ "$signal_preroll" -ge "$DEFAULT_SIGNAL_PREROLL_SECONDS" ] || {
        fail "--signal-preroll-seconds must be at least $DEFAULT_SIGNAL_PREROLL_SECONDS for exact replay"
        return 2
    }
    is_positive_integer "$bytes_per_second" || { fail "--estimated-bytes-per-second must be a positive integer"; return 2; }
    is_nonnegative_integer "$reserve_gb" || { fail "--reserve-gb must be a non-negative integer"; return 2; }
    is_positive_integer "$terminal_wait_attempts" || { fail "--terminal-wait-attempts must be a positive integer"; return 2; }
    is_nonnegative_integer "$terminal_wait_seconds" || { fail "--terminal-wait-seconds must be a non-negative integer"; return 2; }
    if [ "$delete_zero_admissible_frames" = true ] && [ "$continue_after_zero_admissible" != true ]; then
        fail "--delete-session-owned-frames-after-zero-admissible-audit requires --continue-after-zero-admissible"
        return 2
    fi
    if [ -n "$refresh_segment" ]; then
        [ "$dry_run" = false ] || { fail "--refresh-segment cannot be combined with --dry-run"; return 2; }
        [ "$(id -un)" = "polymomentum" ] || {
            fail "run as the polymomentum tenant user (for example: sudo -u polymomentum)"
            return 2
        }
        command -v jq >/dev/null || { fail "jq is required"; return 1; }
        [ -x "$binary" ] || { fail "binary is not executable: $binary"; return 1; }
        "$binary" finalize-recorded-btc-books --help >/dev/null 2>&1 || {
            fail "binary does not expose required command: finalize-recorded-btc-books"
            return 1
        }
        refresh_segment_resolution \
            "$binary" "$refresh_segment" "$terminal_wait_attempts" "$terminal_wait_seconds"
        return
    fi
    validate_private_base_path "$base_dir" || return 2
    if [ -n "$session_id" ]; then
        case "$session_id" in
            *[!A-Za-z0-9._-]*|''|.|..)
                fail "--session-id may contain only letters, digits, dot, underscore, and dash"
                return 2
                ;;
        esac
    else
        session_id="session-$(date -u +%Y%m%dT%H%M%SZ)"
    fi

    local capture_seconds=$(( signal_preroll + windows * WINDOW_SECONDS + 2 * padding ))
    local estimated_gib
    estimated_gib="$(awk -v seconds="$capture_seconds" -v rate="$bytes_per_second" 'BEGIN {printf "%.2f", seconds * rate / 1073741824}')"

    if [ "$dry_run" = true ]; then
        local now next_boundary start_iso example_raw example_converted
        now="$(date -u +%s)"
        next_boundary=$(( ((now + signal_preroll + padding + WINDOW_SECONDS - 1) / WINDOW_SECONDS) * WINDOW_SECONDS ))
        start_iso="$(format_epoch_utc "$next_boundary")"
        example_raw="$base_dir/$session_id/segment_001/raw"
        example_converted="$base_dir/$session_id/segment_001/converted"
        echo "Dry run: non-trading segmented forward capture"
        echo "  segments=$segments windows_per_segment=$windows signal_preroll_seconds=$signal_preroll capture_seconds=$capture_seconds"
        echo "  estimated_raw_per_segment=${estimated_gib}GiB reserve=${reserve_gb}GiB"
        echo "  delete_verified_session_owned_frames=$delete_frames"
        echo "  continue_after_zero_admissible=$continue_after_zero_admissible"
        echo "  delete_zero_admissible_frames=$delete_zero_admissible_frames"
        echo "  terminal_wait_attempts=$terminal_wait_attempts terminal_wait_seconds=$terminal_wait_seconds"
        echo "Example first segment commands:"
        print_command "$binary" record-btc-books --start "$start_iso" --window-minutes 5 --windows "$windows" --duration-seconds "$capture_seconds" --out-dir "$example_raw"
        print_command "$binary" forward-latency-audit --input-dir "$example_raw" --output "$base_dir/$session_id/segment_001/forward_latency_audit.json"
        print_command "$binary" convert-recorded-btc-books --input-dir "$example_raw" --output-dir "$example_converted"
        print_command "$binary" finalize-recorded-btc-books --input-dir "$example_converted" --btc-csv "$example_raw/chainlink_btcusd.csv" --settlement-source-kind chainlink_btc_usd_data_stream --output "$base_dir/$session_id/segment_001/resolution_manifest.json"
        return 0
    fi

    [ "$(id -un)" = "polymomentum" ] || {
        fail "run as the polymomentum tenant user (for example: sudo -u polymomentum)"
        return 2
    }
    command -v jq >/dev/null || { fail "jq is required"; return 1; }
    [ -x "$binary" ] || { fail "binary is not executable: $binary"; return 1; }
    local command_name
    for command_name in record-btc-books forward-latency-audit convert-recorded-btc-books finalize-recorded-btc-books; do
        "$binary" "$command_name" --help >/dev/null 2>&1 || {
            fail "binary does not expose required command: $command_name"
            return 1
        }
    done

    mkdir -p "$base_dir"
    base_dir="$(cd "$base_dir" && pwd -P)"
    validate_private_base_path "$base_dir" || return 2
    local session_dir="$base_dir/$session_id"
    if [ -e "$session_dir" ]; then
        fail "session already exists; refusing to reuse it: $session_dir"
        return 1
    fi
    mkdir "$session_dir"
    printf 'created_at=%s\nscript=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$0" >"$session_dir/.polymomentum-forward-capture-owned"

    local segment_number=1
    while [ "$segment_number" -le "$segments" ]; do
        local segment_name segment_dir raw_dir converted_dir now next_boundary launch_epoch wait_seconds start_iso
        segment_name="$(printf 'segment_%03d' "$segment_number")"
        segment_dir="$session_dir/$segment_name"
        raw_dir="$segment_dir/raw"
        converted_dir="$segment_dir/converted"
        mkdir -p "$raw_dir" "$converted_dir"

        check_disk_capacity "$base_dir" "$capture_seconds" "$bytes_per_second" "$reserve_gb"
        now="$(date -u +%s)"
        next_boundary=$(( ((now + signal_preroll + padding + WINDOW_SECONDS - 1) / WINDOW_SECONDS) * WINDOW_SECONDS ))
        launch_epoch=$(( next_boundary - signal_preroll - padding ))
        wait_seconds=$(( launch_epoch - now ))
        if [ "$wait_seconds" -gt 0 ]; then
            log "$segment_name waiting ${wait_seconds}s for the pre-boundary capture pad"
            sleep "$wait_seconds"
        fi
        start_iso="$(format_epoch_utc "$next_boundary")"
        log "$segment_name recording $windows complete windows from $start_iso (${capture_seconds}s)"

        run_logged "$segment_dir/record.stdout.json" "$segment_dir/record.stderr.log" \
            "$binary" record-btc-books \
            --start "$start_iso" \
            --window-minutes 5 \
            --windows "$windows" \
            --duration-seconds "$capture_seconds" \
            --out-dir "$raw_dir"
        verify_record_capture "$raw_dir" "$windows" || {
            fail "$segment_name capture verification failed; preserving all raw files"
            return 1
        }
        local full_segment_signal_coverage=true
        if ! verify_replay_signal_coverage "$raw_dir/binance_btcusdt_rtds.csv" "$next_boundary" "$windows"; then
            full_segment_signal_coverage=false
            log "$segment_name whole-segment Binance coverage is incomplete; continuing to the fail-closed per-condition admissibility audit"
        fi

        run_logged "$segment_dir/latency.stdout.json" "$segment_dir/latency.stderr.log" \
            "$binary" forward-latency-audit \
            --input-dir "$raw_dir" \
            --output "$segment_dir/forward_latency_audit.json"
        if ! verify_latency_audit "$segment_dir/forward_latency_audit.json"; then
            if [ "$continue_after_zero_admissible" = true ] \
                && verify_zero_admissible_audit "$segment_dir/forward_latency_audit.json"; then
                local rejected_frames_deleted=false
                if [ "$delete_zero_admissible_frames" = true ]; then
                    delete_session_owned_frames "$session_dir" "$raw_dir"
                    rejected_frames_deleted=true
                    log "$segment_name removed only its rejected, audit-sealed market_ws_frames.jsonl"
                fi
                write_zero_admissible_segment_status \
                    "$segment_dir" \
                    "$segment_number" \
                    "$start_iso" \
                    "$rejected_frames_deleted" \
                    "$full_segment_signal_coverage"
                log "$segment_name capture_verified=true admissible_conditions=0 verdict=NO_ADMISSIBLE_CONDITIONS"
                segment_number=$(( segment_number + 1 ))
                continue
            fi
            fail "$segment_name latency gate failed; preserving all raw files"
            return 1
        fi

        local conversion_condition_args=()
        local conversion_condition_id
        while IFS= read -r conversion_condition_id; do
            conversion_condition_args+=(--condition-id "$conversion_condition_id")
        done < <(jq -r '[.window_admissibility.groups[].condition_ids[]] | unique[]' \
            "$segment_dir/forward_latency_audit.json")
        local admissible_condition_count
        admissible_condition_count="$(jq '[.window_admissibility.groups[].condition_ids[]] | unique | length' \
            "$segment_dir/forward_latency_audit.json")"
        [ "$admissible_condition_count" -gt 0 ] || {
            fail "$segment_name audit passed without an admissible conversion allowlist"
            return 1
        }

        run_logged "$segment_dir/convert.stdout.json" "$segment_dir/convert.stderr.log" \
            "$binary" convert-recorded-btc-books \
            --input-dir "$raw_dir" \
            --output-dir "$converted_dir" \
            "${conversion_condition_args[@]}"
        verify_conversion "$converted_dir/manifest.json" "$admissible_condition_count" || {
            fail "$segment_name conversion verification failed; preserving all raw files"
            return 1
        }

        local frames_deleted=false
        if [ "$delete_frames" = true ]; then
            delete_session_owned_frames "$session_dir" "$raw_dir"
            frames_deleted=true
            log "$segment_name removed only its verified market_ws_frames.jsonl"
        fi

        local resolution_ready=false
        local resolution_verdict="GROUP_FINALIZE_NOT_RUN"
        finalize_admissible_groups \
            "$binary" \
            "$segment_dir" \
            "$converted_dir" \
            "$raw_dir/chainlink_btcusd.csv" \
            "$terminal_wait_attempts" \
            "$terminal_wait_seconds"
        if jq -e '.all_ready == true' "$segment_dir/resolution_summary.json" >/dev/null; then
            resolution_ready=true
            resolution_verdict="ALL_ADMISSIBLE_GROUPS_READY"
        else
            resolution_verdict="$(jq -r '[.groups[] | select(.ready != true) | (.group + ":" + .verdict)] | join(",")' "$segment_dir/resolution_summary.json")"
        fi

        write_segment_status "$segment_dir" "$segment_number" "$start_iso" "$frames_deleted" "$resolution_ready" "$resolution_verdict" "$full_segment_signal_coverage"
        log "$segment_name capture_verified=true resolution_ready=$resolution_ready verdict=$resolution_verdict"
        segment_number=$(( segment_number + 1 ))
    done

    write_session_summary "$session_dir"
    log "session complete: $session_dir/session_summary.json"
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    main "$@"
fi
