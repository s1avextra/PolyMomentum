#!/bin/bash
# Replay verified forward-capture segments on a dev box, emit one opportunity
# report per segment, then run the frozen binary-complement block scorer.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=capture-forward-segments.sh
source "$ROOT_DIR/deploy/capture-forward-segments.sh"

DEFAULT_BINARY="$ROOT_DIR/rust_engine/target/release/polymomentum-engine"
DEFAULT_VARIANT="$ROOT_DIR/deploy/promotions/evidence/strategy_registry/20260715_binary_complement_capture_variant.json"
CAPTURE_PARAMS_HASH="34aa177f7ae8614814208cdd81ed74e09199007b924ee16b6e18dfa62fd49aa9"
BINARY_COMPLEMENT_MIN_CONDITIONS=750
BINARY_COMPLEMENT_PREREGISTERED_EPOCH=1784090954

verify_exact_replay_tape_coverage() {
    local key="$1"
    local binance_csv="$2"
    local chainlink_csv="$3"
    local resolution="$4"
    local first_open_s last_close_s signal_required_start_ms required_end_ms
    local binance_first_ms binance_last_ms chainlink_first_ms chainlink_last_ms
    local median_gap_ms max_gap_ms allowed_gap_ms

    first_open_s="$(jq -r '[.markets[].open_ts_s] | min' "$resolution")"
    last_close_s="$(jq -r '[.markets[].close_ts_s] | max' "$resolution")"
    is_positive_integer "$first_open_s" || { fail "$key has no valid first market open"; return 1; }
    is_positive_integer "$last_close_s" || { fail "$key has no valid last market close"; return 1; }
    signal_required_start_ms=$(( first_open_s * 1000 - DEFAULT_SIGNAL_PREROLL_SECONDS * 1000 ))
    required_end_ms=$(( last_close_s * 1000 ))

    read -r binance_first_ms binance_last_ms < <(csv_timestamp_bounds_ms "$binance_csv") || {
        fail "$key Binance RTDS tape has no valid timestamp rows"
        return 1
    }
    read -r chainlink_first_ms chainlink_last_ms < <(csv_timestamp_bounds_ms "$chainlink_csv") || {
        fail "$key Chainlink tape has no valid timestamp rows"
        return 1
    }

    if [ "$binance_first_ms" -gt $(( signal_required_start_ms + 1000 )) ] || [ "$binance_last_ms" -lt "$required_end_ms" ]; then
        fail "$key Binance RTDS tape covers ${binance_first_ms}..${binance_last_ms} ms, but exact replay needs ${signal_required_start_ms}..${required_end_ms} ms (one causal hour before the first market)"
        return 1
    fi
    if [ "$chainlink_first_ms" -gt $(( first_open_s * 1000 + 1000 )) ] || [ "$chainlink_last_ms" -lt "$required_end_ms" ]; then
        fail "$key Chainlink tape covers ${chainlink_first_ms}..${chainlink_last_ms} ms, but settlement replay needs $(( first_open_s * 1000 ))..${required_end_ms} ms"
        return 1
    fi
    read -r median_gap_ms max_gap_ms allowed_gap_ms < <(
        csv_internal_gap_stats_ms "$binance_csv" "$signal_required_start_ms" "$required_end_ms"
    ) || {
        fail "$key Binance RTDS tape has no usable intervals in the required strategy range"
        return 1
    }
    if [ "$max_gap_ms" -gt "$allowed_gap_ms" ]; then
        fail "$key Binance RTDS tape has an internal gap of ${max_gap_ms} ms; median cadence is ${median_gap_ms} ms and the fail-closed limit is ${allowed_gap_ms} ms"
        return 1
    fi
}

usage() {
    cat <<'EOF'
Usage: replay-binary-complement-block.sh --capture-root DIR --output-dir DIR --block-id ID [options]

Options:
  --capture-root DIR   Local copy containing segment_*/status.json files.
  --output-dir DIR     New directory for opportunity reports and the screen.
  --block-id ID        Stable forward-block identifier.
  --binary PATH        Local polymomentum-engine release binary.
  --variant-json PATH  Frozen zero-trade capture variant.
  --strategy-variant-json PATH
                       Optional frozen variant or variant array for a second
                       exact replay pass on every admissible segment group.
  --strategy-preregistration-json PATH
                       Required with --strategy-variant-json. Supplies the
                       immutable score-not-before time and disclosure floor.
  --threads N          Local variant fan-out threads (default: 0 / rayon default).
  --dry-run            Validate artifacts and print exact commands without writing.
  -h, --help           Show this help.

This runner is dev-box only. It requires each segment to be capture-verified,
terminal settlement-aligned, and converted with the exact shared-distilled
replay contract. It never falls back to PMXT parquet or network event data.
EOF
}

main() {
    local capture_root=""
    local output_dir=""
    local block_id=""
    local binary="$DEFAULT_BINARY"
    local variant_json="$DEFAULT_VARIANT"
    local strategy_variant_json=""
    local strategy_preregistration_json=""
    local threads=0
    local dry_run=false

    while [ $# -gt 0 ]; do
        case "$1" in
            --capture-root) capture_root="$2"; shift 2 ;;
            --output-dir) output_dir="$2"; shift 2 ;;
            --block-id) block_id="$2"; shift 2 ;;
            --binary) binary="$2"; shift 2 ;;
            --variant-json) variant_json="$2"; shift 2 ;;
            --strategy-variant-json) strategy_variant_json="$2"; shift 2 ;;
            --strategy-preregistration-json) strategy_preregistration_json="$2"; shift 2 ;;
            --threads) threads="$2"; shift 2 ;;
            --dry-run) dry_run=true; shift ;;
            -h|--help) usage; return 0 ;;
            *) fail "unknown argument: $1"; usage >&2; return 2 ;;
        esac
    done

    [ -n "$capture_root" ] || { fail "--capture-root is required"; return 2; }
    [ -n "$output_dir" ] || { fail "--output-dir is required"; return 2; }
    case "$block_id" in
        ''|*[!A-Za-z0-9._-]*|.|..)
            fail "--block-id may contain only letters, digits, dot, underscore, and dash"
            return 2
            ;;
    esac
    is_nonnegative_integer "$threads" || { fail "--threads must be a non-negative integer"; return 2; }
    [ -d "$capture_root" ] || { fail "capture root is not a directory: $capture_root"; return 1; }
    capture_root="$(cd "$capture_root" && pwd -P)"
    case "$capture_root" in
        /opt/*|*/polyarbitrage/*|*/PolyArbitrage/*)
            fail "replay must run from a local PolyMomentum/dev-box copy, not $capture_root"
            return 1
            ;;
    esac
    [ -x "$binary" ] || { fail "binary is not executable: $binary"; return 1; }
    [ -s "$variant_json" ] || { fail "capture variant is missing: $variant_json"; return 1; }
    if [ -n "$strategy_variant_json" ] && [ ! -s "$strategy_variant_json" ]; then
        fail "strategy variant is missing: $strategy_variant_json"
        return 1
    fi
    if [ -n "$strategy_variant_json" ] && [ ! -s "$strategy_preregistration_json" ]; then
        fail "--strategy-preregistration-json is required with --strategy-variant-json"
        return 1
    fi
    if [ -z "$strategy_variant_json" ] && [ -n "$strategy_preregistration_json" ]; then
        fail "--strategy-preregistration-json requires --strategy-variant-json"
        return 1
    fi
    if [ "$dry_run" = false ] && [ "$(id -un)" = "polymomentum" ]; then
        fail "CPU-intensive block replay is forbidden on the polymomentum VPS tenant"
        return 1
    fi
    if [ -e "$output_dir" ] && [ "$dry_run" = false ]; then
        fail "output directory already exists; refusing to mix block evidence: $output_dir"
        return 1
    fi

    local statuses=()
    while IFS= read -r status; do
        statuses+=("$status")
    done < <(find "$capture_root" -type f -path '*/segment_*/status.json' -print | sort)
    [ "${#statuses[@]}" -gt 0 ] || { fail "no segment status files found under $capture_root"; return 1; }

    local binary_resolutions=()
    local binary_status binary_segment binary_resolution
    for binary_status in "${statuses[@]}"; do
        binary_segment="$(dirname "$binary_status")"
        jq -e '.capture_verified == true and .resolution_ready == true' "$binary_status" >/dev/null || {
            fail "$(basename "$(dirname "$binary_segment")")_$(basename "$binary_segment") is not capture-verified and terminal-resolution-ready"
            return 1
        }
        if [ -s "$binary_segment/resolution_summary.json" ]; then
            jq -e '.all_ready == true and .total_groups > 0 and (.ready_groups == .total_groups)' \
                "$binary_segment/resolution_summary.json" >/dev/null || {
                fail "$binary_segment resolution-group summary is not fully ready"
                return 1
            }
            while IFS= read -r binary_resolution; do
                binary_resolutions+=("$binary_segment/$binary_resolution")
            done < <(jq -r '.groups[] | select(.ready == true) | .manifest' "$binary_segment/resolution_summary.json")
        else
            binary_resolutions+=("$binary_segment/resolution_manifest.json")
        fi
    done
    [ "${#binary_resolutions[@]}" -gt 0 ] || {
        fail "binary-complement disclosure preflight found no resolution manifests"
        return 1
    }
    for binary_resolution in "${binary_resolutions[@]}"; do
        [ -s "$binary_resolution" ] || {
            fail "binary-complement resolution manifest is missing: $binary_resolution"
            return 1
        }
        jq -e '.a_plus_gate.settlement_alignment_ready == true' "$binary_resolution" >/dev/null || {
            fail "binary-complement resolution manifest is not settlement-aligned: $binary_resolution"
            return 1
        }
    done
    local binary_terminal_conditions
    binary_terminal_conditions="$(jq -s --argjson not_before "$BINARY_COMPLEMENT_PREREGISTERED_EPOCH" '
        [ .[] | .markets[]
          | select(.settlement_aligned == true)
          | select(.official_source_matches_btc_tape == true)
          | select(.open_ts_s >= $not_before)
          | select(.terminal_direction == "up" or .terminal_direction == "down")
          | .condition_id ]
        | unique
        | length
    ' "${binary_resolutions[@]}")"
    is_nonnegative_integer "$binary_terminal_conditions" || {
        fail "binary-complement disclosure preflight could not count terminal conditions"
        return 1
    }
    if [ "$binary_terminal_conditions" -lt "$BINARY_COMPLEMENT_MIN_CONDITIONS" ]; then
        fail "binary-complement screen remains sealed: ${binary_terminal_conditions} post-registration terminal conditions, need ${BINARY_COMPLEMENT_MIN_CONDITIONS}"
        return 1
    fi

    if [ -n "$strategy_variant_json" ]; then
        local strategy_min_conditions strategy_min_trades strategy_not_before_epoch strategy_candidate_hash
        strategy_min_conditions="$(jq -r '.evaluation_contract.minimum_preflight_terminal_conditions' "$strategy_preregistration_json")"
        strategy_min_trades="$(jq -r '.evaluation_contract.minimum_disclosure_trades' "$strategy_preregistration_json")"
        strategy_not_before_epoch="$(jq -r '.evaluation_contract.score_not_before_epoch_s' "$strategy_preregistration_json")"
        strategy_candidate_hash="$(jq -r '.candidate.params_hash' "$strategy_preregistration_json")"
        is_positive_integer "$strategy_min_conditions" || {
            fail "strategy preregistration has no valid terminal-condition preflight count"
            return 1
        }
        if [ "$strategy_min_conditions" -lt 100 ]; then
            fail "strategy preflight floor cannot be lower than 100 terminal conditions"
            return 1
        fi
        is_positive_integer "$strategy_min_trades" || {
            fail "strategy preregistration has no valid minimum disclosure trade count"
            return 1
        }
        if [ "$strategy_min_trades" -lt 100 ]; then
            fail "strategy disclosure floor cannot be lower than 100 trades"
            return 1
        fi
        is_positive_integer "$strategy_not_before_epoch" || {
            fail "strategy preregistration has no valid score-not-before epoch"
            return 1
        }

        local strategy_resolutions=()
        local prereg_status prereg_segment prereg_key prereg_resolution
        for prereg_status in "${statuses[@]}"; do
            prereg_segment="$(dirname "$prereg_status")"
            prereg_key="$(basename "$(dirname "$prereg_segment")")_$(basename "$prereg_segment")"
            jq -e '.capture_verified == true and .resolution_ready == true' "$prereg_status" >/dev/null || {
                fail "$prereg_key is not capture-verified and terminal-resolution-ready"
                return 1
            }
            if [ -s "$prereg_segment/resolution_summary.json" ]; then
                jq -e '.all_ready == true and .total_groups > 0 and (.ready_groups == .total_groups)' \
                    "$prereg_segment/resolution_summary.json" >/dev/null || {
                    fail "$prereg_key resolution-group summary is not fully ready"
                    return 1
                }
                while IFS= read -r prereg_resolution; do
                    strategy_resolutions+=("$prereg_segment/$prereg_resolution")
                done < <(jq -r '.groups[] | select(.ready == true) | .manifest' "$prereg_segment/resolution_summary.json")
            else
                strategy_resolutions+=("$prereg_segment/resolution_manifest.json")
            fi
        done
        [ "${#strategy_resolutions[@]}" -gt 0 ] || {
            fail "strategy disclosure preflight found no resolution manifests"
            return 1
        }
        for prereg_resolution in "${strategy_resolutions[@]}"; do
            [ -s "$prereg_resolution" ] || {
                fail "strategy disclosure resolution manifest is missing: $prereg_resolution"
                return 1
            }
            jq -e '.a_plus_gate.settlement_alignment_ready == true' "$prereg_resolution" >/dev/null || {
                fail "strategy disclosure resolution manifest is not settlement-aligned: $prereg_resolution"
                return 1
            }
        done
        local strategy_terminal_conditions
        strategy_terminal_conditions="$(jq -s --argjson not_before "$strategy_not_before_epoch" '
            [ .[] | .markets[]
              | select(.settlement_aligned == true)
              | select(.open_ts_s >= $not_before)
              | select(.terminal_direction == "up" or .terminal_direction == "down")
              | .condition_id ]
            | unique
            | length
        ' "${strategy_resolutions[@]}")"
        is_nonnegative_integer "$strategy_terminal_conditions" || {
            fail "strategy disclosure preflight could not count terminal conditions"
            return 1
        }
        if [ "$strategy_terminal_conditions" -lt "$strategy_min_conditions" ]; then
            fail "strategy comparison remains sealed: ${strategy_terminal_conditions} post-registration terminal conditions, need ${strategy_min_conditions}"
            return 1
        fi
    fi

    local opportunities=()
    local resolutions=()
    local strategy_sealed_reports=()
    local strategy_final_reports=()
    local strategy_sealed_trades=()
    local strategy_final_trades=()
    local strategy_sealed_logs=()
    trap 'if [ "${#strategy_sealed_reports[@]}" -gt 0 ]; then rm -f "${strategy_sealed_reports[@]}" "${strategy_sealed_trades[@]}" "${strategy_sealed_logs[@]}"; fi' RETURN
    local status segment_dir session_dir key converted raw resolution start_epoch start end_epoch end latency
    for status in "${statuses[@]}"; do
        segment_dir="$(dirname "$status")"
        session_dir="$(dirname "$segment_dir")"
        key="$(basename "$session_dir")_$(basename "$segment_dir")"
        converted="$segment_dir/converted"
        raw="$segment_dir/raw"
        jq -e '.capture_verified == true and .resolution_ready == true' "$status" >/dev/null || {
            fail "$key is not capture-verified and terminal-resolution-ready"
            return 1
        }
        jq -e '
            .output.exact_replay_flag == "--require-shared-distilled"
            and (.output.harness_env.PMXT_DISTILLED_DIR | type == "string")
            and ((.hours | length) > 0)
        ' "$converted/manifest.json" >/dev/null || {
            fail "$key converter manifest lacks the exact replay contract"
            return 1
        }
        [ -s "$raw/binance_btcusdt_rtds.csv" ] || { fail "$key Binance RTDS tape is missing"; return 1; }
        [ -s "$raw/chainlink_btcusd.csv" ] || { fail "$key Chainlink tape is missing"; return 1; }
        jq -e 'type == "object" and length > 0' "$raw/gamma_market_cache.json" >/dev/null || {
            fail "$key captured Gamma market cache is missing or empty"
            return 1
        }
        latency="$(jq -r '.recommended_replay_latency_ms | ceil' "$status")"
        is_positive_integer "$latency" || { fail "$key replay latency is invalid"; return 1; }
        if [ "$latency" -lt 202 ]; then
            latency=202
        fi

        local resolution_paths=()
        if [ -s "$segment_dir/resolution_summary.json" ]; then
            jq -e '.all_ready == true and .total_groups > 0 and (.ready_groups == .total_groups)' \
                "$segment_dir/resolution_summary.json" >/dev/null || {
                fail "$key resolution-group summary is not fully ready"
                return 1
            }
            local manifest_name
            while IFS= read -r manifest_name; do
                resolution_paths+=("$segment_dir/$manifest_name")
            done < <(jq -r '.groups[] | select(.ready == true) | .manifest' "$segment_dir/resolution_summary.json")
        else
            resolution_paths+=("$segment_dir/resolution_manifest.json")
        fi
        [ "${#resolution_paths[@]}" -gt 0 ] || { fail "$key has no ready resolution groups"; return 1; }

        local group_key opportunity report cache
        for resolution in "${resolution_paths[@]}"; do
            [ -s "$resolution" ] || { fail "$key resolution manifest is missing: $resolution"; return 1; }
            jq -e '.a_plus_gate.settlement_alignment_ready == true' "$resolution" >/dev/null || {
                fail "$key resolution manifest is not settlement-aligned: $resolution"
                return 1
            }
            local condition_args=()
            local condition_id
            while IFS= read -r condition_id; do
                condition_args+=(--condition-id "$condition_id")
            done < <(jq -r '[.markets[].condition_id] | unique[]' "$resolution")
            [ "${#condition_args[@]}" -gt 0 ] || {
                fail "$key resolution manifest contains no condition IDs: $resolution"
                return 1
            }
            group_key="${key}_$(basename "$resolution" .json)"
            verify_exact_replay_tape_coverage \
                "$group_key" \
                "$raw/binance_btcusdt_rtds.csv" \
                "$raw/chainlink_btcusd.csv" \
                "$resolution" || return 1

            start_epoch="$(jq -r '[.markets[].open_ts_s] | min' "$resolution")"
            end_epoch="$(jq -r '[.markets[].open_ts_s] | max' "$resolution")"
            is_positive_integer "$start_epoch" || { fail "$group_key could not derive the first exact replay window"; return 1; }
            is_positive_integer "$end_epoch" || { fail "$group_key could not derive the last exact replay window"; return 1; }
            [ "$end_epoch" -ge "$start_epoch" ] || { fail "$group_key exact replay bounds are inverted"; return 1; }
            start="$(format_epoch_utc "$start_epoch")"
            end="$(format_epoch_utc "$end_epoch")"

            opportunity="$output_dir/${group_key}_opportunities.json"
            report="$output_dir/${group_key}_report.json"
            cache="$output_dir/cache/$group_key"
            local command=(
                "$binary" harness-sweep
                --start "$start"
                --end "$end"
                --cache-dir "$cache"
                --require-shared-distilled
                --variant-json "$variant_json"
                "${condition_args[@]}"
                --btc-csv "$raw/binance_btcusdt_rtds.csv"
                --settlement-btc-csv "$raw/chainlink_btcusd.csv"
                --latency-ms "$latency"
                --threads "$threads"
                --top 1
                --window-minutes 5
                --continuous
                --report-json "$report"
                --calibration-opportunities-json "$opportunity"
            )
            if [ "$dry_run" = true ]; then
                printf '  PMXT_DISTILLED_DIR=%q ' "$converted"
                printf '%q ' "${command[@]}"
                printf '\n'
            else
                mkdir -p "$cache"
                cp "$raw/gamma_market_cache.json" "$cache/gamma_market_cache.json"
                PMXT_DISTILLED_DIR="$converted" "${command[@]}"
                jq -e --arg hash "$CAPTURE_PARAMS_HASH" '
                    [.variants[] | select(.strategy.params_hash == $hash)] as $capture
                    | ($capture | length) == 1
                    and $capture[0].trades == 0
                ' "$report" >/dev/null || {
                    fail "$group_key capture variant identity drifted or emitted a trade"
                    return 1
                }
            fi
            if [ -n "$strategy_variant_json" ]; then
                local strategy_condition_args=()
                while IFS= read -r condition_id; do
                    strategy_condition_args+=(--condition-id "$condition_id")
                done < <(jq -r --argjson not_before "$strategy_not_before_epoch" '
                    [.markets[]
                     | select(.settlement_aligned == true)
                     | select(.open_ts_s >= $not_before)
                     | select(.terminal_direction == "up" or .terminal_direction == "down")
                     | .condition_id]
                    | unique[]
                ' "$resolution")
                if [ "${#strategy_condition_args[@]}" -eq 0 ]; then
                    opportunities+=("$opportunity")
                    resolutions+=("$resolution")
                    continue
                fi
                local strategy_start_epoch strategy_end_epoch strategy_start strategy_end
                strategy_start_epoch="$(jq -r --argjson not_before "$strategy_not_before_epoch" \
                    '[.markets[]
                      | select(.settlement_aligned == true)
                      | select(.open_ts_s >= $not_before)
                      | select(.terminal_direction == "up" or .terminal_direction == "down")
                      | .open_ts_s] | min' "$resolution")"
                strategy_end_epoch="$(jq -r --argjson not_before "$strategy_not_before_epoch" \
                    '[.markets[]
                      | select(.settlement_aligned == true)
                      | select(.open_ts_s >= $not_before)
                      | select(.terminal_direction == "up" or .terminal_direction == "down")
                      | .open_ts_s] | max' "$resolution")"
                is_positive_integer "$strategy_start_epoch" || {
                    fail "$group_key could not derive the first post-registration replay window"
                    return 1
                }
                is_positive_integer "$strategy_end_epoch" || {
                    fail "$group_key could not derive the last post-registration replay window"
                    return 1
                }
                strategy_start="$(format_epoch_utc "$strategy_start_epoch")"
                strategy_end="$(format_epoch_utc "$strategy_end_epoch")"
                local strategy_report="$output_dir/${group_key}_strategy_report.json"
                local strategy_report_target="$strategy_report"
                local strategy_trades="$output_dir/${group_key}_strategy_trades.json"
                local strategy_trades_target="$strategy_trades"
                if [ "$dry_run" = false ]; then
                    strategy_report_target="${strategy_report}.sealed.$$"
                    strategy_trades_target="${strategy_trades}.sealed.$$"
                fi
                local strategy_command=(
                    "$binary" harness-sweep
                    --start "$strategy_start"
                    --end "$strategy_end"
                    --cache-dir "$cache"
                    --require-shared-distilled
                    --variant-json "$strategy_variant_json"
                    "${strategy_condition_args[@]}"
                    --btc-csv "$raw/binance_btcusdt_rtds.csv"
                    --settlement-btc-csv "$raw/chainlink_btcusd.csv"
                    --latency-ms "$latency"
                    --threads "$threads"
                    --top 2
                    --window-minutes 5
                    --continuous
                    --report-json "$strategy_report_target"
                    --trades-json "$strategy_trades_target"
                )
                if [ "$dry_run" = true ]; then
                    printf '  PMXT_DISTILLED_DIR=%q ' "$converted"
                    printf '%q ' "${strategy_command[@]}"
                    printf '\n'
                else
                    local sealed_stdout="${strategy_report_target}.stdout.log"
                    local sealed_stderr="${strategy_report_target}.stderr.log"
                    if ! PMXT_DISTILLED_DIR="$converted" "${strategy_command[@]}" \
                        >"$sealed_stdout" 2>"$sealed_stderr"; then
                        rm -f "$strategy_report_target" "$strategy_trades_target" \
                            "$sealed_stdout" "$sealed_stderr" \
                            "${strategy_sealed_reports[@]}" "${strategy_sealed_trades[@]}" \
                            "${strategy_sealed_logs[@]}"
                        fail "$group_key sealed strategy replay failed"
                        return 1
                    fi
                    strategy_sealed_reports+=("$strategy_report_target")
                    strategy_final_reports+=("$strategy_report")
                    strategy_sealed_trades+=("$strategy_trades_target")
                    strategy_final_trades+=("$strategy_trades")
                    strategy_sealed_logs+=("$sealed_stdout" "$sealed_stderr")
                fi
            fi
            opportunities+=("$opportunity")
            resolutions+=("$resolution")
        done
    done

    if [ -n "$strategy_variant_json" ] && [ "$dry_run" = false ]; then
        local strategy_trade_count
        if ! strategy_trade_count="$(jq -s --arg hash "$strategy_candidate_hash" '
            [ .[] | .variants[]
              | select(.strategy.params_hash == $hash)
              | .trades ]
            | add // 0
        ' "${strategy_sealed_reports[@]}")"; then
            rm -f "${strategy_sealed_reports[@]}" "${strategy_sealed_trades[@]}" \
                "${strategy_sealed_logs[@]}"
            fail "sealed strategy reports could not be validated"
            return 1
        fi
        if ! is_nonnegative_integer "$strategy_trade_count" \
            || [ "$strategy_trade_count" -lt "$strategy_min_trades" ]; then
            rm -f "${strategy_sealed_reports[@]}" "${strategy_sealed_trades[@]}" \
                "${strategy_sealed_logs[@]}"
            fail "strategy comparison remains sealed below its preregistered trade floor"
            return 1
        fi
        local strategy_index
        for strategy_index in "${!strategy_sealed_reports[@]}"; do
            mv "${strategy_sealed_reports[$strategy_index]}" "${strategy_final_reports[$strategy_index]}"
            mv "${strategy_sealed_trades[$strategy_index]}" "${strategy_final_trades[$strategy_index]}"
            mv "${strategy_sealed_reports[$strategy_index]}.stdout.log" \
                "${strategy_final_reports[$strategy_index]}.stdout.log"
            mv "${strategy_sealed_reports[$strategy_index]}.stderr.log" \
                "${strategy_final_reports[$strategy_index]}.stderr.log"
        done
    fi

    local screen="$output_dir/binary_complement_screen.json"
    local score_command=(
        "$binary" strategy-builder binary-complement-screen
        --block-id "$block_id"
        --output "$screen"
    )
    local path
    for path in "${opportunities[@]}"; do
        score_command+=(--opportunity "$path")
    done
    for path in "${resolutions[@]}"; do
        score_command+=(--resolution-manifest "$path")
    done
    if [ "$dry_run" = true ]; then
        print_command "${score_command[@]}"
    else
        "${score_command[@]}"
    fi
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    main "$@"
fi
