#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/polymomentum-replay-block-test.XXXXXX")"
trap 'rm -rf "$TMP_ROOT"' EXIT

SEGMENT="$TMP_ROOT/captures/session-a/segment_001"
mkdir -p "$SEGMENT/raw" "$SEGMENT/converted"
printf 'timestamp_ms,source,price,received_at_ms\n1784088600000,binance_btcusdt_rtds,1,1784088600000\n1784092500000,binance_btcusdt_rtds,1,1784092500000\n' >"$SEGMENT/raw/binance_btcusdt_rtds.csv"
printf 'timestamp_ms,source,price,received_at_ms\n1784092200000,chainlink_btc_usd_data_stream,1,1784092200000\n1784092500000,chainlink_btc_usd_data_stream,1,1784092500000\n' >"$SEGMENT/raw/chainlink_btcusd.csv"
jq -n '{"0xabc":{condition_id:"0xabc",slug:"btc-updown-5m-1784092200"}}' >"$SEGMENT/raw/gamma_market_cache.json"

jq -n '{
  first_window_start:"2026-07-15T05:10:00Z",
  capture_verified:true,
  resolution_ready:true,
  recommended_replay_latency_ms:151
}' >"$SEGMENT/status.json"

jq -n --arg dir "$SEGMENT/converted" '{
  output:{
    harness_env:{PMXT_DISTILLED_DIR:$dir},
    exact_replay_flag:"--require-shared-distilled"
  },
  hours:[
    {hour:"2026-07-15T05:00:00+00:00"},
    {hour:"2026-07-15T06:00:00+00:00"}
  ]
}' >"$SEGMENT/converted/manifest.json"

jq -n '{
  a_plus_gate:{settlement_alignment_ready:true},
  markets:[range(0; 750) as $i | {
    condition_id:("0x" + ($i | tostring)),
    open_ts_s:1784092200,
    close_ts_s:1784092500,
    settlement_aligned:true,
    official_source_matches_btc_tape:true,
    terminal_direction:"up"
  }]
}' >"$SEGMENT/resolution_manifest.json"

OUTPUT="$TMP_ROOT/output"
PLAN="$TMP_ROOT/plan.txt"
cp "$SEGMENT/resolution_manifest.json" "$TMP_ROOT/resolution-750.json"
jq '.markets = .markets[:749]' "$SEGMENT/resolution_manifest.json" >"$TMP_ROOT/resolution-749.json"
mv "$TMP_ROOT/resolution-749.json" "$SEGMENT/resolution_manifest.json"
if "$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$OUTPUT" \
  --block-id fixture-block \
  --binary /usr/bin/true \
  --dry-run >/dev/null 2>&1; then
    echo "binary-complement replay generated a plan below the 750-condition floor" >&2
    exit 1
fi
test ! -e "$OUTPUT"
mv "$TMP_ROOT/resolution-750.json" "$SEGMENT/resolution_manifest.json"
"$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$OUTPUT" \
  --block-id fixture-block \
  --binary /usr/bin/true \
  --dry-run >"$PLAN"

grep -q 'PMXT_DISTILLED_DIR=' "$PLAN"
grep -q -- '--require-shared-distilled' "$PLAN"
grep -q -- '--settlement-btc-csv' "$PLAN"
grep -q -- '--latency-ms 202' "$PLAN"
grep -q -- '--start 2026-07-15T05:10:00Z' "$PLAN"
grep -q -- '--end 2026-07-15T05:10:00Z' "$PLAN"
grep -q 'binary-complement-screen' "$PLAN"
grep -q -- '--resolution-manifest' "$PLAN"
test "$(grep -c 'harness-sweep' "$PLAN")" -eq 1
test ! -e "$OUTPUT"

jq -n '{
  evaluation_contract:{
    minimum_preflight_terminal_conditions:800,
    minimum_disclosure_trades:100,
    score_not_before_epoch_s:1784092200
  },
  candidate:{params_hash:"c25aa94ad592b6274150e48be7765ace8fa3beba85595e48225906ca01c01363"}
}' >"$TMP_ROOT/strategy-preregistration.json"
if "$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$OUTPUT" \
  --block-id fixture-block \
  --binary /usr/bin/true \
  --strategy-variant-json "$ROOT_DIR/deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_pair.json" \
  --strategy-preregistration-json "$TMP_ROOT/strategy-preregistration.json" \
  --dry-run >/dev/null 2>&1; then
    echo "strategy comparison revealed a block below its disclosure floor" >&2
    exit 1
fi

jq '.markets = [range(0; 800) as $i | {
  condition_id:("0x" + ($i | tostring)),
  open_ts_s:1784092200,
  close_ts_s:1784092500,
  settlement_aligned:true,
  official_source_matches_btc_tape:true,
  terminal_direction:"up"
}]' "$SEGMENT/resolution_manifest.json" >"$TMP_ROOT/resolution-800.json"
mv "$TMP_ROOT/resolution-800.json" "$SEGMENT/resolution_manifest.json"
"$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$OUTPUT" \
  --block-id fixture-block \
  --binary /usr/bin/true \
  --strategy-variant-json "$ROOT_DIR/deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_pair.json" \
  --strategy-preregistration-json "$TMP_ROOT/strategy-preregistration.json" \
  --dry-run >"$PLAN"
test "$(grep -c 'harness-sweep' "$PLAN")" -eq 2
grep -q 'strategy_report.json' "$PLAN"
grep -q 'strategy_trades.json' "$PLAN"
grep -q -- '--trades-json' "$PLAN"
grep -q '20260718_complete_set_lock_v1_pair.json' "$PLAN"
grep -q -- '--condition-id 0x0' "$PLAN"

MOCK_ENGINE="$TMP_ROOT/mock-engine"
cat >"$MOCK_ENGINE" <<'EOF'
#!/bin/bash
set -euo pipefail
command_name="${1:-}"
shift || true
report=""
trades=""
opportunities=""
variant=""
while [ $# -gt 0 ]; do
    case "$1" in
        --report-json) report="$2"; shift 2 ;;
        --trades-json) trades="$2"; shift 2 ;;
        --calibration-opportunities-json) opportunities="$2"; shift 2 ;;
        --variant-json) variant="$2"; shift 2 ;;
        *) shift ;;
    esac
done
if [ "$command_name" != "harness-sweep" ]; then
    exit 0
fi
mkdir -p "$(dirname "$report")"
if [[ "$variant" == *complete_set_lock_v1_pair.json ]]; then
    jq -n '{variants:[{
      strategy:{params_hash:"c25aa94ad592b6274150e48be7765ace8fa3beba85595e48225906ca01c01363"},
      trades:99
    }]}' >"$report"
    jq -n '{variants:[]}' >"$trades"
else
    jq -n --argjson trades "${MOCK_CAPTURE_TRADES:-0}" '{variants:[{
      strategy:{params_hash:"34aa177f7ae8614814208cdd81ed74e09199007b924ee16b6e18dfa62fd49aa9"},
      trades:$trades
    }]}' >"$report"
    jq -n '[]' >"$opportunities"
fi
EOF
chmod +x "$MOCK_ENGINE"
TRADE_OUTPUT="$TMP_ROOT/trade-output"
if MOCK_CAPTURE_TRADES=1 "$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$TRADE_OUTPUT" \
  --block-id fixture-block \
  --binary "$MOCK_ENGINE" >/dev/null 2>&1; then
    echo "binary-complement replay accepted a capture variant that emitted a trade" >&2
    exit 1
fi
SEALED_OUTPUT="$TMP_ROOT/sealed-output"
if "$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$SEALED_OUTPUT" \
  --block-id fixture-block \
  --binary "$MOCK_ENGINE" \
  --strategy-variant-json "$ROOT_DIR/deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_pair.json" \
  --strategy-preregistration-json "$TMP_ROOT/strategy-preregistration.json" \
  >/dev/null 2>&1; then
    echo "strategy comparison published a candidate below its trade floor" >&2
    exit 1
fi
if find "$SEALED_OUTPUT" -type f \( -name '*strategy_report*' -o -name '*strategy_trades*' \) -print | grep -q .; then
    echo "strategy comparison left sealed candidate evidence behind" >&2
    exit 1
fi

cp "$SEGMENT/resolution_manifest.json" "$SEGMENT/resolution_group_001.json"
jq -n '{
  schema_version:1,
  total_groups:1,
  ready_groups:1,
  all_ready:true,
  groups:[{group:"group_001",manifest:"resolution_group_001.json",ready:true}]
}' >"$SEGMENT/resolution_summary.json"
rm "$SEGMENT/resolution_manifest.json"
GROUP_PLAN="$TMP_ROOT/group-plan.txt"
"$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$OUTPUT" \
  --block-id fixture-block \
  --binary /usr/bin/true \
  --dry-run >"$GROUP_PLAN"
grep -q 'resolution_group_001' "$GROUP_PLAN"

jq '.all_ready = false | .ready_groups = 0 | .groups[0].ready = false' \
  "$SEGMENT/resolution_summary.json" >"$TMP_ROOT/resolution-summary.bad.json"
mv "$SEGMENT/resolution_summary.json" "$TMP_ROOT/resolution-summary.good.json"
mv "$TMP_ROOT/resolution-summary.bad.json" "$SEGMENT/resolution_summary.json"
if "$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$OUTPUT" \
  --block-id fixture-block \
  --binary /usr/bin/true \
  --dry-run >/dev/null 2>&1; then
    echo "replay accepted an incomplete resolution-group summary" >&2
    exit 1
fi
mv "$TMP_ROOT/resolution-summary.good.json" "$SEGMENT/resolution_summary.json"

cp "$SEGMENT/raw/binance_btcusdt_rtds.csv" "$TMP_ROOT/binance.good.csv"
printf 'timestamp_ms,source,price,received_at_ms\n1784092170000,binance_btcusdt_rtds,1,1784092170000\n1784092500000,binance_btcusdt_rtds,1,1784092500000\n' >"$SEGMENT/raw/binance_btcusdt_rtds.csv"
if "$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$OUTPUT" \
  --block-id fixture-block \
  --binary /usr/bin/true \
  --dry-run >/dev/null 2>&1; then
    echo "replay accepted a Binance tape without the required causal hour" >&2
    exit 1
fi
mv "$TMP_ROOT/binance.good.csv" "$SEGMENT/raw/binance_btcusdt_rtds.csv"

cp "$SEGMENT/raw/binance_btcusdt_rtds.csv" "$TMP_ROOT/binance.good.csv"
printf 'timestamp_ms,source,price,received_at_ms\n' >"$SEGMENT/raw/binance_btcusdt_rtds.csv"
timestamp_ms=1784088600000
while [ "$timestamp_ms" -le 1784092500000 ]; do
    if [ "$timestamp_ms" -lt 1784090486000 ] || [ "$timestamp_ms" -gt 1784090505000 ]; then
        printf '%s,binance_btcusdt_rtds,1,%s\n' "$timestamp_ms" "$timestamp_ms" >>"$SEGMENT/raw/binance_btcusdt_rtds.csv"
    fi
    timestamp_ms=$(( timestamp_ms + 1000 ))
done
if "$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$OUTPUT" \
  --block-id fixture-block \
  --binary /usr/bin/true \
  --dry-run >/dev/null 2>&1; then
    echo "replay accepted a Binance tape with an internal 21-second gap" >&2
    exit 1
fi
mv "$TMP_ROOT/binance.good.csv" "$SEGMENT/raw/binance_btcusdt_rtds.csv"

jq 'del(.output.exact_replay_flag)' "$SEGMENT/converted/manifest.json" >"$TMP_ROOT/bad.json"
mv "$TMP_ROOT/bad.json" "$SEGMENT/converted/manifest.json"
if "$ROOT_DIR/deploy/replay-binary-complement-block.sh" \
  --capture-root "$TMP_ROOT/captures" \
  --output-dir "$OUTPUT" \
  --block-id fixture-block \
  --binary /usr/bin/true \
  --dry-run >/dev/null 2>&1; then
    echo "replay accepted a converter manifest without exact replay provenance" >&2
    exit 1
fi

echo "replay-binary-complement-block tests passed"
