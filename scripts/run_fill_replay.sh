#!/bin/bash
# Fill-rate replay pipeline over ALL captured campaign hours:
# catalog -> signals -> v3 tables (entry30_* patient entry) per hour.
# Outcome-free: no labels are created anywhere.
set -uo pipefail
cd /Users/ttoomm/Documents/PolyMomentum
BIN=./rust_engine/target/release/polymomentum-engine
DIR=logs/strategy-research/fill-replay
DIST=data/campaign_replay_flat
mkdir -p "$DIR/signals" "$DIR/tables"

HOURS=$(ls $DIST | sed 's/.v1.candles.jsonl.gz//' | sort -u | grep -v '2026-08-24')

if [ ! -s "$DIR/market_catalog.json" ]; then
  HFLAGS=()
  for h in $HOURS; do HFLAGS+=(--hour "${h}:00:00Z"); done
  $BIN strategy-builder opportunity-market-catalog "${HFLAGS[@]}" --family btc-5m \
    --output "$DIR/market_catalog.json" --manifest "$DIR/market_catalog.manifest.json" \
    > /dev/null 2> "$DIR/catalog.err" || { echo "CATALOG FAIL: $(tail -1 $DIR/catalog.err)"; exit 1; }
fi

process_hour() {
  h="$1"
  BIN=./rust_engine/target/release/polymomentum-engine
  DIR=logs/strategy-research/fill-replay
  DIST=data/campaign_replay_flat
  sig="$DIR/signals/$h.jsonl"; tab="$DIR/tables/$h.parquet"
  [ -s "$tab" ] && { echo "SKIP $h"; return 0; }
  $BIN strategy-builder opportunity-signals --hour "${h}:00:00Z" \
    --causal-windows "$DIR/causal_windows_fill_replay.jsonl" \
    --market-catalog "$DIR/market_catalog.json" \
    --output "$sig" --manifest "$DIR/signals/$h.manifest.json" --family btc-5m \
    > /dev/null 2> "$DIR/signals/$h.err" || { echo "SIG FAIL $h: $(tail -1 $DIR/signals/$h.err)"; return 1; }
  $BIN strategy-builder opportunity-table --hour "${h}:00:00Z" --signals "$sig" \
    --distilled-input "$DIST/$h.v1.candles.jsonl.gz" --cache-dir data/pmxt_twap_era \
    --output "$tab" --manifest "$DIR/tables/$h.manifest.json" \
    > /dev/null 2> "$DIR/tables/$h.err" || { echo "TAB FAIL $h: $(tail -1 $DIR/tables/$h.err)"; return 1; }
  echo "OK $h"
}
export -f process_hour

echo "$HOURS" | tr ' ' '\n' | xargs -P 6 -I{} bash -c 'process_hour "$@"' _ {} | sort | uniq -c | sort -rn | head -8
ls "$DIR/tables/"*.parquet 2>/dev/null | wc -l
