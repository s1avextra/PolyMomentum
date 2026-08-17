#!/bin/bash
# Run the one-shot, preregistered v2 historical diagnostic from retained sidecars.
set -euo pipefail

SOURCE_ROOT=/private/tmp/polymomentum_complete_set_historical_tail_20260718
OUTPUT_ROOT=/private/tmp/polymomentum_trailing_complete_set_v2_historical_tail_20260718
BINARY=./rust_engine/target/debug/polymomentum-engine
PAIR=deploy/promotions/evidence/strategy_registry/20260718_trailing_complete_set_lock_v2_pair.json

[ "$#" -gt 0 ] || {
    echo "usage: $0 FOLD [FOLD ...]" >&2
    exit 2
}

for fold in "$@"; do
    case "$fold" in
        0[2-3][0-9]|04[0-2]) ;;
        *) echo "invalid fold: $fold" >&2; exit 2 ;;
    esac
    source_dir="$SOURCE_ROOT/fold_$fold"
    output_dir="$OUTPUT_ROOT/fold_$fold"
    [ -f "$source_dir/trades.json" ] || {
        echo "missing source report: $source_dir/trades.json" >&2
        exit 1
    }
    sidecars="$(find "$source_dir/cache" -maxdepth 1 -name '*.events.bin.gz' | wc -l | tr -d ' ')"
    [ "$sidecars" -eq 8 ] || {
        echo "expected 8 retained sidecars for fold $fold, found $sidecars" >&2
        exit 1
    }
    [ ! -e "$output_dir" ] || {
        echo "output already exists: $output_dir" >&2
        exit 1
    }
    mkdir -p "$output_dir"
    start="$(jq -r '.start' "$source_dir/trades.json")"
    end="$(jq -r '.end' "$source_dir/trades.json")"
    echo "fold $fold: $start through $end"
    RAYON_NUM_THREADS=2 "$BINARY" harness-sweep \
        --start "$start" \
        --end "$end" \
        --bankroll 100 \
        --max-total-exposure-usd 80 \
        --cache-dir "$source_dir/cache" \
        --variant-json "$PAIR" \
        --latency-ms 202 \
        --top 2 \
        --threads 2 \
        --report-json "$output_dir/sweep.json" \
        --trades-json "$output_dir/trades.json" \
        --window-minutes 5 \
        --continuous \
        --atomic-parquet
done
