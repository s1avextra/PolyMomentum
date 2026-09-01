#!/bin/bash
# PolyMomentum hypothesis-factory runner (Mac only; VPS untouched).
# Alternates lanes: the opportunity funnel and the LLM-generator lane -
# run_cycle with no --lane always picks opportunity mode, which starved
# the generator (discovered 2026-09-01).
cd /Users/ttoomm/Documents/PolyMomentum
tick=0
while true; do
    if [ $((tick % 2)) -eq 0 ]; then
        uv run python scripts/strategy_research_loop.py \
            --config logs/strategy-research/loop-config.local.json --once \
            >> logs/strategy-research/runner.log 2>&1
    else
        uv run python scripts/strategy_research_loop.py \
            --config logs/strategy-research/loop-config.local.json --once \
            --lane late_window_mechanisms \
            >> logs/strategy-research/runner.log 2>&1
    fi
    tick=$((tick + 1))
    sleep 600
done
