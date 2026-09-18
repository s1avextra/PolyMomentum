#!/bin/bash
# PolyMomentum hypothesis-factory runner (Mac only; VPS untouched).
#
# Rotates three lanes per 5 min tick (each lane every 15 min): the opportunity funnel (run_cycle with
# no --lane always picks opportunity mode, which starved the generator -
# discovered 2026-09-01), the LLM late-window generator lane, and the band
# lane that searches the family actually trading (2026-09-02).  Before each
# cycle it POSTs a one-token completion to LM Studio for every model the
# overlay can route to (llm.default_model, each llm.sampler_models entry and
# llm.reviewer_model) so they stay loaded across LM Link's 1-hour idle TTL
# (output discarded, failure ignored).  The loop's readiness() probe covers
# default_model only, so a roster model that is not loaded fails its burst
# (60 s timeout or a load error per sample); with LM Studio's JIT
# "unload previous model on load" setting on, every model switch is a cold
# load - pin the roster models in LM Studio when enabling the ensemble.
# Only one instance runs: a second copy (launchd plus a manual run) logs one
# line to runner.log and exits 0.  launchd relaunches only on a non-zero exit
# (KeepAlive SuccessfulExit=false), so once a manual run ends restart the
# agent with:  launchctl kickstart gui/$(id -u)/com.polymomentum.factory-runner
#
# launchd user agent (deploy/com.polymomentum.factory-runner.plist):
#   cp deploy/com.polymomentum.factory-runner.plist ~/Library/LaunchAgents/
#   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.polymomentum.factory-runner.plist
# uninstall:
#   launchctl bootout gui/$(id -u)/com.polymomentum.factory-runner
# Retire the stale Aug-12 copy of the loop (com.polymomentum.strategy-research:
# every 30 min from ~/Library/Application Support/PolyMomentumStrategyResearch
# against the same LM Studio):
#   launchctl bootout gui/$(id -u)/com.polymomentum.strategy-research
#   mv ~/Library/LaunchAgents/com.polymomentum.strategy-research.plist \
#      ~/Library/LaunchAgents/com.polymomentum.strategy-research.plist.disabled
#
# Recommended sampler ensemble and reviewer (off by default; set under "llm"
# in the overlay logs/strategy-research/loop-config.local.json):
#   "sampler_models": ["openai/gpt-oss-20b", "deepseek-v4-flash-0731"],
#   "reviewer_model": "qwen/qwen3.8-27b"
cd /Users/ttoomm/Documents/PolyMomentum
log=logs/strategy-research/runner.log
config=logs/strategy-research/loop-config.local.json
for pid in $(pgrep -f "bash .*$(basename "$0")"); do
    if [ "$pid" != "$$" ]; then
        echo "$(date -u +%FT%TZ) factory-runner: already running as pid $pid; exiting" >> "$log"
        exit 0
    fi
done
tick=0
while true; do
    # Phase 0 of docs/profitability_basement_2026-09-18.md: band lane only,
    # no LLM tunnel or keepalive (the grid is enumerated by the evaluator;
    # LLM turns fall through to the control draw when no model answers).
    if [ -f "$log" ] && [ "$(stat -f %z "$log" 2>/dev/null || echo 0)" -gt 5000000 ]; then
        mv "$log" "$log.1"
    fi
    uv run python scripts/strategy_research_loop.py \
        --config "$config" --once --lane band_mechanisms \
        >> "$log" 2>&1
    sleep 300
done
