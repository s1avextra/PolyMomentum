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
# LM Studio on MainPC is reached through an ssh tunnel over Tailscale
# (127.0.0.1:1235 -> mainpc:1234); LM Link proved to drop models mid-burst.
ensure_tunnel() {
    pgrep -f "ssh .*-L 1235:127.0.0.1:1234 mainpc" >/dev/null && return 0
    ssh -o ExitOnForwardFailure=yes -o ConnectTimeout=8 -f -N -L 1235:127.0.0.1:1234 mainpc \
        >> "$log" 2>&1 || echo "$(date -u +%FT%TZ) factory-runner: tunnel to mainpc failed" >> "$log"
}
while true; do
    ensure_tunnel
    models=$(python3 -c 'import json, sys
llm = json.load(open(sys.argv[1]))["llm"]
seen = []
for model in [llm.get("default_model")] + list(llm.get("sampler_models") or []) + [llm.get("reviewer_model")]:
    if model and model not in seen:
        seen.append(model)
print("\n".join(seen))' "$config" 2>/dev/null)
    for model in $models; do
        curl -s -m 25 -o /dev/null -X POST http://127.0.0.1:1235/v1/chat/completions \
            -H 'Content-Type: application/json' \
            -d "{\"model\":\"$model\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}],\"max_tokens\":1}" \
            || true
    done
    case $((tick % 3)) in
        0) lane_args=() ;;
        1) lane_args=(--lane late_window_mechanisms) ;;
        2) lane_args=(--lane band_mechanisms) ;;
    esac
    uv run python scripts/strategy_research_loop.py \
        --config "$config" --once \
        "${lane_args[@]}" \
        >> "$log" 2>&1
    tick=$((tick + 1))
    sleep 300
done
