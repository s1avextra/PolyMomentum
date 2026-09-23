#!/bin/bash
# PolyMomentum factory runner (Mac only; the VPS is never written).
#
# Every 5 min: one band-lane cycle (scripts/strategy_research_loop.py --lane
# band_mechanisms: public cache refresh, accrual, rescreen, one grammar C
# proposal).  Every third loop (15 min): scripts/executable_truth.py --tick,
# the incremental window build plus the accrual of every registered campaign
# (docs/profitability_basement_2026-09-18.md section C), sequenced after the
# lane so the two never write the public cache at once.  Both logs rotate at
# 5 MB.  No LLM tunnel or keepalive: the proposer is deterministic.
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
# The Aug-12 agent com.polymomentum.strategy-research (its plist template was
# deleted in Phase 3) is retired with:
#   launchctl bootout gui/$(id -u)/com.polymomentum.strategy-research
#   mv ~/Library/LaunchAgents/com.polymomentum.strategy-research.plist \
#      ~/Library/LaunchAgents/com.polymomentum.strategy-research.plist.disabled
cd /Users/ttoomm/Documents/PolyMomentum
log=logs/strategy-research/runner.log
tick_log=logs/strategy-research/executable_truth.log
config=logs/strategy-research/loop-config.local.json
for pid in $(pgrep -f "bash .*$(basename "$0")"); do
    if [ "$pid" != "$$" ]; then
        echo "$(date -u +%FT%TZ) factory-runner: already running as pid $pid; exiting" >> "$log"
        exit 0
    fi
done
rotate() {
    if [ -f "$1" ] && [ "$(stat -f %z "$1" 2>/dev/null || echo 0)" -gt 5000000 ]; then
        mv "$1" "$1.1"
    fi
}
loop=0
while true; do
    rotate "$log"
    uv run --offline python scripts/strategy_research_loop.py \
        --config "$config" --once --lane band_mechanisms \
        >> "$log" 2>&1
    if [ $((loop % 3)) -eq 0 ]; then
        rotate "$tick_log"
        echo "$(date -u +%FT%TZ) tick" >> "$tick_log"
        bash scripts/pull_vps_sessions.sh >/dev/null 2>&1; uv run --offline python scripts/executable_truth.py --tick --loop-config "$config" \
            >> "$tick_log" 2>&1
    fi
    loop=$((loop + 1))
    bash scripts/push_heartbeat.sh >/dev/null 2>&1 || true
    sleep 300
done
