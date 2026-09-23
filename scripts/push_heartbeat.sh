#!/bin/bash
# Mac orchestrator heartbeat: one small JSON pushed to the VPS after every
# runner loop; deploy/polymomentum-nodecheck.sh alerts on staleness.
# The only file the Mac ever writes to the VPS; everything else is pull-only.
set -uo pipefail
cd "$(dirname "$0")/.."
now=$(date +%s)
runner_ok=$([ "$(pgrep -f 'strategy_research_loop.py' | wc -l | tr -d ' ')" != "" ] && echo true || echo false)
tick_ok=$( { tail -c 4000 logs/strategy-research/executable_truth.log 2>/dev/null | grep -q Traceback; } && echo false || echo true)
last_cycle=$(sqlite3 logs/strategy-research/research.sqlite3 "SELECT max(started_at) FROM cycles" 2>/dev/null || echo "")
mac_obs=$(pgrep -f 'polymomentum-engine live --mode paper' >/dev/null && echo true || echo false)
tmp=$(mktemp)
printf '{"ts": %s, "host": "mac", "runner_ok": %s, "tick_ok": %s, "mac_observer": %s, "last_cycle": "%s", "git": "%s"}\n' \
    "$now" "$runner_ok" "$tick_ok" "$mac_obs" "$last_cycle" "$(git rev-parse --short HEAD 2>/dev/null)" > "$tmp"
scp -q -o ConnectTimeout=10 -o BatchMode=yes "$tmp" vps:/opt/polymomentum/export/heartbeat/mac.json.tmp 2>/dev/null \
    && ssh -o ConnectTimeout=10 -o BatchMode=yes vps 'mv -f /opt/polymomentum/export/heartbeat/mac.json.tmp /opt/polymomentum/export/heartbeat/mac.json' 2>/dev/null
rc=$?
rm -f "$tmp"
exit $rc
