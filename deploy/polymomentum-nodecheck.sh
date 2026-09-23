#!/bin/bash
# PolyMomentum node watchdog — invoked by polymomentum-nodecheck.timer (5 min).
#
# The VPS is the always-on node, so it watches everything else: the Mac
# orchestrator's heartbeat (pushed by deploy/factory-runner.sh), the paper
# observer's liveness and record flow, and this box's disk. Sibling of
# healthcheck.sh (which watches the live canary); same Telegram plumbing
# (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID from /etc/polymomentum/env).
#
# Alert policy: ONE message per state CHANGE per check, never repeats.
# The 2026-09-20..23 outage (Mac lost Gamma/Binance for three days, unnoticed)
# is the incident this exists for.
set -uo pipefail

APP_DIR="${POLYMOMENTUM_DIR:-/opt/polymomentum}"
HEARTBEAT="${MAC_HEARTBEAT:-$APP_DIR/export/heartbeat/mac.json}"
OBSERVER_SERVICE="${OBSERVER_SERVICE:-polymomentum-band-observer}"
OBSERVER_SESSIONS="${OBSERVER_SESSIONS:-$APP_DIR/logs/band-observer/sessions}"
STATE_DIR="${NODECHECK_STATE_DIR:-/var/tmp/polymomentum-nodecheck}"
HEARTBEAT_STALE_S="${HEARTBEAT_STALE_S:-1800}"
OBSERVER_STALE_S="${OBSERVER_STALE_S:-1200}"
DISK_WARN_PCT="${DISK_WARN_PCT:-80}"
DISK_CRIT_PCT="${DISK_CRIT_PCT:-90}"
TELEGRAM_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"
mkdir -p "$STATE_DIR"

notify() {
    [ -n "$TELEGRAM_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ] || { echo "notify (no telegram): $1"; return 0; }
    curl -s -m 10 -X POST "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
        -d "chat_id=${TELEGRAM_CHAT_ID}" --data-urlencode "text=$1" >/dev/null || true
}

# transition <check> <state> <message-if-changed>
transition() {
    local prev
    prev=$(cat "$STATE_DIR/$1" 2>/dev/null || echo "unknown")
    if [ "$2" != "$prev" ]; then
        printf '%s' "$2" > "$STATE_DIR/$1"
        [ "$prev" = "unknown" ] && [ "$2" = "ok" ] && return 0   # first run, healthy: silent
        notify "$3"
    fi
    echo "nodecheck: $1=$2"
}

now=$(date +%s)

# 1. Mac orchestrator heartbeat (written by deploy/factory-runner.sh each loop).
if [ -f "$HEARTBEAT" ]; then
    hb_ts=$(grep -oE '"ts": *[0-9]+' "$HEARTBEAT" | grep -oE '[0-9]+' | head -1)
    age=$(( now - ${hb_ts:-0} ))
    if [ "$age" -gt "$HEARTBEAT_STALE_S" ]; then
        transition mac "stale" "⚠ mac orchestrator silent for $((age/60)) min (runner down, Mac asleep, or network)"
    else
        runner_ok=$(grep -oE '"runner_ok": *(true|false)' "$HEARTBEAT" | grep -oE 'true|false' | head -1)
        tick_ok=$(grep -oE '"tick_ok": *(true|false)' "$HEARTBEAT" | grep -oE 'true|false' | head -1)
        if [ "${runner_ok:-true}" = "false" ] || [ "${tick_ok:-true}" = "false" ]; then
            transition mac "degraded" "⚠ mac orchestrator degraded · runner_ok=${runner_ok:-?} tick_ok=${tick_ok:-?}"
        else
            transition mac "ok" "✓ mac orchestrator ok"
        fi
    fi
else
    transition mac "missing" "⚠ mac heartbeat never received at $HEARTBEAT"
fi

# 2. Observer: unit active and the newest session still growing.
if ! systemctl is-active --quiet "$OBSERVER_SERVICE"; then
    transition observer "down" "⚠ observer DOWN ($OBSERVER_SERVICE inactive) — no ladder records"
else
    newest=$(ls -t "$OBSERVER_SESSIONS"/session_*.jsonl 2>/dev/null | head -1)
    if [ -z "$newest" ]; then
        transition observer "nosession" "⚠ observer active but no session file yet"
    else
        mtime=$(stat -c %Y "$newest" 2>/dev/null || stat -f %m "$newest")
        if [ $(( now - mtime )) -gt "$OBSERVER_STALE_S" ]; then
            transition observer "stale" "⚠ observer session idle for $(( (now - mtime)/60 )) min (feeds down?)"
        else
            transition observer "ok" "✓ observer ok"
        fi
    fi
fi

# 3. Disk on /opt with hysteresis (state changes only on crossing).
used_pct=$(df -P "$APP_DIR" | awk 'NR==2 {gsub("%","",$5); print $5}')
prev_disk=$(cat "$STATE_DIR/disk" 2>/dev/null || echo "ok")
if [ "$used_pct" -ge "$DISK_CRIT_PCT" ]; then disk="crit"
elif [ "$used_pct" -ge "$DISK_WARN_PCT" ]; then disk="warn"
elif [ "$prev_disk" != "ok" ] && [ "$used_pct" -ge $(( DISK_WARN_PCT - 5 )) ]; then disk="$prev_disk"  # hysteresis
else disk="ok"; fi
transition disk "$disk" "$( [ "$disk" = ok ] && echo "✓ vps disk back to ${used_pct}%" || echo "⚠ vps disk ${used_pct}% used ($disk)" )"
