#!/bin/bash
# PolyMomentum band-canary watchdog — invoked by polymomentum-healthcheck.timer.
#
# External by design: the canary's own telegram bot cannot report the
# canary's death or a zombie (process alive, cycle loop parked/stuck — the
# 2026-08-31 incident ran 8 hours in that state looking healthy).
#
# Alert policy: ONE message per state CHANGE, never repeats. States:
#   ok | down | zombie | halted:<reason>
set -uo pipefail

APP_DIR="${POLYMOMENTUM_DIR:-/opt/polymomentum}"
SERVICE="polymomentum-band-canary"
TELEGRAM_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"
SESSIONS_DIR="$APP_DIR/logs/band-canary/sessions"
STATE_FILE="/var/tmp/polymomentum-healthcheck.state"
ZOMBIE_AFTER_S="${ZOMBIE_AFTER_S:-600}"

notify() {
    [ -n "$TELEGRAM_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ] || return 0
    curl -s -m 10 -X POST "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
        -d "chat_id=${TELEGRAM_CHAT_ID}" --data-urlencode "text=$1" >/dev/null || true
}

state="ok"
if ! systemctl is-active --quiet "$SERVICE"; then
    state="down"
else
    # Zombie detection: the newest session must contain a recent cycle
    # record. A parked (halted) breaker is a distinct, explained state.
    session=$(ls -t "$SESSIONS_DIR"/session_*.jsonl 2>/dev/null | head -1)
    now=$(date +%s)
    last_cycle=$(grep -a '"type": *"cycle"' "$session" 2>/dev/null | tail -1 \
        | grep -oE '"ts": *[0-9]+' | grep -oE '[0-9]+' | head -1)
    halted=$(journalctl -u "$SERVICE" --since "-10 minutes" --no-pager -o cat 2>/dev/null \
        | grep -a "candle.halted" | tail -1)
    if [ -n "$halted" ]; then
        reason=$(printf '%s' "$halted" | grep -oE 'reason="[^"]*"' | head -1)
        state="halted:${reason:-unknown}"
    elif [ -z "$last_cycle" ]; then
        # Session younger than the zombie window may simply not have cycled yet.
        session_age=$(( now - $(stat -c %Y "$session" 2>/dev/null || echo "$now") ))
        started=$(stat -c %W "$session" 2>/dev/null || echo 0)
        [ $(( now - ${started:-0} )) -gt "$ZOMBIE_AFTER_S" ] && [ "$session_age" -gt "$ZOMBIE_AFTER_S" ] \
            && state="zombie"
    elif [ $(( now - last_cycle )) -gt "$ZOMBIE_AFTER_S" ]; then
        state="zombie"
    fi
fi

prev=$(cat "$STATE_FILE" 2>/dev/null || echo "ok")
if [ "$state" != "$prev" ]; then
    printf '%s' "$state" > "$STATE_FILE"
    case "$state" in
        ok)       notify "✓ canary recovered" ;;
        down)     notify "⚠ canary DOWN (service inactive)" ;;
        zombie)   notify "⚠ canary ZOMBIE — process alive, no cycles for 10m; restart required" ;;
        halted:*) notify "⏹ canary halted · ${state#halted:} · /start to resume" ;;
    esac
fi
echo "healthcheck: $state"
