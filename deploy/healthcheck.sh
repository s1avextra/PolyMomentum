#!/bin/bash
# PolyMomentum healthcheck — invoked by polymomentum-healthcheck.timer.
# Checks: service up, kill switch, breaker, last-trade staleness, disk pressure,
# forward-capture research progress.
set -uo pipefail

APP_DIR="${POLYMOMENTUM_DIR:-/opt/polymomentum}"
SERVICE="${POLYMOMENTUM_SERVICE:-polymomentum-engine}"
WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
TELEGRAM_TOKEN="${TELEGRAM_BOT_TOKEN:-${POLYMOMENTUM_TELEGRAM_BOT_TOKEN:-}}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-${POLYMOMENTUM_TELEGRAM_CHAT_ID:-}}"
KILL_FILE="${KILL_FILE:-${KILL_SWITCH_PATH:-/opt/polymomentum/KILL}}"
STATE_DB="${STATE_DB:-$APP_DIR/logs/candle/state.db}"
INACTIVE_HOURS="${INACTIVE_HOURS:-2}"
LOGS_LIMIT_MB="${LOGS_LIMIT_MB:-2048}"
DISK_WARN_PCT="${DISK_WARN_PCT:-85}"
DISK_CRITICAL_PCT="${DISK_CRITICAL_PCT:-90}"

case "$WEBHOOK_URL" in
    *api.telegram.org/bot*/sendMessage*) WEBHOOK_URL="" ;;
esac

# Per-category cooldown so we don't spam.
COOLDOWN_DIR="${COOLDOWN_DIR:-/var/tmp/polymomentum-healthcheck}"
mkdir -p "$COOLDOWN_DIR"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-1800}"

now=$(date +%s)

alert() {
    local category="$1"; shift
    local msg="$*"
    local last_file="$COOLDOWN_DIR/$category.last"
    if [ -f "$last_file" ]; then
        local last
        last=$(cat "$last_file")
        if [ $((now - last)) -lt "$COOLDOWN_SECONDS" ]; then
            return 0
        fi
    fi
    echo "$now" > "$last_file"
    logger -t polymomentum "HEALTH[$category]: $msg"
    if [ -n "$TELEGRAM_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
        curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
            --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
            --data-urlencode "text=:heart: HEALTH[$category]: $msg" \
            --data-urlencode "disable_web_page_preview=true" \
            >/dev/null 2>&1 || true
    elif [ -n "$WEBHOOK_URL" ]; then
        curl -s -X POST "$WEBHOOK_URL" \
            -H 'Content-Type: application/json' \
            -d "{\"text\": \":heart: HEALTH[$category]: $msg\"}" \
            >/dev/null 2>&1 || true
    fi
}

KILL_ACTIVE=false
if [ -f "$KILL_FILE" ]; then
    KILL_ACTIVE=true
    alert "kill_switch" "kill switch active at $KILL_FILE"
fi

# 1. Disk free. Check this before liveness so a disk-full preflight failure
# does not become an automatic restart loop.
DISK_CRITICAL=false
DISK=$(df "$APP_DIR" 2>/dev/null | awk 'NR==2{gsub("%","",$5); print $5}')
if [ -n "$DISK" ]; then
    if [ "$DISK" -gt "$DISK_CRITICAL_PCT" ]; then
        DISK_CRITICAL=true
        alert "disk_full" "disk usage ${DISK}% on $APP_DIR exceeds critical ${DISK_CRITICAL_PCT}% — not restarting services automatically"
    elif [ "$DISK" -gt "$DISK_WARN_PCT" ]; then
        alert "disk_pressure" "disk usage ${DISK}% on $APP_DIR exceeds warning ${DISK_WARN_PCT}%"
    fi
fi

# 2. Service liveness — restart and alert if dead.
if ! systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
    if [ "$KILL_ACTIVE" = true ]; then
        alert "service_stopped_by_kill" "$SERVICE inactive while kill switch is active — not restarting"
    elif [ "$DISK_CRITICAL" = true ]; then
        alert "service_stopped_by_disk" "$SERVICE inactive while disk is critical — not restarting"
    else
        alert "service_down" "$SERVICE inactive — restarting"
        systemctl restart "$SERVICE" 2>/dev/null || true
    fi
fi

# 3. State DB sanity — circuit breaker, last trade.
if [ -f "$STATE_DB" ] && command -v sqlite3 >/dev/null 2>&1; then
    DB_OUT=$(sqlite3 "$STATE_DB" \
        "SELECT 'breaker=' || COALESCE((SELECT value FROM meta WHERE key='candle_breaker_tripped'), '0'); \
         SELECT 'ts=' || COALESCE((SELECT MAX(timestamp) FROM trades), '0');" 2>/dev/null || echo "")
    BREAKER=$(echo "$DB_OUT" | sed -n 's/^breaker=//p')
    LAST_TRADE_TS=$(echo "$DB_OUT" | sed -n 's/^ts=//p')
    if [ "$BREAKER" = "1" ]; then
        alert "circuit_breaker" "candle circuit breaker is tripped — manual reset required"
    fi

    LAST_TRADE_TS=${LAST_TRADE_TS:-0}
    LAST_TRADE_TS=${LAST_TRADE_TS%.*}
    if [ "$LAST_TRADE_TS" != "0" ]; then
        AGE=$((now - LAST_TRADE_TS))
        MAX_AGE=$((INACTIVE_HOURS * 3600))
        if [ "$AGE" -gt "$MAX_AGE" ]; then
            HOURS=$((AGE / 3600))
            alert "no_trades" "no trades for ${HOURS}h (limit ${INACTIVE_HOURS}h)"
        fi
    fi
fi

# 4. Forward-capture research progress. The binary-complement collector has
# silently stalled twice (disk watermark in July, latency-gate exit in August);
# alert when a COLLECTING block stops producing artifacts or its unit failed.
FLOOR_DIR="${FLOOR_DIR:-$APP_DIR/logs/forward-captures/binary-complement-block1-floor}"
FLOOR_STATUS="$FLOOR_DIR/floor_collection_status.json"
RESEARCH_STALL_HOURS="${RESEARCH_STALL_HOURS:-12}"
if systemctl list-units --state=failed --plain --no-legend 'polymomentum-binary-complement-*' 'polymomentum-twap-era-*' 2>/dev/null | grep -q .; then
    alert "research_collector_failed" "a research capture/collector unit is in failed state — restart required"
fi

# 4b. Band canary: alert on failed state or a restart loop (>=5 starts in the
# last 30 minutes means it is crash-looping rather than trading).
if systemctl is-enabled polymomentum-band-canary >/dev/null 2>&1; then
    if systemctl is-failed --quiet polymomentum-band-canary; then
        alert "band_canary_failed" "polymomentum-band-canary is in failed state — operator attention required"
    else
        CANARY_STARTS=$(journalctl -u polymomentum-band-canary --since "-30 minutes" --no-pager -o cat 2>/dev/null | grep -c "Started polymomentum-band-canary" || true)
        if [ "${CANARY_STARTS:-0}" -ge 5 ]; then
            alert "band_canary_restart_loop" "polymomentum-band-canary restarted ${CANARY_STARTS} times in 30m — crash loop, operator attention required"
        fi
    fi
fi
# Generic capture-progress check: any active capture unit must keep producing
# artifacts under forward-captures/.
CAPTURES_ROOT="${CAPTURES_ROOT:-$APP_DIR/logs/forward-captures}"
if systemctl list-units --state=active --plain --no-legend 'polymomentum-twap-era-*' 'polymomentum-binary-complement-*' 2>/dev/null | grep -q . \
    && [ -d "$CAPTURES_ROOT" ]; then
    NEWEST_CAP_TS=$(find "$CAPTURES_ROOT" -mindepth 1 -maxdepth 3 -printf '%T@\n' 2>/dev/null | sort -nr | head -1 | cut -d. -f1)
    if [ -n "$NEWEST_CAP_TS" ]; then
        CAP_AGE_H=$(( (now - NEWEST_CAP_TS) / 3600 ))
        if [ "$CAP_AGE_H" -ge "$RESEARCH_STALL_HOURS" ]; then
            alert "research_stalled" "a capture unit is active but forward-captures produced no new artifact for ${CAP_AGE_H}h (limit ${RESEARCH_STALL_HOURS}h)"
        fi
    fi
fi
if [ -f "$FLOOR_STATUS" ] && command -v jq >/dev/null 2>&1; then
    FLOOR_STATE=$(jq -r '.state // empty' "$FLOOR_STATUS" 2>/dev/null)
    if [ "$FLOOR_STATE" = "COLLECTING" ]; then
        NEWEST_TS=$(find "$FLOOR_DIR" -mindepth 1 -maxdepth 2 -printf '%T@\n' 2>/dev/null | sort -nr | head -1 | cut -d. -f1)
        [ -n "$NEWEST_TS" ] || NEWEST_TS=$(stat -c %Y "$FLOOR_STATUS" 2>/dev/null || echo "$now")
        AGE_H=$(( (now - NEWEST_TS) / 3600 ))
        if [ "$AGE_H" -ge "$RESEARCH_STALL_HOURS" ]; then
            SUPPORT=$(jq -r '"\(.unique_ready_terminal_conditions)/\(.target_terminal_conditions)"' "$FLOOR_STATUS" 2>/dev/null || echo "?")
            alert "research_stalled" "binary-complement collection is COLLECTING but produced no new artifact for ${AGE_H}h (limit ${RESEARCH_STALL_HOURS}h) — support ${SUPPORT}"
        fi
    fi
fi

# 5. Disk-pressure on logs/.
LOGS_SIZE=$(du -sm "$APP_DIR/logs" 2>/dev/null | awk '{print $1}')
if [ -n "$LOGS_SIZE" ] && [ "$LOGS_SIZE" -gt "$LOGS_LIMIT_MB" ]; then
    alert "logs_full" "logs/ at ${LOGS_SIZE}MB > ${LOGS_LIMIT_MB}MB cap — rotate or trim"
fi

exit 0
