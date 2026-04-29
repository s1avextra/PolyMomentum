#!/bin/bash
# Rust-only deploy: build the polymomentum-engine binary, copy to VPS,
# update systemd unit, restart.
#
# Usage:
#   deploy/deploy.sh user@vps-ip [--enable-service] [--mode paper|live] [--i-understand-live]
#
# Layout on VPS:
#   /opt/polymomentum/
#     polymomentum-engine                 ← Rust binary
#     logs/                               ← shared log dir (state.db, sessions/)
#     data/                               ← shared data dir
#   /etc/polymomentum/env                 ← .env-style config
#   /etc/systemd/system/polymomentum-engine.service
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 user@vps-ip [--enable-service] [--mode paper|live]" >&2
    exit 1
fi

VPS="$1"; shift
ENABLE=false
MODE="paper"
LIVE_ACK=false
while [ $# -gt 0 ]; do
    case "$1" in
        --enable-service) ENABLE=true; shift ;;
        --mode) MODE="$2"; shift 2 ;;
        --i-understand-live) LIVE_ACK=true; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [ "$MODE" != "paper" ] && [ "$MODE" != "live" ]; then
    echo "--mode must be paper or live" >&2
    exit 2
fi
if [ "$MODE" = "live" ] && [ "$LIVE_ACK" != "true" ]; then
    echo "--mode live requires --i-understand-live and a passing binary preflight" >&2
    exit 2
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_DIR="/opt/polymomentum"

echo "=== Building Rust binary (release) ==="
GIT_SHA="$(cd "$ROOT_DIR" && git rev-parse --short HEAD 2>/dev/null || echo unknown)"
BUILD_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
(cd "$ROOT_DIR/rust_engine" && \
    POLYMOMENTUM_GIT_SHA="$GIT_SHA" \
    POLYMOMENTUM_BUILD_TIMESTAMP="$BUILD_TS" \
    cargo build --release --bin polymomentum-engine)

BIN="$ROOT_DIR/rust_engine/target/release/polymomentum-engine"
if [ ! -f "$BIN" ]; then
    echo "Build did not produce $BIN" >&2
    exit 1
fi

echo "=== Copying binary to $VPS ==="
ssh "$VPS" "mkdir -p $APP_DIR/logs/candle $APP_DIR/logs/sessions $APP_DIR/data"
scp "$BIN" "$VPS:$APP_DIR/polymomentum-engine.new"
ssh "$VPS" "chown polymomentum:polymomentum $APP_DIR/polymomentum-engine.new && \
    chmod 0755 $APP_DIR/polymomentum-engine.new && \
    mv $APP_DIR/polymomentum-engine.new $APP_DIR/polymomentum-engine"

echo "=== Updating systemd unit (mode=$MODE) ==="
SERVICE_TMP="$(mktemp)"
sed "s|--mode paper|--mode $MODE|" "$ROOT_DIR/deploy/polymomentum-engine.service" > "$SERVICE_TMP"
if [ "$MODE" = "live" ]; then
    sed -i.bak 's|--mode live$|--mode live --i-understand-live|' "$SERVICE_TMP"
    rm -f "$SERVICE_TMP.bak"
fi
scp "$SERVICE_TMP" "$VPS:/tmp/polymomentum-engine.service"
rm -f "$SERVICE_TMP"
ssh "$VPS" "sudo mv /tmp/polymomentum-engine.service /etc/systemd/system/polymomentum-engine.service && \
    sudo systemctl daemon-reload"

if $ENABLE; then
    echo "=== Checking peer bot state before restart ==="
    ADGTS_STATE="$(ssh "$VPS" "systemctl show adgts -p ActiveState --value 2>/dev/null || true")"
    POLYARB_STATE="$(ssh "$VPS" "systemctl show polyarbitrage -p ActiveState --value 2>/dev/null || true")"
    echo "adgts=${ADGTS_STATE:-unknown} polyarbitrage=${POLYARB_STATE:-unknown}"
    if [ "$ADGTS_STATE" = "deactivating" ] || [ "$POLYARB_STATE" = "deactivating" ]; then
        echo "A peer bot is currently deactivating; not restarting PolyMomentum now." >&2
        exit 1
    fi

    echo "=== Running remote preflight ==="
    PREFLIGHT_ACK="$([ "$MODE" = "live" ] && echo --i-understand-live || true)"
    ssh "$VPS" "sudo -u polymomentum bash -lc 'set -a; [ -f /etc/polymomentum/env ] && . /etc/polymomentum/env; set +a; $APP_DIR/polymomentum-engine preflight --mode $MODE $PREFLIGHT_ACK'"

    echo "=== Enabling service ==="
    ssh "$VPS" "sudo systemctl enable polymomentum-engine && sudo systemctl restart polymomentum-engine"
    ssh "$VPS" "systemctl show polymomentum-engine -p ActiveState -p SubState -p CPUQuotaPerSecUSec -p MemoryMax -p TasksMax --no-pager"
else
    echo "Service installed but not enabled. To enable: ssh $VPS 'sudo systemctl enable --now polymomentum-engine'"
fi

echo "=== Done ==="
echo "Logs: ssh $VPS 'journalctl -u polymomentum-engine -f'"
