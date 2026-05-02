#!/bin/bash
# Rust-only deploy: build the polymomentum-engine binary, copy to VPS,
# update systemd unit, restart.
#
# Usage:
#   deploy/deploy.sh user@vps-ip [--enable-service] [--mode paper|live] [--i-understand-live] [--binary ./polymomentum-engine-linux-x86_64]
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
    echo "Usage: $0 user@vps-ip [--enable-service] [--mode paper|live] [--binary ./polymomentum-engine-linux-x86_64]" >&2
    exit 1
fi

VPS="$1"; shift
ENABLE=false
MODE="paper"
LIVE_ACK=false
BINARY_PATH=""
while [ $# -gt 0 ]; do
    case "$1" in
        --enable-service) ENABLE=true; shift ;;
        --mode) MODE="$2"; shift 2 ;;
        --i-understand-live) LIVE_ACK=true; shift ;;
        --binary) BINARY_PATH="$2"; shift 2 ;;
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

if [ -n "$BINARY_PATH" ]; then
    case "$BINARY_PATH" in
        /*) BIN="$BINARY_PATH" ;;
        *) BIN="$(pwd)/$BINARY_PATH" ;;
    esac
    if [ ! -f "$BIN" ]; then
        echo "--binary path does not exist: $BIN" >&2
        exit 1
    fi
    if command -v file >/dev/null 2>&1 && ! file "$BIN" | grep -Eq 'ELF 64-bit.*x86-64|ELF 64-bit.*x86_64'; then
        echo "--binary must be a Linux x86_64 ELF executable: $BIN" >&2
        file "$BIN" >&2 || true
        exit 1
    fi
else
    HOST_OS="$(uname -s)"
    HOST_ARCH="$(uname -m)"
    if [ "$HOST_OS" != "Linux" ] || { [ "$HOST_ARCH" != "x86_64" ] && [ "$HOST_ARCH" != "amd64" ]; }; then
        echo "Refusing local release build on $HOST_OS/$HOST_ARCH; use --binary with the GitHub Linux artifact." >&2
        exit 2
    fi

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
fi

echo "=== Copying binary to $VPS ==="
ssh "$VPS" "mkdir -p $APP_DIR/logs/candle $APP_DIR/logs/sessions $APP_DIR/data"
scp "$BIN" "$VPS:$APP_DIR/polymomentum-engine.new"
ssh "$VPS" "chown polymomentum:polymomentum $APP_DIR/polymomentum-engine.new && \
    chmod 0755 $APP_DIR/polymomentum-engine.new && \
    mv $APP_DIR/polymomentum-engine.new $APP_DIR/polymomentum-engine"

echo "=== Installing support scripts and timers ==="
scp "$ROOT_DIR/deploy/healthcheck.sh" "$VPS:/tmp/polymomentum-healthcheck.sh"
scp "$ROOT_DIR/deploy/soak-report.sh" "$VPS:/tmp/polymomentum-soak-report.sh"
scp "$ROOT_DIR/deploy/polymomentum-healthcheck.service" "$VPS:/tmp/polymomentum-healthcheck.service"
scp "$ROOT_DIR/deploy/polymomentum-healthcheck.timer" "$VPS:/tmp/polymomentum-healthcheck.timer"
scp "$ROOT_DIR/deploy/polymomentum-soak-report.service" "$VPS:/tmp/polymomentum-soak-report.service"
scp "$ROOT_DIR/deploy/polymomentum-soak-report.timer" "$VPS:/tmp/polymomentum-soak-report.timer"
ssh "$VPS" "sudo install -o polymomentum -g polymomentum -m 0755 /tmp/polymomentum-healthcheck.sh $APP_DIR/healthcheck.sh && \
    sudo install -o polymomentum -g polymomentum -m 0755 /tmp/polymomentum-soak-report.sh $APP_DIR/soak-report.sh && \
    sudo install -o root -g root -m 0644 /tmp/polymomentum-healthcheck.service /etc/systemd/system/polymomentum-healthcheck.service && \
    sudo install -o root -g root -m 0644 /tmp/polymomentum-healthcheck.timer /etc/systemd/system/polymomentum-healthcheck.timer && \
    sudo install -o root -g root -m 0644 /tmp/polymomentum-soak-report.service /etc/systemd/system/polymomentum-soak-report.service && \
    sudo install -o root -g root -m 0644 /tmp/polymomentum-soak-report.timer /etc/systemd/system/polymomentum-soak-report.timer && \
    rm -f /tmp/polymomentum-healthcheck.sh /tmp/polymomentum-soak-report.sh /tmp/polymomentum-healthcheck.service /tmp/polymomentum-healthcheck.timer /tmp/polymomentum-soak-report.service /tmp/polymomentum-soak-report.timer"

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
    sudo systemctl daemon-reload && \
    sudo systemctl enable --now polymomentum-healthcheck.timer polymomentum-soak-report.timer"

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
