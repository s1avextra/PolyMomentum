#!/bin/bash
# Rust-only deploy: build the polymomentum-engine binary, copy it to the
# VPS and install the support scripts and their timers.
#
# This script installs no engine unit. The engine units are the band
# observer and the band canary (deploy/polymomentum-band-*.service), whose
# env files bind the promotion artifact by hash through
# POLYMOMENTUM_PROMOTION_ARTIFACT; the operator installs them by the recipe
# in docs/profitability_basement_2026-09-18.md (F.5). The legacy
# polymomentum-engine.service target - a --mode live deploy that pinned the
# artifact on the command line and enabled a second engine process next to
# the running one - was deleted with the basement's engine cut (section D).
#
# Usage:
#   deploy/deploy.sh user@vps-ip [--binary ./polymomentum-engine-linux-x86_64]
#
# Layout on VPS:
#   /opt/polymomentum/
#     polymomentum-engine                 ← Rust binary
#     logs/                               ← shared log dir (state.db, sessions/)
#     data/                               ← shared data dir
#     config/                             ← promotion artifacts and deploy config
#   /etc/polymomentum/env                 ← .env-style config
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 user@vps-ip [--binary ./polymomentum-engine-linux-x86_64]" >&2
    exit 1
fi

VPS="$1"; shift
BINARY_PATH=""
while [ $# -gt 0 ]; do
    case "$1" in
        --binary) BINARY_PATH="$2"; shift 2 ;;
        --enable-service|--mode|--i-understand-live|--promotion-artifact|--allow-stale-research-artifact)
            echo "$1: the legacy polymomentum-engine.service deploy target was deleted; the band units are installed by the operator recipe (docs/profitability_basement_2026-09-18.md F.5)" >&2
            exit 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_DIR="/opt/polymomentum"
KILL_SWITCH_REMOTE="$APP_DIR/control/KILL"
SCP_CMD="${SCP_CMD:-scp}"

scp_copy() {
    $SCP_CMD "$@"
}

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
        cargo build --release --locked --bin polymomentum-engine)

    BIN="$ROOT_DIR/rust_engine/target/release/polymomentum-engine"
    if [ ! -f "$BIN" ]; then
        echo "Build did not produce $BIN" >&2
        exit 1
    fi
fi

echo "=== Copying binary to $VPS ==="
ssh "$VPS" "mkdir -p $APP_DIR/logs/candle $APP_DIR/logs/sessions $APP_DIR/data $APP_DIR/config && \
    sudo install -d -o polymomentum -g polymomentum -m 0755 $APP_DIR/control"
ssh "$VPS" "if [ -f /tmp/polymomentum/KILL ] || [ -f $APP_DIR/KILL ]; then sudo touch $KILL_SWITCH_REMOTE; fi; \
    sudo chown polymomentum:polymomentum $APP_DIR/control; \
    if [ -f $KILL_SWITCH_REMOTE ]; then sudo chown polymomentum:polymomentum $KILL_SWITCH_REMOTE; fi; \
    if sudo test -f /etc/polymomentum/env; then \
        sudo sed -i 's|^KILL_SWITCH_PATH=/tmp/polymomentum/KILL$|KILL_SWITCH_PATH=$KILL_SWITCH_REMOTE|; s|^KILL_SWITCH_PATH=$APP_DIR/KILL$|KILL_SWITCH_PATH=$KILL_SWITCH_REMOTE|' /etc/polymomentum/env; \
    fi"
scp_copy "$BIN" "$VPS:$APP_DIR/polymomentum-engine.new"
ssh "$VPS" "chown polymomentum:polymomentum $APP_DIR/polymomentum-engine.new && \
    chmod 0755 $APP_DIR/polymomentum-engine.new && \
    mv $APP_DIR/polymomentum-engine.new $APP_DIR/polymomentum-engine"

echo "=== Installing support scripts and timers ==="
scp_copy "$ROOT_DIR/deploy/healthcheck.sh" "$VPS:/tmp/polymomentum-healthcheck.sh"
scp_copy "$ROOT_DIR/deploy/soak-report.sh" "$VPS:/tmp/polymomentum-soak-report.sh"
scp_copy "$ROOT_DIR/deploy/polymomentum-healthcheck.service" "$VPS:/tmp/polymomentum-healthcheck.service"
scp_copy "$ROOT_DIR/deploy/polymomentum-healthcheck.timer" "$VPS:/tmp/polymomentum-healthcheck.timer"
scp_copy "$ROOT_DIR/deploy/polymomentum-soak-report.service" "$VPS:/tmp/polymomentum-soak-report.service"
scp_copy "$ROOT_DIR/deploy/polymomentum-soak-report.timer" "$VPS:/tmp/polymomentum-soak-report.timer"
ssh "$VPS" "sudo install -o polymomentum -g polymomentum -m 0755 /tmp/polymomentum-healthcheck.sh $APP_DIR/healthcheck.sh && \
    sudo install -o polymomentum -g polymomentum -m 0755 /tmp/polymomentum-soak-report.sh $APP_DIR/soak-report.sh && \
    sudo install -o root -g root -m 0644 /tmp/polymomentum-healthcheck.service /etc/systemd/system/polymomentum-healthcheck.service && \
    sudo install -o root -g root -m 0644 /tmp/polymomentum-healthcheck.timer /etc/systemd/system/polymomentum-healthcheck.timer && \
    sudo install -o root -g root -m 0644 /tmp/polymomentum-soak-report.service /etc/systemd/system/polymomentum-soak-report.service && \
    sudo install -o root -g root -m 0644 /tmp/polymomentum-soak-report.timer /etc/systemd/system/polymomentum-soak-report.timer && \
    rm -f /tmp/polymomentum-healthcheck.sh /tmp/polymomentum-soak-report.sh /tmp/polymomentum-healthcheck.service /tmp/polymomentum-healthcheck.timer /tmp/polymomentum-soak-report.service /tmp/polymomentum-soak-report.timer && \
    sudo systemctl daemon-reload && \
    sudo systemctl enable --now polymomentum-healthcheck.timer polymomentum-soak-report.timer"

echo "=== Done ==="
echo "Binary at $APP_DIR/polymomentum-engine; no engine unit was touched (band units: operator recipe, docs/profitability_basement_2026-09-18.md F.5)."
