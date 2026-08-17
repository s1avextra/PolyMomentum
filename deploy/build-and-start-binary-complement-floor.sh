#!/bin/bash
# Deferred VPS build/install for measurement v4, followed by the sealed floor collector.
set -euo pipefail

WAIT_UNIT="polymomentum-binary-complement-block1-continuation-20260718.service"
WAIT_MAX_SECONDS=57600
POLL_SECONDS=30
BUILD_DIR="/opt/polymomentum/builds/7ff0bbe/rust_engine"
TARGET_BINARY="$BUILD_DIR/target/release/polymomentum-engine"
INSTALLED_BINARY="/opt/polymomentum/tools/polymomentum-engine-measurement-v4"
CAPTURE_RUNNER="/opt/polymomentum/tools/capture-forward-segments-v4.sh"
FLOOR_COLLECTOR="/opt/polymomentum/tools/collect-binary-complement-floor.sh"
COLLECTOR_UNIT="polymomentum-binary-complement-block1-floor-20260718.service"
EXPECTED_MAIN_SHA256="9cc65b08f7430b1040eac42ea156326161b91f21acff9d121ba6e7fc5fa763b6"
EXPECTED_CARGO_TOML_SHA256="1c75fdb602caab7a21bf6a94aa4a62a12c7beb517b47aadcd382a6276729c006"
EXPECTED_CARGO_LOCK_SHA256="6f179467536bc4bfa3461640de9cb9dffc9be55ec6b052d8521bf37ec091e031"
EXPECTED_RUNNER_SHA256="741fbf4731ec2ae3b3699cab85297ab84ca6482faa09226cae52af608f0c587a"
EXPECTED_COLLECTOR_SHA256="6dbf0c20ab4368f8e1ebedbf30ab7e82ed2accd7fb84f7a7a3d9c56caa2d1af1"

log() {
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

require_sha256() {
    local path="$1"
    local expected="$2"
    local actual
    actual="$(sha256sum "$path" | awk '{print $1}')"
    [ "$actual" = "$expected" ] || {
        log "SHA-256 mismatch for $path: expected=$expected actual=$actual"
        return 1
    }
}

main() {
    [ "$(id -u)" -eq 0 ] || {
        log "run as root so the isolated binary and transient unit can be installed"
        return 2
    }

    local deadline=$(( $(date -u +%s) + WAIT_MAX_SECONDS ))
    while systemctl is-active --quiet "$WAIT_UNIT"; do
        if [ "$(date -u +%s)" -ge "$deadline" ]; then
            log "timed out waiting for $WAIT_UNIT"
            return 1
        fi
        sleep "$POLL_SECONDS"
    done

    require_sha256 "$BUILD_DIR/src/main.rs" "$EXPECTED_MAIN_SHA256"
    require_sha256 "$BUILD_DIR/Cargo.toml" "$EXPECTED_CARGO_TOML_SHA256"
    require_sha256 "$BUILD_DIR/Cargo.lock" "$EXPECTED_CARGO_LOCK_SHA256"
    require_sha256 "$CAPTURE_RUNNER" "$EXPECTED_RUNNER_SHA256"
    require_sha256 "$FLOOR_COLLECTOR" "$EXPECTED_COLLECTOR_SHA256"
    if pgrep -af '[c]argo build --release' >/dev/null; then
        log "another release build is already running"
        return 1
    fi

    log "building measurement v4 with one low-priority release job"
    (
        cd "$BUILD_DIR"
        PATH="/root/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH" \
            nice -n 10 cargo build --release --locked -j 1
    )
    "$TARGET_BINARY" convert-recorded-btc-books --help | grep -q -- '--condition-id'
    local install_tmp="${INSTALLED_BINARY}.tmp.$$"
    install -o root -g root -m 0755 "$TARGET_BINARY" "$install_tmp"
    mv "$install_tmp" "$INSTALLED_BINARY"
    log "installed measurement v4 sha256=$(sha256sum "$INSTALLED_BINARY" | awk '{print $1}')"

    if systemctl is-active --quiet "$COLLECTOR_UNIT"; then
        log "$COLLECTOR_UNIT is already active"
        return 1
    fi
    systemd-run \
        --unit="${COLLECTOR_UNIT%.service}" \
        --description="PolyMomentum sealed binary-complement 750-condition collector" \
        --uid=polymomentum \
        --gid=polymomentum \
        --property=MemoryMax=3G \
        --property=CPUWeight=10 \
        --property=Nice=10 \
        --property=RuntimeMaxSec=1123200 \
        --collect \
        "$FLOOR_COLLECTOR"
    systemctl is-active --quiet "$COLLECTOR_UNIT"
    log "started $COLLECTOR_UNIT"
}

main "$@"
