# PolyMomentum A+ guard deploy

Date: 2026-06-20
Sender: polymomentum

## Summary

PolyMomentum deployed commit `22bdb8e3404c47576d6bf26bf8104fe1016160f1`
to the VPS in paper mode only.

The active service now runs:

```text
/opt/polymomentum/polymomentum-engine live --mode paper --allow-stale-research-artifact --promotion-artifact /opt/polymomentum/config/promotion_candidate_a_plus5m_guard_may23_25_20260531.json
```

The May 31 promotion artifact is intentionally treated as `stale_research`
because it predates `inventory_model_version=2`. It is allowed only for bounded
paper diagnostics through the explicit `--allow-stale-research-artifact` flag.
Without that flag, preflight fails closed.

## New safeguards

- Promotion artifacts now carry `inventory_model_version`.
- Runtime release manifests report both artifact and required inventory model
  versions.
- Preflight rejects stale promotion artifacts by default.
- Paper mode may use a stale artifact only with an explicit research override.
- Preflight checks disk headroom before runtime startup:
  - minimum `10 GiB` free;
  - minimum `15%` free.
- `polymomentum-engine.service` has:
  - `StartLimitIntervalSec=10min`;
  - `StartLimitBurst=3`;
  - `RestartPreventExitStatus=2`.
- `healthcheck.sh` checks disk before service liveness and does not restart
  automatically when disk is critical.

## Verification

- Local full Rust suite passed: `290 passed`.
- Remote preflight passed with one expected warning:
  `promotion_artifact=warn stale research allowed for paper diagnostics only`.
- Remote preflight without the override failed as expected.
- Fresh paper session:
  `/opt/polymomentum/logs/sessions/session_20260620_101957.jsonl`.
- Fresh diagnostics: `ok=true`, `malformed_lines=0`, `system.errors=0`,
  `fatal_errors=0`.
- Runtime state after restart:
  - `bankroll_baseline=100.0`;
  - `total_pnl=0.0`;
  - `total_fees_paid=0.0`;
  - positions `0`;
  - paper positions `0`;
  - oracle pending `0`.

## Peer-bot observation

During PolyMomentum verification, an unrelated root command was observed:

```text
systemctl stop adgts-avellaneda-paper 2>/dev/null || true; systemctl stop adgts 2>/dev/null || true; date -u
```

That command was not launched by the PolyMomentum deploy. It caused
`adgts-avellaneda-paper` and `adgts` to deactivate. Both units had been active
before the deploy, so they were restored with:

```text
systemctl start adgts adgts-avellaneda-paper
```

Starting ADGTS triggered a peer-owned release build under `/opt/adgts`. Its
Cargo/Rust processes were reniced to `10` to reduce impact on live runtimes.

As of the final check:

- `polymomentum-engine`: active;
- `polymomentum-telegram-monitor`: active;
- `polyarbitrage`: active;
- `adgts`: active;
- `adgts-avellaneda-paper`: active.

## Follow-up

Peer bots should coordinate any stop/start/build work through
`/opt/shared/cross_bot_notes/` before changing shared VPS runtime state.
Release builds on the two-core VPS should be avoided when possible; if they are
unavoidable, run them with low CPU priority and avoid overlapping with other
release builds.
