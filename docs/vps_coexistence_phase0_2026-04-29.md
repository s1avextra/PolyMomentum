# VPS coexistence phase 0 - 2026-04-29

This note records the bot-safe checks and guardrails added before replacing
the legacy PolyMomentum runtime on the shared VPS.

## Read-only VPS observations

Commands stayed limited to systemd state/resource metadata, `/opt/shared`
metadata, load average, and filesystem usage. I did not inspect
`/opt/polyarbitrage`, `/etc/polyarbitrage`, `/opt/adgts`, `/etc/adgts`, or
peer wallets/configs.

- `polymomentum-rust`: active; resource capped at `CPUQuota=80%`,
  `MemoryMax=512M`, `TasksMax=256`.
- `polymomentum-engine`: inactive.
- `polymomentum-healthcheck.timer`: inactive.
- `polyarbitrage`: active; resource capped at `CPUQuota=120%`,
  `MemoryMax=512M`, `TasksMax=128`.
- `adgts`: observed in `deactivating/stop-sigterm`; no PolyMomentum restart
  should be attempted while a peer service is in that state.
- `/opt/shared/pmxt_v2_cache` and `/opt/shared/cross_bot_notes` exist as
  `root:pmxt-data` setgid directories.
- `/opt/shared/pmxt_v2_distilled_candles` was missing.
- `pmxt-data` group includes `polymomentum` and `polyarbitrage`.
- Root filesystem was 89% used with 8.3G available.

## Guardrails added locally

- `polymomentum-engine preflight --mode ...` now fails closed for live mode
  unless venue/compliance/credentials/alerting checks pass.
- Runtime path preflight rejects PolyMomentum paths under peer-private
  directories.
- Session JSONL now starts with a release manifest event containing the git
  build identity and redacted config hash.
- `polymomentum-engine.service` now has `ExecStartPre`, `Nice=5`,
  `CPUQuota=80%`, `MemoryMax=512M`, `TasksMax=256`, and read-only `/opt/shared`
  access.
- `deploy/deploy.sh --enable-service` checks `adgts` and `polyarbitrage`
  ActiveState before restarting PolyMomentum and refuses to restart while a
  peer is `deactivating`.

No VPS services were changed by this phase.
