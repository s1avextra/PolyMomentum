# PArbeiter bot handoff from PolyMomentum

Date: 2026-06-04

## Non-secret bot identity

PolyMomentum found a legacy Telegram bot attached through an old generic alert
webhook:

- Bot username: `PArbeiter_bot`
- Bot first name from Telegram `getMe`: `PolyDub`
- Bot id / token prefix: `8781846433`
- Observed chat id in the old webhook: `415683`

The full token and full webhook URL are intentionally not written here. The old
webhook value was a Telegram `sendMessage` endpoint and therefore contained the
bot token.

## How it was found

During the PMomentum migration, redacted process-env scans showed PolyMomentum
processes inheriting two alert paths:

- `TELEGRAM_BOT_TOKEN` resolved to `PMomentum_bot` (`8405424622`).
- `ALERT_WEBHOOK_URL` resolved to `PArbeiter_bot` (`8781846433`).

That meant PolyMomentum's dedicated Telegram monitor was already on
`PMomentum_bot`, but generic alert paths could still send PolyMomentum alerts
through `PArbeiter_bot`.

The visible symptom was a wrong-bot alert at 2026-06-04 13:09 Bangkok
(06:09 UTC on the Dublin VPS):

```text
PolyMomentum Rust stopped
wins=2 losses=1 pnl=$-1.02
```

That shutdown alert was emitted by the old running engine during restart. It
still had the stale `ALERT_WEBHOOK_URL` in memory even though the env file was
being cleaned.

## PolyMomentum cleanup already done

- Active `/etc/polymomentum/env` now has `ALERT_WEBHOOK_URL=`.
- All `/etc/polymomentum/env.bak*` files were sanitized so their
  `ALERT_WEBHOOK_URL` entries are empty.
- Live PolyMomentum process-env verification shows only the `PMomentum_bot`
  token and an empty `ALERT_WEBHOOK_URL`.
- `deploy/healthcheck.sh` now prefers dedicated PMomentum Telegram env and
  ignores Telegram `sendMessage` URLs supplied through the generic webhook.
- `rust_engine/src/monitoring/alerter.rs` now ignores Telegram `sendMessage`
  `ALERT_WEBHOOK_URL` values when a dedicated Telegram client is configured.
- Only PolyMomentum services were restarted for the cleanup; ADGTS and
  PolyArbitrage services were not changed.

Related note:
`2026-06-04_telegram_duplicate_resolution_from_polymomentum.md`

## Requested owner action

If `PArbeiter_bot` is still owned by another tenant/operator:

- Claim ownership in `/opt/shared/cross_bot_notes/` with a non-secret note.
- Confirm whether `PArbeiter_bot` should continue to exist.
- Rotate or revoke the token if this bot is no longer intentionally used.
- Do not restore old PolyMomentum env backups containing the former webhook.
- Do not attach PolyMomentum alerts/status to `PArbeiter_bot`; PMomentum is the
  authoritative PolyMomentum operator bot.

If a shared Telegram gateway is introduced later, avoid multiple services
long-polling the same bot token. Use one gateway/dispatcher and route bot
commands/events explicitly by tenant.

