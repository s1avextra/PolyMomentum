# Telegram duplicate resolution from PolyMomentum

Date: 2026-06-04

## What was found

PolyMomentum's live VPS processes were correctly configured with the dedicated
`TELEGRAM_BOT_TOKEN` for `PMomentum_bot`, but they also inherited the legacy
`ALERT_WEBHOOK_URL`. That webhook pointed at a Telegram `sendMessage` endpoint
for `PArbeiter_bot`, so engine/healthcheck alert paths could duplicate
PolyMomentum data into the old bot.

Process-level verification after the fix showed:

- `/opt/polymomentum/polymomentum-engine live ...` inherits an empty
  `ALERT_WEBHOOK_URL` and the `PMomentum_bot` Telegram token.
- `/opt/polymomentum/polymomentum-engine telegram poll` inherits an empty
  `ALERT_WEBHOOK_URL` and the `PMomentum_bot` Telegram token.
- ADGTS and PolyArbitrage services were not restarted or changed.

## Fix applied

- Backed up `/etc/polymomentum/env` on the VPS with a timestamped
  `.remove_parbeiter_webhook` suffix.
- Set `ALERT_WEBHOOK_URL=` in `/etc/polymomentum/env`.
- Restarted only:
  - `polymomentum-engine.service`
  - `polymomentum-telegram-monitor.service`
- Installed an updated `/opt/polymomentum/healthcheck.sh` that sends health
  alerts through the dedicated PMomentum Telegram env when available and ignores
  Telegram `sendMessage` URLs supplied through the generic webhook variable.

## Code guard

`rust_engine/src/monitoring/alerter.rs` now ignores a Telegram `sendMessage`
`ALERT_WEBHOOK_URL` when the dedicated Telegram client is configured. Slack or
other non-Telegram webhooks still work as generic webhooks.

This prevents a future stale `ALERT_WEBHOOK_URL` from silently attaching
PolyMomentum alerts to `PArbeiter_bot` or any other legacy Telegram bot.

