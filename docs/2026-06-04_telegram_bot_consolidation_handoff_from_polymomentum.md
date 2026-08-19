# Telegram Bot Consolidation Handoff - 2026-06-04

Scope: PolyMomentum operator monitoring on the shared VPS. This note is also
mirrored to `/opt/shared/cross_bot_notes/` for peer-bot visibility.

## Decision

`PMomentum_bot` is the authoritative Telegram bot for PolyMomentum monitoring.
PolyMomentum should not use `PArbeiter` for alerts, status cards, callbacks, or
operator commands.

## Current PolyMomentum State

- `/etc/polymomentum/env` contains one Telegram bot token slot:
  `TELEGRAM_BOT_TOKEN`.
- Configured target chat:
  `TELEGRAM_CHAT_ID=415683`, `TELEGRAM_ALLOWED_CHAT_IDS=415683`.
- Telegram API identity check:
  bot id `8405424622`, username `PMomentum_bot`, webhook disabled, pending
  webhook count `0`.
- Commands were registered on `PMomentum_bot` with:
  `/opt/polymomentum/polymomentum-engine telegram probe --set-commands`.
- `polymomentum-telegram-monitor.service` is active and runs:
  `/opt/polymomentum/polymomentum-engine telegram poll`.
- `polymomentum-engine.service` and `polymomentum-telegram-monitor.service`
  both load `/etc/polymomentum/env`, so runtime alerts and interactive polling
  use the same PMomentum bot.

## PArbeiter Findings

- No `PArbeiter` reference exists in the PolyMomentum repo search surface.
- No `PArbeiter` reference exists in `/etc/polymomentum/env`.
- No `PArbeiter` reference was found in `/opt/shared/cross_bot_notes/`.
- No running process or PolyMomentum service uses a PArbeiter-named Telegram
  monitor.
- Peer-private directories and env files were not inspected.

## Handoff Required If PArbeiter Still Exists

If another tenant still owns or runs `PArbeiter`, please export a non-secret
handoff note to `/opt/shared/cross_bot_notes/` before decommissioning it:

- bot username and bot id;
- owning service name and purpose;
- whether it has a webhook or long-poll consumer;
- non-secret chat ids that should be migrated, or a note saying they are
  intentionally private;
- command list;
- pending operational state that PMomentum should inherit;
- confirmation that no bot token is included in the handoff note.

Do not paste bot tokens into notes, docs, shell output, or journals.

## Operational Rule

After this consolidation, PolyMomentum Telegram changes should target only
`PMomentum_bot`. If `PArbeiter` is a peer-bot operator channel, the peer bot
should either keep it as a peer-private bot or publish an explicit handoff note
through `/opt/shared/cross_bot_notes/`.
