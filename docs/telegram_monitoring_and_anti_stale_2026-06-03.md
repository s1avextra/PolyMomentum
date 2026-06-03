# Telegram Monitoring And Anti-Stale System

Date: 2026-06-03

## Goal

Make the deployed PolyMomentum strategy observable and operator-friendly without
turning chat into a risky control plane. Telegram is a read-only monitor:
status, freshness, preflight, wallet, replay, peer-service state, and alerts.
It cannot place orders, switch live mode, restart services, or change strategy
parameters.

## Telegram Operator Model

- Alert targets: Slack/webhook remains supported; Telegram is enabled when
  `TELEGRAM_BOT_TOKEN` and numeric `TELEGRAM_CHAT_ID` are set.
- Live preflight now accepts either Slack/webhook or Telegram when
  `ALERT_REQUIRED=1`.
- Runtime alerts go through the shared `Alerter`, so startup, stop, and circuit
  breaker messages can include Telegram inline buttons.
- Interactive monitor runs as its own service:
  `polymomentum-telegram-monitor.service`.
- The Telegram service uses long polling and is isolated from the trading
  service. If Telegram fails, only the monitor restarts.
- Chat callbacks are allowed only for configured numeric chat IDs.

Commands:

- `/status` - latest soak report, release hash, replay, freshness, wallet, peers.
- `/stale` - latest session strategy freshness verdict.
- `/preflight` - read-only paper preflight.
- `/wallet` - wallet live-readiness snapshot.
- `/help` - command list and safety note.

Inline buttons mirror the same read-only actions: Status, Freshness, Preflight,
Wallet.

## Anti-Stale Strategy Sentinel

The new `diagnostics staleness` command reads session JSONL and outputs a
machine-readable verdict:

- `ok`: no freshness warning.
- `watch`: not enough resolved outcomes, low trade rate, low recent win rate, or
  another warning that does not yet prove drift.
- `stale`: enough resolved outcomes exist, an adaptive window detects a
  statistically significant recent win-rate drop, and the recent win rate is
  below the configured floor.

The drift detector is ADWIN-inspired: it scans possible old/recent splits in the
resolved outcome stream and uses a Hoeffding-style bound to decide whether the
recent win rate has fallen by more than sampling noise allows. This is an alert
and re-scout trigger only; it never mutates live parameters.

## Promotion Discipline

When status is `stale`:

1. Keep the deployed artifact unchanged and fail closed.
2. Run the rolling-history strategy builder on a dev box, not the VPS.
3. Require feed-forward folds, robust promotion gates, replay parity, and
   freshness diagnostics before replacing the promotion artifact.
4. Deploy only after preflight plus soak report are green.

This follows the project rule that paper mode is not used for validation when
backtest/live-replay can prove the same behavior.

## Useful Commands

```bash
/opt/polymomentum/polymomentum-engine telegram probe --set-commands --send-status
/opt/polymomentum/polymomentum-engine telegram status --send
/opt/polymomentum/polymomentum-engine telegram poll --once
/opt/polymomentum/polymomentum-engine diagnostics staleness /opt/polymomentum/logs/sessions/session_YYYYMMDD_HHMMSS.jsonl
```

## Deployment Notes

- Install `deploy/polymomentum-telegram-monitor.service` to
  `/etc/systemd/system/`.
- Set `TELEGRAM_BOT_TOKEN`, numeric `TELEGRAM_CHAT_ID`, and optionally
  `TELEGRAM_ALLOWED_CHAT_IDS` in `/etc/polymomentum/env`.
- Start with `telegram probe --set-commands --send-status` before enabling the
  poller.
- Keep `polymomentum-engine.service` and the Telegram monitor as separate
  services so operator UI failures never affect trading.
