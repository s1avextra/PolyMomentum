# Production loop - 2026-05-01

Scope: local diagnostics only. No VPS services were modified and no peer-bot
private directories were read or written.

Runtime artifacts were isolated under `/private/tmp/polymomentum-prodloop`.

## Paper diagnostic soak

Command shape:

```bash
POLYMOMENTUM_DATA_DIR=/private/tmp/polymomentum-prodloop/data \
POLYMOMENTUM_LOGS_DIR=/private/tmp/polymomentum-prodloop/logs \
SESSION_LOG_DIR=/private/tmp/polymomentum-prodloop/sessions \
STATE_DB_PATH=/private/tmp/polymomentum-prodloop/state/state.db \
KILL_SWITCH_PATH=/private/tmp/polymomentum-prodloop/KILL \
BANKROLL_USD=100 \
VENUE=paper_only \
target/debug/polymomentum-engine live --mode paper
```

Result:

- Session: `/private/tmp/polymomentum-prodloop/sessions/session_20260501_072429.jsonl`
- Summary: `/private/tmp/polymomentum-prodloop/sessions/summary_20260501_072429.json`
- Duration: about 1 minute.
- Feeds connected: Binance, Bybit, OKX, Binance alt, Bybit alt.
- Diagnostics events: 293.
- Signal evaluations: 143.
- Decisions requesting trade: 0.
- Orders/fills/rejections: 0 / 0 / 0.
- Fatal system errors: 0.
- Malformed JSONL lines: 0.
- Missing replay fields: 0.

Diagnostics command:

```bash
target/debug/polymomentum-engine diagnostics session \
  /private/tmp/polymomentum-prodloop/sessions/session_20260501_072429.jsonl
```

Result: `ok=true`.

Replay parity:

```bash
target/debug/polymomentum-engine validate-replay \
  /private/tmp/polymomentum-prodloop/sessions/session_20260501_072429.jsonl
```

Result: `total=143 mismatches=0 (0.00%)`.

## Backtest/sweep diagnostic

Command:

```bash
target/debug/polymomentum-engine sweep \
  --session /private/tmp/polymomentum-prodloop/sessions/session_20260501_072429.jsonl \
  --min-trades 0
```

Result:

- Sweep completed successfully.
- All variants reported 0 trades because the short paper session had no trade
  decisions and no resolutions.
- This proves parser compatibility for the current paper diagnostics and replay
  fields, but does not yet prove PnL parity on filled trades.

## Current parity status

Clean:

- Paper runtime starts and shuts down cleanly with local-only state paths.
- Session JSONL has current replay fields:
  `decision_trade`, `execution_attempted`, and `traded`.
- New diagnostics analyzer accepts the session.
- `validate-replay` shows 0 decision mismatches.
- Sweep/backtest parser accepts the session.

Still needs real evidence before live capital:

- A promoted artifact from a real PMXT v2 harness report.
- A longer paper run that actually produces order lifecycle events.
- A live dry run or micro-live run, only after compliance and venue gates pass,
  compared to paper with `diagnostics compare`.
