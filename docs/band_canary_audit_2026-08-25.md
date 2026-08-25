# Band canary: complete pre-restart audit and design revision (2026-08-25)

Trigger: three production bugs found serially on launch night (systemd
namespace dir; venue-timestamp poisoning of the book map; venue clock
skew rejecting the freshest books as "future"). Operator directive:
audit the whole path in ONE pass so the next restart runs flawlessly,
yields all measurable data in one go, and risks no extra money.

Method: five independent audit lenses over the live path (cross-clock,
silent-drops, risk-invariants, observability, config-parity), 73
findings, triaged and fixed in one batch. Test suite: 625 green.

## Critical findings (both fixed)

1. **Oracle PnL replay on restart** (`pipeline.rs` oracle loop). PnL
   realization was not atomic with oracle_pending removal and the
   `pnl_recorded` idempotency flag was never set in production. A breaker
   trip (the NORMAL halt-on-loss path!) aborts the oracle task between
   realization and the batched removal-persist; the restart then replays
   the same resolution — double-counting PnL in the bankroll, breaker,
   and the cross-restart loss ledger. Fixed: `pnl_recorded=true` is set
   and persisted immediately after realization, before the trip check,
   and `oracle_pnl()` refuses to re-realize a recorded entry.

2. **Chainlink RTDS timestamp poisoning** (`exchange.rs` →
   `price_state.rs::update_reference_at`). The signal feed the band
   trades on had NO timestamp clamp: one mis-scaled venue frame becomes
   the monotonic high-water mark, every later legitimate update is
   silently dropped, `btc` goes 0, and the canary idles forever with no
   alert — the exact class of launch-night bug #2, unfixed on the more
   important feed. Fixed: implausible timestamps clamp to local now
   before the monotonic comparison (mirrors
   `polymarket_ws::clamp_timestamp_us_to_local`).

## High-severity fixes in this batch

- **Kill switch during feed outage**: scan_loop checked the kill switch
  AFTER the `btc<=0` early-continue, so KILL_BAND was inert during any
  RTDS outage. The check now runs first, every cycle.
- **btc<=0 stall visibility**: a dead decision feed now emits a
  rate-limited (1/min) `decision_feed_unavailable` error record instead
  of silently sleeping.
- **Window burn on transient FOK reject**: a definitive venue reject
  (book moved in flight) no longer permanently consumes the window — the
  traded-set entry is released so the band can retry within its
  240–270s entry window. Permanent rejects (balance/allowance) still
  trip the breaker; ambiguous submits still lock recovery and stay
  burned (the order may live at the venue).
- **Truthful execution telemetry**: `execute_trade` now returns whether
  an order was actually handed to the venue; internal skips surface as
  `band_execute_skipped`, ambiguous submits as `live_submit_ambiguous`
  records, band evaluation errors as `band_evaluation` error records,
  and a reconciliation loop stuck ≥30s as `live_recovery_stuck`.
- **Restart-proof halt-on-first-loss**: a live restart clears breaker
  session state, so the 20% session floor alone was not restart-proof.
  `CANDLE_LIVE_MAX_CUMULATIVE_LOSS_PCT=0.20` now arms the persisted
  cross-restart ledger: −20% of initial bankroll (≈−$4.14) trips on any
  single $5 loss in any process lifetime.
- **Unit hardening**: `/tmp/polymomentum` removed from ReadWritePaths
  (reboot bomb: 226/NAMESPACE on every boot), StartLimitIntervalSec=600
  + StartLimitBurst=5 (crash loops become clean stops),
  RestartPreventExitStatus=2 (preflight failures do not restart-loop);
  healthcheck.sh now alerts on band-canary failed state and restart
  loops; explicit STATE_DB_PATH/SESSION_LOG_DIR immune to base-env
  overrides.
- **Book map eviction**: books for untracked tokens are evicted on
  refresh (the map grew ~26 tokens/hour and is cloned every cycle).

## All measurable data in one go

New session-log records make one canary run a complete offline dataset:

- `band_skip_detail` (once per window+reason): the numbers every skip
  used to discard — out-of-range verdicts carry vwap/worst/shares/side/
  bound(low|high)/btc/open; quote-unavailable decomposes book-absent vs
  stale vs thin-asks vs budget-below-min-shares; no-capital carries the
  full sizing chain; open-unavailable carries open_ts and price basis.
- Entry evaluations carry the executable quote (vwap/worst/shares), so
  fill records join to their decision-time quote (slippage derivable).
- `contract_map` (per refresh): cid → window end/tokens, so skip-only
  windows can be placed in time offline without re-querying Gamma.
- `ws_health` (~2 min): intake counters + book freshness.
- `wallet` (~5 min): on-chain pUSD/USDC.e/POL timeline.
- `decision_feed_unavailable`, `live_submit_ambiguous`,
  `live_recovery_stuck`, `band_evaluation`, `band_execute_skipped`.

## Risk envelope (unchanged, re-verified)

One $5 FOK taker position at a time (exposure caps), worst-case loss
per trade ≈ $5.36, halt on first realized loss (session floor AND now
the restart-proof ledger), kill switch effective every cycle including
feed outages, maker paths unreachable under the band artifact.

## Deferred (documented, not blocking)

Maker-path defects (timeout sweep type mismatch, multi-fill dedupe drop)
— unreachable under taker-only band; structural one-position invariant
(dollar caps suffice at $5/$5); user-ws timestamp clamp (lower impact:
order events, monotonicity-guarded per order, and reconciliation is
REST-backed); OrderFilled slippage field (derivable offline from the
quote now carried by the entry evaluation); Telegram operator-monitor
service still points at the legacy engine paths (its Stop button does
not stop the canary — use the kill switch or systemctl; in-process
alerter covers breaker/error alerts).

## Ops checklist before the final restart

1. `ls -la /opt/polymomentum/.env` — a stale Python-era dotenv can
   silently override any env not set by the unit files; delete/empty it.
2. State db sanity: no stale paper_positions/oracle_pending rows in
   `/opt/polymomentum/logs/band-canary/candle/state.db`; bankroll
   baseline matches the wallet.
3. Telegram probe: confirm the canary process's own alerter delivers.
4. `rsync` env+unit, `systemctl daemon-reload`, restart, verify
   `books_fresh` > 0 and positive `active_ages` in the cycle log.
