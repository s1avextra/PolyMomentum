# Live Readiness - 2026-05-17

Scope: PolyMomentum on the Dublin VPS. No secrets are recorded here.

## Status

- Running release: `b89558ba3e60188e7e7556f2b373c863db5dfcb9`.
- Mode: `paper`.
- Venue: `paper_only`.
- Settlement alignment: promoted to `CANDLE_SETTLEMENT_ALIGNMENT_READY=true`
  after paper diagnostics showed zero actionable oracle disagreements.
- Latest paper preflight: `ok=true`.
- Latest live preflight: still fails closed, as intended.

## Wallet

- Address: `0xe0ab9972e6ac14c29c06699fb0096a83f2a931ba`
- pUSD: `0.88363`
- CTF Exchange V2 pUSD allowance: `0.88363`
- Neg Risk CTF Exchange V2 pUSD allowance: `0.0`
- POL: `5.2881`
- Live-ready wallet: `false`

The conversion is partially complete but below the bot's live minimum. Live
requires pUSD `>=1.00` and both CLOB V2 pUSD allowances `>=1.00`.

## Evidence

- Long paper diagnostics before the gate flip:
  - `total_events=247705`
  - `oracle.checks=91`
  - `actionable_disagreements=0`
  - `below_floor_disagreements=1`
  - `system.errors=0`
  - `fatal_errors=0`
  - `validate-replay: total=118416 mismatches=0`
- Post-flip paper diagnostics:
  - `settlement_alignment_ready=true`
  - `warnings=[]`
  - `validate-replay: total=211 mismatches=0`
  - peer services active
- Latest manual soak report:
  `/opt/polymomentum/logs/soak/soak_20260517T051309Z.json`
  - `ok=true`
  - `orders.placed=1`
  - `orders.filled=1`
  - `orders.rejected=0`
  - `resolutions.resolved=1`
  - `resolutions.wins=1`
  - `resolutions.total_pnl=5.8366`
  - `orders.max_submit_latency_ms=0.0`
  - `system.avg_cycle_ms=0.8999`
  - `system.max_cycle_ms=8.942`
  - `system.max_price_staleness_ms=960.0`
  - `replay_exit=0`
  - peers active
- Fresh release lifecycle evidence:
  - session: `/opt/polymomentum/logs/sessions/session_20260517_045409.jsonl`
  - decision: `2026-05-17T05:07:33.337082386Z`
  - paper order placed/acked: `2026-05-17T05:07:33.337322950Z`
  - paper order filled: `2026-05-17T05:07:33.337343454Z`
  - resolution: `2026-05-17T05:10:00.295677900Z`
  - `validate-replay: total=1262 mismatches=0`
- Configured live dry preflight:
  - credentials present
  - alerting configured
  - settlement alignment ready
  - still blocked by `VENUE=paper_only`, `CLOB_V2_READY=0`,
    `POLYMOMENTUM_LIVE_RECONCILIATION_READY=0`, and wallet readiness.
- Pre-canary sizing guard:
  - live wallet preflight now requires pUSD and both V2 allowances to cover the
    configured worst-case first live order and the configured minimum order-size
    floor, not just the old `$1.00` floor.
  - with the current `BANKROLL_USD=100`, `CANDLE_POSITION_PCT=0.10`,
    `CANDLE_VOL_EXTREME_MULTIPLIER=2.0`, and
    `MAX_POSITION_PER_MARKET_USD=20`, the live wallet budget requirement is
    `$20.00`.
  - a literal `$1` canary is below the practical CLOB minimum-size floor for
    many candle prices and should not be used as the first real order.
  - for a deliberate minimum-size canary, use approximately
    `BANKROLL_USD=45`, `MAX_POSITION_PER_MARKET_USD=5`, and pUSD/allowances
    `>=5.00`; this keeps the first order capped near `$5` while still allowing
    at least 5 shares at prices up to `0.90`.

## Next Gate

Keep paper running with settlement alignment enabled and do not enable live
until:

1. pUSD and both pUSD allowances are at least `1.00`.
2. Operator/account compliance for the selected venue is explicitly confirmed.
3. `VENUE`, `CLOB_V2_READY`, and
   `POLYMOMENTUM_LIVE_RECONCILIATION_READY` are flipped only after live canary
   reconciliation is prepared.
