# Live Readiness - 2026-05-17

Scope: PolyMomentum on the Dublin VPS. No secrets are recorded here.

## Status

- Running release: `7e5861e52a2a2eda241a59fc39809605d4f58e51`.
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
  `/opt/polymomentum/logs/soak/soak_20260517T040527Z.json`
  - `ok=true`
  - `warnings=[]`
  - `replay_exit=0`
  - peers active

## Next Gate

Keep paper running with settlement alignment enabled until it records actual
paper order lifecycle events. Do not enable live until:

1. pUSD and both pUSD allowances are at least `1.00`.
2. Operator/account compliance for the selected venue is explicitly confirmed.
3. `VENUE`, `CLOB_V2_READY`, and
   `POLYMOMENTUM_LIVE_RECONCILIATION_READY` are flipped only after live canary
   reconciliation is prepared.
