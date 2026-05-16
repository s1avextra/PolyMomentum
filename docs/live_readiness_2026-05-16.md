# Live Readiness - 2026-05-16

Scope: PolyMomentum on the Dublin VPS using the international Polymarket CLOB.
No secrets are recorded here.

## Credential and Connectivity Status

- Local `.env`: required credential variables are present by shape only.
- VPS `/etc/polymomentum/env`: required credential variables are present by
  shape only.
- CLOB auth/heartbeat: read-only authenticated checks pass.
- Open orders: authenticated CLOB query returned zero open orders.
- Running services: `polymomentum-engine`, `adgts`,
  `adgts-avellaneda-paper`, `polyarbitrage`, and
  `polyarbitrage-collector` are active.

## Current Wallet Status

- Address: `0xe0ab9972e6ac14c29c06699fb0096a83f2a931ba`
- POL: `5.375899194625654`
- USDC.e: `6.03363`
- pUSD: `0.0`
- pUSD allowance to CTF Exchange V2: `0.0`
- pUSD allowance to Neg Risk CTF Exchange V2: `0.0`
- Live-ready wallet: `false`

Interpretation: gas is sufficient, but live trading is blocked until USDC.e is
converted/wrapped into pUSD and pUSD allowances are granted to both CLOB V2
exchange contracts.

## Preflight Status

Paper preflight passes in `paper_only` mode. Live preflight correctly fails
closed on these gates:

- `VENUE=paper_only`
- `CLOB_V2_READY=0`
- `POLYMOMENTUM_LIVE_RECONCILIATION_READY=0`
- `CANDLE_SETTLEMENT_ALIGNMENT_READY=false`
- pUSD balance and both pUSD allowances are zero

## A+ Live Plan

1. Keep paper running while wallet funding is fixed.
   Verify: paper preflight remains green and peer services stay active.
2. Convert/wrap a small canary amount of USDC.e into pUSD.
   Verify: `wallet --json` reports `pUSD>=1.00`.
3. Approve pUSD for CTF Exchange V2 and Neg Risk CTF Exchange V2.
   Verify: both pUSD allowances report at least the canary bankroll.
4. Run one fresh diagnostics window after the deployed alerting fix.
   Verify: paper diagnostics, replay validation, CLOB heartbeat, and service
   health are all green.
5. Flip only the compliance-approved live gates:
   `VENUE=polymarket_international`,
   `OPERATOR_COUNTRY=<approved_non_us_operator_country>`,
   `POLYMOMENTUM_VENUE_COMPLIANCE_OK=1`, `POLYMARKET_US_API_ENABLED=0`,
   `CLOB_V2_READY=1`, `POLYMOMENTUM_LIVE_RECONCILIATION_READY=1`, and
   `CANDLE_SETTLEMENT_ALIGNMENT_READY=true`. The country value must reflect the
   approved operator/account jurisdiction, not the VPS location.
   Verify: live preflight passes before any service restart into live mode.
6. Start with a minimum-size canary.
   Verify: every intent has an accepted order or explicit rejection, every
   accepted order reconciles through user-channel/REST evidence, and alerts are
   delivered.

## Hold Conditions

Do not go live if any peer service is inactive, any replay mismatch appears,
the wallet lacks pUSD/allowance, alerting is not active, or operator/account
compliance for the international venue is not explicitly confirmed.
