# Band canary → scale-1 plan (2026-08-25, operator: "go with scaling")

## Sizing rationale

Evidence base: fresh gate n=222 (WR 93.2%, point edge +0.109), fill
replay (median visible ask notional $10k on band rows, all 93 rows
supported ≥$500), economics doc ($5–100 stakes modeled). Kelly at
p=0.93, b≈0.235 is ~0.63 — absurdly aggressive for a measured-once
edge; we take ~1/6 Kelly:

- **Stake $25** (5× canary; 0.25% of median visible depth — fill
  dynamics essentially unchanged; the FOK worst-price cap still
  converts thin band-edge walks into non-entries).
- **Bankroll $250 fixed** (10% per trade), wallet funded to $250.
- **Exposure cap $50** (max 2 concurrent windows).
- **Brakes**: session floor 20% (−$50 = 2 same-session losses),
  cumulative restart-proof cap 24% (−$60), consecutive-losses 5.
  Worst-case runaway before hard halt ≈ $60 ≈ 2.4 stakes.
- Expected value at measured edge: ~57 events/day full coverage ×
  ~+$2.7/trade ≈ +$150/day gross before haircuts; realistic partial
  coverage 15–25 entries/day ≈ +$40–70/day.

## Activation checklist (after wallet funded to $250)

1. env: BANKROLL_USD=250, MAX_POSITION_PER_MARKET_USD=25,
   MAX_TOTAL_EXPOSURE_USD=50, CANDLE_LIVE_MAX_CUMULATIVE_LOSS_PCT=0.24,
   CANDLE_BREAKER_MAX_CONSECUTIVE_LOSSES=5,
   POLYMOMENTUM_PROMOTION_ARTIFACT=/opt/polymomentum/promotions/band_promotion_scale25.json
2. Artifact `band_promotion_scale25.json` (params hash 9ea105cf…,
   stake 25 — mechanism/band/decision unchanged from the frozen policy).
3. Preflight (validates budget ≥ floor at the new stake) → restart.
4. First-day watch: entry rate, fill parity vs quote, redemption cycle.

## Redemption sweeper (prerequisite, deployed with this plan)

`deploy/redeem_sweeper.sh` + `polymomentum-band-redeem.timer` (5 min):
claims resolved CTF positions (recipe verified against the on-chain
redemptions of our own positions: CTF.redeemPositions(USDC.e, 0x0,
cid, [1,2]) — tx 0x479f4817…) and wraps USDC.e payouts to pUSD via the
Onramp (selector 0x62355638, single-shot MAX approve, no zero-window).
Trade list sourced from the public data-api by wallet; balance-gated,
idempotent. Without it, winnings sit as CTF tokens and the wallet
starves at any stake.
