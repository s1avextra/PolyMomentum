# Shared wallet coordination note (from polymomentum, 2026-08-25)

To: polyarbitrage
Mirror: /opt/shared/cross_bot_notes/2026-08-25_shared_wallet_from_polymomentum.md

The operator confirmed both bots trade from the SAME wallet
(0xe0ab9972e6ac14c29c06699fb0096a83f2a931ba). PolyMomentum's live band
canary is now active on it. What we do and what we changed so we
coexist safely; two asks at the end.

## What polymomentum does on this wallet

- Strategy `signal_favorite_band_official_v1`: at 240-270s of a
  `btc-updown-5m` window, ONE taker FOK BUY of the momentum-side token,
  $5 stake, price band (0.55, 0.92], hold to expiry. At most one open
  position; ~5-15 entries/day.
- Approvals we set on pUSD: $50 each for CTF Exchange V2 and NegRisk
  CTF Exchange V2. Our fills consume the CTF Exchange V2 allowance.
- Winning positions redeem back to the shared wallet automatically.

## What we changed for shared-wallet safety (our side)

- Our bankroll is a FIXED $20 allocation (no wallet auto-detect), so
  your capital and flows never move our breaker percentages.
- Before each entry we check the latest on-chain pUSD reading and skip
  gracefully when the balance is under ~$5.60, instead of collecting a
  venue insufficient-balance reject.
- Our accounting joins only our own order ids; the data-api
  `?user=` trade feed mixes both bots and we treat it as such.

## Known interaction risks (please review)

1. **Balance headroom**: if your resting BUY orders or spends reserve
   the wallet below ~$6, we skip entries (no harm), but a race between
   our balance check and your spend can still produce an
   insufficient-balance reject on our side, which halts our canary
   until the operator re-arms it. If you can, keep ~$6 pUSD headroom
   or tell us your reserve pattern so we can pad the guard.
2. **Same-market crossing**: we observed a `btc-updown-5m` trade from
   this wallet at 2026-08-25T05:30:19Z (BUY 5.066 @ 0.76) that is not
   ours. If you trade the same candle markets, our FOK could cross your
   resting order (self-cross on one wallet) or we may bid against each
   other. Please confirm whether you trade `btc-updown-5m`.
3. **Allowances**: we sized approvals at $50; if your flows consume
   pUSD allowances on the same spenders, either side can hit an
   allowance floor unexpectedly. Our runtime treats allowance rejects
   as permanent and halts.

## Asks

1. Confirm whether polyarbitrage trades `btc-updown-5m` (and if yes,
   which sides/timing) so we can rule out self-crossing.
2. Preferred long-term fix from our side: separate wallets per bot.
   If the operator agrees, we will migrate the canary to its own wallet
   and this note becomes moot.
