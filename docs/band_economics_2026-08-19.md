# Band economics & capacity — signal_favorite_band_official_v1 (2026-08-19)

Evidence: `20260819_band_capacity_study.json`
Method: executed third-party BUY prints at band prices (0.55, 0.92] on the
signal-side token within the 30s entry window, across all 284 band windows
of both discovery samples. Executed volume is a LOWER bound on available
depth (someone actually took it) and an UPPER bound on what we could take
without becoming the market.

## Measured liquidity (per band event)

| metric | value |
|---|---|
| events measured | 284 (10 days, 12/24 hours sampled) |
| events/day at full coverage | ~57 |
| executed band notional per event | p25 $434 · median $976 · p75 $1534 |
| events with zero band volume | 0 |

## Gross daily model (point edge +0.10, BEFORE haircuts)

| stake | avg fillable | full-depth events | gross $/day |
|---:|---:|---:|---:|
| $5 | $4.99 | 99.6% | ~$28 |
| $20 | $19.88 | 98.9% | ~$113 |
| $50 | $49.24 | 96.5% | ~$280 |
| $100 | $96.78 | 94.4% | ~$550 |

## Haircuts NOT applied (kept explicit)

1. Our taker order competes with the measured prints and adds demand;
   realized fill rate is unproven until exact-replay + live evidence.
2. Edge decay once we participate (and as others discover the band).
3. VPS latency vs print timing (202 ms floor vs 30 s window — small).
4. Bankroll reality: the wallet holds $6; stakes beyond $5 need funding —
   an operator decision that only makes sense AFTER the fresh gate.

## Verdict

Even under a 60–70% combined haircut, $20–50 stakes clear the venture's
running costs by an order of magnitude. The economics prerequisite for
promotion is satisfied at the analysis level; what remains empirical is
fill rate (fresh gate exact replay) and post-entry decay (live canary
with $5 stakes, if the gate passes).
