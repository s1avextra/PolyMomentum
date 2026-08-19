# cheap_side_twap_v1 — preregistration (2026-08-19)

Status: `PRE_REGISTERED_CANDIDATE_AWAITING_FRESH_GATE`
Discovery evidence: `deploy/promotions/evidence/strategy_registry/20260819_adaptation_persistence_study.json`
Contract: two-stage gate per `docs/wilson_gate_power_analysis_2026-08-17.md`

## Mechanism (frozen)

At the 240-second decision of a `btc-updown-5m` window, when BTC has moved
from the window open, buy the signal-side token as a taker IF its
executable ask is ≤ 0.55. Hold to expiry. Under TWAP resolution the
signal side's win probability is far above what cheap asks imply — the
market still prices close-vs-open reversal risk that the TWAP rule
largely eliminated.

- signal: sign(BTC@240s − BTC@open), no signal on exact tie;
- entry: taker FOK, $5 stake, ask ≤ 0.55, within 30s of the decision;
- no exits, no maker, no other filters. One parameter set, no grid.

## Discovery result (Aug 9–18, executed-trade reconstruction)

510 usable windows across 10 days × 6 fixed calendar hours; entries
proven executable by third-party BUY prints in the entry window:

- cheap bucket: n=85, WR 76.5%, avg break-even 27.2%,
  point edge +0.493, **Wilson edge +0.392**;
- positive point edge EVERY day (+0.22 … +0.66); no adaptation decay —
  days 7–10 are among the strongest;
- favorites (>0.75): flat-to-negative, consistent with fee drag —
  confirming the August families died hunting the wrong bucket.

Power context: at these effect sizes the margin-0 z95 gate needs ~10
fills and the +0.02 fresh gate ~15–20; n=85 clears both many times over.

## Known limitations, stated before the fresh gate

1. Entry prices are OTHER traders' executed BUYs — executability proven,
   but windows without any print in the 30s entry window (183 of 693)
   are unobservable to this reconstruction; live fill rate is the
   fresh-stage question.
2. Labels are Binance-1s TWAP proxy; official Chainlink parity remains a
   later gate (TWAP-vs-TWAP divergence is far tighter than close-vs-close).
3. Discovery days (Aug 9–18) are disjoint from the data that suggested
   the cheap bucket (Aug 8–10 seal) — a genuine out-of-sample
   confirmation — but both live in the first 10 post-change days.

## Fresh gate (one-shot, declared now)

- Source: the twap-era capture campaign (Aug 19+ VPS books) — untouched
  by every diagnostic to date. Collect until ≥ 40 selected entries exist
  (expected ~5–8 calendar days at the observed ~8.5 events/day over a
  6-hour sample → ~34/day full coverage).
- Labels: `twap_vs_open` over checksum-verified Binance 1s (Chainlink
  parity audit alongside).
- Gate: Wilson edge > +0.02 AND positive fee-aware payoff via
  `opportunity-fresh-gate` (consumed-marker one-shot).
- Exact L2 replay at measured latency on captured hours precedes the
  fresh read; `signal_to_attempt_rate` reported.
- Terminal rule: fail → tombstone, no retuning, no second fresh read.

## Economics sketch (to be formalized before any live decision)

~34 cheap-side events/day at full coverage; at $5 stakes and +0.49 point
edge ≈ +$1.7 expected/trade ≈ ~$55/day gross ceiling at study-period
frequency — before fill-rate, capacity, and decay haircuts. A real
capacity study is a promotion prerequisite.
