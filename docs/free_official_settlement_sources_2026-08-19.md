# Free official settlement sources — decision (2026-08-19)

The paid Chainlink Data Streams history API is NOT required. Two free
sources cover both halves of the official-settlement problem:

## 1. Labels: the venue's own resolutions (Gamma, free, authoritative)

Closed markets carry `outcomePrices` + `umaResolutionStatus: resolved` —
the OFFICIAL settlement result, per market, forever. For hold-to-expiry
strategies (including `cheap_side_twap_v1`) the label IS the resolution;
no price tape is needed at all. This is strictly stronger than any
reconstructed tape: it is the exact quantity the position pays out on.

- Verified live: `btc-updown-5m-1786795200` → outcomes ["Up","Down"],
  outcomePrices ["0","1"], resolved.
- Parity audit: `scripts/official_resolution_parity.py` measures the
  Binance-1s TWAP proxy against official resolutions across the whole
  adaptation study and recomputes candidate metrics under official labels.
- Evidence: `20260819_official_resolution_parity.json`.

## 2. Fresh tape: Polymarket RTDS relay (free, already recorded)

The venue relays the settlement stream over its public RTDS websocket
(`crypto_prices_chainlink`); the capture campaign already records it into
each segment's `chainlink_btcusd.csv` (`timestamp_ms,source,price,...`).
Every fresh window therefore has a free official-source tape for
diagnostics that DO need a path (partial-TWAP features, near-boundary
analysis) — recorded at capture time, no subscription.

## What remains for the paid API (explicitly deferred, not blocking)

Historical Chainlink tape for windows nobody recorded — only needed for
path-dependent research on pre-capture history. `chainlink-backfill` is
implemented and tested; if a Data Streams key ever appears, it works
day one. Nothing in the candidate's promotion path depends on it now:

| Gate | Source | Cost |
|---|---|---|
| Discovery labels | Binance-1s TWAP proxy + official-parity audit | free |
| Fresh-gate labels | official Gamma resolutions | free |
| Fresh tape (features) | RTDS capture | free |
| Exact replay books | capture campaign L2 | free |

## Post-audit addendum — semantics confirmed, proxy invalidated a candidate

The parity audit over 510 windows falsified the whole-window-average TWAP
formula (81.4% agreement) and with it the `cheap_side_twap_v1` discovery
evidence (rejected in the registry the same hour, before any fresh or live
exposure). Parity-ranking seven candidate formulas identified the de-facto
official semantics: **trailing-60s stream at close vs at open** — 93.5%
raw parity, 100% at |margin|>$20 (n=197); the near-tie residual is
Binance-vs-Chainlink source noise. Labelers (`join_twap_labels`,
`opportunity-fresh-gate`) now implement the confirmed formula via
`BTCHistory::trailing_twap`. Every future label for resolved markets uses
official Gamma resolutions directly; the stream formula serves features
and pre-resolution modeling only.
