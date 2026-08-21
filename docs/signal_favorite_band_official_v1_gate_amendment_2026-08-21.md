# signal_favorite_band_official_v1 — fresh-gate amendment (2026-08-21)

Declared: **2026-08-21 ~05:10 UTC, before fetching or observing ANY
post-freeze window data** (prints, books, or outcomes). The most recent
Polymarket/Binance data touched by any analysis in this project remains
the 2026-08-18T23:55Z window (verified against the evidence artifacts:
both study JSONs list days 2026-08-09…18; capacity rows max window_start
= 2026-08-18T23:50Z). The hypothesis freeze is commit `3a813bf`
(2026-08-19T08:13:29Z). The Aug 19 capture dry-run touched books only,
zero labels.

## What changes and why

The preregistration bound the one-shot fresh gate to capture-campaign L2
books. That binding conflated two distinct questions:

1. **Validity** — does the frozen mechanism hold on windows that did not
   exist when the hypothesis was formed? The constitutive requirement is
   *temporal disjointness from hypothesis formation*, not the data
   source. Windows after the freeze are fresh regardless of how they are
   observed.
2. **Execution realism** — would OUR taker orders have filled at our
   latency? This genuinely requires captured books.

The capture binding was an accident of history: at prereg time we
believed no other fresh-window instrument existed. Since then it is
established that the **same public instrument used for discovery**
(data-api executed prints + Gamma official resolutions + checksummed
Binance 1s opens) covers post-freeze windows identically. Running the
identical instrument on disjoint windows is methodologically *cleaner*
than switching instruments between discovery and gate. The capture-only
gate is also slower for no validity benefit (coverage yield ≈ 60%,
warm-up holes).

## Amended fresh-gate definition (one-shot, still)

- **Fresh range**: all `btc-updown-5m` windows with
  `window_start ∈ [2026-08-19T09:00:00Z, END]`, all 24 hours (the frozen
  mechanism has no hour filter). `END` = last window whose close is
  ≥ 2 h before consume time (resolution-settlement buffer). Start is the
  first whole hour after the freeze commit; no analysis has ever touched
  any window ≥ this boundary.
- **Instrument** (identical to discovery): signal =
  `sign(BTC@240s − BTC@open)` from Binance 1s opens (checksum-verified
  Vision dailies where published; Binance klines REST for the tail day —
  same venue, same series); entry = first executed BUY print on the
  SIGNAL token in `[decision, decision+30s]`; selected iff entry price
  ∈ (0.55, 0.92]; labels = official Gamma resolutions only; fee =
  `0.072·p·(1−p)` taker model.
- **Support**: ≥ 110 selected band entries (unchanged).
- **Gate statistic**: `wilson_lo(w, n) − avg(entry + fee)` > **+0.02**
  AND positive fee-aware expected payoff (unchanged).
- **Terminal rule**: fail → tombstone, no retuning, no second read
  (unchanged).

## Outcome-blindness protocol

Two phases in `scripts/fresh_gate_public_v1.py`:

- **Phase 1 (`--count-support`)**: fetches market identity (token ids,
  outcome names — never `outcomePrices`, the parser drops the field
  unread) and trade prints, caches them to disk, and reports the
  selected-entry count. Prints at 240–270 s precede resolution and carry
  no outcome information. The **stopping rule depends only on n**: phase
  2 may run at the first check where n ≥ 110. Conditioning a stopping
  time on covariates, never on outcomes, introduces no optional-stopping
  bias.
- **Phase 2 (`--consume`)**: writes the consumed marker
  (`logs/strategy-research/fresh_gate_public_v1.CONSUMED`, create-new
  semantics, refuses if present) **before** the first outcome fetch,
  then performs the single outcome read over the cached identity set and
  emits the verdict. There is no code path that reads outcomes without
  the marker in place.

## What the capture campaign is still for

Unchanged and still required **before any live sizing beyond the $5
canary stake**: exact L2 replay on capture-campaign books at measured
latency (fill-rate reality — third-party prints prove executability
existed, not that we would have been filled). Preliminary evidence from
the outcome-blind dry-run: 100 % of observed valid band-priced books
filled the $5 stake; zero band-priced-but-unfillable cases. The campaign
keeps running; its replay attaches to the promotion decision, not to
validity.

## Honesty note

This amendment is motivated by wall-clock (the public instrument reaches
support ≥ 110 days earlier than capture coverage), and is made strictly
before observing any post-freeze outcome. Residual risk it accepts:
executed-print entries may flatter our achievable fills — mitigated by
the dry-run evidence above and gated again by the capture replay before
sizing. If the gate fails, the tombstone applies to the candidate as a
whole; the capture books will NOT be used for a second validity read.
