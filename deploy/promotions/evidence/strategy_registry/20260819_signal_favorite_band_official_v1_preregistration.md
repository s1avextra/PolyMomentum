# signal_favorite_band_official_v1 — preregistration (2026-08-19)

Status: `PRE_REGISTERED_CANDIDATE_AWAITING_FRESH_GATE`
Discovery evidence:
- `20260819_adaptation_persistence_study.json` + `20260819_official_resolution_parity.json`
  (primary sample, hours 01/05/09/13/17/21 UTC, official-label recompute)
- `20260819_complement_and_band_disjoint_study.json`
  (independent confirmation, disjoint hours 03/07/11/15/19/23 UTC)
Contract: two-stage gate per `docs/wilson_gate_power_analysis_2026-08-17.md`

## Mechanism (frozen — single band, no grid)

At the 240-second decision of a `btc-updown-5m` window, when BTC has moved
from the window open, buy the SIGNAL-side token as a taker IF its
executable price is in **(0.55, 0.92]**. Hold to expiry.

Why it works under the CONFIRMED official semantics (trailing-60s stream
close vs open): the smoothed close makes final-minute reversals nearly
incapable of flipping the outcome, while band prices still carry reversal
premium; below 0.55 the market is efficient-to-adverse, above 0.92 the
crypto taker fee erases the residual edge. Labels here and everywhere
forward are OFFICIAL Gamma resolutions.

## Parameter provenance (honesty note)

The four price buckets predate this mechanism (first calibration map,
2026-08-17); the band is the union of the two middle buckets that clear
fee-adjusted break-even. No threshold search was run on either study
sample; the disjoint sample was fetched AFTER the band hypothesis was
fixed by the primary sample's official-label recompute.

## Discovery results (official labels, executed third-party entries)

| Sample | n | WR | avg BE | point edge | Wilson edge | days>0 |
|---|---:|---:|---:|---:|---:|---|
| Primary (01/05/…/21) | 132 | 93.9% | 82.1% | +0.118 | +0.022…+0.042 | 9/10 |
| Disjoint (03/07/…/23) | 152 | 91.4% | 80.8% | +0.106 | **+0.051** | 10/10 |

Killed alternatives on the same official labels: cheap signal side
(−0.10/−0.12 both samples), complement side at any price (negative or
Wilson-unconfirmed), extreme favorites (fee-erased). The falsified
whole-window-TWAP proxy that produced `cheap_side_twap_v1` is documented
in `20260819_official_twap_semantics_identification.json`.

## Fresh gate (one-shot, declared now)

- Source: the twap-era capture campaign (Aug 19+ books), untouched by
  every study above; entries via exact L2 replay at measured latency
  (taker FOK, $5 stake, band prices), labels via OFFICIAL resolutions.
- Support: ≥110 selected band entries (80% power for true edge ≈ 0.10 at
  the +0.02 margin per the power analysis; observed band frequency ≈ 22%
  of windows → expected ~6–10 calendar days of capture).
- Gate: Wilson edge > +0.02 AND positive fee-aware payoff via
  `opportunity-fresh-gate` (consumed marker; official-resolution labels).
- Terminal rule: fail → tombstone, no retuning, no second read.
- Promotion prerequisites beyond the gate: capacity/economics study,
  live fill-rate reality (third-party prints prove executability existed,
  not that WE would have been filled), Chainlink-vs-Binance near-tie risk
  quantified on fresh data.
