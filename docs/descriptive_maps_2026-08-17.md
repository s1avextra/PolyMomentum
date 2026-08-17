# Descriptive maps + meta-analysis of the August rejections — 2026-08-17

Status: `MEASUREMENT_ONLY_NO_TOMBSTONE_REOPENED`
Evidence: `deploy/promotions/evidence/strategy_registry/20260817_descriptive_maps.json`
Script: `scripts/analyze_descriptive_maps.py`
Companion: `docs/wilson_gate_power_analysis_2026-08-17.md`

Three cheap outcome-safe passes over the sealed 20-hour / 704-row dataset
(571 labeled older+recent rows; all 133 fresh labels remain physically
excluded; tables loaded strictly through the seal index, which excludes the
rejected June hours still present on disk).

## 1. Favorite-longshot calibration map — the headline result

Realized win rate vs fee-aware break-even by executable entry price
(signal-selected token, $5 stakes):

| Price bucket | n | WR | Wilson 95% | avg BE | point edge |
|---|---:|---:|---|---:|---:|
| **(0.00, 0.55]** | **151** | **0.642** | **[0.563, 0.714]** | **0.444** | **+0.198 — lower bound clears BE** |
| (0.55, 0.65] | 88 | 0.511 | [0.409, 0.613] | 0.622 | −0.111 |
| (0.65, 0.75] | 81 | 0.691 | [0.584, 0.781] | 0.719 | −0.027 |
| (0.75, 0.85] | 82 | 0.817 | [0.720, 0.886] | 0.818 | −0.001 |
| (0.85, 0.92] | 64 | 0.906 | [0.810, 0.956] | 0.896 | +0.010 |
| (0.92, 1.00] | 87 | 0.919 | [0.843, 0.961] | 0.968 | **−0.048 — upper bound below BE** |

Two structural facts:

- **The expensive-favorite bucket (ask > 0.92) is systematically priced
  AGAINST the taker buyer** — the fee formula alone guarantees it. Every
  August family (path, probability, liquidity, flow) capped asks at
  0.85–1.00 and therefore hunted mostly inside the flat-to-negative region.
  Four months of rejections have a structural component, not only a power
  component.
- **The cheap bucket (ask ≤ 0.55) carried a large positive edge** — but see
  the honesty check below before concluding anything.

### Temporal stability check (mandatory before excitement)

| Cheap bucket (≤0.55) | n | WR | Wilson lo | BE | point edge |
|---|---:|---:|---:|---:|---:|
| older (April 16) | 81 | 0.741 | 0.636 | 0.407 | **+0.334** |
| recent (July 17–25) | 70 | 0.529 | 0.413 | 0.488 | **+0.041** |

The edge decayed hard between April and July: the April slice is confirmed
at 95%, the July slice is positive but unconfirmed (Wilson lo 0.413 < BE
0.488). Stable across directions (up +0.194 / down +0.205) and decision
times (120s +0.217 / 180s +0.226 / 240s +0.145). Fee-aware PnL proxy at $5
stakes: +$446.51 over 151 entries — dominated by the April window.

**Correct read:** cheap-side entries selected by the causal signal are the
only region whose point edge stayed positive in BOTH chronological windows.
It is a decaying-or-regime-dependent lead, not a confirmed strategy. It is,
however, exactly what the power-corrected successor contract should target.

## 2. Ask-side complete-set scan — tombstone the idea

Over 685/704 rows with both books observable: **zero violations** of
`ask_up + fee + ask_dn + fee < 1`, raw or fee-aware, at any depth. Minimum
fee-aware cost 1.00122, p01 1.01146, median 1.03663. Complete-set entry
arbitrage does not exist at top-of-book on this seal. Family
`ask_side_complete_set_v1` should be recorded as rejected-by-measurement
without spending any replay budget.

## 3. Support summary

Win rate rises monotonically with later decision times (both directions,
~symmetric): 120s ≈ 0.69 → 240s ≈ 0.79. Consistent with the buffer
mechanics documented in the four-minute-rule evaluation.

## Meta-analysis: why five preregistered families died in two days

1. **Power:** the +0.02 Wilson gate needed ~950 fills for 80% power at true
   edge 0.05 and break-even 0.85; budgets allowed 20–65
   (`wilson_gate_power_analysis_2026-08-17`). No plausible edge could pass.
2. **Structure:** all five families expressed the same trade archetype —
   taker buy of a mostly-expensive token late in the window — and the
   calibration map shows that price region is flat-to-negative for the
   buyer after fees. The funnel was underpowered AND aimed at the wrong
   bucket simultaneously.
3. **Direction:** 8/8 exact-replay traces had positive point PnL (correlated,
   post-selection — a direction indicator, not a p-value). Combined with the
   cheap-bucket map, the sealed data suggests residual predictive value in
   the causal signal that fees erase at high prices and preserve at low
   prices.

## Priority decision (single source of truth)

1. **The binary-complement 750-condition forward canary owns the promotion
   path.** Nothing may compete with it for VPS capture capacity. Restarted
   2026-08-17 with restart-on-failure + healthcheck watchdog; 448/750.
2. **The v3 portfolio track continues on the dev Mac only** and its next
   registration must be the power-corrected successor
   (`wilson_gate_power_analysis` recommendation) re-aimed at the cheap
   bucket: entries with executable ask ≤ 0.55, discovery support ≥ 100
   fills, margin-0 z95 advancement, +0.02 at fresh. It must cite tombstones
   `external_causal_probability_dislocation_20260811` and this document.
3. **Multi-asset / multi-horizon generalization (Ф2) proceeds as
   engineering** in parallel — it changes what the funnel CAN express and
   requires no gate decision.
