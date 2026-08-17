# partial_twap_lock_v1 — preregistration (2026-08-17)

Status: `PRE_REGISTERED_NO_LABELS_JOINED`
Contract: two-stage gate per `docs/wilson_gate_power_analysis_2026-08-17.md`
Era: TWAP resolution only (`docs/twap_resolution_rule_change_2026-08-17.md`)

## Causal thesis

Post-2026-08-08 candle markets resolve on the window TWAP vs the open. At a
decision `t` seconds into a `T`-second window, the final TWAP is already
`t/T` determined by observed prices — computable causally as
`partial_twap_approx` (checkpoint step-function, feature semantics v2). When
the locked lead `|partial_twap_lead_usd| * t/T` exceeds any plausible
contribution of the remaining `(T-t)/T` weight, the outcome is
near-deterministic. If books still price close-vs-open volatility (a much
noisier resolving variable), the side favoured by the locked lead is
systematically underpriced — most plausibly on the cheap side the
calibration map already flagged (`docs/descriptive_maps_2026-08-17.md`).

## Frozen feature allowlist

`partial_twap_approx`, `partial_twap_lead_usd`, `twap_locked_fraction`,
`causal_volatility`, `btc_open`, `remaining_seconds`, `best_ask`, `spread`,
`stake_fully_executable`, `fee_aware_break_even_probability`. Forbidden:
every path/move/pressure/model-probability field, all labels, all PnL.

## Frozen 54-policy grid

- decision time: 120 / 180 / 240 s (3)
- lock-strength floor `|lead| / sigma_tail` ≥ 1.0 / 2.0 / 3.0 (3), where
  `sigma_tail = btc_open * causal_volatility * sqrt(remaining_seconds / 31_536_000)`
- executable-ask cap on the lead-favoured side: 0.55 / 0.75 / 0.90 (3)
- minimum `twap_locked_fraction`: 0.6 / 0.8 (2)

Direction is the sign of `partial_twap_lead_usd`. Execution: taker FOK, $5
stake, 128 ms latency, current fee metadata. 3×3×3×2 = 54 policies.

## Data plan (declared before any label exists)

- Source hours: PMXT v2 archive hours strictly after 2026-08-08T16:00Z.
- Signals: compiled from checksummed Binance 1s spot tape (existing
  cross-venue adapter); market catalog via `--family btc-5m`.
- Labels: `opportunity-labels --resolution-rule twap_vs_open` over the same
  tape (era guards enforce the boundary).
- Chronological split: older = Aug 9–10, recent_discovery = newest resolved
  minus 2 days, fresh_holdout = newest fully resolved day, selected by
  calendar rule before any label is joined.

## Budgets and gates

- Discovery-hour budget: ≤ 40 hours total, expandable ONCE by a
  preregistered calendar rule; support target ≥ 100 fills on the
  best-supported decision time.
- Cheap-screen gates: older support ≥ 30 with positive point edge; recent
  support ≥ 100 fills equivalent, point edge > 0.02, positive fee-aware
  payoff.
- Exact replay: ≤ 2 unique decision traces.
- Advancement (discovery): `wilson_lower(z95) − avg_break_even > 0` at the
  replayed support. Terminal reject otherwise; `more_evidence_required`
  cannot extend the budget beyond the one declared expansion.
- Fresh gate (one-shot, sealed): Wilson edge > +0.02 on the fresh block.
- Power context: at break-even 0.55 (cheap-side cap) the margin-0 z95 gate
  at 100 fills has 80% power for true edge ≈ 0.10; at break-even 0.85 it
  requires edge ≈ 0.13 — the grid's ask caps make the powered region
  reachable, unlike every August family.

## Tombstone linkage

Successor-in-spirit to `external_causal_probability_dislocation_20260811`
(power-corrected contract; different feature basis — no BS model, no path
predicates) and informed by `late_window_opportunity_policy_family_20260811`.
This registration changes the decision contract and the resolving-variable
model; it does not retune a rejected rule's thresholds.
