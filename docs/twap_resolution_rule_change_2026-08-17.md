# Polymarket switched candle resolution to Chainlink TWAP — detected 2026-08-17

Status: `RULE_CHANGE_DETECTED_OPERATOR_DECISION_REQUIRED`

## What changed

Current `btc-updown-5m` (and `eth/sol/xrp-updown-5m`, `btc/eth-updown-15m`)
markets resolve as:

> "Up" if the **time-weighted average price (TWAP)** of the asset, generated
> by Chainlink, of the time range specified in the title is **greater than or
> equal to the price at the beginning** of that range.

The entire PolyMomentum research program — settlement anchors, labels,
the binary-complement preregistration, every backtest label — models the
previous rule: **terminal price vs open**. TWAP-vs-open and close-vs-open
disagree whenever the intra-window path crosses the open asymmetrically.

## When it changed (evidence)

Per-session SETTLEMENT_DISAGREEMENT counts from the forward collector
(`binary-complement-block1-floor-*/session_summary.json`):

- Sessions 004–025 (Jul 18 → Aug 8 13:15 UTC): **0 disagreements** across
  every sealed group.
- Session 026 (first window 2026-08-08T16:20Z): **3/6 groups disagree**.
- Every subsequent session (027–047): persistent ~15–40% disagreement rate.

The switch happened around **2026-08-08 ~14:00–16:00 UTC**. This fully
explains the support stall at 448/750: since Aug 8 the engine's
close-vs-open resolution model fails settlement alignment on ~25% of
groups, and admissible-condition throughput collapsed accordingly. (The Aug
11 latency-gate exit then froze the collector entirely — separate issue,
fixed 2026-08-17 with restart-on-failure + watchdog.)

Also confirmed live in Gamma today: `eth/sol/xrp-updown-5m` families exist,
plus new `btc/eth-updown-15m` series (10192/10191) — the multi-asset,
multi-horizon surface for Ф2 is real and TWAP-resolved.

## Consequences

1. **The binary-complement canary (750-condition block) is contaminated
   from Aug 8 onward.** Conditions 449+ would mix two resolution regimes
   under a preregistration that models the old rule. Continuing to collect
   as-is spends capture on data the frozen scorer cannot honestly use.
2. **All future labels must be TWAP labels.** The Binance close-proxy AND
   the Chainlink close-vs-open model are both wrong for post-Aug-8 markets.
   The label pipeline needs a TWAP evaluator over the settlement tape.
3. **Historical seals stay valid for their own era** (pre-Aug-8 markets
   resolved close-vs-open; the 704-row seal's proxy labels match that era's
   rule). Tombstones stand.

## Operator decision required (recommendation prepared)

The preregistration allows terminal `data-quality-blocked` states; a silent
regime mix is exactly what it forbids. Recommended:

- **Freeze the current block at 448 pre-change conditions** with a
  rule-change block record (not a strategy failure — an external contract
  change). The collector should be stopped once the decision is made.
- **Register `binary_complement_twap_v2`**: same mechanism, TWAP-aware
  settlement alignment, fresh 750-condition floor under the new rule, with
  the blinded power amendment carried over.
- The restarted collector currently keeps recording; its segments remain
  raw-capture-valid either way (frames are rule-agnostic), so nothing is
  corrupted while the decision is pending — only alignment verdicts fail.

## The opportunity in the new rule

TWAP resolution makes late windows **more predictable, not less**: at
decision time t of window T, the final TWAP is already t/T determined by
observed prices (causally computable as a partial TWAP). At 240s of a 300s
window, 80% of the resolving quantity is known; the final minute
contributes only 1/5 of the average. When the partial-TWAP lead over the
open exceeds any plausible final-minute contribution, the outcome is
near-deterministic — and if the cheap side still prices close-vs-open
volatility, that is structural mispricing on the side the calibration map
already flagged as underpriced (`descriptive_maps_2026-08-17`).

Proposed first TWAP-native family: `partial_twap_lock_v1` —
outcome-free features: partial TWAP, lead-vs-open, remaining-weight bound,
executable ask of the locked side; preregistered under the power-corrected
two-stage gate (`wilson_gate_power_analysis_2026-08-17`): discovery
support ≥ 100 fills, margin-0 z95 advancement, +0.02 at fresh.
