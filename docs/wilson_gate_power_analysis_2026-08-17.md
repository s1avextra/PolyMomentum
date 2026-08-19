# Power analysis of the +0.02 Wilson advancement gate — 2026-08-17

Status: `MEASUREMENT_ONLY_NO_TOMBSTONE_REOPENED`
Evidence: `deploy/promotions/evidence/strategy_registry/20260817_wilson_gate_power_analysis.json`
Script: `scripts/analyze_wilson_gate_power.py` (stdlib-only, deterministic)

## Question

The v3 funnel advances a replayed trace only when
`wilson_lower(wins, fills; z=1.959964) − avg_break_even > 0.02` and PnL > 0
(`opportunity_replay.rs:781-789`). Five preregistered mechanism families were
terminally rejected by this gate in August 2026 while all eight exact-replay
traces showed positive point PnL. Was the gate ever statistically reachable
at the support the budgets allowed?

## Answer: no — not below true edge ≈ 0.15

Formula parity with the Rust implementation was asserted on all eight recorded
traces first (max drift 9e-06). Fills required for **80% power** at average
break-even 0.85 (typical late-window executable ask + fees):

| True edge | Gate today (m=+0.02, z95) | m=0, z95 | m=0, z90 |
|---:|---:|---:|---:|
| 0.02 | unreachable ≤5000 | 2450 | 1450 |
| 0.03 | unreachable ≤5000 | 1100 | 650 |
| 0.05 | **950** | 400 | 190 |
| 0.08 | 250 | 130 | 80 |
| 0.10 | 110 | 90 | 40 |
| 0.15 | 30 | 30 | 10 |

Family budgets allowed 20–65 fills per trace. At that support the production
gate has meaningful power only for true edges ≥ 0.15 — an edge so large it
would be visible without any of this machinery. **The funnel was configured
so that every plausible real edge fails.** The five August rejections are
therefore evidence about the gate, not only about the mechanisms.

## Research-only re-read of the August traces

Terminal decisions stand; nothing here reopens a tombstone.

| Trace | n | point edge | passes m0/z95 | passes m0/z90 | n for 80% power at observed edge |
|---|---:|---:|---|---|---:|
| path 120s ≤0.85 | 20 | +0.144 | no | **yes** | 100 |
| path 240s ≤1.00 dn | 23 | +0.125 | no | **yes** | 70 |
| path-exp 180s ≤0.90 | 28 | +0.105 | no | no | 190 |
| path-exp 120s ≤0.90 | 28 | +0.084 | no | no | 450 |
| **probability 180s ≤0.85** | 32 | +0.172 | **yes** | **yes** | 70 |
| probability 120s ≤0.90 | 37 | +0.077 | no | no | 600 |
| liquidity gap≥0.5 | 65 | +0.042 | no | no | 4000 |
| liquidity gap≥1.0 | 61 | +0.034 | no | no | — |

The causal-probability 180-second trace (32/32 fills, +$48.00) clears the
margin-0 z95 gate outright and would have needed only ~70 fills for a
properly powered test of its observed edge. The paired-liquidity traces are
honestly weak at any realistic support — those rejections were correct on
substance, not only on power.

Aggregate direction indicator: 8/8 positive point edges. The traces share
discovery hours and are post-screen selected, so this is NOT an independent
p-value — but combined with the power table it justifies re-testing the
strongest mechanism under a correctly sized contract.

## Preregistered recommendation (applies to FUTURE registrations only)

1. **Split the gate into two stages.** Discovery advancement (to the sealed
   fresh test) requires `wilson_lower(z95) − avg_break_even > 0` at
   preregistered support ≥ 100 fills, accumulated through a discovery-hour
   budget declared before labels are joined. The +0.02 margin moves to the
   fresh-holdout stage, where a passing candidate must clear it on the sealed
   fresh block — margin belongs where support is largest.
2. **Budgets must be power-derived.** Every future preregistration states the
   minimum detectable edge at 80% power for its declared fill budget; a
   family whose plausible edge is below that number is not launched.
3. **Successor registration for the probability mechanism.** A new family,
   `causal_probability_powered_v2`, may be registered citing this analysis:
   same frozen 54-policy grid semantics, discovery support target ≥ 100
   fills at 180 seconds, margin-0 z95 advancement, +0.02 at fresh. It must
   reference tombstone `external_causal_probability_dislocation_20260811`
   as its power-corrected predecessor — this is a decision-contract change,
   not a threshold retune of a rejected rule.
4. The binary-complement forward block is unaffected: its 750-condition floor
   received its own blinded power amendment on 2026-07-18 and remains the
   correctly-sized experiment in flight.
