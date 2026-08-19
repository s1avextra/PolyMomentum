# partial_twap_lock_v2 — preregistration (2026-08-19)

Status: `PRE_REGISTERED_NO_LABELS_JOINED`
Predecessor: `partial_twap_lock_v1` (terminal `insufficient_support_data_blocked`,
registry entry #17; fresh outcomes never opened — this redesign is not
tuning-on-outcomes).
Contract: two-stage gate per `docs/wilson_gate_power_analysis_2026-08-17.md`.

## What changed vs v1, and why it is legitimate

1. **Data**: the sealed dataset now includes the frozen canary's captured
   books via `opportunity-table --distilled-input` — 57 hours / 1872 rows
   spanning 2026-08-08T16 → 2026-08-11T15 (v1 had 12 hours / 370 rows).
   Eleven candidate hours failed fail-closed with zero-event coverage
   (inter-segment capture pauses) and are excluded by that error, not by
   choice.
2. **Lock-strength floors recalibrated to the FEATURE distribution**:
   v1's floors {1.0, 2.0, 3.0} kept only 8% of rows because August
   realized vol (~8.8% annualized) makes sigma_tail large relative to
   typical partial-TWAP leads (median |lead| $0.60). The distribution of
   `lock_strength` is an outcome-free causal quantity; v2 floors
   {0.25, 0.5, 1.0} are set from its quartiles. No label influenced the
   choice.
3. **Chronological split by fixed calendar rule** (declared here, before
   any v2 label exists): older < 2026-08-10T00:00Z;
   recent_discovery < 2026-08-11T04:00Z; fresh_holdout = the newest
   captured span 2026-08-11T04:00Z → 15:59Z. The rescued 2026-08-18
   books and everything the new capture campaign records stay OUTSIDE
   this seal as future forward material.

## Frozen 54-policy grid

- decision time: 120 / 180 / 240 s (3)
- lock-strength floor `|lead| / sigma_tail` ≥ 0.25 / 0.5 / 1.0 (3)
- executable-ask cap on the lead-favoured side: 0.55 / 0.75 / 0.90 (3)
- minimum `twap_locked_fraction`: 0.6 / 0.8 (2)

Direction = sign(partial_twap_lead_usd); rows whose lead favours the
non-selected side are excluded (their book is not in the table) and
counted as lead_side_mismatch. Execution semantics: taker FOK, $5 stake,
current fee metadata. Feature allowlist identical to v1.

## Gates and budgets (unchanged contract)

- Cheap screen: older support ≥ 30 with positive point edge; recent
  support ≥ 100, point edge > 0.02, positive fee-aware payoff.
- Exact replay: ≤ 2 unique decision traces at 128 ms latency (requires
  the replay loader to accept distilled sources — engineering follow-up;
  until then a passing screen holds as `screen_passed_awaiting_replay`).
- Advancement: `wilson_lower(z95) − avg_break_even > 0` at replayed
  support; fresh gate +0.02 one-shot on the sealed fresh block.
- Label semantics: `twap_vs_open` over the 1-second checksum-verified
  Binance tape; era guards enforce the 2026-08-08 boundary. Official
  Chainlink parity remains a later gate.

## Context risk, stated up front

All 57 hours sit in the first 71 hours after the rule change — any edge
may be transient adaptation mispricing. A screen pass here is evidence of
"edge existed in the adaptation window", NOT "edge persists". The
persistence question is assigned to the new capture campaign's data
(Aug 19+) and must be answered by the fresh gate or a successor
registration before any live consideration.
