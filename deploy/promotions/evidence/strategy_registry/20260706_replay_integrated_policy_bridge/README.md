# 2026-07-06 Replay-Integrated Policy Bridge

This archive adds and tests the bridge from causal-policy search to actual
rolling-history replay verification.

No heavy replay was executed here. The new command writes dry-run
`rolling-history` manifests by default, using the same manifest generator that
executes replay when `--execute` is explicitly passed.

## Implementation

New command:

```text
polymomentum-engine strategy-builder causal-policy-replay-plan
```

It reads a `causal-policy-search` JSON artifact, selects the top candidates
that passed static search by default, extracts only runtime-supported
`harness_require_args` and `harness_deny_args`, and generates one
`rolling-history` manifest per selected candidate.

This closes the manual copy step between policy search and replay. Static
policy stats remain context only; the generated replay manifest is the next
required evidence before any promotion credit.

## Archived Plans

### `old_static_pass/`

Input:
`../20260705_low_exposure_policy_search_diagnostics/policy_search/polymomentum_latency128_low_exposure_policy_search_3fold_20260705.json`.

Result:

- `search_ok=true`.
- `selected_count=1`.
- Selected rank `1`: require `book_age=lte_100ms`, deny
  `book_imbalance=strong_positive`.
- Wrote a three-fold dry-run rolling-history manifest with those exact
  `--require-causal-tag` and `--deny-causal-tag` arguments.

### `mineligible2_default/`

Input:
`../20260706_policy_search_min_eligible_gate/polymomentum_low_exposure_policy_search_mineligible2_20260706.json`.

Result:

- `search_ok=false`.
- `selected_count=0`.
- Default replay planning selects no candidates, which is the intended
  fail-closed behavior after the eligible-report gate.

### `mineligible2_top_failed_diagnostic/`

Input: same stricter min-eligible artifact.

Result:

- Generated with `--include-failed` for diagnosis only.
- Selected rank `1`: require `book_age=lte_100ms`, deny `edge=gte_0.15`.
- Candidate remained failed in search context: `-4.60148` total PnL, worst
  report `-5.13834`.
- Wrote a dry-run replay manifest so the replacement-loss shape can be replayed
  explicitly if needed, without treating it as promotion evidence.

## Verdict

Replay-integrated selection is now mechanical: a candidate must pass search,
then be converted into a `rolling-history` replay manifest before it can receive
promotion credit. The current stricter low-exposure search has no replay-worthy
candidate, so A+ remains blocked on finding a policy that survives this bridge
and then executed replay.
