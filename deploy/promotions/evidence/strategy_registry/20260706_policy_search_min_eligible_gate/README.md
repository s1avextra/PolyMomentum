# 2026-07-06 Policy Search Eligible-Report Gate

This archive evaluates the two next A+ paths from the July 5 low-exposure
diagnostics:

- replay-integrated policy generation and stricter policy-search credit;
- a new signal family.

The first path was implemented first because the current failures are already
explained by available causal/orderbook buckets. The problem was not missing raw
features; it was that static causal-policy search could give pass credit to a
policy with only one active OOS report and two abstentions.

## Implementation

`strategy-builder causal-policy-search` now has an opt-in gate:

```text
--min-oos-eligible-reports <N>
```

Default `0` preserves existing research behavior. A+ runs can set this above
zero so policies must have selected trades in enough chronological OOS reports
before they can pass.

## Evidence

Artifact:

- `polymomentum_low_exposure_policy_search_mineligible2_20260706.json`

Input reports are the same three low-exposure tail-cluster folds used by the
July 5 policy-search diagnostic.

Comparison:

- Previous artifact:
  `../20260705_low_exposure_policy_search_diagnostics/policy_search/polymomentum_latency128_low_exposure_policy_search_3fold_20260705.json`.
- Previous top policy: `ok=true`, require `book_age=lte_100ms`, deny
  `book_imbalance=strong_positive`.
- Previous feed-forward credit: `1` eligible OOS report, `2` abstained reports,
  `5` trades, `5` wins, `0` losses, `+6.07120` PnL.
- New gate: `--min-oos-eligible-reports 2`.
- New result: `ok=false` across `9046` candidates.
- New top ranked policy had `2` eligible reports but failed tail/PnL gates:
  `6` trades, `4` wins, `2` losses, `-4.60148` PnL, worst report `-5.13834`,
  CVaR `-5.13834`.

## Verdict

The stricter causal-policy search no longer promotes the thin one-report static
hypothesis. When forced to use broader chronological OOS coverage, replacement
losses reappear. The low-exposure family remains rejected; A+ still needs a
candidate that survives replay-integrated policy selection and then full
harness/live-replay validation.

## Verification

```text
cargo test --manifest-path rust_engine/Cargo.toml causal_policy_min_eligible_reports_blocks_thin_oos_credit
cargo test --manifest-path rust_engine/Cargo.toml causal_policy
cargo build --manifest-path rust_engine/Cargo.toml --bin polymomentum-engine
```
