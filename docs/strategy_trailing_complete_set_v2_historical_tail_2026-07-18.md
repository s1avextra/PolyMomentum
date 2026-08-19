# Trailing Complete-Set Lock v2 Historical Tail Diagnostic

## Outcome

Trailing complete-set lock v2 is **rejected without retuning**. It improved the unchanged baseline by `$10.30` across folds 29–42, but still lost `-$2.29` after fees. Profit factor (`0.944`), payoff ratio (`0.157`), first-half PnL (`-$5.32`), and the rolling five-fold loss burst (`4`) all fail the preregistered research gates.

This history earns no promotion credit. Production remains unchanged, and v2 must not be scored on the fresh binary-complement blocks because that canary evaluates a different preregistered mechanism.

## Headline comparison

| Metric | Baseline | Trailing v2 | Gate |
|---|---:|---:|---:|
| Trades | 43 | 56 | — |
| Wins / losses | 33 / 10 | 48 / 8 | — |
| Fee-inclusive PnL | -$12.59 | -$2.29 | candidate > $0: fail |
| Fees | $3.18 | $5.74 | reconciled |
| Profit factor | 0.755 | 0.944 | > 1: fail |
| Payoff ratio | 0.229 | 0.157 | >= 0.30: fail |
| Wilson 95% lower bound | 0.623 | 0.743 | >= 0.70: pass |
| Profitable folds | 4 / 14 | 6 / 14 | — |
| First / second half PnL | -$6.69 / -$5.90 | -$5.32 / +$3.03 | both > $0: fail |
| 20% fold CVaR | -$5.07 | -$5.07 | >= -$8: pass |
| Worst rolling five-fold loss count | 4 | 4 | <= 2: fail |

## Fold-level result

| Fold | Baseline PnL | Trailing v2 PnL | Delta |
|---:|---:|---:|---:|
| 29 | -$0.24 | -$2.25 | -$2.00 |
| 30 | +$4.51 | +$3.08 | -$1.43 |
| 31 | -$3.54 | +$6.02 | +$9.56 |
| 32 | -$5.02 | -$5.02 | $0.00 |
| 33 | -$3.61 | -$4.81 | -$1.20 |
| 34 | -$5.08 | -$5.08 | $0.00 |
| 35 | +$6.28 | +$2.74 | -$3.54 |
| 36 | -$2.97 | -$4.10 | -$1.13 |
| 37 | -$4.13 | +$10.97 | +$15.11 |
| 38 | +$6.25 | +$3.35 | -$2.91 |
| 39 | -$5.12 | -$5.12 | $0.00 |
| 40 | -$2.24 | -$3.50 | -$1.26 |
| 41 | -$1.17 | -$1.09 | +$0.08 |
| 42 | +$3.48 | +$2.52 | -$0.96 |

Folds 31 and 37 contribute `+$24.66` of relative improvement. Outside those two folds, v2 is roughly `-$14.36` worse than baseline. The aggregate improvement is therefore concentrated rather than stable.

## Mechanism accounting

The replay recorded 43 arm transitions and 39 missing-leg FOK signals. Thirty-one locks filled, eight failed closed, no fill was unresolved, and no breaker tripped. Every successful lock guaranteed at least `$0.10` after both fees.

The 31 locks added `+$7.27` versus holding the same entries to terminal resolution:

- five terminal-loser locks recovered `+$28.53`;
- 26 terminal-winner locks surrendered `-$21.26`;
- eight terminal losses never armed or locked and remained fully exposed.

The mechanism changes later entry eligibility. All 43 baseline entries remain common, but v2 adds 13 candidate-only entries after earlier locks alter the state path. Common-entry PnL improves by only `+$1.60`; candidate-only entries add `+$8.70`. The full-policy delta is real, but it is not a pure exit improvement and is dominated by folds 31 and 37.

## Data and reproducibility

The corrected run used the preregistered candidate hash `8554587b2e8bca78c504f3fbb8840737fee1d384567b173ba8efe8d909a4bb11`, 202 ms insertion latency, 14 independent eight-hour folds, 96 markets per fold, and retained sidecars only. Baseline trade rows exactly match the frozen v1 replay. PnL, both fees, summaries, source manifests, market catalogs, exit attempts, locks, and unresolved exposure reconcile exactly.

An initial attempt used a stale standalone debug executable even though the test binaries were current. Its engine hash differed and it silently ignored the new exit field. Those outputs were stopped, quarantined outside the evidence path, and excluded. The standalone executable was rebuilt, all 14 folds were rerun from scratch, and the reconciliation script requires the preregistered engine hash.

Reproducible evidence:

- Diagnostic: `deploy/promotions/evidence/strategy_registry/20260718_trailing_complete_set_lock_v2_historical_tail_diagnostic.json`
- Raw 42-file bundle: `deploy/promotions/evidence/strategy_registry/20260718_trailing_complete_set_lock_v2_historical_tail_raw_reports.tar.gz`
- Reconciler: `scripts/reconcile_trailing_complete_set_v2_tail.py`
- Preregistration: `deploy/promotions/evidence/strategy_registry/20260718_trailing_complete_set_lock_v2_preregistration.json`

## Decision and next step

Do not search neighboring arm, retreat-floor, hold, or retry thresholds. Do not implement v2 in the live runtime. Continue the separately preregistered binary-complement canary because it introduces new causal cross-book information rather than another threshold on the same lifecycle path. That family still requires two disjoint fresh blocks, exact replay, positive PnL in both halves, and unchanged A+ gates before runtime-parity work.

## Limitations

- Folds 29–42 were observed during mechanism design and cannot support promotion.
- Replay does not prove live FOK acknowledgements, fee parameters, merge/redemption operations, or runtime state parity.
- Gas, merge, and redemption operating costs are excluded consistently with the baseline.
- The fresh segment 003 recording is independent evidence for binary-complement coherence, not evidence for trailing v2.
