# 2026-07-05 Low-Exposure Policy Search Diagnostics

This archive extends the low-exposure tail-cluster work with chronological
causal-policy search and direct replay verification.

All runs used the July 5 VPS forward-latency audit and effective latency
`128 ms`.

## Inputs

- Fold 1 baseline is archived in
  `../20260705_low_exposure_remap_diagnostics/baseline_interrupted/`.
- `train_folds2_3/` adds the next two chronological low-exposure reports:
  `2026-05-31T16:00:00Z` through `2026-06-01T07:00:00Z`.

## Results

- Fold 2 top variant: `6` trades, `5` wins, `1` loss, `+1.00454` PnL.
- Fold 3 top variant: `1` trade, `0` wins, `1` loss, `-5.13834` PnL.
- Chronological causal-policy search over three reports found a thin top
  hypothesis: require `book_age=lte_100ms` and deny
  `book_imbalance=strong_positive`.
- Static policy view looked clean: `6` trades, `6` wins, `0` losses,
  `+7.19137` PnL. Feed-forward view had only one eligible OOS report and
  abstained on two reports, so it was research-only.
- Direct replay of the learned tags failed on fold 1: `2` trades, `1` win,
  `1` loss, `-4.18424` PnL. The replacement losing regime used
  `book_imbalance=negative`, proving the static filter did not survive actual
  strategy replay dynamics.

## Verdict

The low-exposure family remains rejected. Static causal filtering can identify
interesting abstention hypotheses, but A+ promotion needs replay-integrated
policy generation where candidate filters are rerun through the full harness
before any gate credit is assigned.
