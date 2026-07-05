# 2026-07-05 Low-Exposure Remap Diagnostics

This archive captures the first-fold fail-fast tests for
`a_plus5m_tail_low_exposure` on the first known tail cluster:
`2026-05-31T08:00:00Z` through `2026-05-31T15:00:00Z`.

All runs used the July 5 VPS forward-latency audit and effective latency
`128 ms`.

## Cases

- `baseline_interrupted/`: the full cluster run was interrupted after fold 1
  because the first fold already had one loss and could no longer satisfy the
  zero-loss promotion gate. Fold 1 produced `2` trades, `1` win, `1` loss,
  and `-4.06889` PnL.
- `deny_exact_regime/`: denied the exact first losing regime. The run still
  produced `2` trades, `1` win, `1` loss, and `-4.43706` PnL because the loss
  moved to the adjacent `book_min_depth=100_250` regime.
- `deny_strong_positive_pressure/`: denied `book_pressure=strong_positive`.
  The run still produced `2` trades, `1` win, `1` loss, and `-4.18424` PnL;
  the loss moved to a `book_pressure=negative` regime.

## Verdict

The low-exposure widening path is rejected for now. Manual micro-regime deny
rules did not remove the toxic entry behavior; they only shifted it. The stable
bad shape in this fold is low-price, high-edge, primary-zone down entries
(`price=0.50_0.75`, `edge=gte_0.15`), which explains why the stricter
down-reversion guard avoided losses by keeping `min_price=0.75` but became too
sparse.
