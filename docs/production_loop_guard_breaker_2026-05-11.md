# Production Loop Hardening - 2026-05-11

Scope: 5-minute BTC candle strategy after the first promoted paper run.

## Paper Finding

The bot executed correctly: signals were evaluated, orders were placed and
filled, and Polymarket oracle checks completed. The run exposed two production
risks:

- The circuit breaker tripped on conservative open-exposure stress while
  realized PnL was still strongly positive.
- Near-threshold local BTC resolution can disagree with official Polymarket
  settlement. Paper must treat local resolution as provisional and official
  oracle resolution as final.

## Changes

- Added a settlement-margin guard to the shared decision path. The default only
  blocks new entries in the final 1 minute when BTC is within $10 of the candle
  threshold. A wider volatility buffer is supported but defaults off because a
  full-window volatility band eliminated all 5-minute strategy trades.
- Changed breaker drawdown logic to use realized drawdown as the primary trip
  condition. Open exposure is still stress-tested, but bounded positions no
  longer trip the breaker while stressed PnL remains positive.
- Added breaker diagnostics for peak PnL, open exposure, stressed PnL, and
  realized/stressed drawdown.
- Changed session diagnostics so corrected oracle disagreements are warnings,
  not fatal parity failures. Uncorrected disagreements and ties remain fatal.
- Fixed `harness-sweep` so a missing BTC tape fails fast instead of producing
  invalid zero-trade reports.

## Validated Backtest

Validated local-only PMXT replay, `--window-minutes 5`, compact 16-variant grid,
real Binance BTC tape, Apr 23-25 2026.

Selected robust candidate remains:

```text
c0.15_z0.30_e0.02_ev-1.00_mk
```

Aggregate:

| Metric | Value |
| --- | ---: |
| Trades | 811 |
| Wins / losses | 558 / 253 |
| Win rate | 68.80% |
| Wilson 95% lower bound | 65.5% |
| Total PnL | +463.25 |
| Worst daily PnL | +91.26 |
| Dominant zone | early, 82.7% |

Daily PnL:

| Day | Trades | Win rate | PnL |
| --- | ---: | ---: | ---: |
| 2026-04-23 | 280 | 67.1% | +138.08 |
| 2026-04-24 | 274 | 71.5% | +233.91 |
| 2026-04-25 | 257 | 67.7% | +91.26 |

New promotion artifact:

```text
logs/experiments/5m_guard_validated_aggregate_candidate_min_daily_50_20260423_25.json
```

## Next Gate

Deploy the new binary and promotion artifact to paper mode, reset only
PolyMomentum paper state, then collect a fresh paper session. The session can
pass if all oracle disagreements have matching correction events and the breaker
does not trip.
