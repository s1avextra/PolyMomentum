# 5m Candidate Search - 2026-05-11

Scope: BTC 5-minute Polymarket candle frames over cached PMXT v2 data for
2026-04-23 through 2026-04-25. Runs were local only; no VPS sweeps were run.

## Tooling changes

- `harness`, `harness-sweep`, and `live-replay` now accept
  `--window-minutes 5` so the candidate loop can isolate 5-minute frames.
- `harness-sweep` now uses cached Gamma metadata directly instead of scanning
  every parquet hour for condition IDs before each sweep.
- `experiment aggregate-promote` now accepts `--min-daily-pnl` so raw aggregate
  PnL does not select a fragile candidate with a weak holdout day.

## Candidate Results

Compact grid:

```text
--conf 0.15,0.25
--z 0.10,0.30
--edge 0.00,0.02
--ev-buffer=-1.0
--window-minutes 5
```

Best robust candidate:

```text
c0.15_z0.30_e0.02_ev-1.00_mk
```

Aggregate across three daily reports:

| Metric | Value |
| --- | ---: |
| Trades | 813 |
| Wins / losses | 560 / 253 |
| Win rate | 68.88% |
| Wilson 95% lower bound | 65.6% |
| Total PnL | +468.26 |
| Avg PnL / trade | +0.576 |
| Worst daily PnL | +96.27 |
| Dominant zone | early, 82.5% of trades |

Daily PnL for the selected robust candidate:

| Day | Trades | Win rate | PnL |
| --- | ---: | ---: | ---: |
| 2026-04-23 | 280 | 67.1% | +138.08 |
| 2026-04-24 | 274 | 71.5% | +233.91 |
| 2026-04-25 | 259 | 68.0% | +96.27 |

Raw aggregate-PnL leader was `c0.15_z0.10_e0.02_ev-1.00_mk` with +505.89
aggregate PnL, but its worst day was only +11.29, so it was rejected by the
new `--min-daily-pnl 50` robustness gate.

## Artifacts

- Robust promotion artifact:
  `logs/experiments/5m_swift_aggregate_candidate_min_daily_50_20260423_25.json`
- Daily reports:
  - `logs/experiments/5m_swift_holdout_compact_20260423.json`
  - `logs/experiments/5m_swift_holdout_compact_20260424.json`
  - `logs/experiments/5m_swift_stage_b_compact_20260425.json`

## Next Gate

Do not go live from this alone. Next step is paper mode with this promotion
artifact, collecting parity diagnostics against the same 5-minute frame logic.
