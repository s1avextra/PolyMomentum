# A+ Sparse Gate Status - 2026-06-08

## Verdict

The project is closer to an A+ validation loop, but the live strategy is not A+
yet. The execution/backtest/replay infrastructure now supports causal
selectivity consistently, and the robust gate can evaluate intentionally sparse
strategies without treating no-trade folds as losses. The current down-only
confidence candidate is a promoted research candidate, not a live deployment
candidate, because the active sample is still only 7 trades over 48 hours.

Current grade:

- Infrastructure and parity wiring: A-
- Strategy evidence: B+ / A- research candidate
- Live readiness: fail-closed until broader feed-forward history passes

## What Changed

- Added first-class causal selectivity to backtest variants, harness sweeps,
  live replay validation, live runtime strategy logging, and live decision
  filtering.
- Added `--require-causal-tag` / `--deny-causal-tag` to `harness-sweep` and
  `strategy-builder rolling-history`.
- Added `--taker-only` so the 5m candidate can avoid maker assumptions when
  the current evidence is taker-execution based.
- Added rolling-history promotion knobs:
  - `--min-promotion-trades`
  - `--min-promotion-daily-trades`
  - `--min-promotion-profitable-reports`
  - `--min-promotion-losses`
  - `--min-neighbor-observations`
- Updated robust promotion diagnostics so neighbor stability reports both:
  - nearby parameter count
  - active neighbor-window observations
- Changed sparse neighbor stability to ignore neighbor windows with zero
  trades. A nearby variant that trades and loses still counts against the
  selected candidate.

## Evidence

Existing confidence-profile reports were re-gated with the release binary:

```bash
./rust_engine/target/release/polymomentum-engine experiment robust-promote \
  --report /private/tmp/polymomentum_down_conf_48h_20260608/reports/fold_001_20260531T000000Z_20260531T070000Z_sweep.json \
  --report /private/tmp/polymomentum_down_conf_48h_20260608/reports/fold_002_20260531T080000Z_20260531T150000Z_sweep.json \
  --report /private/tmp/polymomentum_down_conf_48h_20260608/reports/fold_003_20260531T160000Z_20260531T230000Z_sweep.json \
  --report /private/tmp/polymomentum_down_conf_48h_20260608/reports/fold_004_20260601T000000Z_20260601T070000Z_sweep.json \
  --report /private/tmp/polymomentum_down_conf_tail_20260608/reports/fold_001_20260601T080000Z_20260601T150000Z_sweep.json \
  --report /private/tmp/polymomentum_down_conf_tail_20260608/reports/fold_002_20260601T160000Z_20260601T230000Z_sweep.json \
  --output /private/tmp/polymomentum_down_conf_sparse_gate_release_20260608.json \
  --min-reports 6 --min-profitable-reports 4 --min-trades 7 --min-losses 0 \
  --min-zone-count 1 --max-zone-trade-share 1.0 \
  --min-win-rate 0.70 --min-wilson-win-rate-lower 0.60 \
  --min-total-pnl 0 --max-passive-failed-fills 480 --min-fill-rate 0.55 \
  --min-daily-trades 0 --min-daily-pnl 0 \
  --min-neighbor-count 2 --min-neighbor-observations 8 \
  --min-neighbor-positive-rate 0.60 \
  --max-pbo 0.50 --min-median-oos-percentile 0.80 \
  --min-worst-window-pnl 0 --min-profit-factor 1.20 \
  --min-payoff-ratio 0.20 --max-worst-loss-to-avg-win 6.0 \
  --min-causal-bucket-trades 10 --min-causal-bucket-pnl 0
```

Selected candidate:

- Strategy: `all_c0.60_z0.70_e0.07...selreqdirection-down_tk`
- Trades: 7
- Wins/losses: 7 / 0
- Total PnL: +8.25978
- Fill rate: 100%
- Wilson lower: 0.64566
- Worst window PnL: 0.0
- Median window PnL: 1.193695
- Neighbor count: 7
- Active neighbor observations: 20
- Neighbor positive rate: 100%
- PBO: 0.000 across 20 splits
- Median OOS percentile: 1.000

The confidence gate skipped the previously losing June 1 early fold instead of
trading through it. That is the right behavior for a sparse filter, but it
creates a small active sample that must be expanded before live.

## A+ Promotion Standard

To call this A+ and consider canary, the same causal/taker profile should pass
on broader feed-forward history with all of the following:

- At least 7-14 days of atomic hourly PMXT folds, deleting only session-owned
  parquets/caches.
- At least 50 resolved active trades, preferably 100+.
- Wilson lower at or above 0.60.
- Positive aggregate PnL and no negative worst active window.
- No negative causal bucket with at least 10 trades.
- PBO at or below 0.50 and median OOS percentile at or above 0.80.
- At least 2 neighboring parameter variants and at least 8 active neighbor
  observations, with neighbor positive rate at or above 0.60.
- Backtest/live-replay decision parity with the same selectivity filter.
- No circuit breaker trip, non-passive execution failure, unresolved fill, or
  settlement alignment warning.

## Next Command Shape

Use rolling-history with explicit sparse policy, not paper mode:

```bash
./rust_engine/target/release/polymomentum-engine strategy-builder rolling-history \
  --start <UTC_START> --end <UTC_END> \
  --out-dir /private/tmp/polymomentum_a_plus_sparse_<date> \
  --profile a_plus5m_down_reversion_guard_confidence \
  --zone-mode all --fold-hours 8 --require-full-folds \
  --require-causal-tag direction=down \
  --delete-after-process --atomic-parquet \
  --threads 1 --window-minutes 5 --max-cache-gb 4 \
  --min-fold-trades 1 --min-fold-top-trades 0 \
  --min-promotion-trades 50 \
  --min-promotion-daily-trades 0 \
  --min-promotion-profitable-reports <positive_fold_floor> \
  --min-promotion-losses 0 \
  --min-neighbor-observations 8 \
  --execute
```

On the VPS, keep this as a downloader/runtime-only job and do not run broad CPU
sweeps there. Heavy searches stay on the dev box; only one raw PMXT hour should
be held at a time, and cleanup must delete only session-owned files.
