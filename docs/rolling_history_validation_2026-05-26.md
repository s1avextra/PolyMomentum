# Rolling History Validation - 2026-05-26

Scope: local dev-box validation only. No VPS shared cache, peer bot directory,
paper mode, or live venue was touched.

## Commands

Full-day rolling run:

```text
./rust_engine/target/release/polymomentum-engine strategy-builder rolling-history \
  --start 2026-05-24T00:00:00Z \
  --end 2026-05-24T23:00:00Z \
  --out-dir /private/tmp/polymomentum_rolling_history_20260526_may24 \
  --fold-hours 8 \
  --threads 6 \
  --profile a_plus5m \
  --zone-mode early \
  --delete-after-process \
  --max-cache-gb 40 \
  --execute
```

Current-date probe:

```text
./rust_engine/target/release/polymomentum-engine strategy-builder rolling-history \
  --start 2026-05-25T00:00:00Z \
  --end 2026-05-25T23:00:00Z \
  --out-dir /private/tmp/polymomentum_rolling_history_20260526_may25 \
  --fold-hours 8 \
  --threads 4 \
  --profile a_plus5m \
  --zone-mode early \
  --delete-after-process \
  --max-cache-gb 40 \
  --execute
```

The May 25 run completed fold 1, then correctly exposed PMXT archive lag:
`2026-05-25T09` returned HTTP 404. The failed session-owned `fold_002_*`
cache was deleted manually, and the driver now cleans a failed fold cache when
`--delete-after-process` is set.

## Artifacts

May 24 reports:

- `/private/tmp/polymomentum_rolling_history_20260526_may24/reports/fold_001_20260524T000000Z_20260524T070000Z_sweep.json`
- `/private/tmp/polymomentum_rolling_history_20260526_may24/reports/fold_002_20260524T080000Z_20260524T150000Z_sweep.json`
- `/private/tmp/polymomentum_rolling_history_20260526_may24/reports/fold_003_20260524T160000Z_20260524T230000Z_sweep.json`

Promotion artifact:

- `/private/tmp/polymomentum_rolling_history_20260526_may24/promotions/rolling_20260524T000000Z_20260524T230000Z_robust.json`

The promotion artifact now embeds `robust_diagnostics` so the selected
candidate, PBO, trial ledger, and top robust candidates are preserved in the
artifact itself rather than only in terminal output.

## Selected Candidate

Robust promotion selected:

```text
early_c0.30_z0.50_e0.03_ev-1.00_p0.10-0.90_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk
```

Aggregate metrics:

- trades: `91`
- total PnL: `+43.31`
- win rate: `79.12%`
- Wilson 95% lower bound: `0.697`
- fill rate: `56.88%`
- worst fold PnL: `+3.68`
- median fold PnL: `+6.30`
- robust score: `0.4104`
- neighbor-positive rate: `63.6%` over `11` neighbors
- PBO: `0.333` over `3` split tests
- stressed drawdown: `16.57%`
- passive maker non-fills / post-only rejects: `69`

Fold-by-fold for the selected candidate:

| Fold | Window UTC | Trades | W/L | PnL | Fill rate | Passive rejects |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 2026-05-24T00..07 | 31 | 24/7 | +3.68 | 57.41% | 23 |
| 2 | 2026-05-24T08..15 | 34 | 25/9 | +6.30 | 66.67% | 17 |
| 3 | 2026-05-24T16..23 | 26 | 23/3 | +33.33 | 47.27% | 29 |

## Interpretation

This is a good research candidate, not a live-ready A+ artifact yet.

Positive evidence:

- It stayed profitable in all three 8-hour folds.
- It beat the earlier z=0.70 favorite on robustness after the midday fold
  weakened z=0.70.
- The selected rule is simpler and lower-threshold than the previous spike.
- The full 24h run used PMXT L2 replay and the same fill/order mechanics as the
  backtest harness, not paper mode.

Remaining blockers:

- All trades are in the `early` timing zone, so zone concentration remains
  intentional but high.
- Neighbor-positive rate is only `63.6%`; a stricter `70%` neighbor gate rejects
  all candidates on this 24h set.
- PBO has only `3` split tests because the run has only `3` folds.
- Fold 3 fill rate is only `47.27%`, even though aggregate fill rate passes.
- May 25 current archive is incomplete after `T08`, so current-date validation
  must be availability-aware.

## Production Implication

Do not promote this directly to live. Use it as the current best candidate for
the next broader rolling-history pass:

1. Use `--preflight-pmxt-hours --stop-at-first-missing-hour
   --require-full-folds` for current-date runs so PMXT archive lag truncates to
   the last complete fold instead of failing or scoring a tiny partial fold.
2. Run the same rolling driver over more complete PMXT days, deleting each
   session-owned fold cache.
3. Require the robust artifact to pass stricter stability before canary:
   `neighbor_positive_rate >= 0.70`, more than `3` PBO splits, and positive
   fold-level fill behavior.
4. Only after that, run cached live-replay/parity against the selected artifact.

## Follow-Up Loop: May 23-25

Driver improvements added after the first broader run:

- `--preflight-pmxt-hours --stop-at-first-missing-hour --require-full-folds`
  probes archive availability before downloads and truncates to complete folds.
- PMXT downloads now retry transient chunk-read failures before failing a fold.
- `--min-fold-trades` makes the per-fold promotion sample floor explicit.
- The rolling driver writes `rolling_history_manifest.json` before promotion and
  rewrites it with `promotion_passed` or `promotion_failed`.
- A zero-target-event fold is now treated as data coverage failure, not strategy
  evidence.

Results:

| Set | Reports | Gate | Result |
| --- | ---: | --- | --- |
| May 23 only | 3 | `neighbor>=0.70`, `PBO<=0.50` | rejected: PBO `0.667` |
| May 23 + May 24 | 6 | `min_fold_trades=15` | passed: PBO `0.250`, selected z=0.50 maker |
| May 23 + May 24 + May 25 prefix | 7 | `min_fold_trades=15` | rejected: no variant had 7/7 profitable folds with >=15 trades |
| May 23 + May 24 + May 25 prefix | 7 | `min_fold_trades=8` | passed: PBO `0.400`, selected z=0.90 maker |

The strongest 7-fold candidate is:

```text
early_c0.35_z0.90_e0.03_ev-1.00_p0.10-0.90_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk
```

Seven-fold metrics:

- trades: `96`
- total PnL: `+53.85`
- win rate: `86.46%`
- Wilson 95% lower bound: `0.782`
- fill rate: `59.26%`
- worst fold PnL: `+3.86`
- neighbor-positive rate: `82.4%` over `17` neighbors
- PBO: `0.400` over `35` split tests
- passive maker non-fills / post-only rejects: `66`

The 7-fold artifact is useful research evidence, but still not an A+ live
artifact because it needs `min_fold_trades=8`. The stricter
`min_fold_trades=15` gate correctly rejects the search once the current May 25
prefix is included.

May 22 note:

- PMXT archive HEAD checks passed for all 24 hours.
- The first two 8-hour folds produced zero target BTC L2 events despite Gamma
  returning 96 markets.
- The interrupted session-owned cache was removed; final cache size was `0B`.
- Treat May 22 as a source coverage boundary, not a negative strategy day.

Current status:

- Best production-grade evidence: May 23-24, 6 folds, strict PBO pass.
- Best current-aware evidence: May 23-25 prefix, 7 folds, lower sample floor
  pass.
- Next A+ step: collect more post-May-23 complete folds, then require both
  `min_fold_trades >= 15` and 7+ profitable folds before canary.
