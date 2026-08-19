# Atomic PMXT History Validation - 2026-05-26

Scope: local dev-box validation only. No VPS shared cache, peer bot directory,
paper mode, or live venue was touched.

## Implementation

Added atomic PMXT replay support:

- `harness` / `harness-sweep --atomic-parquet`
  - skips upfront all-hour PMXT downloads;
  - downloads one hourly PMXT parquet inside the replay loop;
  - replays that hour;
  - deletes only the parquet downloaded by that same process;
  - leaves pre-existing cached/shared parquets untouched.
- `harness --metadata-only`
  - hydrates Gamma metadata without replaying PMXT;
  - avoids downloading every parquet twice in `rolling-history`.
- `strategy-builder rolling-history --atomic-parquet`
  - passes atomic mode through to hydrate/sweep child commands;
  - records the storage policy in `rolling_history_manifest.json`.

This preserves continuous per-fold strategy/order-book state while keeping raw
archive storage bounded to one active parquet per fold.

## Command

```text
./rust_engine/target/release/polymomentum-engine strategy-builder rolling-history \
  --start 2026-04-23T00:00:00Z \
  --end 2026-04-25T23:00:00Z \
  --out-dir /private/tmp/polymomentum_atomic_history_20260526_apr23_25 \
  --fold-hours 8 \
  --threads 6 \
  --profile a_plus5m \
  --zone-mode early \
  --atomic-parquet \
  --delete-after-process \
  --max-cache-gb 2 \
  --preflight-pmxt-hours \
  --require-full-folds \
  --min-fold-trades 15 \
  --min-neighbor-positive-rate 0.70 \
  --max-pbo 0.50 \
  --execute
```

## Storage Result

- PMXT raw parquets retained after run: `0`.
- Cache dir after run: `0B`.
- Reports retained: `2.3M`.
- Hydrate reports retained: `36K`.
- Promotion dir after strict run: `0B`.

The run processed 72 hourly PMXT archives as 9 full 8-hour folds. Transient
PMXT download failures occurred on a few hours and recovered via retry.

## Strict Gate Result

Strict robust promotion rejected the run.

Reason: no variant stayed profitable in all 9 folds with the configured sample,
neighbor, and worst-window gates.

Best strict-near candidate:

- strategy: `early_c0.40_z0.50_e0.03_ev-1.00_p0.10-0.90_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk`
- total PnL: `+116.06`
- trades: `272`
- win rate: `75.37%`
- fill rate: `62.96%`
- profitable folds: `8 / 9`
- worst fold: `-23.19` on `2026-04-25T16:00..23:00Z`
- minimum fold trades: `6`
- neighbor-positive rate: `69.7%`
- PBO: `0.127` across `126` splits

The rejection is correct. The strategy family shows a real-looking edge across
most folds, but the late April 25 fold invalidates live promotion without an
additional regime/risk filter.

## Diagnostic Relaxed Gate

Diagnostic-only relaxed promotion was run to identify the best survivor:

- allowed `8 / 9` profitable folds;
- allowed `min_daily_trades=5`;
- allowed `min_worst_window_pnl=-25`;
- allowed `min_neighbor_positive_rate=0.65`;
- allowed `min_robust_score=-1`.

It selected the same candidate above. This is research evidence only, not a
deployable artifact.

## Interpretation

Atomic historical validation worked. The strategy did not earn A+ promotion.

Useful conclusions:

1. The low-z maker family is robust across April 23, April 24, and most of
   April 25.
2. April 25 late session is a hard counterexample: the same family loses
   materially and under-trades.
3. High-z/taker variants occasionally lead individual folds but are less stable
   across the full set.
4. The next improvement should be a feed-forward regime veto that can skip
   low-quality late-session conditions before order placement, rather than
   loosening promotion gates.

Next research loop:

1. Add fold/regime diagnostics for the losing window: signal count, skipped
   reasons, fill pressure, spread/depth, BTC realized volatility, and time-to-
   settlement distribution.
2. Build a causal regime veto using only information available before each
   decision.
3. Re-run the same 72h atomic validation with the veto.
4. Require strict promotion to pass without relaxing `9 / 9` profitable folds.
