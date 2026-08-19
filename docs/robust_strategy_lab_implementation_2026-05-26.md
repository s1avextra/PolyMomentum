# Robust Strategy Lab Implementation - 2026-05-26

Scope: turn the anti-overfitting plan into production tooling and validate it on
fresh May PMXT data without touching the VPS or shared bot caches.

## Implemented

### Robust Promotion

Added:

```text
polymomentum-engine experiment robust-promote
```

This command keeps the existing aggregate promotion gates, then adds:

- trial ledger: reports, windows, report hashes, comparable variant count, and
  total variant-report trials;
- PBO-style combinatorial split diagnostic over the variant x window return
  matrix;
- worst-window PnL and expectancy gates;
- neighbor stability around the selected parameter point;
- robust-score ranking instead of raw total PnL ranking.

The selected score combines:

```text
0.30 * worst_window_expectancy
+ 0.20 * median_window_expectancy
+ 0.15 * Wilson lower win-rate bound
+ 0.15 * neighbor positive rate
+ 0.10 * maker/fill reliability
+ 0.05 * low stressed drawdown pressure
+ 0.05 * simplicity score
```

### Strategy Builder Alignment

`strategy-builder plan` now emits `robust-promote` instead of
`aggregate-promote`.

The `a_plus5m` profile is aligned with the current May evidence:

- confidence: `0.30,0.35,0.40`
- z-score: `0.50,0.70,0.90,1.10`
- edge: `0.03`
- price buckets: `0.10-0.75` and `0.10-0.90`
- settlement floor/guard: `10 USD`, `1 minute`
- microstructure: permissive research gate for the current maker strategy
- sizing: `position_pct=0.05`, `max_per_market_usd=20`
- exposure: `max_total_exposure_usd=15`
- stress cap: `max_projected_stressed_drawdown_pct=0.24`
- maker variants included

This fixes a strategy-lab mismatch where generated sweeps did not carry the
same risk/sizing shape as the latest validated May runs.

## Backtest Validation

Fresh May input cache:

```text
/private/tmp/polymomentum_fresh_may_backtest_20260526T0303Z
```

The first robust gate was run over:

- full 2026-05-24 report;
- available 2026-05-25T00..08 report.

With strict neighbor positivity, the gate rejected the raw `z0.70` winner
because the lower `z0.50` neighbor was negative. That was a useful rejection:
the winner looked like it might be sitting on a boundary.

To test whether the edge extends upward, a high-z neighborhood was run:

```text
z = 0.70,0.90,1.10
conf = 0.30,0.35,0.40
max_price = 0.75,0.90
maker + taker
```

Symmetric windows:

- `2026-05-24T00:00:00Z..2026-05-24T08:00:00Z`
- `2026-05-25T00:00:00Z..2026-05-25T08:00:00Z`

Reports:

```text
/private/tmp/polymomentum_fresh_may_backtest_20260526T0303Z/harness_sweep_early_grid_highz_continuous_20260524T00_08.json
/private/tmp/polymomentum_fresh_may_backtest_20260526T0303Z/harness_sweep_early_grid_highz_continuous_20260525T00_08.json
```

Robust promotion artifact:

```text
/private/tmp/polymomentum_fresh_may_backtest_20260526T0303Z/robust_promotion_highz_may24_25_00_08.json
```

Selected:

```text
early_c0.40_z0.70_e0.03_ev-1.00_p0.10-0.90_sf10_sg1.0_ss0.00_ms1.00_md0_mp-1.00_mk
```

Selected metrics:

| Metric | Value |
| --- | ---: |
| Trades | 51 |
| Wins / losses | 43 / 8 |
| Win rate | 84.31% |
| Wilson lower bound | 71.99% |
| PnL | +28.86 |
| Worst-window PnL | +13.88 |
| Median-window PnL | +14.43 |
| Fill rate | 62.20% |
| Passive maker non-fills | 31 |
| Robust score | 0.6624 |
| Neighbor count | 11 |
| Neighbor positive rate | 81.82% |
| PBO estimate | 0.000 across 2 splits |
| Breakers | 0 |

Interpretation: the `z0.70` maker candidate is not merely the isolated winner
of one raw grid. The high-z neighborhood shows profitable support at stricter
thresholds, especially `z0.90`, although `z1.10` becomes sparse. This supports a
universal rule:

```text
Early 5m BTC entries need strong momentum confirmation before consuming the one
allowed trade in the candle. Maker economics help, but weak early signals should
not be harvested just to increase trade count.
```

## PMXT All-History Plan

We should gradually backtest more PMXT history, but not by hoarding every raw
parquet.

Recommended state:

1. Use rolling chronological windows, preferably 8h or 24h folds.
2. For each fold, create a unique local session cache under `/private/tmp`.
3. Download only that fold's PMXT hours.
4. Run eval-cache/harness-sweep/robust-promote.
5. Persist only compact JSON reports, promotion artifacts, and summaries.
6. Delete only the parquets downloaded by that session.
7. Never delete shared or peer-owned parquets.
8. Run CPU-heavy sweeps on the dev box, not the 2-core VPS.

This lets us build a broad fold matrix across PMXT history while keeping disk
bounded. The output we need for robust promotion is the variant x window return
matrix, not the raw parquet archive after the fold has been scored.

Implemented follow-up: `polymomentum-engine strategy-builder rolling-history`
accepts a date range, fold size, disk budget, and output directory. It dry-runs
by default and executes only with `--execute`, using one session-owned fold
cache at a time.

Example dry run:

```text
polymomentum-engine strategy-builder rolling-history \
  --start 2026-05-24T00:00:00Z \
  --end 2026-05-25T23:00:00Z \
  --out-dir /private/tmp/polymomentum_rolling_history \
  --fold-hours 8 \
  --profile a_plus5m \
  --zone-mode early \
  --delete-after-process \
  --max-cache-gb 40
```

Add `--execute` only after inspecting the manifest. During execution each fold:

1. creates `<out-dir>/cache/fold_*`;
2. hydrates Gamma metadata with a one-contract harness pass;
3. runs the full continuous high-z maker/taker harness sweep;
4. writes compact JSON reports under `<out-dir>/reports`;
5. optionally deletes only that `fold_*` cache;
6. runs `experiment robust-promote` across the fold reports.
