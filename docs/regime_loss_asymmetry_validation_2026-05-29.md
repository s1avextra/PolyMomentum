# Regime and Loss-Asymmetry Validation - 2026-05-29

Scope: local dev-box validation only. No VPS shared cache, peer bot private
directory, paper mode, live venue, or shared PMXT distilled cache was touched.

## Implementation

Added feed-forward regime diagnostics to the shared candle decision path:

- every `CandleDecision` now carries a `DecisionRegime` snapshot derived only
  from pre-trade inputs: zone, direction, market-price bucket, edge bucket,
  z-score bucket, confidence bucket, implied-volatility bucket, reversion
  bucket, and minutes-remaining bucket;
- `Signal::from_candle_decision` injects the regime snapshot into signal
  diagnostics for backtest, replay, paper, and live order-intent parity;
- backtest experiment reports now include:
  - `diagnostics.trade_pnl`;
  - `diagnostics.by_regime`;
  - `diagnostics.by_causal_bucket`.

The PnL diagnostics use resolved binary-option settlement math:

- win: `(1 - fill_price) * filled_size - fee`;
- loss: `-fill_price * filled_size - fee`.

This keeps loss asymmetry visible instead of hiding it behind win rate.

## Fresh PMXT Availability

Preflight results on 2026-05-29:

- `2026-05-26T00:00..23:00Z`: missing from the first hour;
- `2026-05-25T00:00..23:00Z`: missing `2026-05-25T09:00Z`;
- `2026-05-24T00:00..23:00Z`: full 24h available.

Validation therefore used the freshest complete 24h window: 2026-05-24.

## Command

```text
./rust_engine/target/release/polymomentum-engine strategy-builder rolling-history \
  --start 2026-05-24T00:00:00Z \
  --end 2026-05-24T23:00:00Z \
  --out-dir /private/tmp/polymomentum_regime_diag_history_20260529_may24 \
  --fold-hours 8 \
  --threads 6 \
  --profile a_plus5m_adaptive \
  --zone-mode all \
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

- Raw PMXT parquets retained after run: `0`.
- Cache dir after run: `0B`.
- Report artifacts retained: `9.3M`.
- During replay, each downloaded hourly parquet was deleted after that hour was
  processed.

## Promotion Result

Strict promotion rejected the run.

Primary rejection:

- PBO: `1.0000`, above max `0.5000`.

Secondary rejection when only PBO was relaxed:

- best neighbor-positive rate: `61.9%`, below required `70.0%`.

Relaxed diagnostic-only selection, not production-valid:

- strategy: `all_c0.35_z0.90_e0.03_ev-1.00_p0.10-0.90_..._fbL2..._mk`;
- trades: `83`;
- wins/losses: `72 / 11`;
- total PnL: `+25.8890`;
- fill rate: `67.48%`;
- worst fold PnL: `+1.9629`;
- robust score: `0.4439`;
- PBO: `1.000`;
- neighbor-positive rate: `61.9%`.

## Loss-Asymmetry Finding

The relaxed candidate is profitable on the day, but its payout geometry is
fragile:

- gross win PnL: `+82.0665`;
- gross loss PnL: `-56.1775`;
- average win: `+1.1398`;
- average loss: `-5.1070`;
- payoff ratio: `0.2232`;
- worst loss: `-5.5336`;
- profit factor: `1.4608`.

This is a high-win-rate strategy with losses roughly 4.5x larger than average
wins. It should not be promoted without broader walk-forward evidence and an
explicit loss-asymmetry guard.

## Causal Bucket Findings

For the relaxed candidate:

- `price=0.75_0.90`: `61` trades, `55` wins, `6` losses, `+26.7701` PnL.
- `price=0.50_0.75`: `13` trades, `9` wins, `4` losses, `-0.6756` PnL.
- `price=gte_0.90`: `9` trades, `8` wins, `1` loss, `-0.2055` PnL.
- `volatility=0.40_0.80`: `2` trades, `1` win, `1` loss, `-4.6270` PnL.
- `reversion=gte_3`: `14` trades, `12` wins, `2` losses, `+0.3655` PnL.
- `reversion=1_2`: `15` trades, `15` wins, `0` losses, `+14.3916` PnL.
- `zone=early`: `54` trades, `45` wins, `9` losses, `+10.6330` PnL.
- `zone=primary`: `27` trades, `25` wins, `2` losses, `+13.7569` PnL.

Interpretation: the profitable daily result is concentrated in specific
price/volatility/reversion regimes, while lower-price entries and moderate-vol
samples show loss asymmetry. This supports the strict rejection rather than live
promotion.

## Follow-Up Implemented

After this run, robust promotion gained configurable loss-asymmetry gates:

- `--min-profit-factor`;
- `--min-payoff-ratio`;
- `--max-worst-loss-to-avg-win`.

`strategy-builder rolling-history` now passes conservative defaults to robust
promotion: profit factor `>= 1.20`, payoff ratio `>= 0.20`, and
worst-loss / average-win `<= 6.0`. PBO, neighbor stability, and fold gates remain
the stronger blockers on this May 24 run.

CLI probe: tightening `--min-payoff-ratio` to `0.30` rejects the relaxed May 24
candidate directly with `payoff_ratio 0.2232 below minimum 0.3000`.

## Next Engineering Move

Add an optional causal-bucket veto for buckets with enough trades and negative
PnL, then rerun an atomic rolling-history sweep across the freshest complete
multi-day May window, processing one parquet at a time and deleting it after each
hour.

## Causal-Bucket Veto Implemented

Robust promotion now supports:

- `--min-causal-bucket-trades`;
- `--min-causal-bucket-pnl`.

`strategy-builder rolling-history` enables the veto with
`--min-causal-bucket-trades 10 --min-causal-bucket-pnl 0`.

CLI probe on the May 24 reports, with PBO and neighbor gates relaxed so the new
gate is isolated, rejected the candidate because:

- `price=0.50_0.75` had `13` trades and `-0.6756` PnL.

That is the exact repeated-regime weakness the diagnostics exposed.

## Atomic Multi-Day Validation

Attempted window: `2026-05-22T00:00:00Z..2026-05-24T23:00:00Z`.

- PMXT hour preflight reported `72 / 72` remote hours available.
- The first May 22 fold produced zero target BTC 5m PMXT events, so the run
  stopped as a data-coverage failure, not strategy evidence.
- Raw PMXT parquets retained after the failed attempt: `0`.

Fresh usable window: `2026-05-23T00:00:00Z..2026-05-24T23:00:00Z`.

- Folds: `6` feed-forward 8h windows.
- Comparable variants: `96` per fold.
- Raw PMXT parquets retained after run: `0`.
- Cache dir after run: `0B`.
- Report artifacts retained: `18M`.

Strict robust promotion rejected the May 23-24 run because no variant passed all
hard gates. The leading family was rejected by:

- daily trades below the configured `15` minimum on at least one fold;
- causal-bucket vetoes, especially `minutes_remaining=1_2` and
  `price=gte_0.90`.

Relaxed diagnostic-only selection, not production-valid:

- strategy: `all_c0.35_z0.90_e0.03_ev-1.00_p0.10-0.90_..._fbL2..._mk`;
- total PnL: `+69.4403`;
- trades: `164`;
- worst fold PnL: `+1.9629`;
- median fold PnL: `+13.9875`;
- PBO: `0.300` over `20` combinatorial splits;
- median OOS percentile: `0.7708`;
- neighbor-positive rate: `80.95%` over `35` neighbors;
- fill rate: `74.89%`;
- profit factor: `1.7198`;
- payoff ratio: `0.2253`;
- worst-loss / average-win: `4.8360`.

Negative causal buckets for the relaxed selection:

- `minutes_remaining=1_2`: `19` trades, `15 / 4` wins/losses, `-6.1115` PnL.
- `price=gte_0.90`: `16` trades, `14 / 2` wins/losses, `-2.1980` PnL.
- `volatility=0.40_0.80`: `5` trades, `4 / 1` wins/losses, `-2.8988` PnL.

Interpretation: the candidate is materially closer than the May 24-only run
because PBO and neighbor stability improved, but it still should not be
promoted. A production-safe next search should treat the causal-bucket finding
as a hard design constraint: avoid the last two minutes, avoid near-`0.90`
entries unless there is fresh contrary evidence, and require the same
feed-forward veto checks on every future candidate.
