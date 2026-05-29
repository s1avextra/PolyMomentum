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

## Max-Price Guard Probe

To avoid a fresh download before deciding the next search shape, the existing
May 23-24 reports were filtered to variants with `max_price <= 0.75`.

Strict robust promotion still rejected every candidate. The leading filtered
family failed because:

- daily trades fell below the `15` minimum on at least one fold;
- worst fold PnL was negative (`-9.6919`);
- `edge=0.03_0.07` had `20` trades and `-2.9610` PnL.

Relaxed diagnostic-only selection for this subset:

- strategy: `all_c0.35_z0.50_e0.03_ev-1.00_p0.10-0.75_..._fbL2..._mk`;
- total PnL: `+65.7501`;
- trades: `118`;
- worst fold PnL: `-9.6919`;
- median fold PnL: `+13.5854`;
- PBO: `0.350`;
- neighbor-positive rate: `74.24%`;
- profit factor: `1.5638`;
- payoff ratio: `0.3786`;
- worst-loss / average-win: `2.6956`.

Interpretation: simply lowering max price is not enough. It avoids the
near-`0.90` weakness but exposes a low-edge/reversion weakness and a negative
walk-forward fold. The next full sweep should add search dimensions for a
two-minute settlement guard and stricter edge/causal-bucket constraints instead
of only narrowing the price band.

## Causal Guard Profile

Implemented a guarded profile, `a_plus5m_causal_guard`, with:

- hard settlement cutoff: `2.0` minutes;
- settlement margin guard: `2.0` minutes;
- edge grid: `0.07,0.10`;
- max-price grid: `0.75,0.85`;
- causal-bucket veto still enabled during robust promotion.

To support this, `settlement_cutoff_minutes` became a sweep dimension for both
strategy sweep paths. This is the correct primitive for "do not enter in the
last two minutes"; `settlement_guard_minutes` is only a margin-distance guard.

Atomic May 23-24 validation:

- window: `2026-05-23T00:00:00Z..2026-05-24T23:00:00Z`;
- folds: `6` feed-forward 8h windows;
- comparable variants: `192` per fold;
- raw PMXT parquets retained after run: `0`;
- cache dir after run: `0B`;
- report artifacts retained: `27M`.

Strict robust promotion rejected the run. First blocker:

- PBO: `0.650`, above max `0.500`.

With PBO relaxed only, the next blockers were:

- the leading wider-price family had an underfilled or negative fold;
- `reversion=gte_3` had repeatable negative PnL in several high-trade
  candidates.

Relaxed diagnostic-only selection, not production-valid:

- strategy: `all_c0.40_z0.50_e0.10_ev-1.00_p0.10-0.75_sc2.0_..._fbL2..._mk`;
- total PnL: `+82.1354`;
- trades: `89`;
- worst fold PnL: `+4.5310`;
- median fold PnL: `+11.5653`;
- PBO: `0.650`;
- median OOS percentile: `0.4453`;
- neighbor-positive rate: `84.40%`;
- fill rate: `65.93%`;
- profit factor: `2.0161`;
- payoff ratio: `0.4419`;
- worst-loss / average-win: `2.3123`;
- negative causal buckets: none.

Per-fold trade counts for this relaxed selection were `14, 11, 14, 19, 16, 15`.
That is why it still fails production-grade evidence despite attractive PnL:
two folds are below the `15`-trade minimum, total trades are one below the
strict `90` aggregate minimum, and PBO remains too high.

## Reversion Cap Implemented

Added a feed-forward `max_reversion_count` gate to `ZoneConfig` and exposed it
as `--max-reversion-count` in both sweep paths. A value of `0` disables the cap;
`a_plus5m_causal_guard` now sets `--max-reversion-count 2`.

This directly targets the new repeatable blocker (`reversion=gte_3`) without
using future outcomes. The decision skip reason is `high_reversion_count`.

One-hour smoke validation on the largest May 24 hour
(`2026-05-24T23:00:00Z`) passed through the real atomic replay path:

- variants: `192`;
- event rows loaded: `4,349,625`;
- replay time: `159.04s`;
- raw PMXT parquets retained after smoke: `0`;
- temporary smoke cache: removed.

The smoke report confirmed generated variant names include `_rv2_` and
serialized strategy params carry `zone_config.max_reversion_count = 2`.

Next required validation: rerun the full May 23-24 six-fold atomic sweep with
the reversion cap active. The profile is wired and tested, but the full
reversion-capped promotion result has not yet replaced the pre-cap result.
