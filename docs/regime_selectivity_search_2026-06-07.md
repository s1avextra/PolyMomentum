# Regime Selectivity Search - 2026-06-07

## Purpose

Build a feed-forward strategy discovery layer that can react to regime changes without
turning the bot into a timestamp-fit optimizer. The first production-useful layer is
a causal bucket search over the features already emitted by the harness:

- direction
- zone
- price bucket
- edge bucket
- z bucket
- confidence bucket
- volatility bucket
- reversion bucket
- minutes remaining bucket

This is intentionally simpler than an HMM/BOCPD model. It gives us an auditable
filter first, then a clean per-trade feature export for richer models later.

## Research Mapping

- Bayesian Online Changepoint Detection is useful for stale-strategy detection
  because it maintains an online posterior over the current run length. That is
  the right shape for future drift monitoring, not for immediate promotion from
  sparse fold data. Source: https://arxiv.org/abs/0710.3742
- Hidden/regime-switching models are useful when the data supports latent state
  inference, but they are easy to make laggy or lookahead-biased if smoothed with
  future observations. For now, use deterministic causal buckets, then add online
  regime probability only after per-trade features have enough samples.
- Meta-labeling/triple-barrier style thinking maps well to this project as a
  second-stage filter: the base strategy proposes trades, then a selector decides
  when to abstain. It should not be treated as a source of edge by itself.
  Source: https://en.wikipedia.org/wiki/Meta-Labeling
- White's Reality Check and PBO both warn against choosing the best candidate out
  of many correlated trials without accounting for data snooping. The new search
  therefore reports candidate count and fold-forward OOS stats, and selected
  rules must still be rerun through harness/live-replay before promotion.
  Sources: https://onlinelibrary.wiley.com/doi/10.1111/1468-0262.00152 and
  https://usekeel.io/learn/probability-backtest-overfitting
- Polymarket order/live parity still depends on market and user websocket events,
  signed limit orders, and post-only/taker behavior. This work stays offline and
  does not replace executable-order validation. Sources:
  https://docs.polymarket.com/developers/CLOB/orders/create-order and
  https://docs.polymarket.com/market-data/websocket/overview

## Implementation

Added:

- `strategy-builder selectivity-search`
- feed-forward candidate generation over one-dimensional causal bucket rules
- rule types:
  - `allow_only dimension=value`
  - `deny dimension=value` as the complement inside that same dimension
- OOS eligibility gate:
  - a candidate can score report `i` only if reports `< i` have enough trades
    and positive PnL for that same rule
- JSON output with gates, methodology, candidate count, aggregate stats, and
  fold-forward stats
- `harness-sweep --trade-features-json <path>`
  - writes compact per-trade feature rows
  - includes execution fields, BTC resolution fields, PnL, decision fields, and
    `DecisionRegime` causal tags
- tests proving:
  - a stable down-regime rule is selected feed-forward
  - a rule with bad prior folds is not promoted by a later lucky fold

## Validation

Targeted tests:

```bash
cargo test --manifest-path rust_engine/Cargo.toml strategy_builder
```

Result:

- 12 strategy-builder tests passed
- includes the two new selectivity no-lookahead tests

Real-report search input:

```text
/private/tmp/polymomentum_broader_gate_20260607_artifacts/rolling_may30_jun05_sparse_ok/reports/*.json
```

Strict command:

```bash
./rust_engine/target/debug/polymomentum-engine strategy-builder selectivity-search \
  --report /private/tmp/polymomentum_broader_gate_20260607_artifacts/rolling_may30_jun05_sparse_ok/reports/*.json \
  --output /private/tmp/polymomentum_selectivity_search_20260607.json \
  --min-train-reports 3 \
  --min-train-trades 10 \
  --min-oos-trades 20 \
  --min-oos-wilson-win-rate-lower 0.60 \
  --min-oos-total-pnl 0 \
  --min-oos-profitable-reports 5 \
  --min-worst-oos-pnl 0 \
  --top 12
```

Strict result:

- `ok=false`
- `report_count=16`
- `candidate_count=928`
- best whole-window/top price/reversion-like rules still have negative worst OOS
  report PnL
- direction-down rule is strong but fails strict zero-worst-fold gate:
  - fold-forward OOS: 40 trades, 38 wins, 2 losses
  - OOS win rate: 95.00%
  - Wilson lower: 83.50%
  - OOS PnL: +31.45925
  - eligible reports: 12
  - profitable reports: 10
  - losing reports: 2
  - worst report PnL: -4.32797

Relaxed diagnostic command:

```bash
./rust_engine/target/debug/polymomentum-engine strategy-builder selectivity-search \
  --report /private/tmp/polymomentum_broader_gate_20260607_artifacts/rolling_may30_jun05_sparse_ok/reports/*.json \
  --output /private/tmp/polymomentum_selectivity_search_relaxed_20260607.json \
  --min-train-reports 3 \
  --min-train-trades 10 \
  --min-oos-trades 20 \
  --min-oos-wilson-win-rate-lower 0.60 \
  --min-oos-total-pnl 0 \
  --min-oos-profitable-reports 5 \
  --min-worst-oos-pnl -5 \
  --top 8
```

Relaxed result:

- `ok=true`
- top passing rule:
  - variant:
    `all_c0.50_z0.70_e0.07_ev-1.00_p0.75-0.85_sc2.0_minrv1_rv2_sf10_sg2.0_ss0.00_ms1.00_md0_mp-1.00_fbL2d0.00z0.90tk_tk`
  - rule: `allow_only direction=down`
  - aggregate: 55 trades, 52 wins, 3 losses, +43.36658 PnL
  - fold-forward OOS: 40 trades, 38 wins, 2 losses, +31.45925 PnL
  - worst OOS report PnL: -4.32797

## Verdict

This is not A+ production evidence yet. It is a materially better strategy
discovery loop because it found the same intuitive market-state clue in a
feed-forward way: the current reversion guard works much better when the BTC
candle direction bucket is down, and it degrades when the direction bucket is up.

Current grade for the strategy discovery layer: A-

Reason:

- A-level: no-lookahead search, causal feature source, strong direction-down
  hypothesis, per-trade feature export ready for richer models.
- Not A+: strict worst-fold gate still fails, sample size remains small, and the
  selector is one-dimensional rather than an interaction/model layer.

## Next Steps

1. Rerun the top `allow_only direction=down` and `deny direction=up` rules as
   first-class harness variants, not just post-hoc bucket filters.
2. Use `--trade-features-json` on the selected fold runs, then build a per-trade
   selector that only trains on prior folds.
3. Test two interaction families:
   - `direction=down AND zone=primary`
   - `direction=down AND z not 1.1_1.5`
4. Add BOCPD-style online drift diagnostics on the feature rows after the
   selected rule has enough resolved trades.
5. Keep promotion fail-closed until strict worst-fold PnL is non-negative or the
   live risk policy explicitly accepts a bounded per-window loss.
