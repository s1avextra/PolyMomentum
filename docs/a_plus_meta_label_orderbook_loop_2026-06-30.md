# A+ Meta-Label And Orderbook Loop - 2026-06-30

## Objective

Implement the missing selective-prediction treatment as a feed-forward
meta-label risk gate, rerun the strategy-builder backtests, and decide whether
the next layer needs deeper orderbook features.

## Implementation

- Added optional causal-policy meta-label risk control to
  `strategy-builder causal-policy-search`.
- The gate is disabled by default with `--meta-label-min-support 0`.
- When enabled, the current fold is scored only after all aggregate prior gates
  pass. The gate then looks at active current full-regime buckets and compares
  them with strictly prior outcomes for the same bucket.
- Added optional sparse-context fallback:
  `--meta-label-max-generalization-terms N` builds prior-only broader causal
  tag combinations when the exact full-regime bucket lacks support.
- Each decision now emits `meta_label` diagnostics: exact/generalized bucket,
  support, loss rate, left-tail quantile, worst prior PnL, and flatten reason.

This is feed-forward: the current fold's PnL is not used to decide whether that
same fold trades. The current fold only contributes active pre-trade context and
signal occurrence.

## Validation

Commands run:

- `cargo test --manifest-path rust_engine/Cargo.toml causal_policy`
- `cargo test --manifest-path rust_engine/Cargo.toml strategy_builder`
- `cargo build --manifest-path rust_engine/Cargo.toml`

All passed.

## Backtest Evidence

Data window:

- May 28, 2026 through June 10, 2026
- 42 chronological 8-hour fold reports
- Same strict feed-forward gates used in the previous loss-cluster loop

Committed artifact:

- `deploy/promotions/evidence/strategy_registry/20260630_causal_policy_meta_label_summary.json`

Raw search dumps were generated locally, summarized into the committed artifact,
and not committed because full-rank JSON reports are too large for the registry.

Results:

- Strict exact meta-label gate: no pass. Top candidate had clean realized tail
  but only 10 OOS trades; max stored top-50 trades was 32.
- Mild exact meta-label full-rank: no pass. No candidate reached the 80-trade
  gate; max trades was 34.
- Generalized meta-label hard gate: no pass. No candidate reached the 80-trade
  gate; max trades was 38.
- Diagnostic-only meta-label reproduced the previous high-sample leader:
  96 trades, 80 wins, 16 losses, PnL 34.42, Wilson 0.746, worst fold -12.85,
  CVaR -8.18, payoff ratio 0.283, worst-loss/avg-win 3.82.
- Diagnostic exact buckets for that leader showed 83 active exact contexts
  across scored reports, 44 supported contexts, 39 unsupported contexts,
  worst prior bucket PnL -10.17, worst left-tail quantile -5.82, and max prior
  bucket loss rate 0.50.

## Verdict

The meta-label treatment is implemented correctly and is useful as diagnostics,
but it is not yet a promotion gate. As a hard gate it over-selects, removing too
much sample size before the strategy reaches the minimum trade count.

Current promotion state remains fail-closed: `live_ready=false`.

## Orderbook Depth Verdict

We do not need to jump straight to full deep-orderbook strategy search as the
next move. The immediate failure is sparse/unstable context learning: the bad
tails are still explained by existing causal buckets such as zone, direction,
price, edge, z, confidence, volatility, reversion, and minutes remaining.

However, we do need to expose deeper orderbook features before the next serious
A+ candidate:

- Current strategy microstructure uses spread, top-side depth, imbalance,
  microprice, and pressure.
- Backtest microstructure currently builds those features from top 3 levels.
- Session diagnostics record spread/depth/pressure aggregates, but strategy
  fold reports do not expose searchable orderbook buckets.
- A BookWalk taker fill model exists, but it is test-gated and not yet a fold
  diagnostic/search feature.

Next implementation should add orderbook-derived causal buckets to the fold
diagnostics before full L2 model complexity:

- spread bucket;
- min-side depth bucket;
- pressure/imbalance bucket;
- depth-at-size or bookwalk slippage bucket for the intended order size;
- book staleness/age bucket if available from feed timing;
- post-only-cross / maker-fill risk bucket where maker execution is used.

After those are in fold reports, rerun meta-label diagnostics. If losses cluster
by those orderbook buckets, then promote orderbook-aware selection. If they do
not, the next treatment should be adaptive regime/state modeling rather than
deeper L2 execution features.
