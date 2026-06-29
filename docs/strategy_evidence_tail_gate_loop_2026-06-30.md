# Strategy Evidence And Tail Gate Loop - 2026-06-30

## Purpose

This loop fixed two production-readiness gaps in the strategy-finding system:

- Registry evidence could point at scratch or superseded paths, making later review unreproducible.
- Multi-guard search ranked candidates by aggregate OOS behavior but did not expose the same explicit fold-tail checks already used by causal-policy search.

The working rule remains backtest-first: use feed-forward backtest, live-replay, and archived diagnostics for strategy validation; reserve paper mode for venue wiring that offline replay cannot prove.

## Research Basis

- Polymarket CLOB docs describe a live order path where signed orders are placed against the CLOB and then move through acknowledgement, matching/fill, cancellation, or rejection states. Strategy replay should therefore preserve order intent, venue acknowledgements, fills, and terminal settlement rather than treating a decision as instant PnL.
- Polymarket fee docs make fees price-dependent around binary outcomes, so PnL gates must score actual fill economics and not only win rate.
- Expected shortfall/CVaR is a standard left-tail risk measure. For this project it is applied to chronological OOS fold PnL so a strategy cannot pass only because a few strong folds hide clustered losing folds.
- Feed-forward folds are mandatory. Any guard learned from a fold may only affect later folds, never the same fold that revealed the loss.

Primary references used:

- https://docs.polymarket.com/developers/CLOB/introduction
- https://docs.polymarket.com/trading/orders/overview
- https://docs.polymarket.com/trading/fees

## Implemented

### Durable Registry Evidence

New command:

```bash
polymomentum-engine strategy-builder evidence-export \
  --registry docs/strategy_registry.json \
  --out-dir deploy/promotions/evidence/strategy_registry/20260630_tail_loop_v4 \
  --manifest deploy/promotions/evidence/strategy_registry/20260630_tail_loop_v4_manifest.json \
  --rewrite-registry
```

Behavior:

- Copies top-level `artifact_path`, `metrics_path`, `evidence_paths`, and historical `events[].evidence_paths`.
- Writes archive files by atomic temp-file rename.
- Records byte size and SHA-256 in the manifest.
- Reports missing files without rewriting them.
- Deduplicates repeated source paths.
- Rewrites registry paths only for copied evidence when `--rewrite-registry` is set.

Current archive result:

- Archive directory: `deploy/promotions/evidence/strategy_registry/20260630_tail_loop_v4`
- Manifest: `deploy/promotions/evidence/strategy_registry/20260630_tail_loop_v4_manifest.json`
- Copied files: 38
- Missing files: 0

### Multi-Guard Tail Gates

New `strategy-builder multi-guard-search` fields:

```bash
--tail-alpha 0.20
--min-oos-cvar-pnl <threshold>
--loss-burst-lookback <reports>
--max-loss-burst-reports <count>
```

Every candidate now reports:

- `fold_forward.tail.sample_count`
- `fold_forward.tail.tail_count`
- `fold_forward.tail.cvar_pnl`
- `fold_forward.tail.loss_burst_lookback`
- `fold_forward.tail.max_loss_burst_reports`

Promotion meaning:

- `--min-oos-cvar-pnl` prevents a candidate with a severe fold-level left tail from passing.
- `--loss-burst-lookback` plus `--max-loss-burst-reports` rejects clustered losing folds.
- A zero `--max-loss-burst-reports` leaves the burst gate diagnostic-only.

## Verification

Passed:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Result:

- Rust formatting passed
- Clippy passed with warnings denied
- 314 unit/integration tests passed
- 0 failed

CLI smoke:

```bash
polymomentum-engine strategy-builder multi-guard-search \
  --report <chronological archived reports> \
  --min-train-reports 1 \
  --min-train-trades 1 \
  --min-oos-trades 1 \
  --min-oos-wilson-win-rate-lower 0.0 \
  --min-oos-total-pnl 0 \
  --min-oos-profitable-reports 0 \
  --min-worst-oos-pnl=-100 \
  --tail-alpha 0.50 \
  --min-oos-cvar-pnl=-100 \
  --loss-burst-lookback 2 \
  --max-loss-burst-reports 2 \
  --top 3
```

The smoke output included tail metrics and kept current candidates fail-closed on the known losing fold, which is the desired behavior.

## Current Verdict

The strategy infrastructure is stronger, but no current registry entry is live-ready. The registry remains intentionally fail-closed:

- Rejected strategies stay rejected.
- Questionable strategies stay research candidates.
- A candidate must pass known tail clusters, full chronological OOS windows, and freshest available resolved windows before promotion.

## Next Loop

1. Run `multi-guard-search` on the full chronological May28-Jun10 report set with strict tail gates.
2. Re-run on the freshest fully resolved windows using the same gates.
3. If no candidate passes, extend the search space around the observed failure mechanism, not around timestamps:
   - direction/regime interaction
   - settlement distance
   - reversion count
   - price bucket
   - execution mode and minimum order sizing
4. Archive every generated report through `strategy-builder evidence-export`.
5. Keep paper mode out of strategy validation unless the question is live venue connectivity, acknowledgements, rejects, websocket behavior, or process supervision.
