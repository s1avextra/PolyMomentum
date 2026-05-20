# Adaptive Strategy Finder Plan - 2026-05-20

## Research Takeaways

- Use rolling walk-forward validation, not one static backtest. Recent trading
  validation work stresses strict out-of-sample windows, realistic transaction
  costs, position constraints, and complete information-set discipline:
  https://arxiv.org/abs/2512.12924
- Window size is itself a parameter. The 2026 Bitcoin walk-forward study shows
  that intraday strategy results can depend materially on train/test window
  length and that a final independent out-of-sample test is needed:
  https://arxiv.org/abs/2602.10785
- Treat staleness as concept drift. ADWIN-style adaptive windows compare older
  and newer stream segments and shrink the retained window when the mean changes:
  https://www.cs.upc.edu/~gavalda/papers/adwin06.pdf
- Performance-aware drift detectors are the right first layer for us because
  the deployed object is a trading policy whose health is visible through fill
  rate, win rate, PnL/trade, breaker state, latency, and oracle agreement:
  https://arxiv.org/abs/2203.11070
- Drift detection should be conservative in live trading. A survey of drift
  monitoring methods highlights that ADWIN is useful, but block-level and
  independence-test methods need larger samples; our first production version
  should use session-level performance gates and only graduate to richer tests
  when the paper/live sample grows:
  https://www.frontiersin.org/journals/artificial-intelligence/articles/10.3389/frai.2024.1330257/full

## Production Design

1. Scout candidates quickly with `eval-cache --grid`, isolated by timing zone.
2. Validate candidates with full L2 `harness-sweep --zone-mode`.
3. Aggregate-promote only the same parameter hash across independent windows.
4. Replay/paper the promoted artifact through the live path.
5. After each paper/live session, run `strategy-builder audit`.
6. If adaptive drift is `warn`, keep the current artifact active but launch a
   fresh rolling re-scout on the dev box.
7. If adaptive drift is `fail`, freeze live promotion or reduce to paper-only
   until a fresh artifact passes aggregate promotion, replay, and diagnostics.

## Current Implementation

- `strategy-builder plan` now accepts `--zone-mode` and propagates it into fast
  scout sweeps and full L2 harness sweeps.
- The generated plan now includes:
  - `adaptive_health_audit`
  - `adaptive_rescout_trigger`
- `strategy-builder audit` now adds `adaptive.drift` checks when a promotion
  artifact and paper/replay session are supplied.
- The first drift detector is performance-aware and intentionally simple:
  - insufficient resolved forward sample -> `warn`
  - breaker trip, system errors, negative forward PnL, hard win-rate decay, or
    hard expectancy decay -> `fail`
  - softer win-rate or expectancy decay -> `warn`
  - otherwise -> `ok`

## Next Upgrade

Once paper/live produces enough resolved sessions, add an ADWIN/Page-Hinkley
style rolling detector over per-trade PnL and per-trade win/loss outcomes. Keep
that detector as an alert and re-scout trigger, not as an automatic live
parameter mutator.
