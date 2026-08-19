# Zone Concentration Gate Research - 2026-06-24

## Question

The locked May candidate is profitable across seven fresh folds, but strict aggregate
promotion rejects it because the dominant timing zone carries about 75.16% of trades,
above the default `max_zone_trade_share=0.70`. The practical question is whether this
is a real problem or an overly strict threshold.

## Why Early Concentration Matters

Early concentration is not automatically bad. It can mean the edge genuinely lives
near the beginning of a 5-minute market. The problem is promotion confidence: if most
trades come from one timing zone, then the result is more exposed to one microstructure
condition, one data-quality quirk, one timestamp alignment issue, or one market regime.

This is the exact family of risk described in backtest-overfitting literature:

- Carr and Lopez de Prado, "Determining Optimal Trading Rules without Backtesting",
  warn that calibrating trading rules by historical simulation contributes to
  backtest overfitting and later underperformance.
  Source: https://arxiv.org/abs/1408.1159
- Koshiyama and Firoozye, "Avoiding Backtesting Overfitting by Covariance-Penalties",
  frame the problem as parameter choice over historical data producing misleading
  results, with defenses grouped around data snooping, overestimated performance,
  and cross-validation evaluation.
  Source: https://arxiv.org/abs/1905.05023

Our gate is a simple but useful implementation of the same principle: a deployable
artifact should not only be profitable, it should avoid looking like a narrow timing
artifact unless we intentionally classify it as a zone-specific strategy and validate
that regime separately.

## Current Configuration Finding

There was a configuration drift:

- Core `promote`, `aggregate-promote`, and `robust-promote` defaults use
  `max_zone_trade_share=0.70`.
- The strategy-builder generated all-zone robust promotion commands with
  `max_zone_trade_share=0.85`.

That made the same candidate look closer to promotable depending on which path created
the command. The all-zone builder path now uses `0.70`, matching the core gate.
Zone-specific research still permits `1.0` because an early-only search is explicitly
not claiming cross-zone robustness.

## Threshold Policy

Keep `0.70` as the A+ all-zone promotion default.

Relaxing to `0.80` or `0.85` should only be allowed when all of these are true:

1. The candidate is intentionally documented as timing-zone-specialized.
2. Feed-forward replay shows positive PnL in non-dominant zones or a deliberate
   risk reduction when outside the dominant zone.
3. The same candidate passes timestamp-causality and resolution checks.
4. Neighbor-parameter diagnostics show the result is not an isolated spike.
5. The relaxed threshold is written into the artifact as a visible research choice.

## Implemented Follow-Up

Added `polymomentum-engine experiment zone-audit`.

It reads one or more experiment reports, aggregates only variants present in every
report, and prints a JSON audit with aggregate plus per-fold zone concentration. This
lets us compare `0.70`, `0.80`, and `0.85` explicitly before changing promotion policy.

Recommended next command shape:

```bash
polymomentum-engine experiment zone-audit \
  --report <fold-1-report.json> \
  --report <fold-2-report.json> \
  --max-zone-trade-share 0.70
```

## Next Steps

1. Run `zone-audit` on the locked May candidate at `0.70`, `0.80`, and `0.85`.
2. If `0.80` is attractive, require an early-specialist artifact label and do not mark
   it A+ until non-dominant-zone behavior is explicitly modeled.
3. Add search objectives that reward stable PnL across zones instead of only total PnL,
   so the builder naturally finds less brittle candidates.
4. Keep live blocked until a strict or explicitly zone-specialized artifact passes the
   full replay gate.
