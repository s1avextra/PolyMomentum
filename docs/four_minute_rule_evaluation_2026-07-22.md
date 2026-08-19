# Four-minute rule evaluation

## Outcome

The linked rule produces a very high BTC terminal-direction hit rate, but it is not ready to integrate as a PolyMomentum strategy.

On 9,504 complete five-minute BTC windows after the source article's publication, the literal four-checkpoint rule produced 1,032 signals and 95.54% terminal-direction accuracy. The UTC-day block-bootstrap 95% interval was 93.91% to 96.91%. Up and down accuracy were 95.22% and 95.90%, respectively.

The preregistered public screen nevertheless failed because the protected fresh slice produced 98 signals against a fixed floor of 100. That floor is not lowered after observing the result. No strategy, paper, or live behavior changed.

## What was tested

The source is [“4 minute rule on polymarket: holy grail or trap?”](https://x.com/0xbobaaa/status/2036073103281803412), published March 23, 2026. Because the article does not publish code, the rule was frozen before evaluation as:

1. Align a BTC window to a UTC timestamp divisible by 300 seconds.
2. Observe BTC at offsets 0, 60, 120, 180, and 240 seconds.
3. Emit Up only when all four one-minute price changes are strictly positive.
4. Emit Down only when all four changes are strictly negative.
5. Score the prediction against the BTC price at the 300-second boundary versus the window open.

The analysis used 33 checksum-verified official Binance BTCUSDT one-second archives: April 16 through May 15 and July 11 through July 13. July 14 and later archives were not opened because those dates can overlap the protected binary-complement forward population.

## Results

| Measure | Observed |
|---|---:|
| Complete five-minute windows | 9,504 |
| Eligible four-minute signals | 1,032 |
| Signal fraction | 10.86% |
| Terminal-direction accuracy | 95.54% |
| Day-block bootstrap 95% interval | 93.91%–96.91% |
| Up accuracy | 95.22% |
| Down accuracy | 95.90% |
| Older signals / accuracy | 934 / 95.82% |
| Fresh pre-forward signals / accuracy | 98 / 92.86% |
| Longest losing streak | 3 |
| True fifth-minute continuation rate | 45.06% |

The article's $100 and $200 first-two-minute diagnostic rules scored 92.87% and 94.20%, but these were preregistered as report-only diagnostics and receive no promotion credit.

## Why the hit rate is not momentum alpha

The market resolves against the five-minute opening price. After four same-sign minutes, BTC has usually accumulated a large buffer from that open. In this sample the median directional buffer at the decision point was $77.67.

Only 45.06% of fifth-minute returns continued in the signal direction. There were 521 contract wins despite the fifth minute reversing or finishing flat—52.84% of all contract wins. The 95.54% figure therefore measures the chance that the final minute does not erase the full four-minute buffer; it does not show that the fifth-minute return itself has momentum.

## Why 51.02% is not the break-even threshold

A binary purchase breaks even when its realized win probability exceeds its executable cost per share. For a taker under the current crypto fee formula, that is:

`break_even_probability = fill_price + fee_rate × fill_price × (1 - fill_price)`

The fill price is the visible-depth VWAP after latency, not a displayed midpoint. Polymarket documents that buyers pay the ask, that market prices express implied probabilities, and that crypto taker fees depend on the share price. See [Prices & Orderbook](https://docs.polymarket.com/concepts/prices-orderbook), [Fees](https://docs.polymarket.com/trading/fees), and [Orderbook](https://docs.polymarket.com/trading/orderbook).

If the direction token costs approximately 0.95 after four same-sign minutes, a 95% hit rate can have zero or negative expectancy before slippage. The source article provides no entry-price distribution, order size, fee reconciliation, fill model, or reproducible bankroll path, so its 0.5% drawdown claim cannot be audited.

## Integration boundary

No runtime feature should be added under this registration. The preregistered screen failed its fresh-support floor, and directional accuracy alone cannot establish executable edge.

If this idea is revisited after the protected 750-condition block, it should become a distinct `late_window_buffer_mispricing_v1` hypothesis rather than a retuned momentum rule. That study should:

1. Capture the causal BTC-to-strike buffer and four-checkpoint path at offset 240 seconds.
2. Compare the direction token's executable ask and visible-depth VWAP with the existing volatility-based fair probability.
3. Apply at least 202 ms insertion latency and exact market fee metadata.
4. Use taker FOK execution, current order-size/exposure controls, and authoritative resolution identity.
5. Compare the existing `candle_momentum` baseline with the identical strategy requiring direction-matched four-minute consistency.
6. Require positive fee-adjusted expectancy, profit factor of at least 1.20, a positive UTC-day bootstrap lower bound, stability by direction and chronological window, and a maximum 5% drawdown.

The natural implementation point, if a disjoint study later authorizes it, is one default-off `four_minute_consistency` causal tag on `MomentumSignal`/`DecisionRegime`. The existing `SelectivityFilter` can then require that tag without creating a second execution stack.

## Reproduction

```bash
/Users/ttoomm/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3.12 \
  scripts/analyze_four_minute_continuation.py \
  --archive-dir /private/tmp/polymomentum-dvol-volatility-max-20260721/binance_1s \
  --evidence deploy/promotions/evidence/strategy_registry/20260722_four_minute_continuation_public_proxy.json \
  --snapshot deploy/promotions/evidence/strategy_registry/source_snapshots/20260722_four_minute_continuation_signals.jsonl.gz

/Users/ttoomm/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3.12 \
  scripts/validate_four_minute_continuation.py
```
