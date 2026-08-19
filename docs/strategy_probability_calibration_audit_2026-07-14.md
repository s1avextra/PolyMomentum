# Strategy probability-calibration audit — 2026-07-14 to 2026-07-15

## Verdict

The strategy improved materially, but it did **not** reach A+ promotion quality.

- Best supported research candidate at measured latency: one-hour realized-volatility floor `0.30`.
- Strict 42-fold aggregate at `202 ms`: `102` trades, `79W/23L`, `+$13.54`, profit factor `1.114`.
- Hard failures: Wilson lower bound `0.6843 < 0.70`, profitable reports `19 < 20`, and maximum five-report loss burst `4 > 2`.
- Stability failure: first 21 folds `+$17.88`; last 21 folds `-$4.34`.
- A fresh joint CLOB + Binance RTDS + official Chainlink RTDS capture is now available. Seventeen markets pass the unchanged resolution gates; only seven also have the full causal one-hour signal warm-up, producing one trade (`1W`, `+$1.09`).

Final posture: **research-only / no promotion / live trading remains off**. The result remains profitable after retesting at measured p99 latency and has positive fresh-window evidence, but neither result has enough distributed support or tail stability for A+.

The July 15 continuation tested the recommended probability-calibration mechanism and did not change that verdict:

- Four market-anchored logit arms completed all `42` strict folds at `202 ms`; none passed A+.
- The best arm made `+$16.49` over `102` trades, but missed Wilson (`0.6950`), profitable-report (`19`), five-report loss-burst (`4`), and second-half (`-$1.70`) gates.
- Every calibrated arm was worse than the executable market probability on both Brier score and log loss.
- A prior-only search over `14,508` causal policies found zero passers. The top policy retained only `57` trades and still had a four-loss burst.

The rejected calibration knob was removed from production, replay, and monitoring code. Its evidence is retained so the same hypothesis is not rediscovered and mistaken for a promotion candidate.

The final July 15 check removed the remaining selection-bias ambiguity by exporting the strategy's pre-edge opportunity set from exact replay. Across all `42` strict folds at `202 ms`, the harness captured `10,609` one-second observations from `631` terminal conditions without submitting an order. A strictly prior-fold calibrator scored `34` folds / `498` conditions. It was slightly worse than the executable market on Brier score (`-0.000145` market-minus-calibrated), effectively tied on log loss (`+0.000017`), reversed sign between chronological halves, and had 95% condition-bootstrap intervals crossing zero on both metrics. This mechanism is therefore rejected before strategy integration; the A+ verdict remains unchanged.

## A+ contract used for this audit

The candidate had to satisfy all gates without lowering them:

| Gate | Requirement |
|---|---:|
| Trades | `>= 80` |
| Wilson 95% win-rate lower bound | `>= 0.70` |
| Aggregate PnL | `> 0` |
| Profitable eligible reports | `>= 20` |
| Eligible reports | `>= 20` |
| Worst fold PnL | `>= -$13` |
| Left-tail CVaR | `>= -$8` |
| Losing reports in any five-report window | `<= 2` |
| Payoff ratio | `>= 0.30` |
| Worst loss / average win | `<= 3.5` |

Fresh fully resolved coverage and a latency assumption at least as conservative as current measured p99 are additional promotion prerequisites.

## Kept strategy adjustment

An optional `decision_volatility_floor` was added to `StrategyVariant` and applied identically in:

- the event-driven backtest harness;
- cached live replay;
- the live decision pipeline.

The effective decision volatility is:

```text
sigma_decision = max(sigma_realized_1h, configured_floor)
```

The default is zero and is omitted from serialized JSON, so existing strategy artifacts and hashes retain their prior behavior. Non-finite or invalid volatility inputs fail closed. The live path deliberately uses the same one-hour realized-volatility source as replay; Deribit IV is not substituted.

The fresh-window audit also exposed a validation-model error: the harness previously used one BTC tape for both causal signal generation and market resolution. That made it impossible to reproduce the live Binance signal clock while resolving against the official Chainlink source. `harness-sweep` now accepts a separate `--settlement-btc-csv`; signal volatility and momentum use the signal tape, while terminal resolution and breaker accounting use the settlement tape. Coverage is checked over the exact selected contracts, with a full extra hour required only for the causal signal warm-up. Both source roles and their provenance are written into the report manifest.

This is supported by the volatility-regime rationale in Corsi's HAR-RV work, while remaining much simpler than adding a new forecast model: [A Simple Long Memory Model of Realized Volatility](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=626064).

## Strict 42-fold results at measured 202 ms

All folds used executable L2 fills, current Polymarket fees, `$8` maximum total exposure, and feed-forward windows. Polymarket's current crypto fee formula is documented as `C × feeRate × p × (1-p)`, with a `0.07` taker fee rate and zero maker fee: [Polymarket fees](https://docs.polymarket.com/trading/fees).

| Vol floor | Trades | W / L | Wilson LB | PnL | Profit factor | Profitable / eligible | CVaR | Loss burst | Last 21 PnL |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `0.30` | 102 | 79 / 23 | 0.6843 | +$13.54 | 1.114 | 19 / 39 | -$4.97 | 4 | -$4.34 |
| `0.35` | 89 | 68 / 21 | 0.6661 | +$10.91 | 1.100 | 18 / 35 | -$4.95 | 5 | -$10.28 |
| `0.40` | 82 | 62 / 20 | 0.6531 | +$6.46 | 1.063 | 17 / 35 | -$4.93 | 5 | -$1.99 |

All three retain positive aggregate PnL, but none passes A+. Floor `0.30` is the best supported point because it has the highest support, Wilson bound, PnL, profit factor, and profitable-report count. It still misses three hard gates and remains negative in the second half. Its probability estimate is also worse calibrated than the executable market price on both Brier score (`0.17497` versus `0.16820`) and log loss (`0.54150` versus `0.51323`).

Evidence: [`20260714_volatility_floor_strict42_latency202_exact_aggregate.json`](../deploy/promotions/evidence/strategy_registry/20260714_volatility_floor_strict42_latency202_exact_aggregate.json).

## July 15 market-anchored logit experiment

The follow-up tested the smallest calibration family that preserved an explicit identity endpoint:

```text
p_alpha = logistic(logit(p_market) + alpha * (logit(p_model) - logit(p_market)))
```

`alpha = 1` is the existing option-model probability and `alpha = 0` is the executable market probability. This is related to one-parameter logit/temperature calibration, which is often an effective low-variance baseline: [On Calibration of Modern Neural Networks](https://proceedings.mlr.press/v70/guo17a.html). The identity endpoint matters because a calibrator can otherwise make an already useful probability worse; beta-calibration research calls out that failure mode directly: [Beta calibration](https://proceedings.mlr.press/v54/kull17a.html).

Four support-preserving arms were selected for exact testing. All used volatility floor `0.30`, current fees, executable L2 FOK fills, `$8` total exposure, and `202 ms` latency. The coefficient/edge pairs were diagnostic full-window choices, not promotion-safe fitted parameters.

| Model weight | Min edge | Trades | W / L | Wilson LB | PnL | Profitable / eligible | CVaR | Loss burst | Last 21 PnL |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `0.525` | `0.035` | 102 | 80 / 22 | 0.6950 | +$16.49 | 19 / 39 | -$5.08 | 4 | -$1.70 |
| `0.575` | `0.040` | 101 | 79 / 22 | 0.6922 | +$15.36 | 19 / 39 | -$5.09 | 4 | -$1.67 |
| `0.650` | `0.045` | 102 | 80 / 22 | 0.6950 | +$16.35 | 19 / 39 | -$5.08 | 4 | -$0.86 |
| `0.775` | `0.055` | 101 | 78 / 23 | 0.6814 | +$10.57 | 19 / 39 | -$5.08 | 4 | -$2.79 |

All `42` report, trade, and feature artifacts were present; every data manifest was complete, every report used `202 ms`, and no report had zero replay events.

The calibration objective itself failed:

| Model weight | Fair Brier | Market Brier | Fair log loss | Market log loss |
|---:|---:|---:|---:|---:|
| `0.525` | 0.16341 | 0.16223 | 0.50371 | 0.50019 |
| `0.575` | 0.16508 | 0.16286 | 0.50792 | 0.50101 |
| `0.650` | 0.16455 | 0.16215 | 0.50807 | 0.50002 |
| `0.775` | 0.17289 | 0.16736 | 0.52980 | 0.51085 |

An executed-trade-only screen initially made `alpha=0.575` look stronger: `90` projected trades, Wilson `0.7182`, and `+$29.18`. Exact replay instead produced `101` trades, Wilson `0.6922`, and `+$15.36`. Lowering the edge threshold changes the opportunity set, so filtering trades executed by the old threshold cannot estimate the counterfactual. Exact event replay is authoritative; the subset result is rejected as selection-biased.

The exact implementation also caught and corrected a safety-semantics issue before the final run: the stale-price edge cap must remain based on the raw option-model edge, not the shrunken calibrated edge. This prevented calibration from masking a raw stale-edge condition. Once the mechanism failed exact replay, all calibration runtime and parity plumbing was removed.

Evidence:

- [`strict 42-fold aggregate`](../deploy/promotions/evidence/strategy_registry/20260715_probability_calibration_support_grid_strict42_latency202_exact_aggregate.json)
- [`rejected variant definitions`](../deploy/promotions/evidence/strategy_registry/20260715_probability_calibration_rejected_variants.json)
- [`fresh seven-market replay`](../deploy/promotions/evidence/strategy_registry/20260715_probability_calibration_support_grid_fresh7_latency202_report.json)

### Prior-only policy result

The causal selector evaluated `14,508` policies using only prior folds to choose each next-fold policy. No candidate passed. The top result required `book_age=lte_100ms` and `book_spread=0.01_0.03`, with a prior-toxic veto on `utc_hour=16`:

| Metric | Feed-forward result | Gate |
|---|---:|---:|
| Trades | 57 | 80 |
| Wilson lower bound | 0.7264 | 0.70 |
| PnL | +$28.96 | > 0 |
| Profitable / eligible reports | 17 / 26 | 20 / 20 |
| Worst fold | -$5.08 | >= -$13 |
| CVaR | -$3.84 | >= -$8 |
| Loss burst | 4 | <= 2 |
| Payoff ratio | 0.305 | >= 0.30 |
| Worst loss / average win | 3.36 | <= 3.5 |

The selector improves conditional win quality but fails support, profitable-report breadth, and clustered-loss stability. It is not eligible for exact policy replay or promotion. Evidence: [`20260715_probability_calibration_causal_policy_search.json`](../deploy/promotions/evidence/strategy_registry/20260715_probability_calibration_causal_policy_search.json).

### Opportunity-level calibration cohort

The executed-trade screen could only reject weak ideas; it could not estimate the opportunity set created by a different probability. A harness-only diagnostic path now exports the first candidate per condition and UTC second after every non-edge gate and before the final EV, stale-edge, and minimum-edge checks. Capture runs use an impossible edge threshold and fail if they submit any order, so trade state cannot truncate later counterfactual observations. Ordinary harness and live decision paths do not allocate diagnostic candidates.

The strict export passed the data-quality contract:

| Check | Result |
|---|---:|
| Folds / complete manifests | `42 / 42` |
| Replay latency | `202 ms` |
| Opportunity rows | `10,609` |
| Terminal conditions | `631` |
| Duplicate condition-seconds | `0` |
| Conditions crossing fold boundaries | `0` |
| Terminal-direction conflicts | `0` |
| Outcome-mapping mismatches | `0` |
| Invalid probabilities | `0` |
| Order attempts / trades | `0 / 0` |

Repeated seconds from one condition share terminal information, so fitting and scoring give each terminal condition total weight one. Scoring begins only after eight prior folds. For every next fold, executable-market identity, a market-logit intercept, and bounded market-to-model logit shrinkage compete on the last two strictly prior folds across a fixed ridge grid; the winner is then refit on all prior folds.

| Condition-weighted score | Executable market | Raw option fair | Prior-only calibrator | Market minus calibrator |
|---|---:|---:|---:|---:|
| Brier | `0.142733` | `0.148076` | `0.142877` | `-0.000145` |
| Log loss | `0.457394` | `0.474753` | `0.457377` | `+0.000017` |

The calibrator used fair-value shrinkage on `16` scored folds, market identity on `12`, and a market intercept on `6`. That adaptive choice still failed the predeclared stability contract:

- first half market-minus-calibrated Brier / log loss: `-0.001496 / -0.003417`;
- second half: `+0.001174 / +0.003369`;
- 95% condition-bootstrap Brier interval: `[-0.002039, +0.001700]`;
- 95% condition-bootstrap log-loss interval: `[-0.005736, +0.005620]`.

Support is adequate, but Brier, chronological-half, and bootstrap gates fail. No coefficient is added to strategy runtime code, and exact profitability replay is not warranted for this calibration family.

A post-hoc direction interaction was also allowed to compete under the same prior-fold selection because the primary diagnostics showed opposite UP/DOWN signs. It was selected on `14` of `34` scored folds but made both proper scores worse overall: market-minus-calibrated Brier `-0.000800` and log loss `-0.003296`. The first half was positive and the second half sharply negative, while both bootstrap intervals crossed zero. Because the feature was chosen after inspecting these folds, it could not have supplied promotion evidence even if positive; the negative result rejects it outright.

Evidence:

- [`opportunity calibration summary`](../deploy/promotions/evidence/strategy_registry/20260715_probability_opportunity_calibration_strict42.json)
- [`post-hoc direction-interaction rejection`](../deploy/promotions/evidence/strategy_registry/20260715_probability_opportunity_direction_interaction_diagnostic.json)
- [`executed notebook`](notebooks/strategy_probability_opportunity_calibration_2026-07-15.ipynb)

### Diagnostics rejected before runtime implementation

Report- and trade-native screens were used only to reject weak mechanism families cheaply:

- `1,121` sizing rules: zero A+ passers; predicted PnL could rise, but the four-loss burst remained.
- Prior-loss sentinel: reducing the burst to two collapsed support to nine trades; supported settings retained burst three or four.
- Train-only logit intercept: no supported feed-forward pass and the loss burst remained four.
- `3,396` one- and two-condition causal macro guards: zero passers; the best retained 82 trades and `+$51.80` but still had a three-loss burst.
- `601`–`604` causal feature filters per calibration arm: zero passers; the best supported filters still had a three-loss burst or failed payoff geometry.

These screens were not promoted to exact claims. They identify the remaining problem as clustered outcome risk, not position-size choice or a simple static context veto.

## Earlier diagnostic sweep at 128 ms

The broader `128 ms` sweep below established the volatility-floor plateau and motivated the narrow measured-latency retest. It is diagnostic evidence, not current promotion evidence.

| Vol floor | Trades | Wilson LB | PnL | Profitable / eligible | Worst fold | CVaR | Loss burst | Last 21 PnL |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `0.00` | 131 | 0.6674 | -$37.09 | 15 / 40 | -$9.96 | -$5.87 | 5 | -$9.39 |
| `0.20` | 120 | 0.6479 | -$30.82 | 15 / 40 | -$9.80 | -$5.86 | 5 | -$9.39 |
| `0.25` | 112 | 0.6624 | -$6.63 | 18 / 40 | -$9.80 | -$5.69 | 5 | -$9.39 |
| `0.30` | 107 | 0.6879 | +$22.87 | 19 / 39 | -$5.13 | -$4.98 | 5 | -$8.03 |
| `0.35` | 95 | 0.6856 | +$35.19 | 19 / 35 | -$5.13 | -$4.96 | 5 | -$8.84 |
| `0.375` | 89 | 0.6661 | +$27.23 | 18 / 35 | -$5.13 | -$4.96 | 5 | -$13.77 |
| `0.40` | 89 | 0.6661 | +$26.86 | 18 / 35 | -$5.13 | -$4.94 | 5 | -$8.83 |
| `0.50` | 62 | 0.6212 | +$20.72 | 19 / 33 | -$5.17 | -$5.13 | 4 | -$4.68 |
| `0.60` | 42 | 0.5397 | +$8.65 | 17 / 29 | -$5.21 | -$5.15 | 3 | -$10.01 |
| `0.80` | 21 | 0.5491 | +$42.55 | 10 / 13 | -$5.21 | -$4.01 | 2 | +$11.92 |

The curve has a real profitable region from `0.30` through `0.60`, so the mechanism is not an isolated one-point spike. It still fails promotion because the improvement is concentrated in the first half and does not eliminate clustered losses. The attractive `0.80` tail is too sparse to be evidence.

Calibration at floor `0.35` in this earlier run was mixed:

- model Brier `0.17556` versus market Brier `0.17932` — slightly better;
- model log loss `0.54696` versus market log loss `0.54007` — slightly worse.

Evidence:

- [`20260714_volatility_floor_strict42_exact_aggregate.json`](../deploy/promotions/evidence/strategy_registry/20260714_volatility_floor_strict42_exact_aggregate.json)
- [`20260714_volatility_floor_lower_strict42_exact_aggregate.json`](../deploy/promotions/evidence/strategy_registry/20260714_volatility_floor_lower_strict42_exact_aggregate.json)

## Feed-forward policy result

A full causal policy search evaluated `214,737` candidates over the strict reports. The strongest static policy looked good in-sample, but its feed-forward result failed:

| Metric | Feed-forward result | Gate |
|---|---:|---:|
| Trades | 82 | 80 |
| Wilson lower bound | 0.6795 | 0.70 |
| PnL | +$7.17 | > 0 |
| Profitable / eligible reports | 14 / 29 | 20 / 20 |
| Worst fold | -$5.12 | >= -$13 |
| CVaR | -$4.46 | >= -$8 |
| Loss burst | 4 | <= 2 |
| Payoff ratio | 0.303 | >= 0.30 |
| Worst loss / average win | 3.73 | <= 3.5 |

This rejects the tempting static fit and confirms that the remaining loss cluster is not solved by the existing causal regime tags.

## Mechanisms tested and rejected

The exact failing fold class was probed before changing gates.

- Settlement-basis exits destroyed too many winners.
- Minimum price, UTC session, edge cap, direction, impulse, and z-score filters did not generalize.
- Static book-pressure and neutral-pressure filters looked promising in report-native slices but remained negative on exact replay.
- Recent midpoint run-up guards at `0.02`, `0.05`, and `0.08` retained the known failures.
- Longer realized-volatility horizons removed support and left retained PnL negative.
- A post-loss cooldown from `2` through `24` hours never improved the floor `0.30` Wilson bound or four-loss burst. The best positive cooldown, two hours, reduced PnL from `+$13.54` to `+$7.68`, support from `102` to `99` trades, and Wilson from `0.6843` to `0.6754`.
- A Cont-style temporal order-flow imbalance gate was implemented with live/replay parity, tested at the measured `151 ms` latency, and then removed after failing its predeclared four-fold probe. Cont, Kukanov, and Stoikov motivate OFI as a high-frequency price-impact signal: [The Price Impact of Order Book Events](https://arxiv.org/abs/1011.6402). In this strategy, however, permissive thresholds produced `9` trades, `5W/4L`, `-$13.03`; a zero threshold worsened the result to `6` trades, `2W/4L`, `-$17.63`. Every toxic fold stayed negative.
- A fixed `2 s` same-direction entry-persistence recheck retained only `4 / 17` known winners while retaining `5 / 10` known losses. It was rejected before implementation.
- A fee-inclusive executable profit lock (`+$0.50` arm, `+$0.10` floor) improved folds `29–41` by `+$2.91` but made three terminal winners negative after `202 ms` execution.
- A single executable loss cut exactly `30 s` before close improved folds `29–41` by `+$7.08` and turned two negative folds positive, but made two terminal winners negative. No neighboring close horizon was tested.

Rejected OFI evidence is retained without leaving unused runtime code:

- [`20260714_ofi_failfast_latency151_exact_aggregate.json`](../deploy/promotions/evidence/strategy_registry/20260714_ofi_failfast_latency151_exact_aggregate.json)

## Latency and fresh-data audit

The current joint Dublin VPS capture ran for 105 minutes and covered 22 rotating BTC five-minute markets / 44 outcome tokens. It recorded CLOB data plus both RTDS reference streams in the same run.

| Metric | Result |
|---|---:|
| Raw CLOB frames | 3,491,300 |
| Timestamped CLOB events | 3,277,948 |
| Missing event timestamps | 0% |
| Record overhead p99 | 1 ms |
| Source-to-receive p50 | 11 ms |
| Source-to-receive p95 | 62 ms |
| Source-to-receive p99 | 202 ms |
| Source-to-receive p99.5 | 294 ms |
| Maximum stream receive gap | 1,419 ms |
| Recommended replay latency | 202 ms |
| Verdict | `MEASURED_LATENCY_RETEST_REQUIRED` |

Evidence: [`20260714_forward_latency_joint_rtds_fresh.json`](../deploy/promotions/evidence/strategy_registry/20260714_forward_latency_joint_rtds_fresh.json).

The reference collector recorded `6,299` Binance RTDS ticks with a one-second maximum observation gap and `6,201` official Chainlink RTDS ticks with an eight-second maximum gap. It had no reconnects or websocket errors. All 20 markets in the initial resolution block were terminal and matched Chainlink, with no oracle disagreement, but the combined block correctly failed the unchanged five-second internal-gap gate. Excluding only the three windows that crossed those known gaps (`1784041200`, `1784043300`, and `1784045400`) yielded four contiguous segments totaling 17 terminal markets. Each segment passes source provenance, boundary coverage, internal-gap, terminal-ground-truth, and settlement-alignment gates with maximum Chainlink gaps of two to four seconds.

Resolution evidence: [`segment 1`](../deploy/promotions/evidence/strategy_registry/20260714_joint_rtds_fresh_resolution_segment_01.json), [`segment 2`](../deploy/promotions/evidence/strategy_registry/20260714_joint_rtds_fresh_resolution_segment_02.json), [`segment 3`](../deploy/promotions/evidence/strategy_registry/20260714_joint_rtds_fresh_resolution_segment_03.json), and [`segment 4`](../deploy/promotions/evidence/strategy_registry/20260714_joint_rtds_fresh_resolution_segment_04.json).

Only seven of those markets occur after the full one-hour Binance signal warm-up. Their feature-complete replay used `1,951,145` PMXT events, `202 ms` latency, Binance RTDS for causal decisions, and Chainlink RTDS for settlement. All three floors made the same single winning trade for `+$1.09`. This is valid positive evidence but statistically non-discriminating and far below A+ support. Evidence: [`20260714_volatility_floor_fresh7_latency202_report.json`](../deploy/promotions/evidence/strategy_registry/20260714_volatility_floor_fresh7_latency202_report.json).

The July 15 calibration grid was also replayed on the same seven feature-complete markets. All four arms made one winning trade: weights `0.575`, `0.650`, and `0.775` each made `+$1.09`; weight `0.525` made `+$0.96`. This confirms replay compatibility, not calibration quality—the sample is still one trade per arm. Evidence: [`20260715_probability_calibration_support_grid_fresh7_latency202_report.json`](../deploy/promotions/evidence/strategy_registry/20260715_probability_calibration_support_grid_fresh7_latency202_report.json).

The July 13 PMXT target-window probe contained zero target events. A previous report incorrectly marked that run complete because it counted requested hours instead of replayed events. The report manifest now records actual `events_seen`, marks zero-event PMXT evidence incomplete, and rejects legacy reports that contain variants but zero replay events.

## What is good, bad, and blocked

Good:

- The volatility floor turns the full baseline from `-$37.09` into a supported profitable plateau.
- Floor `0.30` remains profitable at measured p99 latency, with acceptable payoff ratio, worst-fold PnL, CVaR, and loss asymmetry.
- The setting is causal, minimal, default-off, and identical across backtest, replay, and live decision code.
- Exact replay caught both the invalid executed-trade counterfactual and the stale-edge safety-semantics issue.
- Opportunity capture now measures non-executed candidates without changing ordinary harness or live decisions, and all `42` strict exports passed leakage and label-integrity checks.
- Cross-venue capture now records causal chosen-token logit changes and direction-aligned BTC returns at `5 s`, `30 s`, and `60 s`; `10,534 / 10,609` rows (`99.293%`) have all six features without imputation.
- Rejected calibration code was removed; only its reproducible evidence remains.
- Fresh replay now preserves the real Binance-signal / Chainlink-settlement split and records both source roles in its manifest.
- Empty fresh windows can no longer masquerade as complete evidence.
- Stateful entry and exit hypotheses are now screened against exact target-token L2 paths, full-size visible-bid quotes, both fees, and measured order latency before they can earn an engine implementation.

Bad:

- The last half remains negative.
- Four- or five-loss report clusters survive every supported floor.
- The Wilson lower bound never reaches `0.70` at adequate support.
- The internal fair probability is more overconfident and less calibrated than the executable market price.
- Every market-anchored logit arm remained worse calibrated than the market and negative in the second half.
- The unbiased prior-only calibrator is also unstable: marginal aggregate results reverse sign between halves, and a direction interaction worsens both proper scores.
- The pre-registered cross-venue meta-label makes both proper scores worse overall and separately in both chronological halves; it also raises the estimated win probability more on baseline losses than wins on average.
- The entry-persistence and two executable-exit screens all improved or removed some losses only by sacrificing known winners; fixed 5/15/30/60/90-second marks expose no clean rescue boundary.
- Static and prior-only causal fits do not survive the support and loss-cluster gates together.
- The valid fresh replay contains only one trade per arm and cannot distinguish either floors or calibration weights.

Blocked:

- No candidate passes the unchanged A+ gate.
- The measured-latency strict retests reject all volatility-floor and logit-calibration finalists.
- Fresh official reference data now exists, but the feature-complete sample is too small for promotion.
- The opportunity-complete cohort now rejects market-anchored probability calibration as the next mechanism.
- Fixed-horizon cross-venue path confirmation is rejected before strategy integration and does not earn exact guard replay.
- The registered entry-persistence, profit-lock, and late-loss-cut lifecycle mechanisms are rejected before integration; tuning adjacent thresholds on the same folds is blocked as in-sample search.
- Any live or shadow promotion remains blocked until a new causal mechanism passes older folds, holdout folds, and a larger fresh fully resolved block.

## July 15 cross-venue path confirmation result

The harness-only opportunity row now includes the chosen token's causal logit change and the direction-aligned BTC log return at fixed `5 s`, `30 s`, and `60 s` horizons. BTC coverage is complete. Token coverage ranges from `99.378%` to `99.849%`; rows without sufficient causal L2 history fail closed and are not imputed. The complete-case cohort contains `10,534` observations from `628` conditions.

One pre-registered ridge-logistic meta-label used executable-market log-odds as an offset and all six path features as bounded standardized corrections. For each scored fold, ridge was selected on the final two strictly prior folds, refit on all prior folds, and applied once to the next fold. Each terminal condition received total weight one.

The model is rejected before strategy integration:

| Cohort | Conditions | Market Brier | Meta Brier | Brier improvement | Market log loss | Meta log loss | Log-loss improvement |
|---|---:|---:|---:|---:|---:|---:|---:|
| Overall | 496 | 0.142191 | 0.143788 | -0.001597 | 0.456199 | 0.459561 | -0.003363 |
| First 17 scored folds | 244 | 0.135292 | 0.137845 | -0.002553 | 0.439294 | 0.443698 | -0.004403 |
| Last 17 scored folds | 252 | 0.148870 | 0.149542 | -0.000672 | 0.472567 | 0.474922 | -0.002355 |

The 95% condition-bootstrap intervals cross zero for both market-minus-meta metrics: Brier `[-0.004264, +0.000791]` and log loss `[-0.011677, +0.004032]`. In the descriptive exact-trade match, the model raised probability by `+0.0430` on losses versus `+0.0246` on wins across `85` out-of-sample trades. It therefore does not isolate the known loss cluster, and no exact guard replay is warranted.

Evidence: [`20260715_cross_venue_path_confirmation_strict42.json`](../deploy/promotions/evidence/strategy_registry/20260715_cross_venue_path_confirmation_strict42.json), [`executed notebook`](notebooks/strategy_cross_venue_path_confirmation_2026-07-15.ipynb), and [`technical report`](reports/strategy_cross_venue_path_confirmation_2026-07-15.html).

## July 15 stateful lifecycle result

The next registered mechanism, a `2 s` pending-entry recheck, was evaluated on the ten negative folds inside the maximum loss-burst window. It required the same direction and a fresh executable edge of at least `0.07`; missing recheck data failed closed. The screen matched all `27` exact trades but retained only `9`: `4 / 17` winners and `5 / 10` losses. Removing thirteen known winners to avoid five losses is not a viable strategy adjustment, so no runtime code or exact replay followed.

The same `40` exact positions across folds `29–41` were then joined to their target-token PMXT v2 book paths. Marks used full-size visible bids, both entry and exit fees, the signal-time worst-price FOK limit, and the measured `202 ms` order latency.

| Screen | Baseline tail PnL | Counterfactual PnL | Profitable folds | New losing winners | Result |
|---|---:|---:|---:|---:|---|
| `+$0.50` → `+$0.10` profit lock | -$16.07 | -$13.15 | 5 / 13 | 3 | reject |
| loss cut at close minus `30 s` | -$16.07 | -$8.99 | 5 / 13 | 2 | reject |

Both exits improved aggregate tail PnL, but both violated the predeclared no-new-loss contract. The fixed post-fill marks explain why a rescue deadline also fails: at `60 s`, `7 / 29` available terminal winners and `7 / 10` losses were negative; at `90 s`, three winners were still negative. The path has real mean-reversion overlap, not a clean causal boundary.

Evidence: [`20260715_strategy_lifecycle_causal_screens.json`](../deploy/promotions/evidence/strategy_registry/20260715_strategy_lifecycle_causal_screens.json), [`executed notebook`](notebooks/strategy_lifecycle_causal_screens_2026-07-15.ipynb), and [`technical report`](reports/strategy_lifecycle_causal_screens_2026-07-15.html).

## July 15 trade-print passive-entry result

The fixed passive-entry diagnostic was run on the exact failing tail before any engine integration. It reused the strategy's existing decision-time `best ask - 0.01` price, the measured `202 ms` insertion latency, and the configured `3 s` maker timeout. Share size was held to the original taker size. Visible size at the bid became frozen queue ahead; only unique `SELL` prints depleted it, cancellations received no credit, and missing tape failed closed.

The retained tape covered `26 / 40` tail trades. Seven fixed limits would have crossed the ask by arrival and rejected post-only. Of the remaining nineteen eligible orders, only six had defensible full fills (`31.6%`; `23.1%` of covered trades): four winners and two losses. Every inferred fill came from an explicit price-through `SELL` print; no order cleared its frozen queue using prints at the limit. On the covered subset, maker-or-skip improved `-$7.81` to `-$5.24`, but remained unprofitable and left only six observations. The mechanism is rejected before a full `42`-fold replay, and no timeout, price-offset, or queue-discount neighbor is searched.

This also closes the current synthetic maker path as promotion evidence: replay still uses a fixed `0.65` Bernoulli fill probability, distilled `trade` events are skipped by the engine reader, and the configured live maker timeout has no cancel implementation. None of those paths should be enabled on the strength of this diagnostic.

Evidence: [`20260715_trade_print_passive_entry_tail.json`](../deploy/promotions/evidence/strategy_registry/20260715_trade_print_passive_entry_tail.json).

## July 15 Binance futures exogenous-data result

The repository had no retained futures-flow or positioning history, so the official Binance USD-M daily archive was audited before implementation. Fourteen BTCUSDT one-minute kline archives and fourteen five-minute metrics archives spanning May 28 through June 10 were downloaded on the dev box and verified against Binance's adjacent SHA-256 checksum files. Features were joined only from records closed before the decision; the positioning screen added a full five-minute publication buffer.

Two fixed, non-overlapping exogenous families were scored with the same strictly prior-fold, condition-weighted meta-label contract:

| Screen | Complete rows | Overall Brier improvement | Overall log-loss improvement | First half | Second half | Result |
|---|---:|---:|---:|---|---|---|
| direction-aligned futures taker flow, prior `1 m` + `5 m` | 10,609 | -0.000785 | -0.002098 | negative | negative | reject |
| lagged top-position crowding + `15 m` open-interest change | 10,555 | +0.001700 | +0.005975 | positive | negative | reject historical integration |

Taker flow is actively harmful: it worsens both proper scores in both halves and raises probability more on the exact known losses than winners. Positioning is the first exogenous feature family to improve aggregate proper scores, but the gain is entirely unstable: the second half reverses to `-0.000786` Brier and `-0.002597` log-loss improvement, both 95% condition-bootstrap intervals cross zero, and the mean correction on twenty scored known losses remains positive (`+0.00168`). No strategy code or exact profitability replay follows.

For future diagnosis only, the post-audit model is frozen after selecting ridge `10.0` on folds `41–42` and refitting all `42` folds. Its two standardized feature coefficients are nearly zero (`-0.00372`, `+0.00127`), which independently confirms that production integration is not justified.

Evidence: [`20260715_binance_futures_exogenous_screens.json`](../deploy/promotions/evidence/strategy_registry/20260715_binance_futures_exogenous_screens.json), [`executed notebook`](notebooks/strategy_binance_futures_exogenous_screens_2026-07-15.ipynb), and [`technical report`](reports/strategy_binance_futures_exogenous_screens_2026-07-15.html).

## July 15 frozen-positioning feasibility result

The planned prospective positioning score was rejected before a full fresh replay because its hard loss gate is not mathematically attainable. The frozen model adds an intercept of `+0.073136` log-odds. Both standardized features are clipped to `[-5, 5]`, so their largest possible combined negative contribution is only:

```text
5 × (|-0.003717| + |+0.001270|) = 0.024935 log-odds
```

The minimum admissible correction is therefore `+0.048201` log-odds. Since the logistic function is strictly increasing, every admissible row receives a probability above the executable market price. Every losing condition must consequently receive a positive correction, contradicting the predeclared requirement that mean correction on losses be non-positive. More prospective observations cannot repair that structural failure. The positioning family is closed rather than refit or transformed again on the same historical folds.

The bounded fresh-source attempt also failed closed. Gamma returned `863 / 864` July 11–13 five-minute markets, and the checksum-verified one-second Binance tape was contiguous. The first two atomically processed PMXT hours nevertheless contained zero events for the 24 expected candle conditions. The report manifest correctly recorded `complete=false`, `row_count=0`, and `PMXT replay contained zero target events`; both session-downloaded parquets were deleted after their single filtered pass. HTTP `200` archive availability is not target-condition coverage.

A monolithic VPS fallback is also unsafe with the current deployment. The installed binary does not expose `record-btc-books`, the VPS had `26 GB` free during the read-only audit, and the observed July 14 recorder rate extrapolates to about `102.4 GB` of raw frames over 72 hours. Any authorized forward capture must therefore use a separate measurement-capable tool binary plus bounded segments that are converted and verified before deleting only their session-owned frame logs. The production service binary does not need to be replaced or restarted. The exact fail-closed procedure is in the [`segmented forward capture runbook`](segmented_forward_capture_runbook_2026-07-15.md).

Evidence: [`20260715_frozen_positioning_forward_feasibility_audit.json`](../deploy/promotions/evidence/strategy_registry/20260715_frozen_positioning_forward_feasibility_audit.json), [`executed notebook`](notebooks/strategy_frozen_positioning_forward_feasibility_2026-07-15.ipynb), and [`technical report`](reports/strategy_frozen_positioning_forward_feasibility_2026-07-15.html).

## July 15 binary-complement coherence pre-registration

One genuinely new mechanism is now frozen before the next forward outcomes exist. It uses the simultaneous causal state of both fully collateralized outcome books and records two residuals: `chosen_mid + opposite_mid - 1` and `chosen_microprice + opposite_microprice - 1`. The microprice uses the existing three-level depth calculation. This is not the existing ask-side outcome overround, chosen-token pressure, or temporal OFI.

The fixed rule requires both absolute residuals to be no greater than two venue ticks; missing or invalid paired books fail closed. The threshold comes from venue discretization and will not be tuned. The existing 42-fold history is explicitly ineligible for scoring this rule. A blinded July 18 power amendment, registered before any forward metric was generated, raised the fixed block floor from `100` to `750` terminal official-source-aligned conditions. The historical baseline fires on only `102 / 631` conditions, so the old floor implied about 16 candidates and four losses and was not decision-useful. Each of two disjoint forward blocks must now contain at least `750` conditions, retain at least `70%` of baseline candidates and `90%` of winners, remove at least `30%` of losses, and reach Wilson `>= 0.70`. The strategy rule and all rate gates remain unchanged. Only two passes earn one exact replay variant. No live or ordinary strategy decision changed.

Evidence: [`pre-registration contract`](../deploy/promotions/evidence/strategy_registry/20260715_binary_complement_coherence_preregistration.json), [`blinded power amendment`](../deploy/promotions/evidence/strategy_registry/20260718_binary_complement_blinded_power_amendment.json), and [`research note`](strategy_binary_complement_preregistration_2026-07-15.md).

## Next concrete run

Stop endpoint transforms, static feature vetoes, lifecycle thresholds, passive-entry tuning, and derivatives-feature mining on the same historical block. Probability calibration, cross-venue path confirmation, delayed entry, executable exits, trade-print maker entry, futures taker flow, and futures positioning have now each failed at least one predeclared causal gate. The frozen positioning follow-up is additionally impossible under its own loss contract.

The next bounded run is a forward-data prerequisite, not another model fit:

1. Obtain explicit authorization before installing any PolyMomentum VPS tool or starting a collector.
2. Install a separate measurement-capable binary under `/opt/polymomentum/tools`; do not replace or restart the production service binary, and do not touch peer private directories or shared peer-owned files.
3. Capture CLOB, Binance RTDS, and official Chainlink RTDS in bounded segments sized below the current free-disk limit.
4. Convert and validate each session-owned segment before deleting only its raw frames; preserve cross-segment boundary gaps explicitly.
5. Preserve the frozen `binary_complement_coherence_v1` rule without inspecting the old 42-fold outcomes or tuning its two-tick threshold.
6. Score it without refit on at least `750` new resolved opportunity conditions, then repeat unchanged on a second disjoint block.
7. Materialize one exact replay variant only if both blocks pass their predeclared gates.

Any exact survivor must still pass the unchanged A+ contract and remain positive in both chronological halves. Until sufficient fresh PMXT/Gamma or owned CLOB, Binance, and Chainlink data are available, the strategy remains profitable but research-only and `LIVE_TRADING_OFF` is the correct state.
