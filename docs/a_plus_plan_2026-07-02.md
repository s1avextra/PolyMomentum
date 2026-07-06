# A+ Plan - 2026-07-02

## Current State

Verdict: A- research system, not live-ready.

Operating stance: KEEP_REPLAY_RESEARCH. Live trading remains blocked until a
candidate passes fresh resolved replay, measured-latency replay, settlement
agreement, tail-risk gates, paper/shadow parity, and live preflight.

Validated strengths:

- Historical PMXT replay and strict-tail validation infrastructure exist.
- Forward exact-slug BTC Up/Down CLOB recording exists.
- Recorded forward books can be converted into distilled replay-cache format.
- Converted forward captures can be refreshed with terminal Gamma outcomes.
- Live BTC signal uses Binance, Bybit, and OKX websockets plus Deribit IV.
- CLOB book prices are taken from Polymarket market websocket data.
- CTF/Gamma verification can correct provisional local outcomes after close.
- Chainlink Data Streams is optional until paid feed IDs are available.

Current blockers:

- No promoted strategy exists in the registry.
- The strict May 28 to June 10 tail loop found no live candidate.
- Clustered losses, CVaR, and loss-burst behavior are the strategy blockers.
- The forward sample is only a smoke: 3 resolved 5-minute markets.
- The July 1 `366 ms`, July 3 `325 ms`, and desktop `408 ms` CLOB delay
  artifacts are superseded as strategy policy. They remain diagnostics only
  because the bot runs from the Dublin VPS, not the desktop.
- The July 4 Dublin VPS segmented aggregate has `675,595` CLOB delay samples,
  raw p99 `97 ms`, raw p99.5 `128 ms`, warm-10 p99 `96 ms`, warm-10 p99.5
  `128 ms`, recorder overhead p99 `1 ms`, and negative-delay rate `0`.
  Current fail-closed replay policy is `128 ms` until repeated VPS captures or
  a worse capture replaces it. Evidence:
  `deploy/promotions/evidence/strategy_registry/20260704_vps_dublin_latency_aggregate.json`.
- The July 5 Dublin VPS uninterrupted capture has `712,139` CLOB delay samples,
  p99 `62 ms`, p99.5 `84 ms`, recorder overhead p99.5 `1 ms`, max
  whole-stream receive gap `1,843 ms`, zero missing event timestamps, zero
  negative-delay samples, and zero websocket reconnects/errors across `1800 s`.
  Evidence:
  `deploy/promotions/evidence/strategy_registry/20260705_vps_dublin_reconnect_forward_latency_audit.json`.
  It is accepted latency evidence, but it is only one clean uninterrupted run,
  so it does not lower the fail-closed `128 ms` promotion policy yet.
- The July 5 `128 ms` first-tail-cluster retest of
  `a_plus5m_down_reversion_guard_confidence` avoided losses but failed
  promotion by becoming too sparse: `6` trades across five 8-hour folds, `0`
  losses, `+7.13485` PnL, and primary-zone trade share `0.8333` above the
  `0.7000` maximum. Evidence:
  `deploy/promotions/evidence/strategy_registry/20260705_latency128_tail_cluster1/`.
  This is not a live candidate; the next challenger must widen participation
  under low exposure rather than flattening the bad cluster.
- The July 5 low-exposure remap diagnostics rejected that widening path on the
  first 8-hour tail-cluster fold. Baseline `a_plus5m_tail_low_exposure` produced
  `2` trades, `1` loss, and `-4.06889` PnL; denying the exact losing regime
  worsened to `-4.43706`; denying `book_pressure=strong_positive` still failed
  at `-4.18424`. Evidence:
  `deploy/promotions/evidence/strategy_registry/20260705_low_exposure_remap_diagnostics/`.
  The stable bad shape is low-price/high-edge primary-zone down entries, so A+
  needs learned chronological causal-policy search or a new signal family, not
  manual micro-regime whack-a-mole.
- The July 5 low-exposure causal-policy search did find a static hypothesis
  (`require book_age=lte_100ms`, `deny book_imbalance=strong_positive`) with
  `6/6` static wins and `+7.19137` PnL, but direct rolling-history replay
  rejected it immediately on fold 1: `2` trades, `1` loss, `-4.18424` PnL, with
  the loss moving to `book_imbalance=negative`. Evidence:
  `deploy/promotions/evidence/strategy_registry/20260705_low_exposure_policy_search_diagnostics/`.
  A+ needs replay-integrated policy generation, not static causal filtering.
- The July 6 causal-policy eligible-report gate implements the first
  replay-credit guard: `--min-oos-eligible-reports` is opt-in and defaults to
  disabled, but A+ policy-search runs can now reject one-active-report
  hypotheses before promotion credit. Rerunning the same three low-exposure
  reports with `--min-oos-eligible-reports 2` changed the old `ok=true` thin
  result into `ok=false`; the best broader-coverage candidate had `2` eligible
  reports but failed with `6` trades, `4` wins, `2` losses, `-4.60148` PnL, and
  worst report/CVaR `-5.13834`. Evidence:
  `deploy/promotions/evidence/strategy_registry/20260706_policy_search_min_eligible_gate/`.
- `record-btc-books` now reconnects/resubscribes after websocket connect,
  close, subscription, or read failures. The July 5 run used a
  measurement-capable `/tmp` probe built from commit `a3aa73d`; production
  binary replacement remains a separate deployment decision.
- Latency policy must now follow
  [latency_measurement_machine_research_2026-07-03.md](latency_measurement_machine_research_2026-07-03.md):
  desktop captures are diagnostic only; current production replay policy comes
  from the Dublin VPS where the bot runs. London measurements matter only if
  execution moves there.
- Binance BTCUSDT is only a proxy for Chainlink-settled BTC markets.
- Without Chainlink feed IDs, settlement-source alignment must remain false.

## A+ Definition

A+ means the project has a candidate that is good enough to run live only after
operator approval. It does not mean "live by default".

Required final state:

- `live_ready=true` in a fresh registry audit.
- At least one active promoted artifact with durable local evidence.
- Fresh PMXT or forward exact-slug replay passes feed-forward gates.
- Measured-latency replay passes at the current latency policy.
- Tail metrics pass: no hidden clustered-loss family, CVaR above threshold, and
  loss bursts inside the budget.
- Delayed Gamma/CTF outcomes agree with provisional local outcomes often enough
  to trust the strategy labels; any disagreement class is understood.
- If Chainlink feed IDs are still unavailable, live order placement remains
  blocked by settlement-alignment policy and the project can only reach A+
  shadow-ready, not A+ live-ready.

## Phase 1 - Data Foundation

Goal: turn the current one-smoke forward pipeline into repeated resolved data.

Work:

- Run bounded `record-btc-books` captures for fresh BTC 5-minute windows.
- Convert each capture with `convert-recorded-btc-books`.
- After close, run `finalize-recorded-btc-books` to attach terminal Gamma
  outcomes and CTF-verifiable labels.
- Keep Binance/Bybit/OKX as low-latency proxy signal data.
- Keep PMXT as the historical replay source and use shared/distilled caches when
  available.
- Do not force-remap PMXT when exact token/condition overlap is absent.

Success criteria:

- At least 3 independent forward capture batches.
- At least 100 resolved 5-minute BTC markets across batches before strategy
  claims become meaningful.
- Zero unknown-token or unknown-market skips in converted forward captures.
- Every capture has a resolution manifest with terminal/pending counts.
- Proxy BTC tape covers every market open and close timestamp.

Failure action:

- If forward captures are sparse or miss tokens, fix the recorder/subscription
  path before touching strategy thresholds.

## Phase 2 - Latency Policy

Goal: remove the old optimistic 50 ms assumption.

Work:

- Run `forward-latency-audit` on every forward capture.
- Set the effective replay latency to observed p99/p99.5 plus a safety buffer.
- Re-run historical and forward replay at the clean current policy, `128 ms`,
  and replace that policy only with repeated clock-safe VPS captures. Do not use
  desktop or old smoke captures as strategy policy.
- Use the July 5 uninterrupted VPS artifact as the current latency retest
  witness: p99 `62 ms`, p99.5 `84 ms`, but promotion still gates at `128 ms`
  until at least two more comparable VPS captures confirm the lower tail.
- Record latency verdicts next to replay artifacts.

Success criteria:

- Stream timestamp coverage is complete.
- No negative-delay samples.
- Token-gap checks pass for active/high-event tokens; sparse future-window
  tokens are skipped by the active-window gate rather than failing an otherwise
  clean latency capture.
- Strategy-builder and harness reports use the measured latency policy.
- No candidate is promoted from a lower-latency-only pass.

Failure action:

- If latency jumps materially, retest candidates at the worse latency rather
  than weakening gates.

## Phase 3 - Strategy Candidate Search

Goal: find a strategy that survives real latency and tail risk, not just gross
PnL.

Work:

- Start from the current `a_plus5m` and tail profiles, but treat them as seeds.
- Search smaller, interpretable grids first: zone, confidence, z, edge, max
  price, settlement guard, reversion count, and microstructure limits.
- Keep maker/taker paths separate until fill evidence supports merging them.
- Require all selector families to share the same CVaR and loss-burst contract:
  causal-policy, multi-guard, adaptive-direction, and adaptive-mode.
- Inspect the exact losing folds before tightening or relaxing any global gate.

Success criteria:

- Candidate passes on older diagnostic windows.
- Candidate passes on newest available fully resolved windows.
- Candidate has positive OOS PnL after fees.
- Worst window is non-negative or explicitly bounded by a documented circuit
  rule.
- Profit factor, payoff ratio, Wilson lower bound, PBO, and neighbor stability
  pass robust promotion gates.
- CVaR and loss-burst gates pass across selector families.

Failure action:

- If only total PnL passes, reject. Clustered losses are still a blocker.

## Phase 4 - Forward Replay And Label Integrity

Goal: prove that fresh forward data agrees with the backtest hypothesis.

Work:

- Run `live-replay` and/or `harness-sweep` on converted forward captures.
- Compare signal rows, order attempts, simulated fills, and final resolutions.
- Separate three label types:
  - `polymarket_terminal`: delayed truth for actual market winner.
  - `ctf`: on-chain verification when available.
  - `proxy_btc`: Binance/Bybit/OKX reference only, never official settlement.
- Keep `CANDLE_SETTLEMENT_ALIGNMENT_READY=false` unless official-source evidence
  exists.

Success criteria:

- Enough resolved forward decisions to validate the candidate outside PMXT.
- Zero causality violations.
- Zero unresolved fills that should have resolved.
- No actionable Gamma/CTF disagreement class remains unexplained.
- Settlement-source mismatch is explicit in the report, not hidden.

Failure action:

- If forward replay produces zero trades, debug selectivity and timing before
  broadening the strategy grid.

## Phase 5 - Registry Promotion

Goal: make promotion mechanical and fail-closed.

Work:

- Package exactly one candidate artifact after Phases 1 to 4 pass.
- Run robust promotion, zone audit, registry audit, and replay validation.
- Ensure the artifact encodes runtime safety floors and does not rely on local
  defaults.
- Archive every report under `deploy/promotions/evidence/strategy_registry/`.

Success criteria:

- Registry audit reports `ok=true`.
- `live_candidate_count >= 1`.
- No missing or non-durable paths.
- Strategy artifact hash is stable.
- Promotion report includes latency policy, data manifest, tail metrics, and
  settlement-source status.

Failure action:

- If registry is clean but `live_ready=false`, do not bypass the gate. The
  missing gate becomes the next task.

## Phase 6 - Shadow/Paper Parity

Goal: prove runtime wiring without risking capital.

Work:

- Run bounded paper/shadow mode only after replay has proven the same behavior.
- Keep `VENUE=paper_only` unless explicitly changed by the operator.
- Keep `CANDLE_SETTLEMENT_ALIGNMENT_READY=false` without Chainlink feed IDs.
- Verify CLOB book subscription, order intent creation, simulated fill path,
  CTF/Gamma resolution, breaker state, alerts, and session diagnostics.

Success criteria:

- Paper/shadow decisions match replay decisions for the same market state.
- Provisional local outcomes are corrected cleanly by CTF/Gamma.
- Breaker and risk state persist across restart.
- Alerts work.
- No live order is sent.

Failure action:

- If paper behavior diverges from replay, fix parity before any live preflight.

## Phase 7 - Live Readiness

Goal: get to a safe live switch, not automatic live trading.

Work:

- Run live preflight with wallet, pUSD allowance, POL, CLOB V2 signing, CLOB
  reconciliation, disk, logs, alerting, kill switch, and artifact checks.
- If Chainlink feed IDs remain unavailable, declare A+ shadow-ready only.
- If Chainlink feed IDs become available, run Chainlink shadow collection and
  settlement alignment before setting `CANDLE_SETTLEMENT_ALIGNMENT_READY=true`.

Success criteria for A+ shadow-ready:

- Strategy and replay gates pass.
- Shadow runtime parity passes.
- All live order gates remain fail-closed.
- Operator can safely continue collecting evidence.

Success criteria for A+ live-ready:

- Everything in A+ shadow-ready passes.
- Chainlink settlement-source alignment passes on repeated resolved markets.
- CLOB V2 signing and reconciliation pass.
- Wallet and allowance checks pass.
- Registry audit reports `live_ready=true`.

## Immediate Next Run

Run the next forward-data batch, not another abstract strategy sweep:

1. Capture fresh BTC 5-minute CLOB books for a bounded window.
2. Convert to distilled replay cache.
3. Finalize after close with Gamma terminal outcomes.
4. Attach proxy BTC tape for coverage checks.
5. Audit latency with a clock-skew-safe method from the Dublin VPS, then rerun
   replay at the measured policy. Current clean retest policy: `128 ms`.
6. If replay has trades, inspect loss classes and only then launch a targeted
   candidate sweep.

This keeps work moving while Chainlink paid stream IDs are unavailable, and it
prevents us from promoting a strategy that only wins under old latency or stale
data.
