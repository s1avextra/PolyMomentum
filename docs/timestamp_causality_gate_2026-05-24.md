# Timestamp Causality Gate - 2026-05-24

Purpose: make a timestamp leak visible before any strategy can be treated as a
live candidate.

## Rule

Every executable order must satisfy:

```text
signal_source_ts_s <= decision_ts_s <= order_ts_s <= fill_ts_s < market_end_ts_s
```

Every recorded resolution must satisfy:

```text
resolution_ts_s >= market_end_ts_s
```

The session log now emits:

- `causality.order_timing` for paper, live, and live-replay order attempts.
- `causality.resolution_timing` for replay and paper/shadow resolutions.

## Command

```text
polymomentum-engine diagnostics causality <session.jsonl>
```

Useful strict gate:

```text
polymomentum-engine diagnostics causality <session.jsonl> \
  --max-clock-skew-s 0.5 \
  --max-post-end-fill-s 0 \
  --min-order-timings 1
```

The command exits non-zero on:

- `future_signal_source`
- `decision_before_market_start`
- `decision_after_market_end`
- `order_before_decision`
- `order_after_market_end`
- `fill_missing_order_timing`
- `fill_after_market_end`
- `resolution_before_market_end`

## Promotion Use

`strategy-builder audit` now runs this causality audit for every replay or
bounded integration session and fails the gate if `replay.causality` is not ok.
That makes timestamp causality a formal promotion requirement, not a manual
inspection step.
