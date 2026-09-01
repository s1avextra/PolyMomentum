# Factory Phase 1: statistical spine (2026-09-01)

Phase 1 of the strategy-hypothesis factory adds three pieces: a trial ledger,
a Sidak family correction on the fresh gate, and an anytime-valid e-process
gate for streaming outcomes.

## Trial ledger

Append-only JSONL at `logs/strategy-research/trial_ledger.jsonl`. Every fresh
gate consume run (PASS, FAIL_TOMBSTONE, READ_INCOMPLETE, or tripwire) appends
one record:

```json
{"ts": "<utc iso>", "source": "fresh_gate_public_v1",
 "candidate": "<candidate>", "stage": "fresh_gate",
 "fresh_range": [start_ts, last_window_ts], "n": 0, "wins": 0,
 "win_rate": 0.0, "avg_break_even": 0.0, "wilson_z": 1.959964,
 "verdict": "<verdict>"}
```

Malformed lines are skipped on read (append-only file, crash-partial lines
possible). The ledger is the factory's memory of every look at fresh data.

## Sidak family rule

Before the verdict, the gate computes `K = 1 +` the number of distinct prior
ledger candidates (`stage == "fresh_gate"`, candidate != this one) whose
`fresh_range` overlaps this run's (`a1 <= b2 and a2 <= b1`; touching counts).
Then `alpha_K = 1 - (1 - 0.05)^(1/K)` and the Wilson lower bound uses the
two-sided Sidak z, `z = inv_cdf(1 - alpha_K / 2)`, instead of a hard-coded
1.96. `K = 1` reproduces the pre-correction `z = 1.959964`. The verdict
artifact records `family_k` and `wilson_z`. Intuition: every extra candidate
judged on the same fresh window is another lottery ticket; the bound widens so
the family-wise false-promotion rate stays at 5%.

## E-process gate (rust_engine/src/backtest/evalue.rs)

For outcome `i` with per-trade break-even `p0_i` (entry price + taker fee) and
win indicator `X_i`, the betting wealth at `lambda` is
`E_lambda = prod (1 + lambda * (X_i - p0_i))`. Under the composite null (true
win prob <= break-even on every trade) each factor has expectation <= 1, so
`E_lambda` is a supermartingale and Ville's inequality bounds
`P(sup_t E >= 1/alpha) <= alpha` at ANY stopping time — the gate may be
checked after every trade with no peeking penalty. `EProcess` mixes a
20-point lambda grid (0.05..1.00, mean of wealths, log-space accumulation) so
no lambda tuning is needed; the mixture is itself an e-process.

- `PROMOTE_E = 20.0`: promote; rejects the null at alpha = 0.05 by Ville.
- `FUTILITY_E = 0.1`: kill; a practical futility stop, not a type-I bound.
- otherwise: continue collecting outcomes.

`update(break_even, won)` returns `Err` for break-even outside (0,1).

## Too-good tripwire

If `n >= 50` and `win_rate > 0.97` the fresh-gate verdict becomes
`IMPLAUSIBLE_MANUAL_AUDIT` (overrides PASS) and the artifact records
`"tripwire": "win_rate>0.97@n>=50"`. A machine-generated candidate that good
is more likely a data leak than an edge; a human must audit the data path
before any promotion.

## Deliberately deferred (next steps)

- Harness-sweep ledger wiring: sweep/harness runs do not yet append
  `stage: "sweep"` records, so K undercounts exploration done outside the
  fresh gate.
- e-BH across concurrent monitors: when several EProcess monitors run at
  once, promote via the e-value Benjamini-Hochberg procedure instead of a
  per-monitor threshold.
- Planted-oracle harness mode: inject a known-edge synthetic candidate to
  verify the pipeline promotes it and the tripwire/null machinery stays
  quiet on shuffled labels.
