# Segmented forward capture runbook

Status: measurement tool deployed. Two bounded 24-window segments completed
fail-closed with zero replay-admissible conditions; source-isolated v3 then
sealed 12 of 24 conditions in segment 003. Its four-segment stability
continuation is active. A separate v4 source snapshot and deferred build unit
are staged to extend block 1 to the fixed `750`-condition disclosure floor.
No opportunity report or strategy score has been emitted.

## Outcome

The repository now has a non-trading runner for the next fresh-data prerequisite:
[`deploy/capture-forward-segments.sh`](../deploy/capture-forward-segments.sh).
It records complete BTC five-minute windows in bounded segments, validates each
capture, measures latency, converts the raw CLOB stream to distilled replay
files, and attempts official Chainlink-aligned finalization.

This is data collection, not strategy promotion. `primary_v6_volfloor_300`
remains A-, `live_ready=false`, and `LIVE_TRADING_OFF` until new replay evidence
passes the unchanged A+ contract.

## July 15 canary outcome

The separate measurement binary was installed at
`/opt/polymomentum/tools/polymomentum-engine-measurement` with SHA-256
`190ec4ae6f614b557ecfbf16f2726fa60655658ee8cf7a742e69ca5b3c5472b3`.
The bounded service recorded 24 complete windows from `06:50Z` through
`08:50Z`; the production service remained active with PID `324003` and zero
restarts when checked after capture.

The monolithic segment stopped at its latency gate and correctly preserved its
owned raw frame file. Local analysis showed that the reported `3,082 ms`
maximum gap occurred after the last market close, inside the post-capture pad.
The corrected active-window maximum is `2,154 ms` at `07:43:10Z`, which still
fails the unchanged `2,000 ms` limit and excludes the `07:40Z` market. Official
Chainlink gaps exclude eight more windows. Five contiguous subsegments retain
15 conditions. A later replay-admissibility audit found that segments 001–003
(six conditions) require Binance history from before the recorder began.
Segments 004–005 (nine conditions) start late enough to be candidates, but
the recovered authoritative tape contains `21,000 ms` and `25,000 ms` Binance
gaps inside both segments' required one-hour histories. Median cadence is
`1,000 ms`, so the harness limit is `5,000 ms`. All 15 retained conditions are
therefore exact-replay inadmissible. Gate-clean and exact-replay-ready are
separate states.

Local conversion emitted three exact-replay cache hours with zero malformed,
missing-field, unknown-market, or unknown-token rows. After VPS recovery, the
preserved 3.48 GB frame file and both RTDS tapes were copied back to the dev box
and hashed. Terminal Gamma refresh was intentionally skipped because outcomes
cannot repair inadmissible signal history. The retained windows remain
unscored; terminal directions were not inferred or synthesized. The enabled
paper-only engine, Telegram monitor, healthcheck timer, and soak-report timer
were also restarted and verified healthy. The full machine-readable diagnostic is
[`20260715_fresh_block_canary_diagnostic.json`](../deploy/promotions/evidence/strategy_registry/20260715_fresh_block_canary_diagnostic.json).

## July 18 replacement segment

After the VPS recovered, the corrected runner was installed with SHA-256
`5467cf923853afc029c8562ed5439fde5f90819d170bf0edc4a4bfb16618b1a8`.
The bounded transient unit
`polymomentum-binary-complement-block1-seg001-20260718.service` began at
`05:49:29Z` and recorded 24 windows from `06:50Z` through `08:50Z` with the full
3,600-second signal pre-roll. At `08:50:30Z` the installed runner rejected the
Binance tape at its monolithic continuity gate and exited 1. It correctly
preserved the session-owned 2,482,154,440-byte frame log. The complete session
was copied without compression to the dev box; the remote source remains intact.

The latest local auditor separated the passing market-stream measurements from
the failed reference evidence. All 24 conditions have both outcome tokens and
continuous CLOB delivery. The latency verdict is `LATENCY_READY`: p50 `9 ms`,
p95 `17 ms`, p99 `48 ms`, and recommended replay latency `50 ms`. Recorder
overhead is p99 `1 ms`.

However, the 1 Hz Binance tape has five gaps of `22,000`, `23,000`, `26,000`,
`24,000`, and `25,000 ms`, against the unchanged `5,000 ms` limit. Because each
condition needs the preceding causal hour, every one of the 24 conditions
intersects at least one gap. The Chainlink tape also has 12 gaps above the limit,
affecting seven settlement windows. The large pauses are commonly mirrored
across both RTDS sources, while the independently collected CLOB stream remains
continuous. The final admissibility result is therefore 24 captured, 24 CLOB
ready, zero replay-admissible, zero groups. Conversion, terminal finalization,
and all strategy scoring were intentionally skipped.

The paper-only engine and Telegram monitor remained active with PIDs `3013` and
`3014`, zero restarts, and no warning-or-higher journal entries since restart at
the `09:01:46Z` check. The healthcheck and soak-report timers remained active.
The final collection status is recorded in
[`20260718_binary_complement_block1_collection_status.json`](../deploy/promotions/evidence/strategy_registry/20260718_binary_complement_block1_collection_status.json).
The implementation evidence is
[`20260718_forward_window_admissibility_pipeline.json`](../deploy/promotions/evidence/strategy_registry/20260718_forward_window_admissibility_pipeline.json).

### RTDS watchdog hardening and segment 002

The failed replacement exposed five shared RTDS pauses of `22–26 s`. The
collector previously waited `20 s` before treating either source as stale, with
a `5 s` health cadence. The measurement-only binary now retains the documented
five-second `PING`, checks source freshness every `250 ms`, and reconnects after
`3 s` without a post-acquisition tick. It allows a source up to `10 s` only for
its first-ever tick inside the pre-boundary capture pad, then gives an already
stale source at most `1 s` on a replacement connection. It never synthesizes or
forward-fills a missing observation; the unchanged `5 s` replay gate remains the
authority.

The official RTDS documentation advertises a Binance symbol filter, but a
two-minute VPS smoke with that shape produced zero Binance ticks, 76 Chainlink
ticks, and 38 reconnects. The filter was rejected empirically. The installed v2
binary restores the previously proven all-symbol `crypto_prices` subscription
while parsing and writing only `btcusdt`. A second two-minute VPS smoke then
recorded 120 Binance ticks with a `1 s` maximum gap and 117 Chainlink ticks with
a `2 s` maximum gap, with one connection, zero reconnects, zero idle timeouts,
and both provenance flags ready.

The v2 measurement binary has SHA-256
`481dc2f2ffbcfad7293958bbaf6cfb5fb77dc02230f9015c8e9194bc56f3cf68`.
The current runner has SHA-256
`be992ae9a93821aa57cd25b45025533ddcfced3e002d95c36d5eb85d81d5a12b`.
Both prior binaries and the prior runner are retained under hash-suffixed
rollback paths. The transient unit
`polymomentum-binary-complement-block1-seg002-20260718.service` started at
`09:34:55Z` and finished recording at `12:40:30Z`. The original runner stopped
at the whole-segment Binance check and preserved its 2,179,493,306-byte raw
frame log. An exact local per-condition audit found healthy CLOB coverage and
continuity for all 24 windows and recommended `82 ms` replay latency, but every
causal Binance hour intersects a `6–8 s` gap. Five conditions also intersect an
official Chainlink gap. Segment 002 therefore has zero admissible conditions,
zero groups, and no strategy score.

### Source isolation v3 and segment 003

Segment 002 recorded 60 reference reconnects because Chainlink and Binance
shared one RTDS websocket and one reconnect lifecycle. A source pause therefore
restarted both tapes together. V3 gives each source an independent subscription,
watchdog state, ping cadence, and reconnect loop; it never fills or fabricates a
missing observation.

The new measurement-only binary was built on the VPS at low priority with one
release job and installed atomically at
`/opt/polymomentum/tools/polymomentum-engine-measurement`. Its SHA-256 is
`f33e3a9b75e3944554cbd80d280899bb6e08a348fb44e8894b89ff3c8e46334c`.
The production binary remained unchanged at SHA-256
`4874df24efffef6f4d0c60aeb74a90c45d5b9492dcb713ffa852d9c6afda27dc`.
The installed runner SHA-256 is
`7ab459831bcdc74b3bf2a0c8099e038a3d74ea85c3977f6018680e611f01ca75`;
it treats incomplete whole-segment Binance coverage as a diagnostic and lets
the unchanged per-condition audit salvage only unaffected windows. Zero
admissible conditions still hard-fail.

The bounded 15-minute v3 smoke ran from `13:49:51Z` through `14:04:51Z`.
Binance recorded 899 ticks with a `1 s` maximum observation gap; Chainlink
recorded 895 with a `2 s` maximum. The two independent sessions had zero
reconnects, zero idle timeouts, and zero websocket errors. The CLOB stream
recorded 340,903 frames with zero reconnects. Peak memory was 198.2 MB and CPU
time was 32.461 seconds.

After the smoke passed, the transient unit
`polymomentum-binary-complement-block1-seg003-20260718.service` started at
`14:07:08Z`. Recording began at `14:09:30Z`; the 3,600-second causal pre-roll
leads into 24 windows from `15:10Z` through `17:10Z`, with expected capture end
at `17:10:30Z`. The service is measurement-only, has `MemoryMax=3G`,
`CPUWeight=10`, `Nice=10`, and a four-hour runtime limit. The production engine,
Telegram monitor, healthcheck timer, and soak-report timer remained active with
zero restarts when the recorder crossed its launch boundary.

### Admissible-only storage and the fixed-support collector

Segment 003 proved the end-to-end path but retained about `117 MB` of compressed
books while only 12 of its 24 conditions were admissible. At that observed
admissibility rate, the 120-condition stability plan cannot meet the registered
`750`-condition floor, and retaining every excluded book would exhaust the VPS
disk before the first valid disclosure.

Measurement v4 therefore adds only an output allowlist to
`convert-recorded-btc-books`. The all-condition raw capture and causal-tape audit
still run first. Conversion then receives the unique condition IDs from the
audit's admissible groups and writes no excluded condition to the exact-replay
cache. The manifest records the source market count, selected market count,
selected IDs, and filtered event count. An unknown requested condition fails
closed.

The v4 runner also has an explicit robust-collection mode. A structurally valid
capture with zero admissible conditions receives a rejected status, produces no
replay cache, and may delete only its session-owned frame log after the audit is
sealed. Other capture, timestamp, latency, conversion, or provenance failures
still stop and preserve raw input.

[`deploy/collect-binary-complement-floor.sh`](../deploy/collect-binary-complement-floor.sh)
counts unique ready condition IDs from official-source-aligned resolution
manifests, but only when the parent segment status proves capture verification,
nonzero admissibility, complete ready resolution groups, positive distilled
events, and an exact resolution-manifest count. Unsealed, partial, corrupt, and
zero-admissible segments contribute no support. It stops at `750` or after 96
new 24-window segments. It does not read
or emit winner, loss, residual, retention, Wilson, opportunity, or PnL metrics.
The storage estimate is `350,000` raw bytes/second with a `5 GiB` free-disk
reserve, based on the `243,414` bytes/second observed in segment 003 plus margin.
Segment 003 retained `120,143,852` converted bytes for 12 admitted conditions;
the corresponding conservative 750-condition projection is about `6.99 GiB`.
After byte-identical local archives were reverified, the two session-owned,
zero-admissible VPS frame copies were removed, reclaiming `4,660,494,336` bytes.
No reference tape, audit, converted evidence, shared cache, or parquet was
deleted. The resulting capacity model retains about `4.77 GiB` beyond the full
750-condition projection, the configured `5 GiB` reserve, and one peak-sized
raw segment at the configured capture rate.
The production binary and service remain untouched.

## Sizing and safety assumptions

The July 14 VPS run produced `2,489,508,456` raw bytes in `6,300` seconds, or
approximately `395,160` bytes/second. The runner uses a deliberately higher
default estimate of `600,000` bytes/second and preserves an additional `8 GiB`
free-disk reserve.

One default segment contains `24` complete five-minute markets. Exact replay's
realized-volatility input requires a full causal hour of Binance RTDS history
before the first selected market. The corrected runner therefore starts one
hour plus the 30-second boundary pad before the first open and retains another
30 seconds after the final close. It runs for `10,860` seconds and its
conservative raw estimate is `6.07 GiB`. Disk is checked again before every
segment. The July 15 canary used the earlier 30-second-only plan; its early
retained windows are not exact-replay-admissible even if terminal labels arrive.

The runner is restricted to the PolyMomentum private tree. It rejects shared
and peer-private paths, requires a new session directory, and records an
ownership marker. The optional cleanup flag deletes exactly one file after
capture, latency, and conversion verification:

```text
<owned-session>/segment_NNN/raw/market_ws_frames.jsonl
```

It retains Gamma metadata, the capture summary, official Chainlink and Binance
RTDS CSVs, distilled gzip files, command logs, latency evidence, and resolution
manifests. By default, a failed capture, latency gate, or conversion preserves
the frame file and stops. Only the explicit robust-collection flags permit a
zero-admissible, otherwise valid audit to write a rejected status and remove its
owned frame file before continuing. A terminal-resolution delay or settlement
gap stays explicitly non-ready but does not relabel the converted capture as
valid strategy evidence.

## Authorized deployment shape

Do not replace `/opt/polymomentum/polymomentum-engine` and do not restart the
production service. Install a measurement-capable Linux x86_64 binary as a
separate tool:

```bash
scp polymomentum-engine-linux-x86_64 VPS:/tmp/polymomentum-engine-measurement
scp deploy/capture-forward-segments.sh VPS:/tmp/capture-forward-segments.sh
ssh VPS 'sudo install -d -o polymomentum -g polymomentum -m 0755 /opt/polymomentum/tools /opt/polymomentum/logs/forward-captures'
ssh VPS 'sudo install -o polymomentum -g polymomentum -m 0755 /tmp/polymomentum-engine-measurement /opt/polymomentum/tools/polymomentum-engine-measurement'
ssh VPS 'sudo install -o polymomentum -g polymomentum -m 0755 /tmp/capture-forward-segments.sh /opt/polymomentum/tools/capture-forward-segments.sh'
```

Before any capture, verify that the separate binary exposes all four required
commands and run the runner's write-free plan:

```bash
ssh VPS 'sudo -u polymomentum /opt/polymomentum/tools/polymomentum-engine-measurement record-btc-books --help >/dev/null'
ssh VPS 'sudo -u polymomentum /opt/polymomentum/tools/polymomentum-engine-measurement forward-latency-audit --help >/dev/null'
ssh VPS 'sudo -u polymomentum /opt/polymomentum/tools/polymomentum-engine-measurement convert-recorded-btc-books --help >/dev/null'
ssh VPS 'sudo -u polymomentum /opt/polymomentum/tools/polymomentum-engine-measurement finalize-recorded-btc-books --help >/dev/null'
ssh VPS 'sudo -u polymomentum /opt/polymomentum/tools/capture-forward-segments.sh --session-id fresh-block-canary --segments 1 --windows-per-segment 24 --delete-session-owned-frames-after-verify --dry-run'
```

No command above authorizes deployment by itself. They are the exact commands
to use only after the operator approves the VPS mutation and collector start.

## Bounded collection sequence

Start with one segment:

```bash
ssh VPS 'sudo -u polymomentum /opt/polymomentum/tools/capture-forward-segments.sh --session-id fresh-block-canary --segments 1 --windows-per-segment 24 --delete-session-owned-frames-after-verify'
```

Inspect the canary before extending collection:

```bash
ssh VPS "jq '{capture_verified,resolution_ready_segments,segments:[.segments[]|{segment,capture_verified,session_owned_frames_deleted,recommended_replay_latency_ms,resolution_ready,resolution_verdict}]}' /opt/polymomentum/logs/forward-captures/fresh-block-canary/session_summary.json"
```

Proceed only if `capture_verified=true`, the frame log was retired, latency is
clock-safe, the converter reported zero malformed/unknown/missing rows, and the
remaining free disk still satisfies the runner's preflight. Four more default
segments produce at most `96` additional markets. Together with the corrected
canary, this is an operational stability block only; it is not enough to score
the strategy.

The blinded power amendment made before any forward score raises the disclosure
floor to `750` terminal settlement-aligned conditions per block. At the
historical `102 / 631` baseline firing rate, the old `100`-condition floor
would yield only about `16` candidates and four losses. Continue unchanged
bounded segments only after the canary seals, and do not invoke the scorer
until the fixed `750`-condition floor is met.

```bash
ssh VPS 'sudo -u polymomentum /opt/polymomentum/tools/capture-forward-segments.sh --session-id fresh-block-rest --segments 5 --windows-per-segment 24 --delete-session-owned-frames-after-verify'
```

Count only terminal, settlement-aligned markets:

```bash
ssh VPS "find /opt/polymomentum/logs/forward-captures/fresh-block-* -name resolution_manifest.json -print0 | xargs -0 jq -s '[.[] | select(.a_plus_gate.settlement_alignment_ready == true) | .stats.terminal] | add // 0'"
```

If the count is below `750`, diagnose the specific capture or reference-tape
failure before adding another segment. Do not weaken the gate and do not use
paper mode as a substitute.

## Dev-box handoff

Copy the retained session artifacts to the dev box without peer directories or
shared-cache mutation. All CPU-intensive replay, candidate scoring, and sweep
work stays on the dev box:

```bash
rsync -az VPS:/opt/polymomentum/logs/forward-captures/fresh-block-canary/ local-forward/fresh-block-canary/
rsync -az VPS:/opt/polymomentum/logs/forward-captures/fresh-block-rest/ local-forward/fresh-block-rest/
```

The new mechanism is already pre-registered as
[`binary_complement_coherence_v1`](strategy_binary_complement_preregistration_2026-07-15.md).
Do not inspect it on the old 42-fold outcomes or tune its two-tick tolerance.
Use the dev-box-only replay runner to generate one opportunity report per
verified segment with the frozen
[`primary_v6_calibration_capture_edge1000`](../deploy/promotions/evidence/strategy_registry/20260715_binary_complement_capture_variant.json)
variant and score them as one block:

```bash
./deploy/replay-binary-complement-block.sh \
  --capture-root local-forward/block-1-captures \
  --output-dir local-forward/block-1 \
  --block-id binary-complement-block-1 \
  --threads 0
```

The runner uses Binance RTDS only for causal signal/volatility, Chainlink only
for settlement, and each segment's measured latency with a `202 ms` minimum.
It fails before replay if the Binance tape does not begin a full hour before
the first selected market, if its maximum internal gap exceeds
`max(5,000 ms, 3 × median cadence)`, or if either reference tape ends before
the last market close.
It sets `PMXT_DISTILLED_DIR` to the retained converted segment and passes
`--require-shared-distilled`, so a missing, corrupt, or empty captured hour
fails instead of silently falling back to a PMXT parquet or network download.
Each replay command also passes the exact condition IDs from its verified
resolution manifest; same-hour markets outside that admissible group cannot
enter the replay universe.
Before replay, the wrapper independently counts unique post-registration
terminal official-source-aligned conditions and refuses below `750`, so it
cannot leave inspectable partial labeled opportunity files. It then invokes the
threshold-free block scorer, which independently enforces the same floor. A
first pass earns only a second
disjoint forward block; it does not earn promotion.

The schema-4 screen publishes the schema-3 non-gating selection-delay,
time-to-close, decision-time ask, final-minute, and direction-change
diagnostics registered before any forward score. They expose a possible late
selection/payoff confound but cannot change the frozen rule. Schema 4 also
normalizes each selected condition to `$1` at its recorded decision ask,
applies the recorded entry fee, and fails the block unless the optimistic unit
profit factor is at least `1.20` and payoff is at least `0.20`. These are the
existing A+ economic thresholds, not a forward-data fit; latency, depth, fills,
and stateful sizing remain for exact replay. See the
[`pre-score economic diagnostics amendment`](../deploy/promotions/evidence/strategy_registry/20260718_binary_complement_prescore_economic_diagnostics_amendment.json)
and the
[`pre-score unit-economics amendment`](../deploy/promotions/evidence/strategy_registry/20260718_binary_complement_prescore_unit_economics_amendment.json).

The separate complete-set comparison remains sealed until its stricter
pre-registration floor is available. Once the accumulated post-correction
capture has at least 100 terminal conditions, add the exact pair and contract:

```bash
./deploy/replay-binary-complement-block.sh \
  --capture-root local-forward/block-1-captures \
  --output-dir local-forward/block-1 \
  --block-id binary-complement-block-1 \
  --strategy-variant-json deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_pair.json \
  --strategy-preregistration-json deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_preregistration.json \
  --threads 0
```

The wrapper suppresses and temporarily seals the candidate reports, per-trade
rows, and logs. It publishes them only if the candidate has at least 100
exact-replay trades; otherwise it deletes every sealed file and returns no
candidate metrics. Preserve the per-trade rows because a profitable lock can
change the already-frozen realized-loss fallback and therefore change later
entry membership. The full stateful policy comparison remains primary, but
the disclosed evidence must also separate common entries, baseline-only
entries, candidate-only entries, and lock delta versus terminal hold.

A no-tuning replay of historical fold 1 on July 18 verified exact baseline
parity and one executable lock. It converted a `-$5.10996` terminal loser into
`+$1.14909`, but the changed realized-loss state admitted a candidate-only
`-$4.98887` trade. Full candidate fold PnL was still `-$3.83978`, only
`+$1.27018` above baseline. This is implementation and state-feedback
diagnostic evidence only; it earns no fresh-block or promotion credit. The
disclosure cutoff was advanced to `2026-07-18T10:07:12Z`, before the first
eligible fresh window opened, without changing candidate parameters or gates.
After a second independent pass, make disjointness and contract parity executable:

```bash
./rust_engine/target/release/polymomentum-engine strategy-builder \
  binary-complement-repeat-audit \
  --screen local-forward/block-1/binary_complement_screen.json \
  --screen local-forward/block-2/binary_complement_screen.json \
  --output local-forward/binary_complement_repeat_audit.json
```

Only an unchanged mechanism whose repeat audit returns
`TWO_BLOCK_SCREEN_PASS_EXACT_REPLAY_ALLOWED` may be materialized as one exact
replay variant and re-enter the historical strict-tail A+ evaluation.

## Local verification

The runner has a focused fixture test:

```bash
./deploy/capture-forward-segments-test.sh
```

The test covers successful capture/latency/conversion/resolution validation,
acceptance of a measured-latency retest above the obsolete 50 ms assumption,
rejection of shared paths, refusal to delete without an ownership marker, and
deletion of only the owned frame log while preserving reference and converted
artifacts.
