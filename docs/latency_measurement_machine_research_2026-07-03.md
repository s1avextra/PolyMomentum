# CLOB latency measurement machine research - 2026-07-03

Purpose: choose the right machine for Polymarket CLOB latency measurement and
define the stats required before changing strategy replay latency.

## Hard facts

- Official market-data websocket endpoint:
  `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
  Source: https://docs.polymarket.com/market-data/websocket/market-channel
- Polymarket documents CLOB matching-engine infrastructure as:
  primary servers in `eu-west-2`, closest non-georestricted region `eu-west-1`,
  and direct co-location in `eu-west-2` after KYC/KYB.
  Source: https://docs.polymarket.com/trading/overview
- AWS maps `eu-west-2` to London and `eu-west-1` to Ireland.
  Source: https://docs.aws.amazon.com/global-infrastructure/latest/regions/aws-regions.html
- The current shared PolyMomentum VPS is `193.24.234.202`, alias `vps`,
  identified by live probe as Dublin, Ireland, AS202448 MVPS LTD.
- The public CLOB and websocket hosts are Cloudflare-fronted. Local desktop
  headers hit `BKK`; the VPS REST header probe hit `DUB`. Cloudflare edge hits
  are useful diagnostics, but they are not enough to infer matching-engine
  latency.

## Verdict

The correct measurement machine depends on the question:

1. Current production replay policy: measure from the actual production host.
   Today that is the Dublin VPS. This is the only host that can set the latency
   assumption for the currently deployed runtime, because the bot will trade
   from there.
2. London `eu-west-2` is only a relocation or comparison target. Its measurements
   must not replace the Dublin VPS policy unless execution is actually moved to
   London.
3. Fallback near-region setup: `eu-west-1` Ireland. This matches Polymarket's
   documented closest non-georestricted region and approximates the current VPS
   geography, but it is not the documented primary region.
4. Desktop/laptop captures: debugging only. They can validate parser logic,
   clock-skew handling, and recorder overhead, but must not lower replay latency
   policy.
5. Direct co-location: optimal if Polymarket approves KYC/KYB access. Treat it
   as a separate deployment target with its own latency policy.

## Required machine controls

Accept a capture only when all controls are recorded:

- Host label: `POLYMOMENTUM_LATENCY_HOST_LABEL`, hostname, OS, arch, PID.
- Clock: `timedatectl` or `chronyc tracking` before and after capture.
- Reject if the clock is not synchronized or offset is above `10 ms`.
  Target offset is below `2 ms`.
- Endpoint: exact websocket URL and subscribed token IDs.
- Load: CPU, memory, and peer service status on the shared VPS before capture.
- Recorder overhead: `ts_recorded_ms - ts_received_ms` p50/p95/p99/max.
- Network path diagnostics: Cloudflare edge from headers when available, plus
  REST `curl` timings as context only.

The recorder now writes host metadata and `ts_recorded_ms` into capture output.
Future production artifacts must include those fields.

## Correct capture design

Minimum A+ dataset:

- Run the primary probe from the current production VPS in Dublin.
- Optionally run simultaneous comparison probes from:
  - a London `eu-west-2` host, only to evaluate a future relocation;
  - desktop as a negative/debug control.
- Subscribe to the same BTC candle token IDs on all probes.
- Prefer one active 5-minute window for low-noise venue timing, plus a
  three-window capture for production token-coverage stress.
- Capture at least `30 minutes` per host, repeated across at least three market
  regimes: quiet, active, and terminal-resolution-adjacent.
- Report both raw stats and warm stats with the first `5-10 seconds` excluded.
  Never hide the raw stats.
- Archive raw `market_ws_frames.jsonl`, `summary.json`, and
  `forward_latency_audit.json`.

Required statistics:

- Event delay by type: p50, p90, p95, p99, p99.5, max.
- Counts above thresholds: `75`, `100`, `150`, `200`, `250`, `300`, `400`,
  `500`, and `750 ms`.
- Negative-delay count and rate.
- Missing timestamp count and rate.
- Token coverage and per-token max update gaps.
- Receive-gap top list and per-second p99.
- Batch-size distribution for websocket frames that contain multiple events.
- Recorder overhead p50/p95/p99/max.

## Decision rule

- Do not reduce strategy latency from desktop evidence.
- For the current deployment, replay at the worst accepted p99 from fresh
  Dublin VPS captures. Keep p99.5 and max as stress sensitivity.
- If p99.5 is repeatedly close to p99 or bursts recur, use p99.5 as the main
  strategy retest latency.
- To lower the policy, require at least three fresh accepted captures on the
  same production host and market class, with no clock failures and no worse
  p99.5 tail.
- If execution moves to London, reset the policy and collect London evidence
  before promoting any strategy.
- If London and Dublin see the same delayed event timestamps at the same time,
  classify the tail as venue/backend/Cloudflare propagation. If only one host
  sees it, classify it as route or host-local.

## Commands

Current local/debug form:

```sh
POLYMOMENTUM_LATENCY_HOST_LABEL=desktop-bangkok \
  ./rust_engine/target/debug/polymomentum-engine record-btc-books \
  --duration-seconds 1800 \
  --windows 1 \
  --out-dir /private/tmp/polymomentum_latency_desktop_$(date -u +%Y%m%dT%H%M%SZ)
```

Production form, after a measurement-capable binary is installed on the target
host:

```sh
POLYMOMENTUM_LATENCY_HOST_LABEL=vps-dublin-eu-west-1 \
  /opt/polymomentum/polymomentum-engine record-btc-books \
  --duration-seconds 1800 \
  --windows 1 \
  --out-dir /opt/polymomentum/logs/latency/$(date -u +%Y%m%dT%H%M%SZ)
```

Then audit:

```sh
/opt/polymomentum/polymomentum-engine forward-latency-audit \
  --input-dir /opt/polymomentum/logs/latency/<capture-dir> \
  --output /opt/polymomentum/logs/latency/<capture-dir>/forward_latency_audit.json
```

The currently deployed VPS binary did not list `record-btc-books` in help during
this research pass, so production measurement requires either deploying a
measurement-capable build or using a separate lightweight probe with equivalent
timestamping semantics.
