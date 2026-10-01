# Ladder analysis 2026-10-01: twelve days of executable truth and the ways forward

Cut: pull + `--build` at 09:12 UTC; newest labelled window 2026-10-01 08:50 UTC; 13,700 VPS ladder rows, 3,429 windows since 09-19 05:25. VPS rows only, $25 quote, entry = FOK worst price, fee 0.07. Where a checker refuted an analyst, the checker's number stands.

## 1. State of play

1. No cell clears break-even (BE) at the Wilson lower bound: 0 of 254 cells with n >= 30.
2. The leaderboard is what 200 correlated zero-edge cells produce: best net +5.7%/USD against a null median of +5.3% (P = 0.45).
3. Pooled edge of the executable region: +0.6%/USD, 95% interval -0.4 to +1.6. Two days ago it was +0.25%.
4. The label is not on the engine's basis: markets settle on a 60 s TWAP, and "oracle noise" is mostly that mismatch.
5. The evaluator reproduces the paper engine: old-rule twin 63/74 at -0.6%/USD, predicted -0.59%, 67/67 windows matched.
6. `--register` as coded would register the wrong family (N = 32, bar e >= 640), and the oracle-ceiling tripwire would hold every candidate as rows accrue.
7. Ops: the Mac factory runs by hand (a reboot cost 40 h of labels); the VPS observer still runs four anchors on the 09-20 binary.
8. Wallet $2.78, `KILL_BAND` set, sanctioned stake 0. Nothing below changes that.

## 2. What the data say

### 2.1 Cells

| cell | W/n | Wilson lower vs BE | net/USD | entries/day (since 09-28) | capacity | excluded W/n |
|---|---|---|---|---|---|---|
| d150_f50_c0.99_p0 | 883/929 | 0.935 vs 0.940 | +1.2% | 76.5 (73.7) | 0.82 | 154/203 |
| d150_f100_c0.99_p0 | 255/262 | 0.946 vs 0.953 | +2.2% | 21.6 (14.7) | 0.77 | 73/78 |
| d180_f75_c0.99_p0 | 436/454 | 0.938 vs 0.957 | +0.3% | 37.4 (28.4) | 0.63 | 252/261 |
| d210_f100_c0.99_p15 | 124/126 | 0.944 vs 0.963 | +2.2% | 10.4 (6.0) | 0.25 | 381/385 |
| d240_f50_c0.99_p0 | 160/163 | 0.947 vs 0.960 | +2.4% | 13.4 (8.9) | 0.12 | 1212/1228 |

- Price is the constraint, not depth: $100 fills in 259 of the 261 windows where $25 fills.
- Capacity of the floor-100 cells fell after 09-25 (0.85 to 0.62; 0.30 to 0.165; day-level p = 0.13 and 0.08).
- One second of latency keeps 0.64-0.75 of fills (0.47 at d240) and costs 0.3-1.1 pp of net. The live path filled 40/57 first attempts: the same miss rate.

### 2.2 Selection

- Split at six days: rank correlation of cell edges across the parts is +0.1. The top ten by net went from +5.4% to +2.2% (47/50), or -0.8% (31/34) when the test stops at 09-29 16:05.
- Leaders lead on one run: d210_f100_c0.99_p15 was 63/63 before the 09-23 look and 61/63 after; d150_f100_c0.99_p0 was 115/121 (+0.1 pp) before it and 139/140 after.
- The slice 09-29 16:10 to 10-01 01:25 went 277/280 pooled. Labels equal Gamma on every row and the next 7 h went 21/23 on the volume cell: a lucky run.

### 2.3 The settlement basis

| basis, against 12,489 official labels | disagreement | beyond 25 USD |
|---|---|---|
| point close minus point open (engine, evaluator) | 13.0% | 338 |
| mean of seconds 240-300 minus mean of the 60 s before the open | 2.1% | 0 |

Gamma names the source as Chainlink `btc-usd-twap-60s`. Consequences:
- Point open minus the 60 s strike has SD 28 USD. Of 35 losing windows among strong fillable rows, 14 were below 75 USD on the settling basis; 34 are true reversals, not oracle flips.
- The oracle-ceiling tripwire rests on the point basis: it fires on d240_f50 today and converges to 0.93-0.96, below BE + 0.5 pp, for the other candidates.

Floor on the settlement basis (`s`: signed margin against the 60 s strike, engine direction):

| cell | W/n | Wilson lower vs BE | net/USD | entries/day (since 09-28) | +1 s: fill, net |
|---|---|---|---|---|---|
| d210_s75_c0.99_p0 | 243/246 | 0.965 vs 0.972 | +1.6% | 20.3 (10.8) | 0.69, +0.8% |
| d210_s100_c0.99_p0 | 105/105 | 0.965 vs 0.971 | +3.1% | 8.6 (6.2) | 0.70, +2.8% |

The complement at d210 (point >= 75, settlement < 75) is 59/66, -6.2%/USD; at d150 and d180 the floor does not help. The finding is post hoc: d210_s75 beats price-matched market calibration by four wins (z = 1.5), 105/105 is an audit item under the 0.995 rule, and the unfillable windows still win more (609/611).

## 3. Is there an edge?

Possibly a small one; it cannot be told from zero.

| estimate | edge/USD | uncertainty |
|---|---|---|
| pooled, four decisions, floor 50, cap 0.99 (2470/2583) | +0.60% | [-0.40, +1.59], P(<= 0) 0.13 |
| same, cut at 09-29 16:05 | +0.25% | [-0.90, +1.34], P 0.31 |
| 16 disjoint strata, precision-weighted | +0.52% | se 0.42; Q = 14.4 on 15 df |
| registrable today (d >= 180, floor >= 75) | +0.37% | [-1.32, +1.86] |
| d150 alone (chosen after looking) | +1.07% | [-0.33, +2.41] |

The shrunk edge of every cell is about +0.5%/USD (se 0.4): no stratum differs from the rest, so raw edges of +2 to +5% are noise around it. In money, +0.5% on $25 stakes at 57 fills a day is about $7 a day, half of it after latency; the sizing rule allows $25 only from $600-1,500 of equity.

## 4. Ways forward, ranked

| option | benefit | cost | risk | acceptance number |
|---|---|---|---|---|
| 1. Fix the basis, register N = 4 (a) | evidence clock on a family with power and one mechanism | 3-4 agent days, tested Python | "no discovery" is likeliest | e-BH: e >= 80 alone (40/26.7/20 for 2/3/4), n >= 100, 14 days, tripwires clear |
| 2. Ops fixes (d) | no silent label gaps; one evidence host | 30 operator minutes | none | runner under launchd; screens score VPS rows only |
| 3. Engine logs the strike, recipe H in the same build (c) | settlement margin at source; anchors 195/225 | one VPS build and restart | minutes of gap | `strike_60s` on records; the anchors within 10 min (five, 120/150/180/210/240, since the adaptive doc of the same day: recipe H as superseded) |
| 4. F1, F2 as paired challengers (b) | tests two filters without spending N | none | false hope | paired e >= 40 over >= 100 paired windows |
| 5. Other assets (c) | up to four times the windows | a Mac day, then engine work | thin books | print model at asks >= 0.92 clears BE on two of ETH/SOL/XRP, else drop |

**(a) Family.** `d150_f50_c0.99_p0` (volume), `d150_f100_c0.99_p0` (early, strong margin), `d210_s75_c0.99_p0` (settlement floor), `d210_f100_c0.99_p15` (the existing twin, control of the third). Overlap of traded windows 0.08-0.35. Left out: cap and patience variants (overlap 0.46-0.89), d240_f50 (half its fills gone at +1 s), d210_s100 (nested, held by the 0.995 rule), all d180.

P(e >= 80 by day 90) at the whole-sample rate (rate since 09-28); promotion also needs every tripwire clear.

| cell | raw edge | about half | +0.5% | zero | futility at zero |
|---|---|---|---|---|---|
| d150_f50_c0.99_p0 | +1.0%: 0.60; +1.2%: 0.79 | - | 0.08 | 0.00 | 0.94 |
| d150_f100_c0.99_p0 | +2.0%: 0.82 (0.61) | +1.0%: 0.20 (0.13) | 0.04 | < 0.01 | 0.67 |
| d210_s75_c0.99_p0 | +1.6%: 0.83 (0.47) | +0.8%: 0.22 (0.09) | 0.07 (0.03) | 0.01 | 0.49 |
| d210_f100_c0.99_p15 | +2.1%: 0.55 (0.26) | +1.4%: 0.22 (0.11) | 0.03 | 0.01 | 0.33 |

Median days to the bar if a raw edge is real: 46-78. At the shrunk edge: over 230, outside the horizon.

**(b) Filters.** None held out of sample. A blind search of 987 single and 39,993 pair rules has discovery-to-test correlation -0.03 and +0.05; the ask itself is the best loss predictor (AUC 0.74). F1 (previous window moved the same way) is 95/96 windows on test, lower 0.943 below BE 0.961. F2's losses sit in 00-03 UTC only (146/162; 03-06 is 100/102). The settlement floor alone has a mechanism, so it is a cell; F1 and "skip 00-03 UTC" run as challengers on the volume cell.

**(c) Widening.** Anchors 195/225 share windows with their neighbours: more rows, little independent evidence. ETH, SOL and XRP 5-minute markets exist; the engine records BTC only, and the old rule ran break-even on their prints in August.

**(d) Ops.** The launchd agent is not installed (the runner was started by hand). Recipe H never ran on the VPS (four anchors, margin50 artifact). Mac ladders agree with the VPS within a tick on 92% of samples (bar 95%), are still scored by `grid()` and `register()`, and shadow 291 VPS records: retire the Mac observer.

**(e) Kill criterion.**
- Day 45 after `registered_at` (about 3,400 fresh entries on the volume cell, se 0.4 pp): a fresh edge <= 0 stops the campaign and the approach; its upper bound is then +0.8%, short of the +1.0% that survives a second of latency.
- Day 90 without an e-BH discovery: stop; no further grid looks on BTC 5m.
- Any time: `capacity_collapse` kills its cell; a discovered cell with +1 s net <= 0 gets no probe.

## 5. The next 14 days

**Day 0, operator, Mac.** Do not run `--register`; do not restart the Mac observer.

```
pkill -f deploy/factory-runner.sh
cp deploy/com.polymomentum.factory-runner.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.polymomentum.factory-runner.plist
```

A LaunchAgent starts at GUI login only: enable auto-login or accept that gap.

**Days 1-4, agent, Mac.** Versioned and tested; evaluator and grammar become v4.
1. The 60 s strike and settlement margins in the table; oracle noise and ceiling on that basis.
2. Grammar: the `s` floor; d150 and floor 50 registrable.
3. `--register <id> --cells ...`: explicit list, evidence hosts only, no print-screen arm; the campaign file carries the day-45 and day-90 rules.
4. VPS ladders replace Mac ladders on the 291 shadowed rows.
5. The retry-after-1 s model beside p0; the Tier 1 gate "FOK-kill share <= 20%" (realized 30-34%) restated as window fill rate after retries.
6. Engine, records only, with a test: strike and settlement margin on `band_anchor` and `band_ladder`.

**Day 4-5, operator, VPS.** Recipe H of the basement doc as superseded 2026-10-01 (five anchors, new checks), from the new tree: one `nice -n 10 cargo build --release --locked -j 1`, the twin and envs installed, both preflights checked, then `sudo systemctl restart polymomentum-band-observer`.

**Day 5, operator, Mac** (`--cells` exists only after step 3), then commit the campaign file and push.

```
bash scripts/pull_vps_sessions.sh
uv run --offline python scripts/executable_truth.py --build
uv run --offline python scripts/executable_truth.py --register 2026-10_band_ladder --cells d150_f50_c0.99_p0,d150_f100_c0.99_p0,d210_s75_c0.99_p0,d210_f100_c0.99_p15
```

**Days 5-14.** Accrual through the runner's `--tick`. Week 2: ETH/SOL/XRP print pre-check. Day 14: first formal look (support, capacity, coverage, +1 s numbers). Money: none.

## 6. Open questions

1. The settlement series here is a Binance proxy (2.1% residual, all within 25 USD). What are the Chainlink stream's exact semantics?
2. Should the engine's signal move to the settlement basis? It is a trade-path change, and windows where the bases disagree on direction are unmeasured (the ladder quotes one side).
3. Did the VPS watchdog's Telegram alert arrive during the 40 h Mac gap?
4. Is "no discovery by day 90, stop" an acceptable answer?
5. $25 stakes at $150 equity fail in 9-28% of 60-day paths at shrunk edges. Restate the basement as $5 from $150 and $25 from $600?
