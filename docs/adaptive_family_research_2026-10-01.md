# Adaptive family research 2026-10-01: fluid floor, event-time entry, cap and patience

Four research tracks, three independent checkers, one synthesis replay, on the evidence of `ladder_analysis_2026-10-01.md`: 3,425 VPS ladder windows (2026-09-19 to 10-01, 11.9 days), 12,484 official labels, 44 days of Binance 1 s closes. Tags: [chk] reproduced by a checker's own code; [syn] synthesis replay under one frozen spec (`scratchpad/adaptive/synth/`); [T] theory track, unchecked. All of it is discovery data: about 600 rule variants were scored on these 12 days.

## 1. Verdict

1. The hypothesis is supported in part and moderately; nothing is proven or promotable.
2. Supported: the settlement basis as the signal (log-loss gain 0.060 [0.048, 0.073] at 210 s [chk]); event-time entry instead of anchor seconds (2.5x the entries at the same win rate [chk]); a volatility-scaled floor against a USD floor in that format (+1.0 to +2.0%/USD against -0.6 to -0.0% [syn]).
3. Refuted: a cap from "model minus ask" as a selector (-0.04 to -3.4%/USD in three tracks), patience (a vanished ask returns in 3.7% of cases), regime filters, a low-volatility guard, an ask floor at 0.97.
4. Size: +1 to +2%/USD for the four larger cells at 21-61 entries a day (12-39 on the last four days) with one second of order delay; the result moves with the volatility estimator and the latency model.
5. Against the incumbent static cell `d150_f100_c0.99_p0` the adaptive cells earn the same net per day (0.58 against 0.60 per $1 staked) on almost disjoint windows, with 2 losses against 11: the gain is a bound that can clear break-even, not more money.
6. Recommendation: register family v4 (five adaptive cells plus the incumbent, N = 6) in place of the static N = 4; no funding.

## 2. Mechanism

**Settlement rule** [chk, three implementations]. Up iff the Chainlink 60 s TWAP at the end is >= the 60 s TWAP before the open (Gamma `btc-5m-twap-60`, tie to Up). On 12,456 labels the 60 s/60 s Binance proxy agrees 97.88%, the point rule 87.38%; the peak is sharp (45/45 95.9%, 75/75 97.3%); shifting 2 s earlier gives 98.14%. Every disagreement has a final margin under $20 (0 of 9,005 above); basis noise is 3.0-4.3 USD.

**Fair value, ready to code.** P[i] is the 1 s close of the second starting at epoch i, W the window start.

```
K     = mean(P[W-60 .. W-1])                                    strike
t<=240: m_t = P[W+t-1] - K               V_t = (240 - t) + 20.5
t> 240: r = 300 - t
        m_t = (sum(P[W+240 .. W+t-1]) + r*P[W+t-1])/60 - K      V_t = r(r+1)(2r+1)/21600
d_i   = (P[i] - P[i-30])^2 / 30
sig2_i = lam*sig2_(i-1) + (1-lam)*d_i,   lam = 2^(-1/300)        EWMA, half-life 300 s
s_t   = sqrt(c * sig2_(W+t-1) * V_t + s_b^2),  c = 1.19, s_b = 3.5 USD
z_t   = |m_t| / s_t;   side = up if m_t >= 0 else down
BE(a) = a + 0.07*a*(1-a)
```

V_t: 110.5 (150 s), 80.5 (180), 50.5 (210), 20.5 (240), 2.63 (270); past 270 s the oracle basis is the residual risk. The 1 s realized variance understates the horizon variance (VR(60) = 1.60), hence the 30 s subsampling; c is fitted label-free on the 8,976 pre-ladder windows. z depends on the spec: "z >= 3" gave 255 to 995 entries across the tracks' estimators.

**The fluid floor in USD** (z = 2.5; sigma at its 10th / 50th / 90th percentile) [syn]: 150 s 56 / 116 / 224; 210 s 38 / 78 / 152; 240 s 25 / 50 / 97; 270 s 12 / 20 / 35. The static $75 at 210 s is its median.

**Calibration.** Gaussian tails are wrong (standardized residual kurtosis 8-14 [chk]); use the frozen empirical table.

| z bucket (pre-ladder, 13 looks per window) [syn] | wins/n | rate | Phi predicts |
|---|---|---|---|
| [2.0, 2.25) | 5,051/5,153 | 0.9802 | 0.983 |
| [2.5, 2.75) | 3,705/3,735 | 0.9920 | 0.996 |
| [3.0, 3.5) | 5,039/5,060 | 0.9958 | 0.9994 |
| [3.5, 4.0) | 3,651/3,660 | 0.9975 | 0.9999 |
| [4.0, 5.0) | 5,066/5,070 | 0.9992 | 1.0000 |

| first crossing in 150-270 s (the stopped rule) [syn] | pre-ladder | ladder period |
|---|---|---|
| z >= 2.5 | 6,888/6,943 = 0.9921 | 2,623/2,637 = 0.9947 |
| z >= 3.0 | 6,437/6,463 = 0.9960 | 2,451/2,457 = 0.9976 |

Fixed-second cells overstate a stopped rule by 0.3-1.3 pp [T]: score the policy, not the cell. Out-of-sample reliability (3,735 test windows [chk]): [0.98, 0.99) observed 0.9859 against 0.9856 predicted; [0.99, 0.995) 0.9950 against 0.9929; >= 0.995: 0.9997 against 0.9991.

**Where the model beats the ask, and where it does not.**

| region | finding |
|---|---|
| all asks pooled, four anchors | the ask wins: logistic slope on logit(ask) 1.02, on z 0.0 +/- 0.12 (not detected; a fee-sized effect is below the test's resolution [chk]) |
| "ask cheap relative to the model", no z floor | loses: 2,233/2,457 (-0.17%/USD [chk]); 1,238/1,359 (-1.10%); the bucket with model edge above 3 pp is the worst (-3.7%, n = 514) |
| z >= 2.5 and ask <= 0.99 | z adds information: 0.9931 where random windows with the same ask in the same 30 s bucket win 0.9801 (p = 0.002 [syn]); high against low z inside ask 0.95-0.99: +0.90 pp (p = 0.048, unadjusted [chk]) |
| windows reaching z >= 2.5 | 27.5% ever fillable; the never-filled win 1,904/1,913 = 0.9953 against 0.9931 filled: no adverse selection detectable |

Inference: asks pile up at 0.99 (60-70% of entries), so the price stops separating a 0.985 window from a 0.999 one; z does. At 0.99 the edge is thin: 431/434, +0.24%/USD; below 0.99: 288/290, +2.06% [syn].

## 3. Executable results

One replay for every row [syn]: $25 worst price, book age <= 1 s, vwap > 0.80, pair sum 0.90-1.10; one entry per window at the first qualifying second; FOK limit = the cap, filled one sample later at that book's price; a kill continues the scan. Constants were fitted before the first ladder window, thresholds chosen on these 12 days: the halves are a stability check, not a hold-out.

| cell | W/n | Wilson lower | mean BE | net/USD | entries/day (last 4 d) | halves | matched-null p |
|---|---|---|---|---|---|---|---|
| z >= 2.5, 150-270, cap 0.98 below z 4 | 334/336 | 0.9786 | 0.9743 | +2.05% | 28.3 (16.2) | 170/172, 164/164 | 0.003 |
| z >= 3.0, 150-270, cap 0.98 | 85/85 | 0.9568 | 0.9722 | +2.92% | 7.1 (3.5) | 41/41, 44/44 | 0.021 |
| z >= 2.5, 210-270, cap 0.99 | 413/415 | 0.9826 | 0.9829 | +1.28% | 34.9 (17.8) | 213/215, 200/200 | 0.001 |
| z >= 3.0, 150-270, cap 0.99 | 254/255 | 0.9781 | 0.9849 | +1.17% | 21.4 (12.2) | 112/112, 142/143 | 0.029 |
| z >= 2.5, 150-270, cap 0.99 | 719/724 | 0.9839 | 0.9838 | +0.97% | 60.9 (38.8) | 381/384, 338/340 | 0.002 |
| static: settlement \|m\| >= 100, 150-270 | 562/587 | 0.9379 | 0.9591 | -0.25% | 49.4 (41.0) | 364/379, 198/208 | 0.51 |
| static: settlement \|m\| >= 150, 150-270 | 198/202 | 0.9502 | 0.9687 | +1.22% | 17.0 (11.8) | 137/141, 61/61 | 0.16 |
| static: settlement \|m\| >= 100, 210-270 | 140/142 | 0.9501 | 0.9725 | +1.45% | 11.9 (8.5) | 95/96, 45/46 | 0.06 |
| static: time-scaled floor, constant sigma, z >= 3 | 588/606 | 0.9535 | 0.9704 | -0.03% | 51.0 (32.2) | 420/431, 168/175 | - |
| static: `d150_f100_c0.99`, point basis, retry to 165 s | 351/362 | 0.9464 | 0.9510 | +1.97% | 30.4 (24.2) | 231/241, 120/121 | - |

- Cross-check: the replay reproduces track T's static rows exactly (1,008/1,064 for the $75 settlement floor); the checkers' own adaptive cells are 572/576 (+1.30%) and 349/350 at +1 s.
- Volatility scaling is the ingredient: at the same z threshold a constant sigma gives -0.03%, the EWMA +1.17%. By volatility tercile the z >= 2.5 cell trades 440 / 182 / 102 windows (+1.38% / -0.33% / +1.51%); the $100 floor trades 3 / 97 / 487: only loud windows, which the market prices correctly.
- Paired net per day at $1 staked, adaptive minus control (day bootstrap): against the $100 settlement floor +0.70 [-0.07, +1.52]; against the $150 floor +0.37 [-0.05, +0.86]; against `d150_f100` -0.02 [-0.79, +0.82], with 23 windows in common out of 336 and 362.
- Latency: a 1 s older price moves the five cells to +0.66 to +3.4%/USD, a 2 s order delay to +0.91 to +3.1%; the dynamics checker's cell fails the Wilson bar at 2 s and in 6 of 10 estimator settings at 1 s [chk]. A stale limit is killed 30% of the time (the live FOK rate); limit = cap fills 82.5% [chk].
- Capacity: the z >= 2.5 cell filled 19-60% of signal windows per day through 09-27 and 14-17% on each of the last four days.
- Permutation verdict. Fixed anchors, 160 policies: family-wise p = 0.64 against break-even, 0.61 against ask-matched selection: nothing [chk]. Event-time z family (the five cells plus z >= 3.5): max-T p = 0.012 against break-even, matched-selection p <= 0.03 in five of six [syn]. The six survived every track's search: a reason to pre-register, not evidence.

## 4. Family v4

**Grammar `band_event_v4`, evaluator `executable_truth_v4`.** Cell = (range [t0, t1], z*, cap rule). Fixed for the family: the spec of section 2 (c, s_b and the P(win | z) table refitted once on all windows before `registered_at`, then frozen in the campaign file); side = sign(m_t); buffer 0.5 pp inside the schedule cap; patience is the scan itself: evaluate every ladder second, send limit = cap, re-scan after a kill; one entry per window; $25.

Schedule cap `k98`: the largest tick with BE(tick) <= Wilson lower of the frozen z-bucket - 0.005; today 0.98 for z in [2.5, 4) and 0.99 from z = 4.

| id | cell | rationale | entries/day, sample (last 4 d) | median days to e >= 120 | P(discovery by day 90) at half the edge |
|---|---|---|---|---|---|
| V1 | `e150-270_z2.5_k98` | the surviving form of a dynamic cap: tighter at lower z | 28 (16) | 22 (37) | 0.42 (0.18) |
| V2 | `e150-270_z3.0_c0.98` | strict; Wilson bound clears BE at its cap from n >= 330 | 7 (3.5) | 47 (> 90) | 0.12 (0.03) |
| V3 | `e210-270_z2.5_c0.99` | late range, locked average | 35 (18) | 32 (60) | 0.24 (0.09) |
| V4 | `e150-270_z3.0_c0.99` | the theory and dynamics tracks' headline | 21 (12) | 41 (71) | 0.15 (0.07) |
| V5 | `e150-270_z2.5_c0.99` | volume; flat-cap control of V1 | 61 (39) | 32 (50) | 0.25 (0.14) |
| V6 | `d150_f100_c0.99_p0` | the incumbent static, disjoint windows | 22 (15) | 46-78 (ladder doc, N = 4 bar) | 0.20 |

Power: 3,000 simulated paths of the repo's mixture e-process on each cell's own entry prices; win probability = the pre-ladder first-crossing rate at every ask (optimistic), at the sample rate (last-four-day rate). At zero edge futility stops V1 and V5 in 39-67% of paths by day 90.

- **Paired static controls, outside N:** V1 and V5 against the $100 settlement floor over 150-270 s and against the constant-sigma z >= 3 floor; V3 against the $100 floor over 210-270 s. Per-window net difference through `update_signed`, K = 3, bar e >= 60 on >= 100 paired windows.
- **e-BH:** N = 6, alpha 0.05: e >= 120 alone, 60 / 40 / 30 for 2 / 3 / 4 discoveries; n >= 100 over >= 14 days; tripwires as today.
- **Stopping:** the e-process is valid at every tick; e <= 0.1 kills a cell; day 45 and day 90 rules in section 6.
- **Reports per cell:** the excluded population (signal windows never filled), the ask-and-time matched contrast, max-T over the family, net at +1 s older price and at 2 s order delay, capacity per day.

## 5. Changes required, in order

| # | change | where | size |
|---|---|---|---|
| 1 | Evaluator v4: `strike_60s`, per-second settlement margin, sigma2, z and P_cal columns; event-time replay over the tiled ladders; book age <= 1 s plus a stall flag (the `fresh` flag admits 19-29 s old books: 8 false clears on 09-19 [chk]); matched null and max-T | Mac, Python, tested | 3-4 agent days |
| 2 | Observer records: strike, sigma2, z, covering tick price and time per sample, quotes for the settlement side (120 of 2,637 signal windows show only the opposite side) | engine, records only, with a test | 1-2 agent days, one VPS build; as built (`px`, `px_age`, `m` per sample and the other side's book on every sample of a directional record) about 11 MB a day at the anchors of row 3 |
| 3 | Anchors `120,150,180,210,240`: continuous 120-270 s; 195 and 225 only duplicate samples | env line | 1,440 records, 5.1 MB a day; 120-150 s stays diagnostic (first crossings there win 0.9906 [chk]) |
| 4 | Log the Chainlink TWAP stream (RTDS `prices.crypto.twap`) beside Binance | engine, new feed, records only | 2 agent days; removes most of s_b and the 2 s oracle delay if it holds |
| 5 | ETH/SOL/XRP 5m and BTC 15m | print pre-check on the Mac, then the engine | 1 day, then several; same rule and fee, $16-145 printed per window against $600-1,250; the "3x trades" claim is unverified [chk] |

Registration does not wait for steps 2-5: the four live anchors already tile 150-270 s.

## 6. Risks, falsifiers, kill

- Selection: no hold-out remains on these 12 days.
- Skew: at 0.99 one loss costs 106 wins; z = 5.4 lost once in the sample (09-19 17:15, a stale-book window).
- Estimator: half-life 900 s gives 279/279 where 300 s gives 254/255; another track saw the edge halve at 900 s.
- Proxy: Binance stands in for Chainlink; the market sees the real strike.
- Competition: 95.6% of fillable signal-seconds have a public print within 2 s; queue position at $25 is unmeasured.
- Capacity is falling.

Falsifiers on fresh windows, fixed now:
1. Day 30: z-selected win rate minus the matched-random rate <= 0 on V5 (about 1,000 entries): z adds nothing beyond the ask.
2. First-crossing accuracy of z >= 2.5 below 0.9897 (the pre-ladder Wilson lower) at n >= 2,000: the calibration broke.
3. Day 45: paired e of V1 against its controls below 1 while V6 leads: "adaptive" is dead even if an edge exists.

Kill: day 45 with fresh net at +1 s <= 0 on both V1 and V5 stops the family; day 90 without an e-BH discovery stops taker work on BTC 5m; on V1, V3 and V5 a 7-day median under 10 entries a day, or a fill share under 0.10, kills the cell.

## 7. Fourteen days

| day | who | action |
|---|---|---|
| 0 | operator | Decide: (a) v4 N = 6 replaces the static N = 4 of the ladder doc (do not run `--register` before day 6); (b) the "WR > 0.995 at n >= 100" tripwire will hold V2 and V4 in `manual_audit` by design (ceiling 0.996): keep it as an audit against the frozen ceiling, or restate it; (c) under the funding rule a cap-0.99 cell needs n >= 1,260 (z 3) to 17,900 (z 2.5) entries, so only V1 (n >= 450-620) and V2 (n >= 330) can be funded within the horizon; (d) a VPS build slot on day 5 |
| 1-4 | agent, Mac | Step 1, one tested commit per item, suite green each time |
| 3-5 | agent, Mac | Step 2 with `cargo test`; anchors in `deploy/band-observer.env` |
| 5 | operator, VPS | Recipe H of the basement doc as superseded 2026-10-01 (H never ran; the env also carries the twin pin and the $100 paper base, and without the twin the observer exits 2 and stays down): `nice -n 10 cargo build --release --locked -j 1`, install the twin under `paper_twins/`, install both envs, `preflight --mode paper` on the new binary, then `sudo systemctl restart polymomentum-band-observer`; check `band_ladder` at 120/150/180/210/240 s with `strike_60s`, per-sample `px`/`px_age`/`m` and `sq`/`sc`/`sage`, `quote_budget_usd` 25.00, no `order_placed` |
| 6 | operator, Mac | `bash scripts/pull_vps_sessions.sh`; `uv run --offline python scripts/executable_truth.py --build`; `--register 2026-10_band_event_v4 --cells V1..V6` (the flag lands in step 1; freezes c, s_b and the table; needs step 1 only, not the VPS build); commit the campaign file, push |
| 6-14 | runner | accrual through `--tick`; agent: step 4 design note, step 5 print pre-check |
| 14 | agent | first formal look: support, capacity, coverage, engine z against evaluator z, +1 s and 2 s numbers, matched contrast. Money: none |

## 8. As built (2026-10-01, commit 1868c36 and the registration commit after it)

Steps 1-3 of section 5 are in the tree: `scripts/settlement_model.py`, evaluator `executable_truth_v4` with grammar `band_event_v4`, the engine records and the five anchors. The VPS still runs the 2026-09-20 binary (four anchors, no settlement fields) until day 5. No campaign is registered.

**Reproduction of section 3** (`--event-grid --cells V1..V6 --s-b 3.5`; rows V1, V2, V3, V4, V5):

| table | windows | c | W/n |
|---|---|---|---|
| research snapshot, c given at the scratch value | 3,425 | 1.1872 | 334/336, 85/85, 413/415, 254/255, 719/724: exact |
| research snapshot, c refitted by the evaluator | 3,425 | 1.1878 | 333/335, 85/85, 412/414, 254/255, 718/723 (first crossings at z = 2.4996 instead of 2.5) |
| live table (291 Mac-held rows upgraded to their VPS records, plus new windows) | 3,529 | 1.1878 | 345/347, 86/86, 422/424, 259/260, 736/741; V6 258/266 |

On the live table: net +2.05 / +2.92 / +1.54 / +1.16 / +1.12 %/USD, max-T p = 0.014, a 1 s older price +0.79 to +3.37%, a 2 s delay +1.07 to +3.09%. With s_b fitted (3.6) instead of given: 343/344, 85/85, 414/415, 251/252, 727/731.

**Where the code differs from sections 3-6** (each needs the operator's yes or a change before `--register`):

1. Fill model: an order in flight is not protected by the entry gates, so it fills at a collapsed ask; the scratch replay refused a fill with vwap <= 0.80 and scanned on. One such fill in the sample (09-19 06:25, limit 0.99 filled at 0.47, won, `fills_outside_gates` = 1): it is +0.25%/USD of V3's +1.54% and +0.14% of V5's +1.12% (without it 421/423 at +1.30% and 735/740 at +0.98%). It also moves the 1 s older price row: on the research snapshot a second such fill (09-22 23:45, 0.69, won) gives +0.77 to +3.40% against +0.66 to +3.4%.
2. `wr_too_good` of an event cell is a 5% binomial test of its wins against the frozen ceiling, not the point comparison: item (b) of day 0 restated, so V2 and V4 are not held by design.
3. The capacity kill of V1, V3 and V5 gives no verdict before 14 UTC days of data, and the fill-share floor is each cell's own: half its share over the last 7 complete days before registration, at most 0.10 (dry run: baselines 0.139 / 0.158 / 0.286, floors 0.069 / 0.079 / 0.10). The flat 0.10 was read off the cap-0.99 cells; V1 and V3 pooled 0.086 and 0.093 over 09-27 to 09-30 and would have been killed on data-day 14. The 10 entries a day floor is unchanged.
4. The day-90 stop reads the discoveries recorded at any tick so far, a cell held for a defect audit included.
5. F2 compares the fresh sample's Wilson upper bound with the frozen pre-ladder rate (0.9919) at n >= 2,000, not the fresh point rate with 0.9897: the point comparison fires on about 12% of healthy samples, the bound on about 2.5%. A fired falsifier holds the event cells out of the e-BH candidate set until `--clear-falsifier <id> --note ...` records the audit (a new operator action).
6. Registration refits c and s_b on every labelled window before `registered_at` (dry run on a copy: c = 1.1786, s_b = 3.6 fitted, not 3.5). That table lifts the `k98` cap of z in [3.5, 4) to 0.99 ("0.99 from z = 4" in section 4 is the pre-ladder table), and F2's bar becomes 0.9895 (6,876/6,932 under the refitted spec) against 0.9897.
7. sigma2, z and P_cal are not table columns: they depend on the frozen spec and are computed when a cell is scored. The table carries `strike_60s`, `settle_margin`, `final_settle_margin`.
8. The engine records `strike_60s`, `px`, `px_age`, `m` and the other side's book, not sigma2 or z (the evaluator computes both from its own 1 s closes; the engine's `px`, `m` and strike are there for the day-14 comparison, which no code reads yet). `--gate-json` refuses an event cell: the engine has no event policy.
9. Halves in the evaluator split the entries in two by count; section 3 split the windows at the midpoint of the period.
