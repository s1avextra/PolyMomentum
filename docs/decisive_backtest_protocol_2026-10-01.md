# Decisive backtest protocol for family v4 (final, 2026-10-01)

Status: design only. No archive bytes were pulled and no label-conditioned statistic of any z or event rule was computed on windows before 2026-09-19. This replaces `protocol_draft.md` after two attacks (statistics, microstructure). While it was written the other workflow committed evaluator v4 (1868c36, bc2188c) and registered the forward campaign `2026-10_band_event_v4` (5298533: N = 6, registered 2026-10-01T12:28:16Z, spec sha256 4a39ff2c...) [V campaign file, read-only]; the text below takes that as given.

Tags: [V] verified in this revision (script rerun or fetch named), [U] unverified (estimate, or a scout claim not rerun). Source key, all under `scratchpad/decisive/`: A1..A5 = `attack/a1_arith.py` .. `a5_power.py`; M:x = `critic_micro/x`; F:x = `final/x` (written for this revision); D:x = `design/x`; P:x = `pilot/x`; B:x = `scoutB/x`.

| name | span (UTC window starts) | windows | status for the frozen v4 cells |
|---|---|---|---|
| P0 | 2026-08-14 00:00 -> 08-18 23:55 | 1,440 | out of this protocol (section 2.5) |
| P1 | 2026-08-19 00:00 -> 09-19 05:20 (ws 1787097600..1789795200) | 8,993 slots, 8,991 in the table [V sqlite, read-only] | execution holdout: signal level published with labels; V1 (cap 0.98) never scored on executable data |
| P2 | 2026-09-19 05:25 -> 10-01 12:05 (ws 1789795500..1790856300, the forward registration cut) | 3,533 VPS ladder windows in the table at 11:45 [V sqlite] | burned: about 600 variants; parity anchor only |
| F | after ws 1790856300 (registered 2026-10-01T12:28:16Z) | 288 a day | fresh; used here label-free for parity only |

## 1. The answer in five lines

1. Yes, in 4 to 6 days: a free full-depth L2 archive (Pendulum Flow V3) covers every hour of P1 and P2, so the engine's per-second $25 ladder can be rebuilt for 31 earlier days (8,991 windows), proven label-free against our own ladders first, and family v4 replayed on it once, blind. Expected entries: V1 830-970, V5 2,060-2,580 [U], against 336 and 724 on P2.
2. What it measures is fill adverse selection and available prices on P1, not the signal: P1 signal accuracy is already published (55 losses in 6,943 first crossings), so the outcome is largely predictable. With r = the odds ratio of a loss window being a filled window: V1 PROCEED has probability 0.78-0.96 at r = 1, 0.38-0.71 at r = 1.5 (P2's own point estimate is 1.47 [0.39, 4.91]), 0.01-0.05 at r = 3; KILL is reachable only at r >= 3 (0.11-0.16) to 4 (0.43-0.52) [V F:decision_frozen.py]. The headline output is r-hat with an exact interval (about [0.5, 1.8] wide at r = 1 on V5) and one result not implied by published numbers: the ask-and-time matched null on about 2,000 rebuilt entries.
3. It cannot tell: today's capacity (V5 fill share fell from 0.322 to 0.156 inside P2 [V A4]), whether the edge outlived September, whether a displayed 0.98-0.99 ask becomes our fill (no live order was ever sent at >= 0.93 [V M:live_orders.py]), or an edge of half the discovery size. The false-confirm rate (at most 4% if missed entries lose up to 3x as often) refers to the P1 population, not to the forward market.
4. The backward look is not on the critical path: the forward family was registered today (N = 6, V1 bar 120), so the forward clock already runs. Start the engine event policy and paper twin now. The probe decision needs fresh VPS ladder rows under CLAUDE.md section 2: median forward day 28 at today's 16.2 V1 entries a day (17 at 28.3 a day, 36 if the true rate is the 0.992 ceiling) [V D:forward_sim.out, A3]: about 2026-10-18 to 11-06, instead of day 45-90. Re-registering within a day or two with V1 at a bar of 40 would move the median to day 24 (section 4): the operator's call.
5. Nothing here funds anything on backward data, and no rule changes. The backward look costs zero forward days, three operator touchpoints, and 45-135 GB of free range reads; its lasting product is a parity-certified book instrument that makes the next family testable in days.

## 2. The layered backtest

### 2.0 What was already looked at before 2026-09-19

- Signal level, with labels, on all pre-ladder windows: the P(win | z) table; first-crossing accuracy z >= 2.5 6,888/6,943, z >= 3.0 6,437/6,463 [V research doc section 2]; checker runs at other thresholds; label fits of s_b and the table.
- Print level, with labels, once: `adaptive/D/d3_prints_cache.py` printed win rate, Wilson lower, BE and net of the first favourite print <= 0.99 at |z_o| >= 3.0/3.5/4.0 per 15 s bin, pooled over P1 and P2 windows [V code read; output not saved, not regenerated]. That is a naive-print sibling of V4 and, by nesting, of V5. V4 is therefore non-gating here.
- Static rules: `band_grid_v3` print model on P1; the live canary; the fresh gate.
- Executable sources inside P1 that the draft missed: 81 Mac-host ladder windows 2026-09-18 12:30-21:25 in `windows.sqlite3` [V]; 827 `band_anchor` records on 09-02/09-03, 55 `placed` records with `book_best_ask`, 53 live fills and 28 venue FOK kills 08-25..09-03 [V M:p1_engine_obs.py, M:live_orders.py]. Whether a static ladder grid touched the 81 Mac windows is [U]; no z rule did, by the code read. All are used here label-free, for parity only.
- Correct claim: V1 (cap 0.98) was never scored on P1 executable data. Label-dependent parts of the spec touch P1 (the P table, hence the `k98` schedule; s_b; c in the registration refit).

Consequence: P1 is an execution holdout, not a signal holdout. The registration file says so.

### 2.1 Layer B: historical order books

Source [V B:manifest_*.json, B:pend_cov.json; no bytes fetched in this revision]: `https://dl.pendulumflow.com/v3/YYYY-MM-DD/HH/YYYY-MM-DDTHH.parquet`, one file per UTC hour from 2026-08-18T06, about 5 h behind real time, 0.8-1.3 GB per hour, range requests, no account, CC BY 4.0 (credit pendulumflow). Products sorted by `market` in separate row-group ranges: `book` (snapshots: `timestamp`, `asset_id`, `bids`, `asks`), `price_change` (deltas: `price`, `size`, `side`, plus `best_bid`, `best_ask` after the change), `last_trade_price` (`price`, `size`, `side`, `transaction_hash`), `tick_size_change`, `new_market`, `market_resolved` (winner: never read by the builder).

| span | hours | complete / partial | capture machines per hour |
|---|---|---|---|
| P1 08-19T00..09-19T05 | 750 | 744 / 6 (42-58 minutes each, 08-20T17..08-21T02) | 1 in 19 hours (all inside 08-20T14..08-23T03), 2 in 108, 3 in 46, 4-6 in 577 |
| P2 09-19T05..10-01T07 | 291 | 291 / 0 | 3-6 |

Era differences that matter [V manifests]: August hours are rebuilt from seven product files (`derived_from`), carry no witness columns, and predate merger stamp `fcbb2804`; `book` snapshot rows per hour differ 4.7x (864,061 on 08-25T12, 184,939 on 09-25T10, 619,320 on 10-01T05). A snapshot-only rebuild would not transfer between eras: the deltas stay in.

Rebuild (label-free by construction):
- Clock: the exchange `timestamp` column (ms) only, never `timestamp_received`, so the phase does not depend on capture machines. Ties at equal (asset, ms) are applied in file order; every snapshot resynchronises the book.
- Per window, both tokens: snapshots plus deltas; sampled at W + s + phase for s = 150..270; the engine quote function (`buy_book_quote_from_budget`, `rust_engine/src/execution/sizing.rs`) ported to Python, with `live_market_tick_size` and the 0.01 -> 0.001 transition driven by `tick_size_change` (open markets quote tick 0.01, closed ones 0.001 [V Gamma/CLOB probe]; sub-cent $25 prices are 0.6% of P2 samples in 0.96-0.98 and 1.3% in (0.98, 0.99], on the cap boundary [V M:subcent.py]). Budgets $5/$25/$100 on P2, $25 only on P1.
- Output in the `band_ladder` JSON shape with `host: "pendulum_v3"` under `logs/strategy-research/backward/`, never in `windows.sqlite3`.
- Replay: the evaluator's own event functions and frozen spec, bound by git sha and spec sha256 at stage 1 (one code path, one spec).

Evidence so far that it is the same book [V B:xcheck.py, M:flicker.py rerun]: on window 1790330700 the archive best ask equals our $5 worst price on 106 of 117 samples at +250 ms (80 at 0 ms, 80 at +500 ms); on 7 windows of 2026-09-25T10, in the region ask 0.90-0.99, exact agreement is 95.6% at +250 ms, 86.7% at 0 ms, 83.6% at +500 ms. That hour suggested the phase, so it is excluded from validation.

### 2.2 The parity gate (stage 0: committed before any pull beyond the pilot)

Benchmarks the bars come from, all label-free on P2 engine ladders, region = engine $25 worst ask in [0.90, 0.99], gated samples [V F:gate_oc.py, F:twin_oc.py, A2]:

| instrument | exact share | within 0.01 | mean signed diff | cell parity (precision / recall) |
|---|---|---|---|---|
| honest twin: the same engine on the Mac against the VPS (568 overlap windows) | 0.895 pooled; 0.883-0.925 by day | 0.981 | -0.0005 | V1 0.927 / 0.974 (n 41, 39); V5 0.990 / 0.990 |
| engine ladder against itself, +1 s (look-ahead) | 0.674; 0.615-0.734 by day | 0.904 | +0.0006 | V1 0.911 / 0.911; V5 0.970 / 0.894 |
| the same, -1 s (stale) | 0.666; 0.607-0.726 | 0.898 | -0.0025 | V1 0.852 / 0.994; V5 0.871 / 0.999 |
| the same, +2 s | 0.531 | 0.828 | +0.0013 | V1 0.841 / 0.848 |

What a tolerated misalignment costs [V A2]: at +1 s V1 shows 3 losses instead of 2; at +2 s 6 instead of 2 (net +2.05% -> +1.30%). The draft's bars (precision and recall >= 0.90, ratio 0.93-1.07) accept the +1 s instrument in 28% of four-day draws; the exact-share bar below accepts a +/-1 s or +/-2 s instrument in 0 of 500 draws and voids the honest twin in 0 of 2,000 [V F:gate_oc.py, F:twin_oc.py]. Entry-second agreement was tested as a candidate bar and does not discriminate (74-94% same second under a 1 s shift): not used. The sign test catches only a stale instrument. Sub-second phase errors up to about 250 ms pass the gate (exact 0.84-0.87 on 7 windows), so the decision must also hold at phase +/-250 ms (section 3.2).

Split of P2 (fixed now): calibration 09-20..09-23 (phase, delta semantics, tick logic; free iteration); development validation 09-24..09-27 without hour 2026-09-25T10, one logged retry; final holdout 09-28..10-01 plus every fresh day up to stage 1, judged once. 09-19 is not pulled. Pilot hours, drawn with seed 20261001 from the 96 calibration hours [V]: 09-20T04, 09-20T15, 09-20T20, 09-21T16, 09-22T10, 09-23T02 (72 windows, all with VPS ladders [V]), plus two P1-era hours for schema and integrity only: 08-21T15 (single witness) and 09-10T07.

Phase procedure: grid -500..+1000 ms in 125 ms steps; the phase maximising the $5 exact share in the region on calibration days; frozen at stage 1.

| gate | bar (label-free) | judged on |
|---|---|---|
| G1 sample parity | exact >= 0.85 at $5 and $25; within 0.01 >= 0.95; abs(mean signed diff) <= 0.002; no day below 0.80 exact; $100 reported | final holdout |
| G3 availability | rebuilt quote present on >= 97% of engine-quoted gated samples (twin: 98.2%) | final holdout |
| G4 cell parity, engine-quoted support only | V5: entries ratio 0.93-1.07, precision and recall >= 0.90, mean entry price within +/-0.002 on matched windows. V1 (pooled development + final + fresh): precision and recall >= 0.90; either in [0.80, 0.90) -> report-only, no PROCEED or KILL; below 0.80 -> void | as stated |
| G6 integrity, relative | per ISO week of P1: share of book levels differing between the delta-replayed book and the next snapshot <= 2 x the final-holdout value + 0.2 pp; share of `price_change` rows whose carried `best_ask` equals the rebuilt best ask >= the final-holdout value - 1 pp (row semantics confirmed in the pilot [U]) | P1, before unblinding |
| G7 book-to-fill | the 53 live fills and 28 FOK kills of 08-25..09-03 replayed on the event-time book: predicted fill iff size at <= limit is resting at the matching instant (record `ts` minus half its `submit_latency_ms`; kills: `ts` - 98 ms); agreement >= 85% (always-fill scores 65%); +/-100 ms reported | 30 P1 hours, before stage 1 |
| G8 in-holdout parity | rebuilt quote at `ts` - 50 ms against the 827 `band_anchor` records ($5.38 budget; 1,303 side quotes, 225 in the region [V]): exact >= 0.80, within 0.01 >= 0.95, abs(signed) <= 0.002; the 81 Mac ladder windows of 09-18 at the fitted phase: exact >= 0.80; the 13 in-region `placed` best asks reported | 29 P1 hours, before stage 1 |

Dropped from the draft: G2 (the +/-1 s envelope passes at every phase from -500 to +1000 ms [V B:xcheck.py]); G5 (replaced by the phase rows of the decision); every bar that used outcomes (net, Wilson verdict). G8's exact bar is 0.80, not 0.85, because its sample instant is fixed rather than fitted: a 250 ms offset alone costs up to 12 points on the 7 windows, while a 1 s shift scores 0.67.

Verdicts. All pass -> ladder-equivalent. G7 below 85% -> every Layer B net is stamped "touch upper bound" and PROCEED is unavailable. G8 fails -> one logged fallback (offset fitted on the 09-02 records, judged on the 09-03 records); fails again -> Layer B void on P1. G1, G3 or G4 fail on the final holdout -> Layer B void. A void ends the backward look as "instrument not certified": no kill, no fall-back test.

pi_lo and rho_lo (used in section 3): 5% lower bounds of V1 precision and recall from a day-block bootstrap (2,000 resamples) over development, final and fresh days, widths published. Expect point - 3 to 4 points [U]: about 200-250 V1 entries.

Also reported, not gated: whether each engine loss window on P2 (5 for V5, 2 for V1) is a rebuilt entry; the 5 s event-gap footprint (on engine ladders 1.0-1.1% of windows, 0.9% of V1 entries, none of the P2 losses [V A4]; above 3% on the archive -> manual audit); the footprint of the "no snapshot in the 60 s before second 150" exclusion, measured on the 56 pre-declared P1 hours before stage 1; archive `last_trade_price` time against the Data-API second for the same `transaction_hash` on 15 seeded windows, 3 per ISO week (weekly median within +/-1 s of the P2 value, else audit of that week).

### 2.3 Layer P: public prints (one audit, nothing else)

Kept: the label-free audit on P1 from the existing cache. The share of persistence-proxy entries (8,910 covered windows; V1 648, V5 1,745 [V P:res_preladder_blind.json]) that are also rebuilt-ladder entries must be >= 0.85 per cell (0.92-0.97 against engine ladders on P2 [U scout C]); lower -> manual audit before unblinding. Cut: the P0 proxy run, the fall-back kill (power 0.13-0.40), re-fetched tapes.

### 2.4 Layer L: our ladders

3,533 VPS windows and growing; burned for selection; here the parity anchor. Continuing parity: each forward day the archive hours are rebuilt and compared under G1 and G3; a failing week voids Layer B for later use. The next observer build should stamp each sample with its cycle time in ms (the tree already adds `px_age`), so phase need not be inferred.

### 2.5 Cut from the draft

Layer S on P0 and the 30 s transfer test (power 0.45-0.56, false trigger up to 8.8% for the two nested triggers [V A5], regime mismatch 1.6 against 4.0 USD/sqrt(s) [U draft], and a label-contamination hazard for the forward refit): report-only, after the look, as a two-sample comparison, with P0 labels kept out of `windows.sqlite3` (a re-registration would refit on them). The P0 tail rebuild and P0 tapes. The capacity KILL on P1. `spec_alt` as a gate. V2, V3, V4, V6 and the static controls as gating cells. The amended-Wilson proposal. PROCEED-SHORTENED and the 60-day horizon.

## 3. Blind pre-registration and the decision

### 3.1 Three commits

`deploy/campaigns/2026-10_band_event_v4_backward.json`, `lane: "band_backward"`: `load_campaigns` keeps only `lane == "band_ladder"` and is the directory's only reader, so `--tick` ignores it [V code read 2026-10-01].

- Stage 0 (day 0, before any pull beyond the pilot): section 2.2 in full, the decision design of 3.2, the pre-declared P1 hours of G7/G8. Content: section 11.
- Stage 1 (after the final parity verdict, G7 and G8; before the P1 pull): evaluator and builder git sha, fitted phase, gate results, pi_lo, rho_lo. The spec is already bound in stage 0.
- Stage 2 (after the label-free P1 rebuild, before any join; same operator session as the unblind): sha256 of the entry list (window, second, side, price, cap, cell; no label) and of the excluded-window list; n and mean BE per cell; the integer loss thresholds from the stage-0 formulas; a simulation on the actual entry lists of P(all PROCEED conditions) at r = 1 and 1.5 and of P(V1 KILL and V5 KILL) under no edge. Then `--unblind` runs once; it refuses without the stage-2 hash and writes one ledger row.

Blindness is procedural: P1 labels are on disk. The builder has no label import (a test enforces it), `market_resolved` is never read, the Gamma id projection keeps `conditionId`, `clobTokenIds`, `outcomes`, `slug` in memory before any write, and the join exists in one script that runs once.

One spec: the forward campaign's frozen spec, sha256 4a39ff2c9f92b373ecfdf30d94314eb1a375848ea4b9da3bbacae651bd05e8d7 (c = 1.1787293210536418, s_b = 3.6; `k98` = 0.98 for z in [2.5, 3.5), 0.99 from 3.5) [V campaign file]. The research-doc constants (c = 1.1871935769912896, s_b = 3.5, 0.99 from z = 4) are a report row, not a gate. Under the frozen spec the pre-ladder reference is 56 losses in 6,932 first crossings (research: 55 in 6,943), and V1 takes more entries at cap 0.99 than in the research replay: the expected n (830-970) and mean BE (0.9749) below are [U] to that extent and become exact at stage 2.

### 3.2 Endpoints and rules

Primary: V1, one-sided alpha 0.04. Secondary: V5, alpha 0.01. Co-primary for PROCEED (both must pass, no further adjustment): the matched null on V5's rebuilt entries. V2, V3, V4, V6: descriptive rows without thresholds. Reasons: V3's kill bar (0.9932) lies above the published ceiling (0.9921) and its confirm power is 0.13-0.19; V2 needs zero losses in about 240; V4 is not blind; V6 needs another code path. The microstructure attack proposed V1, V3, V5 at alpha/3; the single-primary design is kept because it spends no alpha on cells that cannot change the decision: its confirm power for a ceiling-rate V1 matches the draft's corrected 0.92-0.95 although its null is stricter (item 1 below).

n = rebuilt entries, L = all their losses, n_eff = floor(n x pi_lo).

PROCEED (V1), all of:
1. Exact binomial test: P(Bin(n_eff, q0) <= L) <= 0.04 with q0 = (1 - mean BE) / (rho_lo + 3 (1 - rho_lo)). Worst case on both sides: every loss sits in the engine-fillable share, and the engine entries the rebuild missed lose three times as often as the observed ones.
2. Matched null: the win rate of V5's z-selected entries minus that of random windows at the same $25 ask and 30 s bucket is > 0 with one-sided permutation p <= 0.05 (evaluator function, 2,000 replicates, seed 20261001). On P2: 0.9931 against 0.9801.
3. Point net/USD at +1 s >= +1.02% (half the discovery edge).
4. Timing rows: condition 1 also holds at fill lag 0 and 2 s and at phase -250 and +250 ms.
5. Net > 0 in the race-adjusted stratum (an entry counts as killed when taker BUY volume at <= cap on the token plus taker SELL volume on the complement at >= 1 - cap, within 300 ms after the decision, exceeds resting size at <= cap minus our shares), and the verdict is the same with and without settlement-side-only windows and with and without hours under 3 capture machines.
6. Regimes, label-free strata: net > 0 from ws 2026-09-05 22:30 UTC on (the venue's maker-reward config appears between 22:20 and 22:55 [V Gamma probe]; capacity jumps from 09-04 18:00 with falling volatility [V M:break_hours.py]) and net > 0 on the highest tercile of daily mean sigma (cuts 4.50 and 5.11 USD/sqrt(s); 124 of V1's 648 proxy entries [V F:strata.py]; pooled with the middle tercile if it holds fewer than 150 entries). Otherwise the outcome is "capacity-conditional" and counts as INCONCLUSIVE.
7. Day-block bootstrap: the 5% lower bound of net/USD is > 0.
8. Gates passed, G7 >= 85%, no tripwire of section 7 open.

KILL (V1): the 97.5% Wilson upper bound of the win rate is below mean BE + 0.0102 x mean price, the losses fall on at least 3 UTC days, and the gates passed. V5 likewise with 0.0050. Family KILL: V1 and V5 both KILL. INCONCLUSIVE: anything else.

Reported with the verdict: r-hat per cell with its exact conditional 95% interval; n, L, mean BE at entry and at cap; net by ISO week (V1 proxy counts by week: 51, 117, 134, 272, 74 [V F:strata.py]; 360 of 648 fall in the nine days 09-05..09-13); net post-stratified to the entry-price mix of the last four P2 days (the forward-relevant figure); an event-time fill variant at decision + 117, 196 and 301 ms (the measured submit latency: min, median, max of 55 live orders [V]); "ask <= cap at every event in the fill second"; the research-constants row; incident hours with and without.

### 3.3 What to expect (arithmetic on published counts only, [V F:decision.py, F:decision_frozen.py, A1, A5])

V1, mean BE 0.9749 (P1 proxy prices [V P:res_preladder_blind.json]); probabilities conditional on the frozen reference, 56 losses in 6,932, by r:

| n; pi_lo / rho_lo | PROCEED when L <= | KILL when L >= | P(PROCEED) at r = 1 / 1.5 / 2 / 3 / 4 | P(KILL) at r = 1.5 / 2 / 3 / 4 / 6 |
|---|---|---|---|---|
| 830; 0.95 / 0.95 | 10 | 20 | 0.94 / 0.66 / 0.33 / 0.04 / 0.004 | 0.0005 / 0.009 / 0.15 / 0.50 / 0.93 |
| 830; 0.90 / 0.90 | 8 | 20 | 0.78 / 0.38 / 0.13 / 0.01 / 0.000 | same |
| 900; 0.95 / 0.95 | 11 | 21 | 0.95 / 0.68 / 0.35 / 0.05 / 0.004 | 0.0005 / 0.009 / 0.16 / 0.52 / 0.94 |
| 900; 0.90 / 0.90 | 9 | 21 | 0.82 / 0.42 / 0.14 / 0.01 / 0.000 | same |
| 970; 0.95 / 0.95 | 12 | 23 | 0.96 / 0.71 / 0.37 / 0.05 / 0.004 | 0.0002 / 0.004 / 0.11 / 0.43 / 0.90 |
| 970; 0.90 / 0.90 | 10 | 23 | 0.85 / 0.45 / 0.16 / 0.01 / 0.001 | same |

- Unconditional binomial, losses over n: P(PROCEED) at the ceiling rate 0.92-0.95 (0.95/0.95), 0.77-0.83 (0.90/0.90; the same at 0.87/0.90); at 1.5x the loss rate 0.58-0.60 and 0.33-0.37. Conditions 2-7 lower these by an amount stage 2 simulates. The bullets below use the research counts (55 in 6,943); the frozen reference moves them by 0.01-0.03.
- False confirm: 2.3-3.9% by construction if missed entries lose up to 3x as often; 5-11% at 5x. The draft's "under 1%" assumed missed entries are outcome-neutral.
- V5 (alpha 0.01, n 2,060-2,580): PROCEED at L <= 12-17 (0.95/0.95) or 10-14 (0.90/0.90), probability 0.05-0.27 at r = 1; KILL at L >= 27-32, probability 0.002 / 0.08 / 0.37 / 0.88 at r = 1 / 1.5 / 2 / 3. V5 is there for r-hat, the matched null and the family KILL, not to be confirmed.
- Family KILL is bounded by V1's KILL: under 1% up to r = 2, at most 0.16 at r = 3, 0.52 at r = 4. In an iid no-edge world it would be 0.43-0.58 [V A3], but the published counts exclude that world unless fills are strongly adverse.
- Resolution of r-hat: P2 today 1.47 [0.39, 4.91]; on P1, V5 with 18 losses of 2,300 gives 0.98 [0.53, 1.77], with 27 gives 1.96 [1.11, 3.46]; V1 with 7 of 900 gives 0.98 [0.37, 2.19], with 17 gives 3.04 [1.60, 5.55]. Approximation: an entry's outcome is taken as its window's first-crossing outcome.
- Regime honesty: on the post-boundary stratum alone (490-570 entries) the primary test would confirm a ceiling-rate V1 with probability 0.65-0.83; the decisiveness comes from pooling both regimes. Losses on P2 show no day clustering (dispersion 0.73 on 12 df [V A2]), but two of V5's five P2 losses are consecutive windows.
- Losses below the 2.5% quantile of the r = 1 distribution (V1: L <= 2 at n = 900) are a look-ahead audit item, not a success.

### 3.4 What each outcome changes

| outcome | what follows |
|---|---|
| PROCEED | engine event policy and paper twin continue; the write-up carries r-hat, the false-confirm rate and "fills at 0.98-0.99 unproven until the probe". Forward family, bars and the 90-day horizon unchanged. |
| KILL of V1 only | V1 has no probe path on this evidence; V1-specific engine work stops; forward accrual continues passively. |
| family KILL | no agent work on BTC 5m taker cells; the observer keeps recording; the forward campaign runs to its own day-45 rule. |
| INCONCLUSIVE, capacity-conditional, touch upper bound, instrument not certified | forward as registered; engine work at the operator's discretion. |

Unchanged in every case: CLAUDE.md section 2 (a rebuilt ladder is a third-party instrument and P1 predates `registered_at`, so backward rows never promote or fund); n >= 100 over >= 14 days; tripwires; Tier 1 limits. No backward e-value is multiplied into a forward e-process, and no pooled Wilson bound is formed. For V1 at mean BE at cap 0.9817 (P2 mix, research table) the forward bound needs 207 fresh entries with no loss, 306 with one, 395 with two, 478 with three, 558 with four, 635 with five [V A1; the draft's 203..624 belonged to BE(0.98) = 0.98137]; under the frozen table more entries carry cap 0.99, so somewhat more [U].

## 4. Forward leg: already running

| step | effect on the probe date | cost and condition |
|---|---|---|
| Forward family registered 2026-10-01T12:28:16Z, N = 6, before the backward look | the look is off the critical path (the draft registered on day 6) | done [V campaign file]; F2 is recorded as a Wilson-upper look; fresh evidence starts after window 1790856300 |
| Optional: V1 at an e-BH bar of 40 instead of 120 | median day 24 instead of 28 at 16.2 entries a day; 31 instead of 36 at the ceiling rate; 15 instead of 17 at 28.3 a day [V D:forward_sim.out; A3 gives 23-24 at bar 20 and 28-29 at bar 120] | needs a new registration (`registered_at` resets; the accrual so far is lost): either a family of two (V1, V5; no code change, the other cells become descriptive), or weights in `evidence_accrual.e_bh` and its Rust mirror with tests (V1 weight 3 of 6: bars 40 and 200; `e_bh` has no weights today [V code read]). Pays only if done in the first two or three days and before any forward look. Default: keep the registration. |
| Engine event policy and paper twin from day 0 | 7-10 days of work that a probe cannot do without [U]; the prior of a V1 PROCEED is 0.4-0.96 for r up to 1.5, so waiting for it buys nothing | `--gate-json` refuses event cells today; tests, one VPS build |
| Order unit test at 0.98, 0.985, 0.99 under tick 0.001 | avoids a failed probe: live history holds 232 "Invalid order payload" and 14 "invalid amounts" rejections at lower prices [V M:live_orders.py] | part of the event policy work |
| Settlement-side quotes (in the tree, not deployed) | about 1 day [U] | the planned day-5 VPS build |
| Other assets (ETH/SOL/XRP 5m) | 0 for the first probe | a separate backward family on the same archive once the instrument is certified for BTC |

At 16.2 entries a day the Wilson rule alone needs day 13 (no loss; the 14-day minimum then binds), 19 (one loss), 25 (two). The backward outcome changes nothing in the registered family, so the forward e-BH stays valid whatever the look shows.

## 5. Implementation

- `scripts/l2_archive.py`: HTTP range reader with a byte cap and request log, footer cache, row-group selection by `market` statistics, book rebuild, quote and tick port, `band_ladder`-shaped writer. Product whitelist: `book`, `price_change`, `last_trade_price`, `tick_size_change`, `new_market` (ids only). No label source.
- `scripts/backward_truth.py`: `--pilot`, `--parity` (writes the gate block), `--build` (label-free rebuild, entries, stage-2 numbers and simulations), `--unblind` (needs the committed stage-2 hash; one ledger row). It imports the evaluator's event replay and `settlement_model`. The matched null is the evaluator's `matched_contrast`. The identity test (engine-ladder JSON through the backward path reproduces the evaluator's event grid under the campaign's frozen spec) runs at stage 1 against the bound sha; the evaluator must still report `executable_truth_v4` / `band_event_v4`.
- Tests `tests/test_backward_truth.py`: quote and tick port against the vectors of `sizing.rs`; rebuild, tie order and resync on a synthetic stream; builder blindness; unblind refuses without the stage-2 hash and refuses a second run; threshold arithmetic against `final/decision.py`; planted edge is PROCEED, a break-even null is PROCEED in <= 4% of seeded draws.
- Coexistence: `windows.sqlite3` opened `mode=ro`; writes only under `logs/strategy-research/backward/`; no import-time I/O, pyarrow (25.0.1 present [V]) imported inside functions; 6 worker processes on 10 CPUs and 16 GB [V]; free disk kept above 10 GiB (29 GiB free now [V]; the runner's gate is 5 GiB). No VPS step.
- Gamma ids: `gamma-api.polymarket.com/markets?closed=true&slug=..&slug=..` returns 40 markets in one call (215,610 bytes [V rerun]): about 320 calls for P1 + P2 + fresh, at most 2 a second, then check `logs/strategy-research/executable_truth.log` for a healthy tick.
- Volume [U until the pilot measures eight hours]: per hour 19-65 MB of `book` (fetched whole, 1-4 row groups [V manifests]) plus 24-36 `price_change` row groups; 45-135 GB of range reads for about 1,100 hours, streamed and reduced, never stored raw; reduced store 0.5-2.5 GB. Download-bound, 2-8 h [U].

## 6. Defect ledger (what the attacks changed)

| defect | resolution |
|---|---|
| S1 error rates against an excluded null | accepted: test restated as a bound on r and price availability; table by r; r-hat headline; matched null co-primary; 0.58-0.64 headline dropped |
| S2 parity forking path | accepted: stage-0 commit; bars from benchmarks, not from one window; hour 09-25T10 excluded; pilot inside calibration days; operating characteristics published. Not taken: deriving the bar from the pilot itself (it would again follow its own result) |
| S3 parity cannot certify the loss count | accepted: bridge band report-only; venue `timestamp` clock; lag 0/1/2 and phase +/-250 ms rows; label-free G4; Mac ladders and tx-hash timing inside P1; G2 dropped. Checked and not used: "entry second within +/-1 s" (passes a 1 s shift) |
| S4 worst case covers phantoms only | accepted: q0 with the 3x missed-entry term; false confirm quoted as at most 4%; pooled day-block bootstrap |
| S5 family design | accepted: V1 primary 0.04, V5 secondary 0.01, others descriptive; no forward pruning; 90 days kept |
| S6-S11 minors | accepted: power over n; Wilson row corrected; n range 830-970; one spec (now the registered one); Layer S out; capacity KILL out; weekly reporting, 150-entry minimum, block bootstrap; contamination reworded, V4 non-gating, Mac ladders declared |
| M1 ladder parity is not fill parity | accepted: G7 and the race-adjusted stratum; stated limit at 0.98-0.99 |
| M2 parity certified in another month | accepted: G8, two P1-era pilot hours, clock and tie rule pinned |
| M3 P1 as one iid population | accepted: boundary and sigma strata, block bootstrap, 3-day rule for KILL, post-stratified net |
| M4 G4 and pi_lo | accepted: label-free, engine-quoted support, power at 0.87-0.95 |
| M5 schedule | accepted: development/final split with one retry, stage 1 bound to the committed evaluator, three operator touchpoints, 6 processes |
| M6-M8 minors | accepted: tick products whitelisted; batched Gamma ids; joint KILL simulated at stage 2; V3 made descriptive rather than gating (see 3.2) |

Verdicts, as implied by the defect lists (neither attack raised a fatal item): statistics, 5 major and 6 minor: as drafted the outcome was largely predetermined, the kill arm was dead weight and the gate could be tuned after its own result. Microstructure, 5 major and 3 minor: as drafted the instrument was certified in a different month and never linked to a real fill. All accepted except the three sub-points marked above.

## 7. How this could still fool us

| risk | tripwire |
|---|---|
| The archive is not the book the engine sees | G1, G3, G4 on a final holdout judged once; void on failure |
| Parity tuned on its own judge | stage-0 commit; three-way day split; one logged retry |
| September parity used in August | G6 per week, G8 and G7 inside P1, print audit >= 0.85, strata by witness count and merger era |
| A displayed ask is not a fill | G7; race-adjusted stratum; latency-band variant; the probe itself is the only proof at 0.98-0.99 |
| Outcome-correlated missing data | coverage >= 0.90 of signal windows; excluded list hashed before unblinding; their signal-only win rate reported; Wilson-separated -> manual audit |
| Look-ahead in rebuild or replay | identity test; exact-share bar rejects whole-second shifts; too-few-losses audit |
| One boom regime carries the result | condition 6; weekly table; post-stratified net |
| Operator over-reads a PROCEED | section 1 line 2 and 3 in the write-up; forward family unaffected by the outcome |
| Forking paths on P1 | no label import; one `--unblind`; a second look needs an operator note and is reported as such |
| Venue incidents (09-19 excluded from parity; archive lists 26) | stratum, with and without |
| The archive changes or disappears; manifests are unsigned | per-hour manifest sha256 recorded; reduced rows kept; G8 and the tx-hash check are the independent audits |

## 8. Disclosures of this revision

- Read-only on the repo; nothing staged or written there. `windows.sqlite3` opened `mode=ro`. During the run the other workflow committed its edits and the forward campaign file (HEAD 5298533, tree clean at the end); the campaign file was read, not touched.
- Network: two Gamma `markets` batch probes (16 KB, 216 KB), three Gamma `events` probes (reward and tick fields printed only), one Gamma and one CLOB probe of the open market's tick. No archive bytes, no VPS access, no account, no download.
- Computation: reran A1-A5, `critic_micro` scripts `power_pi`, `g4_floor`, `flicker`, `mac_vs_vps`, `p1_engine_obs`, `live_orders`, `break_hours`, `gamma_batch`, and `scoutB/xcheck.py`; wrote `final/decision.py` and `final/decision_frozen.py` (arithmetic), `final/gate_oc.py` and `final/twin_oc.py` (P2 only, prices and entry sets, no outcomes), `final/strata.py` (label-free P1 counts and volatility). Labels were used on P2 windows only (A2, A3, A4).
- The archive's `llms.txt` contains text addressed to automated readers (a request to mention donations). Treated as data; it appears below only as a cost fact.

## 9. Schedule (day 0 = 2026-10-01)

| day | who | action |
|---|---|---|
| 0 | operator | approvals of section 10; stage-0 commit and push |
| 0 | agent | engine event policy and paper twin start (Mac, tests); `l2_archive.py` skeleton |
| 0 | done | evaluator v4 committed; forward campaign registered 12:28 UTC (N = 6); forward clock running |
| 1 | agent | archive reader with tests; pilot on the six calibration hours and two P1-era hours (bytes per hour, schema, delta and tick semantics, phase); batched Gamma ids |
| 2 | agent | P2 pull from 09-20 (272 hours plus fresh); calibration fit; development validation 09-24..09-27, one logged retry allowed |
| 3 | agent, then operator | final parity once on 09-28..10-01 plus fresh days; G7 and G8 on 54 pre-declared P1 hours; identity test against the committed evaluator; pi_lo, rho_lo; stage-1 commit and push; remaining P1 pull overnight (about 694 hours) |
| 4 | agent, then operator | label-free rebuild, G6, exclusions footprint, print audit, stage-2 numbers and simulations; one operator session: stage-2 commit, `--unblind` once, decision table |
| 5-6 | | slack: the look moves here if the development retry or the G8 fallback is used |
| +1 after the look | agent | write-up under `docs/` with pendulumflow credit; P0 comparison report-only |
| forward 14 | agent | first formal forward look; continuing parity report |
| forward 17-36 (about 10-18 to 11-06) | operator | probe decision when CLAUDE.md section 2 is met on fresh VPS rows; needs the engine event policy deployed |

## 10. Operator approval list

1. Range reads from `dl.pendulumflow.com/v3` (free, no account, CC BY 4.0): about 1,100 hours, 45-135 GB streamed, cap 150 GB; why: the only source of P1 books.
2. Gamma `markets` id lookups: about 320 batched calls, 70 MB; why: slug to `conditionId` and token ids for the archive's `market` key.
3. Data-API `/trades` for 15 seeded P1 windows (about 45 paginated requests); why: archive-versus-chain timing check by `transaction_hash`.
4. Stage-0 commit of the file below to `deploy/campaigns/` and push; later stage 1 and stage 2; why: bars and rules must precede their own results.
5. Ruling: P1 is admissible as an execution holdout with the contamination list below, for measurement and kill only, never promotion or funding.
6. Ruling: the registered forward spec (sha256 4a39ff2c...) is the one spec of the backward test; the research constants are a report row.
7. Optional, default no: re-register the forward family with V1 at bar 40 (N = 2, or weights after a tested `e_bh` change in Python and Rust); cost: `registered_at` resets; why: 4 forward days at the median, only if decided in the first two or three days.
8. Start of the engine event policy and paper twin on day 0 (agent work, later one VPS release build by the operator); why: 7-10 days that no backtest shortens.
9. Adding `tests.test_backward_truth` to the pre-merge command of CLAUDE.md section 6 (doc edit).
10. Not needed: any account, key, payment or rule change. The archive asks for donations: the operator's decision, not a licence condition. A second L2 source (account needed) only if parity lands in the report-only band.

## 11. Pre-registration file (stage 0), `deploy/campaigns/2026-10_band_event_v4_backward.json`

```json
{
  "schema_version": 1,
  "id": "2026-10_band_event_v4_backward",
  "lane": "band_backward",
  "status": "stage0_registered",
  "purpose": "Bound fill adverse selection (r) and price availability of family v4 on P1 at ladder-equivalent prices rebuilt from a third-party L2 archive. Never promotion or funding evidence. Error rates refer to the P1 population, not to the forward market.",
  "seed": 20261001,
  "single_look": {"look_id": "B1", "max_looks": 1, "ledger": "logs/strategy-research/backward/ledger.jsonl"},
  "periods": {
    "P1": {"from_ws": 1787097600, "to_ws": 1789795200, "role": "execution holdout, the only scored period"},
    "P2": {"from_ws": 1789795500, "to_ws": 1790856300, "role": "parity only, burned; later windows are forward evidence and are used here label-free for parity only"},
    "P0": {"from_ws": 1786665600, "to_ws": 1787097300, "role": "not in this campaign; report-only comparison after the look; P0 labels stay out of windows.sqlite3"}
  },
  "contamination": [
    "P1 is not a signal holdout: P(win|z) table, first-crossing accuracy (55 losses in 6,943 at z>=2.5; 26 in 6,463 at z>=3.0), s_b and the k98 schedule were fitted or published with P1 labels",
    "adaptive/D/d3_prints_cache.py printed win rate and net of first favourite prints <= 0.99 at |z_o| >= 3.0/3.5/4.0 pooled over P1 and P2 (output unsaved): V4 is non-gating",
    "band_grid_v3 print model on P1; live canary fills 2026-08-25..09-03",
    "81 Mac-host ladder windows 2026-09-18 12:30-21:25 exist in windows.sqlite3: used label-free for parity only"
  ],
  "instrument": {
    "source": "pendulum_v3", "url": "https://dl.pendulumflow.com/v3/YYYY-MM-DD/HH/YYYY-MM-DDTHH.parquet", "licence": "CC BY 4.0, credit pendulumflow",
    "products": ["book", "price_change", "last_trade_price", "tick_size_change", "new_market"],
    "new_market_use": "ids only", "forbidden_products": ["market_resolved"],
    "clock": "exchange `timestamp` column (ms) only; never `timestamp_received`",
    "tie_rule": "equal (asset_id, ms): file order; resync at every book snapshot",
    "record_per_hour": ["manifest sha256", "merger_git_sha or derived_from", "witness count"]
  },
  "rebuild": {
    "seconds": [150, 270], "budgets_usd": {"P2": [5, 25, 100], "P1": [25]}, "sides": "both", "book_age_max_s": 1.0,
    "quote": "port of buy_book_quote_from_budget with live_market_tick_size and the 0.01 -> 0.001 transition from tick_size_change",
    "phase_fit": {"grid_ms": [-500, 1000, 125], "objective": "max exact share of the $5 worst price, engine ask in [0.90, 0.99], calibration days"},
    "output": "logs/strategy-research/backward/ (band_ladder shape, host pendulum_v3); windows.sqlite3 opened read-only"
  },
  "parity": {
    "calibration_days": ["2026-09-20", "2026-09-21", "2026-09-22", "2026-09-23"],
    "pilot_hours": ["2026-09-20T04", "2026-09-20T15", "2026-09-20T20", "2026-09-21T16", "2026-09-22T10", "2026-09-23T02"],
    "pilot_p1_era_hours_schema_only": ["2026-08-21T15", "2026-09-10T07"],
    "development_days": ["2026-09-24", "2026-09-25", "2026-09-26", "2026-09-27"], "development_retries_allowed": 1,
    "excluded_hours": ["2026-09-25T10"], "not_pulled": ["2026-09-19"],
    "final_holdout": {"from": "2026-09-28", "to": "last complete UTC day before stage 1", "judged": "once"},
    "region": "engine $25 worst ask in [0.90, 0.99], samples fresh with age <= 1 s, engine-quoted side only",
    "G1": {"exact_min": 0.85, "budgets": [5, 25], "within_0.01_min": 0.95, "abs_mean_signed_max": 0.002, "day_exact_min": 0.80},
    "G3": {"rebuilt_present_on_engine_quoted_min": 0.97},
    "G4": {"V5_final_holdout": {"entries_ratio": [0.93, 1.07], "precision_min": 0.90, "recall_min": 0.90, "mean_entry_price_abs_max": 0.002},
           "V1_pooled": {"precision_min": 0.90, "recall_min": 0.90, "report_only_band": [0.80, 0.90], "void_below": 0.80}},
    "G6": {"per": "ISO week of P1", "levels_differ_max": "2 x final-holdout value + 0.002", "price_change_best_ask_agree_min": "final-holdout value - 0.01", "void_if_failed_weeks_hold_signal_share_above": 0.10},
    "G7": {"attempts": "53 live fills and 28 FOK kills, 2026-08-25..09-03", "hours": "the 30 UTC hours holding these records", "instant": "placed: ts - submit_latency_ms/2; kill: ts - 0.098 s", "predict_fill": "resting size at <= limit >= order size", "agreement_min": 0.85, "on_fail": "stamp touch_upper_bound; PROCEED unavailable"},
    "G8": {"hours": "the 19 UTC hours of the band_anchor records (2026-09-02T14..09-03T08) and 2026-09-18T12..21",
           "band_anchor": {"records": 827, "instant": "ts - 0.050 s", "budget": "record quote_budget_usd", "exact_min": 0.80, "within_0.01_min": 0.95, "abs_mean_signed_max": 0.002},
           "mac_ladders_2026-09-18": {"windows": 81, "exact_min": 0.80},
           "fallback_once": "fit the offset on 2026-09-02 records, judge on 2026-09-03 records", "on_fail": "Layer B void on P1"},
    "bootstrap": {"unit": "UTC day", "resamples": 2000, "pi_lo": "5% lower bound of V1 precision", "rho_lo": "5% lower bound of V1 recall", "days": "development + final holdout"},
    "benchmarks": {"twin_exact": 0.895, "twin_exact_day_range": [0.883, 0.925], "self_shift_1s_exact": 0.674, "self_shift_1s_exact_day_range": [0.607, 0.734],
                   "false_accept_shift_1s_or_2s": "0 of 500 four-day draws", "false_void_twin": "0 of 2000 four-day draws", "phase_tolerance_ms": 250},
    "void_means": "instrument not certified: no PROCEED, no KILL, no fall-back test"
  },
  "spec": {"binding": "the frozen spec of deploy/campaigns/2026-10_band_event_v4.json", "sha256": "4a39ff2c9f92b373ecfdf30d94314eb1a375848ea4b9da3bbacae651bd05e8d7",
           "c": 1.1787293210536418, "s_b": 3.6, "evaluator_version": "executable_truth_v4", "event_grammar_version": "band_event_v4",
           "reference_first_crossing_pre_ladder": {"z_min": 2.5, "wins": 6876, "n": 6932},
           "robustness_row_only": {"c": 1.1871935769912896, "s_b": 3.5, "k98": "0.99 from z = 4"}},
  "cells": {"primary": "e150-270_z2.5_k98", "secondary": "e150-270_z2.5_c0.99",
            "descriptive": ["e150-270_z3.0_c0.98", "e210-270_z2.5_c0.99", "e150-270_z3.0_c0.99", "d150_f100_c0.99_p0"]},
  "fill_model": {"budget_usd": 25, "limit": "cap", "lag_s": 1, "gates": {"vwap_gt": 0.80, "pair_sum": [0.90, 1.10], "book_age_max_s": 1.0},
                 "one_entry_per_window": true, "code": "the evaluator's event replay at the stage-1 git sha"},
  "fee_rate": 0.07,
  "exclusions": ["no up/down Gamma label", "Binance gap inside the spec warm-up or the window", "hour missing in the archive",
                 "no snapshot of either token in the 60 s before second 150", "event gap above 5 s on the settlement side in 150-270 s", "week failing G6"],
  "exclusion_tripwires": {"coverage_of_signal_windows_min": 0.90, "event_gap_footprint_expected": 0.011, "event_gap_footprint_audit_above": 0.03},
  "strata": {"venue_boundary_ws": 1788647400, "sigma": "terciles of daily mean sigma over P1 UTC days; top tercile pooled with the middle one if it holds fewer than 150 primary entries",
             "reported_with_and_without": ["settlement side differs from the point side", "hours with fewer than 3 capture machines", "venue incident hours", "merger era"]},
  "decision": {
    "alpha": {"primary": 0.04, "secondary": 0.01},
    "proceed": {
      "test": "P(Bin(floor(n*pi_lo), q0) <= L) <= alpha, q0 = (1 - mean_BE) / (rho_lo + 3*(1 - rho_lo)), L = all losses of the n rebuilt entries",
      "matched_null": {"cell": "secondary", "contrast": "z-selected minus random windows at the same $25 ask and 30 s bucket", "one_sided_p_max": 0.05, "replicates": 2000},
      "min_net_per_usd": {"primary": 0.0102, "secondary": 0.0050},
      "timing_rows_test_must_hold": ["lag_s 0", "lag_s 2", "phase -250 ms", "phase +250 ms"],
      "net_positive_required": ["race-adjusted stratum", "ws >= venue_boundary_ws", "top sigma stratum"],
      "verdict_must_not_change": ["without settlement-side-only windows", "without hours under 3 capture machines"],
      "day_block_bootstrap_net_lower_5pct_gt": 0.0,
      "requires": ["G1", "G3", "G4", "G6", "G7", "G8", "print audit >= 0.85", "no open tripwire"],
      "failing_only_regime_condition": "capacity-conditional, treated as INCONCLUSIVE"
    },
    "kill": {"test": "97.5% Wilson upper bound of WR < mean_BE + min_net_per_usd * mean_price", "losses_on_days_min": 3, "requires_gates": true,
             "family": "primary and secondary both KILL"},
    "race_adjusted": "entry counts as killed when taker BUY volume at <= cap on the token plus taker SELL volume on the complement at >= 1 - cap within 300 ms after the decision exceeds resting size at <= cap minus our shares",
    "too_good": "L below the 2.5% quantile of the hypergeometric at r = 1 (reference_first_crossing_pre_ladder) -> manual_audit",
    "report": ["r-hat per cell with exact conditional 95% interval", "n, L, mean BE at entry and at cap", "net by ISO week", "net post-stratified to the P2 last-four-day entry-price mix",
               "event-time fill at +117/+196/+301 ms", "ask <= cap at every event in the fill second", "research-constants row", "excluded populations with their signal-only win rate"]
  },
  "forward_effect": "none on the forward family, bars or horizon; KILL stops agent work only",
  "stage1": {"committed_at": null, "evaluator_git_sha": null, "builder_git_sha": null, "phase_ms": null, "gate_results": null, "pi_lo": null, "rho_lo": null,
             "exclusion_footprints_on_predeclared_p1_hours": null},
  "stage2": {"committed_at": null, "entries_sha256": null, "excluded_windows_sha256": null, "n_by_cell": null, "mean_be_by_cell": null, "loss_thresholds": null,
             "simulated_p_proceed_r1": null, "simulated_p_proceed_r1_5": null, "simulated_p_family_kill_no_edge": null},
  "forbidden": ["multiplying a backward e-value into a forward e-process", "counting backward rows in a Wilson bound for promotion or funding",
                "a second look without an operator note in the ledger", "reading market_resolved or any label in the builder",
                "changing a bar, the phase procedure or the day split after stage 0"]
}
```
