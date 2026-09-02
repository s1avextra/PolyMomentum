# Margin-latch study (2026-09-03)

Question: why did the band, with `min_decision_margin_usd=50` live, buy a
window whose decision-second margin was -15? Verdict: the mechanism was
validated as a POINT rule (one sample per window at `open + 240 s`), but the
live loop re-read the margin every ~100 ms across the whole entry window
[240, 270) and fired on the FIRST crossing of the floor. That is a
different rule with a different, worse accuracy. Fix: latch the decision on
the decision cycle, ahead of every cycle gate (`BandLatch` in
rust_engine/src/live/pipeline.rs), so the live rule equals the validated
rule up to the residual sampling gap stated below.

## Incident (2026-09-02 18:14 UTC, cid 0x83fe5b75e3a342)

Live rule: decision_seconds 240, entry_window_seconds 30,
min_decision_margin_usd 50, band (0.55, 0.92].

| elapsed | margin (btc - open, USD) | engine |
|---|---|---|
| 239 s | -15 | skipped, `band_margin_below_floor` |
| 247 s | -50.24 (composite-feed flash dip) | re-evaluated, bought DOWN at 0.91 |
| ~267 s | reverted within 20 s | position held (hold-to-expiry) |
| 270 s | -9 | entry window closed |
| resolution | UP | -$5.02 |

The window was correctly closed at the decision second. A transient dip in
the exchange-mid composite seven seconds later re-opened it, and the entry
landed at the top of the band because the dip itself had pushed the DOWN
ask to 0.91. (The 239 s in the log is wall clock minus window open; the
engine's integer-second elapsed reads 240 at that instant - see the
residual gap below.)

## Point rule vs first-crossing rule

Public Binance BTCUSDT 1s closes + Gamma official resolutions, both from
the `scripts/margin_floor_study.py` caches
(`logs/strategy-research/margin_study_cache/`), windows opening
2026-08-19 00:00 -> 2026-09-02 19:00 UTC: 4260 windows, 1 without a cached
resolution, floor $50. Reproduce with

    uv run python scripts/margin_latch_study.py --start 2026-08-19 --end 2026-09-02T19:00

Point rule: sign(close(ws+240) - open), traded iff |margin| >= 50.
First-crossing rule: same sign test on each second in [240, 270), traded at
the first second where |margin| >= 50 with that second's sign.

| rule | windows traded | correct | accuracy | Wilson lower |
|---|---|---|---|---|
| point (validated; `margin_floor_study.py` leg C is the same sample) | 1944 | 1914 | 98.46% | 0.978 |
| first-crossing (what the loop ran) | 2376 | 2322 | 97.73% | 0.970 |
| admitted ONLY by first-crossing | 432 | 408 | 94.44% | 0.919 |
| traded ONLY by point | 0 | - | - | - |

(The first draft of this note quoted 1941/1911, 2383/2328 and 442/417 from
a one-off run whose cache state and range were not recorded; the table
above is the script's output on the cache as committed and supersedes it.)

The 432 extra windows have a median |margin| of $36 at 240 s, all 432 below
the floor: they are the $25-50 bucket the floor study measured at
89.9-93.4% (the bucket the floor exists to exclude), and they enter later
and higher. Fee-aware break-even win rate is entry + taker_fee(entry): 0.859
at 0.85, 0.907 at 0.90, 0.926 at 0.92. The extra windows' point estimate
clears 0.92 entries by under two points and their Wilson lower bound
(0.919) does not clear it at all, whereas the point rule's lower bound
(0.978) clears break-even at every price in the band.

## Why first-crossing is a multiple-looks bias

The floor is a threshold on a noisy statistic. One look at 240 s asks "is
|margin| >= 50 now"; 300 looks across [240, 270) ask "does |margin| touch 50
at ANY point in the next 30 s". The second event is strictly more frequent
and is enriched for exactly the windows the floor should reject: those
whose 240 s margin is below the floor and whose path only crosses it on a
transient spike. A crossing that occurs on a spike is, by construction,
followed by a reversion (the 247 s dip reverted within 20 s), so the extra
windows are the ones where the momentum sign is least persistent. The
validation evidence (the 700+ window floor study and the gate rows) never
scored this event, so its accuracy was unknown; measured, it is 94.4% on a
$36 median margin, not 98.5% on a >= $50 margin.

The same bias applies with the floor disabled: a floor-0 rule that
re-derives the direction every cycle is "sign at the first cycle whose ask
happens to be in band", not "sign at 240 s".

A reduced form of the same bias survives any latch that is taken on the
first cycle that CLEARS the entry gates rather than on the decision cycle:
if the fresh-book gate, the decision-feed gate, the venue-incident flag or
a zero mid blocks the 240 s cycle, "decide on the first cycle that passes"
is "sign at the first observable second in [240, 270)" - a rule neither
study scored, and one that re-creates the 18:14 loss whenever the block
clears on a spike. The fix below therefore decides ahead of the gates.

## Fix

`rust_engine/src/live/pipeline.rs`:

- `BandLatch` (per cid, `Pipeline.band_latch`, a tokio
  `Mutex<HashMap<String, (BandLatch, i64)>>` carrying the window end, next
  to `band_detail_logged` / `band_anchor_logged`): `NoSignal(reason)` or
  `Signal { direction, open }`.
- `band_latch_decision(band, elapsed_s, open, btc)`: the one look, pure.
  A first look more than `BAND_DECISION_TOLERANCE_S` (2 s) after
  `decision_seconds` latches `NoSignal("band_decision_missed")` - the loop
  was not evaluating across the decision second (decision feed stall,
  restart, the window absent from a contract refresh) and a late look
  samples a second the rule was not validated on. Mid <= 0 latches
  `NoSignal("band_mid_unavailable")`; open unavailable, btc == open, or
  |margin| < floor latch `NoSignal` with the existing skip reason
  (`band_open_price_unavailable` / `band_no_direction` /
  `band_margin_below_floor`); otherwise `Signal` with the direction and the
  open it was measured against. With `min_decision_margin_usd == 0`
  (legacy artifacts) the direction is still fixed at that one look.
- `Pipeline::latch_band_decision`, called by the cycle loop for every
  contract right after `minutes_elapsed` is known and BEFORE the loop's
  `asset_price` and `pick_book_prices` (`fresh_outcome_book_unavailable`)
  gates: on the first cycle with elapsed inside the entry window and no
  latch for the cid it computes btc (exchange mid) and the window open
  exactly as the evaluation used to, latches the decision, and writes the
  no-signal reason as the window's one signal detail record. It never
  consults the books or the venue-incident flag, so a gate that is closed
  on the decision cycle cannot defer the look. Later cycles return at the
  map lookup.
- `evaluate_band_opportunity` only consults the latch: a `NoSignal` window
  (and, fail closed, a window with no latch - the loop never took the look)
  returns `Ok(false)` at the top of every cycle without reading the margin
  (the aggregate `skip` counter counts one skip per no-signal window instead
  of one per cycle); a `Signal` window continues into the unchanged
  venue-incident, mid, sizing, quote, coherence, band, arbiter and execution
  logic with the latched side: the patient entry still waits up to
  entry_window_seconds for an executable in-band ask, on that side only.
  `venue_incident` and `band_mid_unavailable` remain entry gates for
  latched-signal windows, and the other entry reasons
  (`kelly_no_edge_bucket`, `band_no_capital`, `band_wallet_low`,
  `band_quote_unavailable`, `band_pair_incoherent`,
  `band_price_out_of_range`) can each still be recorded once per window.
- The map is pruned to windows still open (end > now) at each new latch,
  never to the contract list: `refresh_contracts` runs concurrently with
  the cycle loop, which iterates a contract snapshot, so a refresh that
  transiently omitted the live market (empty page, `active:false`, a
  liquidity dip under the scanner floor) would otherwise drop a decided
  window's latch and let the in-flight cycle re-decide it. The map is
  bounded by construction (only windows inside their entry window are
  latched).
- A process restart inside an entry window starts with an empty map; the
  first cycle after the restart is either inside the tolerance (decided
  normally) or a missed decision. It never decides late.

Residual gap between the live look and the replays: the loop computes
`minutes_left` with chrono `num_seconds()` (integer truncation), so
`in_entry_window(240)` first becomes true at TRUE elapsed 239.0 s and the
one look lands at ~239.0-239.1 s (plus cycle overrun; cycles are 100 ms) on
the composite exchange mid, and the window closes at true 269.0 s. The
replays sample Binance 1 s klines: `scripts/fresh_gate_public_v1.py` the
open at exactly ws+240, `scripts/band_lane.py` and
`scripts/margin_floor_study.py` the close of [ws+240, ws+241). The 240 s
`band_anchor` uses the same truncation as the latch, so anchor-vs-entry
parity is exact, while replay-vs-live parity carries 1-2 s of price path:
a window with |margin| 48 at 239.05 s and 52 at 240.0 s is no-signal live
and traded by the replays, and vice versa. Those disagreements are
expected, not regressions. Fixing the truncation
(`num_milliseconds() / 60000.0`) is a separate change because it also
moves the anchor timestamps.

Replay semantics the latch restores: the Rust backtester has no band
replay (`BAND_FAMILY` appears only in the artifact/release checks). The
band's replays are `scripts/fresh_gate_public_v1.py` (the promotion gate:
`sign(BTC@240s - BTC@open)` once, then the first public print of the signal
token in [240, 270]) and `scripts/band_lane.py` (`decision_close =
closes[decision_second]` once, entry at the first BUY print after the
decision). Both sample the decision once at decision_seconds; neither
re-evaluates per tick, so no backtester change is needed.

## Verification plan

1. Unit tests (pipeline.rs):
   - `band_latch_decision_is_the_point_rule`: the pure rule - the incident
     samples (-15 no-signal, -50.24 would be DOWN), the missed-decision
     tolerance bound (242.0 decides, 242.01 misses), mid 0, open
     unavailable, no direction, the floor bound, floor 0.
   - `band_latch_taken_at_decision_second_ahead_of_gates` (a paper
     `Pipeline` on a floor-50 artifact in a temp dir, driving the real
     `latch_band_decision` and `evaluate_band_opportunity`): the incident
     margins with the venue-incident flag SET on the decision cycle latch
     `band_margin_below_floor`; the 247 s dip with a fresh in-band DOWN ask
     and the flag cleared does not trade and adds no detail (the session
     log carries exactly one `band_skip_detail` for the window); a zero mid
     on the decision cycle latches `band_mid_unavailable` and the dip cannot
     re-open it; a first look at 247 s latches `band_decision_missed`; and
     the evaluation with no latch is fail closed on a clean signal and an
     in-band ask (it never decides on its own - the mutation that re-decides
     a no-signal window fails this test).
   - `band_latch_signal_keeps_side_and_waits_for_in_band_ask`: latched UP
     at 240 s with the UP ask above the cap waits (`band_price_out_of_range`);
     at 250 s with the mid BELOW the open the side and the latched open are
     unchanged; at 255 s an in-band UP ask fills a paper position on UP.
   - `band_latch_pruned_to_open_windows`: a new latch after a window's end
     prunes it and keeps a still-open one.
   - Not covered by a harness: the ORDER of the latch step relative to the
     `asset_price` / `pick_book_prices` gates inside `scan_loop` (the loop
     is not driven by a test; verified by reading), and the 60 s venue
     status poll.
2. Session log, per btc-updown-5m window inside the entry window:
   - at most one `band_skip_detail` whose reason is one of the signal
     reasons {`band_open_price_unavailable`, `band_no_direction`,
     `band_margin_below_floor`, `band_mid_unavailable` as a latch,
     `band_decision_missed`}, and it is the window's first detail;
   - a window with such a detail has no band entry and no later
     `band_skip_detail` of any reason;
   - a latched-signal window may carry the entry reasons
     (`venue_incident`, `band_mid_unavailable`, `kelly_no_edge_bucket`,
     `band_no_capital`, `band_wallet_low`, `band_quote_unavailable`,
     `band_pair_incoherent`, `band_price_out_of_range`), each at most once;
   - every band entry's direction equals the sign of that window's 240 s
     `band_anchor` margin with |margin| >= 50. An entry whose direction
     disagrees with its 240 s anchor, or whose anchor margin is below the
     floor, is a regression. `band_decision_missed` details should be rare
     and each should coincide with a logged feed stall, restart or refresh
     gap.
3. Economics: `scripts/band_shadow_race.py` scores the champion on the
   240 s `band_anchor` record (direction from the sign of the anchor
   margin, floor applied to that one margin), i.e. point semantics. Until
   now the anchor replay and the live book could disagree on windows like
   this one; after the latch they agree by construction (the anchor and the
   latch read the same cycle's mid and open), so the anchors confirm the
   fix's economics directly: the paired e-process against the live ledger
   should stop accruing champion-vs-live divergence, and the live win rate
   should track the point rule's 98.5% band on |margin| >= 50 windows rather
   than the 97.7% blend.
4. Freshest windows: after a week of latched trading re-run
   `scripts/margin_latch_study.py` on the new range (both rules, so the gap
   itself is re-measured) and `scripts/margin_floor_study.py` (point rule
   by bucket), and compare the live leg's WR by |margin| against leg C
   allowing for the 1-2 s residual gap above; a live leg at or above leg
   C's >= $50 buckets closes the incident.
