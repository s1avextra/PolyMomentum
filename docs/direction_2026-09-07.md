# Direction 2026-09-07: canary halt, live-vs-validation gap, next moves

Synthesis of tracks A/B/C and their adversarial checks (HEAD 156ebfc); where they disagreed, the check wins.

## 1. State of play

1. Band canary DOWN since 2026-09-03 08:21 UTC. `live_cumulative_loss` tripped (ledger -11.16 + session -5.02 = -16.18 vs floor -11.40 = 0.6 x pinned bankroll 19.0, not -12.92); the process exited, systemd restarted it (gap 6) and only the wallet preflight stopped it: pUSD $2.78 < $5.50. Unit `failed`.
2. Wallet 0x1a58 was funded $19.00 on 2026-08-26 (not Sep 1). Bridge: 19.00 - 5.175 (Aug 26 fill logged "FOK purged", filled on-chain, lost) - 10.997 (47 trades, 35W/12L) - 0.047 = 2.78.
3. v1 reported 19.0-21.53 because `BANKROLL_USD=19` is pinned and double restarts reset it to 19 (four times). Since Sep 1 15:34 the wallet went $8.67 -> $2.78; the v2 shadow book tracks it to $0.0001.
4. Realized 74.5% at ~0.80 average price vs a 93% gate and break-even 0.906 at p=0.90: no bankroll size is positive-EV at that rate.
5. Factory: 290 band hypotheses, 4 promote_candidate flags, all computed on a truncated public tape; on the full tape none has e > 1. Anchor race: 207 windows, 44 challengers, all `continue`, max e 3.05 vs threshold 880; race champion 1 trade vs 3 live (overlap 0). Late lane: 3-min family killed at the 0.972 break-even.
6. Nothing is promotable; no shortlisted rule is deployable as-is (no engine sigma floor or direction filter).

## 2. The last bad order

cid 0x901740001a024c. The engine latched once at 239.81 s wall-clock (engine "240"; truncation puts the look at true elapsed 239.0-240.0 s): composite margin -52.80 >= 50 -> Signal down, DOWN ask 0.97. On the validated basis (Binance close(ws+240)-close(ws)) the same second read -47.98: no signal. The ask sat >= 0.94 until 244.2 s, when the REST arbiter refused 0.93 vs the mirror's 0.92; at 246.0 s, composite already -47.56, the FOK bought 5.48 sh @0.91. By 270 s DOWN asked 0.33 and UP 0.70; close +3.91, official UP, oracle agreed; PnL -5.02; the $5 stake was 64% of the $7.80 wallet (4.2x half-Kelly).

Class: neither execution error nor code defect; eae7343 did what it specifies. It is a **validation-coverage failure** in three layers: (i) the deployed price gate ("first in-band ask within 30 s") was never scored: gate, `band_lane` and race each take one look at the decision second; (ii) the margin gate fired on a composite/239.x-second basis where every study used Binance/second 240, exactly at the floor; (iii) a venue-minimum stake on a $7.80 wallet. Caution: the single-look price fix rests on n=1 (trades 5 and 7 took the same patient path and won); the 8-trade ledger supports the basis gap: both losers were Binance-below-floor windows.

## 3. Why live underperforms the validation

| # | Gap | The number |
|---|---|---|
| 1 | Price gate: live waits up to 30 s for an in-band ask; validation takes one look | 97/98 strong-signal anchors priced >= 0.95 at 240 s; 3/98 ever sized in [239,271]; race champion 1 trade/206 windows vs 3 live fills, overlap 0; patient entries on strong signals 2W/1L |
| 2 | Margin basis: composite mid at 239.x s vs Binance close of second 240 | abs diff median 3.1, p90 10.6, max 36.2 (n=204); 8/204 windows >= 50 composite but < 50 Binance; floor decision flipped within +-1 s of ws+240 in 3 of 8 live trades, both losses among them |
| 3 | Cap economics: stage-1 signal accuracy used as entry accuracy | Break-even 0.9063 @0.90, 0.9252 @0.92 (live fee 0.07; `kelly_lo_stake` uses 0.072). Stage-1 50-75 bucket lo 0.9456 -> +2.2%/$ at 0.92. Entry-level lower bounds 0.921 (fresh gate (0.89,0.92] 45/45), 0.886 (stage-2 in-band 70/73), 0.919 (multiple-looks): none clears 0.9252; joint cell (margin 50-75, entry 0.89-0.92) never measured |
| 4 | Sizing on fictional equity (v1 = 19 pinned + last session's P&L, never reconciled) | Stakes 23-30% of v1 but 41-69% of the wallet; half-Kelly on wallet $1.2-3.1 -> 1.7-4.2x overbet; loss floor -11.40 on the fiction vs -5.20 on real equity |
| 5 | Venue $5 minimum vs bankroll | `kelly_lo_stake(0.92)` unclamped $2.07 at 19, $0.85 at 7.8; $5 <= half-Kelly needs $17/$27/$46 at p 0.85/0.90/0.92 (q_lo 0.9413). MC (200 x $5): 95.9%@0.90 -> ruin 1.2% at $19, 0% at >= $50; 90%@0.90 -> negative EV, ruin 61% at $19 |
| 6 | Halt is not a halt | Trip -> `stop.notify_one()` (pipeline.rs:5095) -> exit 0 -> `ExecStop=/bin/kill $MAINPID` fails (MAINPID empty) -> `Restart=on-failure` -> fold+reset clears breaker keys -> trading resumes. Seen Aug 31 18:00, Sep 1 13:21 (resumed unattended after the 5-loss streak), Sep 3 08:21 |
| 7 | Factory tape truncated, outcome-correlated | `/trades?limit=500`, no pagination (`band_lane.py:307`); 157/157 re-fetched windows hit 500; page 1 starts after the decision in 25-50% of accrual windows; excluded "no print" windows win 0.51-0.90 vs 0.95-1.00 scored. Full-tape e: promotes 26/22/21/20 -> 0.28/0.32/0.81/0.45; 291238c3 7.8 -> 0.17 (35/23) |
| 8 | Factory hygiene | Novelty checked only against killed rules (3 of 4 promotes are one H<=1 cluster); no e-BH or pre-registered N (k*=0 at N=44, threshold 220); WR>0.97@n>=50 tripwire unimplemented; `--json` races append ledger rows |

## 4. Options

### (a) Fix the champion

Counterfactuals on the 8 live trades since Sep 1 15:34 (live: 6W/2L, -5.89):

| Lever | Kept / P&L | Effect and cost |
|---|---|---|
| eae7343 as built (composite one-look + patient price) | 6, -1.38 | baseline for the current build |
| Binance close(ws+240) basis, patient price kept | 6, +4.15 | the validated margin instrument; latch from the Binance 1 s close at true elapsed 240.0; n=8, mechanism coherent |
| + single-look price (decision quote must be in band) | 4, +2.20 (composite: 3, +1.68) | closes gap 1 but removes wins 5 and 7; the point-rule champion traded 1/206 race windows, FOK-rejected 3x |
| Cap 0.85 | 1, +1.44 | removes 5 wins; not the lever |
| Cap 0.91 (largest cap whose entry-level lo 0.921 clears break-even) | 7, -0.87 | cheap, defensible, second-order |
| Margin-dependent cap on stage-1 lower bounds | raises caps (50-75 -> 0.94) | wrong instrument; do not |

Basis parity, the truncation fix, cap 0.91 and fee 0.07 are cheap code changes. Score the patient entry before keeping or dropping it: "first in-band BUY print in (240,270]" on the full paginated tape (same fix as (b)), plus VPS L2 fill realism.

### (b) Promote a factory candidate

Not yet. Best on paper: 9c298eb2 ($25/0.5σ/210 s/both/(0.6,0.85]; stage-2 147/141; anchors 5/5, +$7.26) and 291238c3 ($25/0.5σ/180 s/both/(0.55,0.85]; anchors 21/20, +$21.89, paired e 3.05). Missing, in order: (1) paginated print cache (22,140 files), stage-2 re-run, accrual reset: on the full tape their e is 0.32 and 0.17; (2) e >= 20, then e-BH at a pre-registered N (k=4 at N=44 needs 220); (3) n >= 60 over >= 3 UTC days; (4) >= 100 paired race windows with trades: ~4 and ~15 days of anchors, which only the running engine produces; (5) a race champion with fill parity (now 0/3); (6) engine sigma floor and direction filter, or a $-only re-registration. Through the engine's 30 s entry window, a 210 s rule would trade a population its evidence does not cover.

### (c) Bankroll and accounting

| Threshold | USD |
|---|---|
| Preflight (budget x 1.10) | 5.50 |
| $5 <= half-Kelly, q_lo 0.9413, p 0.85 / 0.90 / 0.92 | 17 / 27 / 46 |
| MC ruin -> 0 at 95.9%@0.90 | >= 50 |

Funding before the rule is fixed is pointless: at 74.5% every size loses. v2 (`book_v2.rs`, 285 lines: postings, sums, `kelly_lo_stake`, shadow telemetry) tracks the wallet exactly; the cutover is unbuilt: `RISK_BOOK` env parse, `RiskBook` trait/V1 adapter, sizing and cumulative floor on wallet-anchored equity, deposit detection, drift halt, lots/recon tables, on-chain detection of purged FOKs.

### (d) Keep the canary off until (a)-(c): recommended order

(d) now -> gap 6 -> (c) accounting phase 0 -> (a) cheap parity fixes + full-tape scoring of the patient entry -> (b) tape rebuild in parallel (code-only) -> (c) funding >= $50 -> resume per step 8 -> anchors accumulate for (b). Reason: the deployed rule is unvalidated, sizing reads a fiction, the halt does not hold, and $2.78 cannot fund an order. Cost: the race stalls (4-15 days of anchors). If anchors are wanted earlier, run the engine with orders disabled (bounded, diagnostics on): live decision-second quotes are what backtests cannot supply (CLAUDE.md §5).

## 5. Next steps

1. **Operator, today:** `touch /opt/polymomentum/control/KILL_BAND` (the canary's `KILL_SWITCH_PATH`) so funding cannot re-create the auto-resume loop. Do not fund; leave `state.db` untouched.
2. **Code:** make a breaker trip hold, or exit with a `RestartPreventExitStatus` code; fix `ExecStop`; verify against the Sep 1 13:21 journal.
3. **Code:** paginate `/trades` in `band_lane.fetch_data_api_trades` (offset loop until the page's oldest trade precedes ws + decision second); rebuild `band_lane_cache/prints`; re-run band stage-2; reset band `evidence_accrual`; demote the four promote rows; novelty check against survivors; e-BH with pre-registered N; the tripwire; stop `--json` races appending ledger rows.
4. **Code:** score the live rule as built (composite 239.x latch, first in-band print <= 0.92 within 30 s) on the rebuilt tape; decide the patient entry on that evidence.
5. **Code (engine):** Binance close(ws+240) margin basis at true elapsed 240.0; cap 0.91; fee 0.07 in `kelly_lo_stake`; re-run `fresh_gate_public_v1` on the newest windows (CLAUDE.md §6).
6. **Code (engine):** v2 phase 0: sizing and cumulative floor on wallet-anchored equity, deposit adjustment, drift halt, on-chain fill detection for purged FOKs.
7. **Operator:** post-mortem into `docs/` + memory: the untracked Aug 26 fill, pinned-bankroll distortion and four resets, the unattended resumes, 74.5% realized vs 93% gate, both -5.02 losses, the truncated tape.
8. **Operator, resume** (only after 2, 5, 6 are merged and a rule's entry-level Wilson-lower clears break-even at its cap): (i) fund 0x1a58 >= $50 (hard floor $5.50); (ii) `/etc/polymomentum/band-canary.env`: `BANKROLL_USD` and `MAX_TOTAL_EXPOSURE_USD` = on-chain pUSD; (iii) unit stopped, `sqlite3 state.db`: `meta.live_cumulative_realized_pnl` = 0 (fresh allocation after the post-mortem, the bail message's own path), delete the four `candle_breaker_*` keys with it (the -5.02 is already in the balance; folding it again double-counts), `state.total_pnl` = 0.0; (iv) `book_v2.db`: post an `adjustment` for the deposit on `band` and the sim; (v) `rm KILL_BAND; systemctl reset-failed polymomentum-band-canary; systemctl start polymomentum-band-canary`; (vi) verify: preflight ok, `pinned=<new>`, no `live start blocked`, first `reconcile` with `v1_equity == wallet` and `v2_minus_wallet ~ 0`, `/status` shows the ledger and `stop at`. Never lower `CANDLE_LIVE_MAX_CUMULATIVE_LOSS_PCT`, set `BANKROLL_USD` above the balance, or clear breaker keys without the ledger post-mortem.
9. **Operator/code, after resume:** re-race with a fill model that reproduces live fills; no promotion before (b)'s six items.

## 6. Independent verification (2026-09-07, main session)

- Gap 6 confirmed from the unit and the journal: `trip_breaker` is followed by `stop.notify_one()` (pipeline.rs ~5095), the unit carries `Restart=on-failure`, `RestartSec=10`, `ExecStop=/bin/kill -SIGTERM $MAINPID`, `RestartPreventExitStatus=2`; on 2026-08-31 17:59 and 2026-09-01 13:21 the trip was followed within 15 s by `Started ... resetting breaker session after bankroll actualization ... candle.start` — trading resumed unattended after the 5-loss streak. On 2026-09-03 only the wallet preflight (exit 2) stopped the loop.
- Gap 7 confirmed on 8 fresh windows (2026-09-06/07): every `/trades?limit=500` page hit the cap; the oldest trade sat 4-241 s after the open; 1/8 pages miss the 240 s decision, 5/8 miss 180 s. Stage-2 evidence for 180/210 s rules is therefore systematically truncated in busy windows; the 240 s champion is mostly covered.
- `KILL_SWITCH_PATH=/opt/polymomentum/control/KILL_BAND` does not exist (only a stale `KILL`). NOT yet created: the operator must run `ssh vps 'touch /opt/polymomentum/control/KILL_BAND && chown polymomentum:polymomentum /opt/polymomentum/control/KILL_BAND'` before any funding, otherwise the unit restarts into trading on the unfixed rule.
