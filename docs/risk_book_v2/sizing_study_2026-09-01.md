# Band-bet sizing study — Monte Carlo, 20k paths x 500 trades, equity $19 start

Script: /private/tmp/claude-501/-Users-ttoomm-Documents-PolyMomentum/0b9ce912-3060-469e-9a4e-9b1bc465c50d/scratchpad/sizing_mc.py
Full output: /private/tmp/claude-501/-Users-ttoomm-Documents-PolyMomentum/0b9ce912-3060-469e-9a4e-9b1bc465c50d/scratchpad/sizing_mc_output.txt
Deterministic (seed 20260901), common random numbers across rules within each world. Payoff per $1 at price p: win +b, b=(1-p)/p-0.072(1-p) (taker_fee=0.072*p*(1-p)/share per scripts/adaptation_persistence_study.py); lose -1. All rules subject to venue min $5, cap $25, money stop cum<=-$11.40 (absorbing), halt if equity<$5.

## 1. Per-bucket edge estimates (222 gate rows; prices 0.560-0.920, median 0.850)

| bucket | n | wins | q_hat | Wilson 95% | avg p | avg b | f*_point | f*_lo | EV/$1 pt | EV/$1 lo |
|---|---|---|---|---|---|---|---|---|---|---|
| <=0.70 | 36 | 24 | 0.667 | (0.503, 0.798) | 0.632 | 0.563 | 0.075 | **-0.379** | +0.042 | **-0.213** |
| 0.70-0.80 | 40 | 40 | 1.000 | (0.912, 1.000) | 0.755 | 0.308 | 1.000 | 0.628 | +0.308 | +0.194 |
| 0.80-0.92 | 146 | 143 | 0.980 | (0.941, 0.993) | 0.874 | 0.137 | 0.830 | 0.514 | +0.114 | +0.071 |

Overall 207/222=0.9324, Wilson (0.8915, 0.9586). Blended with 23 live (21W-2L): 228/245=0.9306, Wilson (0.8917, 0.9562) — live data moves nothing (live-only q=0.913 is inside the gate CI), so sims use the 222 gate rows for the joint (live rows lack entry prices).
Estimation-error honesty: bucket 1 is 40/40 — point Kelly reads that as f*=100% of equity, which is absurd; bucket 0's CI straddles break-even (~0.664 avg) so its true edge may well be negative. Point-q Kelly is not usable raw. Note also per-trade f*(p) goes negative within bucket 0 for p>~0.664 even at point q, so Kelly rules auto-skip ~5% of entries; at Wilson-lower q the entire <=0.70 bucket (16% of entries) sizes to zero.

## 2. Primary results (stop active). med/p5/p95 = terminal equity $; logg = mean log-growth per executed trade (dragged negative by floored paths; loggmed = median path); mdd = max drawdown fraction of peak; forced% = trades where desired stake < $5 was forced up to the venue min (overbet = avg multiple)

WORLD 1 — empirical joint (q=93.2%):
| rule | med | p5 | p95 | logg | loggmed | mdd med/p95 | P(floor) | P(E<5) | forced% (x) |
|---|---|---|---|---|---|---|---|---|---|
| current clamp(25%E,5,25) | 1612 | 1249 | 1920 | .0061 | .0089 | .36/.64 | 2.15% | 0.50% | 1.1 (1.2x) |
| flat $5 | 359 | 290 | 420 | .0034 | .0059 | .22/.51 | 1.77% | 0.40% | 0 |
| Kelly 1.0 [5,25] | 1672 | **4** | 1941 | -.049 | .0095 | .23/.85 | 8.11% | 6.55% | 0.5 (2.8x) |
| Kelly 1/2 | 1671 | 1398 | 1916 | .0065 | .0095 | .22/.56 | 1.43% | 0.22% | 1.3 (3.7x) |
| Kelly 1/4 | 1588 | 1333 | 1822 | .0079 | .0094 | .22/.46 | 0.99% | 0.18% | 4.7 (3.9x) |
| Kelly 1/8 | 1382 | 1148 | 1596 | .0076 | .0091 | .20/.46 | 0.98% | 0.19% | 13.4 (4.4x) |
| **K1/2-of-f*lo** | 1569 | 1404 | 1716 | **.0105** | .0105 | .21/**.37** | **0.03%** | 0.01% | 1.2 (1.5x) |
| K1/4-of-f*lo | 1424 | 1258 | 1572 | .0102 | .0103 | .16/.26 | 0.02% | 0.01% | 7.2 (1.7x) |

WORLD 2 — true q = per-bucket Wilson LOWER bound (sizing unchanged). Note this is very pessimistic: all three buckets simultaneously at their 97.5% lower limits.
| rule | med | p5 | P(floor) | P(E<5) |
|---|---|---|---|---|
| current | 164 | 4 | **46.3%** | 11.8% |
| flat $5 | 121 | 4 | 30.3% | 8.2% |
| Kelly 1.0 | 678 | 0 | 39.7% | 29.2% |
| Kelly 1/2 | 774 | 5 | 25.7% | 6.1% |
| Kelly 1/4 | 753 | 5 | 17.1% | 4.2% |
| Kelly 1/8 | 592 | 5 | 16.7% | 4.4% |
| **K1/2-of-f*lo** | **951** | **569** | **4.1%** | 0.9% |
| K1/4-of-f*lo | 817 | 501 | 2.6% | 0.5% |

WORLD 3 — edge decay, true q = point - 3pp:
| rule | med | p5 | logg | P(floor) |
|---|---|---|---|---|
| current | 1134 | 6 | -.0005 | 9.2% |
| flat $5 | 268 | 7 | -.0021 | 6.8% |
| Kelly 1/2 | 1283 | 7 | -.0035 | 7.7% |
| Kelly 1/4 | 1203 | 805 | +.0040 | 4.0% |
| **K1/2-of-f*lo** | 1207 | **958** | **+.0086** | **0.95%** |
| K1/4-of-f*lo | 1070 | 828 | +.0086 | 0.69% |

Key reading: in the empirical world the current rule, K1/2, K1/4 and K1/2-of-f*lo are all within ~5% on median terminal (~$1.4-1.7k), but their tails differ enormously. The current 25% rule survives only because the world has been kind: 3pp decay drops its p5 to $6 and under Wilson-lower q it hits the floor 46% of the time. Full Kelly on point q is disqualified outright (8% ruin even in the measured world — driven by the 40/40 bucket reading as f*=1). K1/2-of-f*lo gives up ~3% of W1 median vs the current rule and buys: 70x lower floor risk in W1, 11x lower in W2, p5 $958 vs $6 under decay, and the best log-growth of any venue-legal rule in every world.

## 3. Venue-min distortion ($5 min at $19 equity = forced 26% of bankroll minimum)
Same rules with hypothetical $0 min: current rule P(floor) 2.15% -> 0.85% (the min more than doubles floor risk); K1/4 0.99% -> 0.00%; K1/8 W2 16.7% -> 0.01%. Under Wilson-lower q, essentially ALL tail risk of small-fraction rules is manufactured by the venue min: it forces 3.9-4.8x overbets on 10-27% of trades while equity is small. The distortion decays fast as equity grows (forced% ~1% for the recommended rule) — the first ~$20 of compounding is the dangerous zone. K1/2-of-f*lo suffers least (overbet only 1.5x when forced) because its desired stakes are already ~25-31% of equity.

## 4. Rule 5 (drawdown-constrained, P(floor)<=1% under Wilson-lower q): INFEASIBLE as specified
The floor sits 2.28 venue-min losses below start. Frontier under W2 (quarter-Kelly, hard cap C in {5,6,7.5,10,15,20,25}): P(floor) 16.5-17.1% — flat across C, because floor hits happen at low equity where every stake is pinned at $5; the cap only throttles upside (med $168 at C=$5 vs $753 at C=$25). Most conservative venue-legal rule (flat $5): 30.3%. Best venue-legal: K1/4-of-f*lo at 2.6% (skipping the <=0.70 bucket is what buys the reduction — with identical $5 stakes, skipping only p in (0.664,0.70] cuts flat-$5's 30.3% to 16.8%). To actually reach 1% you must either (a) deepen the floor: 1st percentile of min-cum says -$18.40 for flat $5, -$15.28 for K1/2-of-f*lo, -$14.38 for K1/4-of-f*lo under W2 (under the empirical world the rec rule needs only -$6.24), or (b) accept ~2.6-4.1%. Under empirical/decay worlds the recommended rule already beats 1% (0.03%/0.95%).

## 5. Marginal value of raising the $25 cap
World 1, 500 trades: current rule med $1,612 (cap 25) -> $3,065 (cap 50) -> $5,811 (cap 100); K1/2-of-f*lo $1,569 -> $3,007 -> $5,749; logg .0105 -> .0120 -> .0136. Each cap doubling ~doubles median terminal wealth with ZERO change in floor risk (P(floor) invariant to cap in all runs — floor risk lives entirely in the early low-equity phase). Under W2 the rec rule with cap 100 still holds P(floor)=4.1% (med $3,251). One caution: raising the cap under a decayed edge accelerates losses for equity-fraction rules (current rule W2: cap 100 med $35 vs cap 25 med $164), and the empirical joint was measured at ~$5-25 FOK size — asks at $50-100 clips are unmodeled (band_capacity_study territory). Practical: lift the cap to ~min($25 + 0.1*(E-$100), $100) once equity clears ~$100; revisit capacity before going past $100.

## 6. RECOMMENDATION — half-Kelly on the Wilson lower bound, venue-clamped
stake = clamp(0.5 * f_lo(p) * equity, $5, $25), where f_lo(p) = q_lo(bucket) - (1-q_lo(bucket))/b(p), b(p) = (1-p)/p - 0.072(1-p), q_lo = {<=0.70: skip (f_lo<0), 0.70-0.80: 0.9124, 0.80-0.92: 0.9413}; skip the trade when f_lo(p) <= 0.
Concrete schedule at E=$19: p=0.75 -> $6.03; p=0.85 -> $5.58; p=0.90 -> $3.57->$5 (clamped); p=0.92 -> $2.07->$5; p<=0.70 -> no trade. Stakes scale linearly with equity until the $25 cap binds (~E=$160 at p=0.75, ~E=$230 at p=0.92).
Why: dominates the current rule on every risk metric in every world at a ~3% W1 median cost; highest log-growth of all venue-legal rules (.0105/trade W1, positive even under Wilson-lower and 3pp-decay worlds); floor risk 0.03% (W1) / 4.1% (W2) vs current 2.15% / 46%; p95 max-DD 37% vs 64%. Re-fit q_lo per bucket every ~200 resolved trades (gate + live pooled); the rule auto-derates as the CI narrows or the edge decays. Keep the -$11.40 stop as disaster insurance — under this rule it is nearly never touched (deepening it is unnecessary; if you want the literal 1% guarantee under worst-case q, deepen to -$15.30 or drop to K1/4-of-f*lo at 2.6%).
Caveat to check: the sim's "current rule" assumes 25% of compounding total equity, but the two live losses (-$5.13, -$5.45) imply near-$5 stakes at a time when modeled equity was ~$38 — verify what the live sizing actually computes (concurrent-exposure split? base not compounding?). Also: i.i.d. resampling ignores serial correlation/regime clustering of 5m windows (the two live losses came 9.7h apart but window outcomes within a trend regime correlate), so treat all P(floor) figures as lower bounds; and the whole edge rests on 222 rows from one 4.7-day window — Section 1's CIs are the honest width.

## 7. N strategies sharing the bankroll
Kelly is myopic per bet, so with sequential non-overlapping trades each strategy i simply stakes m * f_i_lo(p) * E_shared against the ONE shared equity ledger (m=0.5 global multiplier) — no per-strategy sub-bankrolls, which would forfeit cross-compounding. Two corrections needed: (1) concurrent positions (live data shows entries 8s apart): compute each new stake on free equity E_free = E - sum(open stakes) and cap total concurrent exposure at ~40% of E; (2) correlation — challengers on the same BTC 5m momentum will be highly correlated, and for N simultaneous bets with pairwise correlation rho the joint-Kelly per-bet fraction shrinks by ~1/(1+rho*(N-1)); with rho~0.7, two strategies each get ~0.6x their solo fraction, so fold this into m (m = 0.5/(1+rho_hat*(N-1))) rather than into each f*. The factory's racing challengers should each carry their own per-bucket Wilson-lower q table; the champion/challenger allocation weight multiplies m, not f*.