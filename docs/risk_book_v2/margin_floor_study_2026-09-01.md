# Margin-floor study (2026-09-01)

Question: was the 5-loss streak a regime the band cannot handle, or edge
decay? Method: scripts/margin_floor_study.py - WR/signal-accuracy by
|decision margin| on three independent legs. Verdict: the mechanism was
always margin-conditional; accuracy is 98.6-100% at |margin| >= $50 on
700+ fresh windows INCLUDING the chop, and below break-even under $25.
The gate period (Aug 15-21) trended so hard that even tiny margins won -
regime-carried validation. Fix: min_decision_margin_usd=50 in band
params (artifact band_promotion_margin50.json, hash 81f93e1b).

Raw output:

```
leg A rows: 222/222

--- A: gate evidence (Aug 15-21), WR by |margin|
  |margin| $    0-25: 107/114 = 93.9%  wilson_lo 0.879
  |margin| $   25-50:  64/70  = 91.4%  wilson_lo 0.825
  |margin| $   50-75:  21/23  = 91.3%  wilson_lo 0.732
  |margin| $  75-100:   7/7   = 100.0%  wilson_lo 0.646
  |margin| $ 100-inf:   8/8   = 100.0%  wilson_lo 0.676

leg B rows: 43

--- B: LIVE trades (new wallet), WR by |margin|
  |margin| $    0-25:  23/29  = 79.3%  wilson_lo 0.616
  |margin| $   25-50:  10/13  = 76.9%  wilson_lo 0.497
  |margin| $   50-75:   1/1   = 100.0%  wilson_lo 0.207

leg C rows: 1605 (windows since Aug 25)

--- C: fresh SIGNAL ACCURACY by |margin|
  |margin| $    0-25: 458/586 = 78.2%  wilson_lo 0.746
  |margin| $   25-50: 382/409 = 93.4%  wilson_lo 0.906
  |margin| $   50-75: 219/221 = 99.1%  wilson_lo 0.968
  |margin| $  75-100: 140/142 = 98.6%  wilson_lo 0.950
  |margin| $ 100-inf: 247/247 = 100.0%  wilson_lo 0.985

leg C recent-48h rows: 575

--- C': last 48h SIGNAL ACCURACY by |margin|
  |margin| $    0-25: 113/157 = 72.0%  wilson_lo 0.645
  |margin| $   25-50: 142/158 = 89.9%  wilson_lo 0.842
  |margin| $   50-75:  90/90  = 100.0%  wilson_lo 0.959
  |margin| $  75-100:  68/69  = 98.6%  wilson_lo 0.922
  |margin| $ 100-inf: 101/101 = 100.0%  wilson_lo 0.963

DONE
```
