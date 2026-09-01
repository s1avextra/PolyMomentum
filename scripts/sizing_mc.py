#!/usr/bin/env python
"""Monte Carlo sizing study for the band bet (btc-updown-5m, taker FOK, hold to expiry).

Resamples the empirical joint (entry price, outcome) distribution from the 222
resolved fresh-gate rows. Payoff per $1 staked at price p:
  win: +b where b = (1-p)/p - 0.072*(1-p)   (taker fee 0.072*p*(1-p) per share,
                                             1/p shares per $1)
  lose: -1
Equity starts at $19. Venue min stake $5, cap $25 (applied to all rules).
Money stop: cumulative realized PnL <= -$11.40 halts trading (absorbing).
Equity < $5 also halts (cannot post venue-min order).
"""
import json
import numpy as np

GATE = "/Users/ttoomm/Documents/PolyMomentum/logs/strategy-research/20260821_fresh_gate_public_v1.json"
LIVE = "/private/tmp/claude-501/-Users-ttoomm-Documents-PolyMomentum/0b9ce912-3060-469e-9a4e-9b1bc465c50d/scratchpad/live_trades.json"

E0 = 19.0
VMIN, VCAP = 5.0, 25.0
FLOOR = -11.40
N_PATHS = 20000
N_TRADES = 500
Z = 1.959963984540054

rng_master = np.random.default_rng(20260901)


def taker_fee(p):
    return 0.072 * p * (1.0 - p)


def net_b(p):
    """Net win payoff per $1 staked at price p (fee netted out of winnings)."""
    return (1.0 - p) / p - 0.072 * (1.0 - p)


def wilson(k, n, z=Z):
    ph = k / n
    denom = 1.0 + z * z / n
    center = (ph + z * z / (2 * n)) / denom
    half = z * np.sqrt(ph * (1 - ph) / n + z * z / (4 * n * n)) / denom
    return center - half, center + half


# ---------------- data ----------------
with open(GATE) as f:
    gate = json.load(f)
rows = gate["rows"]
prices = np.array([r["signal_entry"] for r in rows])
won = np.array([bool(r["won"]) for r in rows])
n = len(rows)
assert n == 222

with open(LIVE) as f:
    live = json.load(f)
live_w = sum(1 for t in live if t["won"])
live_n = len(live)

# buckets: 0: <=0.7, 1: (0.7,0.8], 2: (0.8,0.92]
def bucket(p):
    return np.where(p <= 0.7, 0, np.where(p <= 0.8, 1, 2))

bk = bucket(prices)
BNAMES = ["<=0.70", "0.70-0.80", "0.80-0.92"]
q_point = np.zeros(3)
q_lo = np.zeros(3)
q_hi = np.zeros(3)
print("== Per-bucket estimates (222 gate rows) ==")
print(f"{'bucket':>10} {'n':>4} {'wins':>4} {'q_hat':>7} {'wilson_lo':>9} {'wilson_hi':>9} "
      f"{'avg_p':>6} {'avg_b':>6} {'f*_point':>8} {'f*_lo':>7} {'EV/$1_pt':>8} {'EV/$1_lo':>8}")
for i in range(3):
    m = bk == i
    ni, ki = int(m.sum()), int(won[m].sum())
    if ni == 0:
        continue
    qh = ki / ni
    lo, hi = wilson(ki, ni)
    q_point[i], q_lo[i], q_hi[i] = qh, lo, hi
    ap = prices[m].mean()
    ab = net_b(prices[m]).mean()
    fs_pt = qh - (1 - qh) / ab
    fs_lo = lo - (1 - lo) / ab
    ev_pt = qh * ab - (1 - qh)
    ev_lo = lo * ab - (1 - lo)
    print(f"{BNAMES[i]:>10} {ni:>4} {ki:>4} {qh:>7.4f} {lo:>9.4f} {hi:>9.4f} "
          f"{ap:>6.3f} {ab:>6.3f} {fs_pt:>8.3f} {fs_lo:>7.3f} {ev_pt:>8.4f} {ev_lo:>8.4f}")

k_all = int(won.sum())
lo_all, hi_all = wilson(k_all, n)
print(f"\noverall: n={n} wins={k_all} q={k_all/n:.4f} wilson=({lo_all:.4f},{hi_all:.4f})")
blend_k, blend_n = k_all + live_w, n + live_n
blo, bhi = wilson(blend_k, blend_n)
print(f"blended with {live_n} live ({live_w} wins): q={blend_k/blend_n:.4f} "
      f"wilson=({blo:.4f},{bhi:.4f})  [live-only q={live_w/live_n:.4f}]")
print(f"price range: [{prices.min():.3f}, {prices.max():.3f}]  "
      f"mean={prices.mean():.4f} median={np.median(prices):.3f}")

# ---------------- pre-generated randomness (common random numbers) ----------------
idx = rng_master.integers(0, n, size=(N_PATHS, N_TRADES))
U = rng_master.random((N_PATHS, N_TRADES))
P = prices[idx]                      # entry price per (path, trade)
B = net_b(P)                         # net win odds
BK = bucket(P)
WON_EMP = won[idx]

# sizing always uses POINT estimates (what the trader would do)
FSTAR = q_point[BK] - (1 - q_point[BK]) / B      # full-Kelly fraction, per trade
FSTAR = np.maximum(FSTAR, 0.0)
# conservative sizing variant: Kelly computed on the Wilson LOWER bound q
FSTAR_LO = np.maximum(q_lo[BK] - (1 - q_lo[BK]) / B, 0.0)

def world_outcomes(world):
    if world == "emp":
        return WON_EMP
    if world == "wlo":
        return U < q_lo[BK]
    if world == "decay3":
        return U < (q_point[BK] - 0.03)
    raise ValueError(world)


def simulate(world, rule, frac=None, cap=VCAP, vmin=VMIN, stop=True, hard_cap=None):
    """rule: 'cur' | 'flat5' | 'kelly'. frac: Kelly multiple. Returns metric dict."""
    W = world_outcomes(world)
    E = np.full(N_PATHS, E0)
    peak = np.full(N_PATHS, E0)
    mdd = np.zeros(N_PATHS)
    min_cum = np.zeros(N_PATHS)
    floor_hit = np.zeros(N_PATHS, bool)
    below5 = np.zeros(N_PATHS, bool)
    halted = np.zeros(N_PATHS, bool)
    n_exec = np.zeros(N_PATHS, np.int64)
    forced = np.zeros(N_PATHS, np.int64)      # trades where desired < vmin-floor of $5
    overbet_sum = np.zeros(N_PATHS)           # sum of stake/desired on forced trades
    ecap = cap if hard_cap is None else min(cap, hard_cap)

    for t in range(N_TRADES):
        can = (~halted) & (E >= VMIN)         # venue min is physical: need >= $5 to trade
        below5 |= (E < VMIN) & (~halted)
        if rule == "cur":
            desired = 0.25 * E
        elif rule == "flat5":
            desired = np.full(N_PATHS, 5.0)
        elif rule == "kelly":
            desired = frac * FSTAR[:, t] * E
        elif rule == "kelly_lo":
            desired = frac * FSTAR_LO[:, t] * E
        trade = can & (desired > 0)
        stake = np.clip(desired, vmin, ecap)
        stake = np.minimum(stake, E)          # never bet more than equity
        f_mask = trade & (desired < 5.0) & (vmin >= 5.0)
        forced += f_mask
        overbet_sum += np.where(f_mask, stake / np.maximum(desired, 1e-12), 0.0)
        w = W[:, t]
        dE = np.where(trade, np.where(w, stake * B[:, t], -stake), 0.0)
        E = E + dE
        n_exec += trade
        peak = np.maximum(peak, E)
        mdd = np.maximum(mdd, (peak - E) / peak)
        cum = E - E0
        min_cum = np.minimum(min_cum, cum)
        hit = cum <= FLOOR
        floor_hit |= hit
        below5 |= E < VMIN
        if stop:
            halted |= hit
        halted |= E < VMIN

    lg = np.where(n_exec > 0, np.log(np.maximum(E, 1e-9) / E0) / np.maximum(n_exec, 1), 0.0)
    q50, q5, q95 = np.percentile(E, [50, 5, 95])
    tot_forced = forced.sum()
    return dict(
        med=q50, p5=q5, p95=q95, mean=E.mean(),
        logg=lg.mean(), logg_med=np.median(lg),
        mdd_med=np.median(mdd), mdd_p95=np.percentile(mdd, 95),
        p_floor=floor_hit.mean(), p_below5=below5.mean(),
        exec_mean=n_exec.mean(),
        forced_frac=tot_forced / max(n_exec.sum(), 1),
        overbet=overbet_sum.sum() / max(tot_forced, 1),
        min_cum=min_cum,
    )


HDR = (f"{'rule':<22} {'med_T':>8} {'p5_T':>8} {'p95_T':>9} {'logg/tr':>8} {'loggmed':>8} "
       f"{'mdd_med':>7} {'mdd_p95':>7} {'P(floor)':>8} {'P(E<5)':>7} {'n_exec':>6} "
       f"{'forced%':>7} {'overbet':>7}")

def prow(name, r):
    print(f"{name:<22} {r['med']:>8.2f} {r['p5']:>8.2f} {r['p95']:>9.2f} {r['logg']:>8.5f} "
          f"{r['logg_med']:>8.5f} "
          f"{r['mdd_med']:>7.3f} {r['mdd_p95']:>7.3f} {r['p_floor']:>8.4f} {r['p_below5']:>7.4f} "
          f"{r['exec_mean']:>6.0f} {100*r['forced_frac']:>6.1f}% {r['overbet']:>7.2f}")


RULES = [
    ("current 25% [5,25]", dict(rule="cur")),
    ("flat $5", dict(rule="flat5")),
    ("Kelly 1.0 [5,25]", dict(rule="kelly", frac=1.0)),
    ("Kelly 1/2 [5,25]", dict(rule="kelly", frac=0.5)),
    ("Kelly 1/4 [5,25]", dict(rule="kelly", frac=0.25)),
    ("Kelly 1/8 [5,25]", dict(rule="kelly", frac=0.125)),
    ("K1/2-of-f*lo [5,25]", dict(rule="kelly_lo", frac=0.5)),
    ("K1/4-of-f*lo [5,25]", dict(rule="kelly_lo", frac=0.25)),
]

for world, label in [("emp", "WORLD 1: empirical joint (222 rows, q=93.2%)"),
                     ("wlo", "WORLD 2: true q = per-bucket Wilson LOWER bound (sizing still uses point q)"),
                     ("decay3", "WORLD 3: edge decay, true q = point q - 3pp per bucket")]:
    print(f"\n== {label} ==  (20k paths x 500 trades, $19 start, stop at cum<=-11.40)")
    print(HDR)
    for name, kw in RULES:
        prow(name, simulate(world, stop=True, **kw))

# ---------------- venue-min distortion: hypothetical no-min comparison ----------------
print("\n== Venue-min distortion: same rules with hypothetical $0 min (world 1 / world 2) ==")
print(HDR)
for name, kw in [("cur no-min (emp)", dict(rule="cur")),
                 ("K1/4 no-min (emp)", dict(rule="kelly", frac=0.25)),
                 ("K1/8 no-min (emp)", dict(rule="kelly", frac=0.125))]:
    prow(name, simulate("emp", stop=True, vmin=0.0, **kw))
for name, kw in [("cur no-min (wlo)", dict(rule="cur")),
                 ("K1/4 no-min (wlo)", dict(rule="kelly", frac=0.25)),
                 ("K1/8 no-min (wlo)", dict(rule="kelly", frac=0.125))]:
    prow(name, simulate("wlo", stop=True, vmin=0.0, **kw))

# ---------------- drawdown-constrained rule search (world 2) ----------------
print("\n== Rule 5 search: quarter-Kelly with hard per-trade cap C, world 2 (Wilson-lower q) ==")
print("target: P(hit -11.40 floor within 500 trades) <= 1%")
print(HDR)
frontier = {}
for C in [5.0, 6.0, 7.5, 10.0, 15.0, 20.0, 25.0]:
    r = simulate("wlo", rule="kelly", frac=0.25, stop=True, hard_cap=C)
    frontier[C] = r["p_floor"]
    prow(f"K1/4 cap ${C:g}", r)
r5 = simulate("wlo", rule="flat5", stop=True)
print(f"(flat $5 = most conservative venue-legal rule: P(floor)={r5['p_floor']:.4f})")

# what floor depth would make 1% achievable at flat $5 under world 2?
r5ns = simulate("wlo", rule="flat5", stop=False)
f99 = np.percentile(r5ns["min_cum"], 1)
r5ns_emp = simulate("emp", rule="flat5", stop=False)
f99e = np.percentile(r5ns_emp["min_cum"], 1)
print(f"floor depth for P(hit)<=1% at flat $5, no-stop min-cum 1st pct: "
      f"world2 {f99:.2f}, world1 {f99e:.2f}")

# ---------------- marginal value of raising the $25 cap (world 1) ----------------
print("\n== Marginal value of raising the $25 cap (world 1, stop on) ==")
print(HDR)
for name, kw, cap in [("cur cap 25", dict(rule="cur"), 25.0),
                      ("cur cap 50", dict(rule="cur"), 50.0),
                      ("cur cap 100", dict(rule="cur"), 100.0),
                      ("cur cap none", dict(rule="cur"), 1e18),
                      ("K1/2 cap 25", dict(rule="kelly", frac=0.5), 25.0),
                      ("K1/2 cap 50", dict(rule="kelly", frac=0.5), 50.0),
                      ("K1/2 cap 100", dict(rule="kelly", frac=0.5), 100.0),
                      ("K1/2 cap none", dict(rule="kelly", frac=0.5), 1e18),
                      ("K1/4 cap 25", dict(rule="kelly", frac=0.25), 25.0),
                      ("K1/4 cap 100", dict(rule="kelly", frac=0.25), 100.0),
                      ("K1/2-f*lo cap 25", dict(rule="kelly_lo", frac=0.5), 25.0),
                      ("K1/2-f*lo cap 50", dict(rule="kelly_lo", frac=0.5), 50.0),
                      ("K1/2-f*lo cap 100", dict(rule="kelly_lo", frac=0.5), 100.0)]:
    prow(name, simulate("emp", stop=True, cap=cap, **kw))

# same under world 2 for the serious candidates
print("\n== Cap raise under world 2 (Wilson-lower) ==")
print(HDR)
for name, kw, cap in [("cur cap 25 (wlo)", dict(rule="cur"), 25.0),
                      ("cur cap 100 (wlo)", dict(rule="cur"), 100.0),
                      ("K1/2 cap 100 (wlo)", dict(rule="kelly", frac=0.5), 100.0),
                      ("K1/4 cap 100 (wlo)", dict(rule="kelly", frac=0.25), 100.0),
                      ("K1/2-f*lo cap 25", dict(rule="kelly_lo", frac=0.5), 25.0),
                      ("K1/2-f*lo cap 100", dict(rule="kelly_lo", frac=0.5), 100.0)]:
    prow(name, simulate("wlo", stop=True, cap=cap, **kw))

# ---------------- pure-ruin check without money stop (world 1 & 2) ----------------
print("\n== No-money-stop variant: P(equity ever < $5) as pure ruin (stop off) ==")
print(HDR)
for w in ["emp", "wlo"]:
    for name, kw in RULES:
        r = simulate(w, stop=False, **kw)
        prow(f"{name} ({w})", r)

# ---------------- addendum: floor depth for 1% under candidate rules ----------------
print("\n== Addendum: 1st percentile of min cumulative PnL (no-stop) -> floor depth for P(hit)<=1% ==")
for w in ["emp", "wlo", "decay3"]:
    for name, kw in [("K1/2-of-f*lo", dict(rule="kelly_lo", frac=0.5)),
                     ("K1/4-of-f*lo", dict(rule="kelly_lo", frac=0.25)),
                     ("current 25%", dict(rule="cur"))]:
        r = simulate(w, stop=False, **kw)
        mc = r["min_cum"]
        print(f"  {name:<14} ({w:>6}): min-cum p1={np.percentile(mc,1):>8.2f}  p5={np.percentile(mc,5):>7.2f} "
              f"med={np.median(mc):>6.2f}  P(min<=-11.40)={np.mean(mc<=-11.40):.4f}")
