from __future__ import annotations

import importlib.util
import json
import math
from pathlib import Path
import random
import unittest


ROOT = Path(__file__).resolve().parents[1]


def _load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


model = _load("settlement_model", "scripts/settlement_model.py")

W = 1790000100  # a window start (multiple of 300)
FEE = 0.07
# The frozen shape with a short warm-up, so a fixture is minutes of closes.
PARAMS = {**model.DEFAULT_PARAMS, "warmup_s": 600}
LEAD = PARAMS["warmup_s"] + PARAMS["subsample_s"] + PARAMS["twap_s"]


def spec_of(c=1.19, s_b=3.5, params=PARAMS):
    return {"params": params, "c": c, "s_b": s_b}


def ramp(start=W - LEAD, end=W + 300, slope=1.0):
    """P[i] = 70000 + slope * (i - W): every hand value below follows."""
    return {ts: 70000.0 + slope * (ts - W) for ts in range(start, end)}


def walk(seed, start, end, sd=4.0, base=70000.0):
    rng = random.Random(seed)
    closes, price = {}, base
    for ts in range(start, end):
        price += rng.gauss(0.0, sd)
        closes[ts] = price
    return closes


def series_of(closes, params=PARAMS):
    return model.CloseSeries(closes, min(closes), max(closes) + 1, params["max_fill_s"])


class SettlementModelTest(unittest.TestCase):
    def test_variance_weight_before_and_inside_the_closing_twap(self):
        # V_t = (240 - t) + 20.5 up to 240 s, r(r+1)(2r+1)/21600 after.
        self.assertEqual([model.variance_weight(t, PARAMS) for t in (150, 180, 210, 240)], [110.5, 80.5, 50.5, 20.5])
        self.assertAlmostEqual(model.variance_weight(270, PARAMS), 2.63, places=2)
        self.assertAlmostEqual(model.variance_weight(270, PARAMS), 30 * 31 * 61 / 21600.0, places=12)
        self.assertAlmostEqual(model.variance_weight(241, PARAMS), 59 * 60 * 119 / 21600.0, places=12)
        self.assertAlmostEqual(model.variance_weight(299, PARAMS), 6 / 21600.0, places=12)
        self.assertEqual(model.variance_weight(300, PARAMS), 0.0)
        # The frozen 20.5 is V at the lock start from the closing formula (20.503).
        self.assertAlmostEqual(60 * 61 * 121 / 21600.0, PARAMS["v_locked"], places=2)

    def test_strike_margin_and_z_against_hand_values(self):
        series = series_of(ramp())
        # K = mean(P[W-60 .. W-1]) = 70000 - 30.5.
        self.assertAlmostEqual(model.strike(series, W, PARAMS), 70000.0 - 30.5, places=9)
        # t <= 240: m_t = P[W+t-1] - K.
        self.assertAlmostEqual(model.settlement_margin(series, W, 150, PARAMS), 149 + 30.5, places=9)
        self.assertAlmostEqual(model.settlement_margin(series, W, 240, PARAMS), 239 + 30.5, places=9)
        # t = 270, r = 30: (sum(P[W+240 .. W+269]) + 30 * P[W+269]) / 60 - K
        # = (30 * 254.5 + 30 * 269) / 60 + 30.5 = 292.25.
        self.assertAlmostEqual(model.settlement_margin(series, W, 270, PARAMS), 292.25, places=9)
        # The first second of the closing TWAP: (P[W+240] + 59 * P[W+240]) / 60 - K.
        self.assertAlmostEqual(model.settlement_margin(series, W, 241, PARAMS), 240 + 30.5, places=9)
        # The window end: mean(P[W+240 .. W+299]) - K = 269.5 + 30.5.
        self.assertAlmostEqual(model.final_margin(series, W, PARAMS), 300.0, places=9)
        self.assertIsNone(model.settlement_margin(series, W, 0, PARAMS))
        self.assertIsNone(model.settlement_margin(series, W, 301, PARAMS))
        # d_i = (P[i] - P[i-30])^2 / 30 = 30 everywhere: the EWMA is 30.
        self.assertAlmostEqual(series.sigma2(W + 149, PARAMS), 30.0, places=9)
        # s_150 = sqrt(1.19 * 30 * 110.5 + 3.5^2) = sqrt(3957.1); z = 179.5 / s.
        state = model.state(series, W, 150, spec_of())
        self.assertAlmostEqual(state["s"], math.sqrt(3957.1), places=9)
        self.assertAlmostEqual(state["z"], 179.5 / math.sqrt(3957.1), places=9)
        self.assertEqual((state["side"], state["v"], state["t"]), ("up", 110.5, 150))
        self.assertAlmostEqual(state["z"], 2.8535, places=4)
        # s_270 = sqrt(1.19 * 30 * 2.6264 + 12.25); the basis noise dominates late.
        late = model.state(series, W, 270, spec_of())
        self.assertAlmostEqual(late["z"], 292.25 / math.sqrt(1.19 * 30 * (30 * 31 * 61 / 21600.0) + 12.25), places=9)
        # A falling ramp is the mirror: same z, side down; a tie (flat) settles up.
        down = model.state(series_of(ramp(slope=-1.0)), W, 150, spec_of())
        self.assertEqual(down["side"], "down")
        self.assertAlmostEqual(down["z"], state["z"], places=9)
        flat = model.state(series_of(ramp(slope=0.0)), W, 150, spec_of())
        self.assertEqual((flat["margin"], flat["z"], flat["side"], flat["sigma2"]), (0.0, 0.0, "up", 0.0))
        # path() carries the same numbers as state().
        path = model.path(series, W, 148, 270, spec_of())
        self.assertEqual(sorted(path), list(range(148, 271)))
        for t in (148, 150, 240, 241, 270):
            reference = model.state(series, W, t, spec_of())
            self.assertEqual(path[t], (reference["margin"], reference["z"]))
        # The constant-sigma control floor: sig2 replaced by a constant.
        const = model.path(series, W, 150, 150, spec_of(), sigma2_const=7.5)[150]
        self.assertAlmostEqual(const[1], 179.5 / math.sqrt(1.19 * 7.5 * 110.5 + 12.25), places=9)

    def test_ewma_is_the_normalised_half_life_300_average_of_30_s_squares(self):
        closes = walk(3, W - LEAD, W + 300)
        series = series_of(closes)
        lam = 2.0 ** (-1.0 / 300)
        for index in (W + 149, W + 239, W + 299):
            squares = [(closes[index - k] - closes[index - k - 30]) ** 2 / 30 for k in range(PARAMS["warmup_s"])]
            direct = sum(lam**k * value for k, value in enumerate(squares)) / sum(lam**k for k in range(PARAMS["warmup_s"]))
            self.assertAlmostEqual(series.sigma2(index, PARAMS), direct, places=9)
        # A pure function of the closes: the same value whatever the series start.
        longer = model.CloseSeries({**walk(9, W - 3 * LEAD, W - LEAD), **closes}, W - 3 * LEAD, W + 300, PARAMS["max_fill_s"])
        self.assertAlmostEqual(longer.sigma2(W + 149, PARAMS), series.sigma2(W + 149, PARAMS), places=9)
        # Before the warm-up is covered there is no estimate, and no state.
        self.assertIsNone(series.sigma2(W - LEAD + PARAMS["warmup_s"], PARAMS))
        short = series_of({ts: price for ts, price in closes.items() if ts >= W - 300})
        self.assertIsNone(model.state(short, W, 150, spec_of()))
        self.assertEqual(model.path(short, W, 150, 270, spec_of()), {})
        # At the frozen warm-up (24 half-lives) the finite window is the
        # recursion sig2_i = lam * sig2_(i-1) + (1 - lam) * d_i to 6e-8.
        self.assertLess(lam ** model.DEFAULT_PARAMS["warmup_s"], 1e-7)
        self.assertEqual((model.DEFAULT_PARAMS["half_life_s"], model.DEFAULT_PARAMS["subsample_s"]), (300, 30))

    def test_no_lookahead(self):
        closes = walk(5, W - LEAD, W + 300)
        spec = spec_of()
        for t in (150, 200, 240, 241, 270):
            before = model.state(series_of(closes), W, t, spec)
            # Every close of second W + t and later rewritten: nothing moves.
            future = {ts: (price + 500.0 if ts >= W + t else price) for ts, price in closes.items()}
            self.assertEqual(model.state(series_of(future), W, t, spec), before)
            self.assertEqual(model.path(series_of(future), W, 150, t, spec), model.path(series_of(closes), W, 150, t, spec))
            # The newest close it reads is P[W + t - 1].
            latest = dict(closes)
            latest[W + t - 1] += 500.0
            self.assertNotEqual(model.state(series_of(latest), W, t, spec)["margin"], before["margin"])

    def test_forward_fill_is_bounded(self):
        closes = ramp()
        for ts in range(W + 100, W + 105):  # five missing seconds: filled from W + 99
            del closes[ts]
        series = series_of(closes)
        self.assertEqual(series.close(W + 104), closes[W + 99])
        self.assertIsNotNone(model.state(series, W, 105, spec_of()))
        del closes[W + 105]  # the sixth: unusable, and so is what reads it
        series = series_of(closes)
        self.assertIsNone(series.close(W + 105))
        self.assertIsNone(model.state(series, W, 106, spec_of()))
        self.assertIsNotNone(model.state(series, W, 105, spec_of()))
        self.assertIsNone(series.sigma2(W + 105 + 30, PARAMS))  # a square that reads the gap
        self.assertIsNone(model.final_margin(series, W + 300, PARAMS))  # beyond the series
        gap_in_strike = ramp()
        for ts in range(W - 20, W - 10):
            del gap_in_strike[ts]
        self.assertIsNone(model.strike(series_of(gap_in_strike), W, PARAMS))

    def test_calibration_table_wilson_bounds_and_schedule_cap(self):
        looks = (
            [(2.1, True)] * 5051
            + [(2.1, False)] * 102
            + [(2.5, True)] * 3705
            + [(2.74, False)] * 30
            + [(4.2, True)] * 5066
            + [(4.9, False)] * 4
            + [(250.0, True)] * 10
            + [(0.5, False)] * 3
        )
        table = model.calibration_table(looks, FEE, PARAMS)
        by_low = {row["z_low"]: row for row in table}
        self.assertEqual([row["z_low"] for row in table], PARAMS["z_edges"])
        self.assertEqual((table[-1]["z_high"], table[0]["z_high"]), (None, 1.0))
        row = by_low[2.5]
        self.assertEqual((row["n"], row["wins"], row["z_high"]), (3735, 3705, 2.75))
        self.assertAlmostEqual(row["rate"], 0.9920, places=4)
        self.assertAlmostEqual(row["wilson_lower"], 0.98856, places=5)
        self.assertAlmostEqual(row["wilson_upper"], 0.99437, places=5)
        # k98: the largest tick with BE(tick) <= lower - 0.005.  BE(0.98) =
        # 0.981372 <= 0.98356 < BE(0.99) = 0.990693.
        self.assertEqual(row["cap"], 0.98)
        self.assertAlmostEqual(model.break_even(0.98, FEE), 0.981372, places=9)
        self.assertAlmostEqual(model.break_even(0.99, FEE), 0.990693, places=9)
        # [4, 5): 5066/5070, lower 0.99797 - 0.005 >= BE(0.99): the cap opens to 0.99.
        self.assertEqual((by_low[4.0]["n"], by_low[4.0]["cap"]), (5070, 0.99))
        # [2, 2.25): 5051/5153, lower 0.97600: BE(0.96) = 0.962688 fits, BE(0.97) = 0.972037 does not.
        self.assertEqual(by_low[2.0]["cap"], 0.96)
        # A losing bucket has no tick; an empty one no bounds.
        self.assertEqual((by_low[0.0]["n"], by_low[0.0]["wins"], by_low[0.0]["cap"]), (3, 0, None))
        self.assertEqual((by_low[3.0]["n"], by_low[3.0]["wilson_lower"], by_low[3.0]["cap"]), (0, None, None))
        self.assertEqual((by_low[5.0]["n"], by_low[5.0]["wilson_upper"]), (10, 1.0))
        spec = {"table": table}
        self.assertEqual(model.bucket(2.5, spec)["z_low"], 2.5)
        self.assertEqual(model.bucket(2.4999, spec)["z_low"], 2.25)
        self.assertEqual(model.bucket(1e6, spec)["z_low"], 5.0)
        self.assertEqual([model.schedule_cap(z, spec) for z in (0.3, 2.6, 3.2, 4.0, 7.0)], [None, 0.98, None, 0.99, None])
        # The buffer and the fee are the spec's: a thicker buffer lowers the cap.
        thick = model.calibration_table(looks, FEE, {**PARAMS, "cap_buffer": 0.02})
        self.assertEqual({row["z_low"]: row["cap"] for row in thick}[2.5], 0.96)
        # Fee 1.0: BE(0.87) = 0.9831 <= 0.98356 < BE(0.88) = 0.9856.
        self.assertEqual({row["z_low"]: row["cap"] for row in model.calibration_table(looks, 1.0, PARAMS)}[2.5], 0.87)

    def fitted(self, seed=11, windows=120, extra=()):
        start = W - LEAD
        closes = walk(seed, start, W + 300 * windows + 300)
        series = series_of(closes)
        labelled = []
        for index in range(windows):
            final = model.final_margin(series, W + 300 * index, PARAMS)
            labelled.append((W + 300 * index, "up" if final >= 0 else "down"))
        return series, labelled + list(extra)

    def test_spec_is_fitted_strictly_before_the_cut_and_bound_by_its_hash(self):
        series, labelled = self.fitted()
        cut = W + 300 * 80
        spec = model.build_spec(series, labelled, cut, FEE, crossings=[(2.5, 150, 270)], params=PARAMS)
        self.assertEqual((spec["version"], spec["cut_ts"], spec["fee_rate"]), (model.SPEC_VERSION, cut, FEE))
        self.assertEqual((spec["fit"]["windows"], spec["fit"]["looks"], spec["fit"]["last_window_start"]), (80, 80 * 13, cut - 300))
        self.assertEqual((spec["fit"]["c"]["source"], spec["fit"]["s_b"]["source"]), ("fit", "fit"))
        self.assertEqual(sum(row["n"] for row in spec["table"]), 80 * 13)
        # Label-free c on a random walk: the 30 s squares measure the 1 s
        # variance, the remaining move scales with V_t: c is about 1.
        self.assertTrue(0.6 < spec["c"] < 1.6, spec["c"])
        self.assertEqual(spec["c"], model.fit_c(series, [ws for ws, _ in labelled[:80]], PARAMS)["c"])
        # Labels are the proxy's own final side: no basis noise to find, the grid's floor.
        self.assertEqual(spec["s_b"], PARAMS["s_b_grid"][0])
        crossing = spec["first_crossing"][0]
        self.assertEqual((crossing["z_min"], crossing["t_low"], crossing["t_high"]), (2.5, 150, 270))
        self.assertGreater(crossing["n"], 0)
        self.assertGreaterEqual(crossing["rate"], 0.9)
        self.assertGreater(spec["sigma2_median"], 0.0)
        # Windows that end after the cut never reach a constant or the
        # table: flipping every later label (and adding junk) changes nothing.
        flipped = labelled[:80] + [(ws, "down" if official == "up" else "up") for ws, official in labelled[80:]] + [(W + 300 * 500, "up")]
        again = model.build_spec(series, flipped, cut, FEE, crossings=[(2.5, 150, 270)], params=PARAMS)
        self.assertEqual(again, spec)
        self.assertEqual(again["sha256"], spec["sha256"])
        # A window that ends exactly at the cut is before it; one second less and it is not.
        self.assertEqual(model.build_spec(series, labelled, cut - 1, FEE, params=PARAMS)["fit"]["windows"], 79)
        # Hash: stable through JSON, binds every field.
        model.verify_spec(spec)
        loaded = json.loads(json.dumps(spec))
        self.assertEqual(model.spec_sha256(loaded), spec["sha256"])
        model.verify_spec(loaded)
        for tampered in ({**loaded, "c": loaded["c"] + 1e-9}, {**loaded, "s_b": 3.4}, {**loaded, "table": loaded["table"][:-1]}, {**loaded, "params": {**loaded["params"], "cap_buffer": 0.004}}):
            with self.assertRaises(ValueError):
                model.verify_spec(tampered)
        with self.assertRaises(ValueError):
            model.verify_spec({**loaded, "version": "settlement_z_v0"})
        with self.assertRaises(ValueError):
            model.verify_spec(None)
        # Given constants are recorded as given and move the hash.
        given = model.build_spec(series, labelled, cut, FEE, c=1.19, s_b=3.5, params=PARAMS)
        self.assertEqual((given["c"], given["s_b"], given["fit"]["c"]["source"], given["fit"]["s_b"]["source"]), (1.19, 3.5, "given", "given"))
        self.assertNotEqual(given["sha256"], spec["sha256"])
        # Nothing before the cut: no spec.
        with self.assertRaises(ValueError):
            model.build_spec(series, labelled, W, FEE, params=PARAMS)

    def test_basis_noise_fit_recovers_the_label_noise(self):
        # Finals of a quiet walk (a few USD) labelled through 3.5 USD of
        # Gaussian basis noise: the likelihood grid lands near 3.5.
        start = W - 100
        count = 1500
        closes = walk(21, start, W + 300 * count + 300, sd=0.25)
        series = model.CloseSeries(closes, start, W + 300 * count + 300)
        rng = random.Random(4)
        labelled = []
        for index in range(count):
            final = model.final_margin(series, W + 300 * index, PARAMS)
            labelled.append((W + 300 * index, "up" if final + rng.gauss(0.0, 3.5) >= 0 else "down"))
        fit = model.fit_s_b(series, labelled, PARAMS)
        self.assertEqual(fit["windows"], count)
        self.assertTrue(2.8 <= fit["s_b"] <= 4.3, fit)


if __name__ == "__main__":
    unittest.main()
