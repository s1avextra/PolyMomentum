from __future__ import annotations

import importlib.util
import math
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "evidence_accrual", ROOT / "scripts/evidence_accrual.py"
)
assert SPEC and SPEC.loader
accrual = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(accrual)


def direct_mixture(outcomes):
    """Direct product-form mixture (no log space), as in evalue.rs's tests."""
    total = 0.0
    for index in range(1, accrual.LAMBDA_COUNT + 1):
        lambda_ = index * accrual.LAMBDA_STEP
        wealth = 1.0
        for p0, won in outcomes:
            wealth *= 1.0 + lambda_ * ((1.0 if won else 0.0) - p0)
        total += wealth
    return total / accrual.LAMBDA_COUNT


def run(outcomes):
    process = accrual.EProcess()
    for p0, won in outcomes:
        process.update(p0, won)
    return process


class EvidenceAccrualTest(unittest.TestCase):
    """Reference vectors: the #[cfg(test)] block of rust_engine/src/backtest/evalue.rs."""

    def test_grid_and_thresholds_mirror_rust(self):
        self.assertEqual(accrual.PROMOTE_E, 20.0)
        self.assertEqual(accrual.FUTILITY_E, 0.1)
        self.assertEqual(accrual.LAMBDA_STEP, 0.05)
        self.assertEqual(accrual.LAMBDA_COUNT, 20)

    def test_straight_wins_promote(self):
        # evalue.rs: 30 wins at 0.85 reach only ~17.84; 35 (~31.6) clear PROMOTE_E.
        thirty = run([(0.85, True)] * 30)
        self.assertAlmostEqual(thirty.e_value(), 17.84, places=2)
        self.assertEqual(thirty.verdict(), "continue")
        outcomes = [(0.85, True)] * 35
        process = run(outcomes)
        expected = direct_mixture(outcomes)
        self.assertLess(abs(process.e_value() - expected) / expected, 1e-9)
        self.assertAlmostEqual(process.e_value(), 31.6, places=1)
        self.assertGreater(process.e_value(), accrual.PROMOTE_E)
        self.assertEqual(process.verdict(), "promote")
        self.assertEqual(process.n, 35)

    def test_straight_losses_kill(self):
        outcomes = [(0.85, False)] * 10
        process = run(outcomes)
        expected = direct_mixture(outcomes)
        self.assertLess(abs(process.e_value() - expected) / expected, 1e-9)
        self.assertLess(process.e_value(), accrual.FUTILITY_E)
        self.assertEqual(process.verdict(), "kill")

    def test_exact_null_never_promotes(self):
        # 10 blocks of 17 wins + 3 losses at p0 = 0.85: empirical win rate
        # matches break-even exactly, so wealth must not accumulate.
        process = accrual.EProcess()
        for _ in range(10):
            for _ in range(17):
                process.update(0.85, True)
            for _ in range(3):
                process.update(0.85, False)
        self.assertEqual(process.n, 200)
        self.assertLess(process.e_value(), accrual.PROMOTE_E)
        self.assertNotEqual(process.verdict(), "promote")

    def test_invalid_break_even_rejected(self):
        process = accrual.EProcess()
        for break_even in (0.0, 1.0, 1.5, -0.2, math.nan):
            with self.assertRaises(ValueError):
                process.update(break_even, True)
        self.assertEqual(process.n, 0)
        self.assertEqual(process.e_value(), 1.0)

    def test_deterministic_replay_identical(self):
        outcomes = [(0.55 + 0.005 * (index % 40), index % 3 != 0) for index in range(60)]
        a = run(outcomes)
        b = run(outcomes)
        self.assertEqual(a.e_value(), b.e_value())
        self.assertEqual(a.n, b.n)

    def test_json_round_trip_preserves_state(self):
        outcomes = [(0.55 + 0.005 * (index % 40), index % 3 != 0) for index in range(60)]
        process = run(outcomes)
        restored = accrual.EProcess.from_json(process.to_json())
        self.assertEqual(restored.n, process.n)
        self.assertEqual(restored.e_value(), process.e_value())
        for p0, won in outcomes[:7]:
            process.update(p0, won)
            restored.update(p0, won)
        self.assertEqual(restored.e_value(), process.e_value())
        self.assertEqual(restored.verdict(), process.verdict())
        with self.assertRaises(ValueError):
            accrual.EProcess([0.0] * 3, 0)


LAMBDAS = [(index + 1) * accrual.LAMBDA_STEP for index in range(accrual.LAMBDA_COUNT)]


def run_signed(values):
    process = accrual.EProcess()
    for value in values:
        process.update_signed(value)
    return process


class SignedUpdateTest(unittest.TestCase):
    """Reference vectors for update_signed, shared with the Rust side
    (relative error <= 1e-9 against the closed-form mixture)."""

    def assert_matches(self, process, expected):
        self.assertLess(abs(process.e_value() - expected) / expected, 1e-9)

    def test_positive_drift_promotes(self):
        process = run_signed([0.2] * 30)
        expected = sum((1.0 + 0.2 * lambda_) ** 30 for lambda_ in LAMBDAS) / accrual.LAMBDA_COUNT
        self.assert_matches(process, expected)
        self.assertEqual(process.n, 30)
        self.assertGreaterEqual(process.e_value(), accrual.PROMOTE_E)
        self.assertEqual(process.verdict(), "promote")

    def test_negative_drift_kills(self):
        process = run_signed([-0.5] * 20)
        expected = (
            sum(max(1.0 - 0.5 * lambda_, accrual.FACTOR_FLOOR) ** 20 for lambda_ in LAMBDAS)
            / accrual.LAMBDA_COUNT
        )
        self.assert_matches(process, expected)
        self.assertEqual(process.n, 20)
        self.assertLessEqual(process.e_value(), accrual.FUTILITY_E)
        self.assertEqual(process.verdict(), "kill")

    def test_alternating_unit_pairs(self):
        # (1 + lambda)(1 - lambda) per pair; the lambda = 1 pair is floored at
        # 1e-12 * 2 in the process and 0 in the formula, far below 1e-9 relative.
        process = run_signed([1.0, -1.0] * 10)
        expected = sum((1.0 - lambda_ ** 2) ** 10 for lambda_ in LAMBDAS) / accrual.LAMBDA_COUNT
        self.assert_matches(process, expected)
        self.assertEqual(process.n, 20)
        self.assertLess(process.e_value(), 1.0)
        self.assertEqual(process.verdict(), "continue")

    def test_out_of_range_d_is_clipped(self):
        self.assertEqual(run_signed([3.0] * 5).e_value(), run_signed([1.0] * 5).e_value())
        self.assertEqual(run_signed([-3.0] * 5).e_value(), run_signed([-1.0] * 5).e_value())
        self.assertEqual(run_signed([3.0, -3.0]).n, 2)
        process = accrual.EProcess()
        with self.assertRaises(ValueError):
            process.update_signed(math.nan)
        self.assertEqual(process.n, 0)
        self.assertEqual(process.e_value(), 1.0)

    def test_verdict_thresholds_unchanged(self):
        self.assertEqual(accrual.PROMOTE_E, 20.0)
        self.assertEqual(accrual.FUTILITY_E, 0.1)
        self.assertEqual(accrual.EProcess([math.log(20.001)] * accrual.LAMBDA_COUNT, 1).verdict(), "promote")
        self.assertEqual(accrual.EProcess([math.log(19.999)] * accrual.LAMBDA_COUNT, 1).verdict(), "continue")
        self.assertEqual(accrual.EProcess([math.log(0.0999)] * accrual.LAMBDA_COUNT, 1).verdict(), "kill")
        self.assertEqual(accrual.EProcess([math.log(0.1001)] * accrual.LAMBDA_COUNT, 1).verdict(), "continue")
        # update and update_signed share one wealth vector.
        mixed = accrual.EProcess()
        mixed.update(0.85, True)
        mixed.update_signed(0.15)
        expected = sum((1.0 + 0.15 * lambda_) ** 2 for lambda_ in LAMBDAS) / accrual.LAMBDA_COUNT
        self.assertLess(abs(mixed.e_value() - expected) / expected, 1e-9)
        self.assertEqual(mixed.n, 2)


class EBhTest(unittest.TestCase):
    def test_e_bh_discovers_the_largest_k_meeting_its_threshold(self):
        family = accrual.e_bh({"a": 1300.0, "b": 700.0, "c": 100.0, "d": 5.0}, 64)
        self.assertEqual(
            family,
            {
                "campaign_n": 64,
                "alpha": 0.05,
                "candidates": 4,
                "k_star": 2,
                "threshold": 640.0,
                "promoted": ["a", "b"],
                "overflow": False,
            },
        )
        # N=1 is Ville's e >= 1/alpha = 20.
        self.assertEqual(accrual.e_bh({"a": 20.0}, 1)["promoted"], ["a"])
        self.assertEqual(accrual.e_bh({"a": 19.9}, 1)["promoted"], [])
        # The running bar for a lone discovery is N/alpha.
        lone = accrual.e_bh({"a": 1279.9}, 64)
        self.assertEqual((lone["k_star"], lone["threshold"], lone["promoted"]), (0, 1280.0, []))
        # Ties at the boundary enter together.
        self.assertEqual(accrual.e_bh({"b": 640.0, "a": 640.0}, 64)["promoted"], ["a", "b"])
        # k* is the largest k: a weak third leaves the top two standing.
        self.assertEqual(accrual.e_bh({"a": 641.0, "b": 640.0, "c": 1.0}, 64)["k_star"], 2)
        # More candidates than registered: N grows to the count, flagged.
        overflow = accrual.e_bh({"a": 1300.0, "b": 700.0, "c": 100.0, "d": 5.0}, 2)
        self.assertEqual((overflow["campaign_n"], overflow["overflow"], overflow["k_star"]), (4, True, 3))
        self.assertEqual(overflow["promoted"], ["a", "b", "c"])
        # Unregistered: nothing can be discovered.
        self.assertEqual(
            accrual.e_bh({"a": 1e9}, None),
            {"campaign_n": None, "alpha": 0.05, "candidates": 1, "k_star": 0, "threshold": None, "promoted": [], "overflow": False},
        )
        self.assertEqual(accrual.e_bh({}, 64)["promoted"], [])
        self.assertEqual(accrual.e_bh({"a": 20.0}, 1, alpha=0.1)["threshold"], 10.0)


if __name__ == "__main__":
    unittest.main()
