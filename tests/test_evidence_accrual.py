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


if __name__ == "__main__":
    unittest.main()
