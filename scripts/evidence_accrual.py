#!/usr/bin/env python3
"""Anytime-valid e-process for win-rate vs per-trade break-even.

Python mirror of rust_engine/src/backtest/evalue.rs: the same lambda grid
(0.05..1.0 step 0.05), per-lambda log-wealth, log-sum-exp mixture and
thresholds.  The Rust unit tests are the reference vectors; see
tests/test_evidence_accrual.py.

For outcomes i = 1..n with break-even probability p0_i in (0,1) and result
X_i in {0,1}, the betting wealth for a fixed lambda >= 0 is
E_lambda = prod_i (1 + lambda * (X_i - p0_i)).  Under the composite null
(true win prob <= p0_i per trade) each factor has expectation <= 1, so
E_lambda is a supermartingale and Ville's inequality gives
P(sup_t E >= 1/alpha) <= alpha at ANY stopping time.  The discrete mixture
over the lambda grid (mean of the per-lambda wealths) avoids tuning lambda
and inherits the same guarantee.
"""

from __future__ import annotations

import json
import math
from typing import List, Optional, Sequence

# Promote threshold: rejects the null at alpha = 1/20 = 0.05 by Ville.
PROMOTE_E = 20.0
# Practical futility stop; not a type-I bound.
FUTILITY_E = 0.1

LAMBDA_STEP = 0.05
LAMBDA_COUNT = 20
# Factor clamp for the p0 -> 1.0 edge; with p0 < 1 and lambda <= 1 every
# factor is already strictly positive.
FACTOR_FLOOR = 1e-12


class EProcess:
    """Running log-wealth per lambda; the mixture is combined via log-sum-exp
    so long win/loss streaks neither overflow nor underflow."""

    def __init__(self, log_wealth: Optional[Sequence[float]] = None, n: int = 0) -> None:
        values = (
            [0.0] * LAMBDA_COUNT
            if log_wealth is None
            else [float(value) for value in log_wealth]
        )
        if len(values) != LAMBDA_COUNT or int(n) < 0:
            raise ValueError("invalid e-process state")
        self.log_wealth: List[float] = values
        self.n = int(n)

    def update(self, break_even: float, won: bool) -> None:
        break_even = float(break_even)
        # NaN fails both comparisons, exactly like the Rust guard.
        if not (break_even > 0.0 and break_even < 1.0):
            raise ValueError("break_even %r outside (0,1)" % break_even)
        x = 1.0 if won else 0.0
        for index in range(LAMBDA_COUNT):
            lambda_ = (index + 1) * LAMBDA_STEP
            factor = max(1.0 + lambda_ * (x - break_even), FACTOR_FLOOR)
            self.log_wealth[index] += math.log(factor)
        self.n += 1

    def e_value(self) -> float:
        """Current mixture e-value: mean over the lambda grid of exp(log-wealth)."""
        peak = max(self.log_wealth)
        total = sum(math.exp(value - peak) for value in self.log_wealth)
        return math.exp(peak + math.log(total / LAMBDA_COUNT))

    def verdict(self) -> str:
        e = self.e_value()
        if e >= PROMOTE_E:
            return "promote"
        if e <= FUTILITY_E:
            return "kill"
        return "continue"

    def to_json(self) -> str:
        return json.dumps({"log_wealth": self.log_wealth, "n": self.n})

    @classmethod
    def from_json(cls, text: str) -> "EProcess":
        payload = json.loads(text)
        return cls(payload["log_wealth"], payload["n"])
