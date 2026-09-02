from __future__ import annotations

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("factory_kpi", ROOT / "scripts/factory_kpi.py")
assert SPEC and SPEC.loader
kpi = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(kpi)
loop = kpi.loop

LANE = "late_window_mechanisms"


# Public projections of path_and_move rules (the fields stage 1 reads) and the
# execution variants stage 1 cannot see: consecutive indices are distinct
# projections; variants of one index share its projection.
PROJECTIONS = [
    (path, move, buffer, direction)
    for path in (2, 3, 4)
    for move in (100, 200)
    for buffer in sorted(loop.LATE_DECISION_BUFFERS_USD)
    for direction in ("both", "up", "down")
]
VARIANTS = [
    (cap, sigma, pressure)
    for cap in sorted(loop.LATE_MAXIMUM_ENTRY_PRICES)
    for sigma in sorted(loop.LATE_SETTLEMENT_SIGMA_BUFFERS)
    for pressure in sorted(loop.LATE_MINIMUM_BOOK_PRESSURES)
]


def proposal(index, variant=0):
    path, move, buffer, direction = PROJECTIONS[index % len(PROJECTIONS)]
    cap, sigma, pressure = VARIANTS[variant % len(VARIANTS)]
    return {
        "title": "rule %d/%d" % (index, variant),
        "rationale": "public",
        "expected_failure_mode": "chop",
        "rule": {
            "operator": "path_and_move",
            "path_minutes": path,
            "minimum_two_minute_move_usd": move,
            "maximum_entry_price": cap,
            "minimum_decision_buffer_usd": buffer,
            "settlement_sigma_buffer": sigma,
            "minimum_book_pressure": pressure,
            "direction": direction,
        },
    }


def seed(state_dir, arms):
    """arms: source -> (proposals, stage-1 survivors); survivors come first."""
    ledger = loop.Ledger(state_dir / "research.sqlite3")
    index = 0
    try:
        for source, (count, survivors) in arms.items():
            for offset in range(count):
                won = offset < survivors
                fingerprint = "%s-%d" % (source, offset)
                evidence_path = state_dir / ("evidence/%s/%s.json" % (LANE, fingerprint))
                loop.atomic_json(
                    evidence_path,
                    {"stage_1_survivor": won, "overall": {"accuracy": 0.9 if won else 0.4}},
                )
                ledger.add_hypothesis(
                    fingerprint,
                    LANE,
                    proposal(index),
                    None,
                    "stage_1_survivor" if won else "rejected_stage_1",
                    evidence_path,
                    source=source,
                )
                index += 1
    finally:
        ledger.close()


class FactoryKpiTest(unittest.TestCase):
    def report(self, arms, extra=None):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            seed(state_dir, arms)
            if extra:
                extra(state_dir)
            report = kpi.build_report(state_dir)
            text = kpi.render(report)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.assertEqual(kpi.main(["--state-dir", directory, "--json"]), 0)
        self.assertEqual(json.loads(stdout.getvalue())["lanes"], report["lanes"])
        return report, text

    def test_funnel_per_source_and_llm_beats_uniform(self):
        def extra(state_dir):
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                ledger.accrue("llm-0", LANE, [(300, 0.6, True)], 0)
                ledger.accrue("llm-1", LANE, [(300 * index, 0.5, True) for index in range(1, 60)], 0)
                ledger.accrue("uniform_control-0", LANE, [(300 * index, 0.9, False) for index in range(1, 30)], 0)
                for cycle, lane_result in (
                    ("c1", {"burst": {"generated": 6, "invalid": 2, "duplicate": 1, "novelty_rejected": 0, "survivors": 3}}),
                    ("c2", {"llm": {"burst": {"generated": 6, "invalid": 6, "duplicate": 0, "novelty_rejected": 0, "survivors": 0}}}),
                    ("c3", {"status": "not_due"}),
                ):
                    ledger.begin_cycle(cycle, {})
                    ledger.finish_cycle(cycle, "completed", {"lane": LANE, "lane_result": lane_result})
            finally:
                ledger.close()
            with (state_dir / "trial_ledger.jsonl").open("w") as handle:
                for candidate, stage, verdict in (
                    ("llm-0", "public_directional_screen", "stage_1_survivor"),
                    ("llm-0", "economic_opportunity_screen", "passed"),
                    ("llm-1", "economic_opportunity_screen", "rejected"),
                    ("uniform_control-0", "economic_opportunity_screen", "passed"),
                ):
                    handle.write(json.dumps({"candidate": candidate, "stage": stage, "verdict": verdict}) + "\n")

        report, text = self.report(
            {"llm": (20, 8), "burst_queue": (10, 4), "uniform_control": (30, 5), "diagnostic": (6, 6)},
            extra,
        )
        sources = report["lanes"][LANE]["sources"]
        self.assertEqual(set(sources), {"llm", "burst_queue", "uniform_control", "diagnostic"})
        llm = sources["llm"]
        self.assertEqual((llm["proposals"], llm["stage_1_survivors"], llm["stage_1_rate"]), (20, 8, 0.4))
        self.assertEqual((llm["stage_2_survivors"], llm["accruing"], llm["promote"], llm["killed"]), (1, 1, 1, 0))
        self.assertAlmostEqual(llm["mean_stage_1_accuracy"], (8 * 0.9 + 12 * 0.4) / 20)
        # proposal(index) enumerates distinct rules (and projections).
        self.assertEqual(llm["distinct_rules"], 20)
        uniform = sources["uniform_control"]
        self.assertEqual((uniform["stage_2_survivors"], uniform["killed"]), (1, 1))
        self.assertEqual(sources["diagnostic"]["stage_1_rate"], 1.0)
        stats = report["lanes"][LANE]["throughput"]
        self.assertEqual(
            (stats["cycles"], stats["generated"], stats["invalid"], stats["survivors"]), (2, 12, 8, 3)
        )
        self.assertEqual(stats["survivors_per_sample"], 0.25)
        verdict = report["lanes"][LANE]["verdict"]
        self.assertEqual(
            verdict["llm"], {"proposals": 30, "projections": 30, "stage_1_projections": 12}
        )
        self.assertEqual(
            verdict["uniform_control"],
            {"proposals": 30, "projections": 30, "stage_1_projections": 5},
        )
        self.assertIn("SAMPLER VERDICT: LLM beats uniform", text)
        self.assertEqual(report["lanes"]["band_mechanisms"]["sources"], {})
        self.assertIn("SAMPLER VERDICT: insufficient (llm=0, uniform=0)  lane=band_mechanisms", text)

    def test_verdict_demotes_llm_when_it_does_not_beat_uniform(self):
        report, text = self.report({"llm": (25, 5), "uniform_control": (25, 5)})
        self.assertEqual(report["lanes"][LANE]["verdict"]["text"], "demote LLM to reviewer role")
        self.assertIn("SAMPLER VERDICT: demote LLM to reviewer role", text)
        worse, worse_text = self.report({"llm": (40, 4), "uniform_control": (25, 5)})
        self.assertIn("SAMPLER VERDICT: demote LLM to reviewer role", worse_text)

    def test_verdict_is_insufficient_below_minimum_n(self):
        report, text = self.report({"llm": (24, 24), "uniform_control": (30, 0)})
        self.assertEqual(report["lanes"][LANE]["verdict"]["text"], "insufficient (llm=24, uniform=30)")
        self.assertIn("SAMPLER VERDICT: insufficient (llm=24, uniform=30)", text)

    def test_verdict_scores_distinct_projections_not_execution_variants(self):
        def extra(state_dir):
            # Twenty execution-only variants (cap / sigma / pressure) of the
            # first LLM survivor's projection, every one a stage-1 survivor:
            # the public screen reproduces the parent's verdict for each.
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                for variant in range(1, 21):
                    fingerprint = "llm-variant-%d" % variant
                    evidence_path = state_dir / ("evidence/%s/%s.json" % (LANE, fingerprint))
                    loop.atomic_json(
                        evidence_path, {"stage_1_survivor": True, "overall": {"accuracy": 0.9}}
                    )
                    ledger.add_hypothesis(
                        fingerprint,
                        LANE,
                        proposal(0, variant),
                        None,
                        "stage_1_survivor",
                        evidence_path,
                        source="llm",
                    )
            finally:
                ledger.close()

        report, text = self.report({"llm": (25, 5), "uniform_control": (25, 5)}, extra)
        llm = report["lanes"][LANE]["sources"]["llm"]
        verdict = report["lanes"][LANE]["verdict"]
        # Row-level the LLM arm looks like 25/45 against 5/25; the variants are
        # distinct rules but one stage-1 projection, so it ties the control.
        self.assertEqual((llm["proposals"], llm["stage_1_survivors"], llm["distinct_rules"]), (45, 25, 45))
        self.assertEqual(verdict["llm"], {"proposals": 45, "projections": 25, "stage_1_projections": 5})
        self.assertEqual(
            verdict["uniform_control"], {"proposals": 25, "projections": 25, "stage_1_projections": 5}
        )
        self.assertEqual(verdict["text"], "demote LLM to reviewer role")
        self.assertIn(
            "SAMPLER VERDICT: demote LLM to reviewer role  lane=%s llm_s1_projections=5/25 "
            "uniform_s1_projections=5/25" % LANE,
            text,
        )

    def test_verdict_counts_reviewer_rejects_inside_the_llm_arm(self):
        def extra(state_dir):
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                for index, status in enumerate(
                    ("stage_1_survivor", "rejected_stage_1", "rejected_stage_1")
                ):
                    ledger.add_hypothesis(
                        "llm-reviewed-%d" % index,
                        LANE,
                        proposal(66 + index),
                        {"verdict": "reject", "reason": "test"},
                        status,
                        None,
                        source="llm",
                    )
                ledger.add_hypothesis(
                    "llm-accepted",
                    LANE,
                    proposal(69),
                    {"verdict": "accept", "reason": "test"},
                    "rejected_stage_1",
                    None,
                    source="llm",
                )
            finally:
                ledger.close()

        report, text = self.report({"llm": (22, 9), "uniform_control": (25, 5)}, extra)
        verdict = report["lanes"][LANE]["verdict"]
        # Reviewer-rejected samples are still evaluated and stay in the LLM
        # denominator; the subset is reported beside the verdict.
        self.assertEqual(
            verdict["llm"], {"proposals": 26, "projections": 26, "stage_1_projections": 10}
        )
        self.assertEqual(verdict["reviewer_rejected"], {"proposals": 3, "stage_1_survivors": 1})
        self.assertEqual(
            report["lanes"][LANE]["sources"]["llm"]["reviewer_rejected"],
            {"proposals": 3, "stage_1_survivors": 1},
        )
        self.assertIn(
            "SAMPLER VERDICT: LLM beats uniform  lane=%s llm_s1_projections=10/26 "
            "uniform_s1_projections=5/25 reviewer_rejected_s1=1/3" % LANE,
            text,
        )

    def test_legacy_ledger_without_source_column_reads_as_legacy(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            connection = sqlite3.connect(str(state_dir / "research.sqlite3"))
            connection.executescript(
                """
                CREATE TABLE hypotheses (
                    fingerprint TEXT PRIMARY KEY, lane TEXT NOT NULL, created_at TEXT NOT NULL,
                    proposal_json TEXT NOT NULL, review_json TEXT, status TEXT NOT NULL,
                    evidence_path TEXT
                );
                CREATE TABLE cycles (
                    cycle_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
                    status TEXT NOT NULL, details_json TEXT NOT NULL
                );
                """
            )
            connection.execute(
                "INSERT INTO hypotheses VALUES(?, ?, ?, ?, NULL, ?, NULL)",
                ("old", LANE, "2026-07-28T06:18:09+00:00", json.dumps(proposal(0)), "stage_1_survivor"),
            )
            connection.commit()
            connection.close()
            report = kpi.build_report(state_dir)
        legacy = report["lanes"][LANE]["sources"][kpi.LEGACY_SOURCE]
        self.assertEqual((legacy["proposals"], legacy["stage_1_survivors"], legacy["accruing"]), (1, 1, 0))
        self.assertIsNone(legacy["mean_stage_1_accuracy"])
        self.assertEqual(report["lanes"][LANE]["verdict"]["text"], "insufficient (llm=0, uniform=0)")

    def test_missing_state_dir_reports_no_lanes(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            report = kpi.build_report(Path(directory) / "absent")
            text = kpi.render(report)
        self.assertEqual(report["lanes"], {})
        self.assertNotIn("SAMPLER VERDICT", text)


if __name__ == "__main__":
    unittest.main()
