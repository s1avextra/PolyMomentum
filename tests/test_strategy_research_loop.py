from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path
import tempfile
import datetime as dt
import unittest
from unittest import mock
import urllib.error


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "strategy_research_loop", ROOT / "scripts/strategy_research_loop.py"
)
assert SPEC and SPEC.loader
loop = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(loop)


class StrategyResearchLoopTest(unittest.TestCase):
    def valid_proposal(self):
        return {
            "title": "Three minute path plus a material move",
            "rationale": "Both observations exist before the decision.",
            "expected_failure_mode": "The executable ask may consume the advantage.",
            "rule": {
                "operator": "path_and_move",
                "path_minutes": 3,
                "minimum_two_minute_move_usd": 200,
                "maximum_entry_price": 0.90,
                "minimum_decision_buffer_usd": 200,
                "settlement_sigma_buffer": 0.0,
                "minimum_book_pressure": -1.0,
                "direction": "both",
            },
        }

    def test_strict_proposal_accepts_only_frozen_grid(self):
        self.assertEqual(loop.validate_late_proposal(self.valid_proposal()), self.valid_proposal())
        bounded_buffer = self.valid_proposal()
        bounded_buffer["rule"]["minimum_decision_buffer_usd"] = 125
        self.assertEqual(loop.validate_late_proposal(bounded_buffer), bounded_buffer)
        unknown = self.valid_proposal()
        unknown["command"] = "anything"
        with self.assertRaisesRegex(ValueError, "unexpected"):
            loop.validate_late_proposal(unknown)
        out_of_grid = self.valid_proposal()
        out_of_grid["rule"]["minimum_two_minute_move_usd"] = 150
        with self.assertRaisesRegex(ValueError, "frozen grid"):
            loop.validate_late_proposal(out_of_grid)

    def test_payoff_cap_is_fee_derived_and_not_added_to_full_grid(self):
        self.assertEqual(loop.LATE_PAYOFF_DERIVED_ENTRY_CAP, 0.97)
        targeted = [
            proposal
            for proposal in loop.fallback_late_proposals()
            if proposal["rule"]["maximum_entry_price"]
            == loop.LATE_PAYOFF_DERIVED_ENTRY_CAP
        ]
        self.assertEqual(len(targeted), 8)
        self.assertEqual(
            {proposal["rule"]["direction"] for proposal in targeted},
            {"both", "up", "down"},
        )

    def test_rule_semantics_are_not_silently_coerced(self):
        invalid = self.valid_proposal()
        invalid["rule"] = {
            "operator": "path_only",
            "path_minutes": 3,
            "minimum_two_minute_move_usd": 100,
        }
        with self.assertRaisesRegex(ValueError, "path_only"):
            loop.validate_late_proposal(invalid)
        unsupported_or = self.valid_proposal()
        unsupported_or["rule"] = {
            "operator": "path_or_move",
            "path_minutes": 3,
            "minimum_two_minute_move_usd": 200,
        }
        with self.assertRaisesRegex(ValueError, "frozen 4m"):
            loop.validate_late_proposal(unsupported_or)

    def test_survivor_compiles_to_existing_runtime_tags(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            output = Path(directory) / "variant.json"
            artifact = loop.compile_late_variant(
                self.valid_proposal(),
                ROOT
                / "deploy/promotions/evidence/strategy_registry/20260722_late_window_path_3m_move100_exact_l2_variant.json",
                output,
                "a" * 64,
            )
            variant = json.loads(output.read_text())[0]
        self.assertEqual(
            variant["selectivity"],
            {
                "require_tags": {"article_path_3m": "aligned"},
                "require_tag_values": {"article_move_2m": ["aligned_ge_200"]},
            },
        )
        self.assertEqual(variant["zone_config"]["max_price"], 0.90)
        self.assertEqual(variant["zone_config"]["settlement_guard_minutes"], 5.0)
        self.assertEqual(variant["zone_config"]["settlement_min_abs_move_usd"], 200.0)
        self.assertEqual(variant["microstructure"]["min_book_pressure"], -1.0)
        self.assertEqual(len(artifact["sha256"]), 64)

    def test_two_minute_path_and_move_compiles_to_default_off_causal_tags(self):
        proposal = self.valid_proposal()
        proposal["rule"].update(
            {
                "path_minutes": 2,
                "minimum_two_minute_move_usd": 100,
                "maximum_entry_price": 0.85,
                "minimum_decision_buffer_usd": 0,
                "direction": "down",
            }
        )
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            output = Path(directory) / "variant.json"
            loop.compile_late_variant(
                proposal,
                ROOT
                / "deploy/promotions/evidence/strategy_registry/20260722_late_window_path_3m_move100_exact_l2_variant.json",
                output,
                "c" * 64,
            )
            variant = json.loads(output.read_text())[0]
        self.assertEqual(
            variant["selectivity"],
            {
                "require_tags": {
                    "article_path_2m": "aligned",
                    "direction": "down",
                },
                "require_tag_values": {
                    "article_move_2m": ["aligned_100_200", "aligned_ge_200"]
                },
            },
        )

    def test_diagnosed_book_pressure_guard_is_bounded_and_compiled(self):
        proposal = self.valid_proposal()
        proposal["rule"].update(
            {
                "path_minutes": 2,
                "minimum_two_minute_move_usd": 100,
                "maximum_entry_price": 0.95,
                "minimum_decision_buffer_usd": 100,
                "minimum_book_pressure": -0.15,
                "direction": "down",
            }
        )
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            output = Path(directory) / "variant.json"
            artifact = loop.compile_late_variant(
                proposal,
                ROOT
                / "deploy/promotions/evidence/strategy_registry/20260722_late_window_path_3m_move100_exact_l2_variant.json",
                output,
                "d" * 64,
            )
            variant = json.loads(output.read_text())[0]
        self.assertEqual(variant["microstructure"]["min_book_pressure"], -0.15)
        self.assertEqual(
            artifact["execution_filters"]["minimum_book_pressure"], -0.15
        )
        rules = [item["rule"] for item in loop.fallback_late_proposals()]
        self.assertIn(proposal["rule"], rules)

    def test_symmetric_positive_pressure_followup_is_in_bounded_grid(self):
        proposal = self.valid_proposal()
        proposal["rule"].update(
            {
                "path_minutes": 2,
                "minimum_two_minute_move_usd": 100,
                "maximum_entry_price": 0.95,
                "minimum_decision_buffer_usd": 100,
                "minimum_book_pressure": 0.15,
                "direction": "up",
            }
        )
        self.assertEqual(loop.validate_late_proposal(proposal), proposal)
        rules = [item["rule"] for item in loop.fallback_late_proposals()]
        self.assertIn(proposal["rule"], rules)

    def test_density_followup_relaxes_only_one_pressure_bucket(self):
        rules = [item["rule"] for item in loop.fallback_late_proposals()]
        strict = next(
            rule
            for rule in rules
            if rule["path_minutes"] == 2
            and rule["minimum_two_minute_move_usd"] == 100
            and rule["maximum_entry_price"] == 0.95
            and rule["minimum_decision_buffer_usd"] == 100
            and rule["minimum_book_pressure"] == 0.15
            and rule["direction"] == "up"
        )
        relaxed = dict(strict, minimum_book_pressure=-0.15)
        self.assertEqual(rules.index(relaxed), rules.index(strict) + 1)

    def test_path_or_move_payoff_followup_raises_only_one_price_step(self):
        rules = [item["rule"] for item in loop.fallback_late_proposals()]
        followup = {
            "operator": "path_or_move",
            "path_minutes": 4,
            "minimum_two_minute_move_usd": 200,
            "maximum_entry_price": 0.90,
            "minimum_decision_buffer_usd": 200,
            "settlement_sigma_buffer": 0.1,
            "minimum_book_pressure": -1.0,
            "direction": "both",
        }
        self.assertIn(followup, rules)
        final_step = dict(followup, maximum_entry_price=0.95)
        self.assertEqual(rules.index(final_step), rules.index(followup) + 1)

    def test_directional_rule_compiles_to_causal_direction_tag(self):
        proposal = self.valid_proposal()
        proposal["rule"]["direction"] = "down"
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            output = Path(directory) / "variant.json"
            loop.compile_late_variant(
                proposal,
                ROOT
                / "deploy/promotions/evidence/strategy_registry/20260722_late_window_path_3m_move100_exact_l2_variant.json",
                output,
                "b" * 64,
            )
            variant = json.loads(output.read_text())[0]
        self.assertEqual(variant["selectivity"]["require_tags"]["direction"], "down")
        gates = loop.eligibility_gates_for_proposal(
            {"require_both_directions": True}, proposal
        )
        self.assertFalse(gates["require_both_directions"])

    def test_diagnosed_settlement_guard_has_bounded_relaxed_followup(self):
        rules = [proposal["rule"] for proposal in loop.fallback_late_proposals()]
        guarded = rules.index(
            {
                "operator": "path_and_move",
                "path_minutes": 3,
                "minimum_two_minute_move_usd": 100,
                "maximum_entry_price": 0.95,
                "minimum_decision_buffer_usd": 100,
                "settlement_sigma_buffer": 0.1,
                "minimum_book_pressure": -1.0,
                "direction": "down",
            }
        )
        relaxed = rules.index(
            {
                "operator": "path_and_move",
                "path_minutes": 3,
                "minimum_two_minute_move_usd": 100,
                "maximum_entry_price": 0.95,
                "minimum_decision_buffer_usd": 0,
                "settlement_sigma_buffer": 0.0,
                "minimum_book_pressure": -1.0,
                "direction": "down",
            }
        )
        self.assertEqual(relaxed, guarded + 1)

    def test_broad_four_minute_down_path_precedes_unfocused_grid(self):
        rules = [proposal["rule"] for proposal in loop.fallback_late_proposals()]
        broad_path = {
            "operator": "path_only",
            "path_minutes": 4,
            "minimum_two_minute_move_usd": 0,
            "maximum_entry_price": 0.95,
            "minimum_decision_buffer_usd": 0,
            "settlement_sigma_buffer": 0.0,
            "minimum_book_pressure": -1.0,
            "direction": "down",
        }
        unfocused_grid = {
            "operator": "path_and_move",
            "path_minutes": 4,
            "minimum_two_minute_move_usd": 100,
            "maximum_entry_price": 0.90,
            "minimum_decision_buffer_usd": 200,
            "settlement_sigma_buffer": 0.0,
            "minimum_book_pressure": -1.0,
            "direction": "both",
        }
        self.assertLess(rules.index(broad_path), rules.index(unfocused_grid))

    def test_earlier_positive_margin_candidates_precede_unfocused_grid(self):
        rules = [proposal["rule"] for proposal in loop.fallback_late_proposals()]
        earlier_rules = [
            {
                "operator": "move_only",
                "path_minutes": 0,
                "minimum_two_minute_move_usd": 100,
                "maximum_entry_price": 0.85,
                "minimum_decision_buffer_usd": 0,
                "settlement_sigma_buffer": 0.0,
                "minimum_book_pressure": -1.0,
                "direction": "down",
            },
            {
                "operator": "path_only",
                "path_minutes": 3,
                "minimum_two_minute_move_usd": 0,
                "maximum_entry_price": 0.85,
                "minimum_decision_buffer_usd": 0,
                "settlement_sigma_buffer": 0.0,
                "minimum_book_pressure": -1.0,
                "direction": "down",
            },
        ]
        unfocused_index = rules.index(
            {
                "operator": "path_and_move",
                "path_minutes": 4,
                "minimum_two_minute_move_usd": 100,
                "maximum_entry_price": 0.90,
                "minimum_decision_buffer_usd": 200,
                "settlement_sigma_buffer": 0.0,
                "minimum_book_pressure": -1.0,
                "direction": "both",
            }
        )
        self.assertTrue(all(rules.index(rule) < unfocused_index for rule in earlier_rules))

    def test_two_minute_both_direction_followup_precedes_unfocused_grid(self):
        rules = [proposal["rule"] for proposal in loop.fallback_late_proposals()]
        followup = {
            "operator": "path_and_move",
            "path_minutes": 2,
            "minimum_two_minute_move_usd": 100,
            "maximum_entry_price": 0.85,
            "minimum_decision_buffer_usd": 0,
            "settlement_sigma_buffer": 0.0,
            "minimum_book_pressure": -1.0,
            "direction": "both",
        }
        unfocused_grid = {
            "operator": "path_and_move",
            "path_minutes": 4,
            "minimum_two_minute_move_usd": 100,
            "maximum_entry_price": 0.90,
            "minimum_decision_buffer_usd": 200,
            "settlement_sigma_buffer": 0.0,
            "minimum_book_pressure": -1.0,
            "direction": "both",
        }
        self.assertLess(rules.index(followup), rules.index(unfocused_grid))
        guarded_followup = {
            "operator": "path_and_move",
            "path_minutes": 2,
            "minimum_two_minute_move_usd": 100,
            "maximum_entry_price": 0.90,
            "minimum_decision_buffer_usd": 125,
            "settlement_sigma_buffer": 0.0,
            "minimum_book_pressure": -1.0,
            "direction": "down",
        }
        self.assertLess(rules.index(guarded_followup), rules.index(unfocused_grid))

    def test_fresh_research_gate_defers_full_stress_to_confirmation(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        self.assertFalse(
            config["fresh_holdout"]["gates"][
                "require_one_maximum_stake_loss_robustness"
            ]
        )

    def test_public_screen_is_causal_and_group_stable(self):
        windows = []
        for index in range(40):
            direction = 1 if index % 2 == 0 else -1
            base = 70_000.0
            windows.append(
                {
                    "window_start": index,
                    "utc_day": "2026-01-%02d" % (1 + index // 4),
                    "utc_hour": index % 24,
                    "chronological_window": "older" if index < 20 else "fresh",
                    "p0": base,
                    "p60": base + direction * 60,
                    "p120": base + direction * 220,
                    "p180": base + direction * 280,
                    "p240": base + direction * 300,
                    "terminal": base + direction * 320,
                }
            )
        evidence = loop.evaluate_late_rule(
            windows,
            self.valid_proposal(),
            {
                "minimum_signals": 30,
                "minimum_group_signals": 10,
                "minimum_group_accuracy": 0.55,
                "minimum_wilson_lower": 0.50,
            },
        )
        self.assertEqual(evidence["decision_offset_seconds"], 180)
        self.assertEqual(evidence["overall"]["signals"], 40)
        self.assertTrue(evidence["stage_1_survivor"])
        self.assertTrue(evidence["candidate_replay_windows"])
        self.assertIn("executable edge", evidence["does_not_establish"])

    def test_fresh_holdout_selection_does_not_score_terminal_outcome(self):
        proposal = self.valid_proposal()
        windows = []
        for index in range(40):
            direction = 1 if index % 2 == 0 else -1
            windows.append(
                {
                    "window_start": index,
                    "utc_day": "2026-01-%02d" % (1 + index // 4),
                    "utc_hour": index % 24,
                    "chronological_window": "older" if index < 20 else "recent_discovery",
                    "p0": 70_000.0,
                    "p60": 70_000.0 + direction * 100,
                    "p120": 70_000.0 + direction * 220,
                    "p180": 70_000.0 + direction * 280,
                    "p240": 70_000.0 + direction * 300,
                    "terminal": 70_000.0 + direction * 320,
                }
            )
        fresh = dict(windows[-1])
        fresh.update(
            {
                "window_start": 10_000,
                "utc_day": "2026-02-01",
                "chronological_window": "fresh_holdout",
                "terminal": 60_000.0,
            }
        )
        windows.append(fresh)
        evidence = loop.evaluate_late_rule(
            windows,
            proposal,
            {
                "minimum_signals": 30,
                "minimum_group_signals": 10,
                "minimum_group_accuracy": 0.55,
                "minimum_wilson_lower": 0.50,
            },
        )
        self.assertEqual(evidence["overall"]["signals"], 40)
        self.assertEqual(sum(item["public_signals"] for item in evidence["fresh_candidate_windows"]), 1)
        candidate = evidence["fresh_candidate_windows"][0]
        self.assertEqual(candidate["start"][11:13], candidate["end"][11:13])
        self.assertTrue(evidence["fresh_candidate_selection_is_outcome_blind"])
        excluded = loop.evaluate_late_rule(
            windows,
            proposal,
            {
                "minimum_signals": 30,
                "minimum_group_signals": 10,
                "minimum_group_accuracy": 0.55,
                "minimum_wilson_lower": 0.50,
            },
            [candidate["start"]],
        )
        self.assertEqual(excluded["fresh_candidate_windows"], [])
        self.assertEqual(
            excluded["fresh_previously_measured_exclusion"][
                "matching_candidate_window_count"
            ],
            1,
        )
        self.assertTrue(excluded["fresh_candidate_windows_are_globally_unmeasured"])

    def test_replay_window_ranking_is_causally_direction_balanced(self):
        buckets = {
            ("2026-01-01", 0): {
                "public_signals": 10,
                "public_up_signals": 0,
                "public_down_signals": 10,
            },
            ("2026-01-02", 0): {
                "public_signals": 6,
                "public_up_signals": 6,
                "public_down_signals": 0,
            },
            ("2026-01-03", 0): {
                "public_signals": 8,
                "public_up_signals": 4,
                "public_down_signals": 4,
            },
        }
        ranked = loop.rank_replay_buckets(buckets)
        self.assertEqual(ranked[0][0], ("2026-01-02", 0))
        self.assertEqual(ranked[1][0], ("2026-01-01", 0))

    def test_forward_windows_are_post_seal_chronological_and_outcome_blind(self):
        def row(timestamp, hour, terminal):
            return {
                "window_start": timestamp,
                "utc_day": "2026-01-01",
                "utc_hour": hour,
                "chronological_window": "fresh_holdout",
                "p0": 70_000.0,
                "p60": 70_050.0,
                "p120": 70_220.0,
                "p180": 70_280.0,
                "p240": 70_320.0,
                "terminal": terminal,
            }

        seal = "2026-01-01T01:30:00+00:00"
        before = int(loop.dt.datetime(2026, 1, 1, 1, 0, tzinfo=loop.dt.timezone.utc).timestamp())
        later = int(loop.dt.datetime(2026, 1, 1, 3, 0, tzinfo=loop.dt.timezone.utc).timestamp())
        earlier = int(loop.dt.datetime(2026, 1, 1, 2, 0, tzinfo=loop.dt.timezone.utc).timestamp())
        windows = [
            row(before, 1, 80_000.0),
            row(later, 3, 60_000.0),
            row(earlier, 2, 80_000.0),
            row(later + 300, 3, 80_000.0),
        ]
        selected = loop.chronological_forward_windows(
            windows, self.valid_proposal(), seal, []
        )
        self.assertEqual(
            [item["start"] for item in selected],
            ["2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"],
        )
        self.assertEqual(selected[1]["public_signals"], 2)
        self.assertEqual(
            selected,
            loop.chronological_forward_windows(
                [dict(item, terminal=1.0) for item in windows],
                self.valid_proposal(),
                seal,
                [],
            ),
        )

    def test_forward_target_is_derived_from_observed_payoff(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        verdict = {
            "aggregate": {
                "total_pnl": 0.09817,
                "maximum_stake_usd": 5.0,
                "by_direction": {
                    "up": {"wins": 2},
                    "down": {"wins": 3},
                },
            }
        }
        design = loop.fixed_forward_design(verdict, config)
        self.assertEqual(design["payoff_derived_target_fills"], 765)
        self.assertTrue(design["target_is_feasible"])
        self.assertFalse(design["paper_or_live_authorized"])

    def test_cached_fee_aware_screen_rejects_late_or_move_and_keeps_path3(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        late = self.valid_proposal()
        late["rule"].update(
            {
                "operator": "path_or_move",
                "path_minutes": 4,
                "minimum_two_minute_move_usd": 200,
            }
        )
        late_verdict = loop.cached_family_economic_verdict(config, late)
        path = self.valid_proposal()
        path["rule"].update(
            {
                "operator": "path_only",
                "path_minutes": 3,
                "minimum_two_minute_move_usd": 0,
            }
        )
        path_verdict = loop.cached_family_economic_verdict(config, path)
        self.assertFalse(late_verdict["passed"])
        self.assertLess(late_verdict["metrics"]["mean_one_share_payoff"], 0.0)
        self.assertTrue(path_verdict["passed"])
        self.assertLessEqual(path_verdict["metrics"]["loss_recovery_wins"], 50)

    def test_exact_economic_screen_rejects_thin_asymmetric_payoff(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            public = state_dir / "public.json"
            public.write_text(
                json.dumps(
                    {
                        "overall": {
                            "accuracy": 0.9962574850299402,
                            "wilson_95_lower": 0.9912689367840432,
                        }
                    }
                )
            )
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                fingerprint = "thin-payoff"
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    self.valid_proposal(),
                    None,
                    "research_eligible",
                    public,
                )
                payload = {
                    "fresh_holdout_eligibility_policy_version": "test",
                    "fresh_holdout_verdict": {
                        "aggregate": {
                            "fills_success": 5,
                            "total_pnl": 0.09817,
                            "maximum_stake_usd": 5.0,
                            "by_direction": {
                                "up": {"wins": 2},
                                "down": {"wins": 3},
                            },
                        }
                    },
                }
                verdict = loop.exact_fresh_economic_verdict(
                    config, ledger, fingerprint, payload
                )
            finally:
                ledger.close()
        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["metrics"]["loss_recovery_wins"], 255)
        self.assertFalse(verdict["checks"]["public_wilson_above_break_even"])

    def test_historical_exact_cannot_queue_fresh_when_exact_economics_fail(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = str(state_dir)
            variant = state_dir / "variant.json"
            variant.write_text(
                json.dumps({"max_per_market_usd": 5.0, "position_pct": 0.05})
            )
            public = state_dir / "public.json"
            public.write_text(
                json.dumps(
                    {"overall": {"accuracy": 1.0, "wilson_95_lower": 0.99}}
                )
            )
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                fingerprint = "historical-thin-payoff"
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "stage_1_survivor",
                    public,
                )
                payload = {
                    "proposal": proposal,
                    "variant": {"path": str(variant)},
                    "completed_windows": [
                        {
                            "start": "2026-01-01T00:00:00Z",
                            "public_signals": 5,
                            "summary": {
                                "trades": 5,
                                "execution_attempts": 5,
                                "fills_success": 5,
                                "fills_failed": 0,
                                "unresolved_fills": 0,
                                "total_pnl": 0.05,
                                "total_fees": 0.01,
                                "breaker_tripped": False,
                                "by_direction": {
                                    "up": {
                                        "trades": 3,
                                        "wins": 3,
                                        "losses": 0,
                                        "total_pnl": 0.03,
                                    },
                                    "down": {
                                        "trades": 2,
                                        "wins": 2,
                                        "losses": 0,
                                        "total_pnl": 0.02,
                                    },
                                },
                            },
                        }
                    ],
                    "fresh_candidate_windows": [
                        {
                            "start": "2026-01-02T00:00:00Z",
                            "end": "2026-01-02T01:00:00Z",
                        }
                    ],
                }
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "exact_l2_replay",
                    payload,
                    "test",
                    status="queued",
                )
                job = ledger.next_job("exact_l2_replay")
                assert job is not None
                verdict = loop.finalize_historical_exact_job(
                    config, ledger, job, payload
                )
                hypothesis = ledger.hypothesis(fingerprint)
                completed = ledger.jobs("exact_l2_replay", "completed")[0]
                completed_payload = json.loads(completed["payload_json"])
            finally:
                ledger.close()
        self.assertTrue(verdict["eligible"])
        self.assertEqual(hypothesis["status"], "rejected_exact_economics")
        self.assertFalse(completed_payload["historical_economic_verdict"]["passed"])
        self.assertEqual(
            completed_payload["historical_economic_verdict"]["metrics"][
                "loss_recovery_wins"
            ],
            500,
        )

    def test_official_resolution_support_requires_hash_pinned_chainlink(self):
        report = {
            "data_manifest": {
                "sources": [
                    {
                        "name": "btc_settlement_price_tape",
                        "complete": True,
                        "row_count": 3600,
                        "checksum_sha256": "a" * 64,
                        "metadata": {
                            "source_kind": "chainlink_btc_usd_data_stream"
                        },
                    }
                ]
            }
        }
        ready = loop.exact_report_official_resolution_support(
            report, "chainlink_btc_usd_data_stream"
        )
        proxy = json.loads(json.dumps(report))
        proxy["data_manifest"]["sources"][0]["metadata"]["source_kind"] = (
            "binance_btcusdt_klines"
        )
        not_ready = loop.exact_report_official_resolution_support(
            proxy, "chainlink_btc_usd_data_stream"
        )
        self.assertTrue(ready["ready"])
        self.assertFalse(not_ready["ready"])

    def test_bounded_shadow_gate_is_paper_only_and_fail_closed(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        session = {
            "venue": "paper_only",
            "live_order_submissions": 0,
            "duration_seconds": 7200,
            "shadow_resolutions": 24,
            "official_resolution_parity_ready": True,
            "unresolved_positions": 0,
            "breaker_trips": 0,
        }
        self.assertTrue(loop.bounded_shadow_verdict(session, config)["passed"])
        live = dict(session, venue="live")
        self.assertFalse(loop.bounded_shadow_verdict(live, config)["passed"])
        unresolved = dict(session, unresolved_positions=1)
        self.assertFalse(loop.bounded_shadow_verdict(unresolved, config)["passed"])

    def test_completed_economic_rejection_remains_authoritative(self):
        self.assertEqual(
            loop.economic_rejection_status(
                {"source_kind": "fresh_exact_replay"}
            ),
            "rejected_exact_economics",
        )
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            ledger = loop.Ledger(Path(directory) / "research.sqlite3")
            try:
                fingerprint = "economic-rejection"
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    self.valid_proposal(),
                    None,
                    "research_eligible",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "economic_opportunity_screen",
                    {"economic_verdict": {"passed": False}},
                    "rejected",
                    status="queued",
                )
                job = ledger.jobs("economic_opportunity_screen", "queued")[0]
                ledger.update_job(
                    job["job_id"],
                    "completed",
                    json.loads(job["payload_json"]),
                    "rejected",
                )
                reconciled = loop.reconcile_economic_screen_statuses(ledger)
                status = ledger.hypothesis(fingerprint)["status"]
            finally:
                ledger.close()
        self.assertEqual(reconciled, [fingerprint])
        self.assertEqual(status, "rejected_economic_screen")

    def test_blocked_economic_screen_is_not_left_as_stage_1_survivor(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            ledger = loop.Ledger(Path(directory) / "research.sqlite3")
            try:
                fingerprint = "economic-unavailable"
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    self.valid_proposal(),
                    None,
                    "stage_1_survivor",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "economic_opportunity_screen",
                    {},
                    "capture is required",
                    status="blocked",
                )
                reconciled = loop.reconcile_economic_screen_statuses(ledger)
                status = ledger.hypothesis(fingerprint)["status"]
            finally:
                ledger.close()
        self.assertEqual(reconciled, [fingerprint])
        self.assertEqual(status, "economic_screen_unavailable")

    def test_exact_eligibility_requires_support_and_one_maximum_stake_loss(self):
        summary = {
            "trades": 8,
            "execution_attempts": 8,
            "fills_success": 8,
            "fills_failed": 0,
            "unresolved_fills": 0,
            "total_pnl": 32.0,
            "total_fees": 1.0,
            "breaker_tripped": False,
            "by_direction": {
                "up": {"trades": 4, "wins": 4, "losses": 0, "total_pnl": 16.0},
                "down": {"trades": 4, "wins": 4, "losses": 0, "total_pnl": 16.0},
            },
        }
        gates = {
            "minimum_fills": 5,
            "minimum_fill_rate": 0.8,
            "minimum_positive_active_window_fraction": 0.5,
            "minimum_direction_fills": 2,
            "require_both_directions": True,
            "maximum_unresolved_fills": 0,
        }
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            variant = Path(directory) / "variant.json"
            variant.write_text(
                json.dumps([{"max_per_market_usd": 10.0, "position_pct": 0.05}])
            )
            verdict = loop.exact_eligibility([{"summary": summary}], variant, gates)
            weak = dict(summary)
            weak["total_pnl"] = 5.0
            weak_verdict = loop.exact_eligibility([{"summary": weak}], variant, gates)
            one_sided = dict(summary)
            one_sided["by_direction"] = {
                "up": {"trades": 1, "wins": 1, "losses": 0, "total_pnl": 1.0},
                "down": {"trades": 7, "wins": 7, "losses": 0, "total_pnl": 31.0},
            }
            one_sided_verdict = loop.exact_eligibility(
                [{"summary": one_sided}], variant, gates
            )
        self.assertTrue(verdict["eligible"])
        self.assertEqual(verdict["aggregate"]["maximum_stake_usd"], 5.0)
        self.assertEqual(verdict["aggregate"]["pnl_after_one_maximum_stake_loss"], 27.0)
        self.assertFalse(
            weak_verdict["gates"][
                "required_one_maximum_stake_loss_robustness"
            ]
        )
        self.assertFalse(one_sided_verdict["gates"]["direction_robustness"])

    def test_historical_screen_can_defer_full_stake_loss_stress_to_holdout(self):
        summary = {
            "trades": 6,
            "execution_attempts": 6,
            "fills_success": 6,
            "fills_failed": 0,
            "unresolved_fills": 0,
            "total_pnl": 2.0,
            "total_fees": 0.2,
            "breaker_tripped": False,
            "by_direction": {
                "down": {"trades": 6, "wins": 6, "losses": 0, "total_pnl": 2.0}
            },
        }
        gates = {
            "minimum_fills": 5,
            "minimum_fill_rate": 0.8,
            "minimum_positive_active_window_fraction": 0.5,
            "minimum_direction_fills": 2,
            "require_both_directions": False,
            "require_one_maximum_stake_loss_robustness": False,
            "maximum_unresolved_fills": 0,
        }
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            variant = Path(directory) / "variant.json"
            variant.write_text(
                json.dumps([{"max_per_market_usd": 10.0, "position_pct": 0.05}])
            )
            verdict = loop.exact_eligibility([{"summary": summary}], variant, gates)
        self.assertTrue(verdict["eligible"])
        self.assertEqual(verdict["classification"], "research_eligible")
        self.assertFalse(
            verdict["observations"]["raw_one_maximum_stake_loss_robustness"]
        )
        self.assertFalse(verdict["policy"]["require_one_maximum_stake_loss_robustness"])

    def test_exact_eligibility_reports_signal_and_window_coverage(self):
        windows = [
            {
                "public_signals": 8,
                "summary": {
                    "trades": 2,
                    "execution_attempts": 2,
                    "fills_success": 2,
                    "total_pnl": 1.0,
                    "by_direction": {
                        "up": {"trades": 2, "total_pnl": 1.0}
                    },
                },
            },
            {
                "public_signals": 4,
                "summary": {
                    "trades": 0,
                    "execution_attempts": 0,
                    "fills_success": 0,
                    "total_pnl": 0.0,
                    "by_direction": {},
                },
            },
        ]
        gates = {
            "minimum_fills": 1,
            "minimum_fill_rate": 0.8,
            "minimum_signal_to_attempt_rate": 0.25,
            "enforce_minimum_signal_to_attempt_rate": False,
            "minimum_active_window_fraction": 0.5,
            "minimum_positive_active_window_fraction": 0.5,
            "minimum_direction_fills": 1,
            "require_both_directions": False,
            "require_one_maximum_stake_loss_robustness": False,
            "maximum_unresolved_fills": 0,
        }
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            variant = Path(directory) / "variant.json"
            variant.write_text(
                json.dumps([{"max_per_market_usd": 5.0, "position_pct": 0.05}])
            )
            verdict = loop.exact_eligibility(windows, variant, gates)
        self.assertEqual(verdict["aggregate"]["public_signals"], 12)
        self.assertAlmostEqual(verdict["aggregate"]["signal_to_attempt_rate"], 1 / 6)
        self.assertEqual(verdict["aggregate"]["active_window_fraction"], 0.5)
        self.assertFalse(verdict["gates"]["minimum_signal_to_attempt_rate"])
        self.assertTrue(verdict["eligible"])
        self.assertFalse(
            verdict["policy"]["enforce_minimum_signal_to_attempt_rate"]
        )

    def test_fresh_queue_prioritizes_historical_stake_coverage(self):
        weaker = {
            "payload_json": json.dumps(
                {
                    "historical_verdict": {
                        "aggregate": {
                            "maximum_stake_usd": 5.0,
                            "total_pnl": 2.8,
                            "fills_success": 5,
                        }
                    }
                }
            )
        }
        stronger = {
            "payload_json": json.dumps(
                {
                    "historical_verdict": {
                        "aggregate": {
                            "maximum_stake_usd": 5.0,
                            "total_pnl": 4.4,
                            "fills_success": 13,
                        }
                    }
                }
            )
        }
        self.assertIs(
            max([weaker, stronger], key=loop.fresh_holdout_priority), stronger
        )

    def test_exact_queue_prioritizes_clean_minimum_fill_shortfall(self):
        untouched = {"payload_json": json.dumps({"completed_windows": []})}
        nearly_supported = {
            "payload_json": json.dumps(
                {
                    "historical_verdict": {
                        "aggregate": {
                            "maximum_stake_usd": 5.0,
                            "total_pnl": 2.0,
                            "fills_success": 4,
                        }
                    }
                }
            )
        }
        self.assertIs(
            max([untouched, nearly_supported], key=loop.historical_exact_priority),
            nearly_supported,
        )

    def test_reconcile_extends_only_clean_historical_support_shortfall(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        self.assertEqual(config["exact_replay"]["maximum_windows_per_hypothesis"], 8)
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            ledger = loop.Ledger(Path(directory) / "research.sqlite3")
            try:
                fingerprint = "historical-cap-extension"
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "historical_insufficient_support",
                    None,
                )
                replay_windows = [
                    {
                        "start": "2026-05-%02dT08:00:00Z" % day,
                        "end": "2026-05-%02dT08:00:00Z" % day,
                        "public_signals": 1,
                    }
                    for day in range(1, 7)
                ]
                payload = {
                    "proposal": proposal,
                    "variant": {"path": "unused.json"},
                    "candidate_replay_windows": replay_windows,
                    "frozen_candidate_replay_windows": replay_windows,
                    "completed_windows": replay_windows[:4],
                    "historical_eligibility_policy_version": loop.EXACT_ELIGIBILITY_POLICY_VERSION,
                    "historical_verdict": {
                        "classification": "insufficient_support",
                        "aggregate": {
                            "fills_success": 4,
                            "total_pnl": 2.0,
                            "maximum_stake_usd": 5.0,
                        },
                        "gates": {
                            "minimum_fills": False,
                            "minimum_fill_rate": True,
                            "positive_total_net_pnl": True,
                            "positive_mean_net_pnl": True,
                            "minimum_positive_active_window_fraction": True,
                            "direction_robustness": True,
                            "maximum_unresolved_fills": True,
                            "breaker_not_tripped": True,
                            "one_maximum_stake_loss_robustness": True,
                        },
                    },
                }
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "exact_l2_replay",
                    payload,
                    "old cap complete",
                    status="queued",
                )
                job = ledger.jobs("exact_l2_replay", "queued")[0]
                ledger.update_job(job["job_id"], "completed", payload, "old cap complete")
                screen = {
                    "gates": {"fresh_support_capacity": True},
                    "fresh_candidate_windows": replay_windows[-2:],
                    "fresh_capacity_signals": 6,
                    "fresh_previously_measured_exclusion": {
                        "policy": "global_measured_window_reserve_v1"
                    },
                }
                with mock.patch.object(loop, "load_public_windows", return_value=[]), mock.patch.object(
                    loop, "evaluate_late_rule", return_value=screen
                ):
                    reconciled = loop.reconcile_completed_exact_jobs(
                        config, ledger, Path(directory) / "snapshot.jsonl.gz"
                    )
                queued = ledger.jobs("exact_l2_replay", "queued")
                hypothesis = ledger.connection.execute(
                    "SELECT status FROM hypotheses WHERE fingerprint = ?", (fingerprint,)
                ).fetchone()
                queued_payload = json.loads(queued[0]["payload_json"])
            finally:
                ledger.close()
        self.assertEqual(len(reconciled), 1)
        self.assertEqual(reconciled[0]["reconciliation"], "historical_window_cap_expansion")
        self.assertEqual(queued_payload["historical_maximum_windows"], 8)
        self.assertEqual(len(queued_payload["superseded_historical_verdicts"]), 1)
        self.assertEqual(hypothesis["status"], "stage_1_survivor")

    def test_ledger_deduplicates_hypotheses_and_queue_stages(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            ledger = loop.Ledger(Path(directory) / "research.sqlite3")
            try:
                proposal = self.valid_proposal()
                fingerprint = loop.stable_hash(proposal)
                ledger.add_hypothesis(
                    fingerprint, "late_window_mechanisms", proposal, None, "stage_1_survivor", None
                )
                self.assertTrue(ledger.has_hypothesis(fingerprint))
                self.assertTrue(
                    ledger.enqueue(
                        "late_window_mechanisms",
                        fingerprint,
                        "economic_opportunity_screen",
                        {"source": "test"},
                        "waiting for quotes",
                    )
                )
                self.assertFalse(
                    ledger.enqueue(
                        "late_window_mechanisms",
                        fingerprint,
                        "economic_opportunity_screen",
                        {"source": "test"},
                        "waiting for quotes",
                    )
                )
                with self.assertRaisesRegex(ValueError, "unsafe queue stage"):
                    ledger.enqueue(
                        "late_window_mechanisms", fingerprint, "live", {}, "must fail"
                    )
                fresh_fingerprint = "f" * 64
                ledger.enqueue(
                    "late_window_mechanisms",
                    fresh_fingerprint,
                    "fresh_resolved_holdout",
                    {
                        "completed_windows": [{"start": "2026-01-01T00:00:00Z"}],
                        "support_only_windows": [{"start": "2026-01-02T00:00:00Z"}],
                        "superseded_fresh_holdout_windows": [
                            {
                                "windows": [
                                    {"start": "2026-01-03T00:00:00Z"}
                                ]
                            }
                        ],
                    },
                    "measured reserve",
                    status="queued",
                )
                self.assertEqual(
                    ledger.measured_fresh_window_starts(),
                    {
                        "2026-01-01T00:00:00Z",
                        "2026-01-02T00:00:00Z",
                        "2026-01-03T00:00:00Z",
                    },
                )
                fresh_job = ledger.jobs("fresh_resolved_holdout", "queued")[0]
                self.assertEqual(
                    ledger.measured_fresh_window_starts(fresh_job["job_id"]), set()
                )
            finally:
                ledger.close()

    def test_global_reserve_repair_removes_legacy_overwrite_windows(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            ledger = loop.Ledger(Path(directory) / "research.sqlite3")
            try:
                contaminated = {
                    "start": "2026-07-01T00:00:00Z",
                    "end": "2026-07-01T00:00:00Z",
                    "public_signals": 1,
                }
                clean_measured = {
                    "start": "2026-07-02T00:00:00Z",
                    "end": "2026-07-02T00:00:00Z",
                    "public_signals": 1,
                }
                clean_unmeasured = {
                    "start": "2026-07-03T00:00:00Z",
                    "end": "2026-07-03T00:00:00Z",
                    "public_signals": 1,
                }
                ledger.enqueue(
                    "late_window_mechanisms",
                    "other-job",
                    "fresh_resolved_holdout",
                    {"completed_windows": [contaminated]},
                    "other measurement",
                    status="queued",
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    "legacy-job",
                    "fresh_resolved_holdout",
                    {
                        "proposal": self.valid_proposal(),
                        "variant": {"path": "unused.json"},
                        "candidate_replay_windows": [contaminated, clean_measured],
                        "completed_windows": [contaminated, clean_measured],
                        "support_only_windows": [],
                    },
                    "legacy overwritten reserve",
                    status="queued",
                )
                legacy = next(
                    job
                    for job in ledger.jobs("fresh_resolved_holdout", "queued")
                    if job["hypothesis_fingerprint"] == "legacy-job"
                )
                screen = {
                    "fresh_candidate_windows": [clean_measured, clean_unmeasured],
                    "fresh_previously_measured_exclusion": {
                        "policy": "global_measured_window_reserve_v1",
                        "input_window_count": 1,
                    },
                }
                with mock.patch.object(loop, "load_public_windows", return_value=[]), mock.patch.object(
                    loop, "evaluate_late_rule", return_value=screen
                ):
                    reconciled = loop.reconcile_fresh_holdout_global_reserve(
                        config, ledger, Path(directory) / "snapshot.jsonl.gz"
                    )
                payload = json.loads(
                    next(
                        job
                        for job in ledger.jobs("fresh_resolved_holdout", "queued")
                        if job["job_id"] == legacy["job_id"]
                    )["payload_json"]
                )
            finally:
                ledger.close()
        repaired = next(item for item in reconciled if item["job_id"] == legacy["job_id"])
        self.assertEqual(repaired["removed_legacy_windows"], 1)
        self.assertEqual(payload["candidate_replay_windows"], [clean_measured, clean_unmeasured])
        self.assertEqual(payload["completed_windows"], [clean_measured])
        self.assertEqual(payload["global_reserve_version"], loop.FRESH_GLOBAL_RESERVE_VERSION)

    def test_exact_worker_dry_run_leases_one_preregistered_window(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["architecture_migration"]["legacy_exact_replay_enabled"] = True
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                fingerprint = "b" * 64
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    self.valid_proposal(),
                    None,
                    "stage_1_survivor",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "exact_l2_replay",
                    {
                        "candidate_replay_windows": [
                            {
                                "start": "2026-05-04T08:00:00Z",
                                "end": "2026-05-04T15:00:00Z",
                                "public_signals": 5,
                            }
                        ],
                        "completed_windows": [],
                        "variant": {"path": "unused-in-dry-run.json"},
                    },
                    "queued",
                    status="queued",
                )
                result = loop.run_queued_exact_job(config, ledger, state_dir, True)
            finally:
                ledger.close()
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["window"]["public_signals"], 5)

    def test_exact_worker_uses_reserve_after_unobservable_window(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["architecture_migration"]["legacy_exact_replay_enabled"] = True
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                fingerprint = "historical-support-reserve"
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "stage_1_survivor",
                    None,
                )
                windows = [
                    {
                        "start": "2026-06-24T13:00:00Z",
                        "end": "2026-06-24T13:00:00Z",
                        "public_signals": 7,
                    },
                    {
                        "start": "2026-06-25T14:00:00Z",
                        "end": "2026-06-25T14:00:00Z",
                        "public_signals": 6,
                    },
                ]
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "exact_l2_replay",
                    {
                        "proposal": proposal,
                        "candidate_replay_windows": windows,
                        "completed_windows": [],
                        "variant": {"path": "unused.json"},
                    },
                    "queued",
                    status="queued",
                )
                unsupported = {
                    "status": "completed_window",
                    "job_id": "ignored",
                    **windows[0],
                    "data_support": {
                        "observable": False,
                        "pmxt_complete": False,
                        "pmxt_row_count": 0,
                        "reason": "pmxt_target_events_unavailable",
                    },
                    "summary": {},
                }
                with mock.patch.object(
                    loop, "known_unobservable_pmxt_window", return_value=None
                ), mock.patch.object(
                    loop, "execute_replay_window", return_value=unsupported
                ):
                    result = loop.run_queued_exact_job(config, ledger, state_dir, False)
                queued = ledger.jobs("exact_l2_replay", "queued")
                payload = json.loads(queued[0]["payload_json"])
            finally:
                ledger.close()
        self.assertEqual(result["status"], "support_only_window")
        self.assertEqual(payload["completed_windows"], [])
        self.assertEqual(len(payload["support_only_windows"]), 1)
        self.assertEqual(payload["frozen_candidate_replay_windows"], windows)

    def test_exact_worker_stops_when_minimum_fills_are_unreachable(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["architecture_migration"]["legacy_exact_replay_enabled"] = True
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = str(state_dir)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                fingerprint = "historical-support-impossible"
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "stage_1_survivor",
                    None,
                )
                windows = [
                    {
                        "start": "2026-05-%02dT00:00:00Z" % day,
                        "end": "2026-05-%02dT00:00:00Z" % day,
                        "public_signals": 1,
                    }
                    for day in range(1, 8)
                ]
                windows.append(
                    {
                        "start": "2026-05-08T00:00:00Z",
                        "end": "2026-05-08T00:00:00Z",
                        "public_signals": 3,
                    }
                )
                completed = [dict(window, summary={}) for window in windows[:7]]
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "exact_l2_replay",
                    {
                        "proposal": proposal,
                        "candidate_replay_windows": windows,
                        "completed_windows": completed,
                        "variant": {
                            "path": str(
                                ROOT
                                / "deploy/promotions/evidence/strategy_registry/20260722_late_window_path_3m_move100_exact_l2_variant.json"
                            )
                        },
                    },
                    "queued",
                    status="queued",
                )
                result = loop.run_queued_exact_job(config, ledger, state_dir, False)
                stored = next(
                    row
                    for row in ledger.jobs("exact_l2_replay")
                    if row["hypothesis_fingerprint"] == fingerprint
                )
                payload = json.loads(stored["payload_json"])
            finally:
                ledger.close()
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["maximum_possible_fills"], 3)
        self.assertEqual(stored["status"], "completed")
        self.assertEqual(payload["historical_early_stop_reason"], "minimum_fills_unreachable")

    def test_command_allowlist_cannot_trade_or_materialize(self):
        with self.assertRaisesRegex(ValueError, "allowlist"):
            loop.run_command(
                ["engine", "strategy-builder", "materialize-policy-variant"], 1, True
            )
        allowed = loop.run_command(
            ["engine", "strategy-builder", "registry-audit"], 1, True
        )
        self.assertEqual(allowed["status"], "dry_run")
        policy_search = loop.run_command(
            ["engine", "strategy-builder", "opportunity-policy-search"], 1, True
        )
        self.assertEqual(policy_search["status"], "dry_run")
        probability_search = loop.run_command(
            ["engine", "strategy-builder", "opportunity-probability-search"], 1, True
        )
        self.assertEqual(probability_search["status"], "dry_run")
        probability_decision = loop.run_command(
            ["engine", "strategy-builder", "opportunity-probability-decision"],
            1,
            True,
        )
        self.assertEqual(probability_decision["status"], "dry_run")
        pair_features = loop.run_command(
            ["engine", "strategy-builder", "opportunity-pair-features"], 1, True
        )
        self.assertEqual(pair_features["status"], "dry_run")
        flow_features = loop.run_command(
            ["engine", "strategy-builder", "opportunity-flow-features"], 1, True
        )
        self.assertEqual(flow_features["status"], "dry_run")
        flow_search = loop.run_command(
            ["engine", "strategy-builder", "opportunity-flow-search"], 1, True
        )
        self.assertEqual(flow_search["status"], "dry_run")
        flow_decision = loop.run_command(
            ["engine", "strategy-builder", "opportunity-flow-decision"], 1, True
        )
        self.assertEqual(flow_decision["status"], "dry_run")
        liquidity_search = loop.run_command(
            ["engine", "strategy-builder", "opportunity-liquidity-search"], 1, True
        )
        self.assertEqual(liquidity_search["status"], "dry_run")
        liquidity_decision = loop.run_command(
            ["engine", "strategy-builder", "opportunity-liquidity-decision"],
            1,
            True,
        )
        self.assertEqual(liquidity_decision["status"], "dry_run")
        exact_replay = loop.run_command(
            ["engine", "strategy-builder", "opportunity-exact-replay"], 1, True
        )
        self.assertEqual(exact_replay["status"], "dry_run")

    def test_opportunity_policy_search_is_content_addressed_and_idempotent(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        calls = []

        def fake_run(command, timeout, dry_run):
            calls.append(list(command))
            output = Path(command[command.index("--output") + 1])
            loop.atomic_json(
                output,
                {
                    "verdict": "no_candidate_survived_discovery",
                    "policies_evaluated": 7290,
                    "eligible_policy_count": 0,
                    "fresh_holdout_outcomes_accessed": False,
                    "exact_replay_plan": {"unique_replay_count": 0},
                },
            )
            return {"status": "completed", "returncode": 0}

        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                with mock.patch.object(loop, "run_command", side_effect=fake_run):
                    first = loop.run_opportunity_policy_search(
                        config, ledger, state_dir, False
                    )
                    second = loop.run_opportunity_policy_search(
                        config, ledger, state_dir, False
                    )
            finally:
                ledger.close()
        self.assertEqual(first["status"], "completed")
        self.assertFalse(first["fresh_holdout_outcomes_accessed"])
        self.assertEqual(second["status"], "unchanged_inputs")
        self.assertEqual(len(calls), 1)

    def test_opportunity_exact_replay_is_bounded_and_idempotent(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        calls = []

        def fake_run(command, timeout, dry_run):
            calls.append(list(command))
            output = Path(command[command.index("--output") + 1])
            loop.atomic_json(
                output,
                {
                    "verdict": "research_signal_retained_more_evidence_required",
                    "source_pmxt_scans": 4,
                    "duplicate_hour_scans_avoided": 4,
                    "fresh_holdout_outcomes_accessed": False,
                },
            )
            return {"status": "completed", "returncode": 0}

        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            cache_dir = state_dir / "pmxt"
            cache_dir.mkdir()
            config["architecture_migration"]["opportunity_policy_search"][
                "pmxt_cache_dir"
            ] = str(cache_dir)
            policy_report = state_dir / "policy.json"
            loop.atomic_json(
                policy_report,
                {"exact_replay_plan": {"unique_replay_count": 2}},
            )
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                with mock.patch.object(loop, "run_command", side_effect=fake_run):
                    first = loop.run_opportunity_exact_replay(
                        config, ledger, state_dir, policy_report, False
                    )
                    second = loop.run_opportunity_exact_replay(
                        config, ledger, state_dir, policy_report, False
                    )
            finally:
                ledger.close()
        self.assertEqual(first["status"], "completed")
        self.assertFalse(first["fresh_holdout_outcomes_accessed"])
        self.assertEqual(second["status"], "unchanged_inputs")
        self.assertEqual(len(calls), 1)
        self.assertIn("opportunity-exact-replay", calls[0])

    def test_default_cycle_routes_to_opportunity_search_without_public_refresh(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            with mock.patch.object(
                loop, "resource_status", return_value={"passed": True, "checks": {}}
            ), mock.patch.object(
                loop, "run_registry_audit", return_value={"status": "completed"}
            ), mock.patch.object(loop, "refresh_public_snapshot") as refresh, mock.patch.object(
                loop,
                "run_opportunity_policy_search",
                return_value={"status": "unchanged_inputs"},
            ) as search:
                result = loop.run_cycle(config, False, None)
        refresh.assert_not_called()
        search.assert_called_once()
        self.assertEqual(result["lane"], "opportunity_policy_search")
        self.assertEqual(result["lane_result"]["status"], "unchanged_inputs")

    def test_exact_window_keeps_shared_sidecar_cache(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        captured = {}

        def fake_run(command, timeout, dry_run):
            captured["command"] = command
            return {"status": "failed", "returncode": 1}

        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            variant = state_dir / "variant.json"
            variant.write_text("[]")
            with mock.patch.object(
                loop,
                "build_verified_btc_tape_for_window",
                return_value={"tape": str(state_dir / "btc.csv")},
            ), mock.patch.object(loop, "run_command", side_effect=fake_run):
                loop.execute_replay_window(
                    config,
                    {"hypothesis_fingerprint": "candidate", "job_id": "job"},
                    {"variant": {"path": str(variant)}},
                    state_dir,
                    {
                        "start": "2026-06-04T00:00:00Z",
                        "end": "2026-06-04T00:00:00Z",
                        "public_signals": 1,
                    },
                    "exact_l2",
                )
        command = captured["command"]
        self.assertNotIn("--delete-after-process", command)
        self.assertEqual(command[command.index("--fold-hours") + 1], "1")
        cache_root = Path(command[command.index("--cache-root") + 1])
        self.assertEqual(cache_root, state_dir / "cache/pmxt/windows/20260604_00")

    def test_exact_window_defers_transient_pmxt_archive_failure(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        failures = (
            "sweep failed: download PMXT v2 hour 2026-07-23 after 4 attempts",
            "preflight PMXT hour 2026-06-02T07:00:00+00:00: send HEAD: "
            "error sending request for url (https://r2v2.pmxt.dev/hour.parquet): "
            "client error (Connect): Connection reset by peer",
        )
        for stderr_tail in failures:
            with self.subTest(stderr_tail=stderr_tail), tempfile.TemporaryDirectory(
                dir=str(ROOT / "logs")
            ) as directory:
                state_dir = Path(directory)
                variant = state_dir / "variant.json"
                variant.write_text("[]")
                with mock.patch.object(
                    loop,
                    "build_verified_btc_tape_for_window",
                    return_value={"tape": str(state_dir / "btc.csv")},
                ), mock.patch.object(
                    loop,
                    "run_command",
                    return_value={
                        "status": "failed",
                        "returncode": 2,
                        "stderr_tail": stderr_tail,
                    },
                ):
                    result = loop.execute_replay_window(
                        config,
                        {"hypothesis_fingerprint": "candidate", "job_id": "job"},
                        {"variant": {"path": str(variant)}},
                        state_dir,
                        {
                            "start": "2026-07-23T14:00:00Z",
                            "end": "2026-07-23T14:00:00Z",
                            "public_signals": 2,
                        },
                        "fresh_holdout",
                    )
                self.assertEqual(result["status"], "deferred")
                self.assertEqual(result["reason"], "pmxt_archive_unavailable")

    def test_public_snapshot_is_physically_split_into_causal_and_label_views(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            root = Path(directory)
            source = root / "combined.jsonl.gz"
            causal = root / "causal.jsonl.gz"
            labels = root / "labels.jsonl.gz"
            row = {
                "window_start": 1785585600,
                "utc_day": "2026-08-01",
                "utc_hour": 12,
                "chronological_window": "fresh_holdout",
                "p0": 100.0,
                "p60": 101.0,
                "p120": 102.0,
                "p180": 103.0,
                "p240": 104.0,
                "terminal": 105.0,
            }
            with loop.gzip.open(str(source), "wt", encoding="utf-8") as handle:
                handle.write(loop.canonical_json(row) + "\n")
            first = loop.split_public_snapshot_views(source, causal, labels)
            first_hashes = (first["causal_sha256"], first["label_sha256"])
            second = loop.split_public_snapshot_views(source, causal, labels)
            with loop.gzip.open(str(causal), "rt", encoding="utf-8") as handle:
                causal_row = json.loads(next(handle))
            with loop.gzip.open(str(labels), "rt", encoding="utf-8") as handle:
                label_row = json.loads(next(handle))
        self.assertEqual(first_hashes, (second["causal_sha256"], second["label_sha256"]))
        self.assertEqual(set(causal_row), set(loop.CAUSAL_PUBLIC_WINDOW_FIELDS))
        self.assertNotIn("terminal", causal_row)
        self.assertEqual(label_row, {"window_start": 1785585600, "terminal": 105.0})

    def test_cycle_finishes_queued_replay_before_proposing_another_hypothesis(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = str(state_dir)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    "queued-candidate",
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "eligible_for_fresh_holdout",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    "queued-candidate",
                    "fresh_resolved_holdout",
                    {"proposal": proposal},
                    "queued holdout",
                    status="queued",
                )
            finally:
                ledger.close()
            seed = ROOT / config["lanes"]["late_window_mechanisms"]["public_snapshot"]
            with mock.patch.object(
                loop,
                "resource_status",
                return_value={"passed": True, "checks": {}},
            ), mock.patch.object(
                loop, "run_registry_audit", return_value={"status": "completed"}
            ), mock.patch.object(
                loop,
                "refresh_public_snapshot",
                return_value={
                    "status": "fallback_seed",
                    "path": str(seed),
                    "sha256": loop.sha256_file(seed),
                },
            ), mock.patch.object(loop, "run_late_window_lane") as lane, mock.patch.object(
                loop,
                "run_queued_fresh_holdout_job",
                return_value={"status": "processed"},
            ) as fresh:
                result = loop.run_cycle(config, False, "late_window_mechanisms")
        lane.assert_not_called()
        fresh.assert_called_once()
        self.assertEqual(result["lane_result"]["status"], "queued_replay_priority")

    def test_legacy_holdout_job_is_blocked_instead_of_starving_queue(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                fingerprint = "legacy-baseline"
                ledger.add_hypothesis(
                    fingerprint,
                    "baseline_evolution",
                    {"kind": "deterministic_evolution"},
                    None,
                    "historic_screen_complete",
                    None,
                )
                ledger.enqueue(
                    "baseline_evolution",
                    fingerprint,
                    "fresh_resolved_holdout",
                    {"source_summary": "historic.json"},
                    "legacy job",
                    status="queued",
                )
                result = loop.run_queued_fresh_holdout_job(
                    config, ledger, state_dir, False
                )
                jobs = ledger.jobs("fresh_resolved_holdout", "blocked")
            finally:
                ledger.close()
        self.assertEqual(result["reason"], "legacy_unexecutable_job")
        self.assertEqual(len(jobs), 1)

    def test_holdout_skips_replay_when_fill_floor_is_impossible(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = str(state_dir)
            variant = state_dir / "variant.json"
            variant.write_text(json.dumps([{"max_per_market_usd": 10.0}]))
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                fingerprint = "capacity-limited"
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "eligible_for_fresh_holdout",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "fresh_resolved_holdout",
                    {
                        "proposal": proposal,
                        "variant": {"path": str(variant)},
                        "candidate_replay_windows": [
                            {
                                "start": "2026-07-14T00:00:00Z",
                                "end": "2026-07-14T07:00:00Z",
                                "public_signals": 2,
                            }
                        ],
                        "completed_windows": [],
                        "maximum_windows": 8,
                    },
                    "frozen holdout",
                    status="queued",
                )
                with mock.patch.object(loop, "execute_replay_window") as execute:
                    result = loop.run_queued_fresh_holdout_job(
                        config, ledger, state_dir, False
                    )
                jobs = ledger.jobs("fresh_resolved_holdout", "completed")
            finally:
                ledger.close()
        execute.assert_not_called()
        self.assertEqual(result["status"], "insufficient_support")
        self.assertEqual(len(jobs), 1)

    def test_holdout_skips_temporarily_unavailable_window_inside_frozen_set(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            variant = state_dir / "variant.json"
            variant.write_text(
                json.dumps([{"max_per_market_usd": 10.0, "position_pct": 0.05}])
            )
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                fingerprint = "deferred-window"
                proposal = self.valid_proposal()
                windows = [
                    {
                        "start": "2026-07-%02dT00:00:00Z" % day,
                        "end": "2026-07-%02dT07:00:00Z" % day,
                        "public_signals": 1,
                    }
                    for day in range(1, 10)
                ]
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "eligible_for_fresh_holdout",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "fresh_resolved_holdout",
                    {
                        "proposal": proposal,
                        "variant": {"path": str(variant)},
                        "candidate_replay_windows": windows,
                        "completed_windows": [],
                        "maximum_windows": 8,
                    },
                    "frozen holdout",
                    status="queued",
                )
                with mock.patch.object(
                    loop,
                    "execute_replay_window",
                    return_value={"status": "deferred", "reason": "btc_tape_unavailable"},
                ):
                    deferred = loop.run_queued_fresh_holdout_job(
                        config, ledger, state_dir, False
                    )
                next_window = loop.run_queued_fresh_holdout_job(
                    config, ledger, state_dir, True
                )
                payload = json.loads(
                    ledger.jobs("fresh_resolved_holdout", "queued")[0]["payload_json"]
                )
            finally:
                ledger.close()
        self.assertEqual(deferred["deferred_window"], windows[0]["start"])
        self.assertEqual(next_window["window"]["start"], windows[1]["start"])
        self.assertEqual(payload["frozen_candidate_replay_windows"], windows)

    def test_holdout_uses_frozen_reserve_for_unobservable_pmxt_window(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            variant = state_dir / "variant.json"
            variant.write_text(
                json.dumps([{"max_per_market_usd": 10.0, "position_pct": 0.05}])
            )
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            windows = [
                {
                    "start": "2026-07-%02dT00:00:00Z" % day,
                    "end": "2026-07-%02dT07:00:00Z" % day,
                    "public_signals": 1,
                }
                for day in range(1, 10)
            ]
            try:
                fingerprint = "support-reserve"
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "eligible_for_fresh_holdout",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "fresh_resolved_holdout",
                    {
                        "proposal": proposal,
                        "variant": {"path": str(variant)},
                        "candidate_replay_windows": windows,
                        "completed_windows": [],
                        "maximum_windows": 8,
                    },
                    "frozen holdout",
                    status="queued",
                )
                unavailable = {
                    "status": "completed_window",
                    "job_id": "ignored",
                    "start": windows[0]["start"],
                    "end": windows[0]["end"],
                    "public_signals": 1,
                    "data_support": {
                        "observable": False,
                        "pmxt_complete": False,
                        "pmxt_row_count": 0,
                        "reason": "pmxt_target_events_unavailable",
                    },
                    "summary": {},
                }
                with mock.patch.object(
                    loop, "execute_replay_window", return_value=unavailable
                ):
                    first = loop.run_queued_fresh_holdout_job(
                        config, ledger, state_dir, False
                    )
                second = loop.run_queued_fresh_holdout_job(
                    config, ledger, state_dir, True
                )
                payload = json.loads(
                    ledger.jobs("fresh_resolved_holdout", "queued")[0]["payload_json"]
                )
                known_unobservable = loop.known_unobservable_pmxt_window(
                    ledger, windows[0]
                )
            finally:
                ledger.close()
        self.assertEqual(first["status"], "support_only_window")
        self.assertEqual(second["window"]["start"], windows[1]["start"])
        self.assertEqual(payload["completed_windows"], [])
        self.assertEqual(len(payload["support_only_windows"]), 1)
        self.assertEqual(payload["frozen_candidate_replay_windows"], windows)
        self.assertTrue(known_unobservable["reused"])
        self.assertEqual(known_unobservable["pmxt_row_count"], 0)

    def test_completed_holdout_is_recomputed_in_place_after_policy_change(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["exact_replay"]["policy_reconciliation_fingerprints"] = [
            "old-fresh-policy"
        ]
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            variant = state_dir / "variant.json"
            variant.write_text(
                json.dumps([{"max_per_market_usd": 5.0, "position_pct": 0.05}])
            )
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                fingerprint = "old-fresh-policy"
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "holdout_insufficient_support",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "fresh_resolved_holdout",
                    {
                        "proposal": proposal,
                        "variant": {"path": str(variant), "sha256": loop.sha256_file(variant)},
                        "candidate_replay_windows": [],
                        "completed_windows": [],
                        "fresh_holdout_verdict": {"eligible": False},
                        "fresh_holdout_eligibility_policy_version": "staged_stress_v3",
                    },
                    "old completed holdout",
                    status="queued",
                )
                job = ledger.jobs("fresh_resolved_holdout", "queued")[0]
                ledger.update_job(
                    job["job_id"],
                    "completed",
                    json.loads(job["payload_json"]),
                    "old policy complete",
                )
                reconciled = loop.reconcile_completed_fresh_holdout_jobs(
                    config, ledger, state_dir
                )
                completed = ledger.jobs("fresh_resolved_holdout", "completed")
                payload = json.loads(completed[0]["payload_json"])
            finally:
                ledger.close()
        self.assertEqual(reconciled[0]["job_id"], job["job_id"])
        self.assertEqual(len(payload["superseded_fresh_holdout_verdicts"]), 1)
        self.assertIn("fresh_holdout_verdict", payload)
        self.assertEqual(
            payload["fresh_holdout_eligibility_policy_version"],
            loop.EXACT_ELIGIBILITY_POLICY_VERSION,
        )
        self.assertEqual(len(completed), 1)

    def test_legacy_holdout_pool_migrates_to_signal_hours_without_outcomes(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            snapshot = state_dir / "snapshot.gz"
            snapshot.write_bytes(b"causal snapshot")
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            signal_hours = [
                {
                    "start": "2026-07-17T13:00:00Z",
                    "end": "2026-07-17T13:00:00Z",
                    "public_signals": 1,
                }
            ]
            try:
                fingerprint = "legacy-eight-hour-pool"
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "eligible_for_fresh_holdout",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "fresh_resolved_holdout",
                    {
                        "proposal": proposal,
                        "variant": {"path": "variant.json"},
                        "candidate_replay_windows": [
                            {
                                "start": "2026-07-17T08:00:00Z",
                                "end": "2026-07-17T15:00:00Z",
                                "public_signals": 1,
                            }
                        ],
                        "completed_windows": [],
                    },
                    "legacy frozen holdout",
                    status="queued",
                )
                with mock.patch.object(loop, "load_public_windows", return_value=[]), mock.patch.object(
                    loop,
                    "evaluate_late_rule",
                    return_value={"fresh_candidate_windows": signal_hours},
                ):
                    reconciled = loop.reconcile_fresh_holdout_window_granularity(
                        config, ledger, snapshot
                    )
                payload = json.loads(
                    ledger.jobs("fresh_resolved_holdout", "queued")[0]["payload_json"]
                )
            finally:
                ledger.close()
        self.assertEqual(len(reconciled), 1)
        self.assertEqual(payload["candidate_replay_windows"], signal_hours)
        self.assertEqual(
            payload["selection_granularity_version"],
            loop.FRESH_SELECTION_GRANULARITY_VERSION,
        )
        self.assertEqual(len(payload["superseded_candidate_replay_pools"]), 1)
        self.assertTrue(
            loop.replay_window_start_is_covered(
                signal_hours[0],
                [
                    {
                        "start": "2026-07-17T08:00:00Z",
                        "end": "2026-07-17T15:00:00Z",
                    }
                ],
            )
        )

    def test_llm_http_error_is_diagnostic_and_not_retried(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            client = loop.LmStudioClient(config["llm"], Path(directory))
            error = urllib.error.HTTPError(
                client.base_url,
                400,
                "bad request",
                {},
                io.BytesIO(b'{"error":{"message":"No models loaded"}}'),
            )
            with mock.patch.object(loop.urllib.request, "urlopen", side_effect=error) as opened:
                result = client.complete(
                    "Public causal rule only.",
                    "Use public checkpoints.",
                    "test_schema",
                    {"type": "object", "properties": {}, "additionalProperties": False},
                    0.0,
                )
        self.assertFalse(result["ok"])
        self.assertEqual(result["http_status"], 400)
        self.assertIn("No models loaded", result["error_body"])
        self.assertEqual(opened.call_count, 1)

    def test_diagnostic_followups_precede_available_llm(self):
        class UnexpectedLlmClient:
            def readiness(self):
                return {"ready": True}

            def complete(self, *args, **kwargs):
                raise AssertionError("diagnostic followup should precede the LLM")

        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            ledger = loop.Ledger(Path(directory) / "research.sqlite3")
            try:
                proposals = list(loop.fallback_late_proposals())
                for index, proposal in enumerate(
                    proposals[: loop.DIAGNOSTIC_PROPOSAL_PREFIX - 1]
                ):
                    ledger.add_hypothesis(
                        "diagnostic-%s" % index,
                        "late_window_mechanisms",
                        proposal,
                        None,
                        "rejected_stage_1",
                        None,
                    )
                proposal, review, provenance = loop.propose_late_rule(
                    UnexpectedLlmClient(), ledger, "snapshot"
                )
            finally:
                ledger.close()
        self.assertIsNone(review)
        self.assertEqual(loop.DIAGNOSTIC_PROPOSAL_PREFIX, 6)
        self.assertEqual(provenance["fallback"], "diagnostic_priority")
        self.assertEqual(provenance["proposal_source"], "diagnostic")
        self.assertEqual(
            proposal["rule"], proposals[loop.DIAGNOSTIC_PROPOSAL_PREFIX - 1]["rule"]
        )
        # The sixth fallback rule: the 3m up continuation at the payoff-derived cap.
        self.assertEqual(
            (
                proposal["rule"]["operator"],
                proposal["rule"]["path_minutes"],
                proposal["rule"]["minimum_two_minute_move_usd"],
                proposal["rule"]["maximum_entry_price"],
                proposal["rule"]["direction"],
            ),
            ("path_and_move", 3, 100, loop.LATE_PAYOFF_DERIVED_ENTRY_CAP, "up"),
        )

    def test_config_is_fail_closed(self):
        config = json.loads((ROOT / "deploy/strategy-research-loop.json").read_text())
        self.assertEqual(config["mode"], "research_only")
        self.assertEqual(config["maximum_exact_l2_shortlist"], 2)
        self.assertFalse(
            config["architecture_migration"]["legacy_candidate_generation_enabled"]
        )
        self.assertFalse(
            config["architecture_migration"]["legacy_exact_replay_enabled"]
        )
        self.assertEqual(
            config["architecture_migration"]["opportunity_policy_search"][
                "pmxt_cache_dir"
            ],
            "data/pmxt_v2_cache",
        )
        self.assertFalse(
            config["exact_replay"]["historical_gates"][
                "enforce_minimum_signal_to_attempt_rate"
            ]
        )
        self.assertFalse(
            config["fresh_holdout"]["gates"][
                "enforce_minimum_signal_to_attempt_rate"
            ]
        )
        self.assertTrue(
            config["fixed_forward_confirmation"]["gates"][
                "enforce_minimum_signal_to_attempt_rate"
            ]
        )
        # 2026-09-02 factory upgrade: enabled only in the gitignored overlay.
        self.assertFalse(config["lanes"]["band_mechanisms"]["enabled"])
        self.assertEqual(config["llm"]["sampler_models"], [])
        self.assertIsNone(config["llm"]["reviewer_model"])
        config["mode"] = "live"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as handle:
            json.dump(config, handle)
            handle.flush()
            with self.assertRaisesRegex(ValueError, "research_only"):
                loop.load_config(Path(handle.name))

    def test_architecture_migration_pauses_legacy_generation_before_work(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                baseline = loop.run_baseline_evolution(config, ledger, state_dir, False)
                late = loop.run_late_window_lane(config, ledger, state_dir, False)
                exact = loop.run_queued_exact_job(config, ledger, state_dir, False)
            finally:
                ledger.close()
        self.assertEqual(baseline["status"], "paused_architecture_migration")
        self.assertEqual(late["status"], "paused_architecture_migration")
        self.assertEqual(exact["status"], "paused_architecture_migration")

    def test_architecture_migration_supersedes_execution_equivalent_exact_job(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        fingerprint = next(
            iter(
                config["architecture_migration"]["superseded_exact_fingerprints"]
            )
        )
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                ledger.add_hypothesis(
                    fingerprint,
                    "late_window_mechanisms",
                    self.valid_proposal(),
                    None,
                    "stage_1_survivor",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    fingerprint,
                    "exact_l2_replay",
                    {
                        "candidate_replay_windows": [],
                        "completed_windows": [{"start": "window-1"}],
                        "support_only_windows": [{"start": "support-1"}],
                    },
                    "queued",
                    status="queued",
                )
                result = loop.run_queued_exact_job(config, ledger, state_dir, False)
                stored = ledger.jobs("exact_l2_replay")[0]
                payload = json.loads(stored["payload_json"])
                hypothesis = ledger.hypothesis(fingerprint)
            finally:
                ledger.close()
        self.assertEqual(result["status"], "paused_architecture_migration")
        self.assertEqual(stored["status"], "blocked")
        self.assertEqual(hypothesis["status"], "superseded_execution_equivalent")
        self.assertEqual(
            payload["architecture_migration"]["preserved_completed_windows"], 1
        )
        self.assertEqual(
            payload["architecture_migration"]["preserved_support_only_windows"], 1
        )
        self.assertEqual(
            payload["architecture_migration"]["evidence"]["matching_windows"], 3
        )

    def test_baseline_lane_skips_unchanged_inputs_after_success(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["architecture_migration"]["legacy_candidate_generation_enabled"] = True
        config["lanes"]["baseline_evolution"]["minimum_interval_seconds"] = 0
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")

            def completed_evolution(command, _timeout, _dry_run):
                out_dir = Path(command[command.index("--out-dir") + 1])
                out_dir.mkdir(parents=True)
                (out_dir / "evolution_summary.json").write_text(
                    json.dumps({"candidates": []})
                )
                return {"status": "completed", "returncode": 0}

            try:
                with mock.patch.object(
                    loop, "run_command", side_effect=completed_evolution
                ) as run_command:
                    first = loop.run_baseline_evolution(
                        config, ledger, state_dir, False
                    )
                    second = loop.run_baseline_evolution(
                        config, ledger, state_dir, False
                    )
            finally:
                ledger.close()
        self.assertEqual(first["status"], "completed")
        self.assertEqual(second["status"], "unchanged_inputs")
        self.assertEqual(first["input_hash"], second["input_hash"])
        run_command.assert_called_once()

    def test_baseline_lane_blocks_old_or_non_chronological_reports(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["architecture_migration"]["legacy_candidate_generation_enabled"] = True
        config["lanes"]["baseline_evolution"]["reports"] = config["lanes"][
            "baseline_evolution"
        ]["reports"][:2]
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                result = loop.run_baseline_evolution(config, ledger, state_dir, True)
            finally:
                ledger.close()
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(
            result["reason"], "awaiting_current_semantics_chronological_reports"
        )
        self.assertEqual(result["valid_reports"], 2)
        self.assertEqual(result["distinct_windows"], 1)

    def economic_screen_job_payload(self, proposal, source_kind="cached_family_top_of_book"):
        return {
            "source_kind": source_kind,
            "proposal": proposal,
            "variant": None,
            "candidate_replay_windows": [],
            "fresh_candidate_windows": [],
            "fresh_previously_measured_exclusion": {},
        }

    def saturate_exact_shortlist(self, config, ledger):
        for index in range(int(config["maximum_exact_l2_shortlist"])):
            ledger.enqueue(
                "late_window_mechanisms",
                "exact-%d" % index,
                "exact_l2_replay",
                {},
                "queued",
                status="queued",
            )

    PASSING_CACHED_VERDICT = {
        "passed": True,
        "classification": "passed",
        "metrics": {"conditions": 12, "wins": 10},
    }

    def test_saturated_shortlist_defers_passing_screen_without_side_effects(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    "screen-candidate",
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "stage_1_survivor",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    "screen-candidate",
                    "economic_opportunity_screen",
                    self.economic_screen_job_payload(proposal),
                    "queued",
                    status="queued",
                )
                self.saturate_exact_shortlist(config, ledger)
                with mock.patch.object(
                    loop, "cached_family_economic_verdict", return_value=self.PASSING_CACHED_VERDICT
                ) as verdict:
                    results = [
                        loop.run_queued_economic_screen(config, ledger, state_dir, False)
                        for _ in range(2)
                    ]
                    queued = ledger.jobs("economic_opportunity_screen", "queued")
                    status = ledger.hypothesis("screen-candidate")["status"]
                    side_effects = (
                        (state_dir / "trial_ledger.jsonl").exists()
                        or (state_dir / "evidence/economic/screen-candidate.json").exists()
                    )
                    # Once the shortlist frees the deferred candidate is logged once.
                    for job in ledger.jobs("exact_l2_replay", "queued"):
                        ledger.update_job(job["job_id"], "blocked", {}, "freed")
                    released = loop.run_queued_economic_screen(config, ledger, state_dir, False)
                exact_queued = [
                    job["hypothesis_fingerprint"] for job in ledger.jobs("exact_l2_replay", "queued")
                ]
                released_status = ledger.hypothesis("screen-candidate")["status"]
            finally:
                ledger.close()
            trial_rows = (state_dir / "trial_ledger.jsonl").read_text().splitlines()
        self.assertEqual(verdict.call_count, 3)
        self.assertEqual([item["status"] for item in results], ["shortlist_saturated"] * 2)
        self.assertEqual(len(queued), 1)
        self.assertEqual(status, "stage_1_survivor")
        self.assertFalse(side_effects)
        self.assertEqual(released["status"], "economic_screen_passed")
        self.assertEqual(released_status, "eligible_for_exact_l2")
        self.assertEqual(exact_queued, ["screen-candidate"])
        self.assertEqual(len(trial_rows), 1)

    def test_saturated_shortlist_still_rejects_head_and_reaches_fresh_job(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        rejected = {
            "passed": False,
            "classification": "rejected",
            "metrics": {"conditions": 12, "wins": 3},
        }
        fresh_passing = {
            "passed": True,
            "classification": "passed",
            "metrics": {"fills": 6, "wins": 5},
        }
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                proposal = self.valid_proposal()
                for fingerprint, status, source_kind in (
                    ("head-candidate", "stage_1_survivor", "cached_family_top_of_book"),
                    (
                        "fresh-candidate",
                        "historical_eligible_awaiting_fresh_data",
                        "fresh_exact_replay",
                    ),
                ):
                    ledger.add_hypothesis(
                        fingerprint, "late_window_mechanisms", proposal, None, status, None
                    )
                    ledger.enqueue(
                        "late_window_mechanisms",
                        fingerprint,
                        "economic_opportunity_screen",
                        self.economic_screen_job_payload(proposal, source_kind),
                        "queued",
                        status="queued",
                    )
                self.saturate_exact_shortlist(config, ledger)
                with mock.patch.object(
                    loop, "cached_family_economic_verdict", return_value=rejected
                ), mock.patch.object(
                    loop, "exact_fresh_economic_verdict", return_value=fresh_passing
                ):
                    results = [
                        loop.run_queued_economic_screen(config, ledger, state_dir, False)
                        for _ in range(3)
                    ]
                head_status = ledger.hypothesis("head-candidate")["status"]
                exact_queued = len(ledger.jobs("exact_l2_replay", "queued"))
                economic_queued = len(ledger.jobs("economic_opportunity_screen", "queued"))
            finally:
                ledger.close()
        self.assertEqual(
            [item["status"] for item in results],
            ["rejected_economic_screen", "economic_screen_passed", "empty"],
        )
        self.assertEqual(head_status, "rejected_economic_screen")
        self.assertEqual((exact_queued, economic_queued), (2, 0))

    def test_cycle_reaches_exact_dispatcher_while_shortlist_is_saturated(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = str(state_dir)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                proposal = self.valid_proposal()
                ledger.add_hypothesis(
                    "screen-candidate",
                    "late_window_mechanisms",
                    proposal,
                    None,
                    "stage_1_survivor",
                    None,
                )
                ledger.enqueue(
                    "late_window_mechanisms",
                    "screen-candidate",
                    "economic_opportunity_screen",
                    self.economic_screen_job_payload(proposal),
                    "queued",
                    status="queued",
                )
                self.saturate_exact_shortlist(config, ledger)
            finally:
                ledger.close()
            seed = ROOT / config["lanes"]["late_window_mechanisms"]["public_snapshot"]
            with mock.patch.object(
                loop, "resource_status", return_value={"passed": True, "checks": {}}
            ), mock.patch.object(
                loop, "run_registry_audit", return_value={"status": "completed"}
            ), mock.patch.object(
                loop,
                "refresh_public_snapshot",
                return_value={
                    "status": "fallback_seed",
                    "path": str(seed),
                    "sha256": loop.sha256_file(seed),
                },
            ), mock.patch.object(loop, "run_late_window_lane") as lane, mock.patch.object(
                loop, "cached_family_economic_verdict", return_value=self.PASSING_CACHED_VERDICT
            ), mock.patch.object(
                loop, "run_queued_exact_job", return_value={"status": "processed"}
            ) as exact:
                result = loop.run_cycle(config, False, "late_window_mechanisms")
        lane.assert_not_called()
        exact.assert_called_once()
        self.assertEqual(result["lane_result"]["status"], "queued_replay_priority")
        self.assertEqual(result["economic_screen_job"]["status"], "shortlist_saturated")
        self.assertEqual(result["fixed_forward_job"]["status"], "empty")
        self.assertEqual(result["fresh_holdout_job"]["status"], "empty")
        self.assertEqual(result["exact_job"]["status"], "processed")

    def test_evidence_accrual_is_idempotent_on_replay(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            ledger = loop.Ledger(Path(directory) / "research.sqlite3")
            try:
                outcomes = [(300, 0.6, True), (600, 0.6, True), (900, 0.6, False)]
                first = ledger.accrue("accrual", "late_window_mechanisms", outcomes, 0)
                replay = ledger.accrue("accrual", "late_window_mechanisms", outcomes, 0)
                extended = ledger.accrue(
                    "accrual",
                    "late_window_mechanisms",
                    [(1500, 0.6, True), (900, 0.6, True), (1200, 0.6, True)],
                    0,
                )
                row = ledger.accrual("accrual")
                seeded = ledger.accrue("seeded", "late_window_mechanisms", outcomes, 600)
                seeded_row = ledger.accrual("seeded")
            finally:
                ledger.close()
        self.assertEqual((first["n"], first["wins"], first["applied"]), (3, 2, 3))
        self.assertEqual((replay["n"], replay["wins"], replay["applied"]), (3, 2, 0))
        self.assertEqual(replay["e_value"], first["e_value"])
        self.assertEqual((extended["n"], extended["wins"], extended["applied"]), (5, 4, 2))
        self.assertEqual(row["last_window_start"], 1500)
        self.assertEqual(row["verdict"], extended["verdict"])
        restored = loop.evidence_accrual.EProcess.from_json(row["state_json"])
        self.assertEqual(restored.e_value(), extended["e_value"])
        self.assertEqual((seeded["n"], seeded["applied"]), (1, 1))
        self.assertEqual(seeded_row["last_window_start"], 900)

    def accrual_window(self, window_start, direction, won):
        base = 70_000.0
        moment = loop.dt.datetime.fromtimestamp(window_start, tz=loop.dt.timezone.utc)
        return {
            "window_start": window_start,
            "utc_day": moment.date().isoformat(),
            "utc_hour": moment.hour,
            "chronological_window": "fresh_holdout",
            "p0": base,
            "p60": base + direction * 60,
            "p120": base + direction * 220,
            "p180": base + direction * 280,
            "p240": base + direction * 300,
            "terminal": base + direction * (320 if won else -20),
        }

    def accrual_fixture(self, directory, hypotheses, windows):
        """Public snapshot plus a stage-1 artifact whose newest hour is CUT_HOUR."""
        state_dir = Path(directory)
        snapshot = state_dir / "snapshot.jsonl.gz"
        loop.atomic_gzip_jsonl(snapshot, windows)
        ledger = loop.Ledger(state_dir / "research.sqlite3")
        for fingerprint, status, direction in hypotheses:
            proposal = self.valid_proposal()
            proposal["rule"]["direction"] = direction
            evidence_path = state_dir / ("evidence/%s.json" % fingerprint)
            loop.atomic_json(
                evidence_path,
                {
                    "candidate_replay_windows": [
                        {"start": self.CUT_HOUR, "end": self.CUT_HOUR}
                    ],
                    "fresh_candidate_windows": [],
                },
            )
            ledger.add_hypothesis(
                fingerprint, "late_window_mechanisms", proposal, None, status, evidence_path
            )
        return snapshot, ledger

    CUT_HOUR = "2026-08-01T12:00:00Z"
    CUT_EPOCH = 1785585600

    def test_fresh_public_accrual_excludes_windows_at_or_before_screen_cut(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        windows = [
            self.accrual_window(self.CUT_EPOCH + index * 300, 1, True) for index in range(12)
        ] + [
            self.accrual_window(self.CUT_EPOCH + 3600 + index * 300, 1, True)
            for index in range(6)
        ]
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            snapshot, ledger = self.accrual_fixture(
                directory, [("accrual-cut", "stage_1_survivor", "both")], windows
            )
            # A legacy artifact without bucket lists has no safe cut: skipped.
            legacy_evidence = Path(directory) / "evidence/accrual-legacy.json"
            loop.atomic_json(legacy_evidence, {"overall": {"signals": 40, "wins": 40}})
            try:
                ledger.add_hypothesis(
                    "accrual-legacy",
                    "late_window_mechanisms",
                    self.valid_proposal(),
                    None,
                    "stage_1_survivor",
                    legacy_evidence,
                )
                first = loop.run_fresh_public_accrual(config, ledger, snapshot)
                replay = loop.run_fresh_public_accrual(config, ledger, snapshot)
                row = ledger.accrual("accrual-cut")
                status = ledger.hypothesis("accrual-cut")["status"]
                legacy = ledger.accrual("accrual-legacy")
                legacy_status = ledger.hypothesis("accrual-legacy")["status"]
            finally:
                ledger.close()
            trial_rows = [
                json.loads(line)
                for line in (Path(directory) / "trial_ledger.jsonl").read_text().splitlines()
            ]
        expected = {"evaluated": 1, "promoted": 0, "killed": 0, "accruing": 1, "skipped": 1}
        self.assertEqual(first, expected)
        self.assertEqual(replay, expected)
        self.assertIsNone(legacy)
        self.assertEqual(legacy_status, "stage_1_survivor")
        self.assertEqual((row["n"], row["wins"], row["verdict"]), (6, 6, "continue"))
        self.assertEqual(row["last_window_start"], self.CUT_EPOCH + 3600 + 5 * 300)
        self.assertEqual(status, "stage_1_survivor")
        self.assertEqual(len(trial_rows), 1)
        self.assertEqual(
            (trial_rows[0]["stage"], trial_rows[0]["verdict"], trial_rows[0]["n"]),
            ("fresh_public_accrual", "continue", 6),
        )

    def test_fresh_public_accrual_promotes_and_kills_by_e_value(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        # Up windows win for the up rule; the down rule fires on the down
        # path windows and loses because the terminal closes above the open.
        windows = [
            self.accrual_window(self.CUT_EPOCH + 3600 + index * 300, 1, True)
            for index in range(50)
        ] + [
            self.accrual_window(self.CUT_EPOCH + 3600 + (50 + index) * 300, -1, False)
            for index in range(10)
        ]
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            snapshot, ledger = self.accrual_fixture(
                directory,
                [
                    ("accrual-promote", "eligible_for_exact_l2", "up"),
                    ("accrual-kill", "blocked_architecture_migration", "down"),
                    ("accrual-untouched", "rejected_stage_1", "both"),
                ],
                windows,
            )
            try:
                summary = loop.run_fresh_public_accrual(config, ledger, snapshot)
                statuses = {
                    fingerprint: ledger.hypothesis(fingerprint)["status"]
                    for fingerprint in ("accrual-promote", "accrual-kill", "accrual-untouched")
                }
                promote = ledger.accrual("accrual-promote")
                kill = ledger.accrual("accrual-kill")
                untouched = ledger.accrual("accrual-untouched")
            finally:
                ledger.close()
        self.assertEqual(
            summary, {"evaluated": 2, "promoted": 1, "killed": 1, "accruing": 0, "skipped": 0}
        )
        # A promote verdict is informational (the null is the worst-case public
        # break-even, not the executable one): the pipeline stage stays.
        self.assertEqual(
            statuses,
            {
                "accrual-promote": "eligible_for_exact_l2",
                "accrual-kill": "killed_futility",
                "accrual-untouched": "rejected_stage_1",
            },
        )
        self.assertEqual((promote["n"], promote["wins"], promote["verdict"]), (50, 50, "promote"))
        self.assertGreaterEqual(promote["e_value"], loop.evidence_accrual.PROMOTE_E)
        self.assertEqual((kill["n"], kill["wins"], kill["verdict"]), (10, 0, "kill"))
        self.assertLessEqual(kill["e_value"], loop.evidence_accrual.FUTILITY_E)
        self.assertIsNone(untouched)

    def test_fresh_public_accrual_keeps_pipeline_status_and_skips_unscorable_rules(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        # Every window is at or before the cut: zero evidence for every rule.
        windows = [self.accrual_window(self.CUT_EPOCH + index * 300, 1, True) for index in range(3)]
        unscorable = {
            "accrual-sigma": ("settlement_sigma_buffer", 0.1),
            "accrual-pressure": ("minimum_book_pressure", 0.15),
        }
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            snapshot, ledger = self.accrual_fixture(
                directory,
                [
                    ("accrual-exact", "eligible_for_exact_l2", "both"),
                    ("accrual-holdout", "eligible_for_fresh_holdout", "both"),
                    ("accrual-terminal", "holdout_insufficient_support", "both"),
                ],
                windows,
            )
            try:
                for fingerprint, (field, value) in unscorable.items():
                    proposal = self.valid_proposal()
                    proposal["rule"][field] = value
                    ledger.add_hypothesis(
                        fingerprint,
                        "late_window_mechanisms",
                        proposal,
                        None,
                        "stage_1_survivor",
                        Path(directory) / "evidence/accrual-exact.json",
                    )
                summary = loop.run_fresh_public_accrual(config, ledger, snapshot)
                statuses = {
                    row["fingerprint"]: row["status"] for row in ledger.late_hypotheses()
                }
                accruals = {
                    fingerprint: ledger.accrual(fingerprint)
                    for fingerprint in list(unscorable) + ["accrual-terminal"]
                }
                zero_evidence = [
                    (ledger.accrual(fingerprint)["n"], ledger.accrual(fingerprint)["e_value"])
                    for fingerprint in ("accrual-exact", "accrual-holdout")
                ]
            finally:
                ledger.close()
            trial_ledger_exists = (Path(directory) / "trial_ledger.jsonl").exists()
        self.assertEqual(
            summary, {"evaluated": 2, "promoted": 0, "killed": 0, "accruing": 2, "skipped": 2}
        )
        self.assertEqual(
            statuses,
            {
                "accrual-exact": "eligible_for_exact_l2",
                "accrual-holdout": "eligible_for_fresh_holdout",
                "accrual-terminal": "holdout_insufficient_support",
                "accrual-sigma": "stage_1_survivor",
                "accrual-pressure": "stage_1_survivor",
            },
        )
        self.assertEqual(accruals, {fingerprint: None for fingerprint in accruals})
        self.assertEqual(zero_evidence, [(0, 1.0), (0, 1.0)])
        self.assertFalse(trial_ledger_exists)

    def test_eoh_parents_skip_terminal_kills_and_killed_futility_feeds_kill_feedback(self):
        generator = loop.factory_generator
        rows = [
            {
                "fingerprint": fingerprint,
                "status": status,
                "created_at": created_at,
                "proposal": self.valid_proposal(),
            }
            for fingerprint, status, created_at in (
                ("killed", "killed_futility", "2026-09-05T00:00:00Z"),
                ("insufficient", "holdout_insufficient_support", "2026-09-04T00:00:00Z"),
                ("survivor", "stage_1_survivor", "2026-09-03T00:00:00Z"),
                ("exact", "eligible_for_exact_l2", "2026-09-02T00:00:00Z"),
                ("rejected", "rejected_stage_1", "2026-09-01T00:00:00Z"),
            )
        ]
        parents = [row["fingerprint"] for row in generator.eoh_parents(rows)]
        negatives = [item["status"] for item in generator.killed_negative_items(None, rows)]
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            recorded = generator.record_kill_feedback(
                {"kill_feedback_enabled": True},
                Path(directory),
                "killed_futility",
                json.dumps(self.valid_proposal()),
            )
            feedback = [
                json.loads(line)
                for line in (Path(directory) / "kill_feedback.jsonl").read_text().splitlines()
            ]
        self.assertEqual(parents, ["exact", "survivor"])
        # A futility kill joins the ledger rejects in the negative prompt.
        self.assertEqual(negatives, ["killed_futility", "rejected_stage_1"])
        self.assertTrue(recorded)
        self.assertEqual(
            [(entry["stage"], entry["reason"]) for entry in feedback],
            [("fresh_public_accrual", "killed_futility")],
        )

    def test_cycle_accrues_fresh_public_evidence_after_reconciliation(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        windows = [self.accrual_window(self.CUT_EPOCH + 3600, 1, True)]
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            snapshot, ledger = self.accrual_fixture(directory, [], windows)
            ledger.close()
            refreshed = {
                "status": "current",
                "path": str(snapshot),
                "sha256": loop.sha256_file(snapshot),
            }
            with mock.patch.object(
                loop, "resource_status", return_value={"passed": True, "checks": {}}
            ), mock.patch.object(
                loop, "run_registry_audit", return_value={"status": "completed"}
            ), mock.patch.object(
                loop, "refresh_public_snapshot", return_value=refreshed
            ), mock.patch.object(
                loop, "run_late_window_lane", return_value={"status": "dry_run"}
            ), mock.patch.object(
                loop, "run_fresh_public_accrual", return_value={"evaluated": 0}
            ) as accrual:
                live = loop.run_cycle(config, False, "late_window_mechanisms")
                dry = loop.run_cycle(config, True, "late_window_mechanisms")
        accrual.assert_called_once()
        self.assertEqual(accrual.call_args.args[2], snapshot)
        self.assertEqual(live["fresh_public_accrual"], {"evaluated": 0})
        self.assertTrue(dry["dry_run"])
        self.assertNotIn("fresh_public_accrual", dry)
        self.assertEqual(dry["lane_result"]["status"], "dry_run")

    def test_stage_1_survivor_drains_its_economic_screen_in_the_same_cycle(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            snapshot, ledger = self.accrual_fixture(directory, [], [])
            ledger.close()
            refreshed = {
                "status": "current",
                "path": str(snapshot),
                "sha256": loop.sha256_file(snapshot),
            }
            with mock.patch.object(
                loop, "resource_status", return_value={"passed": True, "checks": {}}
            ), mock.patch.object(
                loop, "run_registry_audit", return_value={"status": "completed"}
            ), mock.patch.object(
                loop, "refresh_public_snapshot", return_value=refreshed
            ), mock.patch.object(
                loop, "run_fresh_public_accrual", return_value={"evaluated": 0}
            ), mock.patch.object(
                loop, "run_late_window_lane", return_value={"status": "stage_1_survivor"}
            ), mock.patch.object(
                loop, "run_queued_economic_screen", return_value={"status": "empty"}
            ) as screen:
                live = loop.run_cycle(config, False, "late_window_mechanisms")
        screen.assert_called_once()
        self.assertEqual(live["economic_screen_job"], {"status": "empty"})

    def test_refresh_steps_back_a_day_when_the_newest_archive_is_unpublished(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            windows = [self.accrual_window(self.CUT_EPOCH + 3600, 1, True)]
            snapshot, ledger = self.accrual_fixture(directory, [], windows)
            snapshots = state_dir / "public_snapshots"
            snapshots.mkdir(parents=True, exist_ok=True)
            current = snapshots / "binance_spot_1m_current.jsonl.gz"
            current.write_bytes(snapshot.read_bytes())
            (snapshots / "binance_spot_1m_current.manifest.json").write_text("{}")
            lag = int(config["public_refresh"]["resolution_lag_days"])
            newest = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=lag)
            previous = newest - dt.timedelta(days=1)
            ledger.set_meta("public_snapshot.latest_day", previous.isoformat())
            probed = []

            def published(url, timeout=15):
                probed.append(url)
                return previous.isoformat() in url

            try:
                with mock.patch.object(loop, "public_archive_published", side_effect=published), mock.patch.object(
                    loop, "download_atomic", side_effect=AssertionError("must not download")
                ):
                    result = loop.refresh_public_snapshot(config, ledger, state_dir, False)
            finally:
                ledger.close()
        self.assertEqual(result["status"], "current")
        self.assertEqual(result["latest_day"], previous.isoformat())
        self.assertEqual(len(probed), 2)
        self.assertIn(newest.isoformat(), probed[0])

    def test_cycle_falls_back_to_the_last_good_snapshot_not_the_seed(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            snapshot, ledger = self.accrual_fixture(directory, [], [])
            ledger.close()
            current = state_dir / "public_snapshots/binance_spot_1m_current.jsonl.gz"
            current.parent.mkdir(parents=True, exist_ok=True)
            current.write_bytes(snapshot.read_bytes())
            with mock.patch.object(
                loop, "resource_status", return_value={"passed": True, "checks": {}}
            ), mock.patch.object(
                loop, "run_registry_audit", return_value={"status": "completed"}
            ), mock.patch.object(
                loop, "refresh_public_snapshot", side_effect=urllib.error.HTTPError("u", 404, "Not Found", {}, None)
            ), mock.patch.object(
                loop, "run_late_window_lane", return_value={"status": "not_due"}
            ), mock.patch.object(
                loop, "run_fresh_public_accrual", return_value={"evaluated": 0}
            ) as accrual:
                live = loop.run_cycle(config, False, "late_window_mechanisms")
        self.assertEqual(live["public_snapshot"]["status"], "fallback_current")
        self.assertEqual(live["public_snapshot"]["path"], str(current))
        self.assertIn("404", live["public_snapshot"]["error"])
        accrual.assert_called_once()
        self.assertEqual(accrual.call_args.args[2], current)

    def test_uniform_late_proposal_always_validates_and_replays(self):
        enums = loop.LATE_PROPOSAL_SCHEMA["properties"]["rule"]["properties"]
        rng = loop.random.Random(7)
        proposals = [loop.uniform_late_proposal(rng) for _ in range(1000)]
        for proposal in proposals:
            self.assertEqual(loop.validate_late_proposal(proposal), proposal)
            self.assertEqual(loop.normalized_late_rule(proposal["rule"]), proposal["rule"])
            for field, value in proposal["rule"].items():
                self.assertIn(value, enums[field]["enum"])
        rules = [loop.canonical_json(proposal["rule"]) for proposal in proposals]
        # 7128 executable grid points: a uniform draw rarely repeats in 1000 tries.
        self.assertGreater(len(set(rules)), 850)
        self.assertEqual(
            {proposal["rule"]["operator"] for proposal in proposals}, loop.LATE_OPERATORS
        )
        replay = loop.random.Random(7)
        self.assertEqual(
            [loop.canonical_json(loop.uniform_late_proposal(replay)["rule"]) for _ in rules],
            rules,
        )

    def test_late_lane_alternates_llm_and_uniform_control_after_prefix(self):
        class BurstClient:
            """Hands out distinct grid rules; the reviewer call is unavailable."""

            def __init__(self, ready=True):
                self.ready = ready
                self.calls = 0
                self.rules = iter(list(loop.fallback_late_proposals())[40:])

            def readiness(self):
                return {"ready": self.ready}

            def complete(self, system, user, schema_name, schema, temperature, model=None):
                self.calls += 1
                if schema_name == "late_window_review_v1":
                    return {"ok": False, "reason": "reviewer_offline"}
                return {"ok": True, "value": next(self.rules)}

        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["generator"]["novelty_gate_enabled"] = False  # embeddings need the network
        fallbacks = list(loop.fallback_late_proposals())
        client = BurstClient()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                sources = []
                rules = []
                for _ in range(loop.DIAGNOSTIC_PROPOSAL_PREFIX + 6):
                    proposal, review, provenance = loop.propose_late_rule(
                        client, ledger, "snapshot", config, state_dir
                    )
                    self.assertEqual(loop.validate_late_proposal(proposal), proposal)
                    self.assertIsNone(review)
                    self.assertIn(provenance["proposal_source"], loop.LATE_PROPOSAL_SOURCES)
                    sources.append(provenance["proposal_source"])
                    rules.append(proposal["rule"])
                    ledger.add_hypothesis(
                        "late-%d" % len(rules),
                        "late_window_mechanisms",
                        proposal,
                        review,
                        "rejected_stage_1",
                        None,
                        source=provenance["proposal_source"],
                    )
                stored = [row["source"] for row in ledger.late_hypotheses()]
                queue = (state_dir / "proposal_queue.jsonl").read_text().splitlines()
                offline, _, offline_provenance = loop.propose_late_rule(
                    BurstClient(ready=False), ledger, "snapshot", config, state_dir
                )
            finally:
                ledger.close()
        # After the prefix the arm with fewer entries goes next (LLM on ties), so
        # queued burst survivors interleave one-for-one with uniform controls.
        self.assertEqual(
            sources,
            ["diagnostic"] * loop.DIAGNOSTIC_PROPOSAL_PREFIX
            + ["llm", "uniform_control", "burst_queue", "uniform_control", "burst_queue", "uniform_control"],
        )
        self.assertEqual(stored, sources)
        self.assertEqual(
            rules[: loop.DIAGNOSTIC_PROPOSAL_PREFIX],
            [item["rule"] for item in fallbacks[: loop.DIAGNOSTIC_PROPOSAL_PREFIX]],
        )
        self.assertEqual(
            len({loop.canonical_json(loop.normalized_late_rule(rule)) for rule in rules}),
            len(rules),
        )
        # One burst (samples_per_burst + the reviewer call); the extra survivors
        # are queued and replayed on later LLM turns instead of sampling again.
        self.assertEqual(client.calls, config["generator"]["samples_per_burst"] + 1)
        self.assertEqual(len(queue), config["generator"]["samples_per_burst"] - 3)
        # A not-ready model on an LLM turn degrades to the deterministic grid.
        self.assertNotIn("turn", offline_provenance)
        self.assertEqual(offline_provenance["proposal_source"], "fallback_grid")
        self.assertEqual(offline_provenance["fallback"], "deterministic_finite_grid")
        self.assertEqual(offline["rule"], fallbacks[loop.DIAGNOSTIC_PROPOSAL_PREFIX]["rule"])

    def test_reviewer_reject_is_advisory_and_stays_in_llm_arm(self):
        class RejectingClient:
            """Hands out distinct grid rules; the reviewer rejects every one."""

            def __init__(self):
                self.review_calls = 0
                self.rules = iter(list(loop.fallback_late_proposals())[40:])

            def readiness(self):
                return {"ready": True}

            def complete(self, system, user, schema_name, schema, temperature, model=None):
                if schema_name == "late_window_review_v1":
                    self.review_calls += 1
                    sha = user.split("\n", 1)[0].split("=", 1)[1]
                    return {
                        "ok": True,
                        "value": {"proposal_sha256": sha, "verdict": "reject", "reason": "test"},
                    }
                return {"ok": True, "value": next(self.rules)}

        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["generator"]["novelty_gate_enabled"] = False  # embeddings need the network
        config["generator"]["samples_per_burst"] = 1  # nothing queued: every LLM turn bursts
        fallbacks = list(loop.fallback_late_proposals())
        client = RejectingClient()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                turns = []
                llm_rules = []
                for index in range(loop.DIAGNOSTIC_PROPOSAL_PREFIX + 4):
                    proposal, review, provenance = loop.propose_late_rule(
                        client, ledger, "snapshot", config, state_dir
                    )
                    turns.append(
                        (
                            provenance.get("turn"),
                            provenance["proposal_source"],
                            review["verdict"] if review else None,
                        )
                    )
                    if provenance["proposal_source"] == "llm":
                        llm_rules.append(proposal["rule"])
                        self.assertNotIn("fallback", provenance)
                    ledger.add_hypothesis(
                        "late-%d" % index,
                        "late_window_mechanisms",
                        proposal,
                        review,
                        "rejected_stage_1",
                        None,
                        source=provenance["proposal_source"],
                    )
            finally:
                ledger.close()
        # A rejected LLM sample is still the evaluated proposal (source llm, the
        # verdict travels with it), so the LLM arm's denominator keeps it and
        # the control arm takes the next turn.
        self.assertEqual(
            turns[loop.DIAGNOSTIC_PROPOSAL_PREFIX :],
            [
                ("llm", "llm", "reject"),
                ("uniform_control", "uniform_control", None),
                ("llm", "llm", "reject"),
                ("uniform_control", "uniform_control", None),
            ],
        )
        self.assertEqual(llm_rules, [fallbacks[40]["rule"], fallbacks[41]["rule"]])
        self.assertEqual(client.review_calls, 2)

    def test_llm_turn_without_survivor_falls_through_to_uniform_control(self):
        class InvalidClient:
            """Ready, but every sample fails validation."""

            def __init__(self):
                self.calls = 0

            def readiness(self):
                return {"ready": True}

            def complete(self, system, user, schema_name, schema, temperature, model=None):
                self.calls += 1
                return {"ok": True, "value": {"garbage": 1}}

        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["generator"]["novelty_gate_enabled"] = False  # embeddings need the network
        burst_n = config["generator"]["samples_per_burst"]
        client = InvalidClient()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                provenances = []
                for index in range(loop.DIAGNOSTIC_PROPOSAL_PREFIX + 4):
                    proposal, review, provenance = loop.propose_late_rule(
                        client, ledger, "snapshot", config, state_dir
                    )
                    self.assertEqual(loop.validate_late_proposal(proposal), proposal)
                    self.assertIsNone(review)
                    provenances.append(provenance)
                    ledger.add_hypothesis(
                        "late-%d" % index,
                        "late_window_mechanisms",
                        proposal,
                        review,
                        "rejected_stage_1",
                        None,
                        source=provenance["proposal_source"],
                    )
                stored = [row["source"] for row in ledger.late_hypotheses()]
            finally:
                ledger.close()
        after = provenances[loop.DIAGNOSTIC_PROPOSAL_PREFIX :]
        # A ready model that never validates cannot hold the turn: each of its
        # turns records the empty burst and lands a control draw, not a grid rule.
        self.assertEqual([item["turn"] for item in after], ["llm"] * 4)
        self.assertEqual(stored[loop.DIAGNOSTIC_PROPOSAL_PREFIX :], ["uniform_control"] * 4)
        self.assertEqual({item["burst"]["survivors"] for item in after}, {0})
        self.assertEqual({item["burst"]["invalid"] for item in after}, {burst_n})
        self.assertEqual(after[-1]["control"], {"draws": 1, "duplicate": 0, "novelty_rejected": 0})
        self.assertNotIn("fallback", after[-1])
        self.assertEqual(client.calls, 4 * burst_n)

    def test_structural_novelty_gate_replaces_cosine_for_grid_rules(self):
        killed = [
            {
                "kind": "ledger_rule",
                "rule": "op=path_only path=3m",
                "rule_fields": {"operator": "path_only", "path_minutes": 3, "direction": "both"},
                "status": "rejected_stage_1",
            }
        ]
        gen_cfg = dict(loop.factory_generator.DEFAULTS)
        same = {"rule": {"operator": "path_only", "path_minutes": 3, "direction": "both"}}
        one_off = {"rule": {"operator": "path_only", "path_minutes": 3, "direction": "up"}}
        two_off = {"rule": {"operator": "path_only", "path_minutes": 4, "direction": "up"}}
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            with mock.patch.object(loop.factory_generator, "embed_text", side_effect=AssertionError("no embeddings")):
                verdicts = [
                    loop.factory_generator.novelty_check(p, gen_cfg, "http://unused", 1.0, state_dir, killed)
                    for p in (same, one_off, two_off)
                ]
                nothing_killed = loop.factory_generator.novelty_check(same, gen_cfg, "http://unused", 1.0, state_dir, [])
        self.assertEqual([v["status"] for v in verdicts], ["rejected", "rejected", "accepted"])
        self.assertEqual([v["min_hamming"] for v in verdicts], [0, 1, 2])
        self.assertEqual(verdicts[0]["against"]["status"], "rejected_stage_1")
        self.assertEqual(nothing_killed, {"status": "accepted", "gate": "structural", "min_hamming": None})
        # A support/economics rejection does not mark its neighbourhood dead.
        support_killed = [dict(killed[0], status="rejected_entry_economics")]
        with mock.patch.object(loop.factory_generator, "embed_text", side_effect=AssertionError("no embeddings")):
            lenient = loop.factory_generator.novelty_check(one_off, gen_cfg, "http://unused", 1.0, Path("/tmp"), support_killed)
        self.assertEqual(lenient["status"], "accepted")
        self.assertTrue(loop.factory_generator.novelty_blocking_status("killed_futility"))
        self.assertFalse(loop.factory_generator.novelty_blocking_status("rejected_economic_screen"))
        self.assertTrue(all("rule_fields" in item for item in loop.factory_generator.killed_negative_items(
            None,
            [{"status": "rejected_stage_1", "created_at": "2026-09-02", "proposal": {"rule": same["rule"]}}],
        )))

    def test_novelty_gate_applies_to_uniform_control_draws(self):
        class IdleClient:
            base_url = "http://127.0.0.1:1234/v1"
            config = {"request_timeout_seconds": 1}

            def readiness(self):
                return {"ready": True}

            def complete(self, *args, **kwargs):
                raise AssertionError("a control turn samples nothing")

        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        verdicts = iter(["rejected", "accepted"])
        checked = []

        def novelty_check(candidate, gen_cfg, base_url, timeout, state_dir, killed_items):
            checked.append((candidate["rule"], killed_items))
            return {"status": next(verdicts)}

        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                prefix = list(loop.fallback_late_proposals())[: loop.DIAGNOSTIC_PROPOSAL_PREFIX]
                for index, fallback in enumerate(prefix):
                    ledger.add_hypothesis(
                        "late-%d" % index,
                        "late_window_mechanisms",
                        fallback,
                        None,
                        "rejected_stage_1",
                        None,
                        source="diagnostic",
                    )
                # One LLM row ahead: the control arm goes next.
                ledger.add_hypothesis(
                    "late-llm",
                    "late_window_mechanisms",
                    self.valid_proposal(),
                    None,
                    "stage_1_survivor",
                    None,
                    source="llm",
                )
                rows = len(ledger.late_hypotheses())
                with mock.patch.object(
                    loop.factory_generator, "novelty_check", side_effect=novelty_check
                ):
                    proposal, review, provenance = loop.propose_late_rule(
                        IdleClient(), ledger, "snapshot", config, state_dir
                    )
            finally:
                ledger.close()
        rng = loop.random.Random(rows)
        draws = [loop.uniform_late_proposal(rng)["rule"] for _ in range(2)]
        self.assertEqual(provenance["turn"], "uniform_control")
        self.assertEqual(provenance["proposal_source"], "uniform_control")
        self.assertIsNone(review)
        # The first draw sits too close to a killed rule and is skipped exactly
        # as an LLM sample would be; the killed prefix feeds the gate here too.
        self.assertEqual([rule for rule, _ in checked], draws)
        self.assertEqual(proposal["rule"], draws[1])
        self.assertEqual(provenance["control"], {"draws": 2, "duplicate": 0, "novelty_rejected": 1})
        self.assertEqual(
            sum(1 for item in checked[0][1] if item["kind"] == "ledger_rule"),
            loop.DIAGNOSTIC_PROPOSAL_PREFIX,
        )

    def test_late_lane_persists_proposal_source_from_provenance(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["architecture_migration"]["legacy_candidate_generation_enabled"] = True
        proposal = self.valid_proposal()
        provenance = {
            "readiness": {"ready": True},
            "turn": "uniform_control",
            "proposal_source": "uniform_control",
        }
        evidence = {
            "stage_1_survivor": False,
            "overall": {"signals": 0, "wins": 0},
            "gates": {},
            "candidate_replay_windows": [],
            "fresh_candidate_windows": [],
            "fresh_previously_measured_exclusion": {},
        }
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            snapshot = state_dir / "snapshot.jsonl.gz"
            loop.atomic_gzip_jsonl(snapshot, [])
            config["lanes"]["late_window_mechanisms"]["public_snapshot"] = str(snapshot)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                with mock.patch.object(loop, "LmStudioClient"), mock.patch.object(
                    loop, "propose_late_rule", return_value=(proposal, None, provenance)
                ), mock.patch.object(loop, "evaluate_late_rule", return_value=evidence):
                    result = loop.run_late_window_lane(config, ledger, state_dir, False)
                row = ledger.hypothesis(result["fingerprint"])
            finally:
                ledger.close()
        self.assertEqual(result["status"], "rejected_stage_1")
        self.assertEqual(result["proposal_source"], "uniform_control")
        self.assertIsNone(result["burst"])
        self.assertEqual(row["source"], "uniform_control")

    def test_sampler_models_round_robin_advances_and_persists(self):
        class RecordingClient:
            """Records the model of every call; the reviewer is offline."""

            def __init__(self):
                self.calls = []
                self.rules = iter(list(loop.fallback_late_proposals())[40:])

            def readiness(self):
                return {"ready": True}

            def complete(self, system, user, schema_name, schema, temperature, model=None):
                self.calls.append((schema_name, model))
                if schema_name == "late_window_review_v1":
                    return {"ok": False, "reason": "reviewer_offline"}
                return {"ok": True, "value": next(self.rules)}

        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["generator"]["novelty_gate_enabled"] = False  # embeddings need the network
        config["generator"]["samples_per_burst"] = 1  # nothing queued: every LLM turn bursts
        config["llm"]["sampler_models"] = ["model-a", "model-b"]
        config["llm"]["reviewer_model"] = "reviewer-x"
        client = RecordingClient()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                sampler_models = []
                for index in range(loop.DIAGNOSTIC_PROPOSAL_PREFIX + 5):
                    proposal, review, provenance = loop.propose_late_rule(
                        client, ledger, "snapshot", config, state_dir
                    )
                    sampler_models.append(provenance.get("sampler_model"))
                    ledger.add_hypothesis(
                        "late-%d" % index,
                        "late_window_mechanisms",
                        proposal,
                        review,
                        "rejected_stage_1",
                        None,
                        source=provenance["proposal_source"],
                    )
            finally:
                ledger.close()
            reopened = loop.Ledger(state_dir / "research.sqlite3")
            try:
                cursor_key = "sampler_model_index.late_window_mechanisms"
                persisted = reopened.meta(cursor_key)
                # An empty roster neither picks a model nor moves the cursor.
                self.assertIsNone(
                    loop.factory_generator.next_sampler_model(
                        {"sampler_models": []}, reopened, "late_window_mechanisms"
                    )
                )
                self.assertEqual(reopened.meta(cursor_key), persisted)
                # The cursor is per lane: another lane starts its own rotation
                # instead of continuing (and pinning) this one.
                self.assertEqual(
                    loop.factory_generator.next_sampler_model(
                        config["llm"], reopened, "band_mechanisms"
                    ),
                    "model-a",
                )
                self.assertEqual(reopened.meta(cursor_key), persisted)
            finally:
                reopened.close()
        self.assertEqual(
            sampler_models,
            [None] * loop.DIAGNOSTIC_PROPOSAL_PREFIX + ["model-a", None, "model-b", None, "model-a"],
        )
        self.assertEqual(
            client.calls,
            [
                ("late_window_proposal_v2c", "model-a"),
                ("late_window_review_v1", "reviewer-x"),
                ("late_window_proposal_v2c", "model-b"),
                ("late_window_review_v1", "reviewer-x"),
                ("late_window_proposal_v2c", "model-a"),
                ("late_window_review_v1", "reviewer-x"),
            ],
        )
        self.assertEqual(persisted, "3")

    def test_burst_queue_replays_carry_the_sampler_model(self):
        class BurstClient:
            def __init__(self):
                self.rules = iter(list(loop.fallback_late_proposals())[40:])

            def readiness(self):
                return {"ready": True}

            def complete(self, system, user, schema_name, schema, temperature, model=None):
                if schema_name == "late_window_review_v1":
                    return {"ok": False, "reason": "reviewer_offline"}
                return {"ok": True, "value": next(self.rules)}

        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["generator"]["novelty_gate_enabled"] = False  # embeddings need the network
        config["generator"]["samples_per_burst"] = 3
        config["llm"]["sampler_models"] = ["model-a", "model-b"]
        client = BurstClient()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                provenances = []
                for index in range(loop.DIAGNOSTIC_PROPOSAL_PREFIX + 7):
                    if index == loop.DIAGNOSTIC_PROPOSAL_PREFIX + 1:
                        queued = [
                            json.loads(line)
                            for line in (state_dir / "proposal_queue.jsonl").read_text().splitlines()
                        ]
                    proposal, review, provenance = loop.propose_late_rule(
                        client, ledger, "snapshot", config, state_dir
                    )
                    provenances.append(provenance)
                    ledger.add_hypothesis(
                        "late-%d" % index,
                        "late_window_mechanisms",
                        proposal,
                        review,
                        "rejected_stage_1",
                        None,
                        source=provenance["proposal_source"],
                    )
            finally:
                ledger.close()
        sampled = provenances[loop.DIAGNOSTIC_PROPOSAL_PREFIX :]
        self.assertEqual(
            [item["proposal_source"] for item in sampled],
            ["llm", "uniform_control", "burst_queue", "uniform_control", "burst_queue", "uniform_control", "llm"],
        )
        # Queued survivors keep the model whose burst produced them, so a
        # replay is attributed like the burst turn; the next burst rotates.
        self.assertEqual([entry["sampler_model"] for entry in queued], ["model-a", "model-a"])
        self.assertEqual(
            [item.get("sampler_model") for item in sampled],
            ["model-a", None, "model-a", None, "model-a", None, "model-b"],
        )

    def test_only_a_schema_rejection_falls_back_from_the_constrained_schema(self):
        class FirstSampleFails:
            """Sample 0 returns the given failure; later samples succeed."""

            def __init__(self, failure):
                self.failure = failure
                self.calls = []
                self.rules = iter(list(loop.fallback_late_proposals())[40:])

            def readiness(self):
                return {"ready": True}

            def complete(self, system, user, schema_name, schema, temperature, model=None):
                self.calls.append(schema_name)
                if schema_name == "late_window_review_v1":
                    return {"ok": False, "reason": "reviewer_offline"}
                if len(self.calls) == 1:
                    return dict(self.failure)
                return {"ok": True, "value": next(self.rules)}

        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["generator"]["novelty_gate_enabled"] = False  # embeddings need the network
        config["generator"]["samples_per_burst"] = 3
        config["llm"]["sampler_models"] = ["model-a", "model-b"]
        cases = {
            "cold_timeout": {"ok": False, "reason": "TimeoutError"},
            "cold_load": {
                "ok": False,
                "reason": "HTTPError",
                "http_status": 400,
                "error_body": '{"error":"Failed to load model: LM Link peer_keepalive_timeout"}',
            },
            "schema_rejected": {
                "ok": False,
                "reason": "HTTPError",
                "http_status": 400,
                "error_body": '{"error":"Invalid JSON schema: anyOf is not supported"}',
            },
        }
        results = {}
        for name, failure in cases.items():
            client = FirstSampleFails(failure)
            with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
                state_dir = Path(directory)
                config["state_dir"] = directory
                ledger = loop.Ledger(state_dir / "research.sqlite3")
                try:
                    for index in range(loop.DIAGNOSTIC_PROPOSAL_PREFIX):
                        proposal, review, provenance = loop.propose_late_rule(
                            client, ledger, "snapshot", config, state_dir
                        )
                        ledger.add_hypothesis(
                            "late-%d" % index,
                            "late_window_mechanisms",
                            proposal,
                            review,
                            "rejected_stage_1",
                            None,
                            source=provenance["proposal_source"],
                        )
                    _, _, provenance = loop.propose_late_rule(
                        client, ledger, "snapshot", config, state_dir
                    )
                finally:
                    ledger.close()
            results[name] = (client.calls, provenance)
        # A cold sampler (timeout or model-load error) is a transport failure:
        # the burst keeps the constrained schema and is charged one sample.
        for name in ("cold_timeout", "cold_load"):
            calls, provenance = results[name]
            self.assertEqual(calls, ["late_window_proposal_v2c"] * 3 + ["late_window_review_v1"], name)
            self.assertNotIn("constrained_schema_fallback", provenance, name)
            self.assertTrue(provenance["burst"]["constrained_schema"], name)
            self.assertEqual(
                (provenance["burst"]["generated"], provenance["burst"]["survivors"]), (3, 2), name
            )
            self.assertEqual((provenance["proposal_source"], provenance["sampler_model"]), ("llm", "model-a"))
        # Only a backend that rejects the schema itself falls back to the legacy one.
        calls, provenance = results["schema_rejected"]
        self.assertEqual(
            calls, ["late_window_proposal_v2c"] + ["late_window_proposal_v1"] * 2 + ["late_window_review_v1"]
        )
        self.assertEqual(provenance["constrained_schema_fallback"], "HTTPError")
        self.assertFalse(provenance["burst"]["constrained_schema"])

    def test_model_override_reaches_request_payload(self):
        class FakeResponse(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.close()

        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        config["llm"]["reviewer_model"] = "qwen/qwen3.8-27b"
        envelope = b'{"choices":[{"message":{"content":"{\\"verdict\\":\\"accept\\"}"}}]}'
        schema = {"type": "object", "properties": {}, "additionalProperties": False}
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            client = loop.LmStudioClient(config["llm"], Path(directory))
            with mock.patch.object(
                loop.urllib.request,
                "urlopen",
                side_effect=lambda *args, **kwargs: FakeResponse(envelope),
            ) as opened:
                sampled = client.complete(
                    "Public causal rule only.", "Use public checkpoints.", "test_schema", schema, 0.0
                )
                reviewed = client.complete(
                    "Review a public causal rule.",
                    "proposal=...",
                    "late_window_review_v1",
                    schema,
                    0.0,
                    model=config["llm"]["reviewer_model"],
                )
        payloads = [json.loads(call.args[0].data) for call in opened.call_args_list]
        self.assertEqual(payloads[0]["model"], client.model)
        self.assertEqual(payloads[1]["model"], "qwen/qwen3.8-27b")
        self.assertEqual(sampled["model"], client.model)
        self.assertEqual(reviewed["model"], "qwen/qwen3.8-27b")
        self.assertEqual(reviewed["value"], {"verdict": "accept"})


if __name__ == "__main__":
    unittest.main()
