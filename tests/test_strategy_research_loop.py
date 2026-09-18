from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "strategy_research_loop", ROOT / "scripts/strategy_research_loop.py"
)
assert SPEC and SPEC.loader
loop = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(loop)

DEPLOY_CONFIG = ROOT / "deploy/strategy-research-loop.json"
CELL = {"decision_second": 240, "margin_floor_usd": 75, "favorite_price_cap": 0.97, "patience_s": 15}


def proposal(rule, title="cell"):
    return {"title": title, "rationale": "grammar C", "expected_failure_mode": "chop", "rule": dict(rule)}


class StrategyResearchLoopTest(unittest.TestCase):
    def load_with(self, mutate):
        config = json.loads(DEPLOY_CONFIG.read_text())
        mutate(config)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as handle:
            json.dump(config, handle)
            handle.flush()
            return loop.load_config(Path(handle.name))

    def test_config_contract_is_fail_closed(self):
        config = loop.load_config(DEPLOY_CONFIG)
        # The trimmed shape: nothing of the LLM factory survives in the config.
        self.assertEqual(sorted(config), sorted(loop.CONFIG_BLOCKS))
        self.assertEqual(config["mode"], "research_only")
        self.assertEqual(list(config["lanes"]), ["band_mechanisms"])
        self.assertFalse(config["lanes"]["band_mechanisms"]["enabled"])
        self.assertEqual(config["generator"], {"trial_ledger_enabled": True})
        self.assertEqual(sorted(config["resource_policy"]), sorted(loop.RESOURCE_POLICY_KEYS))
        band = config["lanes"]["band_mechanisms"]
        self.assertEqual(sorted(band["gates"]), sorted(loop.GATE_KEYS))
        self.assertEqual(band["campaign_n"], 64)
        cases = (
            ("research_only", lambda c: c.__setitem__("mode", "live")),
            ("schema_version", lambda c: c.__setitem__("schema_version", 2)),
            ("unknown config blocks", lambda c: c.__setitem__("llm", {"default_model": "x"})),
            ("unknown config blocks", lambda c: c.__setitem__("exact_replay", {"enabled": False})),
            ("unknown config blocks", lambda c: c.__setitem__("architecture_migration", {})),
            ("missing config blocks", lambda c: c.pop("generator")),
            ("lanes must hold exactly", lambda c: c["lanes"].__setitem__("late_window_mechanisms", {"enabled": False})),
            ("lanes must hold exactly", lambda c: c["lanes"].pop("band_mechanisms")),
            ("generator may hold only", lambda c: c["generator"].__setitem__("samples_per_burst", 6)),
            ("trial_ledger_enabled must be a boolean", lambda c: c["generator"].__setitem__("trial_ledger_enabled", 1)),
            ("resource_policy must hold exactly", lambda c: c["resource_policy"].__setitem__("command_timeout_seconds", 900)),
            ("resource_policy must hold exactly", lambda c: c["resource_policy"].pop("minimum_free_disk_gib")),
            ("resource_policy.maximum_load_per_cpu", lambda c: c["resource_policy"].__setitem__("maximum_load_per_cpu", 0)),
            ("gates must hold exactly", lambda c: c["lanes"]["band_mechanisms"]["gates"].pop("minimum_entries")),
            ("gates.minimum_signals must be a positive integer", lambda c: c["lanes"]["band_mechanisms"]["gates"].__setitem__("minimum_signals", 0)),
            ("minimum_interval_seconds must be a positive integer", lambda c: c["lanes"]["band_mechanisms"].__setitem__("minimum_interval_seconds", 0)),
            ("start_ts must be a positive integer", lambda c: c["lanes"]["band_mechanisms"].__setitem__("start_ts", 1.5)),
            ("enabled must be a boolean", lambda c: c["lanes"]["band_mechanisms"].__setitem__("enabled", "yes")),
            ("unknown keys \\['sampler'\\]", lambda c: c["lanes"]["band_mechanisms"].__setitem__("sampler", "llm")),
            ("missing keys \\['gates'\\]", lambda c: c["lanes"]["band_mechanisms"].pop("gates")),
            ("campaign_n must be a positive integer", lambda c: c["lanes"]["band_mechanisms"].__setitem__("campaign_n", 0)),
            ("audit_cleared must be a list", lambda c: c["lanes"]["band_mechanisms"].__setitem__("audit_cleared", "fp")),
            ("entry_semantics must be one of", lambda c: c["lanes"]["band_mechanisms"].__setitem__("entry_semantics", "greedy")),
        )
        for message, mutate in cases:
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                self.load_with(mutate)
        # Optional lane keys are accepted when well-formed; campaign_n may be absent.
        loaded = self.load_with(
            lambda c: c["lanes"]["band_mechanisms"].update({"entry_semantics": "patient", "enabled": True})
        )
        self.assertTrue(loaded["lanes"]["band_mechanisms"]["enabled"])
        unregistered = self.load_with(lambda c: c["lanes"]["band_mechanisms"].pop("campaign_n"))
        self.assertNotIn("campaign_n", unregistered["lanes"]["band_mechanisms"])

    def test_ledger_holds_hypotheses_statuses_and_cycles(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            database = Path(directory) / "research.sqlite3"
            ledger = loop.Ledger(database)
            try:
                fingerprint = loop.stable_hash(proposal(CELL))
                self.assertFalse(ledger.has_hypothesis(fingerprint))
                ledger.add_hypothesis(fingerprint, "band_mechanisms", proposal(CELL), None, "stage_2_survivor", None, source="grid_v2")
                ledger.add_hypothesis("legacy", "band_mechanisms", {"rule": {"margin_floor_usd": 50}}, None, "rejected_signal_screen", Path("/tmp/x.json"))
                ledger.add_hypothesis("late", "late_window_mechanisms", {"rule": {"operator": "x"}}, None, "rejected_stage_1", None)
                with self.assertRaises(loop.sqlite3.IntegrityError):
                    ledger.add_hypothesis(fingerprint, "band_mechanisms", proposal(CELL), None, "accruing", None)
                self.assertTrue(ledger.has_hypothesis(fingerprint))
                ledger.update_hypothesis_status(fingerprint, "accruing")
                row = ledger.hypothesis(fingerprint)
                lane_rows = ledger.lane_hypotheses("band_mechanisms")
                ledger.begin_cycle("c1", {"dry_run": False})
                ledger.finish_cycle("c1", "completed", {"lane": "band_mechanisms"})
                summary = ledger.summary()
                tables = {
                    str(name)
                    for (name,) in ledger.connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
                }
            finally:
                ledger.close()
        self.assertEqual((row["status"], row["source"], row["lane"]), ("accruing", "grid_v2", "band_mechanisms"))
        self.assertEqual(json.loads(row["proposal_json"])["rule"], CELL)
        self.assertEqual([r["fingerprint"] for r in lane_rows], [fingerprint, "legacy"])
        self.assertEqual([r["source"] for r in lane_rows], ["grid_v2", None])
        self.assertEqual(
            sorted((h["lane"], h["status"], h["count"]) for h in summary["hypotheses"]),
            [("band_mechanisms", "accruing", 1), ("band_mechanisms", "rejected_signal_screen", 1), ("late_window_mechanisms", "rejected_stage_1", 1)],
        )
        self.assertEqual((summary["last_cycle"]["cycle_id"], summary["last_cycle"]["status"]), ("c1", "completed"))
        self.assertNotIn("jobs", summary)
        # The LLM-era jobs table is no longer created.
        self.assertEqual(tables, {"meta", "cycles", "hypotheses", "evidence_accrual"})

    def test_evidence_accrual_is_idempotent_on_replay(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            ledger = loop.Ledger(Path(directory) / "research.sqlite3")
            try:
                outcomes = [(300, 0.6, True), (600, 0.6, True), (900, 0.6, False)]
                first = ledger.accrue("accrual", "band_mechanisms", outcomes, 0)
                replay = ledger.accrue("accrual", "band_mechanisms", outcomes, 0)
                extended = ledger.accrue(
                    "accrual",
                    "band_mechanisms",
                    [(1500, 0.6, True), (900, 0.6, True), (1200, 0.6, True)],
                    0,
                )
                row = ledger.accrual("accrual")
                seeded = ledger.accrue("seeded", "band_mechanisms", outcomes, 600)
                seeded_row = ledger.accrual("seeded")
                deleted = ledger.delete_accrual("seeded")
                gone = ledger.accrual("seeded")
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
        self.assertEqual((deleted, gone), (1, None))

    def test_structural_novelty_rejects_hamming_neighbours_of_signal_kills(self):
        generator = loop.factory_generator
        killed = [
            {
                "kind": "ledger_rule",
                "rule": "band t=240s floor=$75 cap=0.97 patience=15s",
                "rule_fields": dict(CELL),
                "status": "rejected_signal_screen",
            }
        ]
        same = dict(CELL)
        one_off = {**CELL, "patience_s": 30}
        two_off = {**CELL, "patience_s": 30, "favorite_price_cap": 0.99}
        verdicts = [generator.structural_novelty(rule, killed) for rule in (same, one_off, two_off)]
        self.assertEqual([v["status"] for v in verdicts], ["rejected", "rejected", "accepted"])
        self.assertEqual([v["min_hamming"] for v in verdicts], [0, 1, 2])
        self.assertEqual(verdicts[0]["against"]["status"], "rejected_signal_screen")
        self.assertEqual(generator.structural_novelty(same, []), {"status": "accepted", "gate": "structural", "min_hamming": None})
        # A support/economics rejection does not mark its neighbourhood dead.
        support_killed = [dict(killed[0], status="rejected_entry_economics")]
        self.assertEqual(generator.structural_novelty(one_off, support_killed)["status"], "accepted")
        self.assertEqual(generator.structural_novelty(one_off, killed, min_hamming=1)["status"], "accepted")
        self.assertTrue(generator.novelty_blocking_status("killed_futility"))
        self.assertTrue(generator.novelty_blocking_status("rejected_signal_screen"))
        self.assertFalse(generator.novelty_blocking_status("rejected_entry_economics"))
        self.assertEqual(generator.generator_config({"generator": {"trial_ledger_enabled": True}}), {"trial_ledger_enabled": True})
        self.assertEqual(generator.generator_config(None), generator.DEFAULTS)

    def test_cycle_lock_is_exclusive(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            path = Path(directory) / "locks/cycle.lock"
            with loop.CycleLock(path):
                with self.assertRaisesRegex(RuntimeError, "another strategy research cycle is active"):
                    with loop.CycleLock(path):
                        pass
            with loop.CycleLock(path):
                pass

    def test_run_cycle_runs_only_the_band_lane_behind_the_resource_gate(self):
        config = loop.load_config(DEPLOY_CONFIG)
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            state_dir = Path(directory)
            self.assertEqual(loop.status(config)["status"], "not_initialized")
            passed = {"passed": True, "checks": {"free_disk": True, "load": True, "dev_box": True}}
            with mock.patch.object(loop, "resource_status", return_value=passed), mock.patch.object(
                loop, "run_band_lane", return_value={"status": "not_due"}
            ) as lane:
                result = loop.run_cycle(config, False, "band_mechanisms")
                unselected = loop.run_cycle(config, True, None)
            lane.assert_has_calls([mock.call(config, mock.ANY, state_dir, False), mock.call(config, mock.ANY, state_dir, True)])
            self.assertEqual(lane.call_count, 2)
            failed = {"passed": False, "checks": {"free_disk": False, "load": True, "dev_box": True}}
            with mock.patch.object(loop, "resource_status", return_value=failed), mock.patch.object(
                loop.band_lane, "BandCache"
            ) as cache:
                deferred = loop.run_cycle(config, False, "band_mechanisms")
            cache.assert_not_called()
            with self.assertRaisesRegex(ValueError, "unknown lane"):
                loop.run_cycle(config, False, "late_window_mechanisms")
            written = json.loads((state_dir / "status.json").read_text())
            current = loop.status(config)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                cycles = [(r["status"], json.loads(r["details_json"]).get("lane_result", {}).get("status")) for r in ledger.connection.execute("SELECT status, details_json FROM cycles ORDER BY rowid")]
            finally:
                ledger.close()
        self.assertEqual((result["lane"], result["lane_result"]), ("band_mechanisms", {"status": "not_due"}))
        self.assertEqual(unselected["lane"], "band_mechanisms")
        self.assertTrue(unselected["dry_run"])
        self.assertEqual(deferred["lane_result"], {"status": "deferred_resource_gate", "failed_checks": ["free_disk"]})
        for key in ("public_snapshot", "registry_audit", "economic_screen_job", "fresh_public_accrual", "exact_job"):
            self.assertNotIn(key, deferred)
        self.assertEqual(written["cycle_id"], deferred["cycle_id"])
        self.assertEqual(written["ledger"]["hypotheses"], [])
        self.assertEqual(current["status"], "initialized")
        self.assertEqual(cycles, [("completed", "not_due"), ("completed", "not_due"), ("completed", "deferred_resource_gate")])


if __name__ == "__main__":
    unittest.main()
