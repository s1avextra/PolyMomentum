from __future__ import annotations

import importlib.util
import itertools
import json
import math
from pathlib import Path
import sqlite3
import statistics
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def _load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


band = _load("band_lane", "scripts/band_lane.py")
loop = _load("strategy_research_loop", "scripts/strategy_research_loop.py")

LIVE_RULE = {
    "margin_floor_usd": 50,
    "margin_floor_sigma": 0.0,
    "decision_second": 240,
    "direction": "both",
    "favorite_price_floor": 0.55,
    "favorite_price_cap": 0.92,
}
BASE_WS = 1787788800  # 2026-08-25T00:00Z, the fixture epoch
GATES = {"minimum_signals": 100, "minimum_recent_signals": 20, "minimum_entries": 50}


def window(ws, margin=60.0, official="up", open_=70000.0, margins=None):
    margins = margins or {}
    return {
        "window_start": ws,
        "open": open_,
        "closes": {d: open_ + margins.get(d, margin) for d in band.BAND_DECISION_SECONDS},
        "official": official,
    }


def windows(count, **kwargs):
    return [window(BASE_WS + index * 300, **kwargs) for index in range(count)]


def proposal(rule, title="sampled"):
    return {
        "title": title,
        "rationale": "public momentum band",
        "expected_failure_mode": "chop",
        "rule": dict(rule),
    }


class StubClient:
    """LmStudioClient stand-in: hands out distinct grid rules in order."""

    def __init__(self, ready=True):
        self.ready = ready
        self.calls = 0
        self.rules = iter(band.grid_rules())

    def readiness(self):
        return {"ready": self.ready}

    def complete(self, system, user, schema_name, schema, temperature, model=None):
        self.calls += 1
        self.last_prompt = (system, user, schema_name, schema, temperature)
        return {"ok": True, "value": proposal(next(self.rules))}


def band_config(**lane_overrides):
    config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
    # Fixtures are built from BASE_WS; the deployed start_ts may move earlier.
    config["lanes"]["band_mechanisms"]["start_ts"] = BASE_WS
    config["generator"]["novelty_gate_enabled"] = False  # embeddings need the network
    config["lanes"]["band_mechanisms"].update(lane_overrides)
    return config


class BandLaneTest(unittest.TestCase):
    # --- grammar -------------------------------------------------------------

    def test_schema_enums_match_grammar_and_validation_is_strict(self):
        properties = band.BAND_PROPOSAL_SCHEMA["properties"]["rule"]["properties"]
        self.assertEqual(set(properties), set(band.BAND_GRID))
        for field, values in band.BAND_GRID.items():
            self.assertEqual(properties[field]["enum"], list(values))
        self.assertEqual(
            band.BAND_PROPOSAL_SCHEMA["properties"]["rule"]["required"], list(band.BAND_GRID)
        )
        self.assertFalse(band.BAND_PROPOSAL_SCHEMA["additionalProperties"])
        combos = list(itertools.product(*band.BAND_GRID.values()))
        self.assertEqual(len(combos), 2880)
        for values in combos:
            rule = dict(zip(band.BAND_GRID, values))
            self.assertEqual(band.validate_band_proposal(proposal(rule))["rule"], rule)
        self.assertEqual(band.normalized_band_rule({**LIVE_RULE, "margin_floor_usd": 50.0}), LIVE_RULE)
        for broken in (
            {**LIVE_RULE, "margin_floor_usd": 60},
            {**LIVE_RULE, "margin_floor_usd": "50"},
            {**LIVE_RULE, "margin_floor_usd": 50.5},
            {**LIVE_RULE, "margin_floor_sigma": "0.5"},
            {**LIVE_RULE, "decision_second": True},
            {**LIVE_RULE, "direction": "sideways"},
            {**LIVE_RULE, "favorite_price_cap": 0.95},
            {key: value for key, value in LIVE_RULE.items() if key != "direction"},
            {**LIVE_RULE, "extra": 1},
        ):
            with self.assertRaises(ValueError):
                band.normalized_band_rule(broken)
        with self.assertRaisesRegex(ValueError, "unexpected"):
            band.validate_band_proposal({**proposal(LIVE_RULE), "command": "x"})
        with self.assertRaisesRegex(ValueError, "non-empty"):
            band.validate_band_proposal({**proposal(LIVE_RULE), "title": " "})
        for values in band.BAND_GRID.values():
            for value in values:
                self.assertIn(str(value), band.BAND_SYSTEM_PROMPT)
        for token in ("pnl", "wallet", "secret", "private_key"):
            self.assertNotIn(token, band.BAND_SYSTEM_PROMPT.lower())

    def test_fingerprint_is_stable(self):
        pinned = "b2bed36ed28ec5a8550073a2430dd35b2e53d2881f8aefaf5823c3b1ea602ae1"
        self.assertEqual(band.band_fingerprint(LIVE_RULE), pinned)
        reordered = dict(reversed(list(LIVE_RULE.items())))
        self.assertEqual(band.band_fingerprint(reordered), pinned)
        self.assertEqual(band.band_fingerprint({**LIVE_RULE, "margin_floor_usd": 50.0}), pinned)
        self.assertEqual(
            band.band_fingerprint(LIVE_RULE),
            loop.stable_hash(
                {"lane": "band_mechanisms", "rule": LIVE_RULE, "evaluator_version": "band_public_v1"}
            ),
        )
        self.assertNotEqual(band.band_fingerprint({**LIVE_RULE, "margin_floor_usd": 75}), pinned)

    # --- evaluator -----------------------------------------------------------

    def test_signal_records_cannot_see_the_label(self):
        unlabelled = [
            {key: value for key, value in row.items() if key != "official"}
            for row in windows(30, margin=60.0)
        ]
        records = band.band_signal_records(unlabelled, LIVE_RULE)
        self.assertEqual(len(records), 30)
        self.assertEqual({tuple(sorted(record)) for record in records},
                         {("direction", "margin", "sigma", "window_start")})
        now_ts = BASE_WS + 30 * 300
        truthful = band.evaluate_band_rule(windows(30, official="up"), {}, LIVE_RULE, GATES, now_ts)
        flipped = band.evaluate_band_rule(windows(30, official="down"), {}, LIVE_RULE, GATES, now_ts)
        self.assertEqual(truthful["stage_1"]["overall"]["signals"], 30)
        self.assertEqual(flipped["stage_1"]["overall"]["signals"], 30)
        self.assertEqual(truthful["stage_1"]["overall"]["wins"], 30)
        self.assertEqual(flipped["stage_1"]["overall"]["wins"], 0)
        unresolved = band.evaluate_band_rule(windows(30, official=None), {}, LIVE_RULE, GATES, now_ts)
        self.assertEqual(unresolved["stage_1"]["overall"]["signals"], 0)
        self.assertEqual(unresolved["labelled_window_count"], 0)

    def test_sigma_uses_only_prior_windows(self):
        rows = [window(BASE_WS + index * 300, margin=10.0 if index % 2 else 30.0) for index in range(12)]
        rows.append(window(BASE_WS + 12 * 300, margin=1000.0))
        rows.append(window(BASE_WS + 13 * 300, margin=60.0))
        rule = {**LIVE_RULE, "margin_floor_usd": 0, "margin_floor_sigma": 1.0}
        records = {record["window_start"]: record for record in band.band_signal_records(rows, rule)}
        # The first twelve windows have no trailing history: excluded.
        self.assertEqual(sorted(records), [BASE_WS + 12 * 300])
        self.assertEqual(records[BASE_WS + 12 * 300]["sigma"], statistics.pstdev([30.0, 10.0] * 6))
        # Window 14 sees the 1000 spike in its trailing sigma (prior windows only).
        expected_sigma_14 = statistics.pstdev([10.0, 30.0] * 5 + [10.0, 1000.0])
        relaxed = band.band_signal_records(rows, {**rule, "margin_floor_sigma": 0.0})
        self.assertEqual([record["sigma"] for record in relaxed[-2:]],
                         [statistics.pstdev([30.0, 10.0] * 6), expected_sigma_14])
        self.assertEqual(len(relaxed), 14)
        # The spike is only rejected once it enters the history, never for itself.
        self.assertLess(60.0, expected_sigma_14)
        self.assertGreaterEqual(1000.0, statistics.pstdev([30.0, 10.0] * 6))

    def test_stage_1_gates(self):
        now_ts = BASE_WS + 120 * 300
        passing = band.evaluate_band_rule(windows(120), {}, LIVE_RULE, GATES, now_ts)
        stage_1 = passing["stage_1"]
        self.assertTrue(stage_1["survivor"])
        self.assertEqual(stage_1["gates"], {
            "support": True,
            "wilson_above_break_even": True,
            "recent_support": True,
            "recent_above_break_even": True,
        })
        self.assertAlmostEqual(stage_1["break_even_at_cap"], 0.92 + 0.072 * 0.92 * 0.08)
        self.assertEqual(stage_1["by_margin_bucket"]["50-75"]["signals"], 120)
        self.assertEqual(stage_1["by_margin_bucket"]["100-inf"]["signals"], 0)
        self.assertEqual(passing["last_window_start"], BASE_WS + 119 * 300)
        short = band.evaluate_band_rule(windows(99), {}, LIVE_RULE, GATES, now_ts)
        self.assertFalse(short["stage_1"]["gates"]["support"])
        self.assertIsNone(short["stage_2"])
        stricter = band.evaluate_band_rule(windows(120), {}, LIVE_RULE, {**GATES, "minimum_signals": 121}, now_ts)
        self.assertFalse(stricter["stage_1"]["survivor"])
        stale = band.evaluate_band_rule(windows(120), {}, LIVE_RULE, GATES, now_ts + 3 * 86400)
        self.assertEqual(stale["stage_1"]["recent_48h"]["signals"], 0)
        self.assertFalse(stale["stage_1"]["gates"]["recent_support"])
        self.assertFalse(stale["stage_1"]["gates"]["recent_above_break_even"])
        mixed = [window(BASE_WS + index * 300, official="up" if index % 10 else "down") for index in range(120)]
        noisy = band.evaluate_band_rule(mixed, {}, LIVE_RULE, GATES, now_ts)
        self.assertEqual(noisy["stage_1"]["overall"]["wins"], 108)
        self.assertFalse(noisy["stage_1"]["gates"]["wilson_above_break_even"])
        self.assertFalse(noisy["stage_1"]["survivor"])
        below_floor = band.evaluate_band_rule(windows(120, margin=40.0), {}, LIVE_RULE, GATES, now_ts)
        self.assertEqual(below_floor["stage_1"]["overall"]["signals"], 0)
        down_only = band.evaluate_band_rule(windows(120), {}, {**LIVE_RULE, "direction": "down"}, GATES, now_ts)
        self.assertEqual(down_only["stage_1"]["overall"]["signals"], 0)

    def test_stage_2_runs_only_for_survivors(self):
        class RecordingPrints(dict):
            reads = 0

            def get(self, key, default=None):
                RecordingPrints.reads += 1
                return super().get(key, default)

        now_ts = BASE_WS + 120 * 300
        prints = RecordingPrints()
        for index in range(120):
            ws = BASE_WS + index * 300
            if index < 60:
                prints[(ws, 240)] = {"status": "ok", "signal": "up", "signal_entry": 0.80}
            elif index < 70:
                prints[(ws, 240)] = {"status": "ok", "signal": "up", "signal_entry": 0.95}
            elif index < 80:
                prints[(ws, 240)] = {"status": "ok", "signal": "up", "signal_entry": None}
        rejected = band.evaluate_band_rule(windows(99), prints, LIVE_RULE, GATES, now_ts)
        self.assertIsNone(rejected["stage_2"])
        self.assertEqual(RecordingPrints.reads, 0)
        survivor = band.evaluate_band_rule(windows(120), prints, LIVE_RULE, GATES, now_ts)
        stage_2 = survivor["stage_2"]
        self.assertEqual(RecordingPrints.reads, 120)
        even = 0.80 + 0.072 * 0.80 * 0.20
        self.assertEqual((stage_2["entries"], stage_2["wins"]), (60, 60))
        self.assertAlmostEqual(stage_2["mean_break_even"], even)
        self.assertAlmostEqual(stage_2["mean_net_per_usd"], 1.0 / even - 1.0)
        self.assertEqual(
            (stage_2["out_of_band_prints"], stage_2["windows_without_print"], stage_2["uncached_windows"]),
            (10, 10, 40),
        )
        self.assertTrue(stage_2["survivor"] and survivor["survivor"])
        thin = band.evaluate_band_rule(windows(120), prints, LIVE_RULE, {**GATES, "minimum_entries": 61}, now_ts)
        self.assertFalse(thin["stage_2"]["gates"]["support"])
        self.assertFalse(thin["survivor"])
        # Stage 1 passes at a 0.80 cap (106/120, Wilson 0.8137 >= 0.8115) while
        # the 50 windows with prints hold every loss: stage 2 fails both gates.
        cheap = {**LIVE_RULE, "favorite_price_cap": 0.80}
        losses = [window(BASE_WS + index * 300, official="down" if index < 14 else "up") for index in range(120)]
        losing_prints = {
            (BASE_WS + index * 300, 240): {"status": "ok", "signal": "up", "signal_entry": 0.80}
            for index in range(50)
        }
        losing = band.evaluate_band_rule(losses, losing_prints, cheap, GATES, now_ts)
        self.assertTrue(losing["stage_1"]["survivor"])
        self.assertEqual((losing["stage_2"]["entries"], losing["stage_2"]["wins"]), (50, 36))
        self.assertEqual(
            losing["stage_2"]["gates"],
            {"support": True, "wilson_above_break_even": False, "positive_mean_net": False},
        )
        self.assertLess(losing["stage_2"]["mean_net_per_usd"], 0.0)
        self.assertFalse(losing["survivor"])

    def test_accrual_outcomes_score_only_the_stage_2_population(self):
        rows = windows(6)
        starts = [row["window_start"] for row in rows]
        prints = {
            (starts[3], 240): {"status": "ok", "signal": "up", "signal_entry": 0.70},
            (starts[4], 240): {"status": "ok", "signal": "up", "signal_entry": 0.95},
            (starts[5], 240): {"status": "ok", "signal": "up", "signal_entry": None},
        }
        # Uncached, no-print and out-of-band windows are not trades: no evidence.
        outcomes = band.band_accrual_outcomes(rows, prints, LIVE_RULE, starts[1])
        self.assertEqual(outcomes, [(starts[3], band.break_even(0.70), True)])
        scored = band._labelled(rows, band.band_signal_records(rows, LIVE_RULE))
        self.assertEqual(
            len(band.band_accrual_outcomes(rows, prints, LIVE_RULE, -1)),
            band.band_entry_economics(scored, prints, LIVE_RULE, GATES)["entries"],
        )

    def test_entry_print_excludes_prints_stamped_at_the_decision_second(self):
        decision_ts = BASE_WS + 240
        trades = [
            {"side": "BUY", "asset": "up", "timestamp": decision_ts, "price": 0.70},
            {"side": "SELL", "asset": "up", "timestamp": decision_ts + 1, "price": 0.71},
            {"side": "BUY", "asset": "down", "timestamp": decision_ts + 1, "price": 0.30},
            {"side": "BUY", "asset": "up", "timestamp": decision_ts + 30, "price": 0.80},
            {"side": "BUY", "asset": "up", "timestamp": decision_ts + 31, "price": 0.90},
        ]
        # The 1s close at decision_ts is only known once that second ends.
        self.assertIsNone(band.entry_print(trades[:1], "up", decision_ts))
        self.assertEqual(band.entry_print(trades, "up", decision_ts), 0.80)
        self.assertEqual(band.entry_print(trades, "down", decision_ts), 0.30)
        # The full entry-window list: BUYs of the token in (decision, decision + 30].
        self.assertEqual(band.entry_prints(trades, "up", decision_ts), [[30, 0.80]])
        self.assertEqual(band.entry_prints(trades, "down", decision_ts), [[1, 0.30]])
        self.assertEqual(band.entry_prints(trades[:1], "up", decision_ts), [])

    # --- proposer ------------------------------------------------------------

    def test_proposer_priors_then_strict_alternation(self):
        config = band_config()
        client = StubClient()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                sources = []
                rules = []
                for _ in range(9):
                    proposed, provenance = band.propose_band_rule(client, ledger, config, state_dir, [])
                    sources.append(provenance["proposal_source"])
                    rules.append(proposed["rule"])
                    ledger.add_hypothesis(
                        band.band_fingerprint(proposed["rule"]),
                        "band_mechanisms",
                        proposed,
                        None,
                        "rejected_signal_screen",
                        None,
                        source=provenance["proposal_source"],
                    )
                    if provenance["proposal_source"] == "llm":
                        self.assertEqual(client.last_prompt[3], band.BAND_PROPOSAL_SCHEMA)
                        self.assertEqual(client.last_prompt[4], config["generator"]["explore_temperature"])
                queue = (state_dir / band.BAND_QUEUE_FILE).read_text().splitlines()
                stored = [ledger.hypothesis(band.band_fingerprint(rule))["source"] for rule in rules]
                cold = StubClient(ready=False)
                offline, offline_provenance = band.propose_band_rule(cold, ledger, config, state_dir, [])
                lane_rows = ledger.lane_hypotheses("band_mechanisms")
            finally:
                ledger.close()
        expected_priors = [dict(zip(band.BAND_GRID, prior)) for prior in band.BAND_PRIORS]
        self.assertEqual(rules[:3], expected_priors)
        self.assertEqual(
            sources,
            ["prior"] * 3 + ["llm", "uniform_control", "llm", "uniform_control", "llm", "uniform_control"],
        )
        self.assertEqual(stored, sources)
        self.assertEqual(len(set(band.band_fingerprint(rule) for rule in rules)), 9)
        for rule in rules[3:]:
            self.assertEqual(band.normalized_band_rule(rule), rule)
        # One burst of samples_per_burst survivors: the first is proposed, the
        # rest are queued and replayed on later LLM turns before sampling again.
        self.assertEqual(client.calls, config["generator"]["samples_per_burst"])
        self.assertEqual(len(queue), config["generator"]["samples_per_burst"] - 3)
        # Uniform control draws are seeded by the hypothesis count: replayable.
        self.assertEqual(
            band.uniform_control_rule(4, lambda rule: False),
            band.uniform_control_rule(4, lambda rule: False),
        )
        self.assertEqual(sources[4], "uniform_control")
        # LLM turn with no ready model degrades to a seeded control draw.
        self.assertEqual(len(lane_rows), 9)
        self.assertEqual(offline_provenance["turn"], "llm")
        # An LLM turn without a model falls through to the control arm, not the grid.
        self.assertEqual(offline_provenance["proposal_source"], "uniform_control")
        self.assertNotIn(offline["rule"], rules)
        self.assertEqual(band.normalized_band_rule(offline["rule"]), offline["rule"])

    def test_burst_queue_replays_carry_the_sampler_model_with_a_lane_cursor(self):
        config = band_config()
        config["generator"]["samples_per_burst"] = 2
        config["llm"]["sampler_models"] = ["model-a", "model-b"]
        client = StubClient()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                provenances = []
                for index in range(len(band.BAND_PRIORS) + 6):
                    if index == len(band.BAND_PRIORS) + 1:
                        queued = [
                            json.loads(line)
                            for line in (state_dir / band.BAND_QUEUE_FILE).read_text().splitlines()
                        ]
                    proposed, provenance = band.propose_band_rule(client, ledger, config, state_dir, [])
                    provenances.append(provenance)
                    ledger.add_hypothesis(
                        band.band_fingerprint(proposed["rule"]),
                        "band_mechanisms",
                        proposed,
                        None,
                        "rejected_signal_screen",
                        None,
                        source=provenance["proposal_source"],
                    )
                cursors = {
                    lane: ledger.meta("sampler_model_index.%s" % lane)
                    for lane in ("band_mechanisms", "late_window_mechanisms")
                }
            finally:
                ledger.close()
        sampled = provenances[len(band.BAND_PRIORS) :]
        self.assertEqual([item["proposal_source"] for item in sampled], ["llm", "uniform_control"] * 3)
        self.assertEqual(
            [item.get("from_burst_queue", False) for item in sampled],
            [False, False, True, False, False, False],
        )
        # The replay is attributed to the burst's model; the cursor that
        # rotated to model-b is the band lane's own, not the late lane's.
        self.assertEqual([entry["sampler_model"] for entry in queued], ["model-a"])
        self.assertEqual(
            [item.get("sampler_model") for item in sampled],
            ["model-a", None, "model-a", None, "model-b", None],
        )
        self.assertEqual(cursors, {"band_mechanisms": "2", "late_window_mechanisms": None})

    def test_llm_prompt_carries_public_aggregates_and_negatives(self):
        config = band_config()
        client = StubClient()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                for index, prior in enumerate(band.BAND_PRIORS):
                    rule = dict(zip(band.BAND_GRID, prior))
                    fingerprint = band.band_fingerprint(rule)
                    evidence_path = state_dir / ("evidence/band_mechanisms/%s.json" % fingerprint)
                    loop.atomic_json(
                        evidence_path,
                        {
                            "stage_1": {"overall": {"signals": 100, "wins": 90 - index, "accuracy": 0.9, "wilson_lower": 0.8 - index * 0.1}},
                            "stage_2": {"entries": 50, "mean_net_per_usd": 0.05},
                        },
                    )
                    ledger.add_hypothesis(
                        fingerprint,
                        "band_mechanisms",
                        proposal(rule),
                        None,
                        "stage_2_survivor" if index == 0 else "rejected_signal_screen",
                        evidence_path,
                        source="prior",
                    )
                rows = windows(30, margin=60.0) + windows(0)
                proposed, provenance = band.propose_band_rule(client, ledger, config, state_dir, rows)
            finally:
                ledger.close()
        self.assertEqual(provenance["proposal_source"], "llm")
        system, user = client.last_prompt[0], client.last_prompt[1]
        self.assertEqual(system, band.BAND_SYSTEM_PROMPT)
        self.assertIn("decision_second=240: 50-75: 100.0% (n=30)", user)
        self.assertIn("Parent rules ranked", user)
        self.assertIn("wilson_lower=0.800", user)
        self.assertIn("KILLED", user)
        self.assertIn("band floor=$75", user)
        for token in ("pnl", "wallet", "secret", "private_key"):
            self.assertNotIn(token, (system + user).lower())

    # --- ledger --------------------------------------------------------------

    def test_ledger_source_column_migrates_and_persists(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            database = Path(directory) / "research.sqlite3"
            legacy = sqlite3.connect(str(database))
            legacy.executescript(
                """
                CREATE TABLE hypotheses (
                    fingerprint TEXT PRIMARY KEY,
                    lane TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    proposal_json TEXT NOT NULL,
                    review_json TEXT,
                    status TEXT NOT NULL,
                    evidence_path TEXT
                );
                INSERT INTO hypotheses VALUES('old', 'late_window_mechanisms', '2026-01-01T00:00:00+00:00', '{}', NULL, 'rejected_stage_1', NULL);
                """
            )
            legacy.commit()
            legacy.close()
            ledger = loop.Ledger(database)
            try:
                ledger.add_hypothesis("new", "band_mechanisms", proposal(LIVE_RULE), None, "accruing", None, source="prior")
                ledger.add_hypothesis("plain", "late_window_mechanisms", {"rule": {}}, None, "rejected_stage_1", None)
                old = ledger.hypothesis("old")
                new = ledger.hypothesis("new")
                plain = ledger.hypothesis("plain")
                reopened = loop.Ledger(database)  # the ALTER TABLE is guarded
                reopened.close()
                lane_rows = ledger.lane_hypotheses("band_mechanisms")
            finally:
                ledger.close()
        self.assertIsNone(old["source"])
        self.assertEqual(new["source"], "prior")
        self.assertIsNone(plain["source"])
        self.assertEqual([row["fingerprint"] for row in lane_rows], ["new"])

    # --- cache and lane ------------------------------------------------------

    def disarm_tripwire(self):
        """The all-win stub fixture (130/130) trips the audit tripwire; the
        pipeline tests below patch it out, test_tripwire_* covers it."""
        self.enterContext(mock.patch.object(band, "TRIPWIRE_MINIMUM_N", 10**9))

    def stub_cache(self, directory, failing=(), unresolved=(), missing=(), closes_failing=(), price=0.80):
        """Network-free BandCache: every window is +60 at every decision second
        and resolves up; the first BUY of the up token prints at `price`."""

        def fetch_closes(start_ts, end_ts):
            if start_ts - start_ts % band.DAY_S in closes_failing:
                raise OSError("binance down")
            return {str(ts): 70000.0 + (60.0 if ts % 300 >= 150 else 0.0) for ts in range(start_ts, end_ts)}

        def fetch_market(ws):
            if ws in failing:
                raise OSError("gamma down")
            if ws in missing:
                return None
            return {
                "conditionId": "0x%d" % ws,
                "outcomes": '["Up", "Down"]',
                "clobTokenIds": '["up-%d", "down-%d"]' % (ws, ws),
                "umaResolutionStatus": "pending" if ws in unresolved else "resolved",
                "outcomePrices": '["1", "0"]',
            }

        def fetch_trades(condition_id, window_start):
            ws = int(condition_id[2:])
            self.assertEqual(window_start, ws)
            trades = [
                {"side": "BUY", "asset": "up-%d" % ws, "timestamp": ws + d + 5, "price": price}
                for d in band.BAND_DECISION_SECONDS
            ] + [{"side": "SELL", "asset": "up-%d" % ws, "timestamp": ws + 241, "price": 0.5}]
            return {"trades": trades, "coverage": {"oldest_offset_s": 185, "pages": 1, "complete": True}}

        return band.BandCache(
            margin_dir=Path(directory) / "margin",
            prints_dir=Path(directory) / "prints",
            fetch_closes=fetch_closes,
            fetch_market=fetch_market,
            fetch_trades=fetch_trades,
        )

    def test_cache_refresh_is_bounded_oldest_first_and_contiguous(self):
        now_ts = BASE_WS + 10 * 300 + 1200
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            cache = self.stub_cache(directory, failing={BASE_WS + 3 * 300}, unresolved={BASE_WS + 7 * 300})
            first = cache.refresh(BASE_WS, now_ts, budget=2)
            first_windows = cache.windows(BASE_WS, now_ts)
            second = cache.refresh(BASE_WS, now_ts, budget=100)
            second_windows = cache.windows(BASE_WS, now_ts)
            cache = self.stub_cache(directory, unresolved={BASE_WS + 7 * 300})
            third = cache.refresh(BASE_WS, now_ts, budget=100)
            third_windows = cache.windows(BASE_WS, now_ts)
            outcomes = json.loads((Path(directory) / "margin/gamma_outcomes.json").read_text())
            day_file = json.loads((Path(directory) / ("margin/binance_%d.json" % BASE_WS)).read_text())
            print_row = json.loads((Path(directory) / ("prints/%d_240.json" % BASE_WS)).read_text())
            prints = cache.load_prints(third_windows)
        self.assertEqual((first["fetched"], first["remaining"], first["stopped"]), (2, 9, None))
        self.assertEqual([row["window_start"] for row in first_windows], [BASE_WS, BASE_WS + 300])
        # The failing window stops the scan: nothing after it is fetched.
        self.assertEqual((second["fetched"], second["remaining"]), (1, 8))
        self.assertEqual(second["stopped"], "market_fetch_OSError")
        self.assertEqual(len(second_windows), 3)
        # Young unresolved window: everything before it settles, it waits.
        self.assertEqual((third["fetched"], third["remaining"], third["stopped"]), (5, 4, "awaiting_resolution"))
        self.assertEqual([row["window_start"] for row in third_windows], [BASE_WS + index * 300 for index in range(7)])
        self.assertEqual(set(outcomes), {str(BASE_WS + index * 300) for index in range(7)})
        self.assertEqual(set(outcomes.values()), {"up"})
        self.assertEqual(len(day_file), now_ts - BASE_WS)
        self.assertEqual(
            print_row,
            {
                "window_start": BASE_WS,
                "decision_second": 240,
                "status": "ok",
                "signal": "up",
                "signal_entry": 0.80,
                "signal_prints": [[5, 0.80]],
                "coverage": {"oldest_offset_s": 185, "pages": 1, "complete": True},
            },
        )
        self.assertEqual(len(prints), 7 * len(band.BAND_DECISION_SECONDS))
        self.assertEqual(third_windows[0]["closes"], {180: 70060.0, 210: 70060.0, 240: 70060.0, 270: 70060.0})
        self.assertEqual(third_windows[0]["official"], "up")

    def test_missing_market_waits_for_the_grace_period(self):
        # Gamma lists a window under closed=true only once it resolves, so an
        # empty answer for a young window means "still resolving", not absent.
        target = BASE_WS + 4 * 300
        now_ts = BASE_WS + 10 * 300 + 1200
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            cache = self.stub_cache(directory, missing={target})
            waiting = cache.refresh(BASE_WS, now_ts, budget=100)
            waiting_windows = cache.windows(BASE_WS, now_ts)
            waiting_outcomes = dict(cache.outcomes)
            waiting_prints = cache.has_prints(target)
            listed = self.stub_cache(directory)
            settled = listed.refresh(BASE_WS, now_ts, budget=100)
            settled_windows = listed.windows(BASE_WS, now_ts)
            # Still unlisted once the grace period has passed: final null.
            stale = BASE_WS + 11 * 300
            later = now_ts + band.UNRESOLVED_FINAL_AFTER_S
            gone = self.stub_cache(directory, missing={stale})
            final = gone.refresh(BASE_WS, later, budget=100)
            final_windows = gone.windows(BASE_WS, later)
            stale_print = json.loads((Path(directory) / ("prints/%d_240.json" % stale)).read_text())
        self.assertEqual((waiting["fetched"], waiting["remaining"], waiting["stopped"]), (5, 7, "awaiting_market"))
        self.assertEqual([row["window_start"] for row in waiting_windows], [BASE_WS + index * 300 for index in range(4)])
        self.assertNotIn(str(target), waiting_outcomes)
        self.assertFalse(waiting_prints)
        self.assertEqual((settled["fetched"], settled["remaining"], settled["stopped"]), (7, 0, None))
        self.assertEqual([row["official"] for row in settled_windows], ["up"] * 11)
        self.assertEqual((final["fetched"], final["remaining"], final["stopped"]), (24, 0, None))
        self.assertIsNone(gone.outcomes[str(stale)])
        self.assertEqual(stale_print["status"], "market_not_found")
        self.assertEqual(len(final_windows), 35)

    def test_closes_fetch_error_stops_the_scan_instead_of_skipping_the_day(self):
        day_b = BASE_WS + band.DAY_S
        start_ts = day_b - 10 * 300
        # Four windows into day C: days A and B are fully eligible.
        now_ts = day_b + band.DAY_S + 4 * 300 + 1200
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            broken = self.stub_cache(directory, closes_failing={day_b})
            first = broken.refresh(start_ts, now_ts, budget=1000)
            first_windows = broken.windows(start_ts, now_ts)
            day_files = sorted(path.name for path in (Path(directory) / "margin").glob("binance_*.json"))
            recovered = self.stub_cache(directory)
            second = recovered.refresh(start_ts, now_ts, budget=1000)
            second_windows = recovered.windows(start_ts, now_ts)
        # Day B fails: the scan waits there rather than settling day C past a hole.
        self.assertEqual(first["closes_errors"], ["%d: OSError" % day_b])
        self.assertEqual((first["fetched"], first["remaining"], first["stopped"]), (10, 293, "closes_unavailable"))
        self.assertEqual(first_windows[-1]["window_start"], day_b - 300)
        self.assertEqual(day_files, ["binance_%d.json" % BASE_WS])
        self.assertEqual((second["fetched"], second["remaining"], second["stopped"]), (293, 0, None))
        self.assertEqual(len(second_windows), 303)
        self.assertEqual(second_windows[-1]["window_start"], now_ts - 1200)

    def test_dry_run_proposes_from_cached_windows_without_network(self):
        config = band_config(enabled=True, maximum_new_windows_per_cycle=1000, minimum_interval_seconds=0)
        now_ts = BASE_WS + 130 * 300 + 1199
        client = StubClient()
        self.disarm_tripwire()

        def offline(*args):
            raise AssertionError("network in dry run")

        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3", config["generator"])
            try:
                screened = band.run_band_lane(config, ledger, state_dir, False, client, self.stub_cache(directory), now_ts)
                fresh = band.BandCache(
                    margin_dir=Path(directory) / "margin",
                    prints_dir=Path(directory) / "prints",
                    fetch_closes=offline,
                    fetch_market=offline,
                    fetch_trades=offline,
                )
                with mock.patch.object(band, "propose_band_rule", wraps=band.propose_band_rule) as propose:
                    dry = band.run_band_lane(config, ledger, state_dir, True, client, fresh, now_ts)
                lane_rows = ledger.lane_hypotheses("band_mechanisms")
            finally:
                ledger.close()
        self.assertEqual(screened["status"], "stage_2_survivor")
        self.assertEqual(dry["status"], "dry_run")
        self.assertEqual(len(propose.call_args[0][4]), 130)
        self.assertEqual(len(lane_rows), 1)

    def test_run_band_lane_screens_prior_then_accrues(self):
        config = band_config(enabled=True, maximum_new_windows_per_cycle=5, minimum_interval_seconds=0)
        # Windows 0..129 are eligible (index 130 would need one more second).
        now_ts = BASE_WS + 130 * 300 + 1199
        client = StubClient()
        self.disarm_tripwire()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.stub_cache(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3", config["generator"])
            try:
                fetching = band.run_band_lane(config, ledger, state_dir, False, client, cache, now_ts)
                config["lanes"]["band_mechanisms"]["maximum_new_windows_per_cycle"] = 1000
                screened = band.run_band_lane(config, ledger, state_dir, False, client, cache, now_ts)
                fingerprint = screened["fingerprint"]
                after_screen = ledger.accrual(fingerprint)
                status_after_screen = ledger.hypothesis(fingerprint)["status"]
                accrued = band.run_band_lane(config, ledger, state_dir, False, client, cache, now_ts + 10 * 300)
                after_accrual = ledger.accrual(fingerprint)
                status_after_accrual = ledger.hypothesis(fingerprint)["status"]
                evidence = json.loads(Path(screened["artifact"]).read_text())
                dry = band.run_band_lane(config, ledger, state_dir, True, client, cache, now_ts + 10 * 300)
                lane_rows = ledger.lane_hypotheses("band_mechanisms")
            finally:
                ledger.close()
            trial_rows = [json.loads(line) for line in (state_dir / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(fetching["status"], "fetching")
        self.assertEqual((fetching["cache"]["fetched"], fetching["cache"]["remaining"]), (5, 125))
        self.assertEqual(fetching["accrual"]["evaluated"], 0)
        self.assertEqual(screened["status"], "stage_2_survivor")
        self.assertEqual(screened["proposal_source"], "prior")
        self.assertEqual(fingerprint, band.band_fingerprint(LIVE_RULE))
        self.assertEqual(screened["cache"]["remaining"], 0)
        self.assertEqual(screened["stage_1"]["overall"]["signals"], 130)
        self.assertEqual(screened["stage_2"]["entries"], 130)
        self.assertEqual(evidence["last_window_start"], BASE_WS + 129 * 300)
        self.assertEqual(evidence["llm"]["proposal_source"], "prior")
        self.assertEqual(status_after_screen, "stage_2_survivor")
        self.assertEqual((after_screen["n"], after_screen["last_window_start"]), (0, BASE_WS + 129 * 300))
        # Ten newer windows resolve: exactly those accrue, at the print break-even;
        # the family decision runs at the deploy config's pre-registered N.
        self.assertEqual(
            accrued["accrual"],
            {
                "evaluated": 1,
                "promoted": 0,
                "killed": 0,
                "accruing": 1,
                "manual_audit": 0,
                "skipped": 0,
                "e_bh": {"campaign_n": 64, "candidates": 1, "k_star": 0, "threshold": 1280.0},
            },
        )
        self.assertEqual((after_accrual["n"], after_accrual["wins"]), (10, 10))
        self.assertEqual(after_accrual["last_window_start"], BASE_WS + 139 * 300)
        self.assertEqual(status_after_accrual, "accruing")
        # The second prior ($75 floor) never fires on a $60 tape: rejected at stage 1.
        self.assertEqual(accrued["status"], "rejected_signal_screen")
        self.assertEqual(accrued["fingerprint"], band.band_fingerprint(dict(zip(band.BAND_GRID, band.BAND_PRIORS[1]))))
        self.assertEqual(accrued["stage_1"]["overall"]["signals"], 0)
        self.assertIsNone(accrued["stage_2"])
        self.assertEqual(dry["status"], "dry_run")
        self.assertEqual(dry["proposal"]["rule"], dict(zip(band.BAND_GRID, band.BAND_PRIORS[2])))
        self.assertEqual(len(lane_rows), 2)
        self.assertEqual(
            [(row["stage"], row["verdict"], row["n"]) for row in trial_rows],
            [
                ("band_signal_screen", "stage_1_survivor", 130),
                ("band_entry_economics", "stage_2_survivor", 130),
                ("fresh_public_accrual", "continue", 10),
                ("band_signal_screen", "rejected_signal_screen", 0),
            ],
        )
        self.assertEqual(client.calls, 0)

    def test_support_only_rejection_is_rescreened_when_the_cache_grows(self):
        config = band_config(
            enabled=True,
            maximum_new_windows_per_cycle=1000,
            minimum_interval_seconds=0,
            gates={"minimum_signals": 10, "minimum_recent_signals": 20, "minimum_entries": 100},
        )
        client = StubClient()
        self.disarm_tripwire()
        early_ts = BASE_WS + 60 * 300 + 1199  # 60 eligible windows: support fails
        late_ts = BASE_WS + 130 * 300 + 1199  # 130 eligible windows: support clears
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.stub_cache(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3", config["generator"])
            try:
                # 70 new windows arrive between the two screens; the production
                # threshold (~8 h) is patched down so the fixture tape suffices.
                self.enterContext(mock.patch.object(band, "RESCREEN_MIN_NEW_WINDOWS", 50))
                first = band.run_band_lane(config, ledger, state_dir, False, client, cache, early_ts)
                fingerprint = first["fingerprint"]
                status_first = ledger.hypothesis(fingerprint)["status"]
                second = band.run_band_lane(config, ledger, state_dir, False, client, cache, late_ts)
                status_second = ledger.hypothesis(fingerprint)["status"]
                accrual = ledger.accrual(fingerprint)
                evidence = json.loads(Path(first["artifact"]).read_text())
                third = band.run_band_lane(config, ledger, state_dir, False, client, cache, late_ts)
            finally:
                ledger.close()
            trial_rows = [json.loads(line) for line in (state_dir / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(first["status"], "rejected_entry_economics")
        self.assertFalse(first["stage_2"]["gates"]["support"])
        self.assertEqual(status_first, "rejected_entry_economics")
        # The next caught-up cycle re-scores it before proposing anything new.
        self.assertEqual(second["rescreen"], {"rescreened": 1, "promoted": 1, "still_rejected": 0})
        self.assertEqual(status_second, "stage_2_survivor")
        self.assertEqual((accrual["n"], accrual["last_window_start"]), (0, BASE_WS + 129 * 300))
        self.assertEqual(evidence["stage_2"]["entries"], 130)
        self.assertEqual(evidence["previous_window_count"], 60)
        self.assertEqual(second["fingerprint"], band.band_fingerprint(dict(zip(band.BAND_GRID, band.BAND_PRIORS[1]))))
        # Nothing left to re-screen once the cache has not grown.
        self.assertEqual(third["rescreen"], {"rescreened": 0, "promoted": 0, "still_rejected": 0})
        self.assertEqual(
            [(row["stage"], row["verdict"], row["n"]) for row in trial_rows if row["candidate"] == fingerprint],
            [
                ("band_signal_screen", "stage_1_survivor", 60),
                ("band_entry_economics", "rejected_entry_economics", 60),
                ("band_entry_economics", "stage_2_survivor", 130),
            ],
        )

    # --- maintenance: paginated tape, rebuild, rescore ----------------------

    def paged_tape(self, ws, count, per_second):
        """Newest-first synthetic tape: `per_second` trades per second going
        back from ws + 349, served in 500-row pages by offset."""
        tape = [
            {"side": "BUY", "asset": "up-%d" % ws, "timestamp": ws + 349 - index // per_second, "price": 0.8}
            for index in range(count)
        ]
        requested = []

        def http_json(url):
            query = dict(item.split("=") for item in url.split("?", 1)[1].split("&"))
            self.assertEqual(url.split("?")[0], band.DATA_API + "/trades")
            self.assertEqual(query["limit"], "500")
            offset = int(query["offset"])
            requested.append(offset)
            return tape[offset : offset + 500]

        return http_json, requested

    def test_trades_pagination_merges_pages_and_records_coverage(self):
        ws = BASE_WS
        cases = {
            # 1300 trades, 4/s: page 1 reaches 225 s, page 2 reaches 100 s <= 150: stop.
            "reaches_coverage": (1300, 4, [0, 500], 1000, {"oldest_offset_s": 100, "pages": 2, "complete": False}, True),
            # 700 trades: the short second page ends the tape above 150 s: complete.
            "short_page": (700, 4, [0, 500], 700, {"oldest_offset_s": 175, "pages": 2, "complete": True}, True),
            # 4200 trades at 30/s: eight full pages reach only 216 s: capped, uncovered.
            "capped": (4200, 30, [index * 500 for index in range(8)], 4000, {"oldest_offset_s": 216, "pages": 8, "complete": False}, False),
            "empty": (0, 1, [0], 0, {"oldest_offset_s": None, "pages": 1, "complete": True}, True),
        }
        for name, (count, per_second, offsets, merged, coverage, covered) in cases.items():
            with self.subTest(name):
                http_json, requested = self.paged_tape(ws, count, per_second)
                with mock.patch.object(band, "http_json", http_json), mock.patch.object(band.time, "sleep"):
                    fetched = band.fetch_data_api_trades("0x%d" % ws, ws)
                self.assertEqual(requested, offsets)
                self.assertEqual(len(fetched["trades"]), merged)
                self.assertEqual(fetched["coverage"], coverage)
                self.assertEqual(band.print_covered({"coverage": coverage}), covered)
        # Print semantics on the merged tape: the first BUY strictly after the decision.
        http_json, _ = self.paged_tape(ws, 1300, 4)
        with mock.patch.object(band, "http_json", http_json), mock.patch.object(band.time, "sleep"):
            fetched = band.fetch_data_api_trades("0x%d" % ws, ws)
        self.assertEqual(band.entry_print(fetched["trades"], "up-%d" % ws, ws + 180), 0.8)
        self.assertFalse(band.print_covered({"status": "ok", "signal_entry": 0.8}))
        self.assertFalse(band.print_covered({"coverage": None}))

    def test_rebuild_prints_skips_covered_windows_and_reports_changes(self):
        now_ts = BASE_WS + 10 * 300 + 1200
        stale = {BASE_WS + 1 * 300, BASE_WS + 2 * 300, BASE_WS + 5 * 300}
        broken = BASE_WS + 5 * 300
        legacy = BASE_WS + 8 * 300
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            seeded = self.stub_cache(directory)
            seeded.refresh(BASE_WS, now_ts, budget=100)  # 11 settled windows, all covered
            # Three windows regress to the pre-pagination shape: no coverage, one
            # with a print the truncated tape had missed.  A fourth is covered
            # but predates signal_prints.
            for ws in stale | {legacy}:
                for d in band.BAND_DECISION_SECONDS:
                    row = json.loads(seeded.print_path(ws, d).read_text())
                    if ws == legacy:
                        del row["signal_prints"]
                    else:
                        del row["coverage"]
                    if ws == BASE_WS + 2 * 300:
                        row["signal_entry"] = None
                    seeded.print_path(ws, d).write_text(json.dumps(row) + "\n")
            calls = []
            cache = self.stub_cache(directory, failing={broken})
            inner = cache.fetch_trades

            def counting(condition_id, window_start):
                calls.append(window_start)
                return inner(condition_id, window_start)

            cache.fetch_trades = counting
            first = band.rebuild_prints(cache, BASE_WS, now_ts, workers=2)
            rebuilt_row = json.loads(cache.print_path(BASE_WS + 2 * 300, 240).read_text())
            legacy_row = json.loads(cache.print_path(legacy, 240).read_text())
            broken_row = json.loads(cache.print_path(broken, 240).read_text())
            again = band.rebuild_prints(self.stub_cache(directory), BASE_WS, now_ts)
            bounded = band.rebuild_prints(self.stub_cache(directory), BASE_WS, now_ts, limit=0)
        self.assertEqual(sorted(calls), sorted((stale | {legacy}) - {broken}))
        self.assertEqual(
            first,
            {
                "settled": 11,
                "covered": 7,
                "unlisted": 0,
                "pending": 4,
                "rebuilt": 3,
                "changed_windows": 1,
                "changed_prints": 4,
                "short_tapes": 0,
                "errors": 1,
            },
        )
        self.assertEqual(rebuilt_row["signal_entry"], 0.80)
        self.assertEqual(rebuilt_row["coverage"], {"oldest_offset_s": 185, "pages": 1, "complete": True})
        self.assertEqual(legacy_row["signal_prints"], [[5, 0.80]])
        # The failed window keeps its old file and stays pending for the next run.
        self.assertNotIn("coverage", broken_row)
        self.assertEqual((again["covered"], again["pending"], again["rebuilt"], again["errors"]), (10, 1, 1, 0))
        self.assertEqual((bounded["covered"], bounded["pending"], bounded["rebuilt"]), (11, 0, 0))

    def test_rescore_all_resets_band_accrual_and_demotes_on_the_rebuilt_tape(self):
        config = band_config(enabled=True)
        self.disarm_tripwire()
        now_ts = BASE_WS + 130 * 300 + 1199
        narrow = {**LIVE_RULE, "favorite_price_floor": 0.60, "favorite_price_cap": 0.80}
        high_floor = {**LIVE_RULE, "margin_floor_usd": 75}
        seeds = [
            ("live", LIVE_RULE, "promote_candidate"),
            ("narrow", narrow, "promote_candidate"),
            ("floor", high_floor, "accruing"),
        ]
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.stub_cache(directory, price=0.85)  # in the live band, above the narrow cap
            cache.refresh(BASE_WS, now_ts, budget=1000)
            windows = cache.windows(BASE_WS, now_ts)
            prints = cache.load_prints(windows)
            ledger = loop.Ledger(state_dir / "research.sqlite3", config["generator"])
            try:
                fingerprints = {}
                for name, rule, status in seeds:
                    fingerprint = band.band_fingerprint(rule)
                    fingerprints[name] = fingerprint
                    evidence_path = state_dir / ("evidence/band_mechanisms/%s.json" % fingerprint)
                    band._atomic_write(
                        evidence_path,
                        json.dumps({"proposal": proposal(rule, name), "llm": {"proposal_source": "prior"}, "window_count": 40, "stage_2": {"entries": 40, "wins": 40}}),
                    )
                    ledger.add_hypothesis(fingerprint, "band_mechanisms", proposal(rule, name), None, status, evidence_path, source="prior")
                    ledger.accrue(fingerprint, "band_mechanisms", [(BASE_WS + 300 * index, 0.9, True) for index in range(25)])
                ledger.accrue("late", "late_window_mechanisms", [(BASE_WS, 0.9, True)])
                report = band.rescore_band_hypotheses(config, ledger, windows, prints, now_ts)
                statuses = {name: ledger.hypothesis(fingerprint)["status"] for name, fingerprint in fingerprints.items()}
                accruals = {name: ledger.accrual(fingerprint) for name, fingerprint in fingerprints.items()}
                late = ledger.accrual("late")
                artifacts = {
                    name: json.loads((state_dir / ("evidence/band_mechanisms/%s.json" % fingerprint)).read_text())
                    for name, fingerprint in fingerprints.items()
                }
                backups = {
                    name: json.loads((state_dir / ("evidence/band_mechanisms/%s.pre_pagination.json" % fingerprint)).read_text())
                    for name, fingerprint in fingerprints.items()
                }
                # A second pass keeps the original pre-pagination artifact.
                band.rescore_band_hypotheses(config, ledger, windows, prints, now_ts)
                backup_again = json.loads((state_dir / ("evidence/band_mechanisms/%s.pre_pagination.json" % fingerprints["live"])).read_text())
            finally:
                ledger.close()
            trial_rows = [json.loads(line) for line in (state_dir / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(report["accrual_rows_deleted"], 3)
        self.assertEqual(report["before"], {"promote_candidate": 2, "accruing": 1})
        self.assertEqual(report["after"], {"stage_2_survivor": 1, "rejected_entry_economics": 1, "rejected_signal_screen": 1})
        self.assertEqual((report["survivors_before"], report["survivors_after"]), (3, 1))
        self.assertEqual((report["promote_candidates_before"], report["promote_candidates_after"]), (2, 0))
        self.assertEqual(
            report["transitions"],
            {
                "promote_candidate->stage_2_survivor": 1,
                "promote_candidate->rejected_entry_economics": 1,
                "accruing->rejected_signal_screen": 1,
            },
        )
        self.assertEqual(statuses, {"live": "stage_2_survivor", "narrow": "rejected_entry_economics", "floor": "rejected_signal_screen"})
        self.assertEqual((accruals["live"]["n"], accruals["live"]["last_window_start"]), (0, BASE_WS + 129 * 300))
        self.assertIsNone(accruals["narrow"])
        self.assertIsNone(accruals["floor"])
        self.assertEqual(late["n"], 1)
        former = {item["rule"]: item for item in report["former_promote_candidates"]}
        self.assertEqual(set(former), {band.compact_band_rule(LIVE_RULE), band.compact_band_rule(narrow)})
        self.assertEqual(former[band.compact_band_rule(LIVE_RULE)]["stage_2"]["entries"], 130)
        self.assertEqual(former[band.compact_band_rule(narrow)]["stage_2"]["entries"], 0)
        self.assertEqual(former[band.compact_band_rule(narrow)]["previous_stage_2"]["entries"], 40)
        self.assertEqual(artifacts["live"]["rescore"]["previous_status"], "promote_candidate")
        self.assertEqual(artifacts["live"]["rescore"]["previous_window_count"], 40)
        self.assertEqual(artifacts["live"]["proposal"]["title"], "live")
        self.assertEqual(artifacts["live"]["window_count"], 130)
        self.assertEqual(backups["narrow"]["window_count"], 40)
        self.assertEqual(backup_again["window_count"], 40)
        self.assertEqual(
            {(row["candidate"], row["verdict"], row["n"], row["wins"]) for row in trial_rows if row["stage"] == "band_rescore_paginated"},
            {
                (fingerprints["live"], "stage_2_survivor", 130, 130),
                (fingerprints["narrow"], "rejected_entry_economics", 0, 0),
                (fingerprints["floor"], "rejected_signal_screen", 0, 0),
            },
        )
        self.assertEqual(sum(1 for row in trial_rows if row["stage"] == "band_rescore_paginated"), 6)

    # --- entry semantics, tripwire, e-BH family, parents ---------------------

    def test_entry_semantics_one_look_versus_patient(self):
        rows = windows(6)
        starts = [row["window_start"] for row in rows]
        prints = {
            # First print out of band; in-band prints 11 s and 20 s after the decision.
            (starts[0], 240): {"status": "ok", "signal": "up", "signal_entry": 0.97, "signal_prints": [[2, 0.97], [11, 0.91], [20, 0.89]]},
            # First print in band under both semantics.
            (starts[1], 240): {"status": "ok", "signal": "up", "signal_entry": 0.80, "signal_prints": [[1, 0.80], [3, 0.95]]},
            # Never in band.
            (starts[2], 240): {"status": "ok", "signal": "up", "signal_entry": 0.95, "signal_prints": [[4, 0.95], [9, 0.96]]},
            # No print in the entry window.
            (starts[3], 240): {"status": "ok", "signal": "up", "signal_entry": None, "signal_prints": []},
            # Row written before signal_prints: only the first print is known.
            (starts[4], 240): {"status": "ok", "signal": "up", "signal_entry": 0.97},
        }
        scored = band._labelled(rows, band.band_signal_records(rows, LIVE_RULE))
        one_look = band.band_entry_economics(scored, prints, LIVE_RULE, GATES)
        patient = band.band_entry_economics(scored, prints, LIVE_RULE, GATES, "patient")
        self.assertEqual(one_look["entry_semantics"], "one_look")
        self.assertEqual((one_look["entries"], one_look["wins"]), (1, 1))
        self.assertEqual(
            tuple(one_look[key] for key in ("out_of_band_prints", "windows_without_print", "windows_without_print_list", "uncached_windows")),
            (3, 1, 0, 1),
        )
        self.assertEqual(patient["entry_semantics"], "patient")
        self.assertEqual((patient["entries"], patient["wins"]), (2, 2))
        self.assertAlmostEqual(patient["mean_break_even"], (band.break_even(0.91) + band.break_even(0.80)) / 2)
        self.assertEqual(
            tuple(patient[key] for key in ("out_of_band_prints", "windows_without_print", "windows_without_print_list", "uncached_windows")),
            (1, 1, 1, 1),
        )
        first = prints[(starts[0], 240)]
        self.assertEqual(band.print_entry_price(first, "up", 0.55, 0.92, "one_look"), (None, "out_of_band"))
        self.assertEqual(band.print_entry_price(first, "up", 0.55, 0.92, "patient"), (0.91, None))
        # A 0.90 cap waits past the 0.91 print for the 0.89 one.
        self.assertEqual(band.print_entry_price(first, "up", 0.55, 0.90, "patient"), (0.89, None))
        self.assertEqual(band.print_entry_price(first, "down", 0.55, 0.92, "patient"), (None, "no_print"))
        self.assertEqual(band.print_entry_price(prints[(starts[4], 240)], "up", 0.55, 0.92, "patient"), (None, "no_print_list"))
        with self.assertRaises(ValueError):
            band.print_entry_price(first, "up", 0.55, 0.92, "greedy")
        self.assertEqual(
            band.band_accrual_outcomes(rows, prints, LIVE_RULE, -1, "patient"),
            [(starts[0], band.break_even(0.91), True), (starts[1], band.break_even(0.80), True)],
        )
        self.assertEqual(band.band_accrual_outcomes(rows, prints, LIVE_RULE, -1), [(starts[1], band.break_even(0.80), True)])
        # The evaluator records the semantics it scored with; the lane defaults to one_look.
        evidence = band.evaluate_band_rule(windows(120), {}, LIVE_RULE, GATES, BASE_WS + 120 * 300, "patient")
        self.assertEqual(evidence["stage_2"]["entry_semantics"], "patient")
        self.assertEqual(band.lane_entry_semantics({}), "one_look")
        self.assertEqual(band.lane_entry_semantics({"entry_semantics": "patient"}), "patient")

    def test_tripwire_holds_too_good_survivors_as_manual_audit_until_cleared(self):
        config = band_config(enabled=True, maximum_new_windows_per_cycle=1000, minimum_interval_seconds=0)
        now_ts = BASE_WS + 130 * 300 + 1199
        client = StubClient()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.stub_cache(directory)  # 130/130 at stage 2: trips
            ledger = loop.Ledger(state_dir / "research.sqlite3", config["generator"])
            try:
                screened = band.run_band_lane(config, ledger, state_dir, False, client, cache, now_ts)
                fingerprint = screened["fingerprint"]
                status_screened = ledger.hypothesis(fingerprint)["status"]
                seeded = ledger.accrual(fingerprint)
                held = band.run_band_lane(config, ledger, state_dir, False, client, cache, now_ts + 10 * 300)
                status_held = ledger.hypothesis(fingerprint)["status"]
                config["lanes"]["band_mechanisms"]["audit_cleared"] = [fingerprint]
                cleared = band.run_band_lane(config, ledger, state_dir, False, client, cache, now_ts + 20 * 300)
                status_cleared = ledger.hypothesis(fingerprint)["status"]
                accrual = ledger.accrual(fingerprint)
                evidence = json.loads(Path(screened["artifact"]).read_text())
            finally:
                ledger.close()
            trial_rows = [json.loads(line) for line in (state_dir / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(screened["status"], "manual_audit")
        self.assertEqual(status_screened, "manual_audit")
        self.assertTrue(evidence["stage_2"]["survivor"])
        self.assertEqual(
            evidence["stage_2"]["tripwire"], {"triggered": True, "maximum_win_rate": 0.97, "minimum_entries": 50}
        )
        # Evidence keeps accruing during the audit; promotion does not, even
        # though the accrual's own 10/10 is below the tripwire's support.
        self.assertEqual((seeded["n"], seeded["last_window_start"]), (0, BASE_WS + 129 * 300))
        self.assertEqual((held["accrual"]["manual_audit"], held["accrual"]["e_bh"]["candidates"]), (1, 0))
        self.assertEqual(status_held, "manual_audit")
        self.assertEqual((cleared["accrual"]["manual_audit"], cleared["accrual"]["accruing"]), (0, 1))
        self.assertEqual(cleared["accrual"]["e_bh"]["candidates"], 1)
        self.assertEqual(status_cleared, "accruing")
        self.assertEqual((accrual["n"], accrual["wins"]), (20, 20))
        self.assertIn(
            ("band_entry_economics", "manual_audit", 130),
            [(row["stage"], row["verdict"], row["n"]) for row in trial_rows],
        )
        stage_1 = {"survivor": True}
        tripped = {"survivor": True, "tripwire": {"triggered": True}}
        self.assertEqual(band.screen_status(stage_1, tripped), "manual_audit")
        self.assertEqual(band.screen_status(stage_1, tripped, cleared=True), "stage_2_survivor")
        self.assertEqual(band.screen_status(stage_1, {**tripped, "survivor": False}), "rejected_entry_economics")
        self.assertFalse(band.tripwire_triggered(49, 49))
        self.assertFalse(band.tripwire_triggered(97, 100))
        self.assertTrue(band.tripwire_triggered(98, 100))
        self.assertTrue(band.tripwire_triggered(50, 50))

    def seed_family(self, ledger, members):
        """members: (name, status, e, n, wins, verdict); the e-process state is
        pinned so the mixture e-value equals e exactly."""
        fingerprints = {}
        for index, (name, status, e, n, wins, verdict) in enumerate(members):
            rule = {**LIVE_RULE, "favorite_price_floor": (0.55, 0.60, 0.65, 0.70)[index % 4], "decision_second": (240, 210)[index // 4]}
            fingerprint = band.band_fingerprint(rule)
            fingerprints[name] = fingerprint
            ledger.add_hypothesis(fingerprint, "band_mechanisms", proposal(rule, name), None, status, None, source="prior")
            state = band.evidence_accrual.EProcess([math.log(e)] * band.evidence_accrual.LAMBDA_COUNT, n).to_json()
            ledger.connection.execute(
                "INSERT INTO evidence_accrual VALUES(?, 'band_mechanisms', ?, ?, ?, ?, ?, ?, 'seeded')",
                (fingerprint, n, wins, state, BASE_WS, e, verdict),
            )
        ledger.connection.commit()
        return fingerprints

    def test_accrual_promotes_only_the_e_bh_discovery_set(self):
        config = band_config(enabled=True)
        members = [
            ("a", "accruing", 1300.0, 80, 70, "promote"),
            ("b", "promote_candidate", 700.0, 80, 70, "promote"),
            ("c", "accruing", 100.0, 40, 35, "promote"),
            ("d", "accruing", 5.0, 20, 15, "continue"),
            ("held", "accruing", 2000.0, 60, 59, "promote"),
            ("dead", "accruing", 0.05, 30, 10, "kill"),
        ]
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3", config["generator"])
            try:
                fingerprints = self.seed_family(ledger, members)
                registered = band.accrue_band_hypotheses(config, ledger, [], {})
                statuses = {name: ledger.hypothesis(fp)["status"] for name, fp in fingerprints.items()}
                config["lanes"]["band_mechanisms"]["campaign_n"] = 2  # exceeded: N grows to the count
                overflow = band.accrue_band_hypotheses(config, ledger, [], {})
                statuses_overflow = {name: ledger.hypothesis(fp)["status"] for name, fp in fingerprints.items()}
                del config["lanes"]["band_mechanisms"]["campaign_n"]
                unregistered = band.accrue_band_hypotheses(config, ledger, [], {})
                statuses_unregistered = {name: ledger.hypothesis(fp)["status"] for name, fp in fingerprints.items()}
            finally:
                ledger.close()
        # N=64: 1300 >= 1280 (k=1), 700 >= 640 (k=2), 100 < 426.7 (k=3): k* = 2.
        # The tripped candidate is out of the family, the kill is futility.
        self.assertEqual(
            registered,
            {
                "evaluated": 6, "promoted": 2, "killed": 1, "accruing": 2, "manual_audit": 1, "skipped": 0,
                "e_bh": {"campaign_n": 64, "candidates": 4, "k_star": 2, "threshold": 640.0},
            },
        )
        self.assertEqual(
            statuses,
            {"a": "promote_candidate", "b": "promote_candidate", "c": "accruing", "d": "accruing", "held": "manual_audit", "dead": "killed_futility"},
        )
        # N=4 (registered 2, four candidates): 100 >= 26.7 at k=3.
        self.assertEqual(overflow["e_bh"], {"campaign_n": 4, "candidates": 4, "k_star": 3, "threshold": 4 / (0.05 * 3)})
        self.assertEqual((statuses_overflow["c"], statuses_overflow["d"]), ("promote_candidate", "accruing"))
        # No registered N: the flags lapse; futility and the audit hold stand.
        self.assertEqual(unregistered["e_bh"], {"campaign_n": None, "candidates": 4, "k_star": 0, "threshold": None})
        self.assertEqual(
            statuses_unregistered,
            {"a": "accruing", "b": "accruing", "c": "accruing", "d": "accruing", "held": "manual_audit", "dead": "killed_futility"},
        )
        self.assertEqual(band.campaign_n({"generator": {"campaign_n": 7}}), 7)
        self.assertEqual(band.campaign_n({"generator": {"campaign_n": 7}, "lanes": {"band_mechanisms": {"campaign_n": 9}}}), 9)
        self.assertIsNone(band.campaign_n({}))

    def test_elite_parents_skip_hamming_1_clones_of_chosen_accruing_elites(self):
        def summary(status, wilson_lower, **fields):
            rule = {**LIVE_RULE, **fields}
            return {"rule": band.compact_band_rule(rule), "rule_fields": rule, "status": status, "wilson_lower": wilson_lower}

        top = summary("accruing", 0.95)
        clone = summary("accruing", 0.94, favorite_price_cap=0.85)  # H1 of top: skipped
        near = summary("rejected_entry_economics", 0.93, favorite_price_floor=0.60)  # H1 of top: skipped
        far = summary("rejected_signal_screen", 0.92, favorite_price_floor=0.60, favorite_price_cap=0.85)  # H2
        beside_far = summary("accruing", 0.91, favorite_price_floor=0.60, favorite_price_cap=0.85, direction="up")
        unscored = summary("accruing", None, margin_floor_usd=75)
        chosen = band.elite_parents([unscored, clone, far, near, beside_far, top])
        # A non-elite parent (far) does not fence its neighbours: beside_far stays.
        self.assertEqual([item["rule"] for item in chosen], [top["rule"], far["rule"], beside_far["rule"]])
        self.assertEqual(band.rule_hamming(top["rule_fields"], clone["rule_fields"]), 1)
        self.assertEqual(band.rule_hamming(top["rule_fields"], far["rule_fields"]), 2)
        self.assertEqual(band.rule_hamming(top["rule_fields"], top["rule_fields"]), 0)
        self.assertEqual(band.elite_parents([]), [])

    def test_run_cycle_band_lane_disabled_returns_disabled(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        self.assertFalse(config["lanes"]["band_mechanisms"]["enabled"])
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            with mock.patch.object(
                loop, "resource_status", return_value={"passed": True, "checks": {}}
            ), mock.patch.object(
                loop, "run_registry_audit", return_value={"status": "completed"}
            ), mock.patch.object(loop, "refresh_public_snapshot") as refresh, mock.patch.object(
                loop.band_lane, "BandCache"
            ) as cache:
                result = loop.run_cycle(config, True, "band_mechanisms")
        refresh.assert_not_called()
        cache.assert_not_called()
        self.assertEqual(result["lane"], "band_mechanisms")
        self.assertEqual(result["lane_result"], {"status": "disabled"})
        self.assertNotIn("economic_screen_job", result)
        self.assertNotIn("public_snapshot", result)

    def test_run_cycle_dispatches_band_lane_without_late_lane_chain(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            ledger = loop.Ledger(Path(directory) / "research.sqlite3")
            try:
                ledger.enqueue(
                    "late_window_mechanisms", "queued", "exact_l2_replay", {}, "queued", status="queued"
                )
            finally:
                ledger.close()
            with mock.patch.object(
                loop, "resource_status", return_value={"passed": True, "checks": {}}
            ), mock.patch.object(
                loop, "run_registry_audit", return_value={"status": "completed"}
            ), mock.patch.object(loop, "refresh_public_snapshot") as refresh, mock.patch.object(
                loop, "run_band_lane", return_value={"status": "not_due"}
            ) as lane, mock.patch.object(loop, "run_queued_economic_screen") as screen:
                result = loop.run_cycle(config, False, "band_mechanisms")
        refresh.assert_not_called()
        lane.assert_called_once()
        screen.assert_not_called()
        self.assertEqual(result["lane_result"], {"status": "not_due"})
        self.assertNotIn("fresh_public_accrual", result)

    def test_deploy_config_is_fail_closed_and_overlay_mirrors_it(self):
        deploy = json.loads((ROOT / "deploy/strategy-research-loop.json").read_text())
        block = deploy["lanes"]["band_mechanisms"]
        self.assertFalse(block["enabled"])
        self.assertEqual(block["minimum_interval_seconds"], 900)
        self.assertEqual(block["start_ts"], 1787097600)
        self.assertEqual(block["maximum_new_windows_per_cycle"], 400)
        self.assertEqual(block["gates"], GATES)
        # e-BH family size, registered before any accrual outcome is read.
        self.assertEqual(block["campaign_n"], 64)
        self.assertEqual(band.campaign_n(deploy), 64)
        self.assertEqual(block["audit_cleared"], [])
        overlay_path = ROOT / "logs/strategy-research/loop-config.local.json"
        if not overlay_path.is_file():
            self.skipTest("gitignored overlay not present")
        overlay = json.loads(overlay_path.read_text())["lanes"]["band_mechanisms"]
        self.assertTrue(overlay["enabled"])
        self.assertEqual({**overlay, "enabled": False}, block)


if __name__ == "__main__":
    unittest.main()
