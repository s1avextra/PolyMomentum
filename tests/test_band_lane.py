from __future__ import annotations

import importlib.util
import itertools
import json
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
BASE_WS = 1787788800  # 2026-08-25T00:00Z, the default start_ts
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
        # LLM turn with no ready model degrades to the fixed grid order.
        self.assertEqual(len(lane_rows), 9)
        self.assertEqual(offline_provenance["turn"], "llm")
        self.assertEqual(offline_provenance["proposal_source"], "fallback_grid")
        self.assertEqual(offline["rule"], next(rule for rule in band.grid_rules() if rule not in rules))

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

        def fetch_trades(condition_id):
            ws = int(condition_id[2:])
            return [
                {"side": "BUY", "asset": "up-%d" % ws, "timestamp": ws + d + 5, "price": price}
                for d in band.BAND_DECISION_SECONDS
            ] + [{"side": "SELL", "asset": "up-%d" % ws, "timestamp": ws + 241, "price": 0.5}]

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
        self.assertEqual(print_row, {"window_start": BASE_WS, "decision_second": 240, "status": "ok", "signal": "up", "signal_entry": 0.80})
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
        # Ten newer windows resolve: exactly those accrue, at the print break-even.
        self.assertEqual(accrued["accrual"], {"evaluated": 1, "promoted": 0, "killed": 0, "accruing": 1, "skipped": 0})
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
        self.assertEqual(block["start_ts"], 1787788800)
        self.assertEqual(block["maximum_new_windows_per_cycle"], 400)
        self.assertEqual(block["gates"], GATES)
        overlay_path = ROOT / "logs/strategy-research/loop-config.local.json"
        if not overlay_path.is_file():
            self.skipTest("gitignored overlay not present")
        overlay = json.loads(overlay_path.read_text())["lanes"]["band_mechanisms"]
        self.assertTrue(overlay["enabled"])
        self.assertEqual({**overlay, "enabled": False}, block)


if __name__ == "__main__":
    unittest.main()
