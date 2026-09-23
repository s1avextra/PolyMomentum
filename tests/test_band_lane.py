from __future__ import annotations

import importlib.util
import itertools
import json
import math
from pathlib import Path
import re
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

# The legacy six-field shape of the 412 hypotheses proposed by the LLM-era lane.
LIVE_RULE = {
    "margin_floor_usd": 50,
    "margin_floor_sigma": 0.0,
    "decision_second": 240,
    "direction": "both",
    "favorite_price_floor": 0.55,
    "favorite_price_cap": 0.92,
}
# Grammar C: the proposer's first cell (registrable, grid order).
FIRST_CELL = {"decision_second": 180, "margin_floor_usd": 75, "favorite_price_cap": 0.92, "patience_s": 0}
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


def band_config(**lane_overrides):
    config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
    # Fixtures are built from BASE_WS; the deployed start_ts may move earlier.
    config["lanes"]["band_mechanisms"]["start_ts"] = BASE_WS
    config["lanes"]["band_mechanisms"].update(lane_overrides)
    return config


class BandLaneTest(unittest.TestCase):
    # --- grammar -------------------------------------------------------------

    def test_grammar_reads_both_shapes_and_validation_is_strict(self):
        cells = band.grid_v2_rules()
        self.assertEqual(len(cells), 504)
        self.assertEqual(sum(1 for rule in cells if band.registrable_v2(rule)), 315)
        for rule in cells:
            self.assertEqual(band.normalized_band_rule(rule), rule)
            self.assertEqual(band.normalized_band_rule_v2(rule), rule)
        self.assertEqual(band.normalized_band_rule({**FIRST_CELL, "margin_floor_usd": 75.0}), FIRST_CELL)
        # The legacy grammar stays readable in full.
        combos = list(itertools.product(*band.BAND_GRID.values()))
        self.assertEqual(len(combos), 2880)
        for values in combos:
            rule = dict(zip(band.BAND_GRID, values))
            self.assertEqual(band.normalized_band_rule(rule), rule)
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
            {**FIRST_CELL, "favorite_price_cap": 0.93},
            {**FIRST_CELL, "patience_s": 10},
            {**FIRST_CELL, "decision_second": 150.5},
            {**FIRST_CELL, "margin_floor_usd": "75"},
            {**FIRST_CELL, "direction": "both"},  # a mixed shape is neither grammar
            {**LIVE_RULE, "patience_s": 0},
        ):
            with self.assertRaises(ValueError):
                band.normalized_band_rule(broken)
        with self.assertRaises(ValueError):
            band.normalized_band_rule_v2(LIVE_RULE)
        # rule_params projects either shape onto one parameter set (no validation).
        self.assertEqual(
            band.rule_params(FIRST_CELL),
            {
                "decision_second": 180,
                "margin_floor_usd": 75.0,
                "margin_floor_sigma": 0.0,
                "direction": "both",
                "favorite_price_floor": band.BAND_V2_ASK_FLOOR,
                "favorite_price_cap": 0.92,
                "patience_s": 0,
            },
        )
        legacy = band.rule_params(LIVE_RULE)
        self.assertEqual((legacy["favorite_price_floor"], legacy["patience_s"], legacy["direction"]), (0.55, None, "both"))
        self.assertEqual(band.compact_band_rule(FIRST_CELL), "band t=180s floor=$75 cap=0.92 patience=0s")
        self.assertEqual(band.compact_band_rule(LIVE_RULE), "band floor=$50 sigma=0.0 t=240s dir=both ask=(0.55,0.92]")
        self.assertEqual((band.BAND_V2_REGISTRABLE_MIN_FLOOR, band.BAND_V2_REGISTRABLE_MIN_DECISION), (75, 180))

    def test_fingerprint_is_stable(self):
        pinned = "a9bfe51acf4aab88162e15f955d95da1a40f6f20760896f086a237338b6b3360"  # band_public_v2: stage-1 break-even gates dropped, tripwire 0.995@100
        self.assertEqual(band.band_fingerprint(LIVE_RULE), pinned)
        reordered = dict(reversed(list(LIVE_RULE.items())))
        self.assertEqual(band.band_fingerprint(reordered), pinned)
        self.assertEqual(band.band_fingerprint({**LIVE_RULE, "margin_floor_usd": 50.0}), pinned)
        self.assertEqual(
            band.band_fingerprint(LIVE_RULE),
            loop.stable_hash(
                {"lane": "band_mechanisms", "rule": LIVE_RULE, "evaluator_version": "band_public_v2"}
            ),
        )
        self.assertNotEqual(band.band_fingerprint({**LIVE_RULE, "margin_floor_usd": 75}), pinned)
        # Grammar C cells fingerprint under their own key set: the legacy
        # rows keep their fingerprints and a cell never collides with one.
        cell = band.band_fingerprint(FIRST_CELL)
        self.assertEqual(cell, band.band_fingerprint({**FIRST_CELL, "patience_s": 0.0}))
        self.assertEqual(
            cell,
            loop.stable_hash({"lane": "band_mechanisms", "rule": FIRST_CELL, "evaluator_version": "band_public_v2"}),
        )
        self.assertNotEqual(cell, band.band_fingerprint({**FIRST_CELL, "patience_s": 15}))
        self.assertNotEqual(cell, pinned)

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
        # A grammar C cell reads the same closes: floor 75 fires on an $80 tape only.
        self.assertEqual(len(band.band_signal_records(unlabelled, FIRST_CELL)), 0)
        self.assertEqual(len(band.band_signal_records(windows(30, margin=80.0), FIRST_CELL)), 30)
        self.assertEqual(len(band.band_signal_records(windows(30, margin=-80.0), FIRST_CELL)), 30)

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
        # Signal accuracy is a ceiling: no stage-1 gate against entry
        # break-even, overall or recent (CLAUDE.md section 4); the
        # break-even at the cap is reported beside the accuracy only.
        self.assertEqual(stage_1["gates"], {"support": True, "recent_support": True})
        self.assertAlmostEqual(stage_1["break_even_at_cap"], 0.92 + 0.07 * 0.92 * 0.08)
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
        self.assertNotIn("recent_above_break_even", stale["stage_1"]["gates"])
        mixed = [window(BASE_WS + index * 300, official="up" if index % 10 else "down") for index in range(120)]
        noisy = band.evaluate_band_rule(mixed, {}, LIVE_RULE, GATES, now_ts)
        # 108/120 = 0.90 signal accuracy under the 0.925 break-even at the
        # cap still reaches stage 2: only the priced subpopulation decides.
        self.assertEqual(noisy["stage_1"]["overall"]["wins"], 108)
        self.assertNotIn("wilson_above_break_even", noisy["stage_1"]["gates"])
        self.assertNotIn("recent_above_break_even", noisy["stage_1"]["gates"])
        self.assertTrue(noisy["stage_1"]["survivor"])
        self.assertIsNotNone(noisy["stage_2"])
        self.assertAlmostEqual(noisy["stage_1"]["break_even_at_cap"], band.break_even(0.92))
        below_floor = band.evaluate_band_rule(windows(120, margin=40.0), {}, LIVE_RULE, GATES, now_ts)
        self.assertEqual(below_floor["stage_1"]["overall"]["signals"], 0)
        down_only = band.evaluate_band_rule(windows(120), {}, {**LIVE_RULE, "direction": "down"}, GATES, now_ts)
        self.assertEqual(down_only["stage_1"]["overall"]["signals"], 0)
        # The evaluator validates a grammar C cell and reports its rule as given.
        cell = band.evaluate_band_rule(windows(120, margin=80.0), {}, {**FIRST_CELL, "decision_second": 240}, GATES, now_ts)
        self.assertEqual(cell["rule"], {**FIRST_CELL, "decision_second": 240})
        self.assertTrue(cell["stage_1"]["survivor"])
        self.assertAlmostEqual(cell["stage_1"]["break_even_at_cap"], band.break_even(0.92))

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
        even = 0.80 + 0.07 * 0.80 * 0.20
        self.assertEqual((stage_2["entries"], stage_2["wins"]), (60, 60))
        self.assertAlmostEqual(stage_2["mean_break_even"], even)
        self.assertAlmostEqual(stage_2["mean_net_per_usd"], 1.0 / even - 1.0)
        self.assertEqual(
            (stage_2["out_of_band_prints"], stage_2["windows_without_print"], stage_2["uncached_windows"]),
            (10, 10, 40),
        )
        self.assertIsNone(stage_2["patience_s"])
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
        # Whole-second stamps on a newest-first tape: within a second the API
        # lists the later print first, so the first print of that second is
        # the one listed last (0.94 printed before 0.91).
        newest_first = [
            {"side": "BUY", "asset": "up", "timestamp": decision_ts + 2, "price": 0.91},
            {"side": "BUY", "asset": "up", "timestamp": decision_ts + 2, "price": 0.94},
            {"side": "BUY", "asset": "up", "timestamp": decision_ts + 1, "price": 0.96},
        ]
        self.assertEqual(band.entry_prints(newest_first, "up", decision_ts), [[1, 0.96], [2, 0.94], [2, 0.91]])
        self.assertEqual(band.entry_print(newest_first, "up", decision_ts), 0.96)
        self.assertEqual(band.print_entry_price({"status": "ok", "signal": "up", "signal_prints": band.entry_prints(newest_first[:2], "up", decision_ts)}, "up", 0.55, 0.92, "patient"), (0.91, None))

    def test_taker_fee_matches_the_engine_rate(self):
        # One fee for the tape and the engine: stage-2 gates, the accrual
        # p0 and the race scores must sit on the rate the live trades pay.
        source = (ROOT / "rust_engine/src/data/models.rs").read_text()
        match = re.search(r"DEFAULT_CRYPTO_TAKER_FEE_RATE: f64 = ([0-9.]+);", source)
        self.assertIsNotNone(match)
        rate = float(match.group(1))
        self.assertEqual(rate, 0.07)
        for price in (0.5, 0.8, 0.92):
            self.assertAlmostEqual(band.taker_fee(price), rate * price * (1.0 - price), places=12)
        self.assertAlmostEqual(band.break_even(0.92), 0.92 + rate * 0.92 * 0.08, places=12)

    # --- proposer ------------------------------------------------------------

    def test_proposer_enumerates_grammar_c_deterministically_without_duplicates(self):
        cells = band.proposer_cells()
        # 504 cells minus the 252 at 150, 195 and 225 s, which have no print column (ladder-only).
        self.assertEqual(len(cells), 252)
        self.assertEqual(len({band._canonical(cell) for cell in cells}), 252)
        self.assertTrue(all(cell["decision_second"] in band.BAND_DECISION_SECONDS for cell in cells))
        self.assertEqual({band._canonical(c) for c in cells}, {band._canonical(c) for c in band.grid_v2_rules() if c["decision_second"] in band.BAND_DECISION_SECONDS})
        self.assertEqual(sorted({c["decision_second"] for c in band.grid_v2_rules()} - {c["decision_second"] for c in cells}), [150, 195, 225])
        # Registrable cells first (floor >= 75), then the $50 control cells, each in grid order.
        self.assertEqual([band.registrable_v2(cell) for cell in cells], [True] * 189 + [False] * 63)
        self.assertEqual(cells[0], FIRST_CELL)
        self.assertEqual(cells[1], {**FIRST_CELL, "patience_s": 15})
        self.assertEqual(cells[3], {**FIRST_CELL, "favorite_price_cap": 0.94})
        self.assertEqual(cells[189], {**FIRST_CELL, "margin_floor_usd": 50})
        self.assertEqual(band.proposer_cells(), cells)  # no state, no randomness
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            ledger = loop.Ledger(Path(directory) / "research.sqlite3")
            try:
                # Legacy and malformed rows are read past, never proposed, never raise.
                ledger.add_hypothesis(band.band_fingerprint(LIVE_RULE), "band_mechanisms", proposal(LIVE_RULE), None, "rejected_entry_economics", None, source="prior")
                ledger.add_hypothesis("malformed", "band_mechanisms", {"rule": {"bad": 1}}, None, "rejected_signal_screen", None, source="llm")
                proposed = []
                for _ in range(5):
                    proposed_rule, provenance = band.propose_band_rule(ledger)
                    proposed.append((proposed_rule, provenance))
                    ledger.add_hypothesis(
                        band.band_fingerprint(proposed_rule["rule"]),
                        "band_mechanisms",
                        proposed_rule,
                        None,
                        "rejected_signal_screen",
                        None,
                        source=provenance["proposal_source"],
                    )
                # Whatever the outcomes, the next cell is the next cell.
                next_rule, next_provenance = band.propose_band_rule(ledger)
                for rule in cells[5:]:
                    ledger.add_hypothesis(band.band_fingerprint(rule), "band_mechanisms", proposal(rule), None, "rejected_signal_screen", None, source="grid_v2")
                exhausted = band.propose_band_rule(ledger)
                sources = {row["source"] for row in ledger.lane_hypotheses("band_mechanisms")}
            finally:
                ledger.close()
        self.assertEqual([item["rule"] for item, _ in proposed], cells[:5])
        self.assertEqual(
            [provenance for _, provenance in proposed],
            [{"proposal_source": "grid_v2", "cell_index": index, "registrable": True} for index in range(5)],
        )
        self.assertEqual((next_rule["rule"], next_provenance["cell_index"]), (cells[5], 5))
        self.assertEqual(next_rule["title"], "Grammar C cell 6: %s" % band.compact_band_rule(cells[5]))
        self.assertIsNone(exhausted)
        self.assertEqual(sources, {"prior", "llm", "grid_v2"})
        for item, _ in proposed:
            self.assertEqual(set(item), {"title", "rationale", "expected_failure_mode", "rule"})
            self.assertEqual(band.normalized_band_rule(item["rule"]), item["rule"])
            self.assertNotEqual(band.band_fingerprint(item["rule"]), band.band_fingerprint(LIVE_RULE))
        self.assertEqual(len({band.band_fingerprint(item["rule"]) for item, _ in proposed}), 5)

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

    def stub_cache(
        self, directory, failing=(), unresolved=(), missing=(), closes_failing=(), price=0.80, margin=60.0, print_offset=5
    ):
        """Network-free BandCache: every window is +`margin` at every decision
        second and resolves up; the first BUY of the up token prints at
        `price`, `print_offset` seconds after each decision."""

        def fetch_closes(start_ts, end_ts):
            if start_ts - start_ts % band.DAY_S in closes_failing:
                raise OSError("binance down")
            return {str(ts): 70000.0 + (margin if ts % 300 >= 150 else 0.0) for ts in range(start_ts, end_ts)}

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
                {"side": "BUY", "asset": "up-%d" % ws, "timestamp": ws + d + print_offset, "price": price}
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

    def cell_cache(self, directory):
        """A tape the first grammar C cell trades: $80 margins clear the $75
        floor, the first print at 0.85 lies inside (0.80, 0.92] and arrives
        1 s after the decision, within patience 0."""
        return self.stub_cache(directory, price=0.85, margin=80.0, print_offset=1)

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
                "writer_version": band.PRINTS_WRITER_VERSION,
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

    def test_dry_run_proposes_from_the_ledger_without_network_or_cache(self):
        config = band_config(enabled=True, minimum_interval_seconds=0)
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                ledger.add_hypothesis(band.band_fingerprint(FIRST_CELL), "band_mechanisms", proposal(FIRST_CELL), None, "rejected_signal_screen", None, source="grid_v2")
                with mock.patch.object(band, "BandCache", side_effect=AssertionError("cache in dry run")):
                    dry = band.run_band_lane(config, ledger, state_dir, True, None, BASE_WS)
                lane_rows = ledger.lane_hypotheses("band_mechanisms")
                disabled = band.run_band_lane(band_config(enabled=False), ledger, state_dir, True, None, BASE_WS)
            finally:
                ledger.close()
            self.assertFalse((state_dir / "trial_ledger.jsonl").exists())
        self.assertEqual(dry["status"], "dry_run")
        self.assertEqual(dry["proposal"]["rule"], {**FIRST_CELL, "patience_s": 15})
        self.assertEqual(dry["provenance"], {"proposal_source": "grid_v2", "cell_index": 1, "registrable": True})
        self.assertEqual(dry["fingerprint"], band.band_fingerprint(dry["proposal"]["rule"]))
        self.assertEqual(len(lane_rows), 1)  # a dry run writes nothing
        self.assertEqual(disabled, {"status": "disabled"})

    def test_run_band_lane_screens_the_first_cell_then_accrues(self):
        config = band_config(enabled=True, maximum_new_windows_per_cycle=5, minimum_interval_seconds=0)
        # Windows 0..129 are eligible (index 130 would need one more second).
        now_ts = BASE_WS + 130 * 300 + 1199
        self.disarm_tripwire()
        cells = band.proposer_cells()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.cell_cache(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                fetching = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts)
                config["lanes"]["band_mechanisms"]["maximum_new_windows_per_cycle"] = 1000
                screened = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts)
                fingerprint = screened["fingerprint"]
                after_screen = ledger.accrual(fingerprint)
                status_after_screen = ledger.hypothesis(fingerprint)["status"]
                accrued = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts + 10 * 300)
                after_accrual = ledger.accrual(fingerprint)
                status_after_accrual = ledger.hypothesis(fingerprint)["status"]
                evidence = json.loads(Path(screened["artifact"]).read_text())
                dry = band.run_band_lane(config, ledger, state_dir, True, cache, now_ts + 10 * 300)
                lane_rows = ledger.lane_hypotheses("band_mechanisms")
            finally:
                ledger.close()
            trial_rows = [json.loads(line) for line in (state_dir / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(fetching["status"], "fetching")
        self.assertEqual((fetching["cache"]["fetched"], fetching["cache"]["remaining"]), (5, 125))
        self.assertEqual(fetching["accrual"]["evaluated"], 0)
        self.assertEqual(screened["status"], "stage_2_survivor")
        self.assertEqual((screened["proposal_source"], screened["cell_index"], screened["registrable"]), ("grid_v2", 0, True))
        self.assertEqual(fingerprint, band.band_fingerprint(FIRST_CELL))
        self.assertEqual(screened["cache"]["remaining"], 0)
        self.assertEqual(screened["stage_1"]["overall"]["signals"], 130)
        self.assertEqual(screened["stage_2"]["entries"], 130)
        self.assertAlmostEqual(screened["stage_2"]["mean_break_even"], band.break_even(0.85))
        self.assertEqual(evidence["last_window_start"], BASE_WS + 129 * 300)
        self.assertEqual(evidence["provenance"], {"proposal_source": "grid_v2", "cell_index": 0, "registrable": True})
        self.assertEqual(evidence["rule"], FIRST_CELL)
        self.assertEqual(evidence["stage_2"]["patience_s"], 0)
        self.assertTrue(evidence["stage_2"]["gates"]["print_lists_complete"])
        self.assertNotIn("llm", evidence)
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
                "e_bh": {"campaign_n": 64, "candidates": 1, "family": 1, "k_star": 0, "threshold": 1280.0, "overflow": False},
            },
        )
        self.assertEqual((after_accrual["n"], after_accrual["wins"]), (10, 10))
        self.assertEqual(after_accrual["last_window_start"], BASE_WS + 139 * 300)
        self.assertEqual(status_after_accrual, "accruing")
        # The next cell (patience 15) is screened on the grown cache.
        self.assertEqual(accrued["status"], "stage_2_survivor")
        self.assertEqual(accrued["fingerprint"], band.band_fingerprint(cells[1]))
        self.assertEqual(accrued["cell_index"], 1)
        self.assertEqual(accrued["stage_1"]["overall"]["signals"], 140)
        self.assertEqual(accrued["stage_2"]["entries"], 140)
        self.assertEqual(dry["status"], "dry_run")
        self.assertEqual(dry["proposal"]["rule"], cells[2])
        self.assertEqual(len(lane_rows), 2)
        self.assertEqual([row["source"] for row in lane_rows], ["grid_v2", "grid_v2"])
        self.assertEqual(
            [(row["stage"], row["verdict"], row["n"]) for row in trial_rows],
            [
                ("band_signal_screen", "stage_1_survivor", 130),
                ("band_entry_economics", "stage_2_survivor", 130),
                ("fresh_public_accrual", "continue", 10),
                ("band_signal_screen", "stage_1_survivor", 140),
                ("band_entry_economics", "stage_2_survivor", 140),
            ],
        )

    def test_lane_reports_grid_exhausted_and_keeps_accruing(self):
        config = band_config(enabled=True, maximum_new_windows_per_cycle=1000, minimum_interval_seconds=0)
        now_ts = BASE_WS + 130 * 300 + 1199
        self.disarm_tripwire()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.cell_cache(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                screened = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts)
                for rule in band.proposer_cells()[1:]:
                    ledger.add_hypothesis(band.band_fingerprint(rule), "band_mechanisms", proposal(rule), None, "rejected_signal_screen", None, source="grid_v2")
                exhausted = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts + 10 * 300)
                accrual = ledger.accrual(screened["fingerprint"])
                dry = band.run_band_lane(config, ledger, state_dir, True, cache, now_ts + 10 * 300)
            finally:
                ledger.close()
        self.assertEqual(screened["status"], "stage_2_survivor")
        self.assertEqual(exhausted["status"], "grid_exhausted")
        self.assertEqual(exhausted["accrual"]["evaluated"], 1)
        self.assertEqual((accrual["n"], accrual["wins"]), (10, 10))
        self.assertEqual(exhausted["rescreen"], {"rescreened": 0, "promoted": 0, "still_rejected": 0})
        self.assertNotIn("fingerprint", exhausted)
        self.assertEqual(dry, {"status": "grid_exhausted"})

    def test_support_only_rejection_is_rescreened_when_the_cache_grows(self):
        config = band_config(
            enabled=True,
            maximum_new_windows_per_cycle=1000,
            minimum_interval_seconds=0,
            gates={"minimum_signals": 10, "minimum_recent_signals": 20, "minimum_entries": 100},
        )
        self.disarm_tripwire()
        cells = band.proposer_cells()
        early_ts = BASE_WS + 60 * 300 + 1199  # 60 eligible windows: support fails
        late_ts = BASE_WS + 130 * 300 + 1199  # 130 eligible windows: support clears
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.cell_cache(directory)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                # 70 new windows arrive between the two screens; the production
                # threshold (~8 h) is patched down so the fixture tape suffices.
                self.enterContext(mock.patch.object(band, "RESCREEN_MIN_NEW_WINDOWS", 50))
                first = band.run_band_lane(config, ledger, state_dir, False, cache, early_ts)
                fingerprint = first["fingerprint"]
                status_first = ledger.hypothesis(fingerprint)["status"]
                second = band.run_band_lane(config, ledger, state_dir, False, cache, late_ts)
                status_second = ledger.hypothesis(fingerprint)["status"]
                accrual = ledger.accrual(fingerprint)
                evidence = json.loads(Path(first["artifact"]).read_text())
                third = band.run_band_lane(config, ledger, state_dir, False, cache, late_ts)
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
        self.assertEqual(evidence["provenance"]["proposal_source"], "grid_v2")
        self.assertEqual(second["fingerprint"], band.band_fingerprint(cells[1]))
        # Nothing left to re-screen once the cache has not grown.
        self.assertEqual(third["rescreen"], {"rescreened": 0, "promoted": 0, "still_rejected": 0})
        self.assertEqual(third["fingerprint"], band.band_fingerprint(cells[2]))
        self.assertEqual(
            [(row["stage"], row["verdict"], row["n"]) for row in trial_rows if row["candidate"] == fingerprint],
            [
                ("band_signal_screen", "stage_1_survivor", 60),
                ("band_entry_economics", "rejected_entry_economics", 60),
                ("band_entry_economics", "stage_2_survivor", 130),
            ],
        )

    def age_print_row(self, cache, window_start, decision_second):
        """Write one ok prints row back to writer v1 (no print list, no
        writer_version): a row --rebuild-prints has not reached or failed
        on.  Returns the row as it was, for restoring."""
        path = cache.print_path(window_start, decision_second)
        row = json.loads(path.read_text())
        self.assertEqual(row["status"], "ok")
        aged = {key: value for key, value in row.items() if key != "writer_version"}
        aged["signal_prints"] = None
        path.write_text(json.dumps(aged) + "\n")
        return row

    def test_listless_print_row_defers_the_cell_instead_of_rejecting_it(self):
        config = band_config(enabled=True, maximum_new_windows_per_cycle=1000, minimum_interval_seconds=0)
        now_ts = BASE_WS + 130 * 300 + 1199
        self.disarm_tripwire()
        cells = band.proposer_cells()
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.cell_cache(directory)
            cache.refresh(BASE_WS, now_ts, 1000)
            good_row = self.age_print_row(cache, BASE_WS + 50 * 300, 180)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                deferred = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts)
                lane_rows_deferred = ledger.lane_hypotheses("band_mechanisms")
                last_at_deferred = ledger.meta("band_mechanisms.last_at")
                again = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts)
                # The rebuild reaches the row: the same cell is screened whole.
                cache.print_path(BASE_WS + 50 * 300, 180).write_text(json.dumps(good_row) + "\n")
                screened = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts)
                status = ledger.hypothesis(screened["fingerprint"])["status"]
                lane_rows = ledger.lane_hypotheses("band_mechanisms")
            finally:
                ledger.close()
            evidence_files = sorted((state_dir / "evidence").rglob("*.json")) if (state_dir / "evidence").exists() else []
            trial_rows = [json.loads(line) for line in (state_dir / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(deferred["status"], "awaiting_print_rebuild")
        self.assertEqual(deferred["fingerprint"], band.band_fingerprint(cells[0]))
        self.assertEqual((deferred["cell_index"], deferred["registrable"], deferred["windows_without_print_list"]), (0, True, 1))
        self.assertEqual(deferred["cache"]["remaining"], 0)
        # A deferral is not a verdict: no hypothesis, no artifact, no trial
        # row, and the lane stays due so the cell is retried next loop.
        self.assertEqual(lane_rows_deferred, [])
        self.assertIsNone(last_at_deferred)
        self.assertEqual((again["status"], again["fingerprint"]), ("awaiting_print_rebuild", deferred["fingerprint"]))
        self.assertEqual(screened["status"], "stage_2_survivor")
        self.assertEqual((screened["fingerprint"], screened["cell_index"]), (deferred["fingerprint"], 0))
        self.assertEqual(screened["stage_2"]["entries"], 130)
        self.assertTrue(screened["stage_2"]["gates"]["print_lists_complete"])
        self.assertEqual(status, "stage_2_survivor")
        self.assertEqual([row["fingerprint"] for row in lane_rows], [deferred["fingerprint"]])
        self.assertEqual([path.name for path in evidence_files], ["%s.json" % deferred["fingerprint"]])
        self.assertEqual(
            [(row["stage"], row["verdict"], row["n"]) for row in trial_rows],
            [("band_signal_screen", "stage_1_survivor", 130), ("band_entry_economics", "stage_2_survivor", 130)],
        )

    def test_print_list_rejection_is_rescreened_once_the_rows_are_rebuilt(self):
        config = band_config(enabled=True, maximum_new_windows_per_cycle=1000, minimum_interval_seconds=0)
        now_ts = BASE_WS + 130 * 300 + 1199
        self.disarm_tripwire()
        cells = band.proposer_cells()
        fingerprint = band.band_fingerprint(cells[0])
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.cell_cache(directory)
            cache.refresh(BASE_WS, now_ts, 1000)
            good_row = self.age_print_row(cache, BASE_WS + 50 * 300, 180)
            windows = cache.windows(BASE_WS, now_ts)
            # A row written while the cache was half-rebuilt (--rescore-all,
            # or the lane before it deferred): rejected on the print-list gate alone.
            stale = band.evaluate_band_rule(windows, cache.load_prints(windows), cells[0], config["lanes"]["band_mechanisms"]["gates"], now_ts)
            stale.update({"fingerprint": fingerprint, "proposal": proposal(cells[0]), "provenance": {"proposal_source": "grid_v2"}})
            evidence_path = state_dir / ("evidence/band_mechanisms/%s.json" % fingerprint)
            band._atomic_write(evidence_path, json.dumps(stale, indent=2, sort_keys=True) + "\n")
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                ledger.add_hypothesis(fingerprint, "band_mechanisms", proposal(cells[0]), None, "rejected_entry_economics", evidence_path, source="grid_v2")
                stalled = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts)
                status_stalled = ledger.hypothesis(fingerprint)["status"]
                trial_exists_stalled = (state_dir / "trial_ledger.jsonl").exists()
                cache.print_path(BASE_WS + 50 * 300, 180).write_text(json.dumps(good_row) + "\n")
                # No new windows since the row was written: the rebuild alone re-opens it.
                healed = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts)
                status_healed = ledger.hypothesis(fingerprint)["status"]
                accrual = ledger.accrual(fingerprint)
                evidence = json.loads(evidence_path.read_text())
            finally:
                ledger.close()
            trial_rows = [json.loads(line) for line in (state_dir / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(
            stale["stage_2"]["gates"],
            {"support": True, "wilson_above_break_even": True, "positive_mean_net": True, "print_lists_complete": False},
        )
        # Still listless: the rescreen evaluates but writes nothing, and the
        # proposer's next cell (same decision second) defers on the same row.
        self.assertEqual(stalled["rescreen"], {"rescreened": 0, "promoted": 0, "still_rejected": 0})
        self.assertEqual((stalled["status"], stalled["fingerprint"]), ("awaiting_print_rebuild", band.band_fingerprint(cells[1])))
        self.assertEqual(status_stalled, "rejected_entry_economics")
        self.assertFalse(trial_exists_stalled)
        self.assertEqual(healed["rescreen"], {"rescreened": 1, "promoted": 1, "still_rejected": 0})
        self.assertEqual(status_healed, "stage_2_survivor")
        self.assertEqual((accrual["n"], accrual["last_window_start"]), (0, BASE_WS + 129 * 300))
        self.assertTrue(evidence["stage_2"]["gates"]["print_lists_complete"])
        self.assertEqual((evidence["stage_2"]["entries"], evidence["previous_window_count"]), (130, 130))
        self.assertEqual((healed["status"], healed["fingerprint"]), ("stage_2_survivor", band.band_fingerprint(cells[1])))
        self.assertEqual(
            [(row["candidate"] == fingerprint, row["stage"], row["verdict"], row["n"]) for row in trial_rows],
            [
                (True, "band_entry_economics", "stage_2_survivor", 130),
                (False, "band_signal_screen", "stage_1_survivor", 130),
                (False, "band_entry_economics", "stage_2_survivor", 130),
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

        def http_json(url, retries=3, timeout=30.0):
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
            # but written by an older writer (no signal_prints, no version).
            for ws in stale | {legacy}:
                for d in band.BAND_DECISION_SECONDS:
                    row = json.loads(seeded.print_path(ws, d).read_text())
                    if ws == legacy:
                        del row["signal_prints"]
                        del row["writer_version"]
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
            # limit=0 is the network-free census: the whole backlog is reported.
            backlog = band.rebuild_prints(self.stub_cache(directory), BASE_WS, now_ts, limit=0)
            first = band.rebuild_prints(cache, BASE_WS, now_ts, workers=2)
            rebuilt_row = json.loads(cache.print_path(BASE_WS + 2 * 300, 240).read_text())
            legacy_row = json.loads(cache.print_path(legacy, 240).read_text())
            broken_row = json.loads(cache.print_path(broken, 240).read_text())
            again = band.rebuild_prints(self.stub_cache(directory), BASE_WS, now_ts)
            bounded = band.rebuild_prints(self.stub_cache(directory), BASE_WS, now_ts, limit=0)
        self.assertEqual(sorted(calls), sorted((stale | {legacy}) - {broken}))
        self.assertEqual((backlog["pending"], backlog["selected"], backlog["rebuilt"]), (4, 0, 0))
        self.assertEqual(
            first,
            {
                "settled": 11,
                "covered": 7,
                "unlisted": 0,
                "pending": 4,
                "selected": 4,
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
        self.assertEqual(legacy_row["writer_version"], band.PRINTS_WRITER_VERSION)
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
            ledger = loop.Ledger(state_dir / "research.sqlite3")
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
                # A second pass keeps the original pre-pagination artifact.  Run
                # three days later on the same (now lagging) cache, the recent
                # gate is anchored on the cache tail, so the live rule stands.
                lagged = band.rescore_band_hypotheses(config, ledger, windows, prints, now_ts + 3 * band.DAY_S)
                status_lagged = ledger.hypothesis(fingerprints["live"])["status"]
                accrual_lagged = ledger.accrual(fingerprints["live"])
                backup_again = json.loads((state_dir / ("evidence/band_mechanisms/%s.pre_pagination.json" % fingerprints["live"])).read_text())
            finally:
                ledger.close()
            trial_rows = [json.loads(line) for line in (state_dir / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(report["accrual_rows_deleted"], 3)
        self.assertEqual(report["recent_clock"], BASE_WS + 129 * 300 + band.WINDOW_S + band.RESOLUTION_LAG_S)
        self.assertEqual(lagged["recent_clock"], report["recent_clock"])
        self.assertEqual(lagged["accrual_rows_deleted"], 1)
        self.assertEqual(status_lagged, "stage_2_survivor")
        self.assertEqual((accrual_lagged["n"], accrual_lagged["last_window_start"]), (0, BASE_WS + 129 * 300))
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
        # The LLM-era artifact's provenance (its "llm" block) is carried over.
        self.assertEqual(artifacts["live"]["provenance"], {"proposal_source": "prior"})
        self.assertNotIn("llm", artifacts["live"])
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

    def test_rescore_leaves_unvisited_hypotheses_whole_when_it_fails_mid_run(self):
        config = band_config(enabled=True)
        self.disarm_tripwire()
        now_ts = BASE_WS + 130 * 300 + 1199
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.stub_cache(directory, price=0.85)
            cache.refresh(BASE_WS, now_ts, budget=1000)
            windows = cache.windows(BASE_WS, now_ts)
            prints = cache.load_prints(windows)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                live = band.band_fingerprint(LIVE_RULE)
                for fingerprint, rule in ((live, LIVE_RULE), ("malformed", {"bad": 1})):
                    evidence_path = state_dir / ("evidence/band_mechanisms/%s.json" % fingerprint)
                    band._atomic_write(evidence_path, json.dumps({"window_count": 40}))
                    ledger.add_hypothesis(fingerprint, "band_mechanisms", proposal(rule), None, "promote_candidate", evidence_path, source="prior")
                    ledger.accrue(fingerprint, "band_mechanisms", [(BASE_WS + 300 * index, 0.9, True) for index in range(25)])
                with self.assertRaises(ValueError):
                    band.rescore_band_hypotheses(config, ledger, windows, prints, now_ts)
                visited = (ledger.hypothesis(live)["status"], ledger.accrual(live)["n"])
                unvisited = (ledger.hypothesis("malformed")["status"], ledger.accrual("malformed")["n"])
            finally:
                ledger.close()
        # The visited hypothesis is fully new (re-seeded at n=0); the one the
        # failure stopped at is fully old: status and e-process both intact,
        # never a survivor status without an accrual row.
        self.assertEqual(visited, ("stage_2_survivor", 0))
        self.assertEqual(unvisited, ("promote_candidate", 25))

    def test_main_resolves_the_runner_config_and_holds_the_cycle_lock(self):
        self.assertEqual(band.config_path("explicit.json"), Path("explicit.json"))
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config = json.loads((ROOT / "deploy/strategy-research-loop.json").read_text())
            config["state_dir"] = directory
            overlay = state_dir / "loop-config.local.json"
            overlay.write_text(json.dumps(config))
            with mock.patch.object(band, "OVERLAY_CONFIG", overlay):
                # The runner's overlay is what the lane accrues under: it wins.
                self.assertEqual(band.config_path(None), overlay)
                # A live factory cycle holds the lock: the action fails before
                # it touches the cache.
                with loop.CycleLock(state_dir / "locks/cycle.lock"), mock.patch.object(band, "BandCache") as cache_class:
                    with self.assertRaises(RuntimeError):
                        band.main(["--rebuild-prints", "--limit", "0"])
                cache_class.assert_not_called()
            with mock.patch.object(band, "OVERLAY_CONFIG", state_dir / "absent.json"):
                self.assertEqual(band.config_path(None), band.DEPLOY_CONFIG)

    # --- entry semantics, tripwire, e-BH family ------------------------------

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
        # A grammar C patience is a horizon on the print offsets
        # (executable_truth.print_look): prints stamped no later than
        # patience + 1 s count, so patience 0 reads only the next second.
        self.assertEqual(band.print_entry_price(first, "up", 0.80, 0.92, "one_look", 0), (None, "no_print"))
        self.assertEqual(band.print_entry_price(first, "up", 0.80, 0.92, "one_look", 15), (None, "out_of_band"))
        self.assertEqual(band.print_entry_price(first, "up", 0.80, 0.92, "patient", 15), (0.91, None))
        self.assertEqual(band.print_entry_price(first, "up", 0.80, 0.90, "patient", 15), (None, "out_of_band"))
        self.assertEqual(band.print_entry_price(first, "up", 0.80, 0.90, "patient", 30), (0.89, None))
        second = prints[(starts[1], 240)]
        self.assertEqual(band.print_entry_price(second, "up", 0.55, 0.92, "one_look", 0), (0.80, None))
        self.assertEqual(band.print_entry_price(second, "up", 0.80, 0.92, "one_look", 0), (None, "out_of_band"))
        self.assertEqual(band.print_entry_price(prints[(starts[4], 240)], "up", 0.80, 0.92, "one_look", 0), (None, "no_print_list"))
        self.assertEqual(band.print_entry_price(prints[(starts[3], 240)], "up", 0.80, 0.92, "one_look", 30), (None, "no_print"))
        cell = {**FIRST_CELL, "decision_second": 240, "patience_s": 15}
        cell_scored = band._labelled(rows, band.band_signal_records(rows, cell))
        self.assertEqual(len(cell_scored), 0)  # $60 margins are below the $75 floor
        wide = [window(ws, margin=80.0) for ws in starts]
        cell_scored = band._labelled(wide, band.band_signal_records(wide, cell))
        economics = band.band_entry_economics(cell_scored, prints, cell, GATES)
        self.assertEqual((economics["patience_s"], economics["entries"]), (15, 0))
        self.assertEqual(
            tuple(economics[key] for key in ("out_of_band_prints", "windows_without_print", "windows_without_print_list", "uncached_windows")),
            (3, 1, 1, 1),
        )
        # A cell needs the print lists whatever the lane semantics: fail closed until rebuilt.
        self.assertFalse(economics["gates"]["print_lists_complete"])
        self.assertEqual(band.band_accrual_outcomes(wide, prints, cell, -1), [])
        patient_cell = band.band_entry_economics(cell_scored, prints, cell, GATES, "patient")
        self.assertEqual((patient_cell["entries"], patient_cell["wins"]), (1, 1))
        self.assertAlmostEqual(patient_cell["mean_break_even"], band.break_even(0.91))

    def test_patient_semantics_fail_closed_on_rows_without_print_lists(self):
        rows = windows(4)
        starts = [row["window_start"] for row in rows]
        listed = {"status": "ok", "signal": "up", "signal_entry": 0.80, "signal_prints": [[1, 0.80]]}
        prints = {(starts[index], 240): listed for index in (0, 2, 3)}
        prints[(starts[1], 240)] = {"status": "ok", "signal": "up", "signal_entry": 0.80}
        scored = band._labelled(rows, band.band_signal_records(rows, LIVE_RULE))
        gates = {**GATES, "minimum_entries": 1}
        patient = band.band_entry_economics(scored, prints, LIVE_RULE, gates, "patient")
        one_look = band.band_entry_economics(scored, prints, LIVE_RULE, gates)
        # One row predates the print list: stage 2 would otherwise score a
        # subset of the windows, so the patient verdict fails closed.
        self.assertEqual((patient["entries"], patient["windows_without_print_list"]), (3, 1))
        self.assertFalse(patient["gates"]["print_lists_complete"])
        self.assertFalse(patient["survivor"])
        # one_look never needs the list: the live lane's artifact shape is unchanged.
        self.assertEqual(set(patient["gates"]) - set(one_look["gates"]), {"print_lists_complete"})
        self.assertEqual((one_look["entries"], one_look["windows_without_print_list"]), (4, 0))
        # Accrual stops at the listless row so its window accrues once rebuilt.
        self.assertEqual(
            band.band_accrual_outcomes(rows, prints, LIVE_RULE, -1, "patient"),
            [(starts[0], band.break_even(0.80), True)],
        )
        self.assertEqual(len(band.band_accrual_outcomes(rows, prints, LIVE_RULE, -1)), 4)
        complete = {**prints, (starts[1], 240): listed}
        self.assertTrue(band.band_entry_economics(scored, complete, LIVE_RULE, gates, "patient")["gates"]["print_lists_complete"])
        self.assertEqual(len(band.band_accrual_outcomes(rows, complete, LIVE_RULE, -1, "patient")), 4)

    def test_capped_tape_short_of_the_decision_second_is_uncovered(self):
        rows = windows(3)
        starts = [row["window_start"] for row in rows]

        def row(oldest, complete):
            return {
                "status": "ok", "signal": "up", "decision_second": 180, "signal_entry": 0.80, "signal_prints": [[1, 0.80]],
                "coverage": {"oldest_offset_s": oldest, "pages": band.TRADES_MAX_PAGES, "complete": complete},
            }

        # Eight full pages that stopped after the decision second: the first
        # print after it may be missing, so the row is not scored.
        self.assertEqual(band.print_entry_price(row(200, False), "up", 0.55, 0.92), (None, "uncovered"))
        self.assertEqual(band.print_entry_price(row(200, False), "up", 0.55, 0.92, "patient"), (None, "uncovered"))
        self.assertEqual(band.print_entry_price(row(200, False), "up", 0.55, 0.92, "one_look", 0), (None, "uncovered"))
        # Reaching the decision second, or read to the tape's end: scored.
        self.assertEqual(band.print_entry_price(row(180, False), "up", 0.55, 0.92), (0.80, None))
        self.assertEqual(band.print_entry_price(row(200, True), "up", 0.55, 0.92), (0.80, None))
        rule = {**LIVE_RULE, "decision_second": 180}
        prints = {(starts[0], 180): row(200, False), (starts[1], 180): row(150, False), (starts[2], 180): row(200, True)}
        scored = band._labelled(rows, band.band_signal_records(rows, rule))
        stage_2 = band.band_entry_economics(scored, prints, rule, GATES)
        self.assertEqual((stage_2["entries"], stage_2["uncovered_windows"], stage_2["windows_without_print"]), (2, 1, 0))
        self.assertEqual([ws for ws, _, _ in band.band_accrual_outcomes(rows, prints, rule, -1)], [starts[1], starts[2]])

    def test_tripwire_holds_too_good_survivors_as_manual_audit_until_cleared(self):
        config = band_config(enabled=True, maximum_new_windows_per_cycle=1000, minimum_interval_seconds=0)
        now_ts = BASE_WS + 130 * 300 + 1199
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            cache = self.cell_cache(directory)  # 130/130 at stage 2: trips
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                screened = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts)
                fingerprint = screened["fingerprint"]
                status_screened = ledger.hypothesis(fingerprint)["status"]
                seeded = ledger.accrual(fingerprint)
                held = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts + 10 * 300)
                status_held = ledger.hypothesis(fingerprint)["status"]
                config["lanes"]["band_mechanisms"]["audit_cleared"] = [fingerprint]
                cleared = band.run_band_lane(config, ledger, state_dir, False, cache, now_ts + 20 * 300)
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
            evidence["stage_2"]["tripwire"], {"triggered": True, "maximum_win_rate": 0.995, "minimum_entries": 100}
        )
        # Evidence keeps accruing during the audit; promotion does not, even
        # though the accrual's own 10/10 is below the tripwire's support.
        self.assertEqual((seeded["n"], seeded["last_window_start"]), (0, BASE_WS + 129 * 300))
        self.assertEqual((held["accrual"]["manual_audit"], held["accrual"]["e_bh"]["candidates"]), (1, 0))
        self.assertEqual(status_held, "manual_audit")
        # The second cell, screened in the held run, trips too and stays held;
        # only the cleared fingerprint returns to the running.
        self.assertEqual(held["status"], "manual_audit")
        self.assertEqual((cleared["accrual"]["manual_audit"], cleared["accrual"]["accruing"]), (1, 1))
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
        # WR > 0.995 at n >= 100 (CLAUDE.md section 5): 99/99 is below support,
        # 199/200 is exactly 0.995 and does not trip, 100/100 and 200/200 do.
        self.assertFalse(band.tripwire_triggered(99, 99))
        self.assertFalse(band.tripwire_triggered(199, 200))
        self.assertTrue(band.tripwire_triggered(100, 100))
        self.assertTrue(band.tripwire_triggered(200, 200))

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
            ("held", "accruing", 2000.0, 120, 120, "promote"),  # WR 1.0 at n >= 100 trips
            ("dead", "accruing", 0.05, 30, 10, "kill"),
        ]
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            state_dir = Path(directory)
            config["state_dir"] = directory
            ledger = loop.Ledger(state_dir / "research.sqlite3")
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
        # The tripped candidate and the futility kill are out of the running
        # but still in the family of six e-processes started.
        self.assertEqual(
            registered,
            {
                "evaluated": 6, "promoted": 2, "killed": 1, "accruing": 2, "manual_audit": 1, "skipped": 0,
                "e_bh": {"campaign_n": 64, "candidates": 4, "family": 6, "k_star": 2, "threshold": 640.0, "overflow": False},
            },
        )
        self.assertEqual(
            statuses,
            {"a": "promote_candidate", "b": "promote_candidate", "c": "accruing", "d": "accruing", "held": "manual_audit", "dead": "killed_futility"},
        )
        # N=6 (registered 2; six e-processes started, the killed one keeps its
        # row, four still running): 100 >= 40 at k=3, flagged as overflow.
        self.assertEqual(
            overflow["e_bh"],
            {"campaign_n": 6, "candidates": 4, "family": 6, "k_star": 3, "threshold": 6 / (0.05 * 3), "overflow": True},
        )
        self.assertEqual((statuses_overflow["c"], statuses_overflow["d"]), ("promote_candidate", "accruing"))
        # No registered N: the flags lapse; futility and the audit hold stand.
        self.assertEqual(
            unregistered["e_bh"],
            {"campaign_n": None, "candidates": 4, "family": 6, "k_star": 0, "threshold": None, "overflow": False},
        )
        self.assertEqual(
            statuses_unregistered,
            {"a": "accruing", "b": "accruing", "c": "accruing", "d": "accruing", "held": "manual_audit", "dead": "killed_futility"},
        )
        self.assertEqual(band.campaign_n({"generator": {"campaign_n": 7}}), 7)
        self.assertEqual(band.campaign_n({"generator": {"campaign_n": 7}, "lanes": {"band_mechanisms": {"campaign_n": 9}}}), 9)
        self.assertIsNone(band.campaign_n({}))

    def test_network_error_budget_fails_fast_after_consecutive_failures(self):
        calls = []

        def failing(url, retries=3, timeout=30.0):
            calls.append((retries, timeout))
            raise OSError("down")

        band.reset_network_error_budget()
        with mock.patch.object(band, "http_json", side_effect=failing):
            for _ in range(band.NETWORK_ERROR_BUDGET):
                with self.assertRaises(OSError):
                    band.lane_http_json("https://x")
            with self.assertRaises(band.NetworkUnavailable):
                band.lane_http_json("https://x")
        # Lane fetches use the short timeout and fewer retries, and the budget
        # blocked the fourth call before it reached the network.
        self.assertEqual(len(calls), band.NETWORK_ERROR_BUDGET)
        self.assertEqual(calls[0], (band.LANE_HTTP_RETRIES, band.LANE_HTTP_TIMEOUT_S))
        band.reset_network_error_budget()
        with mock.patch.object(band, "http_json", return_value={"ok": 1}):
            self.assertEqual(band.lane_http_json("https://x"), {"ok": 1})
        band.reset_network_error_budget()

    def test_run_cycle_band_lane_disabled_returns_disabled(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        self.assertFalse(config["lanes"]["band_mechanisms"]["enabled"])
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            with mock.patch.object(
                loop, "resource_status", return_value={"passed": True, "checks": {}}
            ), mock.patch.object(loop.band_lane, "BandCache") as cache:
                result = loop.run_cycle(config, True, "band_mechanisms")
        cache.assert_not_called()
        self.assertEqual(result["lane"], "band_mechanisms")
        self.assertEqual(result["lane_result"], {"status": "disabled"})
        self.assertEqual(set(result), {"cycle_id", "started_at", "finished_at", "dry_run", "lane", "lane_result", "resources", "ledger"})

    def test_run_cycle_dispatches_the_band_lane(self):
        config = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            config["state_dir"] = directory
            with mock.patch.object(
                loop, "resource_status", return_value={"passed": True, "checks": {}}
            ), mock.patch.object(loop, "run_band_lane", return_value={"status": "not_due"}) as lane:
                result = loop.run_cycle(config, False, "band_mechanisms")
            lane.assert_called_once_with(config, mock.ANY, Path(directory), False)
            written = json.loads((Path(directory) / "status.json").read_text())
        self.assertEqual(result["lane_result"], {"status": "not_due"})
        self.assertEqual(written["lane_result"], {"status": "not_due"})
        self.assertEqual(result["ledger"]["last_cycle"]["status"], "completed")

    def test_deploy_config_is_fail_closed_and_overlay_mirrors_it(self):
        deploy = loop.load_config(ROOT / "deploy/strategy-research-loop.json")
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
        self.assertEqual(deploy["resource_policy"]["minimum_free_disk_gib"], 20)
        overlay_path = ROOT / "logs/strategy-research/loop-config.local.json"
        if not overlay_path.is_file():
            self.skipTest("gitignored overlay not present")
        # The overlay honours the same contract and differs only in the lane
        # switch and the Phase 0 disk gate (5 GiB).
        overlay = loop.load_config(overlay_path)
        self.assertTrue(overlay["lanes"]["band_mechanisms"]["enabled"])
        self.assertEqual(overlay["resource_policy"]["minimum_free_disk_gib"], 5)
        expected = json.loads(json.dumps(deploy))
        expected["lanes"]["band_mechanisms"]["enabled"] = True
        expected["resource_policy"]["minimum_free_disk_gib"] = 5
        self.assertEqual(overlay, expected)


if __name__ == "__main__":
    unittest.main()
