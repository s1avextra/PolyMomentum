from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import math
import os
from pathlib import Path
import random
import subprocess
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


truth = _load("executable_truth", "scripts/executable_truth.py")
race = _load("band_shadow_race", "scripts/band_shadow_race.py")
band = truth.band_lane
generator = truth.factory_generator

BASE_WS = 1787788800  # 2026-08-25T00:00Z
DAY_S = 86400
DECISIONS = truth.DECISION_SECONDS
FEE = 0.07
# The explicit family of the planted fixture: the diluted d210_f75 cell and
# the planted d210_f100 one.
PLANTED_CELLS = ["d210_f75_c0.92_p0", "d210_f100_c0.92_p0"]


def sample(t, worst, vwap=None, c=None, fresh=True, shares=26.0, depth_limited=False, budgets=3):
    """One ladder sample of a directional record: the same quote at every
    budget unless a caller edits the list; complement best ask coherent
    with the VWAP unless given."""
    vwap = worst if vwap is None else vwap
    quote = [worst, vwap, shares, depth_limited]
    return {
        "t": t,
        "q": [list(quote) for _ in range(budgets)],
        "c": round(1.0 - vwap, 4) if c is None else c,
        "age": 0.0,
        "fresh": fresh,
    }


def ladder(ws, anchor_s, direction, margin, samples, host=None, basis="binance", flush_offset=31.2):
    record = {
        "type": "band_ladder",
        "cat": "signal",
        "ts": ws + anchor_s + flush_offset,
        "cid": "%016x" % ws,
        "anchor_s": anchor_s,
        "basis": basis,
        "open": 70000.0,
        "btc": 70000.0 + (margin or 0.0),
        "margin": margin,
        "direction": direction,
        "budgets_usd": [5.0, 25.0, 100.0],
        "samples": samples,
        "cycles": len(samples),
        "max_gap_s": 1.01,
    }
    if host:
        record["host"] = host
    return record


def anchor(ws, anchor_s, margin, ask):
    side = {"best_ask": ask, "book_age_s": 0.1, "vwap": ask, "worst": ask, "shares": 10.0}
    other = {"best_ask": round(1.0 - ask, 4), "book_age_s": 0.1, "vwap": round(1.0 - ask, 4), "worst": round(1.0 - ask, 4), "shares": 10.0}
    up, down = (side, other) if margin > 0 else (other, side)
    return {
        "type": "band_anchor",
        "ts": ws + anchor_s + 0.2,
        "cid": "%016x" % ws,
        "anchor_s": anchor_s,
        "elapsed_s": anchor_s + 0.2,
        "btc": 70000.0 + margin,
        "open": 70000.0,
        "margin": margin,
        "direction": "up" if margin > 0 else "down",
        "stake_usd": 10.0,
        "quote_budget_usd": 10.0,
        "up": up,
        "down": down,
        "pair_sum": 1.0,
    }


def fill(order_id, price, shares, rate=FEE, kind="filled"):
    return {
        "type": kind,
        "cat": "order",
        "order_id": order_id,
        "fill_price": price,
        "filled": shares,
        "fee": rate * price * (1.0 - price) * shares,
        "ts": 1.0,
    }


def write_sessions(directory, records, name="session_20260901_000000.jsonl"):
    path = Path(directory) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ['{"ts": 1.0, "cat": "price", "type": "snapshot", "btc": 70000.0}', "not json {"]
    lines += [json.dumps(record) for record in records]
    with path.open("a") as handle:
        handle.write("\n".join(lines) + "\n")


def prints_row(ws, decision_s, direction, prints, writer_version=2, complete=False):
    row = {
        "window_start": ws,
        "decision_second": decision_s,
        "status": "ok",
        "signal": direction,
        "signal_entry": prints[0][1] if prints else None,
        "coverage": {"oldest_offset_s": 100, "pages": 2, "complete": complete},
    }
    if writer_version == 2:
        row["signal_prints"] = prints
        row["writer_version"] = 2
    return row


def write_cache(directory, specs):
    """Margin-study day files, Gamma outcomes and print rows for window
    specs {ws, open, margins{d}, final_margin, official, prints{d}, writer{d}}.
    Merges into files already there, so a fixture can grow."""
    margin_dir = Path(directory) / "margin"
    prints_dir = Path(directory) / "prints"
    margin_dir.mkdir(parents=True, exist_ok=True)
    prints_dir.mkdir(parents=True, exist_ok=True)
    outcomes_path = margin_dir / "gamma_outcomes.json"
    outcomes = json.loads(outcomes_path.read_text()) if outcomes_path.is_file() else {}
    by_day = {}
    for spec in specs:
        ws = spec["ws"]
        closes = {ws: spec["open"], ws + 300: spec["open"] + spec["final_margin"]}
        for d in DECISIONS:
            margin = spec["margins"].get(d)
            if margin is not None:
                closes[ws + d] = spec["open"] + margin
        for ts, price in closes.items():
            by_day.setdefault(ts - ts % DAY_S, {})[str(ts)] = price
        outcomes[str(ws)] = spec["official"]
        for d in band.BAND_DECISION_SECONDS:
            direction = spec.get("direction_at", {}).get(d) or spec.get("direction")
            row = prints_row(ws, d, direction, spec["prints"].get(d, []), spec.get("writer", {}).get(d, 2))
            (prints_dir / ("%d_%d.json" % (ws, d))).write_text(json.dumps(row) + "\n")
    for day, closes in by_day.items():
        path = margin_dir / ("binance_%d.json" % day)
        merged = json.loads(path.read_text()) if path.is_file() else {}
        merged.update(closes)
        path.write_text(json.dumps(merged))
    outcomes_path.write_text(json.dumps(outcomes))
    return margin_dir, prints_dir


def cache_for(directory):
    def fail(*args, **kwargs):
        raise AssertionError("network fetch in a test")

    return band.BandCache(
        margin_dir=Path(directory) / "margin",
        prints_dir=Path(directory) / "prints",
        fetch_closes=fail,
        fetch_market=fail,
        fetch_trades=fail,
    )


def loop_config(directory):
    return {"state_dir": str(directory), "generator": {"trial_ledger_enabled": True}}


def be(price):
    return price + FEE * price * (1.0 - price)


def population(count, start_index=0, plant=True, spacing=4500, seed=7):
    """Synthetic windows at a 75-minute spacing (300 span 15.6 days).  Prices
    0.82..0.90 (every cap in the grammar admits them; break-even ~0.87).
    Labels are assigned by quota per price level so every cell's realized win
    rate sits at its break-even: the null.  With plant=True 40% of windows
    carry |margin| 120 at 210 s only (60 elsewhere) and win at break-even +
    10 pp: the planted cell is d210_f100 (any cap, any patience)."""
    rng = random.Random(seed + start_index)
    specs = []
    for index in range(start_index, start_index + count):
        ws = BASE_WS + index * spacing
        # 40% planted, 40% |margin| 60, 20% |margin| 90 (null: 50/50).
        kind = ("base60", "planted", "base60", "planted", "base90")[index % 5] if plant else ("base60", "base90")[index % 2]
        sign = 1 if rng.random() < 0.5 else -1
        prices = {d: rng.choice([0.82, 0.84, 0.86, 0.88, 0.90]) for d in DECISIONS}
        size = {"base60": 60.0, "base90": 90.0, "planted": 60.0}[kind]
        margins = {d: sign * size for d in DECISIONS}
        if kind == "planted":
            margins[210] = sign * 120.0
        specs.append(
            {
                "ws": ws,
                "open": 70000.0,
                "margins": margins,
                "final_margin": sign * 200.0,
                "direction": "up" if sign > 0 else "down",
                "prices": prices,
                "kind": kind,
                "prints": {d: [[1, prices.get(d, prices[240])]] for d in band.BAND_DECISION_SECONDS},
            }
        )
    # Quota labels: per (kind, price at 210 s) the realized win rate equals
    # the target; the win pattern is spread evenly through the sequence.
    groups = {}
    for spec in specs:
        groups.setdefault((spec["kind"], spec["prices"][210]), []).append(spec)
    for (kind, price), members in groups.items():
        target = min(0.99, be(price) + (0.10 if kind == "planted" else 0.0))
        wins = round(target * len(members))
        for position, spec in enumerate(members):
            # Bresenham spread: exactly `wins` of len(members) are wins.
            won = ((position + 1) * wins) // len(members) - (position * wins) // len(members) == 1
            spec["official"] = spec["direction"] if won else ("down" if spec["direction"] == "up" else "up")
            # Labels follow the final price (a loss is a reversal), so the
            # fixture carries no oracle noise: the ceiling is 1.0.
            spec["final_margin"] = spec["final_margin"] if won else -spec["final_margin"]
    return specs


def population_records(specs, budgets=3):
    records = []
    for spec in specs:
        for d in DECISIONS:
            price = spec["prices"][d]
            records.append(ladder(spec["ws"], d, spec["direction"], spec["margins"][d], [sample(0, price, budgets=budgets)]))
    return records


class ExecutableTruthTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.directory = self.enterContext(tempfile.TemporaryDirectory(dir=str(ROOT / "logs")))
        self.sessions = Path(self.directory) / "sessions"
        self.db = truth.open_db(Path(self.directory) / "windows.sqlite3")
        self.addCleanup(self.db.close)

    # --- build ---------------------------------------------------------------

    def small_fixture(self):
        """Six windows: five labelled, one unresolved; ladders at every
        decision for the first four; the fifth is young and ladder-less; a
        240 s print row written by writer v1 on the second window."""
        specs = []
        open_ = 70000.0
        for index in range(6):
            ws = BASE_WS + index * 300
            # Consecutive windows share the 1 s close at ws + 300 (this
            # window's final close is the next window's open), as on the tape.
            final = 120.0 if index != 3 else 3.0
            specs.append(
                {
                    "ws": ws,
                    "open": open_,
                    "margins": {d: 80.0 for d in DECISIONS},
                    "final_margin": final,
                    "direction": "up",
                    "official": "up" if index != 5 else None,
                    "prints": {d: [[1, 0.95], [4, 0.97]] for d in band.BAND_DECISION_SECONDS},
                    "writer": {240: 1} if index == 1 else {},
                }
            )
            open_ += final
        write_cache(self.directory, specs)
        records = []
        for index in range(4):
            ws = BASE_WS + index * 300
            for d in DECISIONS:
                samples = [sample(t, 0.95) for t in range(3)]
                if index == 0 and d == 180:
                    samples.append(sample(7, 0.995))  # favourite ask crosses 0.99 at 187 s
                records.append(ladder(ws, d, "up", 80.0, samples, host="mac" if index == 0 else None))
            records.append(anchor(ws, 240, 80.0, 0.95))
        records += [fill("o1", 0.92, 5.42), fill("o1", 0.92, 5.42, kind="reconciled"), fill("o2", 0.80, 6.0), fill("o3", 0.95, 30.0)]
        write_sessions(self.sessions, records)
        return specs

    def test_build_appends_one_row_per_labelled_window_and_decision(self):
        self.small_fixture()
        now_ts = BASE_WS + 5 * 300 + 300 + band.RESOLUTION_LAG_S + 1  # the sixth window just eligible
        cache = cache_for(self.directory)
        summary = truth.build(self.db, cache, BASE_WS, now_ts, [self.sessions], sha="test-sha")
        # Five labelled windows; the fifth has no ladder and is inside the
        # grace period, so its six rows wait; the unresolved sixth is skipped.
        self.assertEqual((summary["rows_inserted"], summary["pending_ladder_grace"]), (24, 6))
        self.assertEqual((summary["windows_total"], summary["ladder_rows"], summary["anchor_rows"]), (4, 24, 4))
        self.assertEqual(summary["ladder_rows_by_host"], {"mac": 6, "vps": 18})
        self.assertEqual(summary["session_records"]["ladders_unplaced"], 0)
        self.assertEqual(summary["fee"]["n"], 3)  # o1's reconciled duplicate is the same order
        self.assertAlmostEqual(summary["fee"]["rate"], FEE, places=9)
        self.assertEqual(summary["fee"]["source"], "engine_rate_via_fills")
        self.assertNotIn("parity", summary["fee"])
        # 12 print rows (180/210/240 x 4 windows), one by writer v1.
        self.assertAlmostEqual(summary["writer_v2_share"], 11 / 12)
        self.assertEqual(summary["rows_without_signal_prints"], 1)
        rows = truth.load_rows(self.db)
        self.assertEqual(len(rows), 24)
        first = [row for row in rows if row["window_start"] == BASE_WS and row["decision_s"] == 240][0]
        self.assertEqual((first["git_sha"], first["host"], first["ladder_host"], first["ladder_basis"]), ("test-sha", "mac", "mac", "binance"))
        self.assertEqual((first["margin"], first["direction"], first["official"]), (80.0, "up", "up"))
        self.assertEqual((first["final_margin"], first["final_bucket"]), (120.0, "100-150"))
        self.assertEqual((first["print_status"], first["print_writer_version"], first["print_signal_entry"]), ("ok", 2, 0.95))
        self.assertEqual(first["prints"], [[1, 0.95], [4, 0.97]])
        self.assertEqual(first["anchor"]["up"]["worst"], 0.95)
        self.assertEqual(len(first["ladder"]["samples"]), 3)
        v1 = [row for row in rows if row["window_start"] == BASE_WS + 300 and row["decision_s"] == 240][0]
        self.assertEqual((v1["print_status"], v1["print_writer_version"], v1["prints"]), ("writer_v1", 1, None))
        at_150 = [row for row in rows if row["decision_s"] == 150][0]
        self.assertEqual((at_150["print_status"], at_150["ladder"]["anchor_s"]), ("no_print_row", 150))
        # The finer anchors have no print column either: ladder-only rows.
        self.assertEqual({(row["decision_s"], row["print_status"], row["ladder"]["anchor_s"]) for row in rows if row["decision_s"] in (195, 225)}, {(195, "no_print_row", 195), (225, "no_print_row", 225)})
        self.assertEqual([row["final_bucket"] for row in rows if row["window_start"] == BASE_WS + 3 * 300][0], "0-5")
        # Edge-migration series: the first window's favourite crossed 0.99 at 187 s.
        series = truth.load_edge_series(self.db)
        self.assertEqual(series[0], (BASE_WS, 187.0))
        self.assertEqual([value for _, value in series[1:]], [None, None, None])
        # Append-only: a later build adds the fifth window once the grace
        # period passed, and never rewrites what is there.
        later = truth.build(self.db, cache, BASE_WS, now_ts + truth.LADDER_GRACE_S, [self.sessions], sha="later")
        self.assertEqual((later["rows_inserted"], later["rows_total"], later["pending_ladder_grace"]), (6, 30, 0))
        rows = truth.load_rows(self.db)
        self.assertEqual({row["git_sha"] for row in rows if row["window_start"] < BASE_WS + 4 * 300}, {"test-sha"})
        fifth = [row for row in rows if row["window_start"] == BASE_WS + 4 * 300]
        self.assertEqual({(row["git_sha"], row["host"], row["ladder"]) for row in fifth}, {("later", "public", None)})
        self.assertEqual(truth.build(self.db, cache, BASE_WS, now_ts + truth.LADDER_GRACE_S, [self.sessions], sha="x")["rows_inserted"], 0)
        # A ladder that reaches the session dirs after the grace (a VPS
        # outage, a missed pull) is attached to the row it belongs to,
        # stamped, with labels, prints and the build untouched.
        late = [ladder(BASE_WS + 4 * 300, d, "up", 80.0, [sample(t, 0.95) for t in range(3)]) for d in DECISIONS]
        write_sessions(self.sessions, late + [anchor(BASE_WS + 4 * 300, 240, 80.0, 0.95)], name="session_20260901_040000.jsonl")
        attached = truth.build(self.db, cache, BASE_WS, now_ts + 2 * truth.LADDER_GRACE_S, [self.sessions], sha="x", refresh_prints=False)
        self.assertEqual((attached["rows_inserted"], attached["ladder_rows_attached"], attached["ladder_rows"]), (0, 6, 30))
        fifth = [row for row in truth.load_rows(self.db) if row["window_start"] == BASE_WS + 4 * 300]
        self.assertEqual({(row["git_sha"], row["host"], row["ladder_host"], len(row["ladder"]["samples"])) for row in fifth}, {("later", "vps", "vps", 3)})
        self.assertTrue(all(row["ladder_attached_at"] and row["built_at"] <= row["ladder_attached_at"] for row in fifth))
        self.assertEqual([row for row in fifth if row["decision_s"] == 240][0]["anchor"]["up"]["worst"], 0.95)
        self.assertEqual(truth.build(self.db, cache, BASE_WS, now_ts + 2 * truth.LADDER_GRACE_S, [self.sessions], sha="x", refresh_prints=False)["ladder_rows_attached"], 0)
        self.assertEqual(len(truth.load_edge_series(self.db)), 5)
        # --rebuild-prints rewrites the writer v1 row as v2: --build refreshes
        # the print columns in place (public tape, not evidence); a tick does not.
        (Path(self.directory) / "prints" / ("%d_240.json" % (BASE_WS + 300))).write_text(
            json.dumps(prints_row(BASE_WS + 300, 240, "up", [[2, 0.96]])) + "\n"
        )
        untouched = truth.build(self.db, cache, BASE_WS, now_ts + truth.LADDER_GRACE_S, [self.sessions], sha="x", refresh_prints=False)
        self.assertEqual(untouched["print_rows_refreshed"], 0)
        refreshed = truth.build(self.db, cache, BASE_WS, now_ts + truth.LADDER_GRACE_S, [self.sessions], sha="x")
        self.assertEqual((refreshed["print_rows_refreshed"], refreshed["writer_v2_share"]), (1, 1.0))
        v2 = [row for row in truth.load_rows(self.db) if row["window_start"] == BASE_WS + 300 and row["decision_s"] == 240][0]
        self.assertEqual((v2["print_status"], v2["print_writer_version"], v2["prints"], v2["git_sha"]), ("ok", 2, [[2, 0.96]], "test-sha"))
        self.assertIsNotNone(v2["print_refreshed_at"])
        # Registration needs seven days of ladder rows.
        refused = truth.register(self.db, "2026-09_test", Path(self.directory) / "campaigns", cells=PLANTED_CELLS)
        self.assertFalse(refused["registered"])
        self.assertIn("ladder rows from vps cover 1 days", refused["reason"])

    # --- ladder model ----------------------------------------------------------

    def test_ladder_model_reproduces_quote_clears_band_and_pair_coherence(self):
        samples = [
            sample(0, 0.97, vwap=0.965),  # worst above a 0.96 cap
            sample(1, 0.95, vwap=0.80),  # vwap not above the 0.80 floor (engine: vwap > floor)
            sample(2, 0.95, fresh=False),  # stale book
            sample(3, 0.95, c=0.30),  # pair sum 1.25: band_pair_incoherent
            sample(4, 0.95, vwap=0.94),  # clears: entry is the worst price, not the vwap
            sample(5, 0.96, vwap=0.955),
            sample(6, 0.95, vwap=0.945),
        ]
        samples[4]["q"][2] = None  # the $100 budget has no quote at t=4
        record = ladder(BASE_WS, 210, "up", 90.0, samples)
        rule = {"favorite_price_cap": 0.96, "patience_s": 15}
        trade = truth.ladder_trade(rule, record, "up")
        self.assertEqual((trade["t"], trade["entry"], trade["worst"], trade["vwap"], trade["budget_usd"]), (4, 0.95, 0.95, 0.94, 25.0))
        self.assertAlmostEqual(trade["pair_sum"], 0.94 + 0.06)
        self.assertIsNone(truth.ladder_trade({**rule, "patience_s": 0}, record, "up"))
        self.assertEqual(truth.ladder_trade({**rule, "favorite_price_cap": 0.97}, record, "up")["t"], 0)
        # Boundary: worst == cap clears (<=), vwap == floor does not (>).
        self.assertIsNotNone(truth.sample_clears(sample(0, 0.96, vwap=0.95), "up", 1, 0.80, 0.96))
        self.assertIsNone(truth.sample_clears(sample(0, 0.96, vwap=0.80), "up", 1, 0.80, 0.96))
        # Latency: the order is a FOK limit at the decision sample's worst
        # (0.95).  One second late the book walks $25 only at 0.96: killed,
        # although 0.96 clears the cap.  Two seconds late it walks at 0.95
        # again: filled at the limit, not at the later vwap.
        self.assertIsNone(truth.ladder_trade(rule, record, "up", shift_s=1))
        shifted = truth.ladder_trade(rule, record, "up", shift_s=2)
        self.assertEqual((shifted["entry"], shifted["decided_t"], shifted["filled_t"]), (0.95, 4, 6))
        # Budget: the $100 column skips t=4 and fills at t=5.
        self.assertEqual(truth.ladder_trade(rule, record, "up", budget=100.0)["t"], 5)
        self.assertIsNone(truth.ladder_trade(rule, record, "up", budget=7.0))
        # A record latched on the other side never trades this direction.
        self.assertIsNone(truth.ladder_trade(rule, record, "down"))
        # No-direction records quote both sides; the complement's $5 worst is its ask.
        both = {"t": 0, "fresh": True, "q": {"up": [[0.95, 0.95, 5.0, False]] * 3, "down": [[0.06, 0.06, 80.0, False]] * 3}, "c": None, "age": 0.0}
        undirected = ladder(BASE_WS, 210, None, None, [both])
        self.assertEqual(truth.ladder_trade(rule, undirected, "up")["entry"], 0.95)
        # band_shadow_race.rule_trade scores ladder rows with the same model
        # (the race rule's own ask floor; the live 0.55 admits the t=1 sample).
        raced_rule = {**race.LIVE_RULE, "favorite_price_floor": 0.80, "favorite_price_cap": 0.96, "patience_s": 15}
        raced = race.rule_trade(raced_rule, record, None)
        self.assertEqual((raced["direction"], raced["entry"], raced["vwap"], raced["t"], raced["stake_usd"]), ("up", 0.95, 0.94, 4, 25.0))
        self.assertEqual(race.rule_trade({**race.LIVE_RULE, "favorite_price_cap": 0.96, "patience_s": 15}, record, None)["t"], 1)
        self.assertIsNone(race.rule_trade({**raced_rule, "patience_s": 0}, record, None))
        self.assertIsNone(race.rule_trade({**raced_rule, "margin_floor_usd": 100}, record, None))
        self.assertIsNone(race.rule_trade({**raced_rule, "direction": "down"}, record, None))
        self.assertIsNotNone(race.rule_trade({**race.LIVE_RULE, "favorite_price_cap": 0.92}, anchor(BASE_WS, 240, 80.0, 0.90), None))

    # --- flags -----------------------------------------------------------------

    def selection(self, trades, excluded):
        return {"model": "ladder", "rule": {"decision_second": 210, "margin_floor_usd": 100, "favorite_price_cap": 0.96, "patience_s": 0}, "trades": trades, "excluded": excluded, "signal_windows": len(trades) + sum(len(v) for v in excluded.values()), "coverage": 1.0, "capacity": None}

    def trade(self, index, entry=0.90, direction="up", bucket="150-inf", final=None):
        final = {"150-inf": 200.0, "0-5": 3.0}[bucket] if final is None else final
        return {"window_start": BASE_WS + index * 300, "direction": direction, "entry": entry, "final_margin": final, "final_bucket": bucket, "shifted": {"1": entry, "2": entry}}

    def test_adverse_selection_flag_compares_the_rule_excluded_population(self):
        labels = {}
        trades = []
        for index in range(30):
            trades.append(self.trade(index))
            labels[BASE_WS + index * 300] = "up" if index % 3 else "down"  # 20/30, upper 0.808
        excluded_rows = []
        for index in range(30, 60):
            excluded_rows.append({"window_start": BASE_WS + index * 300, "direction": "up", "final_margin": 200.0, "final_bucket": "150-inf"})
            labels[BASE_WS + index * 300] = "up"  # 30/30, lower 0.884: separated
        flagged = truth.score_selection(self.selection(trades, {"never_cleared": excluded_rows}), labels, FEE, {})
        self.assertTrue(flagged["tripwires"]["adverse_selected"])
        self.assertTrue(flagged["adverse_raw"])
        self.assertEqual((flagged["excluded"]["never_cleared"]["wins"], flagged["excluded_pooled"]["n"]), (30, 30))
        self.assertIn("adverse_selected", flagged["defects"])
        # Windows the DATA cannot score are coverage, not the rule's selection.
        unpooled = truth.score_selection(self.selection(trades, {"no_ladder": excluded_rows}), labels, FEE, {})
        self.assertFalse(unpooled["tripwires"]["adverse_selected"])
        self.assertEqual(unpooled["excluded_pooled"]["n"], 0)
        # The pool is the market's most confident windows and wins more by
        # construction: a raw difference the Wilson intervals do not
        # separate (29/30 vs 24/30) is reported, never a hold.
        for index in range(30):
            labels[BASE_WS + index * 300] = "up" if index % 5 else "down"
        labels[BASE_WS + 59 * 300] = "down"
        overlapping = truth.score_selection(self.selection(trades, {"never_cleared": excluded_rows}), labels, FEE, {})
        self.assertEqual((overlapping["wins"], overlapping["excluded_pooled"]["wins"]), (24, 29))
        self.assertTrue(overlapping["adverse_raw"])
        self.assertFalse(overlapping["tripwires"]["adverse_selected"])
        # An excluded population that wins less is not adverse selection.
        for index in range(30, 60):
            labels[BASE_WS + index * 300] = "up" if index % 2 else "down"
        less = truth.score_selection(self.selection(trades, {"never_cleared": excluded_rows}), labels, FEE, {})
        self.assertFalse(less["tripwires"]["adverse_selected"] or less["adverse_raw"])

    def test_oracle_ceiling_bounds_the_cell(self):
        rows = []
        for index in range(40):
            final = 3.0 if index < 20 else 200.0
            official = "up" if (index >= 20 or index % 5) else "down"  # 4 of 20 small windows disagree
            rows.append({"window_start": BASE_WS + index * 300, "decision_s": 240, "final_margin": final, "final_bucket": truth.final_bucket(final), "official": official})
        rows.append({**rows[0], "decision_s": 210})  # a second decision row never double counts
        noise = truth.oracle_noise(rows)
        self.assertEqual({key: noise["0-5"][key] for key in ("n", "disagree", "noise")}, {"n": 20, "disagree": 4, "noise": 0.2})
        self.assertAlmostEqual(noise["0-5"]["noise_lower"], band.wilson_lower(4, 20))
        self.assertEqual({key: noise["150-inf"][key] for key in ("n", "disagree", "noise")}, {"n": 20, "disagree": 0, "noise": 0.0})
        self.assertAlmostEqual(noise["150-inf"]["noise_lower"], 0.0)
        # The ceiling is built on the cell's OWN windows: the table-wide
        # marginal noise (0.2 in 0-5 here, from windows the rule never
        # selects) is a diagnostic.  Thirty 0-5 trades whose labels agree
        # with sign(final) carry no noise: ceiling 1.0, nothing trips.
        labels = {BASE_WS + index * 300: "up" for index in range(60)}
        clean = truth.score_selection(self.selection([self.trade(index, 0.95, bucket="0-5") for index in range(30)], {}), labels, FEE, noise)
        self.assertAlmostEqual(clean["oracle_ceiling"], 1.0)
        self.assertAlmostEqual(clean["oracle_ceiling_marginal"], 0.8)
        self.assertEqual(clean["oracle_noise_conditional"]["0-5"]["disagree"], 0)
        self.assertFalse(clean["tripwires"]["oracle_ceiling_below_break_even"] or clean["tripwires"]["wr_above_oracle_ceiling"])
        # Twelve of the thirty resolve against sign(final) yet the cell wins
        # them all: the conditional noise's Wilson lower bound (0.246) caps
        # the achievable win rate at 0.754, below break-even at 0.95 and
        # below the cell's own lower bound (0.884): both trip.
        small = truth.score_selection(
            self.selection([self.trade(index, 0.95, bucket="0-5", final=-3.0 if index < 12 else 3.0) for index in range(30)], {}), labels, FEE, noise
        )
        self.assertAlmostEqual(small["oracle_ceiling"], 1.0 - band.wilson_lower(12, 30))
        self.assertTrue(small["tripwires"]["oracle_ceiling_below_break_even"])
        self.assertTrue(small["tripwires"]["wr_above_oracle_ceiling"])
        # The excluded populations of the cell count toward its noise too.
        noisy_pool = [{"window_start": BASE_WS + index * 300, "direction": "up", "final_margin": -3.0, "final_bucket": "0-5"} for index in range(30, 42)]
        pooled = truth.score_selection(self.selection([self.trade(index, 0.95, bucket="0-5") for index in range(30)], {"never_cleared": noisy_pool}), labels, FEE, noise)
        self.assertEqual((pooled["oracle_noise_conditional"]["0-5"]["n"], pooled["oracle_noise_conditional"]["0-5"]["disagree"]), (42, 12))
        self.assertAlmostEqual(pooled["oracle_ceiling"], 1.0 - band.wilson_lower(12, 42))
        large = truth.score_selection(self.selection([self.trade(index, 0.95, bucket="150-inf") for index in range(30)], {}), labels, FEE, noise)
        self.assertAlmostEqual(large["oracle_ceiling"], 1.0)
        self.assertFalse(large["tripwires"]["oracle_ceiling_below_break_even"] or large["tripwires"]["wr_above_oracle_ceiling"])
        self.assertTrue(large["tripwires"]["tick_fragile"] is False)
        self.assertTrue(large["tripwires"]["insufficient_support"])  # 30 trades over 2.4 h
        # First-crossing summary (informational since v3): the 7-day median
        # of the first second above 0.99 against the decision second;
        # censored windows count as later.  Never a tripwire.
        series = [(BASE_WS + index * 300, 200.0 if index % 2 else None) for index in range(20)]
        self.assertFalse(truth.edge_migration_verdict(series, 240)["median_before_decision"])
        self.assertEqual(truth.edge_migration_verdict(series, 240)["censored"], 10)
        self.assertTrue(truth.edge_migration_verdict([(ws, 200.0) for ws, _ in series], 240)["median_before_decision"])
        self.assertFalse(truth.edge_migration_verdict([(ws, 200.0) for ws, _ in series], 180)["median_before_decision"])
        self.assertNotIn("kill", truth.edge_migration_verdict(series, 240))
        self.assertNotIn("edge_migration", large["tripwires"])
        # A cell reads the series of its own signal windows only: points
        # from windows it never selects (here every odd index) are not its
        # edge, so the median over its 30 windows is censored.
        own = truth.score_selection(self.selection([self.trade(index, 0.95) for index in range(30)], {}), labels, FEE, noise, [(ws, 200.0) for ws, _ in series])
        self.assertEqual((own["edge_migration"]["n"], own["edge_migration"]["median_before_decision"]), (20, True))
        self.assertEqual((own["tripwires"]["capacity_collapse"], own["killed"], own["capacity_trend"]["verdict"]), (False, False, None))
        odd = [(BASE_WS + index * 300, 200.0) for index in range(1, 80, 2)]
        outside = truth.score_selection(self.selection([self.trade(index, 0.95) for index in range(0, 60, 2)], {}), labels, FEE, noise, odd)
        self.assertEqual((outside["edge_migration"]["n"], outside["edge_migration"]["median_before_decision"]), (0, False))

    def test_capacity_trend_kills_a_collapse_not_a_flat_or_short_series(self):
        def points(daily, per_day=20):
            # `daily` capacities per UTC day from BASE_WS: per_day covered windows, the
            # fillable share as given (a None day has no covered window at all).
            out = []
            for day, capacity in enumerate(daily):
                if capacity is None:
                    continue
                fillable = round(capacity * per_day)
                out += [(BASE_WS + day * DAY_S + index * 300, index < fillable) for index in range(per_day)]
            return out

        def counts(daily):
            # (fills, n) per UTC day from BASE_WS; a None day has no covered window.
            out = []
            for day, entry in enumerate(daily):
                if entry is None:
                    continue
                fills, n = entry
                out += [(BASE_WS + day * DAY_S + index * 300, index < fills) for index in range(n)]
            return out

        flat = truth.capacity_trend_verdict(points([0.3] * 21))
        self.assertEqual((flat["n_days"], flat["verdict"], flat["kill"], flat["warn"]), (21, "ok", False, False))
        self.assertAlmostEqual(flat["baseline"]["capacity"], 0.3)
        self.assertAlmostEqual(flat["trailing"]["capacity"], 0.3)
        self.assertAlmostEqual(flat["ratio"], 1.0)
        self.assertGreater(flat["ratio_upper"], 1.0)  # trailing upper / baseline lower
        self.assertEqual((flat["baseline"]["days"], flat["trailing"]["days"], len(flat["weekly"]), flat["consecutive_declines"]), (7, 7, 3, 0))
        self.assertEqual((flat["baseline"]["n"], flat["baseline"]["fills"], flat["support"]), (140, 42, {"ok": True, "reason": None}))
        self.assertEqual([entry["n"] for entry in flat["daily"]][:2], [20, 20])
        # Decay: a registration week at 0.30 and a trailing week at 0.05 (a
        # sixth) kills on the bounds: the trailing Wilson upper bound sits
        # below half the baseline's lower bound.
        decay = truth.capacity_trend_verdict(points([0.3] * 7 + [0.2] * 7 + [0.05] * 7))
        self.assertEqual((decay["verdict"], decay["kill"], decay["warn"]), ("kill", True, False))
        self.assertAlmostEqual(decay["ratio"], 1 / 6)
        self.assertLess(decay["ratio_upper"], truth.CAPACITY_KILL_RATIO)
        # The kill needs support: a third at 20 windows a day (140 a pool) is
        # not separated on the bounds (point ratio 1/3 reported, no kill);
        # the same third at 100 a day is.
        third = truth.capacity_trend_verdict(points([0.3] * 7 + [0.1] * 7))
        self.assertEqual((third["verdict"], third["kill"]), ("ok", False))
        self.assertAlmostEqual(third["ratio"], 1 / 3)
        self.assertGreater(third["ratio_upper"], truth.CAPACITY_KILL_RATIO)
        self.assertEqual(truth.capacity_trend_verdict(points([0.3] * 7 + [0.1] * 7, per_day=100))["verdict"], "kill")
        # A dip that stays above half the registration week is not a kill.
        self.assertEqual(truth.capacity_trend_verdict(points([0.3] * 7 + [0.2] * 14))["verdict"], "ok")
        # Too short: 13 days of data give no verdict, however steep.
        short = truth.capacity_trend_verdict(points([0.3] * 6 + [0.0] * 7))
        self.assertEqual((short["n_days"], short["verdict"], short["kill"], short["warn"], short["ratio"]), (13, None, False, False, None))
        self.assertEqual(truth.capacity_trend_verdict([])["verdict"], None)
        # Days without a covered window carry no capacity: an outage in the
        # trailing week neither kills nor rescues (13 distinct days: no verdict;
        # 14 with the outage day skipped: the pooled trailing week still reads 0.3).
        outage = points([0.3] * 7 + [None] * 7 + [0.3] * 7)
        self.assertEqual(truth.capacity_trend_verdict(outage)["verdict"], "ok")
        self.assertEqual(truth.capacity_trend_verdict(outage)["span_days"], 21)
        # Sparse pools give no verdict, and say why.  A flat 2% cell at 8
        # covered windows a day has one or two fills a week: the old point
        # ratio killed it on a fill-less trailing week (0.988^56 = 0.51 per
        # look) in a perfectly flat regime.
        sparse = truth.capacity_trend_verdict(counts([(0, 8)] * 3 + [(1, 8)] + [(0, 8)] * 10 + [(1, 8)] + [(0, 8)] * 6))
        self.assertEqual((sparse["n_days"], sparse["verdict"], sparse["kill"], sparse["ratio"], sparse["ratio_upper"]), (21, None, False, None, None))
        self.assertEqual(sparse["support"], {"ok": False, "reason": "baseline: 7 days, 56 windows, 1 fills"})
        # Ten baseline fills support a verdict, but 3/56 against 10/56 (point
        # ratio 0.3) is not separated: no kill.
        thin = truth.capacity_trend_verdict(counts([(2, 8)] * 5 + [(0, 8)] * 2 + [(0, 8)] * 7 + [(1, 8)] * 3 + [(0, 8)] * 4))
        self.assertEqual((thin["verdict"], thin["support"]["ok"]), ("ok", True))
        self.assertAlmostEqual(thin["ratio"], 0.3)
        # A registration week that is one sparse day (2 windows, both
        # fillable) ahead of a six-day outage, then 13 days flat at 0.45: 14
        # distinct days, but the baseline pool is one day (the old rule read
        # 0.45 / 1.0 and killed).  The mirror case, a trailing week that is
        # one hour of two depth-limited windows after an outage, likewise.
        outage_first = truth.capacity_trend_verdict(counts([(2, 2)] + [None] * 6 + [(9, 20)] * 13))
        self.assertEqual((outage_first["n_days"], outage_first["verdict"], outage_first["kill"]), (14, None, False))
        self.assertEqual(outage_first["support"]["reason"], "baseline: 1 days, 2 windows, 2 fills")
        outage_last = truth.capacity_trend_verdict(counts([(6, 20)] * 14 + [None] * 6 + [(0, 2)]))
        self.assertEqual((outage_last["n_days"], outage_last["verdict"], outage_last["kill"]), (15, None, False))
        self.assertEqual(outage_last["support"]["reason"], "trailing: 1 days, 2 windows, 0 fills")
        # Warn, never a kill: three complete weeks of decline running (four
        # weeks of data), the last still above half the first.
        declining = truth.capacity_trend_verdict(points([0.40] * 7 + [0.35] * 7 + [0.30] * 7 + [0.25] * 7))
        self.assertEqual((declining["verdict"], declining["kill"], declining["warn"], declining["consecutive_declines"]), ("warn", False, True, 3))
        self.assertEqual([round(week["capacity"], 2) for week in declining["weekly"]], [0.4, 0.35, 0.3, 0.25])
        recovered = truth.capacity_trend_verdict(points([0.40] * 7 + [0.35] * 7 + [0.30] * 7 + [0.40] * 7))
        self.assertEqual((recovered["verdict"], recovered["consecutive_declines"]), ("ok", 0))
        # A partial fifth week is not a complete week: no fourth decline yet.
        partial = truth.capacity_trend_verdict(points([0.40] * 7 + [0.35] * 7 + [0.30] * 7 + [0.25] * 7 + [0.0] * 3))
        self.assertEqual((len(partial["weekly"]), partial["consecutive_declines"]), (4, 3))
        # An empty complete week stays in the series without a capacity and
        # breaks the run (weeks 1 and 3 are not adjacent); a dip under 5% of
        # the week before (one window's worth) is not a decline.
        gapped = truth.capacity_trend_verdict(points([0.8] * 7 + [0.7] * 7 + [None] * 7 + [0.6] * 7 + [0.5] * 7))
        self.assertEqual([None if week["capacity"] is None else round(week["capacity"], 2) for week in gapped["weekly"]], [0.8, 0.7, None, 0.6, 0.5])
        self.assertEqual((gapped["weekly"][2]["n"], gapped["consecutive_declines"], gapped["warn"], gapped["verdict"]), (0, 1, False, "ok"))
        dips = truth.capacity_trend_verdict(counts([(31, 100)] * 7 + [(30, 100)] * 7 + [(29, 100)] * 7 + [(28, 100)] * 7))
        self.assertEqual(([round(week["capacity"], 2) for week in dips["weekly"]], dips["consecutive_declines"], dips["verdict"]), ([0.31, 0.3, 0.29, 0.28], 0, "ok"))
        # Through the score: the ladder selection carries the points, a kill
        # trips capacity_collapse (a kill tripwire, never cleared), a warning
        # is reported beside the tripwires and blocks nothing.
        labels = {BASE_WS + index * 300: "up" for index in range(200)}
        trades = [self.trade(index, 0.95) for index in range(200)]
        killed = truth.score_selection({**self.selection(trades, {}), "capacity_points": points([0.3] * 7 + [0.1] * 7, per_day=100)}, labels, FEE, {})
        self.assertEqual((killed["tripwires"]["capacity_collapse"], killed["killed"], killed["promotable"], killed["warnings"]), (True, True, False, {"capacity_declining": False}))
        warned = truth.score_selection({**self.selection(trades, {}), "capacity_points": points([0.40] * 7 + [0.35] * 7 + [0.30] * 7 + [0.25] * 7)}, labels, FEE, {})
        self.assertEqual((warned["tripwires"]["capacity_collapse"], warned["killed"], warned["warnings"]), (False, False, {"capacity_declining": True}))
        self.assertFalse(warned["held"])
        self.assertIn("warn:capacity_declining", truth._flags(warned))
        self.assertEqual(truth.KILL_TRIPWIRES, ("capacity_collapse",))

    def test_edge_migration_reads_a_pinned_favourite_as_above_threshold(self):
        # The collector writes a null quote when the favourite has no
        # executable ask; with the complement's best ask at 0.01 that is a
        # bid at 0.99 or above, the strong-window shape at 240 s.
        pinned = {"t": 3, "q": [None, None, None], "c": 0.01, "age": 0.0, "fresh": True}
        quoted = {"t": 5, "q": [[0.995, 0.995, 25.0, False]] * 3, "c": 0.005, "age": 0.0, "fresh": True}
        cheap = {"t": 1, "q": [None, None, None], "c": 0.05, "age": 0.0, "fresh": True}
        records = {240: ladder(BASE_WS, 240, "up", 200.0, [cheap, pinned, quoted])}
        self.assertEqual(truth.favourite_first_second_above(records), (243.0, 245.0))
        self.assertEqual(truth.favourite_first_second_above({240: ladder(BASE_WS, 240, "up", 200.0, [cheap, quoted])}), (245.0, 245.0))
        self.assertEqual(truth.favourite_first_second_above({240: ladder(BASE_WS, 240, "up", 200.0, [cheap])}), (None, 241.0))
        both = {"t": 2, "q": {"up": [None, None, None], "down": [[0.01, 0.01, 500.0, False]] * 3}, "c": None, "age": 0.0, "fresh": True}
        self.assertEqual(truth.favourite_first_second_above({210: ladder(BASE_WS, 210, None, None, [both])}), (212.0, 212.0))

    def test_ladder_model_gates_on_the_engine_basis(self):
        # The row's margin is the kline close of the decision second (the
        # last trade of [ws + d, ws + d + 1)); the ladder record's is the
        # tick the engine latched at t = 0.  Real Mac rows: 40.0 vs 119.53
        # (the engine trades, the kline floor excludes), 78.8 vs 62.68 (the
        # engine skips), +57.33 vs -129.13 (a sign flip).
        def row(index, kline, engine, basis="binance", with_ladder=True):
            ws = BASE_WS + index * 300
            record = None
            if with_ladder:
                record = ladder(ws, 210, "up" if (engine or 0) > 0 else "down", engine, [sample(0, 0.95)], basis=basis)
                if basis == "composite":
                    record["direction"], record["margin"] = "up", None
            return {
                "window_start": ws, "decision_s": 210, "margin": kline, "direction": "up" if kline > 0 else "down",
                "official": "up", "final_margin": 150.0, "final_bucket": "150-inf", "ladder": record, "ladder_host": "vps",
                "print_writer_version": 2, "print_status": "ok", "prints": [[1, 0.95]], "coverage": {"complete": True},
            }

        rows = [row(0, 40.0, 119.53), row(1, 78.8, 62.68), row(2, 57.33, -129.13), row(3, 90.0, None, basis="composite"), row(4, 90.0, 90.0, with_ladder=False)]
        rule = truth.rule_from_cell_id("d210_f75_c0.96_p0")
        selection = truth.select_cell(rows, rule, "ladder")
        traded = {trade["window_start"]: (trade["direction"], trade["margin"]) for trade in selection["trades"]}
        self.assertEqual(traded, {BASE_WS: ("up", 119.53), BASE_WS + 600: ("down", -129.13), BASE_WS + 900: ("up", 90.0)})
        self.assertEqual({reason: len(rows) for reason, rows in selection["excluded"].items()}, {"no_ladder": 1})
        self.assertEqual(selection["basis_disagreement"], {"compared": 3, "sign_flips": 1, "floor_changes": 3})
        self.assertEqual(selection["signal_windows"], 4)
        # The print and signal models keep the kline basis.
        printed = truth.select_cell(rows, rule, "print")
        self.assertEqual(sorted(trade["window_start"] for trade in printed["trades"]), [BASE_WS + 300, BASE_WS + 900, BASE_WS + 1200])
        self.assertEqual([trade["margin"] for trade in truth.select_cell(rows, rule, "signal")["trades"]], [78.8, 90.0, 90.0])
        # Scoring: the sign-flipped window is a down trade against an up label.
        score = truth.score_selection(selection, truth.labels_of(rows), FEE, {})
        self.assertEqual((score["n"], score["wins"], score["basis_disagreement"]["sign_flips"]), (3, 2, 1))

    def test_coverage_counts_only_scoreable_windows(self):
        def row(index, prints=None, coverage=None, status="ok", ladder_samples=None):
            ws = BASE_WS + index * 300
            record = None if ladder_samples is None else ladder(ws, 240, "up", 100.0, ladder_samples)
            return {
                "window_start": ws, "decision_s": 240, "margin": 100.0, "direction": "up", "official": "up",
                "final_margin": 150.0, "final_bucket": "150-inf", "print_writer_version": 2, "print_status": status,
                "prints": [[1, 0.95]] if prints is None else prints, "coverage": coverage or {"complete": True}, "ladder": record, "ladder_host": "vps",
            }

        rule = truth.rule_from_cell_id("d240_f100_c0.96_p0")
        # Print model: the tape stopped before the decision second on two
        # of ten windows (an availability exclusion), one row is not v2.
        rows = [row(index) for index in range(7)] + [row(7, coverage={"oldest_offset_s": 250, "complete": False}), row(8, coverage={"oldest_offset_s": 260, "complete": False}), row(9, status="writer_v1")]
        printed = truth.select_cell(rows, rule, "print")
        self.assertEqual((len(printed["trades"]), {reason: len(items) for reason, items in printed["excluded"].items()}), (7, {"uncovered": 2, "no_print_row": 1}))
        self.assertAlmostEqual(printed["coverage"], 0.7)
        self.assertTrue(truth.score_selection(printed, truth.labels_of(rows), FEE, {})["tripwires"]["coverage_low"])
        # Ladder model: a record whose first sample lies beyond the patience
        # (the observer started mid-window) never had a look: not covered,
        # not in the adverse-selection pool.
        rows = [row(index, ladder_samples=[sample(0, 0.95)]) for index in range(8)] + [row(8, ladder_samples=[sample(27, 0.95)]), row(9, ladder_samples=[sample(3, 0.95), sample(4, 0.95)])]
        laddered = truth.select_cell(rows, rule, "ladder")
        self.assertEqual((len(laddered["trades"]), {reason: len(items) for reason, items in laddered["excluded"].items()}), (8, {"ladder_uncovered": 2}))
        self.assertAlmostEqual(laddered["coverage"], 0.8)
        scored = truth.score_selection(laddered, truth.labels_of(rows), FEE, {})
        self.assertEqual((scored["excluded_pooled"]["n"], scored["tripwires"]["coverage_low"]), (0, True))
        patient = truth.select_cell(rows, truth.rule_from_cell_id("d240_f100_c0.96_p15"), "ladder")
        self.assertEqual((len(patient["trades"]), {reason: len(items) for reason, items in patient["excluded"].items()}), (9, {"ladder_uncovered": 1}))
        # A discovery host's ladder is availability, not evidence.
        for item in rows[:3]:
            item["ladder_host"] = "mac"
        vps_only = truth.select_cell(rows, truth.rule_from_cell_id("d240_f100_c0.96_p15"), "ladder", ("vps",))
        self.assertEqual({reason: len(items) for reason, items in vps_only["excluded"].items()}, {"ladder_discovery_host": 3, "ladder_uncovered": 1})
        self.assertAlmostEqual(vps_only["coverage"], 0.6)

    def test_ladder_records_are_placed_by_identity_then_on_time_flush(self):
        ws = BASE_WS + 12 * 300
        on_time = ladder(ws, 210, "up", 90.0, [sample(0, 0.95)])
        gone_late = ladder(ws, 240, "up", 90.0, [sample(0, 0.95)], flush_offset=300 - 240 + 180)  # flushed 180 s after the window end
        stalled = ladder(ws, 150, "up", 90.0, [sample(0, 0.95)], flush_offset=300 - 150 + 4000)  # 4,000 s after
        self.assertEqual(truth.ladder_window_start(on_time), ws)
        self.assertIsNone(truth.ladder_window_start(gone_late))
        self.assertIsNone(truth.ladder_window_start(stalled))
        self.assertEqual(truth.ladder_window_start(gone_late, {gone_late["cid"]: ws}), ws)
        # A flush a minute after the window end misfiles anchor 150 by the old rounding; it is dropped now.
        self.assertIsNone(truth.ladder_window_start(ladder(ws, 150, "up", 90.0, [], flush_offset=300 - 150 + 60)))
        self.assertEqual(truth.ladder_window_start(ladder(ws, 240, "up", 90.0, [], flush_offset=31.2 + 2.0)), ws)
        # In a session the band_anchor of the same cid places both late records; a late orphan is counted, not misfiled.
        orphan = ladder(ws + 300, 240, "up", 90.0, [sample(0, 0.95)], flush_offset=300 - 240 + 180)
        write_sessions(self.sessions, [anchor(ws, 150, 90.0, 0.95), anchor(ws, 240, 90.0, 0.95), on_time, gone_late, stalled, orphan])
        scanned = truth.scan_sessions([self.sessions])
        self.assertEqual(sorted(scanned["ladders"]), [(ws, 150), (ws, 210), (ws, 240)])
        self.assertEqual(scanned["ladders_unplaced"], 1)
        # The VPS record wins over a fuller Mac record for the same key; both are kept for the overlap check.
        mac = ladder(ws, 210, "up", 90.0, [sample(0, 0.90), sample(1, 0.90)], host="mac")
        write_sessions(self.sessions, [mac], name="session_20260901_010000.jsonl")
        scanned = truth.scan_sessions([self.sessions])
        self.assertEqual((scanned["ladders"][(ws, 210)]["host"], scanned["ladders"][(ws, 210)]["samples"][0]["q"][1][0]), ("vps", 0.95))
        self.assertEqual(sorted(scanned["ladders_by_host"][(ws, 210)]), ["mac", "vps"])

    def test_mac_ladder_rows_are_discovery_until_the_overlap_is_accepted(self):
        specs = population(300)
        write_cache(self.directory, specs)
        write_sessions(self.sessions, [{**record, "host": "mac"} for record in population_records(specs)])
        now_ts = specs[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        summary = truth.build(self.db, cache_for(self.directory), BASE_WS, now_ts, [self.sessions], sha="mac")
        self.assertEqual((summary["ladder_rows"], summary["ladder_rows_by_host"]), (1800, {"mac": 1800}))
        self.assertEqual(truth.ladder_days(self.db), {"days": 0, "span_days": 0, "hosts": ["vps"]})
        campaigns = Path(self.directory) / "campaigns"
        refused = truth.register(self.db, "2026-09_mac", campaigns, now_ts, cells=PLANTED_CELLS)
        self.assertFalse(refused["registered"])
        self.assertIn("ladder rows from vps cover 0 days", refused["reason"])
        # Discovery still reads them; the grid says which hosts are evidence.
        report = truth.grid(self.db, fee_rate=FEE)
        self.assertEqual((report["evidence_hosts"], report["ladder_rows_by_host"]), (["vps"], {"mac": 1800}))
        self.assertEqual({cell["cell_id"]: cell for cell in report["cells"]}["d210_f100_c0.96_p15"]["ladder"]["n"], 120)
        # The overlap check needs both hosts over three days agreeing within a tick.
        self.assertFalse(truth.accept_mac_ladders(self.db, [self.sessions], now_ts)["accepted"])
        vps_dir = Path(self.directory) / "mirror"
        agreeing = [record for record in population_records(specs[:40]) if record["anchor_s"] == 210]
        write_sessions(vps_dir, agreeing)
        check = truth.accept_mac_ladders(self.db, [self.sessions, vps_dir], now_ts)
        self.assertEqual((check["accepted"], check["overlap"]["shared_keys"], check["overlap"]["agreement"]), (True, 40, 1.0))
        self.assertGreaterEqual(check["overlap"]["days"], 3)
        self.assertEqual(truth.evidence_hosts(self.db), ("vps", "mac"))
        self.assertEqual(truth.ladder_days(self.db)["days"], 16)
        # The acceptance widens discovery only: a family is registered on
        # VPS rows, and sixteen days of accepted Mac rows are still none.
        refused = truth.register(self.db, "2026-09_mac", campaigns, now_ts, cells=PLANTED_CELLS)
        self.assertEqual((refused["registered"], refused["ladder_rows_by_host"]), (False, {"mac": 1800}))
        self.assertIn("ladder rows from vps cover 0 days", refused["reason"])
        # Disagreement beyond a tick fails the check (a fresh table).
        other = truth.open_db(Path(self.directory) / "other.sqlite3")
        self.addCleanup(other.close)
        off_dir = Path(self.directory) / "mirror_off"
        write_sessions(off_dir, [{**record, "samples": [sample(0, 0.60)]} for record in agreeing])
        failed = truth.accept_mac_ladders(other, [self.sessions, off_dir], now_ts)
        self.assertEqual((failed["accepted"], failed["overlap"]["agreement"]), (False, 0.0))
        self.assertEqual(truth.evidence_hosts(other), ("vps",))
        # Accrual: a campaign's hosts are fixed at registration (the VPS): Mac-only fresh windows never fold, accepted or not.
        base = population(300, start_index=1000)
        write_cache(self.directory, base)
        write_sessions(vps_dir, population_records(base), name="session_20260903_000000.jsonl")
        later = base[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        truth.build(other, cache_for(self.directory), BASE_WS, later, [vps_dir], sha="vps")
        registered = truth.register(other, "2026-09_vps", campaigns, later, cells=PLANTED_CELLS)
        self.assertTrue(registered["registered"], registered)
        fresh = population(60, start_index=1300)
        write_cache(self.directory, fresh)
        write_sessions(self.sessions, [{**record, "host": "mac"} for record in population_records(fresh)], name="session_20260904_000000.jsonl")
        newest = fresh[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        ticked = truth.tick(other, cache_for(self.directory), BASE_WS, newest, loop_config(self.directory), [vps_dir, self.sessions], campaigns)
        planted = [cell for cell in ticked["campaigns"][0]["cells"] if cell["cell_id"].startswith("d210_f100_")][0]
        self.assertEqual((planted["n"], planted["excluded"].get("ladder_discovery_host")), (0, 24))
        self.assertEqual(ticked["campaigns"][0]["ladder_hosts"], ["vps"])
        family = json.loads((campaigns / "2026-09_vps.json").read_text())
        self.assertEqual(len(truth.gate_artifact(other, family, planted["cell_id"])["rows"]), 0)
        truth.set_meta(other, truth.MAC_ACCEPTED_META, {"accepted_at": "test"})
        self.assertEqual(truth.evidence_hosts(other), ("vps", "mac"))
        ticked = truth.tick(other, cache_for(self.directory), BASE_WS, newest, loop_config(self.directory), [vps_dir, self.sessions], campaigns)
        planted = [cell for cell in ticked["campaigns"][0]["cells"] if cell["cell_id"].startswith("d210_f100_")][0]
        self.assertEqual((planted["n"], planted["applied"], planted["excluded"].get("ladder_discovery_host")), (0, 0, 24))
        self.assertEqual(ticked["campaigns"][0]["ladder_hosts"], ["vps"])

    # --- grid, planted edge, permutations -------------------------------------

    def planted_fixture(self, count=300, plant=True):
        specs = population(count, plant=plant)
        write_cache(self.directory, specs)
        write_sessions(self.sessions, population_records(specs))
        now_ts = specs[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        summary = truth.build(self.db, cache_for(self.directory), BASE_WS, now_ts, [self.sessions], sha="planted")
        self.assertEqual(summary["rows_inserted"], len(DECISIONS) * count)
        return specs, now_ts

    def test_grid_finds_the_planted_edge(self):
        specs, now_ts = self.planted_fixture()
        report = truth.grid(self.db, fee_rate=FEE)
        self.assertEqual(len(report["cells"]), 504)
        self.assertEqual((report["evidence_hosts"], report["ladder_rows_by_host"], report["null_check"]), (["vps"], {"vps": 1800}, None))
        self.assertEqual(sum(1 for cell in report["cells"] if cell["registrable"]), 315)
        by_id = {cell["cell_id"]: cell for cell in report["cells"]}
        # The finer anchors are ladder-only cells: no print column, scored
        # by the ladder (and the signal ceiling) alone, marked as such.
        self.assertEqual({cell["rule"]["decision_second"] for cell in report["cells"] if cell["ladder_only"]}, {150, 195, 225})
        finer = by_id["d195_f75_c0.99_p15"]
        self.assertEqual((finer["ladder_only"], finer["registrable"], finer["print"]["n"], finer["print"]["excluded"]["no_print_row"]["n"]), (True, True, 0, 60))
        self.assertEqual((finer["ladder"]["n"], finer["signal"]["n"]), (60, 60))
        self.assertIn("ladder-only", truth.grid_text(report, top=504))
        planted = by_id["d210_f100_c0.96_p15"]
        self.assertFalse(planted["ladder_only"])
        self.assertEqual((planted["ladder"]["capacity_trend"]["verdict"], planted["ladder"]["capacity_trend"]["n_days"]), ("ok", 16))
        self.assertEqual(planted["ladder"]["warnings"], {"capacity_declining": False})
        expected_n = sum(1 for spec in specs if spec["kind"] == "planted")
        for model in ("ladder", "print"):
            score = planted[model]
            self.assertEqual(score["n"], expected_n)
            self.assertGreaterEqual(score["n"], 100)
            self.assertAlmostEqual(score["win_rate"], score["mean_break_even"] + 0.10, delta=0.02)
            self.assertTrue(score["clears_break_even"])
            self.assertEqual(score["tripwires"], {name: False for name in score["tripwires"]}, score["tripwires"])
            self.assertTrue(score["promotable"])
        self.assertEqual(planted["ladder"]["capacity"], {"25": 1.0, "100": 1.0})
        self.assertEqual((planted["ladder"]["latency"]["1"]["n"], planted["ladder"]["latency"]["1"]["fill_rate"]), (0, 0.0))  # one sample per ladder: a late order misses
        self.assertEqual(planted["ladder"]["basis_disagreement"], {"compared": 300, "sign_flips": 0, "floor_changes": 0})
        self.assertEqual(planted["signal"]["n"], expected_n)
        # The strongest ladder cell is a d210_f100 cell (its 21 cap/patience
        # variants share the window set); the null cells do not clear.
        ranked = sorted(report["cells"], key=lambda cell: -((cell["ladder"]["wilson_lower"] or 0.0) - (cell["ladder"]["mean_break_even"] or 1.0)))
        self.assertTrue(ranked[0]["cell_id"].startswith("d210_f100_"))
        nulls = [cell for cell in report["cells"] if cell["rule"]["decision_second"] != 210 and cell["rule"]["margin_floor_usd"] == 75]
        self.assertEqual(sum(1 for cell in nulls if cell["ladder"]["clears_break_even"]), 0)
        # Registration: the family is the explicit list, N its length; cells
        # that would trade the same windows are not folded into one.
        campaigns = Path(self.directory) / "campaigns"
        family = PLANTED_CELLS + ["d210_f100_c0.99_p30", "d150_f100_c0.99_p0"]
        registered = truth.register(self.db, "2026-09_planted", campaigns, now_ts, cells=family)
        self.assertEqual((registered["registered"], registered["family_size"], registered["cells"], registered["spec_sha256"]), (True, 4, family, None))
        campaign = json.loads((campaigns / "2026-09_planted.json").read_text())
        self.assertEqual((campaign["schema_version"], campaign["family_size"], [cell["cell_id"] for cell in campaign["cells"]]), (2, 4, family))
        self.assertEqual([cell["at_registration"]["n"] for cell in campaign["cells"]], [180, 120, 120, 0])
        self.assertEqual({cell["grammar"] for cell in campaign["cells"]}, {truth.GRAMMAR_VERSION})
        self.assertEqual([cell["fingerprint"] for cell in campaign["cells"]], [truth.fingerprint(cell["rule"]) for cell in campaign["cells"]])
        # d150 sits below the grid's registrable floor: an explicit list may name it (V6 of family v4).
        self.assertEqual((campaign["cells"][3]["name"], truth.registrable(campaign["cells"][3]["rule"])), ("V6", False))
        self.assertEqual((campaign["registered_after_window_start"], campaign["registered_at_ts"], campaign["ladder_hosts"]), (specs[-1]["ws"], now_ts, ["vps"]))
        self.assertEqual((campaign["audit_cleared"], campaign["spec"]), ({}, None))
        self.assertEqual((campaign["evaluator_version"], campaign["event_grammar_version"]), ("executable_truth_v4", "band_event_v4"))
        # Static cells only: no capacity, net or falsifier entry applies; futility and day 90 always do.
        self.assertEqual([rule["id"] for rule in campaign["stopping_rules"]], ["futility", "day90_no_discovery"])
        self.assertEqual((campaign["falsifiers"], campaign["paired_controls"]["pairs"]), ([], []))
        self.assertFalse(truth.register(self.db, "2026-09_planted", campaigns, now_ts, cells=family)["registered"])

    def test_register_refuses_anything_but_an_explicit_known_family(self):
        specs, now_ts = self.planted_fixture()
        campaigns = Path(self.directory) / "campaigns"
        # The former path (no list: every grid cell clearing a screen) is refused.
        refused = truth.register(self.db, "2026-10_auto", campaigns, now_ts)
        self.assertFalse(refused["registered"])
        self.assertIn("explicit family", refused["reason"])
        self.assertFalse(truth.register(self.db, "2026-10_auto", campaigns, now_ts, cells=[])["registered"])
        # Unknown ids: malformed, off either grid, an alias that does not exist.
        refused = truth.register(self.db, "2026-10_unknown", campaigns, now_ts, cells=["d210_f100_c0.92_p0", "d200_f100_c0.92_p0", "e150-270_z2.7_k98", "e150-270_z2.5_c0.97", "V7", "x150-270_m100_c0.99", "junk"])
        self.assertEqual(refused, {"registered": False, "reason": "unknown cell id(s): d200_f100_c0.92_p0, e150-270_z2.7_k98, e150-270_z2.5_c0.97, V7, x150-270_m100_c0.99, junk"})
        refused = truth.register(self.db, "2026-10_twice", campaigns, now_ts, cells=["V6", "d150_f100_c0.99_p0"])
        self.assertEqual(refused["reason"], "cell id(s) listed twice: d150_f100_c0.99_p0")
        # An event cell freezes the settlement spec: no closes, no family.
        refused = truth.register(self.db, "2026-10_event", campaigns, now_ts, cells=["V1", "V6"])
        self.assertIn("needs the Binance closes", refused["reason"])
        self.assertFalse(campaigns.exists())
        # The CLI: --register without --cells exits 2 and writes nothing.
        config_path = Path(self.directory) / "loop.json"
        config_path.write_text(json.dumps(loop_config(self.directory)))
        argv = ["--campaigns-dir", str(campaigns), "--db", str(Path(self.directory) / "windows.sqlite3"), "--loop-config", str(config_path), "--now-ts", str(now_ts)]
        with contextlib.redirect_stdout(io.StringIO()) as printed:
            self.assertEqual(truth.main(["--register", "2026-10_cli"] + argv), 2)
        self.assertIn("explicit family", printed.getvalue())
        self.assertFalse(campaigns.exists())
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            truth.main(["--register", "2026-10_cli", "--allow-partial-prints"] + argv)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(truth.main(["--register", "2026-10_cli", "--cells", ",".join(PLANTED_CELLS)] + argv), 0)
        self.assertEqual(json.loads((campaigns / "2026-10_cli.json").read_text())["family_size"], 2)
        # Ids of both grammars resolve; V1..V6 is the family of section 4.
        self.assertEqual([identity for identity, _ in truth.resolve_cell_ids(["V1..V6"])], list(truth.FAMILY_V4.values()))
        self.assertEqual(truth.resolve_cell_ids(["V2"])[0][1], {"trigger": "z", "t0": 150, "t1": 270, "threshold": 3.0, "cap_rule": "c0.98"})
        self.assertEqual(len(truth.event_grid_rules()), 105)
        self.assertEqual({truth.event_cell_id(rule) for rule in truth.event_grid_rules()} >= set(list(truth.FAMILY_V4.values())[:5]), True)
        self.assertEqual(truth.control_rule_from_id("x210-270_m100_c0.99"), {"trigger": "usd", "t0": 210, "t1": 270, "threshold": 100.0, "cap_rule": "c0.99"})
        self.assertEqual(truth.event_cell_id(truth.control_rule_from_id("x150-270_s3.0_c0.99")), "x150-270_s3.0_c0.99")

    def test_break_even_null_bounds_the_false_clear_rate(self):
        self.planted_fixture(count=240, plant=False)
        rows = truth.load_rows(self.db, 210)
        labels = truth.labels_of(rows)
        selections = [truth.select_cell(rows, rule, "ladder") for rule in truth.grid_rules() if rule["decision_second"] == 210]
        self.assertTrue(all(selection["trades"] for selection in selections if selection["rule"]["margin_floor_usd"] <= 90))
        # Under the null every cell's win probability is its break-even at
        # its own entries: the per-cell false-clear rate of the Wilson gate
        # is a few percent (never zero: the check is not vacuous) and the
        # family rate over the 42 populated cells is reported.
        check = truth.null_check(selections, FEE, replicates=200, seed=11)
        self.assertEqual((check["replicates"], check["cells"]), (200, 42))
        self.assertLess(0.0, check["promote_share"])
        self.assertLessEqual(check["promote_share"], 0.05, check)
        self.assertLessEqual(check["promote_share"], check["family_false_clear_share"])
        self.assertLessEqual(check["family_false_clear_share"], 1.0)
        # The unpermuted null clears nowhere either.
        noise = truth.oracle_noise(rows)
        clears = [truth.score_selection(selection, labels, FEE, noise)["clears_break_even"] for selection in selections]
        self.assertLessEqual(sum(clears) / len(clears), 0.05)
        # --grid carries the same check in its JSON (every decision: 6 x 42 populated cells).
        report = truth.grid(self.db, fee_rate=FEE, null_replicates=5, seed=3)
        self.assertEqual((report["null_check"]["replicates"], report["null_check"]["cells"]), (5, 252))
        self.assertIn("break-even null (5 replicates, 252 ladder cells with trades)", truth.grid_text(report))

    # --- accrual, ledger, gate artifact ----------------------------------------

    def test_tick_accrues_registered_cells_with_one_ledger_row_per_look(self):
        specs, now_ts = self.planted_fixture()
        campaigns = Path(self.directory) / "campaigns"
        self.assertTrue(truth.register(self.db, "2026-09_planted", campaigns, now_ts, cells=PLANTED_CELLS)["registered"])
        config = loop_config(self.directory)
        # Nothing after registration yet: no accrual, no ledger row.
        idle = truth.tick(self.db, cache_for(self.directory), BASE_WS, now_ts, config, [self.sessions], campaigns)
        self.assertEqual({cell["n"] for cell in idle["campaigns"][0]["cells"]}, {0})
        self.assertFalse((Path(self.directory) / "trial_ledger.jsonl").exists())
        # 60 more windows (the same generator, seeded) arrive and are accrued.
        fresh = population(60, start_index=300)
        write_cache(self.directory, fresh)
        write_sessions(self.sessions, population_records(fresh), name="session_20260902_000000.jsonl")
        later = fresh[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        ticked = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)
        self.assertEqual(ticked["build"]["rows_inserted"], 360)
        summary = ticked["campaigns"][0]
        family = json.loads((campaigns / "2026-09_planted.json").read_text())
        self.assertEqual(summary["e_bh"]["campaign_n"], family["family_size"])
        self.assertEqual(summary["e_bh"]["family"], family["family_size"])
        planted = [cell for cell in summary["cells"] if cell["cell_id"].startswith("d210_f100_")][0]
        self.assertEqual(planted["n"], sum(1 for spec in fresh if spec["kind"] == "planted"))
        self.assertGreater(planted["e_value"], 1.0)
        self.assertIn(planted["status"], {"accruing", "promote_candidate"})
        self.assertFalse(planted["ready"])  # 60 windows: below the 100-entry, 14-day requirement
        self.assertTrue(all(cell["ledger_row"] for cell in summary["cells"] if cell["applied"]))
        ledger = [json.loads(line) for line in (Path(self.directory) / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(len(ledger), sum(1 for cell in summary["cells"] if cell["applied"]))
        row = [entry for entry in ledger if entry["candidate"] == planted["fingerprint"]][0]
        self.assertEqual(row["stage"], "band_ladder_accrual")
        self.assertEqual(row["look_id"], generator.look_id(planted["fingerprint"], "band_ladder_accrual", row["fresh_range"]))
        self.assertGreater(row["fresh_range"][0], family["registered_after_window_start"])
        # A second tick without new windows applies nothing and writes no row.
        again = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)
        self.assertEqual({cell["applied"] for cell in again["campaigns"][0]["cells"]}, {0})
        self.assertEqual(len((Path(self.directory) / "trial_ledger.jsonl").read_text().splitlines()), len(ledger))
        state = self.db.execute("SELECT n, status FROM campaign_accrual WHERE fingerprint = ?", (planted["fingerprint"],)).fetchone()
        self.assertEqual(state["n"], planted["n"])
        self.assertTrue((campaigns / "status" / "2026-09_planted.json").is_file())
        # Gate artifact: the shape cmd_band_promotion_artifact reads.
        artifact = truth.gate_artifact(self.db, family, planted["cell_id"])
        self.assertEqual((artifact["candidate"], artifact["registration"], artifact["support"]), (truth.BAND_FAMILY, "2026-09_planted", planted["n"]))
        self.assertEqual(artifact["verdict"], "INSUFFICIENT")
        self.assertEqual(len(artifact["rows"]), planted["n"])
        self.assertEqual(set(artifact["rows"][0]), {"window_start", "signal", "official", "signal_entry", "vwap", "t", "won"})
        self.assertTrue(all(0.0 < row["signal_entry"] < 1.0 and isinstance(row["won"], bool) for row in artifact["rows"]))
        self.assertEqual(artifact["fresh_range"], [artifact["rows"][0]["window_start"], artifact["rows"][-1]["window_start"]])
        registered_cell = [cell for cell in family["cells"] if cell["cell_id"] == planted["cell_id"]][0]
        self.assertEqual(artifact["policy_params"]["ask_cap"], registered_cell["rule"]["favorite_price_cap"])
        self.assertEqual(artifact["policy_params"]["min_decision_margin_usd"], 100.0)
        self.assertEqual(artifact["policy_params"]["entry_window_seconds"], 1.0)
        self.assertEqual((artifact["warnings"], artifact["capacity_trend"]["verdict"]), ({"capacity_declining": False}, None))  # 60 fresh windows: 3 days
        self.assertNotIn("discovery_twin", artifact)
        with self.assertRaises(ValueError):
            truth.gate_artifact(self.db, family, "d240_f150_c0.99_p30")
        # Discovery mode: the same schema for an unregistered cell over the
        # whole table (no cut, no accrual), INSUFFICIENT by construction and
        # marked discovery_twin, for the paper observer's twin only.
        twin = truth.discovery_gate_artifact(self.db, "d210_f100_c0.96_p15")
        self.assertEqual((twin["verdict"], twin["discovery_twin"], twin["registration"], twin["registration_cut"], twin["accrual"]), ("INSUFFICIENT", True, "discovery", None, None))
        self.assertEqual((twin["candidate"], twin["cell_id"], twin["fingerprint"], twin["ladder_hosts"]), (truth.BAND_FAMILY, "d210_f100_c0.96_p15", truth.fingerprint(twin["rule"]), ["vps"]))
        self.assertEqual((twin["support"], len(twin["rows"])), (144, 144))  # 300 + 60 windows, 40% planted
        self.assertEqual(set(twin["rows"][0]), set(artifact["rows"][0]))
        self.assertEqual(twin["policy_params"]["ask_cap"], 0.96)
        self.assertEqual(set(twin) - {"discovery_twin"}, set(artifact))
        config_path = Path(self.directory) / "loop.json"
        config_path.write_text(json.dumps(config))
        output = Path(self.directory) / "twin.json"
        with contextlib.redirect_stdout(io.StringIO()) as printed:
            self.assertEqual(truth.main(["--cell-gate-json", "d210_f100_c0.96_p15", "--db", str(Path(self.directory) / "windows.sqlite3"), "--loop-config", str(config_path), "--output", str(output)]), 0)
        self.assertEqual(json.loads(output.read_text())["verdict"], "INSUFFICIENT")
        self.assertIn('"discovery_twin": true', printed.getvalue())

    def _planted_cell(self, ticked):
        return [cell for cell in ticked["campaigns"][0]["cells"] if cell["cell_id"].startswith("d210_f100_")][0]

    def test_late_ladder_is_folded_into_the_e_process_when_it_arrives(self):
        specs, now_ts = self.planted_fixture()
        campaigns = Path(self.directory) / "campaigns"
        self.assertTrue(truth.register(self.db, "2026-09_planted", campaigns, now_ts, cells=PLANTED_CELLS)["registered"])
        config = loop_config(self.directory)
        fresh = population(60, start_index=300)
        write_cache(self.directory, fresh)
        first_planted = next(spec for spec in fresh if spec["kind"] == "planted")
        records = population_records(fresh)
        withheld = [record for record in records if record["cid"] == "%016x" % first_planted["ws"]]
        write_sessions(self.sessions, [record for record in records if record not in withheld], name="session_20260902_000000.jsonl")
        later = fresh[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        # The withheld window is past the grace: its rows land without a
        # ladder while every later window is accrued.
        ticked = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)
        self.assertEqual(ticked["build"]["rows_inserted"], 360)
        planted = self._planted_cell(ticked)
        self.assertEqual((planted["n"], planted["applied"], planted["excluded"].get("no_ladder")), (23, 23, 1))
        # Its ladder arrives (a late pull): attached and folded, not skipped
        # behind the newest accrued window; score, accrual and gate agree.
        write_sessions(self.sessions, withheld, name="session_20260903_000000.jsonl")
        ticked = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)
        self.assertEqual(ticked["build"]["ladder_rows_attached"], 6)
        planted = self._planted_cell(ticked)
        self.assertEqual((planted["n"], planted["applied"], planted["excluded"].get("no_ladder")), (24, 1, None))
        state = self.db.execute("SELECT n, first_window_start FROM campaign_accrual WHERE fingerprint = ?", (planted["fingerprint"],)).fetchone()
        self.assertEqual((state["n"], state["first_window_start"]), (24, first_planted["ws"]))
        self.assertEqual(len(truth.accrued_windows(self.db, "2026-09_planted", planted["fingerprint"])), 24)
        family = json.loads((campaigns / "2026-09_planted.json").read_text())
        artifact = truth.gate_artifact(self.db, family, planted["cell_id"])
        self.assertEqual((artifact["support"], artifact["accrual"]["n"], artifact["fresh_range"][0]), (24, 24, first_planted["ws"]))
        # The cut is the registration clock (later than the newest row at registration).
        self.assertEqual((artifact["registration_cut"], family["registered_after_window_start"]), (now_ts - 1, specs[-1]["ws"]))
        # A window that started before the registration clock never
        # accrues, however late its row is built.
        stale = json.loads((campaigns / "2026-09_planted.json").read_text())
        stale["registered_at_ts"] = fresh[30]["ws"]
        self.assertEqual(truth.registration_cut(stale), fresh[30]["ws"] - 1)
        self.assertGreater(truth.registration_cut(stale), truth.registration_cut(family))
        rows_by_decision = {210: truth.load_rows(self.db, 210)}
        cut = truth._fresh_selection(rows_by_decision, truth.rule_from_cell_id(planted["cell_id"]), truth.registration_cut(stale), ("vps",))
        self.assertTrue(all(trade["window_start"] >= fresh[30]["ws"] for trade in cut["trades"]))
        self.assertLess(len(cut["trades"]), 24)

    def test_a_discovery_host_ladder_is_upgraded_to_the_evidence_record(self):
        # A row built in a VPS pull gap while the Mac observer ran holds the
        # Mac's record (small_fixture: the first window's six ladders).
        self.small_fixture()
        now_ts = BASE_WS + 5 * 300 + 300 + band.RESOLUTION_LAG_S + 1
        cache = cache_for(self.directory)
        first = truth.build(self.db, cache, BASE_WS, now_ts, [self.sessions], sha="first")
        self.assertEqual((first["ladder_rows_by_host"], first["ladder_rows_upgraded"]), ({"mac": 6, "vps": 18}, 0))
        before = {row["decision_s"]: row for row in truth.load_rows(self.db) if row["window_start"] == BASE_WS}
        # The VPS record of the same (window, anchor) reaches the mirror:
        # scan_sessions prefers it whatever the sample counts, and the row
        # takes it (a registered family reads VPS rows only: the Mac's
        # would stay ladder_discovery_host for good).
        vps = [ladder(BASE_WS, d, "up", 80.0, [sample(t, 0.96) for t in range(2)], host="vps") for d in DECISIONS]
        write_sessions(self.sessions, vps + [anchor(BASE_WS, 240, 80.0, 0.96)], name="session_20260901_010000.jsonl")
        again = truth.build(self.db, cache, BASE_WS, now_ts, [self.sessions], sha="second", refresh_prints=False)
        self.assertEqual((again["rows_inserted"], again["ladder_rows_attached"], again["ladder_rows_upgraded"], again["ladder_rows_by_host"]), (0, 0, 6, {"vps": 24}))
        after = {row["decision_s"]: row for row in truth.load_rows(self.db) if row["window_start"] == BASE_WS}
        self.assertEqual({(row["ladder_host"], row["host"], row["ladder"]["host"], len(row["ladder"]["samples"]), row["ladder"]["samples"][0]["q"][1][0]) for row in after.values()}, {("vps", "vps", "vps", 2, 0.96)})
        self.assertTrue(all(row["ladder_attached_at"] and row["built_at"] <= row["ladder_attached_at"] for row in after.values()))
        # Only the instrument moved: labels, prints, the build stamp and the basis columns are what they were.
        for decision_s, row in after.items():
            for column in ("built_at", "git_sha", "official", "open", "margin", "direction", "final_margin", "print_status", "print_prints_json", "strike_60s"):
                self.assertEqual(row[column], before[decision_s][column], column)
        # The static model now reads the window as evidence.
        rule = truth.rule_from_cell_id("d210_f75_c0.99_p0")
        self.assertEqual([trade["entry"] for trade in truth.select_cell([after[210]], rule, "ladder", ("vps",))["trades"]], [0.96])
        self.assertEqual(list(truth.select_cell([before[210]], rule, "ladder", ("vps",))["excluded"]), ["ladder_discovery_host"])
        # Idempotent, and never the other way: a Mac record does not touch a row that holds the VPS's.
        mac = [ladder(BASE_WS + 300, d, "up", 80.0, [sample(t, 0.90) for t in range(9)], host="mac") for d in DECISIONS]
        write_sessions(self.sessions, mac, name="session_20260901_020000.jsonl")
        third = truth.build(self.db, cache, BASE_WS, now_ts, [self.sessions], sha="third", refresh_prints=False)
        self.assertEqual((third["ladder_rows_attached"], third["ladder_rows_upgraded"], third["ladder_rows_by_host"]), (0, 0, {"vps": 24}))
        self.assertEqual({len(row["ladder"]["samples"]) for row in truth.load_rows(self.db) if row["window_start"] == BASE_WS + 300}, {3})

    def test_clear_audit_releases_a_held_cell(self):
        specs, now_ts = self.planted_fixture()
        campaigns = Path(self.directory) / "campaigns"
        self.assertTrue(truth.register(self.db, "2026-09_planted", campaigns, now_ts, cells=PLANTED_CELLS)["registered"])
        config = loop_config(self.directory)
        fresh = population(300, start_index=300)
        for spec in fresh:
            if spec["kind"] == "planted":  # the planted cell wins every ladder trade: WR 1.0 at n >= 100
                spec["official"] = spec["direction"]
                spec["final_margin"] = abs(spec["final_margin"]) * (1 if spec["direction"] == "up" else -1)
        write_cache(self.directory, fresh)
        write_sessions(self.sessions, population_records(fresh), name="session_20260902_000000.jsonl")
        later = fresh[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        ticked = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)
        planted = self._planted_cell(ticked)
        self.assertEqual((planted["n"], planted["wins"], planted["status"], planted["defects"], planted["ready"]), (120, 120, "manual_audit", ["wr_too_good"], True))
        family = json.loads((campaigns / "2026-09_planted.json").read_text())
        self.assertEqual(truth.gate_artifact(self.db, family, planted["cell_id"])["verdict"], "IMPLAUSIBLE_MANUAL_AUDIT")
        # Only a registered cell's defect tripwires can be cleared, with a note.
        with self.assertRaises(ValueError):
            truth.clear_audit(campaigns, "2026-09_planted", "d240_f150_c0.99_p30", ["wr_too_good"], "note")
        with self.assertRaises(ValueError):
            truth.clear_audit(campaigns, "2026-09_planted", planted["cell_id"], ["capacity_collapse"], "note")
        with self.assertRaises(ValueError):
            truth.clear_audit(campaigns, "2026-09_planted", planted["cell_id"], ["wr_too_good"], " ")
        cleared = truth.clear_audit(campaigns, "2026-09_planted", planted["cell_id"], ["wr_too_good"], "tape and labels re-derived by hand: genuine", later)
        self.assertEqual((cleared["cleared"], cleared["tripwires"]), (True, ["wr_too_good"]))
        family = json.loads((campaigns / "2026-09_planted.json").read_text())
        self.assertEqual(family["audit_cleared"][planted["fingerprint"]]["tripwires"], ["wr_too_good"])
        self.assertEqual((family["family_size"], len(family["cells"])), (2, 2))  # N untouched
        ticked = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)
        planted = self._planted_cell(ticked)
        self.assertEqual((planted["status"], planted["defects"], planted["audit_cleared"]), ("promote_candidate", [], ["wr_too_good"]))
        self.assertTrue(planted["tripwires"]["wr_too_good"])  # still reported
        artifact = truth.gate_artifact(self.db, family, planted["cell_id"])
        self.assertEqual((artifact["verdict"], artifact["audit_cleared"], artifact["support"]), ("PASS", ["wr_too_good"], 120))
        # The CLI path: exactly one active campaign, --tripwire and --note required.
        config_path = Path(self.directory) / "loop.json"
        config_path.write_text(json.dumps(config))
        argv = ["--campaigns-dir", str(campaigns), "--db", str(Path(self.directory) / "windows.sqlite3"), "--loop-config", str(config_path), "--now-ts", str(later)]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(truth.main(["--clear-audit", planted["cell_id"], "--tripwire", "wr_too_good", "--note", "re-audited"] + argv), 0)
        self.assertEqual(json.loads((campaigns / "2026-09_planted.json").read_text())["audit_cleared"][planted["fingerprint"]]["note"], "re-audited")
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            truth.main(["--clear-audit", planted["cell_id"], "--tripwire", "wr_too_good"] + argv)

    def test_stale_evaluator_holds_a_campaign_until_re_registration(self):
        specs, now_ts = self.planted_fixture()
        campaigns = Path(self.directory) / "campaigns"
        self.assertTrue(truth.register(self.db, "2026-09_planted", campaigns, now_ts, cells=PLANTED_CELLS)["registered"])
        config = loop_config(self.directory)
        path = campaigns / "2026-09_planted.json"
        family = json.loads(path.read_text())
        self.assertEqual((family["evaluator_version"], family["grammar_version"]), (truth.EVALUATOR_VERSION, truth.GRAMMAR_VERSION))
        self.assertIsNone(truth.campaign_version_error(family))
        # Registered by an older evaluator (its fingerprints, cells and kill
        # rule are that code's): a version bump folds nothing into its
        # e-processes, holds every cell and never passes its gate.
        stale = {**family, "evaluator_version": "executable_truth_v2", "grammar_version": "band_grid_v2"}
        path.write_text(json.dumps(stale, indent=2, sort_keys=True) + "\n")
        self.assertIn("executable_truth_v2/band_grid_v2", truth.campaign_version_error(stale))
        fresh = population(300, start_index=300)
        write_cache(self.directory, fresh)
        write_sessions(self.sessions, population_records(fresh), name="session_20260902_000000.jsonl")
        later = fresh[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        ticked = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)
        self.assertEqual(ticked["build"]["rows_inserted"], 1800)
        summary = ticked["campaigns"][0]
        self.assertIn("re-register", summary["stale_evaluator"])
        self.assertEqual({(cell["status"], cell["reason"], cell["applied"], cell["n"]) for cell in summary["cells"]}, {("manual_audit", "stale_evaluator", 0, 0)})
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM campaign_accrual").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM campaign_accrual_windows").fetchone()[0], 0)
        self.assertFalse((Path(self.directory) / "trial_ledger.jsonl").exists())
        self.assertIn("stale_evaluator", json.loads((campaigns / "status" / "2026-09_planted.json").read_text()))
        artifact = truth.gate_artifact(self.db, stale, "d210_f100_c0.92_p0")
        self.assertEqual((artifact["verdict"], artifact["stale_evaluator"], artifact["evaluator_version"]), ("IMPLAUSIBLE_MANUAL_AUDIT", summary["stale_evaluator"], truth.EVALUATOR_VERSION))
        # Re-registered under this tree: accrual resumes from the cut.
        path.write_text(json.dumps(family, indent=2, sort_keys=True) + "\n")
        ticked = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)
        self.assertNotIn("stale_evaluator", ticked["campaigns"][0])
        planted = self._planted_cell(ticked)
        self.assertEqual(planted["n"], sum(1 for spec in fresh if spec["kind"] == "planted"))
        self.assertNotIn("stale_evaluator", truth.gate_artifact(self.db, family, planted["cell_id"]))

    def test_git_sha_marks_a_dirty_tree(self):
        # A row's git_sha names the tree that built it: an uncommitted tree
        # carries -dirty (tracked changes only, git's own --dirty rule).
        repo = Path(self.directory) / "repo"
        repo.mkdir()
        env = {"PATH": os.environ["PATH"], "HOME": str(repo), "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}

        def git(*args):
            subprocess.run(["git", "-c", "commit.gpgsign=false", *args], cwd=str(repo), env=env, check=True, capture_output=True)

        git("init", "-q")
        (repo / "a.txt").write_text("a\n")
        git("add", "a.txt")
        git("commit", "-q", "-m", "a")
        root = truth.ROOT
        truth.ROOT = repo
        try:
            clean = truth.git_sha()
            self.assertGreaterEqual(len(clean), 7)
            self.assertFalse(clean.endswith(truth.GIT_DIRTY_SUFFIX))
            (repo / "a.txt").write_text("b\n")
            self.assertEqual(truth.git_sha(), clean + truth.GIT_DIRTY_SUFFIX)
            git("checkout", "--", "a.txt")
            (repo / "untracked.txt").write_text("c\n")
            self.assertEqual(truth.git_sha(), clean)
        finally:
            truth.ROOT = root

    def test_look_id_dedups_trial_ledger_rows(self):
        config = loop_config(self.directory)
        self.assertTrue(generator.append_trial_entry(config, "cand", "stage", "accruing", n=3, wins=2, fresh_range=[BASE_WS, BASE_WS + 600]))
        self.assertFalse(generator.append_trial_entry(config, "cand", "stage", "accruing", n=3, wins=2, fresh_range=[BASE_WS, BASE_WS + 600]))
        self.assertTrue(generator.append_trial_entry(config, "cand", "stage", "accruing", n=4, wins=3, fresh_range=[BASE_WS, BASE_WS + 900]))
        self.assertTrue(generator.append_trial_entry(config, "cand", "stage", "accruing", n=4, wins=3))  # legacy rows carry no look
        rows = [json.loads(line) for line in (Path(self.directory) / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["look_id"], generator.look_id("cand", "stage", [BASE_WS, BASE_WS + 600]))
        self.assertEqual(rows[0]["fresh_range"], [BASE_WS, BASE_WS + 600])
        self.assertNotEqual(rows[0]["look_id"], rows[1]["look_id"])
        self.assertNotIn("look_id", rows[2])
        self.assertEqual(generator.look_id("a", "b", [1, 2]), generator.look_id("a", "b", (1, 2)))
        self.assertNotEqual(generator.look_id("a", "b", [1, 2]), generator.look_id("a", "c", [1, 2]))

    def test_fee_is_pinned_from_realized_fills(self):
        fills = [fill("o1", 0.92, 5.42), fill("o1", 0.92, 5.42, kind="reconciled"), fill("o2", 0.80, 6.0), fill("o3", 0.99, 25.0)]
        pinned = truth.pinned_fee_rate(fills)
        self.assertEqual((pinned["n"], pinned["source"], pinned["paper_fills_skipped"]), (3, "engine_rate_via_fills", 0))
        self.assertAlmostEqual(pinned["rate"], FEE, places=9)
        self.assertNotIn("warning", pinned)
        self.assertNotIn("parity", pinned)  # the engine books its own rate: no venue fee is measured
        self.assertAlmostEqual(truth.fill_fee_rate(fill("o", 0.92, 5.423912)), 0.07, places=9)
        self.assertIsNone(truth.fill_fee_rate({"fee": 0.01, "filled": 0.0, "fill_price": 0.5}))
        fallback = truth.pinned_fee_rate([])
        self.assertEqual((fallback["rate"], fallback["n"], fallback["source"]), (truth.DEFAULT_FEE_RATE, 0, "default"))
        self.assertIn("no realized fills", fallback["warning"])
        drifted = truth.pinned_fee_rate([fill("o1", 0.90, 10.0, rate=0.075)])
        self.assertAlmostEqual(drifted["rate"], 0.075)
        self.assertIn("differs from the engine constant", drifted["warning"])
        # The paper observer's fills reproduce the engine constant by
        # construction: skipped, so they can never bury a realized drift.
        mixed = truth.pinned_fee_rate([fill("paper-0x5304efa7", 0.85255, 22.28), fill("paper-0x86054f8a", 0.90, 20.0), fill("o9", 0.90, 10.0, rate=0.075)])
        self.assertEqual((mixed["n"], mixed["paper_fills_skipped"]), (1, 2))
        self.assertAlmostEqual(mixed["rate"], 0.075)
        self.assertIn("warning", mixed)
        self.assertTrue(truth.is_paper_fill(fill("paper-0x1", 0.9, 1.0)))
        self.assertFalse(truth.is_paper_fill(fill("0xabc", 0.9, 1.0)))
        # The synthetic session end to end: build pins the same rate.
        write_sessions(self.sessions, fills)
        write_cache(self.directory, [])
        summary = truth.build(self.db, cache_for(self.directory), BASE_WS, BASE_WS + DAY_S, [self.sessions], sha="fee")
        self.assertEqual(summary["fee"]["n"], 3)
        self.assertAlmostEqual(truth.get_meta(self.db, "fee")["rate"], FEE, places=9)
        self.assertAlmostEqual(truth.break_even(0.95, FEE), band.break_even(0.95))


# --- band_event_v4 -----------------------------------------------------------

settlement = truth.settlement_model
EW = BASE_WS + 40 * 300  # the window of the hand fixtures
# The frozen shape with a two-minute warm-up: a hand fixture is ten minutes of closes.
HAND_PARAMS = {**settlement.DEFAULT_PARAMS, "warmup_s": 120}


def hand_spec(c=0.0, s_b=3.5):
    """A frozen spec written by hand.  With c = 0 the scale is the basis
    noise alone: z_t = |m_t| / 3.5.  Three z buckets: below 2.5 no tick
    clears, [2.5, 4) is capped at 0.98, 4 and above at 0.99."""

    def row(low, high, cap, upper):
        return {"z_low": low, "z_high": high, "n": 1000, "wins": 990, "rate": 0.99, "wilson_lower": 0.98, "wilson_upper": upper, "cap": cap}

    spec = {
        "version": settlement.SPEC_VERSION,
        "params": HAND_PARAMS,
        "c": c,
        "s_b": s_b,
        "fee_rate": FEE,
        "cut_ts": EW,
        "sigma2_median": 4.0,
        "table": [row(0.0, 2.5, None, 0.95), row(2.5, 4.0, 0.98, 0.994), row(4.0, None, 0.99, 0.9997)],
        "first_crossing": [],
    }
    spec["sha256"] = settlement.spec_sha256(spec)
    return spec


def step_closes(ws, steps, base=70000.0):
    """Flat at `base` from ten minutes before the window, then base + level
    from second ws + offset on for each (offset, level): with the strike at
    `base`, m_t = level for every t > offset."""
    closes = {}
    level = 0.0
    changes = dict(steps)
    for ts in range(ws - 600, ws + 300):
        level = changes.get(ts - ws, level)
        closes[ts] = base + level
    return closes


def tiling(ws, direction, prices, default=0.97, ages=None, host=None):
    """Directional ladders at the four live anchors (seconds 150-270): the
    quoted side's $25 worst is `default`, or prices[second]; ages[second]
    is the sampled book's age."""
    records = []
    for anchor_s in (150, 180, 210, 240):
        samples = []
        for t in range(31):
            second = anchor_s + t
            item = sample(t, prices.get(second, default))
            item["age"] = (ages or {}).get(second, 0.0)
            samples.append(item)
        records.append(ladder(ws, anchor_s, direction, 80.0 if direction == "up" else -80.0, samples, host=host))
    return records


def event_rows(ws, records, official="up", host="vps"):
    return [
        {"window_start": ws, "decision_s": record["anchor_s"], "official": official, "ladder": record, "ladder_host": record.get("host") or host, "final_margin": 10.5, "final_bucket": "10-25", "final_settle_margin": 10.5}
        for record in records
    ] or [{"window_start": ws, "decision_s": 150, "official": official, "ladder": None, "ladder_host": None, "final_margin": 10.5, "final_bucket": "10-25", "final_settle_margin": 10.5}]


def event_window(closes, records, spec, ws=EW, official="up"):
    series = truth.event_series(closes, [ws], spec)
    return truth.event_windows(event_rows(ws, records, official), series, spec, ("vps",))[0]


def event_rule(threshold=3.0, cap_rule="c0.99", t0=150, t1=270, trigger="z"):
    return {"trigger": trigger, "t0": t0, "t1": t1, "threshold": threshold, "cap_rule": cap_rule}


class EventModelTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.directory = self.enterContext(tempfile.TemporaryDirectory(dir=str(ROOT / "logs")))
        self.sessions = Path(self.directory) / "sessions"
        self.db = truth.open_db(Path(self.directory) / "windows.sqlite3")
        self.addCleanup(self.db.close)

    # --- the replay ------------------------------------------------------------

    def test_event_model_enters_once_one_sample_later_and_a_kill_continues_the_scan(self):
        spec = hand_spec()
        # m_t = 10.5 from t = 161 on (the close of second 160 is the first
        # at the new level): z = 10.5 / 3.5 = 3.0, side up.
        closes = step_closes(EW, [(160, 10.5)])
        # The up book walks $25 at 0.97, but at 0.995 at 162 s and 0.96 at 164 s.
        window = event_window(closes, tiling(EW, "up", {162: 0.995, 164: 0.96}), spec)
        self.assertEqual(sorted(window["tiled"]), list(range(150, 271)))
        self.assertEqual((window["path"][160], window["path"][161]), ((0.0, 0.0), (10.5, 3.0)))
        trade, reason, signal = truth.event_trade(event_rule(), window, spec)
        # 161 s: signal, 0.97 <= cap, limit 0.99 sent; the book one sample
        # later walks at 0.995: killed.  162 s: above the cap.  163 s:
        # 0.97, and the 164 s book fills at 0.96: the entry is that later
        # book's worst price, not the decision book's.
        self.assertEqual((reason, signal), (None, (161, "up")))
        self.assertEqual({key: trade[key] for key in ("window_start", "direction", "t", "filled_t", "entry", "limit", "z", "margin")}, {"window_start": EW, "direction": "up", "t": 163, "filled_t": 164, "entry": 0.96, "limit": 0.99, "z": 3.0, "margin": 10.5})
        self.assertEqual((trade["frozen_ceiling"], trade["stalled_before"]), (0.994, False))
        # Lag 0 fills the decision sample; lag 2 the book two samples on;
        # a 1 s older price sees the signal a second later.
        self.assertEqual([truth.event_trade(event_rule(), window, spec, lag, info)[0][key] for lag, info in ((0, 0), (2, 0), (1, 1)) for key in ("t", "filled_t", "entry")], [161, 161, 0.97, 161, 163, 0.97, 163, 164, 0.96])
        self.assertEqual(truth.event_trade(event_rule(), window, spec, 1, 1)[2], (162, "up"))
        # One entry per window, although every second after 164 s clears too.
        selection = truth.select_event_cell([window], event_rule(), spec)
        self.assertEqual((selection["model"], selection["cell_id"], len(selection["trades"]), selection["signal_windows"], selection["coverage"]), ("event", "e150-270_z3.0_c0.99", 1, 1, 1.0))
        self.assertEqual((selection["capacity_points"], selection["capacity"], selection["excluded"]), ([(EW, True)], {"25": 1.0}, {}))
        self.assertEqual({name: [(trade["t"], trade["entry"]) for trade in trades] for name, trades in selection["variants"].items()}, {"lag0": [(161, 0.97)], "lag2": [(161, 0.97)], "older1": [(163, 0.96)]})
        self.assertEqual(selection["signals"], [{"window_start": EW, "t": 161, "direction": "up"}])
        # Cap rules: c0.98 trades the same seconds at limit 0.98; k98 reads
        # the frozen schedule (z 3.0 is in [2.5, 4): 0.98).
        self.assertEqual([truth.event_trade(event_rule(cap_rule=cap), window, spec)[0]["limit"] for cap in ("c0.98", "k98")], [0.98, 0.98])
        dearer = event_window(closes, tiling(EW, "up", {}, default=0.985), spec)
        self.assertEqual(truth.event_trade(event_rule(cap_rule="c0.99"), dearer, spec)[0]["entry"], 0.985)
        self.assertEqual(truth.event_trade(event_rule(cap_rule="k98"), dearer, spec)[:2], (None, "never_cleared"))
        # ... and from z = 4 the schedule opens to 0.99 (m = 14: z = 4.0).
        strong = event_window(step_closes(EW, [(160, 14.0)]), tiling(EW, "up", {}, default=0.985), spec)
        k98 = truth.event_trade(event_rule(threshold=2.5, cap_rule="k98"), strong, spec)[0]
        self.assertEqual((k98["z"], k98["limit"], k98["entry"], k98["frozen_ceiling"]), (4.0, 0.99, 0.985, 0.9997))
        # The range and the threshold: no signal below z*, none outside [t0, t1].
        self.assertEqual(truth.event_trade(event_rule(threshold=3.5), window, spec), (None, None, None))
        self.assertEqual(truth.event_trade(event_rule(t0=150, t1=160), window, spec), (None, None, None))
        self.assertEqual(truth.event_trade(event_rule(t0=210), window, spec)[0]["t"], 210)
        # A decision at the last second has no later sample: it cannot fill.
        # (Inside the closing TWAP a step of 21 at 269 s moves the margin by 31/60 of it: z = 3.1.)
        last = event_window(step_closes(EW, [(269, 21.0)]), tiling(EW, "up", {}), spec)
        self.assertAlmostEqual(last["path"][270][0], 21.0 * 31 / 60)
        self.assertEqual(truth.event_trade(event_rule(), last, spec)[:2], (None, "fok_killed"))
        self.assertEqual(truth.event_trade(event_rule(), last, spec, 0)[0]["t"], 270)
        # The paired controls run the same replay behind another trigger:
        # a USD settlement floor, or z at the spec's constant sigma.
        self.assertEqual(truth.event_trade(truth.control_rule_from_id("x150-270_m10_c0.99"), window, spec)[0]["t"], 163)
        self.assertEqual(truth.event_trade(truth.control_rule_from_id("x150-270_m11_c0.99"), window, spec), (None, None, None))
        self.assertEqual(truth.event_trade(truth.control_rule_from_id("x150-270_s3.0_c0.99"), window, spec)[0]["t"], 163)

    def test_event_model_names_why_a_signal_window_never_traded(self):
        spec = hand_spec()
        closes = step_closes(EW, [(160, 10.5)])
        # Killed at its only clearing decision, above the cap ever after.
        killed = event_window(closes, tiling(EW, "up", {161: 0.97}, default=0.995), spec)
        self.assertEqual(truth.event_trade(event_rule(), killed, spec)[:2], (None, "fok_killed"))
        self.assertEqual(truth.event_trade(event_rule(), killed, spec, 0)[0]["entry"], 0.97)
        above = event_window(closes, tiling(EW, "up", {}, default=0.995), spec)
        self.assertEqual(truth.event_trade(event_rule(), above, spec)[:2], (None, "never_cleared"))
        # A quoted side with no executable ask at the budget (a pinned favourite) never cleared either.
        pinned = tiling(EW, "up", {})
        for record in pinned:
            for item in record["samples"]:
                item["q"] = [None, None, None]
        self.assertEqual(truth.event_trade(event_rule(), event_window(closes, pinned, spec), spec)[:2], (None, "never_cleared"))
        # Availability, not the rule: the engine latched the other side (the
        # settlement side was never quoted), no ladder, a discovery host's,
        # none in the range.
        other = event_window(closes, tiling(EW, "down", {}), spec)
        self.assertEqual(truth.event_trade(event_rule(), other, spec), (None, "side_unquoted", (161, "up")))
        self.assertEqual(truth.event_trade(event_rule(), event_window(closes, [], spec), spec)[1], "no_ladder")
        mac = event_window(closes, tiling(EW, "up", {}, host="mac"), spec)
        self.assertEqual((mac["tiled"], truth.event_trade(event_rule(), mac, spec)[1]), ({}, "ladder_discovery_host"))
        early = event_window(closes, tiling(EW, "up", {})[:1], spec)  # anchor 150 only: seconds 150-180
        self.assertEqual(truth.event_trade(event_rule(t0=210), early, spec)[1], "ladder_uncovered")
        selection = truth.select_event_cell([killed, above, other, mac, early], event_rule(), spec)
        self.assertEqual({reason: len(rows) for reason, rows in selection["excluded"].items()}, {"fok_killed": 1, "never_cleared": 1, "side_unquoted": 1, "ladder_discovery_host": 1})
        self.assertEqual((len(selection["trades"]), selection["signal_windows"], selection["coverage"], selection["capacity"]), (1, 5, 0.6, {"25": 1 / 3}))
        # The excluded rows carry the side at the first signal second: the population scoring pools.
        self.assertEqual({row["direction"] for rows in selection["excluded"].values() for row in rows}, {"up"})
        labels = {EW: "up"}
        score = truth.score_selection(selection, labels, FEE, {})
        self.assertEqual((score["cell_id"], score["n"], score["excluded_pooled"]["n"], score["excluded"]["ladder_discovery_host"]["n"]), ("e150-270_z3.0_c0.99", 1, 2, 1))
        self.assertEqual(sorted(score["latency"]), ["lag0", "lag2", "older1"])
        # Lag 0 fills the killed window's decision sample too.
        self.assertEqual((score["latency"]["lag0"]["n"], score["capacity_trend"], score["edge_migration"]), (2, None, None))

    def test_book_age_rule_and_the_stall_flag(self):
        spec = hand_spec()
        closes = step_closes(EW, [(160, 10.5)])
        fresh = sample(0, 0.97)
        self.assertEqual([truth.book_state({**fresh, "age": age}) for age in (0.0, 1.0, 1.01, 19.0, None)], ["ok", "ok", "stalled", "stalled", "stalled"])
        self.assertEqual(truth.book_state({**fresh, "fresh": False}), "stale")
        self.assertEqual(truth.MAX_BOOK_AGE_S, 1.0)
        # The engine's `fresh` flag alone admitted a 19 s old book; the
        # models do not.
        self.assertIsNotNone(truth.sample_clears(fresh, "up", 1, 0.80, 0.99))
        self.assertIsNone(truth.sample_clears({**fresh, "age": 19.0}, "up", 1, 0.80, 0.99))
        self.assertIsNotNone(truth.sample_clears({**fresh, "age": 19.0}, "up", 1, 0.80, 0.99, admit_stalled=True))
        self.assertIsNone(truth.sample_clears({**fresh, "fresh": False}, "up", 1, 0.80, 0.99, admit_stalled=True))
        # Event model: the only clearing second sits on a stalled book.
        stalled = event_window(closes, tiling(EW, "up", {161: 0.97}, default=0.995, ages={161: 19.0}), spec)
        self.assertEqual(stalled["tiled"][161]["book"], "stalled")
        self.assertEqual(truth.event_trade(event_rule(), stalled, spec)[:2], (None, "book_stalled"))
        # A stalled decision book is skipped and the scan goes on ...
        later = event_window(closes, tiling(EW, "up", {}, ages={161: 19.0, 162: 5.0}), spec)
        trade = truth.event_trade(event_rule(), later, spec)[0]
        self.assertEqual((trade["t"], trade["filled_t"], trade["stalled_before"]), (163, 164, True))
        # ... and a stalled book one sample later cannot fill the order.
        late_fill = event_window(closes, tiling(EW, "up", {}, ages={162: 1.5}), spec)
        self.assertEqual(truth.event_trade(event_rule(), late_fill, spec)[0]["t"], 163)
        selection = truth.select_event_cell([stalled, later], event_rule(), spec)
        self.assertEqual(({reason: len(rows) for reason, rows in selection["excluded"].items()}, selection["capacity_points"], selection["coverage"]), ({"book_stalled": 1}, [(EW, True)], 0.5))
        self.assertIn("book_stalled", truth.AVAILABILITY_EXCLUSIONS)
        # Static ladder model: the same rule, the same flag.
        rule = {"decision_second": 210, "margin_floor_usd": 75, "favorite_price_cap": 0.99, "patience_s": 0}
        record = ladder(EW, 210, "up", 90.0, [{**sample(0, 0.95), "age": 25.0}, sample(1, 0.95)])
        self.assertIsNone(truth.ladder_trade(rule, record, "up"))
        self.assertEqual(truth.ladder_trade(rule, record, "up", admit_stalled=True)["t"], 0)
        self.assertEqual(truth.ladder_trade({**rule, "patience_s": 15}, record, "up")["t"], 1)
        row = {"window_start": EW, "decision_s": 210, "direction": "up", "margin": 90.0, "ladder": record, "ladder_host": "vps", "final_margin": 120.0, "final_bucket": "100-150"}
        static = truth.select_cell([row], rule, "ladder")
        self.assertEqual((list(static["excluded"]), static["trades"], static["coverage"]), (["book_stalled"], [], 0.0))
        self.assertEqual(len(truth.select_cell([row], {**rule, "patience_s": 15}, "ladder")["trades"]), 1)

    def test_tiled_ladders_take_the_later_anchor_where_two_cover_a_second(self):
        first = ladder(EW, 150, "up", 80.0, [sample(t, 0.90) for t in range(31)])
        second = ladder(EW, 180, "up", 80.0, [sample(t, 0.95) for t in range(31)])
        tiled = truth.tile_ladders({150: first, 180: second})
        self.assertEqual(sorted(tiled), list(range(150, 211)))
        self.assertEqual((tiled[179]["up"], tiled[180]["up"], tiled[180]["down"], tiled[180]["book"], tiled[180]["sides"]), ((0.90, 0.90, 1.0), (0.95, 0.95, 1.0), None, "ok", ("up",)))
        # A later sample with no book at all (not fresh, no age) yields to the earlier anchor's t = 30.
        empty = {"t": 0, "q": [None, None, None], "c": None, "age": None, "fresh": False}
        second = ladder(EW, 180, "up", 80.0, [empty] + [sample(t, 0.95) for t in range(1, 31)])
        self.assertEqual(truth.tile_ladders({150: first, 180: second})[180]["up"], (0.90, 0.90, 1.0))
        # The gates a tile applies: the budget's quote, vwap above the ask floor, a coherent pair; never the cap.
        gates = ladder(EW, 150, "up", 80.0, [sample(0, 0.999), sample(1, 0.95, vwap=0.80), sample(2, 0.95, c=0.30), {**sample(3, 0.95), "q": [[0.95, 0.95, 5.0, False], None, None]}])
        tiled = truth.tile_ladders({150: gates})
        self.assertEqual([tiled[second]["up"] for second in (150, 151, 152, 153)], [(0.999, 0.999, 1.0), None, None, None])
        # `fill` is the executable quote as it stands, whatever the ask floor or the pair: what an order in flight meets.
        self.assertEqual([tiled[second]["fill"].get("up") for second in (150, 151, 152, 153)], [(0.999, 0.999, 1.0), (0.95, 0.80, 1.0), (0.95, 0.95, 1.25), None])
        self.assertEqual((tiled[150]["books"], tiled[150]["fill"].get("down")), ({"up": "ok"}, None))
        # A no-direction record quotes both sides.
        both = {"t": 0, "fresh": True, "age": 0.2, "c": None, "q": {"up": [[0.95, 0.95, 5.0, False]] * 3, "down": [[0.06, 0.06, 80.0, False]] * 3}}
        tiled = truth.tile_ladders({150: ladder(EW, 150, None, None, [both])})
        self.assertEqual((tiled[150]["up"], tiled[150]["down"], tiled[150]["sides"]), ((0.95, 0.95, 0.95 + 0.06), None, ("up", "down")))
        self.assertEqual((tiled[150]["books"], tiled[150]["fill"]["down"]), ({"up": "ok", "down": "ok"}, (0.06, 0.06, 0.06 + 0.95)))

    def test_settlement_side_quotes_are_read_where_the_engine_recorded_them(self):
        spec = hand_spec()
        closes = step_closes(EW, [(160, 10.5)])  # the settlement side is up from 161 s
        # The engine latched down (the point basis) and quotes that book;
        # from 161 s its samples also carry the settlement side's
        # (band_ladder_sample: sq = the up book at each budget, sc = the
        # down best ask, sage = the up book's own age), and the record its
        # strike.

        def engine_records(sage=None, quotes=([0.97, 0.97, 25.77, False],) * 3):
            records = tiling(EW, "down", {}, default=0.03)
            for record in records:
                record["strike_60s"] = 70000.0
                for item in record["samples"]:
                    second = record["anchor_s"] + item["t"]
                    item.update({"px": closes[EW + second - 1], "px_age": 0.412, "m": closes[EW + second - 1] - 70000.0})
                    if second >= 161:
                        item.update({"sq": [None if quote is None else list(quote) for quote in quotes], "sc": 0.03, "sage": (sage or {}).get(second, 0.0)})
            return records

        records = engine_records()
        window = event_window(closes, records, spec)
        tiled = window["tiled"]
        self.assertEqual((tiled[160]["sides"], tiled[160]["books"], tiled[160]["up"]), (("down",), {"down": "ok"}, None))
        self.assertEqual((tiled[161]["sides"], tiled[161]["books"], tiled[161]["up"]), (("down", "up"), {"down": "ok", "up": "ok"}, (0.97, 0.97, 1.0)))
        # The momentum side reads as before: its 0.03 book is under the ask floor, never an entry.
        self.assertEqual((tiled[161]["down"], tiled[161]["fill"]["down"], tiled[161]["book"]), (None, (0.03, 0.03, 1.0), "ok"))
        trade, reason, signal = truth.event_trade(event_rule(), window, spec)
        self.assertEqual((reason, signal), (None, (161, "up")))
        self.assertEqual({key: trade[key] for key in ("direction", "t", "filled_t", "entry", "fill_outside_gates")}, {"direction": "up", "t": 161, "filled_t": 162, "entry": 0.97, "fill_outside_gates": False})
        # The same window from an engine that did not record the side: availability, as before.
        old = event_window(closes, tiling(EW, "down", {}, default=0.03), spec)
        self.assertEqual((old["tiled"][161]["sides"], truth.event_trade(event_rule(), old, spec)), (("down",), (None, "side_unquoted", (161, "up"))))
        # The settlement side's own book age decides (the down book is live, the up book 2 s old at 161-162 s).
        aged = event_window(closes, engine_records(sage={161: 2.0, 162: 2.0}), spec)
        self.assertEqual((aged["tiled"][161]["books"], aged["tiled"][161]["book"]), ({"down": "ok", "up": "stalled"}, "ok"))
        late = truth.event_trade(event_rule(), aged, spec)[0]
        self.assertEqual((late["t"], late["filled_t"], late["stalled_before"]), (163, 164, True))
        # No executable ask on the recorded side: the rule never cleared (no longer side_unquoted).
        empty = event_window(closes, engine_records(quotes=(None, None, None)), spec)
        self.assertEqual(truth.event_trade(event_rule(), empty, spec)[:2], (None, "never_cleared"))
        # The table row keeps the record's strike and every sample key.
        kept = json.loads(truth._ladder_columns({**records[0], "host": "vps"})["ladder_json"])
        self.assertEqual((kept["strike_60s"], sorted(kept["samples"][11])), (70000.0, ["age", "c", "fresh", "m", "px", "px_age", "q", "sage", "sc", "sq", "t"]))
        self.assertNotIn("strike_60s", json.loads(truth._ladder_columns({**tiling(EW, "down", {})[0], "host": "vps"})["ladder_json"]))
        # So does the anchor row: the engine's settlement read at the anchor second (an older record carries none of the four).
        read = {"strike_60s": 70000.0, "px": 69920.0, "px_age": 0.412, "m": -80.0}
        kept = json.loads(truth._anchor_columns({**anchor(EW, 150, -80.0, 0.95), **read, "host": "vps"}))
        self.assertEqual(({key: kept[key] for key in read}, kept["margin"]), (read, -80.0))
        self.assertFalse(set(read) & set(json.loads(truth._anchor_columns(anchor(EW, 150, -80.0, 0.95)))))

    def test_an_order_in_flight_fills_at_a_collapsed_ask(self):
        spec = hand_spec()
        closes = step_closes(EW, [(160, 10.5)])
        # Signal at 161 s on a 0.97 book: the limit 0.99 is sent.  One
        # sample later the ask has collapsed to 0.60 on a live book (a
        # sharp reversal).  A real FOK fills there; the entry gates (vwap
        # above 0.80) are the decision's, not the fill's.
        collapsed = event_window(closes, tiling(EW, "up", {162: 0.60}), spec)
        self.assertEqual((collapsed["tiled"][162]["up"], collapsed["tiled"][162]["fill"]["up"]), (None, (0.60, 0.60, 1.0)))
        trade, reason, _ = truth.event_trade(event_rule(), collapsed, spec)
        self.assertEqual((reason, {key: trade[key] for key in ("t", "filled_t", "entry", "vwap", "limit", "fill_outside_gates")}), (None, {"t": 161, "filled_t": 162, "entry": 0.60, "vwap": 0.60, "limit": 0.99, "fill_outside_gates": True}))
        # The favourite reprices before its complement: 0.85 + 0.03 = 0.88 is not a coherent pair, and fills all the same.
        records = tiling(EW, "up", {162: 0.85})
        records[0]["samples"][12]["c"] = 0.03
        incoherent = truth.event_trade(event_rule(), event_window(closes, records, spec), spec)[0]
        self.assertEqual((incoherent["filled_t"], incoherent["entry"], incoherent["fill_outside_gates"]), (162, 0.85, True))
        self.assertAlmostEqual(incoherent["pair_sum"], 0.88)
        # As a DECISION book the same quote is no entry: the scan goes on to 162 s.
        decision = truth.event_trade(event_rule(), event_window(closes, tiling(EW, "up", {161: 0.60}), spec), spec)[0]
        self.assertEqual((decision["t"], decision["filled_t"], decision["entry"], decision["fill_outside_gates"]), (162, 163, 0.97, False))
        # Lag 0 fills the decision sample, inside the gates by construction.
        self.assertEqual([truth.event_trade(event_rule(), collapsed, spec, 0)[0][key] for key in ("t", "entry", "fill_outside_gates")], [161, 0.97, False])
        # The loss reaches the score at its price, and the fill is counted.
        selection = truth.select_event_cell([collapsed], event_rule(), spec)
        self.assertEqual((len(selection["trades"]), selection["excluded"]), (1, {}))
        score = truth.score_selection(selection, {EW: "down"}, FEE, {})
        self.assertEqual((score["n"], score["wins"], score["mean_net_per_usd"], score["fills_outside_gates"]), (1, 0, -1.0, 1))
        self.assertAlmostEqual(score["mean_break_even"], be(0.60))
        self.assertIsNone(self.event_selection(1, 1, model="ladder")["fills_outside_gates"])
        # The matched null draws its candidates under the same fill: the 0.60 stratum holds this window.
        contrast = truth.matched_contrast([collapsed], selection, {EW: "down"}, FEE)
        self.assertEqual((contrast["n"], contrast["strata"], contrast["win_rate"], contrast["matched_win_rate"]), (1, 1, 0.0, 0.0))

    def test_event_replay_has_no_lookahead(self):
        # A real volatility scale (c = 1.19) on a random walk: the entry
        # and its z depend on closes of seconds before the decision only.
        spec = hand_spec(c=1.19)
        rng = random.Random(8)
        closes, price = {}, 70000.0
        for ts in range(EW - 600, EW + 300):
            price += rng.gauss(1.0 if ts >= EW else 0.0, 3.0)
            closes[ts] = price
        records = tiling(EW, "up", {})
        window = event_window(closes, records, spec)
        peak = max(range(150, 201), key=lambda second: window["path"][second][1])
        rule = event_rule(threshold=window["path"][peak][1])
        trade, _, signal = truth.event_trade(rule, window, spec)
        self.assertEqual((signal[0], trade["t"], trade["z"], trade["entry"]), (peak, peak, rule["threshold"], 0.97))
        # Every close from the decision second on rewritten: the same signal, z, margin and entry.
        future = {ts: (value - 900.0 if ts >= EW + peak else value) for ts, value in closes.items()}
        again = truth.event_trade(rule, event_window(future, records, spec), spec)
        self.assertEqual(again[0], trade)
        self.assertEqual(again[2], signal)
        # A window whose closes stop short of its end is not replayed yet
        # (whole or not at all): no path, no signal, nothing to fold twice.
        partial = {ts: value for ts, value in closes.items() if ts < EW + 280}
        series = settlement.CloseSeries(partial, EW - 600, EW + 300, HAND_PARAMS["max_fill_s"])
        unfinished = truth.event_windows(event_rows(EW, records), series, spec, ("vps",))[0]
        self.assertEqual((unfinished["path"], truth.event_trade(rule, unfinished, spec)), ({}, (None, None, None)))
        # The newest close the decision reads is the one of the second before it.
        latest = dict(closes)
        latest[EW + peak - 1] -= 900.0
        self.assertNotEqual(event_window(latest, records, spec)["path"][peak], window["path"][peak])
        self.assertEqual(event_window(latest, records, spec)["path"][peak - 1], window["path"][peak - 1])

    # --- scoring ---------------------------------------------------------------

    def event_selection(self, wins, total, ceiling=0.9987, model="event"):
        trades = [
            {"window_start": BASE_WS + index * 300, "direction": "up", "entry": 0.98, "t": 200, "frozen_ceiling": ceiling, "final_margin": 200.0, "final_bucket": "150-inf", "final_settle_margin": 200.0, "shifted": {}}
            for index in range(total)
        ]
        labels = {BASE_WS + index * 300: ("up" if index < wins else "down") for index in range(total)}
        rule = event_rule() if model == "event" else {"decision_second": 210, "margin_floor_usd": 100, "favorite_price_cap": 0.99, "patience_s": 0}
        selection = {"model": model, "rule": rule, "trades": trades, "excluded": {}, "signal_windows": total, "coverage": 1.0, "capacity": None, "capacity_points": [], "variants": {}}
        return truth.score_selection(selection, labels, FEE, {})

    def test_wr_too_good_of_an_event_cell_tests_against_the_frozen_ceiling(self):
        # 399/400 = 0.9975: above the fixed 0.995 of a static cell, inside
        # a frozen ceiling of 0.9987 (the mean Wilson upper bound of the
        # entries' z buckets).
        static = self.event_selection(399, 400, model="ladder")
        self.assertEqual((static["tripwires"]["wr_too_good"], static["frozen_ceiling"], static["frozen_ceiling_p"]), (True, None, None))
        event = self.event_selection(399, 400)
        self.assertEqual((event["tripwires"]["wr_too_good"], event["frozen_ceiling"]), (False, 0.9987))
        self.assertNotIn("wr_too_good", event["defects"])
        # A test, not a point comparison.  V3 on 2026-10-01: 412/414 =
        # 0.99517 against a ceiling of 0.99488, two losses where the
        # ceiling itself expects 2.1: P(X >= 412) = 0.64, no trip.
        noise = self.event_selection(412, 414, ceiling=0.99488)
        self.assertGreater(noise["win_rate"], noise["frozen_ceiling"])
        self.assertAlmostEqual(noise["frozen_ceiling_p"], truth.binomial_upper_tail(412, 414, 0.99488))
        self.assertTrue(0.6 < noise["frozen_ceiling_p"] < 0.7 and not noise["tripwires"]["wr_too_good"])
        # A spotless 400 is what a 0.9987 ceiling gives six times in ten.
        spotless = self.event_selection(400, 400)
        self.assertAlmostEqual(spotless["frozen_ceiling_p"], 0.9987**400)
        self.assertFalse(spotless["tripwires"]["wr_too_good"])
        # It holds a record the ceiling does not explain: 600/600 at 0.9944 has probability 0.034 ...
        held = self.event_selection(600, 600, ceiling=0.9944)
        self.assertAlmostEqual(held["frozen_ceiling_p"], 0.9944**600)
        self.assertTrue(held["tripwires"]["wr_too_good"])
        self.assertIn("wr_too_good", held["defects"])
        # ... one loss in the 600 has 0.15, a bucket that never lost (upper bound 1.0) cannot trip, nor can n < 100.
        self.assertFalse(self.event_selection(599, 600, ceiling=0.9944)["tripwires"]["wr_too_good"])
        self.assertEqual((self.event_selection(600, 600, ceiling=1.0)["frozen_ceiling_p"], self.event_selection(600, 600, ceiling=1.0)["tripwires"]["wr_too_good"]), (1.0, False))
        self.assertFalse(self.event_selection(99, 99, ceiling=0.95)["tripwires"]["wr_too_good"])
        self.assertTrue(self.event_selection(100, 100, ceiling=0.95)["tripwires"]["wr_too_good"])
        # The tail itself.
        self.assertAlmostEqual(truth.binomial_upper_tail(2, 2, 0.5), 0.25)
        self.assertAlmostEqual(truth.binomial_upper_tail(1, 2, 0.5), 0.75)
        self.assertEqual((truth.binomial_upper_tail(0, 5, 0.3), truth.binomial_upper_tail(5, 5, 1.0), truth.binomial_upper_tail(1, 5, 0.0)), (1.0, 1.0, 0.0))
        self.assertEqual(truth.EVENT_WR_TOO_GOOD_ALPHA, 0.05)

    def test_oracle_noise_is_on_the_settlement_basis(self):
        # Point basis +3 (up), settlement basis -40 (down): the label is
        # judged against the side the 60 s TWAPs settle.
        row = {"window_start": BASE_WS, "final_margin": 3.0, "final_bucket": "0-5", "final_settle_margin": -40.0, "official": "down"}
        self.assertEqual(truth.final_basis(row), (-40.0, "25-50", "down"))
        self.assertEqual(truth.final_basis({**row, "final_settle_margin": 0.0}), (0.0, "0-5", "up"))  # a tie settles Up
        # Without the settlement columns: the point basis, where a zero margin has no side.
        self.assertEqual(truth.final_basis({**row, "final_settle_margin": None}), (3.0, "0-5", "up"))
        self.assertEqual(truth.final_basis({"final_margin": 0.0, "final_settle_margin": None}), (None, None, None))
        rows = [{**row, "window_start": BASE_WS + index * 300} for index in range(20)]
        noise = truth.oracle_noise(rows)
        self.assertEqual({bucket: (value["n"], value["disagree"]) for bucket, value in noise.items()}, {"25-50": (20, 0)})
        point = truth.oracle_noise([{**item, "final_settle_margin": None} for item in rows])
        self.assertEqual({bucket: (value["n"], value["disagree"]) for bucket, value in point.items()}, {"0-5": (20, 20)})
        # The ceiling of a cell is built on its trades' settlement buckets.
        trades = [{"window_start": BASE_WS + index * 300, "direction": "down", "entry": 0.95, "final_margin": 3.0, "final_bucket": "0-5", "final_settle_margin": -40.0, "shifted": {}} for index in range(30)]
        selection = {"model": "ladder", "rule": {"decision_second": 210, "margin_floor_usd": 100, "favorite_price_cap": 0.96, "patience_s": 0}, "trades": trades, "excluded": {}, "signal_windows": 30, "coverage": 1.0, "capacity": None}
        score = truth.score_selection(selection, {item["window_start"]: "down" for item in trades}, FEE, point)
        self.assertEqual((list(score["oracle_noise_conditional"]), score["oracle_ceiling"], score["oracle_ceiling_marginal"]), (["25-50"], 1.0, None))
        self.assertFalse(score["tripwires"]["oracle_ceiling_below_break_even"] or score["tripwires"]["wr_above_oracle_ceiling"])

    def test_event_capacity_kill_needs_seven_complete_days(self):
        rule = {"days": 7, "min_entries_per_day": 10, "min_fill_share": 0.10}

        def points(days, entries, signals=100, first_day=20000):
            return [(day * DAY_S + index * 300, index < entries) for day in range(first_day, first_day + days) for index in range(signals)]

        self.assertIsNone(truth.event_capacity_verdict(points(9, 20), None))
        # Eight data days: the first (after the cut) and the newest are partial, six complete: no verdict.
        short = truth.event_capacity_verdict(points(8, 2), rule)
        self.assertEqual((short["verdict"], short["kill"], len(short["daily"])), (None, False, 8))
        healthy = truth.event_capacity_verdict(points(9, 20), rule)
        self.assertEqual((healthy["verdict"], healthy["kill"], healthy["median_entries_per_day"], healthy["fill_share"]), ("ok", False, 20, 0.2))
        # A 7-day median under 10 entries a day kills ...
        thin = truth.event_capacity_verdict(points(9, 9, signals=40), rule)
        self.assertEqual((thin["verdict"], thin["kill"], thin["median_entries_per_day"]), ("kill", True, 9))
        # ... and so does a fill share under 0.10, however many entries.
        diluted = truth.event_capacity_verdict(points(9, 12, signals=200), rule)
        self.assertEqual((diluted["kill"], diluted["median_entries_per_day"], diluted["fill_share"]), (True, 12, 0.06))
        # Three bad days of seven do not move the median.
        mixed = points(5, 30) + points(3, 2, first_day=20005) + points(1, 30, first_day=20008)
        self.assertEqual(truth.event_capacity_verdict(mixed, rule)["kill"], False)
        # The campaign's rule also waits for 14 UTC days of data: the kill
        # is permanent and must not come before the cell could be ready.
        guarded = {**rule, "min_data_days": truth.EVENT_CAPACITY_MIN_DATA_DAYS}
        early = truth.event_capacity_verdict(points(13, 9, signals=40), guarded)
        self.assertEqual((early["verdict"], early["kill"], early["median_entries_per_day"], len(early["daily"])), (None, False, None, 13))
        self.assertEqual(truth.event_capacity_verdict(points(13, 9, signals=40), rule)["kill"], True)
        self.assertEqual(truth.event_capacity_verdict(points(14, 9, signals=40), guarded)["verdict"], "kill")
        self.assertEqual((truth.EVENT_CAPACITY_MIN_DATA_DAYS, truth.PROMOTION_MIN_DAYS), (14, 14))
        score = truth.score_selection(
            {"model": "event", "rule": event_rule(), "trades": [], "excluded": {}, "signal_windows": 0, "coverage": None, "capacity": None, "capacity_points": points(9, 9, signals=40), "capacity_rule": rule, "variants": {}},
            {},
            FEE,
            {},
        )
        self.assertEqual((score["tripwires"]["capacity_collapse"], score["killed"]), (True, True))

    def test_event_capacity_floor_is_each_cells_own(self):
        v1, v3, v5 = truth.EVENT_CAPACITY_KILL_CELLS
        # Registration freezes half of each cell's own share, never above
        # the family floor; a cell without a baseline keeps the family floor.
        rules = truth.family_rules(list(truth.FAMILY_V4.values()), None, fill_share_baseline={v1: 0.08, v5: 0.30, truth.FAMILY_V4["V2"]: 0.02})
        rule = next(item for item in rules["stopping_rules"] if item["id"] == "event_capacity")
        self.assertEqual((rule["baseline_fill_share"], rule["min_fill_share_by_cell"], rule["min_fill_share"]), ({v1: 0.08, v5: 0.30}, {v1: 0.04, v5: 0.10}, 0.10))
        self.assertEqual([truth.event_capacity_rule_for(rule, cell)["min_fill_share"] for cell in (v1, v3, v5)], [0.04, 0.10, 0.10])
        self.assertIsNone(truth.event_capacity_rule_for(rule, truth.FAMILY_V4["V2"]))
        self.assertIsNone(truth.event_capacity_rule_for(None, v1))

        def points(days, entries, signals, first_day=20000):
            return [(day * DAY_S + index * 300, index < entries) for day in range(first_day, first_day + days) for index in range(signals)]

        # 12 entries a day on 200 signal windows (0.06): under the family
        # floor, above V1's own.
        diluted = points(16, 12, 200)
        self.assertEqual(truth.event_capacity_verdict(diluted, truth.event_capacity_rule_for(rule, v3))["kill"], True)
        self.assertEqual(truth.event_capacity_verdict(diluted, truth.event_capacity_rule_for(rule, v1))["kill"], False)
        # The entries floor is unchanged by a cell's own share floor.
        self.assertEqual(truth.event_capacity_verdict(points(16, 9, 200), truth.event_capacity_rule_for(rule, v1))["kill"], True)

    def test_matched_contrast_and_max_t(self):
        spec = hand_spec()
        windows, labels = [], {}
        # Ten windows whose z reaches 3 at 161 s (the rule's), five of them
        # winners; twenty that never signal but offer the same ask in the
        # same 30 s bucket, all winners: the rule picks worse windows.
        for index in range(30):
            ws = EW + index * 300
            signal = index < 10
            closes = step_closes(ws, [(160, 10.5)] if signal else [(160, 1.0)])
            windows.append(event_window(closes, tiling(ws, "up", {}), spec, ws))
            labels[ws] = "up" if (not signal or index < 5) else "down"
        selection = truth.select_event_cell(windows, event_rule(), spec)
        self.assertEqual(len(selection["trades"]), 10)
        contrast = truth.matched_contrast(windows, selection, labels, FEE, replicates=200, seed=5)
        # One stratum (150-179 s, 0.97): 30 candidates, 25 winners.
        self.assertEqual((contrast["n"], contrast["strata"], contrast["win_rate"]), (10, 1, 0.5))
        self.assertAlmostEqual(contrast["matched_win_rate"], 25 / 30)
        self.assertAlmostEqual(contrast["win_rate_minus_matched"], 0.5 - 25 / 30)
        self.assertGreater(contrast["p_value"], 0.95)
        self.assertEqual(contrast, truth.matched_contrast(windows, selection, labels, FEE, replicates=200, seed=5))  # seeded
        self.assertIsNone(truth.matched_contrast(windows, selection, labels, FEE)["p_value"])
        empty = truth.matched_contrast(windows, truth.select_event_cell(windows, event_rule(threshold=4.0), spec), labels, FEE)
        self.assertEqual((empty["n"], empty["win_rate_minus_matched"]), (0, None))
        # max-T: a cell far above break-even is not what the null draws.
        strong = [(EW + index * 300, 0.90, True) for index in range(200)]
        null = [(EW + index * 300, 0.90, index % 10 != 0) for index in range(200)]
        family = truth.max_t_null({"strong": strong, "null": null, "empty": []}, FEE, replicates=200, seed=1)
        even = truth.break_even(0.90, FEE)
        self.assertAlmostEqual(family["statistics"]["strong"], (200 - 200 * even) / math.sqrt(200 * even * (1 - even)))
        self.assertEqual((sorted(family["statistics"]), family["max"], family["p_value"]), (["null", "strong"], family["statistics"]["strong"], 0.0))
        alone = truth.max_t_null({"null": null}, FEE, replicates=200, seed=1)
        self.assertGreater(alone["p_value"], 0.2)
        self.assertEqual(alone, truth.max_t_null({"null": null}, FEE, replicates=200, seed=1))
        self.assertIsNone(truth.max_t_null({"null": null}, FEE, replicates=0, seed=1)["p_value"])

    # --- table, registration, accrual ------------------------------------------

    BLOCK_S = 16200  # 4.5 h of closes per UTC day: two hours of EWMA warm-up, then ten windows
    FIRST_WINDOW_S = 7500

    def dense_days(self, days, first_day=0, flip=False, name="session_20260901_000000.jsonl"):
        """Per UTC day a 4.5 h block of 1 s closes (a 4 USD/s random walk)
        and a window every 15 min of its last 2.5 h, each with no-direction
        ladders at the four live anchors quoting the side the proxy settles
        at 0.97 (the other at 0.04).  Labels are that side, or its opposite
        with `flip`.  Written to the cache and the session dir; returns
        [(window_start, official)]."""
        closes = {}
        windows = []
        for day in range(first_day, first_day + days):
            rng = random.Random(1000 + day)
            start = BASE_WS + day * DAY_S
            price = 70000.0
            for ts in range(start, start + self.BLOCK_S):
                price += rng.gauss(0.0, 4.0)
                closes[ts] = price
            for index in range(10):
                windows.append(start + self.FIRST_WINDOW_S + index * 900)
        margin_dir = Path(self.directory) / "margin"
        prints_dir = Path(self.directory) / "prints"
        margin_dir.mkdir(parents=True, exist_ok=True)
        prints_dir.mkdir(parents=True, exist_ok=True)
        by_day = {}
        for ts, price in closes.items():
            by_day.setdefault(ts - ts % DAY_S, {})[str(ts)] = price
        for day, prices in by_day.items():
            (margin_dir / ("binance_%d.json" % day)).write_text(json.dumps(prices))
        outcomes_path = margin_dir / "gamma_outcomes.json"
        outcomes = json.loads(outcomes_path.read_text()) if outcomes_path.is_file() else {}
        params = settlement.DEFAULT_PARAMS
        labelled, records = [], []
        for ws in windows:
            final = settlement.final_margin(settlement.CloseSeries(closes, ws - 60, ws + 300), ws, params)
            side = "up" if final >= 0 else "down"
            official = side if not flip else ("down" if side == "up" else "up")
            labelled.append((ws, official))
            outcomes[str(ws)] = official
            for d in band.BAND_DECISION_SECONDS:
                (prints_dir / ("%d_%d.json" % (ws, d))).write_text(json.dumps(prints_row(ws, d, side, [])) + "\n")
            quotes = {side: [[0.97, 0.97, 26.0, False]] * 3, ("down" if side == "up" else "up"): [[0.04, 0.04, 600.0, False]] * 3}
            for anchor_s in (150, 180, 210, 240):
                samples = [{"t": t, "q": quotes, "c": None, "age": 0.0, "fresh": True} for t in range(31)]
                records.append(ladder(ws, anchor_s, None, None, samples))
        outcomes_path.write_text(json.dumps(outcomes))
        write_sessions(self.sessions, records, name=name)
        return labelled

    def built(self, days=8):
        labelled = self.dense_days(days)
        # Ladders at four of the six decision seconds: the other rows are built once the ladder grace has passed.
        now_ts = labelled[-1][0] + 300 + truth.LADDER_GRACE_S + 1
        cache = cache_for(self.directory)
        summary = truth.build(self.db, cache, BASE_WS, now_ts, [self.sessions], sha="event")
        self.assertEqual((summary["rows_inserted"], summary["ladder_rows_by_host"]), (len(labelled) * len(DECISIONS), {"vps": len(labelled) * 4}))
        return labelled, now_ts, cache

    def test_build_writes_the_settlement_columns_and_backfills_older_rows(self):
        labelled, now_ts, cache = self.built(days=1)
        ws = labelled[3][0]
        series = settlement.CloseSeries(cache.closes, ws - 60, ws + 300)
        params = settlement.DEFAULT_PARAMS
        rows = {row["decision_s"]: row for row in truth.load_rows(self.db) if row["window_start"] == ws}
        strike = sum(cache.closes[ts] for ts in range(ws - 60, ws)) / 60
        final = sum(cache.closes[ts] for ts in range(ws + 240, ws + 300)) / 60 - strike
        for decision_s, row in rows.items():
            self.assertAlmostEqual(row["strike_60s"], strike, places=6)
            self.assertAlmostEqual(row["final_settle_margin"], final, places=6)
            self.assertAlmostEqual(row["settle_margin"], settlement.settlement_margin(series, ws, decision_s, params), places=9)
        # At 150 s the settlement margin is the close of second 149 minus
        # the strike; the row's point margin is close(150) minus the open.
        self.assertAlmostEqual(rows[150]["settle_margin"], cache.closes[ws + 149] - strike, places=6)
        self.assertAlmostEqual(rows[150]["margin"], cache.closes[ws + 150] - cache.closes[ws], places=6)
        self.assertEqual(truth.final_basis(rows[150])[0], rows[150]["final_settle_margin"])
        # The label is the proxy's settlement side here: no oracle noise on that basis.
        self.assertEqual(sum(value["disagree"] for value in truth.oracle_noise(truth.load_rows(self.db)).values()), 0)
        # Rows built before v4 carry no settlement columns: the next build fills them from the closes.
        self.db.execute("UPDATE windows SET strike_60s = NULL, settle_margin = NULL, final_settle_margin = NULL")
        self.db.commit()
        again = truth.build(self.db, cache, BASE_WS, now_ts, [self.sessions], sha="later")
        self.assertEqual((again["rows_inserted"], again["settlement_rows_backfilled"]), (0, len(labelled) * len(DECISIONS)))
        refilled = {row["decision_s"]: row for row in truth.load_rows(self.db) if row["window_start"] == ws}
        self.assertEqual([(row["strike_60s"], row["settle_margin"], row["final_settle_margin"], row["git_sha"]) for row in refilled.values()], [(row["strike_60s"], row["settle_margin"], row["final_settle_margin"], "event") for row in rows.values()])
        self.assertEqual(truth.build(self.db, cache, BASE_WS, now_ts, [self.sessions], sha="x")["settlement_rows_backfilled"], 0)
        # A row built with its strike but before its closing minute was on
        # disk (the closes stop at ws + 250) takes the final settlement
        # margin when they arrive; the columns it has are not rewritten.
        newer = self.dense_days(1, first_day=1, name="session_20260902_000000.jsonl")
        last = newer[-1][0]
        day_file = Path(self.directory) / "margin" / ("binance_%d.json" % (last - last % DAY_S))
        whole = json.loads(day_file.read_text())
        day_file.write_text(json.dumps({ts: price for ts, price in whole.items() if int(ts) < last + 250}))
        later = last + 300 + truth.LADDER_GRACE_S + 1
        short = truth.build(self.db, cache_for(self.directory), BASE_WS, later, [self.sessions], sha="short")
        self.assertEqual(short["rows_inserted"], len(newer) * len(DECISIONS))
        cut = {row["decision_s"]: row for row in truth.load_rows(self.db) if row["window_start"] == last}
        self.assertEqual(({row["final_settle_margin"] for row in cut.values()}, all(row["strike_60s"] is not None and row["settle_margin"] is not None for row in cut.values())), ({None}, True))
        self.assertEqual(truth.final_basis(cut[240]), (None, None, None))  # out of oracle noise and the ceiling
        self.assertEqual(truth.build(self.db, cache_for(self.directory), BASE_WS, later, [self.sessions], sha="x")["settlement_rows_backfilled"], 0)
        day_file.write_text(json.dumps(whole))
        arrived = truth.build(self.db, cache_for(self.directory), BASE_WS, later, [self.sessions], sha="x")
        self.assertEqual((arrived["rows_inserted"], arrived["settlement_rows_backfilled"]), (0, len(DECISIONS)))
        mended = {row["decision_s"]: row for row in truth.load_rows(self.db) if row["window_start"] == last}
        closing = sum(float(whole[str(ts)]) for ts in range(last + 240, last + 300)) / 60
        for decision_s, row in mended.items():
            self.assertAlmostEqual(row["final_settle_margin"], closing - row["strike_60s"], places=6)
            self.assertEqual((row["strike_60s"], row["settle_margin"], row["git_sha"]), (cut[decision_s]["strike_60s"], cut[decision_s]["settle_margin"], "short"))
        self.assertEqual(truth.build(self.db, cache_for(self.directory), BASE_WS, later, [self.sessions], sha="x")["settlement_rows_backfilled"], 0)

    def register_v4(self, labelled, now_ts, cache, campaign_id="2026-10_band_event_v4"):
        campaigns = Path(self.directory) / "campaigns"
        registered = truth.register(self.db, campaign_id, campaigns, now_ts, cells=["V1..V6"], closes=cache.closes)
        self.assertTrue(registered["registered"], registered)
        return campaigns, registered

    def test_register_freezes_the_spec_and_records_section_6(self):
        labelled, now_ts, cache = self.built()
        campaigns, registered = self.register_v4(labelled, now_ts, cache)
        family = list(truth.FAMILY_V4.values())
        self.assertEqual((registered["family_size"], registered["cells"]), (6, family))
        campaign = json.loads((campaigns / "2026-10_band_event_v4.json").read_text())
        spec = campaign["spec"]
        # The spec: fitted on every labelled window (all ended before
        # registered_at), frozen with its hash.
        settlement.verify_spec(spec)
        after = band.last_eligible_window_start(now_ts)  # later than the newest row: the clock is the cut
        self.assertEqual((spec["sha256"], spec["cut_ts"], spec["fit"]["windows"], spec["fit"]["last_window_start"]), (registered["spec_sha256"], after + 300, len(labelled), labelled[-1][0]))
        self.assertEqual((spec["params"], spec["fee_rate"], spec["fit"]["c"]["source"], spec["fit"]["s_b"]["source"]), (json.loads(json.dumps(settlement.DEFAULT_PARAMS)), FEE, "fit", "fit"))
        self.assertTrue(0.5 < spec["c"] < 2.0, spec["c"])
        self.assertEqual(sorted((row["z_min"], row["t_low"], row["t_high"]) for row in spec["first_crossing"]), [(2.5, 150, 270), (2.5, 210, 270), (3.0, 150, 270)])
        # Deterministic: the same table and clock freeze the same spec.
        _, twin = self.register_v4(labelled, now_ts, cache, "2026-10_twin")
        self.assertEqual(twin["spec_sha256"], registered["spec_sha256"])
        # The family: N = 6, VPS only, each event cell bound to the spec.
        self.assertEqual((campaign["family_size"], campaign["ladder_hosts"], campaign["registered_after_window_start"]), (6, ["vps"], after))
        self.assertEqual([(cell["name"], cell["cell_id"], cell["grammar"]) for cell in campaign["cells"]], [("V%d" % (index + 1), identity, "band_event_v4" if index < 5 else truth.GRAMMAR_VERSION) for index, identity in enumerate(family)])
        for cell in campaign["cells"][:5]:
            self.assertEqual(cell["fingerprint"], truth.event_fingerprint(cell["rule"], spec["sha256"]))
            self.assertNotEqual(cell["fingerprint"], truth.event_fingerprint(cell["rule"], "another spec"))
            self.assertEqual(cell["rule"], truth.event_rule_from_cell_id(cell["cell_id"]))
        self.assertGreater(campaign["cells"][4]["at_registration"]["n"], 0)
        # Section 6, machine-checkable.
        rules = {rule["id"]: rule for rule in campaign["stopping_rules"]}
        self.assertEqual(list(rules), ["futility", "event_capacity", "day45_fresh_net", "day90_no_discovery"])
        self.assertEqual((rules["futility"]["value"], rules["futility"]["cells"]), (0.1, family))
        self.assertEqual({key: rules["event_capacity"][key] for key in ("cells", "days", "min_entries_per_day", "min_fill_share", "min_data_days", "action")}, {"cells": [family[0], family[2], family[4]], "days": 7, "min_entries_per_day": 10, "min_fill_share": 0.10, "min_data_days": 14, "action": "kill_cell"})
        self.assertEqual({key: rules["day45_fresh_net"][key] for key in ("at_day", "cells", "metric", "op", "value", "action")}, {"at_day": 45, "cells": [family[0], family[4]], "metric": "mean_net_per_usd", "op": "<=", "value": 0.0, "action": "stop_family"})
        self.assertEqual((rules["day90_no_discovery"]["at_day"], rules["day90_no_discovery"]["metric"]), (90, "e_bh_discoveries"))
        falsifiers = {rule["id"]: rule for rule in campaign["falsifiers"]}
        self.assertEqual(list(falsifiers), ["F1_z_adds_nothing", "F2_calibration_broke", "F3_adaptive_dead"])
        self.assertEqual((falsifiers["F1_z_adds_nothing"]["at_day"], falsifiers["F1_z_adds_nothing"]["cell"]), (30, family[4]))
        crossing = next(row for row in spec["first_crossing"] if (row["z_min"], row["t_low"]) == (2.5, 150))
        self.assertEqual((falsifiers["F2_calibration_broke"]["at_n"], falsifiers["F2_calibration_broke"]["value"], falsifiers["F2_calibration_broke"]["crossing"]), (2000, crossing["rate"], [2.5, 150, 270]))
        # This table has no window before its first VPS ladder, so F2's bar falls back to the frozen fit's own crossing, and says so.
        self.assertEqual(falsifiers["F2_calibration_broke"]["reference"], {"windows": "fit", "cut_ts": spec["cut_ts"], "n": crossing["n"], "wins": crossing["wins"], "rate": crossing["rate"], "wilson_lower": crossing["wilson_lower"]})
        self.assertEqual(campaign["falsifier_cleared"], {})
        self.assertEqual({key: falsifiers["F3_adaptive_dead"][key] for key in ("at_day", "cell", "leader", "op", "value")}, {"at_day": 45, "cell": family[0], "leader": family[5], "op": "<", "value": 1.0})
        self.assertEqual({rule["action"] for rule in campaign["falsifiers"]}, {"hold_event_cells"})
        paired = campaign["paired_controls"]
        self.assertEqual((paired["k"], paired["bar_e"], paired["min_paired_windows"]), (3, 60.0, 100))
        self.assertEqual([(pair["cell_id"], pair["control_id"]) for pair in paired["pairs"]], [(family[0], "x150-270_m100_c0.99"), (family[0], "x150-270_s3.0_c0.99"), (family[2], "x210-270_m100_c0.99"), (family[4], "x150-270_m100_c0.99"), (family[4], "x150-270_s3.0_c0.99")])
        self.assertEqual(len({pair["fingerprint"] for pair in paired["pairs"]} | {cell["fingerprint"] for cell in campaign["cells"]}), 11)
        # A subset family carries only the entries its cells are in.
        subset = truth.family_rules([family[1], family[5]], spec)
        self.assertEqual(([rule["id"] for rule in subset["stopping_rules"]], [rule["id"] for rule in subset["falsifiers"]], subset["paired_controls"]["pairs"], subset["paired_controls"]["bar_e"]), (["futility", "day90_no_discovery"], ["F2_calibration_broke"], [], None))
        # F2's reference is the frozen rule's pre-ladder accuracy (section
        # 6; its Wilson lower bound, the doc's 0.9897, stays on record),
        # given enough crossings before the first evidence ladder.  The
        # look compares the fresh sample's Wilson upper bound with that
        # rate: a fresh point rate under 0.9897 fires on about 12% of
        # healthy samples at n = 2,000.
        pre_ladder = {"n": 6925, "wins": 6870, "rate": 6870 / 6925, "wilson_lower": band.wilson_lower(6870, 6925), "cut_ts": 1789795500}
        f2 = truth.family_rules(family, spec, pre_ladder=pre_ladder)["falsifiers"][1]
        self.assertEqual((f2["id"], f2["statistic"], f2["value"], f2["reference"]), ("F2_calibration_broke", "wilson_upper", 6870 / 6925, {"windows": "pre_ladder", "cut_ts": 1789795500, "n": 6925, "wins": 6870, "rate": 6870 / 6925, "wilson_lower": pre_ladder["wilson_lower"]}))
        self.assertAlmostEqual(f2["reference"]["wilson_lower"], 0.9897, places=4)
        self.assertIn("before the first evidence ladder", f2["text"])
        # 1,984 of 2,000 (0.992) is the frozen rate itself; 1,978 (0.989)
        # sits under the old bar yet its upper bound 0.9927 still holds
        # the rate; 1,970 (0.985, upper 0.9895) is a broken calibration.
        fired = lambda wins: truth._compare(truth.crossing_statistic(f2, {"n": 2000, "wins": wins, "rate": wins / 2000}), f2["op"], f2["value"])
        self.assertEqual([fired(1984), fired(1978), fired(1970)], [False, False, True])
        self.assertEqual(truth.crossing_statistic({}, {"n": 2000, "wins": 1970, "rate": 0.985}), 0.985)
        thin = truth.family_rules(family, spec, pre_ladder={**pre_ladder, "n": truth.FALSIFIER_CROSSING_REFERENCE_MIN_N - 1})["falsifiers"][1]
        self.assertEqual((thin["value"], thin["reference"]["windows"]), (crossing["rate"], "fit"))
        # register measures that reference with the frozen spec on the
        # windows that ended by the first VPS ladder window (here: day 0,
        # its ladders dropped from the table; ten windows are enough for
        # this fixture only).
        first_ladder = labelled[10][0]
        self.db.execute("UPDATE windows SET ladder_json = NULL, ladder_host = NULL, ladder_basis = NULL WHERE window_start < ?", (first_ladder,))
        self.db.commit()
        with mock.patch.object(truth, "FALSIFIER_CROSSING_REFERENCE_MIN_N", 1):
            _, early = self.register_v4(labelled, now_ts, cache, "2026-10_pre_ladder")
        self.assertEqual(early["spec_sha256"], registered["spec_sha256"])  # the spec is fitted on every labelled window either way
        measured = settlement.first_crossing(truth.event_series(cache.closes, [ws for ws, _ in labelled], spec), labelled[:10], spec, 2.5, 150, 270)
        self.assertGreaterEqual(measured["n"], 1)
        f2 = {rule["id"]: rule for rule in json.loads((campaigns / "2026-10_pre_ladder.json").read_text())["falsifiers"]}["F2_calibration_broke"]
        self.assertEqual((f2["value"], f2["reference"]), (measured["rate"], {"windows": "pre_ladder", "cut_ts": first_ladder, "n": measured["n"], "wins": measured["wins"], "rate": measured["rate"], "wilson_lower": measured["wilson_lower"]}))
        # The CLI path freezes the same spec from the cache on disk.
        config_path = Path(self.directory) / "loop.json"
        config_path.write_text(json.dumps(loop_config(self.directory)))
        argv = ["--campaigns-dir", str(campaigns), "--db", str(Path(self.directory) / "windows.sqlite3"), "--loop-config", str(config_path), "--now-ts", str(now_ts), "--start-ts", str(BASE_WS)]
        on_disk = cache_for(self.directory)
        with mock.patch.object(truth.band_lane, "BandCache", lambda: on_disk), contextlib.redirect_stdout(io.StringIO()) as printed:
            self.assertEqual(truth.main(["--register", "2026-10_cli", "--cells", "V1..V6"] + argv), 0)
        self.assertEqual(json.loads(printed.getvalue())["spec_sha256"], registered["spec_sha256"])

    def test_tick_accrues_event_cells_with_the_frozen_spec_only(self):
        labelled, now_ts, cache = self.built()
        campaigns, registered = self.register_v4(labelled, now_ts, cache)
        path = campaigns / "2026-10_band_event_v4.json"
        frozen = path.read_text()
        campaign = json.loads(frozen)
        config = loop_config(self.directory)
        idle = truth.tick(self.db, cache_for(self.directory), BASE_WS, now_ts, config, [self.sessions], campaigns)["campaigns"][0]
        self.assertEqual(({cell["n"] for cell in idle["cells"]}, idle["paired_controls"][0]["paired_windows"], idle["spec_sha256"]), ({0}, 0, registered["spec_sha256"]))
        # Two more days arrive.
        fresh = self.dense_days(2, first_day=8, name="session_20260909_000000.jsonl")
        later = fresh[-1][0] + 300 + truth.LADDER_GRACE_S + 1
        ticked = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)
        summary = ticked["campaigns"][0]
        self.assertEqual((ticked["build"]["rows_inserted"], summary["spec_sha256"], summary["registration_cut"]), (len(fresh) * len(DECISIONS), registered["spec_sha256"], now_ts - 1))
        # The campaign file is untouched: nothing was refitted on the new windows.
        self.assertEqual(path.read_text(), frozen)
        # Each event cell's accrual is the replay of the fresh windows under the frozen spec.
        fresh_cache = cache_for(self.directory)
        fresh_cache.load_closes(BASE_WS, later, later, fetch=False)
        rows = [row for row in truth.load_rows(self.db) if row["window_start"] > now_ts - 1]
        windows = truth.event_windows(rows, truth.event_series(fresh_cache.closes, [row["window_start"] for row in rows], campaign["spec"]), campaign["spec"], ("vps",))
        self.assertEqual(len(windows), len(fresh))
        by_id = {cell["cell_id"]: cell for cell in summary["cells"]}
        for cell in campaign["cells"][:5]:
            selection = truth.select_event_cell(windows, cell["rule"], campaign["spec"])
            report = by_id[cell["cell_id"]]
            self.assertEqual((report["n"], report["applied"], report["fingerprint"]), (len(selection["trades"]), len(selection["trades"]), cell["fingerprint"]))
            self.assertEqual(sorted(truth.accrued_windows(self.db, campaign["id"], cell["fingerprint"])), [trade["window_start"] for trade in selection["trades"]])
            self.assertEqual(sorted(report["latency"]), ["lag0", "lag2", "older1"])
        volume = by_id[truth.FAMILY_V4["V5"]]
        # The ladders quote the settling side at 0.97: every entry wins at BE(0.97).
        self.assertGreaterEqual(volume["n"], 5)
        self.assertEqual((volume["wins"], volume["status"], volume["ready"]), (volume["n"], "accruing", False))
        process = truth.evidence_accrual.EProcess()
        for _ in range(volume["n"]):
            process.update(be(0.97), True)
        self.assertAlmostEqual(volume["e_value"], process.e_value())
        self.assertGreaterEqual(volume["n"], by_id[truth.FAMILY_V4["V4"]]["n"])  # z >= 3 trades a subset of z >= 2.5 at the same cap
        self.assertEqual(summary["e_bh"]["campaign_n"], 6)
        # The capacity kill is registered for V1, V3 and V5 only; two days give no verdict.
        self.assertEqual([by_id[identity]["capacity_trend"] is not None for identity in truth.FAMILY_V4.values()], [True, False, True, False, True, True])
        self.assertEqual((volume["capacity_trend"]["verdict"], volume["capacity_trend"]["rule"]["min_entries_per_day"], volume["tripwires"]["capacity_collapse"]), (None, 10, False))
        # Paired controls (outside N): one e-process per pair on the windows either side traded.
        self.assertEqual(len(summary["paired_controls"]), 5)
        pair = summary["paired_controls"][3]
        control = truth.select_event_cell(windows, truth.control_rule_from_id("x150-270_m100_c0.99"), campaign["spec"])
        ours = truth.select_event_cell(windows, truth.event_rule_from_cell_id(truth.FAMILY_V4["V5"]), campaign["spec"])
        either = {trade["window_start"] for trade in control["trades"]} | {trade["window_start"] for trade in ours["trades"]}
        self.assertEqual((pair["cell_id"], pair["control_id"], pair["paired_windows"], pair["applied"], pair["status"], pair["bar_e"]), (truth.FAMILY_V4["V5"], "x150-270_m100_c0.99", len(either), len(either), "paired_accruing", 60.0))
        self.assertAlmostEqual(pair["d_scale"], 1.0 / be(0.80))
        self.assertEqual(self.db.execute("SELECT status, n FROM campaign_accrual WHERE fingerprint = ?", (pair["fingerprint"],)).fetchone()[:], ("paired_accruing", len(either)))
        # Section 6's looks are not due two days in.
        self.assertEqual({identity: (check["evaluated"], check["fired"]) for identity, check in summary["checks"].items()}, {identity: (False, False) for identity in ("day45_fresh_net", "day90_no_discovery", "F1_z_adds_nothing", "F2_calibration_broke", "F3_adaptive_dead")})
        self.assertEqual((summary["family_stopped"], summary["falsified"]), (None, []))
        self.assertIsNone(truth.get_meta(self.db, "campaign_checks:2026-10_band_event_v4") or None)
        # A second tick folds nothing twice.
        again = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)["campaigns"][0]
        self.assertEqual(({cell["applied"] for cell in again["cells"]}, {pair["applied"] for pair in again["paired_controls"]}), ({0}, {0}))
        self.assertEqual({cell["cell_id"]: cell["n"] for cell in again["cells"]}, {cell["cell_id"]: cell["n"] for cell in summary["cells"]})
        # No engine policy for an event cell: no promotion artifact; the static member has one.
        with self.assertRaises(ValueError):
            truth.gate_artifact(self.db, campaign, truth.FAMILY_V4["V1"])
        self.assertEqual(truth.gate_artifact(self.db, campaign, truth.FAMILY_V4["V6"])["verdict"], "INSUFFICIENT")
        # The event grid on the campaign: the same fresh windows, the same frozen spec.
        report = truth.event_grid(self.db, fresh_cache.closes, [cell["cell_id"] for cell in campaign["cells"]], campaign, replicates=20, seed=2)
        self.assertEqual((report["mode"], report["covered_windows"], report["spec"]["sha256"], report["after_window_start"]), ("campaign:2026-10_band_event_v4", len(fresh), registered["spec_sha256"], now_ts - 1))
        self.assertEqual([(cell["cell_id"], cell["score"]["n"]) for cell in report["cells"]], [(cell["cell_id"], cell["n"]) for cell in summary["cells"]])
        v5 = report["cells"][4]
        self.assertAlmostEqual(v5["entries_per_day"], v5["score"]["n"] / (len(fresh) / 288.0))
        self.assertEqual((v5["first_crossing"]["n"], sum(day["entries"] for day in v5["capacity_daily"]), v5["matched"]["n"], v5["matched"]["replicates"]), (len(ours["signals"]), v5["score"]["n"], v5["score"]["n"], 20))
        self.assertEqual((report["cells"][5]["matched"], report["max_t"]["replicates"], sorted(report["max_t"]["statistics"]) == sorted(cell["cell_id"] for cell in report["cells"] if cell["score"]["n"])), (None, 20, True))
        text = truth.event_grid_text(report)
        self.assertIn("e150-270_z2.5_c0.99: %d/%d" % (v5["score"]["wins"], v5["score"]["n"]), text)
        self.assertIn("max-T against break-even over the family", text)
        # Discovery mode: a spec fitted before a cut, scored after it; never a campaign's.
        cut = BASE_WS + 5 * DAY_S
        discovery = truth.event_grid(self.db, fresh_cache.closes, ["V5", "x150-270_m100_c0.99"][:1], cut_ts=cut, s_b=3.5)
        self.assertEqual((discovery["mode"], discovery["spec"]["cut_ts"], discovery["spec"]["fit"]["windows"], discovery["spec"]["s_b"], discovery["covered_windows"]), ("discovery", cut, 50, 3.5, 50))
        self.assertNotEqual(discovery["spec"]["sha256"], registered["spec_sha256"])
        with self.assertRaises(ValueError):
            truth.event_grid(self.db, fresh_cache.closes, ["V5"])  # no labelled window ends before the first ladder window
        # A frozen spec edited after the fact scores nothing: the campaign is held like a stale one.
        tampered = json.loads(frozen)
        tampered["spec"]["c"] *= 2.0
        path.write_text(json.dumps(tampered, indent=2, sort_keys=True) + "\n")
        more = self.dense_days(1, first_day=10, name="session_20260911_000000.jsonl")
        newest = more[-1][0] + 300 + truth.LADDER_GRACE_S + 1
        held = truth.tick(self.db, cache_for(self.directory), BASE_WS, newest, config, [self.sessions], campaigns)["campaigns"][0]
        self.assertIn("sha256 does not match", held["stale_evaluator"])
        self.assertEqual({(cell["status"], cell["reason"], cell["applied"]) for cell in held["cells"]}, {("manual_audit", "stale_evaluator", 0)})
        self.assertEqual({cell["cell_id"]: cell["n"] for cell in held["cells"]}, {cell["cell_id"]: cell["n"] for cell in summary["cells"]})
        with self.assertRaises(ValueError):
            truth.event_grid(self.db, fresh_cache.closes, ["V5"], tampered)
        # Restored, it accrues the day it missed.
        path.write_text(frozen)
        resumed = truth.tick(self.db, cache_for(self.directory), BASE_WS, newest, config, [self.sessions], campaigns)["campaigns"][0]
        self.assertNotIn("stale_evaluator", resumed)
        self.assertGreater({cell["cell_id"]: cell["n"] for cell in resumed["cells"]}[truth.FAMILY_V4["V5"]], volume["n"])

    def aged(self, path, days):
        """Move a campaign's registration clock `days` back (its cut, the
        newest row at registration, stays): the data clock reads that day."""
        campaign = json.loads(path.read_text())
        campaign["registered_at_ts"] = campaign["registered_after_window_start"] - int(days * DAY_S)
        path.write_text(json.dumps(campaign, indent=2, sort_keys=True) + "\n")
        return campaign

    def test_section_6_looks_are_taken_once_and_act(self):
        labelled, now_ts, cache = self.built()
        campaigns, registered = self.register_v4(labelled, now_ts, cache)
        path = campaigns / "2026-10_band_event_v4.json"
        config = loop_config(self.directory)
        fresh = self.dense_days(2, first_day=8, name="session_20260909_000000.jsonl")
        later = fresh[-1][0] + 300 + truth.LADDER_GRACE_S + 1
        family = truth.FAMILY_V4
        # Day 31: the F1 look.  Every candidate at 0.97 wins here, so the
        # z-selected rate is the matched rate: z adds nothing, the event
        # cells are held; the static member keeps accruing.
        self.aged(path, 29)
        summary = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)["campaigns"][0]
        checks = summary["checks"]
        self.assertEqual((checks["F1_z_adds_nothing"]["evaluated"], checks["F1_z_adds_nothing"]["fired"], checks["F1_z_adds_nothing"]["value"]), (True, True, 0.0))
        self.assertTrue(30 < checks["F1_z_adds_nothing"]["day"] < 32)
        self.assertEqual({identity: checks[identity]["evaluated"] for identity in ("day45_fresh_net", "day90_no_discovery", "F2_calibration_broke", "F3_adaptive_dead")}, {"day45_fresh_net": False, "day90_no_discovery": False, "F2_calibration_broke": False, "F3_adaptive_dead": False})
        self.assertEqual((summary["falsified"], summary["family_stopped"]), (["F1_z_adds_nothing"], None))
        by_id = {cell["cell_id"]: cell for cell in summary["cells"]}
        self.assertEqual([by_id[identity]["status"] for identity in family.values()], ["manual_audit"] * 5 + ["accruing"])
        self.assertLess(checks["F2_calibration_broke"]["n"], 2000)
        # The look is recorded and never repeated: the stored entry stands.
        stored = truth.get_meta(self.db, "campaign_checks:2026-10_band_event_v4")
        self.assertEqual(list(stored), ["F1_z_adds_nothing"])
        # Day 46: the fresh-net stop (V1 and V5 are up: not fired) and F3
        # (V1's paired e against its controls, and whether V6 leads).
        self.aged(path, 44)
        summary = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)["campaigns"][0]
        checks = summary["checks"]
        self.assertEqual(checks["F1_z_adds_nothing"], stored["F1_z_adds_nothing"])
        self.assertEqual((checks["day45_fresh_net"]["evaluated"], checks["day45_fresh_net"]["fired"]), (True, False))
        # V1's schedule caps come from an 80-window table, too thin for a
        # tick at 0.97: it never trades here (no net); V5's net is positive.
        self.assertEqual((checks["day45_fresh_net"]["value"][0], checks["day45_fresh_net"]["value"][1] > 0), (None, True))
        f3 = checks["F3_adaptive_dead"]
        self.assertEqual((f3["evaluated"], f3["fired"]), (True, f3["value"] < 1.0 and f3["leader_e"] > f3["cell_e"]))
        self.assertEqual(f3["value"], max(pair["e_value"] for pair in summary["paired_controls"] if pair["cell_id"] == family["V1"]))
        self.assertIsNone(summary["family_stopped"])
        # Day 91 without an e-BH discovery: the family stops, for good.
        self.aged(path, 89)
        summary = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)["campaigns"][0]
        self.assertEqual((summary["checks"]["day90_no_discovery"]["fired"], summary["checks"]["day90_no_discovery"]["value"], summary["family_stopped"]), (True, 0, "day90_no_discovery"))
        self.assertEqual({cell["status"] for cell in summary["cells"]}, {"killed_day90_no_discovery"})
        again = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)["campaigns"][0]
        self.assertEqual(({cell["status"] for cell in again["cells"]}, again["checks"]), ({"killed_day90_no_discovery"}, summary["checks"]))
        self.assertEqual(again["discoveries"], {})

    def force_e(self, campaign, cell_id, e):
        """Seed a registered cell's e-process at e (every lambda's wealth at e)."""
        fp = next(cell["fingerprint"] for cell in campaign["cells"] if cell["cell_id"] == cell_id)
        state = json.dumps({"log_wealth": [math.log(e)] * 20, "n": 150})
        self.db.execute("UPDATE campaign_accrual SET state_json = ? WHERE campaign_id = ? AND fingerprint = ?", (state, campaign["id"], fp))
        self.db.commit()

    def decided(self, cells, held=()):
        """score_selection with the named cells ready for the promotion
        decision (the two-day fixture has neither n >= 100 nor 14 days);
        those in `held` carry an uncleared defect.  The e-BH step, the
        looks and the statuses under test are the real code."""
        real = truth.score_selection

        def scored(*args, **kwargs):
            score = real(*args, **kwargs)
            if score["cell_id"] in cells:
                defects = ["halves_below_break_even"] if score["cell_id"] in held else []
                score.update({"ready": True, "defects": defects, "held": bool(defects), "killed": False, "promotable": not defects})
            return score

        return mock.patch.object(truth, "score_selection", scored)

    def falsified_campaign(self):
        """Family v4 two fresh days in, at day 31: F1 has fired (see
        test_section_6_looks_are_taken_once_and_act)."""
        labelled, now_ts, cache = self.built()
        campaigns, _ = self.register_v4(labelled, now_ts, cache)
        path = campaigns / "2026-10_band_event_v4.json"
        fresh = self.dense_days(2, first_day=8, name="session_20260909_000000.jsonl")
        later = fresh[-1][0] + 300 + truth.LADDER_GRACE_S + 1
        config = loop_config(self.directory)

        def tick():
            return truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)["campaigns"][0]

        return path, campaigns, tick

    def test_a_falsifier_hold_leaves_the_e_bh_candidate_set(self):
        path, campaigns, tick = self.falsified_campaign()
        family = truth.FAMILY_V4
        v5, v6 = family["V5"], family["V6"]
        campaign = self.aged(path, 29)
        self.assertEqual(tick()["falsified"], ["F1_z_adds_nothing"])
        # V5 (held by F1) at e = 70, V6 at e = 65.  Over both, e-BH finds
        # k* = 2 at a bar of 60 and V6 passes its gate at about half of
        # what a lone discovery needs (N / alpha = 120).  A held cell is
        # out of the candidate set: V6 stands alone and keeps accruing.
        self.force_e(campaign, v5, 70.0)
        self.force_e(campaign, v6, 65.0)
        self.assertEqual(truth.evidence_accrual.e_bh({"v5": 70.0, "v6": 65.0}, 6, 0.05, family_size=6)["k_star"], 2)
        with self.decided({v5, v6}):
            summary = tick()
            by_id = {cell["cell_id"]: cell for cell in summary["cells"]}
            self.assertEqual((summary["falsified"], summary["e_bh"]["candidates"], summary["e_bh"]["k_star"], summary["e_bh"]["threshold"]), (["F1_z_adds_nothing"], 1, 0, 120.0))
            self.assertEqual((by_id[v5]["status"], by_id[v6]["status"], summary["discoveries"]), ("manual_audit", "accruing", {}))
            self.assertEqual(truth.gate_artifact(self.db, campaign, v6)["verdict"], "INSUFFICIENT")
            # ... the held cells' own e-values do not help it either, however many clear the bar ...
            for name in ("V1", "V2", "V3", "V4"):
                self.force_e(campaign, family[name], 500.0)
            with self.decided(set(family.values())):
                summary = tick()
            self.assertEqual((summary["e_bh"]["k_star"], {cell["cell_id"]: cell["status"] for cell in summary["cells"]}[v6]), (0, "accruing"))
            # ... and at the lone bar V6 is a discovery on its own.
            self.force_e(campaign, v6, 125.0)
            summary = tick()
            by_id = {cell["cell_id"]: cell for cell in summary["cells"]}
            self.assertEqual((summary["e_bh"]["k_star"], by_id[v5]["status"], by_id[v6]["status"], list(summary["discoveries"])), (1, "manual_audit", "promote_candidate", [v6]))
            self.assertEqual(truth.gate_artifact(self.db, campaign, v6)["verdict"], "PASS")

    def test_clear_falsifier_records_the_audit_of_a_fired_look(self):
        path, campaigns, tick = self.falsified_campaign()
        family = truth.FAMILY_V4
        v5, v6 = family["V5"], family["V6"]
        campaign = self.aged(path, 29)
        fired = tick()
        self.assertEqual(fired["falsified"], ["F1_z_adds_nothing"])
        look = fired["checks"]["F1_z_adds_nothing"]
        # Only a falsifier of the campaign whose look has fired, with a note; never a family stop.
        for identity, note in (("F9_unknown", "note"), ("day90_no_discovery", "note"), ("F2_calibration_broke", "note"), ("F1_z_adds_nothing", " ")):
            with self.assertRaises(ValueError):
                truth.clear_falsifier(self.db, campaigns, campaign["id"], identity, note)
        with self.assertRaises(ValueError):  # the defect path does not take a falsifier
            truth.clear_audit(campaigns, campaign["id"], v5, ["F1_z_adds_nothing"], "note")
        cleared = truth.clear_falsifier(self.db, campaigns, campaign["id"], "F1_z_adds_nothing", "every candidate in the fixture wins: the contrast is degenerate", 1790000000)
        self.assertEqual((cleared["cleared"], cleared["falsifier"], cleared["look"]), (True, "F1_z_adds_nothing", look))
        campaign = json.loads(path.read_text())
        self.assertEqual(campaign["falsifier_cleared"], {"F1_z_adds_nothing": {"note": "every candidate in the fixture wins: the contrast is degenerate", "cleared_at": truth._iso(1790000000), "look": look}})
        self.assertEqual(truth.cleared_falsifiers(campaign), ("F1_z_adds_nothing",))
        self.assertIsNone(truth.campaign_version_error(campaign))
        # The look stays recorded, fired; the hold is released and the event cells are candidates again.
        self.force_e(campaign, v5, 70.0)
        self.force_e(campaign, v6, 65.0)
        with self.decided({v5, v6}):
            summary = tick()
        by_id = {cell["cell_id"]: cell for cell in summary["cells"]}
        self.assertEqual((summary["falsified"], summary["falsifier_cleared"], summary["checks"]["F1_z_adds_nothing"]), ([], ["F1_z_adds_nothing"], look))
        self.assertEqual((summary["e_bh"]["k_star"], summary["e_bh"]["threshold"], by_id[v5]["status"], by_id[v6]["status"]), (2, 60.0, "promote_candidate", "promote_candidate"))
        self.assertEqual([by_id[family[name]]["status"] for name in ("V1", "V2", "V3", "V4")], ["accruing"] * 4)
        # The CLI path: exactly one active campaign and a note.
        config_path = Path(self.directory) / "loop.json"
        config_path.write_text(json.dumps(loop_config(self.directory)))
        argv = ["--campaigns-dir", str(campaigns), "--db", str(Path(self.directory) / "windows.sqlite3"), "--loop-config", str(config_path), "--now-ts", "1790000300"]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(truth.main(["--clear-falsifier", "F1_z_adds_nothing", "--note", "re-audited"] + argv), 0)
        self.assertEqual(json.loads(path.read_text())["falsifier_cleared"]["F1_z_adds_nothing"]["note"], "re-audited")
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            truth.main(["--clear-falsifier", "F1_z_adds_nothing"] + argv)

    def test_day_90_reads_the_discoveries_so_far_not_one_tick(self):
        path, campaigns, tick = self.falsified_campaign()
        family = truth.FAMILY_V4
        v5, v6 = family["V5"], family["V6"]
        # Day 70: V6 clears the lone bar, held for a defect audit at that
        # tick.  The discovery is recorded: it is valid when it is made.
        campaign = self.aged(path, 68)
        tick()
        self.force_e(campaign, v6, 125.0)
        with self.decided({v6}, held={v6}):
            day70 = tick()
        by_id = {cell["cell_id"]: cell for cell in day70["cells"]}
        self.assertEqual((day70["e_bh"]["k_star"], by_id[v6]["status"], day70["checks"]["day90_no_discovery"]["evaluated"]), (0, "manual_audit", False))
        self.assertEqual((list(day70["discoveries"]), day70["discoveries"][v6]["held"], day70["discoveries"][v6]["e_value"]), ([v6], True, by_id[v6]["e_value"]))
        # One loss at a 0.98 entry later the e-value is back under the bar
        # (and far above futility): this tick's k* is 0.
        self.force_e(campaign, v6, 18.0)
        self.aged(path, 89)
        with self.decided({v6}):
            day91 = tick()
        check = day91["checks"]["day90_no_discovery"]
        self.assertEqual((day91["e_bh"]["k_star"], check["evaluated"], check["fired"], check["value"], day91["family_stopped"]), (0, True, False, 1, None))
        self.assertEqual({cell["cell_id"]: cell["status"] for cell in day91["cells"]}[v6], "accruing")
        self.assertEqual(day91["discoveries"], day70["discoveries"])
        self.assertFalse(any(cell["status"].startswith("killed") for cell in day91["cells"]))

    def test_day_90_does_not_count_a_cell_a_falsifier_holds(self):
        # F1 has fired and nothing else is above the bar: an event cell at
        # e = 130, held and never promotable, is no discovery and does not
        # keep the family alive at day 90.
        path, campaigns, tick = self.falsified_campaign()
        v5, v6 = truth.FAMILY_V4["V5"], truth.FAMILY_V4["V6"]
        campaign = self.aged(path, 29)
        self.assertEqual(tick()["falsified"], ["F1_z_adds_nothing"])
        self.force_e(campaign, v5, 130.0)
        self.aged(path, 89)
        with self.decided({v5, v6}):
            stopped = tick()
        check = stopped["checks"]["day90_no_discovery"]
        self.assertEqual((stopped["discoveries"], check["fired"], check["value"], stopped["family_stopped"]), ({}, True, 0, "day90_no_discovery"))
        self.assertEqual({cell["status"] for cell in stopped["cells"]}, {"killed_day90_no_discovery"})

    def test_a_pair_folds_a_window_only_when_its_ladders_are_complete(self):
        labelled, now_ts, cache = self.built()
        campaigns, _ = self.register_v4(labelled, now_ts, cache)
        campaign = json.loads((campaigns / "2026-10_band_event_v4.json").read_text())
        config = loop_config(self.directory)
        name = "session_20260909_000000.jsonl"
        fresh = self.dense_days(2, first_day=8, name=name)
        session = self.sessions / name
        whole = session.read_text()
        # A pull that landed mid-window and a next one that failed: of the
        # newest seven windows (all inside the ladder grace at the tick)
        # only the 150 s ladder is in the mirror.
        partial = {"%016x" % ws for ws, _ in fresh[-7:]}

        def withheld(line):
            if '"band_ladder"' not in line:
                return False
            record = json.loads(line)
            return record["cid"] in partial and record["anchor_s"] != 150

        session.write_text("\n".join(line for line in whole.splitlines() if not withheld(line)) + "\n")
        soon = fresh[-1][0] + 300 + band.RESOLUTION_LAG_S + 1
        first = truth.tick(self.db, cache_for(self.directory), BASE_WS, soon, config, [self.sessions], campaigns)["campaigns"][0]
        waiting = {ws for ws, _ in fresh[-7:]}
        self.assertEqual({row["decision_s"] for row in truth.load_rows(self.db) if row["window_start"] in waiting}, {150})
        # On seconds 150-180 alone one side of a pair can trade where both
        # will (window 1788579300: the $100 floor fills at 177 s, V5 at
        # 209 s).  Such a window waits; the difference is folded once, on
        # the complete window.
        self.assertGreaterEqual(sum(pair["pending"] for pair in first["paired_controls"]), 1)
        for pair in first["paired_controls"]:
            self.assertFalse(truth.accrued_windows(self.db, campaign["id"], pair["fingerprint"]) & waiting)
        session.write_text(whole)
        later = fresh[-1][0] + 300 + truth.LADDER_GRACE_S + 1
        second = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, config, [self.sessions], campaigns)["campaigns"][0]
        self.assertEqual({pair["pending"] for pair in second["paired_controls"]}, {0})
        # The same as a table that only ever saw the complete mirror.
        clean_db = truth.open_db(Path(self.directory) / "clean.sqlite3")
        self.addCleanup(clean_db.close)
        truth.build(clean_db, cache_for(self.directory), BASE_WS, now_ts, [self.sessions], sha="event")
        clean = truth.tick(clean_db, cache_for(self.directory), BASE_WS, later, None, [self.sessions], campaigns)["campaigns"][0]
        self.assertEqual(
            [(pair["control_id"], pair["paired_windows"], pair["cell_ahead"], pair["e_value"]) for pair in second["paired_controls"]],
            [(pair["control_id"], pair["paired_windows"], pair["cell_ahead"], pair["e_value"]) for pair in clean["paired_controls"]],
        )
        self.assertGreater(second["paired_controls"][3]["paired_windows"], 0)
        self.assertEqual([(cell["n"], cell["wins"], round(cell["e_value"], 9)) for cell in second["cells"]], [(cell["n"], cell["wins"], round(cell["e_value"], 9)) for cell in clean["cells"]])

    def test_day_45_stops_a_family_whose_fresh_net_is_not_positive(self):
        labelled, now_ts, cache = self.built()
        campaigns, registered = self.register_v4(labelled, now_ts, cache)
        path = campaigns / "2026-10_band_event_v4.json"
        # Fresh windows resolve against the side the proxy settles: every entry loses.
        fresh = self.dense_days(2, first_day=8, flip=True, name="session_20260909_000000.jsonl")
        later = fresh[-1][0] + 300 + truth.LADDER_GRACE_S + 1
        self.aged(path, 44)
        summary = truth.tick(self.db, cache_for(self.directory), BASE_WS, later, loop_config(self.directory), [self.sessions], campaigns)["campaigns"][0]
        check = summary["checks"]["day45_fresh_net"]
        # V5 lost every entry; V1 (no tick under its thin table's caps) has none: no net above zero on either.
        self.assertEqual((check["evaluated"], check["fired"], check["value"], summary["family_stopped"]), (True, True, [None, -1.0], "day45_fresh_net"))
        by_id = {cell["cell_id"]: cell for cell in summary["cells"]}
        volume = by_id[truth.FAMILY_V4["V5"]]
        self.assertEqual((volume["wins"], volume["verdict"], volume["status"]), (0, "kill", "killed_futility"))  # e <= 0.1 came first
        self.assertEqual({cell["status"] for cell in summary["cells"]} - {"killed_futility"}, {"killed_day45_fresh_net"})
        self.assertEqual(by_id[truth.FAMILY_V4["V6"]]["status"], "killed_day45_fresh_net")


if __name__ == "__main__":
    unittest.main()
