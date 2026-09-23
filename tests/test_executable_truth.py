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
        refused = truth.register(self.db, "2026-09_test", Path(self.directory) / "campaigns")
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
        refused = truth.register(self.db, "2026-09_mac", campaigns, now_ts)
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
        # Disagreement beyond a tick fails the check (a fresh table).
        other = truth.open_db(Path(self.directory) / "other.sqlite3")
        self.addCleanup(other.close)
        off_dir = Path(self.directory) / "mirror_off"
        write_sessions(off_dir, [{**record, "samples": [sample(0, 0.60)]} for record in agreeing])
        failed = truth.accept_mac_ladders(other, [self.sessions, off_dir], now_ts)
        self.assertEqual((failed["accepted"], failed["overlap"]["agreement"]), (False, 0.0))
        self.assertEqual(truth.evidence_hosts(other), ("vps",))
        # Accrual: a campaign registered on VPS evidence does not fold Mac-only fresh windows until accepted.
        base = population(300, start_index=1000)
        write_cache(self.directory, base)
        write_sessions(vps_dir, population_records(base), name="session_20260903_000000.jsonl")
        later = base[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        truth.build(other, cache_for(self.directory), BASE_WS, later, [vps_dir], sha="vps")
        registered = truth.register(other, "2026-09_vps", campaigns, later, report=truth.grid(other, fee_rate=FEE))
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
        ticked = truth.tick(other, cache_for(self.directory), BASE_WS, newest, loop_config(self.directory), [vps_dir, self.sessions], campaigns)
        planted = [cell for cell in ticked["campaigns"][0]["cells"] if cell["cell_id"].startswith("d210_f100_")][0]
        self.assertEqual((planted["n"], planted["applied"], ticked["campaigns"][0]["ladder_hosts"]), (24, 24, ["mac", "vps"]))

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
        # Registration: the print survivors dedup to one cell per window set.
        campaigns = Path(self.directory) / "campaigns"
        registered = truth.register(self.db, "2026-09_planted", campaigns, now_ts, report=report)
        self.assertTrue(registered["registered"], registered)
        campaign = json.loads((campaigns / "2026-09_planted.json").read_text())
        self.assertEqual(campaign["family_size"], len(campaign["cells"]))
        self.assertEqual((registered["ladder_only_unscreened"], campaign["ladder_only_unscreened"]), (126, 126))
        # d210_f75 (planted plus the |margin| 90 windows, diluted) and
        # d210_f100 clear; each keeps its first cell in grid order and the 20
        # cap/patience variants with the same window set as aliases (one
        # fresh t = 0 sample at a price every cap admits: they trade the same
        # windows at the same prices under the ladder model too).
        self.assertEqual([cell["cell_id"] for cell in campaign["cells"]], ["d210_f75_c0.92_p0", "d210_f100_c0.92_p0"])
        self.assertEqual([len(cell["aliases"]) for cell in campaign["cells"]], [20, 20])
        self.assertEqual([cell["ladder_windows_at_registration"] for cell in campaign["cells"]], [180, 120])
        self.assertEqual(campaign["cells"][1]["print_model"]["coverage"], 1.0)
        self.assertEqual((campaign["registered_after_window_start"], campaign["registered_at_ts"], campaign["ladder_hosts"]), (specs[-1]["ws"], now_ts, ["vps"]))
        self.assertEqual((campaign["writer_v2_share"], campaign["audit_cleared"]), (1.0, {}))
        self.assertFalse(truth.register(self.db, "2026-09_planted", campaigns, now_ts, report=report)["registered"])

    def test_register_dedups_by_the_ladder_trades_it_will_accrue(self):
        # Every first BUY print lands at offset 1, so the print one-look
        # accepts the same windows for every cap and patience; but the book
        # is stale at t = 0 and comes in band at t = 10: patience 0 never
        # trades on ladder rows, patience 15 and 30 do.  The p0 cell is not
        # an alias of a cell that trades differently under the accrued model.
        specs = population(300)
        write_cache(self.directory, specs)
        records = []
        for spec in specs:
            for d in DECISIONS:
                price = spec["prices"][d]
                records.append(ladder(spec["ws"], d, spec["direction"], spec["margins"][d], [sample(0, price, fresh=False), sample(10, price)]))
        write_sessions(self.sessions, records)
        now_ts = specs[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        truth.build(self.db, cache_for(self.directory), BASE_WS, now_ts, [self.sessions], sha="stale")
        report = truth.grid(self.db, fee_rate=FEE)
        by_id = {cell["cell_id"]: cell for cell in report["cells"]}
        self.assertEqual((by_id["d210_f100_c0.92_p0"]["ladder"]["n"], by_id["d210_f100_c0.92_p15"]["ladder"]["n"]), (0, 120))
        campaigns = Path(self.directory) / "campaigns"
        registered = truth.register(self.db, "2026-09_stale", campaigns, now_ts, report=report)
        campaign = json.loads((campaigns / "2026-09_stale.json").read_text())
        self.assertEqual([cell["cell_id"] for cell in campaign["cells"]], ["d210_f75_c0.92_p0", "d210_f75_c0.92_p15", "d210_f100_c0.92_p0", "d210_f100_c0.92_p15"])
        self.assertEqual([len(cell["aliases"]) for cell in campaign["cells"]], [6, 13, 6, 13])
        self.assertEqual(registered["family_size"], 4)
        self.assertIn("d210_f100_c0.92_p30", campaign["cells"][3]["aliases"])

    def test_register_refuses_an_empty_or_partial_family(self):
        # Nothing clears the print screen on the null: no file is written
        # and the id stays free.
        specs = population(300, plant=False)
        write_cache(self.directory, specs)
        write_sessions(self.sessions, population_records(specs))
        now_ts = specs[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        truth.build(self.db, cache_for(self.directory), BASE_WS, now_ts, [self.sessions], sha="null")
        campaigns = Path(self.directory) / "campaigns"
        refused = truth.register(self.db, "2026-09_null", campaigns, now_ts)
        # The 126 registrable ladder-only cells (195/225 s) have no print column: never admitted, never silent.
        self.assertEqual((refused["registered"], refused["reason"], refused["ladder_only_unscreened"]), (False, "no registrable cell clears the print or ladder screen", 126))
        self.assertFalse((campaigns / "2026-09_null.json").exists())
        # A print cache below writer_v2_share 1.0 is the margin-selected
        # slice (B.6): refused unless the operator says otherwise, and then
        # a cell whose print coverage is low is still not admitted.
        planted = population(300, start_index=300)
        planted[0]["writer"] = {240: 1}
        write_cache(self.directory, planted)
        write_sessions(self.sessions, population_records(planted), name="session_20260902_000000.jsonl")
        later = planted[-1]["ws"] + 300 + band.RESOLUTION_LAG_S + 1
        summary = truth.build(self.db, cache_for(self.directory), BASE_WS, later, [self.sessions], sha="partial")
        self.assertLess(summary["writer_v2_share"], 1.0)
        report = truth.grid(self.db, fee_rate=FEE)
        refused = truth.register(self.db, "2026-09_partial", campaigns, later, report=report)
        self.assertFalse(refused["registered"])
        self.assertIn("writer_v2_share", refused["reason"])
        allowed = truth.register(self.db, "2026-09_partial", campaigns, later, report=report, allow_partial_prints=True)
        self.assertTrue(allowed["registered"], allowed)
        self.assertIn("d210_f100_c0.92_p0", allowed["cells"])

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

    def test_register_admits_a_cell_on_the_ladder_screen_when_prints_reject_it(self):
        specs, now_ts = self.planted_fixture()
        campaigns = Path(self.directory) / "campaigns_ladder_screen"
        report = truth.grid(self.db, fee_rate=FEE)
        registrable = [cell for cell in report["cells"] if cell["registrable"]]
        self.assertTrue(registrable)
        # Force every registrable cell through the print screen's rejection
        # and give exactly one of them a passing ladder model.
        for cell in registrable:
            cell["print"]["clears_break_even"] = False
            cell["ladder"] = dict(cell.get("ladder") or {}, n=0)  # ladder screen off for everyone else
        chosen = registrable[0]
        chosen["ladder"] = dict(chosen.get("ladder") or {}, n=truth.REGISTER_LADDER_MIN_N, wins=truth.REGISTER_LADDER_MIN_N,
                                wilson_lower=0.95, mean_break_even=0.96, mean_net_per_usd=0.03,
                                tripwires=dict((chosen.get("ladder") or {}).get("tripwires") or {}, adverse_selected=False))
        result = truth.register(self.db, "2026-09_ladder_screen", campaigns, now_ts, report=report)
        self.assertTrue(result["registered"], result)
        self.assertEqual(result["cells"], [chosen["cell_id"]])
        cells = json.loads((campaigns / "2026-09_ladder_screen.json").read_text())["cells"]
        self.assertEqual([c["cell_id"] for c in cells], [chosen["cell_id"]])
        self.assertEqual(cells[0]["screen"], "ladder")
        # Below the slack the ladder screen does not admit.
        chosen["ladder"]["wilson_lower"] = 0.96 - truth.REGISTER_LADDER_SLACK - 0.001
        refused = truth.register(self.db, "2026-09_ladder_screen_2", campaigns, now_ts, report=report)
        self.assertFalse(refused["registered"], refused)

    def test_tick_accrues_registered_cells_with_one_ledger_row_per_look(self):
        specs, now_ts = self.planted_fixture()
        campaigns = Path(self.directory) / "campaigns"
        report = truth.grid(self.db, fee_rate=FEE)
        self.assertTrue(truth.register(self.db, "2026-09_planted", campaigns, now_ts, report=report)["registered"])
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
        self.assertTrue(truth.register(self.db, "2026-09_planted", campaigns, now_ts, report=truth.grid(self.db, fee_rate=FEE))["registered"])
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

    def test_clear_audit_releases_a_held_cell(self):
        specs, now_ts = self.planted_fixture()
        campaigns = Path(self.directory) / "campaigns"
        self.assertTrue(truth.register(self.db, "2026-09_planted", campaigns, now_ts, report=truth.grid(self.db, fee_rate=FEE))["registered"])
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
        self.assertTrue(truth.register(self.db, "2026-09_planted", campaigns, now_ts, report=truth.grid(self.db, fee_rate=FEE))["registered"])
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


if __name__ == "__main__":
    unittest.main()
