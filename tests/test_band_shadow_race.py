from __future__ import annotations

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
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


race = _load("band_shadow_race", "scripts/band_shadow_race.py")
band = race.band_lane
accrual = race.evidence_accrual

BASE_WS = 1787788800 + 30 * 300  # 2026-08-25T02:30Z: sigma lookback stays inside the day file
CHAMPION = dict(race.LIVE_RULE)
CHALLENGER_210 = {**CHAMPION, "decision_second": 210}
LAMBDAS = [(index + 1) * accrual.LAMBDA_STEP for index in range(accrual.LAMBDA_COUNT)]


def quote(vwap, worst=None, shares=14.0):
    return {"best_ask": vwap, "book_age_s": 0.4, "vwap": vwap, "worst": vwap if worst is None else worst, "shares": shares}


def complement(side):
    """A fresh quote at 1 - side's best ask: the pair the engine calls coherent."""
    ask = (side or {}).get("best_ask")
    return quote(round(1.0 - ask, 4)) if ask else quote(0.30)


def anchor(ws, anchor_s, margin, up=None, down=None, stake=10.0, late=0.31):
    # One side given: the other defaults to its coherent complement.
    if up is None:
        up = quote(0.70) if down is None else complement(down)
    if down is None:
        down = complement(up)
    return {
        "type": "band_anchor",
        "ts": ws + anchor_s + late,
        "cid": "%016x" % ws,
        "anchor_s": anchor_s,
        "elapsed_s": anchor_s + late,
        "btc": 70000.0 + (margin or 0.0),
        "open": 70000.0,
        "margin": margin,
        "direction": None if not margin else ("up" if margin > 0 else "down"),
        "stake_usd": stake,
        # The engine quotes at the frozen target when the sizing policy declines.
        "quote_budget_usd": 10.0 if stake is None else stake,
        "up": up,
        "down": down,
        "pair_sum": 1.0,
    }


def write_sessions(directory, records, name="session_20260901_000000.jsonl"):
    path = Path(directory) / "sessions" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ['{"ts": 1.0, "cat": "price", "type": "snapshot", "btc": 70000.0}', "not json {"]
    lines += [json.dumps(record) for record in records]
    path.write_text("\n".join(lines) + "\n")
    return path.parent


def stub_cache(directory, outcomes, closes=None, fetch_market=None):
    """Network-free BandCache over a temp margin-study cache."""
    margin = Path(directory) / "margin"
    margin.mkdir(parents=True, exist_ok=True)
    (margin / "gamma_outcomes.json").write_text(json.dumps({str(key): value for key, value in outcomes.items()}))
    for ts, price in (closes or {}).items():
        day = ts - ts % band.DAY_S
        path = margin / ("binance_%d.json" % day)
        prices = json.loads(path.read_text()) if path.is_file() else {}
        prices[str(ts)] = price
        path.write_text(json.dumps(prices))

    def offline(*args):
        raise AssertionError("network in test")

    return band.BandCache(
        margin_dir=margin,
        prints_dir=Path(directory) / "prints",
        fetch_closes=offline,
        fetch_market=fetch_market or offline,
        fetch_trades=offline,
    )


SCALE = 1.0 / band.break_even(CHAMPION["favorite_price_floor"])


def reference_e_value(differences):
    """Closed-form mixture over the clipped d sequence."""
    total = 0.0
    for lambda_ in LAMBDAS:
        wealth = 1.0
        for d in differences:
            wealth *= max(1.0 + lambda_ * max(-1.0, min(1.0, d)), accrual.FACTOR_FLOOR)
        total += wealth
    return total / accrual.LAMBDA_COUNT


def loop_config(directory):
    path = Path(directory) / "loop.json"
    path.write_text(json.dumps({"state_dir": str(directory), "generator": {"trial_ledger_enabled": True}}))
    return path


class BandShadowRaceTest(unittest.TestCase):
    # --- anchors -------------------------------------------------------------

    def test_load_anchors_groups_by_window_and_anchor_second(self):
        first = [anchor(BASE_WS, 210, 60.0), anchor(BASE_WS, 240, 60.0), anchor(BASE_WS + 300, 240, -70.0, late=0.9)]
        # A restart re-emits the 240 anchor later; the earliest elapsed wins.
        second = [{**anchor(BASE_WS, 240, 61.0, late=1.5)}, {"type": "band_anchor", "ts": 1.0}]
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            sessions = write_sessions(directory, first)
            write_sessions(directory, second, name="session_20260901_010000.jsonl")
            anchors = race.load_anchors(sessions)
        self.assertEqual(sorted(anchors), [BASE_WS, BASE_WS + 300])
        self.assertEqual(sorted(anchors[BASE_WS]), [210, 240])
        self.assertEqual(anchors[BASE_WS][240]["margin"], 60.0)
        self.assertEqual(anchors[BASE_WS][240]["elapsed_s"], 240.31)
        self.assertEqual(list(anchors[BASE_WS + 300]), [240])
        self.assertEqual(race.load_anchors(Path(directory) / "missing"), {})

    # --- rule application ---------------------------------------------------

    def test_rule_trade_margin_floors_direction_and_sigma(self):
        self.assertIsNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 40.0), None))
        self.assertEqual(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 50.0), None)["direction"], "up")
        self.assertEqual(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, -50.0, down=quote(0.7)), None)["direction"], "down")
        self.assertIsNone(race.rule_trade({**CHAMPION, "direction": "up"}, anchor(BASE_WS, 240, -60.0), None))
        self.assertIsNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, None), None))
        self.assertIsNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 0.0), None))
        sigma_rule = {**CHAMPION, "margin_floor_sigma": 1.0}
        self.assertIsNone(race.rule_trade(sigma_rule, anchor(BASE_WS, 240, 60.0), None))
        self.assertIsNone(race.rule_trade(sigma_rule, anchor(BASE_WS, 240, 60.0), 100.0))
        self.assertIsNotNone(race.rule_trade(sigma_rule, anchor(BASE_WS, 240, 60.0), 50.0))
        trade = race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, stake=7.5), None)
        self.assertEqual((trade["vwap"], trade["worst"], trade["stake_usd"]), (0.7, 0.7, 7.5))

    def test_rule_trade_band_on_vwap_and_worst_and_missing_quote(self):
        def with_quote(side_quote):
            return race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, up=side_quote), None)

        # BandPolicyParams::quote_clears_band: vwap > floor, FOK worst <= cap.
        self.assertIsNone(with_quote(quote(0.55)))
        self.assertIsNotNone(with_quote(quote(0.5501)))
        self.assertIsNotNone(with_quote(quote(0.91, worst=0.92)))
        self.assertIsNone(with_quote(quote(0.91, worst=0.9201)))
        self.assertIsNotNone(with_quote(quote(0.92)))
        self.assertIsNone(with_quote(quote(0.93)))
        # No executable quote for the stake: the rule does not trade.
        self.assertIsNone(with_quote({"best_ask": 0.7, "book_age_s": 0.4, "vwap": None, "worst": None, "shares": 0.0}))
        self.assertIsNone(with_quote(quote(0.0)))
        self.assertIsNone(with_quote({}))
        self.assertIsNone(race.rule_trade(CHAMPION, {**anchor(BASE_WS, 240, -60.0), "down": None}, None))

    def test_rule_trade_replays_the_engine_cycle_gates(self):
        # pick_book_prices: a fresh best ask on BOTH sides, else the cycle
        # skips 'fresh_outcome_book_unavailable' before any rule runs. The
        # engine still quotes from a stale book, so vwap/worst are populated.
        stale_up = {**quote(0.7), "book_age_s": None}
        self.assertIsNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, up=stale_up), None))
        self.assertIsNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, down={**quote(0.3), "book_age_s": None}), None))
        self.assertIsNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, down={**quote(0.3), "best_ask": None}), None))
        self.assertIsNone(race.rule_trade(CHAMPION, {**anchor(BASE_WS, 240, 60.0), "down": None}, None))
        self.assertIsNone(race.rule_trade(CHAMPION, {**anchor(BASE_WS, 240, 60.0), "down": {}}, None))
        # stake_usd null: the live sizing policy declined the window (kelly_lo's
        # <= 0.70 bucket) although a quote at the fallback budget exists. Every
        # rule is replayed under the live policy, challengers included.
        declined = anchor(BASE_WS, 240, 60.0, up=quote(0.66), stake=None)
        self.assertEqual(declined["quote_budget_usd"], 10.0)
        self.assertIsNone(race.rule_trade(CHAMPION, declined, None))
        self.assertIsNone(race.rule_trade(CHALLENGER_210, anchor(BASE_WS, 210, 60.0, up=quote(0.66), stake=None), None))
        # A zero stake is a value, not the null marker.
        self.assertEqual(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, stake=0.0), None)["stake_usd"], 0.0)
        trade = race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, up=quote(0.75), stake=7.5), None)
        self.assertEqual((trade["vwap"], trade["stake_usd"]), (0.75, 7.5))
        # band_pair_incoherent: the momentum VWAP plus the complement's best
        # ask outside [0.90, 1.10] (live incident 2026-08-26: a frozen 0.71
        # against 0.40 while the venue had 0.415/0.545). Both books fresh.
        self.assertIsNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, up=quote(0.71), down=quote(0.40)), None))
        self.assertIsNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, -60.0, up=quote(0.46), down=quote(0.71)), None))
        self.assertIsNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, up=quote(0.60), down=quote(0.29)), None))
        # Inclusive on both ends, like the engine's 0.90..=1.10.
        self.assertIsNotNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, up=quote(0.65), down=quote(0.25)), None))
        self.assertIsNotNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, up=quote(0.70), down=quote(0.40)), None))
        # The gate is on the walked VWAP, not the anchor's pair_sum of best asks.
        deep = {**quote(0.75), "best_ask": 0.70}
        self.assertIsNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, up=deep, down=quote(0.36)), None))
        self.assertIsNotNone(race.rule_trade(CHAMPION, anchor(BASE_WS, 240, 60.0, up=deep, down=quote(0.35)), None))

    def test_trade_score_matches_band_lane_fee_model(self):
        vwap = 0.7
        even = vwap + band.taker_fee(vwap)
        self.assertAlmostEqual(even, 0.7 + 0.072 * 0.7 * 0.3, places=12)
        fee_per_usd = 1.0 / vwap - 1.0 / even
        self.assertAlmostEqual(race.trade_score(vwap, True), (1.0 / vwap - 1.0) - fee_per_usd, places=12)
        self.assertAlmostEqual(race.trade_score(vwap, True), 1.0 / band.break_even(vwap) - 1.0, places=12)
        self.assertEqual(race.trade_score(vwap, False), -1.0)

    def test_binance_sigmas_mirror_band_lane(self):
        magnitudes = [10.0, 30.0, 20.0, 45.0, 5.0, 60.0, 25.0, 35.0, 15.0, 50.0, 40.0, 55.0, 12.0, 70.0, 22.0]
        closes = {}
        for index, magnitude in enumerate(magnitudes):
            ws = BASE_WS + index * 300
            closes[ws] = 70000.0
            closes[ws + 240] = 70000.0 + magnitude * (1 if index % 2 else -1)
        windows = [
            {"window_start": BASE_WS + index * 300, "open": 70000.0, "closes": {240: closes[BASE_WS + index * 300 + 240]}}
            for index in range(len(magnitudes))
        ]
        permissive = {**CHAMPION, "margin_floor_usd": 0}
        expected = {row["window_start"]: row["sigma"] for row in band.band_signal_records(windows, permissive)}
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            cache = stub_cache(directory, {}, closes=closes)
            cache.load_closes(BASE_WS, BASE_WS + len(magnitudes) * 300, BASE_WS + len(magnitudes) * 300, fetch=False)
            sigmas = race.binance_sigmas(cache, [window["window_start"] for window in windows], 240)
        self.assertEqual(sigmas, expected)
        self.assertEqual([sigmas[BASE_WS + index * 300] for index in range(12)], [None] * 12)
        self.assertGreater(sigmas[BASE_WS + 12 * 300], 0.0)
        self.assertNotEqual(sigmas[BASE_WS + 12 * 300], sigmas[BASE_WS + 13 * 300])

    # --- race ----------------------------------------------------------------

    def test_race_pairs_windows_scores_and_scales(self):
        ws = [BASE_WS + index * 300 for index in range(8)]
        anchors = {
            # both trade up and win; the challenger at a better price
            ws[0]: {210: anchor(ws[0], 210, 60.0, up=quote(0.65)), 240: anchor(ws[0], 240, 60.0)},
            # champion loses, challenger wins: raw d > 1, inside [-1, 1] once scaled
            ws[1]: {210: anchor(ws[1], 210, 55.0, up=quote(0.60)), 240: anchor(ws[1], 240, -55.0, down=quote(0.60))},
            # champion (up) wins, challenger (down) loses: raw d < -1, scaled likewise
            ws[2]: {210: anchor(ws[2], 210, -55.0, down=quote(0.60)), 240: anchor(ws[2], 240, 55.0)},
            # only the 240 anchor: seen by the champion, never paired
            ws[3]: {240: anchor(ws[3], 240, 80.0, stake=20.0)},
            # both anchors, neither trades (below the floor)
            ws[4]: {210: anchor(ws[4], 210, 30.0), 240: anchor(ws[4], 240, 30.0)},
            # same side, same price, both win: d = 0 but paired
            ws[5]: {210: anchor(ws[5], 210, 60.0), 240: anchor(ws[5], 240, 60.0)},
            # unresolved on Gamma: excluded entirely
            ws[6]: {210: anchor(ws[6], 210, 60.0), 240: anchor(ws[6], 240, 60.0)},
            # both trade down and lose
            ws[7]: {210: anchor(ws[7], 210, -60.0, down=quote(0.7)), 240: anchor(ws[7], 240, -60.0, down=quote(0.7))},
        }
        outcomes = {ws[0]: "up", ws[1]: "up", ws[2]: "up", ws[3]: "up", ws[4]: "up", ws[5]: "up", ws[6]: None, ws[7]: "up"}
        report = race.race(anchors, outcomes, {}, CHAMPION, [CHALLENGER_210], 0.05)
        win_70, win_65, win_60 = (race.trade_score(v, True) for v in (0.7, 0.65, 0.6))

        champion = report["champion"]
        self.assertEqual(champion["fingerprint"], band.band_fingerprint(CHAMPION))
        self.assertEqual((champion["windows"], champion["trades"], champion["wins"]), (7, 6, 4))
        champion_scores = [win_70, -1.0, win_70, win_70, win_70, -1.0]
        self.assertAlmostEqual(champion["net_per_usd"], sum(champion_scores) / 6, places=12)
        self.assertAlmostEqual(champion["net_at_stake"], 10.0 * (win_70 + -1.0 + win_70 + win_70 + -1.0) + 20.0 * win_70, places=9)

        challenger = report["challengers"][0]
        self.assertEqual(challenger["fingerprint"], band.band_fingerprint(CHALLENGER_210))
        self.assertEqual((challenger["windows"], challenger["trades"], challenger["wins"]), (6, 5, 3))
        paired = challenger["paired"]
        raw = [win_65 - win_70, win_60 + 1.0, -1.0 - win_70, 0.0, 0.0]
        scaled = [d / SCALE for d in raw]
        self.assertTrue(all(abs(d) <= 1.0 for d in scaled))
        self.assertEqual((paired["n"], paired["wins"], paired["losses"], paired["overlap"], paired["clipped"]), (5, 2, 1, 3, 0))
        self.assertAlmostEqual(paired["mean_d"], sum(scaled) / 5, places=12)
        expected = reference_e_value(scaled)
        self.assertLess(abs(paired["e_value"] - expected) / expected, 1e-9)
        self.assertEqual(paired["verdict"], "continue")
        self.assertEqual(report["windows"], {"anchored": 8, "resolved": 7, "unresolved": 1})
        self.assertAlmostEqual(report["d_scale"], SCALE, places=12)
        self.assertIn("/ %.4f" % SCALE, report["scaling"])
        text = race.markdown(report)
        self.assertIn("| challenger | n | mean d |", text)
        self.assertIn(challenger["fingerprint"][:12], text)
        self.assertNotIn("WARNING", text)

    def test_race_scale_keeps_d_in_unit_interval_and_preserves_sign(self):
        # The widest pair: a win just above the 0.55 floor against a loss.
        ws = [BASE_WS + index * 300 for index in range(2)]
        anchors = {
            ws[0]: {210: anchor(ws[0], 210, 60.0, up=quote(0.5501)), 240: anchor(ws[0], 240, -60.0, down=quote(0.92))},
            ws[1]: {210: anchor(ws[1], 210, -60.0, down=quote(0.92)), 240: anchor(ws[1], 240, 60.0, up=quote(0.5501))},
        }
        paired = race.race(anchors, {w: "up" for w in ws}, {}, CHAMPION, [CHALLENGER_210], 0.05)["challengers"][0]["paired"]
        widest = (1.0 + race.trade_score(0.5501, True)) / SCALE
        self.assertLess(widest, 1.0)
        self.assertGreater(widest, 0.999)
        self.assertEqual((paired["clipped"], paired["wins"], paired["losses"]), (0, 1, 1))
        self.assertAlmostEqual(paired["mean_d"], 0.0, places=12)
        # A wider band on either side raises the divisor: a 0.70-floor
        # challenger against the 0.55-floor champion still divides by the
        # champion's 1/break_even(0.55).
        tight = {**CHALLENGER_210, "favorite_price_floor": 0.70}
        self.assertAlmostEqual(race.race(anchors, {w: "up" for w in ws}, {}, CHAMPION, [tight], 0.05)["d_scale"], SCALE, places=12)

        # The tighter-band challenger that wins small (0.85) and loses big
        # against a champion at 0.60: with the champion winning 20 of 45
        # disagreement windows the challenger loses money per USD (E[d] < 0)
        # although it wins more windows (clipping would have read +5/45).
        ws = [BASE_WS + index * 300 for index in range(45)]
        anchors = {w: {210: anchor(w, 210, -60.0, down=quote(0.85)), 240: anchor(w, 240, 60.0, up=quote(0.60))} for w in ws}
        outcomes = {w: ("down" if index < 25 else "up") for index, w in enumerate(ws)}
        report = race.race(anchors, outcomes, {}, CHAMPION, [{**CHALLENGER_210, "favorite_price_floor": 0.70}], 0.05)
        paired = report["challengers"][0]["paired"]
        raw_sum = 25 * (1.0 + race.trade_score(0.85, True)) - 20 * (1.0 + race.trade_score(0.60, True))
        self.assertLess(raw_sum, 0.0)
        self.assertEqual((paired["n"], paired["wins"], paired["losses"], paired["clipped"]), (45, 25, 20, 0))
        self.assertAlmostEqual(paired["mean_d"], raw_sum / 45 / report["d_scale"], places=12)
        self.assertLess(paired["mean_d"], 0.0)

    def test_race_null_stake_windows_are_not_trades(self):
        # kelly_lo declined the 0.66 favorite: the engine did not trade, so the
        # window is neither in net_per_usd nor in net_at_stake (one trade set).
        ws = [BASE_WS, BASE_WS + 300]
        anchors = {
            ws[0]: {240: anchor(ws[0], 240, 60.0, up=quote(0.66), stake=None)},
            ws[1]: {240: anchor(ws[1], 240, 60.0, up=quote(0.75), stake=8.0)},
        }
        champion = race.race(anchors, {w: "up" for w in ws}, {}, CHAMPION, [], 0.05)["champion"]
        self.assertEqual((champion["windows"], champion["trades"], champion["wins"]), (2, 1, 1))
        self.assertAlmostEqual(champion["net_per_usd"], race.trade_score(0.75, True), places=12)
        self.assertAlmostEqual(champion["net_at_stake"], 8.0 * race.trade_score(0.75, True), places=12)

    def test_race_stale_or_incoherent_champion_anchor_is_not_a_trade(self):
        # The DOWN feed stalls before 240 s (book_age_s null, asks frozen at
        # 0.71) or the pair is incoherent: the engine skipped, so the champion
        # did not trade and the challenger's genuine loss pairs as d = -1/scale,
        # not the d = 0 of an overlap with a phantom champion trade.
        ws = [BASE_WS, BASE_WS + 300]
        stale_down = {**quote(0.71), "book_age_s": None}
        anchors = {
            ws[0]: {210: anchor(ws[0], 210, -60.0, down=quote(0.60)), 240: anchor(ws[0], 240, -80.0, up=quote(0.46), down=stale_down)},
            ws[1]: {210: anchor(ws[1], 210, -60.0, down=quote(0.60)), 240: anchor(ws[1], 240, -80.0, up=quote(0.40), down=quote(0.71))},
        }
        report = race.race(anchors, {w: "up" for w in ws}, {}, CHAMPION, [CHALLENGER_210], 0.05)
        self.assertEqual((report["champion"]["windows"], report["champion"]["trades"]), (2, 0))
        self.assertEqual(report["champion"]["net_at_stake"], 0.0)
        paired = report["challengers"][0]["paired"]
        self.assertEqual((paired["n"], paired["wins"], paired["losses"], paired["overlap"]), (2, 0, 2, 0))
        self.assertAlmostEqual(paired["mean_d"], -1.0 / SCALE, places=12)

    def test_race_promotion_threshold_is_bonferroni_over_challengers(self):
        anchors = {
            BASE_WS + index * 300: {210: anchor(BASE_WS + index * 300, 210, 60.0, up=quote(0.6)), 240: anchor(BASE_WS + index * 300, 240, 10.0)}
            for index in range(12)
        }
        outcomes = {ws: "up" for ws in anchors}
        e_value = reference_e_value([race.trade_score(0.6, True) / SCALE] * 12)
        alpha = 1.5 / e_value  # 1/alpha < e < 2/alpha
        alone = race.race(anchors, outcomes, {}, CHAMPION, [CHALLENGER_210], alpha)
        self.assertAlmostEqual(alone["promote_e"], 1.0 / alpha, places=9)
        self.assertEqual(alone["challengers"][0]["paired"]["verdict"], "promote")
        # A second challenger (same rule, looser USD floor, also trades every
        # window) doubles the threshold: the same e-value no longer promotes.
        pair = race.race(anchors, outcomes, {}, CHAMPION, [CHALLENGER_210, {**CHALLENGER_210, "margin_floor_usd": 25}], alpha)
        self.assertAlmostEqual(pair["promote_e"], 2.0 / alpha, places=9)
        self.assertEqual([row["paired"]["verdict"] for row in pair["challengers"]], ["continue", "continue"])
        self.assertAlmostEqual(pair["challengers"][0]["paired"]["e_value"], alone["challengers"][0]["paired"]["e_value"], places=9)
        text = race.markdown(pair)
        self.assertIn("promote at e >= %.1f (alpha %s Bonferroni over 2 challengers" % (2.0 / alpha, alpha), text)
        self.assertIn("champion = the band rule replayed on the anchor quote", text)
        # No challengers at all: the threshold is the plain 1/alpha.
        self.assertAlmostEqual(race.race(anchors, outcomes, {}, CHAMPION, [], 0.05)["promote_e"], 20.0, places=12)

    def test_race_verdicts_follow_alpha_and_futility(self):
        # Champion never trades (no 240 anchor at all); challenger wins at 0.6 every window.
        anchors = {
            BASE_WS + index * 300: {210: anchor(BASE_WS + index * 300, 210, 60.0, up=quote(0.6)), 240: anchor(BASE_WS + index * 300, 240, 10.0)}
            for index in range(40)
        }
        outcomes = {ws: "up" for ws in anchors}
        score = race.trade_score(0.6, True)
        loose = race.race(anchors, outcomes, {}, CHAMPION, [CHALLENGER_210], 0.5)["challengers"][0]["paired"]
        strict = race.race(anchors, outcomes, {}, CHAMPION, [CHALLENGER_210], 1e-9)["challengers"][0]["paired"]
        self.assertEqual(loose["n"], 40)
        self.assertLess(abs(loose["e_value"] - reference_e_value([score / SCALE] * 40)) / loose["e_value"], 1e-9)
        self.assertEqual(loose["verdict"], "promote")
        self.assertEqual(strict["verdict"], "continue")
        losing = race.race(anchors, {ws: "down" for ws in anchors}, {}, CHAMPION, [CHALLENGER_210], 0.05)
        self.assertEqual(losing["challengers"][0]["paired"]["verdict"], "kill")

    # --- inputs ----------------------------------------------------------------

    def test_resolve_rule_from_json_and_sqlite_fingerprint(self):
        rule = {**CHAMPION, "margin_floor_usd": 75}
        fingerprint = band.band_fingerprint(rule)
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            database = Path(directory) / "research.sqlite3"
            connection = sqlite3.connect(str(database))
            connection.execute(
                "CREATE TABLE hypotheses (fingerprint TEXT PRIMARY KEY, lane TEXT NOT NULL, created_at TEXT NOT NULL, "
                "proposal_json TEXT NOT NULL, review_json TEXT, status TEXT NOT NULL, evidence_path TEXT, source TEXT)"
            )
            connection.execute(
                "INSERT INTO hypotheses VALUES (?, ?, ?, ?, NULL, ?, NULL, 'prior')",
                (fingerprint, band.LANE, "2026-09-01T00:00:00+00:00", json.dumps({"title": "t", "rule": rule}), "accruing"),
            )
            connection.execute(
                "INSERT INTO hypotheses VALUES (?, ?, ?, ?, NULL, ?, NULL, NULL)",
                ("late-fp", "late_window_mechanisms", "2026-09-01T00:00:00+00:00", json.dumps({"rule": {"operator": "x"}}), "new"),
            )
            connection.commit()
            connection.close()
            self.assertEqual(race.resolve_rule(fingerprint, database), rule)
            self.assertEqual(race.resolve_rule(" " + json.dumps(rule) + "\n", database), rule)
            with self.assertRaises(ValueError):
                race.resolve_rule("late-fp", database)
            with self.assertRaises(ValueError):
                race.resolve_rule("0" * 64, database)
            with self.assertRaises(ValueError):
                race.resolve_rule(json.dumps({**rule, "margin_floor_usd": 60}), database)

    def test_resolve_outcomes_fetches_only_eligible_windows(self):
        ws = [BASE_WS + index * 300 for index in range(6)]
        now_ts = ws[3] + 300 + band.RESOLUTION_LAG_S + 10  # ws[4], ws[5] too young
        calls = []

        def fetch_market(window_start):
            calls.append(window_start)
            if window_start == ws[5 - 3]:
                raise OSError("gamma down")
            if window_start == ws[1]:
                return {
                    "conditionId": "0x1",
                    "outcomes": '["Up", "Down"]',
                    "clobTokenIds": '["u", "d"]',
                    "umaResolutionStatus": "resolved",
                    "outcomePrices": '["0", "1"]',
                }
            return None  # unlisted / unresolved

        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            cache = stub_cache(directory, {ws[0]: "up"}, fetch_market=fetch_market)
            # ws[3] is young enough to wait; make ws[2] old enough to be final None
            # by fetching with a later clock for that window alone.
            outcomes, counts = race.resolve_outcomes(cache, ws, now_ts)
            saved = json.loads((Path(directory) / "margin/gamma_outcomes.json").read_text())
        self.assertEqual(calls, [ws[1], ws[2], ws[3]])
        self.assertEqual(outcomes, {ws[0]: "up", ws[1]: "down", ws[2]: None, ws[3]: None, ws[4]: None, ws[5]: None})
        self.assertEqual(counts, {"fetched": 1, "errors": 1})
        self.assertEqual(saved, {str(ws[0]): "up", str(ws[1]): "down"})

    def test_resolve_outcomes_keeps_outcomes_the_research_loop_saved_meanwhile(self):
        # band_lane's loop refreshes the same gamma_outcomes.json between the
        # race's load and its save: its fetch must survive the race's save.
        ws = [BASE_WS, BASE_WS + 300]
        now_ts = ws[1] + 300 + band.RESOLUTION_LAG_S + 10
        up_market = {
            "conditionId": "0x1",
            "outcomes": '["Up", "Down"]',
            "clobTokenIds": '["u", "d"]',
            "umaResolutionStatus": "resolved",
            "outcomePrices": '["1", "0"]',
        }
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            race_cache = stub_cache(directory, {}, fetch_market=lambda window_start: up_market)
            loop_cache = band.BandCache(
                margin_dir=race_cache.margin_dir, prints_dir=race_cache.prints_dir,
                fetch_closes=None, fetch_market=None, fetch_trades=None,
            )
            loop_cache.outcomes[str(ws[0])] = "down"
            loop_cache.save_outcomes()
            outcomes, counts = race.resolve_outcomes(race_cache, [ws[1]], now_ts)
            saved = json.loads((race_cache.margin_dir / "gamma_outcomes.json").read_text())
        self.assertEqual((outcomes, counts), ({ws[1]: "up"}, {"fetched": 1, "errors": 0}))
        self.assertEqual(saved, {str(ws[0]): "down", str(ws[1]): "up"})
        self.assertEqual(race_cache.outcomes, saved)

    def test_resolve_outcomes_records_final_none_after_two_hours(self):
        ws = BASE_WS
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            cache = stub_cache(directory, {}, fetch_market=lambda window_start: None)
            young, _ = race.resolve_outcomes(cache, [ws], ws + 300 + band.UNRESOLVED_FINAL_AFTER_S - 1)
            self.assertEqual(young, {ws: None})
            self.assertNotIn(str(ws), cache.outcomes)
            old, counts = race.resolve_outcomes(cache, [ws], ws + 300 + band.UNRESOLVED_FINAL_AFTER_S)
            saved = json.loads((Path(directory) / "margin/gamma_outcomes.json").read_text())
        self.assertEqual((old, counts), ({ws: None}, {"fetched": 1, "errors": 0}))
        self.assertEqual(saved, {str(ws): None})

    # --- CLI -------------------------------------------------------------------

    def test_main_reports_and_writes_ledger_without_pull(self):
        ws = [BASE_WS + index * 300 for index in range(3)]
        records = []
        for window_start in ws:
            records += [anchor(window_start, 210, 60.0, up=quote(0.65)), anchor(window_start, 240, 60.0)]
        market_calls = mock.Mock(return_value=None)
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            sessions = write_sessions(directory, records)
            cache = stub_cache(directory, {w: "up" for w in ws}, fetch_market=market_calls)
            config = loop_config(directory)
            argv = [
                "--sessions-dir", str(sessions),
                "--challengers", json.dumps(CHALLENGER_210),
                "--json",
                "--loop-config", str(config),
                "--research-db", str(Path(directory) / "missing.sqlite3"),
            ]
            stdout = io.StringIO()
            with mock.patch.object(race.subprocess, "run") as run, contextlib.redirect_stdout(stdout):
                status = race.main(argv, cache=cache, now_ts=ws[-1] + 3000)
            run.assert_not_called()
            market_calls.assert_not_called()
            reports = sorted((Path(directory) / "band_race").glob("*.json"))
            self.assertEqual(len(reports), 1)
            report = json.loads(reports[0].read_text())
            ledger = [json.loads(line) for line in (Path(directory) / "trial_ledger.jsonl").read_text().splitlines()]
        self.assertEqual(status, 0)
        text = stdout.getvalue()
        self.assertIn("| rule | fingerprint | windows |", text)
        self.assertIn("(champion)", text)
        self.assertIn("written ", text)
        self.assertEqual(report["champion"]["trades"], 3)
        self.assertEqual(report["challengers"][0]["paired"]["n"], 3)
        self.assertEqual(report["challengers"][0]["paired"]["wins"], 3)
        self.assertEqual(report["outcome_fetch"], {"fetched": 0, "errors": 0})
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger[0]["stage"], race.STAGE)
        self.assertEqual(ledger[0]["candidate"], band.band_fingerprint(CHALLENGER_210))
        self.assertEqual((ledger[0]["n"], ledger[0]["wins"], ledger[0]["verdict"]), (3, 3, "continue"))
        self.assertEqual(ledger[0]["source"], "research_loop")

    def test_main_sigma_challenger_reads_closes_from_cache(self):
        ws = [BASE_WS + index * 300 for index in range(16)]
        closes = {}
        for index, window_start in enumerate(ws):
            for second in range(0, 300):
                closes[window_start + second] = 70000.0 + (3.0 if index % 2 else -3.0) * (second >= 150)
        # Windows 12..15 carry a full 12-window history; sigma there is 0, so
        # any non-zero margin clears 1.5 sigma. Earlier windows have no sigma.
        records = [anchor(window_start, 240, 60.0) for window_start in ws]
        newest = max(closes)
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            sessions = write_sessions(directory, records)
            cache = stub_cache(directory, {w: "up" for w in ws}, closes=closes)
            stdout = io.StringIO()
            with mock.patch.object(race.subprocess, "run") as run, contextlib.redirect_stdout(stdout):
                status = race.main(
                    ["--sessions-dir", str(sessions), "--challengers", json.dumps({**CHAMPION, "margin_floor_sigma": 1.5})],
                    cache=cache,
                    now_ts=newest + 30,
                )
            run.assert_not_called()
        self.assertEqual(status, 0)
        self.assertEqual(cache.closes_errors, [])
        rows = [line for line in stdout.getvalue().splitlines() if "(challenger)" in line]
        self.assertEqual(len(rows), 1)
        cells = [cell.strip() for cell in rows[0].split("|")]
        self.assertEqual((cells[3], cells[4], cells[5]), ("16", "4", "4"))

    def test_pull_rsyncs_from_vps_into_the_local_mirror_only(self):
        with tempfile.TemporaryDirectory(dir=str(ROOT / "logs")) as directory:
            sessions = Path(directory) / "mirror"
            with mock.patch.object(race.subprocess, "run") as run, contextlib.redirect_stdout(io.StringIO()):
                status = race.main(["--pull", "--sessions-dir", str(sessions), "--challengers", json.dumps(CHALLENGER_210)])
            self.assertTrue(sessions.is_dir())
        self.assertEqual(status, 1)  # nothing mirrored by the mocked rsync
        run.assert_called_once_with(["rsync", "-az", race.VPS_SESSIONS, str(sessions) + "/"], check=True)
        source, destination = run.call_args[0][0][2], run.call_args[0][0][3]
        self.assertTrue(source.startswith("vps:"))
        self.assertNotIn(":", destination)


if __name__ == "__main__":
    unittest.main()
