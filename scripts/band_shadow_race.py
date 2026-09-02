#!/usr/bin/env python3
"""Shadow race: score band rules on the canary's band_anchor records.

The live engine writes one ``band_anchor`` record per btc-updown-5m window
and anchor second into its session log: exchange price, window open,
decision margin and, for both sides, the budget-aware FOK quote (VWAP, worst
price, shares) the venue offered at that instant.  Nothing here touches
money: the champion (by default the live band rule, replayed like every
challenger on the anchor quote: the engine's capital, wallet and breaker
gates are not modelled, so its trades need not match the canary's order
log) and any challenger band rules are replayed offline on the same
anchors, scored against the official Gamma outcomes and compared with a
paired anytime-valid e-process (scripts/evidence_accrual.py
``update_signed``).

Per window and rule: the anchor at the rule's ``decision_second`` (a window
is paired only when both rules' anchors exist).  The engine's own cycle
gates come first and apply to every rule, because they are not the rule's:
a fresh best ask on BOTH sides (``book_age_s`` / ``best_ask`` null on either
side is the ``fresh_outcome_book_unavailable`` skip) and a non-null
``stake_usd`` (null is the live sizing policy declining the window, e.g.
kelly_lo's <= 0.70 bucket; the quote is then sized at the fallback
``quote_budget_usd`` and is not a trade).  Then direction from the sign of
the anchor margin, gated by the rule's USD floor and sigma floor (band_lane's
sigma over Binance 1s closes from the margin-study cache); the momentum
side's VWAP plus the complement's best ask must lie in [0.90, 1.10] (the
engine's ``band_pair_incoherent`` skip) and the momentum side's quote must
clear the band exactly as the engine's
``BandPolicyParams::quote_clears_band`` does (vwap > floor and worst <= cap).
A rule that trades scores net per 1 USD staked with band_lane's fee model:
win -> 1/break_even(vwap) - 1, i.e. (1/vwap - 1) minus the fee per USD
1/vwap - 1/(vwap + taker_fee(vwap)); loss -> -1.  A rule that does not trade
scores 0.  The paired difference d = challenger - champion is divided by a
constant fixed for the race, the largest 1/break_even(favorite_price_floor)
among the rules, so |d| <= 1 always holds and ``update_signed``'s clamp
never fires: the sign of E[d] is preserved and the null is E[d] <= 0, the
challenger is no better than the champion per window.  (Clipping instead
would test E[clip(d)] <= 0, which can be positive while E[d] is negative
when a challenger wins small and loses big against the champion.)  The
anchor quote is sized at the live target before capital caps, so the race
is rule-vs-rule on a common budget, not a replica of the live ledger.
Promotion is Bonferroni over the run's K challengers, e >= K / alpha, so the
family-wise false-promote rate of one run is alpha; challengers raced
against the same champion history in other runs are the operator's count.

Read-only on the VPS: ``--pull`` only rsyncs the canary's session logs into
the local mirror.
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import sqlite3
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

SCRIPTS = Path(__file__).resolve().parent
ROOT = SCRIPTS.parent
sys.path.insert(0, str(SCRIPTS))
import factory_generator  # noqa: E402


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


band_lane = _load("band_lane", "band_lane.py")
evidence_accrual = _load("evidence_accrual", "evidence_accrual.py")

DEFAULT_SESSIONS_DIR = ROOT / "logs/band-canary-mirror/sessions"
VPS_SESSIONS = "vps:/opt/polymomentum/logs/band-canary/sessions/"
DEFAULT_RESEARCH_DB = ROOT / "logs/strategy-research/research.sqlite3"
DEFAULT_LOOP_CONFIG = ROOT / "deploy/strategy-research-loop.json"
STAGE = "champion_challenger_race"
LIVE_RULE = {
    "margin_floor_usd": 50,
    "margin_floor_sigma": 0.0,
    "decision_second": 240,
    "direction": "both",
    "favorite_price_floor": 0.55,
    "favorite_price_cap": 0.92,
}
WINDOW_S = band_lane.WINDOW_S
# Windows scanned before the first anchor so the sigma history can be full.
SIGMA_HISTORY_WINDOWS = 2 * band_lane.SIGMA_LOOKBACK_WINDOWS
SCALING_NOTE = (
    "d = (challenger - champion net per USD) / %.4f, the largest 1/break_even(favorite_price_floor) "
    "among the rules, so |d| <= 1 and update_signed's clamp never fires; a clipped count above 0 "
    "means the e-value tests E[clip(d)] <= 0, not E[d] <= 0"
)


def load_anchors(sessions_dir: Path) -> Dict[int, Dict[int, Dict[str, Any]]]:
    """band_anchor records by window_start (ts - elapsed_s on the 300 s grid)
    then anchor second; a duplicate keeps the record closest to its anchor."""
    anchors: Dict[int, Dict[int, Dict[str, Any]]] = {}
    for path in sorted(Path(sessions_dir).rglob("*.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, Mapping) or record.get("type") != "band_anchor":
                    continue
                try:
                    ts = float(record["ts"])
                    elapsed = float(record["elapsed_s"])
                    anchor_s = int(record["anchor_s"])
                except (KeyError, TypeError, ValueError):
                    continue
                window_start = int(round((ts - elapsed) / WINDOW_S)) * WINDOW_S
                current = anchors.setdefault(window_start, {}).get(anchor_s)
                if current is None or elapsed < float(current["elapsed_s"]):
                    anchors[window_start][anchor_s] = dict(record)
    return anchors


def resolve_rule(text: str, research_db: Path) -> Dict[str, Any]:
    """A JSON band rule, or a band-lane fingerprint looked up read-only in
    the research ledger's hypotheses.proposal_json."""
    text = text.strip()
    if text.startswith("{"):
        return band_lane.normalized_band_rule(json.loads(text))
    connection = sqlite3.connect("file:%s?mode=ro" % Path(research_db), uri=True)
    try:
        row = connection.execute(
            "SELECT proposal_json FROM hypotheses WHERE fingerprint = ? AND lane = ?",
            (text, band_lane.LANE),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise ValueError("unknown band fingerprint %s" % text)
    return band_lane.normalized_band_rule(json.loads(row[0])["rule"])


def resolve_outcomes(
    cache: Any, window_starts: Sequence[int], now_ts: int
) -> Tuple[Dict[int, Optional[str]], Dict[str, int]]:
    """Official outcomes from band_lane's Gamma cache.  Windows missing from
    it are fetched once eligible (window end + RESOLUTION_LAG_S) and, like
    BandCache.refresh, recorded as final None only after
    UNRESOLVED_FINAL_AFTER_S."""
    last = band_lane.last_eligible_window_start(now_ts)
    counts = {"fetched": 0, "errors": 0}
    fetched: Dict[str, Optional[str]] = {}
    for window_start in sorted(window_starts):
        key = str(window_start)
        if key in cache.outcomes or window_start > last:
            continue
        try:
            market = cache.fetch_market(window_start)
        except Exception:
            counts["errors"] += 1
            continue
        parsed = band_lane.parse_market(market) if market else None
        official = parsed["official"] if parsed else None
        if official is None and now_ts - (window_start + WINDOW_S) < band_lane.UNRESOLVED_FINAL_AFTER_S:
            continue
        cache.outcomes[key] = fetched[key] = official
        counts["fetched"] += 1
    if fetched:
        # The file is shared with band_lane's research loop, which may have
        # saved its own fetches since this cache was loaded: re-read and add
        # only this run's windows, so neither writer loses the other's.
        path = cache.margin_dir / "gamma_outcomes.json"
        try:
            on_disk = json.loads(path.read_text()) if path.is_file() else {}
        except (OSError, ValueError):
            on_disk = {}
        cache.outcomes.update(on_disk)
        cache.outcomes.update(fetched)
        cache.save_outcomes()
    return {ws: cache.outcomes.get(str(ws)) for ws in window_starts}, counts


def binance_sigmas(
    cache: Any, window_starts: Sequence[int], decision_second: int
) -> Dict[int, Optional[float]]:
    """band_lane's sigma per window: population stdev of |Binance margin| at
    decision_second over the SIGMA_LOOKBACK_WINDOWS preceding windows that
    have both closes; None until that history is full."""
    wanted = set(int(ws) for ws in window_starts)
    history: List[float] = []
    sigmas: Dict[int, Optional[float]] = {}
    first = min(wanted) - SIGMA_HISTORY_WINDOWS * WINDOW_S
    for window_start in range(first, max(wanted) + 1, WINDOW_S):
        if window_start in wanted:
            sigmas[window_start] = (
                statistics.pstdev(history[-band_lane.SIGMA_LOOKBACK_WINDOWS :])
                if len(history) >= band_lane.SIGMA_LOOKBACK_WINDOWS
                else None
            )
        open_close = cache.close(window_start)
        decision_close = cache.close(window_start + decision_second)
        if open_close is not None and decision_close is not None:
            history.append(abs(float(decision_close) - float(open_close)))
    return sigmas


def rule_trade(
    rule: Mapping[str, Any], anchor: Mapping[str, Any], sigma: Optional[float]
) -> Optional[Dict[str, Any]]:
    """The trade `rule` takes on one anchor, or None when it does not trade."""
    # The engine's cycle gates, replayed for every rule: pick_book_prices
    # needs a fresh best ask on both sides, and the live sizing policy must
    # size the window (stake_usd null: stale books, or kelly_lo declined the
    # bucket; the quote at quote_budget_usd is then not a trade).
    for side in ("up", "down"):
        book = anchor.get(side) or {}
        if book.get("book_age_s") is None or book.get("best_ask") is None:
            return None
    if anchor.get("stake_usd") is None:
        return None
    margin = anchor.get("margin")
    if margin is None or float(margin) == 0.0:
        return None
    margin = float(margin)
    direction = "up" if margin > 0 else "down"
    if rule["direction"] != "both" and direction != rule["direction"]:
        return None
    if abs(margin) < float(rule["margin_floor_usd"]):
        return None
    floor_sigma = float(rule["margin_floor_sigma"])
    if floor_sigma > 0 and (sigma is None or abs(margin) < floor_sigma * sigma):
        return None
    quote = anchor.get(direction) or {}
    vwap, worst = quote.get("vwap"), quote.get("worst")
    if vwap is None or worst is None or float(vwap) <= 0.0 or float(worst) <= 0.0:
        return None  # no executable quote for the stake
    vwap, worst = float(vwap), float(worst)
    # evaluate_band_opportunity's pair-coherence gate (band_pair_incoherent):
    # the momentum VWAP plus the fresh complement best ask within [0.90, 1.10].
    complement = anchor.get("down" if direction == "up" else "up") or {}
    if not 0.90 <= vwap + float(complement["best_ask"]) <= 1.10:
        return None
    # BandPolicyParams::quote_clears_band: VWAP above the floor, FOK worst within the cap.
    if not (vwap > float(rule["favorite_price_floor"]) and worst <= float(rule["favorite_price_cap"])):
        return None
    return {
        "direction": direction,
        "vwap": vwap,
        "worst": worst,
        "stake_usd": float(anchor["stake_usd"]),
    }


def trade_score(vwap: float, won: bool) -> float:
    """Net per 1 USD staked, band_lane.band_entry_economics's formula: 1 USD
    buys 1/(vwap + taker_fee(vwap)) shares paying 1 each on a win."""
    return (1.0 / band_lane.break_even(vwap) - 1.0) if won else -1.0


def _mean(values: Sequence[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def race(
    anchors: Mapping[int, Mapping[int, Mapping[str, Any]]],
    outcomes: Mapping[int, Optional[str]],
    sigmas: Mapping[int, Mapping[int, Optional[float]]],
    champion: Mapping[str, Any],
    challengers: Sequence[Mapping[str, Any]],
    alpha: float,
) -> Dict[str, Any]:
    """Score every rule on the resolved windows and pair each challenger
    with the champion in window order."""
    rules = [champion] + list(challengers)
    # Fixed for the race so d stays linear in the raw score difference: above
    # its floor a rule wins under 1/break_even(floor) - 1 per USD and loses
    # -1, so |challenger - champion| < scale and the e-process clamp is idle.
    scale = max(1.0 / band_lane.break_even(float(rule["favorite_price_floor"])) for rule in rules)
    resolved = sorted(ws for ws in anchors if outcomes.get(ws) in ("up", "down"))
    trades: List[Dict[int, Dict[str, Any]]] = []
    stats: List[Dict[str, Any]] = []
    for rule in rules:
        second = int(rule["decision_second"])
        taken: Dict[int, Dict[str, Any]] = {}
        seen = 0
        for window_start in resolved:
            anchor = anchors[window_start].get(second)
            if anchor is None:
                continue
            seen += 1
            trade = rule_trade(rule, anchor, sigmas.get(second, {}).get(window_start))
            if trade is None:
                continue
            trade["won"] = trade["direction"] == outcomes[window_start]
            trade["score"] = trade_score(trade["vwap"], trade["won"])
            taken[window_start] = trade
        trades.append(taken)
        stats.append(
            {
                "rule": dict(rule),
                "fingerprint": band_lane.band_fingerprint(rule),
                "label": band_lane.compact_band_rule(rule),
                "windows": seen,
                "trades": len(taken),
                "wins": sum(1 for trade in taken.values() if trade["won"]),
                "net_per_usd": _mean([trade["score"] for trade in taken.values()]),
                "net_at_stake": sum(trade["score"] * trade["stake_usd"] for trade in taken.values()),
            }
        )
    champion_second = int(champion["decision_second"])
    # Bonferroni over this run's challengers: each e-process is bounded by
    # Ville at its own level, so promoting any of K at e >= K / alpha keeps
    # the family-wise false-promote rate at alpha (union bound).
    promote_e = max(1, len(challengers)) / alpha
    paired: List[Dict[str, Any]] = []
    for index, rule in enumerate(challengers, start=1):
        second = int(rule["decision_second"])
        process = evidence_accrual.EProcess()
        differences: List[float] = []
        counts = {"wins": 0, "losses": 0, "overlap": 0, "clipped": 0}
        for window_start in resolved:
            if champion_second not in anchors[window_start] or second not in anchors[window_start]:
                continue
            base, other = trades[0].get(window_start), trades[index].get(window_start)
            if base is None and other is None:
                continue
            d = ((other["score"] if other else 0.0) - (base["score"] if base else 0.0)) / scale
            if base and other and base["direction"] == other["direction"]:
                counts["overlap"] += 1
            if abs(d) > 1.0:
                counts["clipped"] += 1
            d = max(-1.0, min(1.0, d))
            counts["wins"] += int(d > 0.0)
            counts["losses"] += int(d < 0.0)
            differences.append(d)
            process.update_signed(d)
        e_value = process.e_value()
        if e_value >= promote_e:
            verdict = "promote"
        elif e_value <= evidence_accrual.FUTILITY_E:
            verdict = "kill"
        else:
            verdict = "continue"
        paired.append(
            {
                **stats[index],
                "paired": {
                    "n": process.n,
                    "mean_d": _mean(differences),
                    **counts,
                    "e_value": e_value,
                    "verdict": verdict,
                },
            }
        )
    return {
        "alpha": alpha,
        "promote_e": promote_e,
        "windows": {
            "anchored": len(anchors),
            "resolved": len(resolved),
            "unresolved": len(anchors) - len(resolved),
        },
        "d_scale": scale,
        "scaling": SCALING_NOTE % scale,
        "champion": stats[0],
        "challengers": paired,
    }


def _number(value: Optional[float], digits: int = 4) -> str:
    return "-" if value is None else "%.*f" % (digits, value)


def markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "| rule | fingerprint | windows | trades | wins | net/USD | net@stake |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    champion = report["champion"]
    for role, row in [("champion", champion)] + [("challenger", row) for row in report["challengers"]]:
        lines.append(
            "| %s (%s) | %s | %d | %d | %d | %s | %s |"
            % (
                row["label"],
                role,
                row["fingerprint"][:12],
                row["windows"],
                row["trades"],
                row["wins"],
                _number(row["net_per_usd"]),
                _number(row["net_at_stake"], 2),
            )
        )
    lines += [
        "",
        "| challenger | n | mean d | d>0 | d<0 | overlap | clipped | e-value | verdict |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in report["challengers"]:
        paired = row["paired"]
        lines.append(
            "| %s | %d | %s | %d | %d | %d | %d | %s | %s |"
            % (
                row["fingerprint"][:12],
                paired["n"],
                _number(paired["mean_d"]),
                paired["wins"],
                paired["losses"],
                paired["overlap"],
                paired["clipped"],
                _number(paired["e_value"], 3),
                paired["verdict"],
            )
        )
    windows = report["windows"]
    lines += [
        "",
        "windows: anchored=%d resolved=%d unresolved=%d; promote at e >= %.1f (alpha %s Bonferroni over %d "
        "challengers in this run; challengers raced against this champion history in other runs are not counted), "
        "kill at e <= %s"
        % (
            windows["anchored"],
            windows["resolved"],
            windows["unresolved"],
            report["promote_e"],
            report["alpha"],
            len(report["challengers"]),
            evidence_accrual.FUTILITY_E,
        ),
        report["scaling"],
        "champion = the band rule replayed on the anchor quote at the pre-cap budget, like every challenger; "
        "the engine's fresh-book, sizing-policy and pair-coherence gates are replayed, its capital, wallet and "
        "breaker gates are not, so trades here need not match the canary's order log",
    ]
    if any(row["paired"]["clipped"] for row in report["challengers"]):
        lines.append("WARNING: clipped pairs present; the e-value tests E[clip(d)] <= 0, not E[d] <= 0")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None, cache: Any = None, now_ts: Optional[int] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sessions-dir", type=Path, default=DEFAULT_SESSIONS_DIR)
    parser.add_argument(
        "--pull",
        action="store_true",
        help="rsync the canary's session logs from the VPS into --sessions-dir first (never writes to the VPS)",
    )
    parser.add_argument("--champion", default=json.dumps(LIVE_RULE), help="JSON band rule or band-lane fingerprint")
    parser.add_argument("--challengers", nargs="+", required=True, help="JSON band rules or band-lane fingerprints")
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument(
        "--json",
        action="store_true",
        help="also write <state_dir>/band_race/<utc>.json and one trial-ledger row per challenger",
    )
    parser.add_argument("--research-db", type=Path, default=DEFAULT_RESEARCH_DB)
    parser.add_argument("--loop-config", type=Path, default=DEFAULT_LOOP_CONFIG)
    args = parser.parse_args(argv)
    if not 0.0 < args.alpha < 1.0:
        parser.error("--alpha must be in (0, 1)")
    if args.pull:
        args.sessions_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run(["rsync", "-az", VPS_SESSIONS, str(args.sessions_dir) + "/"], check=True)
    champion = resolve_rule(args.champion, args.research_db)
    challengers = [resolve_rule(text, args.research_db) for text in args.challengers]
    anchors = load_anchors(args.sessions_dir)
    if not anchors:
        print("no band_anchor records under %s" % args.sessions_dir)
        return 1
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    cache = cache or band_lane.BandCache()
    window_starts = sorted(anchors)
    outcomes, fetch = resolve_outcomes(cache, window_starts, now_ts)
    sigmas: Dict[int, Dict[int, Optional[float]]] = {}
    sigma_seconds = sorted(
        {int(rule["decision_second"]) for rule in [champion] + challengers if float(rule["margin_floor_sigma"]) > 0}
    )
    if sigma_seconds:
        cache.load_closes(
            window_starts[0] - SIGMA_HISTORY_WINDOWS * WINDOW_S,
            window_starts[-1] + max(band_lane.BAND_DECISION_SECONDS),
            now_ts,
        )
        for second in sigma_seconds:
            sigmas[second] = binance_sigmas(cache, window_starts, second)
    report = race(anchors, outcomes, sigmas, champion, challengers, args.alpha)
    report.update(
        {
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "sessions_dir": str(args.sessions_dir),
            "outcome_fetch": fetch,
        }
    )
    print(markdown(report))
    if args.json:
        config = json.loads(args.loop_config.read_text())
        state_dir = Path(str(config["state_dir"]))
        if not state_dir.is_absolute():
            state_dir = ROOT / state_dir
        path = state_dir / "band_race" / (dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        for row in report["challengers"]:
            factory_generator.append_trial_entry(
                config,
                row["fingerprint"],
                STAGE,
                row["paired"]["verdict"],
                n=row["paired"]["n"],
                wins=row["paired"]["wins"],
            )
        print("written %s" % path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
