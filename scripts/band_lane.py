#!/usr/bin/env python3
"""Band lane for the strategy research loop: the public-print screen of grammar C.

The live strategy is the band mechanism.  At ``decision_second`` into a
btc-updown-5m window it compares the Binance BTCUSDT 1s close with the close
at the window open (the decision margin); when |margin| clears the floor it
buys the momentum side as a taker if that side's ask is inside
(favorite_price_floor, favorite_price_cap].  Official resolution comes from
Gamma/UMA, so the signal basis and the label are different sources.

The proposer is deterministic: it walks the grammar C cells (BAND_GRID_V2,
docs/profitability_basement_2026-09-18.md section C: ask floor 0.80,
direction both, no sigma floor), registrable cells first (floor >= 75 and
decision >= 180), and proposes the first cell without a hypothesis.  Each
cell is screened feed-forward on public cached data and accrues
anytime-valid e-value evidence (scripts/evidence_accrual.py via
Ledger.accrue) when it survives:

  stage 1  band_signal_screen     momentum sign vs official outcome
  stage 2  band_entry_economics   realized win rate vs fee-aware break-even at
                                  the public BUY print the entry semantics
                                  select within the cell's patience
                                  (ENTRY_SEMANTICS; the tripwire holds
                                  too-good survivors as manual_audit)
  stage 3  fresh_public_accrual   e-process over strictly newer windows; the
                                  family is decided by e-BH at the campaign
                                  size pre-registered as
                                  lanes.band_mechanisms.campaign_n

Public prints are an upper bound on executability: the ladder model of
scripts/executable_truth.py is the truth (CLAUDE.md section 4).  The
hypotheses proposed under the legacy six-field grammar (BAND_GRID) stay
readable, normalized_band_rule accepts both shapes, so their accrual,
rescreen and rescore continue unchanged.

Data is public only and cached: Binance 1s closes and Gamma outcomes share
scripts/margin_floor_study.py's cache files; entry prints live under
logs/strategy-research/band_lane_cache/prints/<ws>_<decision_second>.json
(signal_entry = first BUY print of the signal token after the decision,
signal_prints = every such print in the 30 s entry window).
Network work is bounded per cycle and always oldest-first, so the cached
window set is a contiguous prefix: nothing at or before the newest cached
window can arrive later and be double counted or silently skipped.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import itertools
import json
import math
import os
import statistics
import sys
import time
import urllib.parse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evidence_accrual  # noqa: E402
import factory_generator  # noqa: E402
import margin_floor_study  # noqa: E402
from adaptation_persistence_study import DATA_API, http_json, taker_fee  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
LANE = "band_mechanisms"
# v2: stage 1 no longer gates signal accuracy against entry break-even
# (wilson_above_break_even and recent_above_break_even dropped) and the
# tripwire is WR > 0.995 at n >= 100; fingerprints scored under v1 stay
# distinct in the ledger (CLAUDE.md section 5).
BAND_EVALUATOR_VERSION = "band_public_v2"
PRINTS_DIR = ROOT / "logs/strategy-research/band_lane_cache/prints"

# The print cache's decision columns: one prints file per (window, second).
# A change here needs a --rebuild-prints.  Grammar C's 150 s cells have no
# column and are scored by scripts/executable_truth.py only.
BAND_DECISION_SECONDS: Tuple[int, ...] = (180, 210, 240, 270)
# Legacy grammar (the six-field rules of the LLM-era lane).  Kept so the
# hypotheses proposed under it stay readable (accrual, rescreen, rescore, the
# race's live rule); the proposer no longer draws from it.
BAND_GRID: Dict[str, Tuple[Any, ...]] = {
    "margin_floor_usd": (0, 25, 50, 75, 100),
    "margin_floor_sigma": (0.0, 0.5, 1.0, 1.5),
    "decision_second": BAND_DECISION_SECONDS,
    "direction": ("both", "up", "down"),
    "favorite_price_floor": (0.55, 0.60, 0.65, 0.70),
    "favorite_price_cap": (0.80, 0.85, 0.92),
}
_CASTS = {
    "margin_floor_usd": int,
    "margin_floor_sigma": float,
    "decision_second": int,
    "direction": str,
    "favorite_price_floor": float,
    "favorite_price_cap": float,
}
# Grammar C (docs/profitability_basement_2026-09-18.md section C), the
# proposer's grammar, scored here on public prints and by
# scripts/executable_truth.py on ladder rows.  Ask floor 0.80 and direction
# both are fixed (up/down is a tripwire, never a rule), no sigma floor; 336
# cells, the 189 with floor >= 75 and decision >= 180 registrable, floor 50
# the control.
BAND_GRID_V2_VERSION = "band_grid_v2"
BAND_V2_ASK_FLOOR = 0.80
BAND_GRID_V2: Dict[str, Tuple[Any, ...]] = {
    "decision_second": (150, 180, 210, 240),
    "margin_floor_usd": (50, 75, 100, 150),
    "favorite_price_cap": (0.92, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99),
    "patience_s": (0, 15, 30),
}
_CASTS_V2 = {
    "decision_second": int,
    "margin_floor_usd": int,
    "favorite_price_cap": float,
    "patience_s": int,
}
BAND_V2_REGISTRABLE_MIN_FLOOR = 75
BAND_V2_REGISTRABLE_MIN_DECISION = 180
PROPOSAL_SOURCE = "grid_v2"

WINDOW_S = 300
ENTRY_WINDOW_S = 30
# A window is eligible once its end is at least this far in the past.
RESOLUTION_LAG_S = 900
# Windows still unresolved or unlisted on Gamma this long after their end are
# recorded as null (final).
UNRESOLVED_FINAL_AFTER_S = 7200
SIGMA_LOOKBACK_WINDOWS = 12
RECENT_SECONDS = 48 * 3600
DAY_S = 86400
# A day file is extended when its newest close is older than this.
CLOSES_TAIL_GRACE_S = 60
BINANCE_PAUSE_S = 0.15
API_PAUSE_S = 0.12
# /trades pages newest-first and `offset` walks back in time (verified
# 2026-09-08: offset=500 is the next-older 500 with no overlap, past the
# end an empty list).  A window's tape is covered once it reaches back to
# TRADES_COVERAGE_S: every anchor second in [180, 270] then has its prints.
TRADES_PAGE_SIZE = 500
TRADES_MAX_PAGES = 8
TRADES_COVERAGE_S = 150
# Every prints row records the writer that produced it; a bump makes
# --rebuild-prints re-fetch every older row.  2: within-second tie order of
# entry_prints (rows before 2 held the LAST print of a second as the first).
PRINTS_WRITER_VERSION = 2
# A support-only rejection is re-scored only after this many new windows
# (~8 h) so the trial ledger does not gain a row every 15-minute tick.
RESCREEN_MIN_NEW_WINDOWS = 96

# Statuses that keep accruing fresh evidence.  A promote_candidate stays in
# the family: e-BH re-decides the whole family every cycle, so the flag can
# lapse when the family grows or its own e-value falls back.
ACCRUAL_STATUSES = {"stage_2_survivor", "accruing", "promote_candidate", "manual_audit"}
# Entry-price semantics for stage 2 and accrual.  one_look: the first BUY
# print after the decision is the only look, out of band means no trade
# (the factory's rule so far).  patient: the first in-band BUY print within
# the entry window, the live engine's wait of up to 30 s for an in-band ask.
ENTRY_SEMANTICS = ("one_look", "patient")
# Tripwire: a realized win rate this high on this much support is more
# likely a tape or label defect than an edge (CLAUDE.md section 5: WR >
# 0.995 at n >= 100; the earlier 0.97 held the edge region, whose break-even
# at a 0.97 cap is 0.972).  The candidate holds status manual_audit (no
# promotion) until its fingerprint is listed under
# lanes.band_mechanisms.audit_cleared by an operator.
TRIPWIRE_WIN_RATE = 0.995
TRIPWIRE_MINIMUM_N = 100


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _seconds_since(raw: Optional[str]) -> float:
    if not raw:
        return math.inf
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return math.inf
    return (dt.datetime.now(dt.timezone.utc) - parsed).total_seconds()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("%s.tmp.%s" % (path.name, os.getpid()))
    temporary.write_text(text)
    os.replace(str(temporary), str(path))


def sign(value: float) -> int:
    return 1 if value > 0 else (-1 if value < 0 else 0)


def break_even(price: float) -> float:
    """Win probability needed to buy at `price` with the taker fee (payout 1/share)."""
    return float(price) + taker_fee(float(price))


def wilson_lower(wins: int, total: int) -> Optional[float]:
    return margin_floor_study.wilson_lo(wins, total) if total else None


# --- grammar -----------------------------------------------------------------


def _normalize_rule(
    raw: Mapping[str, Any], grid: Mapping[str, Sequence[Any]], casts: Mapping[str, Callable[[Any], Any]]
) -> Dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != set(grid):
        raise ValueError("invalid band rule fields")
    rule: Dict[str, Any] = {}
    for field, cast in casts.items():
        value = raw[field]
        try:
            normalized = cast(value)
        except (TypeError, ValueError):
            raise ValueError("%s is not a %s" % (field, cast.__name__))
        # int(50.7) == 50 and int("50") == 50 would silently coerce.
        if isinstance(value, bool) or normalized != value:
            raise ValueError("%s is not a %s" % (field, cast.__name__))
        if normalized not in grid[field]:
            raise ValueError("%s is outside the band grid" % field)
        rule[field] = normalized
    return rule


def normalized_band_rule(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """A grammar C rule (BAND_GRID_V2) or a legacy six-field rule (BAND_GRID),
    told apart by their field sets; anything else is invalid."""
    if isinstance(raw, Mapping) and set(raw) == set(BAND_GRID_V2):
        return _normalize_rule(raw, BAND_GRID_V2, _CASTS_V2)
    return _normalize_rule(raw, BAND_GRID, _CASTS)


def normalized_band_rule_v2(raw: Mapping[str, Any]) -> Dict[str, Any]:
    return _normalize_rule(raw, BAND_GRID_V2, _CASTS_V2)


def grid_v2_rules() -> List[Dict[str, Any]]:
    return [dict(zip(BAND_GRID_V2, values)) for values in itertools.product(*BAND_GRID_V2.values())]


def registrable_v2(rule: Mapping[str, Any]) -> bool:
    return (
        int(rule["margin_floor_usd"]) >= BAND_V2_REGISTRABLE_MIN_FLOOR
        and int(rule["decision_second"]) >= BAND_V2_REGISTRABLE_MIN_DECISION
    )


def rule_params(rule: Mapping[str, Any]) -> Dict[str, Any]:
    """The evaluation parameters of a rule of either shape (no grid
    validation: normalized_band_rule does that).  A grammar C rule reads ask
    floor BAND_V2_ASK_FLOOR, direction both and no sigma floor; a legacy rule
    has no patience (None: the whole entry window)."""
    patience = rule.get("patience_s")
    return {
        "decision_second": int(rule["decision_second"]),
        "margin_floor_usd": float(rule["margin_floor_usd"]),
        "margin_floor_sigma": float(rule.get("margin_floor_sigma", 0.0)),
        "direction": str(rule.get("direction", "both")),
        "favorite_price_floor": float(rule.get("favorite_price_floor", BAND_V2_ASK_FLOOR)),
        "favorite_price_cap": float(rule["favorite_price_cap"]),
        "patience_s": None if patience is None else int(patience),
    }


def band_fingerprint(rule: Mapping[str, Any]) -> str:
    payload = {
        "lane": LANE,
        "rule": normalized_band_rule(rule),
        "evaluator_version": BAND_EVALUATOR_VERSION,
    }
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def compact_band_rule(rule: Mapping[str, Any]) -> str:
    if "patience_s" in rule:
        return "band t=%ss floor=$%s cap=%s patience=%ss" % (
            rule.get("decision_second"),
            rule.get("margin_floor_usd"),
            rule.get("favorite_price_cap"),
            rule.get("patience_s"),
        )
    return "band floor=$%s sigma=%s t=%ss dir=%s ask=(%s,%s]" % (
        rule.get("margin_floor_usd"),
        rule.get("margin_floor_sigma"),
        rule.get("decision_second"),
        rule.get("direction"),
        rule.get("favorite_price_floor"),
        rule.get("favorite_price_cap"),
    )


def _deterministic_proposal(rule: Mapping[str, Any], title: str, rationale: str) -> Dict[str, Any]:
    return {
        "title": title,
        "rationale": rationale,
        "expected_failure_mode": (
            "The band is margin-conditional: a chop regime with small decision margins "
            "pushes the realized win rate below the fee-aware break-even."
        ),
        "rule": normalized_band_rule(rule),
    }


# --- public data cache -------------------------------------------------------


def last_eligible_window_start(now_ts: int) -> int:
    return ((int(now_ts) - RESOLUTION_LAG_S - WINDOW_S) // WINDOW_S) * WINDOW_S


# Network discipline for the lane (2026-09-23 outage: three days of 45-minute
# cycles spent in 30 s x 3 retries per request). Lane fetches use short
# timeouts, and after NETWORK_ERROR_BUDGET consecutive failures in one
# process every further fetch raises NetworkUnavailable immediately so a
# cycle fails fast and the next cycle retries.
LANE_HTTP_TIMEOUT_S = 12.0
LANE_HTTP_RETRIES = 2
NETWORK_ERROR_BUDGET = 3
_consecutive_network_errors = 0


class NetworkUnavailable(RuntimeError):
    pass


def lane_http_json(url: str) -> Any:
    global _consecutive_network_errors
    if _consecutive_network_errors >= NETWORK_ERROR_BUDGET:
        raise NetworkUnavailable("network error budget exhausted (%d)" % _consecutive_network_errors)
    try:
        value = http_json(url, retries=LANE_HTTP_RETRIES, timeout=LANE_HTTP_TIMEOUT_S)
    except Exception:
        _consecutive_network_errors += 1
        raise
    _consecutive_network_errors = 0
    return value


def reset_network_error_budget() -> None:
    global _consecutive_network_errors
    _consecutive_network_errors = 0


def fetch_binance_closes(start_ts: int, end_ts: int) -> Dict[str, float]:
    """1s closes for [start_ts, end_ts) keyed by epoch-second string, the
    scripts/margin_floor_study.py cache format."""
    prices: Dict[str, float] = {}
    cursor = int(start_ts)
    while cursor < end_ts:
        rows = lane_http_json(
            "%s?symbol=BTCUSDT&interval=1s&startTime=%d&limit=1000"
            % (margin_floor_study.BINANCE, cursor * 1000)
        )
        if not rows:
            break
        for row in rows:
            ts = int(row[0]) // 1000
            if start_ts <= ts < end_ts:
                prices[str(ts)] = float(row[4])
        cursor = int(rows[-1][0]) // 1000 + 1
        time.sleep(BINANCE_PAUSE_S)
    return prices


def fetch_gamma_market(window_start: int) -> Optional[Dict[str, Any]]:
    markets = lane_http_json(
        "%s/markets?slug=btc-updown-5m-%d&closed=true" % (margin_floor_study.GAMMA, window_start)
    )
    time.sleep(API_PAUSE_S)
    return markets[0] if markets else None


def fetch_data_api_trades(condition_id: str, window_start: int) -> Dict[str, Any]:
    """Merged newest-first /trades pages until the oldest trade is at or
    before window_start + TRADES_COVERAGE_S or a short page ends the tape,
    at most TRADES_MAX_PAGES pages.  The coverage rides along so the cached
    prints file records how far back its tape reached."""
    cutoff = int(window_start) + TRADES_COVERAGE_S
    trades: List[Dict[str, Any]] = []
    pages = 0
    complete = False
    while pages < TRADES_MAX_PAGES:
        params = urllib.parse.urlencode(
            {"market": condition_id, "limit": TRADES_PAGE_SIZE, "offset": pages * TRADES_PAGE_SIZE}
        )
        page = list(lane_http_json("%s/trades?%s" % (DATA_API, params)) or [])
        time.sleep(API_PAUSE_S)
        pages += 1
        trades.extend(page)
        if len(page) < TRADES_PAGE_SIZE:
            complete = True
            break
        if min(int(trade.get("timestamp", 0)) for trade in page) <= cutoff:
            break
    oldest = min((int(trade.get("timestamp", 0)) for trade in trades), default=None)
    return {
        "trades": trades,
        "coverage": {
            "oldest_offset_s": None if oldest is None else oldest - int(window_start),
            "pages": pages,
            "complete": complete,
        },
    }


def print_covered(row: Mapping[str, Any]) -> bool:
    """A prints row whose tape reaches back to TRADES_COVERAGE_S or was read
    to its end.  Rows written before pagination carry no coverage."""
    coverage = row.get("coverage") or {}
    oldest = coverage.get("oldest_offset_s")
    return bool(coverage.get("complete")) or (oldest is not None and int(oldest) <= TRADES_COVERAGE_S)


def parse_market(market: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """Token ids by outcome name plus the official winner (None while unresolved)."""

    def listed(value: Any) -> List[Any]:
        return json.loads(value) if isinstance(value, str) else list(value or [])

    names = [str(name).lower() for name in listed(market.get("outcomes"))]
    tokens = [str(token) for token in listed(market.get("clobTokenIds"))]
    condition_id = market.get("conditionId")
    if not condition_id or len(names) != 2 or len(tokens) != 2:
        return None
    official = None
    if market.get("umaResolutionStatus") == "resolved":
        winners = [
            name
            for name, price in zip(names, listed(market.get("outcomePrices")))
            if float(price) > 0.5
        ]
        official = winners[0] if len(winners) == 1 else None
    return {
        "condition_id": str(condition_id),
        "token_by_name": dict(zip(names, tokens)),
        "official": official,
    }


def entry_prints(
    trades: Sequence[Mapping[str, Any]], token: Optional[str], decision_ts: int
) -> List[List[float]]:
    """Public BUY prints of `token` within (decision_ts, decision_ts + 30s] as
    [seconds after the decision, price] oldest-first.

    Timestamps are whole seconds and `trades` is the merged newest-first
    /trades tape, so prints stamped in the same second are ordered by
    reverse tape position (a larger index is older): the first print of a
    second is the one the API listed last.  The 1s close at decision_ts is
    the last trade of that second, so a print stamped decision_ts can
    precede the signal: it is excluded."""
    prints: List[List[float]] = []
    ordered = sorted(enumerate(trades), key=lambda item: (int(item[1].get("timestamp", 0)), -item[0]))
    for _, trade in ordered:
        if trade.get("side") != "BUY" or str(trade.get("asset")) != token:
            continue
        ts = int(trade.get("timestamp", 0))
        if decision_ts < ts <= decision_ts + ENTRY_WINDOW_S:
            prints.append([ts - decision_ts, float(trade["price"])])
    return prints


def entry_print(trades: Sequence[Mapping[str, Any]], token: Optional[str], decision_ts: int) -> Optional[float]:
    """First public BUY of `token` within (decision_ts, decision_ts + 30s]."""
    prints = entry_prints(trades, token, decision_ts)
    return prints[0][1] if prints else None


class BandCache:
    """Disk caches: Binance 1s closes and Gamma outcomes in the margin-study
    format/location, entry prints per (window_start, decision_second).

    Fetchers are injectable so tests never touch the network."""

    def __init__(
        self,
        margin_dir: Path = margin_floor_study.CACHE,
        prints_dir: Path = PRINTS_DIR,
        fetch_closes: Callable[[int, int], Mapping[str, float]] = fetch_binance_closes,
        fetch_market: Callable[[int], Optional[Mapping[str, Any]]] = fetch_gamma_market,
        fetch_trades: Callable[[str, int], Mapping[str, Any]] = fetch_data_api_trades,
    ) -> None:
        self.margin_dir = Path(margin_dir)
        self.prints_dir = Path(prints_dir)
        self.fetch_closes = fetch_closes
        self.fetch_market = fetch_market
        self.fetch_trades = fetch_trades
        self.closes: Dict[int, float] = {}
        self.closes_end = -1
        self.closes_errors: List[str] = []
        outcomes_path = self.margin_dir / "gamma_outcomes.json"
        self.outcomes: Dict[str, Optional[str]] = (
            json.loads(outcomes_path.read_text()) if outcomes_path.is_file() else {}
        )

    def close(self, ts: int) -> Optional[float]:
        return self.closes.get(int(ts))

    def load_closes(self, start_ts: int, end_ts: int, now_ts: int, fetch: bool = True) -> None:
        """Load every UTC day file covering [start_ts, end_ts]; a day whose
        file stops early (the current day, or a study run mid-day) is
        extended from its newest close and rewritten in place.  A fetch
        error ends the load at that day so its windows wait as
        closes_unavailable instead of passing for a permanent tape gap."""
        day = start_ts - start_ts % DAY_S
        while day <= end_ts:
            path = self.margin_dir / ("binance_%d.json" % day)
            prices: Dict[str, float] = json.loads(path.read_text()) if path.is_file() else {}
            newest = max((int(key) for key in prices), default=day - 1)
            end = min(day + DAY_S, now_ts)
            failed = False
            if fetch and newest < end - CLOSES_TAIL_GRACE_S:
                try:
                    fetched = self.fetch_closes(newest + 1, end)
                except Exception as error:
                    self.closes_errors.append("%d: %s" % (day, type(error).__name__))
                    fetched, failed = {}, True
                if fetched:
                    prices.update({str(key): float(value) for key, value in fetched.items()})
                    _atomic_write(path, json.dumps(prices))
            for key, value in prices.items():
                self.closes[int(key)] = float(value)
            if failed:
                break
            day += DAY_S
        self.closes_end = max(self.closes) if self.closes else -1

    def print_path(self, window_start: int, decision_second: int) -> Path:
        return self.prints_dir / ("%d_%d.json" % (window_start, decision_second))

    def has_prints(self, window_start: int) -> bool:
        return all(self.print_path(window_start, d).is_file() for d in BAND_DECISION_SECONDS)

    def read_prints(self, window_start: int) -> Dict[int, Dict[str, Any]]:
        return {d: json.loads(self.print_path(window_start, d).read_text()) for d in BAND_DECISION_SECONDS}

    def write_prints(
        self,
        window_start: int,
        token_by_name: Optional[Mapping[str, str]],
        trades: Sequence[Mapping[str, Any]],
        coverage: Optional[Mapping[str, Any]] = None,
    ) -> None:
        open_close = self.close(window_start)
        for decision_second in BAND_DECISION_SECONDS:
            decision_close = self.close(window_start + decision_second)
            row: Dict[str, Any] = {
                "window_start": window_start,
                "decision_second": decision_second,
                "status": "no_signal",
                "signal": None,
                "signal_entry": None,
                "signal_prints": None,
                "coverage": dict(coverage) if coverage else None,
                "writer_version": PRINTS_WRITER_VERSION,
            }
            if token_by_name is None:
                row["status"] = "market_not_found"
            elif open_close is not None and decision_close is not None and decision_close != open_close:
                signal = "up" if decision_close > open_close else "down"
                prints = entry_prints(trades, token_by_name.get(signal), window_start + decision_second)
                row.update(
                    {
                        "status": "ok",
                        "signal": signal,
                        "signal_entry": prints[0][1] if prints else None,
                        "signal_prints": prints,
                    }
                )
            _atomic_write(self.print_path(window_start, decision_second), json.dumps(row) + "\n")

    def settled(self, window_start: int) -> bool:
        return str(window_start) in self.outcomes and self.has_prints(window_start)

    def save_outcomes(self) -> None:
        _atomic_write(self.margin_dir / "gamma_outcomes.json", json.dumps(self.outcomes))

    def refresh(self, start_ts: int, now_ts: int, budget: int) -> Dict[str, Any]:
        """Fetch outcomes and prints oldest-first for at most `budget` windows.

        The scan stops at the first window that cannot be settled yet (missing
        closes, fetch error, too young to be resolved), never skips past it,
        so the settled set stays a contiguous prefix."""
        last = last_eligible_window_start(now_ts)
        self.load_closes(start_ts, last + max(BAND_DECISION_SECONDS), now_ts)
        fetched = remaining = 0
        stopped: Optional[str] = None
        for window_start in range(start_ts, last + 1, WINDOW_S):
            if self.settled(window_start):
                continue
            if window_start + max(BAND_DECISION_SECONDS) > self.closes_end:
                stopped = stopped or "closes_unavailable"
            elif self.close(window_start) is None:
                continue  # permanent gap in the exchange tape: never evaluable
            if stopped or fetched >= budget:
                remaining += 1
                continue
            try:
                market = self.fetch_market(window_start)
            except Exception as error:
                stopped = "market_fetch_%s" % type(error).__name__
                remaining += 1
                continue
            fetched += 1
            parsed = parse_market(market) if market else None
            if (parsed is None or parsed["official"] is None) and (
                now_ts - (window_start + WINDOW_S) < UNRESOLVED_FINAL_AFTER_S
            ):
                # Gamma lists a window under closed=true only once it has
                # resolved, so a missing market is usually still resolving.
                stopped = "awaiting_market" if parsed is None else "awaiting_resolution"
                remaining += 1
                continue
            if parsed is None:
                self.outcomes[str(window_start)] = None
                self.write_prints(window_start, None, [])
                continue
            if not self.has_prints(window_start):
                try:
                    fetched_trades = self.fetch_trades(parsed["condition_id"], window_start)
                except Exception as error:
                    stopped = "trades_fetch_%s" % type(error).__name__
                    remaining += 1
                    continue
                self.write_prints(
                    window_start, parsed["token_by_name"], fetched_trades["trades"], fetched_trades["coverage"]
                )
            self.outcomes[str(window_start)] = parsed["official"]
        if fetched:
            self.save_outcomes()
        return {
            "fetched": fetched,
            "remaining": remaining,
            "stopped": stopped,
            "closes_errors": list(self.closes_errors),
        }

    def windows(self, start_ts: int, now_ts: int) -> List[Dict[str, Any]]:
        """Settled windows oldest-first up to the first unsettled one."""
        rows: List[Dict[str, Any]] = []
        for window_start in range(start_ts, last_eligible_window_start(now_ts) + 1, WINDOW_S):
            if window_start + max(BAND_DECISION_SECONDS) > self.closes_end:
                break
            open_close = self.close(window_start)
            if open_close is None:
                continue
            if not self.settled(window_start):
                break
            rows.append(
                {
                    "window_start": window_start,
                    "open": open_close,
                    "closes": {d: self.close(window_start + d) for d in BAND_DECISION_SECONDS},
                    "official": self.outcomes.get(str(window_start)),
                }
            )
        return rows

    def load_prints(self, windows: Sequence[Mapping[str, Any]]) -> Dict[Tuple[int, int], Dict[str, Any]]:
        prints: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for window in windows:
            for decision_second in BAND_DECISION_SECONDS:
                path = self.print_path(int(window["window_start"]), decision_second)
                if path.is_file():
                    prints[(int(window["window_start"]), decision_second)] = json.loads(path.read_text())
        return prints


# --- feed-forward evaluation -------------------------------------------------


def band_signal_records(
    windows: Sequence[Mapping[str, Any]], rule: Mapping[str, Any]
) -> List[Dict[str, Any]]:
    """Windows the rule would act on, using only closes at or before
    window_start + decision_second.  Labels are never read here."""
    params = rule_params(rule)
    decision_second = params["decision_second"]
    floor_usd = params["margin_floor_usd"]
    floor_sigma = params["margin_floor_sigma"]
    wanted = params["direction"]
    history: List[float] = []
    records: List[Dict[str, Any]] = []
    for window in windows:
        decision_close = window["closes"].get(decision_second)
        if decision_close is None:
            continue
        margin = float(decision_close) - float(window["open"])
        sigma = (
            statistics.pstdev(history[-SIGMA_LOOKBACK_WINDOWS:])
            if len(history) >= SIGMA_LOOKBACK_WINDOWS
            else None
        )
        history.append(abs(margin))
        direction = sign(margin)
        if direction == 0:
            continue
        name = "up" if direction > 0 else "down"
        if wanted != "both" and name != wanted:
            continue
        if abs(margin) < floor_usd:
            continue
        if floor_sigma > 0 and (sigma is None or abs(margin) < floor_sigma * sigma):
            continue
        records.append(
            {
                "window_start": int(window["window_start"]),
                "direction": name,
                "margin": margin,
                "sigma": sigma,
            }
        )
    return records


def _score(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    wins = sum(1 for row in rows if row["won"])
    return {
        "signals": total,
        "wins": wins,
        "accuracy": wins / float(total) if total else None,
        "wilson_lower": wilson_lower(wins, total),
    }


def _labelled(
    windows: Sequence[Mapping[str, Any]], records: Sequence[Mapping[str, Any]]
) -> List[Dict[str, Any]]:
    labels = {int(window["window_start"]): window.get("official") for window in windows}
    return [
        {**record, "won": record["direction"] == labels[record["window_start"]]}
        for record in records
        if labels.get(record["window_start"]) in ("up", "down")
    ]


def print_entry_price(
    cached: Optional[Mapping[str, Any]],
    direction: str,
    floor: float,
    cap: float,
    semantics: str = "one_look",
    patience_s: Optional[int] = None,
) -> Tuple[Optional[float], Optional[str]]:
    """(in-band entry price, rejection) of one cached prints row under
    ENTRY_SEMANTICS.  A grammar C patience is a horizon on the print offsets:
    only prints stamped no later than patience + 1 s count (prints carry
    whole seconds and the decision second is excluded, so patience 0 reads
    the first print of the next second: executable_truth.print_look), which
    needs the row's print list.  Rows written before signal_prints was
    recorded hold only the first print and cannot be scored patiently or
    under a patience (no_print_list).  A tape that stopped at
    TRADES_MAX_PAGES before reaching the row's decision second may be
    missing the first print after it (uncovered)."""
    if semantics not in ENTRY_SEMANTICS:
        raise ValueError("unknown entry semantics %r" % semantics)
    if not cached or cached.get("status") != "ok" or cached.get("signal") != direction:
        return None, "no_print"
    coverage = cached.get("coverage") or {}
    oldest = coverage.get("oldest_offset_s")
    decision_second = cached.get("decision_second")
    if (
        not coverage.get("complete")
        and oldest is not None
        and decision_second is not None
        and int(oldest) > int(decision_second)
    ):
        return None, "uncovered"
    if semantics == "one_look" and patience_s is None:
        price = cached.get("signal_entry")
        if price is None:
            return None, "no_print"
        return (float(price), None) if floor < float(price) <= cap else (None, "out_of_band")
    prints = cached.get("signal_prints")
    if prints is None:
        return None, "no_print_list"
    if patience_s is not None:
        prints = [item for item in prints if int(item[0]) <= int(patience_s) + 1]
    if not prints:
        return None, "no_print"
    for _, price in prints[:1] if semantics == "one_look" else prints:
        if floor < float(price) <= cap:
            return float(price), None
    return None, "out_of_band"


_REJECTION_COUNTS = {
    "no_print": "windows_without_print",
    "out_of_band": "out_of_band_prints",
    "no_print_list": "windows_without_print_list",
    "uncovered": "uncovered_windows",
}


def tripwire_triggered(wins: int, total: int) -> bool:
    return int(total) >= TRIPWIRE_MINIMUM_N and int(wins) / float(total) > TRIPWIRE_WIN_RATE


def band_entry_economics(
    scored: Sequence[Mapping[str, Any]],
    prints: Mapping[Tuple[int, int], Mapping[str, Any]],
    rule: Mapping[str, Any],
    gates: Mapping[str, Any],
    semantics: str = "one_look",
) -> Dict[str, Any]:
    params = rule_params(rule)
    decision_second = params["decision_second"]
    floor, cap, patience = params["favorite_price_floor"], params["favorite_price_cap"], params["patience_s"]
    entries: List[Dict[str, Any]] = []
    counts = {column: 0 for column in ("uncached_windows",) + tuple(_REJECTION_COUNTS.values())}
    for row in scored:
        cached = prints.get((row["window_start"], decision_second))
        if cached is None:
            counts["uncached_windows"] += 1
            continue
        price, rejection = print_entry_price(cached, row["direction"], floor, cap, semantics, patience)
        if price is None:
            counts[_REJECTION_COUNTS[str(rejection)]] += 1
            continue
        even = break_even(price)
        # 1 USD buys 1/(price + fee) shares paying 1 each on a win.
        net = (1.0 / even - 1.0) if row["won"] else -1.0
        entries.append({"price": price, "break_even": even, "won": row["won"], "net": net})
    total = len(entries)
    wins = sum(1 for entry in entries if entry["won"])
    mean_break_even = sum(entry["break_even"] for entry in entries) / total if total else None
    mean_net = sum(entry["net"] for entry in entries) / total if total else None
    lower = wilson_lower(wins, total)
    gate_results = {
        "support": total >= int(gates["minimum_entries"]),
        "wilson_above_break_even": lower is not None and lower >= float(mean_break_even),
        "positive_mean_net": mean_net is not None and mean_net > 0.0,
    }
    if semantics == "patient" or patience is not None:
        # A half-rebuilt cache would score only the windows that carry a
        # print list (a margin-selected subset): fail closed until rebuilt.
        gate_results["print_lists_complete"] = counts["windows_without_print_list"] == 0
    return {
        "stage": "band_entry_economics",
        "entry_semantics": semantics,
        "patience_s": patience,
        "entries": total,
        "wins": wins,
        "win_rate": wins / float(total) if total else None,
        "wilson_lower": lower,
        "mean_break_even": mean_break_even,
        "mean_net_per_usd": mean_net,
        **counts,
        "gates": gate_results,
        "survivor": all(gate_results.values()),
        "tripwire": {
            "triggered": tripwire_triggered(wins, total),
            "maximum_win_rate": TRIPWIRE_WIN_RATE,
            "minimum_entries": TRIPWIRE_MINIMUM_N,
        },
    }


def screen_status(
    stage_1: Mapping[str, Any], stage_2: Optional[Mapping[str, Any]], cleared: bool = False
) -> str:
    """Hypothesis status after the screens; a stage-2 survivor that trips the
    win-rate tripwire waits as manual_audit unless an operator cleared it."""
    if not stage_1["survivor"]:
        return "rejected_signal_screen"
    if not (stage_2 and stage_2["survivor"]):
        return "rejected_entry_economics"
    return "manual_audit" if stage_2["tripwire"]["triggered"] and not cleared else "stage_2_survivor"


def lane_entry_semantics(lane: Mapping[str, Any]) -> str:
    return str(lane.get("entry_semantics", "one_look"))


def audit_cleared(lane: Mapping[str, Any], fingerprint: str) -> bool:
    return str(fingerprint) in (lane.get("audit_cleared") or [])


def promotable(
    lane: Mapping[str, Any], fingerprint: str, status: str, verdict: str, wins: int, n: int
) -> bool:
    """A candidate e-BH may discover: not at futility, and not held by the
    tripwire (held at the screen, or tripping on its own accrual) unless an
    operator cleared it."""
    if str(verdict) == "kill":
        return False
    held = str(status) == "manual_audit" or tripwire_triggered(wins, n)
    return not held or audit_cleared(lane, fingerprint)


def campaign_n(config: Mapping[str, Any]) -> Optional[int]:
    """Pre-registered e-BH family size: lanes.band_mechanisms.campaign_n,
    else generator.campaign_n; None (nothing promotable) when unregistered."""
    lane = (config.get("lanes") or {}).get(LANE) or {}
    value = lane.get("campaign_n", (config.get("generator") or {}).get("campaign_n"))
    return None if value is None else int(value)


def evaluate_band_rule(
    windows: Sequence[Mapping[str, Any]],
    prints: Mapping[Tuple[int, int], Mapping[str, Any]],
    rule: Mapping[str, Any],
    gates: Mapping[str, Any],
    now_ts: int,
    semantics: str = "one_look",
) -> Dict[str, Any]:
    rule = normalized_band_rule(rule)
    scored = _labelled(windows, band_signal_records(windows, rule))
    overall = _score(scored)
    by_direction = {
        name: _score([row for row in scored if row["direction"] == name])
        for name in ("up", "down")
    }
    buckets = {
        "%s-%s" % (low, high if high < 10**9 else "inf"): _score(
            [row for row in scored if low <= abs(row["margin"]) < high]
        )
        for low, high in margin_floor_study.BUCKETS
    }
    recent = _score([row for row in scored if row["window_start"] >= int(now_ts) - RECENT_SECONDS])
    even_at_cap = break_even(rule["favorite_price_cap"])
    # Signal accuracy is a ceiling on what any entry can realize, never the
    # entry evidence itself: stage 1 gates support and recency only (no
    # comparison of signal accuracy, overall or recent, with break-even at
    # the cap, which is reported), and stage 2 gates the realized win rate
    # at the entry print against its break-even (CLAUDE.md section 4).
    gate_results = {
        "support": overall["signals"] >= int(gates["minimum_signals"]),
        "recent_support": recent["signals"] >= int(gates["minimum_recent_signals"]),
    }
    stage_1 = {
        "stage": "band_signal_screen",
        "overall": overall,
        "by_direction": by_direction,
        "by_margin_bucket": buckets,
        "recent_48h": recent,
        "break_even_at_cap": even_at_cap,
        "gates": gate_results,
        "survivor": all(gate_results.values()),
    }
    stage_2 = band_entry_economics(scored, prints, rule, gates, semantics) if stage_1["survivor"] else None
    return {
        "schema_version": 1,
        "evaluator_version": BAND_EVALUATOR_VERSION,
        "generated_at": _utc_now(),
        "research_only": True,
        "rule": rule,
        "window_count": len(windows),
        "labelled_window_count": sum(1 for window in windows if window.get("official") in ("up", "down")),
        "last_window_start": int(windows[-1]["window_start"]) if windows else None,
        "stage_1": stage_1,
        "stage_2": stage_2,
        "survivor": bool(stage_2 and stage_2["survivor"]),
        "does_not_establish": ["fills", "live slippage", "promotion eligibility"],
    }


def band_accrual_outcomes(
    windows: Sequence[Mapping[str, Any]],
    prints: Mapping[Tuple[int, int], Mapping[str, Any]],
    rule: Mapping[str, Any],
    after_window_start: int,
    semantics: str = "one_look",
) -> List[Tuple[int, float, bool]]:
    """(window_start, break_even, won) for windows strictly after the cut.

    Same population as band_entry_economics: windows with a cached public
    print inside the band, at that print's break-even.  A window without a
    print is not a trade the rule executes, so it carries no evidence.  A
    row without a print list (patient semantics on a row the rebuild has
    not reached) ends the scan: the cut must not move past a window that
    will hold its prints once rebuilt."""
    params = rule_params(rule)
    decision_second = params["decision_second"]
    floor, cap, patience = params["favorite_price_floor"], params["favorite_price_cap"], params["patience_s"]
    outcomes: List[Tuple[int, float, bool]] = []
    for row in _labelled(windows, band_signal_records(windows, rule)):
        if row["window_start"] <= int(after_window_start):
            continue
        price, rejection = print_entry_price(
            prints.get((row["window_start"], decision_second)), row["direction"], floor, cap, semantics, patience
        )
        if rejection == "no_print_list":
            break
        if price is None:
            continue
        outcomes.append((row["window_start"], break_even(price), bool(row["won"])))
    return outcomes


def accrue_band_hypotheses(
    config: Mapping[str, Any],
    ledger: Any,
    windows: Sequence[Mapping[str, Any]],
    prints: Mapping[Tuple[int, int], Mapping[str, Any]],
) -> Dict[str, Any]:
    """Fold fresh windows into every accruing candidate, then decide the
    family: killed_futility on a kill verdict, manual_audit while the
    tripwire holds, promote_candidate only inside the e-BH discovery set at
    the pre-registered campaign_n, accruing otherwise."""
    lane = (config.get("lanes") or {}).get(LANE) or {}
    semantics = lane_entry_semantics(lane)
    summary: Dict[str, Any] = {
        "evaluated": 0, "promoted": 0, "killed": 0, "accruing": 0, "manual_audit": 0, "skipped": 0
    }
    results: List[Tuple[Mapping[str, Any], Dict[str, Any]]] = []
    # e-BH family: every band candidate that ever started an e-process
    # (killed and held rows keep theirs), not only those still in the running.
    family_size = 0
    for hypothesis in ledger.lane_hypotheses(LANE):
        fingerprint = hypothesis["fingerprint"]
        row = ledger.accrual(fingerprint)
        if row is not None:
            family_size += 1
        if hypothesis["status"] not in ACCRUAL_STATUSES:
            continue
        if row is None:
            summary["skipped"] += 1
            continue
        rule = normalized_band_rule(hypothesis["proposal"]["rule"])
        outcomes = band_accrual_outcomes(windows, prints, rule, int(row["last_window_start"]), semantics)
        result = ledger.accrue(fingerprint, LANE, outcomes)
        if result["applied"]:
            factory_generator.append_trial_entry(
                config,
                fingerprint,
                "fresh_public_accrual",
                result["verdict"],
                n=result["n"],
                wins=result["wins"],
            )
        results.append((hypothesis, result))
    eligible = {
        hypothesis["fingerprint"]: float(result["e_value"])
        for hypothesis, result in results
        if promotable(
            lane, hypothesis["fingerprint"], hypothesis["status"], result["verdict"], result["wins"], result["n"]
        )
    }
    family = evidence_accrual.e_bh(eligible, campaign_n(config), family_size=family_size)
    for hypothesis, result in results:
        fingerprint = hypothesis["fingerprint"]
        if result["verdict"] == "kill":
            status, bucket = "killed_futility", "killed"
        elif fingerprint not in eligible:
            status, bucket = "manual_audit", "manual_audit"
        elif fingerprint in family["promoted"]:
            status, bucket = "promote_candidate", "promoted"
        else:
            status, bucket = "accruing", "accruing"
        if hypothesis["status"] != status:
            ledger.update_hypothesis_status(fingerprint, status)
        summary["evaluated"] += 1
        summary[bucket] += 1
    summary["e_bh"] = {
        key: family[key] for key in ("campaign_n", "candidates", "family", "k_star", "threshold", "overflow")
    }
    return summary


def rescreen_support_rejections(
    config: Mapping[str, Any],
    ledger: Any,
    windows: Sequence[Mapping[str, Any]],
    prints: Mapping[Tuple[int, int], Mapping[str, Any]],
    now_ts: int,
    limit: int = 3,
) -> Dict[str, int]:
    """Re-score rules rejected ONLY for stage-2 support once the cache grew,
    and rules rejected on the print-list gate once the rebuild reached
    their rows.

    A support rejection is a statement about the data seen, not about the
    rule; the live band rule itself fell to it on 8.5 days.  A print-list
    rejection (a row written by --rescore-all or an older lane while
    --rebuild-prints was under way) is a statement about the cache: it is
    re-scored without the new-window wait (the cache did not grow, it was
    rebuilt), and while its rows still lack their lists nothing is written.
    Bounded per cycle so a long backlog cannot starve the proposer."""
    lane = (config.get("lanes") or {}).get(LANE) or {}
    summary = {"rescreened": 0, "promoted": 0, "still_rejected": 0}
    for hypothesis in ledger.lane_hypotheses(LANE):
        if summary["rescreened"] >= limit:
            break
        if hypothesis["status"] != "rejected_entry_economics":
            continue
        record = ledger.hypothesis(hypothesis["fingerprint"])
        if not record or not record.get("evidence_path"):
            continue
        evidence_path = Path(str(record["evidence_path"]))
        try:
            evidence = json.loads(evidence_path.read_text())
        except (OSError, ValueError):
            continue
        gates = ((evidence.get("stage_2") or {}).get("gates") or {})
        listless = not gates.get("print_lists_complete", True)
        if gates.get("support", True) and not listless:
            continue
        if not listless and len(windows) - int(evidence.get("window_count", 0)) < RESCREEN_MIN_NEW_WINDOWS:
            continue
        rule = normalized_band_rule(hypothesis["proposal"]["rule"])
        fresh = evaluate_band_rule(windows, prints, rule, lane["gates"], now_ts, lane_entry_semantics(lane))
        if not ((fresh["stage_2"] or {}).get("gates") or {}).get("print_lists_complete", True):
            continue  # the rebuild has not reached its rows: no verdict yet
        fresh.update(
            {
                "fingerprint": hypothesis["fingerprint"],
                "proposal": evidence.get("proposal", hypothesis["proposal"]),
                "provenance": evidence.get("provenance", evidence.get("llm")),
                "rescreened_at": _utc_now(),
                "previous_window_count": int(evidence.get("window_count", 0)),
            }
        )
        _atomic_write(evidence_path, json.dumps(fresh, indent=2, sort_keys=True) + "\n")
        stage_1 = fresh["stage_1"]
        stage_2 = fresh["stage_2"]
        status = screen_status(stage_1, stage_2, audit_cleared(lane, hypothesis["fingerprint"]))
        if stage_2 is not None:
            factory_generator.append_trial_entry(
                config,
                hypothesis["fingerprint"],
                "band_entry_economics",
                status,
                n=stage_2["entries"],
                wins=stage_2["wins"],
            )
        summary["rescreened"] += 1
        if status in ("stage_2_survivor", "manual_audit"):
            ledger.update_hypothesis_status(hypothesis["fingerprint"], status)
            ledger.accrue(hypothesis["fingerprint"], LANE, [], fresh["last_window_start"])
            summary["promoted"] += 1
        else:
            if status != hypothesis["status"]:
                ledger.update_hypothesis_status(hypothesis["fingerprint"], status)
            summary["still_rejected"] += 1
    return summary


# --- proposer ----------------------------------------------------------------


def proposer_cells() -> List[Dict[str, Any]]:
    """Grammar C cells in proposal order: registrable cells first, each group
    in grid order.  Cells whose decision second has no print column (150 s)
    are left to scripts/executable_truth.py."""
    cells = [rule for rule in grid_v2_rules() if rule["decision_second"] in BAND_DECISION_SECONDS]
    return [rule for rule in cells if registrable_v2(rule)] + [rule for rule in cells if not registrable_v2(rule)]


def propose_band_rule(ledger: Any) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """The first grammar C cell without a hypothesis in the ledger, or None
    once the grid is exhausted.  Deterministic: the order never depends on
    outcomes, and a cell is proposed once (both rule shapes are read, so a
    legacy row never raises)."""
    existing = set()
    for row in ledger.lane_hypotheses(LANE):
        try:
            existing.add(_canonical(normalized_band_rule(row["proposal"]["rule"])))
        except (KeyError, TypeError, ValueError):
            continue
    for index, rule in enumerate(proposer_cells()):
        if _canonical(normalized_band_rule(rule)) in existing:
            continue
        registrable = registrable_v2(rule)
        proposal = _deterministic_proposal(
            rule,
            "Grammar C cell %d: %s" % (index + 1, compact_band_rule(rule)),
            "Registrable cell of grammar C (floor >= 75, decision >= 180)."
            if registrable
            else "Diagnostic cell of grammar C (the $50 control floor): never registrable.",
        )
        return proposal, {"proposal_source": PROPOSAL_SOURCE, "cell_index": index, "registrable": registrable}
    return None


# --- lane entry point --------------------------------------------------------


def run_band_lane(
    config: Mapping[str, Any],
    ledger: Any,
    state_dir: Path,
    dry_run: bool,
    cache: Optional[BandCache] = None,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    lane = (config.get("lanes") or {}).get(LANE) or {}
    if not lane.get("enabled", False):
        return {"status": "disabled"}
    if _seconds_since(ledger.meta("band_mechanisms.last_at")) < int(lane["minimum_interval_seconds"]):
        return {"status": "not_due"}
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    start_ts = int(lane["start_ts"])
    if dry_run:
        # Ledger only: the proposer needs neither the cache nor the network.
        proposed = propose_band_rule(ledger)
        if proposed is None:
            return {"status": "grid_exhausted"}
        proposal, provenance = proposed
        return {
            "status": "dry_run",
            "fingerprint": band_fingerprint(proposal["rule"]),
            "proposal": proposal,
            "provenance": provenance,
        }
    cache = cache or BandCache()
    refresh = cache.refresh(start_ts, now_ts, int(lane["maximum_new_windows_per_cycle"]))
    windows = cache.windows(start_ts, now_ts)
    prints = cache.load_prints(windows)
    result: Dict[str, Any] = {
        "cache": {
            **refresh,
            "windows": len(windows),
            "last_window_start": int(windows[-1]["window_start"]) if windows else None,
        },
        "accrual": accrue_band_hypotheses(config, ledger, windows, prints),
    }
    if refresh["remaining"] > 0:
        # Screening on a lagging prefix would fail the recent-window guard
        # for reasons of data lag, not edge; propose once caught up.
        result["status"] = "fetching"
        return result
    result["rescreen"] = rescreen_support_rejections(config, ledger, windows, prints, now_ts)
    proposed = propose_band_rule(ledger)
    if proposed is None:
        return {**result, "status": "grid_exhausted"}
    proposal, provenance = proposed
    fingerprint = band_fingerprint(proposal["rule"])
    if ledger.has_hypothesis(fingerprint):
        return {**result, "status": "duplicate", "fingerprint": fingerprint}
    evidence = evaluate_band_rule(
        windows, prints, proposal["rule"], lane["gates"], now_ts, lane_entry_semantics(lane)
    )
    evidence.update({"fingerprint": fingerprint, "proposal": proposal, "provenance": provenance})
    stage_1 = evidence["stage_1"]
    stage_2 = evidence["stage_2"]
    if stage_2 is not None and not stage_2["gates"].get("print_lists_complete", True):
        # A row the print rebuild has not reached (or failed on) is a state
        # of the cache, not a verdict on the cell: nothing is written and
        # the same cell is proposed again once its rows carry their lists
        # (band_mechanisms.last_at stays, as after "fetching", so the lane
        # retries on the runner's next loop).
        return {
            **result,
            "status": "awaiting_print_rebuild",
            "fingerprint": fingerprint,
            "cell_index": provenance["cell_index"],
            "registrable": provenance["registrable"],
            "windows_without_print_list": stage_2["windows_without_print_list"],
        }
    evidence_path = state_dir / ("evidence/band_mechanisms/%s.json" % fingerprint)
    _atomic_write(evidence_path, json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    status = screen_status(stage_1, stage_2, audit_cleared(lane, fingerprint))
    factory_generator.append_trial_entry(
        config,
        fingerprint,
        "band_signal_screen",
        "stage_1_survivor" if stage_1["survivor"] else status,
        n=stage_1["overall"]["signals"],
        wins=stage_1["overall"]["wins"],
    )
    if stage_2 is not None:
        factory_generator.append_trial_entry(
            config,
            fingerprint,
            "band_entry_economics",
            status,
            n=stage_2["entries"],
            wins=stage_2["wins"],
        )
    ledger.add_hypothesis(
        fingerprint, LANE, proposal, None, status, evidence_path, source=provenance["proposal_source"]
    )
    if status in ("stage_2_survivor", "manual_audit"):
        # Accrual starts strictly after the newest window the screens used.
        ledger.accrue(fingerprint, LANE, [], evidence["last_window_start"])
    ledger.set_meta("band_mechanisms.last_at", _utc_now())
    result.update(
        {
            "status": status,
            "fingerprint": fingerprint,
            "artifact": str(evidence_path),
            "proposal_source": provenance["proposal_source"],
            "cell_index": provenance["cell_index"],
            "registrable": provenance["registrable"],
            "stage_1": {"overall": stage_1["overall"], "gates": stage_1["gates"]},
            "stage_2": (
                {key: stage_2[key] for key in ("entries", "wins", "mean_break_even", "mean_net_per_usd", "gates")}
                if stage_2
                else None
            ),
        }
    )
    return result


# --- maintenance CLI ---------------------------------------------------------


def rebuild_window_prints(cache: BandCache, window_start: int) -> Dict[str, Any]:
    """Re-fetch one settled window's tape with pagination and rewrite its
    prints (same per-(window, decision) semantics).  A fetch failure leaves
    the files untouched, so the window stays uncovered for the next run."""
    before = cache.read_prints(window_start)
    try:
        market = cache.fetch_market(window_start)
        parsed = parse_market(market) if market else None
        if parsed is None:
            return {"window_start": window_start, "error": "market_not_found"}
        fetched = cache.fetch_trades(parsed["condition_id"], window_start)
    except Exception as error:
        return {"window_start": window_start, "error": type(error).__name__}
    cache.write_prints(window_start, parsed["token_by_name"], fetched["trades"], fetched["coverage"])
    after = cache.read_prints(window_start)
    return {
        "window_start": window_start,
        "changed": [
            d for d in BAND_DECISION_SECONDS if before[d].get("signal_entry") != after[d].get("signal_entry")
        ],
        "coverage": fetched["coverage"],
    }


def rebuild_prints(
    cache: BandCache,
    start_ts: int,
    now_ts: int,
    workers: int = 1,
    limit: Optional[int] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """Rebuild the prints of every settled window from start_ts whose tape is
    not covered (print_covered) or was written by an older writer than
    PRINTS_WRITER_VERSION; resumable because current windows are skipped.
    `pending` is the whole backlog, `selected` what this run fetches after
    --limit.  short_tapes counts windows still short of TRADES_COVERAGE_S
    after TRADES_MAX_PAGES pages."""
    last = last_eligible_window_start(now_ts)
    cache.load_closes(start_ts, last + max(BAND_DECISION_SECONDS), now_ts, fetch=False)
    summary = {
        "settled": 0,
        "covered": 0,
        "unlisted": 0,
        "pending": 0,
        "selected": 0,
        "rebuilt": 0,
        "changed_windows": 0,
        "changed_prints": 0,
        "short_tapes": 0,
        "errors": 0,
    }
    pending: List[int] = []
    for window_start in range(start_ts, last + 1, WINDOW_S):
        if not cache.settled(window_start):
            continue
        summary["settled"] += 1
        rows = cache.read_prints(window_start)
        if any(row.get("status") == "market_not_found" for row in rows.values()):
            summary["unlisted"] += 1
        elif all(print_covered(row) and row.get("writer_version") == PRINTS_WRITER_VERSION for row in rows.values()):
            summary["covered"] += 1
        else:
            pending.append(window_start)
    summary["pending"] = len(pending)
    if limit is not None:
        pending = pending[: max(0, int(limit))]
    summary["selected"] = len(pending)
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        for index, result in enumerate(pool.map(lambda ws: rebuild_window_prints(cache, ws), pending), 1):
            if "error" in result:
                summary["errors"] += 1
            else:
                summary["rebuilt"] += 1
                summary["changed_prints"] += len(result["changed"])
                summary["changed_windows"] += 1 if result["changed"] else 0
                summary["short_tapes"] += 0 if print_covered({"coverage": result["coverage"]}) else 1
            if progress and (index % 100 == 0 or index == len(pending)):
                progress(
                    "%d/%d rebuilt=%d changed=%d errors=%d"
                    % (index, len(pending), summary["rebuilt"], summary["changed_windows"], summary["errors"])
                )
    return summary


def _stage_2_brief(stage_2: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    if not stage_2:
        return None
    return {
        key: stage_2.get(key)
        for key in ("entries", "wins", "win_rate", "wilson_lower", "mean_break_even", "mean_net_per_usd", "gates")
    }


def rescore_band_hypotheses(
    config: Mapping[str, Any],
    ledger: Any,
    windows: Sequence[Mapping[str, Any]],
    prints: Mapping[Tuple[int, int], Mapping[str, Any]],
    now_ts: int,
) -> Dict[str, Any]:
    """Re-run stage 1/2 for every band hypothesis on the rebuilt tape.

    Each evidence artifact is rewritten (the truncated-tape version is kept
    as <fingerprint>.pre_pagination.json), hypotheses.status follows the
    fresh verdict, the hypothesis's accrual row is deleted as it is
    re-decided (its windows were scored on the truncated tape; per
    hypothesis, so an exception mid-run leaves every other hypothesis
    either fully old or fully new) and survivors are re-seeded at their
    newest scored window, so promote flags earned on the old tape are
    withdrawn.  The recent-window gate runs on the cache's own clock (the
    newest window plus the resolution lag), not wall-clock time, so a cache
    that lags behind does not fail borderline rules for data lag.  One
    trial-ledger row per hypothesis, stage band_rescore_paginated (n/wins
    from stage 2 when it ran, else stage 1)."""
    lane = (config.get("lanes") or {}).get(LANE) or {}
    if windows:
        now_ts = int(windows[-1]["window_start"]) + WINDOW_S + RESOLUTION_LAG_S
    rows = ledger.lane_hypotheses(LANE)
    before = Counter(row["status"] for row in rows)
    transitions: Counter = Counter()
    report: Dict[str, Any] = {
        "hypotheses": len(rows),
        "window_count": len(windows),
        "recent_clock": int(now_ts),
        "accrual_rows_deleted": 0,
        "before": dict(before),
        "former_promote_candidates": [],
        "skipped_without_evidence": 0,
    }
    for hypothesis in rows:
        fingerprint = hypothesis["fingerprint"]
        record = ledger.hypothesis(fingerprint) or {}
        if not record.get("evidence_path"):
            report["skipped_without_evidence"] += 1
            continue
        evidence_path = Path(str(record["evidence_path"]))
        old: Dict[str, Any] = {}
        if evidence_path.is_file():
            try:
                old = json.loads(evidence_path.read_text())
            except ValueError:
                old = {}
            backup = evidence_path.with_name("%s.pre_pagination.json" % fingerprint)
            if not backup.is_file():
                _atomic_write(backup, evidence_path.read_text())
        rule = normalized_band_rule(hypothesis["proposal"]["rule"])
        fresh = evaluate_band_rule(windows, prints, rule, lane["gates"], now_ts, lane_entry_semantics(lane))
        fresh.update(
            {
                "fingerprint": fingerprint,
                "proposal": old.get("proposal", hypothesis["proposal"]),
                "provenance": old.get("provenance", old.get("llm")),
                "rescored_at": _utc_now(),
                "rescore": {
                    "reason": "paginated_print_tape",
                    "previous_status": hypothesis["status"],
                    "previous_window_count": old.get("window_count"),
                    "previous_stage_2": _stage_2_brief(old.get("stage_2")),
                },
            }
        )
        _atomic_write(evidence_path, json.dumps(fresh, indent=2, sort_keys=True) + "\n")
        stage_1 = fresh["stage_1"]
        stage_2 = fresh["stage_2"]
        status = screen_status(stage_1, stage_2, audit_cleared(lane, fingerprint))
        factory_generator.append_trial_entry(
            config,
            fingerprint,
            "band_rescore_paginated",
            status,
            n=stage_2["entries"] if stage_2 else stage_1["overall"]["signals"],
            wins=stage_2["wins"] if stage_2 else stage_1["overall"]["wins"],
        )
        report["accrual_rows_deleted"] += ledger.delete_accrual(fingerprint)
        if status != hypothesis["status"]:
            ledger.update_hypothesis_status(fingerprint, status)
        if status in ("stage_2_survivor", "manual_audit"):
            ledger.accrue(fingerprint, LANE, [], fresh["last_window_start"])
        transitions["%s->%s" % (hypothesis["status"], status)] += 1
        if hypothesis["status"] == "promote_candidate":
            report["former_promote_candidates"].append(
                {
                    "fingerprint": fingerprint,
                    "rule": compact_band_rule(rule),
                    "status": status,
                    "stage_1": stage_1["overall"],
                    "previous_stage_2": _stage_2_brief(old.get("stage_2")),
                    "stage_2": _stage_2_brief(stage_2),
                }
            )
    after = Counter(row["status"] for row in ledger.lane_hypotheses(LANE))
    report.update(
        {
            "after": dict(after),
            "transitions": dict(transitions),
            "survivors_before": sum(before[status] for status in ACCRUAL_STATUSES | {"promote_candidate"}),
            "survivors_after": after["stage_2_survivor"],
            "promote_candidates_before": before["promote_candidate"],
            "promote_candidates_after": after["promote_candidate"],
        }
    )
    return report


def _load_loop_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "strategy_research_loop", Path(__file__).with_name("strategy_research_loop.py")
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


OVERLAY_CONFIG = ROOT / "logs/strategy-research/loop-config.local.json"
DEPLOY_CONFIG = ROOT / "deploy/strategy-research-loop.json"


def config_path(explicit: Optional[str]) -> Path:
    """The runner's overlay when present (the config the lane accrues
    under), else the fail-closed deploy config: factory_kpi's precedence."""
    if explicit:
        return Path(explicit)
    return OVERLAY_CONFIG if OVERLAY_CONFIG.is_file() else DEPLOY_CONFIG


def main(argv: Optional[Sequence[str]] = None) -> int:
    global API_PAUSE_S
    parser = argparse.ArgumentParser(
        description="Band lane maintenance; stop deploy/factory-runner.sh before running either action "
        "(both hold the cycle lock, so a live cycle makes them fail loudly)."
    )
    parser.add_argument(
        "--config",
        help="default: the runner's overlay %s when present, else %s"
        % (OVERLAY_CONFIG.relative_to(ROOT), DEPLOY_CONFIG.relative_to(ROOT)),
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument(
        "--rebuild-prints",
        action="store_true",
        help="re-fetch every settled window's tape with pagination (resumable: covered windows are skipped)",
    )
    action.add_argument(
        "--rescore-all",
        action="store_true",
        help="re-run stage 1/2 for every band hypothesis on the rebuilt tape and reset the band accrual",
    )
    parser.add_argument("--start-ts", type=int, help="first window start (default: the lane's start_ts)")
    parser.add_argument("--workers", type=int, default=1, help="concurrent rebuild fetchers")
    parser.add_argument("--pause", type=float, default=API_PAUSE_S, help="seconds between API requests per worker")
    parser.add_argument("--limit", type=int, help="rebuild at most this many windows")
    args = parser.parse_args(argv)
    loop = _load_loop_module()
    path = config_path(args.config)
    config = loop.load_config(path)
    lane = config["lanes"][LANE]
    state_dir = Path(str(config["state_dir"]))
    if not state_dir.is_absolute():
        state_dir = ROOT / state_dir
    start_ts = int(lane["start_ts"] if args.start_ts is None else args.start_ts)
    now_ts = int(time.time())
    API_PAUSE_S = float(args.pause)
    with loop.CycleLock(state_dir / "locks/cycle.lock"):
        cache = BandCache()
        if args.rebuild_prints:
            report = rebuild_prints(
                cache,
                start_ts,
                now_ts,
                args.workers,
                args.limit,
                progress=lambda line: print("%s %s" % (_utc_now(), line), file=sys.stderr, flush=True),
            )
        else:
            cache.load_closes(
                start_ts, last_eligible_window_start(now_ts) + max(BAND_DECISION_SECONDS), now_ts, fetch=False
            )
            windows = cache.windows(start_ts, now_ts)
            ledger = loop.Ledger(state_dir / "research.sqlite3")
            try:
                report = rescore_band_hypotheses(config, ledger, windows, cache.load_prints(windows), now_ts)
            finally:
                ledger.close()
    report.update({"config": str(path), "entry_semantics": lane_entry_semantics(lane)})
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
