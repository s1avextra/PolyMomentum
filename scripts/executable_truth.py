#!/usr/bin/env python3
"""The ONE evaluator: executable truth for the band family (roadmap section C).

Table logs/strategy-research/executable_truth/windows.sqlite3: one row per
(window_start, decision_s in {150, 180, 210, 240}), append-only, each row
stamped with its build time (the accrual cut) and git sha.  Per row: the
Binance basis at the open and the decision (margin, direction), the official
Gamma label, the final margin close(ws + 300) - open with its oracle-noise
bucket, the public print summary (writer v2 rows only; the writer version
share is recorded), the engine's band_anchor quote and band_ladder samples
when a session recorded them, and the host that recorded them.

Grammar (band_lane.BAND_GRID_V2): decision_s x floor_usd in {50 control, 75,
100, 150} x cap in {0.92 .. 0.99} x patience in {0, 15, 30} s; ask floor
0.80, direction both (up/down is a tripwire, never a rule); 336 cells, the
189 with floor >= 75 and decision >= 180 registrable.  Three models per
cell: signal-only (a ceiling, never gated against break-even), print
one-look (the first public BUY print after the decision within the patience
look: an upper bound) and ladder truth (trade iff the first ladder sample
with t <= patience has worst <= cap, vwap > 0.80, a fresh book and a
coherent pair; entry = worst, the FOK limit, never vwap).  The ladder model
gates floor and direction on the record's own Binance tick basis, the one
the engine latched; the kline basis of the row serves the signal ceiling
and the print model.  Selection never reads a label; scoring does.

Fee: the rate on the engine's filled records (fee / (shares x price x
(1 - price))), which is the engine's own rate booked through real fills,
not a venue-reported fee; paper fills are skipped.

Hosts: ladder rows recorded by the VPS (the trading IP) are evidence; the
Mac stopgap observer's rows are discovery data until --accept-mac-ladders
records a >= 3-day overlap agreeing within one tick on >= 95% of shared
samples (section C).

Protocol: --register writes deploy/campaigns/<id>.json (N fixed before any
outcome after registered_at is read); --tick builds incrementally and
accrues every registered cell with an e-process on ladder rows at $25,
decides the family by e-BH at alpha 0.05 with family_size = N, and fails
closed to manual_audit on any tripwire until --clear-audit records the
operator's audit of that defect; --gate-json emits the evidence artifact
the engine's band-promotion-artifact command consumes.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import random
import re
import sqlite3
import statistics
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
import band_lane  # noqa: E402
import evidence_accrual  # noqa: E402
import factory_generator  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
# v2: ladder gating on the record's own basis, conditional oracle ceiling,
# Wilson-separated adverse selection, FOK-limit latency look.
EVALUATOR_VERSION = "executable_truth_v2"
GRAMMAR_VERSION = band_lane.BAND_GRID_V2_VERSION
LANE = "band_ladder"
TRUTH_DIR = ROOT / "logs/strategy-research/executable_truth"
DEFAULT_DB = TRUTH_DIR / "windows.sqlite3"
CAMPAIGNS_DIR = ROOT / "deploy/campaigns"
SESSION_DIRS = (ROOT / "logs/band-canary-mirror/sessions", ROOT / "logs/band-observer/sessions")
BAND_FAMILY = "signal_favorite_band_official_v1"

DECISION_SECONDS: Tuple[int, ...] = band_lane.BAND_GRID_V2["decision_second"]
FLOORS: Tuple[int, ...] = band_lane.BAND_GRID_V2["margin_floor_usd"]
CAPS: Tuple[float, ...] = band_lane.BAND_GRID_V2["favorite_price_cap"]
PATIENCES: Tuple[int, ...] = band_lane.BAND_GRID_V2["patience_s"]
ASK_FLOOR = band_lane.BAND_V2_ASK_FLOOR
CONTROL_FLOOR = 50
REGISTRABLE_MIN_FLOOR = 75
REGISTRABLE_MIN_DECISION = 180
PAIR_SUM_RANGE = (0.90, 1.10)
WINDOW_S = band_lane.WINDOW_S
LADDER_SPAN_S = 30
LADDER_BUDGET_USD = 25.0
CAPACITY_BUDGETS = (25.0, 100.0)
# Oracle noise is steepest under $25 (29% disagreement over 0-25 on the
# public data, concentrated at the smallest margins), so that range is
# split finer than the margin-study buckets to keep the ceiling honest.
FINAL_MARGIN_BUCKETS = ((0, 5), (5, 10), (10, 25), (25, 50), (50, 75), (75, 100), (100, 150), (150, 10**9))
# Excluded populations that the RULE excludes (its adverse-selection pool),
# as opposed to windows the data cannot score (coverage, reported apart):
# no ladder record, a record whose first sample lies beyond the patience
# (the engine latches band_decision_missed there), or a discovery host's.
SELECTION_EXCLUSIONS = ("no_print", "out_of_band", "never_cleared", "ladder_direction_mismatch")
AVAILABILITY_EXCLUSIONS = ("no_print_row", "uncovered", "no_ladder", "ladder_uncovered", "ladder_discovery_host")
# Ladder rows are promotion evidence from the trading IP only; the Mac
# stopgap observer's rows count once the overlap check has been recorded
# in meta (section C: >= 3 days agreeing within one tick on >= 95%).
EVIDENCE_HOST = "vps"
DISCOVERY_HOST = "mac"
MAC_ACCEPTED_META = "mac_ladder_accepted_at"
OVERLAP_MIN_DAYS = 3
OVERLAP_MIN_AGREEMENT = 0.95
OVERLAP_TICK = 0.01
# A ladder record without a band_anchor of the same cid is placed by its
# flush time only when the flush was on time (anchor + 31 s, before the
# window end plus this slack); a late `gone` flush cannot be placed.
LADDER_FLUSH_SLACK_S = 5.0
PAPER_ORDER_PREFIX = "paper-"
DEFAULT_FEE_RATE = 0.07
FEE_PARITY_TOLERANCE = 0.001
LATENCY_SHIFTS = (1, 2)
TICK_FRAGILE_SHIFT = 0.01
ORACLE_MARGIN = 0.005
HALVES_TOLERANCE = 0.01
DIRECTIONS_TOLERANCE = 0.02
EXCLUDED_MIN_N = 20
PROMOTION_MIN_N = 100
PROMOTION_MIN_DAYS = 14
COVERAGE_MIN = 0.9
REGISTER_MIN_LADDER_DAYS = 7
EDGE_MIGRATION_ASK = 0.99
EDGE_MIGRATION_DAYS = 7
# A settled window's row is built once its ladder record is in a session
# file or this long after the window end, whichever comes first, so a row
# is written exactly once with everything it will ever hold.
LADDER_GRACE_S = band_lane.UNRESOLVED_FINAL_AFTER_S
E_BH_ALPHA = evidence_accrual.E_BH_ALPHA
MODELS = ("signal", "print", "ladder")
# Defect tripwires hold a cell as manual_audit; readiness requirements keep
# it accruing; a kill is permanent.
DEFECT_TRIPWIRES = (
    "wr_too_good",
    "adverse_selected",
    "halves_below_break_even",
    "directions_below_break_even",
    "wr_above_oracle_ceiling",
    "oracle_ceiling_below_break_even",
    "tick_fragile",
)
READINESS_REQUIREMENTS = ("insufficient_support", "coverage_low")
KILL_TRIPWIRES = ("edge_migration",)
CELL_ID_RE = re.compile(r"^d(\d+)_f(\d+)_c(\d\.\d+)_p(\d+)$")


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _mean(values: Sequence[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def git_sha() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=str(ROOT), capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


# --- fee ---------------------------------------------------------------------


def fill_fee_rate(record: Mapping[str, Any]) -> Optional[float]:
    """The venue's per-share fee rate implied by one fill record: the engine
    books fee = rate x shares x price x (1 - price)."""
    try:
        fee, shares, price = float(record["fee"]), float(record["filled"]), float(record["fill_price"])
    except (KeyError, TypeError, ValueError):
        return None
    base = shares * price * (1.0 - price)
    return fee / base if base > 0.0 and fee >= 0.0 else None


def is_paper_fill(record: Mapping[str, Any]) -> bool:
    """The paper observer's synthetic fills (order_id paper-...) reproduce
    the engine constant by construction and never touch the venue."""
    return str(record.get("order_id") or "").startswith(PAPER_ORDER_PREFIX)


def pinned_fee_rate(fills: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Median implied rate over the distinct venue-confirmed fills (one per
    order), or the engine's constant with a warning when there are none.
    The fee on a filled record is booked by the engine at its own rate
    (PendingLivePosition::fill_fee), so this recovers the rate the engine
    applied through real fills, not a venue-reported fee: the source says
    so, and a rate away from the constant is a warning, never parity."""
    rates: Dict[str, float] = {}
    paper = 0
    for record in fills:
        if is_paper_fill(record):
            paper += 1
            continue
        rate = fill_fee_rate(record)
        if rate is None:
            continue
        key = str(record.get("order_id") or record.get("intent_id") or len(rates))
        rates.setdefault(key, rate)
    if not rates:
        return {
            "rate": DEFAULT_FEE_RATE,
            "n": 0,
            "source": "default",
            "paper_fills_skipped": paper,
            "warning": "no realized fills in the session logs; fee pinned at the engine constant %.3f"
            % DEFAULT_FEE_RATE,
        }
    rate = statistics.median(rates.values())
    result: Dict[str, Any] = {
        "rate": rate,
        "n": len(rates),
        "source": "engine_rate_via_fills",
        "engine_rate": DEFAULT_FEE_RATE,
        "paper_fills_skipped": paper,
        "note": "the fee on a filled record is the engine's own rate x shares x price x (1 - price); no venue-reported fee is recorded",
    }
    if abs(rate - DEFAULT_FEE_RATE) > FEE_PARITY_TOLERANCE:
        result["warning"] = "fee rate on realized fills %.4f differs from the engine constant %.3f" % (rate, DEFAULT_FEE_RATE)
    return result


def break_even(price: float, fee_rate: float) -> float:
    return float(price) + float(fee_rate) * float(price) * (1.0 - float(price))


def net_per_usd(price: float, won: bool, fee_rate: float) -> float:
    """1 USD buys 1/(price + fee) shares paying 1 each on a win."""
    return (1.0 / break_even(price, fee_rate) - 1.0) if won else -1.0


# --- grammar -----------------------------------------------------------------


def normalized_rule(raw: Mapping[str, Any]) -> Dict[str, Any]:
    return band_lane.normalized_band_rule_v2(raw)


def cell_id(rule: Mapping[str, Any]) -> str:
    return "d%d_f%d_c%.2f_p%d" % (
        int(rule["decision_second"]),
        int(rule["margin_floor_usd"]),
        float(rule["favorite_price_cap"]),
        int(rule["patience_s"]),
    )


def rule_from_cell_id(text: str) -> Dict[str, Any]:
    match = CELL_ID_RE.match(text.strip())
    if not match:
        raise ValueError("cell id %r is not d<decision>_f<floor>_c<cap>_p<patience>" % text)
    return normalized_rule(
        {
            "decision_second": int(match.group(1)),
            "margin_floor_usd": int(match.group(2)),
            "favorite_price_cap": float(match.group(3)),
            "patience_s": int(match.group(4)),
        }
    )


def fingerprint(rule: Mapping[str, Any]) -> str:
    payload = {
        "lane": LANE,
        "grammar": GRAMMAR_VERSION,
        "rule": normalized_rule(rule),
        "evaluator_version": EVALUATOR_VERSION,
    }
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def registrable(rule: Mapping[str, Any]) -> bool:
    return int(rule["margin_floor_usd"]) >= REGISTRABLE_MIN_FLOOR and int(rule["decision_second"]) >= REGISTRABLE_MIN_DECISION


def grid_rules() -> List[Dict[str, Any]]:
    return band_lane.grid_v2_rules()


# --- session records ---------------------------------------------------------


def ladder_window_start(record: Mapping[str, Any], cid_windows: Optional[Mapping[str, int]] = None) -> Optional[int]:
    """The window of a band_ladder record: by identity through the
    band_anchor records of the same short cid when one was seen, else from
    the flush time when the flush was on time.  The engine flushes at
    anchor + 31 s, but a contract that left the map is flushed on a later
    contract's cycle (minutes after the window end, arbitrarily late after
    a stall), so ts - anchor - 31 places the record only while it lies
    within [ws, ws + 300 - anchor - 31 + slack]; otherwise None."""
    cid = record.get("cid")
    if cid_windows and cid in cid_windows:
        return int(cid_windows[cid])
    anchor_s = int(record["anchor_s"])
    base = float(record["ts"]) - anchor_s - LADDER_SPAN_S - 1
    window_start = int(math.floor(base / WINDOW_S)) * WINDOW_S
    if base - window_start > WINDOW_S - anchor_s - LADDER_SPAN_S - 1 + LADDER_FLUSH_SLACK_S:
        return None
    return window_start


def anchor_window_start(record: Mapping[str, Any]) -> int:
    return int(round((float(record["ts"]) - float(record["elapsed_s"])) / WINDOW_S)) * WINDOW_S


def _preferred_ladder(by_host: Mapping[str, Mapping[str, Any]]) -> Mapping[str, Any]:
    """The evidence host's record over a discovery host's, whatever their
    sample counts; the fuller record among the same host."""
    return min(by_host.values(), key=lambda record: (record.get("host") != EVIDENCE_HOST, -len(record.get("samples") or [])))


def scan_sessions(session_dirs: Sequence[Path]) -> Dict[str, Any]:
    """band_ladder and band_anchor records keyed (window_start, anchor_s),
    plus every fill.  A record's host is its own `host` field, else the
    directory's (the Mac stopgap observer writes under band-observer/, the
    VPS mirror everything else).  Ladders are placed by the band_anchor of
    the same cid (ladders_unplaced counts those without one whose flush was
    late); `ladders` holds one record per key (the VPS record over the
    Mac's, then the fuller), `ladders_by_host` every host's record for the
    overlap check; a duplicate anchor keeps the one closest to its second."""
    raw_ladders: List[Dict[str, Any]] = []
    anchors: Dict[Tuple[int, int], Dict[str, Any]] = {}
    cid_windows: Dict[str, int] = {}
    fills: List[Dict[str, Any]] = []
    for directory in session_dirs:
        directory = Path(directory)
        if not directory.is_dir():
            continue
        default_host = DISCOVERY_HOST if "band-observer" in directory.as_posix() else EVIDENCE_HOST
        for path in sorted(directory.rglob("*.jsonl")):
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    if '"band_ladder"' not in line and '"band_anchor"' not in line and '"fee"' not in line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(record, Mapping):
                        continue
                    kind = record.get("type")
                    try:
                        if kind == "band_ladder":
                            int(record["anchor_s"])
                            raw_ladders.append({**record, "host": record.get("host") or default_host})
                        elif kind == "band_anchor":
                            window_start = anchor_window_start(record)
                            key = (window_start, int(record["anchor_s"]))
                            current = anchors.get(key)
                            if current is None or float(record["elapsed_s"]) < float(current["elapsed_s"]):
                                anchors[key] = {**record, "host": record.get("host") or default_host}
                            if record.get("cid"):
                                cid_windows.setdefault(str(record["cid"]), window_start)
                        elif kind in ("filled", "reconciled") and "fee" in record:
                            fills.append(dict(record))
                    except (KeyError, TypeError, ValueError):
                        continue
    ladders_by_host: Dict[Tuple[int, int], Dict[str, Dict[str, Any]]] = {}
    unplaced = 0
    for record in raw_ladders:
        try:
            window_start = ladder_window_start(record, cid_windows)
        except (KeyError, TypeError, ValueError):
            continue
        if window_start is None:
            unplaced += 1
            continue
        hosts = ladders_by_host.setdefault((window_start, int(record["anchor_s"])), {})
        current = hosts.get(record["host"])
        if current is None or len(record.get("samples") or []) > len(current.get("samples") or []):
            hosts[record["host"]] = record
    ladders = {key: _preferred_ladder(hosts) for key, hosts in ladders_by_host.items()}
    return {"ladders": ladders, "ladders_by_host": ladders_by_host, "ladders_unplaced": unplaced, "anchors": anchors, "fills": fills}


def ladder_overlap(ladders_by_host: Mapping[Tuple[int, int], Mapping[str, Mapping[str, Any]]], budget: float = LADDER_BUDGET_USD) -> Dict[str, Any]:
    """Section C's Mac/VPS agreement: over the (window, anchor) keys both
    hosts recorded, the share of shared samples (same t) whose worst price
    at the budget agrees within one tick on every quoted side (both absent
    counts as agreement), and the distinct UTC days covered."""
    shared = agree = 0
    days = set()
    keys = 0
    for (window_start, _), hosts in ladders_by_host.items():
        vps, mac = hosts.get(EVIDENCE_HOST), hosts.get(DISCOVERY_HOST)
        if vps is None or mac is None:
            continue
        keys += 1
        days.add(window_start // 86400)
        index_vps, index_mac = _budget_index(vps, budget), _budget_index(mac, budget)
        direction = vps.get("direction")
        sides = (direction,) if direction in ("up", "down") else ("up", "down")
        mac_samples = {int(sample["t"]): sample for sample in mac.get("samples") or [] if "t" in sample}
        for sample in vps.get("samples") or []:
            other = mac_samples.get(int(sample.get("t", -1)))
            if other is None or index_vps is None or index_mac is None:
                continue
            shared += 1
            # Hosts that latched different directions disagree outright.
            same = mac.get("direction") == direction
            for side in sides:
                worst_vps = _quote_worst(_side_quotes(sample, side)[0], index_vps)
                worst_mac = _quote_worst(_side_quotes(other, side)[0], index_mac)
                if (worst_vps is None) != (worst_mac is None) or (worst_vps is not None and abs(worst_vps - worst_mac) > OVERLAP_TICK + 1e-9):
                    same = False
            agree += int(same)
    agreement = (agree / shared) if shared else None
    return {
        "shared_keys": keys,
        "shared_samples": shared,
        "agreeing_samples": agree,
        "agreement": agreement,
        "days": len(days),
        "passes": bool(len(days) >= OVERLAP_MIN_DAYS and agreement is not None and agreement >= OVERLAP_MIN_AGREEMENT),
    }


def _quote_worst(side: Optional[list], index: int) -> Optional[float]:
    if not isinstance(side, list) or index >= len(side) or not isinstance(side[index], list) or side[index][0] is None:
        return None
    return float(side[index][0])


# --- windows table -----------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS windows (
    window_start INTEGER NOT NULL,
    decision_s INTEGER NOT NULL,
    built_at TEXT NOT NULL,
    git_sha TEXT NOT NULL,
    host TEXT NOT NULL,
    open REAL NOT NULL,
    decision_close REAL,
    margin REAL,
    direction TEXT,
    official TEXT NOT NULL,
    final_close REAL,
    final_margin REAL,
    final_bucket TEXT,
    print_status TEXT NOT NULL,
    print_writer_version INTEGER,
    print_signal_entry REAL,
    print_prints_json TEXT,
    print_coverage_json TEXT,
    anchor_json TEXT,
    ladder_json TEXT,
    ladder_host TEXT,
    ladder_basis TEXT,
    print_refreshed_at TEXT,
    PRIMARY KEY (window_start, decision_s)
);
CREATE TABLE IF NOT EXISTS edge_migration (
    window_start INTEGER PRIMARY KEY,
    first_second_above REAL,
    observed_until REAL NOT NULL,
    host TEXT NOT NULL,
    built_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS campaign_accrual (
    campaign_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    n INTEGER NOT NULL,
    wins INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    first_window_start INTEGER,
    last_window_start INTEGER NOT NULL,
    e_value REAL NOT NULL,
    verdict TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (campaign_id, fingerprint)
);
CREATE TABLE IF NOT EXISTS campaign_accrual_windows (
    campaign_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    window_start INTEGER NOT NULL,
    PRIMARY KEY (campaign_id, fingerprint, window_start)
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""
# Columns added after the first tables were built (ALTER TABLE on open).
ADDED_COLUMNS = {"windows": (("ladder_attached_at", "TEXT"),)}


def open_db(path: Path) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA)
    for table, columns in ADDED_COLUMNS.items():
        present = {row[1] for row in connection.execute("PRAGMA table_info(%s)" % table)}
        for name, kind in columns:
            if name not in present:
                connection.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, name, kind))
    connection.commit()
    return connection


def set_meta(connection: sqlite3.Connection, key: str, value: Any) -> None:
    connection.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, _canonical(value)))


def get_meta(connection: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return default if row is None else json.loads(row[0])


def final_bucket(final_margin: Optional[float]) -> Optional[str]:
    if final_margin is None:
        return None
    size = abs(float(final_margin))
    for low, high in FINAL_MARGIN_BUCKETS:
        if low <= size < high:
            return "%s-%s" % (low, high if high < 10**9 else "inf")
    return None


def _print_columns(cache: band_lane.BandCache, window_start: int, decision_s: int) -> Dict[str, Any]:
    """The print summary of one cached prints row, writer v2 only: an older
    writer's row is recorded by version and carries no prints (its
    within-second tie order is wrong, and its list may be absent)."""
    if decision_s not in band_lane.BAND_DECISION_SECONDS:
        return {"print_status": "no_print_row", "print_writer_version": None}
    path = cache.print_path(window_start, decision_s)
    if not path.is_file():
        return {"print_status": "no_print_row", "print_writer_version": None}
    try:
        row = json.loads(path.read_text())
    except (OSError, ValueError):
        return {"print_status": "unreadable", "print_writer_version": None}
    version = int(row.get("writer_version") or 1)
    if version != band_lane.PRINTS_WRITER_VERSION:
        return {"print_status": "writer_v%d" % version, "print_writer_version": version}
    return {
        "print_status": str(row.get("status")),
        "print_writer_version": version,
        "print_signal_entry": row.get("signal_entry"),
        "print_prints_json": _canonical(row.get("signal_prints")) if row.get("signal_prints") is not None else None,
        "print_coverage_json": _canonical(row.get("coverage")) if row.get("coverage") else None,
    }


def _anchor_columns(anchor: Optional[Mapping[str, Any]]) -> Optional[str]:
    if anchor is None:
        return None
    keep = ("anchor_s", "basis", "btc", "open", "margin", "direction", "up", "down", "pair_sum", "stake_usd", "quote_budget_usd", "elapsed_s", "host")
    return _canonical({key: anchor.get(key) for key in keep if key in anchor})


def _ladder_columns(ladder: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if ladder is None:
        return {"ladder_json": None, "ladder_host": None, "ladder_basis": None}
    keep = ("anchor_s", "basis", "btc", "open", "margin", "direction", "budgets_usd", "samples", "cycles", "max_gap_s", "host")
    return {
        "ladder_json": _canonical({key: ladder.get(key) for key in keep if key in ladder}),
        "ladder_host": str(ladder.get("host")),
        "ladder_basis": ladder.get("basis"),
    }


def favourite_first_second_above(
    ladders: Mapping[int, Mapping[str, Any]], threshold: float = EDGE_MIGRATION_ASK
) -> Tuple[Optional[float], float]:
    """Edge-migration series point for one window: the first observed
    second (anchor + t) at which the favourite's smallest-budget FOK worst
    price exceeded the threshold, and the last second observed.  The
    collector writes a null quote when no executable ask exists; with the
    complement's best ask at or below 1 - threshold that is the favourite
    bid at the threshold or above with no ask: above it, not unobserved."""
    first: Optional[float] = None
    observed_until = 0.0
    for anchor_s in sorted(ladders):
        record = ladders[anchor_s]
        direction = record.get("direction")
        sides = (direction,) if direction in ("up", "down") else ("up", "down")
        for sample in record.get("samples") or []:
            try:
                t = int(sample["t"])
            except (KeyError, TypeError, ValueError):
                continue
            observed_until = max(observed_until, float(anchor_s + t))
            if first is not None:
                continue
            for side in sides:
                quotes, complement = _side_quotes(sample, side)
                worst = _quote_worst(quotes, 0)
                if worst is not None:
                    above = worst > threshold
                else:
                    above = complement is not None and float(complement) <= 1.0 - threshold + 1e-9
                if above:
                    first = float(anchor_s + t)
                    break
    return first, observed_until


def build(
    connection: sqlite3.Connection,
    cache: band_lane.BandCache,
    start_ts: int,
    now_ts: int,
    session_dirs: Sequence[Path] = SESSION_DIRS,
    sha: Optional[str] = None,
    refresh_prints: bool = True,
) -> Dict[str, Any]:
    """Append the rows of every settled, labelled window not yet in the
    table.  Disk only: closes come from the day files, labels from the Gamma
    cache, prints from the print cache, quotes from the session logs.  A
    row written without a ladder (its record reached the session dirs after
    the grace, e.g. behind a VPS outage or a missed pull) takes the
    instrument columns when the record arrives, stamped ladder_attached_at;
    labels, prints and built_at are never rewritten."""
    sha = sha or git_sha()
    built_at = _utc_now()
    last = band_lane.last_eligible_window_start(now_ts)
    cache.load_closes(start_ts, last + WINDOW_S, now_ts, fetch=False)
    sessions = scan_sessions(session_dirs)
    ladders, anchors = sessions["ladders"], sessions["anchors"]
    fee = pinned_fee_rate(sessions["fills"])
    existing = {
        (int(row[0]), int(row[1])) for row in connection.execute("SELECT window_start, decision_s FROM windows")
    }
    attached = 0
    for window_start, decision_s in connection.execute("SELECT window_start, decision_s FROM windows WHERE ladder_json IS NULL").fetchall():
        ladder = ladders.get((int(window_start), int(decision_s)))
        if ladder is None:
            continue
        anchor = anchors.get((int(window_start), int(decision_s)))
        columns = _ladder_columns(ladder)
        connection.execute(
            "UPDATE windows SET ladder_json = ?, ladder_host = ?, ladder_basis = ?, anchor_json = ?, host = ?, ladder_attached_at = ? "
            "WHERE window_start = ? AND decision_s = ?",
            (columns["ladder_json"], columns["ladder_host"], columns["ladder_basis"], _anchor_columns(anchor), ladder.get("host") or "public", built_at, int(window_start), int(decision_s)),
        )
        attached += 1
    rows: List[Dict[str, Any]] = []
    pending = 0
    for window_start in range(int(start_ts), last + 1, WINDOW_S):
        official = cache.outcomes.get(str(window_start))
        if official not in ("up", "down"):
            continue
        open_close = cache.close(window_start)
        if open_close is None or not cache.has_prints(window_start):
            continue
        final_close = cache.close(window_start + WINDOW_S)
        final_margin = None if final_close is None else float(final_close) - float(open_close)
        for decision_s in DECISION_SECONDS:
            if (window_start, decision_s) in existing:
                continue
            ladder = ladders.get((window_start, decision_s))
            if ladder is None and now_ts - (window_start + WINDOW_S) < LADDER_GRACE_S:
                pending += 1
                continue
            anchor = anchors.get((window_start, decision_s))
            decision_close = cache.close(window_start + decision_s)
            margin = None if decision_close is None else float(decision_close) - float(open_close)
            direction = None if not margin else ("up" if margin > 0 else "down")
            row: Dict[str, Any] = {
                "window_start": window_start,
                "decision_s": decision_s,
                "built_at": built_at,
                "git_sha": sha,
                "host": (ladder or anchor or {}).get("host") or "public",
                "open": float(open_close),
                "decision_close": decision_close,
                "margin": margin,
                "direction": direction,
                "official": official,
                "final_close": final_close,
                "final_margin": final_margin,
                "final_bucket": final_bucket(final_margin),
                "print_signal_entry": None,
                "print_prints_json": None,
                "print_coverage_json": None,
                "anchor_json": _anchor_columns(anchor),
            }
            row.update(_print_columns(cache, window_start, decision_s))
            row.update(_ladder_columns(ladder))
            rows.append(row)
    if rows:
        columns = list(rows[0])
        connection.executemany(
            "INSERT INTO windows (%s) VALUES (%s)" % (", ".join(columns), ", ".join("?" * len(columns))),
            [tuple(row[column] for column in columns) for row in rows],
        )
    # The print summary is a re-read of public tape, not evidence: a row
    # written from an older print writer takes the writer v2 row once
    # --rebuild-prints has produced it (labels, quotes, built_at untouched).
    refreshed = 0
    if refresh_prints:
        stale = connection.execute(
            "SELECT window_start, decision_s FROM windows WHERE print_writer_version IS NULL OR print_writer_version != ?",
            (band_lane.PRINTS_WRITER_VERSION,),
        ).fetchall()
        for window_start, decision_s in stale:
            fresh = _print_columns(cache, int(window_start), int(decision_s))
            if fresh.get("print_writer_version") != band_lane.PRINTS_WRITER_VERSION:
                continue
            connection.execute(
                "UPDATE windows SET print_status = ?, print_writer_version = ?, print_signal_entry = ?, "
                "print_prints_json = ?, print_coverage_json = ?, print_refreshed_at = ? WHERE window_start = ? AND decision_s = ?",
                (
                    fresh["print_status"],
                    fresh["print_writer_version"],
                    fresh.get("print_signal_entry"),
                    fresh.get("print_prints_json"),
                    fresh.get("print_coverage_json"),
                    built_at,
                    int(window_start),
                    int(decision_s),
                ),
            )
            refreshed += 1
    # Edge-migration series: one point per window with ladder records,
    # recomputed from the session records on every build (a derived
    # instrument series, not label evidence: a late ladder or a fix to the
    # reading of a quote must reach every window).
    by_window: Dict[int, Dict[int, Mapping[str, Any]]] = {}
    for (window_start, anchor_s), record in ladders.items():
        by_window.setdefault(window_start, {})[anchor_s] = record
    points = []
    for window_start, records in sorted(by_window.items()):
        complete = all(anchor_s in records for anchor_s in DECISION_SECONDS)
        if not complete and now_ts - (window_start + WINDOW_S) < LADDER_GRACE_S:
            continue
        first, until = favourite_first_second_above(records)
        host = next(iter(records.values())).get("host") or EVIDENCE_HOST
        points.append((window_start, first, until, host, built_at))
    connection.executemany("INSERT OR REPLACE INTO edge_migration VALUES (?, ?, ?, ?, ?)", points)
    totals = connection.execute(
        "SELECT COUNT(*), COUNT(DISTINCT window_start), "
        "SUM(CASE WHEN print_writer_version = ? THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN decision_s IN (180, 210, 240) THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN decision_s IN (180, 210, 240) AND print_prints_json IS NULL THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN ladder_json IS NOT NULL THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN anchor_json IS NOT NULL THEN 1 ELSE 0 END) FROM windows",
        (band_lane.PRINTS_WRITER_VERSION,),
    ).fetchone()
    print_rows = int(totals[3] or 0)
    summary = {
        "built_at": built_at,
        "git_sha": sha,
        "rows_inserted": len(rows),
        "ladder_rows_attached": attached,
        "print_rows_refreshed": refreshed,
        "rows_total": int(totals[0]),
        "windows_total": int(totals[1]),
        "pending_ladder_grace": pending,
        "writer_v2_share": (int(totals[2] or 0) / print_rows) if print_rows else None,
        "rows_without_signal_prints": int(totals[4] or 0),
        "ladder_rows": int(totals[5] or 0),
        "ladder_rows_by_host": ladder_rows_by_host(connection),
        "anchor_rows": int(totals[6] or 0),
        "edge_migration_points": len(points),
        "fee": fee,
        "session_records": {
            "ladders": len(ladders),
            "ladders_unplaced": sessions["ladders_unplaced"],
            "anchors": len(anchors),
            "fills": len(sessions["fills"]),
        },
    }
    set_meta(connection, "fee", fee)
    set_meta(connection, "last_build", {key: summary[key] for key in ("built_at", "git_sha", "rows_total", "writer_v2_share")})
    connection.commit()
    return summary


def load_rows(connection: sqlite3.Connection, decision_s: Optional[int] = None) -> List[Dict[str, Any]]:
    query = "SELECT * FROM windows"
    params: Tuple[Any, ...] = ()
    if decision_s is not None:
        query += " WHERE decision_s = ?"
        params = (int(decision_s),)
    rows = []
    for raw in connection.execute(query + " ORDER BY window_start, decision_s", params):
        row = dict(raw)
        row["prints"] = json.loads(row["print_prints_json"]) if row["print_prints_json"] else None
        row["coverage"] = json.loads(row["print_coverage_json"]) if row["print_coverage_json"] else None
        row["anchor"] = json.loads(row["anchor_json"]) if row["anchor_json"] else None
        row["ladder"] = json.loads(row["ladder_json"]) if row["ladder_json"] else None
        rows.append(row)
    return rows


def ladder_rows_by_host(connection: sqlite3.Connection) -> Dict[str, int]:
    return {
        str(row[0]): int(row[1])
        for row in connection.execute("SELECT ladder_host, COUNT(*) FROM windows WHERE ladder_json IS NOT NULL GROUP BY ladder_host ORDER BY 1")
    }


def evidence_hosts(connection: sqlite3.Connection) -> Tuple[str, ...]:
    """Hosts whose ladder rows are promotion evidence: the VPS, plus the Mac
    once --accept-mac-ladders recorded the overlap agreement in meta."""
    return (EVIDENCE_HOST, DISCOVERY_HOST) if get_meta(connection, MAC_ACCEPTED_META) else (EVIDENCE_HOST,)


def oracle_noise(
    rows: Sequence[Mapping[str, Any]], labels: Optional[Mapping[int, Optional[str]]] = None
) -> Dict[str, Dict[str, Any]]:
    """Per final-margin bucket: the share of windows whose official label
    disagrees with the sign of close(ws + 300) - open, with the Wilson lower
    bound of that share.  Over every row of the table it is the marginal
    diagnostic; over one cell's own signal windows (labels supplied, rows
    without `official`) it is the noise the cell's ceiling is built on."""
    seen = set()
    counts: Dict[str, Counter] = {}
    for row in rows:
        window_start = int(row["window_start"])
        if window_start in seen or row.get("final_margin") in (None, 0.0):
            continue
        official = row.get("official") if labels is None else labels.get(window_start)
        if official not in ("up", "down"):
            continue
        seen.add(window_start)
        bucket = row.get("final_bucket") or final_bucket(row["final_margin"])
        if bucket is None:
            continue
        counter = counts.setdefault(bucket, Counter())
        counter["n"] += 1
        expected = "up" if float(row["final_margin"]) > 0 else "down"
        counter["disagree"] += int(official != expected)
    return {
        bucket: {
            "n": counter["n"],
            "disagree": counter["disagree"],
            "noise": counter["disagree"] / counter["n"],
            "noise_lower": band_lane.wilson_lower(counter["disagree"], counter["n"]),
        }
        for bucket, counter in sorted(counts.items(), key=lambda item: FINAL_MARGIN_BUCKETS.index(_bucket_bounds(item[0])))
    }


def _bucket_bounds(label: str) -> Tuple[int, int]:
    low, high = label.split("-")
    return int(low), (10**9 if high == "inf" else int(high))


# --- the three models (selection: label-blind) -------------------------------


def signal_windows(rows: Sequence[Mapping[str, Any]], rule: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    floor = float(rule["margin_floor_usd"])
    return [row for row in rows if row.get("direction") and abs(float(row["margin"])) >= floor]


def print_look(row: Mapping[str, Any], rule: Mapping[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    """One look at the public tape: the first BUY print of the signal token
    after the decision, stamped no later than patience + 1 s (prints carry
    whole seconds and the decision second is excluded, so patience 0 reads
    the first print of the next second).  In band means a trade at that
    print; out of band means none.  Writer v2 rows only."""
    if row.get("print_writer_version") != band_lane.PRINTS_WRITER_VERSION or row.get("print_status") != "ok":
        return None, "no_print_row"
    coverage = row.get("coverage") or {}
    oldest = coverage.get("oldest_offset_s")
    if not coverage.get("complete") and oldest is not None and int(oldest) > int(row["decision_s"]):
        return None, "uncovered"
    prints = row.get("prints")
    if prints is None:
        return None, "no_print_row"
    if not prints:
        return None, "no_print"
    offset, price = prints[0]
    if int(offset) > int(rule["patience_s"]) + 1:
        return None, "no_print"
    price = float(price)
    if ASK_FLOOR < price <= float(rule["favorite_price_cap"]):
        return price, None
    return None, "out_of_band"


def _budget_index(ladder: Mapping[str, Any], budget: float) -> Optional[int]:
    budgets = ladder.get("budgets_usd") or []
    for index, value in enumerate(budgets):
        if abs(float(value) - float(budget)) < 1e-6:
            return index
    return None


def _side_quotes(sample: Mapping[str, Any], direction: str) -> Tuple[Optional[list], Optional[float]]:
    """(per-budget quotes of the momentum side, complement best ask) of one
    sample.  A directional record quotes one side and carries the
    complement's best ask; a no-direction record quotes both sides, and the
    complement's smallest-budget worst price stands in for its best ask."""
    quotes = sample.get("q")
    if isinstance(quotes, Mapping):
        side = quotes.get(direction)
        other = quotes.get("down" if direction == "up" else "up") or []
        complement = float(other[0][0]) if other and isinstance(other[0], list) and other[0][0] is not None else None
        return (side if isinstance(side, list) else None), complement
    complement = sample.get("c")
    return (quotes if isinstance(quotes, list) else None), (None if complement is None else float(complement))


def sample_clears(
    sample: Mapping[str, Any], direction: str, budget_index: int, floor: float, cap: float
) -> Optional[Dict[str, Any]]:
    """The engine's gates on one ladder sample: a fresh book, a quote for
    the budget, BandPolicyParams::quote_clears_band (vwap > floor and worst
    <= cap) and pair coherence (vwap + complement best ask in [0.90, 1.10]).
    Returns the executable quote or None."""
    if not sample.get("fresh"):
        return None
    side, complement = _side_quotes(sample, direction)
    if side is None or budget_index >= len(side) or not isinstance(side[budget_index], list):
        return None
    worst, vwap, shares, depth_limited = side[budget_index]
    if worst is None or vwap is None or float(vwap) <= 0.0 or float(worst) <= 0.0:
        return None
    worst, vwap = float(worst), float(vwap)
    if complement is None or not PAIR_SUM_RANGE[0] <= vwap + complement <= PAIR_SUM_RANGE[1]:
        return None
    if not (vwap > floor and worst <= cap):
        return None
    return {
        "t": int(sample["t"]),
        "worst": worst,
        "vwap": vwap,
        "shares": float(shares) if shares is not None else None,
        "depth_limited": bool(depth_limited),
        "pair_sum": vwap + complement,
    }


def ladder_trade(
    rule: Mapping[str, Any],
    ladder: Mapping[str, Any],
    direction: str,
    budget: float = LADDER_BUDGET_USD,
    shift_s: int = 0,
    floor: float = ASK_FLOOR,
) -> Optional[Dict[str, Any]]:
    """The ladder model on one band_ladder record: the first sample with
    t <= patience whose quote at `budget` clears the band on a fresh,
    coherent book is the entry, at its worst (FOK limit) price.  `shift_s`
    is the latency sensitivity: the FOK limit at the decision sample's
    worst price lands shift_s samples later and fills only if that later
    book is fresh, coherent and still walks the budget at or below the
    limit (the engine's order, pipeline.rs execute_trade); the entry stays
    the limit.  Returns None when the rule does not trade."""
    if ladder.get("direction") not in (None, direction):
        return None
    index = _budget_index(ladder, budget)
    if index is None:
        return None
    cap = float(rule["favorite_price_cap"])
    patience = int(rule["patience_s"])
    samples = {int(sample["t"]): sample for sample in ladder.get("samples") or [] if "t" in sample}
    for t in sorted(samples):
        if t > patience:
            break
        quote = sample_clears(samples[t], direction, index, floor, cap)
        if quote is None:
            continue
        trade = {**quote, "direction": direction, "entry": quote["worst"], "budget_usd": float(budget), "decided_t": t}
        if shift_s:
            later = samples.get(t + int(shift_s))
            filled = sample_clears(later, direction, index, floor, cap) if later else None
            if filled is None or filled["worst"] > quote["worst"] + 1e-9:
                return None
            trade["filled_t"] = t + int(shift_s)
        return trade
    return None


def ladder_first_sample(ladder: Mapping[str, Any]) -> Optional[int]:
    offsets = [int(sample["t"]) for sample in ladder.get("samples") or [] if "t" in sample]
    return min(offsets) if offsets else None


def engine_basis(ladder: Optional[Mapping[str, Any]]) -> Optional[Tuple[str, float]]:
    """(direction, margin) a band_ladder record latched on the Binance tick
    basis (band_basis_prices: the tick at-or-before the decision instant
    against the tick at-or-before the open); None for a composite-basis or
    no-direction record, which falls back to the row's kline basis."""
    if ladder is None or ladder.get("basis") != "binance":
        return None
    direction, margin = ladder.get("direction"), ladder.get("margin")
    if direction not in ("up", "down") or margin is None:
        return None
    return str(direction), float(margin)


def select_cell(
    rows: Sequence[Mapping[str, Any]],
    rule: Mapping[str, Any],
    model: str,
    ladder_hosts: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Label-blind selection under one model: the trades the cell takes
    (entry prices) and every excluded population by reason.  The signal
    and print models gate on the row's kline basis; the ladder model gates
    floor and direction on the record's own Binance tick basis, the one the
    engine latched (the kline close of the decision second is the last
    trade of [ws + d, ws + d + 1), up to a second after the t = 0 book
    sample), and reports how often the two bases disagree.  With
    `ladder_hosts`, a ladder from another host is discovery data: filed as
    ladder_discovery_host, not covered."""
    if model not in MODELS:
        raise ValueError("unknown model %r" % model)
    floor = float(rule["margin_floor_usd"])
    patience = int(rule["patience_s"])
    trades: List[Dict[str, Any]] = []
    excluded: Dict[str, List[Mapping[str, Any]]] = {}
    covered = 0
    capacity = {"%d" % budget: [0, 0] for budget in CAPACITY_BUDGETS}
    basis = {"compared": 0, "sign_flips": 0, "floor_changes": 0}
    for row in rows:
        direction, margin = row.get("direction"), row.get("margin")
        ladder = row.get("ladder") if model == "ladder" else None
        discovery = False
        if ladder is not None and ladder_hosts is not None and (row.get("ladder_host") or ladder.get("host")) not in ladder_hosts:
            ladder, discovery = None, True
        engine = engine_basis(ladder)
        if engine is not None:
            if direction:
                basis["compared"] += 1
                basis["sign_flips"] += int(engine[0] != direction)
                basis["floor_changes"] += int((abs(float(margin)) >= floor) != (abs(engine[1]) >= floor))
            direction, margin = engine
        if not direction:
            continue
        margin = float(margin)
        if abs(margin) < floor:
            if model == "signal":
                excluded.setdefault("below_floor", []).append(row)
            continue
        base = {
            "window_start": int(row["window_start"]),
            "direction": direction,
            "margin": margin,
            "final_margin": row.get("final_margin"),
            "final_bucket": row.get("final_bucket"),
        }
        if model == "signal":
            trades.append({**base, "entry": None})
            continue
        if model == "print":
            price, rejection = print_look(row, rule)
            if rejection not in AVAILABILITY_EXCLUSIONS:
                covered += 1
            if price is None:
                excluded.setdefault(str(rejection), []).append(row)
            else:
                trades.append({**base, "entry": price, "t": int(row["prints"][0][0])})
            continue
        if ladder is None:
            excluded.setdefault("ladder_discovery_host" if discovery else "no_ladder", []).append(row)
            continue
        if ladder.get("direction") not in (None, direction):
            excluded.setdefault("ladder_direction_mismatch", []).append(row)
            continue
        first_t = ladder_first_sample(ladder)
        if first_t is None or first_t > patience:
            excluded.setdefault("ladder_uncovered", []).append(row)
            continue
        covered += 1
        for budget in CAPACITY_BUDGETS:
            cleared = ladder_trade(rule, ladder, direction, budget)
            capacity["%d" % budget][1] += 1
            capacity["%d" % budget][0] += int(cleared is not None and not cleared["depth_limited"])
        trade = ladder_trade(rule, ladder, direction, LADDER_BUDGET_USD)
        if trade is None:
            excluded.setdefault("never_cleared", []).append(row)
            continue
        shifted = {
            str(shift): (ladder_trade(rule, ladder, direction, LADDER_BUDGET_USD, shift) or {}).get("entry")
            for shift in LATENCY_SHIFTS
        }
        trades.append({**base, **trade, "shifted": shifted})
    signal_count = len(trades) + sum(len(items) for reason, items in excluded.items() if reason != "below_floor")
    return {
        "model": model,
        "rule": dict(rule),
        "trades": trades,
        "excluded": excluded,
        "signal_windows": signal_count,
        "coverage": (covered / signal_count) if (model != "signal" and signal_count) else None,
        "capacity": {key: (value[0] / value[1] if value[1] else None) for key, value in capacity.items()}
        if model == "ladder"
        else None,
        "basis_disagreement": basis if model == "ladder" else None,
    }


# --- scoring (labels enter here) ---------------------------------------------


def _win_stats(won: Sequence[bool]) -> Dict[str, Any]:
    total = len(won)
    wins = sum(1 for flag in won if flag)
    return {
        "n": total,
        "wins": wins,
        "win_rate": wins / total if total else None,
        "wilson_lower": band_lane.wilson_lower(wins, total),
    }


def _economics(entries: Sequence[float], won: Sequence[bool], fee_rate: float, shift: float = 0.0) -> Dict[str, Any]:
    if not entries:
        return {"mean_break_even": None, "mean_net_per_usd": None}
    prices = [min(0.999, float(price) + shift) for price in entries]
    return {
        "mean_break_even": _mean([break_even(price, fee_rate) for price in prices]),
        "mean_net_per_usd": _mean([net_per_usd(price, flag, fee_rate) for price, flag in zip(prices, won)]),
    }


def _wilson_upper(wins: int, total: int) -> Optional[float]:
    lower = band_lane.wilson_lower(total - wins, total)
    return None if lower is None else 1.0 - lower


def score_selection(
    selection: Mapping[str, Any],
    labels: Mapping[int, Optional[str]],
    fee_rate: float,
    noise: Mapping[str, Mapping[str, Any]],
    edge_series: Optional[Sequence[Tuple[int, Optional[float]]]] = None,
    cleared: Sequence[str] = (),
) -> Dict[str, Any]:
    """Everything section C asks of a cell: n, W, Wilson lower, mean
    break-even, net/USD, halves, directions, excluded populations and the
    adverse-selection flag, the oracle ceiling on the final-margin mix,
    tick fragility, latency sensitivity, capacity, and the tripwires.
    `noise` is the table-wide marginal oracle noise, reported as a
    diagnostic; the ceiling is built on the noise among the cell's own
    signal windows.  `cleared` names the defect tripwires an operator has
    audited (--clear-audit): they are still reported, but no longer hold."""
    rule = selection["rule"]
    model = selection["model"]
    trades = [trade for trade in selection["trades"] if labels.get(trade["window_start"]) in ("up", "down")]
    won = [trade["direction"] == labels[trade["window_start"]] for trade in trades]
    entries = [trade["entry"] for trade in trades]
    priced = model != "signal"
    overall = _win_stats(won)
    economics = _economics(entries, won, fee_rate) if priced else {"mean_break_even": None, "mean_net_per_usd": None}
    even = economics["mean_break_even"]
    half = len(trades) // 2
    halves = [_win_stats(won[:half]), _win_stats(won[half:])]
    directions = {
        name: _win_stats([flag for trade, flag in zip(trades, won) if trade["direction"] == name])
        for name in ("up", "down")
    }
    excluded: Dict[str, Dict[str, Any]] = {}
    pooled: List[bool] = []
    population: List[Mapping[str, Any]] = list(trades)
    for reason, rows in selection["excluded"].items():
        flags = [row["direction"] == labels[row["window_start"]] for row in rows if labels.get(row["window_start"]) in ("up", "down")]
        excluded[reason] = _win_stats(flags)
        if reason in SELECTION_EXCLUSIONS:
            pooled.extend(flags)
        if reason != "below_floor":
            population.extend(rows)
    excluded_pooled = _win_stats(pooled)
    # The pool is the market's most confident windows (favourite unbuyable
    # or above the cap) and wins more by construction; the flag needs the
    # pool's Wilson interval to sit above the cell's, the raw comparison
    # is reported.
    supported = bool(priced and overall["n"] >= EXCLUDED_MIN_N and excluded_pooled["n"] >= EXCLUDED_MIN_N)
    adverse_raw = bool(supported and excluded_pooled["win_rate"] > overall["win_rate"])
    adverse = bool(supported and excluded_pooled["wilson_lower"] > _wilson_upper(overall["wins"], overall["n"]))
    # Oracle ceiling: an upper confidence bound on the win rate the cell's
    # own windows admit, from the disagreement of the official label with
    # sign(final) among them per final-margin bucket (the table-wide
    # marginal mixes populations the rule never selects and is diagnostic).
    conditional = oracle_noise(population, labels)
    ceilings = [
        1.0 - float(conditional[trade["final_bucket"]]["noise_lower"])
        for trade in trades
        if trade.get("final_bucket") in conditional
    ]
    oracle_ceiling = _mean(ceilings)
    oracle_ceiling_marginal = _mean(
        [1.0 - float(noise[trade["final_bucket"]]["noise"]) for trade in trades if trade.get("final_bucket") in noise]
    )
    fragile = None
    if priced and trades:
        fragile = _economics(entries, won, fee_rate, TICK_FRAGILE_SHIFT)["mean_net_per_usd"] <= 0.0
    latency = None
    if model == "ladder":
        latency = {}
        for shift in LATENCY_SHIFTS:
            kept = [(trade["shifted"].get(str(shift)), flag) for trade, flag in zip(trades, won)]
            kept = [(price, flag) for price, flag in kept if price is not None]
            latency[str(shift)] = {
                **_win_stats([flag for _, flag in kept]),
                **_economics([price for price, _ in kept], [flag for _, flag in kept], fee_rate),
                "fill_rate": (len(kept) / len(trades)) if trades else None,
            }
    first = trades[0]["window_start"] if trades else None
    last = trades[-1]["window_start"] if trades else None
    span_days = ((last - first) / 86400.0) if trades else 0.0
    # The edge-migration series of the cell's own signal windows (a series
    # pooled over weak windows is censored wherever they are the majority).
    migration = None
    if edge_series:
        windows = {int(row["window_start"]) for row in population}
        migration = edge_migration_verdict([point for point in edge_series if point[0] in windows], int(rule["decision_second"]))
    lower = overall["wilson_lower"]
    cleared = tuple(name for name in cleared if name in DEFECT_TRIPWIRES)
    tripwires = {
        "wr_too_good": overall["n"] >= band_lane.TRIPWIRE_MINIMUM_N
        and overall["win_rate"] > band_lane.TRIPWIRE_WIN_RATE,
        "adverse_selected": adverse,
        "halves_below_break_even": bool(
            priced and even is not None and any(part["n"] and part["wilson_lower"] < even - HALVES_TOLERANCE for part in halves)
        ),
        "directions_below_break_even": bool(
            priced
            and even is not None
            and any(part["n"] and part["wilson_lower"] < even - DIRECTIONS_TOLERANCE for part in directions.values())
        ),
        "wr_above_oracle_ceiling": bool(
            oracle_ceiling is not None and lower is not None and overall["n"] >= EXCLUDED_MIN_N and lower > oracle_ceiling
        ),
        "oracle_ceiling_below_break_even": bool(
            priced and even is not None and oracle_ceiling is not None and oracle_ceiling < even + ORACLE_MARGIN
        ),
        "tick_fragile": bool(fragile),
        "insufficient_support": overall["n"] < PROMOTION_MIN_N or span_days < PROMOTION_MIN_DAYS,
        "coverage_low": bool(priced and (selection["coverage"] is None or selection["coverage"] < COVERAGE_MIN)),
        "edge_migration": bool(migration and migration["kill"]),
    }
    clears = bool(priced and lower is not None and even is not None and lower >= even)
    defects = [name for name in DEFECT_TRIPWIRES if tripwires[name] and name not in cleared]
    ready = not any(tripwires[name] for name in READINESS_REQUIREMENTS)
    return {
        "model": model,
        "rule": dict(rule),
        "cell_id": cell_id(rule),
        **overall,
        **economics,
        "halves": halves,
        "directions": directions,
        "excluded": excluded,
        "excluded_pooled": excluded_pooled,
        "adverse_raw": adverse_raw,
        "signal_windows": selection["signal_windows"],
        "coverage": selection["coverage"],
        "capacity": selection.get("capacity"),
        "basis_disagreement": selection.get("basis_disagreement"),
        "oracle_ceiling": oracle_ceiling,
        "oracle_ceiling_marginal": oracle_ceiling_marginal,
        "oracle_noise_conditional": conditional,
        "tick_fragile": fragile,
        "latency": latency,
        "first_window_start": first,
        "last_window_start": last,
        "span_days": span_days,
        "edge_migration": migration,
        "clears_break_even": clears,
        "tripwires": tripwires,
        "defects": defects,
        "audit_cleared": [name for name in cleared if tripwires[name]],
        "ready": ready,
        # Defects are always reported; they hold a cell (manual_audit) once
        # it is ready for the promotion decision, where the tripwires fail
        # closed.  Before that the halves, directions and tick checks have no
        # support to speak of and the cell keeps accruing.
        "held": bool(ready and defects),
        "killed": any(tripwires[name] for name in KILL_TRIPWIRES),
        "promotable": clears and not any(fired for name, fired in tripwires.items() if name not in cleared),
    }


def edge_migration_verdict(
    series: Sequence[Tuple[int, Optional[float]]], decision_s: int, days: int = EDGE_MIGRATION_DAYS
) -> Dict[str, Any]:
    """The 7-day median of the first second the favourite ask exceeded 0.99;
    a window that never did counts as later than every observed second.  A
    registered cell dies when the median drifts below its decision second:
    the edge has migrated earlier than the rule looks."""
    if not series:
        return {"n": 0, "median": None, "kill": False}
    newest = max(window_start for window_start, _ in series)
    recent = [value for window_start, value in series if window_start > newest - days * 86400]
    values = sorted(math.inf if value is None else float(value) for value in recent)
    median = statistics.median(values) if values else None
    return {
        "n": len(values),
        "censored": sum(1 for value in values if value == math.inf),
        "median": None if median is None or median == math.inf else median,
        "kill": bool(median is not None and median < float(decision_s)),
    }


def load_edge_series(connection: sqlite3.Connection) -> List[Tuple[int, Optional[float]]]:
    return [
        (int(row[0]), None if row[1] is None else float(row[1]))
        for row in connection.execute("SELECT window_start, first_second_above FROM edge_migration ORDER BY window_start")
    ]


# --- grid --------------------------------------------------------------------


def labels_of(rows: Iterable[Mapping[str, Any]]) -> Dict[int, Optional[str]]:
    return {int(row["window_start"]): row.get("official") for row in rows}


def evaluate_cell(
    rows_by_decision: Mapping[int, Sequence[Mapping[str, Any]]],
    rule: Mapping[str, Any],
    fee_rate: float,
    noise: Mapping[str, Mapping[str, Any]],
    edge_series: Optional[Sequence[Tuple[int, Optional[float]]]] = None,
    labels: Optional[Mapping[int, Optional[str]]] = None,
    ladder_hosts: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    rule = normalized_rule(rule)
    rows = rows_by_decision.get(int(rule["decision_second"]), [])
    labels = labels_of(rows) if labels is None else labels
    report: Dict[str, Any] = {"cell_id": cell_id(rule), "fingerprint": fingerprint(rule), "rule": rule, "registrable": registrable(rule)}
    for model in MODELS:
        report[model] = score_selection(select_cell(rows, rule, model, ladder_hosts), labels, fee_rate, noise, edge_series)
    return report


def grid(
    connection: sqlite3.Connection,
    fee_rate: Optional[float] = None,
    rules: Optional[Sequence[Mapping[str, Any]]] = None,
    null_replicates: int = 0,
    seed: int = 0,
) -> Dict[str, Any]:
    """Score every cell of the grammar under the three models.  Discovery
    reads every host's ladder rows (the evidence hosts are reported); with
    null_replicates > 0 the ladder model's false-clear rate under the
    break-even null is reported beside the cells."""
    rows = load_rows(connection)
    rows_by_decision: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        rows_by_decision.setdefault(int(row["decision_s"]), []).append(row)
    fee = get_meta(connection, "fee") or {"rate": DEFAULT_FEE_RATE, "source": "default"}
    fee_rate = float(fee["rate"]) if fee_rate is None else float(fee_rate)
    noise = oracle_noise(rows)
    series = load_edge_series(connection)
    rules = [normalized_rule(rule) for rule in (rules or grid_rules())]
    cells = [evaluate_cell(rows_by_decision, rule, fee_rate, noise, series) for rule in rules]
    null = None
    if int(null_replicates) > 0:
        selections = [select_cell(rows_by_decision.get(int(rule["decision_second"]), []), rule, "ladder") for rule in rules]
        null = null_check(selections, fee_rate, int(null_replicates), int(seed))
    last_build = get_meta(connection, "last_build") or {}
    return {
        "generated_at": _utc_now(),
        "evaluator_version": EVALUATOR_VERSION,
        "grammar_version": GRAMMAR_VERSION,
        "git_sha": last_build.get("git_sha"),
        "fee": {**fee, "rate": fee_rate},
        "windows": {"rows": len(rows), "windows": len({row["window_start"] for row in rows}), "writer_v2_share": last_build.get("writer_v2_share")},
        "ladder_rows": sum(1 for row in rows if row.get("ladder")),
        "ladder_rows_by_host": ladder_rows_by_host(connection),
        "evidence_hosts": list(evidence_hosts(connection)),
        "oracle_noise": noise,
        "edge_migration_points": len(series),
        "null_check": null,
        "cells": cells,
    }


def null_check(selections: Sequence[Mapping[str, Any]], fee_rate: float, replicates: int, seed: int) -> Dict[str, Any]:
    """Re-score the same label-blind selections under the break-even null
    that clears_break_even tests: per replicate one uniform draw per
    window, and a cell's trade in that window wins iff the draw is below
    break-even at its entry, so every cell's win probability is exactly
    its break-even and cells sharing a window share its luck.  Reports the
    share of (replicate, cell) pairs whose Wilson lower bound clears
    break-even (the per-cell false-clear rate, over cells with trades) and
    the share of replicates in which any cell clears (the family
    false-clear rate of the search).  A label shuffle across windows would
    destroy the direction/label link and collapse every cell to ~50%, far
    below any break-even: vacuous at the margin the evaluator gates on."""
    rng = random.Random(int(seed))
    populated = [selection for selection in selections if selection["trades"]]
    evens = [[break_even(trade["entry"], fee_rate) for trade in selection["trades"]] for selection in populated]
    windows = sorted({trade["window_start"] for selection in populated for trade in selection["trades"]})
    promoted = 0
    family_hits = 0
    for _ in range(int(replicates)):
        draws = {window_start: rng.random() for window_start in windows}
        cleared = 0
        for selection, cell_evens in zip(populated, evens):
            won = [draws[trade["window_start"]] < even for trade, even in zip(selection["trades"], cell_evens)]
            cleared += int(_win_stats(won)["wilson_lower"] >= _mean(cell_evens))
        promoted += cleared
        family_hits += int(cleared > 0)
    total = int(replicates) * len(populated)
    return {
        "replicates": int(replicates),
        "cells": len(populated),
        "promoted": promoted,
        "promote_share": (promoted / total) if total else None,
        "family_false_clear_share": (family_hits / int(replicates)) if replicates and populated else None,
    }


def _flags(score: Mapping[str, Any]) -> str:
    names = [name for name, fired in score["tripwires"].items() if fired]
    return ",".join(names) if names else "-"


def _fmt(value: Optional[float], digits: int = 3) -> str:
    return "-" if value is None else "%.*f" % (digits, value)


def grid_text(report: Mapping[str, Any], top: int = 10) -> str:
    lines = [
        "executable_truth grid %s: %d rows / %d windows, writer_v2_share=%s, ladder_rows=%d, fee=%.4f (%s, n=%s)"
        % (
            report["generated_at"],
            report["windows"]["rows"],
            report["windows"]["windows"],
            _fmt(report["windows"].get("writer_v2_share"), 3),
            report["ladder_rows"],
            report["fee"]["rate"],
            report["fee"].get("source"),
            report["fee"].get("n"),
        ),
        "ladder rows by host: %s (evidence hosts: %s)"
        % (", ".join("%s=%d" % item for item in report["ladder_rows_by_host"].items()) or "-", ",".join(report["evidence_hosts"])),
        "oracle noise by final-margin bucket (marginal, diagnostic): "
        + ", ".join("%s: %.3f (n=%d)" % (bucket, value["noise"], value["n"]) for bucket, value in report["oracle_noise"].items()),
    ]
    null = report.get("null_check")
    if null:
        lines.append(
            "break-even null (%d replicates, %d ladder cells with trades): per-cell false-clear %s, family false-clear %s"
            % (null["replicates"], null["cells"], _fmt(null["promote_share"]), _fmt(null["family_false_clear_share"]))
        )
    for model in MODELS:
        scored = [cell for cell in report["cells"] if cell[model]["n"]]
        if model == "signal":
            seen = set()
            unique = []
            for cell in scored:
                key = (cell["rule"]["decision_second"], cell["rule"]["margin_floor_usd"])
                if key not in seen:
                    seen.add(key)
                    unique.append(cell)
            scored = sorted(unique, key=lambda cell: -(cell[model]["wilson_lower"] or 0.0))
            lines.append("\n%s model (ceiling, never gated): decision/floor n W WR lower halves up/down" % model)
        else:
            # Ranked by the Wilson edge (lower bound minus break-even), so a
            # one-trade cell at 100% never outranks a supported one.
            scored = sorted(scored, key=lambda cell: -(cell[model]["wilson_lower"] - cell[model]["mean_break_even"]))
            lines.append("\n%s model, top %d by Wilson edge (lower - BE): cell n W WR lower BE net/USD excl_WR oracle flags" % (model, top))
        for cell in scored[:top]:
            score = cell[model]
            if model == "signal":
                lines.append(
                    "  d%d f%d: n=%d W=%d WR=%s lower=%s halves=%s/%s up/down=%s/%s"
                    % (
                        cell["rule"]["decision_second"],
                        cell["rule"]["margin_floor_usd"],
                        score["n"],
                        score["wins"],
                        _fmt(score["win_rate"]),
                        _fmt(score["wilson_lower"]),
                        _fmt(score["halves"][0]["win_rate"]),
                        _fmt(score["halves"][1]["win_rate"]),
                        _fmt(score["directions"]["up"]["win_rate"]),
                        _fmt(score["directions"]["down"]["win_rate"]),
                    )
                )
            else:
                lines.append(
                    "  %s: n=%d W=%d WR=%s lower=%s BE=%s net=%s excl=%s(n=%d) oracle=%s %s%s"
                    % (
                        cell["cell_id"],
                        score["n"],
                        score["wins"],
                        _fmt(score["win_rate"]),
                        _fmt(score["wilson_lower"]),
                        _fmt(score["mean_break_even"]),
                        _fmt(score["mean_net_per_usd"], 4),
                        _fmt(score["excluded_pooled"]["win_rate"]),
                        score["excluded_pooled"]["n"],
                        _fmt(score["oracle_ceiling"]),
                        "CLEARS " if score["clears_break_even"] else "",
                        _flags(score),
                    )
                )
    return "\n".join(lines)


# --- registration, accrual, gate artifact ------------------------------------


def ladder_days(connection: sqlite3.Connection, hosts: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    """Distinct UTC days with a ladder row from an evidence host (the Mac
    stopgap's rows count once accepted), and the span they cover."""
    hosts = tuple(evidence_hosts(connection) if hosts is None else hosts)
    rows = connection.execute(
        "SELECT DISTINCT window_start / 86400 FROM windows WHERE ladder_json IS NOT NULL AND ladder_host IN (%s) ORDER BY 1"
        % ", ".join("?" * len(hosts)),
        hosts,
    ).fetchall()
    days = [int(row[0]) for row in rows]
    return {"days": len(days), "span_days": (days[-1] - days[0] + 1) if days else 0, "hosts": list(hosts)}


def _iso(ts: int) -> str:
    return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).isoformat()


def register(
    connection: sqlite3.Connection,
    campaign_id: str,
    campaigns_dir: Path = CAMPAIGNS_DIR,
    now_ts: Optional[int] = None,
    report: Optional[Mapping[str, Any]] = None,
    allow_partial_prints: bool = False,
) -> Dict[str, Any]:
    """Pre-register the family: registrable cells whose print model clears
    break-even on a covered tape (coverage >= 0.9 on the rebuilt writer v2
    cache: the partial cache is the margin-selected slice finding B.6
    names, unless --allow-partial-prints says otherwise) without the
    adverse-selection flag, deduped by accepted window set under BOTH the
    print model and the accrued ladder model (window, entry): cells that
    would trade the same windows at the same prices are one cell and its
    aliases; a patience or cap variant that trades differently on ladder
    rows is its own cell.  Refused before 7 days of evidence-host ladder
    rows, over an existing registration, or when no cell qualifies (an
    empty family would occupy the id).  N is fixed here and never grows.
    Fresh evidence starts strictly after both the newest table row and the
    registration clock (registered_at_ts), so a window that started before
    registration but was built later never accrues."""
    path = Path(campaigns_dir) / ("%s.json" % campaign_id)
    if path.exists():
        return {"registered": False, "reason": "campaign %s already registered" % campaign_id, "path": str(path)}
    hosts = evidence_hosts(connection)
    days = ladder_days(connection, hosts)
    if days["days"] < REGISTER_MIN_LADDER_DAYS or days["span_days"] < REGISTER_MIN_LADDER_DAYS:
        return {
            "registered": False,
            "reason": "ladder rows from %s cover %d days (span %d); %d required" % (",".join(hosts), days["days"], days["span_days"], REGISTER_MIN_LADDER_DAYS),
            "ladder_rows_by_host": ladder_rows_by_host(connection),
        }
    last_build = get_meta(connection, "last_build") or {}
    v2_share = last_build.get("writer_v2_share")
    if not allow_partial_prints and (v2_share is None or float(v2_share) < 1.0):
        return {
            "registered": False,
            "reason": "print cache writer_v2_share is %s, not 1.0: run --rebuild-prints and --build first (or --allow-partial-prints)" % _fmt(v2_share),
        }
    report = report or grid(connection)
    rows = load_rows(connection)
    rows_by_decision: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        rows_by_decision.setdefault(int(row["decision_s"]), []).append(row)
    cells: List[Dict[str, Any]] = []
    by_set: Dict[Tuple[frozenset, frozenset], Dict[str, Any]] = {}
    for cell in report["cells"]:
        score = cell["print"]
        if not (cell["registrable"] and score["clears_break_even"] and not score["tripwires"]["adverse_selected"]):
            continue
        if score["tripwires"]["coverage_low"] and not allow_partial_prints:
            continue
        decision_rows = rows_by_decision.get(int(cell["rule"]["decision_second"]), [])
        accepted = (
            frozenset(trade["window_start"] for trade in select_cell(decision_rows, cell["rule"], "print")["trades"]),
            frozenset((trade["window_start"], trade["entry"]) for trade in select_cell(decision_rows, cell["rule"], "ladder", hosts)["trades"]),
        )
        if accepted in by_set:
            by_set[accepted]["aliases"].append(cell["cell_id"])
            continue
        entry = {
            "cell_id": cell["cell_id"],
            "fingerprint": cell["fingerprint"],
            "rule": cell["rule"],
            "aliases": [],
            "print_model": {key: score[key] for key in ("n", "wins", "wilson_lower", "mean_break_even", "mean_net_per_usd", "coverage")},
            "ladder_windows_at_registration": len(accepted[1]),
        }
        by_set[accepted] = entry
        cells.append(entry)
    if not cells:
        return {"registered": False, "reason": "no registrable cell clears the print screen"}
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    newest = connection.execute("SELECT MAX(window_start) FROM windows").fetchone()[0]
    after = max(int(newest) if newest is not None else -1, band_lane.last_eligible_window_start(now_ts))
    campaign = {
        "schema_version": 1,
        "id": campaign_id,
        "lane": LANE,
        "status": "active",
        "registered_at": _iso(now_ts),
        "registered_at_ts": now_ts,
        "registered_after_window_start": after,
        "family_size": len(cells),
        "alpha": E_BH_ALPHA,
        "promote_e": evidence_accrual.PROMOTE_E,
        "futility_e": evidence_accrual.FUTILITY_E,
        "budget_usd": LADDER_BUDGET_USD,
        "fee": report["fee"],
        "grammar_version": GRAMMAR_VERSION,
        "evaluator_version": EVALUATOR_VERSION,
        "git_sha": report.get("git_sha") or git_sha(),
        "ladder_days": days,
        "ladder_hosts": list(hosts),
        "ladder_rows_by_host": ladder_rows_by_host(connection),
        "writer_v2_share": v2_share,
        "audit_cleared": {},
        "cells": cells,
    }
    band_lane._atomic_write(path, json.dumps(campaign, indent=2, sort_keys=True) + "\n")
    return {"registered": True, "path": str(path), "family_size": len(cells), "cells": [cell["cell_id"] for cell in cells]}


def registration_cut(campaign: Mapping[str, Any]) -> int:
    """The last window start that is NOT fresh evidence: the newest row at
    registration, or the registration clock, whichever is later."""
    after = int(campaign.get("registered_after_window_start") or -1)
    registered_at_ts = campaign.get("registered_at_ts")
    if registered_at_ts is None and campaign.get("registered_at"):
        registered_at_ts = dt.datetime.fromisoformat(str(campaign["registered_at"]).replace("Z", "+00:00")).timestamp()
    return max(after, int(registered_at_ts) - 1) if registered_at_ts is not None else after


def campaign_ladder_hosts(connection: sqlite3.Connection, campaign: Mapping[str, Any]) -> Tuple[str, ...]:
    """The hosts a campaign accrues: those fixed at registration, widened
    only by an overlap acceptance recorded since."""
    fixed = tuple(campaign.get("ladder_hosts") or (EVIDENCE_HOST,))
    return tuple(sorted(set(fixed) | set(evidence_hosts(connection))))


def cleared_tripwires(campaign: Mapping[str, Any], fp: str) -> Tuple[str, ...]:
    entry = (campaign.get("audit_cleared") or {}).get(str(fp)) or {}
    return tuple(str(name) for name in entry.get("tripwires") or [] if name in DEFECT_TRIPWIRES)


def clear_audit(
    campaigns_dir: Path, campaign_id: str, cell_id_text: str, tripwires: Sequence[str], note: str, now_ts: Optional[int] = None
) -> Dict[str, Any]:
    """Record an operator's completed audit of a cell's defect tripwires
    (the band_lane audit_cleared analogue): accrual and the gate no longer
    hold the cell on them.  Kill and readiness tripwires cannot be cleared;
    a later clear replaces the earlier one for the same cell."""
    path = Path(campaigns_dir) / ("%s.json" % campaign_id)
    campaign = json.loads(path.read_text())
    cell = next((item for item in campaign["cells"] if item["cell_id"] == cell_id_text), None)
    if cell is None:
        raise ValueError("cell %s is not registered in campaign %s" % (cell_id_text, campaign_id))
    names = sorted(set(str(name) for name in tripwires))
    unknown = [name for name in names if name not in DEFECT_TRIPWIRES]
    if unknown or not names:
        raise ValueError("only defect tripwires can be cleared (%s), not %s" % (", ".join(DEFECT_TRIPWIRES), unknown or "none"))
    if not str(note).strip():
        raise ValueError("an audit clear needs a note")
    cleared = dict(campaign.get("audit_cleared") or {})
    cleared[str(cell["fingerprint"])] = {
        "cell_id": cell["cell_id"],
        "tripwires": names,
        "note": str(note),
        "cleared_at": _iso(int(time.time()) if now_ts is None else int(now_ts)),
    }
    campaign["audit_cleared"] = cleared
    band_lane._atomic_write(path, json.dumps(campaign, indent=2, sort_keys=True) + "\n")
    return {"cleared": True, "cell_id": cell["cell_id"], "fingerprint": cell["fingerprint"], "tripwires": names, "path": str(path)}


def accept_mac_ladders(
    connection: sqlite3.Connection, session_dirs: Sequence[Path] = SESSION_DIRS, now_ts: Optional[int] = None
) -> Dict[str, Any]:
    """Section C's gate on the Mac stopgap's rows: record the acceptance in
    meta only when the Mac/VPS overlap covers >= 3 days and agrees within
    one tick on >= 95% of shared samples; otherwise report why not."""
    overlap = ladder_overlap(scan_sessions(session_dirs)["ladders_by_host"])
    result = {"accepted": bool(overlap["passes"]), "overlap": overlap, "already_accepted": get_meta(connection, MAC_ACCEPTED_META)}
    if overlap["passes"] and not result["already_accepted"]:
        accepted_at = _iso(int(time.time()) if now_ts is None else int(now_ts))
        set_meta(connection, MAC_ACCEPTED_META, {"accepted_at": accepted_at, "overlap": overlap})
        connection.commit()
        result["accepted_at"] = accepted_at
    return result


def load_campaigns(campaigns_dir: Path = CAMPAIGNS_DIR) -> List[Dict[str, Any]]:
    campaigns = []
    for path in sorted(Path(campaigns_dir).glob("*.json")):
        try:
            campaign = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(campaign, Mapping) and campaign.get("lane") == LANE:
            campaigns.append({**campaign, "path": str(path)})
    return campaigns


def look_id(candidate: str, stage: str, fresh_range: Sequence[int]) -> str:
    return factory_generator.look_id(candidate, stage, fresh_range)


def _fresh_selection(
    rows_by_decision: Mapping[int, Sequence[Mapping[str, Any]]],
    rule: Mapping[str, Any],
    after_window_start: int,
    ladder_hosts: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    rows = [
        row
        for row in rows_by_decision.get(int(rule["decision_second"]), [])
        if int(row["window_start"]) > int(after_window_start)
    ]
    return select_cell(rows, rule, "ladder", ladder_hosts)


def accrued_windows(connection: sqlite3.Connection, campaign_id: str, fp: str) -> set:
    return {
        int(row[0])
        for row in connection.execute(
            "SELECT window_start FROM campaign_accrual_windows WHERE campaign_id = ? AND fingerprint = ?", (campaign_id, fp)
        )
    }


def accrue_campaign(
    connection: sqlite3.Connection,
    campaign: Mapping[str, Any],
    loop_config: Optional[Mapping[str, Any]] = None,
    fee_rate: Optional[float] = None,
) -> Dict[str, Any]:
    """Fold every labelled ladder trade after the registration cut that the
    cell's e-process has not seen (campaign_accrual_windows names them, so
    a row built late, behind a ladder grace or a pull gap, is folded when
    it arrives instead of skipped behind a high-water mark) into every
    registered cell's e-process (break-even at the FOK worst price of the
    $25 quote), then decide the family: e-BH at alpha over the cells that
    are ready and hold no uncleared defect; a defect fails closed to
    manual_audit, futility and edge migration kill, everything else keeps
    accruing.  Evidence-host ladder rows only.  One trial-ledger row per
    (fingerprint, look_id)."""
    campaign_id = str(campaign["id"])
    after = registration_cut(campaign)
    hosts = campaign_ladder_hosts(connection, campaign)
    rows = load_rows(connection)
    rows_by_decision: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        rows_by_decision.setdefault(int(row["decision_s"]), []).append(row)
    labels = labels_of(rows)
    noise = oracle_noise(rows)
    series = load_edge_series(connection)
    fee = get_meta(connection, "fee") or {"rate": DEFAULT_FEE_RATE}
    fee_rate = float(fee["rate"]) if fee_rate is None else float(fee_rate)
    now = _utc_now()
    results: List[Dict[str, Any]] = []
    for cell in campaign["cells"]:
        rule = normalized_rule(cell["rule"])
        fp = str(cell["fingerprint"])
        state = connection.execute(
            "SELECT * FROM campaign_accrual WHERE campaign_id = ? AND fingerprint = ?", (campaign_id, fp)
        ).fetchone()
        process = evidence_accrual.EProcess.from_json(state["state_json"]) if state else evidence_accrual.EProcess()
        wins = int(state["wins"]) if state else 0
        status = str(state["status"]) if state else "accruing"
        seen = accrued_windows(connection, campaign_id, fp)
        selection = _fresh_selection(rows_by_decision, rule, after, hosts)
        applied = 0
        for trade in selection["trades"]:
            window_start = int(trade["window_start"])
            if window_start in seen or labels.get(window_start) not in ("up", "down"):
                continue
            won = trade["direction"] == labels[window_start]
            process.update(break_even(trade["entry"], fee_rate), won)
            wins += int(won)
            seen.add(window_start)
            connection.execute(
                "INSERT OR IGNORE INTO campaign_accrual_windows VALUES (?, ?, ?)", (campaign_id, fp, window_start)
            )
            applied += 1
        cleared = cleared_tripwires(campaign, fp)
        score = score_selection(selection, labels, fee_rate, noise, series, cleared)
        verdict = process.verdict()
        results.append(
            {
                "cell_id": cell["cell_id"],
                "fingerprint": fp,
                "process": process,
                "n": process.n,
                "wins": wins,
                "first_window_start": min(seen) if seen else None,
                "last_window_start": max(seen) if seen else after,
                "e_value": process.e_value(),
                "verdict": verdict,
                "applied": applied,
                "score": score,
                "previous_status": status,
                "audit_cleared": list(cleared),
            }
        )
    eligible = {
        result["fingerprint"]: result["e_value"]
        for result in results
        if result["verdict"] != "kill"
        and result["score"]["ready"]
        and not result["score"]["held"]
        and not result["score"]["killed"]
        and not result["previous_status"].startswith("killed")
    }
    family = evidence_accrual.e_bh(eligible, int(campaign["family_size"]), float(campaign.get("alpha", E_BH_ALPHA)), family_size=len(results))
    summary: Dict[str, Any] = {
        "campaign": campaign_id,
        "registration_cut": after,
        "ladder_hosts": list(hosts),
        "ladder_rows_by_host": ladder_rows_by_host(connection),
        "cells": [],
        "e_bh": {key: family[key] for key in ("campaign_n", "candidates", "family", "k_star", "threshold", "overflow")},
    }
    for result in results:
        score = result["score"]
        if result["previous_status"].startswith("killed"):
            status = result["previous_status"]
        elif score["killed"]:
            status = "killed_edge_migration"
        elif result["verdict"] == "kill":
            status = "killed_futility"
        elif score["held"]:
            status = "manual_audit"
        elif result["fingerprint"] in family["promoted"]:
            status = "promote_candidate"
        else:
            status = "accruing"
        connection.execute(
            "INSERT OR REPLACE INTO campaign_accrual VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                campaign_id,
                result["fingerprint"],
                result["n"],
                result["wins"],
                result["process"].to_json(),
                result["first_window_start"],
                result["last_window_start"],
                result["e_value"],
                result["verdict"],
                status,
                now,
            ),
        )
        ledger_row = False
        if result["applied"] and loop_config is not None and result["first_window_start"] is not None:
            ledger_row = factory_generator.append_trial_entry(
                loop_config,
                result["fingerprint"],
                "band_ladder_accrual",
                status,
                n=result["n"],
                wins=result["wins"],
                fresh_range=[result["first_window_start"], result["last_window_start"]],
            )
        summary["cells"].append(
            {
                "cell_id": result["cell_id"],
                "fingerprint": result["fingerprint"],
                "n": result["n"],
                "wins": result["wins"],
                "e_value": result["e_value"],
                "verdict": result["verdict"],
                "status": status,
                "applied": result["applied"],
                "ledger_row": ledger_row,
                "defects": score["defects"],
                "audit_cleared": score["audit_cleared"],
                "ready": score["ready"],
                "tripwires": score["tripwires"],
                "excluded": {reason: value["n"] for reason, value in score["excluded"].items()},
            }
        )
    connection.commit()
    return summary


def gate_artifact(
    connection: sqlite3.Connection, campaign: Mapping[str, Any], cell_id_text: str, fee_rate: Optional[float] = None
) -> Dict[str, Any]:
    """The evidence artifact cmd_band_promotion_artifact (rust_engine
    main.rs) reads: verdict PASS, candidate = the band family, rows with
    signal_entry and won, fresh_range, registration.  Rows are the cell's
    ladder trades after registration at the FOK worst price, evidence hosts
    only; PASS only on an e-BH discovery with every uncleared tripwire
    quiet."""
    cell = next((item for item in campaign["cells"] if item["cell_id"] == cell_id_text), None)
    if cell is None:
        raise ValueError("cell %s is not registered in campaign %s" % (cell_id_text, campaign["id"]))
    rule = normalized_rule(cell["rule"])
    hosts = campaign_ladder_hosts(connection, campaign)
    rows = load_rows(connection)
    rows_by_decision: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        rows_by_decision.setdefault(int(row["decision_s"]), []).append(row)
    labels = labels_of(rows)
    fee = get_meta(connection, "fee") or {"rate": DEFAULT_FEE_RATE}
    fee_rate = float(fee["rate"]) if fee_rate is None else float(fee_rate)
    selection = _fresh_selection(rows_by_decision, rule, registration_cut(campaign), hosts)
    cleared = cleared_tripwires(campaign, str(cell["fingerprint"]))
    score = score_selection(selection, labels, fee_rate, oracle_noise(rows), load_edge_series(connection), cleared)
    state = connection.execute(
        "SELECT * FROM campaign_accrual WHERE campaign_id = ? AND fingerprint = ?", (str(campaign["id"]), cell["fingerprint"])
    ).fetchone()
    status = str(state["status"]) if state else "unaccrued"
    trades = [trade for trade in selection["trades"] if labels.get(trade["window_start"]) in ("up", "down")]
    gate_rows = [
        {
            "window_start": trade["window_start"],
            "signal": trade["direction"],
            "official": labels[trade["window_start"]],
            "signal_entry": trade["entry"],
            "vwap": trade["vwap"],
            "t": trade["t"],
            "won": trade["direction"] == labels[trade["window_start"]],
        }
        for trade in trades
    ]
    if status == "promote_candidate" and score["promotable"]:
        verdict = "PASS"
    elif score["held"] or status == "manual_audit":
        verdict = "IMPLAUSIBLE_MANUAL_AUDIT"
    elif status.startswith("killed"):
        verdict = "FAIL_TOMBSTONE"
    else:
        verdict = "INSUFFICIENT"
    return {
        "schema_version": 1,
        "registration": str(campaign["id"]),
        "candidate": BAND_FAMILY,
        "evaluator_version": EVALUATOR_VERSION,
        "grammar_version": GRAMMAR_VERSION,
        "cell_id": cell["cell_id"],
        "fingerprint": cell["fingerprint"],
        "rule": rule,
        "model": "ladder",
        "budget_usd": LADDER_BUDGET_USD,
        "ladder_hosts": list(hosts),
        "fee_rate": fee_rate,
        "fee": fee,
        "fresh_range": [score["first_window_start"], score["last_window_start"]],
        "registration_cut": registration_cut(campaign),
        "support": score["n"],
        "wins": score["wins"],
        "win_rate": score["win_rate"],
        "wilson_lo": score["wilson_lower"],
        "avg_break_even": score["mean_break_even"],
        "point_edge": None if score["win_rate"] is None else score["win_rate"] - score["mean_break_even"],
        "wilson_edge": None if score["wilson_lower"] is None else score["wilson_lower"] - score["mean_break_even"],
        "net_per_usd": score["mean_net_per_usd"],
        "accrual": None if state is None else {key: state[key] for key in ("n", "wins", "e_value", "verdict", "status", "updated_at")},
        "tripwires": score["tripwires"],
        "audit_cleared": score["audit_cleared"],
        "verdict": verdict,
        "policy_params": {
            "family": BAND_FAMILY,
            "decision_seconds": float(rule["decision_second"]),
            "entry_window_seconds": float(max(int(rule["patience_s"]), 1)),
            "ask_floor": ASK_FLOOR,
            "ask_cap": float(rule["favorite_price_cap"]),
            "stake_usd": 5.0,
            "position_pct": 1.0,
            "min_decision_margin_usd": float(rule["margin_floor_usd"]),
        },
        "rows": gate_rows,
    }


def tick(
    connection: sqlite3.Connection,
    cache: band_lane.BandCache,
    start_ts: int,
    now_ts: int,
    loop_config: Optional[Mapping[str, Any]],
    session_dirs: Sequence[Path] = SESSION_DIRS,
    campaigns_dir: Path = CAMPAIGNS_DIR,
    sha: Optional[str] = None,
) -> Dict[str, Any]:
    # The 15-minute tick never re-reads 20k print files; --build does after --rebuild-prints.
    summary: Dict[str, Any] = {
        "build": build(connection, cache, start_ts, now_ts, session_dirs, sha, refresh_prints=False),
        "campaigns": [],
    }
    for campaign in load_campaigns(campaigns_dir):
        if campaign.get("status") != "active":
            continue
        accrual = accrue_campaign(connection, campaign, loop_config)
        status_path = TRUTH_DIR / "campaigns" / ("%s.json" % campaign["id"])
        if Path(campaigns_dir) != CAMPAIGNS_DIR:
            status_path = Path(campaigns_dir) / "status" / ("%s.json" % campaign["id"])
        band_lane._atomic_write(status_path, json.dumps({**accrual, "updated_at": _utc_now()}, indent=2, sort_keys=True) + "\n")
        summary["campaigns"].append({**accrual, "status_path": str(status_path)})
    return summary


# --- CLI ---------------------------------------------------------------------


def _load_loop_config(explicit: Optional[str]) -> Dict[str, Any]:
    path = band_lane.config_path(explicit)
    return json.loads(path.read_text())


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--build", action="store_true", help="append the rows of newly settled windows to the table")
    action.add_argument("--grid", action="store_true", help="score every grammar cell under the three models")
    action.add_argument("--cell", help="full report for one cell, e.g. d240_f75_c0.97_p15")
    action.add_argument("--register", metavar="ID", help="write deploy/campaigns/<ID>.json with the registrable cells")
    action.add_argument("--gate-json", action="store_true", help="emit the promotion evidence artifact for --campaign/--cell-id")
    action.add_argument("--tick", action="store_true", help="incremental build plus accrual of every registered campaign")
    action.add_argument("--clear-audit", metavar="CELL_ID", help="record a completed audit of a registered cell's defect tripwires (--campaign, --tripwire, --note)")
    action.add_argument("--accept-mac-ladders", action="store_true", help="run the Mac/VPS overlap check and, when it passes, accept Mac ladder rows as evidence")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--start-ts", type=int, help="first window start (default: the band lane's start_ts)")
    parser.add_argument("--now-ts", type=int, help="clock override (tests)")
    parser.add_argument("--sessions-dir", type=Path, action="append", help="session log directory (repeatable; default: the VPS mirror and the Mac observer)")
    parser.add_argument("--campaigns-dir", type=Path, default=CAMPAIGNS_DIR)
    parser.add_argument("--campaign", help="campaign id for --gate-json (default: the only active campaign)")
    parser.add_argument("--cell-id", help="cell id for --gate-json")
    parser.add_argument("--loop-config", help="research loop config for the trial ledger (default: the runner's overlay)")
    parser.add_argument("--output", type=Path, help="where --grid/--cell/--gate-json write their JSON")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--null-replicates", type=int, default=200, help="--grid: replicates of the break-even null on the ladder model (0 skips)")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--allow-partial-prints", action="store_true", help="--register: accept a print cache below writer_v2_share 1.0 / coverage 0.9")
    parser.add_argument("--tripwire", action="append", help="--clear-audit: defect tripwire cleared (repeatable)")
    parser.add_argument("--note", help="--clear-audit: what the audit found")
    args = parser.parse_args(argv)
    loop_config = _load_loop_config(args.loop_config)
    lane = (loop_config.get("lanes") or {}).get(band_lane.LANE) or {}
    start_ts = int(lane.get("start_ts", 0) if args.start_ts is None else args.start_ts)
    now_ts = int(time.time()) if args.now_ts is None else int(args.now_ts)
    session_dirs = tuple(args.sessions_dir) if args.sessions_dir else SESSION_DIRS
    connection = open_db(args.db)
    try:
        if args.build:
            report = build(connection, band_lane.BandCache(), start_ts, now_ts, session_dirs)
            if report["fee"].get("warning"):
                print("WARNING: %s" % report["fee"]["warning"], file=sys.stderr)
            print(json.dumps(report, indent=2, sort_keys=True))
        elif args.grid:
            report = grid(connection, null_replicates=args.null_replicates, seed=args.seed)
            output = args.output or (TRUTH_DIR / ("grid_%s.json" % dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")))
            band_lane._atomic_write(output, json.dumps(report, indent=2, sort_keys=True) + "\n")
            print(grid_text(report, args.top))
            print("written %s" % output)
        elif args.cell:
            report = grid(connection, rules=[rule_from_cell_id(args.cell)])
            cell = report["cells"][0]
            if args.output:
                band_lane._atomic_write(args.output, json.dumps(cell, indent=2, sort_keys=True) + "\n")
            print(json.dumps({**cell, "fee": report["fee"], "oracle_noise": report["oracle_noise"]}, indent=2, sort_keys=True))
        elif args.register:
            report = register(connection, args.register, args.campaigns_dir, now_ts, allow_partial_prints=args.allow_partial_prints)
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0 if report["registered"] else 2
        elif args.clear_audit:
            campaigns = [c for c in load_campaigns(args.campaigns_dir) if c.get("status") == "active"]
            if args.campaign:
                campaigns = [c for c in campaigns if c["id"] == args.campaign]
            if len(campaigns) != 1:
                parser.error("--clear-audit needs exactly one active campaign (use --campaign)")
            if not args.tripwire or not args.note:
                parser.error("--clear-audit needs --tripwire and --note")
            print(json.dumps(clear_audit(args.campaigns_dir, campaigns[0]["id"], args.clear_audit, args.tripwire, args.note, now_ts), indent=2, sort_keys=True))
        elif args.accept_mac_ladders:
            report = accept_mac_ladders(connection, session_dirs, now_ts)
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0 if report["accepted"] else 2
        elif args.gate_json:
            campaigns = [c for c in load_campaigns(args.campaigns_dir) if c.get("status") == "active"]
            if args.campaign:
                campaigns = [c for c in campaigns if c["id"] == args.campaign]
            if len(campaigns) != 1:
                parser.error("--gate-json needs exactly one active campaign (use --campaign)")
            if not args.cell_id:
                parser.error("--gate-json needs --cell-id")
            artifact = gate_artifact(connection, campaigns[0], args.cell_id)
            output = args.output or (TRUTH_DIR / "gates" / ("%s_%s.json" % (campaigns[0]["id"], args.cell_id)))
            band_lane._atomic_write(output, json.dumps(artifact, indent=1, sort_keys=True) + "\n")
            print(json.dumps({key: artifact[key] for key in ("verdict", "support", "wins", "wilson_lo", "avg_break_even", "tripwires")}, indent=2, sort_keys=True))
            print("written %s" % output)
        elif args.tick:
            report = tick(connection, band_lane.BandCache(), start_ts, now_ts, loop_config, session_dirs, args.campaigns_dir)
            print(json.dumps(report, indent=2, sort_keys=True))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
