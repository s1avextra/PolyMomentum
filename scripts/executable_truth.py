#!/usr/bin/env python3
"""The ONE evaluator: executable truth for the band family (roadmap section C).

Table logs/strategy-research/executable_truth/windows.sqlite3: one row per
(window_start, decision_s in {150, 180, 195, 210, 225, 240}), append-only, each row
stamped with its build time (the accrual cut) and git sha.  Per row: the
Binance basis at the open and the decision (margin, direction), the official
Gamma label, the final margin close(ws + 300) - open with its oracle-noise
bucket, the public print summary (writer v2 rows only; the writer version
share is recorded), the engine's band_anchor quote and band_ladder samples
when a session recorded them, and the host that recorded them.

Grammar (band_lane.BAND_GRID_V2): decision_s x floor_usd in {50 control, 75,
100, 150} x cap in {0.92 .. 0.99} x patience in {0, 15, 30} s; ask floor
0.80, direction both (up/down is a tripwire, never a rule); 504 cells, the
315 with floor >= 75 and decision >= 180 registrable.  The 150, 195 and 225
s cells have no print column (`ladder_only`: the ladder is their only
instrument, and the print screen of --register never admits them).  Three models per
cell: signal-only (a ceiling, never gated against break-even), print
one-look (the first public BUY print after the decision within the patience
look: an upper bound) and ladder truth (trade iff the first ladder sample
with t <= patience has worst <= cap, vwap > 0.80, a fresh book and a
coherent pair; entry = worst, the FOK limit, never vwap).  The ladder model
gates floor and direction on the record's own Binance tick basis, the one
the engine latched; the kline basis of the row serves the signal ceiling
and the print model.  Selection never reads a label; scoring does.

Grammar band_event_v4 (docs/adaptive_family_research_2026-10-01.md section
4), beside the cells above: an event cell e<t0>-<t1>_z<z*>_<cap rule> is
(range [t0, t1], z*, cap rule in {c0.98, c0.99, k98}).  The window's ladder
records are tiled into one book sample per elapsed second (anchors every
30 s give 150-270 s); at each second with a live book (age <= 1 s) the
settlement-basis z of scripts/settlement_model.py is computed from Binance
closes of the seconds before it, and the cell enters at the first second
with z >= z*, worst <= cap, vwap > 0.80 and a coherent pair: a FOK limit at
the cap, filled one sample later at that book's worst price (the +1 s
model; entry = that worst: the order is in flight, so the ask floor and the
pair gate do not protect the fill and a collapsed ask fills), a kill
continues the scan, one entry per window.  A directional record quotes the
side the engine latched (the point basis) and, in engine records since
2026-10-01, the other side too on every sample (sq/sc/sage).
Lag 0, lag 2 and a 1 s older price are reported beside it.  The
model's constants and its calibration table are data: a frozen spec bound
by a sha256, fitted on labelled windows before a cut, never in the scoring
path.  k98 is the spec's schedule cap: the largest tick whose break-even
sits 0.5 pp inside the Wilson lower bound of the z bucket.

Book age: the engine's `fresh` flag tolerates books up to 30 s old (19-29 s
old books cleared on 2026-09-19), so every model here also requires the
sampled book's age <= 1 s; a window whose only clearing sample is such a
stalled book is filed `book_stalled` (the instrument failed, not the rule).

Oracle noise and the oracle ceiling are on the settlement basis (closing
60 s TWAP minus the 60 s strike, columns strike_60s, settle_margin,
final_settle_margin); a row without those closes falls back to the point
basis.

Fee: the rate on the engine's filled records (fee / (shares x price x
(1 - price))), which is the engine's own rate booked through real fills,
not a venue-reported fee; paper fills are skipped.

Hosts: ladder rows recorded by the VPS (the trading IP) are evidence; the
Mac stopgap observer's rows are discovery data until --accept-mac-ladders
records a >= 3-day overlap agreeing within one tick on >= 95% of shared
samples (section C).  That acceptance widens discovery (--grid, the paper
twin); a registered family's hosts are fixed at registration: the VPS.

Protocol: --register <id> --cells <ids> writes deploy/campaigns/<id>.json:
the family is the explicit list (N fixed = its length before any outcome
after registered_at is read; V1..V6 name the family of section 4), VPS
ladder rows are its only evidence, and with an event cell the settlement
spec is refitted on every labelled window before registered_at and frozen
in the file with its sha256, beside the stopping rules, the falsifiers and
the paired static controls of section 6.  --register without --cells is
refused: the former screen admitted every qualifying grid cell (32 on
2026-10-01, Mac rows included).  --tick builds incrementally and accrues
every registered cell with an e-process at $25 (event cells with the frozen
spec only), decides the family by e-BH at alpha 0.05 with family_size = N,
and fails closed to manual_audit on any tripwire until --clear-audit
records the operator's audit of that defect; --gate-json emits the evidence
artifact the engine's band-promotion-artifact command consumes (static
cells only: the engine has no event policy).  A registered static cell is
killed when its capacity (the share of its covered signal windows whose
$25 ladder clears at the cap, per UTC day) over the trailing week falls
below half its registration week's, given 14 days of data; three weekly
declines running are a warning.  --cell-gate-json emits the same artifact
for an unregistered cell (verdict INSUFFICIENT, discovery_twin) so the
paper observer can run the candidate as its twin; live mode refuses it.
--event-grid scores event cells on every evidence ladder window under a
discovery spec (or a campaign's frozen spec on its fresh windows).

Tripwire wr_too_good: WR > 0.995 at n >= 100 for a static cell.  For an
event cell it is a test against the frozen ceiling (the mean, over its
entries, of the Wilson upper bound of each entry's frozen z bucket; the
table expects 0.992-1.0 by z bucket): the cell is held when its wins at
n >= 100 are improbable even at that ceiling, P(X >= wins | n, ceiling) <
0.05.  A point comparison with the ceiling tripped on 23-45% of looks
under the frozen calibration itself; a record the ceiling explains never
trips, and a ceiling of 1.0 (buckets that never lost) cannot.

Section 6's looks (accrue_campaign): each is taken once.  F2's bar is the
Wilson lower bound of the frozen rule's first-crossing accuracy on the
windows before the first evidence ladder (the doc's "pre-ladder Wilson
lower"), not of the whole refit.  A fired falsifier holds the event cells
(manual_audit, out of the e-BH candidate set) until --clear-falsifier
records the operator's audit of it.  The day-90 stop fires only when no
cell has been an e-BH discovery at any tick so far; a cell held for a
defect audit counts there.
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
import settlement_model  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
# v2: ladder gating on the record's own basis, conditional oracle ceiling,
# Wilson-separated adverse selection, FOK-limit latency look.
# v3: the kill is the cell's own capacity trend (capacity_trend_verdict),
# not the 7-day median first second above 0.99, which killed every cell at
# decision >= 210 s by construction (2026-09-23: median 193-213 s while
# the edge lives exactly in the windows that have not crossed); the
# first-crossing series stays informational.  Grammar C gains 195/225 s.
# v4 (2026-10-01): grammar band_event_v4 (event-time entry on the
# settlement-basis z of a frozen spec); book age <= 1 s on every ladder
# model; oracle noise and ceiling on the settlement basis; registration by
# explicit cell list, VPS rows only; wr_too_good of an event cell audits
# against the frozen ceiling.
EVALUATOR_VERSION = "executable_truth_v4"
GRAMMAR_VERSION = band_lane.BAND_GRID_V2_VERSION
EVENT_GRAMMAR_VERSION = "band_event_v4"
# An event cell = (range [t0, t1], z*, cap rule); k98 is the frozen spec's
# schedule cap.  The live anchors tile 150-270 s.
EVENT_GRID: Dict[str, Tuple[Any, ...]] = {
    "t0": (150, 180, 210, 240),
    "t1": (240, 270),
    "threshold": (2.0, 2.5, 3.0, 3.5, 4.0),
    "cap_rule": ("c0.98", "c0.99", "k98"),
}
EVENT_CAPS = {"c0.98": 0.98, "c0.99": 0.99}
EVENT_SCHEDULE_CAP = "k98"
EVENT_CELL_ID_RE = re.compile(r"^e(\d+)-(\d+)_z(\d+\.\d+)_(c0\.\d+|k98)$")
# Paired static controls (outside N): the same event-time replay triggered
# by a USD settlement floor (m) or by z at a constant sigma (s).
CONTROL_ID_RE = re.compile(r"^x(\d+)-(\d+)_(m|s)(\d+(?:\.\d+)?)_(c0\.\d+)$")
CONTROL_TRIGGERS = {"m": "usd", "s": "const_sigma"}
# The headline is the +1 s model; lag 0 (fill at the decision sample), lag 2
# and a 1 s older price (z from the closes one second earlier) are reported.
EVENT_LAG = 1
EVENT_VARIANTS: Dict[str, Tuple[int, int]] = {"lag0": (0, 0), "lag2": (2, 0), "older1": (1, 1)}
EVENT_MATCH_BUCKET_S = 30
# Family v4 (docs/adaptive_family_research_2026-10-01.md section 4).
FAMILY_V4: Dict[str, str] = {
    "V1": "e150-270_z2.5_k98",
    "V2": "e150-270_z3.0_c0.98",
    "V3": "e210-270_z2.5_c0.99",
    "V4": "e150-270_z3.0_c0.99",
    "V5": "e150-270_z2.5_c0.99",
    "V6": "d150_f100_c0.99_p0",
}
# Section 4: V1 and V5 against the $100 settlement floor and the
# constant-sigma z >= 3 floor over 150-270 s, V3 against the $100 floor
# over 210-270 s; per-window net difference through update_signed, bar
# e >= K / alpha over the K distinct controls on >= 100 paired windows.
PAIRED_CONTROLS_V4: Dict[str, Tuple[str, ...]] = {
    FAMILY_V4["V1"]: ("x150-270_m100_c0.99", "x150-270_s3.0_c0.99"),
    FAMILY_V4["V5"]: ("x150-270_m100_c0.99", "x150-270_s3.0_c0.99"),
    FAMILY_V4["V3"]: ("x210-270_m100_c0.99",),
}
PAIRED_MIN_WINDOWS = 100
# Section 6 stopping rules and falsifiers (recorded in the campaign file).
FAMILY_STOP_NET_DAY = 45
FAMILY_STOP_NET_CELLS = (FAMILY_V4["V1"], FAMILY_V4["V5"])
FAMILY_STOP_DISCOVERY_DAY = 90
EVENT_CAPACITY_KILL_CELLS = (FAMILY_V4["V1"], FAMILY_V4["V3"], FAMILY_V4["V5"])
EVENT_CAPACITY_DAYS = 7
EVENT_CAPACITY_MIN_ENTRIES = 10
EVENT_CAPACITY_MIN_SHARE = 0.10
# The 0.10 floor was read off the cap-0.99 cells; a cell with a tighter cap
# fills a smaller share by construction (V1 and V3 ran at 0.086 and 0.093
# over 09-27..09-30).  Registration freezes each cell's own floor: this
# ratio times its fill share over the last EVENT_CAPACITY_DAYS complete days
# before registered_at, never above EVENT_CAPACITY_MIN_SHARE.
EVENT_CAPACITY_SHARE_RATIO = 0.5
FALSIFIER_MATCHED_DAY = 30
FALSIFIER_MATCHED_CELL = FAMILY_V4["V5"]
FALSIFIER_CROSSING = (2.5, 150, 270)
FALSIFIER_CROSSING_MIN_N = 2000
# F2's bar is the Wilson lower bound of the frozen rule's first-crossing
# accuracy on the windows before the first evidence ladder (section 6:
# "the pre-ladder Wilson lower", 0.9897), when that reference holds this
# many crossings; a table without such a period falls back to the frozen
# spec's own crossing over every fitted window.
FALSIFIER_CROSSING_REFERENCE_MIN_N = 2000
FALSIFIER_PAIRED_DAY = 45
FALSIFIER_PAIRED_CELL = FAMILY_V4["V1"]
FALSIFIER_PAIRED_LEADER = FAMILY_V4["V6"]
# The engine's `fresh` flag is its 30 s replay freshness; a ladder sample
# is executable only when the sampled book is at most this old.
MAX_BOOK_AGE_S = 1.0
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
REGISTRABLE_MIN_FLOOR = band_lane.BAND_V2_REGISTRABLE_MIN_FLOOR
REGISTRABLE_MIN_DECISION = band_lane.BAND_V2_REGISTRABLE_MIN_DECISION
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
# (the engine latches band_decision_missed there), a discovery host's, a
# window whose only clearing sample sits on a stalled book, or (event
# model) one whose records quote only the side opposite the signal (the
# settlement side's sq/sc/sage is read where the engine recorded it).
SELECTION_EXCLUSIONS = ("no_print", "out_of_band", "never_cleared", "ladder_direction_mismatch", "fok_killed")
AVAILABILITY_EXCLUSIONS = ("no_print_row", "uncovered", "no_ladder", "ladder_uncovered", "ladder_discovery_host", "book_stalled", "side_unquoted")
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
# The event capacity kill is permanent, so it gives no verdict before the
# cell could be ready: this many UTC days of data (its 7 complete days
# alone would let it fire on day 8, six days ahead of readiness).
EVENT_CAPACITY_MIN_DATA_DAYS = PROMOTION_MIN_DAYS
# wr_too_good of an event cell: the level of the binomial test of its wins
# against the frozen ceiling.
EVENT_WR_TOO_GOOD_ALPHA = 0.05
COVERAGE_MIN = 0.9
REGISTER_MIN_LADDER_DAYS = 7
EDGE_MIGRATION_ASK = 0.99
EDGE_MIGRATION_DAYS = 7
# Capacity trend (the kill): the share of a cell's covered signal windows
# whose $25 ladder is fillable at the cap within the patience, per UTC day;
# the trailing week against the first week of data (the registration week
# under accrual), once 14 days of data exist; a warning, not a hold, when
# the weekly capacity has fallen three complete weeks running.  The kill
# is permanent, so it needs support: no verdict unless each pooled week
# holds CAPACITY_POOL_MIN_DAYS data days and CAPACITY_POOL_MIN_N covered
# windows and the baseline CAPACITY_BASELINE_MIN_FILLS fillable ones (at
# today's support many registrable cells see 4-8 covered windows a day
# and one or two fills a week: a ratio of two such counts is noise, and
# a single sparse day on either side of an outage is not a week), and
# the kill fires only when the trailing week's Wilson upper bound is
# below CAPACITY_KILL_RATIO x the baseline's Wilson lower bound, never on
# the point ratio (which is reported).  A weekly decline counts only past
# CAPACITY_DECLINE_MIN_DROP (one window's worth of noise is not a
# decline) and an empty week breaks the run.
CAPACITY_BASELINE_DAYS = 7
CAPACITY_TRAILING_DAYS = 7
CAPACITY_MIN_DAYS = 14
CAPACITY_POOL_MIN_DAYS = 4
CAPACITY_POOL_MIN_N = 20
CAPACITY_BASELINE_MIN_FILLS = 10
CAPACITY_KILL_RATIO = 0.5
CAPACITY_WARN_WEEKS = 3
CAPACITY_DECLINE_MIN_DROP = 0.05
GIT_DIRTY_SUFFIX = "-dirty"
# A settled window's row is built once its ladder record is in a session
# file or this long after the window end, whichever comes first, so a row
# is written exactly once with everything it will ever hold.
LADDER_GRACE_S = band_lane.UNRESOLVED_FINAL_AFTER_S
E_BH_ALPHA = evidence_accrual.E_BH_ALPHA
MODELS = ("signal", "print", "ladder")
EVENT_MODEL = "event"
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
KILL_TRIPWIRES = ("capacity_collapse",)
# Reported next to the tripwires, never a hold or a kill.
WARNINGS = ("capacity_declining",)
CELL_ID_RE = re.compile(r"^d(\d+)_f(\d+)_c(\d\.\d+)_p(\d+)$")


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _mean(values: Sequence[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def git_sha() -> str:
    """The short HEAD sha, suffixed -dirty when a tracked file differs from
    it (git's own `describe --dirty` rule: untracked files do not count),
    so a row built from an uncommitted tree names a tree its sha cannot
    reproduce instead of the commit it happens to sit on (the 195/225 s
    rows of 2026-09-23T12:06-12:38Z carry a bare b2bae3f whose grammar has
    no such anchors: they were built by the tree that became the next
    commit)."""
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=str(ROOT), capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"], cwd=str(ROOT), capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return sha + GIT_DIRTY_SUFFIX if dirty else sha


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


def is_event_rule(rule: Mapping[str, Any]) -> bool:
    return "trigger" in rule


def normalized_event_rule(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """A band_event_v4 cell: trigger z, every field on EVENT_GRID, t0 < t1."""
    fields = set(EVENT_GRID) | {"trigger"}
    if not isinstance(raw, Mapping) or set(raw) != fields or raw["trigger"] != "z":
        raise ValueError("invalid event rule fields")
    rule: Dict[str, Any] = {"trigger": "z"}
    for field, cast in (("t0", int), ("t1", int), ("threshold", float), ("cap_rule", str)):
        value = raw[field]
        if isinstance(value, bool) or cast(value) != value or cast(value) not in EVENT_GRID[field]:
            raise ValueError("%s is outside the event grid" % field)
        rule[field] = cast(value)
    if rule["t0"] >= rule["t1"]:
        raise ValueError("event range is empty")
    return rule


def event_rule_from_cell_id(text: str) -> Dict[str, Any]:
    match = EVENT_CELL_ID_RE.match(text.strip())
    if not match:
        raise ValueError("cell id %r is not e<t0>-<t1>_z<z*>_<c0.98|c0.99|k98>" % text)
    return normalized_event_rule(
        {"trigger": "z", "t0": int(match.group(1)), "t1": int(match.group(2)), "threshold": float(match.group(3)), "cap_rule": match.group(4)}
    )


def control_rule_from_id(text: str) -> Dict[str, Any]:
    """A paired static control x<t0>-<t1>_<m<usd>|s<z>>_<cap>: the event-time
    replay behind a USD settlement floor or a constant-sigma z floor.
    Never a family member."""
    match = CONTROL_ID_RE.match(text.strip())
    if not match or match.group(5) not in EVENT_CAPS or int(match.group(1)) >= int(match.group(2)):
        raise ValueError("control id %r is not x<t0>-<t1>_<m<usd>|s<z>>_<c0.98|c0.99>" % text)
    return {
        "trigger": CONTROL_TRIGGERS[match.group(3)],
        "t0": int(match.group(1)),
        "t1": int(match.group(2)),
        "threshold": float(match.group(4)),
        "cap_rule": match.group(5),
    }


def event_cell_id(rule: Mapping[str, Any]) -> str:
    span = "%d-%d" % (int(rule["t0"]), int(rule["t1"]))
    if rule["trigger"] == "z":
        return "e%s_z%.1f_%s" % (span, float(rule["threshold"]), rule["cap_rule"])
    if rule["trigger"] == "usd":
        return "x%s_m%d_%s" % (span, int(rule["threshold"]), rule["cap_rule"])
    return "x%s_s%.1f_%s" % (span, float(rule["threshold"]), rule["cap_rule"])


def event_fingerprint(rule: Mapping[str, Any], spec_sha256: str) -> str:
    """An event cell is a hypothesis under one frozen spec: the same rule
    under a refitted spec is another fingerprint."""
    payload = {
        "lane": LANE,
        "grammar": EVENT_GRAMMAR_VERSION,
        "rule": dict(rule),
        "evaluator_version": EVALUATOR_VERSION,
        "spec_sha256": str(spec_sha256),
    }
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def event_grid_rules() -> List[Dict[str, Any]]:
    rules = []
    for t0 in EVENT_GRID["t0"]:
        for t1 in EVENT_GRID["t1"]:
            if t0 >= t1:
                continue
            for threshold in EVENT_GRID["threshold"]:
                for cap_rule in EVENT_GRID["cap_rule"]:
                    rules.append({"trigger": "z", "t0": t0, "t1": t1, "threshold": threshold, "cap_rule": cap_rule})
    return rules


def any_cell_id(rule: Mapping[str, Any]) -> str:
    return event_cell_id(rule) if is_event_rule(rule) else cell_id(rule)


def resolve_cell_ids(cells: Sequence[str]) -> List[Tuple[str, Dict[str, Any]]]:
    """(cell id, rule) for an explicit list of ids of either grammar; V1..V6
    name the cells of family v4 and Va..Vb their range.  Raises ValueError
    naming every id that is neither, or one listed twice."""
    names: List[str] = []
    for raw in cells:
        text = str(raw).strip()
        span = re.match(r"^V(\d+)\.\.V(\d+)$", text)
        names.extend(["V%d" % index for index in range(int(span.group(1)), int(span.group(2)) + 1)] if span else [text])
    resolved: List[Tuple[str, Dict[str, Any]]] = []
    unknown: List[str] = []
    for name in names:
        text = FAMILY_V4.get(name, name)
        try:
            rule = event_rule_from_cell_id(text) if text.startswith("e") else rule_from_cell_id(text)
        except ValueError:
            unknown.append(name)
            continue
        resolved.append((any_cell_id(rule), rule))
    if unknown:
        raise ValueError("unknown cell id(s): %s" % ", ".join(unknown))
    ids = [identity for identity, _ in resolved]
    repeated = sorted({identity for identity in ids if ids.count(identity) > 1})
    if repeated or not resolved:
        raise ValueError("cell id(s) listed twice: %s" % ", ".join(repeated) if repeated else "no cell ids")
    return resolved


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
ADDED_COLUMNS = {
    "windows": (
        ("ladder_attached_at", "TEXT"),
        # v4, the settlement basis (settlement_model on the Binance proxy):
        # the 60 s strike, the settlement margin as it stands at the
        # decision second and at the window end.
        ("strike_60s", "REAL"),
        ("settle_margin", "REAL"),
        ("final_settle_margin", "REAL"),
    )
}


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


def final_basis(row: Mapping[str, Any]) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    """(final margin, its bucket, the side it settles) of a row or trade on
    the settlement basis: closing 60 s TWAP minus the 60 s strike, a tie
    settling Up.  Without the settlement columns (no closes for the strike)
    it falls back to the point basis close(ws + 300) - open, where a zero
    margin has no side."""
    settle = row.get("final_settle_margin")
    if settle is not None:
        return float(settle), final_bucket(settle), "up" if float(settle) >= 0 else "down"
    point = row.get("final_margin")
    if point in (None, 0.0):
        return None, None, None
    return float(point), row.get("final_bucket") or final_bucket(point), "up" if float(point) > 0 else "down"


def settlement_columns(closes: Mapping[int, float], window_start: int, decision_seconds: Sequence[int]) -> Dict[int, Dict[str, Optional[float]]]:
    """The settlement-basis columns of one window per decision second, from
    the closes of [ws - 60, ws + 300) under the venue's rule (the shape of
    settlement_model.DEFAULT_PARAMS: no fitted constant enters)."""
    params = settlement_model.DEFAULT_PARAMS
    series = settlement_model.CloseSeries(closes, int(window_start) - int(params["twap_s"]), int(window_start) + int(params["window_s"]), params["max_fill_s"])
    strike = settlement_model.strike(series, int(window_start), params)
    final = settlement_model.final_margin(series, int(window_start), params)
    return {
        int(decision_s): {
            "strike_60s": strike,
            "settle_margin": settlement_model.settlement_margin(series, int(window_start), int(decision_s), params),
            "final_settle_margin": final,
        }
        for decision_s in decision_seconds
    }


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
    keep = ("anchor_s", "basis", "btc", "open", "margin", "direction", "strike_60s", "px", "px_age", "m", "up", "down", "pair_sum", "stake_usd", "quote_budget_usd", "elapsed_s", "host")
    return _canonical({key: anchor.get(key) for key in keep if key in anchor})


def _ladder_columns(ladder: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if ladder is None:
        return {"ladder_json": None, "ladder_host": None, "ladder_basis": None}
    keep = ("anchor_s", "basis", "btc", "open", "margin", "direction", "strike_60s", "budgets_usd", "samples", "cycles", "max_gap_s", "host")
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
    so does a row that took a discovery host's ladder (built in a VPS pull
    gap while the Mac observer ran) once the evidence host's record is in
    the session dirs: the instrument is replaced by the evidence record,
    never the other way.  Labels, prints and built_at are never rewritten.
    The settlement-basis columns are derived from the closes alone: a row
    still without one of them (built before v4, or before its closes were
    on disk) takes whichever has become computable; a value once written
    stays."""
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
    attached = upgraded = 0
    for window_start, decision_s, held_host in connection.execute(
        "SELECT window_start, decision_s, ladder_host FROM windows WHERE ladder_json IS NULL OR ladder_host != ?", (EVIDENCE_HOST,)
    ).fetchall():
        ladder = ladders.get((int(window_start), int(decision_s)))
        # A row that holds a ladder is rewritten by the evidence host's record only.
        if ladder is None or (held_host is not None and ladder.get("host") != EVIDENCE_HOST):
            continue
        anchor = anchors.get((int(window_start), int(decision_s)))
        columns = _ladder_columns(ladder)
        connection.execute(
            "UPDATE windows SET ladder_json = ?, ladder_host = ?, ladder_basis = ?, anchor_json = ?, host = ?, ladder_attached_at = ? "
            "WHERE window_start = ? AND decision_s = ?",
            (columns["ladder_json"], columns["ladder_host"], columns["ladder_basis"], _anchor_columns(anchor), ladder.get("host") or "public", built_at, int(window_start), int(decision_s)),
        )
        attached += int(held_host is None)
        upgraded += int(held_host is not None)
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
    by_start: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        by_start.setdefault(row["window_start"], []).append(row)
    for window_start, members in by_start.items():
        columns = settlement_columns(cache.closes, window_start, [row["decision_s"] for row in members])
        for row in members:
            row.update(columns[row["decision_s"]])
    names = ("strike_60s", "settle_margin", "final_settle_margin")
    backfill: Dict[int, Dict[int, Tuple[Optional[float], ...]]] = {}
    for window_start, decision_s, *held in connection.execute(
        "SELECT window_start, decision_s, %s FROM windows WHERE %s" % (", ".join(names), " OR ".join("%s IS NULL" % name for name in names))
    ).fetchall():
        backfill.setdefault(int(window_start), {})[int(decision_s)] = tuple(held)
    backfilled = 0
    for window_start, members in backfill.items():
        for decision_s, columns in settlement_columns(cache.closes, window_start, list(members)).items():
            held = members[decision_s]
            # Whichever column has become computable; one already written stays.
            filled = tuple(columns[name] if value is None else value for name, value in zip(names, held))
            if filled == held:
                continue
            connection.execute(
                "UPDATE windows SET strike_60s = ?, settle_margin = ?, final_settle_margin = ? WHERE window_start = ? AND decision_s = ?",
                (*filled, window_start, decision_s),
            )
            backfilled += 1
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
        # Rows stamped with a -dirty sha were built by an uncommitted tree:
        # the commit they name cannot reproduce them.
        "git_dirty": sha.endswith(GIT_DIRTY_SUFFIX),
        "rows_inserted": len(rows),
        "ladder_rows_attached": attached,
        "ladder_rows_upgraded": upgraded,
        "print_rows_refreshed": refreshed,
        "settlement_rows_backfilled": backfilled,
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
    disagrees with the side the final margin settles (final_basis: the
    settlement basis, the point basis for a row without it), with the
    Wilson lower bound of that share.  Over every row of the table it is
    the marginal diagnostic; over one cell's own signal windows (labels
    supplied, rows without `official`) it is the noise the cell's ceiling
    is built on."""
    seen = set()
    counts: Dict[str, Counter] = {}
    for row in rows:
        window_start = int(row["window_start"])
        _, bucket, expected = final_basis(row)
        if window_start in seen or bucket is None:
            continue
        official = row.get("official") if labels is None else labels.get(window_start)
        if official not in ("up", "down"):
            continue
        seen.add(window_start)
        counter = counts.setdefault(bucket, Counter())
        counter["n"] += 1
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


def book_state(sample: Mapping[str, Any]) -> str:
    """ok: the engine's `fresh` flag and a sampled book at most
    MAX_BOOK_AGE_S old; stalled: flagged fresh but older (the flag is the
    30 s replay freshness: it cleared 19-29 s old books on 2026-09-19);
    stale: not fresh."""
    if not sample.get("fresh"):
        return "stale"
    age = sample.get("age")
    return "ok" if age is not None and float(age) <= MAX_BOOK_AGE_S else "stalled"


def sample_clears(
    sample: Mapping[str, Any], direction: str, budget_index: int, floor: float, cap: float, admit_stalled: bool = False
) -> Optional[Dict[str, Any]]:
    """The engine's gates on one ladder sample: a live book (book_state ok;
    `admit_stalled` reads a stalled one too, to tell an instrument failure
    from a rule that did not clear), a quote for the budget,
    BandPolicyParams::quote_clears_band (vwap > floor and worst <= cap) and
    pair coherence (vwap + complement best ask in [0.90, 1.10]).  Returns
    the executable quote or None."""
    state = book_state(sample)
    if state != "ok" and not (admit_stalled and state == "stalled"):
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
    admit_stalled: bool = False,
) -> Optional[Dict[str, Any]]:
    """The ladder model on one band_ladder record: the first sample with
    t <= patience whose quote at `budget` clears the band on a live,
    coherent book is the entry, at its worst (FOK limit) price.  `shift_s`
    is the latency sensitivity: the FOK limit at the decision sample's
    worst price lands shift_s samples later and fills only if that later
    book is live, coherent and still walks the budget at or below the
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
        quote = sample_clears(samples[t], direction, index, floor, cap, admit_stalled)
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
    capacity_points: List[Tuple[int, bool]] = []
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
            "final_settle_margin": row.get("final_settle_margin"),
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
        if ladder_trade(rule, ladder, direction, LADDER_BUDGET_USD) is None and ladder_trade(rule, ladder, direction, LADDER_BUDGET_USD, admit_stalled=True) is not None:
            # The only sample that clears sits on a stalled book: the
            # instrument cannot say what was executable.
            excluded.setdefault("book_stalled", []).append(row)
            continue
        covered += 1
        for budget in CAPACITY_BUDGETS:
            cleared = ladder_trade(rule, ladder, direction, budget)
            fillable = cleared is not None and not cleared["depth_limited"]
            capacity["%d" % budget][1] += 1
            capacity["%d" % budget][0] += int(fillable)
            if budget == LADDER_BUDGET_USD:
                capacity_points.append((base["window_start"], fillable))
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
        "capacity_points": capacity_points if model == "ladder" else None,
        "basis_disagreement": basis if model == "ladder" else None,
    }


# --- the event model (band_event_v4; selection: label-blind) -------------------


def tile_ladders(ladders: Mapping[int, Mapping[str, Any]], budget: float = LADDER_BUDGET_USD) -> Dict[int, Dict[str, Any]]:
    """One book sample per elapsed second of a window from its band_ladder
    records keyed by anchor (second = anchor + t).  Where two anchors cover
    a second the later anchor's sample stands (its t = 0 over the earlier
    one's t = 30) unless it carries no book at all.  Each entry holds the
    record's own book state (`book`, `age`), the sides the sample quotes
    (`sides`: a directional record quotes the side the engine latched, and
    the other side too on a sample that carries its book as sq/sc/sage,
    which the engine writes on every sample since 2026-10-01; a record
    without those keys reads as before), each quoted side's book state
    (`books`: the other side's from its own age `sage`, on the engine's
    sub-second clock) and, per side, two quotes at `budget`: `fill`, the
    executable quote as it stands, (worst, vwap, pair sum or None), which
    is what an order already in flight meets; and under the side's own
    key that quote when it also passes the entry gates (vwap above the
    ask floor, a coherent pair; the cap and the book's age are the
    caller's), else None."""
    tiled: Dict[int, Dict[str, Any]] = {}
    for anchor_s in sorted(ladders, reverse=True):
        record = ladders[anchor_s]
        index = _budget_index(record, budget)
        latched = record.get("direction")
        for sample in record.get("samples") or []:
            try:
                second = int(anchor_s) + int(sample["t"])
            except (KeyError, TypeError, ValueError):
                continue
            current = tiled.get(second)
            if current is not None and (current["book"] != "stale" or current["age"] is not None):
                continue
            state = book_state(sample)
            sides = (latched,) if latched in ("up", "down") else ("up", "down")
            sources = {side: (*_side_quotes(sample, side), state) for side in sides}
            if len(sides) == 1 and "sq" in sample:
                settled = "down" if latched == "up" else "up"
                quotes, complement = sample.get("sq"), sample.get("sc")
                sources[settled] = (
                    quotes if isinstance(quotes, list) else None,
                    None if complement is None else float(complement),
                    book_state({"fresh": sample.get("fresh"), "age": sample.get("sage")}),
                )
                sides = (latched, settled)
            entry: Dict[str, Any] = {"book": state, "age": sample.get("age"), "sides": sides, "books": {}, "fill": {}, "up": None, "down": None}
            for side in sides:
                quotes, complement, entry["books"][side] = sources[side]
                if index is None or quotes is None or index >= len(quotes) or not isinstance(quotes[index], list):
                    continue
                worst, vwap = quotes[index][0], quotes[index][1]
                if worst is None or vwap is None or float(worst) <= 0.0 or float(vwap) <= 0.0:
                    continue
                pair_sum = None if complement is None else float(vwap) + complement
                entry["fill"][side] = (float(worst), float(vwap), pair_sum)
                if float(vwap) > ASK_FLOOR and pair_sum is not None and PAIR_SUM_RANGE[0] <= pair_sum <= PAIR_SUM_RANGE[1]:
                    entry[side] = entry["fill"][side]
            tiled[second] = entry
    return tiled


def event_cap(rule: Mapping[str, Any], z: Optional[float], spec: Mapping[str, Any]) -> Optional[float]:
    """The FOK limit of an event rule at one second: its flat cap, or for
    k98 the frozen schedule cap of the z bucket (None: no tick clears)."""
    if rule["cap_rule"] == EVENT_SCHEDULE_CAP:
        return None if z is None else settlement_model.schedule_cap(z, spec)
    return EVENT_CAPS[rule["cap_rule"]]


def event_series(closes: Mapping[int, float], window_starts: Sequence[int], spec: Mapping[str, Any]) -> settlement_model.CloseSeries:
    """The close series the spec needs for these windows: back to the EWMA
    warm-up of the earliest, forward to the end of the latest."""
    params = spec["params"]
    lead = int(params["warmup_s"]) + int(params["subsample_s"]) + int(params["twap_s"])
    return settlement_model.CloseSeries(closes, min(window_starts) - lead, max(window_starts) + int(params["window_s"]), params["max_fill_s"])


def event_windows(
    rows: Sequence[Mapping[str, Any]],
    series: settlement_model.CloseSeries,
    spec: Mapping[str, Any],
    ladder_hosts: Sequence[str],
    after_window_start: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """One record per window of the table (after the cut when given): the
    model's path over the event range from the closes alone, and the
    evidence hosts' ladders tiled per second.  A window whose closes cannot
    carry the model (no warm-up, a gap), or are not on disk through its
    end yet, has an empty path and never signals: it is replayed whole or
    not at all, so a trade folded once is the trade of the complete window.
    `complete` says the table holds the window's row at every decision
    second, i.e. each of its ladders is attached or past the grace (the
    condition build() inserts a row on): until then a later ladder can
    still change what a rule trades in it.  `official` rides along for
    scoring; selection never reads it."""
    low = min(EVENT_GRID["t0"]) - max(info for _, info in EVENT_VARIANTS.values())
    high = max(EVENT_GRID["t1"])
    by_window: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        window_start = int(row["window_start"])
        if after_window_start is not None and window_start <= int(after_window_start):
            continue
        window = by_window.get(window_start)
        if window is None:
            window = by_window[window_start] = {
                "window_start": window_start,
                "official": row.get("official"),
                "final_margin": row.get("final_margin"),
                "final_bucket": row.get("final_bucket"),
                "final_settle_margin": row.get("final_settle_margin"),
                "ladders": {},
                "discovery": False,
                "built": set(),
            }
        window["built"].add(int(row["decision_s"]))
        ladder = row.get("ladder")
        if ladder is None:
            continue
        if (row.get("ladder_host") or ladder.get("host")) in ladder_hosts:
            window["ladders"][int(row["decision_s"])] = ladder
        else:
            window["discovery"] = True
    windows = [by_window[window_start] for window_start in sorted(by_window)]
    params = spec["params"]
    for window in windows:
        complete = series.covers(window["window_start"] - int(params["twap_s"]), window["window_start"] + int(params["window_s"]))
        window["path"] = settlement_model.path(series, window["window_start"], low, high, spec) if complete else {}
        window["series"] = series
        window["tiled"] = tile_ladders(window["ladders"])
        window["complete"] = window.pop("built").issuperset(DECISION_SECONDS)
    return windows


def _trigger_path(window: Dict[str, Any], rule: Mapping[str, Any], spec: Mapping[str, Any]) -> Dict[int, Tuple[float, float]]:
    """{second: (m_t, statistic)} of a rule's trigger: z, |m_t| in USD, or
    z at the spec's constant (median) sigma."""
    if rule["trigger"] == "z":
        return window["path"]
    if rule["trigger"] == "usd":
        return {second: (margin, abs(margin)) for second, (margin, _) in window["path"].items()}
    if "path_const" not in window:
        window["path_const"] = (
            {}
            if spec.get("sigma2_median") is None
            else settlement_model.path(
                window["series"], window["window_start"], min(window["path"], default=0), max(window["path"], default=-1), spec, spec["sigma2_median"]
            )
        )
    return window["path_const"]


def event_trade(
    rule: Mapping[str, Any], window: Dict[str, Any], spec: Mapping[str, Any], lag: int = EVENT_LAG, info_lag: int = 0
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[Tuple[int, str]]]:
    """The event model on one window: scan the seconds of [t0, t1]; at the
    first second whose trigger statistic (from closes of the seconds
    before it, `info_lag` seconds older when given) reaches the threshold
    and whose live book quotes the settlement side at or below the cap
    (through the entry gates: vwap above the ask floor, a coherent pair),
    send a FOK limit at the cap; it fills `lag` samples later iff that
    side's book is live and walks the budget at or below the limit, at
    that book's worst price (the entry) whatever its ask floor or pair
    sum: the order is in flight and no gate protects it, so a collapsed
    ask fills (`fill_outside_gates` marks a fill the entry gates would
    have refused); a kill continues the scan.  One entry per window.
    Returns (trade, reason it never traded, first signal (second, side)):
    the reason is None without a signal; a window with no ladder sample at
    any signal second, whose samples quote only the other side (the engine
    latches the point-basis direction, and records the settlement side's
    book only since 2026-10-01), or whose only clearing sample sits on a
    stalled book is availability, not the rule's miss."""
    key = ("signal_seconds", rule["trigger"], float(rule["threshold"]), int(rule["t0"]), int(rule["t1"]), int(info_lag))
    if key not in window:
        statistics_path = _trigger_path(window, rule, spec)
        window[key] = [
            (second, statistics_path[second - int(info_lag)][0])
            for second in range(int(rule["t0"]), int(rule["t1"]) + 1)
            if second - int(info_lag) in statistics_path and statistics_path[second - int(info_lag)][1] >= float(rule["threshold"])
        ]
    tiled = window["tiled"]
    signal: Optional[Tuple[int, str]] = None
    sampled = quoted = killed = stalled = False
    for second, margin in window[key]:
        side = "up" if margin >= 0 else "down"
        if signal is None:
            signal = (second, side)
        adaptive = window["path"].get(second - int(info_lag))
        z = None if adaptive is None else adaptive[1]
        cap = event_cap(rule, z, spec)
        book = tiled.get(second)
        if book is None:
            continue
        sampled = True
        if side not in book["sides"]:
            continue
        quoted = True
        quote = book[side]
        if quote is None or cap is None or quote[0] > cap + 1e-9:
            continue
        if book["books"][side] != "ok":
            stalled = stalled or book["books"][side] == "stalled"
            continue
        fill, outside = quote, False
        if lag:
            later = tiled.get(second + int(lag))
            fill = later["fill"].get(side) if later is not None and later["books"].get(side) == "ok" else None
            if fill is None or fill[0] > cap + 1e-9:
                killed = True
                continue
            outside = later[side] is None
        bucket = None if z is None else settlement_model.bucket(z, spec)
        return (
            {
                "window_start": window["window_start"],
                "direction": side,
                "entry": fill[0],
                "vwap": fill[1],
                "pair_sum": fill[2],
                "t": second,
                "filled_t": second + int(lag),
                "fill_outside_gates": outside,
                "limit": cap,
                "margin": margin,
                "z": z,
                "frozen_ceiling": None if bucket is None else bucket["wilson_upper"],
                "stalled_before": stalled,
                "final_margin": window["final_margin"],
                "final_bucket": window["final_bucket"],
                "final_settle_margin": window["final_settle_margin"],
            },
            None,
            signal,
        )
    if signal is None:
        return None, None, None
    if not tiled:
        return None, "ladder_discovery_host" if window["discovery"] else "no_ladder", signal
    if not sampled:
        return None, "ladder_uncovered", signal
    if stalled:
        return None, "book_stalled", signal
    if killed:
        return None, "fok_killed", signal
    return None, "never_cleared" if quoted else "side_unquoted", signal


def select_event_cell(
    windows: Sequence[Dict[str, Any]], rule: Mapping[str, Any], spec: Mapping[str, Any], capacity_rule: Optional[Mapping[str, Any]] = None
) -> Dict[str, Any]:
    """Label-blind selection of an event rule over event_windows records,
    in select_cell's shape: the +1 s trades, every signal window that never
    traded by reason (its direction is the side at the first signal
    second), the capacity points (covered signal window, filled) and the
    latency variants.  Windows the data cannot score (no evidence ladder, a
    stalled book) are availability, not the rule's selection."""
    trades: List[Dict[str, Any]] = []
    excluded: Dict[str, List[Dict[str, Any]]] = {}
    variants: Dict[str, List[Dict[str, Any]]] = {name: [] for name in EVENT_VARIANTS}
    capacity_points: List[Tuple[int, bool]] = []
    signals: List[Dict[str, Any]] = []
    for window in windows:
        trade, reason, signal = event_trade(rule, window, spec)
        if signal is not None:
            signals.append({"window_start": window["window_start"], "t": signal[0], "direction": signal[1]})
        if trade is not None:
            trades.append(trade)
            capacity_points.append((window["window_start"], True))
        elif reason is not None:
            excluded.setdefault(reason, []).append(
                {
                    "window_start": window["window_start"],
                    "direction": signal[1],
                    "t": signal[0],
                    "final_margin": window["final_margin"],
                    "final_bucket": window["final_bucket"],
                    "final_settle_margin": window["final_settle_margin"],
                }
            )
            if reason not in AVAILABILITY_EXCLUSIONS:
                capacity_points.append((window["window_start"], False))
        if window["tiled"]:
            for name, (lag, info_lag) in EVENT_VARIANTS.items():
                variant = event_trade(rule, window, spec, lag, info_lag)[0]
                if variant is not None:
                    variants[name].append(variant)
    signal_count = len(trades) + sum(len(items) for items in excluded.values())
    covered = len(capacity_points)
    return {
        "model": EVENT_MODEL,
        "rule": dict(rule),
        "cell_id": event_cell_id(rule),
        "trades": trades,
        "excluded": excluded,
        "signals": signals,
        "signal_windows": signal_count,
        "coverage": (covered / signal_count) if signal_count else None,
        "capacity": {"%d" % LADDER_BUDGET_USD: (len(trades) / covered) if covered else None},
        "capacity_points": capacity_points,
        "capacity_rule": None if capacity_rule is None else dict(capacity_rule),
        "basis_disagreement": None,
        "variants": variants,
    }


def event_capacity_verdict(points: Sequence[Tuple[int, bool]], rule: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """Section 6's capacity kill of an event cell, when the campaign set one
    (`rule`: days, min_entries_per_day, min_fill_share, min_data_days):
    over the trailing `days` complete UTC days of data (the first day after
    the cut and the newest day are partial and never count; a day without a
    covered signal window is an observer outage and carries nothing), kill
    when the median entries per day is under the floor or the pooled fill
    share is.  No verdict before that many complete days, nor before
    `min_data_days` UTC days of data (the kill is permanent: it waits until
    the cell could be ready)."""
    if rule is None:
        return None
    by_day: Dict[int, List[int]] = {}
    for window_start, filled in points:
        counts = by_day.setdefault(int(window_start) // 86400, [0, 0])
        counts[0] += int(bool(filled))
        counts[1] += 1
    days = sorted(by_day)
    complete = days[1:-1][-int(rule["days"]) :]
    verdict: Dict[str, Any] = {
        "rule": dict(rule),
        "daily": [{"day": _day_iso(day), "entries": by_day[day][0], "signal_windows": by_day[day][1]} for day in days],
        "median_entries_per_day": None,
        "fill_share": None,
        "verdict": None,
        "kill": False,
        "warn": False,
    }
    if len(complete) < int(rule["days"]) or len(days) < int(rule.get("min_data_days") or 0):
        return verdict
    entries = sum(by_day[day][0] for day in complete)
    median = statistics.median(by_day[day][0] for day in complete)
    share = entries / sum(by_day[day][1] for day in complete)
    kill = bool(median < float(rule["min_entries_per_day"]) or share < float(rule["min_fill_share"]))
    verdict.update({"median_entries_per_day": median, "fill_share": share, "kill": kill, "verdict": "kill" if kill else "ok"})
    return verdict


def event_capacity_rule_for(rule: Optional[Mapping[str, Any]], cell_id: str) -> Optional[Dict[str, Any]]:
    """The campaign's capacity rule as it applies to one cell: its own
    fill-share floor when registration froze one, else the family floor;
    None for a cell the rule does not name."""
    if not rule or cell_id not in rule["cells"]:
        return None
    floor = (rule.get("min_fill_share_by_cell") or {}).get(cell_id, rule["min_fill_share"])
    return {**rule, "min_fill_share": float(floor)}


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


def binomial_upper_tail(wins: int, total: int, p: float) -> float:
    """P(X >= wins) for X ~ Binomial(total, p), summed over the losses."""
    if p >= 1.0 or wins <= 0:
        return 1.0
    if p <= 0.0:
        return 0.0
    log_p, log_q = math.log(p), math.log1p(-p)
    tail = sum(
        math.exp(math.lgamma(total + 1) - math.lgamma(k + 1) - math.lgamma(total - k + 1) + k * log_p + (total - k) * log_q)
        for k in range(int(wins), int(total) + 1)
    )
    return min(1.0, tail)


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
    tick fragility, latency sensitivity, capacity and its trend (the
    kill), the first-crossing summary (informational) and the tripwires.
    `noise` is the table-wide marginal oracle noise, reported as a
    diagnostic; the ceiling is built on the noise among the cell's own
    signal windows.  `cleared` names the defect tripwires an operator has
    audited (--clear-audit): they are still reported, but no longer hold.
    An event selection (select_event_cell) is scored the same way; its
    latency rows are its variants, its capacity kill the campaign's rule,
    and its wr_too_good the test against the frozen ceiling
    (`frozen_ceiling_p`: P(X >= wins) at that rate)."""
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
    buckets = [final_basis(trade)[1] for trade in trades]
    oracle_ceiling = _mean([1.0 - float(conditional[bucket]["noise_lower"]) for bucket in buckets if bucket in conditional])
    oracle_ceiling_marginal = _mean([1.0 - float(noise[bucket]["noise"]) for bucket in buckets if bucket in noise])
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
    elif model == EVENT_MODEL:
        latency = {}
        for name, variant in selection["variants"].items():
            kept = [(trade["entry"], trade["direction"] == labels[trade["window_start"]]) for trade in variant if labels.get(trade["window_start"]) in ("up", "down")]
            latency[name] = {
                **_win_stats([flag for _, flag in kept]),
                **_economics([price for price, _ in kept], [flag for _, flag in kept], fee_rate),
                "fill_rate": (len(kept) / len(trades)) if trades else None,
            }
    # wr_too_good: the fixed 0.995 for a static cell.  For an event cell a
    # test against the frozen ceiling (the mean over its entries of the
    # Wilson upper bound of each entry's frozen z bucket): the wins must be
    # improbable even at that rate.  The point comparison tripped on
    # sampling noise (2026-10-01: 412/414 against 0.99488, 2 losses where
    # the entries' frozen buckets expect 2.7).
    frozen = [trade["frozen_ceiling"] for trade in trades if trade.get("frozen_ceiling") is not None]
    frozen_ceiling = _mean(frozen) if model == EVENT_MODEL else None
    if frozen_ceiling is None:
        ceiling_p = None
        too_good = overall["n"] >= band_lane.TRIPWIRE_MINIMUM_N and overall["win_rate"] > band_lane.TRIPWIRE_WIN_RATE
    else:
        ceiling_p = binomial_upper_tail(overall["wins"], overall["n"], frozen_ceiling)
        too_good = overall["n"] >= band_lane.TRIPWIRE_MINIMUM_N and ceiling_p < EVENT_WR_TOO_GOOD_ALPHA
    first = trades[0]["window_start"] if trades else None
    last = trades[-1]["window_start"] if trades else None
    span_days = ((last - first) / 86400.0) if trades else 0.0
    # The first-crossing series of the cell's own signal windows (a series
    # pooled over weak windows is censored wherever they are the majority):
    # informational since v3.  The kill is the cell's capacity trend.
    migration = None
    if edge_series and model != EVENT_MODEL:
        windows = {int(row["window_start"]) for row in population}
        migration = edge_migration_verdict([point for point in edge_series if point[0] in windows], int(rule["decision_second"]))
    trend = capacity_trend_verdict(selection.get("capacity_points") or ()) if model == "ladder" else None
    if model == EVENT_MODEL:
        trend = event_capacity_verdict(selection.get("capacity_points") or (), selection.get("capacity_rule"))
    lower = overall["wilson_lower"]
    cleared = tuple(name for name in cleared if name in DEFECT_TRIPWIRES)
    tripwires = {
        "wr_too_good": bool(too_good),
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
        "capacity_collapse": bool(trend and trend["kill"]),
    }
    warnings = {"capacity_declining": bool(trend and trend["warn"])}
    clears = bool(priced and lower is not None and even is not None and lower >= even)
    defects = [name for name in DEFECT_TRIPWIRES if tripwires[name] and name not in cleared]
    ready = not any(tripwires[name] for name in READINESS_REQUIREMENTS)
    return {
        "model": model,
        "rule": dict(rule),
        "cell_id": any_cell_id(rule),
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
        "capacity_trend": trend,
        "basis_disagreement": selection.get("basis_disagreement"),
        "oracle_ceiling": oracle_ceiling,
        "oracle_ceiling_marginal": oracle_ceiling_marginal,
        "oracle_noise_conditional": conditional,
        "frozen_ceiling": frozen_ceiling,
        "frozen_ceiling_p": ceiling_p,
        # +1 s fills the entry gates would have refused (a collapsed ask, an incoherent pair): booked, and counted here.
        "fills_outside_gates": sum(1 for trade in trades if trade.get("fill_outside_gates")) if model == EVENT_MODEL else None,
        "tick_fragile": fragile,
        "latency": latency,
        "first_window_start": first,
        "last_window_start": last,
        "span_days": span_days,
        "edge_migration": migration,
        "clears_break_even": clears,
        "tripwires": tripwires,
        "warnings": warnings,
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
    a window that never did counts as later than every observed second.
    Informational since v3 (`median_before_decision`): a median earlier
    than the decision second says the market's most confident windows
    have crossed, not that the cell's own windows (those that have not)
    are gone; the kill is capacity_trend_verdict."""
    if not series:
        return {"n": 0, "median": None, "median_before_decision": False}
    newest = max(window_start for window_start, _ in series)
    recent = [value for window_start, value in series if window_start > newest - days * 86400]
    values = sorted(math.inf if value is None else float(value) for value in recent)
    median = statistics.median(values) if values else None
    return {
        "n": len(values),
        "censored": sum(1 for value in values if value == math.inf),
        "median": None if median is None or median == math.inf else median,
        "median_before_decision": bool(median is not None and median < float(decision_s)),
    }


def _day_iso(day: int) -> str:
    return dt.datetime.fromtimestamp(int(day) * 86400, dt.timezone.utc).strftime("%Y-%m-%d")


def capacity_trend_verdict(
    points: Sequence[Tuple[int, bool]],
    baseline_days: int = CAPACITY_BASELINE_DAYS,
    trailing_days: int = CAPACITY_TRAILING_DAYS,
    min_days: int = CAPACITY_MIN_DAYS,
    kill_ratio: float = CAPACITY_KILL_RATIO,
    warn_weeks: int = CAPACITY_WARN_WEEKS,
    pool_min_days: int = CAPACITY_POOL_MIN_DAYS,
    pool_min_n: int = CAPACITY_POOL_MIN_N,
    baseline_min_fills: int = CAPACITY_BASELINE_MIN_FILLS,
    min_drop: float = CAPACITY_DECLINE_MIN_DROP,
) -> Dict[str, Any]:
    """The cell's capacity per UTC day: of its covered signal windows
    (`points`: window start, fillable at $25 within the patience), the
    share fillable at the cap.  Kill when the trailing `trailing_days`
    capacity is below `kill_ratio` x the first `baseline_days` of data
    (the registration week under accrual, where the points start after
    the cut) with at least `min_days` UTC days of data: on the Wilson
    bounds (`ratio_upper` = trailing upper / baseline lower), never on
    the point `ratio`, which is reported; warn (reported, never a hold)
    when the weekly capacity has fallen `warn_weeks` complete weeks
    running, each by at least `min_drop` of the week before.  Both
    windows pool windows, not daily rates; a day without a covered window
    carries no capacity and an empty complete week breaks the decline run
    (an observer outage is not the edge leaving).  Too short, or a pool
    below `pool_min_days` data days, `pool_min_n` windows or (baseline)
    `baseline_min_fills` fills: no verdict (`support` says which)."""
    by_day: Dict[int, List[int]] = {}
    for window_start, fillable in points:
        counts = by_day.setdefault(int(window_start) // 86400, [0, 0])
        counts[0] += int(bool(fillable))
        counts[1] += 1
    days = sorted(by_day)
    daily = [{"day": _day_iso(day), "capacity": by_day[day][0] / by_day[day][1], "n": by_day[day][1]} for day in days]
    verdict: Dict[str, Any] = {
        "n_days": len(days),
        "span_days": (days[-1] - days[0] + 1) if days else 0,
        "daily": daily,
        "baseline": None,
        "trailing": None,
        "ratio": None,
        "ratio_upper": None,
        "support": None,
        "weekly": [],
        "consecutive_declines": 0,
        "verdict": None,
        "kill": False,
        "warn": False,
    }
    if not days:
        return verdict
    first, last = days[0], days[-1]

    def pooled(selected: Sequence[int]) -> Dict[str, Any]:
        cleared = sum(by_day[day][0] for day in selected)
        total = sum(by_day[day][1] for day in selected)
        return {
            "capacity": (cleared / total) if total else None,
            "n": total,
            "fills": cleared,
            "days": len(selected),
            "wilson_lower": band_lane.wilson_lower(cleared, total),
            "wilson_upper": _wilson_upper(cleared, total),
        }

    baseline = pooled([day for day in days if day < first + baseline_days])
    trailing = pooled([day for day in days if day > last - trailing_days])
    weekly = []
    week = 0
    while first + 7 * (week + 1) - 1 <= last:  # complete weeks only; an empty one stays (capacity None)
        members = [day for day in days if first + 7 * week <= day < first + 7 * (week + 1)]
        weekly.append({"week": week, "from": _day_iso(first + 7 * week), **pooled(members)})
        week += 1
    declines = 0
    for previous, current in zip(weekly, weekly[1:]):
        if previous["capacity"] is None or current["capacity"] is None:
            declines = 0
        elif current["capacity"] < previous["capacity"] * (1.0 - min_drop):
            declines += 1
        else:
            declines = 0
    verdict.update({"baseline": baseline, "trailing": trailing, "weekly": weekly, "consecutive_declines": declines})
    if len(days) < min_days:
        return verdict
    short = [
        "%s: %d days, %d windows, %d fills" % (name, pool["days"], pool["n"], pool["fills"])
        for name, pool, min_fills in (("baseline", baseline, baseline_min_fills), ("trailing", trailing, 0))
        if pool["days"] < pool_min_days or pool["n"] < pool_min_n or pool["fills"] < min_fills
    ]
    verdict["support"] = {"ok": not short, "reason": "; ".join(short) if short else None}
    if short:
        return verdict
    if baseline["capacity"]:
        verdict["ratio"] = trailing["capacity"] / baseline["capacity"]
    if baseline["wilson_lower"]:
        verdict["ratio_upper"] = trailing["wilson_upper"] / baseline["wilson_lower"]
    kill = bool(verdict["ratio_upper"] is not None and verdict["ratio_upper"] < kill_ratio)
    warn = declines >= warn_weeks
    verdict.update({"kill": kill, "warn": warn, "verdict": "kill" if kill else ("warn" if warn else "ok")})
    return verdict


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
    report: Dict[str, Any] = {
        "cell_id": cell_id(rule),
        "fingerprint": fingerprint(rule),
        "rule": rule,
        "registrable": registrable(rule),
        # No print column at this decision second (band_lane's proposer
        # skips it): the ladder is the only instrument.
        "ladder_only": int(rule["decision_second"]) not in band_lane.BAND_DECISION_SECONDS,
    }
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


def matched_contrast(
    windows: Sequence[Mapping[str, Any]],
    selection: Mapping[str, Any],
    labels: Mapping[int, Optional[str]],
    fee_rate: float,
    replicates: int = 0,
    seed: int = 0,
) -> Dict[str, Any]:
    """Does the rule pick better windows than any window offering the same
    ask at the same time?  Candidates: per window and stratum (30 s bucket
    of the decision second, entry tick) the first second at which the
    settlement side fills under the +1 s model at the rule's largest cap,
    whatever its z.  The matched rate is the expectation of drawing, per
    stratum, as many candidates as the rule traded there (exact, no
    draw); with `replicates` the seeded draws give the share whose net is
    at least the rule's (p_value)."""
    rule = selection["rule"]
    cap = EVENT_CAPS.get(rule["cap_rule"], max(EVENT_CAPS.values()))
    pools: Dict[Tuple[int, int], Dict[int, Tuple[bool, float]]] = {}
    for window in windows:
        official = labels.get(window["window_start"])
        if official not in ("up", "down"):
            continue
        tiled = window["tiled"]
        for second in range(int(rule["t0"]), int(rule["t1"]) + 1):
            point, book, later = window["path"].get(second), tiled.get(second), tiled.get(second + EVENT_LAG)
            if point is None or book is None or later is None:
                continue
            side = "up" if point[0] >= 0 else "down"
            if book["books"].get(side) != "ok" or later["books"].get(side) != "ok":
                continue
            quote, fill = book[side], later["fill"].get(side)
            if quote is None or fill is None or quote[0] > cap + 1e-9 or fill[0] > cap + 1e-9:
                continue
            stratum = (second // EVENT_MATCH_BUCKET_S, int(round(fill[0] * 100)))
            pools.setdefault(stratum, {}).setdefault(window["window_start"], (side == official, fill[0]))
    trades = [trade for trade in selection["trades"] if labels.get(trade["window_start"]) in ("up", "down")]
    result: Dict[str, Any] = {
        "n": len(trades),
        "strata": 0,
        "win_rate": None,
        "matched_win_rate": None,
        "win_rate_minus_matched": None,
        "net_per_usd": None,
        "matched_net_per_usd": None,
        "p_value": None,
        "replicates": int(replicates),
        "seed": int(seed),
    }
    if not trades:
        return result
    counts: Counter = Counter()
    wins = net = 0.0
    for trade in trades:
        won = trade["direction"] == labels[trade["window_start"]]
        stratum = (int(trade["t"]) // EVENT_MATCH_BUCKET_S, int(round(trade["entry"] * 100)))
        counts[stratum] += 1
        pools.setdefault(stratum, {}).setdefault(trade["window_start"], (won, trade["entry"]))
        wins += int(won)
        net += net_per_usd(trade["entry"], won, fee_rate)
    members = {stratum: [(float(won), net_per_usd(entry, won, fee_rate)) for won, entry in pools[stratum].values()] for stratum in counts}
    matched_wins = sum(count * _mean([won for won, _ in members[stratum]]) for stratum, count in counts.items())
    matched_net = sum(count * _mean([value for _, value in members[stratum]]) for stratum, count in counts.items())
    total = len(trades)
    result.update(
        {
            "strata": len(counts),
            "win_rate": wins / total,
            "matched_win_rate": matched_wins / total,
            "win_rate_minus_matched": (wins - matched_wins) / total,
            "net_per_usd": net / total,
            "matched_net_per_usd": matched_net / total,
        }
    )
    if int(replicates) > 0:
        rng = random.Random(int(seed))
        ordered = sorted(counts.items())
        at_least = 0
        for _ in range(int(replicates)):
            drawn = 0.0
            for stratum, count in ordered:
                pool = members[stratum]
                picks = rng.sample(pool, count) if len(pool) >= count else rng.choices(pool, k=count)
                drawn += sum(value for _, value in picks)
            at_least += int(drawn >= net - 1e-12)
        result["p_value"] = at_least / int(replicates)
    return result


def max_t_null(cells: Mapping[str, Sequence[Tuple[int, float, bool]]], fee_rate: float, replicates: int, seed: int) -> Dict[str, Any]:
    """Family-wise test against break-even: per cell T = (wins - sum of
    break-evens) / sqrt(sum of BE x (1 - BE)) over its (window, entry, won)
    trades; under the null one uniform per window decides every cell's
    trade in it (a win iff below break-even at its entry), so cells that
    share a window share its luck.  p_value: the share of seeded
    replicates whose largest T reaches the largest observed one."""

    def statistic(evens: Sequence[float], wins: float) -> float:
        spread = math.sqrt(sum(even * (1.0 - even) for even in evens))
        return (wins - sum(evens)) / spread if spread > 0 else 0.0

    populated = {name: trades for name, trades in cells.items() if trades}
    evens = {name: [break_even(entry, fee_rate) for _, entry, _ in trades] for name, trades in populated.items()}
    observed = {name: statistic(evens[name], sum(1.0 for _, _, won in trades if won)) for name, trades in populated.items()}
    result = {"statistics": observed, "max": max(observed.values(), default=None), "p_value": None, "replicates": int(replicates), "seed": int(seed)}
    if not populated or int(replicates) <= 0:
        return result
    rng = random.Random(int(seed))
    windows = sorted({window_start for trades in populated.values() for window_start, _, _ in trades})
    at_least = 0
    for _ in range(int(replicates)):
        draws = {window_start: rng.random() for window_start in windows}
        largest = max(
            statistic(evens[name], sum(1.0 for (window_start, _, _), even in zip(trades, evens[name]) if draws[window_start] < even))
            for name, trades in populated.items()
        )
        at_least += int(largest >= result["max"])
    result["p_value"] = at_least / int(replicates)
    return result


def event_grid(
    connection: sqlite3.Connection,
    closes: Mapping[int, float],
    cells: Sequence[str],
    campaign: Optional[Mapping[str, Any]] = None,
    cut_ts: Optional[int] = None,
    s_b: Optional[float] = None,
    fee_rate: Optional[float] = None,
    replicates: int = 0,
    seed: int = 0,
    recent_days: int = 4,
) -> Dict[str, Any]:
    """Score a family (ids of either grammar) on VPS ladder rows.  With a
    campaign: its frozen spec on the windows after its registration cut
    (the fresh evidence).  Without: DISCOVERY, under a spec fitted on the
    labelled windows that ended by `cut_ts` (default: the first VPS ladder
    window, so the calibration is strictly pre-ladder; `s_b` given instead
    of fitted reproduces a research value) and scored on the windows after
    it.  Per cell: the score of score_selection, entries per day over the
    covered windows (and the last `recent_days`), the first-crossing
    accuracy, capacity per day, the matched contrast; over the family the
    max-T test against break-even."""
    rows = load_rows(connection)
    fee = get_meta(connection, "fee") or {"rate": DEFAULT_FEE_RATE, "source": "default"}
    fee_rate = float(fee["rate"]) if fee_rate is None else float(fee_rate)
    hosts = (EVIDENCE_HOST,)
    resolved = resolve_cell_ids(cells)
    labels = labels_of(rows)
    series = None
    if campaign is not None:
        stale = campaign_version_error(campaign)
        if stale:
            raise ValueError(stale)
        spec, after = campaign.get("spec"), registration_cut(campaign)
    else:
        if cut_ts is None:
            cut_ts = connection.execute("SELECT MIN(window_start) FROM windows WHERE ladder_json IS NOT NULL AND ladder_host = ?", (EVIDENCE_HOST,)).fetchone()[0]
        if cut_ts is None:
            raise ValueError("no %s ladder row in the table" % EVIDENCE_HOST)
        series = event_series(closes, sorted(labels), {"params": settlement_model.DEFAULT_PARAMS})
        crossings = sorted({(float(rule["threshold"]), int(rule["t0"]), int(rule["t1"])) for _, rule in resolved if is_event_rule(rule)} | {FALSIFIER_CROSSING})
        spec = settlement_model.build_spec(series, sorted(labels.items()), int(cut_ts), fee_rate, s_b=s_b, crossings=crossings)
        after = int(cut_ts) - WINDOW_S
    fresh = [row for row in rows if int(row["window_start"]) > after]
    windows: List[Dict[str, Any]] = []
    if fresh and spec is not None:
        windows = event_windows(fresh, series or event_series(closes, [int(row["window_start"]) for row in fresh], spec), spec, hosts)
    covered = sorted({int(row["window_start"]) for row in fresh if row.get("ladder") and (row.get("ladder_host") or row["ladder"].get("host")) in hosts})
    per_day = 86400.0 / WINDOW_S
    recent_from = (covered[-1] - int(recent_days) * 86400) if covered else None
    recent_span = sum(1 for window_start in covered if window_start >= recent_from) / per_day if covered else 0.0
    noise = oracle_noise(rows)
    rows_by_decision: Dict[int, List[Dict[str, Any]]] = {}
    for row in fresh:
        rows_by_decision.setdefault(int(row["decision_s"]), []).append(row)
    reports: List[Dict[str, Any]] = []
    family_trades: Dict[str, List[Tuple[int, float, bool]]] = {}
    for identity, rule in resolved:
        event = is_event_rule(rule)
        if event:
            selection = select_event_cell(windows, rule, spec)
        else:
            selection = select_cell(rows_by_decision.get(int(rule["decision_second"]), []), rule, "ladder", hosts)
        score = score_selection(selection, labels, fee_rate, noise)
        trades = [trade for trade in selection["trades"] if labels.get(trade["window_start"]) in ("up", "down")]
        family_trades[identity] = [(trade["window_start"], trade["entry"], trade["direction"] == labels[trade["window_start"]]) for trade in trades]
        recent = [(entry, won) for window_start, entry, won in family_trades[identity] if recent_from is not None and window_start >= recent_from]
        report: Dict[str, Any] = {
            "cell_id": identity,
            "grammar": EVENT_GRAMMAR_VERSION if event else GRAMMAR_VERSION,
            "fingerprint": event_fingerprint(rule, spec["sha256"]) if event else fingerprint(rule),
            "rule": rule,
            "score": score,
            "entries_per_day": (len(trades) / (len(covered) / per_day)) if covered else None,
            "recent": {
                "days": int(recent_days),
                "entries_per_day": (len(recent) / recent_span) if recent_span else None,
                **_win_stats([won for _, won in recent]),
                **_economics([entry for entry, _ in recent], [won for _, won in recent], fee_rate),
            },
            "first_crossing": None,
            "capacity_daily": None,
            "matched": None,
        }
        if event:
            crossed = [signal["direction"] == labels[signal["window_start"]] for signal in selection["signals"] if labels.get(signal["window_start"]) in ("up", "down")]
            by_day: Dict[int, List[int]] = {}
            for window_start, filled in selection["capacity_points"]:
                counts = by_day.setdefault(int(window_start) // 86400, [0, 0])
                counts[0] += int(filled)
                counts[1] += 1
            report.update(
                {
                    "first_crossing": _win_stats(crossed),
                    "capacity_daily": [
                        {"day": _day_iso(day), "entries": by_day[day][0], "signal_windows": by_day[day][1], "fill_share": by_day[day][0] / by_day[day][1]}
                        for day in sorted(by_day)
                    ],
                    "matched": matched_contrast(windows, selection, labels, fee_rate, replicates, seed),
                }
            )
        reports.append(report)
    last_build = get_meta(connection, "last_build") or {}
    return {
        "generated_at": _utc_now(),
        "evaluator_version": EVALUATOR_VERSION,
        "event_grammar_version": EVENT_GRAMMAR_VERSION,
        "git_sha": last_build.get("git_sha"),
        "mode": "discovery" if campaign is None else "campaign:%s" % campaign["id"],
        "fee": {**fee, "rate": fee_rate},
        "ladder_hosts": list(hosts),
        "after_window_start": after,
        "covered_windows": len(covered),
        "covered_days": len(covered) / per_day,
        "spec": spec,
        "cells": reports,
        "max_t": max_t_null(family_trades, fee_rate, replicates, seed),
    }


def event_grid_text(report: Mapping[str, Any]) -> str:
    spec = report.get("spec") or {}
    lines = [
        "executable_truth event grid %s (%s): %d covered windows (%.1f days) after %s, fee=%.4f"
        % (report["generated_at"], report["mode"], report["covered_windows"], report["covered_days"], report["after_window_start"], report["fee"]["rate"]),
    ]
    if spec:
        lines.append(
            "spec %s: c=%.4f s_b=%.2f cut=%s windows=%s; k98 caps: %s"
            % (
                spec["sha256"][:12],
                spec["c"],
                spec["s_b"],
                spec["cut_ts"],
                spec["fit"]["windows"],
                ", ".join("z>=%s: %s" % (row["z_low"], row["cap"]) for row in spec["table"] if row["z_low"] >= 2.0),
            )
        )
    lines.append("cell W/n lower BE net/USD entries/day (last %d d) halves | never filled W/n | matched WR, p | +1 s older / 2 s delay / lag 0 net" % report["cells"][0]["recent"]["days"] if report["cells"] else "no cells")
    for cell in report["cells"]:
        score = cell["score"]
        latency = score.get("latency") or {}
        matched = cell.get("matched") or {}
        lines.append(
            "  %s: %d/%d lower=%s BE=%s net=%s %s/day (%s) halves %s | %d/%d | %s, p=%s | %s / %s / %s %s"
            % (
                cell["cell_id"],
                score["wins"],
                score["n"],
                _fmt(score["wilson_lower"], 4),
                _fmt(score["mean_break_even"], 4),
                _fmt(score["mean_net_per_usd"], 4),
                _fmt(cell["entries_per_day"], 1),
                _fmt(cell["recent"]["entries_per_day"], 1),
                ", ".join("%d/%d" % (part["wins"], part["n"]) for part in score["halves"]),
                score["excluded_pooled"]["wins"],
                score["excluded_pooled"]["n"],
                _fmt(matched.get("matched_win_rate"), 4),
                _fmt(matched.get("p_value"), 4),
                _fmt((latency.get("older1") or {}).get("mean_net_per_usd"), 4),
                _fmt((latency.get("lag2") or {}).get("mean_net_per_usd"), 4),
                _fmt((latency.get("lag0") or {}).get("mean_net_per_usd"), 4),
                _flags(score),
            )
        )
    family = report["max_t"]
    lines.append("max-T against break-even over the family: max=%s p=%s (%d replicates, seed %d)" % (_fmt(family["max"], 2), _fmt(family["p_value"], 4), family["replicates"], family["seed"]))
    return "\n".join(lines)


def _flags(score: Mapping[str, Any]) -> str:
    names = [name for name, fired in score["tripwires"].items() if fired]
    names += ["warn:%s" % name for name, fired in (score.get("warnings") or {}).items() if fired]
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
                        ("ladder-only " if cell.get("ladder_only") else "") + ("CLEARS " if score["clears_break_even"] else ""),
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


def family_rules(
    cell_ids: Sequence[str],
    spec: Optional[Mapping[str, Any]],
    alpha: float = E_BH_ALPHA,
    pre_ladder: Optional[Mapping[str, Any]] = None,
    fill_share_baseline: Optional[Mapping[str, float]] = None,
) -> Dict[str, Any]:
    """Section 6 of docs/adaptive_family_research_2026-10-01.md as
    machine-checkable entries for the cells of a family: the stopping
    rules (a cell's futility and capacity kills, the day-45 and day-90
    family stops), the three falsifiers and the paired static controls.
    An entry whose cells are not in the family is left out.  accrue_campaign
    evaluates each entry once, at the first tick on or after its day (or
    support), and records the look in meta.  `pre_ladder` is the frozen
    rule's first-crossing accuracy on the windows before the first
    evidence ladder (register computes it): its Wilson lower bound is F2's
    bar when it holds FALSIFIER_CROSSING_REFERENCE_MIN_N crossings, else
    the frozen spec's own crossing over every fitted window is.
    `fill_share_baseline` is each capacity cell's fill share over the last
    complete days before registration (register computes it): the cell's
    floor is EVENT_CAPACITY_SHARE_RATIO times it, never above the family
    floor; a cell without a baseline keeps the family floor."""
    present = set(cell_ids)
    events = [identity for identity in cell_ids if identity.startswith("e")]
    stopping: List[Dict[str, Any]] = [
        {"id": "futility", "scope": "cell", "cells": list(cell_ids), "metric": "e_value", "op": "<=", "value": evidence_accrual.FUTILITY_E, "action": "kill_cell"}
    ]
    capacity = [identity for identity in EVENT_CAPACITY_KILL_CELLS if identity in present]
    if capacity:
        baseline = {identity: float(share) for identity, share in (fill_share_baseline or {}).items() if identity in capacity and share is not None}
        stopping.append(
            {
                "id": "event_capacity",
                "scope": "cell",
                "cells": capacity,
                "days": EVENT_CAPACITY_DAYS,
                "min_entries_per_day": EVENT_CAPACITY_MIN_ENTRIES,
                "min_fill_share": EVENT_CAPACITY_MIN_SHARE,
                "baseline_fill_share": baseline,
                "min_fill_share_by_cell": {
                    identity: min(EVENT_CAPACITY_MIN_SHARE, EVENT_CAPACITY_SHARE_RATIO * share) for identity, share in baseline.items()
                },
                "min_data_days": EVENT_CAPACITY_MIN_DATA_DAYS,
                "action": "kill_cell",
                "text": "a 7-day median under 10 entries a day, or a fill share under the cell's floor (half its share over the last 7 complete days before registration, at most 0.10), kills the cell; no verdict before %d UTC days of data (the kill is permanent and must not precede readiness)"
                % EVENT_CAPACITY_MIN_DATA_DAYS,
            }
        )
    net_cells = [identity for identity in FAMILY_STOP_NET_CELLS if identity in present]
    if net_cells:
        stopping.append(
            {
                "id": "day%d_fresh_net" % FAMILY_STOP_NET_DAY,
                "scope": "family",
                "at_day": FAMILY_STOP_NET_DAY,
                "cells": net_cells,
                "metric": "mean_net_per_usd",
                "op": "<=",
                "value": 0.0,
                "action": "stop_family",
                "text": "day 45 with fresh net at +1 s <= 0 on every listed cell stops the family",
            }
        )
    stopping.append(
        {
            "id": "day%d_no_discovery" % FAMILY_STOP_DISCOVERY_DAY,
            "scope": "family",
            "at_day": FAMILY_STOP_DISCOVERY_DAY,
            "metric": "e_bh_discoveries",
            "op": "<=",
            "value": 0,
            "action": "stop_family",
            "text": "day 90 without an e-BH discovery at any tick so far (a cell held for a defect audit counts, one held by a falsifier does not) stops the family (and taker work on BTC 5m)",
        }
    )
    pairs = []
    for identity in cell_ids:
        for control in PAIRED_CONTROLS_V4.get(identity, ()):
            rule = control_rule_from_id(control)
            payload = {"lane": LANE, "pair": [identity, control], "evaluator_version": EVALUATOR_VERSION, "spec_sha256": (spec or {}).get("sha256")}
            pairs.append(
                {
                    "cell_id": identity,
                    "control_id": control,
                    "control_rule": rule,
                    "fingerprint": hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest(),
                }
            )
    controls = sorted({pair["control_id"] for pair in pairs})
    falsifiers: List[Dict[str, Any]] = []
    if FALSIFIER_MATCHED_CELL in present:
        falsifiers.append(
            {
                "id": "F1_z_adds_nothing",
                "at_day": FALSIFIER_MATCHED_DAY,
                "cell": FALSIFIER_MATCHED_CELL,
                "metric": "win_rate_minus_matched",
                "op": "<=",
                "value": 0.0,
                "action": "hold_event_cells",
                "text": "day 30: z-selected win rate minus the ask-and-time matched random rate <= 0: z adds nothing beyond the ask",
            }
        )
    crossing = next(
        (row for row in (spec or {}).get("first_crossing", []) if (row["z_min"], row["t_low"], row["t_high"]) == FALSIFIER_CROSSING), None
    )
    reference, reference_cut = "fit", (spec or {}).get("cut_ts")
    if pre_ladder is not None and pre_ladder["n"] >= FALSIFIER_CROSSING_REFERENCE_MIN_N:
        crossing, reference, reference_cut = pre_ladder, "pre_ladder", pre_ladder["cut_ts"]
    if events and crossing is not None and crossing["wilson_lower"] is not None:
        falsifiers.append(
            {
                "id": "F2_calibration_broke",
                "at_n": FALSIFIER_CROSSING_MIN_N,
                "crossing": list(FALSIFIER_CROSSING),
                "metric": "first_crossing_accuracy",
                # A fresh point rate under the reference's Wilson lower bound
                # fires on about 12% of healthy samples at n = 2,000; the
                # fresh sample's Wilson upper bound under the reference rate
                # is the same look at about 2.5%.
                "statistic": "wilson_upper",
                "op": "<",
                "value": crossing["rate"],
                # The windows the rate was measured on: those that ended by cut_ts.
                "reference": {"windows": reference, "cut_ts": reference_cut, **{key: crossing[key] for key in ("n", "wins", "rate", "wilson_lower")}},
                "action": "hold_event_cells",
                "text": "Wilson upper bound of the first-crossing accuracy of z >= 2.5 on fresh windows below the frozen rule's accuracy on the %s at n >= 2,000: the calibration broke"
                % ("windows before the first evidence ladder" if reference == "pre_ladder" else "fitted windows (no pre-ladder period in the table)"),
            }
        )
    if FALSIFIER_PAIRED_CELL in present and FALSIFIER_PAIRED_LEADER in present and any(pair["cell_id"] == FALSIFIER_PAIRED_CELL for pair in pairs):
        falsifiers.append(
            {
                "id": "F3_adaptive_dead",
                "at_day": FALSIFIER_PAIRED_DAY,
                "cell": FALSIFIER_PAIRED_CELL,
                "leader": FALSIFIER_PAIRED_LEADER,
                "metric": "paired_e_max",
                "op": "<",
                "value": 1.0,
                "action": "hold_event_cells",
                "text": "day 45: every paired e of the cell against its static controls below 1 while the leader's e-value exceeds the cell's: adaptive is dead even if an edge exists",
            }
        )
    return {
        "stopping_rules": stopping,
        "falsifiers": falsifiers,
        "paired_controls": {
            "k": len(controls),
            "bar_e": (len(controls) / alpha) if controls else None,
            "min_paired_windows": PAIRED_MIN_WINDOWS,
            "statistic": "per-window net per USD, cell minus control (0 where one does not trade), through update_signed after a fixed scale",
            "pairs": pairs,
        },
    }


def register(
    connection: sqlite3.Connection,
    campaign_id: str,
    campaigns_dir: Path = CAMPAIGNS_DIR,
    now_ts: Optional[int] = None,
    cells: Optional[Sequence[str]] = None,
    closes: Optional[Mapping[int, float]] = None,
    fee_rate: Optional[float] = None,
) -> Dict[str, Any]:
    """Pre-register an explicit family: N is fixed at the length of
    `cells` (ids of either grammar; V1..V6 name family v4) and never
    grows.  Refused without a list (the former screen admitted every grid
    cell that cleared it, Mac rows included), on an unknown or repeated
    id, over an existing registration, or before 7 days of VPS ladder
    rows: the VPS is the family's only evidence host, whatever the Mac
    overlap check says.  With an event cell the settlement spec is fitted
    on every labelled window of the table (all ended before registered_at)
    and frozen in the file with its sha256; `closes` are the Binance 1 s
    closes it is fitted on.  The file also carries section 6's stopping
    rules, falsifiers and paired controls (family_rules; F2's bar is the
    frozen rule's first-crossing accuracy on the windows that ended by the
    first VPS ladder window, the doc's pre-ladder bound) and each cell's
    in-sample score on the evidence rows (discovery data, for the record).
    Fresh evidence starts strictly after both the newest table row and
    the registration clock (registered_at_ts), so a window that started
    before registration but was built later never accrues."""
    path = Path(campaigns_dir) / ("%s.json" % campaign_id)
    if path.exists():
        return {"registered": False, "reason": "campaign %s already registered" % campaign_id, "path": str(path)}
    if not cells:
        return {
            "registered": False,
            "reason": "an explicit family is required: --register <id> --cells <cell ids> (the screen that admitted every qualifying grid cell, Mac rows included, is gone)",
        }
    try:
        resolved = resolve_cell_ids(cells)
    except ValueError as error:
        return {"registered": False, "reason": str(error)}
    hosts = (EVIDENCE_HOST,)
    days = ladder_days(connection, hosts)
    if days["days"] < REGISTER_MIN_LADDER_DAYS or days["span_days"] < REGISTER_MIN_LADDER_DAYS:
        return {
            "registered": False,
            "reason": "ladder rows from %s cover %d days (span %d); %d required" % (",".join(hosts), days["days"], days["span_days"], REGISTER_MIN_LADDER_DAYS),
            "ladder_rows_by_host": ladder_rows_by_host(connection),
        }
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    newest = connection.execute("SELECT MAX(window_start) FROM windows").fetchone()[0]
    after = max(int(newest) if newest is not None else -1, band_lane.last_eligible_window_start(now_ts))
    rows = [row for row in load_rows(connection) if int(row["window_start"]) <= after]
    labels = labels_of(rows)
    fee = get_meta(connection, "fee") or {"rate": DEFAULT_FEE_RATE, "source": "default"}
    fee_rate = float(fee["rate"]) if fee_rate is None else float(fee_rate)
    spec = pre_ladder = None
    windows: List[Dict[str, Any]] = []
    if any(is_event_rule(rule) for _, rule in resolved):
        if closes is None:
            return {"registered": False, "reason": "an event cell needs the Binance closes to freeze the settlement spec"}
        starts = sorted(labels)
        series = event_series(closes, starts, {"params": settlement_model.DEFAULT_PARAMS})
        crossings = sorted({(float(rule["threshold"]), int(rule["t0"]), int(rule["t1"])) for _, rule in resolved if is_event_rule(rule)} | {FALSIFIER_CROSSING})
        try:
            spec = settlement_model.build_spec(series, sorted(labels.items()), after + WINDOW_S, fee_rate, crossings=crossings)
        except ValueError as error:
            return {"registered": False, "reason": str(error)}
        windows = event_windows(rows, series, spec, hosts)
        first_ladder = int(connection.execute("SELECT MIN(window_start) FROM windows WHERE ladder_json IS NOT NULL AND ladder_host = ?", (EVIDENCE_HOST,)).fetchone()[0])
        before = [(start, labels[start]) for start in starts if labels[start] in ("up", "down") and start + WINDOW_S <= first_ladder]
        pre_ladder = {**settlement_model.first_crossing(series, before, spec, *FALSIFIER_CROSSING), "cut_ts": first_ladder}
    noise = oracle_noise(rows)
    rows_by_decision: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        rows_by_decision.setdefault(int(row["decision_s"]), []).append(row)
    aliases = {identity: name for name, identity in FAMILY_V4.items()}
    entries: List[Dict[str, Any]] = []
    fill_share_baseline: Dict[str, float] = {}
    baseline_rule = {"days": EVENT_CAPACITY_DAYS, "min_entries_per_day": 0, "min_fill_share": 0.0}
    for identity, rule in resolved:
        if is_event_rule(rule):
            selection = select_event_cell(windows, rule, spec)
            fp = event_fingerprint(rule, spec["sha256"])
            share = (event_capacity_verdict(selection["capacity_points"], baseline_rule) or {}).get("fill_share")
            if identity in EVENT_CAPACITY_KILL_CELLS and share is not None:
                fill_share_baseline[identity] = share
        else:
            selection = select_cell(rows_by_decision.get(int(rule["decision_second"]), []), rule, "ladder", hosts)
            fp = fingerprint(rule)
        score = score_selection(selection, labels, fee_rate, noise)
        entries.append(
            {
                "cell_id": identity,
                "name": aliases.get(identity),
                "fingerprint": fp,
                "grammar": EVENT_GRAMMAR_VERSION if is_event_rule(rule) else GRAMMAR_VERSION,
                "rule": rule,
                # In-sample on the evidence rows before registered_at:
                # discovery data, never evidence.
                "at_registration": {key: score[key] for key in ("n", "wins", "wilson_lower", "mean_break_even", "mean_net_per_usd")},
            }
        )
    last_build = get_meta(connection, "last_build") or {}
    campaign = {
        "schema_version": 2,
        "id": campaign_id,
        "lane": LANE,
        "status": "active",
        "registered_at": _iso(now_ts),
        "registered_at_ts": now_ts,
        "registered_after_window_start": after,
        "family_size": len(entries),
        "alpha": E_BH_ALPHA,
        "promote_e": evidence_accrual.PROMOTE_E,
        "futility_e": evidence_accrual.FUTILITY_E,
        "budget_usd": LADDER_BUDGET_USD,
        "fee": {**fee, "rate": fee_rate},
        "grammar_version": GRAMMAR_VERSION,
        "event_grammar_version": EVENT_GRAMMAR_VERSION,
        "evaluator_version": EVALUATOR_VERSION,
        "git_sha": last_build.get("git_sha") or git_sha(),
        "ladder_days": days,
        "ladder_hosts": list(hosts),
        "ladder_rows_by_host": ladder_rows_by_host(connection),
        "audit_cleared": {},
        "falsifier_cleared": {},
        "cells": entries,
        "spec": spec,
        **family_rules([entry["cell_id"] for entry in entries], spec, pre_ladder=pre_ladder, fill_share_baseline=fill_share_baseline),
    }
    band_lane._atomic_write(path, json.dumps(campaign, indent=2, sort_keys=True) + "\n")
    return {
        "registered": True,
        "path": str(path),
        "family_size": len(entries),
        "cells": [entry["cell_id"] for entry in entries],
        "spec_sha256": None if spec is None else spec["sha256"],
    }


def registration_cut(campaign: Mapping[str, Any]) -> int:
    """The last window start that is NOT fresh evidence: the newest row at
    registration, or the registration clock, whichever is later."""
    after = int(campaign.get("registered_after_window_start") or -1)
    registered_at_ts = campaign.get("registered_at_ts")
    if registered_at_ts is None and campaign.get("registered_at"):
        registered_at_ts = dt.datetime.fromisoformat(str(campaign["registered_at"]).replace("Z", "+00:00")).timestamp()
    return max(after, int(registered_at_ts) - 1) if registered_at_ts is not None else after


def campaign_ladder_hosts(campaign: Mapping[str, Any]) -> Tuple[str, ...]:
    """The hosts a campaign accrues: those fixed at registration (the VPS).
    An overlap acceptance recorded since widens discovery, never a
    registered family."""
    return tuple(campaign.get("ladder_hosts") or (EVIDENCE_HOST,))


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


def cleared_falsifiers(campaign: Mapping[str, Any]) -> Tuple[str, ...]:
    return tuple(str(identity) for identity in (campaign.get("falsifier_cleared") or {}))


def clear_falsifier(
    connection: sqlite3.Connection, campaigns_dir: Path, campaign_id: str, falsifier_id: str, note: str, now_ts: Optional[int] = None
) -> Dict[str, Any]:
    """Record an operator's completed audit of a FIRED falsifier (the
    clear_audit analogue for section 6's holds): accrual no longer holds
    the event cells on it and they return to the e-BH candidate set.  The
    look itself stays in meta, fired, and is never repeated.  Only a
    falsifier of the campaign whose recorded look fired can be cleared (an
    audit of a look not yet taken is not an audit), with a note; a family
    stop cannot."""
    path = Path(campaigns_dir) / ("%s.json" % campaign_id)
    campaign = json.loads(path.read_text())
    if falsifier_id not in {entry["id"] for entry in campaign.get("falsifiers") or []}:
        raise ValueError("%s is not a falsifier of campaign %s" % (falsifier_id, campaign_id))
    look = (get_meta(connection, "campaign_checks:%s" % campaign_id) or {}).get(falsifier_id)
    if not look or not look.get("fired"):
        raise ValueError("falsifier %s of campaign %s has not fired: nothing to audit" % (falsifier_id, campaign_id))
    if not str(note).strip():
        raise ValueError("a falsifier clear needs a note")
    cleared = dict(campaign.get("falsifier_cleared") or {})
    cleared[falsifier_id] = {"note": str(note), "cleared_at": _iso(int(time.time()) if now_ts is None else int(now_ts)), "look": look}
    campaign["falsifier_cleared"] = cleared
    band_lane._atomic_write(path, json.dumps(campaign, indent=2, sort_keys=True) + "\n")
    return {"cleared": True, "falsifier": falsifier_id, "look": look, "path": str(path)}


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


def campaign_version_error(campaign: Mapping[str, Any]) -> Optional[str]:
    """A campaign registered under another evaluator or grammar version is
    stale: its fingerprints, cells and tripwires were fixed by code this
    tree no longer runs, so it accrues nothing and every cell is held
    (manual_audit, `stale_evaluator`) until it is re-registered: a versioned
    change resets registered_at (CLAUDE.md section 5).  So is a campaign
    with an event cell whose frozen spec fails its sha256."""
    registered = (str(campaign.get("evaluator_version")), str(campaign.get("grammar_version")))
    if registered != (EVALUATOR_VERSION, GRAMMAR_VERSION) or campaign.get("event_grammar_version", EVENT_GRAMMAR_VERSION) != EVENT_GRAMMAR_VERSION:
        return "campaign %s was registered under %s/%s; this tree runs %s/%s: re-register (registered_at resets)" % (
            campaign.get("id"),
            registered[0],
            registered[1],
            EVALUATOR_VERSION,
            GRAMMAR_VERSION,
        )
    # An event cell is scored with the spec frozen at registration and
    # nothing else: a spec that is missing or no longer matches its hash
    # holds the campaign the same way.
    if any(is_event_rule(cell["rule"]) for cell in campaign["cells"]):
        try:
            settlement_model.verify_spec(campaign.get("spec"))
        except ValueError as error:
            return "campaign %s: %s: re-register (registered_at resets)" % (campaign.get("id"), error)
    return None


def accrued_windows(connection: sqlite3.Connection, campaign_id: str, fp: str) -> set:
    return {
        int(row[0])
        for row in connection.execute(
            "SELECT window_start FROM campaign_accrual_windows WHERE campaign_id = ? AND fingerprint = ?", (campaign_id, fp)
        )
    }


def _compare(value: Optional[float], op: str, bar: float) -> bool:
    if value is None:
        return False
    return value <= bar if op == "<=" else value < bar


def fresh_first_crossing(windows: Sequence[Mapping[str, Any]], crossing: Sequence[float]) -> Dict[str, Any]:
    """Accuracy of the stopped rule on event_windows records: over the
    labelled windows whose z reaches z_min in [t_low, t_high], the share
    whose side at the first such second is the official outcome."""
    z_min, t_low, t_high = float(crossing[0]), int(crossing[1]), int(crossing[2])
    wins = total = 0
    for window in windows:
        if window.get("official") not in ("up", "down"):
            continue
        for second in range(t_low, t_high + 1):
            point = window["path"].get(second)
            if point is not None and point[1] >= z_min:
                total += 1
                wins += int(("up" if point[0] >= 0 else "down") == window["official"])
                break
    return {"n": total, "wins": wins, "rate": (wins / total) if total else None, "wilson_lower": band_lane.wilson_lower(wins, total)}


def crossing_statistic(entry: Mapping[str, Any], crossing: Mapping[str, Any]) -> Optional[float]:
    """What F2 compares with its bar: the fresh sample's Wilson upper bound
    when the campaign says so, else its point rate (a campaign registered
    before the restatement)."""
    if entry.get("statistic") == "wilson_upper":
        return settlement_model.wilson_upper(int(crossing["wins"]), int(crossing["n"]))
    return crossing["rate"]


def _trade_nets(selection: Mapping[str, Any], labels: Mapping[int, Optional[str]], fee_rate: float) -> Dict[int, float]:
    return {
        int(trade["window_start"]): net_per_usd(trade["entry"], trade["direction"] == labels[trade["window_start"]], fee_rate)
        for trade in selection["trades"]
        if labels.get(trade["window_start"]) in ("up", "down")
    }


def accrue_campaign(
    connection: sqlite3.Connection,
    campaign: Mapping[str, Any],
    loop_config: Optional[Mapping[str, Any]] = None,
    fee_rate: Optional[float] = None,
    closes: Optional[Mapping[int, float]] = None,
) -> Dict[str, Any]:
    """Fold every labelled trade after the registration cut that the
    cell's e-process has not seen (campaign_accrual_windows names them, so
    a row built late, behind a ladder grace or a pull gap, is folded when
    it arrives instead of skipped behind a high-water mark) into every
    registered cell's e-process (break-even at the FOK worst price of the
    $25 quote), then decide the family: e-BH at alpha over the cells that
    are ready and hold neither an uncleared defect nor a falsifier's hold;
    a defect fails closed to manual_audit, futility and a capacity
    collapse kill, everything else keeps accruing.  The campaign's hosts
    only.  One trial-ledger row per (fingerprint, look_id).  An event cell
    is replayed with the campaign's frozen spec on `closes` and nothing is
    refitted.  The paired controls accrue beside the family (outside N)
    and section 6's entries are evaluated once each, when due
    (campaign_checks), the falsifiers ahead of the e-BH step: a fired
    falsifier holds the event cells in manual_audit and out of the
    candidate set (a held cell that counted toward k* would lower the bar
    for the static member) until --clear-falsifier records its audit; a
    fired family stop kills every cell still running.  The day-90 stop
    reads the discoveries recorded so far (campaign_discoveries: every
    cell that was in an e-BH rejection set at some tick, a cell held for a
    defect audit included), not this tick's k*.  A campaign registered
    under another evaluator or grammar version, or whose spec fails its
    hash (campaign_version_error), touches nothing: every cell is reported
    manual_audit with reason stale_evaluator."""
    campaign_id = str(campaign["id"])
    stale = campaign_version_error(campaign)
    if stale:
        cells = []
        for cell in campaign["cells"]:
            state = connection.execute(
                "SELECT * FROM campaign_accrual WHERE campaign_id = ? AND fingerprint = ?", (campaign_id, str(cell["fingerprint"]))
            ).fetchone()
            cells.append(
                {
                    "cell_id": cell["cell_id"],
                    "fingerprint": str(cell["fingerprint"]),
                    "n": int(state["n"]) if state else 0,
                    "wins": int(state["wins"]) if state else 0,
                    "e_value": float(state["e_value"]) if state else None,
                    "verdict": str(state["verdict"]) if state else None,
                    "status": "manual_audit",
                    "reason": "stale_evaluator",
                    "applied": 0,
                    "ledger_row": False,
                }
            )
        return {"campaign": campaign_id, "stale_evaluator": stale, "ladder_hosts": [], "cells": cells, "e_bh": None}
    after = registration_cut(campaign)
    hosts = campaign_ladder_hosts(campaign)
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
    spec = campaign.get("spec")
    fresh = [row for row in rows if int(row["window_start"]) > after]
    windows: List[Dict[str, Any]] = []
    if fresh and any(is_event_rule(cell["rule"]) for cell in campaign["cells"]):
        if closes is None:
            raise ValueError("campaign %s has event cells: accrual needs the Binance closes" % campaign_id)
        windows = event_windows(fresh, event_series(closes, [int(row["window_start"]) for row in fresh], spec), spec, hosts)
    stopping = {rule["id"]: rule for rule in campaign.get("stopping_rules") or []}
    capacity_rule = stopping.get("event_capacity")
    selections: Dict[str, Dict[str, Any]] = {}
    results: List[Dict[str, Any]] = []
    for cell in campaign["cells"]:
        fp = str(cell["fingerprint"])
        state = connection.execute(
            "SELECT * FROM campaign_accrual WHERE campaign_id = ? AND fingerprint = ?", (campaign_id, fp)
        ).fetchone()
        process = evidence_accrual.EProcess.from_json(state["state_json"]) if state else evidence_accrual.EProcess()
        wins = int(state["wins"]) if state else 0
        status = str(state["status"]) if state else "accruing"
        seen = accrued_windows(connection, campaign_id, fp)
        if is_event_rule(cell["rule"]):
            selection = select_event_cell(windows, cell["rule"], spec, event_capacity_rule_for(capacity_rule, cell["cell_id"]))
        else:
            selection = _fresh_selection(rows_by_decision, normalized_rule(cell["rule"]), after, hosts)
        selections[cell["cell_id"]] = selection
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
                "event": is_event_rule(cell["rule"]),
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
    paired = _accrue_paired_controls(connection, campaign, windows, selections, labels, fee_rate, now)
    # The falsifiers come first: a cell they hold is out of the e-BH step.
    looks = _campaign_checks(connection, campaign, campaign.get("falsifiers") or [], fresh, windows, selections, results, paired, labels, fee_rate, now)
    audited = cleared_falsifiers(campaign)
    falsified = sorted(identity for identity, check in looks.items() if check["fired"] and check["action"] == "hold_event_cells" and identity not in audited)
    running = [
        result
        for result in results
        if result["verdict"] != "kill"
        and result["score"]["ready"]
        and not result["score"]["killed"]
        and not result["previous_status"].startswith("killed")
        and not (falsified and result["event"])
    ]
    size, alpha = int(campaign["family_size"]), float(campaign.get("alpha", E_BH_ALPHA))
    eligible = {result["fingerprint"]: result["e_value"] for result in running if not result["score"]["held"]}
    family = evidence_accrual.e_bh(eligible, size, alpha, family_size=len(results))
    # Discoveries so far, for the day-90 stop: a discovery is valid when it
    # is made, so each is recorded at its first tick; a cell held for a
    # defect audit whose e clears the bar is one (the audit may clear it).
    discoveries: Dict[str, Any] = dict(get_meta(connection, "campaign_discoveries:%s" % campaign_id) or {})
    crossed = set(evidence_accrual.e_bh({result["fingerprint"]: result["e_value"] for result in running}, size, alpha, family_size=len(results))["promoted"])
    for result in running:
        if result["fingerprint"] in crossed and result["cell_id"] not in discoveries:
            discoveries[result["cell_id"]] = {"at": now, "e_value": result["e_value"], "held": bool(result["score"]["held"])}
    set_meta(connection, "campaign_discoveries:%s" % campaign_id, discoveries)
    checks = _campaign_checks(
        connection, campaign, campaign.get("stopping_rules") or [], fresh, windows, selections, results, paired, labels, fee_rate, now, len(discoveries)
    )
    checks.update(looks)
    stopped = next((identity for identity, check in checks.items() if check["fired"] and check["action"] == "stop_family"), None)
    summary: Dict[str, Any] = {
        "campaign": campaign_id,
        "registration_cut": after,
        "ladder_hosts": list(hosts),
        "ladder_rows_by_host": ladder_rows_by_host(connection),
        "spec_sha256": None if spec is None else spec["sha256"],
        "cells": [],
        "e_bh": {key: family[key] for key in ("campaign_n", "candidates", "family", "k_star", "threshold", "overflow")},
        "paired_controls": paired,
        "checks": checks,
        "family_stopped": stopped,
        "falsified": falsified,
        "falsifier_cleared": sorted(identity for identity in audited if identity in looks),
        "discoveries": discoveries,
    }
    for result in results:
        score = result["score"]
        if result["previous_status"].startswith("killed"):
            status = result["previous_status"]
        elif score["killed"]:
            status = "killed_%s" % next(name for name in KILL_TRIPWIRES if score["tripwires"][name])
        elif result["verdict"] == "kill":
            status = "killed_futility"
        elif stopped:
            status = "killed_%s" % stopped
        elif score["held"] or (falsified and result["event"]):
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
                "warnings": score["warnings"],
                "capacity_trend": score["capacity_trend"],
                "frozen_ceiling": score["frozen_ceiling"],
                "frozen_ceiling_p": score["frozen_ceiling_p"],
                "fills_outside_gates": score["fills_outside_gates"],
                "latency": score["latency"] if result["event"] else None,
                "excluded": {reason: value["n"] for reason, value in score["excluded"].items()},
            }
        )
    connection.commit()
    return summary


def _accrue_paired_controls(
    connection: sqlite3.Connection,
    campaign: Mapping[str, Any],
    windows: Sequence[Dict[str, Any]],
    selections: Mapping[str, Mapping[str, Any]],
    labels: Mapping[int, Optional[str]],
    fee_rate: float,
    now: str,
) -> List[Dict[str, Any]]:
    """The paired static controls (outside N): per pair an e-process on the
    per-window net difference at $1 staked, cell minus control (0 where
    one does not trade; a window neither trades carries nothing), divided
    by the largest payoff per USD (1 / break-even at the ask floor) so
    |d| < 1 and update_signed's clamp is idle.  A pair is a discovery at
    e >= bar_e (K / alpha over the K distinct controls) on at least
    min_paired_windows.  State lives in campaign_accrual under the pair's
    fingerprint; a window is folded once, and only when it is complete
    (event_windows: every ladder attached or past the grace): on a partial
    ladder set one side can trade where both will, and the difference
    folded then would never be corrected (`pending` counts the windows
    that wait)."""
    config = campaign.get("paired_controls") or {}
    campaign_id = str(campaign["id"])
    spec = campaign.get("spec")
    scale = 1.0 / break_even(ASK_FLOOR, fee_rate)
    complete = {window["window_start"] for window in windows if window["complete"]}
    controls: Dict[str, Dict[int, float]] = {}
    reports: List[Dict[str, Any]] = []
    for pair in config.get("pairs") or []:
        if pair["control_id"] not in controls:
            controls[pair["control_id"]] = _trade_nets(select_event_cell(windows, pair["control_rule"], spec), labels, fee_rate)
        ours, theirs = _trade_nets(selections[pair["cell_id"]], labels, fee_rate), controls[pair["control_id"]]
        fp = str(pair["fingerprint"])
        state = connection.execute("SELECT * FROM campaign_accrual WHERE campaign_id = ? AND fingerprint = ?", (campaign_id, fp)).fetchone()
        process = evidence_accrual.EProcess.from_json(state["state_json"]) if state else evidence_accrual.EProcess()
        ahead = int(state["wins"]) if state else 0
        seen = accrued_windows(connection, campaign_id, fp)
        applied = pending = 0
        for window_start in sorted(set(ours) | set(theirs)):
            if window_start in seen:
                continue
            if window_start not in complete:
                pending += 1
                continue
            d = (ours.get(window_start, 0.0) - theirs.get(window_start, 0.0)) / scale
            process.update_signed(d)
            ahead += int(d > 0)
            seen.add(window_start)
            connection.execute("INSERT OR IGNORE INTO campaign_accrual_windows VALUES (?, ?, ?)", (campaign_id, fp, window_start))
            applied += 1
        e_value = process.e_value()
        discovery = bool(config.get("bar_e") and e_value >= float(config["bar_e"]) and process.n >= int(config["min_paired_windows"]))
        status = "paired_discovery" if discovery else "paired_accruing"
        if seen:
            connection.execute(
                "INSERT OR REPLACE INTO campaign_accrual VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (campaign_id, fp, process.n, ahead, process.to_json(), min(seen), max(seen), e_value, "discovery" if discovery else "continue", status, now),
            )
        reports.append(
            {
                "cell_id": pair["cell_id"],
                "control_id": pair["control_id"],
                "fingerprint": fp,
                "paired_windows": process.n,
                "cell_ahead": ahead,
                "e_value": e_value,
                "bar_e": config.get("bar_e"),
                "status": status,
                "applied": applied,
                "pending": pending,
                "d_scale": scale,
            }
        )
    return reports


def _campaign_checks(
    connection: sqlite3.Connection,
    campaign: Mapping[str, Any],
    entries: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    windows: Sequence[Dict[str, Any]],
    selections: Mapping[str, Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
    paired: Sequence[Mapping[str, Any]],
    labels: Mapping[int, Optional[str]],
    fee_rate: float,
    now: str,
    discoveries: Optional[int] = None,
) -> Dict[str, Dict[str, Any]]:
    """Section 6's family stops and falsifiers (`entries`: the campaign's
    falsifiers, or its stopping rules), each evaluated ONCE: at the first
    accrual on or after its day (the data clock: the end of the newest
    fresh window, in days since registered_at) or its support, on the
    fresh evidence to that point; the look is recorded in meta and never
    repeated (a pre-registered look, not a running test).  `discoveries`
    is the number of cells recorded as an e-BH discovery at any tick so
    far (the day-90 stop's metric).  Cell kills (futility, capacity) run
    every tick in accrue_campaign."""
    key = "campaign_checks:%s" % campaign["id"]
    done: Dict[str, Dict[str, Any]] = dict(get_meta(connection, key) or {})
    newest = max((int(row["window_start"]) for row in rows), default=None)
    day = None if newest is None else (newest + WINDOW_S - int(campaign["registered_at_ts"])) / 86400.0
    by_cell = {result["cell_id"]: result for result in results}
    checks: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        if entry["action"] == "kill_cell":
            continue
        identity = entry["id"]
        if identity in done:
            checks[identity] = done[identity]
            continue
        check: Dict[str, Any] = {"action": entry["action"], "fired": False, "evaluated": False, "day": day}
        checks[identity] = check
        metric = entry["metric"]
        crossing = fresh_first_crossing(windows, entry["crossing"]) if metric == "first_crossing_accuracy" else None
        if "at_day" in entry and (day is None or day < float(entry["at_day"])):
            continue
        if "at_n" in entry and crossing["n"] < int(entry["at_n"]):
            check["n"] = crossing["n"]
            continue
        if metric == "mean_net_per_usd":
            values = [by_cell[cell]["score"]["mean_net_per_usd"] for cell in entry["cells"]]
            # A cell without one fresh entry has shown no net above zero.
            fired = all(value is None or _compare(value, entry["op"], entry["value"]) for value in values)
            check["value"] = values
        elif metric == "e_bh_discoveries":
            check["value"] = int(discoveries or 0)
            fired = _compare(check["value"], entry["op"], entry["value"])
        elif metric == "win_rate_minus_matched":
            contrast = matched_contrast(windows, selections[entry["cell"]], labels, fee_rate)
            check["value"] = contrast["win_rate_minus_matched"]
            check["n"] = contrast["n"]
            fired = _compare(check["value"], entry["op"], entry["value"])
        elif metric == "first_crossing_accuracy":
            check["value"], check["n"], check["rate"] = crossing_statistic(entry, crossing), crossing["n"], crossing["rate"]
            fired = _compare(check["value"], entry["op"], entry["value"])
        elif metric == "paired_e_max":
            check["value"] = max((pair["e_value"] for pair in paired if pair["cell_id"] == entry["cell"]), default=None)
            check["leader_e"], check["cell_e"] = by_cell[entry["leader"]]["e_value"], by_cell[entry["cell"]]["e_value"]
            fired = _compare(check["value"], entry["op"], entry["value"]) and check["leader_e"] > check["cell_e"]
        else:
            raise ValueError("campaign %s: unknown check metric %r" % (campaign["id"], metric))
        check.update({"fired": bool(fired), "evaluated": True, "evaluated_at": now})
        done[identity] = check
    set_meta(connection, key, done)
    return checks


def gate_artifact(
    connection: sqlite3.Connection, campaign: Mapping[str, Any], cell_id_text: str, fee_rate: Optional[float] = None
) -> Dict[str, Any]:
    """The evidence artifact cmd_band_promotion_artifact (rust_engine
    main.rs) reads: verdict PASS, candidate = the band family, rows with
    signal_entry and won, fresh_range, registration.  Rows are the cell's
    ladder trades after registration at the FOK worst price, evidence hosts
    only; PASS only on an e-BH discovery with every uncleared tripwire
    quiet, and never for a campaign registered under another evaluator or
    grammar version (IMPLAUSIBLE_MANUAL_AUDIT, `stale_evaluator`).  Static
    cells only: an event cell has no engine policy to promote."""
    cell = next((item for item in campaign["cells"] if item["cell_id"] == cell_id_text), None)
    if cell is None:
        raise ValueError("cell %s is not registered in campaign %s" % (cell_id_text, campaign["id"]))
    if is_event_rule(cell["rule"]):
        raise ValueError("cell %s is an event cell: the engine has no event policy, so there is no promotion artifact for it" % cell_id_text)
    hosts = campaign_ladder_hosts(campaign)
    state = connection.execute(
        "SELECT * FROM campaign_accrual WHERE campaign_id = ? AND fingerprint = ?", (str(campaign["id"]), cell["fingerprint"])
    ).fetchone()
    status = str(state["status"]) if state else "unaccrued"
    stale = campaign_version_error(campaign)
    payload = _gate_payload(connection, normalized_rule(cell["rule"]), hosts, registration_cut(campaign), cleared_tripwires(campaign, str(cell["fingerprint"])), fee_rate)
    score = payload.pop("score")
    if stale:
        verdict = "IMPLAUSIBLE_MANUAL_AUDIT"
    elif status == "promote_candidate" and score["promotable"]:
        verdict = "PASS"
    elif score["held"] or status == "manual_audit":
        verdict = "IMPLAUSIBLE_MANUAL_AUDIT"
    elif status.startswith("killed"):
        verdict = "FAIL_TOMBSTONE"
    else:
        verdict = "INSUFFICIENT"
    return {
        **payload,
        **({"stale_evaluator": stale} if stale else {}),
        "registration": str(campaign["id"]),
        "cell_id": cell["cell_id"],
        "fingerprint": cell["fingerprint"],
        "accrual": None if state is None else {key: state[key] for key in ("n", "wins", "e_value", "verdict", "status", "updated_at")},
        "verdict": verdict,
    }


def discovery_gate_artifact(connection: sqlite3.Connection, cell_id_text: str, fee_rate: Optional[float] = None) -> Dict[str, Any]:
    """The same artifact for an UNREGISTERED cell, for a paper twin only:
    every evidence-host ladder trade in the table (no registration cut, no
    e-process), verdict INSUFFICIENT by construction and `discovery_twin`
    set, which cmd_band_promotion_artifact turns into a paper-only
    promotion artifact (the live preflight refuses it).  It is not
    promotion evidence and never becomes one: registration comes first."""
    rule = rule_from_cell_id(cell_id_text)
    payload = _gate_payload(connection, rule, evidence_hosts(connection), -1, (), fee_rate)
    payload.pop("score")
    return {
        **payload,
        "registration": "discovery",
        "discovery_twin": True,
        "cell_id": cell_id(rule),
        "fingerprint": fingerprint(rule),
        "accrual": None,
        "verdict": "INSUFFICIENT",
    }


def _gate_payload(
    connection: sqlite3.Connection,
    rule: Mapping[str, Any],
    hosts: Sequence[str],
    after_window_start: int,
    cleared: Sequence[str],
    fee_rate: Optional[float],
) -> Dict[str, Any]:
    rows = load_rows(connection)
    rows_by_decision: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        rows_by_decision.setdefault(int(row["decision_s"]), []).append(row)
    labels = labels_of(rows)
    fee = get_meta(connection, "fee") or {"rate": DEFAULT_FEE_RATE}
    fee_rate = float(fee["rate"]) if fee_rate is None else float(fee_rate)
    selection = _fresh_selection(rows_by_decision, rule, after_window_start, hosts)
    score = score_selection(selection, labels, fee_rate, oracle_noise(rows), load_edge_series(connection), cleared)
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
    return {
        "schema_version": 1,
        "candidate": BAND_FAMILY,
        "evaluator_version": EVALUATOR_VERSION,
        "grammar_version": GRAMMAR_VERSION,
        "rule": dict(rule),
        "model": "ladder",
        "budget_usd": LADDER_BUDGET_USD,
        "ladder_hosts": list(hosts),
        "fee_rate": fee_rate,
        "fee": fee,
        "fresh_range": [score["first_window_start"], score["last_window_start"]],
        "registration_cut": None if after_window_start < 0 else int(after_window_start),
        "support": score["n"],
        "wins": score["wins"],
        "win_rate": score["win_rate"],
        "wilson_lo": score["wilson_lower"],
        "avg_break_even": score["mean_break_even"],
        "point_edge": None if score["win_rate"] is None else score["win_rate"] - score["mean_break_even"],
        "wilson_edge": None if score["wilson_lower"] is None else score["wilson_lower"] - score["mean_break_even"],
        "net_per_usd": score["mean_net_per_usd"],
        "tripwires": score["tripwires"],
        "warnings": score["warnings"],
        "capacity": score["capacity"],
        "capacity_trend": score["capacity_trend"],
        "audit_cleared": score["audit_cleared"],
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
        "score": score,
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
        accrual = accrue_campaign(connection, campaign, loop_config, closes=cache.closes)
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


def _warn_build(report: Mapping[str, Any]) -> None:
    if report["fee"].get("warning"):
        print("WARNING: %s" % report["fee"]["warning"], file=sys.stderr)
    if report.get("git_dirty") and report.get("rows_inserted"):
        print(
            "WARNING: %d rows stamped %s: built by an uncommitted tree (commit first, or expect an audit at that sha to fail)"
            % (report["rows_inserted"], report["git_sha"]),
            file=sys.stderr,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--build", action="store_true", help="append the rows of newly settled windows to the table")
    action.add_argument("--grid", action="store_true", help="score every grammar cell under the three models")
    action.add_argument("--cell", help="full report for one cell, e.g. d240_f75_c0.97_p15")
    action.add_argument("--event-grid", action="store_true", help="score event cells (--cells, default V1..V5) on VPS ladder rows: discovery spec, or --campaign's frozen spec on its fresh windows")
    action.add_argument("--register", metavar="ID", help="write deploy/campaigns/<ID>.json with the explicit family --cells (N fixed = the list)")
    action.add_argument("--gate-json", action="store_true", help="emit the promotion evidence artifact for --campaign/--cell-id")
    action.add_argument("--cell-gate-json", metavar="CELL_ID", help="discovery mode: the same artifact for an unregistered cell (verdict INSUFFICIENT, discovery_twin) for a PAPER twin only")
    action.add_argument("--tick", action="store_true", help="incremental build plus accrual of every registered campaign")
    action.add_argument("--clear-audit", metavar="CELL_ID", help="record a completed audit of a registered cell's defect tripwires (--campaign, --tripwire, --note)")
    action.add_argument("--clear-falsifier", metavar="FALSIFIER_ID", help="record a completed audit of a fired falsifier, releasing the event cells it holds (--campaign, --note)")
    action.add_argument("--accept-mac-ladders", action="store_true", help="run the Mac/VPS overlap check and, when it passes, accept Mac ladder rows as evidence")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--start-ts", type=int, help="first window start (default: the band lane's start_ts)")
    parser.add_argument("--now-ts", type=int, help="clock override (tests)")
    parser.add_argument("--sessions-dir", type=Path, action="append", help="session log directory (repeatable; default: the VPS mirror and the Mac observer)")
    parser.add_argument("--campaigns-dir", type=Path, default=CAMPAIGNS_DIR)
    parser.add_argument("--campaign", help="campaign id for --gate-json (default: the only active campaign) and --event-grid")
    parser.add_argument("--cells", help="--register / --event-grid: comma list of cell ids of either grammar; V1..V6 name family v4")
    parser.add_argument("--spec-cut-ts", type=int, help="--event-grid discovery: fit the spec on windows that ended by this time (default: the first VPS ladder window)")
    parser.add_argument("--s-b", type=float, help="--event-grid discovery: basis noise given instead of fitted (the research value is 3.5)")
    parser.add_argument("--cell-id", help="cell id for --gate-json")
    parser.add_argument("--loop-config", help="research loop config for the trial ledger (default: the runner's overlay)")
    parser.add_argument("--output", type=Path, help="where --grid/--cell/--gate-json write their JSON")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--null-replicates", type=int, default=200, help="--grid: replicates of the break-even null on the ladder model; --event-grid: of the matched and max-T nulls (0 skips)")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--tripwire", action="append", help="--clear-audit: defect tripwire cleared (repeatable)")
    parser.add_argument("--note", help="--clear-audit / --clear-falsifier: what the audit found")
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
            _warn_build(report)
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
        elif args.event_grid:
            campaign = None
            if args.campaign:
                campaign = next((c for c in load_campaigns(args.campaigns_dir) if c["id"] == args.campaign), None)
                if campaign is None:
                    parser.error("--event-grid: no campaign %s" % args.campaign)
            cells = args.cells.split(",") if args.cells else ([cell["cell_id"] for cell in campaign["cells"]] if campaign else ["V1..V5"])
            cache = band_lane.BandCache()
            cache.load_closes(start_ts, now_ts, now_ts, fetch=False)
            report = event_grid(connection, cache.closes, cells, campaign, args.spec_cut_ts, args.s_b, replicates=args.null_replicates, seed=args.seed)
            output = args.output or (TRUTH_DIR / ("event_grid_%s.json" % dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")))
            band_lane._atomic_write(output, json.dumps(report, indent=2, sort_keys=True) + "\n")
            print(event_grid_text(report))
            print("written %s" % output)
        elif args.register:
            cells = args.cells.split(",") if args.cells else None
            closes = None
            if cells:
                cache = band_lane.BandCache()
                cache.load_closes(start_ts, now_ts, now_ts, fetch=False)
                closes = cache.closes
            report = register(connection, args.register, args.campaigns_dir, now_ts, cells, closes)
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
        elif args.clear_falsifier:
            campaigns = [c for c in load_campaigns(args.campaigns_dir) if c.get("status") == "active"]
            if args.campaign:
                campaigns = [c for c in campaigns if c["id"] == args.campaign]
            if len(campaigns) != 1:
                parser.error("--clear-falsifier needs exactly one active campaign (use --campaign)")
            if not args.note:
                parser.error("--clear-falsifier needs --note")
            print(json.dumps(clear_falsifier(connection, args.campaigns_dir, campaigns[0]["id"], args.clear_falsifier, args.note, now_ts), indent=2, sort_keys=True))
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
        elif args.cell_gate_json:
            artifact = discovery_gate_artifact(connection, args.cell_gate_json)
            output = args.output or (TRUTH_DIR / "gates" / ("discovery_%s.json" % artifact["cell_id"]))
            band_lane._atomic_write(output, json.dumps(artifact, indent=1, sort_keys=True) + "\n")
            print(json.dumps({key: artifact[key] for key in ("verdict", "discovery_twin", "support", "wins", "wilson_lo", "avg_break_even", "tripwires", "warnings")}, indent=2, sort_keys=True))
            print("written %s" % output)
        elif args.tick:
            report = tick(connection, band_lane.BandCache(), start_ts, now_ts, loop_config, session_dirs, args.campaigns_dir)
            _warn_build(report["build"])
            for campaign in report["campaigns"]:
                if campaign.get("stale_evaluator"):
                    print("WARNING: %s" % campaign["stale_evaluator"], file=sys.stderr)
            print(json.dumps(report, indent=2, sort_keys=True))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
