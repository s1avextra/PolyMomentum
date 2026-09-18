#!/usr/bin/env python3
"""Factory KPI: funnel per lane x proposal source, sampler throughput, the
LLM-versus-uniform verdict (with the reviewer-rejected subset of the LLM arm)
and, for the band lane, the e-BH family decision at the pre-registered
campaign_n (the `promote` column counts e-BH discoveries, not raw promote
verdicts).  The verdict scores each arm over distinct stage-1 projections of
its rules, not over rows, so execution-only variants of one rule count once.

Read-only over one state dir: the research ledger (hypotheses incl. source and
review, evidence_accrual, cycles), trial_ledger.jsonl, the evidence artifacts
and the lane config (the overlay loop-config.local.json in the state dir, else
deploy/strategy-research-loop.json).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set

ROOT = Path(__file__).resolve().parents[1]
_LOOP_SPEC = importlib.util.spec_from_file_location(
    "strategy_research_loop", Path(__file__).resolve().parent / "strategy_research_loop.py"
)
assert _LOOP_SPEC and _LOOP_SPEC.loader
loop = importlib.util.module_from_spec(_LOOP_SPEC)
_LOOP_SPEC.loader.exec_module(loop)

DEFAULT_STATE_DIR = ROOT / "logs/strategy-research"
LANES = ("late_window_mechanisms", "band_mechanisms")
# Rows written before proposal_source was persisted.
LEGACY_SOURCE = "legacy"
STAGE_1_REJECTS = {
    "rejected_stage_1",
    "rejected_stage_1_fresh_capacity",
    "rejected_signal_screen",
}
# (trial-ledger stage, verdict) pairs that mean the stage-2 screen passed.
STAGE_2_PASSES = {("economic_opportunity_screen", "passed")}
# Band stage-2 verdicts supersede each other per candidate in ledger order:
# a rescreen (band_entry_economics again) or a rescore on the paginated tape
# (band_rescore_paginated) withdraws an earlier pass.
BAND_STAGE_2_STAGES = {"band_entry_economics", "band_rescore_paginated"}
BAND_STAGE_2_PASS_VERDICTS = {"stage_2_survivor", "manual_audit"}
ACCRUAL_BUCKETS = {"continue": "accruing", "promote": "promote", "kill": "killed"}
BAND_LANE = "band_mechanisms"
# The LLM arm of the retired late lane: llm samples plus burst-queue replays
# (kept so its historic rows still read as one arm).
LATE_LLM_ARM_SOURCES = ("llm", "burst_queue")
# The late-lane stage-1 screen (evaluate_late_rule / causal_late_signal) read
# only these rule fields; the entry cap, sigma buffer and book pressure are
# execution variants that reproduce the same public verdict.
LATE_STAGE_1_FIELDS = (
    "operator",
    "path_minutes",
    "minimum_two_minute_move_usd",
    "minimum_decision_buffer_usd",
    "direction",
)
BURST_FIELDS = ("generated", "invalid", "duplicate", "novelty_rejected", "survivors")
VERDICT_MINIMUM_N = 25
SOURCE_COLUMNS = (
    "proposals",
    "stage_1_survivors",
    "stage_1_rate",
    "stage_2_survivors",
    "accruing",
    "promote",
    "killed",
    "mean_stage_1_accuracy",
    "distinct_rules",
)
SOURCE_HEADERS = ("n", "s1", "s1_rate", "s2", "accruing", "promote", "killed", "s1_acc", "distinct")


def _columns(connection: sqlite3.Connection, table: str) -> Set[str]:
    return {str(row["name"]) for row in connection.execute("PRAGMA table_info(%s)" % table)}


def load_hypotheses(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
    has_source = "source" in _columns(connection, "hypotheses")
    rows: List[Dict[str, Any]] = []
    for row in connection.execute(
        "SELECT fingerprint, lane, status, proposal_json, review_json, evidence_path%s "
        "FROM hypotheses ORDER BY created_at, rowid" % (", source" if has_source else "")
    ):
        if str(row["lane"]) not in LANES:
            continue
        try:
            proposal = json.loads(row["proposal_json"])
            review = json.loads(row["review_json"]) if row["review_json"] else None
        except (TypeError, ValueError):
            continue
        rows.append(
            {
                "fingerprint": str(row["fingerprint"]),
                "lane": str(row["lane"]),
                "status": str(row["status"]),
                "proposal": proposal,
                "review": review if isinstance(review, dict) else None,
                "evidence_path": row["evidence_path"],
                "source": (row["source"] if has_source else None) or LEGACY_SOURCE,
            }
        )
    return rows


def load_accrual(connection: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    if "verdict" not in _columns(connection, "evidence_accrual"):
        return {}
    return {
        str(row["fingerprint"]): {
            "verdict": str(row["verdict"]),
            "e_value": float(row["e_value"]),
            "n": int(row["n"]),
            "wins": int(row["wins"]),
        }
        for row in connection.execute("SELECT fingerprint, verdict, e_value, n, wins FROM evidence_accrual")
    }


def lane_config(state_dir: Path) -> Dict[str, Any]:
    for path in (state_dir / "loop-config.local.json", ROOT / "deploy/strategy-research-loop.json"):
        if path.is_file():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except ValueError:
                continue
    return {}


def band_family(
    rows: Sequence[Mapping[str, Any]], accrual: Mapping[str, Mapping[str, Any]], config: Mapping[str, Any]
) -> Dict[str, Any]:
    """e-BH over the band candidates the lane may still promote, sized by
    every band candidate that ever started an e-process: the same decision
    accrue_band_hypotheses makes each cycle."""
    lane = (config.get("lanes") or {}).get(BAND_LANE) or {}
    eligible = {}
    family_size = 0
    for row in rows:
        state = accrual.get(row["fingerprint"])
        if state is None:
            continue
        family_size += 1
        if row["status"] not in loop.band_lane.ACCRUAL_STATUSES:
            continue
        if loop.band_lane.promotable(
            lane, row["fingerprint"], row["status"], state["verdict"], state["wins"], state["n"]
        ):
            eligible[row["fingerprint"]] = state["e_value"]
    family = loop.evidence_accrual.e_bh(eligible, loop.band_lane.campaign_n(config), family_size=family_size)
    family["raw_promote_verdicts"] = sum(
        1 for row in rows if (accrual.get(row["fingerprint"]) or {}).get("verdict") == "promote"
    )
    family["manual_audit"] = sum(1 for row in rows if row["status"] == "manual_audit")
    return family


def load_bursts(connection: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    bursts: Dict[str, List[Dict[str, Any]]] = {lane: [] for lane in LANES}
    for row in connection.execute("SELECT details_json FROM cycles"):
        try:
            details = json.loads(row["details_json"])
        except (TypeError, ValueError):
            continue
        lane_result = details.get("lane_result") or {}
        burst = lane_result.get("burst") or (lane_result.get("llm") or {}).get("burst")
        if details.get("lane") in bursts and isinstance(burst, dict):
            bursts[str(details["lane"])].append(burst)
    return bursts


def load_stage_2_passes(path: Path) -> Set[str]:
    """Candidates whose newest stage-2 verdict is a pass.  The ledger is
    append-only in time order, so for the band lane the last band stage-2
    row per candidate stands."""
    passes: Set[str] = set()
    if not path.is_file():
        return passes
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        stage, verdict = record.get("stage"), record.get("verdict")
        candidate = str(record.get("candidate"))
        if stage in BAND_STAGE_2_STAGES:
            if verdict in BAND_STAGE_2_PASS_VERDICTS:
                passes.add(candidate)
            else:
                passes.discard(candidate)
        elif (stage, verdict) in STAGE_2_PASSES:
            passes.add(candidate)
    return passes


def stage_1_accuracy(lane: str, evidence_path: Optional[str]) -> Optional[float]:
    if not evidence_path:
        return None
    path = Path(evidence_path)
    if not path.is_absolute():
        path = ROOT / path
    try:
        evidence = json.loads(path.read_text(encoding="utf-8"))
        block = evidence["stage_1"] if lane == "band_mechanisms" else evidence
        return float(block["overall"]["accuracy"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def rule_key(lane: str, proposal: Mapping[str, Any]) -> Optional[str]:
    try:
        return loop.canonical_json(proposal["rule"])
    except (KeyError, TypeError, ValueError):
        return None


def projection_key(lane: str, proposal: Mapping[str, Any]) -> Optional[str]:
    """Stage-1 projection: the late rule restricted to the fields the public
    screen read (rows were normalized on insert); every field of a band rule
    reaches its stage 1."""
    try:
        rule = proposal["rule"]
        if lane == "late_window_mechanisms":
            rule = {field: value for field, value in rule.items() if field in LATE_STAGE_1_FIELDS}
        return loop.canonical_json(rule)
    except (KeyError, TypeError, ValueError):
        return None


def _stage_1_survivors(rows: Sequence[Mapping[str, Any]]) -> int:
    return sum(1 for row in rows if row["status"] not in STAGE_1_REJECTS)


def source_metrics(
    rows: Sequence[Mapping[str, Any]],
    accrual: Mapping[str, Mapping[str, Any]],
    stage_2_passes: Set[str],
    discoveries: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    survivors = _stage_1_survivors(rows)
    # The reviewer is advisory (a reject never swaps the proposal), so its
    # rejects stay inside their arm; this subset shows what a gate would cut.
    reviewer_rejected = [row for row in rows if (row["review"] or {}).get("verdict") == "reject"]
    accuracies = [row["accuracy"] for row in rows if row["accuracy"] is not None]
    buckets = {"accruing": 0, "promote": 0, "killed": 0}
    for row in rows:
        verdict = (accrual.get(row["fingerprint"]) or {}).get("verdict", "")
        bucket = ACCRUAL_BUCKETS.get(verdict)
        # With a family decision, only an e-BH discovery is a promote.
        if discoveries is not None and bucket == "promote" and row["fingerprint"] not in discoveries:
            bucket = "accruing"
        if bucket:
            buckets[bucket] += 1
    return {
        "proposals": len(rows),
        "stage_1_survivors": survivors,
        "stage_1_rate": survivors / len(rows) if rows else None,
        "stage_2_survivors": sum(1 for row in rows if row["fingerprint"] in stage_2_passes),
        **buckets,
        "mean_stage_1_accuracy": sum(accuracies) / len(accuracies) if accuracies else None,
        "distinct_rules": len({row["rule_key"] for row in rows if row["rule_key"]}),
        "reviewer_rejected": {
            "proposals": len(reviewer_rejected),
            "stage_1_survivors": _stage_1_survivors(reviewer_rejected),
        },
    }


def throughput(bursts: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    totals = {field: sum(int(burst.get(field) or 0) for burst in bursts) for field in BURST_FIELDS}
    return {
        "cycles": len(bursts),
        **totals,
        "survivors_per_sample": (
            totals["survivors"] / totals["generated"] if totals["generated"] else None
        ),
    }


def sampler_verdict(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """LLM arm (llm + burst_queue replays) against the uniform control arm.

    Survival is counted over distinct stage-1 projections per arm: the LLM
    arm is deduplicated on the full rule, so re-proposing execution-only
    variants of one known survivor must not bank extra survivors."""
    arms = {
        "llm": [row for row in rows if row["source"] in LATE_LLM_ARM_SOURCES],
        "uniform_control": [row for row in rows if row["source"] == "uniform_control"],
    }
    counts = {}
    for arm, items in arms.items():
        keyed = [row for row in items if row["projection_key"]]
        counts[arm] = {
            "proposals": len(items),
            "projections": len({row["projection_key"] for row in keyed}),
            "stage_1_projections": len(
                {row["projection_key"] for row in keyed if row["status"] not in STAGE_1_REJECTS}
            ),
        }
    rejected = [row for row in arms["llm"] if (row["review"] or {}).get("verdict") == "reject"]
    reviewer_rejected = {
        "proposals": len(rejected),
        "stage_1_survivors": _stage_1_survivors(rejected),
    }
    llm, uniform = counts["llm"], counts["uniform_control"]
    if llm["projections"] < VERDICT_MINIMUM_N or uniform["projections"] < VERDICT_MINIMUM_N:
        text = "insufficient (llm=%d, uniform=%d)" % (llm["projections"], uniform["projections"])
    elif (
        llm["stage_1_projections"] / llm["projections"]
        <= uniform["stage_1_projections"] / uniform["projections"]
    ):
        text = "demote LLM to reviewer role"
    else:
        text = "LLM beats uniform"
    return {"text": text, **counts, "reviewer_rejected": reviewer_rejected}


def build_report(state_dir: Path) -> Dict[str, Any]:
    report: Dict[str, Any] = {"state_dir": str(state_dir), "lanes": {}}
    database = state_dir / "research.sqlite3"
    if not database.is_file():
        return report
    connection = sqlite3.connect("file:%s?mode=ro" % database, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        hypotheses = load_hypotheses(connection)
        accrual = load_accrual(connection)
        bursts = load_bursts(connection)
    finally:
        connection.close()
    stage_2_passes = load_stage_2_passes(state_dir / "trial_ledger.jsonl")
    for row in hypotheses:
        row["accuracy"] = stage_1_accuracy(row["lane"], row["evidence_path"])
        row["rule_key"] = rule_key(row["lane"], row["proposal"])
        row["projection_key"] = projection_key(row["lane"], row["proposal"])
    config = lane_config(state_dir)
    for lane in LANES:
        lane_rows = [row for row in hypotheses if row["lane"] == lane]
        family = band_family(lane_rows, accrual, config) if lane == BAND_LANE else None
        discoveries = set(family["promoted"]) if family else None
        sources = {
            source: source_metrics(
                [row for row in lane_rows if row["source"] == source], accrual, stage_2_passes, discoveries
            )
            for source in sorted({row["source"] for row in lane_rows})
        }
        report["lanes"][lane] = {
            "sources": sources,
            "throughput": throughput(bursts[lane]),
            "verdict": sampler_verdict(lane_rows),
        }
        if family:
            report["lanes"][lane]["e_bh"] = family
    return report


def _cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return "%.3f" % value
    return str(value)


def render(report: Mapping[str, Any]) -> str:
    lines = ["factory KPI  state_dir=%s" % report["state_dir"]]
    row_format = "%-24s %-16s" + " %9s" * len(SOURCE_HEADERS)
    lines.append(row_format % (("lane", "source") + SOURCE_HEADERS))
    for lane, block in report["lanes"].items():
        for source, metrics in block["sources"].items():
            lines.append(
                row_format % ((lane, source) + tuple(_cell(metrics[key]) for key in SOURCE_COLUMNS))
            )
    lines.append("")
    lines.append("sampler throughput (burst stats from cycles' lane_result)")
    for lane, block in report["lanes"].items():
        stats = block["throughput"]
        lines.append(
            "%-24s cycles=%d %s survivors_per_sample=%s"
            % (
                lane,
                stats["cycles"],
                " ".join("%s=%d" % (field, stats[field]) for field in BURST_FIELDS),
                _cell(stats["survivors_per_sample"]),
            )
        )
    lines.append("")
    for lane, block in report["lanes"].items():
        verdict = block["verdict"]
        lines.append(
            "SAMPLER VERDICT: %s  lane=%s llm_s1_projections=%d/%d "
            "uniform_s1_projections=%d/%d reviewer_rejected_s1=%d/%d"
            % (
                verdict["text"],
                lane,
                verdict["llm"]["stage_1_projections"],
                verdict["llm"]["projections"],
                verdict["uniform_control"]["stage_1_projections"],
                verdict["uniform_control"]["projections"],
                verdict["reviewer_rejected"]["stage_1_survivors"],
                verdict["reviewer_rejected"]["proposals"],
            )
        )
    for lane, block in report["lanes"].items():
        family = block.get("e_bh")
        if not family:
            continue
        lines.append(
            "E-BH FAMILY: lane=%s campaign_n=%s alpha=%s candidates=%d family=%d k_star=%d threshold=%s "
            "promote=%d raw_promote_verdicts=%d manual_audit=%d%s"
            % (
                lane,
                _cell(family["campaign_n"]),
                family["alpha"],
                family["candidates"],
                family["family"],
                family["k_star"],
                _cell(family["threshold"]),
                len(family["promoted"]),
                family["raw_promote_verdicts"],
                family["manual_audit"],
                " OVERFLOW: the family exceeds the pre-registered N" if family["overflow"] else "",
            )
        )
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    parser.add_argument("--json", action="store_true", help="machine-readable report")
    args = parser.parse_args(argv)
    report = build_report(args.state_dir.resolve())
    print(json.dumps(report, indent=2, sort_keys=True) if args.json else render(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
