#!/usr/bin/env python3
"""Exploratory screen for the finite late-window rule family from the X article."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

from analyze_four_minute_continuation import (
    BOOTSTRAP_RESAMPLES,
    BOOTSTRAP_SEED,
    REPOSITORY_ROOT,
    build_windows,
    load_archives,
    portable_path,
    sha256_file,
    write_json_atomic,
)


REGISTRY = REPOSITORY_ROOT / "deploy/promotions/evidence/strategy_registry"
REGISTRATION = (
    REGISTRY / "20260722_late_window_article_family_exploratory_registration.json"
)
FUNNEL = REGISTRY / "20260722_strategy_validation_funnel_v2.json"


def sign_direction(values: pd.Series) -> pd.Series:
    return pd.Series(
        np.where(values > 0, "up", np.where(values < 0, "down", "tie")),
        index=values.index,
    )


def build_rule_frame(windows: pd.DataFrame, rule_id: str) -> pd.DataFrame:
    frame = windows.copy()
    r1 = frame["r1_usd"]
    r2 = frame["r2_usd"]
    r3 = frame["r3_usd"]
    r4 = frame["r4_usd"]
    move_2m = frame["p120"] - frame["p0"]
    move_4m = frame["p240"] - frame["p0"]
    path_3m_up = (r1 > 0) & (r2 > 0) & (r3 > 0)
    path_3m_down = (r1 < 0) & (r2 < 0) & (r3 < 0)
    path_4m_up = path_3m_up & (r4 > 0)
    path_4m_down = path_3m_down & (r4 < 0)
    path_3m = path_3m_up | path_3m_down
    path_4m = path_4m_up | path_4m_down
    direction_3m = pd.Series(np.where(path_3m_up, "up", "down"), index=frame.index)
    direction_4m = pd.Series(np.where(path_4m_up, "up", "down"), index=frame.index)
    direction_2m = sign_direction(move_2m)
    direction_4m_position = sign_direction(move_4m)

    if rule_id == "path_3m":
        eligible = path_3m
        direction = direction_3m
        decision_offset = 180
        decision_price = frame["p180"]
    elif rule_id == "path_4m":
        eligible = path_4m
        direction = direction_4m
        decision_offset = 240
        decision_price = frame["p240"]
    elif rule_id == "move_2m_100":
        eligible = move_2m.abs() >= 100.0
        direction = direction_2m
        decision_offset = 120
        decision_price = frame["p120"]
    elif rule_id == "move_2m_200":
        eligible = move_2m.abs() >= 200.0
        direction = direction_2m
        decision_offset = 120
        decision_price = frame["p120"]
    elif rule_id == "path_3m_and_move_2m_100":
        eligible = path_3m & (move_2m.abs() >= 100.0)
        direction = direction_3m
        decision_offset = 180
        decision_price = frame["p180"]
    elif rule_id == "path_4m_and_move_2m_100":
        eligible = path_4m & (move_2m.abs() >= 100.0)
        direction = direction_4m
        decision_offset = 240
        decision_price = frame["p240"]
    elif rule_id == "path_4m_and_move_2m_200":
        eligible = path_4m & (move_2m.abs() >= 200.0)
        direction = direction_4m
        decision_offset = 240
        decision_price = frame["p240"]
    elif rule_id == "path_4m_or_move_2m_200_aligned":
        aligned_move = (
            (move_2m.abs() >= 200.0)
            & (direction_2m == direction_4m_position)
            & (direction_4m_position != "tie")
        )
        eligible = path_4m | aligned_move
        direction = direction_4m_position
        decision_offset = 240
        decision_price = frame["p240"]
    else:
        raise ValueError(f"unknown rule: {rule_id}")

    eligible &= direction != "tie"
    frame["rule_id"] = rule_id
    frame["eligible"] = eligible
    frame["signal_direction"] = direction
    frame["decision_offset_seconds"] = decision_offset
    frame["decision_price"] = decision_price
    frame["decision_buffer_usd"] = np.where(
        direction == "up", decision_price - frame["p0"], frame["p0"] - decision_price
    )
    frame["won"] = direction == frame["terminal_direction"]
    frame["post_decision_return_usd"] = frame["terminal"] - decision_price
    frame["post_decision_continued"] = np.where(
        direction == "up",
        frame["post_decision_return_usd"] > 0,
        frame["post_decision_return_usd"] < 0,
    )
    return frame.loc[frame["eligible"] & ~frame["terminal_tie"]].copy()


def score(frame: pd.DataFrame) -> dict:
    wins = int(frame["won"].sum())
    signals = int(len(frame))
    return {
        "eligible_signals": signals,
        "wins": wins,
        "losses": signals - wins,
        "accuracy": float(wins / signals) if signals else None,
    }


def score_by(frame: pd.DataFrame, column: str) -> dict:
    return {str(key).lower(): score(group) for key, group in frame.groupby(column)}


def day_bootstrap(frame: pd.DataFrame, seed_offset: int) -> dict:
    daily = frame.groupby("utc_day", sort=True).agg(
        wins=("won", "sum"), signals=("won", "size")
    )
    if daily.empty:
        return {"days": 0, "accuracy_95pct": [None, None]}
    rng = np.random.default_rng(BOOTSTRAP_SEED + seed_offset)
    sampled = rng.integers(0, len(daily), size=(BOOTSTRAP_RESAMPLES, len(daily)))
    wins = daily["wins"].to_numpy()[sampled].sum(axis=1)
    signals = daily["signals"].to_numpy()[sampled].sum(axis=1)
    accuracy = wins / signals
    return {
        "unit": "UTC day",
        "days": int(len(daily)),
        "resamples": BOOTSTRAP_RESAMPLES,
        "seed": BOOTSTRAP_SEED + seed_offset,
        "accuracy_95pct": [
            float(value) for value in np.quantile(accuracy, [0.025, 0.975])
        ],
    }


def evaluate_rule(frame: pd.DataFrame, seed_offset: int) -> dict:
    total = score(frame)
    chronological = score_by(frame, "chronological_window")
    directions = score_by(frame, "signal_direction")
    bootstrap = day_bootstrap(frame, seed_offset)
    buffer = frame["decision_buffer_usd"]
    triage_checks = {
        "minimum_total_signals_30": total["eligible_signals"] >= 30,
        "accuracy_each_direction_above_0_55": bool(directions)
        and all(value["accuracy"] > 0.55 for value in directions.values()),
        "accuracy_each_chronological_window_above_0_55": bool(chronological)
        and all(value["accuracy"] > 0.55 for value in chronological.values()),
    }
    triage_warnings = {
        "fresh_signals_below_10": chronological.get("fresh_pre_forward", {}).get(
            "eligible_signals", 0
        )
        < 10
    }
    return {
        "decision_offset_seconds": int(frame["decision_offset_seconds"].iloc[0]),
        "overall": total,
        "chronological_windows": chronological,
        "directions": directions,
        "bootstrap": bootstrap,
        "decision_buffer_usd": {
            "mean": float(buffer.mean()),
            "median": float(buffer.median()),
            "p10": float(buffer.quantile(0.10)),
            "p90": float(buffer.quantile(0.90)),
        },
        "post_decision_continuation_rate": float(
            frame["post_decision_continued"].mean()
        ),
        "triage_checks": triage_checks,
        "triage_warnings": triage_warnings,
        "stage_1_survivor": all(triage_checks.values()),
    }


def write_snapshot(path: Path, frames: list[pd.DataFrame]) -> dict:
    combined = pd.concat(frames, ignore_index=True)
    columns = [
        "rule_id",
        "window_start",
        "utc_day",
        "utc_hour",
        "chronological_window",
        "decision_offset_seconds",
        "signal_direction",
        "p0",
        "p60",
        "p120",
        "p180",
        "p240",
        "terminal",
        "decision_price",
        "decision_buffer_usd",
        "terminal_direction",
        "post_decision_continued",
        "won",
    ]
    lines = [
        json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False)
        for record in combined[columns].to_dict(orient="records")
    ]
    raw = ("\n".join(lines) + "\n").encode()
    compressed = gzip.compress(raw, compresslevel=9, mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    temporary.write_bytes(compressed)
    os.replace(temporary, path)
    return {
        "path": portable_path(path),
        "rows": int(len(combined)),
        "sha256": hashlib.sha256(compressed).hexdigest(),
        "uncompressed_sha256": hashlib.sha256(raw).hexdigest(),
    }


def run(archive_dir: Path, evidence_path: Path, snapshot_path: Path) -> dict:
    data, archive_manifest, source_quality = load_archives(archive_dir)
    windows, window_quality = build_windows(data)
    registration = json.loads(REGISTRATION.read_text())
    rule_ids = [rule["id"] for rule in registration["frozen_rules"]]
    frames = [build_rule_frame(windows, rule_id) for rule_id in rule_ids]
    results = {
        rule_id: evaluate_rule(frame, index + 1)
        for index, (rule_id, frame) in enumerate(zip(rule_ids, frames, strict=True))
    }
    survivors = [
        rule_id for rule_id, result in results.items() if result["stage_1_survivor"]
    ]
    evidence = {
        "schema_version": 1,
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "mechanism_id": "late_window_article_family_v1",
        "status": "STAGE_1_EXPLORATORY_SCREEN_COMPLETE",
        "authority": {
            "registration_path": portable_path(REGISTRATION),
            "registration_sha256": sha256_file(REGISTRATION),
            "funnel_path": portable_path(FUNNEL),
            "funnel_sha256": sha256_file(FUNNEL),
            "archive_directory": str(archive_dir),
            "archive_manifest": archive_manifest,
        },
        "source_data_quality": source_quality,
        "window_data_quality": window_quality,
        "results": results,
        "stage_1": {
            "survivors": survivors,
            "survivor_count": len(survivors),
            "triage_not_promotion": True,
            "next_step": "Run one shared historical Polymarket opportunity capture for survivors, then shortlist at most two exact-L2 variants by fee-aware ask cushion and support.",
        },
        "article_claim_audit": {
            "reported_path_3m_accuracy": 0.8096,
            "observed_path_3m_accuracy": results["path_3m"]["overall"]["accuracy"],
            "reported_path_4m_up_accuracy": 0.9683,
            "reported_path_4m_down_accuracy": 0.9597,
            "reported_move_2m_100_accuracy": 0.8858,
            "observed_move_2m_100_accuracy": results["move_2m_100"]["overall"][
                "accuracy"
            ],
            "reported_move_2m_200_accuracy": 0.9276,
            "observed_move_2m_200_accuracy": results["move_2m_200"]["overall"][
                "accuracy"
            ],
            "reported_overall_strategy_accuracy": 0.7846,
            "overall_strategy_still_not_reproducible": True,
            "reason": "The article never specifies how overlapping rules are combined, priced, sized, or deduplicated.",
        },
        "snapshot": write_snapshot(snapshot_path, frames),
        "decision": {
            "historical_polymarket_opportunity_screen_authorized": bool(survivors),
            "maximum_exact_l2_shortlist": 2,
            "runtime_paper_or_live_change_authorized": False,
            "profitability_or_a_plus_claim": False,
        },
        "integrity": {
            "active_binary_complement_outcomes_accessed": False,
            "active_binary_complement_strategy_metrics_accessed": False,
            "july_14_or_later_archive_opened": False,
        },
    }
    write_json_atomic(evidence_path, evidence)
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    args = parser.parse_args()
    evidence = run(args.archive_dir, args.evidence, args.snapshot)
    print(
        json.dumps(
            {
                "status": evidence["status"],
                "survivors": evidence["stage_1"]["survivors"],
                "results": {
                    rule_id: {
                        "signals": result["overall"]["eligible_signals"],
                        "accuracy": result["overall"]["accuracy"],
                        "fresh_signals": result["chronological_windows"].get(
                            "fresh_pre_forward", {}
                        ).get("eligible_signals", 0),
                    }
                    for rule_id, result in evidence["results"].items()
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
