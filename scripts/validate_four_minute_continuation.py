#!/usr/bin/env python3
"""Validate the four-minute continuation preregistration and public evidence."""

from __future__ import annotations

import gzip
import hashlib
import json
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "deploy/promotions/evidence/strategy_registry"
PREREGISTRATION = REGISTRY / "20260722_four_minute_continuation_preregistration.json"
EVIDENCE = REGISTRY / "20260722_four_minute_continuation_public_proxy.json"


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def close(actual: float, expected: float, tolerance: float = 1e-12) -> None:
    if not math.isclose(actual, expected, rel_tol=tolerance, abs_tol=tolerance):
        raise AssertionError(f"{actual} != {expected}")


def main() -> None:
    preregistration = json.loads(PREREGISTRATION.read_text())
    evidence = json.loads(EVIDENCE.read_text())
    checks = 0

    def check(condition: bool) -> None:
        nonlocal checks
        assert condition
        checks += 1

    check(preregistration["mechanism_id"] == "four_minute_continuation_v1")
    check(
        preregistration["status"]
        == "PREREGISTERED_BEFORE_FOUR_MINUTE_LABEL_EVALUATION"
    )
    check(preregistration["frozen_signal"]["checkpoint_offsets_seconds"] == [0, 60, 120, 180, 240])
    check(preregistration["frozen_signal"]["decision_offset_seconds"] == 240)
    check(preregistration["frozen_signal"]["coefficient_or_threshold_grid"] is False)
    check(
        evidence["authority"]["preregistration_sha256"]
        == sha256(PREREGISTRATION.read_bytes())
    )
    for pin in preregistration["source_pins_before_evaluation"].values():
        check(sha256((ROOT / pin["path"]).read_bytes()) == pin["sha256"])

    snapshot_meta = evidence["authority"]["signal_snapshot"]
    compressed = (ROOT / snapshot_meta["path"]).read_bytes()
    uncompressed = gzip.decompress(compressed)
    check(sha256(compressed) == snapshot_meta["sha256"])
    check(sha256(uncompressed) == snapshot_meta["uncompressed_sha256"])
    rows = [json.loads(line) for line in uncompressed.splitlines()]
    check(len(rows) == snapshot_meta["rows"] == 1032)
    check(max(row["window_start"] for row in rows) < 1_783_987_200)
    check(len(evidence["authority"]["archive_manifest"]) == 33)
    check(
        all(
            "2026-07-14" not in item["archive"]
            and "2026-07-15" not in item["archive"]
            for item in evidence["authority"]["archive_manifest"]
        )
    )

    wins = 0
    direction_counts = {"up": 0, "down": 0}
    direction_wins = {"up": 0, "down": 0}
    fifth_continuations = 0
    wins_despite_reversal = 0
    for row in rows:
        returns = [row[f"r{index}_usd"] for index in range(1, 5)]
        direction = row["signal_direction"]
        check(direction in direction_counts)
        check(all(value > 0 for value in returns) if direction == "up" else all(value < 0 for value in returns))
        terminal_direction = "up" if row["terminal"] > row["p0"] else "down"
        check(row["terminal_direction"] == terminal_direction)
        won = direction == terminal_direction
        check(row["won"] is won)
        fifth_continued = (
            row["terminal"] > row["p240"]
            if direction == "up"
            else row["terminal"] < row["p240"]
        )
        check(row["fifth_minute_continued"] is fifth_continued)
        wins += int(won)
        direction_counts[direction] += 1
        direction_wins[direction] += int(won)
        fifth_continuations += int(fifth_continued)
        wins_despite_reversal += int(won and not fifth_continued)

    overall = evidence["results"]["overall"]
    check(overall["eligible_signals"] == len(rows))
    check(overall["wins"] == wins == 986)
    close(overall["accuracy"], wins / len(rows))
    checks += 1
    for direction in ("up", "down"):
        score = evidence["results"]["directions"][direction]
        check(score["eligible_signals"] == direction_counts[direction])
        check(score["wins"] == direction_wins[direction])
        close(score["accuracy"], direction_wins[direction] / direction_counts[direction])
        checks += 1

    decomposition = evidence["results"]["mechanism_decomposition"]
    check(decomposition["diagnostic_only"] is True)
    check(decomposition["true_fifth_minute_continuations"] == fifth_continuations)
    check(
        decomposition["contract_wins_despite_fifth_minute_reversal_or_tie"]
        == wins_despite_reversal
    )
    close(
        decomposition["true_fifth_minute_continuation_rate"],
        fifth_continuations / len(rows),
    )
    checks += 1
    close(
        decomposition[
            "fraction_of_contract_wins_despite_fifth_minute_reversal_or_tie"
        ],
        wins_despite_reversal / wins,
    )
    checks += 1

    check(
        evidence["status"]
        == "PUBLIC_DIRECTIONAL_PROXY_REJECTED_NO_RETUNING"
    )
    check(
        evidence["gate_evaluation"]["failed_checks"]
        == ["minimum_eligible_signals_fresh"]
    )
    check(evidence["gate_evaluation"]["passed"] is False)
    check(
        evidence["gate_evaluation"]["claim_replication_passed_report_only"]
        is True
    )
    check(evidence["economic_audit"]["fixed_breakeven_is_valid"] is False)
    check(evidence["economic_audit"]["profitability_established"] is False)
    check(evidence["decision"]["default_off_feature_authorized"] is False)
    check(evidence["decision"]["exact_polymarket_replay_authorized"] is False)
    check(evidence["decision"]["runtime_change_authorized"] is False)
    check(evidence["decision"]["paper_or_live_trading_authorized"] is False)
    check(evidence["decision"]["profitability_claim"] is False)
    check(evidence["decision"]["a_plus_claim"] is False)
    check(evidence["integrity"]["active_binary_complement_outcomes_accessed"] is False)
    check(evidence["integrity"]["july_14_or_later_archive_opened"] is False)

    print(
        json.dumps(
            {
                "ok": True,
                "checks": checks,
                "conditions": evidence["results"]["overall"]["conditions"],
                "eligible_signals": len(rows),
                "directional_accuracy": overall["accuracy"],
                "true_fifth_minute_continuation_rate": decomposition[
                    "true_fifth_minute_continuation_rate"
                ],
                "public_proxy_passed": evidence["gate_evaluation"]["passed"],
                "runtime_authorized": evidence["decision"][
                    "runtime_change_authorized"
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
