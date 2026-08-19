#!/usr/bin/env python3
"""Apply the frozen late-window holdout gate without introducing new thresholds."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, prefix=f"{path.name}.tmp.", delete=False) as output:
        json.dump(payload, output, indent=2, sort_keys=False)
        output.write("\n")
        temporary_path = Path(output.name)
    os.replace(temporary_path, path)


def normalized_timestamp(value: str) -> str:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()


def run(
    registration_path: Path,
    evidence_path: Path,
    raw_reports_archive: Path,
    output_path: Path,
) -> dict:
    registration = json.loads(registration_path.read_text())
    evidence = json.loads(evidence_path.read_text())
    candidate_id = registration["candidate_id"]
    result = evidence["results"][candidate_id]
    overall = result["overall"]
    gate_config = registration["progression_gate"]
    registered_variant = registration["frozen_candidate"]["variant"]
    observed_windows = [
        [normalized_timestamp(source["start"]), normalized_timestamp(source["end"])]
        for source in evidence["authority"]["trade_reports"]
    ]
    expected_windows = [
        [normalized_timestamp(start), normalized_timestamp(end)]
        for start, end in registration["holdout"]["windows_utc"]
    ]
    source_hashes_match = all(
        Path(source["path"]).is_file()
        and sha256(Path(source["path"])) == source["sha256"]
        for source in evidence["authority"]["trade_reports"]
    )
    integrity_checks = {
        "candidate_is_only_reported_result": list(evidence["results"]) == [candidate_id],
        "variant_path_matches_registration": evidence["authority"]["variant_path"]
        == registered_variant["path"],
        "variant_hash_matches_registration": evidence["authority"]["variant_sha256"]
        == registered_variant["sha256"],
        "all_source_hashes_recompute": source_hashes_match,
        "holdout_windows_match_registration": observed_windows == expected_windows,
        "all_sources_use_registered_latency": all(
            source["latency_ms"] == registration["frozen_candidate"]["latency_ms"]
            for source in evidence["authority"]["trade_reports"]
        ),
        "raw_reports_archive_present": raw_reports_archive.is_file(),
    }
    integrity_passed = all(integrity_checks.values())
    active_folds = [fold for fold in result["folds"] if fold["trades"] > 0]
    positive_active_fraction = (
        sum(fold["total_pnl_usd"] > 0 for fold in active_folds) / len(active_folds)
        if active_folds
        else 0.0
    )
    direction_checks = {
        direction: {
            "fills": values["trades"],
            "total_pnl_usd": values["total_pnl_usd"],
            "applicable": values["trades"] >= 5,
            "passed": values["trades"] < 5 or values["total_pnl_usd"] > 0,
        }
        for direction, values in result["by_direction"].items()
    }
    gates = {
        "minimum_fills": {
            "observed": overall["fills"],
            "required": gate_config["minimum_fills"],
            "passed": overall["fills"] >= gate_config["minimum_fills"],
        },
        "minimum_fill_rate": {
            "observed": overall["fill_rate"],
            "required": gate_config["minimum_fill_rate"],
            "passed": overall["fill_rate"] >= gate_config["minimum_fill_rate"],
        },
        "positive_total_net_pnl": {
            "observed_usd": overall["total_pnl_usd"],
            "passed": overall["total_pnl_usd"] > 0,
        },
        "positive_mean_net_pnl_per_fill": {
            "observed_usd": overall["mean_pnl_per_trade_usd"],
            "passed": overall["mean_pnl_per_trade_usd"] > 0,
        },
        "minimum_fraction_positive_active_folds": {
            "observed": positive_active_fraction,
            "required": gate_config["minimum_fraction_positive_active_folds"],
            "active_folds": len(active_folds),
            "passed": positive_active_fraction
            >= gate_config["minimum_fraction_positive_active_folds"],
        },
        "direction_rule": {
            "directions": direction_checks,
            "passed": all(check["passed"] for check in direction_checks.values()),
        },
        "maximum_unresolved_fills": {
            "observed": overall["unresolved_fills"],
            "required_maximum": 0,
            "passed": overall["unresolved_fills"] == 0,
        },
        "maximum_resolution_disagreements": {
            "observed": overall["resolution_disagreements"],
            "required_maximum": 0,
            "passed": overall["resolution_disagreements"] == 0,
        },
        "loss_robustness": {
            "observed_pnl_after_one_average_full_loss_usd": overall[
                "pnl_after_one_additional_average_loss_usd"
            ],
            "required": "greater than zero",
            "passed": overall["pnl_after_one_additional_average_loss_usd"] > 0,
        },
    }
    progression_passed = integrity_passed and all(gate["passed"] for gate in gates.values())
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mechanism_id": registration["mechanism_id"],
        "candidate_id": candidate_id,
        "status": "HOLDOUT_GATE_PASSED" if progression_passed else "HOLDOUT_GATE_FAILED",
        "authority": {
            "registration_path": str(registration_path),
            "registration_sha256": sha256(registration_path),
            "holdout_evidence_path": str(evidence_path),
            "holdout_evidence_sha256": sha256(evidence_path),
            "raw_reports_archive_path": str(raw_reports_archive),
            "raw_reports_archive_sha256": sha256(raw_reports_archive),
        },
        "integrity": {
            "checks": integrity_checks,
            "passed": integrity_passed,
        },
        "gates": gates,
        "decision": {
            "progression_passed": progression_passed,
            "bounded_forward_measurement_design_permitted": progression_passed,
            "paper_or_live_runtime_change_authorized": False,
            "profitability_or_a_plus_claim": False,
        },
        "interpretation": (
            "The literal price-insensitive candidate failed because its holdout profit does not survive "
            "one average observed-size full loss. Do not retune this holdout; register any price-aware "
            "or staged-entry successor as a new mechanism."
            if not progression_passed
            else "The candidate may advance only to a separately registered bounded forward measurement design."
        ),
    }
    write_json_atomic(output_path, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registration", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--raw-reports-archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.registration, args.evidence, args.raw_reports_archive, args.output)
    print(json.dumps({"status": result["status"], "gates": result["gates"], "decision": result["decision"]}, indent=2))


if __name__ == "__main__":
    main()
