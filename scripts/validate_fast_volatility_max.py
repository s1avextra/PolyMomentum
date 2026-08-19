#!/usr/bin/env python3
"""Independently validate the fast-volatility maximum research screen."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import zipfile
from collections import defaultdict
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "deploy/promotions/evidence/strategy_registry"
PREREGISTRATION = REGISTRY / "20260721_fast_volatility_max_preregistration.json"
EVIDENCE = REGISTRY / "20260721_fast_volatility_max_public_calibration.json"
AMENDMENT = REGISTRY / "20260721_fast_volatility_max_integrity_amendment.json"
SNAPSHOT = (
    REGISTRY / "source_snapshots/20260721_fast_volatility_max_forecasts.jsonl.gz"
)


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def close(actual: float, expected: float, tolerance: float = 1e-12) -> None:
    if not math.isclose(actual, expected, rel_tol=tolerance, abs_tol=tolerance):
        raise AssertionError(f"{actual} != {expected}")


def engine_norm_cdf(value: float) -> float:
    x = value / math.sqrt(2.0)
    sign = 1.0 if x >= 0 else -1.0
    x = abs(x)
    t = 1.0 / (1.0 + 0.3275911 * x)
    polynomial = (
        (
            (
                (
                    (1.061405429 * t - 1.453152027) * t + 1.421413741
                )
                * t
                - 0.284496736
            )
            * t
            + 0.254829592
        )
        * t
    )
    erf = sign * (1.0 - polynomial * math.exp(-x * x))
    return 0.5 * (1.0 + erf)


def fair_probability(row: dict, volatility_key: str) -> float:
    volatility = row[volatility_key]
    years = row["remaining_seconds"] / (365.25 * 86_400.0)
    d2 = (
        math.log(row["spot"] / row["strike"])
        + (0.05 - 0.5 * volatility * volatility) * years
    ) / (volatility * math.sqrt(years))
    return min(0.99, max(0.01, engine_norm_cdf(d2)))


def audit_archives(archive_dir: Path, manifest: list[dict]) -> dict:
    expected = {item["archive"]: item for item in manifest}
    archive_paths = sorted(archive_dir.glob("BTCUSDT-1s-*.zip"))
    assert len(archive_paths) == 31
    previous_open_time = None
    rows = 0
    gaps = 0
    duplicates = 0
    regressions = 0
    invalid_durations = 0
    invalid_prices = 0
    schema_widths: set[int] = set()
    for archive_path in archive_paths:
        item = expected[archive_path.name]
        payload = archive_path.read_bytes()
        assert sha256(payload) == item["archive_sha256"]
        checksum_path = archive_path.with_suffix(archive_path.suffix + ".CHECKSUM")
        assert sha256(checksum_path.read_bytes()) == item["adjacent_checksum_sha256"]
        checksum_fields = checksum_path.read_text().strip().split()
        assert checksum_fields == [item["archive_sha256"], archive_path.name]
        archive_rows = 0
        with zipfile.ZipFile(archive_path) as archive:
            assert len(archive.namelist()) == 1
            with archive.open(archive.namelist()[0]) as raw:
                reader = csv.reader((line.decode("utf-8") for line in raw))
                for row in reader:
                    schema_widths.add(len(row))
                    open_time = int(row[0])
                    close_time = int(row[6])
                    if previous_open_time is not None:
                        delta = open_time - previous_open_time
                        gaps += delta != 1_000_000
                        duplicates += delta == 0
                        regressions += delta < 0
                    invalid_durations += close_time - open_time != 999_999
                    invalid_prices += not (
                        math.isfinite(float(row[1]))
                        and float(row[1]) > 0
                        and math.isfinite(float(row[4]))
                        and float(row[4]) > 0
                    )
                    previous_open_time = open_time
                    archive_rows += 1
        assert archive_rows == item["rows"] == 86_400
        rows += archive_rows
    return {
        "archives": len(archive_paths),
        "rows": rows,
        "schema_widths": sorted(schema_widths),
        "gaps": gaps,
        "duplicates": duplicates,
        "regressions": regressions,
        "invalid_durations": invalid_durations,
        "invalid_prices": invalid_prices,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-dir", type=Path, required=True)
    args = parser.parse_args()

    preregistration = load_json(PREREGISTRATION)
    evidence = load_json(EVIDENCE)
    amendment = load_json(AMENDMENT)
    compressed_snapshot = SNAPSHOT.read_bytes()
    uncompressed_snapshot = gzip.decompress(compressed_snapshot)
    rows = [json.loads(line) for line in uncompressed_snapshot.splitlines()]

    assert preregistration["status"] == (
        "PREREGISTERED_BEFORE_PUBLIC_WINDOW_DOWNLOAD_OR_LABEL_INSPECTION"
    )
    for item in preregistration["source_pins_before_evaluation"].values():
        assert sha256((ROOT / item["path"]).read_bytes()) == item["sha256"]
    authority = evidence["authority"]
    assert authority["preregistration_path"] == str(PREREGISTRATION.relative_to(ROOT))
    assert sha256(PREREGISTRATION.read_bytes()) == authority["preregistration_sha256"]
    snapshot_authority = authority["forecast_snapshot"]
    assert snapshot_authority["path"] == str(SNAPSHOT.relative_to(ROOT))
    assert sha256(compressed_snapshot) == snapshot_authority["sha256"]
    assert sha256(uncompressed_snapshot) == snapshot_authority["uncompressed_sha256"]
    assert len(rows) == snapshot_authority["rows"] == 25_899

    archive_audit = audit_archives(args.archive_dir, authority["archive_manifest"])
    assert archive_audit == {
        "archives": 31,
        "rows": 2_678_400,
        "schema_widths": [12],
        "gaps": 0,
        "duplicates": 0,
        "regressions": 0,
        "invalid_durations": 0,
        "invalid_prices": 0,
    }

    condition_offsets = {
        (row["window_start"], row["elapsed_seconds"]) for row in rows
    }
    assert len(condition_offsets) == len(rows)
    conditions = {row["window_start"] for row in rows}
    assert len(conditions) == 8_633
    assert {row["elapsed_seconds"] for row in rows} == {120, 150, 179}
    assert all(
        sum(candidate["window_start"] == condition for candidate in rows) == 3
        for condition in list(conditions)[:10]
    )

    score_sums = defaultdict(float)
    daily = defaultdict(lambda: defaultdict(float))
    more_confident = 0
    maximum_confidence_increase = 0.0
    for row in rows:
        baseline_probability = fair_probability(row, "baseline_volatility")
        candidate_probability = fair_probability(row, "candidate_volatility")
        close(baseline_probability, row["baseline_probability"])
        close(candidate_probability, row["candidate_probability"])
        outcome = float(row["terminal_up"])
        baseline_brier = (baseline_probability - outcome) ** 2
        candidate_brier = (candidate_probability - outcome) ** 2
        baseline_log_loss = -(
            outcome * math.log(baseline_probability)
            + (1 - outcome) * math.log(1 - baseline_probability)
        )
        candidate_log_loss = -(
            outcome * math.log(candidate_probability)
            + (1 - outcome) * math.log(1 - candidate_probability)
        )
        brier_improvement = baseline_brier - candidate_brier
        log_loss_improvement = baseline_log_loss - candidate_log_loss
        close(brier_improvement, row["brier_improvement"])
        close(log_loss_improvement, row["log_loss_improvement"])
        score_sums["brier"] += brier_improvement
        score_sums["log_loss"] += log_loss_improvement
        daily[row["utc_day"]]["brier"] += brier_improvement
        daily[row["utc_day"]]["log_loss"] += log_loss_improvement
        daily[row["utc_day"]]["count"] += 1
        confidence_increase = abs(candidate_probability - 0.5) - abs(
            baseline_probability - 0.5
        )
        more_confident += confidence_increase > 1e-15
        maximum_confidence_increase = max(
            maximum_confidence_increase, confidence_increase
        )

    brier_improvement = score_sums["brier"] / len(rows)
    log_loss_improvement = score_sums["log_loss"] / len(rows)
    close(brier_improvement, evidence["results"]["overall"]["brier_improvement"])
    close(
        log_loss_improvement,
        evidence["results"]["overall"]["log_loss_improvement"],
    )
    assert brier_improvement < preregistration["fixed_pass_gates"]["overall"][
        "minimum_brier_improvement"
    ]
    assert log_loss_improvement >= preregistration["fixed_pass_gates"]["overall"][
        "minimum_log_loss_improvement"
    ]

    days = sorted(daily)
    daily_brier = np.array([daily[day]["brier"] for day in days])
    daily_log_loss = np.array([daily[day]["log_loss"] for day in days])
    daily_count = np.array([daily[day]["count"] for day in days])
    rng = np.random.default_rng(20_260_721)
    samples = rng.integers(0, len(days), size=(10_000, len(days)))
    counts = daily_count[samples].sum(axis=1)
    brier_bootstrap = daily_brier[samples].sum(axis=1) / counts
    log_loss_bootstrap = daily_log_loss[samples].sum(axis=1) / counts
    bootstrap = evidence["results"]["bootstrap"]
    for actual, expected in zip(
        np.quantile(brier_bootstrap, [0.025, 0.975]),
        bootstrap["brier_improvement_95pct"],
    ):
        close(float(actual), expected)
    for actual, expected in zip(
        np.quantile(log_loss_bootstrap, [0.025, 0.975]),
        bootstrap["log_loss_improvement_95pct"],
    ):
        close(float(actual), expected)

    assert evidence["status"] == "PUBLIC_PROXY_CALIBRATION_REJECTED_NO_RETUNING"
    assert evidence["gate_evaluation"]["failed_checks"] == [
        "overall_brier_improvement_at_least_0_0005"
    ]
    assert evidence["decision"]["strategy_variant_authorized"] is False
    assert evidence["decision"]["runtime_change_authorized"] is False
    assert evidence["decision"]["profitability_claim"] is False
    assert evidence["decision"]["a_plus_claim"] is False

    pins = amendment["source_pins"]
    assert sha256(PREREGISTRATION.read_bytes()) == pins["preregistration"]["sha256"]
    assert sha256((ROOT / pins["analysis_script"]["path"]).read_bytes()) == pins[
        "analysis_script"
    ]["sha256"]
    assert sha256(EVIDENCE.read_bytes()) == pins["final_evidence"]["sha256"]
    assert sha256(SNAPSHOT.read_bytes()) == pins["forecast_snapshot"]["sha256"]
    assert more_confident == amendment["correction"]["observed_scope"][
        "candidate_more_confident_forecasts"
    ]
    close(
        maximum_confidence_increase,
        amendment["correction"]["observed_scope"][
            "maximum_absolute_confidence_increase"
        ],
    )
    assert all(not value for value in amendment["frozen_contract"].values())
    assert amendment["reproducibility_correction"] == {
        "issue": "The first evidence write embedded the invocation-specific absolute or relative path used for repository-owned artifacts.",
        "resolution": "Repository-owned paths are now serialized relative to the repository root, while the external archive directory remains explicit.",
        "analysis_changed": False,
        "data_changed": False,
        "gate_result_changed": False,
        "cli_and_notebook_outputs_byte_identical": True,
    }
    assert amendment["decision"]["candidate_rejected"] is True
    assert amendment["decision"]["retuning_authorized"] is False

    print(
        json.dumps(
            {
                "ok": True,
                "archive_audit": archive_audit,
                "conditions": len(conditions),
                "forecasts": len(rows),
                "brier_improvement": brier_improvement,
                "log_loss_improvement": log_loss_improvement,
                "failed_checks": evidence["gate_evaluation"]["failed_checks"],
                "candidate_more_confident_forecasts": more_confident,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
