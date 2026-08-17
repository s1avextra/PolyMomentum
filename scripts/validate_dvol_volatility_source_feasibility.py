#!/usr/bin/env python3
"""Validate the preregistered DVOL source-feasibility rejection."""

from __future__ import annotations

import bisect
import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "deploy/promotions/evidence/strategy_registry"
PREREGISTRATION = REGISTRY / "20260721_dvol_volatility_max_preregistration.json"
EVIDENCE = REGISTRY / "20260721_dvol_volatility_max_source_feasibility.json"
SNAPSHOT = REGISTRY / "source_snapshots/20260721_btcdvol_usdc_1y.json.gz"


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def main() -> None:
    preregistration = json.loads(PREREGISTRATION.read_text())
    evidence = json.loads(EVIDENCE.read_text())
    compressed = SNAPSHOT.read_bytes()
    uncompressed = gzip.decompress(compressed)
    response = json.loads(uncompressed)
    pins = evidence["authority"]["raw_response_snapshot"]
    assert sha256(compressed) == pins["compressed_sha256"]
    assert sha256(uncompressed) == pins["uncompressed_sha256"]
    assert len(uncompressed) == pins["uncompressed_bytes"]
    assert sha256(PREREGISTRATION.read_bytes()) == evidence["authority"][
        "preregistration_sha256"
    ]
    assert preregistration["status"] == (
        "PREREGISTERED_BEFORE_DVOL_OR_EVALUATION_LABEL_DOWNLOAD"
    )
    assert preregistration["frozen_formula"]["dvol_maximum_age_seconds"] == 7_200
    assert preregistration["fixed_pass_gates"]["data_quality"][
        "minimum_fresh_dvol_forecast_fraction"
    ] == 0.99
    points = response["result"]
    times = [int(row[0] // 1000) for row in points]
    values = [float(row[1]) for row in points]
    intervals = [right - left for left, right in zip(times, times[1:])]
    assert response["jsonrpc"] == "2.0"
    assert len(points) == 1_460
    assert len(times) == len(set(times))
    assert times == sorted(times)
    assert all(value > 0 for value in values)
    assert min(intervals) == max(intervals) == 21_600

    periods = [
        ("2026-04-16T00:00:00Z", "2026-05-16T00:00:00Z"),
        ("2026-07-16T00:00:00Z", "2026-07-21T00:00:00Z"),
    ]
    ages: list[int] = []
    window_counts: list[dict] = []
    for start, end in periods:
        local_ages = []
        for window_start in range(timestamp(start), timestamp(end), 300):
            for offset in (120, 150, 179):
                decision_time = window_start + offset
                index = bisect.bisect_right(times, decision_time) - 1
                assert index >= 0
                local_ages.append(decision_time - times[index])
        ages.extend(local_ages)
        window_counts.append(
            {
                "possible": len(local_ages),
                "fresh": sum(age <= 7_200 for age in local_ages),
            }
        )
    fresh = sum(age <= 7_200 for age in ages)
    feasibility = evidence["pre_label_feasibility"]
    assert len(ages) == feasibility["possible_forecasts"] == 30_240
    assert fresh == feasibility["fresh_dvol_forecasts"] == 10_080
    assert fresh / len(ages) == feasibility["fresh_dvol_forecast_fraction"] == 1 / 3
    assert window_counts == [
        {"possible": 25_920, "fresh": 8_640},
        {"possible": 4_320, "fresh": 1_440},
    ]
    assert feasibility["failed_checks"] == ["minimum_fresh_dvol_forecast_fraction"]
    assert evidence["label_access_audit"]["binance_evaluation_archives_downloaded"] == 0
    assert evidence["label_access_audit"]["binance_evaluation_labels_loaded"] is False
    assert evidence["label_access_audit"]["brier_or_log_loss_computed"] is False
    assert evidence["decision"]["candidate_rejected"] is True
    assert evidence["decision"]["retuning_authorized"] is False
    assert evidence["decision"]["runtime_change_authorized"] is False
    print(
        json.dumps(
            {
                "ok": True,
                "points": len(points),
                "sampling_interval_seconds": intervals[0],
                "possible_forecasts": len(ages),
                "fresh_forecasts": fresh,
                "fresh_fraction": fresh / len(ages),
                "labels_downloaded": 0,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
