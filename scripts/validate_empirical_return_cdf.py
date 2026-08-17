#!/usr/bin/env python3
"""Independently recompute the empirical standardized-return CDF evidence."""

from __future__ import annotations

import argparse
import bisect
import gzip
import hashlib
import json
import math
import zipfile
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "deploy/promotions/evidence/strategy_registry"
PREREGISTRATION = REGISTRY / "20260721_empirical_return_cdf_preregistration.json"
EVIDENCE = REGISTRY / "20260721_empirical_return_cdf_public_calibration.json"
SNAPSHOT = REGISTRY / "source_snapshots/20260721_empirical_return_cdf_forecasts.jsonl.gz"
SECONDS_PER_YEAR = 365.25 * 86_400.0
LOOKBACK_SECONDS = 604_800
OFFSETS = (120, 150, 179)


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def close(actual: float, expected: float, tolerance: float = 1e-11) -> None:
    if not math.isclose(actual, expected, rel_tol=tolerance, abs_tol=tolerance):
        raise AssertionError(f"{actual} != {expected}")


def engine_norm_cdf(value: float) -> float:
    x = value / math.sqrt(2.0)
    sign = 1.0 if x >= 0 else -1.0
    x = abs(x)
    t = 1.0 / (1.0 + 0.3275911 * x)
    polynomial = (
        (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t)
    )
    erf = sign * (1.0 - polynomial * math.exp(-x * x))
    return 0.5 * (1.0 + erf)


def expected_dates() -> list[str]:
    older = pd.date_range("2026-04-09", "2026-05-15", freq="D")
    fresh = pd.date_range("2026-07-09", "2026-07-20", freq="D")
    return [value.strftime("%Y-%m-%d") for value in older.append(fresh)]


def independent_rolling_volatility(close_prices: np.ndarray) -> np.ndarray:
    returns = np.empty(len(close_prices), dtype=np.float64)
    returns[0] = np.nan
    returns[1:] = np.log(close_prices[1:] / close_prices[:-1])
    valid = np.isfinite(returns)
    values = np.where(valid, returns, 0.0)
    cumulative = np.concatenate(([0.0], np.cumsum(values)))
    cumulative_squares = np.concatenate(([0.0], np.cumsum(values * values)))
    cumulative_counts = np.concatenate(([0], np.cumsum(valid.astype(np.int64))))
    volatility = np.full(len(close_prices), np.nan)
    for index in range(len(close_prices)):
        start = max(0, index - 3599)
        count = int(cumulative_counts[index + 1] - cumulative_counts[start])
        if count < 20:
            continue
        total = cumulative[index + 1] - cumulative[start]
        total_squares = cumulative_squares[index + 1] - cumulative_squares[start]
        variance = max(0.0, total_squares / count - (total / count) ** 2)
        volatility[index] = min(5.0, max(0.05, math.sqrt(variance * SECONDS_PER_YEAR)))
    return volatility


def load_and_audit_archives(archive_dir: Path, manifest: list[dict]) -> dict[str, dict]:
    expected = {item["archive"]: item for item in manifest}
    names = [f"BTCUSDT-1s-{date}.zip" for date in expected_dates()]
    assert sorted(path.name for path in archive_dir.glob("*.zip")) == names
    segments: dict[str, list[pd.DataFrame]] = {"older": [], "fresh": []}
    previous_time = {"older": None, "fresh": None}
    audit = defaultdict(int)
    schema_widths: set[int] = set()
    maximum_gap = 0
    for name in names:
        path = archive_dir / name
        item = expected[name]
        payload = path.read_bytes()
        assert sha256(payload) == item["archive_sha256"]
        checksum_path = path.with_suffix(path.suffix + ".CHECKSUM")
        assert sha256(checksum_path.read_bytes()) == item["adjacent_checksum_sha256"]
        assert checksum_path.read_text().strip().split() == [item["archive_sha256"], name]
        with zipfile.ZipFile(path) as archive:
            assert len(archive.namelist()) == 1
            with archive.open(archive.namelist()[0]) as raw:
                schema_widths.add(len(raw.readline().decode().rstrip().split(",")))
        frame = pd.read_csv(
            path,
            header=None,
            usecols=[0, 1, 4, 6],
            names=["open_time_us", "open_price", "close_price", "close_time_us"],
            dtype={
                "open_time_us": "int64",
                "open_price": "float64",
                "close_price": "float64",
                "close_time_us": "int64",
            },
        )
        assert len(frame) == item["rows"] == 86_400
        archive_date = name.removeprefix("BTCUSDT-1s-")[:10]
        segment_name = "older" if archive_date < "2026-06-01" else "fresh"
        times = frame["open_time_us"].to_numpy()
        if previous_time[segment_name] is not None:
            boundary_delta = int(times[0] - previous_time[segment_name])
            audit["gaps"] += boundary_delta != 1_000_000
            maximum_gap = max(maximum_gap, boundary_delta)
        deltas = np.diff(times)
        audit["duplicates"] += int(np.sum(deltas == 0))
        audit["regressions"] += int(np.sum(deltas < 0))
        audit["gaps"] += int(np.sum(deltas != 1_000_000))
        maximum_gap = max(maximum_gap, int(deltas.max()))
        audit["invalid_durations"] += int(
            np.sum(frame["close_time_us"].to_numpy() - times != 999_999)
        )
        prices = frame[["open_price", "close_price"]].to_numpy()
        audit["invalid_prices"] += int(np.sum(~np.isfinite(prices) | (prices <= 0)))
        audit["rows"] += len(frame)
        previous_time[segment_name] = int(times[-1])
        segments[segment_name].append(frame)
    assert schema_widths == {12}
    expected_audit = {
        "rows": 4_233_600,
        "duplicates": 0,
        "regressions": 0,
        "gaps": 0,
        "invalid_durations": 0,
        "invalid_prices": 0,
    }
    assert audit == expected_audit, dict(audit)
    assert maximum_gap == 1_000_000
    return {
        name: {
            "frame": pd.concat(frames, ignore_index=True),
        }
        for name, frames in segments.items()
    }


def build_reference_rows(segments: dict[str, dict]) -> dict[tuple[int, int], dict]:
    rows: dict[tuple[int, int], dict] = {}
    for segment_name, segment in segments.items():
        frame = segment["frame"]
        close_prices = frame["close_price"].to_numpy()
        open_prices = frame["open_price"].to_numpy()
        volatility = independent_rolling_volatility(close_prices)
        first_second = int(frame["open_time_us"].iloc[0] // 1_000_000)
        last_second = int(frame["open_time_us"].iloc[-1] // 1_000_000)
        for window_start in range(first_second, last_second + 1, 300):
            base_index = window_start - first_second
            if base_index + 299 >= len(frame):
                continue
            strike = float(open_prices[base_index])
            terminal_close = float(close_prices[base_index + 299])
            for offset in OFFSETS:
                decision_index = base_index + offset - 1
                sigma = max(0.30, float(volatility[decision_index]))
                remaining = 300 - offset
                spot = float(close_prices[decision_index])
                residual = math.log(terminal_close / spot) / (
                    sigma * math.sqrt(remaining / SECONDS_PER_YEAR)
                )
                rows[(window_start, offset)] = {
                    "segment": segment_name,
                    "decision_timestamp": window_start + offset,
                    "remaining": remaining,
                    "spot": spot,
                    "strike": strike,
                    "terminal_close": terminal_close,
                    "sigma": sigma,
                    "residual": residual,
                }
    return rows


def is_evaluation_window(window_start: int) -> bool:
    return (
        1_776_297_600 <= window_start < 1_778_889_600
        or 1_784_160_000 <= window_start < 1_784_592_000
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-dir", type=Path, required=True)
    args = parser.parse_args()
    preregistration = load_json(PREREGISTRATION)
    evidence = load_json(EVIDENCE)
    compressed_snapshot = SNAPSHOT.read_bytes()
    uncompressed_snapshot = gzip.decompress(compressed_snapshot)
    snapshot_rows = [json.loads(line) for line in uncompressed_snapshot.splitlines()]

    assert preregistration["status"] == "PREREGISTERED_BEFORE_NEW_EVALUATION_LABEL_DOWNLOAD"
    for item in preregistration["source_pins_before_evaluation"].values():
        assert sha256((ROOT / item["path"]).read_bytes()) == item["sha256"]
    authority = evidence["authority"]
    assert sha256(PREREGISTRATION.read_bytes()) == authority["preregistration_sha256"]
    assert authority["preregistration_path"] == str(PREREGISTRATION.relative_to(ROOT))
    assert sha256(compressed_snapshot) == authority["forecast_snapshot"]["sha256"]
    assert sha256(uncompressed_snapshot) == authority["forecast_snapshot"][
        "uncompressed_sha256"
    ]
    assert len(snapshot_rows) == authority["forecast_snapshot"]["rows"] == 30_186

    segments = load_and_audit_archives(args.archive_dir, authority["archive_manifest"])
    reference = build_reference_rows(segments)
    histories: dict[tuple[str, int], tuple[list[int], np.ndarray]] = {}
    for segment_name in ("older", "fresh"):
        for offset in OFFSETS:
            selected = sorted(
                (
                    row["decision_timestamp"],
                    row["residual"],
                )
                for (window_start, candidate_offset), row in reference.items()
                if candidate_offset == offset and row["segment"] == segment_name
            )
            histories[(segment_name, offset)] = (
                [row[0] for row in selected],
                np.array([row[1] for row in selected]),
            )

    snapshot_by_key = {
        (int(row["window_start"]), int(row["elapsed_seconds"])): row
        for row in snapshot_rows
    }
    assert len(snapshot_by_key) == len(snapshot_rows)
    score_sums = defaultdict(float)
    daily = defaultdict(lambda: defaultdict(float))
    terminal_ties = 0
    evaluated_conditions: set[int] = set()
    for (window_start, offset), raw in reference.items():
        if not is_evaluation_window(window_start):
            continue
        if raw["terminal_close"] == raw["strike"]:
            terminal_ties += offset == OFFSETS[0]
            assert (window_start, offset) not in snapshot_by_key
            continue
        row = snapshot_by_key[(window_start, offset)]
        evaluated_conditions.add(window_start)
        close(raw["spot"], row["spot"])
        close(raw["strike"], row["strike"])
        close(raw["terminal_close"], row["terminal_close"])
        close(raw["sigma"], row["baseline_volatility"], tolerance=2e-9)
        threshold = math.log(raw["strike"] / raw["spot"]) / (
            raw["sigma"] * math.sqrt(raw["remaining"] / SECONDS_PER_YEAR)
        )
        close(threshold, row["current_threshold"], tolerance=2e-9)
        history_times, history_residuals = histories[(raw["segment"], offset)]
        lower = bisect.bisect_left(history_times, raw["decision_timestamp"] - LOOKBACK_SECONDS)
        upper = bisect.bisect_right(
            history_times, raw["decision_timestamp"] - raw["remaining"]
        )
        prior = history_residuals[lower:upper]
        exceedances = int(np.count_nonzero(prior > threshold))
        assert len(prior) == row["prior_sample_count"] == 2_016
        assert exceedances == row["prior_exceedance_count"]
        candidate = min(0.99, max(0.01, (exceedances + 0.5) / (len(prior) + 1.0)))
        close(candidate, row["candidate_probability"])
        years = raw["remaining"] / SECONDS_PER_YEAR
        d2 = (
            math.log(raw["spot"] / raw["strike"])
            + (0.05 - 0.5 * raw["sigma"] ** 2) * years
        ) / (raw["sigma"] * math.sqrt(years))
        baseline = min(0.99, max(0.01, engine_norm_cdf(d2)))
        close(baseline, row["baseline_probability"])
        outcome = float(raw["terminal_close"] > raw["strike"])
        baseline_brier = (baseline - outcome) ** 2
        candidate_brier = (candidate - outcome) ** 2
        baseline_log = -(outcome * math.log(baseline) + (1 - outcome) * math.log(1 - baseline))
        candidate_log = -(outcome * math.log(candidate) + (1 - outcome) * math.log(1 - candidate))
        brier_delta = baseline_brier - candidate_brier
        log_delta = baseline_log - candidate_log
        close(brier_delta, row["brier_improvement"])
        close(log_delta, row["log_loss_improvement"])
        score_sums["brier"] += brier_delta
        score_sums["log"] += log_delta
        day = row["utc_day"]
        daily[day]["brier"] += brier_delta
        daily[day]["log"] += log_delta
        daily[day]["count"] += 1

    assert terminal_ties == 18
    assert len(evaluated_conditions) == 10_062
    overall_brier = score_sums["brier"] / len(snapshot_rows)
    overall_log = score_sums["log"] / len(snapshot_rows)
    close(overall_brier, evidence["results"]["overall"]["brier_improvement"])
    close(overall_log, evidence["results"]["overall"]["log_loss_improvement"])
    days = sorted(daily)
    daily_brier = np.array([daily[day]["brier"] for day in days])
    daily_log = np.array([daily[day]["log"] for day in days])
    daily_count = np.array([daily[day]["count"] for day in days])
    rng = np.random.default_rng(20_260_721)
    samples = rng.integers(0, len(days), size=(10_000, len(days)))
    counts = daily_count[samples].sum(axis=1)
    for actual, expected in zip(
        np.quantile(daily_brier[samples].sum(axis=1) / counts, [0.025, 0.975]),
        evidence["results"]["bootstrap"]["brier_improvement_95pct"],
    ):
        close(float(actual), expected)
    for actual, expected in zip(
        np.quantile(daily_log[samples].sum(axis=1) / counts, [0.025, 0.975]),
        evidence["results"]["bootstrap"]["log_loss_improvement_95pct"],
    ):
        close(float(actual), expected)

    assert evidence["status"] == "PUBLIC_PROXY_CALIBRATION_REJECTED_NO_RETUNING"
    assert evidence["gate_evaluation"]["failed_checks"] == [
        "overall_log_loss_improvement_at_least_0_001",
        "brier_bootstrap_lower_bound_positive",
        "log_loss_bootstrap_lower_bound_positive",
        "all_chronological_windows_improve_brier",
        "all_chronological_windows_improve_log_loss",
        "each_decision_offset_log_loss_nonnegative",
        "overconfidence_tail_brier_nonnegative",
        "overconfidence_tail_log_loss_nonnegative",
    ]
    assert evidence["decision"]["strategy_variant_authorized"] is False
    assert evidence["decision"]["runtime_change_authorized"] is False
    assert evidence["decision"]["profitability_claim"] is False
    assert evidence["decision"]["a_plus_claim"] is False
    print(
        json.dumps(
            {
                "ok": True,
                "archives": 49,
                "source_rows": 4_233_600,
                "conditions": len(evaluated_conditions),
                "forecasts": len(snapshot_rows),
                "terminal_ties": terminal_ties,
                "brier_improvement": overall_brier,
                "log_loss_improvement": overall_log,
                "failed_checks": evidence["gate_evaluation"]["failed_checks"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
