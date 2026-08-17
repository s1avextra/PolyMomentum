#!/usr/bin/env python3
"""Evaluate the preregistered causal empirical standardized-return CDF."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import tempfile
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd


SECONDS_PER_YEAR = 365.25 * 86_400.0
VOLATILITY_FLOOR = 0.30
RISK_FREE_RATE = 0.05
LOOKBACK_SECONDS = 7 * 86_400
MINIMUM_PRIOR_SAMPLES = 1_800
DECISION_OFFSETS_SECONDS = (120, 150, 179)
BOOTSTRAP_SEED = 20_260_721
BOOTSTRAP_RESAMPLES = 10_000
OLDER_START = pd.Timestamp("2026-04-16T00:00:00Z")
OLDER_MIDPOINT = pd.Timestamp("2026-05-01T00:00:00Z")
OLDER_END = pd.Timestamp("2026-05-16T00:00:00Z")
FRESH_START = pd.Timestamp("2026-07-16T00:00:00Z")
FRESH_END = pd.Timestamp("2026-07-21T00:00:00Z")
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def portable_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(REPOSITORY_ROOT))
    except ValueError:
        return str(resolved)


def timestamp_seconds(timestamp: pd.Timestamp) -> int:
    return int(timestamp.timestamp())


def expected_dates() -> list[str]:
    older = pd.date_range("2026-04-09", "2026-05-15", freq="D")
    fresh = pd.date_range("2026-07-09", "2026-07-20", freq="D")
    return [value.strftime("%Y-%m-%d") for value in older.append(fresh)]


def normal_cdf(values: np.ndarray) -> np.ndarray:
    """Exact vectorization of the engine's A&S 7.1.26 implementation."""
    erf_input = values / math.sqrt(2.0)
    sign = np.where(erf_input >= 0.0, 1.0, -1.0)
    absolute = np.abs(erf_input)
    t = 1.0 / (1.0 + 0.3275911 * absolute)
    polynomial = (
        (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t)
    )
    erf = sign * (1.0 - polynomial * np.exp(-absolute * absolute))
    return 0.5 * (1.0 + erf)


def baseline_probability(
    spot: np.ndarray,
    strike: np.ndarray,
    remaining_seconds: np.ndarray,
    volatility: np.ndarray,
) -> np.ndarray:
    years = remaining_seconds / SECONDS_PER_YEAR
    d2 = (
        np.log(spot / strike)
        + (RISK_FREE_RATE - 0.5 * volatility * volatility) * years
    ) / (volatility * np.sqrt(years))
    return np.clip(normal_cdf(d2), 0.01, 0.99)


def read_checksum(checksum_path: Path) -> tuple[str, str]:
    fields = checksum_path.read_text().strip().split()
    if len(fields) != 2:
        raise ValueError(f"invalid checksum file: {checksum_path}")
    return fields[0], fields[1]


def load_archives(archive_dir: Path) -> tuple[pd.DataFrame, list[dict], dict]:
    dates = expected_dates()
    expected_names = [f"BTCUSDT-1s-{date}.zip" for date in dates]
    archive_paths = sorted(archive_dir.glob("BTCUSDT-1s-*.zip"))
    actual_names = [path.name for path in archive_paths]
    if actual_names != expected_names:
        missing = sorted(set(expected_names) - set(actual_names))
        extra = sorted(set(actual_names) - set(expected_names))
        raise ValueError(f"archive set mismatch: missing={missing} extra={extra}")

    frames: list[pd.DataFrame] = []
    manifest: list[dict] = []
    schema_widths: set[int] = set()
    for archive_path, date in zip(archive_paths, dates, strict=True):
        checksum_path = archive_path.with_suffix(archive_path.suffix + ".CHECKSUM")
        expected_sha256, expected_name = read_checksum(checksum_path)
        actual_sha256 = sha256_file(archive_path)
        if expected_name != archive_path.name or expected_sha256 != actual_sha256:
            raise ValueError(f"checksum mismatch: {archive_path}")
        with zipfile.ZipFile(archive_path) as archive:
            members = archive.namelist()
            if len(members) != 1:
                raise ValueError(f"expected one CSV member: {archive_path}")
            with archive.open(members[0]) as raw:
                schema_widths.add(len(raw.readline().decode("utf-8").rstrip().split(",")))
        frame = pd.read_csv(
            archive_path,
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
        frame["source_segment"] = "older" if date < "2026-06-01" else "fresh"
        frames.append(frame)
        manifest.append(
            {
                "archive": archive_path.name,
                "archive_sha256": actual_sha256,
                "adjacent_checksum": checksum_path.name,
                "adjacent_checksum_sha256": sha256_file(checksum_path),
                "rows": int(len(frame)),
                "url": (
                    "https://data.binance.vision/data/spot/daily/klines/"
                    f"BTCUSDT/1s/{archive_path.name}"
                ),
            }
        )

    data = pd.concat(frames, ignore_index=True)
    duplicate_count = 0
    regression_count = 0
    gap_violations = 0
    maximum_internal_gap = 0.0
    for _, segment in data.groupby("source_segment", sort=False):
        delta = np.diff(segment["open_time_us"].to_numpy())
        duplicate_count += int(np.sum(delta == 0))
        regression_count += int(np.sum(delta < 0))
        gap_violations += int(np.sum(delta != 1_000_000))
        maximum_internal_gap = max(maximum_internal_gap, float(delta.max() / 1_000_000))
    close_duration = data["close_time_us"].to_numpy() - data["open_time_us"].to_numpy()
    prices = data[["open_price", "close_price"]].to_numpy()
    quality = {
        "archives": len(archive_paths),
        "rows": int(len(data)),
        "source_segments": 2,
        "schema_widths": sorted(schema_widths),
        "rows_per_archive_minimum": min(item["rows"] for item in manifest),
        "rows_per_archive_maximum": max(item["rows"] for item in manifest),
        "timestamp_unit": "microseconds",
        "timestamp_duplicates_within_segments": duplicate_count,
        "timestamp_regressions_within_segments": regression_count,
        "one_second_gap_violations_within_segments": gap_violations,
        "maximum_internal_gap_seconds": maximum_internal_gap,
        "expected_gap_between_segments": True,
        "invalid_close_durations": int(np.sum(close_duration != 999_999)),
        "invalid_prices": int(np.sum(~np.isfinite(prices) | (prices <= 0))),
        "checksum_failures": 0,
    }
    return data, manifest, quality


def attach_realized_volatility(data: pd.DataFrame) -> pd.DataFrame:
    pieces: list[pd.DataFrame] = []
    for _, segment in data.groupby("source_segment", sort=False):
        segment = segment.copy()
        close = segment["close_price"].to_numpy()
        log_returns = np.empty(len(segment), dtype=np.float64)
        log_returns[0] = np.nan
        log_returns[1:] = np.log(close[1:] / close[:-1])
        variance = (
            pd.Series(log_returns)
            .rolling(window=3600, min_periods=20)
            .var(ddof=0)
            .to_numpy()
        )
        segment["rolling_one_hour_volatility"] = np.clip(
            np.sqrt(variance * SECONDS_PER_YEAR), 0.05, 5.0
        )
        pieces.append(segment)
    return pd.concat(pieces, ignore_index=True)


def build_condition_offsets(data: pd.DataFrame) -> pd.DataFrame:
    data = attach_realized_volatility(data)
    observed_second = ((data["close_time_us"].to_numpy() + 1) // 1_000_000).astype(np.int64)
    open_second = (data["open_time_us"].to_numpy() // 1_000_000).astype(np.int64)
    data = data.assign(
        observed_second=observed_second,
        window_start=(open_second // 300) * 300,
    )
    data["elapsed_seconds"] = data["observed_second"] - data["window_start"]
    opens = data.loc[
        data["elapsed_seconds"] == 1,
        ["source_segment", "window_start", "open_price"],
    ].rename(columns={"open_price": "strike"})
    terminals = data.loc[
        data["elapsed_seconds"] == 300,
        ["source_segment", "window_start", "close_price"],
    ].rename(columns={"close_price": "terminal_close"})
    decisions = data.loc[
        data["elapsed_seconds"].isin(DECISION_OFFSETS_SECONDS),
        [
            "source_segment",
            "window_start",
            "elapsed_seconds",
            "close_price",
            "rolling_one_hour_volatility",
        ],
    ].rename(columns={"close_price": "spot"})
    if opens.duplicated(["source_segment", "window_start"]).any():
        raise ValueError("duplicate five-minute opening rows")
    if terminals.duplicated(["source_segment", "window_start"]).any():
        raise ValueError("duplicate five-minute terminal rows")
    rows = decisions.merge(
        opens,
        on=["source_segment", "window_start"],
        validate="many_to_one",
    ).merge(
        terminals,
        on=["source_segment", "window_start"],
        validate="many_to_one",
    )
    rows["decision_timestamp"] = rows["window_start"] + rows["elapsed_seconds"]
    rows["terminal_timestamp"] = rows["window_start"] + 300
    rows["remaining_seconds"] = 300 - rows["elapsed_seconds"]
    rows["baseline_volatility"] = np.maximum(
        VOLATILITY_FLOOR, rows["rolling_one_hour_volatility"]
    )
    scale = rows["baseline_volatility"] * np.sqrt(
        rows["remaining_seconds"] / SECONDS_PER_YEAR
    )
    rows["historical_residual"] = np.log(
        rows["terminal_close"] / rows["spot"]
    ) / scale
    return rows


def chronological_window(window_start: int) -> str | None:
    if timestamp_seconds(OLDER_START) <= window_start < timestamp_seconds(OLDER_MIDPOINT):
        return "older_first"
    if timestamp_seconds(OLDER_MIDPOINT) <= window_start < timestamp_seconds(OLDER_END):
        return "older_second"
    if timestamp_seconds(FRESH_START) <= window_start < timestamp_seconds(FRESH_END):
        return "fresh_holdout"
    return None


def empirical_probabilities(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    evaluation = rows.copy()
    evaluation["chronological_window"] = evaluation["window_start"].map(chronological_window)
    evaluation = evaluation.loc[evaluation["chronological_window"].notna()].copy()
    evaluation["prior_sample_count"] = 0
    evaluation["prior_exceedance_count"] = 0
    evaluation["current_threshold"] = np.nan
    evaluation["candidate_probability"] = np.nan

    for (segment_name, offset), index in evaluation.groupby(
        ["source_segment", "elapsed_seconds"], sort=True
    ).groups.items():
        history = rows.loc[
            (rows["source_segment"] == segment_name)
            & (rows["elapsed_seconds"] == offset)
            & rows["historical_residual"].notna()
        ].sort_values("decision_timestamp")
        history_times = history["decision_timestamp"].to_numpy(dtype=np.int64)
        history_residuals = history["historical_residual"].to_numpy(dtype=np.float64)
        remaining = 300 - int(offset)
        for row_index in index:
            decision_timestamp = int(evaluation.at[row_index, "decision_timestamp"])
            lower = np.searchsorted(
                history_times, decision_timestamp - LOOKBACK_SECONDS, side="left"
            )
            upper = np.searchsorted(
                history_times, decision_timestamp - remaining, side="right"
            )
            prior = history_residuals[lower:upper]
            count = len(prior)
            evaluation.at[row_index, "prior_sample_count"] = count
            if count < MINIMUM_PRIOR_SAMPLES:
                continue
            sigma_scale = float(evaluation.at[row_index, "baseline_volatility"]) * math.sqrt(
                remaining / SECONDS_PER_YEAR
            )
            threshold = math.log(
                float(evaluation.at[row_index, "strike"])
                / float(evaluation.at[row_index, "spot"])
            ) / sigma_scale
            exceedances = int(np.count_nonzero(prior > threshold))
            probability = (exceedances + 0.5) / (count + 1.0)
            evaluation.at[row_index, "prior_exceedance_count"] = exceedances
            evaluation.at[row_index, "current_threshold"] = threshold
            evaluation.at[row_index, "candidate_probability"] = min(
                0.99, max(0.01, probability)
            )

    expected_conditions = 35 * 24 * 12
    expected_forecasts = expected_conditions * len(DECISION_OFFSETS_SECONDS)
    eligible_before_ties = evaluation["candidate_probability"].notna()
    evaluation["terminal_up"] = evaluation["terminal_close"] > evaluation["strike"]
    evaluation["terminal_tie"] = evaluation["terminal_close"] == evaluation["strike"]
    forecasts = evaluation.loc[eligible_before_ties & ~evaluation["terminal_tie"]].copy()
    forecasts["baseline_probability"] = baseline_probability(
        forecasts["spot"].to_numpy(),
        forecasts["strike"].to_numpy(),
        forecasts["remaining_seconds"].to_numpy(),
        forecasts["baseline_volatility"].to_numpy(),
    )
    outcome = forecasts["terminal_up"].astype(float)
    forecasts["baseline_brier"] = (forecasts["baseline_probability"] - outcome) ** 2
    forecasts["candidate_brier"] = (forecasts["candidate_probability"] - outcome) ** 2
    epsilon = 1e-15
    forecasts["baseline_log_loss"] = -(
        outcome * np.log(forecasts["baseline_probability"].clip(epsilon, 1 - epsilon))
        + (1 - outcome)
        * np.log((1 - forecasts["baseline_probability"]).clip(epsilon, 1 - epsilon))
    )
    forecasts["candidate_log_loss"] = -(
        outcome * np.log(forecasts["candidate_probability"].clip(epsilon, 1 - epsilon))
        + (1 - outcome)
        * np.log((1 - forecasts["candidate_probability"]).clip(epsilon, 1 - epsilon))
    )
    forecasts["brier_improvement"] = forecasts["baseline_brier"] - forecasts["candidate_brier"]
    forecasts["log_loss_improvement"] = (
        forecasts["baseline_log_loss"] - forecasts["candidate_log_loss"]
    )
    forecasts["probability_displacement"] = (
        forecasts["candidate_probability"] - forecasts["baseline_probability"]
    )
    forecasts["utc_day"] = pd.to_datetime(
        forecasts["window_start"], unit="s", utc=True
    ).dt.strftime("%Y-%m-%d")
    conditions_by_window = {
        key: int(group["window_start"].nunique())
        for key, group in forecasts.groupby("chronological_window", sort=True)
    }
    older_conditions = conditions_by_window.get("older_first", 0) + conditions_by_window.get(
        "older_second", 0
    )
    fresh_conditions = conditions_by_window.get("fresh_holdout", 0)
    quality = {
        "expected_conditions": expected_conditions,
        "retained_conditions": int(forecasts["window_start"].nunique()),
        "terminal_tie_conditions": int(
            evaluation.loc[eligible_before_ties & evaluation["terminal_tie"], "window_start"].nunique()
        ),
        "forecasts_missing_prior_support": int((~eligible_before_ties).sum()),
        "expected_registered_forecasts": expected_forecasts,
        "retained_registered_forecasts": int(len(forecasts)),
        "complete_registered_forecasts_fraction": float(len(forecasts) / expected_forecasts),
        "duplicate_condition_offsets": int(
            forecasts.duplicated(["window_start", "elapsed_seconds"]).sum()
        ),
        "invalid_probabilities": int(
            (
                ~forecasts[["baseline_probability", "candidate_probability"]]
                .apply(np.isfinite)
                .all(axis=1)
                | (forecasts["baseline_probability"] <= 0)
                | (forecasts["baseline_probability"] >= 1)
                | (forecasts["candidate_probability"] <= 0)
                | (forecasts["candidate_probability"] >= 1)
            ).sum()
        ),
        "prior_sample_count_minimum": int(forecasts["prior_sample_count"].min()),
        "prior_sample_count_median": float(forecasts["prior_sample_count"].median()),
        "prior_sample_count_maximum": int(forecasts["prior_sample_count"].max()),
        "forecasts_per_condition_values": sorted(
            forecasts.groupby("window_start").size().unique().astype(int).tolist()
        ),
        "conditions_by_chronological_window": conditions_by_window,
        "retained_conditions_older": older_conditions,
        "retained_conditions_fresh_holdout": fresh_conditions,
    }
    return forecasts, quality


def score_frame(frame: pd.DataFrame) -> dict:
    baseline = frame["baseline_probability"]
    candidate = frame["candidate_probability"]
    outcome = frame["terminal_up"].astype(float)
    confidence_change = (candidate - 0.5).abs() - (baseline - 0.5).abs()
    return {
        "forecasts": int(len(frame)),
        "conditions": int(frame["window_start"].nunique()),
        "baseline": {
            "brier": float(frame["baseline_brier"].mean()),
            "log_loss": float(frame["baseline_log_loss"].mean()),
            "mean_probability": float(baseline.mean()),
        },
        "candidate": {
            "brier": float(frame["candidate_brier"].mean()),
            "log_loss": float(frame["candidate_log_loss"].mean()),
            "mean_probability": float(candidate.mean()),
        },
        "outcome_rate": float(outcome.mean()),
        "brier_improvement": float(frame["brier_improvement"].mean()),
        "log_loss_improvement": float(frame["log_loss_improvement"].mean()),
        "mean_absolute_probability_displacement": float(
            frame["probability_displacement"].abs().mean()
        ),
        "moved_toward_half_fraction": float((confidence_change < -1e-15).mean()),
        "moved_away_from_half_fraction": float((confidence_change > 1e-15).mean()),
        "maximum_absolute_confidence_increase": float(
            confidence_change.clip(lower=0).max()
        ),
    }


def paired_day_bootstrap(forecasts: pd.DataFrame) -> dict:
    daily = forecasts.groupby("utc_day", sort=True).agg(
        brier_sum=("brier_improvement", "sum"),
        log_loss_sum=("log_loss_improvement", "sum"),
        forecasts=("window_start", "size"),
    )
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    samples = rng.integers(0, len(daily), size=(BOOTSTRAP_RESAMPLES, len(daily)))
    counts = daily["forecasts"].to_numpy()[samples].sum(axis=1)
    brier = daily["brier_sum"].to_numpy()[samples].sum(axis=1) / counts
    log_loss = daily["log_loss_sum"].to_numpy()[samples].sum(axis=1) / counts
    return {
        "unit": "UTC day",
        "days": int(len(daily)),
        "resamples": BOOTSTRAP_RESAMPLES,
        "seed": BOOTSTRAP_SEED,
        "brier_improvement_95pct": [
            float(value) for value in np.quantile(brier, [0.025, 0.975])
        ],
        "log_loss_improvement_95pct": [
            float(value) for value in np.quantile(log_loss, [0.025, 0.975])
        ],
    }


def evaluate_gates(results: dict, quality: dict, source_quality: dict) -> dict:
    overall = results["overall"]
    bootstrap = results["bootstrap"]
    windows = results["chronological_windows"]
    offsets = results["decision_offsets"]
    tail = results["overconfidence_tail"]
    checks = {
        "source_checksum_failures_zero": source_quality["checksum_failures"] == 0,
        "source_timestamp_duplicates_zero": source_quality[
            "timestamp_duplicates_within_segments"
        ]
        == 0,
        "source_timestamp_regressions_zero": source_quality[
            "timestamp_regressions_within_segments"
        ]
        == 0,
        "source_internal_gaps_at_most_two_seconds": source_quality[
            "maximum_internal_gap_seconds"
        ]
        <= 2,
        "source_invalid_prices_zero": source_quality["invalid_prices"] == 0,
        "minimum_conditions_older": quality["retained_conditions_older"] >= 8000,
        "minimum_conditions_fresh_holdout": quality[
            "retained_conditions_fresh_holdout"
        ]
        >= 1300,
        "complete_registered_forecasts_at_least_99pct": quality[
            "complete_registered_forecasts_fraction"
        ]
        >= 0.99,
        "minimum_prior_samples_per_forecast": quality["prior_sample_count_minimum"]
        >= MINIMUM_PRIOR_SAMPLES,
        "overall_brier_improvement_at_least_0_0005": overall["brier_improvement"]
        >= 0.0005,
        "overall_log_loss_improvement_at_least_0_001": overall[
            "log_loss_improvement"
        ]
        >= 0.001,
        "brier_bootstrap_lower_bound_positive": bootstrap[
            "brier_improvement_95pct"
        ][0]
        > 0,
        "log_loss_bootstrap_lower_bound_positive": bootstrap[
            "log_loss_improvement_95pct"
        ][0]
        > 0,
        "all_chronological_windows_improve_brier": all(
            score["brier_improvement"] > 0 for score in windows.values()
        ),
        "all_chronological_windows_improve_log_loss": all(
            score["log_loss_improvement"] > 0 for score in windows.values()
        ),
        "each_decision_offset_brier_nonnegative": all(
            score["brier_improvement"] >= 0 for score in offsets.values()
        ),
        "each_decision_offset_log_loss_nonnegative": all(
            score["log_loss_improvement"] >= 0 for score in offsets.values()
        ),
        "overconfidence_tail_brier_nonnegative": tail["brier_improvement"] >= 0,
        "overconfidence_tail_log_loss_nonnegative": tail["log_loss_improvement"]
        >= 0,
    }
    return {
        "checks": checks,
        "passed": all(checks.values()),
        "failed_checks": [name for name, passed in checks.items() if not passed],
    }


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=path.parent, prefix=f"{path.name}.tmp.", delete=False
    ) as output:
        json.dump(payload, output, indent=2, allow_nan=False)
        output.write("\n")
        temporary_path = Path(output.name)
    os.replace(temporary_path, path)


def write_forecast_snapshot(path: Path, forecasts: pd.DataFrame) -> dict:
    columns = [
        "window_start",
        "utc_day",
        "chronological_window",
        "elapsed_seconds",
        "remaining_seconds",
        "decision_timestamp",
        "spot",
        "strike",
        "terminal_close",
        "terminal_up",
        "rolling_one_hour_volatility",
        "baseline_volatility",
        "prior_sample_count",
        "prior_exceedance_count",
        "current_threshold",
        "baseline_probability",
        "candidate_probability",
        "baseline_brier",
        "candidate_brier",
        "baseline_log_loss",
        "candidate_log_loss",
        "brier_improvement",
        "log_loss_improvement",
        "probability_displacement",
    ]
    lines = [
        json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False)
        for record in forecasts[columns].to_dict(orient="records")
    ]
    uncompressed = ("\n".join(lines) + "\n").encode()
    compressed = gzip.compress(uncompressed, compresslevel=9, mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    temporary_path.write_bytes(compressed)
    os.replace(temporary_path, path)
    return {
        "path": portable_path(path),
        "rows": int(len(forecasts)),
        "sha256": sha256_bytes(compressed),
        "uncompressed_sha256": sha256_bytes(uncompressed),
        "compressed_bytes": len(compressed),
        "uncompressed_bytes": len(uncompressed),
    }


def run(archive_dir: Path, evidence_path: Path, snapshot_path: Path) -> dict:
    preregistration_path = (
        evidence_path.parent / "20260721_empirical_return_cdf_preregistration.json"
    )
    data, archive_manifest, source_quality = load_archives(archive_dir)
    rows = build_condition_offsets(data)
    forecasts, forecast_quality = empirical_probabilities(rows)
    tail = forecasts.loc[
        (forecasts["baseline_probability"] >= 0.75)
        | (forecasts["baseline_probability"] <= 0.25)
    ]
    results = {
        "overall": score_frame(forecasts),
        "chronological_windows": {
            key: score_frame(group)
            for key, group in forecasts.groupby("chronological_window", sort=True)
        },
        "decision_offsets": {
            str(int(key)): score_frame(group)
            for key, group in forecasts.groupby("elapsed_seconds", sort=True)
        },
        "overconfidence_tail": score_frame(tail),
        "bootstrap": paired_day_bootstrap(forecasts),
    }
    gate_evaluation = evaluate_gates(results, forecast_quality, source_quality)
    snapshot = write_forecast_snapshot(snapshot_path, forecasts)
    evidence = {
        "schema_version": 1,
        "status": (
            "PUBLIC_PROXY_CALIBRATION_PASS_REQUIRES_INDEPENDENT_POLYMARKET_SCREEN"
            if gate_evaluation["passed"]
            else "PUBLIC_PROXY_CALIBRATION_REJECTED_NO_RETUNING"
        ),
        "mechanism_id": "empirical_standardized_return_cdf_v1",
        "authority": {
            "preregistration_path": portable_path(preregistration_path),
            "preregistration_sha256": sha256_file(preregistration_path),
            "archive_directory": str(archive_dir),
            "archive_manifest": archive_manifest,
            "forecast_snapshot": snapshot,
        },
        "source_data_quality": source_quality,
        "forecast_data_quality": forecast_quality,
        "results": results,
        "gate_evaluation": gate_evaluation,
        "decision": {
            "public_proxy_calibration_passed": gate_evaluation["passed"],
            "strategy_variant_authorized": False,
            "exact_strategy_replay_authorized": False,
            "runtime_change_authorized": False,
            "paper_or_live_trading_authorized": False,
            "profitability_claim": False,
            "a_plus_claim": False,
            "next_step": (
                "Register one default-off opportunity-capture field and require an independent Polymarket opportunity-level screen."
                if gate_evaluation["passed"]
                else "Reject empirical_standardized_return_cdf_v1 and do not tune this family on the registered windows."
            ),
        },
        "limitations": [
            "Binance terminal direction is a proxy for the official Chainlink settlement source.",
            "The population contains every complete five-minute Binance window, not the strategy's gated Polymarket opportunities.",
            "Overlapping seven-day historical samples are a causal reference distribution, not independent observations; uncertainty is therefore bootstrapped by evaluation UTC day.",
            "Proper-score improvement cannot establish executable edge, fills, fee-inclusive PnL, breadth, or tail stability.",
            "The evaluation excludes strict-42, prior proxy screens, retained July 14-15 captures, and every active or sealed forward-block condition.",
        ],
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
                "conditions": evidence["results"]["overall"]["conditions"],
                "forecasts": evidence["results"]["overall"]["forecasts"],
                "brier_improvement": evidence["results"]["overall"][
                    "brier_improvement"
                ],
                "log_loss_improvement": evidence["results"]["overall"][
                    "log_loss_improvement"
                ],
                "failed_checks": evidence["gate_evaluation"]["failed_checks"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
