#!/usr/bin/env python3
"""Evaluate the preregistered fast-volatility maximum on public BTC windows."""

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
FAST_HALF_LIFE_SECONDS = 15.0 * 60.0
FAST_TAU_SECONDS = FAST_HALF_LIFE_SECONDS / math.log(2.0)
FAST_ALPHA_ONE_SECOND = 1.0 - math.exp(-1.0 / FAST_TAU_SECONDS)
VOLATILITY_FLOOR = 0.30
RISK_FREE_RATE = 0.05
DECISION_OFFSETS_SECONDS = (120, 150, 179)
BOOTSTRAP_SEED = 20_260_721
BOOTSTRAP_RESAMPLES = 10_000
OLDER_START = pd.Timestamp("2026-06-11T00:00:00Z")
HOLDOUT_START = pd.Timestamp("2026-06-26T00:00:00Z")
EVALUATION_END = pd.Timestamp("2026-07-11T00:00:00Z")
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


def normal_cdf(values: np.ndarray) -> np.ndarray:
    # Exact vectorization of rust_engine/src/fair_value.rs A&S 7.1.26.
    erf_input = values / math.sqrt(2.0)
    sign = np.where(erf_input >= 0.0, 1.0, -1.0)
    absolute = np.abs(erf_input)
    t = 1.0 / (1.0 + 0.3275911 * absolute)
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
    erf = sign * (1.0 - polynomial * np.exp(-absolute * absolute))
    return 0.5 * (1.0 + erf)


def fair_probability(
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
    archive_paths = sorted(archive_dir.glob("BTCUSDT-1s-*.zip"))
    if len(archive_paths) != 31:
        raise ValueError(f"expected 31 archives, found {len(archive_paths)}")

    frames: list[pd.DataFrame] = []
    archive_manifest: list[dict] = []
    schema_widths: set[int] = set()
    for archive_path in archive_paths:
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
                first_line = raw.readline().decode("utf-8").rstrip("\n")
                schema_widths.add(len(first_line.split(",")))

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
        frames.append(frame)
        archive_manifest.append(
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
    open_time_us = data["open_time_us"].to_numpy()
    close_time_us = data["close_time_us"].to_numpy()
    open_time_delta = np.diff(open_time_us)
    close_duration = close_time_us - open_time_us
    prices = data[["open_price", "close_price"]].to_numpy()
    quality = {
        "archives": len(archive_paths),
        "rows": int(len(data)),
        "schema_widths": sorted(schema_widths),
        "rows_per_archive_minimum": min(item["rows"] for item in archive_manifest),
        "rows_per_archive_maximum": max(item["rows"] for item in archive_manifest),
        "timestamp_unit": "microseconds",
        "timestamp_duplicates": int(np.sum(open_time_delta == 0)),
        "timestamp_regressions": int(np.sum(open_time_delta < 0)),
        "one_second_gap_violations": int(np.sum(open_time_delta != 1_000_000)),
        "maximum_gap_seconds": float(open_time_delta.max() / 1_000_000),
        "invalid_close_durations": int(np.sum(close_duration != 999_999)),
        "invalid_prices": int(np.sum(~np.isfinite(prices) | (prices <= 0))),
        "checksum_failures": 0,
    }
    return data, archive_manifest, quality


def build_forecasts(data: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    open_time_us = data["open_time_us"].to_numpy()
    close_time_us = data["close_time_us"].to_numpy()
    close_price = data["close_price"].to_numpy()
    open_price = data["open_price"].to_numpy()
    observed_second = ((close_time_us + 1) // 1_000_000).astype(np.int64)
    kline_open_second = (open_time_us // 1_000_000).astype(np.int64)
    window_start = (kline_open_second // 300) * 300
    elapsed_seconds = observed_second - window_start

    log_returns = np.empty(len(close_price), dtype=np.float64)
    log_returns[0] = np.nan
    log_returns[1:] = np.log(close_price[1:] / close_price[:-1])
    squared_return_rate = log_returns * log_returns
    fast_variance = (
        pd.Series(squared_return_rate)
        .ewm(alpha=FAST_ALPHA_ONE_SECOND, adjust=False, ignore_na=True)
        .mean()
        .to_numpy()
    )
    fast_volatility = np.clip(
        np.sqrt(fast_variance * SECONDS_PER_YEAR), 0.10, 5.0
    )
    rolling_variance = (
        pd.Series(log_returns)
        .rolling(window=3600, min_periods=20)
        .var(ddof=0)
        .to_numpy()
    )
    rolling_one_hour_volatility = np.clip(
        np.sqrt(rolling_variance * SECONDS_PER_YEAR), 0.05, 5.0
    )

    data = data.assign(
        observed_second=observed_second,
        window_start=window_start,
        elapsed_seconds=elapsed_seconds,
        fast_volatility=fast_volatility,
        rolling_one_hour_volatility=rolling_one_hour_volatility,
    )
    evaluation_start_second = timestamp_seconds(OLDER_START)
    evaluation_end_second = timestamp_seconds(EVALUATION_END)
    in_evaluation = (data["window_start"] >= evaluation_start_second) & (
        data["window_start"] < evaluation_end_second
    )

    opens = data.loc[
        in_evaluation & (data["elapsed_seconds"] == 1),
        ["window_start", "open_price"],
    ].rename(columns={"open_price": "strike"})
    terminals = data.loc[
        in_evaluation & (data["elapsed_seconds"] == 300),
        ["window_start", "close_price"],
    ].rename(columns={"close_price": "terminal_close"})
    decisions = data.loc[
        in_evaluation & data["elapsed_seconds"].isin(DECISION_OFFSETS_SECONDS),
        [
            "window_start",
            "elapsed_seconds",
            "close_price",
            "rolling_one_hour_volatility",
            "fast_volatility",
        ],
    ].rename(columns={"close_price": "spot"})

    if opens["window_start"].duplicated().any():
        raise ValueError("duplicate five-minute opening rows")
    if terminals["window_start"].duplicated().any():
        raise ValueError("duplicate five-minute terminal rows")
    forecasts = decisions.merge(opens, on="window_start", validate="many_to_one")
    forecasts = forecasts.merge(terminals, on="window_start", validate="many_to_one")
    forecasts["terminal_up"] = forecasts["terminal_close"] > forecasts["strike"]
    forecasts["terminal_tie"] = forecasts["terminal_close"] == forecasts["strike"]
    forecasts = forecasts.loc[~forecasts["terminal_tie"]].copy()
    forecasts["remaining_seconds"] = 300 - forecasts["elapsed_seconds"]
    forecasts["baseline_volatility"] = np.maximum(
        VOLATILITY_FLOOR, forecasts["rolling_one_hour_volatility"]
    )
    forecasts["candidate_volatility"] = np.maximum(
        forecasts["baseline_volatility"], forecasts["fast_volatility"]
    )
    forecasts["baseline_probability"] = fair_probability(
        forecasts["spot"].to_numpy(),
        forecasts["strike"].to_numpy(),
        forecasts["remaining_seconds"].to_numpy(),
        forecasts["baseline_volatility"].to_numpy(),
    )
    forecasts["candidate_probability"] = fair_probability(
        forecasts["spot"].to_numpy(),
        forecasts["strike"].to_numpy(),
        forecasts["remaining_seconds"].to_numpy(),
        forecasts["candidate_volatility"].to_numpy(),
    )
    outcome = forecasts["terminal_up"].astype(float)
    forecasts["baseline_brier"] = (
        forecasts["baseline_probability"] - outcome
    ) ** 2
    forecasts["candidate_brier"] = (
        forecasts["candidate_probability"] - outcome
    ) ** 2
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
    forecasts["brier_improvement"] = (
        forecasts["baseline_brier"] - forecasts["candidate_brier"]
    )
    forecasts["log_loss_improvement"] = (
        forecasts["baseline_log_loss"] - forecasts["candidate_log_loss"]
    )
    forecasts["probability_displacement"] = (
        forecasts["candidate_probability"] - forecasts["baseline_probability"]
    )
    forecasts["utc_day"] = pd.to_datetime(
        forecasts["window_start"], unit="s", utc=True
    ).dt.strftime("%Y-%m-%d")
    forecasts["chronological_window"] = np.where(
        forecasts["window_start"] < timestamp_seconds(HOLDOUT_START),
        "older",
        "fresh_holdout",
    )
    expected_conditions = 30 * 24 * 12
    retained_conditions = int(forecasts["window_start"].nunique())
    expected_forecasts = expected_conditions * len(DECISION_OFFSETS_SECONDS)
    quality = {
        "expected_conditions": expected_conditions,
        "retained_conditions": retained_conditions,
        "terminal_tie_conditions": expected_conditions - retained_conditions,
        "expected_registered_forecasts": expected_forecasts,
        "retained_registered_forecasts": int(len(forecasts)),
        "complete_registered_forecasts_fraction": float(
            len(forecasts) / expected_forecasts
        ),
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
        "forecasts_per_condition_values": sorted(
            forecasts.groupby("window_start").size().unique().astype(int).tolist()
        ),
        "conditions_by_chronological_window": {
            key: int(group["window_start"].nunique())
            for key, group in forecasts.groupby("chronological_window")
        },
    }
    return forecasts, quality


def score_frame(frame: pd.DataFrame) -> dict:
    baseline_probability = frame["baseline_probability"]
    candidate_probability = frame["candidate_probability"]
    outcome = frame["terminal_up"].astype(float)
    confidence_change = (
        (candidate_probability - 0.5).abs()
        - (baseline_probability - 0.5).abs()
    )
    return {
        "forecasts": int(len(frame)),
        "conditions": int(frame["window_start"].nunique()),
        "baseline": {
            "brier": float(frame["baseline_brier"].mean()),
            "log_loss": float(frame["baseline_log_loss"].mean()),
            "mean_probability": float(baseline_probability.mean()),
        },
        "candidate": {
            "brier": float(frame["candidate_brier"].mean()),
            "log_loss": float(frame["candidate_log_loss"].mean()),
            "mean_probability": float(candidate_probability.mean()),
        },
        "outcome_rate": float(outcome.mean()),
        "brier_improvement": float(frame["brier_improvement"].mean()),
        "log_loss_improvement": float(frame["log_loss_improvement"].mean()),
        "mean_absolute_probability_displacement": float(
            frame["probability_displacement"].abs().mean()
        ),
        "candidate_volatility_active_fraction": float(
            (frame["candidate_volatility"] > frame["baseline_volatility"] + 1e-15).mean()
        ),
        "candidate_more_confident_fraction": float((confidence_change > 1e-15).mean()),
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
        "source_timestamp_duplicates_zero": source_quality["timestamp_duplicates"] == 0,
        "source_timestamp_regressions_zero": source_quality["timestamp_regressions"] == 0,
        "source_gaps_at_most_two_seconds": source_quality["maximum_gap_seconds"] <= 2,
        "source_invalid_prices_zero": source_quality["invalid_prices"] == 0,
        "minimum_conditions_each_half": min(
            quality["conditions_by_chronological_window"].values()
        )
        >= 3500,
        "complete_registered_forecasts_at_least_99pct": quality[
            "complete_registered_forecasts_fraction"
        ]
        >= 0.99,
        "overall_brier_improvement_at_least_0_0005": overall[
            "brier_improvement"
        ]
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
        "both_chronological_windows_improve_brier": all(
            score["brier_improvement"] > 0 for score in windows.values()
        ),
        "both_chronological_windows_improve_log_loss": all(
            score["log_loss_improvement"] > 0 for score in windows.values()
        ),
        "each_decision_offset_brier_nonnegative": all(
            score["brier_improvement"] >= 0 for score in offsets.values()
        ),
        "each_decision_offset_log_loss_nonnegative": all(
            score["log_loss_improvement"] >= 0 for score in offsets.values()
        ),
        "overconfidence_tail_brier_nonnegative": tail["brier_improvement"] >= 0,
        "overconfidence_tail_log_loss_nonnegative": tail[
            "log_loss_improvement"
        ]
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
        json.dump(payload, output, indent=2, sort_keys=False, allow_nan=False)
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
        "spot",
        "strike",
        "terminal_close",
        "terminal_up",
        "rolling_one_hour_volatility",
        "fast_volatility",
        "baseline_volatility",
        "candidate_volatility",
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
    lines = []
    for record in forecasts[columns].to_dict(orient="records"):
        lines.append(
            json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False)
        )
    uncompressed = ("\n".join(lines) + "\n").encode("utf-8")
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
        evidence_path.parent / "20260721_fast_volatility_max_preregistration.json"
    )
    data, archive_manifest, source_quality = load_archives(archive_dir)
    forecasts, forecast_quality = build_forecasts(data)
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
        "mechanism_id": "fast_volatility_max_v1",
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
                else "Reject fast_volatility_max_v1 and do not tune this family on the registered windows."
            ),
        },
        "limitations": [
            "Binance terminal direction is a proxy for the official Chainlink settlement source.",
            "The population contains every complete five-minute Binance window, not the strategy's gated Polymarket opportunities.",
            "Proper-score improvement cannot establish executable edge, fills, fee-inclusive PnL, breadth, or tail stability.",
            "The analysis intentionally excludes every known strict-42, July 14, July 15, and active forward-block outcome window.",
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
