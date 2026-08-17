#!/usr/bin/env python3
"""Evaluate the preregistered four-minute continuation rule on public BTC data."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import tempfile
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
REGISTRY = REPOSITORY_ROOT / "deploy/promotions/evidence/strategy_registry"
PREREGISTRATION = REGISTRY / "20260722_four_minute_continuation_preregistration.json"
OLDER_START = pd.Timestamp("2026-04-16T00:00:00Z")
OLDER_END = pd.Timestamp("2026-05-16T00:00:00Z")
FRESH_START = pd.Timestamp("2026-07-11T00:00:00Z")
FRESH_END = pd.Timestamp("2026-07-14T00:00:00Z")
CHECKPOINT_SECONDS = (60, 120, 180, 240, 300)
BOOTSTRAP_SEED = 20_260_722
BOOTSTRAP_RESAMPLES = 10_000


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


def selected_days() -> list[pd.Timestamp]:
    older = pd.date_range(OLDER_START, OLDER_END, inclusive="left", freq="1D")
    fresh = pd.date_range(FRESH_START, FRESH_END, inclusive="left", freq="1D")
    return list(older) + list(fresh)


def read_checksum(checksum_path: Path) -> tuple[str, str]:
    fields = checksum_path.read_text().strip().split()
    if len(fields) != 2:
        raise ValueError(f"invalid checksum file: {checksum_path}")
    return fields[0], fields[1].lstrip("*")


def load_archives(archive_dir: Path) -> tuple[pd.DataFrame, list[dict], dict]:
    frames: list[pd.DataFrame] = []
    manifest: list[dict] = []
    duplicate_timestamps = 0
    timestamp_regressions = 0
    one_second_gap_violations = 0
    maximum_internal_gap_seconds = 0.0
    invalid_close_durations = 0
    invalid_prices = 0
    schema_widths: set[int] = set()

    for day in selected_days():
        date_label = day.strftime("%Y-%m-%d")
        archive_path = archive_dir / f"BTCUSDT-1s-{date_label}.zip"
        checksum_path = archive_path.with_suffix(archive_path.suffix + ".CHECKSUM")
        if not archive_path.is_file() or not checksum_path.is_file():
            raise FileNotFoundError(f"missing archive or checksum for {date_label}")
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
        if frame.empty:
            raise ValueError(f"empty archive: {archive_path}")
        open_time_us = frame["open_time_us"].to_numpy()
        close_time_us = frame["close_time_us"].to_numpy()
        deltas = np.diff(open_time_us)
        durations = close_time_us - open_time_us
        prices = frame[["open_price", "close_price"]].to_numpy()
        duplicate_timestamps += int(np.sum(deltas == 0))
        timestamp_regressions += int(np.sum(deltas < 0))
        one_second_gap_violations += int(np.sum(deltas != 1_000_000))
        maximum_internal_gap_seconds = max(
            maximum_internal_gap_seconds,
            float(deltas.max() / 1_000_000) if len(deltas) else 0.0,
        )
        invalid_close_durations += int(np.sum(durations != 999_999))
        invalid_prices += int(np.sum(~np.isfinite(prices) | (prices <= 0)))
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
    quality = {
        "archives": len(manifest),
        "rows": int(len(data)),
        "schema_widths": sorted(schema_widths),
        "rows_per_archive_minimum": min(item["rows"] for item in manifest),
        "rows_per_archive_maximum": max(item["rows"] for item in manifest),
        "timestamp_unit": "microseconds",
        "timestamp_duplicates": duplicate_timestamps,
        "timestamp_regressions": timestamp_regressions,
        "one_second_gap_violations": one_second_gap_violations,
        "maximum_internal_gap_seconds": maximum_internal_gap_seconds,
        "invalid_close_durations": invalid_close_durations,
        "invalid_prices": invalid_prices,
        "checksum_failures": 0,
    }
    return data, manifest, quality


def build_windows(data: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    open_time_us = data["open_time_us"].to_numpy()
    close_time_us = data["close_time_us"].to_numpy()
    observed_second = ((close_time_us + 1) // 1_000_000).astype(np.int64)
    kline_open_second = (open_time_us // 1_000_000).astype(np.int64)
    window_start = (kline_open_second // 300) * 300
    elapsed_seconds = observed_second - window_start
    data = data.assign(
        window_start=window_start,
        elapsed_seconds=elapsed_seconds,
    )

    older_start_s = timestamp_seconds(OLDER_START)
    older_end_s = timestamp_seconds(OLDER_END)
    fresh_start_s = timestamp_seconds(FRESH_START)
    fresh_end_s = timestamp_seconds(FRESH_END)
    selected = (
        ((data["window_start"] >= older_start_s) & (data["window_start"] < older_end_s))
        | ((data["window_start"] >= fresh_start_s) & (data["window_start"] < fresh_end_s))
    )
    data = data.loc[selected]

    opens = data.loc[
        data["elapsed_seconds"] == 1,
        ["window_start", "open_price"],
    ].rename(columns={"open_price": "p0"})
    checkpoints = data.loc[
        data["elapsed_seconds"].isin(CHECKPOINT_SECONDS),
        ["window_start", "elapsed_seconds", "close_price"],
    ].pivot(index="window_start", columns="elapsed_seconds", values="close_price")
    checkpoints = checkpoints.rename(
        columns={60: "p60", 120: "p120", 180: "p180", 240: "p240", 300: "terminal"}
    ).reset_index()
    windows = opens.merge(checkpoints, on="window_start", validate="one_to_one")

    expected_conditions = int(
        (OLDER_END - OLDER_START).total_seconds() / 300
        + (FRESH_END - FRESH_START).total_seconds() / 300
    )
    if opens["window_start"].duplicated().any():
        raise ValueError("duplicate five-minute opening rows")
    if checkpoints["window_start"].duplicated().any():
        raise ValueError("duplicate five-minute checkpoint rows")

    prices = windows[["p0", "p60", "p120", "p180", "p240", "terminal"]]
    incomplete = ~prices.apply(np.isfinite).all(axis=1) | (prices <= 0).any(axis=1)
    windows = windows.loc[~incomplete].copy()
    windows["r1_usd"] = windows["p60"] - windows["p0"]
    windows["r2_usd"] = windows["p120"] - windows["p60"]
    windows["r3_usd"] = windows["p180"] - windows["p120"]
    windows["r4_usd"] = windows["p240"] - windows["p180"]
    returns = windows[["r1_usd", "r2_usd", "r3_usd", "r4_usd"]]
    windows["signal_up"] = (returns > 0).all(axis=1)
    windows["signal_down"] = (returns < 0).all(axis=1)
    windows["eligible"] = windows["signal_up"] | windows["signal_down"]
    windows["signal_direction"] = np.where(windows["signal_up"], "up", "down")
    windows["terminal_up"] = windows["terminal"] > windows["p0"]
    windows["terminal_tie"] = windows["terminal"] == windows["p0"]
    windows["terminal_direction"] = np.where(windows["terminal_up"], "up", "down")
    windows["won"] = windows["signal_direction"] == windows["terminal_direction"]
    windows["fifth_minute_return_usd"] = windows["terminal"] - windows["p240"]
    windows["fifth_minute_continued"] = np.where(
        windows["signal_up"],
        windows["fifth_minute_return_usd"] > 0,
        windows["fifth_minute_return_usd"] < 0,
    )
    windows["decision_margin_usd"] = np.where(
        windows["signal_up"],
        windows["p240"] - windows["p0"],
        windows["p0"] - windows["p240"],
    )
    windows["first_two_minute_move_usd"] = windows["p120"] - windows["p0"]
    windows["utc_timestamp"] = pd.to_datetime(windows["window_start"], unit="s", utc=True)
    windows["utc_day"] = windows["utc_timestamp"].dt.strftime("%Y-%m-%d")
    windows["utc_hour"] = windows["utc_timestamp"].dt.hour
    windows["weekday"] = windows["utc_timestamp"].dt.dayofweek < 5
    windows["chronological_window"] = np.where(
        windows["window_start"] < older_end_s,
        "older",
        "fresh_pre_forward",
    )

    quality = {
        "expected_conditions": expected_conditions,
        "complete_conditions": int(len(windows)),
        "complete_conditions_fraction": float(len(windows) / expected_conditions),
        "incomplete_conditions": int(incomplete.sum()),
        "terminal_tie_conditions": int(windows["terminal_tie"].sum()),
        "zero_one_minute_return_conditions": int((returns == 0).any(axis=1).sum()),
        "duplicate_conditions": int(windows["window_start"].duplicated().sum()),
        "conditions_by_chronological_window": {
            key: int(len(group))
            for key, group in windows.groupby("chronological_window", sort=True)
        },
    }
    return windows, quality


def score_signals(frame: pd.DataFrame) -> dict:
    eligible = frame.loc[frame["eligible"] & ~frame["terminal_tie"]]
    wins = int(eligible["won"].sum())
    signals = int(len(eligible))
    return {
        "conditions": int(len(frame)),
        "eligible_signals": signals,
        "signal_fraction": float(signals / len(frame)) if len(frame) else 0.0,
        "wins": wins,
        "losses": signals - wins,
        "accuracy": float(wins / signals) if signals else None,
    }


def score_magnitude(frame: pd.DataFrame, minimum_move_usd: float) -> dict:
    move = frame["first_two_minute_move_usd"]
    eligible = frame.loc[
        (move.abs() >= minimum_move_usd) & (move != 0) & ~frame["terminal_tie"]
    ].copy()
    predicted_up = eligible["first_two_minute_move_usd"] > 0
    won = predicted_up == eligible["terminal_up"]
    return {
        "minimum_absolute_move_usd": minimum_move_usd,
        "eligible_signals": int(len(eligible)),
        "wins": int(won.sum()),
        "losses": int((~won).sum()),
        "accuracy": float(won.mean()) if len(eligible) else None,
    }


def accuracy_by(frame: pd.DataFrame, column: str) -> dict:
    eligible = frame.loc[frame["eligible"] & ~frame["terminal_tie"]]
    return {
        str(key).lower(): score_signals(group)
        for key, group in eligible.groupby(column, sort=True)
    }


def day_block_bootstrap(frame: pd.DataFrame) -> dict:
    eligible = frame.loc[frame["eligible"] & ~frame["terminal_tie"]]
    daily = eligible.groupby("utc_day", sort=True).agg(
        wins=("won", "sum"),
        signals=("won", "size"),
    )
    if daily.empty:
        raise ValueError("no eligible days for bootstrap")
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    sampled = rng.integers(0, len(daily), size=(BOOTSTRAP_RESAMPLES, len(daily)))
    wins = daily["wins"].to_numpy()[sampled].sum(axis=1)
    signals = daily["signals"].to_numpy()[sampled].sum(axis=1)
    accuracy = wins / signals
    return {
        "unit": "UTC day",
        "days": int(len(daily)),
        "resamples": BOOTSTRAP_RESAMPLES,
        "seed": BOOTSTRAP_SEED,
        "accuracy_95pct": [float(value) for value in np.quantile(accuracy, [0.025, 0.975])],
    }


def longest_losing_streak(frame: pd.DataFrame) -> int:
    eligible = frame.loc[frame["eligible"] & ~frame["terminal_tie"]].sort_values(
        "window_start"
    )
    longest = 0
    current = 0
    for won in eligible["won"]:
        if won:
            current = 0
        else:
            current += 1
            longest = max(longest, current)
    return longest


def mechanism_decomposition(frame: pd.DataFrame) -> dict:
    eligible = frame.loc[frame["eligible"] & ~frame["terminal_tie"]]
    fifth_minute_ties = eligible["fifth_minute_return_usd"] == 0
    reversed_or_tied = ~eligible["fifth_minute_continued"]
    wins_despite_reversal = eligible["won"] & reversed_or_tied
    winning = eligible.loc[eligible["won"]]
    reversal = eligible.loc[reversed_or_tied]
    margin = eligible["decision_margin_usd"]
    return {
        "diagnostic_only": True,
        "eligible_signals": int(len(eligible)),
        "true_fifth_minute_continuations": int(
            eligible["fifth_minute_continued"].sum()
        ),
        "true_fifth_minute_continuation_rate": float(
            eligible["fifth_minute_continued"].mean()
        ),
        "fifth_minute_reversals_or_ties": int(reversed_or_tied.sum()),
        "fifth_minute_ties": int(fifth_minute_ties.sum()),
        "contract_wins_despite_fifth_minute_reversal_or_tie": int(
            wins_despite_reversal.sum()
        ),
        "fraction_of_contract_wins_despite_fifth_minute_reversal_or_tie": float(
            wins_despite_reversal.sum() / len(winning)
        ),
        "contract_accuracy_when_fifth_minute_reverses_or_ties": float(
            reversal["won"].mean()
        ),
        "directional_buffer_at_decision_usd": {
            "mean": float(margin.mean()),
            "median": float(margin.median()),
            "p10": float(margin.quantile(0.10)),
            "p90": float(margin.quantile(0.90)),
        },
        "interpretation": "The contract resolves against the five-minute open, so a profitable-direction label can survive a fifth-minute price reversal when the first-four-minute buffer is not fully erased."
    }


def evaluate_gates(results: dict, source_quality: dict, window_quality: dict) -> dict:
    overall = results["overall"]
    windows = results["chronological_windows"]
    directions = results["directions"]
    bootstrap = results["bootstrap"]
    checks = {
        "source_checksum_failures_zero": source_quality["checksum_failures"] == 0,
        "source_timestamp_duplicates_zero": source_quality["timestamp_duplicates"] == 0,
        "source_timestamp_regressions_zero": source_quality["timestamp_regressions"] == 0,
        "source_gaps_at_most_two_seconds": source_quality[
            "maximum_internal_gap_seconds"
        ]
        <= 2,
        "source_invalid_prices_zero": source_quality["invalid_prices"] == 0,
        "complete_conditions_at_least_99pct": window_quality[
            "complete_conditions_fraction"
        ]
        >= 0.99,
        "minimum_eligible_signals_older": windows["older"]["eligible_signals"] >= 800,
        "minimum_eligible_signals_fresh": windows["fresh_pre_forward"][
            "eligible_signals"
        ]
        >= 100,
        "overall_accuracy_at_least_0_60": overall["accuracy"] >= 0.60,
        "bootstrap_accuracy_lower_bound_above_0_55": bootstrap["accuracy_95pct"][0]
        > 0.55,
        "each_chronological_window_accuracy_above_0_55": all(
            score["accuracy"] > 0.55 for score in windows.values()
        ),
        "each_direction_accuracy_above_0_55": all(
            score["accuracy"] > 0.55 for score in directions.values()
        ),
    }
    claim_checks = {
        "strict_up_accuracy_at_least_0_90": directions["up"]["accuracy"] >= 0.90,
        "strict_down_accuracy_at_least_0_90": directions["down"]["accuracy"] >= 0.90,
    }
    return {
        "checks": checks,
        "passed": all(checks.values()),
        "failed_checks": [name for name, passed in checks.items() if not passed],
        "claim_replication_checks_report_only": claim_checks,
        "claim_replication_passed_report_only": all(claim_checks.values()),
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


def write_signal_snapshot(path: Path, windows: pd.DataFrame) -> dict:
    eligible = windows.loc[windows["eligible"] & ~windows["terminal_tie"]].copy()
    columns = [
        "window_start",
        "utc_day",
        "utc_hour",
        "weekday",
        "chronological_window",
        "signal_direction",
        "p0",
        "p60",
        "p120",
        "p180",
        "p240",
        "terminal",
        "r1_usd",
        "r2_usd",
        "r3_usd",
        "r4_usd",
        "first_two_minute_move_usd",
        "fifth_minute_return_usd",
        "fifth_minute_continued",
        "decision_margin_usd",
        "terminal_direction",
        "won",
    ]
    lines = [
        json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False)
        for record in eligible[columns].to_dict(orient="records")
    ]
    uncompressed = ("\n".join(lines) + "\n").encode("utf-8")
    compressed = gzip.compress(uncompressed, compresslevel=9, mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    temporary_path.write_bytes(compressed)
    os.replace(temporary_path, path)
    return {
        "path": portable_path(path),
        "rows": int(len(eligible)),
        "sha256": sha256_bytes(compressed),
        "uncompressed_sha256": sha256_bytes(uncompressed),
        "compressed_bytes": len(compressed),
        "uncompressed_bytes": len(uncompressed),
    }


def run(archive_dir: Path, evidence_path: Path, snapshot_path: Path) -> dict:
    data, archive_manifest, source_quality = load_archives(archive_dir)
    windows, window_quality = build_windows(data)
    results = {
        "overall": score_signals(windows),
        "chronological_windows": {
            key: score_signals(group)
            for key, group in windows.groupby("chronological_window", sort=True)
        },
        "directions": accuracy_by(windows, "signal_direction"),
        "weekday_weekend": accuracy_by(windows, "weekday"),
        "utc_hours": accuracy_by(windows, "utc_hour"),
        "article_magnitude_diagnostics": {
            "absolute_first_two_minute_move_at_least_100_usd": score_magnitude(
                windows, 100.0
            ),
            "absolute_first_two_minute_move_at_least_200_usd": score_magnitude(
                windows, 200.0
            ),
        },
        "bootstrap": day_block_bootstrap(windows),
        "longest_losing_streak": longest_losing_streak(windows),
        "mechanism_decomposition": mechanism_decomposition(windows),
    }
    gate_evaluation = evaluate_gates(results, source_quality, window_quality)
    snapshot = write_signal_snapshot(snapshot_path, windows)
    evidence = {
        "schema_version": 1,
        "status": (
            "PUBLIC_DIRECTIONAL_PROXY_PASS_REQUIRES_EXACT_POLYMARKET_ECONOMIC_SCREEN"
            if gate_evaluation["passed"]
            else "PUBLIC_DIRECTIONAL_PROXY_REJECTED_NO_RETUNING"
        ),
        "mechanism_id": "four_minute_continuation_v1",
        "authority": {
            "preregistration_path": portable_path(PREREGISTRATION),
            "preregistration_sha256": sha256_file(PREREGISTRATION),
            "archive_directory": str(archive_dir),
            "archive_manifest": archive_manifest,
            "signal_snapshot": snapshot,
        },
        "source_data_quality": source_quality,
        "window_data_quality": window_quality,
        "results": results,
        "gate_evaluation": gate_evaluation,
        "source_claim_audit": {
            "reported_strict_up_accuracy": 0.9683,
            "observed_strict_up_accuracy": results["directions"]["up"]["accuracy"],
            "reported_strict_down_accuracy": 0.9597,
            "observed_strict_down_accuracy": results["directions"]["down"]["accuracy"],
            "reported_first_two_minute_move_over_100_accuracy": 0.8858,
            "observed_first_two_minute_move_over_100_accuracy": results[
                "article_magnitude_diagnostics"
            ]["absolute_first_two_minute_move_at_least_100_usd"]["accuracy"],
            "reported_first_two_minute_move_over_200_accuracy": 0.9276,
            "observed_first_two_minute_move_over_200_accuracy": results[
                "article_magnitude_diagnostics"
            ]["absolute_first_two_minute_move_at_least_200_usd"]["accuracy"],
            "reported_full_strategy_accuracy": 0.7846,
            "observed_full_strategy_accuracy": None,
            "full_strategy_not_reproducible_reason": "the article does not define how strict consistency and magnitude rules are combined",
            "reported_maximum_losing_streak": 6,
            "observed_directional_proxy_longest_losing_streak": results[
                "longest_losing_streak"
            ],
            "reported_maximum_drawdown": 0.005,
            "observed_maximum_drawdown": None,
            "drawdown_not_reproducible_reason": "the article does not define entry prices, order sizes, bankroll path, fills, fees, or slippage"
        },
        "economic_audit": {
            "reported_fixed_breakeven_accuracy": 0.5102,
            "fixed_breakeven_is_valid": False,
            "correct_taker_break_even_per_share": "executable_fill_price + market_fee_rate * executable_fill_price * (1 - executable_fill_price)",
            "directional_proxy_contains_polymarket_prices": False,
            "directional_proxy_contains_fills_or_slippage": False,
            "profitability_established": False
        },
        "decision": {
            "public_directional_proxy_passed": gate_evaluation["passed"],
            "source_article_claim_replicated": gate_evaluation[
                "claim_replication_passed_report_only"
            ],
            "default_off_feature_authorized": gate_evaluation["passed"],
            "exact_polymarket_replay_authorized": gate_evaluation["passed"],
            "runtime_change_authorized": False,
            "paper_or_live_trading_authorized": False,
            "profitability_claim": False,
            "a_plus_claim": False,
            "next_step": (
                "Add one default-off four_minute_consistency opportunity feature and run the preregistered 202 ms exact-L2 Polymarket economic screen."
                if gate_evaluation["passed"]
                else "Reject four_minute_continuation_v1 without tuning this family on the registered labels."
            )
        },
        "integrity": {
            "active_binary_complement_outcomes_accessed": False,
            "active_binary_complement_strategy_metrics_accessed": False,
            "july_14_or_later_archive_opened": False,
            "strategy_or_runtime_changed": False,
            "paper_or_live_behavior_changed": False
        },
        "limitations": [
            "Binance terminal direction is only a public proxy for authoritative Polymarket resolution.",
            "The public proxy cannot measure executable token price, queue position, fill probability, book-walk slippage, fees, or net PnL.",
            "The literal four-checkpoint interpretation is frozen because the article does not publish code or a complete formal rule.",
            "The July 14 and later archives are deliberately unopened to preserve the active binary-complement outcome seal."
        ]
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
                "eligible_signals": evidence["results"]["overall"][
                    "eligible_signals"
                ],
                "accuracy": evidence["results"]["overall"]["accuracy"],
                "accuracy_95pct": evidence["results"]["bootstrap"][
                    "accuracy_95pct"
                ],
                "failed_checks": evidence["gate_evaluation"]["failed_checks"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
