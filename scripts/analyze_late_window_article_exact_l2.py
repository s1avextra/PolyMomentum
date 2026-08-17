#!/usr/bin/env python3
"""Aggregate exact-L2 article-family trade reports without retuning."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from analyze_four_minute_continuation import REPOSITORY_ROOT, portable_path, write_json_atomic


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def profit_factor(values: pd.Series) -> float | None:
    positive = float(values.loc[values > 0].sum())
    negative = float(-values.loc[values < 0].sum())
    return positive / negative if negative > 0 else None


def maximum_drawdown(values: pd.Series, bankroll: float = 100.0) -> dict:
    equity = bankroll + values.cumsum()
    running_high = equity.cummax().clip(lower=bankroll)
    drawdown = running_high - equity
    drawdown_fraction = drawdown / running_high
    return {
        "usd": float(drawdown.max()) if len(drawdown) else 0.0,
        "fraction": float(drawdown_fraction.max()) if len(drawdown_fraction) else 0.0,
    }


def summarize_trades(frame: pd.DataFrame, summaries: list[dict]) -> dict:
    frame = frame.sort_values("fill_timestamp_s")
    pnl = frame["pnl_after_fee"]
    wins = int(frame["won"].sum())
    attempts = sum(item["fills_success"] + item["fills_failed"] for item in summaries)
    average_full_loss = float((frame["cost"] + frame["fee"]).mean()) if len(frame) else None
    winning = pnl.loc[pnl > 0]
    losing = pnl.loc[pnl < 0]
    return {
        "execution_attempts": attempts,
        "fills": int(len(frame)),
        "fill_failures": sum(item["fills_failed"] for item in summaries),
        "fill_rate": float(len(frame) / attempts) if attempts else None,
        "wins": wins,
        "losses": int(len(frame) - wins),
        "win_rate": float(wins / len(frame)) if len(frame) else None,
        "total_pnl_usd": float(pnl.sum()),
        "mean_pnl_per_trade_usd": float(pnl.mean()) if len(frame) else None,
        "profit_factor": profit_factor(pnl),
        "mean_winner_usd": float(winning.mean()) if len(winning) else None,
        "mean_loser_usd": float(losing.mean()) if len(losing) else None,
        "payoff_ratio": float(winning.mean() / -losing.mean())
        if len(winning) and len(losing)
        else None,
        "total_fees_usd": float(frame["fee"].sum()),
        "fill_price": {
            "mean": float(frame["fill_price"].mean()),
            "median": float(frame["fill_price"].median()),
            "p90": float(frame["fill_price"].quantile(0.90)),
            "maximum": float(frame["fill_price"].max()),
        }
        if len(frame)
        else None,
        "bookwalk_slippage": {
            "mean": float(frame["slippage"].mean()),
            "maximum": float(frame["slippage"].max()),
        }
        if len(frame)
        else None,
        "insertion_latency_ms": {
            "minimum": float(frame["insertion_latency_ms"].min()),
            "median": float(frame["insertion_latency_ms"].median()),
            "maximum": float(frame["insertion_latency_ms"].max()),
        }
        if len(frame)
        else None,
        "maximum_drawdown": maximum_drawdown(pnl),
        "unresolved_fills": sum(item["unresolved_fills"] for item in summaries),
        "resolution_disagreements": int(frame["resolution_disagreed"].sum()),
        "average_full_loss_at_observed_size_usd": average_full_loss,
        "pnl_after_one_additional_average_loss_usd": float(pnl.sum() - average_full_loss)
        if average_full_loss is not None
        else None,
    }


def run(paths: list[Path], output: Path, phase: str, variant_path: Path) -> dict:
    records: dict[str, list[dict]] = {}
    summaries: dict[str, list[dict]] = {}
    folds: dict[str, list[dict]] = {}
    sources: list[dict] = []
    for path in paths:
        payload = json.loads(path.read_text())
        fold = f"{payload['start']}__{payload['end']}"
        sources.append(
            {
                "path": portable_path(path),
                "sha256": sha256_file(path),
                "start": payload["start"],
                "end": payload["end"],
                "latency_ms": payload["latency_ms"],
            }
        )
        for variant in payload["variants"]:
            name = variant["strategy_name"]
            summary = variant["summary"]
            summaries.setdefault(name, []).append(summary)
            folds.setdefault(name, []).append(
                {
                    "fold": fold,
                    "trades": summary["trades"],
                    "wins": summary["wins"],
                    "losses": summary["losses"],
                    "total_pnl_usd": summary["total_pnl"],
                    "fill_failures": summary["fills_failed"],
                    "unresolved_fills": summary["unresolved_fills"],
                }
            )
            for trade in variant["trades"]:
                records.setdefault(name, []).append(
                    {
                        "condition_id": trade["fill"]["order"]["condition_id"],
                        "direction": trade["decision"]["direction"],
                        "won": trade["won"],
                        "pnl_after_fee": trade["pnl_after_fee"],
                        "fee": trade["fill"]["fee"],
                        "cost": trade["fill"]["cost"],
                        "fill_price": trade["fill"]["fill_price"],
                        "fill_timestamp_s": trade["fill"]["fill_timestamp_s"],
                        "slippage": trade["fill"]["slippage"],
                        "insertion_latency_ms": (
                            trade["fill"]["fill_timestamp_s"]
                            - trade["fill"]["order"]["timestamp_s"]
                        )
                        * 1000.0,
                        "resolution_disagreed": trade["resolution_disagreed"],
                    }
                )

    results: dict[str, dict] = {}
    for name, strategy_records in records.items():
        frame = pd.DataFrame.from_records(strategy_records)
        overall = summarize_trades(frame, summaries[name])
        by_direction = {
            direction: {
                "trades": int(len(group)),
                "wins": int(group["won"].sum()),
                "losses": int(len(group) - group["won"].sum()),
                "total_pnl_usd": float(group["pnl_after_fee"].sum()),
            }
            for direction, group in frame.groupby("direction")
        }
        results[name] = {
            "overall": overall,
            "folds": folds[name],
            "by_direction": by_direction,
            "positive_pnl_folds": sum(item["total_pnl_usd"] > 0 for item in folds[name]),
            "negative_pnl_folds": sum(item["total_pnl_usd"] < 0 for item in folds[name]),
        }

    path_3m = results.get("late_window_path_3m_literal_taker")
    hybrid = results.get("late_window_path_3m_and_move_2m_100_literal_taker")
    hybrid_progresses = bool(
        hybrid
        and hybrid["overall"]["total_pnl_usd"] > 0
        and hybrid["negative_pnl_folds"] == 0
        and hybrid["overall"]["unresolved_fills"] == 0
        and hybrid["overall"]["fill_rate"] >= 0.8
    )
    evidence = {
        "schema_version": 1,
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "mechanism_id": "late_window_article_family_v1",
        "phase": phase,
        "status": "EXACT_L2_DISCOVERY_COMPLETE" if phase == "discovery" else "EXACT_L2_HOLDOUT_COMPLETE",
        "authority": {
            "trade_reports": sources,
            "variant_path": portable_path(variant_path),
            "variant_sha256": sha256_file(variant_path),
        },
        "execution_contract": {
            "latency_ms": 202,
            "order": "taker FOK visible-depth book walk",
            "fee": "market metadata with engine fallback",
            "position_pct": 0.05,
            "maximum_per_market_usd": 10.0,
            "maximum_projected_stressed_drawdown_fraction": 0.12,
            "fair_value_or_edge_gate_used": False,
        },
        "results": results,
        "decision": {
            "path_3m_rejected": bool(path_3m and path_3m["overall"]["total_pnl_usd"] <= 0),
            "path_3m_and_move_2m_100_progresses": hybrid_progresses,
            "progression_scope": "next chronological exact-L2 holdout only" if phase == "discovery" else "research decision",
            "runtime_paper_or_live_change_authorized": False,
            "profitability_or_a_plus_claim": False,
        },
        "limitations": [
            "Each eight-hour fold resets bankroll and strategy state; aggregate drawdown is a stitched diagnostic, not one continuous bankroll replay.",
            "The selected hybrid has extremely asymmetric payoff near probability one; zero observed losses makes profit factor and payoff ratio unidentified.",
            "Historical exact-L2 evidence is not forward evidence and does not authorize paper or live trading.",
        ],
    }
    write_json_atomic(output, evidence)
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-report", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--phase", choices=("discovery", "holdout"), required=True)
    parser.add_argument(
        "--variant-path",
        type=Path,
        default=REPOSITORY_ROOT
        / "deploy/promotions/evidence/strategy_registry/20260722_late_window_article_family_exact_l2_variants.json",
    )
    args = parser.parse_args()
    evidence = run(args.trade_report, args.output, args.phase, args.variant_path.resolve())
    print(json.dumps({"status": evidence["status"], "results": evidence["results"], "decision": evidence["decision"]}, indent=2))


if __name__ == "__main__":
    main()
