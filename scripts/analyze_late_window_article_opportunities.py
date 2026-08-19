#!/usr/bin/env python3
"""Rank the frozen article family on one shared top-of-book opportunity capture."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from analyze_four_minute_continuation import REPOSITORY_ROOT, portable_path, write_json_atomic


REGISTRY = REPOSITORY_ROOT / "deploy/promotions/evidence/strategy_registry"
PUBLIC_SCREEN = REGISTRY / "20260722_late_window_article_family_public_screen.json"


RULES = {
    "path_3m": {
        "decision_offset_seconds": 180,
        "required": {"article_path_3m_alignment": {"aligned"}},
    },
    "path_4m": {
        "decision_offset_seconds": 240,
        "required": {"article_path_4m_alignment": {"aligned"}},
    },
    "move_2m_100": {
        "decision_offset_seconds": 120,
        "required": {
            "article_move_2m_alignment": {"aligned_100_200", "aligned_ge_200"}
        },
    },
    "move_2m_200": {
        "decision_offset_seconds": 120,
        "required": {"article_move_2m_alignment": {"aligned_ge_200"}},
    },
    "path_3m_and_move_2m_100": {
        "decision_offset_seconds": 180,
        "required": {
            "article_path_3m_alignment": {"aligned"},
            "article_move_2m_alignment": {"aligned_100_200", "aligned_ge_200"},
        },
    },
    "path_4m_and_move_2m_100": {
        "decision_offset_seconds": 240,
        "required": {
            "article_path_4m_alignment": {"aligned"},
            "article_move_2m_alignment": {"aligned_100_200", "aligned_ge_200"},
        },
    },
    "path_4m_or_move_2m_200_aligned": {
        "decision_offset_seconds": 240,
        "required": {
            "article_path_4m_or_move_2m_200_alignment": {"aligned"}
        },
    },
}


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_rows(paths: list[Path]) -> tuple[pd.DataFrame, list[dict]]:
    records: list[dict] = []
    sources: list[dict] = []
    for path in paths:
        payload = json.loads(path.read_text())
        sources.append(
            {
                "path": portable_path(path),
                "sha256": sha256_file(path),
                "rows": int(payload["row_count"]),
                "start": payload["start"],
                "end": payload["end"],
                "latency_ms": payload["latency_ms"],
            }
        )
        for wrapped in payload["rows"]:
            opportunity = wrapped["opportunity"]
            decision = opportunity["decision"]
            regime = decision["regime"]
            ask = opportunity.get("chosen_best_ask")
            if ask is None or not (0.0 < ask <= 1.0):
                continue
            fee_rate = opportunity["entry_fee_rate"]
            fee = fee_rate * ask * (1.0 - ask)
            records.append(
                {
                    "source_path": str(path),
                    "condition_id": opportunity["condition_id"],
                    "timestamp_s": opportunity["decision_timestamp_s"],
                    "utc_day": pd.to_datetime(
                        opportunity["decision_timestamp_s"], unit="s", utc=True
                    ).strftime("%Y-%m-%d"),
                    "elapsed_seconds": (5.0 - decision["minutes_remaining"]) * 60.0,
                    "direction": decision["direction"],
                    "won": bool(opportunity["won"]),
                    "ask": ask,
                    "ask_depth": opportunity.get("chosen_ask_depth"),
                    "fee_rate": fee_rate,
                    "fee_per_share": fee,
                    "cost_per_share": ask + fee,
                    "one_share_payoff": (1.0 if opportunity["won"] else 0.0)
                    - ask
                    - fee,
                    **{
                        key: regime.get(key)
                        for rule in RULES.values()
                        for key in rule["required"]
                    },
                }
            )
    if not records:
        raise ValueError("no valid top-of-book opportunities")
    return pd.DataFrame.from_records(records), sources


def rule_rows(all_rows: pd.DataFrame, rule: dict) -> pd.DataFrame:
    selected = all_rows.loc[
        all_rows["elapsed_seconds"] + 1e-6 >= rule["decision_offset_seconds"]
    ].copy()
    for field, allowed in rule["required"].items():
        selected = selected.loc[selected[field].isin(allowed)]
    return (
        selected.sort_values(["condition_id", "timestamp_s"])
        .groupby("condition_id", as_index=False, sort=False)
        .first()
    )


def score(frame: pd.DataFrame) -> dict:
    count = int(len(frame))
    wins = int(frame["won"].sum())
    positive = float(frame.loc[frame["one_share_payoff"] > 0, "one_share_payoff"].sum())
    negative = float(-frame.loc[frame["one_share_payoff"] < 0, "one_share_payoff"].sum())
    return {
        "conditions": count,
        "wins": wins,
        "losses": count - wins,
        "accuracy": float(wins / count) if count else None,
        "mean_best_ask": float(frame["ask"].mean()) if count else None,
        "median_best_ask": float(frame["ask"].median()) if count else None,
        "mean_fee_aware_cost_per_share": float(frame["cost_per_share"].mean())
        if count
        else None,
        "net_one_share_payoff": float(frame["one_share_payoff"].sum()),
        "mean_one_share_payoff": float(frame["one_share_payoff"].mean())
        if count
        else None,
        "unit_profit_factor": positive / negative if negative > 0 else None,
        "positive_unit_payoff": positive,
        "negative_unit_payoff": negative,
    }


def run(paths: list[Path], output: Path) -> dict:
    all_rows, sources = load_rows(paths)
    public = json.loads(PUBLIC_SCREEN.read_text())
    survivors = set(public["stage_1"]["survivors"])
    results: dict[str, dict] = {}
    for rule_id, rule in RULES.items():
        if rule_id not in survivors:
            continue
        frame = rule_rows(all_rows, rule)
        overall = score(frame)
        results[rule_id] = {
            "decision_offset_seconds": rule["decision_offset_seconds"],
            "overall": overall,
            "by_direction": {
                str(key): score(group) for key, group in frame.groupby("direction")
            },
            "by_utc_day": {
                str(key): score(group) for key, group in frame.groupby("utc_day")
            },
            "cheap_screen_only": True,
        }

    rankable = [
        (rule_id, result)
        for rule_id, result in results.items()
        if result["overall"]["conditions"] >= 10
        and result["overall"]["mean_one_share_payoff"] is not None
        and result["overall"]["mean_one_share_payoff"] > 0.0
    ]
    rankable.sort(
        key=lambda item: (
            item[1]["overall"]["mean_one_share_payoff"],
            item[1]["overall"]["conditions"],
            -item[1]["decision_offset_seconds"],
        ),
        reverse=True,
    )
    shortlist = [rule_id for rule_id, _ in rankable[:2]]
    evidence = {
        "schema_version": 1,
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "mechanism_id": "late_window_article_family_v1",
        "status": "STAGE_2_TOP_OF_BOOK_SCREEN_COMPLETE",
        "authority": {
            "public_screen_path": portable_path(PUBLIC_SCREEN),
            "public_screen_sha256": sha256_file(PUBLIC_SCREEN),
            "opportunity_sources": sources,
        },
        "sampling": {
            "one_row_per_rule_condition": "earliest causal row at or after the frozen decision offset",
            "input_rows": int(len(all_rows)),
            "latency_applied_to_quote": False,
            "visible_depth_bookwalk_applied": False,
            "fee_formula": "rate * ask * (1 - ask)",
            "payoff_formula": "outcome - ask - fee",
        },
        "results": results,
        "stage_2": {
            "exact_l2_shortlist": shortlist,
            "maximum_shortlist": 2,
            "ranking": "positive mean one-share payoff, then support, then earlier decision",
            "no_shortlist_reason": None
            if shortlist
            else "No rule with at least ten observed conditions had positive top-of-book one-share payoff.",
        },
        "decision": {
            "exact_l2_replay_authorized": bool(shortlist),
            "runtime_paper_or_live_change_authorized": False,
            "profitability_or_a_plus_claim": False,
        },
        "limitations": [
            "The quote is contemporaneous top-of-book, not the 202 ms post-signal visible-depth VWAP.",
            "One-share payoff ignores the engine's FOK size optimization, exposure state, and breaker.",
            "This stage intentionally ranks candidates but cannot establish executable profitability.",
        ],
    }
    write_json_atomic(output, evidence)
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--opportunities", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    evidence = run(args.opportunities, args.output)
    print(
        json.dumps(
            {
                "status": evidence["status"],
                "shortlist": evidence["stage_2"]["exact_l2_shortlist"],
                "results": {
                    rule_id: result["overall"]
                    for rule_id, result in evidence["results"].items()
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
