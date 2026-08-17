#!/usr/bin/env python3
"""Validate the late-window article-family decision and report snapshot."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "deploy/promotions/evidence/strategy_registry"


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def close(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-12)


def main() -> None:
    checks = 0
    decision = load(REGISTRY / "20260722_late_window_article_family_decision.json")
    public = load(REGISTRY / "20260722_late_window_article_family_public_screen.json")
    topbook = load(REGISTRY / "20260722_late_window_article_family_top_of_book_screen.json")
    discovery = load(REGISTRY / "20260722_late_window_article_family_exact_l2_discovery.json")
    holdout = load(REGISTRY / "20260722_late_window_path_3m_move100_exact_l2_holdout.json")
    holdout_gate = load(REGISTRY / "20260722_late_window_path_3m_move100_holdout_decision.json")
    support = load(REGISTRY / "20260722_binary_complement_support_only_status_352.json")
    report = load(ROOT / "docs/reports/late_window_strategy_decision_2026-07-22.artifact.json")

    for authority in decision["authority"]:
        path = ROOT / authority["path"]
        assert path.is_file() and sha256(path) == authority["sha256"], authority["path"]
        checks += 1

    for row in decision["public_screen"]:
        source = public["results"][row["rule"]]
        assert row["signals"] == source["overall"]["eligible_signals"]
        assert close(row["accuracy"], source["overall"]["accuracy"])
        assert row["fresh_pre_forward_signals"] == source["chronological_windows"]["fresh_pre_forward"]["eligible_signals"]
        assert close(row["fresh_pre_forward_accuracy"], source["chronological_windows"]["fresh_pre_forward"]["accuracy"])
        assert close(row["post_decision_continuation_rate"], source["post_decision_continuation_rate"])
        checks += 5

    for row in decision["top_of_book_screen"]:
        source = topbook["results"][row["rule"]]["overall"]
        assert row["conditions"] == source["conditions"]
        assert close(row["accuracy"], source["accuracy"])
        assert close(row["mean_fee_aware_cost_per_share"], source["mean_fee_aware_cost_per_share"])
        assert close(row["mean_net_payoff_per_share"], source["mean_one_share_payoff"])
        if row["unit_profit_factor"] is None:
            assert source["unit_profit_factor"] is None
        else:
            assert close(row["unit_profit_factor"], source["unit_profit_factor"])
        checks += 5

    exact = decision["exact_l2_results"]
    plain = discovery["results"]["late_window_path_3m_literal_taker"]["overall"]
    hybrid_discovery = discovery["results"]["late_window_path_3m_and_move_2m_100_literal_taker"]["overall"]
    hybrid_holdout = holdout["results"]["late_window_path_3m_and_move_2m_100_literal_taker"]["overall"]
    for row, source in zip(exact, (plain, hybrid_discovery, hybrid_holdout), strict=True):
        assert row["fills"] == source["fills"]
        assert row["wins"] == source["wins"]
        assert row["losses"] == source["losses"]
        assert close(row["net_pnl_usd"], source["total_pnl_usd"])
        assert close(row["mean_fill_price"], source["fill_price"]["mean"])
        assert close(
            row["pnl_after_one_average_full_loss_usd"],
            source["pnl_after_one_additional_average_loss_usd"],
        )
        checks += 6

    assert holdout_gate["integrity"]["passed"] is True
    assert holdout_gate["decision"]["progression_passed"] is False
    failed_gates = [name for name, gate in holdout_gate["gates"].items() if not gate["passed"]]
    assert failed_gates == ["loss_robustness"]
    checks += 3

    assert support["floor"]["unique_ready_terminal_conditions"] == 352
    assert support["floor"]["remaining_terminal_conditions"] == 398
    assert support["blindness"]["active_forward_outcomes_inspected"] is False
    assert support["blindness"]["active_forward_rates_or_economics_inspected"] is False
    checks += 4

    headline = report["snapshot"]["datasets"]["headline"][0]
    assert close(headline["plain_path_3m_pnl_usd"], plain["total_pnl_usd"])
    assert close(headline["hybrid_holdout_pnl_usd"], hybrid_holdout["total_pnl_usd"])
    assert close(
        headline["hybrid_one_loss_stress_pnl_usd"],
        hybrid_holdout["pnl_after_one_additional_average_loss_usd"],
    )
    assert headline["binary_complement_support"] == support["floor"]["unique_ready_terminal_conditions"]
    assert report["manifest"]["blocks"][0]["body"] == f"# {report['manifest']['title']}"
    checks += 5

    print(json.dumps({"status": "PASS", "checks": checks}, indent=2))


if __name__ == "__main__":
    main()
