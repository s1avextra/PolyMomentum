#!/usr/bin/env python3
"""Validate the July 18 strategy decision notebook and portable report inputs."""

from __future__ import annotations

import hashlib
import gzip
import json
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "deploy/promotions/evidence/strategy_registry"
REPORT = ROOT / "docs/reports/strategy_a_plus_decision_2026-07-18.artifact.json"
HTML = ROOT / "docs/reports/strategy_a_plus_decision_2026-07-18.html"
NOTEBOOK = ROOT / "docs/notebooks/strategy_portfolio_power_decision_2026-07-18.ipynb"
PAIRED_PRESSURE_NOTEBOOK = (
    ROOT / "docs/notebooks/strategy_paired_book_pressure_redundancy_2026-07-21.ipynb"
)
RESIDUAL_INDEPENDENCE_NOTEBOOK = (
    ROOT / "docs/notebooks/strategy_complement_residual_independence_2026-07-21.ipynb"
)
RESIDUAL_CROSS_MARKET_NOTEBOOK = (
    ROOT
    / "docs/notebooks/strategy_complement_residual_cross_market_replication_2026-07-21.ipynb"
)
PAIRED_DEPTH_NOTEBOOK = (
    ROOT / "docs/notebooks/strategy_paired_depth_capacity_diagnostic_2026-07-21.ipynb"
)
PAIRED_WAIT_COST_NOTEBOOK = (
    ROOT / "docs/notebooks/strategy_paired_book_wait_cost_diagnostic_2026-07-21.ipynb"
)
SETTLEMENT_ANCHOR_NOTEBOOK = (
    ROOT / "docs/notebooks/strategy_settlement_source_anchor_diagnostic_2026-07-21.ipynb"
)
SETTLEMENT_ANCHOR_HISTORICAL_OUTCOME_NOTEBOOK = (
    ROOT
    / "docs/notebooks/strategy_settlement_source_anchor_historical_outcome_diagnostic_2026-07-21.ipynb"
)
FAST_VOLATILITY_NOTEBOOK = (
    ROOT
    / "docs/notebooks/strategy_fast_volatility_max_public_calibration_2026-07-21.ipynb"
)
EMPIRICAL_CDF_NOTEBOOK = (
    ROOT
    / "docs/notebooks/strategy_empirical_return_cdf_public_calibration_2026-07-21.ipynb"
)
NON_CANDLE_SNAPSHOT = (
    REGISTRY / "source_snapshots/20260721_non_candle_public_books.json.gz"
)


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def close(actual: float, expected: float, tolerance: float = 1e-9) -> None:
    if not math.isclose(actual, expected, rel_tol=tolerance, abs_tol=tolerance):
        raise AssertionError(f"{actual} != {expected}")


def binomial_tail(n: int, probability: float, minimum: int) -> float:
    return sum(
        math.comb(n, successes)
        * probability**successes
        * (1.0 - probability) ** (n - successes)
        for successes in range(minimum, n + 1)
    )


def recompute_cross_market_snapshot(snapshot: dict) -> dict:
    """Independently recompute the public paired-book headline metrics."""

    def book_metrics(book: dict) -> dict | None:
        bids = sorted(
            (
                (float(level["price"]), float(level["size"]))
                for level in book["bids"]
                if float(level["size"]) > 0
            ),
            reverse=True,
        )
        asks = sorted(
            (
                (float(level["price"]), float(level["size"]))
                for level in book["asks"]
                if float(level["size"]) > 0
            )
        )
        if not bids or not asks:
            return None
        best_bid = bids[0][0]
        best_ask = asks[0][0]
        if not 0 < best_bid < best_ask < 1:
            return None
        bid_depth = sum(size for _, size in bids[:3])
        ask_depth = sum(size for _, size in asks[:3])
        microprice = (best_ask * bid_depth + best_bid * ask_depth) / (
            bid_depth + ask_depth
        )
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "midpoint": (best_bid + best_ask) / 2,
            "microprice": microprice,
            "timestamp_ms": int(book["timestamp"]),
            "tick_size": float(book["tick_size"]),
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "bid_notional": sum(price * size for price, size in bids[:3]),
            "ask_notional": sum(price * size for price, size in asks[:3]),
        }

    possible = 0
    valid = 0
    invalid = 0
    registered_rejections = 0
    strict_rejections = 0
    maximum_residual = 0.0
    maximum_timestamp_skew_ms = 0
    matched_depth_ratio_passes = 0
    both_entry_capacity_passes = 0
    four_side_capacity_passes = 0
    yes_entry_capacity_passes = 0
    no_entry_capacity_passes = 0
    opposite_incremental_if_yes_chosen = 0
    opposite_incremental_if_no_chosen = 0
    four_side_passes_by_condition: dict[str, list[bool]] = {}
    for round_data in snapshot["rounds"]:
        books = {str(book["asset_id"]): book for book in round_data["books"]}
        for market in snapshot["markets"]:
            possible += 1
            yes_book = books[market["token_ids"][0]]
            no_book = books[market["token_ids"][1]]
            yes = book_metrics(yes_book)
            no = book_metrics(no_book)
            if yes is None or no is None:
                invalid += 1
                continue
            assert str(yes_book["market"]) == market["condition_id"]
            assert str(no_book["market"]) == market["condition_id"]
            valid += 1
            midpoint_residual = yes["midpoint"] + no["midpoint"] - 1
            microprice_residual = yes["microprice"] + no["microprice"] - 1
            maximum = max(abs(midpoint_residual), abs(microprice_residual))
            declared_tick = max(yes["tick_size"], no["tick_size"])
            band_tick = (
                0.001
                if min(
                    yes["best_bid"],
                    yes["best_ask"],
                    no["best_bid"],
                    no["best_ask"],
                )
                < 0.04
                or max(
                    yes["best_bid"],
                    yes["best_ask"],
                    no["best_bid"],
                    no["best_ask"],
                )
                > 0.96
                else 0.01
            )
            effective_tick = min(declared_tick, band_tick)
            registered_rejections += maximum > 2 * effective_tick + 1e-12
            strict_rejections += maximum > 0.002 + 1e-12
            maximum_residual = max(maximum_residual, maximum)
            maximum_timestamp_skew_ms = max(
                maximum_timestamp_skew_ms,
                abs(yes["timestamp_ms"] - no["timestamp_ms"]),
            )
            matched_ratios = (
                min(yes["bid_depth"], no["ask_depth"])
                / max(yes["bid_depth"], no["ask_depth"]),
                min(yes["ask_depth"], no["bid_depth"])
                / max(yes["ask_depth"], no["bid_depth"]),
            )
            matched_depth_ratio_passes += min(matched_ratios) >= 0.25
            yes_entry_pass = yes["ask_notional"] >= 10
            no_entry_pass = no["ask_notional"] >= 10
            both_entry_pass = yes_entry_pass and no_entry_pass
            four_side_pass = min(
                yes["bid_notional"],
                yes["ask_notional"],
                no["bid_notional"],
                no["ask_notional"],
            ) >= 10
            yes_entry_capacity_passes += yes_entry_pass
            no_entry_capacity_passes += no_entry_pass
            both_entry_capacity_passes += both_entry_pass
            four_side_capacity_passes += four_side_pass
            opposite_incremental_if_yes_chosen += yes_entry_pass and not both_entry_pass
            opposite_incremental_if_no_chosen += no_entry_pass and not both_entry_pass
            four_side_passes_by_condition.setdefault(market["condition_id"], []).append(
                four_side_pass
            )
    complete_five_round = [
        passes for passes in four_side_passes_by_condition.values() if len(passes) == 5
    ]
    return {
        "possible": possible,
        "valid": valid,
        "invalid": invalid,
        "registered_rejections": registered_rejections,
        "strict_rejections": strict_rejections,
        "maximum_residual": maximum_residual,
        "maximum_timestamp_skew_ms": maximum_timestamp_skew_ms,
        "matched_depth_ratio_passes": matched_depth_ratio_passes,
        "both_entry_capacity_passes": both_entry_capacity_passes,
        "four_side_capacity_passes": four_side_capacity_passes,
        "yes_entry_capacity_passes": yes_entry_capacity_passes,
        "no_entry_capacity_passes": no_entry_capacity_passes,
        "opposite_incremental_if_yes_chosen": opposite_incremental_if_yes_chosen,
        "opposite_incremental_if_no_chosen": opposite_incremental_if_no_chosen,
        "complete_five_round_markets": len(complete_five_round),
        "four_side_always_pass": sum(all(passes) for passes in complete_five_round),
        "four_side_always_fail": sum(not any(passes) for passes in complete_five_round),
        "four_side_mixed": sum(any(passes) and not all(passes) for passes in complete_five_round),
    }


def main() -> None:
    strict42 = load(REGISTRY / "20260718_forward_window_admissibility_pipeline.json")
    strict42_exact = load(
        REGISTRY / "20260714_volatility_floor_strict42_latency202_exact_aggregate.json"
    )
    v1 = load(REGISTRY / "20260718_complete_set_lock_v1_historical_tail_diagnostic.json")
    v2 = load(
        REGISTRY / "20260718_trailing_complete_set_lock_v2_historical_tail_diagnostic.json"
    )
    power = load(REGISTRY / "20260718_binary_complement_blinded_power_amendment.json")
    realized_support = load(
        REGISTRY / "20260721_binary_complement_realized_support_amendment.json"
    )
    preregistration = load(
        REGISTRY / "20260715_binary_complement_coherence_preregistration.json"
    )
    collection = load(REGISTRY / "20260718_binary_complement_block1_collection_status.json")
    current_collection = load(
        REGISTRY / "20260720_binary_complement_floor_recovery_status.json"
    )
    support_310 = load(
        REGISTRY / "20260721_binary_complement_support_only_status_310.json"
    )
    support_345 = load(
        REGISTRY / "20260721_binary_complement_support_only_status_345.json"
    )
    vps_restart = load(REGISTRY / "20260721_polymomentum_vps_restart_status.json")
    strategy_diagnostic = load(
        REGISTRY / "20260720_strategy_overconfidence_and_microstructure_audit.json"
    )
    paired_pressure = load(
        REGISTRY / "20260721_paired_book_pressure_redundancy_diagnostic.json"
    )
    residual_independence = load(
        REGISTRY
        / "20260721_binary_complement_residual_independence_diagnostic.json"
    )
    residual_cross_market = load(
        REGISTRY
        / "20260721_binary_complement_residual_cross_market_replication.json"
    )
    residual_attribution = load(
        REGISTRY
        / "20260721_binary_complement_residual_attribution_amendment.json"
    )
    tick_conformance = load(
        REGISTRY
        / "20260721_binary_complement_tick_conformance_prescore_amendment.json"
    )
    baseline_reproduction = load(
        REGISTRY
        / "20260721_binary_complement_baseline_reproduction_prescore_amendment.json"
    )
    paired_depth = load(
        REGISTRY
        / "20260721_binary_complement_paired_depth_capacity_diagnostic.json"
    )
    paired_wait_cost = load(
        REGISTRY
        / "20260721_binary_complement_paired_book_wait_cost_diagnostic.json"
    )
    settlement_anchor = load(
        REGISTRY / "20260721_settlement_source_anchor_diagnostic.json"
    )
    settlement_anchor_preregistration = load(
        REGISTRY / "20260721_settlement_source_anchor_preregistration.json"
    )
    settlement_anchor_evaluator = load(
        REGISTRY / "20260721_settlement_source_anchor_offline_evaluator_amendment.json"
    )
    settlement_anchor_historical_outcome = load(
        REGISTRY
        / "20260721_settlement_source_anchor_historical_outcome_diagnostic.json"
    )
    fast_volatility_preregistration = load(
        REGISTRY / "20260721_fast_volatility_max_preregistration.json"
    )
    fast_volatility = load(
        REGISTRY / "20260721_fast_volatility_max_public_calibration.json"
    )
    fast_volatility_amendment = load(
        REGISTRY / "20260721_fast_volatility_max_integrity_amendment.json"
    )
    dvol_preregistration = load(
        REGISTRY / "20260721_dvol_volatility_max_preregistration.json"
    )
    dvol_source_feasibility = load(
        REGISTRY / "20260721_dvol_volatility_max_source_feasibility.json"
    )
    empirical_cdf_preregistration = load(
        REGISTRY / "20260721_empirical_return_cdf_preregistration.json"
    )
    empirical_cdf = load(
        REGISTRY / "20260721_empirical_return_cdf_public_calibration.json"
    )
    four_minute_preregistration = load(
        REGISTRY / "20260722_four_minute_continuation_preregistration.json"
    )
    four_minute_evidence = load(
        REGISTRY / "20260722_four_minute_continuation_public_proxy.json"
    )
    four_minute_amendment = load(
        REGISTRY / "20260722_four_minute_continuation_integrity_amendment.json"
    )
    probability_model_validation = load(
        REGISTRY / "20260721_probability_model_challenger_validation.json"
    )
    settlement_anchor_price_manifest = load(
        REGISTRY
        / "source_snapshots/20260721_settlement_anchor_price_to_beat_manifest.json"
    )
    non_candle_manifest = load(
        REGISTRY
        / "source_snapshots/20260721_non_candle_public_books_manifest.json"
    )
    support_quality = load(
        REGISTRY / "20260720_binary_complement_support_data_quality_audit.json"
    )
    tick_integrity = load(
        REGISTRY / "20260720_binary_complement_causal_tick_integrity_amendment.json"
    )
    pair_reproduction = load(
        REGISTRY
        / "20260720_binary_complement_paired_book_reproduction_amendment.json"
    )
    execution_tick_parity = load(
        REGISTRY
        / "20260720_binary_complement_execution_tick_parity_amendment.json"
    )
    live_tick_parity = load(
        REGISTRY
        / "20260720_binary_complement_live_tick_parity_amendment.json"
    )
    live_reconciliation_parity = load(
        REGISTRY
        / "20260721_binary_complement_live_reconciliation_parity_amendment.json"
    )
    rest_recovery_parity = load(
        REGISTRY
        / "20260721_binary_complement_rest_recovery_parity_amendment.json"
    )
    fixed_support = load(
        REGISTRY / "20260718_binary_complement_fixed_support_collection_plan.json"
    )
    economic_diagnostics = load(
        REGISTRY
        / "20260718_binary_complement_prescore_economic_diagnostics_amendment.json"
    )
    unit_economics = load(
        REGISTRY
        / "20260718_binary_complement_prescore_unit_economics_amendment.json"
    )
    artifact = load(REPORT)
    notebook = load(NOTEBOOK)
    paired_pressure_notebook = load(PAIRED_PRESSURE_NOTEBOOK)
    residual_independence_notebook = load(RESIDUAL_INDEPENDENCE_NOTEBOOK)
    residual_cross_market_notebook = load(RESIDUAL_CROSS_MARKET_NOTEBOOK)
    paired_depth_notebook = load(PAIRED_DEPTH_NOTEBOOK)
    paired_wait_cost_notebook = load(PAIRED_WAIT_COST_NOTEBOOK)
    settlement_anchor_notebook = load(SETTLEMENT_ANCHOR_NOTEBOOK)
    settlement_anchor_historical_outcome_notebook = load(
        SETTLEMENT_ANCHOR_HISTORICAL_OUTCOME_NOTEBOOK
    )
    fast_volatility_notebook = load(FAST_VOLATILITY_NOTEBOOK)
    empirical_cdf_notebook = load(EMPIRICAL_CDF_NOTEBOOK)

    assert strict42["baseline"]["trades"] == 102
    assert strict42["baseline"]["wins"] == 79
    assert strict42["baseline"]["losses"] == 23
    assert v1["status"] == "REJECTED_BY_HISTORICAL_RESEARCH_DIAGNOSTIC"
    assert v2["status"] == "REJECTED_BY_HISTORICAL_RESEARCH_DIAGNOSTIC"
    assert v1["aggregate"]["candidate"]["total_pnl_usd"] < 0
    assert v2["aggregate"]["candidate"]["total_pnl_usd"] < 0
    assert collection["strategy_decision"]["strategy_score_emitted"] is False
    assert collection["next_segment"]["capture_verified"] is True
    assert collection["next_segment"]["admissible_conditions"] == 12
    assert collection["next_segment"]["resolution_ready"] is True
    assert collection["bounded_continuation"]["planned_segments"] == 4
    assert collection["bounded_continuation"]["strategy_score_emitted"] is False
    assert collection["fixed_support_extension"]["strategy_score_emitted"] is False
    assert collection["local_pipeline_validation"]["rust_tests"] == 493
    assert (
        collection["local_pipeline_validation"][
            "binary_complement_screen_schema_version"
        ]
        == 4
    )
    assert (
        collection["rejected_raw_local_archives"]["vps_raw_frame_deletion_state"]
        == "COMPLETED_AFTER_HASH_REVERIFICATION"
    )

    assert fixed_support["decision"]["strategy_metrics_seen"] is False
    assert fixed_support["decision"]["strategy_rule_changed"] is False
    assert fixed_support["decision"]["rate_gate_changed"] is False
    assert fixed_support["decision"]["places_orders"] is False
    assert fixed_support["support_integrity"]["strategy_outcomes_or_rates_used"] is False
    assert fixed_support["support_integrity"]["unsealed_or_rejected_segments_count"] is False
    assert fixed_support["verified_cleanup"]["local_byte_identical_archives_preserved"] is True
    assert fixed_support["verified_cleanup"]["parquet_files_deleted"] == 0
    assert fixed_support["capacity_model"]["maximum_new_segments"] == 96
    assert fixed_support["capacity_model"]["target_unique_ready_terminal_conditions"] == 750
    close(
        fixed_support["observed_storage_inputs"]["raw_bytes_per_second"],
        2643474538 / 10860,
    )
    close(
        fixed_support["observed_storage_inputs"][
            "converted_bytes_per_admissible_condition"
        ],
        120143852 / 12,
    )
    close(
        fixed_support["capacity_model"]["projected_admissible_storage_bytes_at_750"],
        (120143852 / 12) * 750,
    )
    assert fixed_support["capacity_model"]["conservative_cushion_gib"] > 0
    assert (
        current_collection["deployed_artifacts"]["floor_collector_sha256_before_repair"]
        == fixed_support["deployed_runtime"]["floor_collector_sha256"]
    )
    assert (
        current_collection["deployed_artifacts"]["floor_collector_sha256_after_repair"]
        == "8bb66f98d94b6b347807d92523ac1e89d59101588709724570cf1fe5d62b88ae"
    )
    assert (
        current_collection["deployed_artifacts"]["capture_runner_sha256_after_repair"]
        == "f0255acfb4add5d5d852eadd8d6ea189f2bd0b7b85c7faa20b81b60cec51540b"
    )
    assert (
        hashlib.sha256(
            (ROOT / "deploy/build-and-start-binary-complement-floor.sh").read_bytes()
        ).hexdigest()
        == fixed_support["deployed_runtime"]["deferred_build_launcher_sha256"]
    )

    amendment = power["decision"]
    assert amendment["forward_metrics_seen_before_decision"] is False
    assert amendment["original_minimum_terminal_conditions_per_block"] == 100
    assert amendment["amended_minimum_terminal_conditions_per_block"] == 750
    assert amendment["strategy_rule_changed"] is False
    assert amendment["rate_gates_changed"] is False
    assert preregistration["instrumentation"]["scorer"]["schema_version"] == 6
    assert (
        preregistration["forward_screen"]["block_1"][
            "minimum_terminal_settlement_aligned_conditions"
        ]
        == 750
    )
    assert realized_support["status"] == "REALIZED_SUPPORT_GATES_REGISTERED_BEFORE_FORWARD_SCORE"
    assert realized_support["decision"]["forward_strategy_metrics_seen_before_decision"] is False
    assert realized_support["decision"]["forward_screen_artifacts_generated"] == 0
    assert realized_support["decision"]["strategy_feature_or_selection_rule_changed"] is False
    assert realized_support["decision"]["classification_rate_threshold_changed"] is False
    assert realized_support["decision"]["fee_aware_economic_threshold_changed"] is False
    assert realized_support["decision"]["realized_support_gate_set_changed"] is True
    assert realized_support["registered_gates"] == {
        "minimum_terminal_settlement_aligned_conditions": 750,
        "minimum_baseline_candidates": 100,
        "minimum_baseline_losses": 15,
        "minimum_selected_candidates": 80,
        "failure_action": "reject the family at the fixed 750-condition score; do not extend collection adaptively",
    }
    for name, expected in (
        ("minimum_baseline_candidates", 100),
        ("minimum_baseline_losses", 15),
        ("minimum_selected_candidates", 80),
    ):
        assert preregistration["forward_screen"]["block_1"][name] == expected
    assert realized_support["implementation"]["screen_schema_version_before"] == 4
    assert realized_support["implementation"]["screen_schema_version_after"] == 5
    assert realized_support["implementation"]["tests"]["result"] == "13 passed; 0 failed"
    deployed_v11 = realized_support["implementation"]["measurement_only_linux_binary"]
    assert deployed_v11["status"] == "INSTALLED_HASH_VERIFIED_NOT_ACTIVE"
    assert deployed_v11["path"] == "/opt/polymomentum/tools/polymomentum-engine-measurement-v11"
    assert deployed_v11["sha256"] == "07a386c748d3756462cb3a654c5999936b929f9be93f8190ebbfb80c34f00b89"
    assert deployed_v11["architecture"] == "ELF64 x86-64"
    assert deployed_v11["active_collector_binary_changed"] is False
    assert deployed_v11["production_binary_changed"] is False
    assert realized_support["build_overlap_audit"]["collector_restarts"] == 0
    assert realized_support["build_overlap_audit"]["new_recorder_stderr_warnings_during_build"] == 0
    assert "only if" in realized_support["build_overlap_audit"]["evidence_action"]
    assert (
        economic_diagnostics["status"]
        == "PRE_SCORE_DESCRIPTIVE_DIAGNOSTICS_REGISTERED_SCHEMA_3_SUPERSEDED"
    )
    assert economic_diagnostics["decision"]["forward_strategy_metrics_seen"] is False
    assert economic_diagnostics["decision"]["current_terminal_support_disclosed"] == 12
    assert economic_diagnostics["decision"]["screen_schema_version_after"] == 3
    assert economic_diagnostics["decision"]["strategy_rule_changed"] is False
    assert economic_diagnostics["decision"]["screen_rate_or_threshold_changed"] is False
    assert economic_diagnostics["diagnostics"]["gating_use"] == "descriptive_only_non_gating"
    assert (
        economic_diagnostics["superseded_by"]["screen_schema_version"] == 4
    )
    assert unit_economics["status"] == "PRE_SCORE_UNIT_ECONOMICS_GATES_REGISTERED"
    assert unit_economics["decision"]["forward_strategy_metrics_seen"] is False
    assert unit_economics["decision"]["current_terminal_support_disclosed"] == 12
    assert unit_economics["decision"]["strategy_feature_or_two_tick_rule_changed"] is False
    assert unit_economics["decision"]["classification_rate_thresholds_changed"] is False
    assert unit_economics["decision"]["economic_gate_set_changed"] is True
    assert unit_economics["decision"]["screen_schema_version_after"] == 4
    assert len(unit_economics["new_fail_closed_gates"]) == 2
    assert (
        preregistration["forward_screen"]["block_1"][
            "minimum_fee_aware_unit_profit_factor"
        ]
        == 1.20
    )
    assert (
        preregistration["forward_screen"]["block_1"][
            "minimum_fee_aware_unit_payoff_ratio"
        ]
        == 0.20
    )
    assert (
        unit_economics["integrity"]["scorer_source_sha256_after_schema_4"]
        == "6e661887245634c8085009bcb17506d3fa13a16939334fb2ce6c810d9cae7312"
    )

    economic_history = unit_economics["historical_sanity_envelope"]
    proportional_pf = (
        economic_history["baseline_profit_factor"]
        * economic_history["registered_minimum_winner_retention"]
        / economic_history["registered_maximum_loss_retention"]
    )
    deterioration_budget = 1.0 - 1.20 / proportional_pf
    close(
        economic_history["proportional_dollar_retention_profit_factor"],
        proportional_pf,
    )
    close(
        economic_history["gross_winner_pnl_deterioration_budget_before_pf_1_20"],
        deterioration_budget,
    )

    exact_baseline = strict42_exact["floors"][0]
    calibration = strategy_diagnostic["verified_driver"]
    close(calibration["realized_win_rate"], exact_baseline["win_rate"])
    close(calibration["internal_fair_mean_probability"], exact_baseline["fair_calibration"]["mean_p"])
    close(calibration["market_mean_probability"], exact_baseline["market_calibration"]["mean_p"])
    close(calibration["internal_fair_brier"], exact_baseline["fair_calibration"]["brier"])
    close(calibration["market_brier"], exact_baseline["market_calibration"]["brier"])
    close(
        calibration["market_brier_improvement_fraction"],
        (exact_baseline["fair_calibration"]["brier"] - exact_baseline["market_calibration"]["brier"])
        / exact_baseline["fair_calibration"]["brier"],
    )
    assert strategy_diagnostic["decision"]["strategy_adjustment"] == "NO_PARAMETER_CHANGE_DURING_FROZEN_FORWARD_BLOCK"
    assert strategy_diagnostic["decision"]["a_plus_claim"] is False
    assert strategy_diagnostic["decision"]["profitability_claim"] is False
    assert paired_pressure["schema_version"] == 2
    assert (
        paired_pressure["status"]
        == "DIAGNOSTIC_ONLY_REJECT_OPPOSITE_PRESSURE_CLAUSE_ACTIVE_RULE_UNCHANGED"
    )
    assert paired_pressure["source_authority"]["terminal_labels_used_in_primary_analysis"] is False
    assert paired_pressure["data_quality"]["promotion_or_exact_replay_eligible"] is False
    assert paired_pressure["data_quality"]["quality_assessment"] == "SHARE_WITH_CAVEATS_FOR_MECHANISM_SCREEN_ONLY"
    pressure_results = paired_pressure["structural_results"]
    assert pressure_results["possible_one_hz_samples"] == 7200
    assert pressure_results["valid_paired_samples"] == 6465
    assert pressure_results["conditions_with_valid_pair"] == 24
    assert pressure_results["candidate_vs_chosen_positive_comparisons"] == 12930
    assert pressure_results["candidate_vs_chosen_positive_disagreement_count"] == 0
    assert pressure_results["chosen_positive_count"] == pressure_results["paired_candidate_count"] == 6464
    close(pressure_results["valid_paired_sample_coverage"], 6465 / 7200)
    assert pressure_results["abs_cross_touch_mirror_error"]["max"] == 0
    assert paired_pressure["structural_identity_checks"]["all_pass"] is True
    assert (
        paired_pressure["mechanism_assessment"]["opposite_book_pressure_clause"]
        == "REJECT_AS_NON_INDEPENDENT_IN_OBSERVED_VALID_STATES"
    )
    assert (
        paired_pressure["mechanism_assessment"]["chosen_token_pressure_family"]
        == "TERMINAL_PREDICTION_AND_ECONOMIC_VALUE_NOT_EVALUATED_BY_THIS_DIAGNOSTIC"
    )
    assert paired_pressure["mechanism_assessment"]["active_binary_complement_rule_changed"] is False
    assert paired_pressure["decision"]["a_plus_claim"] is False
    assert paired_pressure["decision"]["profitability_claim"] is False
    assert paired_pressure["decision"]["live_trading"] == "OFF"

    assert (
        residual_independence["status"]
        == "DIAGNOSTIC_ONLY_REGISTERED_RESIDUAL_MAGNITUDE_HAS_ZERO_OBSERVED_SELECTIVITY_ACTIVE_RULE_UNCHANGED"
    )
    assert residual_independence["source_authority"]["resolution_manifest_loaded"] is False
    assert residual_independence["source_authority"]["terminal_labels_loaded"] is False
    assert residual_independence["source_authority"]["strategy_outcomes_loaded"] is False
    assert residual_independence["data_quality"]["possible_samples"] == 7200
    residual_full = residual_independence["structural_results"]["full_window"]
    residual_candidate = residual_independence["structural_results"][
        "registered_candidate_interval"
    ]
    assert residual_full["valid_pair_samples"] == 6465
    assert residual_full["pair_availability_rejections"] == 735
    assert residual_full["fixed_rule_rejections"] == 0
    assert residual_candidate["possible_samples"] == 1440
    assert residual_candidate["valid_pair_samples"] == 1403
    assert residual_candidate["fixed_rule_rejections"] == 0
    residual_drivers = residual_independence["structural_results"]["driver_diagnostic"]
    assert residual_drivers["invalid_reason_counts"] == {"invalid_top": 735}
    assert residual_drivers["observed_freshness_rejections"] == 0
    assert residual_drivers["observed_missing_snapshot_rejections"] == 0
    assert residual_drivers["nonzero_microprice_residual_samples"] == 5
    assert residual_drivers["nonzero_microprice_and_depth_mismatch_overlap"] == 5
    assert (
        residual_independence["mechanism_assessment"]["paired_top_validity"]
        == "ONLY_OBSERVED_SOURCE_OF_SELECTIVITY_IN_THIS_CAPTURE"
    )
    assert residual_independence["mechanism_assessment"]["active_binary_complement_rule_changed"] is False
    assert residual_independence["decision"]["a_plus_claim"] is False
    assert residual_independence["decision"]["profitability_claim"] is False

    assert (
        paired_wait_cost["status"]
        == "LABEL_FREE_PAIRED_BOOK_WAIT_COST_QUANTIFIED_ACTIVE_RULE_UNCHANGED"
    )
    assert paired_wait_cost["source_authority"]["resolution_manifest_loaded"] is False
    assert paired_wait_cost["source_authority"]["terminal_labels_loaded"] is False
    assert paired_wait_cost["source_authority"]["strategy_outcomes_loaded"] is False
    assert paired_wait_cost["source_authority"]["active_forward_block_loaded"] is False
    for source_name, source_hash in paired_wait_cost["source_authority"][
        "sha256"
    ].items():
        assert (
            residual_independence["source_authority"]["sha256"][source_name]
            == source_hash
        )
    assert paired_wait_cost["data_quality"]["native_timestamp_regressions_observed"] == 865
    wait_results = paired_wait_cost["structural_results"]
    wait_interval = wait_results["candidate_interval"]
    assert wait_interval["condition_seconds"] == 1440
    assert wait_interval["orientation_seconds"] == 2880
    assert wait_interval["valid_pair_condition_seconds"] == 1403
    assert wait_interval["invalid_pair_condition_seconds"] == 37
    close(wait_interval["valid_pair_coverage"], 1403 / 1440)
    assert wait_interval["conditions_with_pair_invalidity"] == 1
    assert wait_interval["invalid_reason_counts_by_side"] == {"invalid_top": 74}
    wait_episodes = wait_results["invalid_pair_episodes"]
    assert wait_episodes["count"] == 2
    assert wait_episodes["conditions"] == 1
    assert wait_episodes["duration_sample_seconds"]["min"] == 10
    assert wait_episodes["duration_sample_seconds"]["max"] == 27
    wait_exposure = wait_results["pair_gate_exposure"]
    assert wait_exposure["pair_only_exposed_orientation_seconds"] == 0
    assert wait_exposure["share_of_all_orientation_seconds"] == 0
    assert wait_exposure["conditions"] == 0
    assert wait_exposure["recovered_within_market"] == 0
    assert wait_exposure["unrecovered_within_market"] == 0
    assert wait_exposure["recovery_delay_seconds"]["count"] == 0
    assert wait_exposure["chosen_ask_change"]["count"] == 0
    assert wait_exposure["adverse_chosen_ask_change"]["count"] == 0
    assert wait_exposure["ask_deteriorations"] == 0
    assert wait_exposure["ask_unchanged"] == 0
    assert wait_exposure["ask_improvements"] == 0
    assert wait_exposure["directions"] == {}
    assert wait_exposure["opposite_invalid_reasons"] == {}
    assert (
        paired_wait_cost["mechanism_assessment"]["active_binary_complement_rule_changed"]
        is False
    )
    assert paired_wait_cost["decision"]["a_plus_claim"] is False
    assert paired_wait_cost["decision"]["profitability_claim"] is False
    assert paired_wait_cost["decision"]["live_trading"] == "OFF"

    assert (
        settlement_anchor["status"]
        == "LABEL_FREE_SETTLEMENT_SOURCE_ANCHOR_SHOWS_DISTINCT_DECISION_SELECTIVITY_PREREGISTRATION_WARRANTED"
    )
    anchor_authority = settlement_anchor["source_authority"]
    assert anchor_authority["resolution_manifest_loaded"] is False
    assert anchor_authority["terminal_labels_loaded"] is False
    assert anchor_authority["strategy_outcomes_loaded"] is False
    assert anchor_authority["active_forward_block_loaded"] is False
    anchor_quality = settlement_anchor["data_quality"]
    assert anchor_quality["possible_candidate_condition_seconds"] == 1440
    assert anchor_quality["paired_book_valid_condition_seconds"] == 1403
    assert anchor_quality["book_invalid_condition_seconds"] == 37
    assert anchor_quality["fresh_official_anchor_condition_seconds"] == 1389
    assert anchor_quality["fresh_official_anchor_orientation_seconds"] == 2778
    assert anchor_quality["official_current_stale_over_10s_condition_seconds"] == 14
    assert anchor_quality["published_price_to_beat_matches"] == 24
    assert anchor_quality["promotion_or_exact_replay_eligible"] is False
    anchor_results = settlement_anchor["structural_results"]["fresh_official_anchor"]
    assert anchor_results["states"] == 1389
    assert anchor_results["orientation_edge_disagreements"] == 215
    assert anchor_results["proxy_only_passes"] == 112
    assert anchor_results["official_only_passes"] == 103
    assert anchor_results["both_pass"] == 106
    assert anchor_results["neither_pass"] == 2457
    assert anchor_results["conditions_with_edge_disagreement"] == 21
    assert anchor_results["direction_disagreements"] == 34
    assert anchor_results["edge_disagreements_by_direction"] == {
        "down": 91,
        "up": 124,
    }
    assert anchor_results["edge_disagreements_by_chronological_half"] == {
        "first": 30,
        "second": 185,
    }
    close(settlement_anchor["structural_results"]["edge_disagreement_rate"], 215 / 2778)
    close(anchor_results["abs_fair_probability_delta"]["p50"], 0.02023274404531128)
    close(anchor_results["abs_fair_probability_delta"]["p90"], 0.055125344702577475)
    close(anchor_results["abs_fair_probability_delta"]["max"], 0.23354981543852243)
    assert settlement_anchor["model_contract"]["alternate_thresholds_tested"] == 0
    assert settlement_anchor["decision"]["candidate_preregistration_warranted"] is True
    assert settlement_anchor["decision"]["same_block_second_hypothesis_score_permitted"] is False
    assert settlement_anchor["decision"]["runtime_or_live_strategy_changed"] is False
    assert settlement_anchor["decision"]["profitability_claim"] is False
    assert settlement_anchor["decision"]["a_plus_claim"] is False

    assert (
        settlement_anchor_price_manifest["status"]
        == "PUBLIC_PRICE_TO_BEAT_MATCHES_CAPTURED_CHAINLINK_OPEN_24_OF_24"
    )
    assert len(settlement_anchor_price_manifest["pages"]) == 24
    assert len({page["slug"] for page in settlement_anchor_price_manifest["pages"]}) == 24
    assert all(
        page["absolute_difference_usd"] < 1e-9
        for page in settlement_anchor_price_manifest["pages"]
    )

    anchor_registration = settlement_anchor_preregistration
    assert (
        anchor_registration["status"]
        == "PREREGISTERED_FOR_DISJOINT_FUTURE_BLOCK_NO_ACTIVE_BLOCK_SCORE"
    )
    assert anchor_registration["mechanism_id"] == "settlement_source_anchor_v1"
    assert anchor_registration["blindness"]["active_block_eligibility"] == "forbidden"
    assert anchor_registration["blindness"]["active_binary_complement_strategy_metrics_accessed"] is False
    assert anchor_registration["blindness"]["diagnostic_terminal_labels_loaded"] is False
    assert anchor_registration["frozen_candidate"]["official_current_max_age_ms"] == 10000
    assert anchor_registration["frozen_candidate"]["official_open_max_distance_ms"] == 2000
    assert anchor_registration["frozen_candidate"]["alternate_source_freshness_thresholds_permitted"] is False
    assert anchor_registration["frozen_candidate"]["alternate_edge_or_probability_thresholds_permitted"] is False
    assert anchor_registration["condition_allocation"]["same_condition_id_in_both_mechanism_families"] == "forbidden"
    assert anchor_registration["condition_allocation"]["adaptive_reallocation_after_viewing_metrics"] == "forbidden"
    anchor_forward = anchor_registration["forward_evaluation"]
    assert anchor_forward["blocks_required"] == 2
    assert anchor_forward["minimum_terminal_official_source_aligned_conditions_per_block"] == 750
    assert anchor_forward["minimum_official_anchor_coverage"] == 0.95
    assert anchor_forward["minimum_candidate_trades"] == 80
    assert anchor_forward["score_frequency"] == "once per sealed block"
    assert anchor_forward["threshold_changes_after_any_score"] == "forbidden"
    assert anchor_forward["minimum_replay_latency_ms"] == 202
    assert anchor_forward["exact_execution_required"] is True
    assert anchor_forward["required_absolute_gates_each_block"] == {
        "wilson_95_lower_bound": 0.7,
        "fee_inclusive_pnl_usd": "greater than 0",
        "profit_factor": 1.2,
        "payoff_ratio": 0.3,
        "profitable_eligible_reports": 20,
        "eligible_reports": 20,
        "worst_fold_pnl_usd": -13.0,
        "left_tail_cvar_usd": -8.0,
        "maximum_losing_reports_in_any_five_report_window": 2,
        "worst_loss_over_average_win": 3.5,
        "first_half_fee_inclusive_pnl_usd": "greater than 0",
        "second_half_fee_inclusive_pnl_usd": "greater than 0",
    }
    for source in anchor_registration["source_evidence"]:
        if source["path"].startswith("rust_engine/"):
            assert source["sha256"] in {
                "5ea335ce79c9a0c061b44ee1479da2a6e06ae8d2365902f264df3f6f61372216",
                "5bc2f98a6a9e7eafa0dce66fe005beafb9247c790fb58e980d76ccc3e2a33ad0",
            }
        else:
            assert hashlib.sha256((ROOT / source["path"]).read_bytes()).hexdigest() == source["sha256"]
    assert anchor_registration["decision"]["candidate_registered"] is True
    assert anchor_registration["decision"]["runtime_implementation_authorized"] is False
    assert anchor_registration["decision"]["active_collector_change_authorized"] is False
    assert anchor_registration["decision"]["active_block_score_authorized"] is False
    assert anchor_registration["decision"]["paper_or_live_trading_authorized"] is False

    assert (
        settlement_anchor_evaluator["status"]
        == "OUTCOME_BLIND_EVALUATOR_ALLOCATION_SOURCE_AUDIT_AND_PAIRED_SCORER_READY_RUNTIME_UNCHANGED"
    )
    evaluator_authority = settlement_anchor_evaluator["authority"]
    assert evaluator_authority["active_binary_complement_strategy_metrics_accessed"] is False
    assert evaluator_authority["active_binary_complement_terminal_outcomes_accessed"] is False
    assert evaluator_authority["settlement_anchor_forward_outcomes_exist"] is False
    assert evaluator_authority["active_collector_changed"] is False
    assert evaluator_authority["production_or_paper_runtime_changed"] is False
    assert (
        settlement_anchor_evaluator["registered_contract_preserved"][
            "preregistration_sha256"
        ]
        == hashlib.sha256(
            (REGISTRY / "20260721_settlement_source_anchor_preregistration.json").read_bytes()
        ).hexdigest()
    )
    assert settlement_anchor_evaluator["implementation"]["surface"] == "offline harness-sweep only"
    assert settlement_anchor_evaluator["implementation"]["activation"].startswith(
        "explicit --fair-value-btc-csv"
    )
    assert "--settlement-anchor-allocation-lock" in settlement_anchor_evaluator["implementation"]["activation"]
    assert "--settlement-anchor-source-audit" in settlement_anchor_evaluator["implementation"]["activation"]
    assert "--pin-input-artifacts" in settlement_anchor_evaluator["implementation"]["activation"]
    assert settlement_anchor_evaluator["implementation"]["default_behavior"].startswith(
        "unchanged proxy-anchor"
    )
    assert settlement_anchor_evaluator["implementation"]["source_guard"].endswith(
        "chainlink_btc_usd_data_stream"
    )
    for source in settlement_anchor_evaluator["implementation"]["files"]:
        if source["path"] == "rust_engine/src/backtest/harness.rs":
            assert source["sha256"] == "6579c455ae504567bb0355829ff68897a99530cb68676ab96f4d6bd918871ae4"
        else:
            assert hashlib.sha256((ROOT / source["path"]).read_bytes()).hexdigest() == source["sha256"]
    multiplicity_guards = settlement_anchor_evaluator["implementation"]["multiplicity_guards"]
    assert any("exact 750-ID allowlist" in guard for guard in multiplicity_guards)
    assert any("complete prior binary-complement block shape" in guard for guard in multiplicity_guards)
    assert any("fixed contiguous report partition" in guard for guard in multiplicity_guards)
    assert any("zero condition-ID overlap" in guard for guard in multiplicity_guards)
    assert any("all three output paths" in guard and "reused" in guard for guard in multiplicity_guards)
    assert any("byte-revalidated PMXT" in guard for guard in multiplicity_guards)
    assert any("Wilson lower bound" in guard and "CVaR" in guard for guard in multiplicity_guards)
    assert settlement_anchor_evaluator["validation"]["cargo_test"].startswith("PASS: 545 passed")
    assert settlement_anchor_evaluator["validation"]["cargo_clippy_all_targets_deny_warnings"] == "PASS"
    assert settlement_anchor_evaluator["validation"]["cli_help_verified"] is True
    assert settlement_anchor_evaluator["validation"]["cli_multiplicity_guard_verified"].startswith(
        "PASS:"
    )
    assert settlement_anchor_evaluator["validation"]["cli_allocation_guard_verified"].startswith(
        "PASS:"
    )
    assert settlement_anchor_evaluator["validation"]["cli_flags"] == [
        "--fair-value-btc-csv",
        "--pin-input-artifacts",
        "--settlement-anchor-allocation-lock",
        "--settlement-anchor-source-audit",
    ]
    assert settlement_anchor_evaluator["validation"]["paired_scorer_cli"] == (
        "strategy-builder settlement-anchor-pair-audit"
    )
    assert settlement_anchor_evaluator["validation"]["forward_strategy_scores_disclosed"] == 0
    assert settlement_anchor_evaluator["decision"]["offline_evaluator_ready"] is True
    assert settlement_anchor_evaluator["decision"]["block_allocation_wrapper_ready"] is True
    assert settlement_anchor_evaluator["decision"]["future_block_allocation_lock_issued"] is False
    assert settlement_anchor_evaluator["decision"]["label_free_source_audit_ready"] is True
    assert settlement_anchor_evaluator["decision"]["paired_block_scorer_ready"] is True
    assert settlement_anchor_evaluator["decision"]["runtime_implementation_authorized"] is False
    assert settlement_anchor_evaluator["decision"]["active_collector_change_authorized"] is False
    assert settlement_anchor_evaluator["decision"]["active_block_score_authorized"] is False
    assert settlement_anchor_evaluator["decision"]["paper_or_live_trading_authorized"] is False
    assert settlement_anchor_evaluator["remaining_allocation_control"]["status"] == (
        "IMPLEMENTED_AWAITING_FUTURE_CONDITION_SET"
    )
    assert "hash-pins the exact 750 allowed IDs" in (
        settlement_anchor_evaluator["remaining_allocation_control"]["implemented_control"]
    )
    assert settlement_anchor_evaluator["remaining_pairing_control"]["status"] == (
        "IMPLEMENTED_AWAITING_FUTURE_LOCKED_ARTIFACTS"
    )
    assert "only the fair-value tape may differ" in (
        settlement_anchor_evaluator["remaining_pairing_control"]["required_before_first_candidate_score"]
    )

    assert settlement_anchor_historical_outcome["status"] == (
        "RETROSPECTIVE_SIGNAL_GATED_DIAGNOSTIC_DIRECTIONALLY_NEGATIVE_INADEQUATE_SUPPORT"
    )
    historical_decision = settlement_anchor_historical_outcome["decision"]
    assert historical_decision == {
        "candidate_promoted": False,
        "candidate_rejected": False,
        "runtime_change_authorized": False,
        "paper_or_live_trading_authorized": False,
        "profitability_claim": False,
        "a_plus_claim": False,
        "future_disjoint_blocks_still_required": 2,
    }
    historical_event = settlement_anchor_historical_outcome["results"][
        "event_level_descriptive"
    ]
    assert historical_event["rows"] == 81
    assert historical_event["conditions"] == 3
    assert historical_event["official_source_fresh_rows"] == 81
    assert historical_event["baseline_edge_pass_rows"] == 1
    assert historical_event["official_edge_pass_rows"] == 0
    assert historical_event["eligibility_disagreement_rows"] == 1
    historical_primary = settlement_anchor_historical_outcome["results"][
        "condition_level_primary"
    ]
    assert historical_primary["baseline"]["selected_conditions"] == 1
    assert historical_primary["baseline"]["wins"] == 1
    assert historical_primary["official"]["selected_conditions"] == 0
    close(historical_primary["baseline"]["decision_time_one_share_pnl"], 0.314523)
    close(historical_primary["official"]["decision_time_one_share_pnl"], 0.0)
    close(
        historical_primary["paired_one_share_pnl_delta_across_all_24_conditions"],
        -0.314523,
    )
    assert len(settlement_anchor_historical_outcome["changed_condition_rows"]) == 1
    source_authority = settlement_anchor_historical_outcome["source_authority"]
    assert source_authority["captured_markets"] == 24
    assert source_authority["engine_opportunity_conditions"] == 3
    assert source_authority["official_vs_proxy_terminal_direction_disagreements"] == 0
    historical_snapshot_files = {
        "opportunities": "20260721_settlement_anchor_historical_signal_opportunities.json.gz",
        "replay_report": "20260721_settlement_anchor_historical_signal_replay_report.json.gz",
        "chainlink": "20260721_settlement_anchor_historical_chainlink_btcusd.csv.gz",
        "captured_binance": "20260721_settlement_anchor_historical_binance_btcusdt_rtds.csv.gz",
        "resolution": "20260721_settlement_anchor_historical_resolution_manifest.json.gz",
        "binance_preroll": "20260721_binance_btcusdt_1s_preroll.csv.gz",
        "binance_gapfill": "20260721_binance_btcusdt_1s_gapfill.csv.gz",
    }
    for source_name, filename in historical_snapshot_files.items():
        compressed = (REGISTRY / "source_snapshots" / filename).read_bytes()
        assert hashlib.sha256(compressed).hexdigest() == source_authority[
            "source_sha256"
        ][source_name]
        assert hashlib.sha256(gzip.decompress(compressed)).hexdigest() == source_authority[
            "source_uncompressed_sha256"
        ][source_name]

    assert fast_volatility_preregistration["status"] == (
        "PREREGISTERED_BEFORE_PUBLIC_WINDOW_DOWNLOAD_OR_LABEL_INSPECTION"
    )
    assert fast_volatility_preregistration["mechanism_id"] == "fast_volatility_max_v1"
    assert fast_volatility_preregistration["fixed_pass_gates"]["overall"] == {
        "minimum_brier_improvement": 0.0005,
        "minimum_log_loss_improvement": 0.001,
        "brier_bootstrap_lower_bound_strictly_positive": True,
        "log_loss_bootstrap_lower_bound_strictly_positive": True,
    }
    for source in fast_volatility_preregistration["source_pins_before_evaluation"].values():
        assert hashlib.sha256((ROOT / source["path"]).read_bytes()).hexdigest() == source[
            "sha256"
        ]
    assert fast_volatility["status"] == "PUBLIC_PROXY_CALIBRATION_REJECTED_NO_RETUNING"
    assert fast_volatility["mechanism_id"] == "fast_volatility_max_v1"
    assert len(fast_volatility["authority"]["archive_manifest"]) == 31
    assert fast_volatility["source_data_quality"] == {
        "archives": 31,
        "rows": 2_678_400,
        "schema_widths": [12],
        "rows_per_archive_minimum": 86_400,
        "rows_per_archive_maximum": 86_400,
        "timestamp_unit": "microseconds",
        "timestamp_duplicates": 0,
        "timestamp_regressions": 0,
        "one_second_gap_violations": 0,
        "maximum_gap_seconds": 1.0,
        "invalid_close_durations": 0,
        "invalid_prices": 0,
        "checksum_failures": 0,
    }
    assert fast_volatility["forecast_data_quality"]["retained_conditions"] == 8_633
    assert fast_volatility["forecast_data_quality"]["terminal_tie_conditions"] == 7
    assert fast_volatility["forecast_data_quality"]["retained_registered_forecasts"] == 25_899
    overall_fast_volatility = fast_volatility["results"]["overall"]
    close(overall_fast_volatility["brier_improvement"], 0.00017704880919652607)
    close(overall_fast_volatility["log_loss_improvement"], 0.0010404508337223364)
    assert fast_volatility["gate_evaluation"]["failed_checks"] == [
        "overall_brier_improvement_at_least_0_0005"
    ]
    assert fast_volatility["gate_evaluation"]["passed"] is False
    assert all(
        row["brier_improvement"] > 0 and row["log_loss_improvement"] > 0
        for row in fast_volatility["results"]["chronological_windows"].values()
    )
    assert all(
        row["brier_improvement"] >= 0 and row["log_loss_improvement"] >= 0
        for row in fast_volatility["results"]["decision_offsets"].values()
    )
    assert min(fast_volatility["results"]["bootstrap"]["brier_improvement_95pct"]) > 0
    assert min(fast_volatility["results"]["bootstrap"]["log_loss_improvement_95pct"]) > 0
    assert fast_volatility["decision"]["strategy_variant_authorized"] is False
    assert fast_volatility["decision"]["runtime_change_authorized"] is False
    assert fast_volatility["decision"]["profitability_claim"] is False
    assert fast_volatility["decision"]["a_plus_claim"] is False
    fast_volatility_pins = fast_volatility_amendment["source_pins"]
    assert hashlib.sha256(
        (ROOT / fast_volatility_pins["analysis_script"]["path"]).read_bytes()
    ).hexdigest() == fast_volatility_pins["analysis_script"]["sha256"]
    assert hashlib.sha256(
        (ROOT / fast_volatility_pins["final_evidence"]["path"]).read_bytes()
    ).hexdigest() == fast_volatility_pins["final_evidence"]["sha256"]
    assert fast_volatility_amendment["decision"]["candidate_rejected"] is True
    assert fast_volatility_amendment["decision"]["retuning_authorized"] is False
    assert fast_volatility_amendment["reproducibility_correction"][
        "cli_and_notebook_outputs_byte_identical"
    ] is True

    assert dvol_preregistration["status"] == (
        "PREREGISTERED_BEFORE_DVOL_OR_EVALUATION_LABEL_DOWNLOAD"
    )
    assert dvol_preregistration["mechanism_id"] == "dvol_volatility_max_v1"
    assert (
        dvol_preregistration["fixed_pass_gates"]["data_quality"][
            "minimum_fresh_dvol_forecast_fraction"
        ]
        == 0.99
    )
    for source in dvol_preregistration["source_pins_before_evaluation"].values():
        assert hashlib.sha256((ROOT / source["path"]).read_bytes()).hexdigest() == source[
            "sha256"
        ]
    assert dvol_source_feasibility["status"] == (
        "REJECTED_BEFORE_LABEL_DOWNLOAD_DVOL_SOURCE_GRANULARITY_FAILED"
    )
    assert dvol_source_feasibility["mechanism_id"] == "dvol_volatility_max_v1"
    dvol_quality = dvol_source_feasibility["source_quality"]
    assert dvol_quality["points"] == 1_460
    assert dvol_quality["minimum_spacing_seconds"] == 21_600
    assert dvol_quality["median_spacing_seconds"] == 21_600
    assert dvol_quality["maximum_spacing_seconds"] == 21_600
    dvol_feasibility = dvol_source_feasibility["pre_label_feasibility"]
    assert dvol_feasibility["possible_forecasts"] == 30_240
    assert dvol_feasibility["fresh_dvol_forecasts"] == 10_080
    close(dvol_feasibility["fresh_dvol_forecast_fraction"], 1 / 3)
    assert dvol_feasibility["required_fresh_dvol_forecast_fraction"] == 0.99
    assert dvol_feasibility["failed_checks"] == [
        "minimum_fresh_dvol_forecast_fraction"
    ]
    assert dvol_source_feasibility["label_access_audit"] == {
        "binance_evaluation_archives_downloaded": 0,
        "binance_evaluation_labels_loaded": False,
        "brier_or_log_loss_computed": False,
        "active_binary_complement_outcomes_accessed": False,
        "active_binary_complement_strategy_metrics_accessed": False,
    }
    assert dvol_source_feasibility["decision"]["candidate_rejected"] is True
    assert dvol_source_feasibility["decision"]["retuning_authorized"] is False
    assert dvol_source_feasibility["decision"]["runtime_change_authorized"] is False

    assert empirical_cdf_preregistration["status"] == (
        "PREREGISTERED_BEFORE_NEW_EVALUATION_LABEL_DOWNLOAD"
    )
    assert (
        empirical_cdf_preregistration["mechanism_id"]
        == "empirical_standardized_return_cdf_v1"
    )
    assert empirical_cdf_preregistration["fixed_pass_gates"]["overall"] == {
        "minimum_brier_improvement": 0.0005,
        "minimum_log_loss_improvement": 0.001,
        "brier_bootstrap_lower_bound_strictly_positive": True,
        "log_loss_bootstrap_lower_bound_strictly_positive": True,
    }
    for source in empirical_cdf_preregistration[
        "source_pins_before_evaluation"
    ].values():
        assert hashlib.sha256((ROOT / source["path"]).read_bytes()).hexdigest() == source[
            "sha256"
        ]
    assert empirical_cdf["status"] == "PUBLIC_PROXY_CALIBRATION_REJECTED_NO_RETUNING"
    assert empirical_cdf["mechanism_id"] == "empirical_standardized_return_cdf_v1"
    assert len(empirical_cdf["authority"]["archive_manifest"]) == 49
    assert empirical_cdf["source_data_quality"]["rows"] == 4_233_600
    assert empirical_cdf["source_data_quality"]["checksum_failures"] == 0
    empirical_quality = empirical_cdf["forecast_data_quality"]
    assert empirical_quality["expected_conditions"] == 10_080
    assert empirical_quality["retained_conditions"] == 10_062
    assert empirical_quality["terminal_tie_conditions"] == 18
    assert empirical_quality["retained_registered_forecasts"] == 30_186
    assert empirical_quality["prior_sample_count_minimum"] == 2_016
    assert empirical_quality["prior_sample_count_maximum"] == 2_016
    empirical_overall = empirical_cdf["results"]["overall"]
    close(empirical_overall["brier_improvement"], 0.0005205290753615272)
    close(empirical_overall["log_loss_improvement"], 0.00030154935441539864)
    close(empirical_overall["moved_away_from_half_fraction"], 0.938382031405286)
    assert empirical_cdf["results"]["chronological_windows"]["older_first"][
        "brier_improvement"
    ] < 0
    assert empirical_cdf["results"]["chronological_windows"]["older_first"][
        "log_loss_improvement"
    ] < 0
    assert empirical_cdf["results"]["decision_offsets"]["120"][
        "log_loss_improvement"
    ] < 0
    assert empirical_cdf["results"]["overconfidence_tail"]["brier_improvement"] < 0
    assert empirical_cdf["results"]["overconfidence_tail"]["log_loss_improvement"] < 0
    assert min(empirical_cdf["results"]["bootstrap"]["brier_improvement_95pct"]) < 0
    assert min(empirical_cdf["results"]["bootstrap"]["log_loss_improvement_95pct"]) < 0
    assert len(empirical_cdf["gate_evaluation"]["failed_checks"]) == 8
    assert empirical_cdf["gate_evaluation"]["passed"] is False
    assert empirical_cdf["decision"]["strategy_variant_authorized"] is False
    assert empirical_cdf["decision"]["runtime_change_authorized"] is False
    assert empirical_cdf["decision"]["profitability_claim"] is False
    assert empirical_cdf["decision"]["a_plus_claim"] is False

    assert four_minute_preregistration["status"] == (
        "PREREGISTERED_BEFORE_FOUR_MINUTE_LABEL_EVALUATION"
    )
    assert four_minute_preregistration["mechanism_id"] == (
        "four_minute_continuation_v1"
    )
    assert four_minute_preregistration["frozen_signal"][
        "checkpoint_offsets_seconds"
    ] == [0, 60, 120, 180, 240]
    assert four_minute_evidence["status"] == (
        "PUBLIC_DIRECTIONAL_PROXY_REJECTED_NO_RETUNING"
    )
    assert four_minute_evidence["results"]["overall"]["eligible_signals"] == 1_032
    close(
        four_minute_evidence["results"]["overall"]["accuracy"],
        0.9554263565891473,
    )
    close(
        four_minute_evidence["results"]["mechanism_decomposition"][
            "true_fifth_minute_continuation_rate"
        ],
        0.45058139534883723,
    )
    assert four_minute_evidence["gate_evaluation"]["failed_checks"] == [
        "minimum_eligible_signals_fresh"
    ]
    assert four_minute_evidence["gate_evaluation"]["passed"] is False
    assert four_minute_evidence["decision"]["runtime_change_authorized"] is False
    assert four_minute_evidence["decision"]["paper_or_live_trading_authorized"] is False
    assert four_minute_evidence["economic_audit"]["profitability_established"] is False
    assert four_minute_amendment["status"] == (
        "DIRECTIONAL_CLAIM_REPLICATED_PUBLIC_GATE_FAILED_FRESH_SUPPORT_"
        "RUNTIME_UNCHANGED"
    )
    assert four_minute_amendment["decision"][
        "article_directional_claim_broadly_replicated"
    ] is True
    assert four_minute_amendment["decision"][
        "preregistered_public_proxy_gate_passed"
    ] is False
    assert four_minute_amendment["decision"]["add_default_off_runtime_feature"] is False
    for pin in four_minute_amendment["source_pins"].values():
        assert hashlib.sha256((ROOT / pin["path"]).read_bytes()).hexdigest() == pin[
            "sha256"
        ]

    assert probability_model_validation["status"] == (
        "TWO_CHALLENGERS_REJECTED_NO_RUNTIME_CHANGE"
    )
    for family in ("dvol_volatility_max_v1", "empirical_standardized_return_cdf_v1"):
        for pinned in probability_model_validation[family]["artifacts"].values():
            assert hashlib.sha256((ROOT / pinned["path"]).read_bytes()).hexdigest() == pinned[
                "sha256"
            ]
    assert all(
        value is False
        for value in probability_model_validation["authority_limits"].values()
    )

    assert (
        residual_cross_market["status"]
        == "DIAGNOSTIC_ONLY_CROSS_MARKET_REPLICATION_ZERO_REGISTERED_RESIDUAL_REJECTIONS_ACTIVE_RULE_UNCHANGED"
    )
    assert residual_cross_market["source_authority"]["resolution_labels_loaded"] is False
    assert residual_cross_market["source_authority"]["strategy_outcomes_loaded"] is False
    assert residual_cross_market["population"]["markets"] == 100
    assert residual_cross_market["population"]["rounds"] == 5
    assert residual_cross_market["population"]["possible_paired_states"] == 500
    cross_quality = residual_cross_market["data_quality"]
    assert cross_quality["valid_paired_states"] == 490
    assert cross_quality["invalid_reason_counts"] == {"missing_side": 10}
    assert cross_quality["token_coverage_failures"] == 0
    assert cross_quality["condition_id_mismatches"] == 0
    assert cross_quality["negative_risk_books"] == 0
    assert cross_quality["documented_sort_direction_checks"]["books"] == 1000
    assert cross_quality["documented_sort_direction_checks"]["bids_descending"] == 10
    assert cross_quality["documented_sort_direction_checks"]["asks_ascending"] == 10
    assert cross_quality["pair_timestamp_skew_ms"]["max"] == 2
    cross_results = residual_cross_market["structural_results"]
    assert cross_results["registered_fixed_rule_rejections"] == 0
    assert cross_results["midpoint_clause_rejections"] == 0
    assert cross_results["microprice_incremental_rejections_beyond_midpoint"] == 0
    assert cross_results["strict_0_002_threshold_rejections"] == 0
    assert cross_results["microprice_residual_nonzero_states"] == 7
    assert cross_results["depth_mirror_mismatch_states"] == 7
    close(cross_results["max_abs_residual"]["max"], 0.0001179054732232121)
    assert (
        residual_cross_market["driver_diagnostic"][
            "nonzero_microprice_and_depth_mismatch_overlap"
        ]
        == 7
    )
    assert residual_cross_market["mechanism_assessment"]["active_binary_complement_rule_changed"] is False
    assert residual_cross_market["decision"]["a_plus_claim"] is False
    assert residual_cross_market["decision"]["profitability_claim"] is False

    compressed_snapshot = NON_CANDLE_SNAPSHOT.read_bytes()
    canonical_snapshot = gzip.decompress(compressed_snapshot)
    assert hashlib.sha256(compressed_snapshot).hexdigest() == non_candle_manifest["snapshot_sha256"]
    assert hashlib.sha256(canonical_snapshot).hexdigest() == non_candle_manifest["snapshot_uncompressed_sha256"]
    recomputed_cross_market = recompute_cross_market_snapshot(
        json.loads(canonical_snapshot)
    )
    assert recomputed_cross_market["possible"] == 500
    assert recomputed_cross_market["valid"] == 490
    assert recomputed_cross_market["invalid"] == 10
    assert recomputed_cross_market["registered_rejections"] == 0
    assert recomputed_cross_market["strict_rejections"] == 0
    close(
        recomputed_cross_market["maximum_residual"],
        cross_results["max_abs_residual"]["max"],
    )
    assert recomputed_cross_market["maximum_timestamp_skew_ms"] == 2
    assert non_candle_manifest["selection"]["selected_markets"] == 100
    assert non_candle_manifest["selection"]["selected_tokens"] == 200
    assert len(non_candle_manifest["rounds"]) == 5
    assert all(round_row["response_books"] == 200 for round_row in non_candle_manifest["rounds"])

    assert (
        paired_depth["status"]
        == "LABEL_FREE_PAIRED_DEPTH_CHALLENGER_REJECTED_AS_NON_DISTINCT"
    )
    assert paired_depth["source_authority"]["snapshot_sha256"] == hashlib.sha256(
        compressed_snapshot
    ).hexdigest()
    assert paired_depth["source_authority"]["terminal_labels_loaded"] is False
    assert paired_depth["source_authority"]["strategy_outcomes_loaded"] is False
    assert paired_depth["source_authority"]["active_forward_block_loaded"] is False
    assert paired_depth["population"]["valid_paired_states"] == 490
    depth_results = paired_depth["structural_results"]
    assert depth_results["coverage_counts"] == {
        "entry_capacity_10usd": 470,
        "yes_entry_capacity_10usd": 470,
        "no_entry_capacity_10usd": 490,
        "four_side_capacity_10usd": 460,
        "matched_depth_ratio_0_25": 490,
    }
    assert depth_results["disagreement"] == {
        "four_side_vs_residual": 30,
        "depth_ratio_vs_residual_nonzero": 0,
        "opposite_capacity_incremental_if_yes_chosen": 0,
        "opposite_capacity_incremental_if_no_chosen": 20,
    }
    assert recomputed_cross_market["matched_depth_ratio_passes"] == 490
    assert recomputed_cross_market["both_entry_capacity_passes"] == 470
    assert recomputed_cross_market["four_side_capacity_passes"] == 460
    assert recomputed_cross_market["yes_entry_capacity_passes"] == 470
    assert recomputed_cross_market["no_entry_capacity_passes"] == 490
    assert recomputed_cross_market["opposite_incremental_if_yes_chosen"] == 0
    assert recomputed_cross_market["opposite_incremental_if_no_chosen"] == 20
    assert recomputed_cross_market["complete_five_round_markets"] == 98
    assert recomputed_cross_market["four_side_always_pass"] == 92
    assert recomputed_cross_market["four_side_always_fail"] == 6
    assert recomputed_cross_market["four_side_mixed"] == 0
    assert paired_depth["mechanism_assessment"]["distinct_pair_signal_observed"] is False
    assert paired_depth["decision"]["paired_depth_challenger_preregistered"] is False
    assert paired_depth["decision"]["strategy_rule_changed"] is False
    assert paired_depth["decision"]["a_plus_claim"] is False
    assert paired_depth["decision"]["profitability_claim"] is False

    assert (
        residual_attribution["status"]
        == "PREREGISTERED_AND_IMPLEMENTED_POST_BLOCK_ATTRIBUTION_ABLATION_ACTIVE_RULE_UNCHANGED"
    )
    assert residual_attribution["blindness"]["sealed_block_1_strategy_metrics_accessed"] is False
    assert residual_attribution["blindness"]["sealed_block_1_terminal_outcomes_accessed"] is False
    assert residual_attribution["blindness"]["registered_before_block_1_scoring"] is True
    assert residual_attribution["blindness"]["active_rule_or_threshold_changed"] is False
    assert [row["name"] for row in residual_attribution["registered_comparators"]] == [
        "baseline_primary_low_edge",
        "paired_book_validity_only",
        "frozen_residual_rule",
    ]
    assert residual_attribution["promotion_effect"]["changes_current_block_decision_boundary"] is False
    assert residual_attribution["promotion_effect"]["authorizes_a_plus_claim"] is False
    assert residual_attribution["promotion_effect"]["authorizes_profitability_claim"] is False
    attribution_impl = residual_attribution["implementation"]
    assert attribution_impl["screen_schema_version_before"] == 5
    assert attribution_impl["screen_schema_version_after"] == 6
    assert attribution_impl["gating_behavior_changed"] is False
    assert attribution_impl["strategy_rule_or_threshold_changed"] is False
    assert attribution_impl["tests"]["result"] == "23 passed; 0 failed"
    assert attribution_impl["linux_measurement_binary"]["schema_6_binary_built_or_installed"] is False
    for source in residual_attribution["source_evidence"]:
        source_path = ROOT / source["path"]
        assert hashlib.sha256(source_path.read_bytes()).hexdigest() == source["sha256"]

    assert tick_conformance["status"] == (
        "PRESCORE_TICK_CONFORMANCE_HARDENED_ACTIVE_RULE_UNCHANGED"
    )
    assert tick_conformance["blindness"]["sealed_block_1_strategy_metrics_accessed"] is False
    assert tick_conformance["blindness"]["sealed_block_1_terminal_outcomes_accessed"] is False
    assert tick_conformance["blindness"]["sealed_support_at_decision"] == 310
    assert tick_conformance["blindness"]["registered_before_block_1_scoring"] is True
    assert tick_conformance["frozen_rule_integrity"]["rule_expression_changed"] is False
    assert tick_conformance["frozen_rule_integrity"]["residual_multiplier"] == 2.0
    assert tick_conformance["frozen_rule_integrity"]["residual_threshold_changed"] is False
    assert tick_conformance["frozen_rule_integrity"]["classification_gates_changed"] is False
    assert tick_conformance["frozen_rule_integrity"]["economic_gates_changed"] is False
    assert tick_conformance["implementation"]["targeted_test_result"].startswith(
        "PASS: 24 binary_complement tests"
    )
    assert tick_conformance["implementation"]["full_suite_result"].startswith(
        "PASS: 546 tests"
    )
    assert tick_conformance["deployment_boundary"]["active_measurement_binary_changed"] is False
    assert tick_conformance["deployment_boundary"]["schema_6_linux_scorer_built_or_installed"] is False
    assert tick_conformance["promotion_effect"]["authorizes_scoring_before_750"] is False
    assert tick_conformance["promotion_effect"]["authorizes_a_plus_claim"] is False
    assert tick_conformance["promotion_effect"]["authorizes_profitability_claim"] is False
    for pin_name in (
        "parent_preregistration",
        "residual_attribution_amendment",
        "causal_tick_integrity_amendment",
    ):
        pin = tick_conformance["source_pins"][pin_name]
        assert hashlib.sha256((ROOT / pin["path"]).read_bytes()).hexdigest() == pin["sha256"]

    assert baseline_reproduction["status"] == (
        "PRESCORE_BASELINE_DECISION_AND_TOKEN_IDENTITY_REPRODUCTION_HARDENED_ACTIVE_RULE_UNCHANGED"
    )
    assert baseline_reproduction["blindness"]["sealed_block_1_strategy_metrics_accessed"] is False
    assert baseline_reproduction["blindness"]["sealed_block_1_terminal_outcomes_accessed"] is False
    assert baseline_reproduction["blindness"]["sealed_support_at_decision"] == 310
    assert baseline_reproduction["frozen_strategy_integrity"]["baseline_primary_minimum_edge"] == 0.07
    assert baseline_reproduction["frozen_strategy_integrity"]["residual_multiplier"] == 2.0
    assert baseline_reproduction["frozen_strategy_integrity"]["strategy_rule_or_threshold_changed"] is False
    assert baseline_reproduction["frozen_strategy_integrity"]["block_membership_policy_changed"] is True
    assert baseline_reproduction["implementation"]["opportunity_report_fields_added"] == [
        "fair_value_btc",
        "fair_value_open_btc",
        "market_fees_enabled",
        "market_taker_fee_rate",
        "market_category",
    ]
    assert baseline_reproduction["implementation"]["active_raw_capture_format_changed"] is False
    assert baseline_reproduction["implementation"]["resolution_manifest_outcome_identity_required"] is True
    assert baseline_reproduction["implementation"]["terminal_direction_reproduced_from_btc_and_outcome_prices"] is True
    assert baseline_reproduction["implementation"]["entry_fee_rate_reproduced_from_market_metadata"] is True
    assert baseline_reproduction["implementation"]["exact_five_minute_resolution_window_required"] is True
    assert baseline_reproduction["implementation"]["collector_overshoot_truncated_deterministically"] is True
    assert baseline_reproduction["implementation"]["recollection_required"] is False
    assert baseline_reproduction["validation"]["focused_result"].startswith(
        "PASS: 30 binary_complement tests"
    )
    assert baseline_reproduction["validation"]["full_suite_result"].startswith(
        "PASS: 552 tests"
    )
    assert baseline_reproduction["promotion_effect"]["authorizes_scoring_before_750"] is False
    assert baseline_reproduction["promotion_effect"]["authorizes_a_plus_claim"] is False
    assert baseline_reproduction["promotion_effect"]["authorizes_profitability_claim"] is False
    for pin_name in ("capture_variant", "prior_tick_conformance_amendment"):
        pin = baseline_reproduction["source_pins"][pin_name]
        assert hashlib.sha256((ROOT / pin["path"]).read_bytes()).hexdigest() == pin["sha256"]

    assert current_collection["floor"]["unique_ready_terminal_conditions"] == 241
    assert current_collection["floor"]["target_terminal_conditions"] == 750
    assert current_collection["incident"]["observed_support_before_recovery"] == 43
    assert current_collection["repair"]["support_recovered"] == 198
    assert current_collection["repair"]["segments_ready_after_refresh"] == 11
    assert current_collection["strategy_metrics_disclosed"] is False
    assert current_collection["strategy_score_emitted"] is False
    assert support_310["status"] == (
        "SEALED_SUPPORT_310_OF_750_SESSION_019_COLLECTING_NO_STRATEGY_DISCLOSURE"
    )
    assert support_310["floor"] == {
        "previous_unique_ready_terminal_conditions": 290,
        "unique_ready_terminal_conditions": 310,
        "support_added_by_session_018": 20,
        "target_terminal_conditions": 750,
        "remaining_terminal_conditions": 440,
        "completion_fraction": 310 / 750,
        "new_segments_completed": 16,
        "maximum_new_segments": 96,
        "state": "COLLECTING",
    }
    assert support_310["session_018"]["captured_conditions"] == 24
    assert support_310["session_018"]["admissible_conditions"] == 20
    assert support_310["session_018"]["excluded_conditions"] == 4
    assert support_310["session_018"]["capture_verified"] is True
    assert support_310["session_018"]["full_segment_signal_coverage"] is True
    assert support_310["session_018"]["resolution_ready"] is True
    assert support_310["session_018"]["resolution_verdict"] == "ALL_ADMISSIBLE_GROUPS_READY"
    assert support_310["session_018"]["session_owned_frames_deleted"] is True
    assert support_310["session_018"]["recommended_replay_latency_ms"] == 108
    assert support_310["session_018"]["registered_minimum_replay_latency_ms_unchanged"] == 202
    assert support_310["operator_observation"]["active_session"] == (
        "binary-complement-block1-floor-019"
    )
    assert support_310["operator_observation"]["active_process_verified"] is True
    assert support_310["operator_observation"]["failed_systemd_units"] == 0
    assert support_310["blindness"] == {
        "strategy_metrics_disclosed": False,
        "strategy_score_emitted": False,
        "strategy_candidates_inspected": False,
        "strategy_outcomes_inspected": False,
        "strategy_rates_or_economics_inspected": False,
        "support_and_operational_health_only": True,
    }
    assert support_310["decision"] == {
        "continue_bounded_collection": True,
        "score_below_750": False,
        "change_rule_or_threshold": False,
        "a_plus_claim": False,
        "profitability_claim": False,
        "paper_or_live_trading_authorized": False,
    }
    for source in support_310["source_snapshots"]:
        assert hashlib.sha256((ROOT / source["path"]).read_bytes()).hexdigest() == source["sha256"]

    assert support_345["status"] == (
        "SEALED_SUPPORT_345_OF_750_SESSION_020_INDEPENDENTLY_VERIFIED_"
        "SESSION_022_COLLECTING_NO_STRATEGY_DISCLOSURE"
    )
    assert support_345["floor"] == {
        "support_before_session_020": 310,
        "support_after_session_020": 329,
        "unique_ready_terminal_conditions": 345,
        "support_added_by_session_020": 19,
        "support_added_by_session_021": 16,
        "target_terminal_conditions": 750,
        "remaining_terminal_conditions": 405,
        "completion_fraction": 345 / 750,
        "new_segments_completed": 18,
        "maximum_new_segments": 96,
        "state": "COLLECTING",
    }
    for session in ("session_020", "session_021"):
        assert support_345[session]["captured_conditions"] == 24
        assert support_345[session]["capture_verified"] is True
        assert support_345[session]["resolution_ready"] is True
        assert support_345[session]["resolution_total_groups"] == support_345[session][
            "resolution_ready_groups"
        ]
        assert support_345[session]["resolution_verdict"] == (
            "ALL_ADMISSIBLE_GROUPS_READY"
        )
        assert support_345[session]["session_owned_frames_deleted"] is True
    assert support_345["session_020"]["admissible_conditions"] == 19
    assert support_345["session_020"]["full_segment_signal_coverage"] is True
    assert support_345["session_021"]["admissible_conditions"] == 16
    assert support_345["session_021"]["full_segment_signal_coverage"] is False
    assert support_345["session_021"]["fail_closed_per_condition_audit_applied"] is True
    assert support_345["independent_reconciliation"] == {
        "observed_at": "2026-07-21T17:34:35Z",
        "registered_source_roots": 3,
        "collector_unique_ready_condition_count": 345,
        "floor_ledger_unique_ready_condition_count": 345,
        "counts_match": True,
    }
    assert support_345["operator_observation"]["active_session"] == (
        "binary-complement-block1-floor-022"
    )
    assert support_345["operator_observation"]["collector_service_active"] is True
    assert support_345["operator_observation"]["collector_service_restarts"] == 0
    assert support_345["operator_observation"]["failed_systemd_units"] == 0
    assert support_345["blindness"] == support_310["blindness"]
    assert support_345["decision"] == support_310["decision"]
    for source in support_345["source_snapshots"]:
        assert hashlib.sha256((ROOT / source["path"]).read_bytes()).hexdigest() == source["sha256"]

    assert vps_restart["status"] == (
        "POLYMOMENTUM_SERVICES_RESTARTED_BOUNDED_COLLECTION_RESUMED"
    )
    assert vps_restart["service_observation"]["engine"]["venue"] == "paper_only"
    assert vps_restart["service_observation"]["failed_polymomentum_units"] == 0
    assert vps_restart["bounded_collection"]["sealed_support_before_restart"] == 310
    assert vps_restart["bounded_collection"]["target_terminal_conditions"] == 750
    assert vps_restart["bounded_collection"]["interrupted_session_disposition"] == (
        "FAIL_CLOSED_UNSCORED"
    )
    assert vps_restart["bounded_collection"]["active_session"] == (
        "binary-complement-block1-floor-020"
    )
    assert vps_restart["bounded_collection"]["strategy_metrics_disclosed"] is False
    assert vps_restart["bounded_collection"]["strategy_score_emitted"] is False
    assert vps_restart["authority"]["production_strategy_changed"] is False
    assert vps_restart["authority"]["paper_strategy_changed"] is False
    assert vps_restart["authority"]["active_forward_outcomes_accessed"] is False
    assert support_quality["checks"]["accounting_reconciles"] is True
    assert support_quality["support_accounting"]["unique_ready_terminal_conditions"] == 241
    assert support_quality["support_accounting"]["admissible_conditions"] == 241
    assert support_quality["checks"]["manifest_market_rows"] == 241
    assert support_quality["checks"]["unique_condition_ids"] == 241
    assert support_quality["checks"]["duplicate_condition_rows"] == 0
    assert support_quality["checks"]["terminal_pending_segments"] == 0
    assert support_quality["decision"]["safe_to_continue_collection"] is True
    assert support_quality["decision"]["safe_to_score_strategy_now"] is False

    assert (
        tick_integrity["status"]
        == "PRE_SCORE_CAUSAL_TICK_INPUT_HARDENED_FORWARD_METRICS_SEALED"
    )
    assert tick_integrity["integrity_boundary"]["forward_strategy_metrics_seen"] is False
    assert tick_integrity["integrity_boundary"]["opportunity_reports_generated"] is False
    assert tick_integrity["integrity_boundary"]["feature_input_contract_changed"] is True
    assert tick_integrity["integrity_boundary"]["fixed_rule_expression_changed"] is False
    observed_tick = tick_integrity["observed_forward_capture"]
    assert observed_tick["raw_tick_size_change_rows"] == 24
    assert observed_tick["distinct_markets_with_tick_change"] == 6
    assert observed_tick["old_tick_sizes"] == ["0.01"]
    assert observed_tick["new_tick_sizes"] == ["0.001"]
    assert observed_tick["events_before_240_seconds"] == 0
    assert (
        observed_tick["minimum_event_offset_from_market_open_seconds"]
        > 180
    )
    assert tick_integrity["candidate_timing_proof"]["observed_overlap_with_eligible_window"] is False
    assert tick_integrity["decision"]["current_block_remains_usable"] is True
    assert tick_integrity["decision"]["strategy_score_remains_sealed"] is True
    assert tick_integrity["validation"]["full_suite_result"].startswith(
        "PASS: cargo test; 497 passed"
    )
    reconstruction = tick_integrity["direct_reconstruction_spot_check"]
    assert reconstruction["threshold_crossing_preserved"] is True
    assert reconstruction["reconstruction_delay_ms"] == 3
    assert (
        reconstruction["first_subsequent_preserved_price_change_ts_ms"]
        - reconstruction["first_explicit_tick_event_ts_ms"]
        == 3
    )
    assert (
        tick_integrity["implementation"]["source_sha256"]
        == execution_tick_parity["implementation"][
            "harness_sha256_before_execution_parity_repair"
        ]
    )
    assert (
        hashlib.sha256((ROOT / "rust_engine/src/strategy/microstructure.rs").read_bytes()).hexdigest()
        == tick_integrity["implementation"]["shared_tick_policy_source_sha256"]
    )
    assert (
        tick_integrity["implementation"]["converter_source_sha256"]
        == "fc7e2aa4f0c2544c6dd4ff77bc7e157d0378a9056a9085237eb656e70d2472b2"
    )
    assert (
        hashlib.sha256((ROOT / "deploy/capture-forward-segments.sh").read_bytes()).hexdigest()
        == tick_integrity["implementation"]["capture_preflight_sha256"]
    )
    assert (
        tick_integrity["implementation"]["scorer_source_sha256_unchanged"]
        == "6e661887245634c8085009bcb17506d3fa13a16939334fb2ce6c810d9cae7312"
    )
    assert tick_integrity["implementation"]["measurement_recorder_changed"] is False
    assert tick_integrity["implementation"]["measurement_converter_changed"] is True
    assert tick_integrity["implementation"]["conversion_manifest_tick_audit_added"] is True
    assert len(tick_integrity["implementation"]["conversion_fail_closed_conditions"]) == 3
    deployed_v5 = tick_integrity["deployed_measurement_v5"]
    assert deployed_v5["binary_sha256"] == "13db9884e6a1ee5d6b21bac640f62bc40124e8b4bbe01de342cc7dcbf10e6b5a"
    assert deployed_v5["runner_sha256"] == tick_integrity["implementation"]["capture_preflight_sha256"]
    assert (
        hashlib.sha256((ROOT / "deploy/collect-binary-complement-floor.sh").read_bytes()).hexdigest()
        == deployed_v5["collector_sha256"]
    )
    assert deployed_v5["linux_linkage_verified"] is True
    assert deployed_v5["bounded_dry_run_verified"] is True
    assert deployed_v5["collector_switched_to_v5"] is True
    assert deployed_v5["active_session_binary"] == "measurement-v5"
    assert deployed_v5["completed_session_011_interrupted"] is False
    assert deployed_v5["partial_v4_session_012_stopped_during_preroll"] is True
    assert deployed_v5["partial_session_012_has_status_or_promotion_evidence"] is False
    assert deployed_v5["partial_session_012_data_deleted"] is False
    assert deployed_v5["active_unit_restarts"] == 0
    causal_tick_registration = preregistration["causal_tick_input_amendment"]
    assert causal_tick_registration["forward_strategy_metrics_seen"] is False
    assert causal_tick_registration["fixed_rule_expression_or_multiplier_changed"] is False
    assert causal_tick_registration["feature_input_contract_changed"] is True
    assert causal_tick_registration["artifact"].endswith(
        "20260720_binary_complement_causal_tick_integrity_amendment.json"
    )
    assert "0.001" in preregistration["feature_contract"]["tick_size"]
    prereg_tick = preregistration["instrumentation"]["causal_tick_input"]
    assert prereg_tick["harness_source_sha256"] == tick_integrity["implementation"]["source_sha256"]
    assert prereg_tick["shared_tick_policy_source_sha256"] == tick_integrity["implementation"]["shared_tick_policy_source_sha256"]
    assert prereg_tick["converter_source_sha256"] == tick_integrity["implementation"]["converter_source_sha256"]
    assert prereg_tick["conversion_manifest_audit_required_for_v5_segments"] is True

    assert (
        pair_reproduction["status"]
        == "PRE_SCORE_PAIRED_BOOK_REPRODUCTION_HARDENED_FORWARD_METRICS_SEALED"
    )
    assert pair_reproduction["defect"]["forward_strategy_metrics_inspected"] is False
    assert pair_reproduction["research_interpretation"]["fixed_rule_changed"] is False
    assert pair_reproduction["research_interpretation"]["threshold_search_performed"] is False
    assert pair_reproduction["implementation"]["screen_schema_version"] == 4
    assert pair_reproduction["implementation"]["capture_format_changed"] is False
    assert pair_reproduction["implementation"]["recollection_required"] is False
    assert len(pair_reproduction["implementation"]["tests_added"]) == 4
    current_scorer_hash = hashlib.sha256(
        (ROOT / "rust_engine/src/strategy_builder.rs").read_bytes()
    ).hexdigest()
    historical_scorer_hash = pair_reproduction["implementation"]["source_sha256"]
    schema_5_scorer_hash = realized_support["implementation"]["source_sha256_after"]
    assert historical_scorer_hash == realized_support["implementation"]["source_sha256_before"]
    assert schema_5_scorer_hash == attribution_impl["source_sha256_before"]
    assert (
        attribution_impl["source_sha256_after"]
        == tick_conformance["source_pins"]["scorer_source_before"]["sha256"]
    )
    assert (
        preregistration["instrumentation"]["scorer"]["source_sha256"]
        == tick_conformance["source_pins"]["scorer_source_before"]["sha256"]
    )
    assert (
        tick_conformance["implementation"]["source_sha256"]
        == baseline_reproduction["source_pins"]["scorer_source_before"]["sha256"]
    )
    assert current_scorer_hash == baseline_reproduction["implementation"]["scorer_source_sha256"]
    assert hashlib.sha256(
        (ROOT / baseline_reproduction["implementation"]["harness_source"]).read_bytes()
    ).hexdigest() == baseline_reproduction["implementation"]["harness_source_sha256"]
    assert hashlib.sha256(
        (REGISTRY / "20260715_binary_complement_coherence_preregistration.json").read_bytes()
    ).hexdigest() == attribution_impl["parent_preregistration_sha256_after_implementation"]
    assert (
        residual_attribution["parent_preregistration_at_registration"]["sha256"]
        == realized_support["implementation"]["preregistration_sha256_after_amendment"]
    )
    assert (
        preregistration["instrumentation"]["scorer"]["residual_attribution_audit"]
        == "deploy/promotions/evidence/strategy_registry/20260721_binary_complement_residual_attribution_amendment.json"
    )
    assert (
        preregistration["instrumentation"]["scorer"]["paired_book_reproduction_audit"]
        == "deploy/promotions/evidence/strategy_registry/20260720_binary_complement_paired_book_reproduction_amendment.json"
    )
    deployed_v6 = pair_reproduction["deployed_scorer_v6"]
    assert deployed_v6["sha256"] == "0e90894902fdc5c5beaed072f3535fcc8cecb1764761ec2581aaabaa63b29140"
    assert deployed_v6["linux_x86_64_linkage_verified"] is True
    assert deployed_v6["help_verified_as_polymomentum"] is True
    assert deployed_v6["active_v5_capture_interrupted"] is False
    assert deployed_v6["active_v5_capture_restarts"] == 0
    assert pair_reproduction["validation"]["cargo_test"].startswith(
        "PASS: 501 passed"
    )
    assert pair_reproduction["validation"]["forward_scores_disclosed"] == 0
    assert pair_reproduction["decision"]["safe_to_continue_sealed_collection"] is True
    assert pair_reproduction["decision"]["safe_to_score_below_750"] is False
    assert pair_reproduction["decision"]["a_plus_claim"] is False
    assert pair_reproduction["decision"]["profitability_claim"] is False

    assert (
        execution_tick_parity["status"]
        == "PRE_SCORE_EXACT_REPLAY_TICK_PARITY_HARDENED_FORWARD_METRICS_SEALED"
    )
    assert execution_tick_parity["defect"]["observed_forward_strategy_metrics_inspected"] is False
    assert execution_tick_parity["defect"]["active_capture_data_invalidated"] is False
    assert execution_tick_parity["repair_contract"]["production_behavior_changed"] is False
    assert execution_tick_parity["repair_contract"]["capture_format_changed"] is False
    assert execution_tick_parity["repair_contract"]["scorer_schema_or_rule_changed"] is False
    assert execution_tick_parity["repair_contract"]["recollection_required"] is False
    assert execution_tick_parity["fee_and_order_contract_audit"]["crypto_taker_fee_rate"] == 0.07
    assert execution_tick_parity["fee_and_order_contract_audit"]["maker_fee_rate"] == 0.0
    assert execution_tick_parity["fee_and_order_contract_audit"]["systematic_profitability_inflation_found"] is False
    assert execution_tick_parity["fee_and_order_contract_audit"]["exact_match_level_rounding_parity_proven"] is False
    execution_impl = execution_tick_parity["implementation"]
    assert (
        execution_impl["harness_sha256_after_execution_parity_repair"]
        == "5ea335ce79c9a0c061b44ee1479da2a6e06ae8d2365902f264df3f6f61372216"
    )
    assert hashlib.sha256(
        (ROOT / execution_impl["live_replay_file"]).read_bytes()
    ).hexdigest() == execution_impl["live_replay_sha256_after_execution_parity_repair"]
    assert hashlib.sha256(
        (ROOT / execution_impl["sizing_file"]).read_bytes()
    ).hexdigest() == execution_impl["sizing_sha256"]
    assert hashlib.sha256(
        (ROOT / execution_impl["fee_file"]).read_bytes()
    ).hexdigest() == execution_impl["fee_sha256"]
    assert hashlib.sha256(
        (ROOT / execution_impl["fill_model_file"]).read_bytes()
    ).hexdigest() == execution_impl["fill_model_sha256"]
    assert historical_scorer_hash == execution_impl["scorer_sha256_unchanged"]
    assert len(execution_impl["tests_added"]) == 3
    deployed_v7 = execution_tick_parity["deployed_measurement_v7"]
    assert deployed_v7["sha256"] == "5f89c797c0e5eb0ca4758a89eaa4a16e00b1480894b17cf5c9926878175277a0"
    assert deployed_v7["linux_x86_64_linkage_verified"] is True
    assert deployed_v7["strategy_builder_help_verified_as_polymomentum"] is True
    assert deployed_v7["production_binary_replaced"] is False
    assert deployed_v7["active_v5_capture_interrupted"] is False
    assert deployed_v7["active_v5_capture_restarts"] == 0
    assert execution_tick_parity["validation"]["cargo_test"].startswith(
        "PASS: 504 passed"
    )
    assert execution_tick_parity["validation"]["forward_scores_disclosed"] == 0
    assert execution_tick_parity["decision"]["safe_to_score_below_750"] is False
    assert execution_tick_parity["decision"]["fixed_strategy_rule_or_gate_changed"] is False
    assert execution_tick_parity["decision"]["exact_replay_contract_changed"] is True
    assert execution_tick_parity["decision"]["a_plus_claim"] is False
    assert execution_tick_parity["decision"]["profitability_claim"] is False
    prereg_execution = preregistration["instrumentation"]["exact_replay_execution"]
    assert prereg_execution["audit"].endswith(
        "20260720_binary_complement_execution_tick_parity_amendment.json"
    )
    assert prereg_execution["harness_source_sha256"] == execution_impl["harness_sha256_after_execution_parity_repair"]
    assert prereg_execution["live_replay_source_sha256"] == execution_impl["live_replay_sha256_after_execution_parity_repair"]
    assert prereg_execution["measurement_v7_binary_sha256"] == deployed_v7["sha256"]
    assert (
        live_tick_parity["status"]
        == "PRE_SCORE_LIVE_RUNTIME_DYNAMIC_TICK_PARITY_HARDENED_FORWARD_METRICS_SEALED"
    )
    assert live_tick_parity["defect"]["forward_strategy_metrics_inspected"] is False
    assert live_tick_parity["defect"]["active_capture_data_invalidated"] is False
    live_repair = live_tick_parity["repair_contract"]
    assert live_repair["production_source_behavior_changed"] is True
    assert live_repair["production_binary_replaced_or_restarted"] is False
    assert live_repair["capture_format_changed"] is False
    assert live_repair["scorer_rule_schema_gate_or_threshold_changed"] is False
    assert live_repair["recollection_required"] is False
    live_impl = live_tick_parity["implementation"]
    assert hashlib.sha256(
        (ROOT / live_impl["market_feed_file"]).read_bytes()
    ).hexdigest() == live_impl["market_feed_sha256"]
    assert live_impl["live_pipeline_sha256"] == "a78abd0dbe6d92bb571a42c664f12d8e68c714d54f5a90c5ba6531bed918fd6e"
    assert hashlib.sha256(
        (ROOT / live_impl["shared_tick_policy_file"]).read_bytes()
    ).hexdigest() == live_impl["shared_tick_policy_sha256"]
    assert live_impl["exact_harness_sha256"] == execution_impl["harness_sha256_after_execution_parity_repair"]
    assert live_impl["cached_live_replay_sha256"] == execution_impl["live_replay_sha256_after_execution_parity_repair"]
    assert live_impl["scorer_sha256_unchanged"] == historical_scorer_hash
    assert len(live_impl["tests_added"]) == 2
    deployed_v8 = live_tick_parity["deployed_measurement_v8"]
    assert deployed_v8["sha256"] == "ee9886d80838ec5b4e5bde96795362c46061623f5a98e369c405f192b63e44be"
    assert deployed_v8["linux_x86_64_linkage_verified"] is True
    assert deployed_v8["strategy_builder_help_verified"] is True
    assert deployed_v8["production_binary_replaced"] is False
    assert deployed_v8["production_service_restarted"] is False
    assert deployed_v8["active_v5_capture_interrupted"] is False
    assert deployed_v8["active_v5_capture_restarts"] == 0
    assert live_tick_parity["validation"]["cargo_test"].startswith(
        "PASS: 506 passed"
    )
    assert live_tick_parity["validation"]["forward_scores_disclosed"] == 0
    assert live_tick_parity["decision"]["safe_to_score_below_750"] is False
    assert live_tick_parity["decision"]["fixed_strategy_rule_or_gate_changed"] is False
    assert live_tick_parity["decision"]["live_runtime_parity_contract_changed"] is True
    assert live_tick_parity["decision"]["a_plus_claim"] is False
    assert live_tick_parity["decision"]["profitability_claim"] is False
    prereg_live = preregistration["instrumentation"]["live_runtime_execution"]
    assert prereg_live["audit"].endswith(
        "20260720_binary_complement_live_tick_parity_amendment.json"
    )
    assert prereg_live["market_feed_source_sha256"] == live_impl["market_feed_sha256"]
    assert prereg_live["live_pipeline_source_sha256"] == live_impl["live_pipeline_sha256"]
    assert prereg_live["measurement_v8_binary_sha256"] == deployed_v8["sha256"]
    assert (
        live_reconciliation_parity["status"]
        == "PRE_SCORE_LIVE_USER_RECONCILIATION_PARITY_HARDENED_FORWARD_METRICS_SEALED"
    )
    reconciliation_impl = live_reconciliation_parity["implementation"]
    for hash_key in (
        "user_channel_sha256",
        "gamma_sha256",
        "live_pipeline_sha256",
        "fee_sha256",
    ):
        assert len(reconciliation_impl[hash_key]) == 64
    assert reconciliation_impl["scorer_sha256_unchanged"] == historical_scorer_hash
    assert len(reconciliation_impl["tests_added"]) == 4
    deployed_v9 = live_reconciliation_parity["deployed_measurement_v9"]
    assert deployed_v9["sha256"] == "bdf301f5ea28cdd28c42be3f1b9288ecd71b91f05af18d986f211b3e0efa2511"
    assert deployed_v9["linux_x86_64_linkage_verified"] is True
    assert deployed_v9["strategy_builder_help_verified"] is True
    assert deployed_v9["production_binary_replaced"] is False
    assert deployed_v9["production_service_restarted"] is False
    assert deployed_v9["active_v5_capture_interrupted"] is False
    assert deployed_v9["active_v5_capture_restarts"] == 0
    assert live_reconciliation_parity["validation"]["cargo_test"].startswith(
        "PASS: 510 passed"
    )
    assert live_reconciliation_parity["validation"]["forward_scores_disclosed"] == 0
    assert live_reconciliation_parity["known_remaining_boundary"]["automated_rest_recovery_implemented"] is False
    assert live_reconciliation_parity["decision"]["safe_to_score_below_750"] is False
    assert live_reconciliation_parity["decision"]["safe_for_live_promotion_now"] is False
    assert live_reconciliation_parity["decision"]["a_plus_claim"] is False
    assert live_reconciliation_parity["decision"]["profitability_claim"] is False
    prereg_reconciliation = preregistration["instrumentation"]["live_runtime_reconciliation"]
    assert prereg_reconciliation["audit"].endswith(
        "20260721_binary_complement_live_reconciliation_parity_amendment.json"
    )
    assert prereg_reconciliation["user_channel_source_sha256"] == reconciliation_impl["user_channel_sha256"]
    assert prereg_reconciliation["gamma_source_sha256"] == reconciliation_impl["gamma_sha256"]
    assert prereg_reconciliation["live_pipeline_source_sha256"] == reconciliation_impl["live_pipeline_sha256"]
    assert prereg_reconciliation["measurement_v9_binary_sha256"] == deployed_v9["sha256"]
    assert prereg_reconciliation["live_promotion_blocked_on_automated_rest_recovery"] is False
    assert (
        rest_recovery_parity["status"]
        == "PRE_SCORE_AUTHENTICATED_REST_RECOVERY_PARITY_HARDENED_FORWARD_METRICS_SEALED"
    )
    rest_impl = rest_recovery_parity["implementation"]
    for file_key, hash_key in (
        ("clob_file", "clob_sha256"),
        ("user_channel_file", "user_channel_sha256"),
        ("order_manager_file", "order_manager_sha256"),
        ("live_pipeline_file", "live_pipeline_sha256"),
        ("risk_manager_file", "risk_manager_sha256"),
        ("signing_file", "signing_sha256"),
    ):
        assert hashlib.sha256(
            (ROOT / rest_impl[file_key]).read_bytes()
        ).hexdigest() == rest_impl[hash_key]
    assert rest_impl["scorer_sha256_unchanged"] == historical_scorer_hash
    deployed_v10 = rest_recovery_parity["deployed_measurement_v10"]
    assert deployed_v10["sha256"] == "03073aefcad8f5a403e55e5a472d81a915632c131db3c3a241d326845e08585f"
    assert deployed_v10["linux_x86_64_linkage_verified"] is True
    assert deployed_v10["cli_help_verified"] is True
    assert deployed_v10["production_binary_replaced"] is False
    assert deployed_v10["production_service_restarted"] is False
    assert deployed_v10["active_v5_capture_interrupted"] is False
    assert rest_recovery_parity["validation"]["cargo_test"].startswith("PASS: 517 passed")
    assert rest_recovery_parity["validation"]["forward_scores_disclosed"] == 0
    assert rest_recovery_parity["decision"]["automated_rest_recovery_implemented"] is True
    assert rest_recovery_parity["decision"]["safe_to_score_below_750"] is False
    assert rest_recovery_parity["decision"]["safe_for_live_promotion_now"] is False
    assert rest_recovery_parity["decision"]["a_plus_claim"] is False
    assert rest_recovery_parity["decision"]["profitability_claim"] is False
    prereg_rest = preregistration["instrumentation"]["authenticated_rest_recovery"]
    assert prereg_rest["audit"].endswith(
        "20260721_binary_complement_rest_recovery_parity_amendment.json"
    )
    assert prereg_rest["measurement_v10_binary_sha256"] == deployed_v10["sha256"]
    for prereg_key, implementation_key in (
        ("clob_source_sha256", "clob_sha256"),
        ("user_channel_source_sha256", "user_channel_sha256"),
        ("order_manager_source_sha256", "order_manager_sha256"),
        ("live_pipeline_source_sha256", "live_pipeline_sha256"),
        ("risk_manager_source_sha256", "risk_manager_sha256"),
        ("signing_source_sha256", "signing_sha256"),
    ):
        assert prereg_rest[prereg_key] == rest_impl[implementation_key]
    scorer_deployment = preregistration["instrumentation"]["scorer"]
    assert scorer_deployment["previous_schema_5_binary"] == deployed_v11["path"]
    assert scorer_deployment["previous_schema_5_binary_sha256"] == deployed_v11["sha256"]
    assert scorer_deployment["schema_6_linux_binary_status"].startswith("NOT_BUILT_OR_INSTALLED")
    assert scorer_deployment["realized_support_audit"].endswith(
        "20260721_binary_complement_realized_support_amendment.json"
    )

    candidate_rate = 102 / 631
    loss_rate = 23 / 631
    expected_candidates = 750 * candidate_rate
    expected_losses = 750 * loss_rate
    close(
        power["amended_floor_diagnostic"]["expected_baseline_candidates"],
        expected_candidates,
    )
    close(
        power["amended_floor_diagnostic"]["expected_baseline_losses"],
        expected_losses,
    )
    close(
        power["amended_floor_diagnostic"][
            "probability_of_at_least_100_baseline_candidates"
        ],
        binomial_tail(750, candidate_rate, 100),
        tolerance=5e-5,
    )
    close(
        power["amended_floor_diagnostic"]["probability_of_at_least_15_baseline_losses"],
        binomial_tail(750, loss_rate, 15),
        tolerance=5e-5,
    )

    datasets = artifact["snapshot"]["datasets"]
    headline = datasets["headline"][0]
    assert headline == {
        "grade": "A-",
        "active_hypotheses": 1,
        "conditions_per_block": 750,
        "forward_scores": 0,
    }
    portfolio = {row["mechanism"]: row for row in datasets["portfolio"]}
    close(portfolio["Strict-42 baseline"]["pnl_usd"], strict42["baseline"]["fee_inclusive_pnl_usd"])
    close(
        portfolio["Immediate complete-set v1"]["pnl_usd"],
        v1["aggregate"]["candidate"]["total_pnl_usd"],
    )
    close(
        portfolio["Trailing complete-set v2"]["pnl_usd"],
        v2["aggregate"]["candidate"]["total_pnl_usd"],
    )
    assert len(artifact["manifest"]["charts"]) == 3
    assert len(artifact["manifest"]["tables"]) == 12
    assert "fixed_support_capacity" in datasets
    assert len(datasets["fixed_support_capacity"]) == 4
    assert {
        block["id"] for block in artifact["manifest"]["blocks"]
    } >= {
        "capacity_finding",
        "capacity_table_block",
        "calibration_finding",
        "calibration_chart_block",
        "pressure_redundancy_finding",
        "pressure_redundancy_table_block",
        "residual_independence_finding",
        "residual_independence_table_block",
        "paired_wait_cost_finding",
        "paired_wait_cost_table_block",
        "settlement_anchor_finding",
        "settlement_anchor_table_block",
        "settlement_anchor_historical_outcome_finding",
        "settlement_anchor_historical_outcome_table_block",
        "fast_volatility_finding",
        "fast_volatility_table_block",
        "dvol_source_finding",
        "empirical_cdf_finding",
        "empirical_cdf_table_block",
        "probability_model_decision",
        "paired_depth_finding",
        "paired_depth_table_block",
        "economic_alignment_finding",
        "economic_alignment_table_block",
        "tick_integrity_finding",
        "pair_reproduction_finding",
        "execution_tick_parity_finding",
        "live_tick_parity_finding",
        "live_reconciliation_parity_finding",
        "rest_recovery_parity_finding",
        "realized_support_finding",
    }
    assert {
        source["id"] for source in artifact["manifest"]["sources"]
    } >= {
        "fixed_support_plan",
        "strategy_diagnostic",
        "paired_pressure_diagnostic",
        "paired_pressure_notebook",
        "residual_independence_diagnostic",
        "residual_independence_notebook",
        "residual_cross_market_replication",
        "residual_cross_market_notebook",
        "non_candle_snapshot_manifest",
        "paired_wait_cost_diagnostic",
        "paired_wait_cost_notebook",
        "settlement_anchor_diagnostic",
        "settlement_anchor_notebook",
        "settlement_anchor_price_manifest",
        "settlement_anchor_preregistration",
        "settlement_anchor_evaluator_amendment",
        "settlement_anchor_historical_outcome",
        "settlement_anchor_historical_outcome_notebook",
        "fast_volatility_preregistration",
        "fast_volatility_evidence",
        "fast_volatility_integrity_amendment",
        "fast_volatility_notebook",
        "dvol_preregistration",
        "dvol_source_feasibility",
        "empirical_cdf_preregistration",
        "empirical_cdf_evidence",
        "empirical_cdf_notebook",
        "probability_model_validation",
        "paired_depth_diagnostic",
        "paired_depth_notebook",
        "residual_attribution_amendment",
        "economic_diagnostics_amendment",
        "unit_economics_amendment",
        "tick_integrity_amendment",
        "pair_reproduction_amendment",
        "execution_tick_parity_amendment",
        "live_tick_parity_amendment",
        "live_reconciliation_parity_amendment",
        "rest_recovery_parity_amendment",
        "realized_support_amendment",
        "support_only_status_310",
    }
    assert len(datasets["fast_volatility"]) == 6
    assert datasets["fast_volatility"][1]["observed"] == (
        "+0.000177 versus +0.000500 minimum"
    )
    assert datasets["fast_volatility"][-1]["observed"] == (
        "Reject fast_volatility_max_v1 without retuning"
    )
    assert len(datasets["empirical_return_cdf"]) == 9
    assert datasets["empirical_return_cdf"][2]["observed"] == (
        "+0.000521 versus +0.000500 minimum"
    )
    assert datasets["empirical_return_cdf"][3]["observed"] == (
        "+0.000302 versus +0.001000 minimum"
    )
    assert datasets["empirical_return_cdf"][-1]["observed"] == (
        "Reject empirical_standardized_return_cdf_v1 without retuning"
    )
    economic_alignment = datasets["economic_alignment"]
    assert len(economic_alignment) == 4
    close(economic_alignment[0]["value"], economic_history["baseline_profit_factor"], tolerance=1e-6)
    close(economic_alignment[1]["value"], proportional_pf, tolerance=1e-6)
    close(economic_alignment[2]["value"], 1.20)
    close(economic_alignment[3]["value"], deterioration_budget, tolerance=1e-6)
    calibration_gap = {row["estimate"]: row for row in datasets["calibration_gap"]}
    close(calibration_gap["Internal fair value"]["probability"], calibration["internal_fair_mean_probability"])
    close(calibration_gap["Market price"]["probability"], calibration["market_mean_probability"])
    close(calibration_gap["Realized outcomes"]["probability"], calibration["realized_win_rate"])
    pressure_redundancy = datasets["pressure_redundancy"]
    assert len(pressure_redundancy) == 5
    assert "6,465 / 7,200" in pressure_redundancy[0]["observed"]
    assert "0 disagreements / 12,930" in pressure_redundancy[1]["observed"]
    assert "Median, p99, and max error = 0" == pressure_redundancy[2]["observed"]
    assert "865 timestamp regressions" in pressure_redundancy[4]["observed"]
    residual_independence_rows = datasets["residual_independence"]
    assert len(residual_independence_rows) == 6
    assert "0 registered residual rejections" in residual_independence_rows[0]["observed"]
    assert "0 residual rejections" in residual_independence_rows[1]["observed"]
    assert "490 / 500 valid" in residual_independence_rows[2]["observed"]
    assert "0 / 490 rejections" in residual_independence_rows[3]["observed"]
    assert "5 / 5 BTC and 7 / 7 public" in residual_independence_rows[4]["observed"]
    assert "outcomes still sealed" in residual_independence_rows[5]["observed"]
    paired_wait_cost_rows = datasets["paired_wait_cost"]
    assert len(paired_wait_cost_rows) == 5
    assert "1,403 / 1,440" in paired_wait_cost_rows[0]["observed"]
    assert "10 s and 27 s" in paired_wait_cost_rows[1]["observed"]
    assert "0 / 2,880" in paired_wait_cost_rows[2]["observed"]
    assert "0 ask-deterioration" in paired_wait_cost_rows[3]["observed"]
    assert "active block excluded" in paired_wait_cost_rows[4]["observed"]
    settlement_anchor_rows = datasets["settlement_anchor"]
    assert len(settlement_anchor_rows) == 8
    assert "resolves on Chainlink" in settlement_anchor_rows[0]["observed"]
    assert "24 / 24" in settlement_anchor_rows[1]["observed"]
    assert "1,389 / 1,403" in settlement_anchor_rows[2]["observed"]
    assert "Median 2.02 pp" in settlement_anchor_rows[3]["observed"]
    assert "215 / 2,778" in settlement_anchor_rows[4]["observed"]
    assert "21 / 24" in settlement_anchor_rows[5]["observed"]
    assert "30 first-half versus 185 second-half" in settlement_anchor_rows[6]["observed"]
    assert "label-free source audit" in settlement_anchor_rows[7]["observed"]
    assert "paired scorer ready" in settlement_anchor_rows[7]["observed"]
    assert "concrete future lock pending" in settlement_anchor_rows[7]["observed"]
    assert "active block forbidden" in settlement_anchor_rows[7]["observed"]
    settlement_anchor_historical_rows = datasets["settlement_anchor_historical_outcome"]
    assert len(settlement_anchor_historical_rows) == 6
    assert "81 opportunity rows from 3 / 24" in settlement_anchor_historical_rows[0]["observed"]
    assert "81 / 81" in settlement_anchor_historical_rows[1]["observed"]
    assert "edge 0.072011" in settlement_anchor_historical_rows[2]["observed"]
    assert "edge 0.001399" in settlement_anchor_historical_rows[3]["observed"]
    assert "-0.314523" in settlement_anchor_historical_rows[4]["observed"]
    assert "Keep frozen contract unchanged" in settlement_anchor_historical_rows[5]["observed"]
    paired_depth_rows = datasets["paired_depth_capacity"]
    assert len(paired_depth_rows) == 5
    assert "490 / 490" in paired_depth_rows[0]["observed"]
    assert "470 / 490" in paired_depth_rows[1]["observed"]
    assert "0 incremental" in paired_depth_rows[2]["observed"]
    assert "460 / 490" in paired_depth_rows[3]["observed"]
    assert "No challenger" in paired_depth_rows[4]["observed"]
    evidence_state = {row["stage"]: row for row in datasets["evidence_state"]}
    assert evidence_state["Terminal refresh recovery"]["status"] == "pass"
    assert evidence_state["Operational continuation"]["status"] == "in progress"
    assert "310 of 750" in evidence_state["Block 1 disclosure"]["observed"]
    assert "schema-6 attribution implementation" in evidence_state["Measurement artifacts"]["observed"]

    code_cells = [cell for cell in notebook["cells"] if cell["cell_type"] == "code"]
    assert code_cells
    assert all(cell["execution_count"] is not None for cell in code_cells)
    assert not [
        output
        for cell in code_cells
        for output in cell.get("outputs", [])
        if output.get("output_type") == "error"
    ]
    notebook_source = "".join(
        source_line
        for cell in notebook["cells"]
        for source_line in cell.get("source", [])
    )
    assert "fixed_support" in notebook_source
    assert "conservative_cushion_gib" in notebook_source
    assert "economic_diagnostics" in notebook_source
    assert "unit_economics_amendment" in notebook_source
    assert "economic_alignment" in notebook_source
    assert "proportional_pf" in notebook_source

    paired_code_cells = [
        cell for cell in paired_pressure_notebook["cells"] if cell["cell_type"] == "code"
    ]
    assert paired_code_cells
    assert all(cell["execution_count"] is not None for cell in paired_code_cells)
    assert not [
        output
        for cell in paired_code_cells
        for output in cell.get("outputs", [])
        if output.get("output_type") == "error"
    ]
    paired_notebook_source = "".join(
        source_line
        for cell in paired_pressure_notebook["cells"]
        for source_line in cell.get("source", [])
    )
    assert "structural_identity_checks" in paired_notebook_source
    assert "terminal labels are never loaded" in paired_notebook_source
    assert "0 / 12,930" in paired_notebook_source
    assert "does **not** establish whether" in paired_notebook_source

    residual_notebook_code_cells = [
        cell
        for cell in residual_independence_notebook["cells"]
        if cell["cell_type"] == "code"
    ]
    assert residual_notebook_code_cells
    assert all(cell["execution_count"] is not None for cell in residual_notebook_code_cells)
    assert not [
        output
        for cell in residual_notebook_code_cells
        for output in cell.get("outputs", [])
        if output.get("output_type") == "error"
    ]
    residual_notebook_source = "".join(
        source_line
        for cell in residual_independence_notebook["cells"]
        for source_line in cell.get("source", [])
    )
    assert "paired-top validity" in residual_notebook_source
    assert "observed_freshness_rejections" in residual_notebook_source
    assert "strategy outcomes are never loaded" in residual_notebook_source

    paired_wait_cost_code_cells = [
        cell
        for cell in paired_wait_cost_notebook["cells"]
        if cell["cell_type"] == "code"
    ]
    assert paired_wait_cost_code_cells
    assert all(
        cell["execution_count"] is not None for cell in paired_wait_cost_code_cells
    )
    assert not [
        output
        for cell in paired_wait_cost_code_cells
        for output in cell.get("outputs", [])
        if output.get("output_type") == "error"
    ]
    paired_wait_cost_source = "".join(
        source_line
        for cell in paired_wait_cost_notebook["cells"]
        for source_line in cell.get("source", [])
    )
    assert "pair-only exposure" in paired_wait_cost_source
    assert "0 / 2,880" in paired_wait_cost_source
    assert "active_forward_block_loaded': False" in paired_wait_cost_source
    assert "strategy_outcomes_loaded': False" in paired_wait_cost_source

    settlement_anchor_code_cells = [
        cell
        for cell in settlement_anchor_notebook["cells"]
        if cell["cell_type"] == "code"
    ]
    assert len(settlement_anchor_code_cells) == 4
    assert all(
        cell["execution_count"] is not None for cell in settlement_anchor_code_cells
    )
    assert not [
        output
        for cell in settlement_anchor_code_cells
        for output in cell.get("outputs", [])
        if output.get("output_type") == "error"
    ]
    settlement_anchor_source = "".join(
        source_line
        for cell in settlement_anchor_notebook["cells"]
        for source_line in cell.get("source", [])
    )
    assert "settlement_source_anchor_v1" in settlement_anchor_source
    assert "orientation_edge_disagreements" in settlement_anchor_source
    assert "price_to_beat" in settlement_anchor_source
    assert "active_forward_block_loaded': False" in settlement_anchor_source
    assert "strategy_outcomes_loaded': False" in settlement_anchor_source

    settlement_anchor_historical_code_cells = [
        cell
        for cell in settlement_anchor_historical_outcome_notebook["cells"]
        if cell["cell_type"] == "code"
    ]
    assert len(settlement_anchor_historical_code_cells) == 5
    assert all(
        cell["execution_count"] is not None
        for cell in settlement_anchor_historical_code_cells
    )
    assert not [
        output
        for cell in settlement_anchor_historical_code_cells
        for output in cell.get("outputs", [])
        if output.get("output_type") == "error"
    ]
    assert all(
        "id" in cell for cell in settlement_anchor_historical_outcome_notebook["cells"]
    )
    settlement_anchor_historical_source = "".join(
        source_line
        for cell in settlement_anchor_historical_outcome_notebook["cells"]
        for source_line in cell.get("source", [])
    )
    assert (
        "RETROSPECTIVE_SIGNAL_GATED_DIAGNOSTIC_DIRECTIONALLY_NEGATIVE_INADEQUATE_SUPPORT"
        in settlement_anchor_historical_source
    )
    assert "sha256_gzip_payload" in settlement_anchor_historical_source
    assert (
        "paired_one_share_pnl_delta_across_all_24_conditions"
        in settlement_anchor_historical_source
    )
    assert "KEEP_FROZEN_FORWARD_CONTRACT_UNCHANGED" in settlement_anchor_historical_source

    fast_volatility_code_cells = [
        cell for cell in fast_volatility_notebook["cells"] if cell["cell_type"] == "code"
    ]
    assert len(fast_volatility_code_cells) == 5
    assert all(
        cell["execution_count"] is not None for cell in fast_volatility_code_cells
    )
    assert not [
        output
        for cell in fast_volatility_code_cells
        for output in cell.get("outputs", [])
        if output.get("output_type") == "error"
    ]
    assert all("id" in cell for cell in fast_volatility_notebook["cells"])
    fast_volatility_source = "".join(
        source_line
        for cell in fast_volatility_notebook["cells"]
        for source_line in cell.get("source", [])
    )
    assert "PUBLIC_PROXY_CALIBRATION_REJECTED_NO_RETUNING" in fast_volatility_source
    assert "overall_brier_improvement_at_least_0_0005" in fast_volatility_source
    assert "candidate_more_confident_forecasts" in fast_volatility_source
    assert "Preserve the active decision order" in fast_volatility_source

    empirical_cdf_code_cells = [
        cell for cell in empirical_cdf_notebook["cells"] if cell["cell_type"] == "code"
    ]
    assert len(empirical_cdf_code_cells) == 5
    assert all(cell["execution_count"] is not None for cell in empirical_cdf_code_cells)
    assert not [
        output
        for cell in empirical_cdf_code_cells
        for output in cell.get("outputs", [])
        if output.get("output_type") == "error"
    ]
    assert all("id" in cell for cell in empirical_cdf_notebook["cells"])
    empirical_cdf_source = "".join(
        source_line
        for cell in empirical_cdf_notebook["cells"]
        for source_line in cell.get("source", [])
    )
    assert "PUBLIC_PROXY_CALIBRATION_REJECTED_NO_RETUNING" in empirical_cdf_source
    assert "analyze_empirical_return_cdf.py" in empirical_cdf_source
    assert "validate_empirical_return_cdf.py" in empirical_cdf_source
    assert "Reject the family as registered" in empirical_cdf_source
    assert "Preserve the active decision order" in empirical_cdf_source

    cross_market_code_cells = [
        cell
        for cell in residual_cross_market_notebook["cells"]
        if cell["cell_type"] == "code"
    ]
    assert cross_market_code_cells
    assert all(cell["execution_count"] is not None for cell in cross_market_code_cells)
    assert not [
        output
        for cell in cross_market_code_cells
        for output in cell.get("outputs", [])
        if output.get("output_type") == "error"
    ]
    cross_market_source = "".join(
        source_line
        for cell in residual_cross_market_notebook["cells"]
        for source_line in cell.get("source", [])
    )
    assert "price extrema; do not trust REST array position" in cross_market_source
    assert "effective_live_tick = min(declared_tick, current_band_tick)" in cross_market_source
    assert "strict_0_002_threshold_rejections" in cross_market_source

    paired_depth_code_cells = [
        cell for cell in paired_depth_notebook["cells"] if cell["cell_type"] == "code"
    ]
    assert paired_depth_code_cells
    assert all(cell["execution_count"] is not None for cell in paired_depth_code_cells)
    assert not [
        output
        for cell in paired_depth_code_cells
        for output in cell.get("outputs", [])
        if output.get("output_type") == "error"
    ]
    paired_depth_source = "".join(
        source_line
        for cell in paired_depth_notebook["cells"]
        for source_line in cell.get("source", [])
    )
    assert "price extrema" not in paired_depth_source
    assert "derive best bid by maximum price" in paired_depth_source
    assert "paired_depth_challenger_preregistered" in paired_depth_source
    assert "strategy_outcomes_loaded': False" in paired_depth_source
    assert "active_forward_block_loaded': False" in paired_depth_source

    html = HTML.read_text()
    assert "Technical Summary" in html
    assert "Conditions per block" in html
    assert ">750<" in html
    assert "Fixed-support capacity checks" in html
    assert "4.77 GiB" in html
    assert "late-selection/payoff" in html
    assert "Classification-to-economics alignment" in html
    assert "fee-aware unit-economics" in html
    assert "Entry confidence is the dominant economic defect" in html
    assert "Opposite-book pressure adds no observed feature information" in html
    assert "0 / 12,930" in html
    assert "Paired-pressure mechanism screen" in html
    assert "chosen-token pressure predicts settlement or profitability" in html
    assert "Residual magnitude has no observed selectivity after paired-top validity" in html
    assert "0 / 6,465" in html
    assert "490 / 500" in html
    assert "fixed <code>0.002</code>" in html
    assert "Complement-residual mechanism replication" in html
    assert "three-way post-block attribution" in html
    assert "Paired validity caused no incremental wait" in html
    assert "0 / 2,880" in html
    assert "Paired-validity wait-cost screen" in html
    assert "10 s and 27 s" in html
    assert "negative structural prior" in html
    assert "three probability-model challengers are rejected" in html
    assert "The fair-value anchor does not match the market" in html
    assert "215 / 2,778" in html
    assert "112" in html and "103" in html
    assert "21 / 24" in html
    assert "24 / 24 prices to beat" in html
    assert "30 first-half versus 185 second-half" in html
    assert "Official settlement-anchor mechanism screen" in html
    assert "active block forbidden" in html
    assert "two future disjoint" in html
    assert "offline-only evaluator" in html
    assert "--fair-value-btc-csv" in html
    assert "--settlement-anchor-allocation-lock" in html
    assert "all <code>545</code> Rust tests pass" in html
    assert "Its enforced allocation wrapper pins exactly" in html
    assert "no concrete future allocation lock can be issued" in html.lower()
    assert "--pin-input-artifacts" in html
    assert "--settlement-anchor-source-audit" in html
    assert "paired scorer" in html
    assert "three single-use" in html
    assert "The only signal-gated historical outcome check is negative and underpowered" in html
    assert "Retrospective signal-gated anchor outcome diagnostic" in html
    assert "81 / 81" in html
    assert "0.072011" in html
    assert "0.001399" in html
    assert "-0.314523" in html
    assert "Keep frozen contract unchanged" in html
    assert "Fast volatility improves calibration too little" in html
    assert "Preregistered fast-volatility calibration screen" in html
    assert "8,633" in html
    assert "25,899" in html
    assert "+0.000177" in html
    assert "+0.000500 minimum" in html
    assert "+0.001040" in html
    assert "Reject fast_volatility_max_v1 without retuning" in html
    assert "2,678,400" in html
    assert "byte-identical" in html
    assert "DVOL cannot support the registered causal volatility test" in html
    assert "10,080 / 30,240" in html
    assert "33.33%" in html
    assert "before any Binance evaluation labels were downloaded" in html
    assert "A causal empirical return law sharpens forecasts but fails robustness" in html
    assert "Preregistered empirical-return CDF calibration screen" in html
    assert "10,062" in html
    assert "30,186" in html
    assert "+0.000521" in html
    assert "+0.000302" in html
    assert "93.84%" in html
    assert "Reject empirical_standardized_return_cdf_v1 without retuning" in html
    assert "The next decision remains the sealed canary" in html
    assert "Do not reuse the existing broad live Chainlink switch" in html
    assert "Paired depth is mirrored, not a new uncertainty signal" in html
    assert "490 / 490" in html
    assert "Paired-depth structural screen" in html
    assert "No challenger preregistration or implementation" in html
    assert "310 / 750" in html
    assert "Session 018" in html
    assert "Session 019 was interrupted" in html
    assert "session 020" in html
    assert "108 ms" in html
    assert "Dynamic tick changes are real" in html
    assert "254.187" in html
    assert "Measurement v5" in html
    assert "schema-6 Linux scorer" in html
    assert "maximum drawdown" in html
    assert "Decision-useful support is now a hard gate" in html
    assert "Measurement-v11" in html
    assert "100" in html and "15" in html and "80" in html
    assert "A coherent score must now reproduce" in html
    assert "501" in html
    assert "Exact replay now uses the same causal tick" in html
    assert "Measurement-v7" in html
    assert "504" in html
    assert "Future live orders now retain" in html
    assert "Measurement-v8" in html
    assert "506" in html
    assert "Restart-safe authenticated recovery" in html
    assert "Measurement-v10" in html
    assert "517" in html
    assert "3 ms" in html
    assert ">undefined<" not in html
    assert ">NaN<" not in html

    print(
        json.dumps(
            {
                "ok": True,
                "checks": 925,
                "forward_scores": 0,
                "active_hypotheses": 1,
                "conditions_per_block": 750,
                "notebook_code_cells": len(code_cells),
                "paired_pressure_notebook_code_cells": len(paired_code_cells),
                "residual_independence_notebook_code_cells": len(residual_notebook_code_cells),
                "residual_cross_market_notebook_code_cells": len(cross_market_code_cells),
                "paired_wait_cost_notebook_code_cells": len(paired_wait_cost_code_cells),
                "settlement_anchor_notebook_code_cells": len(settlement_anchor_code_cells),
                "settlement_anchor_historical_notebook_code_cells": len(
                    settlement_anchor_historical_code_cells
                ),
                "fast_volatility_notebook_code_cells": len(fast_volatility_code_cells),
                "empirical_cdf_notebook_code_cells": len(empirical_cdf_code_cells),
                "paired_depth_notebook_code_cells": len(paired_depth_code_cells),
                "report_charts": len(artifact["manifest"]["charts"]),
                "report_tables": len(artifact["manifest"]["tables"]),
                "visual_verification": "structural_only_browser_unavailable",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
