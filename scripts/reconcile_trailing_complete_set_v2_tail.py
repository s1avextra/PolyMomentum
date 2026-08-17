#!/usr/bin/env python3
"""Reconcile the preregistered trailing complete-set v2 tail replay."""

import contextlib
import io
import json
from pathlib import Path

import reconcile_complete_set_tail as core


ROOT = Path("/private/tmp/polymomentum_trailing_complete_set_v2_historical_tail_20260718")
SOURCE_ROOT = Path("/private/tmp/polymomentum_complete_set_historical_tail_20260718")
BASELINE_HASH = "a5d67641653ae85a853aab531060a240eade257e32fd5bf0e46392c7934302d5"
CANDIDATE_HASH = "8554587b2e8bca78c504f3fbb8840737fee1d384567b173ba8efe8d909a4bb11"


def main():
    core.ROOT = ROOT
    core.BASELINE_HASH = BASELINE_HASH
    core.CANDIDATE_HASH = CANDIDATE_HASH
    core.LABELS = {BASELINE_HASH: "baseline", CANDIDATE_HASH: "candidate"}

    rendered = io.StringIO()
    with contextlib.redirect_stdout(rendered):
        core.main()
    result = json.loads(rendered.getvalue())

    armed_positions = 0
    exit_signals = 0
    exit_fills = 0
    exit_failures = 0
    for fold_id in range(29, 43):
        fold = f"fold_{fold_id:03d}"
        source_report = json.loads((SOURCE_ROOT / fold / "trades.json").read_text())
        replay_report = json.loads((ROOT / fold / "trades.json").read_text())
        assert replay_report["variants"][0]["strategy_params"] == source_report["variants"][0]["strategy_params"]
        assert replay_report["variants"][0]["summary"] == source_report["variants"][0]["summary"]
        assert replay_report["variants"][0]["trades"] == source_report["variants"][0]["trades"]

        sweep = json.loads((ROOT / fold / "sweep.json").read_text())
        candidate = next(
            variant for variant in sweep["variants"]
            if variant["strategy"]["params_hash"] == CANDIDATE_HASH
        )
        diagnostics = candidate["diagnostics"]
        armed_positions += diagnostics["skip_reasons"].get("complete_set_trailing_armed", 0)
        exit_signals += diagnostics["exit_signals"]
        exit_fills += diagnostics["exit_fills"]
        exit_failures += diagnostics["exit_failures"]

    candidate = result["aggregate"]["candidate"]
    baseline = result["aggregate"]["baseline"]
    result["mechanism"] = {
        "armed_positions": armed_positions,
        "exit_signals": exit_signals,
        "exit_fills": exit_fills,
        "exit_failures": exit_failures,
        "successful_locks": candidate["complete_set_locks"],
        "minimum_lock_profit_usd": 0.10,
    }
    result["quality"]["baseline_trade_rows_exactly_match_frozen_v1_replay"] = True
    result["quality"]["candidate_params_hash_matches_preregistration"] = True
    result["historical_research_gates"] = {
        "higher_pnl_than_baseline": candidate["total_pnl"] > baseline["total_pnl"],
        "positive_total_pnl": candidate["total_pnl"] > 0.0,
        "positive_first_half": candidate["first_half_pnl"] > 0.0,
        "positive_second_half": candidate["second_half_pnl"] > 0.0,
        "wilson_95_lower_at_least_0_70": candidate["wilson_95_lower"] >= 0.70,
        "profit_factor_above_1": candidate["profit_factor"] is not None and candidate["profit_factor"] > 1.0,
        "payoff_ratio_at_least_0_30": candidate["payoff_ratio"] is not None and candidate["payoff_ratio"] >= 0.30,
        "five_fold_loss_burst_at_most_2": candidate["max_loss_burst_reports"] <= 2,
        "tail_cvar_at_least_minus_8": candidate["tail_cvar_pnl"] >= -8.0,
        "all_locks_meet_floor": candidate["complete_set_locks"] == 0 or all(
            trade["pnl_after_fee"] >= 0.10 - 1e-8
            for fold_id in range(29, 43)
            for trade in json.loads((ROOT / f"fold_{fold_id:03d}" / "trades.json").read_text())["variants"][1]["trades"]
            if trade.get("exit")
        ),
        "execution_reconciled": (
            exit_signals == exit_fills + exit_failures
            and exit_fills == candidate["complete_set_locks"]
            and result["quality"]["unresolved_fills"] == 0
            and result["quality"]["breaker_trips"] == 0
        ),
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
