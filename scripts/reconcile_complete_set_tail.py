#!/usr/bin/env python3
"""Reconcile the frozen complete-set-lock historical tail reports."""

import json
import math
from pathlib import Path

ROOT = Path("/private/tmp/polymomentum_complete_set_historical_tail_20260718")
BASELINE_HASH = "a5d67641653ae85a853aab531060a240eade257e32fd5bf0e46392c7934302d5"
CANDIDATE_HASH = "c25aa94ad592b6274150e48be7765ace8fa3beba85595e48225906ca01c01363"
LABELS = {BASELINE_HASH: "baseline", CANDIDATE_HASH: "candidate"}
REPORT_INDEX = {"baseline": 0, "candidate": 1}


def close(left, right, tolerance=1e-8):
    return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=tolerance)


def wilson_lower(wins, trades, z=1.959963984540054):
    if trades == 0:
        return 0.0
    observed = wins / trades
    denominator = 1.0 + z * z / trades
    centre = observed + z * z / (2.0 * trades)
    margin = z * math.sqrt((observed * (1.0 - observed) + z * z / (4.0 * trades)) / trades)
    return (centre - margin) / denominator


def trade_signature(trade):
    fill = trade["fill"]
    order = fill["order"]
    return order["condition_id"], round(fill["fill_timestamp_s"], 6), order["token_id"]


def trade_fee(trade):
    fee = trade["fill"]["fee"]
    if trade.get("exit") and trade["exit"].get("fill"):
        fee += trade["exit"]["fill"]["fee"]
    return fee


def terminal_hold_won(trade, winning_token):
    return trade["fill"]["order"]["token_id"] == winning_token


def hold_pnl_after_fee(trade, winning_token):
    fill = trade["fill"]
    redemption = fill["filled_size"] if terminal_hold_won(trade, winning_token) else 0.0
    return redemption - fill["cost"] - fill["fee"]


def load_and_validate():
    folds = []
    all_trades = {"baseline": [], "candidate": []}
    winning_tokens = {}
    for fold_id in range(29, 43):
        fold_dir = ROOT / f"fold_{fold_id:03d}"
        sweep = json.loads((fold_dir / "sweep.json").read_text())
        report = json.loads((fold_dir / "trades.json").read_text())
        hydrate = json.loads((fold_dir / "hydrate.json").read_text())

        assert sweep["mode"] == "backtest"
        assert report["mode"] == "harness_sweep_trades"
        assert hydrate["mode"] == "metadata_only"
        assert hydrate["markets"] == 96
        assert hydrate["window_minutes"] == 5.0
        assert hydrate["start"] == report["start"]
        assert hydrate["end"] == report["end"]
        assert sweep["data_manifest"]["complete"] is True
        assert len(sweep["market_catalog"]["markets"]) == 96
        assert len(sweep["market_catalog"]["token_to_condition"]) == 192
        for source in sweep["data_manifest"]["sources"]:
            assert source["complete"] is True
            assert source["row_count"] > 0
        pmxt = next(source for source in sweep["data_manifest"]["sources"] if source["name"] == "pmxt_v2_archive")
        assert pmxt["metadata"]["hours"] == "8"
        assert pmxt["metadata"]["market_count"] == "96"

        sweep_by_hash = {variant["strategy"]["params_hash"]: variant for variant in sweep["variants"]}
        assert set(sweep_by_hash) == {BASELINE_HASH, CANDIDATE_HASH}
        market_by_condition = sweep["market_catalog"]["markets"]
        fold_row = {"fold": fold_id, "start": report["start"], "end": report["end"], "variants": {}}
        for variant_hash, label in LABELS.items():
            sweep_variant = sweep_by_hash[variant_hash]
            report_variant = report["variants"][REPORT_INDEX[label]]
            summary = report_variant["summary"]
            trades = report_variant["trades"]
            assert report_variant["variant_index"] == REPORT_INDEX[label]
            assert report_variant["strategy_name"] == sweep_variant["strategy_params"]["name"]
            assert report_variant["strategy_params"] == sweep_variant["strategy_params"]
            assert not report_variant["unresolved_fills"]
            assert summary["unresolved_fills"] == sweep_variant["unresolved_fills"] == 0
            assert sweep_variant["breaker_tripped"] is False
            assert summary["trades"] == sweep_variant["trades"] == len(trades)
            assert summary["wins"] == sweep_variant["wins"] == sum(bool(trade["won"]) for trade in trades)
            assert summary["losses"] == sweep_variant["losses"] == sum(not bool(trade["won"]) for trade in trades)
            assert close(summary["total_pnl"], sweep_variant["total_pnl"])
            assert close(summary["total_pnl"], sum(trade["pnl_after_fee"] for trade in trades))
            assert close(summary["total_fees"], sweep_variant["total_fees"])
            assert close(summary["total_fees"], sum(trade_fee(trade) for trade in trades))
            signatures = [trade_signature(trade) for trade in trades]
            assert len(signatures) == len(set(signatures))
            for trade in trades:
                assert trade["fill"]["success"] is True
                condition_id = trade["fill"]["order"]["condition_id"]
                direction_key = f"{trade['actual_direction']}_token_id"
                winning_token = market_by_condition[condition_id][direction_key]
                winning_tokens[(fold_id, condition_id)] = winning_token
                if trade.get("exit"):
                    assert trade["exit"]["fill"]["success"] is True
                    assert trade["exit"]["reason"] == "guaranteed_complete_set"
                    assert trade["resolution_source"] == "complete_set_lock"
                    assert trade["exit"]["pnl_after_fee"] >= 0.1 - 1e-8
                    assert close(trade["pnl_after_fee"], trade["exit"]["pnl_after_fee"])
                else:
                    assert trade["won"] == terminal_hold_won(trade, winning_token)
            all_trades[label].extend((fold_id, trade) for trade in trades)
            fold_row["variants"][label] = {
                "trades": summary["trades"],
                "wins": summary["wins"],
                "losses": summary["losses"],
                "total_pnl": summary["total_pnl"],
                "total_fees": summary["total_fees"],
            }
        folds.append(fold_row)
    return folds, all_trades, winning_tokens


def aggregate(label, folds, all_trades, winning_tokens):
    indexed_rows = all_trades[label]
    rows = [trade for _, trade in indexed_rows]
    fold_pnls = [fold["variants"][label]["total_pnl"] for fold in folds]
    trade_pnls = [trade["pnl_after_fee"] for trade in rows]
    wins = sum(trade["won"] for trade in rows)
    losses = len(rows) - wins
    gross_profit = sum(pnl for pnl in trade_pnls if pnl > 0)
    gross_loss = -sum(pnl for pnl in trade_pnls if pnl < 0)
    positive_count = sum(pnl > 0 for pnl in trade_pnls)
    negative_count = sum(pnl < 0 for pnl in trade_pnls)
    average_win = gross_profit / positive_count if positive_count else 0.0
    average_loss = gross_loss / negative_count if negative_count else 0.0
    tail_count = max(1, math.ceil(len(fold_pnls) * 0.20))
    rolling_bursts = [
        sum(pnl < 0 for pnl in fold_pnls[index:index + 5])
        for index in range(len(fold_pnls) - 4)
    ]
    half = len(folds) // 2
    result = {
        "trades": len(rows),
        "wins": wins,
        "losses": losses,
        "total_pnl": sum(trade_pnls),
        "total_fees": sum(trade_fee(trade) for trade in rows),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": gross_profit / gross_loss if gross_loss else None,
        "average_positive_trade": average_win,
        "average_negative_trade_abs": average_loss,
        "payoff_ratio": average_win / average_loss if average_loss else None,
        "worst_loss_to_average_win": (
            max((-pnl for pnl in trade_pnls if pnl < 0), default=0.0) / average_win
            if average_win else None
        ),
        "wilson_95_lower": wilson_lower(wins, len(rows)),
        "eligible_folds": len(fold_pnls),
        "profitable_folds": sum(pnl > 0 for pnl in fold_pnls),
        "losing_folds": sum(pnl < 0 for pnl in fold_pnls),
        "worst_fold_pnl": min(fold_pnls),
        "tail_alpha": 0.20,
        "tail_fold_count": tail_count,
        "tail_cvar_pnl": sum(sorted(fold_pnls)[:tail_count]) / tail_count,
        "loss_burst_lookback": 5,
        "max_loss_burst_reports": (
            max(rolling_bursts) if rolling_bursts else sum(pnl < 0 for pnl in fold_pnls)
        ),
        "first_half_pnl": sum(fold_pnls[:half]),
        "second_half_pnl": sum(fold_pnls[half:]),
        "fold_pnls": fold_pnls,
    }
    if label == "candidate":
        locks = [(fold, trade) for fold, trade in indexed_rows if trade.get("exit")]
        winning_locks = [
            (fold, trade) for fold, trade in locks
            if terminal_hold_won(
                trade,
                winning_tokens[(fold, trade["fill"]["order"]["condition_id"])],
            )
        ]
        losing_locks = [
            (fold, trade) for fold, trade in locks
            if (fold, trade) not in winning_locks
        ]
        result.update({
            "complete_set_locks": len(locks),
            "terminal_winner_locks": len(winning_locks),
            "terminal_loser_locks": len(losing_locks),
            "lock_pnl": sum(trade["pnl_after_fee"] for _, trade in locks),
            "lock_hold_counterfactual_pnl": sum(
                hold_pnl_after_fee(
                    trade,
                    winning_tokens[(fold, trade["fill"]["order"]["condition_id"])],
                )
                for fold, trade in locks
            ),
            "winning_lock_delta_vs_hold": sum(
                trade["pnl_after_fee"] - hold_pnl_after_fee(
                    trade,
                    winning_tokens[(fold, trade["fill"]["order"]["condition_id"])],
                )
                for fold, trade in winning_locks
            ),
            "losing_lock_delta_vs_hold": sum(
                trade["pnl_after_fee"] - hold_pnl_after_fee(
                    trade,
                    winning_tokens[(fold, trade["fill"]["order"]["condition_id"])],
                )
                for fold, trade in losing_locks
            ),
            "unhedged_losses": sum(
                not trade["won"] and not trade.get("exit") for trade in rows
            ),
        })
        result["lock_delta_vs_hold"] = (
            result["lock_pnl"] - result["lock_hold_counterfactual_pnl"]
        )
    return result


def main():
    folds, all_trades, winning_tokens = load_and_validate()
    baseline_signatures = {
        trade_signature(trade): (fold, trade) for fold, trade in all_trades["baseline"]
    }
    candidate_signatures = {
        trade_signature(trade): (fold, trade) for fold, trade in all_trades["candidate"]
    }
    common = sorted(set(baseline_signatures) & set(candidate_signatures))
    baseline_only = sorted(set(baseline_signatures) - set(candidate_signatures))
    candidate_only = sorted(set(candidate_signatures) - set(baseline_signatures))
    for signature in common:
        baseline_trade = baseline_signatures[signature][1]
        candidate_trade = candidate_signatures[signature][1]
        assert baseline_trade["local_direction"] == candidate_trade["local_direction"]
        assert baseline_trade["actual_direction"] == candidate_trade["actual_direction"]
        assert close(baseline_trade["fill"]["fill_price"], candidate_trade["fill"]["fill_price"])
        assert baseline_trade["decision"]["direction"] == candidate_trade["decision"]["direction"]

    common_baseline_trades = [baseline_signatures[signature][1] for signature in common]
    common_candidate_trades = [candidate_signatures[signature][1] for signature in common]
    candidate_only_indexed = [candidate_signatures[signature] for signature in candidate_only]
    candidate_only_trades = [trade for _, trade in candidate_only_indexed]

    output = {
        "schema_version": 1,
        "status": "validated",
        "scope": {
            "fold_start": 29,
            "fold_end": 42,
            "fold_count": 14,
            "hours_per_fold": 8,
            "markets_per_fold": 96,
        },
        "variant_hashes": {"baseline": BASELINE_HASH, "candidate": CANDIDATE_HASH},
        "quality": {
            "all_manifests_complete": True,
            "all_hydration_manifests_reconciled": True,
            "all_sources_complete": True,
            "all_report_summaries_reconciled": True,
            "all_trade_pnl_reconciled": True,
            "all_fees_reconciled": True,
            "unresolved_fills": 0,
            "breaker_trips": 0,
        },
        "folds": folds,
        "aggregate": {
            "baseline": aggregate("baseline", folds, all_trades, winning_tokens),
            "candidate": aggregate("candidate", folds, all_trades, winning_tokens),
        },
        "entry_cohorts": {
            "common": len(common),
            "baseline_only": len(baseline_only),
            "candidate_only": len(candidate_only),
            "common_baseline_pnl": sum(trade["pnl_after_fee"] for trade in common_baseline_trades),
            "common_candidate_pnl": sum(trade["pnl_after_fee"] for trade in common_candidate_trades),
            "common_candidate_delta": sum(
                candidate["pnl_after_fee"] - baseline["pnl_after_fee"]
                for baseline, candidate in zip(common_baseline_trades, common_candidate_trades)
            ),
            "common_baseline_losses": sum(not trade["won"] for trade in common_baseline_trades),
            "common_losses_converted_to_positive_lock": sum(
                (not baseline["won"])
                and candidate.get("exit") is not None
                and candidate["pnl_after_fee"] > 0
                for baseline, candidate in zip(common_baseline_trades, common_candidate_trades)
            ),
            "candidate_only_pnl": sum(trade["pnl_after_fee"] for trade in candidate_only_trades),
            "candidate_only_fees": sum(trade_fee(trade) for trade in candidate_only_trades),
            "candidate_only_wins": sum(trade["won"] for trade in candidate_only_trades),
            "candidate_only_losses": sum(not trade["won"] for trade in candidate_only_trades),
            "candidate_only_locks": sum(trade.get("exit") is not None for trade in candidate_only_trades),
            "candidate_only_terminal_winners": sum(
                terminal_hold_won(
                    trade,
                    winning_tokens[(fold, trade["fill"]["order"]["condition_id"])],
                )
                for fold, trade in candidate_only_indexed
            ),
            "candidate_only_terminal_losers": sum(
                not terminal_hold_won(
                    trade,
                    winning_tokens[(fold, trade["fill"]["order"]["condition_id"])],
                )
                for fold, trade in candidate_only_indexed
            ),
            "candidate_only_hold_counterfactual_pnl": sum(
                hold_pnl_after_fee(
                    trade,
                    winning_tokens[(fold, trade["fill"]["order"]["condition_id"])],
                )
                for fold, trade in candidate_only_indexed
            ),
            "baseline_only_rows": [
                {
                    "fold": baseline_signatures[signature][0],
                    "condition_id": signature[0],
                    "fill_timestamp_s": signature[1],
                    "token_id": signature[2],
                }
                for signature in baseline_only
            ],
            "candidate_only_rows": [
                {
                    "fold": candidate_signatures[signature][0],
                    "condition_id": signature[0],
                    "fill_timestamp_s": signature[1],
                    "token_id": signature[2],
                }
                for signature in candidate_only
            ],
        },
    }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
