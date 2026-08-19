#!/usr/bin/env python3
"""Build the canonical portable-report artifact for the v1 tail diagnostic."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "deploy/promotions/evidence/strategy_registry"
SOURCE_PATH = "deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_historical_tail_diagnostic.json"
OUTPUT = ROOT / "docs/reports/strategy_complete_set_lock_v1_historical_tail_2026-07-18.artifact.json"
diagnostic = json.loads((ROOT / SOURCE_PATH).read_text())

source = {
    "id": "complete_set_tail",
    "label": "Complete-set lock v1 exact historical-tail diagnostic",
    "path": SOURCE_PATH,
    "query": {
        "description": "Reconciled exact 202 ms replay of the frozen baseline and immediate complete-set lock across folds 29-42.",
        "engine": "duckdb",
        "language": "sql",
        "sql": f"SELECT * FROM read_json_auto('{SOURCE_PATH}');",
        "filters": [
            "strict folds 29 through 42",
            "14 independent 8-hour reports and 96 five-minute markets per report",
            "202 ms order insertion latency",
            "fee-inclusive missing-leg FOK ceiling accounting",
            "historical diagnostic only; no promotion credit",
        ],
        "metric_definitions": [
            "Total PnL is the sum of resolved trade PnL after entry and exit fees.",
            "Profit factor is gross positive trade PnL divided by absolute gross negative trade PnL.",
            "Payoff ratio is average positive trade PnL divided by absolute average negative trade PnL.",
            "Wilson lower is the 95 percent Wilson lower confidence bound for policy win rate.",
            "Tail CVaR is the mean of the worst ceil(20 percent) chronological fold PnLs.",
            "Loss burst is the maximum number of negative folds in any rolling five-fold window.",
            "Lock delta versus terminal hold is successful lock PnL minus the same entry's official terminal counterfactual PnL.",
        ],
    },
}

fold_rows = []
for row in diagnostic["folds"]:
    for label, key in [("Baseline", "baseline_pnl"), ("Immediate lock v1", "candidate_pnl")]:
        fold_rows.append({
            "fold": str(row["fold"]),
            "policy": label,
            "pnl_usd": row[key],
            "baseline_trades": row["baseline_trades"],
            "candidate_trades": row["candidate_trades"],
        })

locks = diagnostic["lock_accounting"]
cohorts = diagnostic["state_feedback_decomposition"]
candidate = diagnostic["aggregate"]["candidate"]
baseline = diagnostic["aggregate"]["baseline"]
gates = [
    {
        "gate_order": index,
        "metric": row["gate"],
        "observed": str(row["observed"]),
        "required": str(row["required"]),
        "status": "pass" if row["pass"] else "fail",
    }
    for index, row in enumerate(diagnostic["research_gate_evaluation"], 1)
]

manifest = {
    "version": 1,
    "surface": "report",
    "title": "Immediate complete-set locking improves losses but remains unprofitable",
    "description": "Technical audit of the frozen complete_set_lock_v1 mechanism on 14 exact historical tail folds.",
    "generatedAt": diagnostic["generated_at"],
    "blocks": [
        {
            "id": "title",
            "type": "markdown",
            "layout": "full",
            "body": "# Immediate complete-set locking improves losses but remains unprofitable",
        },
        {
            "id": "technical_summary",
            "type": "markdown",
            "layout": "full",
            "sourceId": "complete_set_tail",
            "body": "## Technical summary\n\nThe frozen immediate complete-set lock is **rejected as a research candidate**. Across 14 exact 8-hour folds it improves the unchanged baseline by `+$4.44`, but remains negative at `-$8.15`. Its `0.8346` Wilson lower bound is not enough: profit factor is `0.6795`, payoff ratio collapses to `0.0557`, three losses occur inside a rolling five-fold window, and both chronological halves are negative. This history earns no promotion credit and no v1 threshold was retuned.",
        },
        {"id": "metrics", "type": "metric-strip", "layout": "full", "cardIds": ["pnl_card", "delta_card", "profit_factor_card", "payoff_card"]},
        {
            "id": "fold_finding",
            "type": "markdown",
            "layout": "full",
            "sourceId": "complete_set_tail",
            "body": "## Higher win rate does not produce a profitable tail\n\nThe candidate turns five additional folds positive and records `61 / 66` policy wins, but loses `-$8.15` overall. The fold bars show why the result is not a near-pass: losses near `-$5` persist while many winning folds are capped close to `+$1–2`. Both halves remain negative (`-$1.83` and `-$6.32`).",
        },
        {"id": "fold_chart_block", "type": "chart", "layout": "full", "chartId": "fold_pnl_chart"},
        {
            "id": "lock_finding",
            "type": "markdown",
            "layout": "full",
            "sourceId": "complete_set_tail",
            "body": "## The lock converts losers but surrenders too much winner upside\n\nFifty-nine locks add `+$14.71` versus terminal hold. The composition is fragile: 12 terminal-loser locks recover `+$63.63`, while 47 terminal-winner locks surrender `-$48.92`. Five unhedged losses remain. Immediate locking therefore solves part of the left tail while destroying the payoff needed to absorb the losses that escape.",
        },
        {"id": "lock_chart_block", "type": "chart", "layout": "full", "chartId": "lock_delta_chart"},
        {
            "id": "cohort_finding",
            "type": "markdown",
            "layout": "full",
            "sourceId": "complete_set_tail",
            "body": "## Stateful feedback, not common-entry improvement, drives the aggregate delta\n\nAll 43 baseline entries also appear under the candidate, and the candidate is `-$1.37` worse on those common entries. Profitable locks prevent the existing degraded-loss control from activating and admit 23 candidate-only entries; those add `+$5.80`. Seven of the 23 would have lost at terminal hold, so hiding cohort drift would overstate a pure lock effect.",
        },
        {"id": "cohort_table_block", "type": "table", "layout": "full", "tableId": "cohort_table"},
        {
            "id": "gate_finding",
            "type": "markdown",
            "layout": "full",
            "sourceId": "complete_set_tail",
            "body": "## The candidate fails six research gates\n\nWilson and fold CVaR pass, but fee-inclusive PnL, profit factor, support, payoff ratio, rolling loss burst, and both-half profitability fail. The result is a mechanism rejection, not a tuning invitation.",
        },
        {"id": "gate_table_block", "type": "table", "layout": "full", "tableId": "gate_table"},
        {
            "id": "scope",
            "type": "markdown",
            "layout": "full",
            "sourceId": "complete_set_tail",
            "body": "## Scope, data, and definitions\n\nThe audit covers folds 29–42: 14 independent 8-hour reports, 1,344 BTC five-minute market windows, 96 markets per fold, and exact `202 ms` replay. Every hydration manifest, market catalog, PMXT/Binance source, trade summary, PnL row, and fee total reconciles; unresolved fills and breaker trips are zero. The candidate buys exactly the missing outcome quantity only when its FOK ceiling guarantees at least `$0.10` after both fees and retains all collateral to close.",
        },
        {
            "id": "methodology",
            "type": "markdown",
            "layout": "full",
            "sourceId": "complete_set_tail",
            "body": "## Methodology\n\nSweep rows were matched by stable parameter hash because PnL ranking changes row order; trade reports were matched by fixed variant index and exact strategy parameters. Aggregate PnL and fees were rederived from every trade. Official winning tokens came from each fold's market catalog, allowing every successful lock to be compared with the same entry's terminal hold. Entry cohorts use condition ID, fill timestamp rounded to one microsecond, and token ID.",
        },
        {
            "id": "limitations",
            "type": "markdown",
            "layout": "full",
            "sourceId": "complete_set_tail",
            "body": "## Limitations, uncertainty, and robustness\n\nFolds 29–42 were observed during mechanism diagnosis and are ineligible for promotion. Stateful realized-loss feedback means lock-only and full-policy effects are not interchangeable. Gas, merge, and redemption operations are excluded consistently with the baseline. This diagnostic does not estimate fresh-regime performance, and it cannot authorize live runtime integration.",
        },
        {
            "id": "next_steps",
            "type": "markdown",
            "layout": "full",
            "body": "## Recommended next steps\n\n1. Keep live trading off and reject immediate lock v1 without retuning.\n2. Preserve the validated missing-leg FOK, fee, cash-reservation, and terminal-collateral accounting.\n3. Evaluate only the separately preregistered trailing v2 mechanism: arm at the pre-existing `+$0.50`, preserve upside above it, and lock on retreat into the pre-existing `+$0.10–$0.50` band.\n4. Give the historical tail no promotion credit and forbid neighboring thresholds.\n5. Require two disjoint post-registration fresh blocks and unchanged A+ gates before any runtime-parity work.",
        },
        {
            "id": "further_questions",
            "type": "markdown",
            "layout": "full",
            "body": "## Further questions\n\n- Does trailing v2 preserve enough terminal-winner payoff while still converting reversals?\n- How often does executable complete-set profit jump through the `$0.10` floor after arming?\n- Can two disjoint fresh blocks reach the 100-trade disclosure floor without entry-cohort drift dominating the result?",
        },
    ],
    "cards": [
        {"id": "pnl_card", "dataset": "headline", "description": "Candidate fee-inclusive PnL across the 14-fold historical tail.", "sourceId": "complete_set_tail", "metrics": [{"field": "candidate_pnl_usd", "format": "number", "label": "Candidate tail PnL, USD", "signed": True}]},
        {"id": "delta_card", "dataset": "headline", "description": "Full stateful candidate PnL minus the unchanged baseline; still not a profitable result.", "sourceId": "complete_set_tail", "metrics": [{"field": "candidate_delta_usd", "format": "number", "label": "Delta vs baseline, USD", "signed": True}]},
        {"id": "profit_factor_card", "dataset": "headline", "description": "Gross positive trade PnL divided by absolute gross negative trade PnL.", "sourceId": "complete_set_tail", "metrics": [{"field": "profit_factor", "format": "number", "label": "Profit factor"}]},
        {"id": "payoff_card", "dataset": "headline", "description": "Average positive trade divided by absolute average negative trade.", "sourceId": "complete_set_tail", "metrics": [{"field": "payoff_ratio", "format": "number", "label": "Payoff ratio"}]},
    ],
    "charts": [
        {
            "id": "fold_pnl_chart", "type": "bar", "intent": "comparison", "layout": "full", "dataset": "fold_pnl",
            "title": "Fee-inclusive PnL by historical fold",
            "subtitle": "Folds 29–42; exact 202 ms replay, USD after fees; 43 baseline and 66 candidate trades.",
            "question": "Does immediate complete-set locking make the historical tail profitable across folds?",
            "rationale": "Grouped bars compare two stateful policies over 14 ordered folds and retain the break-even boundary.",
            "maxRows": 28, "palette": {"kind": "categorical", "name": "blue-gold"},
            "legend": {"position": "top", "title": "Policy", "interactive": True, "sort": "labelAsc"},
            "comparisonContext": {"baseline": "primary_v6_volfloor_300", "grain": "8-hour fold", "unit": "USD after fees"},
            "referenceLines": [{"axis": "y", "value": 0, "label": "Break-even", "color": "neutral", "lineStyle": "dashed"}],
            "encodings": {
                "x": {"field": "fold", "type": "nominal", "label": "Chronological fold"},
                "y": {"field": "pnl_usd", "type": "quantitative", "label": "PnL after fees, USD", "format": "number"},
                "color": {"field": "policy", "type": "nominal", "label": "Policy"},
                "tooltip": [
                    {"field": "baseline_trades", "type": "quantitative", "label": "Baseline trades", "format": "number"},
                    {"field": "candidate_trades", "type": "quantitative", "label": "Candidate trades", "format": "number"},
                ],
            },
            "sourceId": "complete_set_tail",
        },
        {
            "id": "lock_delta_chart", "type": "bar", "intent": "comparison", "layout": "full", "dataset": "lock_deltas",
            "title": "Complete-set lock PnL delta versus terminal hold",
            "subtitle": "59 successful locks split by official terminal outcome; USD after both fees.",
            "question": "How do terminal-winner and terminal-loser locks combine into the net lock effect?",
            "rationale": "Three signed bars expose the winner-upside cost, loser recovery, and net effect on one common scale.",
            "maxRows": 3, "palette": {"kind": "single", "name": "blue"},
            "comparisonContext": {"baseline": "same entry held to terminal resolution", "grain": "lock outcome group", "unit": "USD after fees"},
            "referenceLines": [{"axis": "y", "value": 0, "label": "No change", "color": "neutral", "lineStyle": "dashed"}],
            "encodings": {
                "x": {"field": "component", "type": "nominal", "label": "Lock outcome component"},
                "y": {"field": "delta_usd", "type": "quantitative", "label": "PnL delta, USD", "format": "number"},
                "tooltip": [{"field": "locks", "type": "quantitative", "label": "Locks", "format": "number"}],
            },
            "sourceId": "complete_set_tail",
        },
    ],
    "tables": [
        {
            "id": "cohort_table", "dataset": "cohorts", "title": "State-feedback entry cohorts",
            "subtitle": "Candidate-only entries arise after profitable locks change the frozen degraded-loss state.",
            "density": "spacious", "layout": "full", "defaultSort": {"field": "cohort_order", "direction": "asc"}, "sourceId": "complete_set_tail",
            "columns": [
                {"field": "cohort_order", "label": "Order", "format": "number"},
                {"field": "cohort", "label": "Entry cohort", "type": "text"},
                {"field": "entries", "label": "Entries", "format": "number"},
                {"field": "baseline_pnl_usd", "label": "Baseline PnL, USD", "format": "number"},
                {"field": "candidate_pnl_usd", "label": "Candidate PnL, USD", "format": "number"},
                {"field": "terminal_losers", "label": "Terminal losers", "format": "number"},
            ],
        },
        {
            "id": "gate_table", "dataset": "gates", "title": "Frozen research-gate evaluation",
            "subtitle": "A historical diagnostic cannot earn promotion credit even where a gate passes.",
            "density": "spacious", "layout": "full", "defaultSort": {"field": "gate_order", "direction": "asc"}, "sourceId": "complete_set_tail",
            "columns": [
                {"field": "gate_order", "label": "Order", "format": "number"},
                {"field": "metric", "label": "Metric", "type": "text"},
                {"field": "observed", "label": "Observed", "type": "text"},
                {"field": "required", "label": "Required", "type": "text"},
                {"field": "status", "label": "Status", "type": "text"},
            ],
        },
    ],
    "sources": [source],
}

snapshot = {
    "version": 1,
    "status": "ready",
    "accessIssues": [],
    "datasets": {
        "headline": [{
            "candidate_pnl_usd": candidate["total_pnl_usd"],
            "candidate_delta_usd": candidate["candidate_minus_baseline_pnl_usd"],
            "profit_factor": candidate["profit_factor"],
            "payoff_ratio": candidate["payoff_ratio"],
        }],
        "fold_pnl": fold_rows,
        "lock_deltas": [
            {"component": "Terminal-winner locks", "delta_usd": locks["terminal_winner_lock_delta_usd"], "locks": locks["terminal_winner_locks"]},
            {"component": "Terminal-loser locks", "delta_usd": locks["terminal_loser_lock_delta_usd"], "locks": locks["terminal_loser_locks"]},
            {"component": "Net lock effect", "delta_usd": locks["lock_delta_vs_terminal_hold_usd"], "locks": locks["successful_locks"]},
        ],
        "cohorts": [
            {"cohort_order": 1, "cohort": "Common entries", "entries": cohorts["common_entries"], "baseline_pnl_usd": cohorts["common_baseline_pnl_usd"], "candidate_pnl_usd": cohorts["common_candidate_pnl_usd"], "terminal_losers": cohorts["common_baseline_losses"]},
            {"cohort_order": 2, "cohort": "Baseline-only entries", "entries": cohorts["baseline_only_entries"], "baseline_pnl_usd": 0.0, "candidate_pnl_usd": None, "terminal_losers": 0},
            {"cohort_order": 3, "cohort": "Candidate-only entries", "entries": cohorts["candidate_only_entries"], "baseline_pnl_usd": None, "candidate_pnl_usd": cohorts["candidate_only_pnl_usd"], "terminal_losers": cohorts["candidate_only_terminal_losers"]},
        ],
        "gates": gates,
    },
}

artifact = {"surface": "report", "manifest": manifest, "snapshot": snapshot, "sources": [source]}
OUTPUT.parent.mkdir(parents=True, exist_ok=True)
OUTPUT.write_text(json.dumps(artifact, indent=2) + "\n")
print(OUTPUT)
