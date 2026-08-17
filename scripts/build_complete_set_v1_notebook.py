#!/usr/bin/env python3
"""Build the reproducible complete-set-lock v1 historical diagnostic notebook."""

from pathlib import Path

import nbformat as nbf
from nbclient import NotebookClient


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "docs/notebooks/strategy_complete_set_lock_v1_historical_tail_2026-07-18.ipynb"


def code(source):
    return nbf.v4.new_code_cell(source.strip())


notebook = nbf.v4.new_notebook()
notebook["metadata"] = {
    "kernelspec": {
        "display_name": "PolyMomentum Notebook",
        "language": "python",
        "name": "polymomentum-notebook",
    },
    "language_info": {"name": "python", "version": "3"},
}
notebook["cells"] = [
    nbf.v4.new_markdown_cell(
        """# Complete-Set Lock v1 Historical Tail Diagnostic

## Technical summary

The frozen immediate complete-set lock is **rejected as a research candidate**. Across 14 exact 8-hour folds it improved the unchanged baseline by `$4.43718`, but remained negative at `-$8.15098`. Its high Wilson bound hides severe payoff compression: 47 terminal winners were locked early, the payoff ratio fell to `0.055696`, profit factor stayed below one, and both chronological halves were negative.

This notebook recomputes the result from the sealed raw reports. The history earns no promotion credit and no v1 parameter or neighbor is searched."""
    ),
    code(
        """
import json
import math
import tarfile
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

ROOT = Path.cwd()
assert (ROOT / 'deploy').is_dir(), 'execute from the repository root'
EVIDENCE = ROOT / 'deploy/promotions/evidence/strategy_registry'
ARCHIVE = EVIDENCE / '20260718_complete_set_lock_v1_historical_tail_raw_reports.tar.gz'
DIAGNOSTIC = EVIDENCE / '20260718_complete_set_lock_v1_historical_tail_diagnostic.json'
BASELINE_HASH = 'a5d67641653ae85a853aab531060a240eade257e32fd5bf0e46392c7934302d5'
CANDIDATE_HASH = 'c25aa94ad592b6274150e48be7765ace8fa3beba85595e48225906ca01c01363'
diagnostic = json.loads(DIAGNOSTIC.read_text())
        """
    ),
    code(
        """
def read_member(archive, name):
    handle = archive.extractfile(name)
    assert handle is not None, name
    return json.load(handle)

folds = []
all_trades = {'baseline': [], 'candidate': []}
winning_tokens = {}
with tarfile.open(ARCHIVE, 'r:gz') as archive:
    assert len(archive.getmembers()) == 42
    for fold in range(29, 43):
        prefix = f'fold_{fold:03d}'
        hydrate = read_member(archive, f'{prefix}/hydrate.json')
        sweep = read_member(archive, f'{prefix}/sweep.json')
        report = read_member(archive, f'{prefix}/trades.json')
        assert hydrate['markets'] == 96
        assert sweep['data_manifest']['complete'] is True
        assert all(source['complete'] and source['row_count'] > 0 for source in sweep['data_manifest']['sources'])
        assert len(sweep['market_catalog']['markets']) == 96
        variants = {row['strategy']['params_hash']: row for row in sweep['variants']}
        assert set(variants) == {BASELINE_HASH, CANDIDATE_HASH}
        market_map = sweep['market_catalog']['markets']
        fold_row = {'fold': fold}
        for label, variant_hash, report_index in [
            ('baseline', BASELINE_HASH, 0), ('candidate', CANDIDATE_HASH, 1)
        ]:
            sweep_variant = variants[variant_hash]
            report_variant = report['variants'][report_index]
            trades = report_variant['trades']
            summary = report_variant['summary']
            assert report_variant['strategy_params'] == sweep_variant['strategy_params']
            assert not report_variant['unresolved_fills'] and summary['unresolved_fills'] == 0
            assert not sweep_variant['breaker_tripped']
            assert summary['trades'] == len(trades) == sweep_variant['trades']
            pnl = sum(trade['pnl_after_fee'] for trade in trades)
            fees = sum(
                trade['fill']['fee'] + (trade.get('exit', {}).get('fill', {}).get('fee', 0.0))
                for trade in trades
            )
            assert math.isclose(pnl, summary['total_pnl'], abs_tol=1e-8)
            assert math.isclose(fees, summary['total_fees'], abs_tol=1e-8)
            fold_row[f'{label}_pnl'] = pnl
            all_trades[label].extend((fold, trade) for trade in trades)
            for trade in trades:
                cid = trade['fill']['order']['condition_id']
                winning_tokens[(fold, cid)] = market_map[cid][f"{trade['actual_direction']}_token_id"]
        folds.append(fold_row)

quality = {
    'archive_members': 42,
    'folds': len(folds),
    'markets_per_fold': 96,
    'unresolved_fills': 0,
    'breaker_trips': 0,
    'status': 'PASS',
}
quality
        """
    ),
    code(
        """
def wilson_lower(wins, total, z=1.959963984540054):
    p = wins / total
    denominator = 1 + z*z/total
    centre = p + z*z/(2*total)
    margin = z * math.sqrt((p*(1-p) + z*z/(4*total))/total)
    return (centre - margin) / denominator

def aggregate(label):
    trades = [trade for _, trade in all_trades[label]]
    pnls = [trade['pnl_after_fee'] for trade in trades]
    fold_pnls = [fold[f'{label}_pnl'] for fold in folds]
    wins = sum(trade['won'] for trade in trades)
    gross_profit = sum(value for value in pnls if value > 0)
    gross_loss = -sum(value for value in pnls if value < 0)
    avg_win = gross_profit / sum(value > 0 for value in pnls)
    avg_loss = gross_loss / sum(value < 0 for value in pnls)
    tail_count = math.ceil(0.20 * len(fold_pnls))
    bursts = [sum(value < 0 for value in fold_pnls[i:i+5]) for i in range(len(fold_pnls)-4)]
    return {
        'trades': len(trades), 'wins': wins, 'losses': len(trades)-wins,
        'total_pnl': sum(pnls), 'gross_profit': gross_profit, 'gross_loss': gross_loss,
        'profit_factor': gross_profit/gross_loss, 'payoff_ratio': avg_win/avg_loss,
        'wilson_95_lower': wilson_lower(wins, len(trades)),
        'profitable_folds': sum(value > 0 for value in fold_pnls),
        'tail_cvar': sum(sorted(fold_pnls)[:tail_count])/tail_count,
        'max_loss_burst': max(bursts),
        'first_half_pnl': sum(fold_pnls[:7]), 'second_half_pnl': sum(fold_pnls[7:]),
    }

aggregate_result = {'baseline': aggregate('baseline'), 'candidate': aggregate('candidate')}
aggregate_result
        """
    ),
    code(
        """
def signature(trade):
    fill = trade['fill']
    return fill['order']['condition_id'], round(fill['fill_timestamp_s'], 6), fill['order']['token_id']

baseline = {signature(trade): (fold, trade) for fold, trade in all_trades['baseline']}
candidate = {signature(trade): (fold, trade) for fold, trade in all_trades['candidate']}
common = sorted(set(baseline) & set(candidate))
candidate_only = sorted(set(candidate) - set(baseline))
locks = [(fold, trade) for fold, trade in all_trades['candidate'] if trade.get('exit')]

def hold_pnl(fold, trade):
    fill = trade['fill']
    cid = fill['order']['condition_id']
    won = fill['order']['token_id'] == winning_tokens[(fold, cid)]
    return (fill['filled_size'] if won else 0.0) - fill['cost'] - fill['fee']

winner_locks = [(fold, trade) for fold, trade in locks if hold_pnl(fold, trade) > 0]
loser_locks = [(fold, trade) for fold, trade in locks if hold_pnl(fold, trade) < 0]
state_feedback = {
    'common_entries': len(common),
    'baseline_only_entries': len(set(baseline) - set(candidate)),
    'candidate_only_entries': len(candidate_only),
    'common_baseline_pnl': sum(baseline[key][1]['pnl_after_fee'] for key in common),
    'common_candidate_pnl': sum(candidate[key][1]['pnl_after_fee'] for key in common),
    'candidate_only_pnl': sum(candidate[key][1]['pnl_after_fee'] for key in candidate_only),
    'locks': len(locks), 'terminal_winner_locks': len(winner_locks),
    'terminal_loser_locks': len(loser_locks),
    'winner_lock_delta': sum(trade['pnl_after_fee'] - hold_pnl(fold, trade) for fold, trade in winner_locks),
    'loser_lock_delta': sum(trade['pnl_after_fee'] - hold_pnl(fold, trade) for fold, trade in loser_locks),
}
state_feedback['lock_delta'] = state_feedback['winner_lock_delta'] + state_feedback['loser_lock_delta']
state_feedback
        """
    ),
    code(
        """
expected = diagnostic['aggregate']
assert math.isclose(aggregate_result['baseline']['total_pnl'], expected['baseline']['total_pnl_usd'], abs_tol=1e-8)
assert math.isclose(aggregate_result['candidate']['total_pnl'], expected['candidate']['total_pnl_usd'], abs_tol=1e-8)
assert aggregate_result['candidate']['trades'] == expected['candidate']['trades']
assert math.isclose(state_feedback['lock_delta'], diagnostic['lock_accounting']['lock_delta_vs_terminal_hold_usd'], abs_tol=1e-8)
assert state_feedback['common_entries'] == diagnostic['state_feedback_decomposition']['common_entries']

verdict = {
    'quality': quality,
    'candidate_pnl_usd': aggregate_result['candidate']['total_pnl'],
    'candidate_minus_baseline_pnl_usd': aggregate_result['candidate']['total_pnl'] - aggregate_result['baseline']['total_pnl'],
    'candidate_profit_factor': aggregate_result['candidate']['profit_factor'],
    'candidate_payoff_ratio': aggregate_result['candidate']['payoff_ratio'],
    'candidate_wilson_95_lower': aggregate_result['candidate']['wilson_95_lower'],
    'candidate_first_half_pnl': aggregate_result['candidate']['first_half_pnl'],
    'candidate_second_half_pnl': aggregate_result['candidate']['second_half_pnl'],
    'decision': 'REJECT_COMPLETE_SET_LOCK_V1',
}
print(json.dumps(verdict, indent=2))
        """
    ),
    code(
        """
# Chart contract: ordered comparison; grouped bars; 14 independent 8-hour folds;
# blue baseline vs orange candidate plus direct zero-line context.
x = np.arange(len(folds))
width = 0.38
fig, ax = plt.subplots(figsize=(12, 5.2))
ax.bar(x-width/2, [row['baseline_pnl'] for row in folds], width, label='Baseline', color='#3B6EA8')
ax.bar(x+width/2, [row['candidate_pnl'] for row in folds], width, label='Immediate lock v1', color='#D9772E')
ax.axhline(0, color='#333333', linewidth=1)
ax.set_title('Fee-inclusive PnL by historical fold')
ax.set_ylabel('PnL (USD)')
ax.set_xticks(x, [str(row['fold']) for row in folds])
ax.set_xlabel('Chronological 8-hour fold')
ax.legend(frameon=False, ncol=2)
ax.grid(axis='y', color='#D9DDE3', linewidth=0.7)
ax.set_axisbelow(True)
fig.tight_layout()
plt.show()
        """
    ),
    code(
        """
# Chart contract: signed component comparison; horizontal bars; exact lock-only
# terminal-hold deltas; dark/open two-tone encoding and visible zero line.
labels = ['Terminal-winner locks', 'Terminal-loser locks', 'Net lock effect']
values = [state_feedback['winner_lock_delta'], state_feedback['loser_lock_delta'], state_feedback['lock_delta']]
colors = ['#A9B8CA', '#D9772E', '#3B6EA8']
fig, ax = plt.subplots(figsize=(10, 4.4))
bars = ax.barh(labels, values, color=colors, edgecolor='#2F3640', linewidth=0.8)
ax.axvline(0, color='#333333', linewidth=1)
ax.set_title('Complete-set lock PnL delta versus terminal hold')
ax.set_xlabel('PnL delta (USD)')
ax.grid(axis='x', color='#D9DDE3', linewidth=0.7)
ax.set_axisbelow(True)
for bar, value in zip(bars, values):
    ax.text(value + (1.0 if value >= 0 else -1.0), bar.get_y()+bar.get_height()/2,
            f'{value:+.2f}', va='center', ha='left' if value >= 0 else 'right')
fig.tight_layout()
plt.show()
        """
    ),
    nbf.v4.new_markdown_cell(
        """## Limitations and next step

- Folds 29–42 were already observed during mechanism diagnosis and cannot support promotion.
- Stateful realized-loss feedback changes later entry membership; the full-policy effect and common-entry effect are reported separately.
- Gas, merge, and redemption operating costs are excluded consistently with the baseline.
- The immediate v1 lock is not retuned. Any trailing redesign needs a new hash, preregistration, one-shot diagnostic, and two disjoint fresh blocks."""
    ),
]

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
NotebookClient(
    notebook,
    timeout=600,
    kernel_name="polymomentum-notebook",
    resources={"metadata": {"path": str(ROOT)}},
).execute()
nbf.write(notebook, OUTPUT)
print(OUTPUT)
