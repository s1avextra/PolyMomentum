//! Strategy freshness diagnostics.
//!
//! This is deliberately an alerting and re-scout trigger, not an automatic
//! live retuning path. A stale verdict should push the strategy back through
//! feed-forward backtest/live-replay gates before any promotion changes.

use std::collections::{BTreeMap, HashMap};
use std::io::BufRead;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct StalenessConfig {
    pub min_outcomes: usize,
    pub min_recent_window: usize,
    pub min_recent_win_rate: f64,
    pub delta: f64,
    pub min_trade_rate: f64,
}

impl Default for StalenessConfig {
    fn default() -> Self {
        Self {
            min_outcomes: 30,
            min_recent_window: 10,
            min_recent_win_rate: 0.55,
            delta: 0.01,
            min_trade_rate: 0.0005,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StalenessReport {
    pub schema_version: u32,
    pub ok: bool,
    pub status: String,
    pub path: String,
    pub promotion_strategy_hash: Option<String>,
    pub runtime_strategy_hash: Option<String>,
    pub sample: StalenessSample,
    pub drift: DriftSummary,
    pub warnings: Vec<String>,
    pub recommendation: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct StalenessSample {
    pub outcomes: usize,
    pub wins: usize,
    pub losses: usize,
    pub win_rate: Option<f64>,
    pub recent_window: usize,
    pub recent_wins: usize,
    pub recent_losses: usize,
    pub recent_win_rate: Option<f64>,
    pub total_pnl: f64,
    pub avg_pnl: Option<f64>,
    pub signal_evaluations: u64,
    pub execution_attempted: u64,
    pub trade_rate: Option<f64>,
    pub top_skip_reasons: Vec<(String, u64)>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DriftSummary {
    pub checked: bool,
    pub significant: bool,
    pub best_cut: Option<usize>,
    pub old_window: usize,
    pub recent_window: usize,
    pub old_win_rate: Option<f64>,
    pub recent_win_rate: Option<f64>,
    pub drop: Option<f64>,
    pub epsilon: Option<f64>,
    pub delta: f64,
}

#[derive(Debug, Clone, Copy)]
struct Outcome {
    won: bool,
    pnl: f64,
}

pub fn analyze_staleness(
    path: impl AsRef<Path>,
    config: StalenessConfig,
) -> Result<StalenessReport> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)
        .with_context(|| format!("open session log {}", path.display()))?;
    let reader = std::io::BufReader::new(file);

    let mut outcomes_by_cid: HashMap<String, Outcome> = HashMap::new();
    let mut ordered_cids: Vec<String> = Vec::new();
    let mut signal_evaluations = 0_u64;
    let mut execution_attempted = 0_u64;
    let mut skip_reasons: BTreeMap<String, u64> = BTreeMap::new();
    let mut promotion_strategy_hash = None;
    let mut runtime_strategy_hash = None;
    let mut warnings = Vec::new();

    for line in reader.lines() {
        let line = line.with_context(|| format!("read session log {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(&line) else {
            warnings.push("malformed_jsonl_line".to_string());
            continue;
        };
        let cat = v.get("cat").and_then(|x| x.as_str()).unwrap_or_default();
        let ty = v.get("type").and_then(|x| x.as_str()).unwrap_or_default();
        match (cat, ty) {
            ("system", "release_manifest") => {
                promotion_strategy_hash = v
                    .get("promotion")
                    .and_then(|p| p.get("strategy"))
                    .and_then(|s| s.get("params_hash"))
                    .and_then(|x| x.as_str())
                    .map(str::to_string);
            }
            ("system", "runtime_strategy") => {
                runtime_strategy_hash = v
                    .get("strategy")
                    .and_then(|s| s.get("params_hash"))
                    .and_then(|x| x.as_str())
                    .map(str::to_string);
            }
            ("signal", "evaluation") => {
                signal_evaluations += 1;
                if v.get("execution_attempted")
                    .and_then(|x| x.as_bool())
                    .unwrap_or(false)
                {
                    execution_attempted += 1;
                }
            }
            ("signal", "skip") => {
                let reason = v
                    .get("reason")
                    .and_then(|x| x.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                *skip_reasons.entry(reason).or_insert(0) += 1;
            }
            ("resolution", "resolved") | ("resolution", "realized") => {
                let cid = v
                    .get("cid")
                    .and_then(|x| x.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let Some(won) = v.get("won").and_then(|x| x.as_bool()) else {
                    continue;
                };
                let pnl = v.get("pnl").and_then(|x| x.as_f64()).unwrap_or(0.0);
                if !outcomes_by_cid.contains_key(&cid) {
                    ordered_cids.push(cid.clone());
                }
                // A realized outcome supersedes a provisional resolved outcome.
                outcomes_by_cid.insert(cid, Outcome { won, pnl });
            }
            ("system", "error") => warnings.push("system_error_seen".to_string()),
            _ => {}
        }
    }

    let outcomes: Vec<Outcome> = ordered_cids
        .iter()
        .filter_map(|cid| outcomes_by_cid.get(cid).copied())
        .collect();
    let sample = summarize_sample(
        &outcomes,
        signal_evaluations,
        execution_attempted,
        skip_reasons,
    );
    let drift = detect_win_rate_drift(
        &outcomes,
        config.min_recent_window,
        config.delta.clamp(1e-9, 0.5),
    );

    if sample.outcomes < config.min_outcomes {
        warnings.push(format!(
            "insufficient_outcomes: {} < {}",
            sample.outcomes, config.min_outcomes
        ));
    }
    if let Some(recent_win_rate) = sample.recent_win_rate {
        if sample.recent_window >= config.min_recent_window
            && recent_win_rate < config.min_recent_win_rate
        {
            warnings.push(format!(
                "recent_win_rate_low: {:.3} < {:.3}",
                recent_win_rate, config.min_recent_win_rate
            ));
        }
    }
    if let Some(trade_rate) = sample.trade_rate {
        if signal_evaluations >= 10_000 && trade_rate < config.min_trade_rate {
            warnings.push(format!(
                "trade_rate_low: {:.6} < {:.6}",
                trade_rate, config.min_trade_rate
            ));
        }
    }
    if promotion_strategy_hash.is_some()
        && runtime_strategy_hash.is_some()
        && promotion_strategy_hash != runtime_strategy_hash
    {
        warnings.push("promotion_runtime_hash_mismatch".to_string());
    }

    let stale = sample.outcomes >= config.min_outcomes
        && drift.significant
        && sample
            .recent_win_rate
            .map(|wr| wr < config.min_recent_win_rate)
            .unwrap_or(false);
    let status = if stale {
        "stale"
    } else if warnings.is_empty() {
        "ok"
    } else {
        "watch"
    };
    let recommendation = match status {
        "stale" => "freeze promotion changes, keep paper/live risk fail-closed, and run rolling-history re-scout plus live-replay gates before any new artifact".to_string(),
        "watch" => "keep deployed artifact unchanged, collect more outcomes, and schedule an offline re-scout if warnings persist across the next resolved window".to_string(),
        _ => "strategy freshness checks are green; continue monitoring and only promote through feed-forward offline gates".to_string(),
    };

    Ok(StalenessReport {
        schema_version: 1,
        ok: status != "stale",
        status: status.to_string(),
        path: path.display().to_string(),
        promotion_strategy_hash,
        runtime_strategy_hash,
        sample,
        drift,
        warnings,
        recommendation,
    })
}

fn summarize_sample(
    outcomes: &[Outcome],
    signal_evaluations: u64,
    execution_attempted: u64,
    skip_reasons: BTreeMap<String, u64>,
) -> StalenessSample {
    let wins = outcomes.iter().filter(|o| o.won).count();
    let losses = outcomes.len().saturating_sub(wins);
    let total_pnl = outcomes.iter().map(|o| o.pnl).sum::<f64>();
    let recent_window = outcomes.len().min(20);
    let recent = &outcomes[outcomes.len().saturating_sub(recent_window)..];
    let recent_wins = recent.iter().filter(|o| o.won).count();
    let recent_losses = recent.len().saturating_sub(recent_wins);
    let mut top_skip_reasons: Vec<(String, u64)> = skip_reasons.into_iter().collect();
    top_skip_reasons.sort_by_key(|item| std::cmp::Reverse(item.1));
    top_skip_reasons.truncate(8);

    StalenessSample {
        outcomes: outcomes.len(),
        wins,
        losses,
        win_rate: rate(wins, outcomes.len()),
        recent_window,
        recent_wins,
        recent_losses,
        recent_win_rate: rate(recent_wins, recent.len()),
        total_pnl,
        avg_pnl: (!outcomes.is_empty()).then_some(total_pnl / outcomes.len() as f64),
        signal_evaluations,
        execution_attempted,
        trade_rate: (signal_evaluations > 0)
            .then_some(execution_attempted as f64 / signal_evaluations as f64),
        top_skip_reasons,
    }
}

fn rate(n: usize, d: usize) -> Option<f64> {
    (d > 0).then_some(n as f64 / d as f64)
}

fn detect_win_rate_drift(
    outcomes: &[Outcome],
    min_recent_window: usize,
    delta: f64,
) -> DriftSummary {
    let n = outcomes.len();
    let mut out = DriftSummary {
        delta,
        ..DriftSummary::default()
    };
    if n < min_recent_window.saturating_mul(2).max(2) {
        return out;
    }
    out.checked = true;

    let wins: Vec<f64> = outcomes
        .iter()
        .map(|o| if o.won { 1.0 } else { 0.0 })
        .collect();
    let mut prefix = Vec::with_capacity(wins.len() + 1);
    prefix.push(0.0);
    for w in &wins {
        prefix.push(prefix.last().copied().unwrap_or(0.0) + *w);
    }

    let mut best_drop = 0.0;
    for cut in min_recent_window..=(n - min_recent_window) {
        let old_n = cut;
        let recent_n = n - cut;
        let old_rate = prefix[cut] / old_n as f64;
        let recent_rate = (prefix[n] - prefix[cut]) / recent_n as f64;
        let drop = old_rate - recent_rate;
        let epsilon =
            (0.5 * (4.0 / delta).ln() * (1.0 / old_n as f64 + 1.0 / recent_n as f64)).sqrt();
        if drop > best_drop {
            best_drop = drop;
            out.best_cut = Some(cut);
            out.old_window = old_n;
            out.recent_window = recent_n;
            out.old_win_rate = Some(old_rate);
            out.recent_win_rate = Some(recent_rate);
            out.drop = Some(drop);
            out.epsilon = Some(epsilon);
            out.significant = drop > epsilon;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_recent_win_rate_drop() {
        let mut outcomes = Vec::new();
        outcomes.extend((0..40).map(|_| Outcome {
            won: true,
            pnl: 0.4,
        }));
        outcomes.extend((0..20).map(|_| Outcome {
            won: false,
            pnl: -0.8,
        }));
        let drift = detect_win_rate_drift(&outcomes, 10, 0.01);
        assert!(drift.checked);
        assert!(drift.significant);
        assert!(drift.drop.unwrap_or(0.0) > 0.5);
    }

    #[test]
    fn sample_uses_recent_tail() {
        let outcomes = vec![
            Outcome {
                won: true,
                pnl: 1.0,
            },
            Outcome {
                won: false,
                pnl: -1.0,
            },
            Outcome {
                won: false,
                pnl: -1.0,
            },
        ];
        let sample = summarize_sample(&outcomes, 100, 2, BTreeMap::new());
        assert_eq!(sample.outcomes, 3);
        assert_eq!(sample.wins, 1);
        assert_eq!(sample.recent_losses, 2);
        assert_eq!(sample.trade_rate, Some(0.02));
    }
}
