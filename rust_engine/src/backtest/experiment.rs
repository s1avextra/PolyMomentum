//! Reproducible experiment reports for harness runs.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::backtest::harness::{HarnessConfig, HarnessRun};
use crate::backtest::resolver::BacktestDiagnostics;
use crate::backtest::strategies::StrategyVariant;
use crate::data::catalog::MarketCatalog;
use crate::data::manifest::{DataManifest, DataSourceManifest};
use crate::strategy::spec::StrategySpec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentReport {
    pub schema_version: u32,
    pub generated_at: String,
    pub label: String,
    pub mode: String,
    pub start: String,
    pub end: String,
    pub bankroll_usd: f64,
    pub latency_ms: u64,
    pub market_catalog: MarketCatalog,
    pub data_manifest: DataManifest,
    pub variants: Vec<VariantReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantReport {
    pub strategy: StrategySpec,
    #[serde(default)]
    pub strategy_params: serde_json::Value,
    pub trades: usize,
    pub wins: usize,
    pub losses: usize,
    pub unresolved_fills: usize,
    #[serde(default)]
    pub execution_attempts: usize,
    #[serde(default)]
    pub fills_success: usize,
    #[serde(default)]
    pub fills_failed: usize,
    #[serde(default)]
    pub fill_rate: f64,
    #[serde(default)]
    pub reject_reasons: BTreeMap<String, usize>,
    #[serde(default)]
    pub breaker_tripped: bool,
    #[serde(default)]
    pub breaker_reason: Option<String>,
    #[serde(default)]
    pub breaker_tripped_at_s: Option<f64>,
    #[serde(default)]
    pub breaker_realized_drawdown_pct: f64,
    #[serde(default)]
    pub breaker_stressed_drawdown_pct: f64,
    #[serde(default)]
    pub diagnostics: BacktestDiagnostics,
    pub win_rate: f64,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub total_fees: f64,
    pub sharpe_like: f64,
    pub by_zone: BTreeMap<String, ZoneReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneReport {
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub win_rate: f64,
    pub pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionGate {
    #[serde(default = "default_min_trades")]
    pub min_trades: usize,
    #[serde(default = "default_min_losses")]
    pub min_losses: usize,
    #[serde(default = "default_min_zone_count")]
    pub min_zone_count: usize,
    #[serde(default)]
    pub min_win_rate: f64,
    #[serde(default)]
    pub min_wilson_win_rate_lower: f64,
    #[serde(default)]
    pub min_total_pnl: f64,
    #[serde(default)]
    pub min_sharpe_like: f64,
    #[serde(default)]
    pub max_unresolved_fills: usize,
    #[serde(default)]
    pub max_failed_fills: usize,
    #[serde(default)]
    pub max_passive_failed_fills: usize,
    #[serde(default)]
    pub min_fill_rate: f64,
    #[serde(default = "default_max_zone_trade_share")]
    pub max_zone_trade_share: f64,
    #[serde(default = "default_require_complete_data")]
    pub require_complete_data: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiReportPromotionGate {
    #[serde(default = "default_min_reports")]
    pub min_reports: usize,
    #[serde(default = "default_min_profitable_reports")]
    pub min_profitable_reports: usize,
    #[serde(default)]
    pub min_daily_trades: usize,
    #[serde(default)]
    pub min_daily_pnl: f64,
    #[serde(default)]
    pub max_daily_loss: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RobustPromotionGate {
    #[serde(default = "default_min_neighbor_count")]
    pub min_neighbor_count: usize,
    #[serde(default = "default_min_neighbor_positive_rate")]
    pub min_neighbor_positive_rate: f64,
    #[serde(default = "default_max_pbo")]
    pub max_pbo: f64,
    #[serde(default)]
    pub min_worst_window_pnl: f64,
    #[serde(default)]
    pub min_robust_score: f64,
    #[serde(default)]
    pub min_profit_factor: f64,
    #[serde(default)]
    pub min_payoff_ratio: f64,
    #[serde(default)]
    pub max_worst_loss_to_avg_win: f64,
}

impl Default for PromotionGate {
    fn default() -> Self {
        Self {
            min_trades: default_min_trades(),
            min_losses: default_min_losses(),
            min_zone_count: default_min_zone_count(),
            min_win_rate: 0.0,
            min_wilson_win_rate_lower: 0.0,
            min_total_pnl: 0.0,
            min_sharpe_like: 0.0,
            max_unresolved_fills: 0,
            max_failed_fills: 0,
            max_passive_failed_fills: 0,
            min_fill_rate: 0.0,
            max_zone_trade_share: default_max_zone_trade_share(),
            require_complete_data: default_require_complete_data(),
        }
    }
}

impl Default for MultiReportPromotionGate {
    fn default() -> Self {
        Self {
            min_reports: default_min_reports(),
            min_profitable_reports: default_min_profitable_reports(),
            min_daily_trades: 0,
            min_daily_pnl: 0.0,
            max_daily_loss: 0.0,
        }
    }
}

impl Default for RobustPromotionGate {
    fn default() -> Self {
        Self {
            min_neighbor_count: default_min_neighbor_count(),
            min_neighbor_positive_rate: default_min_neighbor_positive_rate(),
            max_pbo: default_max_pbo(),
            min_worst_window_pnl: 0.0,
            min_robust_score: 0.0,
            min_profit_factor: 0.0,
            min_payoff_ratio: 0.0,
            max_worst_loss_to_avg_win: 0.0,
        }
    }
}

fn default_min_trades() -> usize {
    30
}

fn default_min_losses() -> usize {
    1
}

fn default_min_zone_count() -> usize {
    2
}

fn default_min_reports() -> usize {
    3
}

fn default_min_profitable_reports() -> usize {
    2
}

fn default_min_neighbor_count() -> usize {
    2
}

fn default_min_neighbor_positive_rate() -> f64 {
    0.60
}

fn default_max_pbo() -> f64 {
    0.50
}

fn default_max_zone_trade_share() -> f64 {
    0.70
}

fn default_require_complete_data() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionArtifact {
    pub schema_version: u32,
    pub created_at: String,
    pub source_report_hash: String,
    pub source_label: String,
    pub source_window: String,
    pub selected_strategy: StrategySpec,
    #[serde(default)]
    pub strategy_params: serde_json::Value,
    pub data_manifest_hash: String,
    pub market_count: usize,
    pub trades: usize,
    pub win_rate: f64,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub total_fees: f64,
    #[serde(default)]
    pub sharpe_like: f64,
    pub dominant_zone: Option<String>,
    pub dominant_zone_trade_share: Option<f64>,
    pub risk_notes: Vec<String>,
    pub promotion_gate: PromotionGate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub robust_diagnostics: Option<RobustPromotionDiagnostics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RobustPromotionDiagnostics {
    pub schema_version: u32,
    pub trial_ledger: TrialLedger,
    pub pbo: PboDiagnostic,
    pub selected: RobustVariantDiagnostics,
    pub top_candidates: Vec<RobustVariantDiagnostics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrialLedger {
    pub report_count: usize,
    pub variant_count: usize,
    pub comparable_variant_count: usize,
    pub total_variant_report_trials: usize,
    pub windows: Vec<String>,
    pub report_hashes: Vec<String>,
    pub selection_objective: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PboDiagnostic {
    pub splits: usize,
    pub pbo: f64,
    pub median_oos_percentile: f64,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RobustVariantDiagnostics {
    pub strategy_name: String,
    pub params_hash: String,
    pub total_pnl: f64,
    pub trades: usize,
    pub worst_window_pnl: f64,
    pub median_window_pnl: f64,
    pub worst_window_expectancy: f64,
    pub median_window_expectancy: f64,
    pub wilson_win_rate_lower: f64,
    pub neighbor_count: usize,
    pub neighbor_positive_rate: f64,
    pub fill_rate: f64,
    pub max_stressed_drawdown_pct: f64,
    pub profit_factor: f64,
    pub payoff_ratio: f64,
    pub worst_loss_to_avg_win: f64,
    pub robust_score: f64,
}

impl ExperimentReport {
    pub fn from_harness(
        label: impl Into<String>,
        cfg: &HarnessConfig,
        runs: &[HarnessRun],
    ) -> Self {
        let market_catalog = MarketCatalog::from_candle_contracts(&cfg.universe.contracts);
        let data_manifest = harness_data_manifest(cfg, &market_catalog);
        let start = cfg
            .hours
            .first()
            .map(|h| h.to_rfc3339())
            .unwrap_or_default();
        let end = cfg.hours.last().map(|h| h.to_rfc3339()).unwrap_or_default();
        let mut variants: Vec<VariantReport> = runs.iter().map(VariantReport::from_run).collect();
        variants.sort_by(|a, b| {
            b.total_pnl
                .partial_cmp(&a.total_pnl)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Self {
            schema_version: 1,
            generated_at: Utc::now().to_rfc3339(),
            label: label.into(),
            mode: "backtest".to_string(),
            start,
            end,
            bankroll_usd: cfg.bankroll_usd,
            latency_ms: cfg.latency.insert_ms,
            market_catalog,
            data_manifest,
            variants,
        }
    }
}

impl VariantReport {
    fn from_run(run: &HarnessRun) -> Self {
        let strategy = StrategySpec::from_serializable_params(
            "candle_momentum",
            "1",
            &run.variant,
            run.variant.risk_profile(),
        );
        let mut diagnostics = run.results.diagnostics.clone();
        diagnostics.trade_pnl = run.results.pnl_diagnostics();
        diagnostics.by_regime = run.results.by_regime();
        diagnostics.by_causal_bucket = run.results.by_causal_bucket();
        let by_zone = run
            .results
            .by_zone()
            .into_iter()
            .map(|(zone, bucket)| {
                (
                    zone,
                    ZoneReport {
                        trades: bucket.trades,
                        wins: bucket.wins,
                        losses: bucket.losses,
                        win_rate: bucket.win_rate(),
                        pnl: bucket.pnl,
                    },
                )
            })
            .collect();
        Self {
            strategy,
            strategy_params: serde_json::to_value(&run.variant).unwrap_or(serde_json::Value::Null),
            trades: run.results.n_trades(),
            wins: run.results.n_wins(),
            losses: run.results.n_losses(),
            unresolved_fills: run.results.unresolved_fills.len(),
            execution_attempts: run.results.execution_attempts,
            fills_success: run.results.fills_success,
            fills_failed: run.results.fills_failed,
            fill_rate: run.results.fill_rate(),
            reject_reasons: run.results.reject_reasons.clone(),
            breaker_tripped: run.results.breaker.tripped,
            breaker_reason: run.results.breaker.reason.clone(),
            breaker_tripped_at_s: run.results.breaker.tripped_at_s,
            breaker_realized_drawdown_pct: run.results.breaker.metrics.realized_drawdown_pct,
            breaker_stressed_drawdown_pct: run.results.breaker.metrics.stressed_drawdown_pct,
            diagnostics,
            win_rate: run.results.win_rate(),
            total_pnl: run.results.total_pnl(),
            avg_pnl: run.results.avg_pnl(),
            total_fees: run.results.total_fees(),
            sharpe_like: run.results.sharpe(),
            by_zone,
        }
    }
}

impl PromotionArtifact {
    pub fn from_report(report: &ExperimentReport, gate: PromotionGate) -> Result<Self> {
        if gate.require_complete_data && !report.data_manifest.complete {
            bail!("promotion rejected: data manifest is incomplete");
        }
        if report.variants.is_empty() {
            bail!("promotion rejected: report has no variants");
        }
        let selected = report
            .variants
            .iter()
            .filter(|variant| promotion_rejection_reasons(variant, &gate).is_empty())
            .max_by(|a, b| {
                a.total_pnl
                    .partial_cmp(&b.total_pnl)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        let selected = match selected {
            Some(selected) => selected,
            None => {
                let best = report
                    .variants
                    .iter()
                    .max_by(|a, b| {
                        a.total_pnl
                            .partial_cmp(&b.total_pnl)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .expect("checked non-empty report variants");
                bail!(
                    "promotion rejected: no variants passed gates; best candidate failed: {}",
                    promotion_rejection_reasons(best, &gate).join("; ")
                );
            }
        };

        let mut risk_notes = Vec::new();
        if selected.unresolved_fills > 0 {
            risk_notes.push(format!(
                "selected variant has {} unresolved fills",
                selected.unresolved_fills
            ));
        }
        let passive_failed = passive_failed_fills(selected);
        let non_passive_failed = selected.fills_failed.saturating_sub(passive_failed);
        if non_passive_failed > 0 {
            risk_notes.push(format!(
                "selected variant has {} non-passive failed execution attempts: {:?}",
                non_passive_failed, selected.reject_reasons
            ));
        }
        if passive_failed > 0 {
            risk_notes.push(format!(
                "selected variant has {} passive maker non-fills/post-only rejects: {:?}",
                passive_failed, selected.reject_reasons
            ));
        }
        if selected.breaker_tripped {
            risk_notes.push(format!(
                "selected variant tripped circuit breaker: {}",
                selected.breaker_reason.as_deref().unwrap_or("unknown")
            ));
        }
        let (dominant_zone, dominant_zone_trade_share) = dominant_zone_share(selected);
        if let (Some(zone), Some(share)) = (&dominant_zone, dominant_zone_trade_share) {
            risk_notes.push(format!(
                "dominant zone {zone} carries {:.1}% of selected trades",
                100.0 * share
            ));
        }
        risk_notes.push(format!(
            "wilson win-rate lower bound 95%: {:.3}",
            wilson_win_rate_lower(selected.wins, selected.trades)
        ));
        risk_notes.extend(report.data_manifest.notes.iter().cloned());

        Ok(Self {
            schema_version: 1,
            created_at: Utc::now().to_rfc3339(),
            source_report_hash: crate::strategy::spec::stable_json_hash(report),
            source_label: report.label.clone(),
            source_window: format!("{}..{}", report.start, report.end),
            selected_strategy: selected.strategy.clone(),
            strategy_params: selected.strategy_params.clone(),
            data_manifest_hash: report.data_manifest.manifest_hash.clone(),
            market_count: report.market_catalog.market_count(),
            trades: selected.trades,
            win_rate: selected.win_rate,
            total_pnl: selected.total_pnl,
            avg_pnl: selected.avg_pnl,
            total_fees: selected.total_fees,
            sharpe_like: selected.sharpe_like,
            dominant_zone,
            dominant_zone_trade_share,
            risk_notes,
            promotion_gate: gate,
            robust_diagnostics: None,
        })
    }

    pub fn from_reports(
        reports: &[ExperimentReport],
        gate: PromotionGate,
        multi_gate: MultiReportPromotionGate,
    ) -> Result<Self> {
        let aggregate = aggregate_reports(reports, &multi_gate)?;
        if gate.require_complete_data && !aggregate.data_manifest.complete {
            bail!("promotion rejected: aggregate data manifest is incomplete");
        }
        if aggregate.variants.is_empty() {
            bail!("promotion rejected: aggregate report has no variants");
        }

        let selected = aggregate
            .variants
            .iter()
            .filter(|variant| {
                promotion_rejection_reasons(variant, &gate).is_empty()
                    && multi_report_rejection_reasons(reports, variant, &gate, &multi_gate)
                        .is_empty()
            })
            .max_by(|a, b| {
                a.total_pnl
                    .partial_cmp(&b.total_pnl)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        let selected = match selected {
            Some(selected) => selected,
            None => {
                let best = aggregate
                    .variants
                    .iter()
                    .max_by(|a, b| {
                        a.total_pnl
                            .partial_cmp(&b.total_pnl)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .expect("checked non-empty aggregate variants");
                let mut reasons = promotion_rejection_reasons(best, &gate);
                reasons.extend(multi_report_rejection_reasons(
                    reports,
                    best,
                    &gate,
                    &multi_gate,
                ));
                bail!(
                    "promotion rejected: no variants passed aggregate gates; best candidate failed: {}",
                    reasons.join("; ")
                );
            }
        };

        let mut selected_report = aggregate.clone();
        selected_report.variants = vec![(*selected).clone()];
        let mut artifact = Self::from_report(&selected_report, gate)?;
        artifact
            .risk_notes
            .extend(multi_report_risk_notes(reports, selected, &multi_gate));
        Ok(artifact)
    }

    pub fn from_reports_robust(
        reports: &[ExperimentReport],
        gate: PromotionGate,
        multi_gate: MultiReportPromotionGate,
        robust_gate: RobustPromotionGate,
    ) -> Result<(Self, RobustPromotionDiagnostics)> {
        let aggregate = aggregate_reports(reports, &multi_gate)?;
        if gate.require_complete_data && !aggregate.data_manifest.complete {
            bail!("promotion rejected: aggregate data manifest is incomplete");
        }
        if aggregate.variants.is_empty() {
            bail!("promotion rejected: aggregate report has no variants");
        }

        let pbo = estimate_pbo(reports);
        if pbo.splits > 0 && pbo.pbo > robust_gate.max_pbo {
            bail!(
                "robust promotion rejected: PBO {:.4} above maximum {:.4}",
                pbo.pbo,
                robust_gate.max_pbo
            );
        }

        let mut scored = Vec::new();
        let mut rejection_samples = Vec::new();
        for variant in &aggregate.variants {
            let mut reasons = promotion_rejection_reasons(variant, &gate);
            reasons.extend(multi_report_rejection_reasons(
                reports,
                variant,
                &gate,
                &multi_gate,
            ));
            let diagnostic = robust_variant_diagnostics(reports, &aggregate, variant);
            reasons.extend(robust_rejection_reasons(&diagnostic, &robust_gate));
            if reasons.is_empty() {
                scored.push((variant, diagnostic));
            } else if rejection_samples.len() < 3 {
                rejection_samples.push(format!(
                    "{} failed: {}",
                    variant_label(variant),
                    reasons.join("; ")
                ));
            }
        }

        scored.sort_by(|(_, a), (_, b)| {
            b.robust_score
                .partial_cmp(&a.robust_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    b.total_pnl
                        .partial_cmp(&a.total_pnl)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
        });

        let Some((selected, selected_diag)) = scored.first().cloned() else {
            let suffix = if rejection_samples.is_empty() {
                "no comparable variants".to_string()
            } else {
                rejection_samples.join(" | ")
            };
            bail!("robust promotion rejected: no variants passed robust gates; {suffix}");
        };

        let mut selected_report = aggregate.clone();
        selected_report.variants = vec![selected.clone()];
        let mut artifact = Self::from_report(&selected_report, gate)?;
        artifact
            .risk_notes
            .extend(multi_report_risk_notes(reports, selected, &multi_gate));
        artifact.risk_notes.push(format!(
            "robust score {:.4}; worst_window_pnl {:.2}; median_window_pnl {:.2}; neighbor_positive_rate {:.1}% over {} neighbor(s)",
            selected_diag.robust_score,
            selected_diag.worst_window_pnl,
            selected_diag.median_window_pnl,
            100.0 * selected_diag.neighbor_positive_rate,
            selected_diag.neighbor_count,
        ));
        artifact.risk_notes.push(format!(
            "loss asymmetry: profit_factor {:.3}; payoff_ratio {:.3}; worst_loss_to_avg_win {:.3}",
            selected_diag.profit_factor,
            selected_diag.payoff_ratio,
            selected_diag.worst_loss_to_avg_win
        ));
        artifact.risk_notes.push(format!(
            "PBO estimate {:.3} across {} split(s); median OOS percentile {:.3}",
            pbo.pbo, pbo.splits, pbo.median_oos_percentile
        ));
        artifact
            .risk_notes
            .push(format!("trial ledger: {}", trial_ledger_note(reports)));

        let diagnostics = RobustPromotionDiagnostics {
            schema_version: 1,
            trial_ledger: build_trial_ledger(reports),
            pbo,
            selected: selected_diag.clone(),
            top_candidates: scored
                .into_iter()
                .map(|(_, diagnostic)| diagnostic)
                .take(10)
                .collect(),
        };
        artifact.robust_diagnostics = Some(diagnostics.clone());
        Ok((artifact, diagnostics))
    }
}

fn aggregate_reports(
    reports: &[ExperimentReport],
    multi_gate: &MultiReportPromotionGate,
) -> Result<ExperimentReport> {
    if reports.len() < multi_gate.min_reports {
        bail!(
            "promotion rejected: report count {} below minimum {}",
            reports.len(),
            multi_gate.min_reports
        );
    }

    let mut groups: BTreeMap<String, Vec<&VariantReport>> = BTreeMap::new();
    for report in reports {
        for variant in &report.variants {
            groups
                .entry(variant_key(variant))
                .or_default()
                .push(variant);
        }
    }

    let mut variants = Vec::new();
    for (_key, group) in groups {
        if group.len() != reports.len() {
            continue;
        }
        variants.push(aggregate_variant_reports(&group));
    }
    variants.sort_by(|a, b| {
        b.total_pnl
            .partial_cmp(&a.total_pnl)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let first = reports
        .first()
        .context("aggregate reports requires at least one report")?;
    let last = reports.last().unwrap_or(first);
    let mut market_catalog = MarketCatalog::default();
    for report in reports {
        market_catalog
            .markets
            .extend(report.market_catalog.markets.clone());
        market_catalog
            .token_to_condition
            .extend(report.market_catalog.token_to_condition.clone());
    }

    let mut src = DataSourceManifest::new("experiment_reports", "aggregate_backtest");
    src.start = Some(first.start.clone());
    src.end = Some(last.end.clone());
    src.row_count = Some(reports.len() as u64);
    src.complete = reports.iter().all(|r| r.data_manifest.complete);
    src.metadata.insert(
        "report_hashes".to_string(),
        reports
            .iter()
            .map(crate::strategy::spec::stable_json_hash)
            .collect::<Vec<_>>()
            .join(","),
    );
    src.metadata.insert(
        "windows".to_string(),
        reports
            .iter()
            .map(|r| format!("{}..{}", r.start, r.end))
            .collect::<Vec<_>>()
            .join(","),
    );

    let mut notes = Vec::new();
    for report in reports {
        notes.extend(report.data_manifest.notes.iter().cloned());
    }

    Ok(ExperimentReport {
        schema_version: 1,
        generated_at: Utc::now().to_rfc3339(),
        label: format!("aggregate_{}d", reports.len()),
        mode: "backtest_aggregate".to_string(),
        start: first.start.clone(),
        end: last.end.clone(),
        bankroll_usd: first.bankroll_usd,
        latency_ms: first.latency_ms,
        market_catalog,
        data_manifest: DataManifest::new(vec![src], notes),
        variants,
    })
}

fn aggregate_variant_reports(group: &[&VariantReport]) -> VariantReport {
    let first = group[0];
    let trades: usize = group.iter().map(|v| v.trades).sum();
    let wins: usize = group.iter().map(|v| v.wins).sum();
    let losses: usize = group.iter().map(|v| v.losses).sum();
    let unresolved_fills: usize = group.iter().map(|v| v.unresolved_fills).sum();
    let execution_attempts: usize = group.iter().map(|v| v.execution_attempts).sum();
    let fills_success: usize = group.iter().map(|v| v.fills_success).sum();
    let fills_failed: usize = group.iter().map(|v| v.fills_failed).sum();
    let mut reject_reasons: BTreeMap<String, usize> = BTreeMap::new();
    for variant in group {
        for (reason, count) in &variant.reject_reasons {
            *reject_reasons.entry(reason.clone()).or_insert(0) += count;
        }
    }
    let mut diagnostics = BacktestDiagnostics::default();
    for variant in group {
        diagnostics.merge_from(variant.diagnostics.clone());
    }
    let total_pnl: f64 = group.iter().map(|v| v.total_pnl).sum();
    let total_fees: f64 = group.iter().map(|v| v.total_fees).sum();
    let mut by_zone: BTreeMap<String, ZoneReport> = BTreeMap::new();
    for variant in group {
        for (zone, stats) in &variant.by_zone {
            let entry = by_zone.entry(zone.clone()).or_insert(ZoneReport {
                trades: 0,
                wins: 0,
                losses: 0,
                win_rate: 0.0,
                pnl: 0.0,
            });
            entry.trades += stats.trades;
            entry.wins += stats.wins;
            entry.losses += stats.losses;
            entry.pnl += stats.pnl;
        }
    }
    for stats in by_zone.values_mut() {
        let resolved = stats.wins + stats.losses;
        stats.win_rate = if resolved == 0 {
            0.0
        } else {
            stats.wins as f64 / resolved as f64
        };
    }
    let strategy = serde_json::from_value::<crate::backtest::strategies::StrategyVariant>(
        first.strategy_params.clone(),
    )
    .map(|variant| {
        StrategySpec::from_serializable_params(
            first.strategy.name.clone(),
            first.strategy.version.clone(),
            &variant,
            variant.risk_profile(),
        )
    })
    .unwrap_or_else(|_| first.strategy.clone());

    VariantReport {
        strategy,
        strategy_params: first.strategy_params.clone(),
        trades,
        wins,
        losses,
        unresolved_fills,
        execution_attempts,
        fills_success,
        fills_failed,
        fill_rate: if execution_attempts == 0 {
            0.0
        } else {
            fills_success as f64 / execution_attempts as f64
        },
        reject_reasons,
        breaker_tripped: group.iter().any(|v| v.breaker_tripped),
        breaker_reason: group.iter().find_map(|v| v.breaker_reason.clone()),
        breaker_tripped_at_s: group.iter().find_map(|v| v.breaker_tripped_at_s),
        breaker_realized_drawdown_pct: group
            .iter()
            .map(|v| v.breaker_realized_drawdown_pct)
            .fold(0.0, f64::max),
        breaker_stressed_drawdown_pct: group
            .iter()
            .map(|v| v.breaker_stressed_drawdown_pct)
            .fold(0.0, f64::max),
        diagnostics,
        win_rate: if wins + losses == 0 {
            0.0
        } else {
            wins as f64 / (wins + losses) as f64
        },
        total_pnl,
        avg_pnl: if trades == 0 {
            0.0
        } else {
            total_pnl / trades as f64
        },
        total_fees,
        sharpe_like: daily_sharpe(group),
        by_zone,
    }
}

fn build_trial_ledger(reports: &[ExperimentReport]) -> TrialLedger {
    let comparable_variant_count = comparable_variant_groups(reports).len();
    TrialLedger {
        report_count: reports.len(),
        variant_count: reports.iter().map(|r| r.variants.len()).sum(),
        comparable_variant_count,
        total_variant_report_trials: reports.iter().map(|r| r.variants.len()).sum(),
        windows: reports
            .iter()
            .map(|r| format!("{}..{}", r.start, r.end))
            .collect(),
        report_hashes: reports
            .iter()
            .map(crate::strategy::spec::stable_json_hash)
            .collect(),
        selection_objective:
            "robust_score with hard vetoes, neighbor stability, worst-window pnl, and PBO"
                .to_string(),
    }
}

fn trial_ledger_note(reports: &[ExperimentReport]) -> String {
    let ledger = build_trial_ledger(reports);
    format!(
        "reports={} comparable_variants={} variant_report_trials={} windows={}",
        ledger.report_count,
        ledger.comparable_variant_count,
        ledger.total_variant_report_trials,
        ledger.windows.join(",")
    )
}

fn robust_variant_diagnostics(
    reports: &[ExperimentReport],
    aggregate: &ExperimentReport,
    selected: &VariantReport,
) -> RobustVariantDiagnostics {
    let daily = matching_daily_variants(reports, selected);
    let mut pnls: Vec<f64> = daily.iter().map(|v| v.total_pnl).collect();
    pnls.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let worst_window_pnl = pnls.first().copied().unwrap_or(0.0);
    let median_window_pnl = median_sorted(&pnls);

    let mut expectancies: Vec<f64> = daily
        .iter()
        .map(|v| {
            if v.trades == 0 {
                0.0
            } else {
                v.total_pnl / v.trades as f64
            }
        })
        .collect();
    expectancies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let worst_window_expectancy = expectancies.first().copied().unwrap_or(0.0);
    let median_window_expectancy = median_sorted(&expectancies);

    let (neighbor_count, neighbor_positive_rate) = neighbor_stability(reports, aggregate, selected);
    let wilson = wilson_win_rate_lower(selected.wins, selected.trades);
    let drawdown_inverse = (1.0 - selected.breaker_stressed_drawdown_pct).clamp(0.0, 1.0);
    let trade_pnl = &selected.diagnostics.trade_pnl;
    let simplicity = simplicity_score(selected);
    let robust_score = 0.30 * worst_window_expectancy
        + 0.20 * median_window_expectancy
        + 0.15 * wilson
        + 0.15 * neighbor_positive_rate
        + 0.10 * selected.fill_rate
        + 0.05 * drawdown_inverse
        + 0.05 * simplicity;

    RobustVariantDiagnostics {
        strategy_name: variant_label(selected),
        params_hash: selected.strategy.params_hash.clone(),
        total_pnl: selected.total_pnl,
        trades: selected.trades,
        worst_window_pnl,
        median_window_pnl,
        worst_window_expectancy,
        median_window_expectancy,
        wilson_win_rate_lower: wilson,
        neighbor_count,
        neighbor_positive_rate,
        fill_rate: selected.fill_rate,
        max_stressed_drawdown_pct: selected.breaker_stressed_drawdown_pct,
        profit_factor: trade_pnl.profit_factor,
        payoff_ratio: trade_pnl.payoff_ratio,
        worst_loss_to_avg_win: trade_pnl.worst_loss_to_avg_win,
        robust_score,
    }
}

fn robust_rejection_reasons(
    diagnostic: &RobustVariantDiagnostics,
    gate: &RobustPromotionGate,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if diagnostic.worst_window_pnl < gate.min_worst_window_pnl {
        reasons.push(format!(
            "worst_window_pnl {:.4} below minimum {:.4}",
            diagnostic.worst_window_pnl, gate.min_worst_window_pnl
        ));
    }
    if diagnostic.neighbor_count < gate.min_neighbor_count {
        reasons.push(format!(
            "neighbor_count {} below minimum {}",
            diagnostic.neighbor_count, gate.min_neighbor_count
        ));
    }
    if diagnostic.neighbor_positive_rate < gate.min_neighbor_positive_rate {
        reasons.push(format!(
            "neighbor_positive_rate {:.4} below minimum {:.4}",
            diagnostic.neighbor_positive_rate, gate.min_neighbor_positive_rate
        ));
    }
    if diagnostic.robust_score < gate.min_robust_score {
        reasons.push(format!(
            "robust_score {:.4} below minimum {:.4}",
            diagnostic.robust_score, gate.min_robust_score
        ));
    }
    if gate.min_profit_factor > 0.0 && diagnostic.profit_factor < gate.min_profit_factor {
        reasons.push(format!(
            "profit_factor {:.4} below minimum {:.4}",
            diagnostic.profit_factor, gate.min_profit_factor
        ));
    }
    if gate.min_payoff_ratio > 0.0 && diagnostic.payoff_ratio < gate.min_payoff_ratio {
        reasons.push(format!(
            "payoff_ratio {:.4} below minimum {:.4}",
            diagnostic.payoff_ratio, gate.min_payoff_ratio
        ));
    }
    if gate.max_worst_loss_to_avg_win > 0.0
        && diagnostic.worst_loss_to_avg_win > gate.max_worst_loss_to_avg_win
    {
        reasons.push(format!(
            "worst_loss_to_avg_win {:.4} above maximum {:.4}",
            diagnostic.worst_loss_to_avg_win, gate.max_worst_loss_to_avg_win
        ));
    }
    reasons
}

fn neighbor_stability(
    reports: &[ExperimentReport],
    aggregate: &ExperimentReport,
    selected: &VariantReport,
) -> (usize, f64) {
    let Some(selected_knobs) = VariantKnobs::from_variant(selected) else {
        return (0, 0.0);
    };
    let mut neighbor_count = 0_usize;
    let mut positive = 0_usize;
    let mut total = 0_usize;
    for candidate in &aggregate.variants {
        if candidate.strategy.params_hash == selected.strategy.params_hash {
            continue;
        }
        let Some(candidate_knobs) = VariantKnobs::from_variant(candidate) else {
            continue;
        };
        if !selected_knobs.is_neighbor(&candidate_knobs) {
            continue;
        }
        let daily = matching_daily_variants(reports, candidate);
        if daily.len() != reports.len() {
            continue;
        }
        neighbor_count += 1;
        for variant in daily {
            total += 1;
            if variant.total_pnl > 0.0 {
                positive += 1;
            }
        }
    }
    if total == 0 {
        (neighbor_count, 0.0)
    } else {
        (neighbor_count, positive as f64 / total as f64)
    }
}

#[derive(Debug, Clone, Copy)]
struct VariantKnobs {
    conf: f64,
    z: f64,
    edge: f64,
    min_price: f64,
    max_price: f64,
    prefer_maker: bool,
}

impl VariantKnobs {
    fn from_variant(variant: &VariantReport) -> Option<Self> {
        let params =
            serde_json::from_value::<StrategyVariant>(variant.strategy_params.clone()).ok()?;
        Some(Self {
            conf: params.min_confidence,
            z: params
                .zone_config
                .early_min_z
                .min(params.zone_config.primary_min_z)
                .min(params.zone_config.late_min_z)
                .min(params.zone_config.terminal_min_z),
            edge: params.min_edge,
            min_price: params.zone_config.min_price,
            max_price: params.zone_config.max_price,
            prefer_maker: params.prefer_maker,
        })
    }

    fn is_neighbor(&self, other: &Self) -> bool {
        self.prefer_maker == other.prefer_maker
            && (self.conf - other.conf).abs() <= 0.101
            && (self.z - other.z).abs() <= 0.251
            && (self.edge - other.edge).abs() <= 0.051
            && (self.min_price - other.min_price).abs() <= 0.051
            && (self.max_price - other.max_price).abs() <= 0.201
    }
}

fn simplicity_score(selected: &VariantReport) -> f64 {
    let Some(knobs) = VariantKnobs::from_variant(selected) else {
        return 0.5;
    };
    let strictness = (knobs.conf + knobs.z.min(2.0) / 2.0 + knobs.edge.min(0.10) * 10.0) / 3.0;
    (1.0 - (strictness - 0.5).abs()).clamp(0.0, 1.0)
}

fn estimate_pbo(reports: &[ExperimentReport]) -> PboDiagnostic {
    let groups = comparable_variant_groups(reports);
    let n = reports.len();
    if n < 2 || groups.len() < 2 {
        return PboDiagnostic {
            splits: 0,
            pbo: 0.0,
            median_oos_percentile: 1.0,
            truncated: false,
        };
    }
    let train_size = (n / 2).max(1);
    let max_splits = 4096_usize;
    let mut splits = 0_usize;
    let mut overfit = 0_usize;
    let mut percentiles = Vec::new();
    let mut truncated = false;
    for mask in 1_usize..(1_usize << n) {
        if mask.count_ones() as usize != train_size {
            continue;
        }
        if splits >= max_splits {
            truncated = true;
            break;
        }
        let train_idxs: Vec<usize> = (0..n).filter(|i| (mask & (1_usize << i)) != 0).collect();
        let test_idxs: Vec<usize> = (0..n).filter(|i| (mask & (1_usize << i)) == 0).collect();
        if test_idxs.is_empty() {
            continue;
        }
        let mut train_scores = Vec::with_capacity(groups.len());
        let mut test_scores = Vec::with_capacity(groups.len());
        for group in &groups {
            train_scores.push(sum_group_pnl(group, &train_idxs));
            test_scores.push(sum_group_pnl(group, &test_idxs));
        }
        let Some((winner_idx, _)) = train_scores
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        else {
            continue;
        };
        let winner_oos = test_scores[winner_idx];
        let rank = test_scores
            .iter()
            .filter(|score| **score <= winner_oos)
            .count();
        let percentile = rank as f64 / test_scores.len() as f64;
        if percentile < 0.5 {
            overfit += 1;
        }
        percentiles.push(percentile);
        splits += 1;
    }
    percentiles.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    PboDiagnostic {
        splits,
        pbo: if splits == 0 {
            0.0
        } else {
            overfit as f64 / splits as f64
        },
        median_oos_percentile: median_sorted(&percentiles),
        truncated,
    }
}

fn comparable_variant_groups(reports: &[ExperimentReport]) -> Vec<Vec<&VariantReport>> {
    if reports.is_empty() {
        return Vec::new();
    }
    let mut groups: BTreeMap<String, Vec<&VariantReport>> = BTreeMap::new();
    for report in reports {
        for variant in &report.variants {
            groups
                .entry(variant_key(variant))
                .or_default()
                .push(variant);
        }
    }
    groups
        .into_values()
        .filter(|group| group.len() == reports.len())
        .collect()
}

fn sum_group_pnl(group: &[&VariantReport], indexes: &[usize]) -> f64 {
    indexes
        .iter()
        .filter_map(|idx| group.get(*idx))
        .map(|variant| variant.total_pnl)
        .sum()
}

fn median_sorted(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn variant_label(variant: &VariantReport) -> String {
    variant
        .strategy_params
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or(&variant.strategy.params_hash)
        .to_string()
}

fn daily_sharpe(group: &[&VariantReport]) -> f64 {
    if group.len() < 2 {
        return group.first().map(|v| v.sharpe_like).unwrap_or(0.0);
    }
    let pnls: Vec<f64> = group.iter().map(|v| v.total_pnl).collect();
    let mean = pnls.iter().sum::<f64>() / pnls.len() as f64;
    let variance = pnls
        .iter()
        .map(|p| {
            let d = p - mean;
            d * d
        })
        .sum::<f64>()
        / pnls.len() as f64;
    let std = variance.sqrt();
    if std <= f64::EPSILON {
        0.0
    } else {
        mean / std
    }
}

fn variant_key(variant: &VariantReport) -> String {
    format!(
        "{}:{}:{}",
        variant.strategy.name, variant.strategy.version, variant.strategy.params_hash
    )
}

fn matching_daily_variants<'a>(
    reports: &'a [ExperimentReport],
    selected: &VariantReport,
) -> Vec<&'a VariantReport> {
    let key = variant_key(selected);
    reports
        .iter()
        .filter_map(|report| report.variants.iter().find(|v| variant_key(v) == key))
        .collect()
}

fn multi_report_rejection_reasons(
    reports: &[ExperimentReport],
    selected: &VariantReport,
    gate: &PromotionGate,
    multi_gate: &MultiReportPromotionGate,
) -> Vec<String> {
    let daily = matching_daily_variants(reports, selected);
    let mut reasons = Vec::new();
    if daily.len() < multi_gate.min_reports {
        reasons.push(format!(
            "daily reports {} below minimum {}",
            daily.len(),
            multi_gate.min_reports
        ));
    }
    let profitable = daily.iter().filter(|v| v.total_pnl > 0.0).count();
    if profitable < multi_gate.min_profitable_reports {
        reasons.push(format!(
            "profitable reports {} below minimum {}",
            profitable, multi_gate.min_profitable_reports
        ));
    }
    if multi_gate.min_daily_trades > 0 {
        if let Some(min_trades) = daily.iter().map(|v| v.trades).min() {
            if min_trades < multi_gate.min_daily_trades {
                reasons.push(format!(
                    "daily trades {} below minimum {}",
                    min_trades, multi_gate.min_daily_trades
                ));
            }
        }
    }
    if multi_gate.min_daily_pnl > 0.0 {
        if let Some(worst) = daily
            .iter()
            .map(|v| v.total_pnl)
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        {
            if worst < multi_gate.min_daily_pnl {
                reasons.push(format!(
                    "worst daily pnl {:.4} below minimum {:.4}",
                    worst, multi_gate.min_daily_pnl
                ));
            }
        }
    }
    if multi_gate.max_daily_loss > 0.0 {
        if let Some(worst) = daily
            .iter()
            .map(|v| v.total_pnl)
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        {
            if worst < -multi_gate.max_daily_loss {
                reasons.push(format!(
                    "worst daily pnl {:.4} below loss cap -{:.4}",
                    worst, multi_gate.max_daily_loss
                ));
            }
        }
    }
    if daily
        .iter()
        .any(|v| v.unresolved_fills > gate.max_unresolved_fills)
    {
        reasons.push(format!(
            "one or more daily reports exceed unresolved fill maximum {}",
            gate.max_unresolved_fills
        ));
    }
    reasons
}

fn multi_report_risk_notes(
    reports: &[ExperimentReport],
    selected: &VariantReport,
    multi_gate: &MultiReportPromotionGate,
) -> Vec<String> {
    let daily = matching_daily_variants(reports, selected);
    let profitable = daily.iter().filter(|v| v.total_pnl > 0.0).count();
    let worst = daily
        .iter()
        .map(|v| v.total_pnl)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0);
    vec![format!(
        "multi-report gate: reports={} profitable={} min_profitable={} min_daily_trades={} min_daily_pnl={:.2} worst_daily_pnl={:.2}",
        daily.len(),
        profitable,
        multi_gate.min_profitable_reports,
        multi_gate.min_daily_trades,
        multi_gate.min_daily_pnl,
        worst,
    )]
}

fn promotion_rejection_reasons(selected: &VariantReport, gate: &PromotionGate) -> Vec<String> {
    let mut reasons = Vec::new();
    if selected.strategy_params.is_null() {
        reasons.push("selected variant lacks strategy_params; regenerate the report".to_string());
    }
    if selected.breaker_tripped {
        let reason = selected.breaker_reason.as_deref().unwrap_or("unknown");
        reasons.push(format!("circuit breaker tripped during backtest: {reason}"));
    }
    if selected.diagnostics.adaptive_rearms > 0 {
        reasons.push(format!(
            "adaptive_rearms {} above maximum 0; static promotion requires uninterrupted replay",
            selected.diagnostics.adaptive_rearms
        ));
    }
    if selected.trades < gate.min_trades {
        reasons.push(format!(
            "trades {} below minimum {}",
            selected.trades, gate.min_trades
        ));
    }
    if selected.losses < gate.min_losses {
        reasons.push(format!(
            "losses {} below minimum {}",
            selected.losses, gate.min_losses
        ));
    }
    let zone_count = active_zone_count(selected);
    if zone_count < gate.min_zone_count {
        reasons.push(format!(
            "active zones {} below minimum {}",
            zone_count, gate.min_zone_count
        ));
    }
    if selected.win_rate < gate.min_win_rate {
        reasons.push(format!(
            "win_rate {:.4} below minimum {:.4}",
            selected.win_rate, gate.min_win_rate
        ));
    }
    let wilson_lower = wilson_win_rate_lower(selected.wins, selected.trades);
    if wilson_lower < gate.min_wilson_win_rate_lower {
        reasons.push(format!(
            "wilson_win_rate_lower {:.4} below minimum {:.4}",
            wilson_lower, gate.min_wilson_win_rate_lower
        ));
    }
    if selected.total_pnl < gate.min_total_pnl {
        reasons.push(format!(
            "total_pnl {:.4} below minimum {:.4}",
            selected.total_pnl, gate.min_total_pnl
        ));
    }
    if selected.sharpe_like < gate.min_sharpe_like {
        reasons.push(format!(
            "sharpe_like {:.4} below minimum {:.4}",
            selected.sharpe_like, gate.min_sharpe_like
        ));
    }
    if selected.unresolved_fills > gate.max_unresolved_fills {
        reasons.push(format!(
            "unresolved_fills {} above maximum {}",
            selected.unresolved_fills, gate.max_unresolved_fills
        ));
    }
    let passive_failed = passive_failed_fills(selected);
    let non_passive_failed = selected.fills_failed.saturating_sub(passive_failed);
    if non_passive_failed > gate.max_failed_fills {
        reasons.push(format!(
            "non_passive_fills_failed {} above maximum {} ({:?})",
            non_passive_failed, gate.max_failed_fills, selected.reject_reasons
        ));
    }
    if passive_failed > gate.max_passive_failed_fills {
        reasons.push(format!(
            "passive_fills_failed {} above maximum {} ({:?})",
            passive_failed, gate.max_passive_failed_fills, selected.reject_reasons
        ));
    }
    if selected.execution_attempts > 0 && selected.fill_rate < gate.min_fill_rate {
        reasons.push(format!(
            "fill_rate {:.4} below minimum {:.4}",
            selected.fill_rate, gate.min_fill_rate
        ));
    }
    if selected.trades > 0 && gate.max_zone_trade_share < 1.0 {
        match dominant_zone_share(selected) {
            (Some(zone), Some(share)) if share > gate.max_zone_trade_share => {
                reasons.push(format!(
                    "zone {zone} trade share {:.4} above maximum {:.4}",
                    share, gate.max_zone_trade_share
                ));
            }
            (None, None) => reasons.push("by_zone breakdown is empty".to_string()),
            _ => {}
        }
    }
    reasons
}

fn passive_failed_fills(selected: &VariantReport) -> usize {
    selected
        .reject_reasons
        .iter()
        .filter(|(reason, _)| is_passive_failed_fill_reason(reason))
        .map(|(_, count)| *count)
        .sum()
}

fn is_passive_failed_fill_reason(reason: &str) -> bool {
    matches!(reason, "maker_unfilled" | "post_only_cross")
}

fn active_zone_count(selected: &VariantReport) -> usize {
    selected
        .by_zone
        .values()
        .filter(|report| report.trades > 0)
        .count()
}

fn wilson_win_rate_lower(wins: usize, trades: usize) -> f64 {
    if trades == 0 {
        return 0.0;
    }
    let n = trades as f64;
    let p = wins as f64 / n;
    let z = 1.96;
    let z2 = z * z;
    let denom = 1.0 + z2 / n;
    let centre = p + z2 / (2.0 * n);
    let margin = z * ((p * (1.0 - p) + z2 / (4.0 * n)) / n).sqrt();
    ((centre - margin) / denom).clamp(0.0, 1.0)
}

fn dominant_zone_share(selected: &VariantReport) -> (Option<String>, Option<f64>) {
    let Some((zone, report)) = selected
        .by_zone
        .iter()
        .max_by_key(|(_, report)| report.trades)
    else {
        return (None, None);
    };
    if selected.trades == 0 {
        return (Some(zone.clone()), Some(0.0));
    }
    (
        Some(zone.clone()),
        Some(report.trades as f64 / selected.trades as f64),
    )
}

fn harness_data_manifest(cfg: &HarnessConfig, catalog: &MarketCatalog) -> DataManifest {
    let start = cfg.hours.first().map(|h| h.to_rfc3339());
    let end = cfg.hours.last().map(|h| h.to_rfc3339());

    let mut pmxt = DataSourceManifest::new("pmxt_v2_archive", "order_book_l2");
    pmxt.path = Some(cfg.cache_dir.display().to_string());
    pmxt.start = start.clone();
    pmxt.end = end.clone();
    pmxt.row_count = Some(cfg.hours.len() as u64);
    pmxt.complete = !cfg.hours.is_empty() && catalog.is_complete();
    pmxt.metadata
        .insert("hours".to_string(), cfg.hours.len().to_string());
    pmxt.metadata.insert(
        "market_count".to_string(),
        catalog.market_count().to_string(),
    );
    pmxt.metadata
        .insert("token_count".to_string(), catalog.token_count().to_string());
    pmxt.metadata
        .insert("assets".to_string(), catalog.assets().join(","));
    if let Some(rearm_s) = cfg.adaptive_rearm_after_s {
        pmxt.metadata.insert(
            "adaptive_health_rearm_seconds".to_string(),
            format!("{rearm_s:.3}"),
        );
    }
    if let Some(shared) = &cfg.shared_distilled_dir {
        pmxt.metadata.insert(
            "shared_distilled_dir".to_string(),
            shared.display().to_string(),
        );
    }

    let mut btc = DataSourceManifest::new("btc_price_tape", "external_price");
    btc.start = start;
    btc.end = end;
    btc.row_count = Some(cfg.btc_history.n_ticks() as u64);
    btc.complete = cfg.btc_history.n_ticks() >= 50;
    btc.metadata
        .insert("symbol".to_string(), "BTCUSDT".to_string());

    let mut notes = Vec::new();
    let missing = catalog.missing_required_tokens();
    if !missing.is_empty() {
        notes.push(format!("missing required token ids: {}", missing.join(",")));
    }
    DataManifest::new(vec![pmxt, btc], notes)
}

pub fn read_report(path: impl AsRef<Path>) -> Result<ExperimentReport> {
    let path = path.as_ref();
    let payload = std::fs::read(path)
        .with_context(|| format!("read experiment report {}", path.display()))?;
    serde_json::from_slice(&payload)
        .with_context(|| format!("parse experiment report {}", path.display()))
}

pub fn read_promotion(path: impl AsRef<Path>) -> Result<PromotionArtifact> {
    let path = path.as_ref();
    let payload = std::fs::read(path)
        .with_context(|| format!("read promotion artifact {}", path.display()))?;
    serde_json::from_slice(&payload)
        .with_context(|| format!("parse promotion artifact {}", path.display()))
}

pub fn write_report_atomic(path: impl AsRef<Path>, report: &ExperimentReport) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create report dir {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(report).context("serialize ExperimentReport")?;
    let tmp = path.with_extension(format!(
        "{}.tmp.{}",
        path.extension().and_then(|s| s.to_str()).unwrap_or("json"),
        std::process::id()
    ));
    std::fs::write(&tmp, payload)
        .with_context(|| format!("write tmp experiment report {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

pub fn write_promotion_atomic(path: impl AsRef<Path>, artifact: &PromotionArtifact) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create promotion dir {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(artifact).context("serialize PromotionArtifact")?;
    let tmp = path.with_extension(format!(
        "{}.tmp.{}",
        path.extension().and_then(|s| s.to_str()).unwrap_or("json"),
        std::process::id()
    ));
    std::fs::write(&tmp, payload)
        .with_context(|| format!("write tmp promotion artifact {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::btc_history::BTCHistory;
    use crate::backtest::harness::{CandleUniverse, HarnessConfig};
    use crate::backtest::l2_replay::StaticLatencyConfig;
    use crate::backtest::resolver::BacktestResults;
    use crate::backtest::strategies::StrategyVariant;
    use crate::data::models::{Market, Outcome};
    use crate::data::scanner::CandleContract;

    fn cfg() -> HarnessConfig {
        let mut btc = BTCHistory::default();
        for i in 0..60 {
            btc.timestamps_ms.push(1_700_000_000_000 + i * 1000);
            btc.prices.push(70_000.0 + i as f64);
        }
        HarnessConfig {
            hours: vec![chrono::DateTime::parse_from_rfc3339("2026-05-01T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc)],
            universe: CandleUniverse {
                contracts: vec![CandleContract {
                    market: Market {
                        condition_id: "0xabc".to_string(),
                        question: "Bitcoin Up or Down - test".to_string(),
                        slug: "btc-test".to_string(),
                        outcomes: vec![
                            Outcome {
                                token_id: "up-token".to_string(),
                                name: "Up".to_string(),
                                price: 0.5,
                            },
                            Outcome {
                                token_id: "down-token".to_string(),
                                name: "Down".to_string(),
                                price: 0.5,
                            },
                        ],
                        tags: Vec::new(),
                        category: String::new(),
                        active: true,
                        closed: false,
                        volume: 1000.0,
                        liquidity: 500.0,
                        end_date: "2026-05-01T00:05:00Z".to_string(),
                        event_slug: String::new(),
                        event_id: String::new(),
                        event_title: String::new(),
                        group_slug: String::new(),
                        neg_risk: false,
                        neg_risk_augmented: false,
                        minimum_tick_size: None,
                        fees_enabled: None,
                        taker_fee_rate: None,
                        maker_fee_rate: None,
                    },
                    up_token_id: "up-token".to_string(),
                    down_token_id: "down-token".to_string(),
                    up_price: 0.5,
                    down_price: 0.5,
                    end_date: "2026-05-01T00:05:00Z".to_string(),
                    hours_left: 0.0,
                    volume: 1000.0,
                    liquidity: 500.0,
                    window_description: "test".to_string(),
                    asset: "BTC".to_string(),
                }],
            },
            btc_history: std::sync::Arc::new(btc),
            bankroll_usd: 100.0,
            max_total_exposure_usd: 80.0,
            min_order_size_shares: 0.0,
            cache_dir: std::path::PathBuf::from("/tmp/pmxt"),
            latency: StaticLatencyConfig { insert_ms: 50 },
            breaker_cfg: crate::live::breaker::BreakerConfig::default(),
            adaptive_rearm_after_s: None,
            shared_distilled_dir: None,
            threads: Some(1),
            checkpoint_dir: None,
            stop_flag: None,
            continuous: false,
            delete_downloaded_parquet_after_hour: false,
        }
    }

    fn zone_split(primary: u64, terminal: u64) -> BTreeMap<String, ZoneReport> {
        BTreeMap::from([
            (
                "primary".to_string(),
                ZoneReport {
                    trades: primary,
                    wins: primary / 2,
                    losses: primary - primary / 2,
                    win_rate: 0.5,
                    pnl: primary as f64 * 0.01,
                },
            ),
            (
                "terminal".to_string(),
                ZoneReport {
                    trades: terminal,
                    wins: terminal / 2,
                    losses: terminal - terminal / 2,
                    win_rate: 0.5,
                    pnl: terminal as f64 * 0.01,
                },
            ),
        ])
    }

    fn robust_variant(
        name: &str,
        _hash: &str,
        pnl: f64,
        conf: f64,
        z: f64,
        max_price: f64,
    ) -> VariantReport {
        let mut variant = StrategyVariant::baseline();
        variant.name = name.to_string();
        variant.prefer_maker = true;
        variant.min_confidence = conf;
        variant.min_edge = 0.03;
        variant.zone_config.early_min_confidence = conf;
        variant.zone_config.early_min_z = z;
        variant.zone_config.primary_min_z = z;
        variant.zone_config.late_min_z = z;
        variant.zone_config.terminal_min_z = z;
        variant.zone_config.min_price = 0.10;
        variant.zone_config.max_price = max_price;
        let strategy =
            StrategySpec::from_serializable_params("s", "1", &variant, variant.risk_profile());
        VariantReport {
            strategy,
            strategy_params: serde_json::to_value(&variant).unwrap(),
            trades: 30,
            wins: 22,
            losses: 8,
            unresolved_fills: 0,
            execution_attempts: 45,
            fills_success: 30,
            fills_failed: 15,
            fill_rate: 30.0 / 45.0,
            reject_reasons: BTreeMap::from([("maker_unfilled".to_string(), 15)]),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: BacktestDiagnostics::default(),
            win_rate: 22.0 / 30.0,
            total_pnl: pnl,
            avg_pnl: pnl / 30.0,
            total_fees: 0.0,
            sharpe_like: 0.1,
            by_zone: zone_split(30, 0),
        }
    }

    #[test]
    fn report_contains_manifest_and_sorted_variant() {
        let cfg = cfg();
        let runs = vec![HarnessRun {
            variant: StrategyVariant::baseline(),
            results: BacktestResults::default(),
        }];
        let report = ExperimentReport::from_harness("test", &cfg, &runs);
        assert_eq!(report.mode, "backtest");
        assert_eq!(report.market_catalog.market_count(), 1);
        assert!(report.data_manifest.complete);
        assert_eq!(report.variants.len(), 1);
        assert_eq!(report.variants[0].strategy.name, "candle_momentum");
    }

    #[test]
    fn promotion_selects_best_passing_variant() {
        let cfg = cfg();
        let mut worse = VariantReport::from_run(&HarnessRun {
            variant: StrategyVariant::baseline(),
            results: BacktestResults::default(),
        });
        worse.trades = 30;
        worse.wins = 12;
        worse.losses = 18;
        worse.win_rate = 0.4;
        worse.total_pnl = 1.0;
        worse.avg_pnl = 0.03;
        worse.by_zone = zone_split(16, 14);
        let mut better = worse.clone();
        better.strategy.risk_profile = "better".to_string();
        better.wins = 20;
        better.losses = 10;
        better.win_rate = 20.0 / 30.0;
        better.total_pnl = 2.0;
        better.sharpe_like = 1.0;
        let mut overfit = better.clone();
        overfit.strategy.risk_profile = "overfit".to_string();
        overfit.total_pnl = 3.0;
        overfit.by_zone = zone_split(30, 0);
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants = vec![worse, overfit, better];

        let artifact = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap();

        assert_eq!(artifact.selected_strategy.risk_profile, "better");
        assert_eq!(artifact.trades, 30);
        assert_eq!(
            artifact.data_manifest_hash,
            report.data_manifest.manifest_hash
        );
        assert_eq!(artifact.dominant_zone.as_deref(), Some("primary"));
        assert!((artifact.dominant_zone_trade_share.unwrap() - (16.0 / 30.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn promotion_rejects_incomplete_data() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.data_manifest.complete = false;
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 30,
            wins: 20,
            losses: 10,
            unresolved_fills: 0,
            execution_attempts: 0,
            fills_success: 0,
            fills_failed: 0,
            fill_rate: 0.0,
            reject_reasons: BTreeMap::new(),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: BacktestDiagnostics::default(),
            win_rate: 0.66,
            total_pnl: 1.0,
            avg_pnl: 0.03,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: zone_split(15, 15),
        });

        let err = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap_err();

        assert!(err.to_string().contains("data manifest is incomplete"));
    }

    #[test]
    fn promotion_rejects_gate_failures() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 5,
            wins: 3,
            losses: 2,
            unresolved_fills: 0,
            execution_attempts: 0,
            fills_success: 0,
            fills_failed: 0,
            fill_rate: 0.0,
            reject_reasons: BTreeMap::new(),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: BacktestDiagnostics::default(),
            win_rate: 0.60,
            total_pnl: 1.0,
            avg_pnl: 0.20,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: zone_split(3, 2),
        });

        let err = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap_err();

        assert!(err.to_string().contains("trades 5 below minimum"));
    }

    #[test]
    fn promotion_rejects_backtest_breaker_trip() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 30,
            wins: 20,
            losses: 10,
            unresolved_fills: 0,
            execution_attempts: 0,
            fills_success: 0,
            fills_failed: 0,
            fill_rate: 0.0,
            reject_reasons: BTreeMap::new(),
            breaker_tripped: true,
            breaker_reason: Some("win_rate_low".to_string()),
            breaker_tripped_at_s: Some(1_700_000_000.0),
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: BacktestDiagnostics::default(),
            win_rate: 0.66,
            total_pnl: 1.0,
            avg_pnl: 0.03,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: zone_split(15, 15),
        });

        let err = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap_err();

        assert!(err
            .to_string()
            .contains("circuit breaker tripped during backtest"));
    }

    #[test]
    fn promotion_rejects_adaptive_rearm_diagnostics() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        let diagnostics = BacktestDiagnostics {
            adaptive_rearms: 1,
            ..Default::default()
        };
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 30,
            wins: 20,
            losses: 10,
            unresolved_fills: 0,
            execution_attempts: 30,
            fills_success: 30,
            fills_failed: 0,
            fill_rate: 1.0,
            reject_reasons: BTreeMap::new(),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics,
            win_rate: 20.0 / 30.0,
            total_pnl: 2.0,
            avg_pnl: 2.0 / 30.0,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: zone_split(15, 15),
        });

        let err = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap_err();

        assert!(err
            .to_string()
            .contains("adaptive_rearms 1 above maximum 0"));
    }

    #[test]
    fn promotion_rejects_unresolved_fills_by_default() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 30,
            wins: 20,
            losses: 10,
            unresolved_fills: 1,
            execution_attempts: 1,
            fills_success: 0,
            fills_failed: 1,
            fill_rate: 0.0,
            reject_reasons: BTreeMap::from([("maker_unfilled".to_string(), 1)]),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: BacktestDiagnostics::default(),
            win_rate: 0.66,
            total_pnl: 1.0,
            avg_pnl: 0.03,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: zone_split(15, 15),
        });

        let err = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap_err();

        assert!(err
            .to_string()
            .contains("unresolved_fills 1 above maximum 0"));
    }

    #[test]
    fn promotion_distinguishes_passive_maker_nonfills_from_execution_failures() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 30,
            wins: 22,
            losses: 8,
            unresolved_fills: 0,
            execution_attempts: 40,
            fills_success: 30,
            fills_failed: 10,
            fill_rate: 0.75,
            reject_reasons: BTreeMap::from([
                ("maker_unfilled".to_string(), 7),
                ("post_only_cross".to_string(), 3),
            ]),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: BacktestDiagnostics::default(),
            win_rate: 22.0 / 30.0,
            total_pnl: 10.0,
            avg_pnl: 10.0 / 30.0,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: zone_split(15, 15),
        });

        let artifact = PromotionArtifact::from_report(
            &report,
            PromotionGate {
                max_passive_failed_fills: 10,
                min_fill_rate: 0.70,
                ..PromotionGate::default()
            },
        )
        .unwrap();

        assert_eq!(artifact.trades, 30);
        assert!(artifact
            .risk_notes
            .iter()
            .any(|note| note.contains("10 passive maker")));
    }

    #[test]
    fn promotion_rejects_non_passive_execution_failures() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 30,
            wins: 22,
            losses: 8,
            unresolved_fills: 0,
            execution_attempts: 31,
            fills_success: 30,
            fills_failed: 1,
            fill_rate: 30.0 / 31.0,
            reject_reasons: BTreeMap::from([("invalid book".to_string(), 1)]),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: BacktestDiagnostics::default(),
            win_rate: 22.0 / 30.0,
            total_pnl: 10.0,
            avg_pnl: 10.0 / 30.0,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: zone_split(15, 15),
        });

        let err = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap_err();

        assert!(err
            .to_string()
            .contains("non_passive_fills_failed 1 above maximum 0"));
    }

    #[test]
    fn promotion_rejects_zone_concentration() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 30,
            wins: 20,
            losses: 10,
            unresolved_fills: 0,
            execution_attempts: 0,
            fills_success: 0,
            fills_failed: 0,
            fill_rate: 0.0,
            reject_reasons: BTreeMap::new(),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: BacktestDiagnostics::default(),
            win_rate: 0.66,
            total_pnl: 1.0,
            avg_pnl: 0.03,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: zone_split(29, 1),
        });

        let err = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap_err();

        assert!(err.to_string().contains("zone primary trade share"));
    }

    #[test]
    fn promotion_rejects_lossless_tiny_sample_by_default() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 30,
            wins: 30,
            losses: 0,
            unresolved_fills: 0,
            execution_attempts: 0,
            fills_success: 0,
            fills_failed: 0,
            fill_rate: 0.0,
            reject_reasons: BTreeMap::new(),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: BacktestDiagnostics::default(),
            win_rate: 1.0,
            total_pnl: 1.0,
            avg_pnl: 0.03,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: zone_split(15, 15),
        });

        let err = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap_err();

        assert!(err.to_string().contains("losses 0 below minimum 1"));
    }

    #[test]
    fn promotion_rejects_low_wilson_bound_when_requested() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 30,
            wins: 20,
            losses: 10,
            unresolved_fills: 0,
            execution_attempts: 0,
            fills_success: 0,
            fills_failed: 0,
            fill_rate: 0.0,
            reject_reasons: BTreeMap::new(),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: BacktestDiagnostics::default(),
            win_rate: 20.0 / 30.0,
            total_pnl: 1.0,
            avg_pnl: 0.03,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: zone_split(15, 15),
        });

        let err = PromotionArtifact::from_report(
            &report,
            PromotionGate {
                min_wilson_win_rate_lower: 0.50,
                ..PromotionGate::default()
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("wilson_win_rate_lower"));
    }

    #[test]
    fn aggregate_promotion_selects_consistent_profitable_variant() {
        let cfg = cfg();
        let mut reports = Vec::new();
        for (i, consistent_pnl) in [5.0, 4.0, 3.0].into_iter().enumerate() {
            let mut report = ExperimentReport::from_harness(format!("day{i}"), &cfg, &[]);
            report.variants.push(VariantReport {
                strategy: StrategySpec::new("s", "1", "consistent", "consistent"),
                strategy_params: serde_json::json!({"name": "consistent"}),
                trades: 30,
                wins: 20,
                losses: 10,
                unresolved_fills: 0,
                execution_attempts: 0,
                fills_success: 0,
                fills_failed: 0,
                fill_rate: 0.0,
                reject_reasons: BTreeMap::new(),
                breaker_tripped: false,
                breaker_reason: None,
                breaker_tripped_at_s: None,
                breaker_realized_drawdown_pct: 0.0,
                breaker_stressed_drawdown_pct: 0.0,
                diagnostics: BacktestDiagnostics::default(),
                win_rate: 20.0 / 30.0,
                total_pnl: consistent_pnl,
                avg_pnl: consistent_pnl / 30.0,
                total_fees: 0.0,
                sharpe_like: 0.1,
                by_zone: zone_split(16, 14),
            });
            report.variants.push(VariantReport {
                strategy: StrategySpec::new("s", "1", "lucky", "lucky"),
                strategy_params: serde_json::json!({"name": "lucky"}),
                trades: 30,
                wins: 20,
                losses: 10,
                unresolved_fills: 0,
                execution_attempts: 0,
                fills_success: 0,
                fills_failed: 0,
                fill_rate: 0.0,
                reject_reasons: BTreeMap::new(),
                breaker_tripped: false,
                breaker_reason: None,
                breaker_tripped_at_s: None,
                breaker_realized_drawdown_pct: 0.0,
                breaker_stressed_drawdown_pct: 0.0,
                diagnostics: BacktestDiagnostics::default(),
                win_rate: 20.0 / 30.0,
                total_pnl: if i == 0 { 100.0 } else { -10.0 },
                avg_pnl: 0.0,
                total_fees: 0.0,
                sharpe_like: 0.1,
                by_zone: zone_split(16, 14),
            });
            reports.push(report);
        }

        let artifact = PromotionArtifact::from_reports(
            &reports,
            PromotionGate {
                min_trades: 90,
                ..PromotionGate::default()
            },
            MultiReportPromotionGate {
                min_reports: 3,
                min_profitable_reports: 2,
                min_daily_trades: 30,
                min_daily_pnl: 0.0,
                max_daily_loss: 0.0,
            },
        )
        .unwrap();

        assert_eq!(artifact.selected_strategy.params_hash, "consistent");
        assert_eq!(artifact.trades, 90);
        assert_eq!(artifact.total_pnl, 12.0);
    }

    #[test]
    fn aggregate_promotion_respects_min_daily_pnl() {
        let cfg = cfg();
        let mut reports = Vec::new();
        for (fragile_pnl, robust_pnl) in [(100.0, 20.0), (100.0, 20.0), (1.0, 20.0)] {
            let mut report = ExperimentReport::from_harness("day", &cfg, &[]);
            report.variants.push(VariantReport {
                strategy: StrategySpec::new("s", "1", "fragile", "fragile"),
                strategy_params: serde_json::json!({"name": "fragile"}),
                trades: 40,
                wins: 28,
                losses: 12,
                unresolved_fills: 0,
                execution_attempts: 0,
                fills_success: 0,
                fills_failed: 0,
                fill_rate: 0.0,
                reject_reasons: BTreeMap::new(),
                breaker_tripped: false,
                breaker_reason: None,
                breaker_tripped_at_s: None,
                breaker_realized_drawdown_pct: 0.0,
                breaker_stressed_drawdown_pct: 0.0,
                diagnostics: BacktestDiagnostics::default(),
                win_rate: 0.7,
                total_pnl: fragile_pnl,
                avg_pnl: fragile_pnl / 40.0,
                total_fees: 0.0,
                sharpe_like: 0.1,
                by_zone: zone_split(20, 20),
            });
            report.variants.push(VariantReport {
                strategy: StrategySpec::new("s", "1", "robust", "robust"),
                strategy_params: serde_json::json!({"name": "robust"}),
                trades: 40,
                wins: 28,
                losses: 12,
                unresolved_fills: 0,
                execution_attempts: 0,
                fills_success: 0,
                fills_failed: 0,
                fill_rate: 0.0,
                reject_reasons: BTreeMap::new(),
                breaker_tripped: false,
                breaker_reason: None,
                breaker_tripped_at_s: None,
                breaker_realized_drawdown_pct: 0.0,
                breaker_stressed_drawdown_pct: 0.0,
                diagnostics: BacktestDiagnostics::default(),
                win_rate: 0.7,
                total_pnl: robust_pnl,
                avg_pnl: robust_pnl / 40.0,
                total_fees: 0.0,
                sharpe_like: 0.1,
                by_zone: zone_split(20, 20),
            });
            reports.push(report);
        }

        let artifact = PromotionArtifact::from_reports(
            &reports,
            PromotionGate {
                min_trades: 120,
                ..PromotionGate::default()
            },
            MultiReportPromotionGate {
                min_reports: 3,
                min_profitable_reports: 3,
                min_daily_trades: 40,
                min_daily_pnl: 10.0,
                max_daily_loss: 0.0,
            },
        )
        .unwrap();

        assert_eq!(
            artifact
                .strategy_params
                .get("name")
                .and_then(|v| v.as_str()),
            Some("robust")
        );
        assert_eq!(artifact.total_pnl, 60.0);
    }

    #[test]
    fn robust_promotion_prefers_plateau_over_spike() {
        let cfg = cfg();
        let mut reports = Vec::new();
        for (i, (robust_pnl, spike_pnl)) in [(10.0, 80.0), (10.0, 1.0), (10.0, 1.0)]
            .into_iter()
            .enumerate()
        {
            let mut report = ExperimentReport::from_harness(format!("day{i}"), &cfg, &[]);
            report.variants = vec![
                robust_variant("spike", "spike", spike_pnl, 0.80, 1.50, 0.90),
                robust_variant("robust", "robust", robust_pnl, 0.40, 0.70, 0.90),
                robust_variant(
                    "robust_neighbor_conf",
                    "robust_neighbor_conf",
                    8.0,
                    0.35,
                    0.70,
                    0.90,
                ),
                robust_variant(
                    "robust_neighbor_z",
                    "robust_neighbor_z",
                    7.0,
                    0.40,
                    0.50,
                    0.90,
                ),
            ];
            reports.push(report);
        }

        let (artifact, diagnostics) = PromotionArtifact::from_reports_robust(
            &reports,
            PromotionGate {
                min_trades: 90,
                min_zone_count: 1,
                max_zone_trade_share: 1.0,
                max_passive_failed_fills: 200,
                min_fill_rate: 0.50,
                ..PromotionGate::default()
            },
            MultiReportPromotionGate {
                min_reports: 3,
                min_profitable_reports: 3,
                min_daily_trades: 30,
                min_daily_pnl: 0.0,
                max_daily_loss: 0.0,
            },
            RobustPromotionGate {
                min_neighbor_count: 2,
                min_neighbor_positive_rate: 1.0,
                max_pbo: 1.0,
                min_worst_window_pnl: 5.0,
                min_robust_score: 0.0,
                min_profit_factor: 0.0,
                min_payoff_ratio: 0.0,
                max_worst_loss_to_avg_win: 0.0,
            },
        )
        .unwrap();

        assert_eq!(
            artifact
                .strategy_params
                .get("name")
                .and_then(|v| v.as_str()),
            Some("robust")
        );
        assert_eq!(diagnostics.selected.neighbor_count, 2);
        assert_eq!(diagnostics.selected.worst_window_pnl, 10.0);
        assert_eq!(
            artifact
                .robust_diagnostics
                .as_ref()
                .map(|d| d.selected.strategy_name.as_str()),
            Some("robust")
        );
        assert!(artifact
            .risk_notes
            .iter()
            .any(|note| note.contains("PBO estimate")));
    }

    #[test]
    fn robust_rejection_reasons_include_loss_asymmetry_gates() {
        let diagnostic = RobustVariantDiagnostics {
            strategy_name: "candidate".to_string(),
            params_hash: "hash".to_string(),
            total_pnl: 10.0,
            trades: 50,
            worst_window_pnl: 1.0,
            median_window_pnl: 5.0,
            worst_window_expectancy: 0.02,
            median_window_expectancy: 0.10,
            wilson_win_rate_lower: 0.70,
            neighbor_count: 3,
            neighbor_positive_rate: 0.80,
            fill_rate: 0.70,
            max_stressed_drawdown_pct: 0.05,
            profit_factor: 1.10,
            payoff_ratio: 0.10,
            worst_loss_to_avg_win: 8.0,
            robust_score: 0.40,
        };

        let reasons = robust_rejection_reasons(
            &diagnostic,
            &RobustPromotionGate {
                min_profit_factor: 1.20,
                min_payoff_ratio: 0.20,
                max_worst_loss_to_avg_win: 6.0,
                ..RobustPromotionGate::default()
            },
        );

        assert!(reasons
            .iter()
            .any(|reason| reason.contains("profit_factor")));
        assert!(reasons.iter().any(|reason| reason.contains("payoff_ratio")));
        assert!(reasons
            .iter()
            .any(|reason| reason.contains("worst_loss_to_avg_win")));
    }
}
