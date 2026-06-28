//! Strategy-builder orchestration and audit helpers.
//!
//! This module does not invent a new research engine. It makes the existing
//! stages explicit and reproducible: one-pass PMXT eval-cache scouting, cached
//! PMXT harness sweep, aggregate promotion, cached live-replay parity, and
//! session diagnostics.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;

use crate::backtest::experiment::{self, PromotionArtifact};
use crate::backtest::resolver::TradePnlDiagnostics;
use crate::backtest::strategies::StrategyVariant;
use crate::monitoring::{causality, diagnostics};
use crate::strategy::spec::stable_json_hash;

#[derive(Debug, Clone)]
pub struct StrategyBuilderPlanInput {
    pub start: String,
    pub end: Option<String>,
    pub out_dir: PathBuf,
    pub cache_dir: Option<String>,
    pub btc_csv: Option<String>,
    pub bankroll: f64,
    pub latency_ms: u64,
    pub threads: usize,
    pub window_minutes: f64,
    pub fold_hours: i64,
    pub profile: String,
    pub zone_mode: String,
    pub promotion_output: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderAuditInput {
    pub report_paths: Vec<String>,
    pub adaptive_report_paths: Vec<String>,
    pub promotion_artifact: Option<String>,
    pub replay_sessions: Vec<String>,
    pub min_trades: usize,
    pub min_win_rate: f64,
    pub min_wilson_win_rate_lower: f64,
    pub min_total_pnl: f64,
    pub min_shadow_resolutions: u64,
    pub min_research_reports: usize,
    pub min_replay_sessions: usize,
    pub a_plus_min_shadow_resolutions: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderPlan {
    pub schema_version: u32,
    pub profile: String,
    pub start: String,
    pub end: String,
    pub out_dir: String,
    pub window_minutes: f64,
    pub fold_hours: i64,
    pub zone_mode: String,
    pub stages: Vec<StrategyBuilderStage>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderStage {
    pub name: String,
    pub purpose: String,
    pub command: String,
    pub outputs: Vec<String>,
    pub verify: Vec<String>,
    pub resource_policy: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderAudit {
    pub schema_version: u32,
    pub ok: bool,
    pub a_plus_ready: bool,
    pub grade: String,
    pub checks: Vec<StrategyBuilderCheck>,
    pub next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderCheck {
    pub name: String,
    pub status: StrategyBuilderCheckStatus,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderSelectivitySearchInput {
    pub report_paths: Vec<String>,
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub top: usize,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderMultiGuardSearchInput {
    pub report_paths: Vec<String>,
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub max_rules: usize,
    pub min_guard_trades: u64,
    pub min_guard_loss_pnl: f64,
    pub min_guard_loss_reports: usize,
    pub recent_report_lookback: usize,
    pub pattern_guards: bool,
    pub top: usize,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderAdaptiveDirectionInput {
    pub report_paths: Vec<String>,
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub top: usize,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderAdaptiveModeInput {
    pub report_paths: Vec<String>,
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub max_guard_rules: usize,
    pub min_guard_trades: u64,
    pub min_guard_loss_pnl: f64,
    pub min_guard_loss_reports: usize,
    pub recent_report_lookback: usize,
    pub pattern_guards: bool,
    pub flat_if_worst_train_below: f64,
    pub top: usize,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderCausalPolicySearchInput {
    pub report_paths: Vec<String>,
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub max_require_terms: usize,
    pub max_deny_rules: usize,
    pub max_deny_terms: usize,
    pub min_deny_trades: u64,
    pub min_deny_loss_pnl: f64,
    pub min_deny_loss_reports: usize,
    pub top: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderSelectivitySearch {
    pub schema_version: u32,
    pub ok: bool,
    pub report_count: usize,
    pub candidate_count: usize,
    pub methodology: Vec<String>,
    pub gates: SelectivitySearchGates,
    pub candidates: Vec<SelectivityCandidateReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderMultiGuardSearch {
    pub schema_version: u32,
    pub ok: bool,
    pub report_count: usize,
    pub candidate_count: usize,
    pub methodology: Vec<String>,
    pub gates: MultiGuardSearchGates,
    pub candidates: Vec<MultiGuardCandidateReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderAdaptiveDirectionSearch {
    pub schema_version: u32,
    pub ok: bool,
    pub report_count: usize,
    pub candidate_count: usize,
    pub methodology: Vec<String>,
    pub gates: SelectivitySearchGates,
    pub candidates: Vec<AdaptiveDirectionCandidateReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderAdaptiveModeSearch {
    pub schema_version: u32,
    pub ok: bool,
    pub report_count: usize,
    pub candidate_count: usize,
    pub methodology: Vec<String>,
    pub gates: AdaptiveModeSearchGates,
    pub candidates: Vec<AdaptiveModeCandidateReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderCausalPolicySearch {
    pub schema_version: u32,
    pub ok: bool,
    pub report_count: usize,
    pub candidate_count: usize,
    pub methodology: Vec<String>,
    pub gates: CausalPolicySearchGates,
    pub candidates: Vec<CausalPolicyCandidateReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectivitySearchGates {
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_worst_oos_pnl: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MultiGuardSearchGates {
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub max_rules: usize,
    pub min_guard_trades: u64,
    pub min_guard_loss_pnl: f64,
    pub min_guard_loss_reports: usize,
    pub recent_report_lookback: usize,
    pub pattern_guards: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveModeSearchGates {
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub max_guard_rules: usize,
    pub min_guard_trades: u64,
    pub min_guard_loss_pnl: f64,
    pub min_guard_loss_reports: usize,
    pub recent_report_lookback: usize,
    pub pattern_guards: bool,
    pub flat_if_worst_train_below: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CausalPolicySearchGates {
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub max_require_terms: usize,
    pub max_deny_rules: usize,
    pub max_deny_terms: usize,
    pub min_deny_trades: u64,
    pub min_deny_loss_pnl: f64,
    pub min_deny_loss_reports: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectivityCandidateReport {
    pub rank: usize,
    pub passed: bool,
    pub variant: String,
    pub rule: SelectivityRule,
    pub aggregate: SelectivityStatsReport,
    pub fold_forward: SelectivityFoldForwardReport,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MultiGuardCandidateReport {
    pub rank: usize,
    pub passed: bool,
    pub variant: String,
    pub final_guard: MultiGuardPolicyReport,
    pub aggregate_static_final_guard: SelectivityStatsReport,
    pub fold_forward: MultiGuardFoldForwardReport,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveDirectionCandidateReport {
    pub rank: usize,
    pub passed: bool,
    pub variant: String,
    pub fold_forward: AdaptiveDirectionFoldForwardReport,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveModeCandidateReport {
    pub rank: usize,
    pub passed: bool,
    pub variant: String,
    pub fold_forward: AdaptiveModeFoldForwardReport,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CausalPolicyCandidateReport {
    pub rank: usize,
    pub passed: bool,
    pub variant: String,
    pub base_require: BTreeMap<String, String>,
    pub final_policy: CausalPolicyReport,
    pub aggregate_static_final_policy: SelectivityStatsReport,
    pub fold_forward: CausalPolicyFoldForwardReport,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectivityStatsReport {
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub win_rate: f64,
    pub wilson_win_rate_lower: f64,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub gross_win_pnl: f64,
    pub gross_loss_pnl: f64,
    pub avg_win_pnl: f64,
    pub avg_loss_pnl: f64,
    pub max_win_pnl: f64,
    pub max_loss_pnl: f64,
    pub profit_factor: f64,
    pub payoff_ratio: f64,
    pub worst_loss_to_avg_win: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MultiGuardFoldForwardReport {
    pub eligible_reports: usize,
    pub profitable_reports: usize,
    pub losing_reports: usize,
    pub abstained_reports: usize,
    pub worst_report_pnl: f64,
    pub stats: SelectivityStatsReport,
    pub decisions: Vec<MultiGuardDecisionReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MultiGuardDecisionReport {
    pub report_index: usize,
    pub train_reports: usize,
    pub recent_losing_reports: usize,
    pub recent_worst_report_pnl: f64,
    pub guard: MultiGuardPolicyReport,
    pub train: Option<SelectivityStatsReport>,
    pub oos: Option<SelectivityStatsReport>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MultiGuardPolicyReport {
    pub deny_regimes: Vec<MultiGuardRuleReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MultiGuardRuleReport {
    pub regime: String,
    pub match_tags: BTreeMap<String, String>,
    pub train_reports_with_trades: usize,
    pub train_stats: SelectivityStatsReport,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectivityFoldForwardReport {
    pub eligible_reports: usize,
    pub profitable_reports: usize,
    pub losing_reports: usize,
    pub worst_report_pnl: f64,
    pub stats: SelectivityStatsReport,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveDirectionFoldForwardReport {
    pub eligible_reports: usize,
    pub profitable_reports: usize,
    pub losing_reports: usize,
    pub abstained_reports: usize,
    pub worst_report_pnl: f64,
    pub stats: SelectivityStatsReport,
    pub decisions: Vec<AdaptiveDirectionDecisionReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveDirectionDecisionReport {
    pub report_index: usize,
    pub train_reports: usize,
    pub selected_direction: Option<String>,
    pub train: Option<SelectivityStatsReport>,
    pub oos: Option<SelectivityStatsReport>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveModeFoldForwardReport {
    pub eligible_reports: usize,
    pub profitable_reports: usize,
    pub losing_reports: usize,
    pub abstained_reports: usize,
    pub worst_report_pnl: f64,
    pub stats: SelectivityStatsReport,
    pub decisions: Vec<AdaptiveModeDecisionReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveModeDecisionReport {
    pub report_index: usize,
    pub train_reports: usize,
    pub recent_losing_reports: usize,
    pub recent_worst_report_pnl: f64,
    pub selected_mode: AdaptiveModeKind,
    pub selected_direction: Option<String>,
    pub guard: MultiGuardPolicyReport,
    pub train: Option<SelectivityStatsReport>,
    pub train_summary: Option<AdaptiveModeTrainSummaryReport>,
    pub oos: Option<SelectivityStatsReport>,
    pub active_options: Vec<AdaptiveModeOptionReport>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveModeOptionReport {
    pub mode: AdaptiveModeKind,
    pub direction: Option<String>,
    pub guard: MultiGuardPolicyReport,
    pub train: SelectivityStatsReport,
    pub train_summary: AdaptiveModeTrainSummaryReport,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveModeTrainSummaryReport {
    pub eligible_reports: usize,
    pub profitable_reports: usize,
    pub losing_reports: usize,
    pub worst_report_pnl: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CausalPolicyFoldForwardReport {
    pub eligible_reports: usize,
    pub profitable_reports: usize,
    pub losing_reports: usize,
    pub abstained_reports: usize,
    pub worst_report_pnl: f64,
    pub stats: SelectivityStatsReport,
    pub decisions: Vec<CausalPolicyDecisionReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CausalPolicyDecisionReport {
    pub report_index: usize,
    pub train_reports: usize,
    pub policy: CausalPolicyReport,
    pub train: Option<SelectivityStatsReport>,
    pub oos: Option<SelectivityStatsReport>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CausalPolicyReport {
    pub require_tags: BTreeMap<String, String>,
    pub deny_rules: Vec<CausalPolicyRuleReport>,
    pub harness_require_args: Vec<String>,
    pub harness_deny_args: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CausalPolicyRuleReport {
    pub label: String,
    pub match_tags: BTreeMap<String, String>,
    pub train_reports_with_trades: usize,
    pub train_stats: SelectivityStatsReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveModeKind {
    Flat,
    Direction,
    Guarded,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct SelectivityRule {
    pub dimension: String,
    pub value: String,
    pub action: SelectivityAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectivityAction {
    AllowOnly,
    Deny,
}

#[derive(Debug, Clone)]
struct SelectivityFold {
    variants: Vec<SelectivityVariantFold>,
}

#[derive(Debug, Clone)]
struct SelectivityVariantFold {
    name: String,
    buckets: BTreeMap<String, TradePnlDiagnostics>,
    regimes: BTreeMap<String, TradePnlDiagnostics>,
    tagged_regimes: Vec<TaggedRegimeStats>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SelectivityCandidateKey {
    variant: String,
    rule: SelectivityRule,
}

#[derive(Debug, Clone)]
struct TaggedRegimeStats {
    tags: BTreeMap<String, String>,
    stats: TradePnlDiagnostics,
}

#[derive(Debug, Clone)]
struct CalibrationReportPaths {
    static_report: PathBuf,
    adaptive_report: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyBuilderCheckStatus {
    Ok,
    Warn,
    Fail,
}

pub fn build_plan(input: StrategyBuilderPlanInput) -> Result<StrategyBuilderPlan> {
    let start = parse_rfc3339(&input.start, "--start")?;
    let end = match &input.end {
        Some(end) => parse_rfc3339(end, "--end")?,
        None => start,
    };
    if end < start {
        bail!("--end must be >= --start");
    }
    if input.window_minutes <= 0.0 {
        bail!("--window-minutes must be > 0");
    }
    if input.fold_hours <= 0 {
        bail!("--fold-hours must be > 0");
    }
    let zone_mode = parse_zone_mode(&input.zone_mode)?;

    let out_dir = input.out_dir;
    let eval_cache_dir = out_dir.join("eval_cache");
    let scout_reports_dir = out_dir.join("scout_reports");
    let reports_dir = out_dir.join("reports");
    let checkpoint_dir = out_dir.join("checkpoints");
    let holdout_replay_dir = out_dir.join("holdout_live_replay_sessions");
    let holdout_replay_reports_dir = out_dir.join("holdout_live_replay_reports");
    let promotion_output = input
        .promotion_output
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            out_dir.join(format!(
                "promotion_{}_{}.json",
                compact_stamp(start),
                compact_stamp(end)
            ))
        });

    let profile = StrategyBuilderProfile::from_name(&input.profile)?;
    let windows = feed_forward_windows(start, end, input.fold_hours)?;
    if windows.len() < 2 {
        bail!(
            "feed-forward plan requires at least two folds; got {} window(s). Use a wider --start/--end range or a smaller --fold-hours.",
            windows.len()
        );
    }
    let mut stages = Vec::new();
    let mut calibration_reports = Vec::new();

    for holdout_idx in 1..windows.len() {
        let calibration_idx = holdout_idx - 1;
        let (calibration_start, calibration_end) = windows[calibration_idx];
        let calibration_report = push_calibration_window_stages(
            &mut stages,
            calibration_idx + 1,
            calibration_start,
            calibration_end,
            &eval_cache_dir,
            &scout_reports_dir,
            &reports_dir,
            &checkpoint_dir,
            &profile,
            zone_mode,
            input.cache_dir.as_ref(),
            input.btc_csv.as_ref(),
            input.bankroll,
            input.latency_ms,
            input.threads,
            input.window_minutes,
        );
        calibration_reports.push(calibration_report);

        let (holdout_start, holdout_end) = windows[holdout_idx];
        let fold_promotion = out_dir.join(format!(
            "promotion_ff_fold_{:02}_train_{}_test_{}.json",
            holdout_idx,
            window_stamp(start, calibration_end),
            window_stamp(holdout_start, holdout_end)
        ));
        let fold_zone_audit = zone_audit_output_for_promotion(&fold_promotion);
        stages.push(StrategyBuilderStage {
            name: format!("feed_forward_promote_{}", holdout_idx),
            purpose:
                "Select a fixed artifact using only calibration windows that end before the holdout starts."
                    .to_string(),
            command: promotion_command(
                &static_calibration_reports(&calibration_reports),
                &fold_promotion,
                zone_mode,
            ),
            outputs: vec![fold_promotion.display().to_string()],
            verify: vec![
                format!("train_end={} < holdout_start={}", calibration_end.to_rfc3339(), holdout_start.to_rfc3339()),
                "promotion artifact params hash matches strategy_params".to_string(),
            ],
            resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
        });
        stages.push(StrategyBuilderStage {
            name: format!("feed_forward_zone_audit_{}", holdout_idx),
            purpose:
                "Record timing-zone concentration for the selected calibration candidate before holdout replay."
                    .to_string(),
            command: zone_audit_command(
                &static_calibration_reports(&calibration_reports),
                &fold_zone_audit,
                zone_mode,
            ),
            outputs: vec![fold_zone_audit.display().to_string()],
            verify: vec![
                "zone audit pass=true for A+ all-zone promotion".to_string(),
                "dominant zone share and per-zone PnL are reviewed before holdout scoring"
                    .to_string(),
            ],
            resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
        });

        let holdout_stamp = window_stamp(holdout_start, holdout_end);
        let replay_report =
            holdout_replay_reports_dir.join(format!("fold_{holdout_idx:02}_{holdout_stamp}.json"));
        let replay_session_dir =
            holdout_replay_dir.join(format!("fold_{holdout_idx:02}_{holdout_stamp}"));
        let mut replay_args = vec![
            "polymomentum-engine".to_string(),
            "live-replay".to_string(),
            "--start".to_string(),
            holdout_start.to_rfc3339(),
            "--end".to_string(),
            holdout_end.to_rfc3339(),
            "--bankroll".to_string(),
            money_arg(input.bankroll),
            "--latency-ms".to_string(),
            input.latency_ms.to_string(),
            "--window-minutes".to_string(),
            float_arg(input.window_minutes),
            "--promotion-artifact".to_string(),
            fold_promotion.display().to_string(),
            "--settlement-alignment-ready".to_string(),
            "--session-log-dir".to_string(),
            replay_session_dir.display().to_string(),
            "--allow-gamma-fetch".to_string(),
            "--report-json".to_string(),
            replay_report.display().to_string(),
        ];
        if let Some(cache_dir) = &input.cache_dir {
            replay_args.extend(["--cache-dir".to_string(), cache_dir.clone()]);
        }
        if let Some(btc_csv) = &input.btc_csv {
            replay_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
        } else {
            replay_args.extend([
                "--btc-csv".to_string(),
                "<required-btc-tick-csv>".to_string(),
            ]);
        }
        stages.push(StrategyBuilderStage {
            name: format!("feed_forward_holdout_replay_{}", holdout_idx),
            purpose:
                "Test the already-promoted artifact on the next unseen holdout window through the live replay path."
                    .to_string(),
            command: shell_command(&replay_args),
            outputs: vec![
                replay_report.display().to_string(),
                replay_session_dir.display().to_string(),
            ],
            verify: vec![
                "holdout replay uses a promotion artifact trained only on prior windows".to_string(),
                "live-replay runs with --settlement-alignment-ready so executable order mechanics are validated offline".to_string(),
                "live-replay report has resolved fills or shadow resolutions".to_string(),
                "session diagnostics have oracle.checks >= resolved/shadow samples and zero actionable disagreements".to_string(),
                "causality diagnostics prove signal_source <= decision <= order <= fill < market_end".to_string(),
            ],
            resource_policy:
                "Can be short on the VPS, but full feed-forward replays should run on a dev box first."
                    .to_string(),
        });

        let diagnostic_session = format!(
            "$(jq -r .session_path {})",
            shell_quote_path(&replay_report)
        );
        stages.push(StrategyBuilderStage {
            name: format!("feed_forward_diagnostics_{}", holdout_idx),
            purpose: "Turn the holdout replay session into a machine-readable gate.".to_string(),
            command: shell_command(&[
                "polymomentum-engine".to_string(),
                "diagnostics".to_string(),
                "session".to_string(),
                diagnostic_session.clone(),
            ]),
            outputs: Vec::new(),
            verify: vec![
                "diagnostics ok=true".to_string(),
                "warnings are explainable; no oracle disagreement on executable candidates"
                    .to_string(),
            ],
            resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
        });
        stages.push(StrategyBuilderStage {
            name: format!("feed_forward_causality_{}", holdout_idx),
            purpose:
                "Falsify timestamp leakage by auditing order/fill/resolution chronology."
                    .to_string(),
            command: shell_command(&[
                "polymomentum-engine".to_string(),
                "diagnostics".to_string(),
                "causality".to_string(),
                diagnostic_session.clone(),
            ]),
            outputs: Vec::new(),
            verify: vec![
                "causality ok=true".to_string(),
                "no future_signal_source, order_before_decision, fill_after_market_end, or resolution_before_market_end violations".to_string(),
            ],
            resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
        });

        let mut fold_audit_args = vec![
            "polymomentum-engine".to_string(),
            "strategy-builder".to_string(),
            "audit".to_string(),
        ];
        for report in &calibration_reports {
            fold_audit_args.extend([
                "--report".to_string(),
                report.static_report.display().to_string(),
            ]);
            fold_audit_args.extend([
                "--adaptive-report".to_string(),
                report.adaptive_report.display().to_string(),
            ]);
        }
        fold_audit_args.extend([
            "--promotion-artifact".to_string(),
            fold_promotion.display().to_string(),
            "--replay-session".to_string(),
            diagnostic_session,
            "--min-trades".to_string(),
            "750".to_string(),
            "--min-win-rate".to_string(),
            "0.63".to_string(),
            "--min-wilson-win-rate-lower".to_string(),
            "0.60".to_string(),
            "--min-total-pnl".to_string(),
            "250".to_string(),
            "--min-shadow-resolutions".to_string(),
            "50".to_string(),
            "--min-research-reports".to_string(),
            calibration_reports.len().to_string(),
        ]);
        stages.push(StrategyBuilderStage {
            name: format!("feed_forward_audit_{}", holdout_idx),
            purpose:
                "Audit calibration robustness and the future holdout session without selecting on holdout PnL."
                    .to_string(),
            command: shell_command(&fold_audit_args),
            outputs: Vec::new(),
            verify: vec![
                "audit ok=true before treating the fold as validation evidence".to_string(),
                "adaptive.drift is based on the holdout replay session only".to_string(),
                "replay.causality is ok for every replay session".to_string(),
            ],
            resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
        });
    }

    let final_idx = windows.len() - 1;
    let (final_calibration_start, final_calibration_end) = windows[final_idx];
    let final_report = push_calibration_window_stages(
        &mut stages,
        final_idx + 1,
        final_calibration_start,
        final_calibration_end,
        &eval_cache_dir,
        &scout_reports_dir,
        &reports_dir,
        &checkpoint_dir,
        &profile,
        zone_mode,
        input.cache_dir.as_ref(),
        input.btc_csv.as_ref(),
        input.bankroll,
        input.latency_ms,
        input.threads,
        input.window_minutes,
    );
    calibration_reports.push(final_report);

    stages.push(StrategyBuilderStage {
        name: "final_feed_forward_promote".to_string(),
        purpose:
            "Train the deployable artifact on all now-historical windows after feed-forward holdouts pass."
                .to_string(),
        command: promotion_command(
            &static_calibration_reports(&calibration_reports),
            &promotion_output,
            zone_mode,
        ),
        outputs: vec![promotion_output.display().to_string()],
        verify: vec![
            "this artifact is for future integration/live only, not for scoring the historical holdouts"
                .to_string(),
            "promotion artifact params hash matches strategy_params".to_string(),
        ],
        resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
    });

    let final_zone_audit = zone_audit_output_for_promotion(&promotion_output);
    stages.push(StrategyBuilderStage {
        name: "final_zone_audit".to_string(),
        purpose:
            "Record timing-zone concentration for the final deployable candidate before runtime preflight."
                .to_string(),
        command: zone_audit_command(
            &static_calibration_reports(&calibration_reports),
            &final_zone_audit,
            zone_mode,
        ),
        outputs: vec![final_zone_audit.display().to_string()],
        verify: vec![
            "zone audit pass=true for A+ all-zone promotion".to_string(),
            "negative active-zone PnL is treated as a research warning even when promotion passes"
                .to_string(),
        ],
        resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
    });

    stages.push(StrategyBuilderStage {
        name: "runtime_preflight".to_string(),
        purpose:
            "Verify the promoted artifact and non-live runtime configuration before deployment."
                .to_string(),
        command: shell_command(&[
            "polymomentum-engine".to_string(),
            "preflight".to_string(),
            "--mode".to_string(),
            "paper".to_string(),
            "--promotion-artifact".to_string(),
            promotion_output.display().to_string(),
        ]),
        outputs: Vec::new(),
        verify: vec![
            "preflight ok=true".to_string(),
            "release manifest records the promoted strategy hash".to_string(),
        ],
        resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
    });

    let mut audit_args = vec![
        "polymomentum-engine".to_string(),
        "strategy-builder".to_string(),
        "audit".to_string(),
    ];
    for report in &calibration_reports {
        audit_args.extend([
            "--report".to_string(),
            report.static_report.display().to_string(),
        ]);
        audit_args.extend([
            "--adaptive-report".to_string(),
            report.adaptive_report.display().to_string(),
        ]);
    }
    audit_args.extend([
        "--promotion-artifact".to_string(),
        promotion_output.display().to_string(),
        "--replay-session".to_string(),
        "<live-replay-or-venue-integration-session.jsonl>".to_string(),
        "--min-trades".to_string(),
        "750".to_string(),
        "--min-win-rate".to_string(),
        "0.63".to_string(),
        "--min-wilson-win-rate-lower".to_string(),
        "0.60".to_string(),
        "--min-total-pnl".to_string(),
        "250".to_string(),
        "--min-shadow-resolutions".to_string(),
        "50".to_string(),
        "--min-research-reports".to_string(),
        calibration_reports.len().to_string(),
    ]);
    stages.push(StrategyBuilderStage {
        name: "adaptive_health_audit".to_string(),
        purpose:
            "Continuously compare forward replay/integration outcomes against the promoted backtest baseline and flag strategy decay."
                .to_string(),
        command: shell_command(&audit_args),
        outputs: Vec::new(),
        verify: vec![
            "adaptive.drift checks are ok before increasing size".to_string(),
            "any warning starts a rolling re-scout; any failure freezes live promotion".to_string(),
            "replay.causality is ok; otherwise treat the strategy as possibly timestamp-leaked"
                .to_string(),
        ],
        resource_policy: "Lightweight; safe on the VPS after each replay or bounded integration session.".to_string(),
    });

    let rolling_out_dir = out_dir.join("rolling_rescout");
    let rolling_start = format!("<latest-{}m-window-start>", float_arg(input.window_minutes));
    let rolling_end = format!("<latest-{}m-window-end>", float_arg(input.window_minutes));
    let mut rescout_args = vec![
        "polymomentum-engine".to_string(),
        "strategy-builder".to_string(),
        "plan".to_string(),
        "--start".to_string(),
        rolling_start,
        "--end".to_string(),
        rolling_end,
        "--out-dir".to_string(),
        rolling_out_dir.display().to_string(),
        "--bankroll".to_string(),
        money_arg(input.bankroll),
        "--latency-ms".to_string(),
        input.latency_ms.to_string(),
        "--threads".to_string(),
        input.threads.to_string(),
        "--window-minutes".to_string(),
        float_arg(input.window_minutes),
        "--fold-hours".to_string(),
        input.fold_hours.to_string(),
        "--profile".to_string(),
        profile.name.to_string(),
        "--zone-mode".to_string(),
        zone_mode.to_string(),
    ];
    if let Some(cache_dir) = &input.cache_dir {
        rescout_args.extend(["--cache-dir".to_string(), cache_dir.clone()]);
    }
    if let Some(btc_csv) = &input.btc_csv {
        rescout_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
    }
    stages.push(StrategyBuilderStage {
        name: "adaptive_rescout_trigger".to_string(),
        purpose:
            "When drift warnings appear, re-run the same walk-forward loop on the freshest resolved window before touching runtime params."
                .to_string(),
        command: shell_command(&rescout_args),
        outputs: vec![rolling_out_dir.display().to_string()],
        verify: vec![
            "fresh candidate must pass aggregate promotion before replacing the current artifact"
                .to_string(),
            "old artifact remains active until a new artifact passes replay and diagnostics gates"
                .to_string(),
        ],
        resource_policy:
            "Plan is lightweight; execute the generated heavy sweeps on the dev box, not the VPS."
                .to_string(),
    });

    Ok(StrategyBuilderPlan {
        schema_version: 1,
        profile: profile.name.to_string(),
        start: start.to_rfc3339(),
        end: end.to_rfc3339(),
        out_dir: out_dir.display().to_string(),
        window_minutes: input.window_minutes,
        fold_hours: input.fold_hours,
        zone_mode: zone_mode.to_string(),
        stages,
        notes: vec![
            "The builder is feed-forward: every holdout replay uses a promotion artifact selected only from strictly earlier calibration windows.".to_string(),
            "Historical holdout performance is measured by fixed-artifact live-replay, not by selecting the best grid cell on the holdout window.".to_string(),
            "The final promotion artifact is for future integration/live only; it is not reused to score the historical windows that trained it.".to_string(),
            "Zone-specific sweeps keep timing regimes isolated; aggregate promotion must prove the same parameter hash survives the calibration windows available at that point in time.".to_string(),
            "Adaptive operation is reactive, not self-mutating: drift warnings trigger a fresh scout, while runtime keeps the last promoted artifact until a new one passes all gates.".to_string(),
            "Do not run CPU-heavy sweeps on the multi-tenant VPS; run them on the dev box and copy artifacts over.".to_string(),
            "BTC tape coverage is now a hard gate; stale CSVs are rejected instead of producing flat fake resolutions.".to_string(),
            "Use backtest/live-replay validation first; paper mode is only for live venue plumbing that cannot be reproduced offline.".to_string(),
            "Promotion uses robust-promote: hard gates first, then worst-window expectancy, neighbor stability, zone balance, zone PnL coverage, Wilson lower bound, maker fill reliability, and PBO diagnostics.".to_string(),
            "Every promotion stage has a matching zone-audit artifact; A+ all-zone candidates must keep dominant-zone share within the strict concentration gate.".to_string(),
            "For broad PMXT history, scan gradually in time-boxed local caches: hydrate one rolling window, emit reports/artifacts, then delete only the parquets downloaded by that session.".to_string(),
        ],
    })
}

pub fn audit(input: StrategyBuilderAuditInput) -> StrategyBuilderAudit {
    let mut checks = Vec::new();
    let report_count = input.report_paths.len().max(1);
    let per_report_min_trades = (input.min_trades / report_count).max(1);
    let per_report_min_pnl = input.min_total_pnl / report_count as f64;

    for report_path in &input.report_paths {
        match experiment::read_report(report_path) {
            Ok(report) => {
                checks.push(check(
                    "report.load",
                    StrategyBuilderCheckStatus::Ok,
                    format!(
                        "{} variants={} complete={}",
                        report_path,
                        report.variants.len(),
                        report.data_manifest.complete
                    ),
                ));
                if report.data_manifest.complete {
                    checks.push(check(
                        "report.data_manifest",
                        StrategyBuilderCheckStatus::Ok,
                        format!("{report_path} complete data manifest"),
                    ));
                } else {
                    checks.push(check(
                        "report.data_manifest",
                        StrategyBuilderCheckStatus::Fail,
                        format!("{report_path} incomplete data manifest"),
                    ));
                }
                if let Some(best) = report.variants.first() {
                    let wilson = wilson_lower(best.wins, best.trades);
                    let passive_failures_only =
                        only_passive_execution_failures(&best.reject_reasons);
                    let status = if best.trades >= per_report_min_trades
                        && best.win_rate >= input.min_win_rate
                        && wilson >= input.min_wilson_win_rate_lower
                        && best.total_pnl >= per_report_min_pnl
                        && best.unresolved_fills == 0
                        && (best.fills_failed == 0 || passive_failures_only)
                    {
                        StrategyBuilderCheckStatus::Ok
                    } else {
                        StrategyBuilderCheckStatus::Warn
                    };
                    checks.push(check(
                        "report.best_variant",
                        status,
                        format!(
                            "{} trades={} attempts={} fill_rate={:.3} failed={} passive_failures_only={} win_rate={:.3} wilson95={:.3} pnl={:.2} unresolved={} per_report_gates[min_trades={}, min_pnl={:.2}]",
                            report_path,
                            best.trades,
                            best.execution_attempts,
                            best.fill_rate,
                            best.fills_failed,
                            passive_failures_only,
                            best.win_rate,
                            wilson,
                            best.total_pnl,
                            best.unresolved_fills,
                            per_report_min_trades,
                            per_report_min_pnl,
                        ),
                    ));
                    checks.push(check(
                        "report.best_variant_health",
                        if best.breaker_tripped || best.diagnostics.adaptive_rearms > 0 {
                            StrategyBuilderCheckStatus::Fail
                        } else {
                            StrategyBuilderCheckStatus::Ok
                        },
                        format!(
                            "{} breaker_tripped={} breaker_reason={} adaptive_rearms={} breaker_paused_events={}",
                            report_path,
                            best.breaker_tripped,
                            best.breaker_reason.as_deref().unwrap_or("none"),
                            best.diagnostics.adaptive_rearms,
                            best.diagnostics.breaker_paused_events,
                        ),
                    ));
                } else {
                    checks.push(check(
                        "report.best_variant",
                        StrategyBuilderCheckStatus::Fail,
                        format!("{report_path} has no variants"),
                    ));
                }
            }
            Err(e) => checks.push(check(
                "report.load",
                StrategyBuilderCheckStatus::Fail,
                format!("{report_path}: {e:#}"),
            )),
        }
    }
    checks.push(check(
        "a_plus.research_reports",
        if input.report_paths.len() >= input.min_research_reports {
            StrategyBuilderCheckStatus::Ok
        } else {
            StrategyBuilderCheckStatus::Warn
        },
        format!(
            "reports={} min_research_reports={}",
            input.report_paths.len(),
            input.min_research_reports
        ),
    ));
    audit_adaptive_probe_reports(&input, &mut checks);

    if let Some(path) = &input.promotion_artifact {
        audit_promotion(path, &input, &mut checks);
        audit_adaptive_drift(path, &input, &mut checks);
    } else {
        checks.push(check(
            "promotion.load",
            StrategyBuilderCheckStatus::Warn,
            "no promotion artifact supplied".to_string(),
        ));
        checks.push(check(
            "adaptive.baseline",
            StrategyBuilderCheckStatus::Warn,
            "no promotion artifact supplied, so forward decay cannot be compared to a locked baseline"
                .to_string(),
        ));
    }

    checks.push(check(
        "a_plus.replay_sessions",
        if input.replay_sessions.len() >= input.min_replay_sessions {
            StrategyBuilderCheckStatus::Ok
        } else {
            StrategyBuilderCheckStatus::Warn
        },
        format!(
            "replay_sessions={} min_replay_sessions={}",
            input.replay_sessions.len(),
            input.min_replay_sessions
        ),
    ));

    let mut replay_oracle_samples_total = 0_u64;
    let mut replay_resolved_total = 0_u64;
    let mut replay_shadow_total = 0_u64;
    for session in &input.replay_sessions {
        match diagnostics::analyze_session(session) {
            Ok(diag) => {
                let shadow = *diag.event_counts.get("shadow.resolved").unwrap_or(&0);
                let resolved = diag.resolutions.resolved;
                let oracle_samples = shadow.max(resolved);
                replay_oracle_samples_total += oracle_samples;
                replay_resolved_total += resolved;
                replay_shadow_total += shadow;
                checks.push(check(
                    "replay.session",
                    if diag.ok {
                        StrategyBuilderCheckStatus::Ok
                    } else {
                        StrategyBuilderCheckStatus::Fail
                    },
                    format!(
                        "{} ok={} events={} resolved={} shadow={} oracle={} disagreements={} actionable_disagreements={} below_floor_disagreements={} errors={}",
                        session,
                        diag.ok,
                        diag.total_events,
                        resolved,
                        shadow,
                        diag.oracle.checks,
                        diag.oracle.disagreements,
                        diag.oracle.actionable_disagreements,
                        diag.oracle.below_floor_disagreements,
                        diag.system.errors
                    ),
                ));
                let status = if oracle_samples >= input.min_shadow_resolutions
                    && diag.oracle.checks >= oracle_samples
                    && diag.oracle.actionable_disagreements == 0
                    && diag.oracle.ties == 0
                    && diag.system.errors == 0
                {
                    StrategyBuilderCheckStatus::Ok
                } else {
                    StrategyBuilderCheckStatus::Fail
                };
                checks.push(check(
                    "replay.shadow_oracle",
                    status,
                    format!(
                        "{} resolved={} shadow={} samples={} min_samples={} oracle={} ties={} disagreements={} actionable_disagreements={} below_floor_disagreements={} errors={}",
                        session,
                        resolved,
                        shadow,
                        oracle_samples,
                        input.min_shadow_resolutions,
                        diag.oracle.checks,
                        diag.oracle.ties,
                        diag.oracle.disagreements,
                        diag.oracle.actionable_disagreements,
                        diag.oracle.below_floor_disagreements,
                        diag.system.errors
                    ),
                ));
                checks.push(check(
                    "replay.settlement_alignment",
                    match diag.system.settlement_alignment_ready {
                        Some(true) => StrategyBuilderCheckStatus::Ok,
                        Some(false) | None => StrategyBuilderCheckStatus::Warn,
                    },
                    format!(
                        "{} settlement_alignment_ready={:?}; executable order-path parity requires true",
                        session, diag.system.settlement_alignment_ready
                    ),
                ));
                checks.push(check(
                    "replay.below_floor_oracle",
                    StrategyBuilderCheckStatus::Ok,
                    format!(
                        "{} below_floor_disagreements={} excluded_from_executable_gate=true",
                        session, diag.oracle.below_floor_disagreements
                    ),
                ));
                match causality::audit_session(session, causality::CausalityAuditConfig::default())
                {
                    Ok(audit) => checks.push(check(
                        "replay.causality",
                        if audit.ok {
                            StrategyBuilderCheckStatus::Ok
                        } else {
                            StrategyBuilderCheckStatus::Fail
                        },
                        format!(
                            "{} ok={} order_timings={} placed={} filled={} resolution_timings={} violations={}",
                            session,
                            audit.ok,
                            audit.order_timings,
                            audit.order_placed,
                            audit.order_filled,
                            audit.resolution_timings,
                            audit.violations.len()
                        ),
                    )),
                    Err(e) => checks.push(check(
                        "replay.causality",
                        StrategyBuilderCheckStatus::Fail,
                        format!("{session}: {e:#}"),
                    )),
                }
            }
            Err(e) => checks.push(check(
                "replay.session",
                StrategyBuilderCheckStatus::Fail,
                format!("{session}: {e:#}"),
            )),
        }
    }

    if input.replay_sessions.is_empty() {
        checks.push(check(
            "replay.session",
            StrategyBuilderCheckStatus::Warn,
            "no live-replay or bounded integration session supplied".to_string(),
        ));
    } else {
        checks.push(check(
            "replay.a_plus_sample",
            if replay_oracle_samples_total >= input.a_plus_min_shadow_resolutions {
                StrategyBuilderCheckStatus::Ok
            } else {
                StrategyBuilderCheckStatus::Warn
            },
            format!(
                "sessions={} samples={} resolved={} shadow={} a_plus_min_samples={}",
                input.replay_sessions.len(),
                replay_oracle_samples_total,
                replay_resolved_total,
                replay_shadow_total,
                input.a_plus_min_shadow_resolutions
            ),
        ));
    }

    let fail_count = checks
        .iter()
        .filter(|c| c.status == StrategyBuilderCheckStatus::Fail)
        .count();
    let warn_count = checks
        .iter()
        .filter(|c| c.status == StrategyBuilderCheckStatus::Warn)
        .count();
    let ok = fail_count == 0;
    let a_plus_ready = ok && warn_count == 0;
    let grade = match (fail_count, warn_count) {
        (0, 0) => "A+",
        (0, 1..=2) => "A-",
        (0, _) => "B",
        (1..=2, _) => "C",
        _ => "D",
    }
    .to_string();
    let next_steps = next_steps(ok, warn_count, a_plus_ready);

    StrategyBuilderAudit {
        schema_version: 1,
        ok,
        a_plus_ready,
        grade,
        checks,
        next_steps,
    }
}

pub fn selectivity_search(
    input: StrategyBuilderSelectivitySearchInput,
) -> Result<StrategyBuilderSelectivitySearch> {
    if input.report_paths.len() < 2 {
        bail!("selectivity search needs at least two reports");
    }
    if input.min_train_reports == 0 {
        bail!("--min-train-reports must be > 0");
    }
    if input.report_paths.len() <= input.min_train_reports {
        bail!(
            "report count ({}) must be greater than --min-train-reports ({})",
            input.report_paths.len(),
            input.min_train_reports
        );
    }

    let mut folds = Vec::new();
    for report_path in &input.report_paths {
        let report = experiment::read_report(report_path)
            .with_context(|| format!("load selectivity report {report_path}"))?;
        let variants = report
            .variants
            .iter()
            .map(|variant| {
                let regimes = variant.diagnostics.by_regime.clone();
                SelectivityVariantFold {
                    name: variant_report_name(variant),
                    buckets: selectivity_buckets_for_variant(variant),
                    tagged_regimes: tagged_regimes_from_map(&regimes),
                    regimes,
                }
            })
            .collect();
        folds.push(SelectivityFold { variants });
    }

    Ok(selectivity_search_from_folds(&folds, &input))
}

pub fn multi_guard_search(
    input: StrategyBuilderMultiGuardSearchInput,
) -> Result<StrategyBuilderMultiGuardSearch> {
    if input.report_paths.len() < 2 {
        bail!("multi-guard search needs at least two reports");
    }
    if input.min_train_reports == 0 {
        bail!("--min-train-reports must be > 0");
    }
    if input.max_rules == 0 {
        bail!("--max-rules must be > 0");
    }
    if input.report_paths.len() <= input.min_train_reports {
        bail!(
            "report count ({}) must be greater than --min-train-reports ({})",
            input.report_paths.len(),
            input.min_train_reports
        );
    }

    let mut folds = Vec::new();
    for report_path in &input.report_paths {
        let report = experiment::read_report(report_path)
            .with_context(|| format!("load multi-guard report {report_path}"))?;
        let variants = report
            .variants
            .iter()
            .map(|variant| {
                let regimes = variant.diagnostics.by_regime.clone();
                SelectivityVariantFold {
                    name: variant_report_name(variant),
                    buckets: selectivity_buckets_for_variant(variant),
                    tagged_regimes: tagged_regimes_from_map(&regimes),
                    regimes,
                }
            })
            .collect();
        folds.push(SelectivityFold { variants });
    }

    Ok(multi_guard_search_from_folds(&folds, &input))
}

pub fn adaptive_direction_search(
    input: StrategyBuilderAdaptiveDirectionInput,
) -> Result<StrategyBuilderAdaptiveDirectionSearch> {
    if input.report_paths.len() < 2 {
        bail!("adaptive direction search needs at least two reports");
    }
    if input.min_train_reports == 0 {
        bail!("--min-train-reports must be > 0");
    }
    if input.report_paths.len() <= input.min_train_reports {
        bail!(
            "report count ({}) must be greater than --min-train-reports ({})",
            input.report_paths.len(),
            input.min_train_reports
        );
    }

    let mut folds = Vec::new();
    for report_path in &input.report_paths {
        let report = experiment::read_report(report_path)
            .with_context(|| format!("load adaptive direction report {report_path}"))?;
        let variants = report
            .variants
            .iter()
            .map(|variant| {
                let regimes = variant.diagnostics.by_regime.clone();
                SelectivityVariantFold {
                    name: variant_report_name(variant),
                    buckets: selectivity_buckets_for_variant(variant),
                    tagged_regimes: tagged_regimes_from_map(&regimes),
                    regimes,
                }
            })
            .collect();
        folds.push(SelectivityFold { variants });
    }

    Ok(adaptive_direction_search_from_folds(&folds, &input))
}

pub fn adaptive_mode_search(
    input: StrategyBuilderAdaptiveModeInput,
) -> Result<StrategyBuilderAdaptiveModeSearch> {
    if input.report_paths.len() < 2 {
        bail!("adaptive mode search needs at least two reports");
    }
    if input.min_train_reports == 0 {
        bail!("--min-train-reports must be > 0");
    }
    if input.max_guard_rules == 0 {
        bail!("--max-guard-rules must be > 0");
    }
    if input.report_paths.len() <= input.min_train_reports {
        bail!(
            "report count ({}) must be greater than --min-train-reports ({})",
            input.report_paths.len(),
            input.min_train_reports
        );
    }

    let mut folds = Vec::new();
    for report_path in &input.report_paths {
        let report = experiment::read_report(report_path)
            .with_context(|| format!("load adaptive mode report {report_path}"))?;
        let variants = report
            .variants
            .iter()
            .map(|variant| {
                let regimes = variant.diagnostics.by_regime.clone();
                SelectivityVariantFold {
                    name: variant_report_name(variant),
                    buckets: selectivity_buckets_for_variant(variant),
                    tagged_regimes: tagged_regimes_from_map(&regimes),
                    regimes,
                }
            })
            .collect();
        folds.push(SelectivityFold { variants });
    }

    Ok(adaptive_mode_search_from_folds(&folds, &input))
}

pub fn causal_policy_search(
    input: StrategyBuilderCausalPolicySearchInput,
) -> Result<StrategyBuilderCausalPolicySearch> {
    if input.report_paths.len() < 2 {
        bail!("causal policy search needs at least two reports");
    }
    if input.min_train_reports == 0 {
        bail!("--min-train-reports must be > 0");
    }
    if input.max_require_terms == 0 {
        bail!("--max-require-terms must be > 0");
    }
    if input.report_paths.len() <= input.min_train_reports {
        bail!(
            "report count ({}) must be greater than --min-train-reports ({})",
            input.report_paths.len(),
            input.min_train_reports
        );
    }

    let mut folds = Vec::new();
    for report_path in &input.report_paths {
        let report = experiment::read_report(report_path)
            .with_context(|| format!("load causal policy report {report_path}"))?;
        let variants = report
            .variants
            .iter()
            .map(|variant| {
                let regimes = variant.diagnostics.by_regime.clone();
                SelectivityVariantFold {
                    name: variant_report_name(variant),
                    buckets: selectivity_buckets_for_variant(variant),
                    tagged_regimes: tagged_regimes_from_map(&regimes),
                    regimes,
                }
            })
            .collect();
        folds.push(SelectivityFold { variants });
    }

    Ok(causal_policy_search_from_folds(&folds, &input))
}

fn selectivity_search_from_folds(
    folds: &[SelectivityFold],
    input: &StrategyBuilderSelectivitySearchInput,
) -> StrategyBuilderSelectivitySearch {
    let mut candidates = candidate_keys(folds)
        .into_iter()
        .map(|candidate| evaluate_selectivity_candidate(folds, input, candidate))
        .collect::<Vec<_>>();
    let candidate_count = candidates.len();

    candidates.sort_by(|a, b| {
        b.passed
            .cmp(&a.passed)
            .then_with(|| {
                f64_desc(
                    a.fold_forward.stats.total_pnl,
                    b.fold_forward.stats.total_pnl,
                )
            })
            .then_with(|| {
                f64_desc(
                    a.fold_forward.stats.wilson_win_rate_lower,
                    b.fold_forward.stats.wilson_win_rate_lower,
                )
            })
            .then_with(|| {
                b.fold_forward
                    .stats
                    .trades
                    .cmp(&a.fold_forward.stats.trades)
            })
            .then_with(|| f64_desc(a.aggregate.total_pnl, b.aggregate.total_pnl))
            .then_with(|| a.variant.cmp(&b.variant))
            .then_with(|| a.rule.cmp(&b.rule))
    });

    for (idx, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank = idx + 1;
    }
    let top = input.top.max(1);
    candidates.truncate(top);

    StrategyBuilderSelectivitySearch {
        schema_version: 1,
        ok: candidates.iter().any(|candidate| candidate.passed),
        report_count: folds.len(),
        candidate_count,
        methodology: vec![
            "Generate allow-only and deny rules from causal PnL buckets and full regime-interaction buckets already emitted by feed-forward harness reports.".to_string(),
            "Score each OOS report only when the same rule had enough prior-report trades and positive prior-report PnL.".to_string(),
            "Do not use future folds to decide whether a current fold is eligible; late lucky regimes cannot select themselves backward.".to_string(),
            "Treat these results as strategy hypotheses; rerun the selected rule through full harness/live-replay before promotion.".to_string(),
        ],
        gates: SelectivitySearchGates {
            min_train_reports: input.min_train_reports,
            min_train_trades: input.min_train_trades,
            min_oos_trades: input.min_oos_trades,
            min_oos_wilson_win_rate_lower: input.min_oos_wilson_win_rate_lower,
            min_oos_total_pnl: input.min_oos_total_pnl,
            min_oos_profitable_reports: input.min_oos_profitable_reports,
            min_worst_oos_pnl: input.min_worst_oos_pnl,
        },
        candidates,
    }
}

fn multi_guard_search_from_folds(
    folds: &[SelectivityFold],
    input: &StrategyBuilderMultiGuardSearchInput,
) -> StrategyBuilderMultiGuardSearch {
    let mut candidates = variant_names(folds)
        .into_iter()
        .map(|variant| evaluate_multi_guard_candidate(folds, input, variant))
        .collect::<Vec<_>>();
    let candidate_count = candidates.len();

    candidates.sort_by(|a, b| {
        b.passed
            .cmp(&a.passed)
            .then_with(|| {
                f64_desc(
                    a.fold_forward.stats.total_pnl,
                    b.fold_forward.stats.total_pnl,
                )
            })
            .then_with(|| {
                f64_desc(
                    a.fold_forward.stats.wilson_win_rate_lower,
                    b.fold_forward.stats.wilson_win_rate_lower,
                )
            })
            .then_with(|| {
                b.fold_forward
                    .stats
                    .trades
                    .cmp(&a.fold_forward.stats.trades)
            })
            .then_with(|| a.variant.cmp(&b.variant))
    });

    for (idx, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank = idx + 1;
    }
    let top = input.top.max(1);
    candidates.truncate(top);

    StrategyBuilderMultiGuardSearch {
        schema_version: 1,
        ok: candidates.iter().any(|candidate| candidate.passed),
        report_count: folds.len(),
        candidate_count,
        methodology: vec![
            "Learn a set of denied full-regime buckets from strictly prior reports only."
                .to_string(),
            "Only full-regime buckets are composed because they are mutually exclusive; overlapping dimensions are deliberately excluded from multi-rule arithmetic."
                .to_string(),
            "Each OOS fold is scored after its guard is fixed from prior folds; future folds never choose current guards."
                .to_string(),
            "Guard ranking uses prior-fold loss support and payoff-asymmetry diagnostics: profit factor, average loss, payoff ratio, and worst loss to average win."
                .to_string(),
            "Treat the result as a strategy hypothesis; rerun any static guard through full harness/live-replay before promotion."
                .to_string(),
        ],
        gates: MultiGuardSearchGates {
            min_train_reports: input.min_train_reports,
            min_train_trades: input.min_train_trades,
            min_oos_trades: input.min_oos_trades,
            min_oos_wilson_win_rate_lower: input.min_oos_wilson_win_rate_lower,
            min_oos_total_pnl: input.min_oos_total_pnl,
            min_oos_profitable_reports: input.min_oos_profitable_reports,
            min_worst_oos_pnl: input.min_worst_oos_pnl,
            max_rules: input.max_rules,
            min_guard_trades: input.min_guard_trades,
            min_guard_loss_pnl: input.min_guard_loss_pnl,
            min_guard_loss_reports: input.min_guard_loss_reports,
            recent_report_lookback: input.recent_report_lookback,
            pattern_guards: input.pattern_guards,
        },
        candidates,
    }
}

fn adaptive_direction_search_from_folds(
    folds: &[SelectivityFold],
    input: &StrategyBuilderAdaptiveDirectionInput,
) -> StrategyBuilderAdaptiveDirectionSearch {
    let mut candidates = variant_names(folds)
        .into_iter()
        .map(|variant| evaluate_adaptive_direction_candidate(folds, input, variant))
        .collect::<Vec<_>>();
    let candidate_count = candidates.len();

    candidates.sort_by(|a, b| {
        b.passed
            .cmp(&a.passed)
            .then_with(|| {
                f64_desc(
                    a.fold_forward.stats.total_pnl,
                    b.fold_forward.stats.total_pnl,
                )
            })
            .then_with(|| {
                f64_desc(
                    a.fold_forward.stats.wilson_win_rate_lower,
                    b.fold_forward.stats.wilson_win_rate_lower,
                )
            })
            .then_with(|| {
                b.fold_forward
                    .stats
                    .trades
                    .cmp(&a.fold_forward.stats.trades)
            })
            .then_with(|| a.variant.cmp(&b.variant))
    });

    for (idx, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank = idx + 1;
    }
    let top = input.top.max(1);
    candidates.truncate(top);

    StrategyBuilderAdaptiveDirectionSearch {
        schema_version: 1,
        ok: candidates.iter().any(|candidate| candidate.passed),
        report_count: folds.len(),
        candidate_count,
        methodology: vec![
            "For each OOS report, aggregate only strictly earlier reports for the same variant."
                .to_string(),
            "Score the up and down direction buckets from prior folds; choose the best positive, sufficiently sampled side or abstain flat."
                .to_string(),
            "Apply the selected direction to the next report only after the choice is fixed; future folds never influence current choices."
                .to_string(),
            "Treat this as a regime-selection hypothesis; rerun any selected adaptive policy through full harness/live-replay before promotion."
                .to_string(),
        ],
        gates: SelectivitySearchGates {
            min_train_reports: input.min_train_reports,
            min_train_trades: input.min_train_trades,
            min_oos_trades: input.min_oos_trades,
            min_oos_wilson_win_rate_lower: input.min_oos_wilson_win_rate_lower,
            min_oos_total_pnl: input.min_oos_total_pnl,
            min_oos_profitable_reports: input.min_oos_profitable_reports,
            min_worst_oos_pnl: input.min_worst_oos_pnl,
        },
        candidates,
    }
}

fn adaptive_mode_search_from_folds(
    folds: &[SelectivityFold],
    input: &StrategyBuilderAdaptiveModeInput,
) -> StrategyBuilderAdaptiveModeSearch {
    let mut candidates = variant_names(folds)
        .into_iter()
        .map(|variant| evaluate_adaptive_mode_candidate(folds, input, variant))
        .collect::<Vec<_>>();
    let candidate_count = candidates.len();

    candidates.sort_by(|a, b| {
        b.passed
            .cmp(&a.passed)
            .then_with(|| {
                f64_desc(
                    a.fold_forward.stats.total_pnl,
                    b.fold_forward.stats.total_pnl,
                )
            })
            .then_with(|| {
                f64_desc(
                    a.fold_forward.stats.wilson_win_rate_lower,
                    b.fold_forward.stats.wilson_win_rate_lower,
                )
            })
            .then_with(|| {
                b.fold_forward
                    .stats
                    .trades
                    .cmp(&a.fold_forward.stats.trades)
            })
            .then_with(|| a.variant.cmp(&b.variant))
    });

    for (idx, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank = idx + 1;
    }
    let top = input.top.max(1);
    candidates.truncate(top);

    StrategyBuilderAdaptiveModeSearch {
        schema_version: 1,
        ok: candidates.iter().any(|candidate| candidate.passed),
        report_count: folds.len(),
        candidate_count,
        methodology: vec![
            "For each OOS report, build direction and guarded options from strictly prior reports only.".to_string(),
            "Rank active options by prior worst-fold PnL first, then prior aggregate PnL, Wilson lower bound, profit factor, and trade count.".to_string(),
            "Choose flat when no active mode passes prior gates, or when the best active prior worst-fold PnL is below the configured flat threshold.".to_string(),
            "Score the selected mode on the current fold only after the mode is fixed; future folds never influence current choices.".to_string(),
            "Treat passing results as strategy hypotheses; rerun selected policies in full harness/live-replay before promotion.".to_string(),
        ],
        gates: AdaptiveModeSearchGates {
            min_train_reports: input.min_train_reports,
            min_train_trades: input.min_train_trades,
            min_oos_trades: input.min_oos_trades,
            min_oos_wilson_win_rate_lower: input.min_oos_wilson_win_rate_lower,
            min_oos_total_pnl: input.min_oos_total_pnl,
            min_oos_profitable_reports: input.min_oos_profitable_reports,
            min_worst_oos_pnl: input.min_worst_oos_pnl,
            max_guard_rules: input.max_guard_rules,
            min_guard_trades: input.min_guard_trades,
            min_guard_loss_pnl: input.min_guard_loss_pnl,
            min_guard_loss_reports: input.min_guard_loss_reports,
            recent_report_lookback: input.recent_report_lookback,
            pattern_guards: input.pattern_guards,
            flat_if_worst_train_below: input.flat_if_worst_train_below,
        },
        candidates,
    }
}

fn causal_policy_search_from_folds(
    folds: &[SelectivityFold],
    input: &StrategyBuilderCausalPolicySearchInput,
) -> StrategyBuilderCausalPolicySearch {
    let mut candidates = causal_policy_candidate_keys(folds, input.max_require_terms)
        .into_iter()
        .map(|candidate| evaluate_causal_policy_candidate(folds, input, candidate))
        .collect::<Vec<_>>();
    let candidate_count = candidates.len();

    candidates.sort_by(|a, b| {
        let a_trade_gate = a.fold_forward.stats.trades >= input.min_oos_trades;
        let b_trade_gate = b.fold_forward.stats.trades >= input.min_oos_trades;
        let a_profitable_gate =
            a.fold_forward.profitable_reports >= input.min_oos_profitable_reports;
        let b_profitable_gate =
            b.fold_forward.profitable_reports >= input.min_oos_profitable_reports;
        let a_pnl_gate = a.fold_forward.stats.total_pnl >= input.min_oos_total_pnl;
        let b_pnl_gate = b.fold_forward.stats.total_pnl >= input.min_oos_total_pnl;
        let a_wilson_gate =
            a.fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower;
        let b_wilson_gate =
            b.fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower;
        let a_tail_gate = a.fold_forward.worst_report_pnl >= input.min_worst_oos_pnl;
        let b_tail_gate = b.fold_forward.worst_report_pnl >= input.min_worst_oos_pnl;
        b.passed
            .cmp(&a.passed)
            .then_with(|| b_trade_gate.cmp(&a_trade_gate))
            .then_with(|| b_profitable_gate.cmp(&a_profitable_gate))
            .then_with(|| b_pnl_gate.cmp(&a_pnl_gate))
            .then_with(|| b_wilson_gate.cmp(&a_wilson_gate))
            .then_with(|| b_tail_gate.cmp(&a_tail_gate))
            .then_with(|| {
                f64_desc(
                    a.fold_forward.stats.total_pnl,
                    b.fold_forward.stats.total_pnl,
                )
            })
            .then_with(|| {
                b.fold_forward
                    .stats
                    .trades
                    .cmp(&a.fold_forward.stats.trades)
            })
            .then_with(|| {
                f64_desc(
                    a.fold_forward.stats.wilson_win_rate_lower,
                    b.fold_forward.stats.wilson_win_rate_lower,
                )
            })
            .then_with(|| {
                f64_desc(
                    a.fold_forward.worst_report_pnl,
                    b.fold_forward.worst_report_pnl,
                )
            })
            .then_with(|| a.base_require.len().cmp(&b.base_require.len()))
            .then_with(|| a.variant.cmp(&b.variant))
            .then_with(|| policy_label(&a.base_require).cmp(&policy_label(&b.base_require)))
    });

    for (idx, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank = idx + 1;
    }
    let top = input.top.max(1);
    candidates.truncate(top);

    StrategyBuilderCausalPolicySearch {
        schema_version: 1,
        ok: candidates.iter().any(|candidate| candidate.passed),
        report_count: folds.len(),
        candidate_count,
        methodology: vec![
            "Generate causal require-policy conjunctions from observed pre-trade regime tags."
                .to_string(),
            "For each OOS report, train the require policy and optional deny tags on strictly prior reports only."
                .to_string(),
            "Default deny rules are single-tag vetoes so the result maps directly to existing --require-causal-tag and --deny-causal-tag runtime filters."
                .to_string(),
            "Rank candidates by pass status, worst OOS fold, aggregate OOS PnL, Wilson lower bound, trade count, and policy simplicity."
                .to_string(),
            "Treat passing results as hypotheses; rerun the selected require/deny policy in full harness/live-replay before promotion."
                .to_string(),
        ],
        gates: CausalPolicySearchGates {
            min_train_reports: input.min_train_reports,
            min_train_trades: input.min_train_trades,
            min_oos_trades: input.min_oos_trades,
            min_oos_wilson_win_rate_lower: input.min_oos_wilson_win_rate_lower,
            min_oos_total_pnl: input.min_oos_total_pnl,
            min_oos_profitable_reports: input.min_oos_profitable_reports,
            min_worst_oos_pnl: input.min_worst_oos_pnl,
            max_require_terms: input.max_require_terms,
            max_deny_rules: input.max_deny_rules,
            max_deny_terms: input.max_deny_terms,
            min_deny_trades: input.min_deny_trades,
            min_deny_loss_pnl: input.min_deny_loss_pnl,
            min_deny_loss_reports: input.min_deny_loss_reports,
        },
        candidates,
    }
}

fn variant_names(folds: &[SelectivityFold]) -> Vec<String> {
    let mut names = BTreeSet::new();
    for fold in folds {
        for variant in &fold.variants {
            names.insert(variant.name.clone());
        }
    }
    names.into_iter().collect()
}

#[derive(Debug, Clone)]
struct MultiGuardRule {
    label: String,
    match_tags: BTreeMap<String, String>,
    reports_with_trades: usize,
    stats: TradePnlDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CausalPolicyKey {
    variant: String,
    require_tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct CausalPolicyRule {
    label: String,
    match_tags: BTreeMap<String, String>,
    reports_with_trades: usize,
    stats: TradePnlDiagnostics,
}

#[derive(Debug, Clone)]
struct CausalPolicy {
    require_tags: BTreeMap<String, String>,
    deny_rules: Vec<CausalPolicyRule>,
}

#[derive(Debug, Clone)]
struct AdaptiveModeOption {
    mode: AdaptiveModeKind,
    direction: Option<String>,
    guard: Vec<MultiGuardRule>,
    train: TradePnlDiagnostics,
    train_summary: AdaptiveModeTrainSummary,
}

#[derive(Debug, Clone, Default)]
struct AdaptiveModeTrainSummary {
    eligible_reports: usize,
    profitable_reports: usize,
    losing_reports: usize,
    worst_report_pnl: f64,
}

fn evaluate_adaptive_mode_candidate(
    folds: &[SelectivityFold],
    input: &StrategyBuilderAdaptiveModeInput,
    variant: String,
) -> AdaptiveModeCandidateReport {
    let mut oos = TradePnlDiagnostics::default();
    let mut eligible_reports = 0_usize;
    let mut profitable_reports = 0_usize;
    let mut losing_reports = 0_usize;
    let mut abstained_reports = 0_usize;
    let mut worst_report_pnl: Option<f64> = None;
    let mut decisions = Vec::new();

    for idx in 0..folds.len() {
        if idx < input.min_train_reports {
            let (recent_losing_reports, recent_worst_report_pnl) =
                recent_loss_context(&folds[..idx], &variant, input.recent_report_lookback);
            decisions.push(AdaptiveModeDecisionReport {
                report_index: idx,
                train_reports: idx,
                recent_losing_reports,
                recent_worst_report_pnl,
                selected_mode: AdaptiveModeKind::Flat,
                selected_direction: None,
                guard: MultiGuardPolicyReport {
                    deny_regimes: Vec::new(),
                },
                train: None,
                train_summary: None,
                oos: None,
                active_options: Vec::new(),
                reason: "insufficient_prior_reports".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        let prior_folds = &folds[..idx];
        let mut options = adaptive_mode_options(prior_folds, input, &variant);
        options.sort_by(compare_adaptive_mode_options);
        let option_reports = options
            .iter()
            .map(adaptive_mode_option_report)
            .collect::<Vec<_>>();
        let (recent_losing_reports, recent_worst_report_pnl) =
            recent_loss_context(prior_folds, &variant, input.recent_report_lookback);
        let selected = options.first().cloned();
        let Some(selected) = selected else {
            decisions.push(AdaptiveModeDecisionReport {
                report_index: idx,
                train_reports: idx,
                recent_losing_reports,
                recent_worst_report_pnl,
                selected_mode: AdaptiveModeKind::Flat,
                selected_direction: None,
                guard: MultiGuardPolicyReport {
                    deny_regimes: Vec::new(),
                },
                train: None,
                train_summary: None,
                oos: None,
                active_options: option_reports,
                reason: "no_active_mode_passed_prior_gates".to_string(),
            });
            abstained_reports += 1;
            continue;
        };
        if selected.train_summary.worst_report_pnl < input.flat_if_worst_train_below {
            decisions.push(AdaptiveModeDecisionReport {
                report_index: idx,
                train_reports: idx,
                recent_losing_reports,
                recent_worst_report_pnl,
                selected_mode: AdaptiveModeKind::Flat,
                selected_direction: None,
                guard: MultiGuardPolicyReport {
                    deny_regimes: Vec::new(),
                },
                train: Some(stats_report(&selected.train)),
                train_summary: Some(adaptive_mode_train_summary_report(&selected.train_summary)),
                oos: None,
                active_options: option_reports,
                reason: "best_active_mode_prior_tail_below_flat_threshold".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        let fold_stats = adaptive_mode_stats_for_fold(&folds[idx], &variant, &selected);
        if fold_stats.trades == 0 {
            decisions.push(AdaptiveModeDecisionReport {
                report_index: idx,
                train_reports: idx,
                recent_losing_reports,
                recent_worst_report_pnl,
                selected_mode: selected.mode,
                selected_direction: selected.direction.clone(),
                guard: guard_policy_report(&selected.guard),
                train: Some(stats_report(&selected.train)),
                train_summary: Some(adaptive_mode_train_summary_report(&selected.train_summary)),
                oos: None,
                active_options: option_reports,
                reason: "selected_mode_had_no_oos_trades".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        eligible_reports += 1;
        if fold_stats.total_pnl > 0.0 {
            profitable_reports += 1;
        } else if fold_stats.total_pnl < 0.0 {
            losing_reports += 1;
        }
        worst_report_pnl = Some(match worst_report_pnl {
            Some(current) => current.min(fold_stats.total_pnl),
            None => fold_stats.total_pnl,
        });
        oos.merge_from(&fold_stats);
        decisions.push(AdaptiveModeDecisionReport {
            report_index: idx,
            train_reports: idx,
            recent_losing_reports,
            recent_worst_report_pnl,
            selected_mode: selected.mode,
            selected_direction: selected.direction.clone(),
            guard: guard_policy_report(&selected.guard),
            train: Some(stats_report(&selected.train)),
            train_summary: Some(adaptive_mode_train_summary_report(&selected.train_summary)),
            oos: Some(stats_report(&fold_stats)),
            active_options: option_reports,
            reason: "selected_from_prior_tail_ranked_modes".to_string(),
        });
    }

    let fold_forward = AdaptiveModeFoldForwardReport {
        eligible_reports,
        profitable_reports,
        losing_reports,
        abstained_reports,
        worst_report_pnl: worst_report_pnl.unwrap_or(0.0),
        stats: stats_report(&oos),
        decisions,
    };
    let passed = fold_forward.stats.trades >= input.min_oos_trades
        && fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower
        && fold_forward.stats.total_pnl >= input.min_oos_total_pnl
        && fold_forward.profitable_reports >= input.min_oos_profitable_reports
        && fold_forward.worst_report_pnl >= input.min_worst_oos_pnl;

    let mut notes = vec![
        "adaptive mode selector; rerun any selected policy in full harness before promotion"
            .to_string(),
        "mode decisions use only prior reports and rank prior tail before aggregate PnL"
            .to_string(),
        "flat decisions are abstentions and do not contribute OOS trades or PnL".to_string(),
    ];
    if !passed {
        notes.push("candidate did not pass configured OOS gates".to_string());
    }

    AdaptiveModeCandidateReport {
        rank: 0,
        passed,
        variant,
        fold_forward,
        notes,
    }
}

fn adaptive_mode_options(
    prior_folds: &[SelectivityFold],
    input: &StrategyBuilderAdaptiveModeInput,
    variant: &str,
) -> Vec<AdaptiveModeOption> {
    let mut options = Vec::new();
    if let Some((direction, train)) = select_direction_from_prior_folds(
        prior_folds,
        &StrategyBuilderAdaptiveDirectionInput {
            report_paths: Vec::new(),
            min_train_reports: input.min_train_reports,
            min_train_trades: input.min_train_trades,
            min_oos_trades: input.min_oos_trades,
            min_oos_wilson_win_rate_lower: input.min_oos_wilson_win_rate_lower,
            min_oos_total_pnl: input.min_oos_total_pnl,
            min_oos_profitable_reports: input.min_oos_profitable_reports,
            min_worst_oos_pnl: input.min_worst_oos_pnl,
            top: input.top,
        },
        variant,
    ) {
        let option = AdaptiveModeOption {
            mode: AdaptiveModeKind::Direction,
            direction: Some(direction.to_string()),
            guard: Vec::new(),
            train,
            train_summary: adaptive_mode_train_summary_for_direction(
                prior_folds,
                variant,
                direction,
            ),
        };
        if option.train.trades >= input.min_train_trades && option.train.total_pnl > 0.0 {
            options.push(option);
        }
    }

    let guard_input = StrategyBuilderMultiGuardSearchInput {
        report_paths: Vec::new(),
        min_train_reports: input.min_train_reports,
        min_train_trades: input.min_train_trades,
        min_oos_trades: input.min_oos_trades,
        min_oos_wilson_win_rate_lower: input.min_oos_wilson_win_rate_lower,
        min_oos_total_pnl: input.min_oos_total_pnl,
        min_oos_profitable_reports: input.min_oos_profitable_reports,
        min_worst_oos_pnl: input.min_worst_oos_pnl,
        max_rules: input.max_guard_rules,
        min_guard_trades: input.min_guard_trades,
        min_guard_loss_pnl: input.min_guard_loss_pnl,
        min_guard_loss_reports: input.min_guard_loss_reports,
        recent_report_lookback: input.recent_report_lookback,
        pattern_guards: input.pattern_guards,
        top: input.top,
    };
    let guard = learn_multi_guard_from_prior_folds(prior_folds, &guard_input, variant);
    if !guard.is_empty() {
        let train = stats_for_regime_guard(prior_folds, variant, &guard);
        let train_summary = adaptive_mode_train_summary_for_guard(prior_folds, variant, &guard);
        let option = AdaptiveModeOption {
            mode: AdaptiveModeKind::Guarded,
            direction: None,
            guard,
            train,
            train_summary,
        };
        if option.train.trades >= input.min_train_trades && option.train.total_pnl > 0.0 {
            options.push(option);
        }
    }

    options
}

fn compare_adaptive_mode_options(
    left: &AdaptiveModeOption,
    right: &AdaptiveModeOption,
) -> Ordering {
    f64_desc(
        left.train_summary.worst_report_pnl,
        right.train_summary.worst_report_pnl,
    )
    .then_with(|| f64_desc(left.train.total_pnl, right.train.total_pnl))
    .then_with(|| {
        f64_desc(
            wilson_lower(left.train.wins as usize, left.train.trades as usize),
            wilson_lower(right.train.wins as usize, right.train.trades as usize),
        )
    })
    .then_with(|| f64_desc(left.train.profit_factor, right.train.profit_factor))
    .then_with(|| right.train.trades.cmp(&left.train.trades))
    .then_with(|| left.mode.cmp(&right.mode))
}

fn adaptive_mode_stats_for_fold(
    fold: &SelectivityFold,
    variant: &str,
    option: &AdaptiveModeOption,
) -> TradePnlDiagnostics {
    match option.mode {
        AdaptiveModeKind::Flat => TradePnlDiagnostics::default(),
        AdaptiveModeKind::Direction => option
            .direction
            .as_deref()
            .map(|direction| direction_stats_for_fold(fold, variant, direction))
            .unwrap_or_default(),
        AdaptiveModeKind::Guarded => {
            stats_for_regime_guard(std::slice::from_ref(fold), variant, &option.guard)
        }
    }
}

fn adaptive_mode_train_summary_for_direction(
    folds: &[SelectivityFold],
    variant: &str,
    direction: &str,
) -> AdaptiveModeTrainSummary {
    let mut summary = AdaptiveModeTrainSummary::default();
    let mut worst_report_pnl: Option<f64> = None;
    for fold in folds {
        let stats = direction_stats_for_fold(fold, variant, direction);
        update_adaptive_mode_train_summary(&mut summary, &mut worst_report_pnl, &stats);
    }
    summary.worst_report_pnl = worst_report_pnl.unwrap_or(0.0);
    summary
}

fn adaptive_mode_train_summary_for_guard(
    folds: &[SelectivityFold],
    variant: &str,
    guard: &[MultiGuardRule],
) -> AdaptiveModeTrainSummary {
    let mut summary = AdaptiveModeTrainSummary::default();
    let mut worst_report_pnl: Option<f64> = None;
    for fold in folds {
        let stats = stats_for_regime_guard(std::slice::from_ref(fold), variant, guard);
        update_adaptive_mode_train_summary(&mut summary, &mut worst_report_pnl, &stats);
    }
    summary.worst_report_pnl = worst_report_pnl.unwrap_or(0.0);
    summary
}

fn update_adaptive_mode_train_summary(
    summary: &mut AdaptiveModeTrainSummary,
    worst_report_pnl: &mut Option<f64>,
    stats: &TradePnlDiagnostics,
) {
    if stats.trades == 0 {
        return;
    }
    summary.eligible_reports += 1;
    if stats.total_pnl > 0.0 {
        summary.profitable_reports += 1;
    } else if stats.total_pnl < 0.0 {
        summary.losing_reports += 1;
    }
    *worst_report_pnl = Some(match *worst_report_pnl {
        Some(current) => current.min(stats.total_pnl),
        None => stats.total_pnl,
    });
}

fn adaptive_mode_option_report(option: &AdaptiveModeOption) -> AdaptiveModeOptionReport {
    AdaptiveModeOptionReport {
        mode: option.mode,
        direction: option.direction.clone(),
        guard: guard_policy_report(&option.guard),
        train: stats_report(&option.train),
        train_summary: adaptive_mode_train_summary_report(&option.train_summary),
    }
}

fn adaptive_mode_train_summary_report(
    summary: &AdaptiveModeTrainSummary,
) -> AdaptiveModeTrainSummaryReport {
    AdaptiveModeTrainSummaryReport {
        eligible_reports: summary.eligible_reports,
        profitable_reports: summary.profitable_reports,
        losing_reports: summary.losing_reports,
        worst_report_pnl: summary.worst_report_pnl,
    }
}

fn causal_policy_candidate_keys(
    folds: &[SelectivityFold],
    max_require_terms: usize,
) -> Vec<CausalPolicyKey> {
    let mut keys = BTreeSet::new();
    for fold in folds {
        for variant in &fold.variants {
            for regime in &variant.tagged_regimes {
                for require_tags in policy_tag_combinations(&regime.tags, max_require_terms) {
                    keys.insert(CausalPolicyKey {
                        variant: variant.name.clone(),
                        require_tags,
                    });
                }
            }
        }
    }
    keys.into_iter().collect()
}

fn evaluate_causal_policy_candidate(
    folds: &[SelectivityFold],
    input: &StrategyBuilderCausalPolicySearchInput,
    candidate: CausalPolicyKey,
) -> CausalPolicyCandidateReport {
    let mut oos = TradePnlDiagnostics::default();
    let mut eligible_reports = 0_usize;
    let mut profitable_reports = 0_usize;
    let mut losing_reports = 0_usize;
    let mut abstained_reports = 0_usize;
    let mut worst_report_pnl: Option<f64> = None;
    let mut decisions = Vec::new();

    for idx in 0..folds.len() {
        if idx < input.min_train_reports {
            decisions.push(CausalPolicyDecisionReport {
                report_index: idx,
                train_reports: idx,
                policy: causal_policy_report(&CausalPolicy {
                    require_tags: candidate.require_tags.clone(),
                    deny_rules: Vec::new(),
                }),
                train: None,
                oos: None,
                reason: "insufficient_prior_reports".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        let prior_folds = &folds[..idx];
        let deny_rules = learn_causal_policy_deny_rules(
            prior_folds,
            input,
            &candidate.variant,
            &candidate.require_tags,
        );
        let policy = CausalPolicy {
            require_tags: candidate.require_tags.clone(),
            deny_rules,
        };
        let train_stats = stats_for_causal_policy(prior_folds, &candidate.variant, &policy);
        let train_reports_with_trades =
            reports_with_causal_policy_trades(prior_folds, &candidate.variant, &policy);

        if train_reports_with_trades < input.min_train_reports
            || train_stats.trades < input.min_train_trades
            || train_stats.total_pnl <= 0.0
        {
            decisions.push(CausalPolicyDecisionReport {
                report_index: idx,
                train_reports: idx,
                policy: causal_policy_report(&policy),
                train: Some(stats_report(&train_stats)),
                oos: None,
                reason: "policy_prior_stats_failed_train_gates".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        let fold_stats = stats_for_causal_policy(&folds[idx..=idx], &candidate.variant, &policy);
        if fold_stats.trades == 0 {
            decisions.push(CausalPolicyDecisionReport {
                report_index: idx,
                train_reports: idx,
                policy: causal_policy_report(&policy),
                train: Some(stats_report(&train_stats)),
                oos: None,
                reason: "policy_had_no_oos_trades".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        eligible_reports += 1;
        if fold_stats.total_pnl > 0.0 {
            profitable_reports += 1;
        } else if fold_stats.total_pnl < 0.0 {
            losing_reports += 1;
        }
        worst_report_pnl = Some(match worst_report_pnl {
            Some(current) => current.min(fold_stats.total_pnl),
            None => fold_stats.total_pnl,
        });
        oos.merge_from(&fold_stats);
        decisions.push(CausalPolicyDecisionReport {
            report_index: idx,
            train_reports: idx,
            policy: causal_policy_report(&policy),
            train: Some(stats_report(&train_stats)),
            oos: Some(stats_report(&fold_stats)),
            reason: "policy_selected_from_prior_causal_tags".to_string(),
        });
    }

    let fold_forward = CausalPolicyFoldForwardReport {
        eligible_reports,
        profitable_reports,
        losing_reports,
        abstained_reports,
        worst_report_pnl: worst_report_pnl.unwrap_or(0.0),
        stats: stats_report(&oos),
        decisions,
    };
    let passed = fold_forward.stats.trades >= input.min_oos_trades
        && fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower
        && fold_forward.stats.total_pnl >= input.min_oos_total_pnl
        && fold_forward.profitable_reports >= input.min_oos_profitable_reports
        && fold_forward.worst_report_pnl >= input.min_worst_oos_pnl;

    let final_policy = CausalPolicy {
        require_tags: candidate.require_tags.clone(),
        deny_rules: learn_causal_policy_deny_rules(
            folds,
            input,
            &candidate.variant,
            &candidate.require_tags,
        ),
    };
    let aggregate_static_final_policy =
        stats_for_causal_policy(folds, &candidate.variant, &final_policy);

    let mut notes = vec![
        "causal policy selector; rerun final require/deny tags in full harness before promotion"
            .to_string(),
        "fold decisions use only prior report causal-regime evidence".to_string(),
        "require tags are conjunctions and map directly to --require-causal-tag".to_string(),
    ];
    if input.max_deny_terms > 1 {
        notes.push(
            "multi-term deny rules are analytic only unless runtime conjunction-deny support is added"
                .to_string(),
        );
    }
    if !passed {
        notes.push("candidate did not pass configured OOS gates".to_string());
    }

    CausalPolicyCandidateReport {
        rank: 0,
        passed,
        variant: candidate.variant,
        base_require: candidate.require_tags,
        final_policy: causal_policy_report(&final_policy),
        aggregate_static_final_policy: stats_report(&aggregate_static_final_policy),
        fold_forward,
        notes,
    }
}

fn learn_causal_policy_deny_rules(
    prior_folds: &[SelectivityFold],
    input: &StrategyBuilderCausalPolicySearchInput,
    variant: &str,
    require_tags: &BTreeMap<String, String>,
) -> Vec<CausalPolicyRule> {
    if input.max_deny_rules == 0 || input.max_deny_terms == 0 {
        return Vec::new();
    }

    let mut patterns: BTreeMap<String, (BTreeMap<String, String>, TradePnlDiagnostics, usize)> =
        BTreeMap::new();
    for fold in prior_folds {
        let Some(variant_fold) = fold.variants.iter().find(|entry| entry.name == variant) else {
            continue;
        };
        let mut fold_patterns: BTreeMap<String, (BTreeMap<String, String>, TradePnlDiagnostics)> =
            BTreeMap::new();
        for regime in &variant_fold.tagged_regimes {
            if !policy_tags_match(require_tags, &regime.tags) {
                continue;
            }
            for match_tags in policy_tag_combinations(&regime.tags, input.max_deny_terms) {
                if policy_rule_is_redundant_with_require(&match_tags, require_tags) {
                    continue;
                }
                let label = policy_label(&match_tags);
                let entry = fold_patterns
                    .entry(label)
                    .or_insert_with(|| (match_tags, TradePnlDiagnostics::default()));
                entry.1.merge_from(&regime.stats);
            }
        }
        for (label, (match_tags, stats)) in fold_patterns {
            let entry = patterns
                .entry(label)
                .or_insert_with(|| (match_tags, TradePnlDiagnostics::default(), 0_usize));
            entry.1.merge_from(&stats);
            if stats.trades > 0 {
                entry.2 += 1;
            }
        }
    }

    let mut toxic = patterns
        .into_iter()
        .filter_map(|(label, (match_tags, stats, reports_with_trades))| {
            if reports_with_trades < input.min_deny_loss_reports
                || stats.trades < input.min_deny_trades
                || stats.total_pnl >= -input.min_deny_loss_pnl.max(0.0)
            {
                return None;
            }
            Some(CausalPolicyRule {
                label,
                match_tags,
                reports_with_trades,
                stats,
            })
        })
        .collect::<Vec<_>>();

    toxic.sort_by(|a, b| {
        a.stats
            .total_pnl
            .partial_cmp(&b.stats.total_pnl)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                a.stats
                    .profit_factor
                    .partial_cmp(&b.stats.profit_factor)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| b.stats.losses.cmp(&a.stats.losses))
            .then_with(|| b.stats.trades.cmp(&a.stats.trades))
            .then_with(|| a.label.cmp(&b.label))
    });

    let mut selected = Vec::new();
    let mut current = stats_for_causal_policy(
        prior_folds,
        variant,
        &CausalPolicy {
            require_tags: require_tags.clone(),
            deny_rules: Vec::new(),
        },
    );
    for rule in toxic {
        if selected.len() >= input.max_deny_rules {
            break;
        }
        let mut trial_rules = selected.clone();
        trial_rules.push(rule.clone());
        let trial = stats_for_causal_policy(
            prior_folds,
            variant,
            &CausalPolicy {
                require_tags: require_tags.clone(),
                deny_rules: trial_rules,
            },
        );
        if trial.trades < input.min_train_trades {
            continue;
        }
        if trial.total_pnl <= current.total_pnl + 1e-9 {
            continue;
        }
        current = trial;
        selected.push(rule);
    }
    selected
}

fn stats_for_causal_policy(
    folds: &[SelectivityFold],
    variant: &str,
    policy: &CausalPolicy,
) -> TradePnlDiagnostics {
    let mut stats = TradePnlDiagnostics::default();
    for fold in folds {
        let Some(variant_fold) = fold.variants.iter().find(|entry| entry.name == variant) else {
            continue;
        };
        for regime in &variant_fold.tagged_regimes {
            if !policy_tags_match(&policy.require_tags, &regime.tags) {
                continue;
            }
            if policy
                .deny_rules
                .iter()
                .any(|rule| policy_tags_match(&rule.match_tags, &regime.tags))
            {
                continue;
            }
            stats.merge_from(&regime.stats);
        }
    }
    stats
}

fn reports_with_causal_policy_trades(
    folds: &[SelectivityFold],
    variant: &str,
    policy: &CausalPolicy,
) -> usize {
    folds
        .iter()
        .filter(|fold| {
            stats_for_causal_policy(std::slice::from_ref(fold), variant, policy).trades > 0
        })
        .count()
}

fn causal_policy_report(policy: &CausalPolicy) -> CausalPolicyReport {
    let harness_require_args = policy.require_tags.iter().map(tag_arg).collect();
    let harness_deny_args = policy
        .deny_rules
        .iter()
        .filter(|rule| rule.match_tags.len() == 1)
        .flat_map(|rule| rule.match_tags.iter().map(tag_arg))
        .collect();
    CausalPolicyReport {
        require_tags: policy.require_tags.clone(),
        deny_rules: policy
            .deny_rules
            .iter()
            .map(|rule| CausalPolicyRuleReport {
                label: rule.label.clone(),
                match_tags: rule.match_tags.clone(),
                train_reports_with_trades: rule.reports_with_trades,
                train_stats: stats_report(&rule.stats),
            })
            .collect(),
        harness_require_args,
        harness_deny_args,
    }
}

fn evaluate_multi_guard_candidate(
    folds: &[SelectivityFold],
    input: &StrategyBuilderMultiGuardSearchInput,
    variant: String,
) -> MultiGuardCandidateReport {
    let mut oos = TradePnlDiagnostics::default();
    let mut eligible_reports = 0_usize;
    let mut profitable_reports = 0_usize;
    let mut losing_reports = 0_usize;
    let mut abstained_reports = 0_usize;
    let mut worst_report_pnl: Option<f64> = None;
    let mut decisions = Vec::new();

    for idx in 0..folds.len() {
        if idx < input.min_train_reports {
            let (recent_losing_reports, recent_worst_report_pnl) =
                recent_loss_context(&folds[..idx], &variant, input.recent_report_lookback);
            decisions.push(MultiGuardDecisionReport {
                report_index: idx,
                train_reports: idx,
                recent_losing_reports,
                recent_worst_report_pnl,
                guard: MultiGuardPolicyReport {
                    deny_regimes: Vec::new(),
                },
                train: None,
                oos: None,
                reason: "insufficient_prior_reports".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        let guard = learn_multi_guard_from_prior_folds(&folds[..idx], input, &variant);
        let train_stats = stats_for_regime_guard(&folds[..idx], &variant, &guard);
        let (recent_losing_reports, recent_worst_report_pnl) =
            recent_loss_context(&folds[..idx], &variant, input.recent_report_lookback);
        if train_stats.trades < input.min_train_trades || train_stats.total_pnl <= 0.0 {
            decisions.push(MultiGuardDecisionReport {
                report_index: idx,
                train_reports: idx,
                recent_losing_reports,
                recent_worst_report_pnl,
                guard: guard_policy_report(&guard),
                train: Some(stats_report(&train_stats)),
                oos: None,
                reason: "guarded_prior_stats_failed_train_gates".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        let fold_stats = stats_for_regime_guard(&folds[idx..=idx], &variant, &guard);
        if fold_stats.trades == 0 {
            decisions.push(MultiGuardDecisionReport {
                report_index: idx,
                train_reports: idx,
                recent_losing_reports,
                recent_worst_report_pnl,
                guard: guard_policy_report(&guard),
                train: Some(stats_report(&train_stats)),
                oos: None,
                reason: "guard_removed_all_oos_trades".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        eligible_reports += 1;
        if fold_stats.total_pnl > 0.0 {
            profitable_reports += 1;
        } else if fold_stats.total_pnl < 0.0 {
            losing_reports += 1;
        }
        worst_report_pnl = Some(match worst_report_pnl {
            Some(current) => current.min(fold_stats.total_pnl),
            None => fold_stats.total_pnl,
        });
        oos.merge_from(&fold_stats);
        decisions.push(MultiGuardDecisionReport {
            report_index: idx,
            train_reports: idx,
            recent_losing_reports,
            recent_worst_report_pnl,
            guard: guard_policy_report(&guard),
            train: Some(stats_report(&train_stats)),
            oos: Some(stats_report(&fold_stats)),
            reason: "guard_selected_from_prior_regime_stats".to_string(),
        });
    }

    let fold_forward = MultiGuardFoldForwardReport {
        eligible_reports,
        profitable_reports,
        losing_reports,
        abstained_reports,
        worst_report_pnl: worst_report_pnl.unwrap_or(0.0),
        stats: stats_report(&oos),
        decisions,
    };
    let passed = fold_forward.stats.trades >= input.min_oos_trades
        && fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower
        && fold_forward.stats.total_pnl >= input.min_oos_total_pnl
        && fold_forward.profitable_reports >= input.min_oos_profitable_reports
        && fold_forward.worst_report_pnl >= input.min_worst_oos_pnl;

    let final_guard = learn_multi_guard_from_prior_folds(folds, input, &variant);
    let aggregate_static_final_guard = stats_for_regime_guard(folds, &variant, &final_guard);

    let mut notes = vec![
        "multi-guard selector; rerun the final static guard in full harness before promotion"
            .to_string(),
        "fold decisions use only prior full-regime bucket evidence".to_string(),
        "recent loss context is reported from prior folds only and is not allowed to peek at the current fold".to_string(),
    ];
    if !passed {
        notes.push("candidate did not pass configured OOS gates".to_string());
    }

    MultiGuardCandidateReport {
        rank: 0,
        passed,
        variant,
        final_guard: guard_policy_report(&final_guard),
        aggregate_static_final_guard: stats_report(&aggregate_static_final_guard),
        fold_forward,
        notes,
    }
}

fn learn_multi_guard_from_prior_folds(
    prior_folds: &[SelectivityFold],
    input: &StrategyBuilderMultiGuardSearchInput,
    variant: &str,
) -> Vec<MultiGuardRule> {
    let mut patterns: BTreeMap<String, (BTreeMap<String, String>, TradePnlDiagnostics, usize)> =
        BTreeMap::new();
    for fold in prior_folds {
        let Some(variant_fold) = fold.variants.iter().find(|entry| entry.name == variant) else {
            continue;
        };
        let mut fold_patterns: BTreeMap<String, (BTreeMap<String, String>, TradePnlDiagnostics)> =
            BTreeMap::new();
        for (regime, stats) in &variant_fold.regimes {
            for (label, match_tags) in guard_patterns_for_regime(regime, input.pattern_guards) {
                let entry = fold_patterns
                    .entry(label)
                    .or_insert_with(|| (match_tags, TradePnlDiagnostics::default()));
                entry.1.merge_from(stats);
            }
        }
        for (label, (match_tags, stats)) in fold_patterns {
            let entry = patterns
                .entry(label)
                .or_insert_with(|| (match_tags, TradePnlDiagnostics::default(), 0_usize));
            entry.1.merge_from(&stats);
            if stats.trades > 0 {
                entry.2 += 1;
            }
        }
    }

    let mut toxic = patterns
        .into_iter()
        .filter_map(|(label, (match_tags, stats, reports_with_trades))| {
            if reports_with_trades < input.min_guard_loss_reports
                || stats.trades < input.min_guard_trades
                || stats.total_pnl >= -input.min_guard_loss_pnl.max(0.0)
            {
                return None;
            }
            Some(MultiGuardRule {
                label,
                match_tags,
                reports_with_trades,
                stats,
            })
        })
        .collect::<Vec<_>>();

    toxic.sort_by(|a, b| {
        a.stats
            .total_pnl
            .partial_cmp(&b.stats.total_pnl)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                a.stats
                    .profit_factor
                    .partial_cmp(&b.stats.profit_factor)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| b.stats.losses.cmp(&a.stats.losses))
            .then_with(|| b.stats.trades.cmp(&a.stats.trades))
            .then_with(|| a.label.cmp(&b.label))
    });

    let mut selected = Vec::new();
    let mut current = stats_for_regime_guard(prior_folds, variant, &selected);
    for rule in toxic {
        if selected.len() >= input.max_rules {
            break;
        }
        let mut trial_guard = selected.clone();
        trial_guard.push(rule.clone());
        let trial = stats_for_regime_guard(prior_folds, variant, &trial_guard);
        if trial.trades < input.min_train_trades {
            continue;
        }
        if trial.total_pnl <= current.total_pnl + 1e-9 {
            continue;
        }
        current = trial;
        selected.push(rule);
    }
    selected
}

fn stats_for_regime_guard(
    folds: &[SelectivityFold],
    variant: &str,
    guard: &[MultiGuardRule],
) -> TradePnlDiagnostics {
    let mut stats = TradePnlDiagnostics::default();
    for fold in folds {
        let Some(variant_fold) = fold.variants.iter().find(|entry| entry.name == variant) else {
            continue;
        };
        for (regime, regime_stats) in &variant_fold.regimes {
            if guard.iter().any(|rule| guard_rule_matches(rule, regime)) {
                continue;
            }
            stats.merge_from(regime_stats);
        }
    }
    stats
}

fn recent_loss_context(
    prior_folds: &[SelectivityFold],
    variant: &str,
    lookback: usize,
) -> (usize, f64) {
    if lookback == 0 || prior_folds.is_empty() {
        return (0, 0.0);
    }
    let start = prior_folds.len().saturating_sub(lookback);
    let mut losing_reports = 0_usize;
    let mut worst: Option<f64> = None;
    let empty_guard = Vec::new();
    for fold in &prior_folds[start..] {
        let stats = stats_for_regime_guard(std::slice::from_ref(fold), variant, &empty_guard);
        if stats.trades == 0 {
            continue;
        }
        if stats.total_pnl < 0.0 {
            losing_reports += 1;
        }
        worst = Some(match worst {
            Some(current) => current.min(stats.total_pnl),
            None => stats.total_pnl,
        });
    }
    (losing_reports, worst.unwrap_or(0.0))
}

fn guard_policy_report(guard: &[MultiGuardRule]) -> MultiGuardPolicyReport {
    MultiGuardPolicyReport {
        deny_regimes: guard
            .iter()
            .map(|rule| MultiGuardRuleReport {
                regime: rule.label.clone(),
                match_tags: rule.match_tags.clone(),
                train_reports_with_trades: rule.reports_with_trades,
                train_stats: stats_report(&rule.stats),
            })
            .collect(),
    }
}

fn guard_rule_matches(rule: &MultiGuardRule, regime: &str) -> bool {
    let tags = parse_regime_tags(regime);
    rule.match_tags
        .iter()
        .all(|(dimension, value)| tags.get(dimension).is_some_and(|actual| actual == value))
}

fn guard_patterns_for_regime(
    regime: &str,
    pattern_guards: bool,
) -> Vec<(String, BTreeMap<String, String>)> {
    let tags = parse_regime_tags(regime);
    if tags.is_empty() {
        return Vec::new();
    }

    let mut patterns = BTreeMap::new();
    patterns.insert(regime.to_string(), tags.clone());
    if pattern_guards {
        for dimensions in PATTERN_GUARD_DIMENSIONS {
            let mut pattern = BTreeMap::new();
            for dimension in *dimensions {
                let Some(value) = tags.get(*dimension) else {
                    pattern.clear();
                    break;
                };
                pattern.insert((*dimension).to_string(), value.clone());
            }
            if !pattern.is_empty() && pattern.len() < tags.len() {
                patterns.insert(pattern_label(&pattern), pattern);
            }
        }
    }
    patterns.into_iter().collect()
}

fn parse_regime_tags(regime: &str) -> BTreeMap<String, String> {
    regime
        .split('|')
        .filter_map(|part| {
            let (dimension, value) = part.split_once('=')?;
            Some((dimension.to_string(), value.to_string()))
        })
        .collect()
}

fn causal_tags_from_regime(regime: &str) -> BTreeMap<String, String> {
    parse_regime_tags(regime)
        .into_iter()
        .filter_map(|(dimension, value)| {
            let dimension = match dimension.as_str() {
                "dir" => "direction",
                "conf" => "confidence",
                "vol" => "volatility",
                "rev" => "reversion",
                "min" => "minutes_remaining",
                "zone" | "price" | "edge" | "z" => dimension.as_str(),
                _ => return None,
            };
            Some((dimension.to_string(), value))
        })
        .collect()
}

fn policy_tag_combinations(
    tags: &BTreeMap<String, String>,
    max_terms: usize,
) -> Vec<BTreeMap<String, String>> {
    let max_terms = max_terms.min(CAUSAL_POLICY_DIMENSIONS.len());
    let entries = CAUSAL_POLICY_DIMENSIONS
        .iter()
        .filter_map(|dimension| {
            tags.get(*dimension)
                .map(|value| ((*dimension).to_string(), value.clone()))
        })
        .collect::<Vec<_>>();
    let mut combinations = Vec::new();
    for terms in 1..=max_terms.min(entries.len()) {
        push_policy_tag_combinations(&entries, terms, 0, &mut Vec::new(), &mut combinations);
    }
    combinations
}

fn push_policy_tag_combinations(
    entries: &[(String, String)],
    remaining: usize,
    start: usize,
    current: &mut Vec<(String, String)>,
    out: &mut Vec<BTreeMap<String, String>>,
) {
    if remaining == 0 {
        out.push(current.iter().cloned().collect());
        return;
    }
    for idx in start..entries.len() {
        current.push(entries[idx].clone());
        push_policy_tag_combinations(entries, remaining - 1, idx + 1, current, out);
        current.pop();
    }
}

fn policy_tags_match(
    expected: &BTreeMap<String, String>,
    actual: &BTreeMap<String, String>,
) -> bool {
    expected
        .iter()
        .all(|(dimension, value)| actual.get(dimension).is_some_and(|actual| actual == value))
}

fn policy_rule_is_redundant_with_require(
    rule: &BTreeMap<String, String>,
    require: &BTreeMap<String, String>,
) -> bool {
    rule.iter().all(|(dimension, value)| {
        require
            .get(dimension)
            .is_some_and(|required| required == value)
    })
}

fn policy_label(tags: &BTreeMap<String, String>) -> String {
    tags.iter().map(tag_arg).collect::<Vec<_>>().join("|")
}

fn tag_arg((dimension, value): (&String, &String)) -> String {
    format!("{dimension}={value}")
}

fn pattern_label(tags: &BTreeMap<String, String>) -> String {
    tags.iter()
        .map(|(dimension, value)| format!("{dimension}={value}"))
        .collect::<Vec<_>>()
        .join("|")
}

const CAUSAL_POLICY_DIMENSIONS: &[&str] = &[
    "direction",
    "zone",
    "price",
    "edge",
    "z",
    "confidence",
    "volatility",
    "reversion",
    "minutes_remaining",
];

const PATTERN_GUARD_DIMENSIONS: &[&[&str]] = &[
    &["zone", "dir", "price", "edge", "z", "conf", "rev", "min"],
    &["zone", "dir", "price", "edge", "z", "conf", "min"],
    &["zone", "dir", "price", "z", "conf", "min"],
    &["zone", "price", "edge", "z", "conf", "vol", "min"],
    &["zone", "price", "edge", "z", "conf", "min"],
    &["zone", "price", "z", "conf", "min"],
    &["zone", "dir", "z", "conf", "vol", "min"],
    &["zone", "dir", "price", "conf", "min"],
    &["zone", "z", "conf", "min"],
    &["price", "z", "conf", "min"],
    &["z", "conf", "min"],
];

fn evaluate_adaptive_direction_candidate(
    folds: &[SelectivityFold],
    input: &StrategyBuilderAdaptiveDirectionInput,
    variant: String,
) -> AdaptiveDirectionCandidateReport {
    let mut oos = TradePnlDiagnostics::default();
    let mut eligible_reports = 0_usize;
    let mut profitable_reports = 0_usize;
    let mut losing_reports = 0_usize;
    let mut abstained_reports = 0_usize;
    let mut worst_report_pnl: Option<f64> = None;
    let mut decisions = Vec::new();

    for idx in 0..folds.len() {
        if idx < input.min_train_reports {
            decisions.push(AdaptiveDirectionDecisionReport {
                report_index: idx,
                train_reports: idx,
                selected_direction: None,
                train: None,
                oos: None,
                reason: "insufficient_prior_reports".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        let selected = select_direction_from_prior_folds(&folds[..idx], input, &variant);
        let Some((direction, train_stats)) = selected else {
            decisions.push(AdaptiveDirectionDecisionReport {
                report_index: idx,
                train_reports: idx,
                selected_direction: None,
                train: None,
                oos: None,
                reason: "no_direction_passed_prior_gates".to_string(),
            });
            abstained_reports += 1;
            continue;
        };

        let fold_stats = direction_stats_for_fold(&folds[idx], &variant, direction);
        if fold_stats.trades == 0 {
            decisions.push(AdaptiveDirectionDecisionReport {
                report_index: idx,
                train_reports: idx,
                selected_direction: Some(direction.to_string()),
                train: Some(stats_report(&train_stats)),
                oos: None,
                reason: "selected_direction_had_no_oos_trades".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        eligible_reports += 1;
        if fold_stats.total_pnl > 0.0 {
            profitable_reports += 1;
        } else if fold_stats.total_pnl < 0.0 {
            losing_reports += 1;
        }
        worst_report_pnl = Some(match worst_report_pnl {
            Some(current) => current.min(fold_stats.total_pnl),
            None => fold_stats.total_pnl,
        });
        oos.merge_from(&fold_stats);
        decisions.push(AdaptiveDirectionDecisionReport {
            report_index: idx,
            train_reports: idx,
            selected_direction: Some(direction.to_string()),
            train: Some(stats_report(&train_stats)),
            oos: Some(stats_report(&fold_stats)),
            reason: "selected_from_prior_direction_stats".to_string(),
        });
    }

    let fold_forward = AdaptiveDirectionFoldForwardReport {
        eligible_reports,
        profitable_reports,
        losing_reports,
        abstained_reports,
        worst_report_pnl: worst_report_pnl.unwrap_or(0.0),
        stats: stats_report(&oos),
        decisions,
    };
    let passed = fold_forward.stats.trades >= input.min_oos_trades
        && fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower
        && fold_forward.stats.total_pnl >= input.min_oos_total_pnl
        && fold_forward.profitable_reports >= input.min_oos_profitable_reports
        && fold_forward.worst_report_pnl >= input.min_worst_oos_pnl;

    let mut notes = vec![
        "adaptive direction selector; rerun selected policy in full harness before promotion"
            .to_string(),
        "fold decisions use only prior report direction buckets".to_string(),
        "flat abstention is used when prior evidence is insufficient or negative".to_string(),
    ];
    if !passed {
        notes.push("candidate did not pass configured OOS gates".to_string());
    }

    AdaptiveDirectionCandidateReport {
        rank: 0,
        passed,
        variant,
        fold_forward,
        notes,
    }
}

fn select_direction_from_prior_folds(
    prior_folds: &[SelectivityFold],
    input: &StrategyBuilderAdaptiveDirectionInput,
    variant: &str,
) -> Option<(&'static str, TradePnlDiagnostics)> {
    ["up", "down"]
        .into_iter()
        .filter_map(|direction| {
            let mut stats = TradePnlDiagnostics::default();
            let mut reports_with_trades = 0_usize;
            for fold in prior_folds {
                let fold_stats = direction_stats_for_fold(fold, variant, direction);
                if fold_stats.trades > 0 {
                    reports_with_trades += 1;
                }
                stats.merge_from(&fold_stats);
            }
            if reports_with_trades < input.min_train_reports
                || stats.trades < input.min_train_trades
                || stats.total_pnl <= 0.0
            {
                return None;
            }
            Some((direction, stats))
        })
        .max_by(|(_, left), (_, right)| {
            left.total_pnl
                .partial_cmp(&right.total_pnl)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    left.avg_pnl
                        .partial_cmp(&right.avg_pnl)
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| left.trades.cmp(&right.trades))
        })
}

fn direction_stats_for_fold(
    fold: &SelectivityFold,
    variant_name: &str,
    direction: &str,
) -> TradePnlDiagnostics {
    let Some(variant) = fold
        .variants
        .iter()
        .find(|variant| variant.name == variant_name)
    else {
        return TradePnlDiagnostics::default();
    };
    variant
        .buckets
        .get(&format!("direction={direction}"))
        .cloned()
        .unwrap_or_default()
}

fn candidate_keys(folds: &[SelectivityFold]) -> Vec<SelectivityCandidateKey> {
    let mut dimensions_by_variant: BTreeMap<String, BTreeMap<String, BTreeSet<String>>> =
        BTreeMap::new();
    for fold in folds {
        for variant in &fold.variants {
            let dimensions = dimensions_by_variant
                .entry(variant.name.clone())
                .or_default();
            for key in variant.buckets.keys() {
                if let Some((dimension, value)) = split_bucket_key(key) {
                    dimensions
                        .entry(dimension.to_string())
                        .or_default()
                        .insert(value.to_string());
                }
            }
        }
    }

    let mut candidates = Vec::new();
    for (variant, dimensions) in dimensions_by_variant {
        for (dimension, values) in dimensions {
            for value in &values {
                candidates.push(SelectivityCandidateKey {
                    variant: variant.clone(),
                    rule: SelectivityRule {
                        dimension: dimension.clone(),
                        value: value.clone(),
                        action: SelectivityAction::AllowOnly,
                    },
                });
                if values.len() > 1 {
                    candidates.push(SelectivityCandidateKey {
                        variant: variant.clone(),
                        rule: SelectivityRule {
                            dimension: dimension.clone(),
                            value: value.clone(),
                            action: SelectivityAction::Deny,
                        },
                    });
                }
            }
        }
    }
    candidates.sort();
    candidates.dedup();
    candidates
}

fn evaluate_selectivity_candidate(
    folds: &[SelectivityFold],
    input: &StrategyBuilderSelectivitySearchInput,
    candidate: SelectivityCandidateKey,
) -> SelectivityCandidateReport {
    let mut aggregate = TradePnlDiagnostics::default();
    for fold in folds {
        aggregate.merge_from(&rule_stats_for_fold(
            fold,
            &candidate.variant,
            &candidate.rule,
        ));
    }

    let mut oos = TradePnlDiagnostics::default();
    let mut eligible_reports = 0_usize;
    let mut profitable_reports = 0_usize;
    let mut losing_reports = 0_usize;
    let mut worst_report_pnl: Option<f64> = None;

    for idx in 0..folds.len() {
        if idx < input.min_train_reports {
            continue;
        }

        let mut train = TradePnlDiagnostics::default();
        let mut train_reports_with_trades = 0_usize;
        for train_fold in &folds[..idx] {
            let fold_stats = rule_stats_for_fold(train_fold, &candidate.variant, &candidate.rule);
            if fold_stats.trades > 0 {
                train_reports_with_trades += 1;
            }
            train.merge_from(&fold_stats);
        }

        if train_reports_with_trades < input.min_train_reports
            || train.trades < input.min_train_trades
            || train.total_pnl <= 0.0
        {
            continue;
        }

        let fold_stats = rule_stats_for_fold(&folds[idx], &candidate.variant, &candidate.rule);
        if fold_stats.trades == 0 {
            continue;
        }

        eligible_reports += 1;
        if fold_stats.total_pnl > 0.0 {
            profitable_reports += 1;
        } else if fold_stats.total_pnl < 0.0 {
            losing_reports += 1;
        }
        worst_report_pnl = Some(match worst_report_pnl {
            Some(current) => current.min(fold_stats.total_pnl),
            None => fold_stats.total_pnl,
        });
        oos.merge_from(&fold_stats);
    }

    let fold_forward = SelectivityFoldForwardReport {
        eligible_reports,
        profitable_reports,
        losing_reports,
        worst_report_pnl: worst_report_pnl.unwrap_or(0.0),
        stats: stats_report(&oos),
    };
    let passed = fold_forward.stats.trades >= input.min_oos_trades
        && fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower
        && fold_forward.stats.total_pnl >= input.min_oos_total_pnl
        && fold_forward.profitable_reports >= input.min_oos_profitable_reports
        && fold_forward.worst_report_pnl >= input.min_worst_oos_pnl;

    let mut notes = vec![
        "aggregate bucket rule; rerun selected candidates in full harness before promotion"
            .to_string(),
        "fold-forward eligibility uses only prior reports".to_string(),
    ];
    if candidate.rule.action == SelectivityAction::Deny {
        notes.push(
            "deny rule is the complement inside one bucket dimension; regime-deny rules abstain from one full interaction bucket"
                .to_string(),
        );
    }
    if !passed {
        notes.push("candidate did not pass configured OOS gates".to_string());
    }

    SelectivityCandidateReport {
        rank: 0,
        passed,
        variant: candidate.variant,
        rule: candidate.rule,
        aggregate: stats_report(&aggregate),
        fold_forward,
        notes,
    }
}

fn rule_stats_for_fold(
    fold: &SelectivityFold,
    variant_name: &str,
    rule: &SelectivityRule,
) -> TradePnlDiagnostics {
    let Some(variant) = fold
        .variants
        .iter()
        .find(|variant| variant.name == variant_name)
    else {
        return TradePnlDiagnostics::default();
    };
    let key = format!("{}={}", rule.dimension, rule.value);
    match rule.action {
        SelectivityAction::AllowOnly => variant.buckets.get(&key).cloned().unwrap_or_default(),
        SelectivityAction::Deny => {
            let mut stats = TradePnlDiagnostics::default();
            for (bucket_key, bucket_stats) in &variant.buckets {
                let Some((dimension, value)) = split_bucket_key(bucket_key) else {
                    continue;
                };
                if dimension == rule.dimension && value != rule.value {
                    stats.merge_from(bucket_stats);
                }
            }
            stats
        }
    }
}

fn stats_report(stats: &TradePnlDiagnostics) -> SelectivityStatsReport {
    SelectivityStatsReport {
        trades: stats.trades,
        wins: stats.wins,
        losses: stats.losses,
        win_rate: stats.win_rate,
        wilson_win_rate_lower: wilson_lower(stats.wins as usize, stats.trades as usize),
        total_pnl: stats.total_pnl,
        avg_pnl: stats.avg_pnl,
        gross_win_pnl: stats.gross_win_pnl,
        gross_loss_pnl: stats.gross_loss_pnl,
        avg_win_pnl: stats.avg_win_pnl,
        avg_loss_pnl: stats.avg_loss_pnl,
        max_win_pnl: stats.max_win_pnl,
        max_loss_pnl: stats.max_loss_pnl,
        profit_factor: stats.profit_factor,
        payoff_ratio: stats.payoff_ratio,
        worst_loss_to_avg_win: stats.worst_loss_to_avg_win,
    }
}

fn split_bucket_key(key: &str) -> Option<(&str, &str)> {
    key.split_once('=')
}

fn variant_report_name(variant: &experiment::VariantReport) -> String {
    variant
        .strategy_params
        .get("name")
        .and_then(|value| value.as_str())
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| variant.strategy.params_hash.clone())
}

fn selectivity_buckets_for_variant(
    variant: &experiment::VariantReport,
) -> BTreeMap<String, TradePnlDiagnostics> {
    let mut buckets = variant.diagnostics.by_causal_bucket.clone();
    for (key, stats) in &variant.diagnostics.by_regime {
        buckets.insert(format!("regime={key}"), stats.clone());
    }
    buckets
}

fn tagged_regimes_from_map(
    regimes: &BTreeMap<String, TradePnlDiagnostics>,
) -> Vec<TaggedRegimeStats> {
    regimes
        .iter()
        .map(|(regime, stats)| TaggedRegimeStats {
            tags: causal_tags_from_regime(regime),
            stats: stats.clone(),
        })
        .collect()
}

fn f64_desc(left: f64, right: f64) -> Ordering {
    right.partial_cmp(&left).unwrap_or(Ordering::Equal)
}

fn only_passive_execution_failures(reject_reasons: &BTreeMap<String, usize>) -> bool {
    !reject_reasons.is_empty()
        && reject_reasons
            .keys()
            .all(|reason| matches!(reason.as_str(), "maker_unfilled" | "post_only_cross"))
}

fn audit_promotion(
    path: &str,
    input: &StrategyBuilderAuditInput,
    checks: &mut Vec<StrategyBuilderCheck>,
) {
    match experiment::read_promotion(path) {
        Ok(artifact) => {
            checks.push(check(
                "promotion.load",
                StrategyBuilderCheckStatus::Ok,
                format!(
                    "{} trades={} win_rate={:.3} pnl={:.2}",
                    path, artifact.trades, artifact.win_rate, artifact.total_pnl
                ),
            ));
            let hash_status = promotion_hash_status(&artifact);
            checks.push(check("promotion.params_hash", hash_status.0, hash_status.1));
            let wilson = wilson_lower(
                (artifact.win_rate * artifact.trades as f64).round() as usize,
                artifact.trades,
            );
            let status = if artifact.trades >= input.min_trades
                && artifact.win_rate >= input.min_win_rate
                && wilson >= input.min_wilson_win_rate_lower
                && artifact.total_pnl >= input.min_total_pnl
            {
                StrategyBuilderCheckStatus::Ok
            } else {
                StrategyBuilderCheckStatus::Fail
            };
            checks.push(check(
                "promotion.robustness",
                status,
                format!(
                    "trades={} win_rate={:.3} wilson95~={:.3} pnl={:.2} gates[min_trades={}, min_wr={:.3}, min_wilson={:.3}, min_pnl={:.2}]",
                    artifact.trades,
                    artifact.win_rate,
                    wilson,
                    artifact.total_pnl,
                    input.min_trades,
                    input.min_win_rate,
                    input.min_wilson_win_rate_lower,
                    input.min_total_pnl,
                ),
            ));
        }
        Err(e) => checks.push(check(
            "promotion.load",
            StrategyBuilderCheckStatus::Fail,
            format!("{path}: {e:#}"),
        )),
    }
}

fn audit_adaptive_probe_reports(
    input: &StrategyBuilderAuditInput,
    checks: &mut Vec<StrategyBuilderCheck>,
) {
    if input.adaptive_report_paths.is_empty() {
        checks.push(check(
            "adaptive_probe.reports",
            StrategyBuilderCheckStatus::Warn,
            "no adaptive breaker probe reports supplied; A+ requires checking whether rearm-only runs change the picture"
                .to_string(),
        ));
        return;
    }

    checks.push(check(
        "adaptive_probe.reports",
        StrategyBuilderCheckStatus::Ok,
        format!(
            "adaptive_reports={} static_reports={}",
            input.adaptive_report_paths.len(),
            input.report_paths.len()
        ),
    ));
    for report_path in &input.adaptive_report_paths {
        match experiment::read_report(report_path) {
            Ok(report) => {
                let best = report.variants.first();
                let best_rearms = best.map(|v| v.diagnostics.adaptive_rearms).unwrap_or(0);
                let best_paused = best
                    .map(|v| v.diagnostics.breaker_paused_events)
                    .unwrap_or(0);
                let best_breaker = best.map(|v| v.breaker_tripped).unwrap_or(false);
                let max_rearms = report
                    .variants
                    .iter()
                    .map(|v| v.diagnostics.adaptive_rearms)
                    .max()
                    .unwrap_or(0);
                let max_paused = report
                    .variants
                    .iter()
                    .map(|v| v.diagnostics.breaker_paused_events)
                    .max()
                    .unwrap_or(0);
                let variants_with_rearms = report
                    .variants
                    .iter()
                    .filter(|v| v.diagnostics.adaptive_rearms > 0)
                    .count();
                let status = if best.is_none() || best_breaker || best_rearms > 0 {
                    StrategyBuilderCheckStatus::Fail
                } else if variants_with_rearms > 0 {
                    StrategyBuilderCheckStatus::Warn
                } else {
                    StrategyBuilderCheckStatus::Ok
                };
                checks.push(check(
                    "adaptive_probe.health",
                    status,
                    format!(
                        "{} variants={} best_breaker={} best_adaptive_rearms={} best_paused_events={} variants_with_rearms={} max_adaptive_rearms={} max_paused_events={}",
                        report_path,
                        report.variants.len(),
                        best_breaker,
                        best_rearms,
                        best_paused,
                        variants_with_rearms,
                        max_rearms,
                        max_paused,
                    ),
                ));
            }
            Err(e) => checks.push(check(
                "adaptive_probe.load",
                StrategyBuilderCheckStatus::Fail,
                format!("{report_path}: {e:#}"),
            )),
        }
    }
}

fn audit_adaptive_drift(
    promotion_path: &str,
    input: &StrategyBuilderAuditInput,
    checks: &mut Vec<StrategyBuilderCheck>,
) {
    let artifact = match experiment::read_promotion(promotion_path) {
        Ok(artifact) => artifact,
        Err(_) => return,
    };
    if input.replay_sessions.is_empty() {
        checks.push(check(
            "adaptive.drift",
            StrategyBuilderCheckStatus::Warn,
            "no replay or bounded integration session supplied; adaptive stale checks need resolved forward outcomes"
                .to_string(),
        ));
        return;
    }

    let baseline_avg_pnl = if artifact.trades == 0 {
        0.0
    } else {
        artifact.total_pnl / artifact.trades as f64
    };
    let mut session_count = 0usize;
    let mut wins = 0_u64;
    let mut losses = 0_u64;
    let mut total_pnl = 0.0;
    let mut breaker_tripped = false;
    let mut system_errors = 0_u64;
    let mut failed_sessions = 0usize;
    for session in &input.replay_sessions {
        match diagnostics::analyze_session(session) {
            Ok(diag) => {
                session_count += 1;
                wins += diag.resolutions.wins;
                losses += diag.resolutions.losses;
                total_pnl += diag.resolutions.total_pnl;
                breaker_tripped |= diag.risk.breaker_tripped;
                system_errors += diag.system.errors;
            }
            Err(e) => {
                failed_sessions += 1;
                checks.push(check(
                    "adaptive.drift",
                    StrategyBuilderCheckStatus::Fail,
                    format!("{session}: {e:#}"),
                ));
            }
        }
    }
    if failed_sessions > 0 {
        return;
    }
    let resolved = wins + losses;
    let (status, reason) = classify_adaptive_drift(
        artifact.win_rate,
        baseline_avg_pnl,
        wins,
        losses,
        total_pnl,
        input.min_shadow_resolutions,
        input.min_win_rate,
        breaker_tripped,
        system_errors,
    );
    checks.push(check(
        "adaptive.drift",
        status,
        format!(
            "sessions={} baseline_wr={:.3} baseline_avg_pnl={:.4} resolved={} wins={} losses={} pnl={:.2}; {}",
            session_count,
            artifact.win_rate,
            baseline_avg_pnl,
            resolved,
            wins,
            losses,
            total_pnl,
            reason
        ),
    ));
}

#[allow(clippy::too_many_arguments)]
fn classify_adaptive_drift(
    baseline_win_rate: f64,
    baseline_avg_pnl: f64,
    session_wins: u64,
    session_losses: u64,
    session_pnl: f64,
    min_samples: u64,
    min_win_rate: f64,
    breaker_tripped: bool,
    system_errors: u64,
) -> (StrategyBuilderCheckStatus, String) {
    let resolved = session_wins + session_losses;
    if resolved < min_samples {
        return (
            StrategyBuilderCheckStatus::Warn,
            format!("sample below drift threshold: resolved={resolved} min_samples={min_samples}"),
        );
    }
    if breaker_tripped {
        return (
            StrategyBuilderCheckStatus::Fail,
            "breaker tripped during forward session".to_string(),
        );
    }
    if system_errors > 0 {
        return (
            StrategyBuilderCheckStatus::Fail,
            format!("system errors during forward session: {system_errors}"),
        );
    }

    let session_win_rate = if resolved == 0 {
        0.0
    } else {
        session_wins as f64 / resolved as f64
    };
    let session_avg_pnl = if resolved == 0 {
        0.0
    } else {
        session_pnl / resolved as f64
    };
    let hard_win_floor = min_win_rate.max(baseline_win_rate - 0.20);
    if session_pnl < 0.0 {
        return (
            StrategyBuilderCheckStatus::Fail,
            format!(
                "negative forward pnl: win_rate={session_win_rate:.3} avg_pnl={session_avg_pnl:.4}"
            ),
        );
    }
    if session_win_rate < hard_win_floor {
        return (
            StrategyBuilderCheckStatus::Fail,
            format!(
                "win-rate decay beyond hard floor: win_rate={session_win_rate:.3} floor={hard_win_floor:.3}"
            ),
        );
    }
    if baseline_avg_pnl > 0.0 && session_avg_pnl < baseline_avg_pnl * 0.25 {
        return (
            StrategyBuilderCheckStatus::Fail,
            format!(
                "expectancy decay beyond hard floor: avg_pnl={session_avg_pnl:.4} floor={:.4}",
                baseline_avg_pnl * 0.25
            ),
        );
    }

    let warn_win_floor = baseline_win_rate - 0.12;
    if session_win_rate < warn_win_floor {
        return (
            StrategyBuilderCheckStatus::Warn,
            format!(
                "win-rate decay warning: win_rate={session_win_rate:.3} warn_floor={warn_win_floor:.3}"
            ),
        );
    }
    if baseline_avg_pnl > 0.0 && session_avg_pnl < baseline_avg_pnl * 0.50 {
        return (
            StrategyBuilderCheckStatus::Warn,
            format!(
                "expectancy decay warning: avg_pnl={session_avg_pnl:.4} warn_floor={:.4}",
                baseline_avg_pnl * 0.50
            ),
        );
    }

    (
        StrategyBuilderCheckStatus::Ok,
        format!(
            "forward performance inside adaptive band: win_rate={session_win_rate:.3} avg_pnl={session_avg_pnl:.4}"
        ),
    )
}

fn promotion_hash_status(artifact: &PromotionArtifact) -> (StrategyBuilderCheckStatus, String) {
    match serde_json::from_value::<StrategyVariant>(artifact.strategy_params.clone()) {
        Ok(variant) => {
            let actual = stable_json_hash(&variant);
            if actual == artifact.selected_strategy.params_hash {
                (
                    StrategyBuilderCheckStatus::Ok,
                    format!("params hash verified: {actual}"),
                )
            } else {
                (
                    StrategyBuilderCheckStatus::Fail,
                    format!(
                        "params hash mismatch: strategy_params {} != selected_strategy {}",
                        actual, artifact.selected_strategy.params_hash
                    ),
                )
            }
        }
        Err(e) => (
            StrategyBuilderCheckStatus::Fail,
            format!("strategy_params do not parse as StrategyVariant: {e}"),
        ),
    }
}

fn next_steps(ok: bool, warn_count: usize, a_plus_ready: bool) -> Vec<String> {
    if !ok {
        return vec![
            "Fix failed checks before promoting or changing runtime gates.".to_string(),
            "Re-run strategy-builder audit with the new report, promotion, and replay sessions."
                .to_string(),
        ];
    }
    if warn_count > 0 {
        return vec![
            "Review warnings and document why they are acceptable for the current gate.".to_string(),
            "Run fresh feed-forward live-replay/backtest diagnostics before using paper mode for irreducible venue plumbing."
                .to_string(),
        ];
    }
    if a_plus_ready {
        return vec![
            "Begin or continue bounded venue-integration checks on the A+ artifact with diagnostics collection."
                .to_string(),
            "Keep live trading gated until offline replay remains A+ across a fresh resolved sample and venue plumbing is clean."
                .to_string(),
        ];
    }
    vec![
        "Run the promoted artifact through feed-forward live-replay until multiple resolved oracle checks agree."
            .to_string(),
        "Only use paper mode after offline validation, and only for checks that need the real venue."
            .to_string(),
    ]
}

fn check(
    name: impl Into<String>,
    status: StrategyBuilderCheckStatus,
    detail: impl Into<String>,
) -> StrategyBuilderCheck {
    StrategyBuilderCheck {
        name: name.into(),
        status,
        detail: detail.into(),
    }
}

fn parse_rfc3339(value: &str, label: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|d| d.with_timezone(&Utc))
        .with_context(|| format!("{label} must be RFC3339"))
}

fn parse_zone_mode(value: &str) -> Result<&str> {
    match value {
        "all" | "early" | "primary" | "late" | "terminal" => Ok(value),
        _ => bail!("--zone-mode must be one of: all, early, primary, late, terminal"),
    }
}

fn feed_forward_windows(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    fold_hours: i64,
) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
    let mut out = Vec::new();
    let mut cur = start;
    while cur <= end {
        let inclusive_end = cur + ChronoDuration::hours(fold_hours - 1);
        let window_end = inclusive_end.min(end);
        out.push((cur, window_end));
        cur = window_end + ChronoDuration::hours(1);
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn push_calibration_window_stages(
    stages: &mut Vec<StrategyBuilderStage>,
    idx: usize,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    eval_cache_dir: &Path,
    scout_reports_dir: &Path,
    reports_dir: &Path,
    checkpoint_dir: &Path,
    profile: &StrategyBuilderProfile,
    zone_mode: &str,
    cache_dir: Option<&String>,
    btc_csv: Option<&String>,
    bankroll: f64,
    latency_ms: u64,
    threads: usize,
    window_minutes: f64,
) -> CalibrationReportPaths {
    let stamp = window_stamp(window_start, window_end);
    let eval_cache_path = eval_cache_dir.join(format!("eval_cache_{stamp}.jsonl"));
    let scout_report_path = scout_reports_dir.join(format!("eval_sweep_{stamp}.json"));
    let report_path = reports_dir.join(format!("harness_sweep_{stamp}.json"));
    let checkpoint = checkpoint_dir.join(&stamp);

    let mut eval_args = vec![
        "polymomentum-engine".to_string(),
        "eval-cache".to_string(),
        "--start".to_string(),
        window_start.to_rfc3339(),
        "--end".to_string(),
        window_end.to_rfc3339(),
        "--window-minutes".to_string(),
        float_arg(window_minutes),
        "--output".to_string(),
        eval_cache_path.display().to_string(),
        "--allow-gamma-fetch".to_string(),
    ];
    if let Some(cache_dir) = cache_dir {
        eval_args.extend(["--cache-dir".to_string(), cache_dir.clone()]);
    }
    if let Some(btc_csv) = btc_csv {
        eval_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
    }
    stages.push(StrategyBuilderStage {
        name: format!("calibration_eval_cache_{idx}"),
        purpose:
            "Replay a historical calibration window into live-like signal/resolution rows for candidate scouting."
                .to_string(),
        command: shell_command(&eval_args),
        outputs: vec![eval_cache_path.display().to_string()],
        verify: vec![
            "summary evaluations > 0 and resolutions > 0".to_string(),
            "BTC tape coverage check passes for the full calibration window".to_string(),
        ],
        resource_policy:
            "Run on a dev box for multi-hour windows; safe on VPS only for short diagnostics."
                .to_string(),
    });

    let mut scout_args = vec![
        "polymomentum-engine".to_string(),
        "sweep".to_string(),
        "--session".to_string(),
        eval_cache_path.display().to_string(),
        "--bankroll".to_string(),
        money_arg(bankroll),
        "--position-pct".to_string(),
        profile.position_pct.to_string(),
        "--max-per-market-usd".to_string(),
        profile.max_per_market_usd.to_string(),
        "--min-trades".to_string(),
        "30".to_string(),
        "--grid".to_string(),
        "--zone-mode".to_string(),
        zone_mode.to_string(),
        "--top".to_string(),
        "25".to_string(),
        "--report-json".to_string(),
        scout_report_path.display().to_string(),
        "--conf".to_string(),
        profile.conf.to_string(),
        "--z".to_string(),
        profile.z.to_string(),
        "--edge".to_string(),
        profile.edge.to_string(),
        format!("--ev-buffer={}", profile.ev_buffer),
        format!("--min-price={}", profile.min_price),
        format!("--max-price={}", profile.max_price),
        format!("--min-reversion-count={}", profile.min_reversion_count),
        format!("--max-reversion-count={}", profile.max_reversion_count),
        format!("--settlement-floor={}", profile.settlement_floor),
        format!(
            "--settlement-guard-minutes={}",
            profile.settlement_guard_minutes
        ),
        format!(
            "--settlement-sigma-buffer={}",
            profile.settlement_sigma_buffer
        ),
        format!("--micro-max-spread={}", profile.micro_max_spread),
        format!("--micro-min-depth={}", profile.micro_min_depth),
        format!("--micro-min-pressure={}", profile.micro_min_pressure),
    ];
    if profile.also_maker {
        scout_args.push("--also-maker".to_string());
    }
    stages.push(StrategyBuilderStage {
        name: format!("calibration_eval_sweep_{idx}"),
        purpose:
            "Search a broad strategy grid over the cached calibration stream before spending full L2 harness time."
                .to_string(),
        command: shell_command(&scout_args),
        outputs: vec![scout_report_path.display().to_string()],
        verify: vec![
            "top variants have enough resolved calibration trades before being trusted".to_string(),
            "candidate parameters are re-run through harness-sweep before promotion".to_string(),
        ],
        resource_policy:
            "CPU-light compared with raw replay; still keep broad multi-window searches on the dev box."
                .to_string(),
    });

    let mut harness_base_args = vec![
        "polymomentum-engine".to_string(),
        "harness-sweep".to_string(),
        "--start".to_string(),
        window_start.to_rfc3339(),
        "--end".to_string(),
        window_end.to_rfc3339(),
        "--bankroll".to_string(),
        money_arg(bankroll),
        "--latency-ms".to_string(),
        latency_ms.to_string(),
        "--window-minutes".to_string(),
        float_arg(window_minutes),
        "--position-pct".to_string(),
        profile.position_pct.to_string(),
        "--max-per-market-usd".to_string(),
        profile.max_per_market_usd.to_string(),
        "--max-total-exposure-usd".to_string(),
        profile.max_total_exposure_usd.to_string(),
        "--max-projected-stressed-drawdown-pct".to_string(),
        profile.max_projected_stressed_drawdown_pct.to_string(),
        "--degraded-after-losses".to_string(),
        profile.degraded_after_losses.to_string(),
        "--degraded-after-drawdown-pct".to_string(),
        profile.degraded_after_drawdown_pct.to_string(),
        "--degraded-min-z".to_string(),
        profile.degraded_min_z.to_string(),
        "--degraded-max-price".to_string(),
        profile.degraded_max_price.to_string(),
        "--zone-mode".to_string(),
        zone_mode.to_string(),
        "--conf".to_string(),
        profile.conf.to_string(),
        "--z".to_string(),
        profile.z.to_string(),
        "--edge".to_string(),
        profile.edge.to_string(),
        format!("--ev-buffer={}", profile.ev_buffer),
        format!("--min-price={}", profile.min_price),
        format!("--max-price={}", profile.max_price),
        format!("--min-reversion-count={}", profile.min_reversion_count),
        format!("--max-reversion-count={}", profile.max_reversion_count),
        format!("--settlement-floor={}", profile.settlement_floor),
        format!(
            "--settlement-guard-minutes={}",
            profile.settlement_guard_minutes
        ),
        format!(
            "--settlement-sigma-buffer={}",
            profile.settlement_sigma_buffer
        ),
        format!("--micro-max-spread={}", profile.micro_max_spread),
        format!("--micro-min-depth={}", profile.micro_min_depth),
        format!("--micro-min-pressure={}", profile.micro_min_pressure),
        "--threads".to_string(),
        threads.to_string(),
    ];
    if profile.also_maker {
        harness_base_args.push("--also-maker".to_string());
    }
    if profile.degraded_force_taker {
        harness_base_args.push("--degraded-force-taker".to_string());
    }
    if let Some(cache_dir) = cache_dir {
        harness_base_args.extend(["--cache-dir".to_string(), cache_dir.clone()]);
    }
    if let Some(btc_csv) = btc_csv {
        harness_base_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
    }
    let mut args = harness_base_args.clone();
    args.extend([
        "--checkpoint".to_string(),
        checkpoint.display().to_string(),
        "--report-json".to_string(),
        report_path.display().to_string(),
    ]);
    stages.push(StrategyBuilderStage {
        name: format!("calibration_harness_sweep_{idx}"),
        purpose:
            "Fit candidate parameters on a calibration window that is strictly before any later holdout using it."
                .to_string(),
        command: shell_command(&args),
        outputs: vec![report_path.display().to_string()],
        verify: vec![
            "report JSON exists and data_manifest.complete=true".to_string(),
            "best variant has breaker_tripped=false and diagnostics.adaptive_rearms=0".to_string(),
            "this report is used only for later-window promotion, not as holdout evidence".to_string(),
        ],
        resource_policy:
            "Run on a dev box; on the 2-core VPS keep --threads 1 and avoid concurrent heavy scans."
                .to_string(),
    });

    let adaptive_report_path =
        reports_dir.join(format!("harness_sweep_adaptive_rearm_{stamp}.json"));
    let adaptive_checkpoint = checkpoint_dir.join(format!("{stamp}_adaptive_rearm"));
    let mut adaptive_args = harness_base_args;
    adaptive_args.extend([
        "--checkpoint".to_string(),
        adaptive_checkpoint.display().to_string(),
        "--report-json".to_string(),
        adaptive_report_path.display().to_string(),
        "--adaptive-health-rearm-minutes".to_string(),
        "15".to_string(),
    ]);
    stages.push(StrategyBuilderStage {
        name: format!("calibration_adaptive_breaker_probe_{idx}"),
        purpose:
            "Stress-test whether a calibration window only survives by pausing and rearming the low-win-rate breaker."
                .to_string(),
        command: shell_command(&adaptive_args),
        outputs: vec![adaptive_report_path.display().to_string()],
        verify: vec![
            "diagnostic report is never passed to robust-promote".to_string(),
            "compare diagnostics.adaptive_rearms and breaker_paused_events against the static harness report".to_string(),
            "adaptive_rearms > 0 is research evidence for regime instability, not promotion evidence".to_string(),
        ],
        resource_policy:
            "Heavy diagnostic; run on the dev box unless the window is tiny and --threads 1 on the VPS."
                .to_string(),
    });

    CalibrationReportPaths {
        static_report: report_path,
        adaptive_report: adaptive_report_path,
    }
}

fn static_calibration_reports(reports: &[CalibrationReportPaths]) -> Vec<PathBuf> {
    reports
        .iter()
        .map(|report| report.static_report.clone())
        .collect()
}

fn promotion_command(reports: &[PathBuf], output: &Path, zone_mode: &str) -> String {
    let mut args = vec![
        "polymomentum-engine".to_string(),
        "experiment".to_string(),
        "robust-promote".to_string(),
    ];
    for report in reports {
        args.extend(["--report".to_string(), report.display().to_string()]);
    }
    let min_zone_count = if zone_mode == "all" { "2" } else { "1" };
    let max_zone_trade_share = if zone_mode == "all" { "0.70" } else { "1.0" };
    args.extend([
        "--output".to_string(),
        output.display().to_string(),
        "--min-trades".to_string(),
        "750".to_string(),
        "--min-losses".to_string(),
        "50".to_string(),
        "--min-zone-count".to_string(),
        min_zone_count.to_string(),
        "--min-win-rate".to_string(),
        "0.63".to_string(),
        "--min-wilson-win-rate-lower".to_string(),
        "0.60".to_string(),
        "--min-total-pnl".to_string(),
        "250".to_string(),
        "--min-sharpe-like".to_string(),
        "0.02".to_string(),
        "--max-zone-trade-share".to_string(),
        max_zone_trade_share.to_string(),
        "--min-reports".to_string(),
        reports.len().to_string(),
        "--min-profitable-reports".to_string(),
        reports.len().to_string(),
        "--min-daily-trades".to_string(),
        "50".to_string(),
        "--min-daily-pnl".to_string(),
        "50".to_string(),
        "--min-neighbor-count".to_string(),
        "2".to_string(),
        "--min-neighbor-positive-rate".to_string(),
        "0.60".to_string(),
        "--max-pbo".to_string(),
        "0.50".to_string(),
        "--min-worst-window-pnl".to_string(),
        "0".to_string(),
    ]);
    shell_command(&args)
}

fn zone_audit_command(reports: &[PathBuf], output: &Path, zone_mode: &str) -> String {
    let max_zone_trade_share = if zone_mode == "all" { "0.70" } else { "1.0" };
    let mut args = vec![
        "polymomentum-engine".to_string(),
        "experiment".to_string(),
        "zone-audit".to_string(),
        "--output".to_string(),
        output.display().to_string(),
        "--max-zone-trade-share".to_string(),
        max_zone_trade_share.to_string(),
        "--min-zone-pnl".to_string(),
        "0".to_string(),
    ];
    for report in reports {
        args.extend(["--report".to_string(), report.display().to_string()]);
    }
    shell_command(&args)
}

fn zone_audit_output_for_promotion(path: &Path) -> PathBuf {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("promotion");
    path.with_file_name(format!("{stem}.zone_audit.json"))
}

#[derive(Debug)]
struct StrategyBuilderProfile {
    name: &'static str,
    conf: &'static str,
    z: &'static str,
    edge: &'static str,
    ev_buffer: &'static str,
    min_price: &'static str,
    max_price: &'static str,
    min_reversion_count: &'static str,
    max_reversion_count: &'static str,
    settlement_floor: &'static str,
    settlement_guard_minutes: &'static str,
    settlement_sigma_buffer: &'static str,
    micro_max_spread: &'static str,
    micro_min_depth: &'static str,
    micro_min_pressure: &'static str,
    position_pct: &'static str,
    max_per_market_usd: &'static str,
    max_total_exposure_usd: &'static str,
    max_projected_stressed_drawdown_pct: &'static str,
    degraded_after_losses: &'static str,
    degraded_after_drawdown_pct: &'static str,
    degraded_min_z: &'static str,
    degraded_max_price: &'static str,
    degraded_force_taker: bool,
    also_maker: bool,
}

impl StrategyBuilderProfile {
    fn from_name(name: &str) -> Result<Self> {
        match name {
            "swift5m" => Ok(Self {
                name: "swift5m",
                conf: "0.15,0.25",
                z: "0.10,0.30",
                edge: "0.00,0.02",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.90",
                min_reversion_count: "0",
                max_reversion_count: "9999",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "0",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.0",
                degraded_max_price: "0.0",
                degraded_force_taker: false,
                also_maker: true,
            }),
            "guarded5m" => Ok(Self {
                name: "guarded5m",
                conf: "0.35,0.45,0.55",
                z: "0.50,1.00,1.25",
                edge: "0.02,0.05,0.07",
                ev_buffer: "-1.0,0.05",
                min_price: "0.10",
                max_price: "0.75",
                min_reversion_count: "0",
                max_reversion_count: "9999",
                settlement_floor: "25.0,35.0",
                settlement_guard_minutes: "5.0",
                settlement_sigma_buffer: "0.20",
                micro_max_spread: "0.02",
                micro_min_depth: "20.0",
                micro_min_pressure: "0.0,0.10",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "0",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.0",
                degraded_max_price: "0.0",
                degraded_force_taker: false,
                also_maker: true,
            }),
            "a_plus5m" => Ok(Self {
                name: "a_plus5m",
                conf: "0.30,0.35,0.40",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.03",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.75,0.90",
                min_reversion_count: "0",
                max_reversion_count: "9999",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "0",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.0",
                degraded_max_price: "0.0",
                degraded_force_taker: false,
                also_maker: true,
            }),
            "a_plus5m_regime" => Ok(Self {
                name: "a_plus5m_regime",
                conf: "0.30,0.35,0.40",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.03",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.75,0.90",
                min_reversion_count: "0",
                max_reversion_count: "9999",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.12,0.16,0.24",
                degraded_after_losses: "0",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.0",
                degraded_max_price: "0.0",
                degraded_force_taker: false,
                also_maker: true,
            }),
            "a_plus5m_adaptive" => Ok(Self {
                name: "a_plus5m_adaptive",
                conf: "0.30,0.35,0.40",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.03",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.75,0.90",
                min_reversion_count: "0",
                max_reversion_count: "9999",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "1,2",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.90",
                degraded_max_price: "0.0",
                degraded_force_taker: true,
                also_maker: true,
            }),
            "a_plus5m_adaptive_price" => Ok(Self {
                name: "a_plus5m_adaptive_price",
                conf: "0.30,0.35,0.40",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.03",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.75,0.90",
                min_reversion_count: "0",
                max_reversion_count: "9999",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "1,2",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.90",
                degraded_max_price: "0.75,0.90",
                degraded_force_taker: true,
                also_maker: true,
            }),
            "a_plus5m_ev_guard" => Ok(Self {
                name: "a_plus5m_ev_guard",
                conf: "0.40",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.03,0.07",
                ev_buffer: "0.05",
                min_price: "0.10",
                max_price: "0.90",
                min_reversion_count: "0",
                max_reversion_count: "9999",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "2",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.90",
                degraded_max_price: "0.0",
                degraded_force_taker: true,
                also_maker: true,
            }),
            "a_plus5m_causal_guard_selected" => Ok(Self {
                name: "a_plus5m_causal_guard_selected",
                conf: "0.40",
                z: "0.90",
                edge: "0.07",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.85",
                min_reversion_count: "0",
                max_reversion_count: "2",
                settlement_floor: "10.0",
                settlement_guard_minutes: "2.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "2",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.90",
                degraded_max_price: "0.0",
                degraded_force_taker: true,
                also_maker: false,
            }),
            "a_plus5m_reversion_guard" => Ok(Self {
                name: "a_plus5m_reversion_guard",
                conf: "0.50",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.07,0.10",
                ev_buffer: "-1.0",
                min_price: "0.75",
                max_price: "0.85",
                min_reversion_count: "1",
                max_reversion_count: "2",
                settlement_floor: "10.0",
                settlement_guard_minutes: "2.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "1,2",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.90",
                degraded_max_price: "0.75,0.90",
                degraded_force_taker: true,
                also_maker: true,
            }),
            _ => bail!(
                "unknown strategy-builder profile `{name}`; supported profiles: guarded5m, a_plus5m, a_plus5m_regime, a_plus5m_adaptive, a_plus5m_adaptive_price, a_plus5m_ev_guard, a_plus5m_causal_guard_selected, a_plus5m_reversion_guard, swift5m"
            ),
        }
    }
}

fn shell_command(args: &[String]) -> String {
    args.iter()
        .map(|arg| {
            if arg.starts_with("$(") {
                arg.clone()
            } else {
                shell_quote(arg)
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote_path(path: &Path) -> String {
    shell_quote(&path.display().to_string())
}

fn shell_quote(arg: &str) -> String {
    if !arg.is_empty()
        && arg
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_./:=,+".contains(&b))
    {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', "'\\''"))
    }
}

fn compact_stamp(dt: DateTime<Utc>) -> String {
    dt.format("%Y%m%dT%H%M%SZ").to_string()
}

fn window_stamp(start: DateTime<Utc>, end: DateTime<Utc>) -> String {
    format!("{}_{}", compact_stamp(start), compact_stamp(end))
}

fn money_arg(value: f64) -> String {
    format!("{value:.2}")
}

fn float_arg(value: f64) -> String {
    if (value - value.round()).abs() < 1e-9 {
        format!("{value:.0}")
    } else {
        format!("{value}")
    }
}

fn wilson_lower(wins: usize, trades: usize) -> f64 {
    if trades == 0 {
        return 0.0;
    }
    let z = 1.96_f64;
    let n = trades as f64;
    let p = wins as f64 / n;
    let denom = 1.0 + z * z / n;
    let center = p + z * z / (2.0 * n);
    let margin = z * ((p * (1.0 - p) + z * z / (4.0 * n)) / n).sqrt();
    ((center - margin) / denom).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_includes_replay_and_diagnostics_stages() {
        let plan = build_plan(StrategyBuilderPlanInput {
            start: "2026-04-23T00:00:00Z".to_string(),
            end: Some("2026-04-25T23:00:00Z".to_string()),
            out_dir: PathBuf::from("logs/strategy_builder/test"),
            cache_dir: Some("data/pmxt_cache".to_string()),
            btc_csv: Some("data/btc.csv".to_string()),
            bankroll: 100.0,
            latency_ms: 50,
            threads: 4,
            window_minutes: 5.0,
            fold_hours: 24,
            profile: "guarded5m".to_string(),
            zone_mode: "primary".to_string(),
            promotion_output: None,
        })
        .unwrap();

        assert_eq!(plan.stages.len(), 29);
        assert_eq!(plan.zone_mode, "primary");
        assert_eq!(plan.fold_hours, 24);
        assert!(plan
            .stages
            .iter()
            .any(|s| s.command.contains("eval-cache") && s.command.contains("--window-minutes 5")));
        assert!(plan
            .stages
            .iter()
            .any(|s| s.command.contains("sweep") && s.command.contains("--grid")));
        assert!(plan
            .stages
            .iter()
            .any(|s| s.command.contains("live-replay") && s.command.contains("--report-json")));
        assert!(plan
            .stages
            .iter()
            .any(|s| s.command.contains("robust-promote") && s.command.contains("--max-pbo")));
        assert!(plan.stages.iter().any(|s| {
            s.command.contains("experiment zone-audit")
                && s.command.contains("--max-zone-trade-share 1.0")
                && s.command.contains("--min-zone-pnl 0")
        }));
        assert!(plan.stages.iter().any(|s| {
            s.name == "final_zone_audit"
                && s.outputs.iter().any(|o| o.ends_with(".zone_audit.json"))
        }));
        assert!(plan.stages.iter().any(|s| {
            s.name.starts_with("calibration_adaptive_breaker_probe_")
                && s.command.contains("--adaptive-health-rearm-minutes 15")
                && s.command.contains("harness_sweep_adaptive_rearm_")
        }));
        assert!(plan
            .stages
            .iter()
            .filter(|s| s.command.contains("harness-sweep"))
            .all(|s| {
                s.command.contains("--also-maker")
                    && s.command.contains("--zone-mode primary")
                    && s.command.contains("--position-pct 0.05")
                    && s.command
                        .contains("--max-projected-stressed-drawdown-pct 0.24")
                    && !s.command.contains("--also-maker true")
            }));
        assert!(plan
            .stages
            .iter()
            .any(|s| s.name == "adaptive_health_audit"));
        assert!(plan.stages.iter().any(|s| {
            s.name.starts_with("feed_forward_causality_")
                && s.command.contains("diagnostics causality")
        }));
        assert!(!plan
            .stages
            .iter()
            .any(|s| s.name == "cached_live_replay_shadow"));

        let fold1_promote = plan
            .stages
            .iter()
            .find(|s| s.name == "feed_forward_promote_1")
            .unwrap();
        assert!(fold1_promote
            .command
            .contains("harness_sweep_20260423T000000Z_20260423T230000Z.json"));
        assert!(!fold1_promote.command.contains("adaptive_rearm"));
        assert!(!fold1_promote
            .command
            .contains("harness_sweep_20260424T000000Z_20260424T230000Z.json"));

        let fold1_replay = plan
            .stages
            .iter()
            .find(|s| s.name == "feed_forward_holdout_replay_1")
            .unwrap();
        assert!(fold1_replay
            .command
            .contains("--start 2026-04-24T00:00:00+00:00"));
        assert!(fold1_replay.command.contains("promotion_ff_fold_01"));
        assert!(fold1_replay
            .command
            .contains("--settlement-alignment-ready"));

        let fold1_audit = plan
            .stages
            .iter()
            .find(|s| s.name == "feed_forward_audit_1")
            .unwrap();
        assert!(fold1_audit.command.contains("--adaptive-report"));
        assert!(fold1_audit
            .command
            .contains("harness_sweep_adaptive_rearm_20260423T000000Z_20260423T230000Z.json"));
    }

    #[test]
    fn plan_rejects_single_fold_to_prevent_lookahead() {
        let err = build_plan(StrategyBuilderPlanInput {
            start: "2026-04-25T10:00:00Z".to_string(),
            end: Some("2026-04-25T17:00:00Z".to_string()),
            out_dir: PathBuf::from("logs/strategy_builder/test"),
            cache_dir: None,
            btc_csv: None,
            bankroll: 100.0,
            latency_ms: 50,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 24,
            profile: "swift5m".to_string(),
            zone_mode: "primary".to_string(),
            promotion_output: None,
        })
        .unwrap_err();
        assert!(err.to_string().contains("at least two folds"));
    }

    #[test]
    fn wilson_lower_is_conservative() {
        let lower = wilson_lower(560, 813);
        assert!(lower > 0.65 && lower < 0.66);
    }

    #[test]
    fn selectivity_search_prefers_feed_forward_down_rule() {
        let folds = vec![
            selectivity_fold(vec![
                ("direction=down", pnl_stats(4, 0, 4.0, 0.0)),
                ("direction=up", pnl_stats(0, 2, 0.0, -2.0)),
            ]),
            selectivity_fold(vec![
                ("direction=down", pnl_stats(4, 0, 4.0, 0.0)),
                ("direction=up", pnl_stats(1, 2, 1.0, -2.0)),
            ]),
            selectivity_fold(vec![
                ("direction=down", pnl_stats(5, 0, 5.0, 0.0)),
                ("direction=up", pnl_stats(0, 3, 0.0, -3.0)),
            ]),
            selectivity_fold(vec![
                ("direction=down", pnl_stats(5, 0, 5.0, 0.0)),
                ("direction=up", pnl_stats(1, 3, 1.0, -3.0)),
            ]),
        ];
        let search = selectivity_search_from_folds(&folds, &selectivity_input(20));

        let down_rule = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.variant == "candidate"
                    && candidate.rule.dimension == "direction"
                    && candidate.rule.value == "down"
                    && candidate.rule.action == SelectivityAction::AllowOnly
            })
            .expect("down allow-only candidate");

        assert!(search.ok);
        assert!(down_rule.passed);
        assert_eq!(down_rule.fold_forward.eligible_reports, 2);
        assert_eq!(down_rule.fold_forward.stats.trades, 10);
        assert_eq!(down_rule.fold_forward.stats.wins, 10);
        assert!(down_rule.fold_forward.stats.total_pnl > 9.0);
    }

    #[test]
    fn selectivity_search_does_not_promote_future_luck() {
        let folds = vec![
            selectivity_fold(vec![
                ("direction=down", pnl_stats(0, 2, 0.0, -2.0)),
                ("direction=up", pnl_stats(1, 0, 1.0, 0.0)),
            ]),
            selectivity_fold(vec![
                ("direction=down", pnl_stats(0, 2, 0.0, -2.0)),
                ("direction=up", pnl_stats(1, 0, 1.0, 0.0)),
            ]),
            selectivity_fold(vec![
                ("direction=down", pnl_stats(12, 0, 12.0, 0.0)),
                ("direction=up", pnl_stats(0, 2, 0.0, -2.0)),
            ]),
        ];
        let search = selectivity_search_from_folds(&folds, &selectivity_input(20));

        let down_rule = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.variant == "candidate"
                    && candidate.rule.dimension == "direction"
                    && candidate.rule.value == "down"
                    && candidate.rule.action == SelectivityAction::AllowOnly
            })
            .expect("down allow-only candidate");

        assert!(!search.ok);
        assert!(down_rule.aggregate.total_pnl > 0.0);
        assert_eq!(down_rule.fold_forward.eligible_reports, 0);
        assert_eq!(down_rule.fold_forward.stats.trades, 0);
        assert!(!down_rule.passed);
    }

    #[test]
    fn selectivity_search_can_deny_full_regime_interaction() {
        let folds = vec![
            selectivity_fold(vec![
                (
                    "regime=zone=early|dir=up|price=0.50_0.75",
                    pnl_stats(4, 0, 4.0, 0.0),
                ),
                (
                    "regime=zone=early|dir=down|price=0.50_0.75",
                    pnl_stats(0, 2, 0.0, -8.0),
                ),
            ]),
            selectivity_fold(vec![
                (
                    "regime=zone=early|dir=up|price=0.50_0.75",
                    pnl_stats(5, 0, 5.0, 0.0),
                ),
                (
                    "regime=zone=early|dir=down|price=0.50_0.75",
                    pnl_stats(0, 2, 0.0, -8.0),
                ),
            ]),
            selectivity_fold(vec![
                (
                    "regime=zone=early|dir=up|price=0.50_0.75",
                    pnl_stats(5, 0, 5.0, 0.0),
                ),
                (
                    "regime=zone=early|dir=down|price=0.50_0.75",
                    pnl_stats(0, 2, 0.0, -8.0),
                ),
            ]),
        ];
        let mut input = selectivity_input(20);
        input.min_train_reports = 1;
        let search = selectivity_search_from_folds(&folds, &input);

        let deny_toxic_regime = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.variant == "candidate"
                    && candidate.rule.dimension == "regime"
                    && candidate.rule.value == "zone=early|dir=down|price=0.50_0.75"
                    && candidate.rule.action == SelectivityAction::Deny
            })
            .expect("regime deny candidate");

        assert!(search.ok);
        assert!(deny_toxic_regime.passed);
        assert_eq!(deny_toxic_regime.fold_forward.eligible_reports, 2);
        assert_eq!(deny_toxic_regime.fold_forward.stats.trades, 10);
        assert!(deny_toxic_regime.fold_forward.stats.total_pnl > 9.0);
    }

    #[test]
    fn multi_guard_search_combines_prior_toxic_regimes() {
        let folds = vec![
            selectivity_fold(vec![
                (
                    "regime=zone=early|dir=up|price=0.50_0.75",
                    pnl_stats(4, 0, 4.0, 0.0),
                ),
                (
                    "regime=zone=early|dir=down|price=0.75_0.90",
                    pnl_stats(0, 2, 0.0, -8.0),
                ),
                (
                    "regime=zone=primary|dir=up|price=0.75_0.90",
                    pnl_stats(0, 1, 0.0, -3.0),
                ),
            ]),
            selectivity_fold(vec![
                (
                    "regime=zone=early|dir=up|price=0.50_0.75",
                    pnl_stats(4, 0, 4.0, 0.0),
                ),
                (
                    "regime=zone=early|dir=down|price=0.75_0.90",
                    pnl_stats(0, 1, 0.0, -4.0),
                ),
                (
                    "regime=zone=primary|dir=up|price=0.75_0.90",
                    pnl_stats(0, 1, 0.0, -3.0),
                ),
            ]),
            selectivity_fold(vec![
                (
                    "regime=zone=early|dir=up|price=0.50_0.75",
                    pnl_stats(5, 0, 5.0, 0.0),
                ),
                (
                    "regime=zone=early|dir=down|price=0.75_0.90",
                    pnl_stats(0, 1, 0.0, -4.0),
                ),
                (
                    "regime=zone=primary|dir=up|price=0.75_0.90",
                    pnl_stats(0, 1, 0.0, -3.0),
                ),
            ]),
        ];

        let search = multi_guard_search_from_folds(&folds, &multi_guard_input(5));
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("multi-guard candidate");

        assert!(search.ok);
        assert!(candidate.passed);
        assert_eq!(candidate.fold_forward.eligible_reports, 1);
        assert_eq!(candidate.fold_forward.stats.trades, 5);
        assert_eq!(candidate.fold_forward.stats.total_pnl, 5.0);
        assert_eq!(
            candidate.fold_forward.decisions[2].guard.deny_regimes.len(),
            2
        );
        assert!(candidate
            .final_guard
            .deny_regimes
            .iter()
            .any(|rule| rule.regime == "zone=early|dir=down|price=0.75_0.90"));
    }

    #[test]
    fn multi_guard_search_does_not_deny_future_only_loss() {
        let folds = vec![
            selectivity_fold(vec![
                (
                    "regime=zone=early|dir=up|price=0.50_0.75",
                    pnl_stats(2, 0, 2.0, 0.0),
                ),
                (
                    "regime=zone=primary|dir=down|price=0.75_0.90",
                    pnl_stats(1, 0, 1.0, 0.0),
                ),
            ]),
            selectivity_fold(vec![
                (
                    "regime=zone=early|dir=up|price=0.50_0.75",
                    pnl_stats(2, 0, 2.0, 0.0),
                ),
                (
                    "regime=zone=primary|dir=down|price=0.75_0.90",
                    pnl_stats(1, 0, 1.0, 0.0),
                ),
            ]),
            selectivity_fold(vec![
                (
                    "regime=zone=early|dir=up|price=0.50_0.75",
                    pnl_stats(2, 0, 2.0, 0.0),
                ),
                (
                    "regime=zone=primary|dir=down|price=0.75_0.90",
                    pnl_stats(0, 2, 0.0, -10.0),
                ),
            ]),
        ];

        let mut input = multi_guard_input(5);
        input.min_worst_oos_pnl = 0.0;
        let search = multi_guard_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("multi-guard candidate");
        let decision = &candidate.fold_forward.decisions[2];

        assert!(!search.ok);
        assert!(decision.guard.deny_regimes.is_empty());
        assert_eq!(decision.oos.as_ref().unwrap().total_pnl, -8.0);
        assert!(!candidate.passed);
    }

    #[test]
    fn multi_guard_pattern_search_generalizes_prior_toxic_context() {
        let prior_bad =
            "regime=zone=early|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=lt_0.50|vol=lt_0.40|rev=1_2|min=2_4";
        let sibling_bad =
            "regime=zone=early|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=lt_0.50|vol=0.40_0.80|rev=1_2|min=2_4";
        let good =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![
                (good, pnl_stats(4, 0, 4.0, 0.0)),
                (prior_bad, pnl_stats(0, 2, 0.0, -8.0)),
            ]),
            selectivity_fold(vec![
                (good, pnl_stats(4, 0, 4.0, 0.0)),
                (prior_bad, pnl_stats(0, 1, 0.0, -4.0)),
            ]),
            selectivity_fold(vec![
                (good, pnl_stats(5, 0, 5.0, 0.0)),
                (sibling_bad, pnl_stats(0, 1, 0.0, -5.0)),
            ]),
        ];

        let mut input = multi_guard_input(5);
        input.pattern_guards = true;
        input.max_rules = 1;
        input.min_guard_loss_reports = 2;
        let search = multi_guard_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("multi-guard candidate");
        let guard = &candidate.fold_forward.decisions[2].guard.deny_regimes[0];

        assert!(search.ok);
        assert!(candidate.passed);
        assert_eq!(candidate.fold_forward.stats.total_pnl, 5.0);
        assert_eq!(
            guard.match_tags.get("zone").map(String::as_str),
            Some("early")
        );
        assert_eq!(
            guard.match_tags.get("conf").map(String::as_str),
            Some("lt_0.50")
        );
        assert!(!guard.match_tags.contains_key("vol"));
    }

    #[test]
    fn causal_policy_search_selects_feed_forward_interaction() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let early_down =
            "regime=zone=early|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=lt_0.50|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![
                (primary_down, pnl_stats(4, 0, 4.0, 0.0)),
                (early_down, pnl_stats(0, 2, 0.0, -3.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(4, 0, 4.0, 0.0)),
                (early_down, pnl_stats(0, 2, 0.0, -3.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(5, 0, 5.0, 0.0)),
                (early_down, pnl_stats(0, 2, 0.0, -3.0)),
            ]),
        ];

        let mut input = causal_policy_input(200);
        input.max_require_terms = 2;
        input.max_deny_rules = 0;
        let search = causal_policy_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.base_require.get("direction").map(String::as_str) == Some("down")
                    && candidate.base_require.get("zone").map(String::as_str) == Some("primary")
            })
            .expect("primary/down policy");

        assert!(search.ok);
        assert!(candidate.passed);
        assert_eq!(candidate.fold_forward.eligible_reports, 1);
        assert_eq!(candidate.fold_forward.stats.trades, 5);
        assert_eq!(candidate.fold_forward.stats.total_pnl, 5.0);
        assert_eq!(
            candidate.final_policy.harness_require_args,
            vec!["direction=down".to_string(), "zone=primary".to_string()]
        );
    }

    #[test]
    fn causal_policy_search_does_not_promote_future_only_interaction() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(0, 2, 0.0, -4.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 2, 0.0, -4.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(12, 0, 20.0, 0.0))]),
        ];

        let mut input = causal_policy_input(200);
        input.max_require_terms = 2;
        let search = causal_policy_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.base_require.get("direction").map(String::as_str) == Some("down")
                    && candidate.base_require.get("zone").map(String::as_str) == Some("primary")
            })
            .expect("primary/down policy");

        assert!(!search.ok);
        assert!(!candidate.passed);
        assert_eq!(candidate.fold_forward.eligible_reports, 0);
        assert_eq!(candidate.fold_forward.stats.trades, 0);
        assert_eq!(
            candidate.fold_forward.decisions[2].reason,
            "policy_prior_stats_failed_train_gates"
        );
    }

    #[test]
    fn causal_policy_search_learns_prior_only_single_tag_deny() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let early_down =
            "regime=zone=early|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![
                (primary_down, pnl_stats(4, 0, 4.0, 0.0)),
                (early_down, pnl_stats(0, 2, 0.0, -3.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(4, 0, 4.0, 0.0)),
                (early_down, pnl_stats(0, 2, 0.0, -3.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(5, 0, 5.0, 0.0)),
                (early_down, pnl_stats(0, 2, 0.0, -5.0)),
            ]),
        ];

        let mut input = causal_policy_input(200);
        input.max_require_terms = 1;
        input.max_deny_rules = 1;
        input.min_deny_loss_reports = 2;
        let search = causal_policy_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.base_require.len() == 1
                    && candidate.base_require.get("direction").map(String::as_str) == Some("down")
            })
            .expect("direction/down policy");
        let decision = &candidate.fold_forward.decisions[2];

        assert!(search.ok);
        assert!(candidate.passed);
        assert_eq!(decision.oos.as_ref().unwrap().total_pnl, 5.0);
        assert_eq!(
            decision.policy.deny_rules[0]
                .match_tags
                .get("zone")
                .map(String::as_str),
            Some("early")
        );
        assert_eq!(
            decision.policy.harness_deny_args,
            vec!["zone=early".to_string()]
        );
    }

    #[test]
    fn adaptive_mode_search_does_not_flat_future_only_loss() {
        let folds = vec![
            selectivity_fold(vec![
                ("direction=up", pnl_stats(2, 0, 2.0, 0.0)),
                ("direction=down", pnl_stats(0, 1, 0.0, -1.0)),
            ]),
            selectivity_fold(vec![
                ("direction=up", pnl_stats(2, 0, 2.0, 0.0)),
                ("direction=down", pnl_stats(0, 1, 0.0, -1.0)),
            ]),
            selectivity_fold(vec![
                ("direction=up", pnl_stats(0, 2, 0.0, -8.0)),
                ("direction=down", pnl_stats(2, 0, 2.0, 0.0)),
            ]),
        ];

        let search = adaptive_mode_search_from_folds(&folds, &adaptive_mode_input(5));
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("adaptive mode candidate");
        let decision = &candidate.fold_forward.decisions[2];

        assert!(!search.ok);
        assert_eq!(decision.selected_mode, AdaptiveModeKind::Direction);
        assert_eq!(decision.selected_direction.as_deref(), Some("up"));
        assert_eq!(decision.oos.as_ref().unwrap().total_pnl, -8.0);
    }

    #[test]
    fn adaptive_mode_search_flats_when_prior_tail_is_too_bad() {
        let folds = vec![
            selectivity_fold(vec![("direction=up", pnl_stats(10, 0, 10.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(0, 1, 0.0, -6.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(4, 0, 4.0, 0.0))]),
        ];

        let mut input = adaptive_mode_input(5);
        input.flat_if_worst_train_below = -5.0;
        let search = adaptive_mode_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("adaptive mode candidate");
        let decision = &candidate.fold_forward.decisions[2];

        assert!(!search.ok);
        assert_eq!(decision.selected_mode, AdaptiveModeKind::Flat);
        assert_eq!(
            decision.reason,
            "best_active_mode_prior_tail_below_flat_threshold"
        );
        assert!(decision.oos.is_none());
        assert_eq!(candidate.fold_forward.stats.trades, 0);
    }

    #[test]
    fn adaptive_direction_search_switches_only_after_prior_evidence_changes() {
        let folds = vec![
            selectivity_fold(vec![
                ("direction=up", pnl_stats(3, 0, 3.0, 0.0)),
                ("direction=down", pnl_stats(1, 0, 1.0, 0.0)),
            ]),
            selectivity_fold(vec![
                ("direction=up", pnl_stats(3, 0, 3.0, 0.0)),
                ("direction=down", pnl_stats(1, 0, 1.0, 0.0)),
            ]),
            selectivity_fold(vec![
                ("direction=up", pnl_stats(0, 2, 0.0, -10.0)),
                ("direction=down", pnl_stats(4, 0, 8.0, 0.0)),
            ]),
            selectivity_fold(vec![
                ("direction=up", pnl_stats(0, 2, 0.0, -2.0)),
                ("direction=down", pnl_stats(10, 0, 20.0, 0.0)),
            ]),
            selectivity_fold(vec![
                ("direction=up", pnl_stats(0, 2, 0.0, -2.0)),
                ("direction=down", pnl_stats(10, 0, 20.0, 0.0)),
            ]),
        ];
        let search = adaptive_direction_search_from_folds(&folds, &adaptive_input(5));
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("adaptive candidate");

        assert!(search.ok);
        assert!(candidate.passed);
        assert_eq!(candidate.fold_forward.eligible_reports, 3);
        assert_eq!(candidate.fold_forward.profitable_reports, 2);
        assert_eq!(candidate.fold_forward.losing_reports, 1);
        assert_eq!(candidate.fold_forward.abstained_reports, 2);
        assert_eq!(candidate.fold_forward.stats.trades, 22);
        assert!(candidate.fold_forward.stats.total_pnl > 29.0);

        let choices = candidate
            .fold_forward
            .decisions
            .iter()
            .map(|decision| decision.selected_direction.as_deref())
            .collect::<Vec<_>>();
        assert_eq!(
            choices,
            vec![None, None, Some("up"), Some("down"), Some("down")]
        );
        assert_eq!(candidate.fold_forward.decisions[2].train_reports, 2);
        assert!(
            candidate.fold_forward.decisions[2]
                .train
                .as_ref()
                .unwrap()
                .total_pnl
                > 5.0
        );
        assert!(
            candidate.fold_forward.decisions[3]
                .train
                .as_ref()
                .unwrap()
                .total_pnl
                > 9.0
        );
    }

    #[test]
    fn adaptive_direction_search_does_not_promote_future_down_luck() {
        let folds = vec![
            selectivity_fold(vec![
                ("direction=down", pnl_stats(0, 2, 0.0, -2.0)),
                ("direction=up", pnl_stats(1, 0, 1.0, 0.0)),
            ]),
            selectivity_fold(vec![
                ("direction=down", pnl_stats(0, 2, 0.0, -2.0)),
                ("direction=up", pnl_stats(1, 0, 1.0, 0.0)),
            ]),
            selectivity_fold(vec![
                ("direction=down", pnl_stats(12, 0, 20.0, 0.0)),
                ("direction=up", pnl_stats(0, 2, 0.0, -2.0)),
            ]),
        ];
        let search = adaptive_direction_search_from_folds(&folds, &adaptive_input(5));
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("adaptive candidate");
        let decision = &candidate.fold_forward.decisions[2];

        assert!(!search.ok);
        assert_eq!(decision.selected_direction.as_deref(), Some("up"));
        assert_ne!(decision.selected_direction.as_deref(), Some("down"));
        assert!(decision.train.as_ref().unwrap().total_pnl > 1.0);
        assert_eq!(decision.oos.as_ref().unwrap().total_pnl, -2.0);
        assert!(!candidate.passed);
    }

    #[test]
    fn adaptive_direction_search_abstains_without_positive_prior_edge() {
        let folds = vec![
            selectivity_fold(vec![
                ("direction=up", pnl_stats(0, 1, 0.0, -1.0)),
                ("direction=down", pnl_stats(0, 2, 0.0, -2.0)),
            ]),
            selectivity_fold(vec![
                ("direction=up", pnl_stats(0, 1, 0.0, -1.0)),
                ("direction=down", pnl_stats(0, 2, 0.0, -2.0)),
            ]),
            selectivity_fold(vec![
                ("direction=up", pnl_stats(8, 0, 20.0, 0.0)),
                ("direction=down", pnl_stats(8, 0, 20.0, 0.0)),
            ]),
        ];
        let search = adaptive_direction_search_from_folds(&folds, &adaptive_input(5));
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("adaptive candidate");

        assert!(!search.ok);
        assert_eq!(candidate.fold_forward.eligible_reports, 0);
        assert_eq!(candidate.fold_forward.abstained_reports, 3);
        assert_eq!(
            candidate.fold_forward.decisions[2].reason,
            "no_direction_passed_prior_gates"
        );
        assert_eq!(
            candidate.fold_forward.decisions[2]
                .selected_direction
                .as_deref(),
            None
        );
    }

    #[test]
    fn unknown_profile_is_rejected() {
        let err = StrategyBuilderProfile::from_name("mystery").unwrap_err();
        assert!(err.to_string().contains("unknown strategy-builder profile"));
    }

    #[test]
    fn reversion_guard_profile_matches_validated_grid() {
        let profile = StrategyBuilderProfile::from_name("a_plus5m_reversion_guard").unwrap();
        assert_eq!(profile.conf, "0.50");
        assert_eq!(profile.min_price, "0.75");
        assert_eq!(profile.max_price, "0.85");
        assert_eq!(profile.min_reversion_count, "1");
        assert_eq!(profile.max_reversion_count, "2");
        assert!(profile.degraded_force_taker);
    }

    fn selectivity_input(top: usize) -> StrategyBuilderSelectivitySearchInput {
        StrategyBuilderSelectivitySearchInput {
            report_paths: Vec::new(),
            min_train_reports: 2,
            min_train_trades: 4,
            min_oos_trades: 5,
            min_oos_wilson_win_rate_lower: 0.50,
            min_oos_total_pnl: 0.0,
            min_oos_profitable_reports: 1,
            min_worst_oos_pnl: 0.0,
            top,
        }
    }

    fn adaptive_input(top: usize) -> StrategyBuilderAdaptiveDirectionInput {
        StrategyBuilderAdaptiveDirectionInput {
            report_paths: Vec::new(),
            min_train_reports: 2,
            min_train_trades: 2,
            min_oos_trades: 4,
            min_oos_wilson_win_rate_lower: 0.50,
            min_oos_total_pnl: 0.0,
            min_oos_profitable_reports: 2,
            min_worst_oos_pnl: -10.0,
            top,
        }
    }

    fn adaptive_mode_input(top: usize) -> StrategyBuilderAdaptiveModeInput {
        StrategyBuilderAdaptiveModeInput {
            report_paths: Vec::new(),
            min_train_reports: 2,
            min_train_trades: 2,
            min_oos_trades: 4,
            min_oos_wilson_win_rate_lower: 0.50,
            min_oos_total_pnl: 0.0,
            min_oos_profitable_reports: 1,
            min_worst_oos_pnl: 0.0,
            max_guard_rules: 4,
            min_guard_trades: 1,
            min_guard_loss_pnl: 0.0,
            min_guard_loss_reports: 1,
            recent_report_lookback: 2,
            pattern_guards: false,
            flat_if_worst_train_below: -1.0e9,
            top,
        }
    }

    fn causal_policy_input(top: usize) -> StrategyBuilderCausalPolicySearchInput {
        StrategyBuilderCausalPolicySearchInput {
            report_paths: Vec::new(),
            min_train_reports: 2,
            min_train_trades: 4,
            min_oos_trades: 5,
            min_oos_wilson_win_rate_lower: 0.50,
            min_oos_total_pnl: 0.0,
            min_oos_profitable_reports: 1,
            min_worst_oos_pnl: 0.0,
            max_require_terms: 3,
            max_deny_rules: 1,
            max_deny_terms: 1,
            min_deny_trades: 1,
            min_deny_loss_pnl: 0.0,
            min_deny_loss_reports: 1,
            top,
        }
    }

    fn multi_guard_input(top: usize) -> StrategyBuilderMultiGuardSearchInput {
        StrategyBuilderMultiGuardSearchInput {
            report_paths: Vec::new(),
            min_train_reports: 2,
            min_train_trades: 4,
            min_oos_trades: 4,
            min_oos_wilson_win_rate_lower: 0.50,
            min_oos_total_pnl: 0.0,
            min_oos_profitable_reports: 1,
            min_worst_oos_pnl: 0.0,
            max_rules: 4,
            min_guard_trades: 1,
            min_guard_loss_pnl: 0.0,
            min_guard_loss_reports: 1,
            recent_report_lookback: 2,
            pattern_guards: false,
            top,
        }
    }

    fn selectivity_fold(buckets: Vec<(&str, TradePnlDiagnostics)>) -> SelectivityFold {
        let bucket_map = buckets
            .into_iter()
            .map(|(key, stats)| (key.to_string(), stats))
            .collect::<BTreeMap<_, _>>();
        let regimes = bucket_map
            .iter()
            .filter_map(|(key, stats)| {
                key.strip_prefix("regime=")
                    .map(|regime| (regime.to_string(), stats.clone()))
            })
            .collect();
        SelectivityFold {
            variants: vec![SelectivityVariantFold {
                name: "candidate".to_string(),
                buckets: bucket_map,
                tagged_regimes: tagged_regimes_from_map(&regimes),
                regimes,
            }],
        }
    }

    fn pnl_stats(
        wins: u64,
        losses: u64,
        gross_win_pnl: f64,
        gross_loss_pnl: f64,
    ) -> TradePnlDiagnostics {
        let trades = wins + losses;
        let total_pnl = gross_win_pnl + gross_loss_pnl;
        let avg_win_pnl = if wins == 0 {
            0.0
        } else {
            gross_win_pnl / wins as f64
        };
        let avg_loss_pnl = if losses == 0 {
            0.0
        } else {
            gross_loss_pnl / losses as f64
        };
        let profit_factor = if gross_loss_pnl.abs() > 0.0 {
            gross_win_pnl / gross_loss_pnl.abs()
        } else if gross_win_pnl > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
        let payoff_ratio = if avg_loss_pnl.abs() > 0.0 {
            avg_win_pnl / avg_loss_pnl.abs()
        } else if avg_win_pnl > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
        TradePnlDiagnostics {
            trades,
            wins,
            losses,
            win_rate: if trades == 0 {
                0.0
            } else {
                wins as f64 / trades as f64
            },
            total_pnl,
            avg_pnl: if trades == 0 {
                0.0
            } else {
                total_pnl / trades as f64
            },
            gross_win_pnl,
            gross_loss_pnl,
            avg_win_pnl,
            avg_loss_pnl,
            max_win_pnl: avg_win_pnl,
            max_loss_pnl: avg_loss_pnl,
            profit_factor,
            payoff_ratio,
            worst_loss_to_avg_win: if avg_win_pnl > 0.0 {
                avg_loss_pnl.abs() / avg_win_pnl
            } else {
                0.0
            },
        }
    }

    #[test]
    fn audit_allows_below_floor_oracle_disagreement() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("session.jsonl");
        let lines = [
            serde_json::json!({
                "cat": "system",
                "type": "release_manifest",
                "mode": "paper",
                "promotion": {
                    "status": "ok",
                    "source_report_hash": "report",
                    "data_manifest_hash": "data",
                    "strategy": {"params_hash": "strategy"}
                }
            }),
            serde_json::json!({
                "cat": "system",
                "type": "runtime_strategy",
                "settlement_alignment_ready": false,
                "settlement_min_abs_move_usd": 25.0,
                "strategy": {"params_hash": "strategy"}
            }),
            serde_json::json!({
                "cat": "shadow",
                "type": "resolved"
            }),
            serde_json::json!({
                "cat": "oracle",
                "type": "resolution",
                "cid": "0xabc",
                "our_actual": "up",
                "polymarket_actual": "down",
                "our_open_btc": 100000.0,
                "our_close_btc": 100011.61,
                "agreed": false
            }),
            serde_json::json!({
                "cat": "signal",
                "type": "evaluation",
                "decision_trade": false,
                "execution_attempted": false,
                "traded": false
            }),
        ];
        let payload = lines
            .into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, payload).unwrap();

        let audit = audit(StrategyBuilderAuditInput {
            report_paths: Vec::new(),
            adaptive_report_paths: Vec::new(),
            promotion_artifact: None,
            replay_sessions: vec![path.display().to_string()],
            min_trades: 1,
            min_win_rate: 0.0,
            min_wilson_win_rate_lower: 0.0,
            min_total_pnl: 0.0,
            min_shadow_resolutions: 1,
            min_research_reports: 0,
            min_replay_sessions: 1,
            a_plus_min_shadow_resolutions: 1,
        });

        assert!(audit.ok);
        assert!(audit.checks.iter().any(|c| {
            c.name == "replay.shadow_oracle" && c.status == StrategyBuilderCheckStatus::Ok
        }));
        assert!(audit.checks.iter().any(|c| {
            c.name == "replay.below_floor_oracle" && c.status == StrategyBuilderCheckStatus::Ok
        }));
    }

    #[test]
    fn audit_accepts_executable_resolved_replay_samples() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("session.jsonl");
        let lines = [
            serde_json::json!({
                "cat": "system",
                "type": "release_manifest",
                "mode": "paper",
                "promotion": {
                    "status": "ok",
                    "source_report_hash": "report",
                    "data_manifest_hash": "data",
                    "strategy": {"params_hash": "strategy"}
                }
            }),
            serde_json::json!({
                "cat": "system",
                "type": "runtime_strategy",
                "settlement_alignment_ready": true,
                "settlement_min_abs_move_usd": 25.0,
                "strategy": {"params_hash": "strategy"}
            }),
            serde_json::json!({
                "cat": "signal",
                "type": "evaluation",
                "decision_trade": true,
                "execution_attempted": true,
                "traded": false,
                "book_spread": 0.01
            }),
            serde_json::json!({
                "cat": "causality",
                "type": "order_timing",
                "intent_id": "intent_1",
                "condition_id": "0xabc",
                "token_id": "token",
                "signal_source_ts_s": 100.0,
                "decision_ts_s": 100.0,
                "order_ts_s": 100.05,
                "market_start_ts_s": 0.0,
                "market_end_ts_s": 300.0
            }),
            serde_json::json!({
                "cat": "order",
                "type": "placed",
                "intent_id": "intent_1"
            }),
            serde_json::json!({
                "cat": "order",
                "type": "filled",
                "intent_id": "intent_1",
                "fill_time_s": 0.05
            }),
            serde_json::json!({
                "cat": "resolution",
                "type": "resolved",
                "won": true,
                "pnl": 2.0,
                "btc_move": 44.0
            }),
            serde_json::json!({
                "cat": "causality",
                "type": "resolution_timing",
                "condition_id": "0xabc",
                "market_end_ts_s": 300.0,
                "resolution_ts_s": 301.0
            }),
            serde_json::json!({
                "cat": "oracle",
                "type": "resolution",
                "agreed": true,
                "our_open_btc": 100000.0,
                "our_close_btc": 100044.0
            }),
        ];
        let payload = lines
            .into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, payload).unwrap();

        let audit = audit(StrategyBuilderAuditInput {
            report_paths: Vec::new(),
            adaptive_report_paths: Vec::new(),
            promotion_artifact: None,
            replay_sessions: vec![path.display().to_string()],
            min_trades: 1,
            min_win_rate: 0.0,
            min_wilson_win_rate_lower: 0.0,
            min_total_pnl: 0.0,
            min_shadow_resolutions: 1,
            min_research_reports: 0,
            min_replay_sessions: 1,
            a_plus_min_shadow_resolutions: 1,
        });

        assert!(audit.checks.iter().any(|c| {
            c.name == "replay.shadow_oracle" && c.status == StrategyBuilderCheckStatus::Ok
        }));
        assert!(audit.checks.iter().any(|c| {
            c.name == "replay.settlement_alignment" && c.status == StrategyBuilderCheckStatus::Ok
        }));
        assert!(audit.checks.iter().any(|c| {
            c.name == "replay.causality" && c.status == StrategyBuilderCheckStatus::Ok
        }));
    }

    #[test]
    fn audit_fails_best_report_variant_that_needed_adaptive_rearm() {
        use std::collections::BTreeMap;

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("report.json");
        let mut src = crate::data::manifest::DataSourceManifest::new("pmxt", "order_book_l2");
        src.complete = true;
        let diagnostics = crate::backtest::resolver::BacktestDiagnostics {
            adaptive_rearms: 1,
            breaker_paused_events: 42,
            ..Default::default()
        };
        let report = crate::backtest::experiment::ExperimentReport {
            schema_version: 1,
            generated_at: "2026-05-22T00:00:00Z".to_string(),
            label: "test".to_string(),
            mode: "backtest".to_string(),
            start: "2026-05-21T00:00:00Z".to_string(),
            end: "2026-05-21T23:00:00Z".to_string(),
            bankroll_usd: 100.0,
            latency_ms: 50,
            market_catalog: crate::data::catalog::MarketCatalog::default(),
            data_manifest: crate::data::manifest::DataManifest::new(vec![src], Vec::new()),
            variants: vec![crate::backtest::experiment::VariantReport {
                strategy: crate::strategy::spec::StrategySpec::new("s", "1", "hash", "risk"),
                strategy_params: serde_json::json!({"name": "test"}),
                trades: 100,
                wins: 70,
                losses: 30,
                unresolved_fills: 0,
                execution_attempts: 100,
                fills_success: 100,
                fills_failed: 0,
                fill_rate: 1.0,
                reject_reasons: BTreeMap::new(),
                breaker_tripped: false,
                breaker_reason: None,
                breaker_tripped_at_s: None,
                breaker_realized_drawdown_pct: 0.0,
                breaker_stressed_drawdown_pct: 0.0,
                diagnostics,
                win_rate: 0.70,
                total_pnl: 100.0,
                avg_pnl: 1.0,
                total_fees: 0.0,
                sharpe_like: 1.0,
                by_zone: BTreeMap::new(),
            }],
        };
        std::fs::write(&path, serde_json::to_vec(&report).unwrap()).unwrap();

        let audit = audit(StrategyBuilderAuditInput {
            report_paths: vec![path.display().to_string()],
            adaptive_report_paths: Vec::new(),
            promotion_artifact: None,
            replay_sessions: Vec::new(),
            min_trades: 50,
            min_win_rate: 0.60,
            min_wilson_win_rate_lower: 0.50,
            min_total_pnl: 10.0,
            min_shadow_resolutions: 1,
            min_research_reports: 1,
            min_replay_sessions: 0,
            a_plus_min_shadow_resolutions: 1,
        });

        assert!(audit.checks.iter().any(|c| {
            c.name == "report.best_variant_health"
                && c.status == StrategyBuilderCheckStatus::Fail
                && c.detail.contains("adaptive_rearms=1")
        }));
    }

    #[test]
    fn audit_fails_adaptive_probe_when_best_variant_rearms() {
        use std::collections::BTreeMap;

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("adaptive_report.json");
        let mut src = crate::data::manifest::DataSourceManifest::new("pmxt", "order_book_l2");
        src.complete = true;
        let diagnostics = crate::backtest::resolver::BacktestDiagnostics {
            adaptive_rearms: 2,
            breaker_paused_events: 100,
            ..Default::default()
        };
        let report = crate::backtest::experiment::ExperimentReport {
            schema_version: 1,
            generated_at: "2026-05-22T00:00:00Z".to_string(),
            label: "adaptive".to_string(),
            mode: "backtest".to_string(),
            start: "2026-05-21T00:00:00Z".to_string(),
            end: "2026-05-21T23:00:00Z".to_string(),
            bankroll_usd: 100.0,
            latency_ms: 50,
            market_catalog: crate::data::catalog::MarketCatalog::default(),
            data_manifest: crate::data::manifest::DataManifest::new(vec![src], Vec::new()),
            variants: vec![crate::backtest::experiment::VariantReport {
                strategy: crate::strategy::spec::StrategySpec::new("s", "1", "hash", "risk"),
                strategy_params: serde_json::json!({"name": "test"}),
                trades: 100,
                wins: 70,
                losses: 30,
                unresolved_fills: 0,
                execution_attempts: 100,
                fills_success: 100,
                fills_failed: 0,
                fill_rate: 1.0,
                reject_reasons: BTreeMap::new(),
                breaker_tripped: true,
                breaker_reason: Some("win_rate_low".to_string()),
                breaker_tripped_at_s: Some(1_700_000_000.0),
                breaker_realized_drawdown_pct: 0.0,
                breaker_stressed_drawdown_pct: 0.0,
                diagnostics,
                win_rate: 0.70,
                total_pnl: 100.0,
                avg_pnl: 1.0,
                total_fees: 0.0,
                sharpe_like: 1.0,
                by_zone: BTreeMap::new(),
            }],
        };
        std::fs::write(&path, serde_json::to_vec(&report).unwrap()).unwrap();

        let audit = audit(StrategyBuilderAuditInput {
            report_paths: Vec::new(),
            adaptive_report_paths: vec![path.display().to_string()],
            promotion_artifact: None,
            replay_sessions: Vec::new(),
            min_trades: 50,
            min_win_rate: 0.60,
            min_wilson_win_rate_lower: 0.50,
            min_total_pnl: 10.0,
            min_shadow_resolutions: 1,
            min_research_reports: 0,
            min_replay_sessions: 0,
            a_plus_min_shadow_resolutions: 1,
        });

        assert!(audit.checks.iter().any(|c| {
            c.name == "adaptive_probe.health"
                && c.status == StrategyBuilderCheckStatus::Fail
                && c.detail.contains("best_adaptive_rearms=2")
        }));
    }

    #[test]
    fn adaptive_drift_flags_forward_decay() {
        let (status, detail) = classify_adaptive_drift(0.80, 1.0, 5, 5, -1.0, 10, 0.60, false, 0);
        assert_eq!(status, StrategyBuilderCheckStatus::Fail);
        assert!(detail.contains("negative forward pnl"));

        let (status, detail) = classify_adaptive_drift(0.80, 1.0, 8, 2, 7.0, 10, 0.60, false, 0);
        assert_eq!(status, StrategyBuilderCheckStatus::Ok);
        assert!(detail.contains("inside adaptive band"));
    }
}
