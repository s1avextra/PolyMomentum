//! Strategy-builder orchestration and audit helpers.
//!
//! This module does not invent a new research engine. It makes the existing
//! stages explicit and reproducible: one-pass PMXT eval-cache scouting, cached
//! PMXT harness sweep, aggregate promotion, cached live-replay parity, and
//! session diagnostics.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::backtest::experiment::{self, PromotionArtifact};
use crate::backtest::resolver::TradePnlDiagnostics;
use crate::backtest::strategies::{SelectivityFilter, StrategyVariant};
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
    pub tail_alpha: f64,
    pub min_oos_cvar_pnl: f64,
    pub loss_burst_lookback: usize,
    pub max_loss_burst_reports: usize,
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
    pub tail_alpha: f64,
    pub min_oos_cvar_pnl: f64,
    pub loss_burst_lookback: usize,
    pub max_loss_burst_reports: usize,
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
    pub tail_alpha: f64,
    pub min_oos_cvar_pnl: f64,
    pub loss_burst_lookback: usize,
    pub max_loss_burst_reports: usize,
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
    pub min_oos_eligible_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub max_require_terms: usize,
    pub max_deny_rules: usize,
    pub max_deny_terms: usize,
    pub min_deny_trades: u64,
    pub min_deny_loss_pnl: f64,
    pub min_deny_loss_reports: usize,
    pub tail_alpha: f64,
    pub min_oos_cvar_pnl: f64,
    pub loss_burst_lookback: usize,
    pub max_loss_burst_reports: usize,
    pub tail_first_ranking: bool,
    pub min_oos_payoff_ratio: f64,
    pub max_oos_worst_loss_to_avg_win: f64,
    pub prior_loss_cluster_lookback: usize,
    pub max_prior_loss_burst_reports: usize,
    pub min_prior_payoff_ratio: f64,
    pub max_prior_worst_loss_to_avg_win: f64,
    pub meta_label_min_support: usize,
    pub meta_label_alpha: f64,
    pub meta_label_min_quantile_pnl: f64,
    pub meta_label_max_loss_rate: f64,
    pub meta_label_require_supported: bool,
    pub meta_label_max_generalization_terms: usize,
    pub top: usize,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderEvolveSearchInput {
    pub report_paths: Vec<String>,
    pub historical_search_paths: Vec<String>,
    pub out_dir: PathBuf,
    pub seed: u64,
    pub population: usize,
    pub generations: usize,
    pub elite_count: usize,
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_oos_eligible_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub max_require_terms: usize,
    pub max_deny_rules: usize,
    pub max_deny_terms: usize,
    pub min_deny_trades: u64,
    pub min_deny_loss_pnl: f64,
    pub min_deny_loss_reports: usize,
    pub tail_alpha: f64,
    pub min_oos_cvar_pnl: f64,
    pub loss_burst_lookback: usize,
    pub max_loss_burst_reports: usize,
    pub min_oos_payoff_ratio: f64,
    pub max_oos_worst_loss_to_avg_win: f64,
    pub prior_loss_cluster_lookback: usize,
    pub max_prior_loss_burst_reports: usize,
    pub min_prior_payoff_ratio: f64,
    pub max_prior_worst_loss_to_avg_win: f64,
    pub meta_label_min_support: usize,
    pub meta_label_alpha: f64,
    pub meta_label_min_quantile_pnl: f64,
    pub meta_label_max_loss_rate: f64,
    pub meta_label_require_supported: bool,
    pub meta_label_max_generalization_terms: usize,
    pub top: usize,
    pub replay_start: Option<String>,
    pub replay_end: Option<String>,
    pub replay_profile: String,
    pub replay_zone_mode: String,
    pub latency_ms: u64,
    pub latency_audit_json: Option<String>,
    pub btc_csv: Option<String>,
    pub fold_hours: i64,
    pub threads: usize,
    pub window_minutes: f64,
    pub atomic_parquet: bool,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderMaterializePolicyVariantInput {
    pub search_path: PathBuf,
    pub source_report_paths: Vec<String>,
    pub rank: usize,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderMaterializeSweepVariantInput {
    pub report_path: PathBuf,
    pub rank: usize,
    pub output_path: PathBuf,
    pub require_causal_tag: Vec<String>,
    pub deny_causal_tag: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderFeatureFilterSearchInput {
    pub feature_paths: Vec<String>,
    pub base_variant_path: PathBuf,
    pub out_dir: PathBuf,
    pub top: usize,
    pub max_require_terms: usize,
    pub max_deny_terms: usize,
    pub min_atom_trades: u64,
    pub max_atoms: usize,
    pub min_total_trades: u64,
    pub min_eligible_reports: usize,
    pub min_total_pnl: f64,
    pub min_worst_report_pnl: f64,
}

#[derive(Debug, Clone)]
pub struct StrategyRegistryMarkInput {
    pub registry_path: PathBuf,
    pub strategy_id: String,
    pub parent_id: Option<String>,
    pub status: StrategyRegistryStatus,
    pub reason: String,
    pub artifact_path: Option<String>,
    pub metrics_path: Option<String>,
    pub evidence_paths: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderEvidenceExportInput {
    pub registry_path: PathBuf,
    pub out_dir: PathBuf,
    pub rewrite_registry: bool,
}

#[derive(Debug, Clone)]
pub struct StrategyRegistryAuditInput {
    pub registry_path: PathBuf,
    pub durable_prefix: String,
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
    pub gates: AdaptiveDirectionSearchGates,
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
pub struct StrategyBuilderEvolveSearch {
    pub schema_version: u32,
    pub ok: bool,
    pub report_count: usize,
    pub candidate_count: usize,
    pub run: EvolutionRunManifest,
    pub methodology: Vec<String>,
    pub gates: CausalPolicySearchGates,
    pub generations: Vec<EvolutionGeneration>,
    pub candidates: Vec<EvolutionCandidate>,
    pub notes: Vec<String>,
    #[serde(skip_serializing)]
    pub trial_ledger: Vec<EvolutionTrialLedgerRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderMaterializedPolicyVariant {
    pub schema_version: u32,
    pub rank: usize,
    pub search_path: String,
    pub source_report_path: String,
    pub source_variant: String,
    pub output_path: String,
    pub variant_hash: String,
    pub require_tags: BTreeMap<String, String>,
    pub deny_tag_values: BTreeMap<String, BTreeSet<String>>,
    pub selectivity: SelectivityFilter,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderMaterializedSweepVariant {
    pub schema_version: u32,
    pub rank: usize,
    pub report_path: String,
    pub source_variant: String,
    pub output_path: String,
    pub variant_hash: String,
    pub require_tags: BTreeMap<String, String>,
    pub deny_tag_values: BTreeMap<String, BTreeSet<String>>,
    pub selectivity: SelectivityFilter,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderFeatureFilterSearch {
    pub schema_version: u32,
    pub ok: bool,
    pub feature_report_count: usize,
    pub candidate_count: usize,
    pub base_variant_path: String,
    pub out_dir: String,
    pub gates: FeatureFilterSearchGates,
    pub candidates: Vec<FeatureFilterCandidate>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureFilterSearchGates {
    pub top: usize,
    pub max_require_terms: usize,
    pub max_deny_terms: usize,
    pub min_atom_trades: u64,
    pub max_atoms: usize,
    pub min_total_trades: u64,
    pub min_eligible_reports: usize,
    pub min_total_pnl: f64,
    pub min_worst_report_pnl: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureFilterCandidate {
    pub rank: usize,
    pub passed: bool,
    pub candidate_id: String,
    pub variant_path: String,
    pub variant_hash: String,
    pub require_tags: BTreeMap<String, String>,
    pub deny_tag_values: BTreeMap<String, BTreeSet<String>>,
    pub selectivity: SelectivityFilter,
    pub fitness: FeatureFilterFitness,
    pub fold_reports: Vec<FeatureFilterFoldReport>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureFilterFitness {
    pub passed: bool,
    pub failure_reasons: Vec<String>,
    pub eligible_reports: usize,
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub total_pnl: f64,
    pub worst_report_pnl: f64,
    pub cvar_pnl: f64,
    pub profit_factor: f64,
    pub payoff_ratio: f64,
    pub wilson_win_rate_lower: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureFilterFoldReport {
    pub feature_path: String,
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub total_pnl: f64,
}

#[derive(Debug, Clone)]
struct FeatureFilterDraft {
    require_tags: BTreeMap<String, String>,
    deny_tag_values: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Debug, Clone)]
struct FeatureFilterFold {
    path: String,
    rows: Vec<FeatureFilterRow>,
}

#[derive(Debug, Clone)]
struct FeatureFilterRow {
    pnl: f64,
    won: bool,
    causal_tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct FeatureFilterAtom {
    dimension: String,
    value: String,
}

#[derive(Debug, Clone, Default)]
struct FeatureFilterAtomStats {
    trades: u64,
    losses: u64,
    pnl: f64,
}

#[derive(Debug, Deserialize)]
struct FeatureFilterReportJson {
    rows: Vec<FeatureFilterRowJson>,
}

#[derive(Debug, Deserialize)]
struct FeatureFilterRowJson {
    pnl: Option<f64>,
    pnl_after_fee: Option<f64>,
    won: Option<bool>,
    causal_tags: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvolutionRunConfig {
    pub seed: u64,
    pub population: usize,
    pub generations: usize,
    pub elite_count: usize,
    pub top: usize,
    pub report_paths: Vec<String>,
    pub historical_search_paths: Vec<String>,
    pub out_dir: String,
    pub replay: EvolutionReplayConfig,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvolutionReplayConfig {
    pub start: Option<String>,
    pub end: Option<String>,
    pub profile: String,
    pub zone_mode: String,
    pub latency_ms: u64,
    pub latency_audit_json: Option<String>,
    pub btc_csv: Option<String>,
    pub fold_hours: i64,
    pub threads: usize,
    pub window_minutes: f64,
    pub atomic_parquet: bool,
    pub execute: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvolutionRunManifest {
    pub schema_version: u32,
    pub run_id: String,
    pub generated_at: String,
    pub config: EvolutionRunConfig,
    pub artifact_paths: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvolutionGenome {
    pub schema_version: u32,
    pub variant: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub require_tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deny_tags: BTreeMap<String, String>,
    #[serde(default)]
    pub knobs: EvolutionStrategyKnobs,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct EvolutionStrategyKnobs {
    pub min_confidence: Option<f64>,
    pub min_edge: Option<f64>,
    pub early_min_z: Option<f64>,
    pub primary_min_z: Option<f64>,
    pub late_min_z: Option<f64>,
    pub terminal_min_z: Option<f64>,
    pub min_price: Option<f64>,
    pub max_price: Option<f64>,
    pub min_ev_buffer: Option<f64>,
    pub settlement_guard_minutes: Option<f64>,
    pub settlement_min_abs_move_usd: Option<f64>,
    pub min_reversion_count: Option<u64>,
    pub max_reversion_count: Option<u64>,
    pub prefer_maker: Option<bool>,
    pub max_spread: Option<f64>,
    pub min_book_depth: Option<f64>,
    pub min_book_pressure: Option<f64>,
    pub recent_mid_lookback_seconds: Option<f64>,
    pub max_recent_mid_runup: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvolutionCandidate {
    pub rank: usize,
    pub passed: bool,
    pub generation: usize,
    pub pareto_front: usize,
    pub candidate_id: String,
    pub genome_hash: String,
    pub parent_hashes: Vec<String>,
    pub genome: EvolutionGenome,
    pub fitness: EvolutionFitness,
    pub final_policy: CausalPolicyReport,
    pub aggregate_static_final_policy: SelectivityStatsReport,
    pub fold_forward: CausalPolicyFoldForwardReport,
    pub variant_path: Option<String>,
    pub replay_manifest_path: Option<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvolutionFitness {
    pub passed: bool,
    pub replayable_policy: bool,
    pub static_fitness_exact: bool,
    pub gate_failures: usize,
    pub failure_reasons: Vec<String>,
    pub eligible_reports: usize,
    pub profitable_reports: usize,
    pub losing_reports: usize,
    pub abstained_reports: usize,
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub wilson_win_rate_lower: f64,
    pub total_pnl: f64,
    pub worst_report_pnl: f64,
    pub cvar_pnl: f64,
    pub max_loss_burst_reports: usize,
    pub worst_loss_to_avg_win: f64,
    pub payoff_ratio: f64,
    pub profit_factor: f64,
    pub median_expectancy: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvolutionGeneration {
    pub generation: usize,
    pub population_count: usize,
    pub evaluated_count: usize,
    pub pareto_front_count: usize,
    pub best_candidate_ids: Vec<String>,
    pub survivor_hashes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvolutionTrialLedgerRow {
    pub generation: usize,
    pub candidate_id: String,
    pub genome_hash: String,
    pub parent_hashes: Vec<String>,
    pub passed: bool,
    pub replayable_policy: bool,
    pub static_fitness_exact: bool,
    pub gate_failures: usize,
    pub pareto_front: usize,
    pub failure_reasons: Vec<String>,
    pub eligible_reports: usize,
    pub abstained_reports: usize,
    pub trades: u64,
    pub total_pnl: f64,
    pub worst_report_pnl: f64,
    pub cvar_pnl: f64,
    pub max_loss_burst_reports: usize,
    pub payoff_ratio: f64,
    pub profit_factor: f64,
    pub wilson_win_rate_lower: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderEvidenceExport {
    pub schema_version: u32,
    pub registry_path: String,
    pub out_dir: String,
    pub registry_rewritten: bool,
    pub copied: Vec<StrategyBuilderEvidenceCopy>,
    pub missing: Vec<StrategyBuilderEvidenceMissing>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderEvidenceCopy {
    pub strategy_id: String,
    pub role: String,
    pub source_path: String,
    pub archived_path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderEvidenceMissing {
    pub strategy_id: String,
    pub role: String,
    pub source_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyRegistryAudit {
    pub schema_version: u32,
    pub registry_path: String,
    pub durable_prefix: String,
    pub ok: bool,
    pub live_ready: bool,
    pub grade: String,
    pub entries: usize,
    pub status_counts: BTreeMap<String, usize>,
    pub live_candidate_count: usize,
    pub missing_paths: Vec<StrategyRegistryPathIssue>,
    pub non_durable_paths: Vec<StrategyRegistryPathIssue>,
    pub checks: Vec<String>,
    pub next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyRegistryPathIssue {
    pub strategy_id: String,
    pub status: StrategyRegistryStatus,
    pub role: String,
    pub path: String,
    pub blocking_live: bool,
    pub detail: String,
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
    pub tail_alpha: f64,
    pub min_oos_cvar_pnl: f64,
    pub loss_burst_lookback: usize,
    pub max_loss_burst_reports: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveDirectionSearchGates {
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub tail_alpha: f64,
    pub min_oos_cvar_pnl: f64,
    pub loss_burst_lookback: usize,
    pub max_loss_burst_reports: usize,
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
    pub tail_alpha: f64,
    pub min_oos_cvar_pnl: f64,
    pub loss_burst_lookback: usize,
    pub max_loss_burst_reports: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CausalPolicySearchGates {
    pub min_train_reports: usize,
    pub min_train_trades: u64,
    pub min_oos_trades: u64,
    pub min_oos_wilson_win_rate_lower: f64,
    pub min_oos_total_pnl: f64,
    pub min_oos_profitable_reports: usize,
    pub min_oos_eligible_reports: usize,
    pub min_worst_oos_pnl: f64,
    pub max_require_terms: usize,
    pub max_deny_rules: usize,
    pub max_deny_terms: usize,
    pub min_deny_trades: u64,
    pub min_deny_loss_pnl: f64,
    pub min_deny_loss_reports: usize,
    pub tail_alpha: f64,
    pub min_oos_cvar_pnl: f64,
    pub loss_burst_lookback: usize,
    pub max_loss_burst_reports: usize,
    pub tail_first_ranking: bool,
    pub min_oos_payoff_ratio: f64,
    pub max_oos_worst_loss_to_avg_win: f64,
    pub prior_loss_cluster_lookback: usize,
    pub max_prior_loss_burst_reports: usize,
    pub min_prior_payoff_ratio: f64,
    pub max_prior_worst_loss_to_avg_win: f64,
    pub meta_label_min_support: usize,
    pub meta_label_alpha: f64,
    pub meta_label_min_quantile_pnl: f64,
    pub meta_label_max_loss_rate: f64,
    pub meta_label_require_supported: bool,
    pub meta_label_max_generalization_terms: usize,
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
    pub tail: TailRiskReport,
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
    pub tail: TailRiskReport,
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
    pub tail: TailRiskReport,
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
    pub tail: TailRiskReport,
    pub stats: SelectivityStatsReport,
    pub decisions: Vec<CausalPolicyDecisionReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TailRiskReport {
    pub alpha: f64,
    pub sample_count: usize,
    pub tail_count: usize,
    pub cvar_pnl: f64,
    pub worst_pnl: f64,
    pub losing_reports: usize,
    pub loss_burst_lookback: usize,
    pub max_loss_burst_reports: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CausalPolicyDecisionReport {
    pub report_index: usize,
    pub train_reports: usize,
    pub policy: CausalPolicyReport,
    pub train: Option<SelectivityStatsReport>,
    pub prior_tail: Option<TailRiskReport>,
    pub prior_recent_loss_reports: usize,
    pub meta_label: Option<MetaLabelRiskReport>,
    pub oos: Option<SelectivityStatsReport>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetaLabelRiskReport {
    pub active_buckets: usize,
    pub supported_buckets: usize,
    pub unsupported_buckets: usize,
    pub min_support: usize,
    pub alpha: f64,
    pub min_quantile_pnl: f64,
    pub max_loss_rate: f64,
    pub require_supported: bool,
    pub max_generalization_terms: usize,
    pub worst_quantile_pnl: f64,
    pub worst_prior_pnl: f64,
    pub max_loss_rate_seen: f64,
    pub flattened: bool,
    pub reason: String,
    pub buckets: Vec<MetaLabelBucketReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetaLabelBucketReport {
    pub kind: String,
    pub label: String,
    pub match_tags: BTreeMap<String, String>,
    pub support: usize,
    pub supported: bool,
    pub loss_rate: f64,
    pub quantile_pnl: f64,
    pub worst_pnl: f64,
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
struct SelectivityReportSet {
    folds: Vec<SelectivityFold>,
    variants: BTreeMap<String, StrategyVariant>,
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
struct EvolutionPopulationMember {
    genome: EvolutionGenome,
    parent_hashes: Vec<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyRegistry {
    pub schema_version: u32,
    pub updated_at: String,
    pub entries: Vec<StrategyRegistryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyRegistryEntry {
    pub strategy_id: String,
    pub version_id: String,
    pub parent_id: Option<String>,
    pub status: StrategyRegistryStatus,
    pub reason: String,
    pub artifact_path: Option<String>,
    pub metrics_path: Option<String>,
    pub evidence_paths: Vec<String>,
    pub notes: Vec<String>,
    pub first_seen_at: String,
    pub updated_at: String,
    pub events: Vec<StrategyRegistryEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyRegistryEvent {
    pub at: String,
    pub status: StrategyRegistryStatus,
    pub reason: String,
    pub evidence_paths: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyRegistryStatus {
    Candidate,
    Active,
    Questionable,
    DeadEnd,
    Promoted,
    Rejected,
}

impl StrategyRegistryStatus {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "candidate" => Ok(Self::Candidate),
            "active" => Ok(Self::Active),
            "questionable" | "questionnable" => Ok(Self::Questionable),
            "dead_end" | "deadend" => Ok(Self::DeadEnd),
            "promoted" => Ok(Self::Promoted),
            "rejected" => Ok(Self::Rejected),
            other => bail!(
                "unknown strategy status `{other}`; use candidate, active, questionable, dead_end, promoted, or rejected"
            ),
        }
    }
}

fn strategy_registry_status_label(status: StrategyRegistryStatus) -> &'static str {
    match status {
        StrategyRegistryStatus::Candidate => "candidate",
        StrategyRegistryStatus::Active => "active",
        StrategyRegistryStatus::Questionable => "questionable",
        StrategyRegistryStatus::DeadEnd => "dead_end",
        StrategyRegistryStatus::Promoted => "promoted",
        StrategyRegistryStatus::Rejected => "rejected",
    }
}

fn registry_entry_paths(entry: &StrategyRegistryEntry) -> Vec<(String, String)> {
    let mut paths = Vec::new();
    if let Some(path) = &entry.artifact_path {
        paths.push(("artifact".to_string(), path.clone()));
    }
    if let Some(path) = &entry.metrics_path {
        paths.push(("metrics".to_string(), path.clone()));
    }
    for (idx, path) in entry.evidence_paths.iter().enumerate() {
        paths.push((format!("evidence_{idx:02}"), path.clone()));
    }
    for (event_idx, event) in entry.events.iter().enumerate() {
        for (evidence_idx, path) in event.evidence_paths.iter().enumerate() {
            paths.push((
                format!("event_{event_idx:02}_evidence_{evidence_idx:02}"),
                path.clone(),
            ));
        }
    }
    paths
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

pub fn mark_strategy_version(input: StrategyRegistryMarkInput) -> Result<StrategyRegistry> {
    let strategy_id = input.strategy_id.trim();
    if strategy_id.is_empty() {
        bail!("--strategy-id must not be empty");
    }
    let reason = input.reason.trim();
    if reason.is_empty() {
        bail!("--reason must explain why this strategy is being marked");
    }

    let mut registry = read_strategy_registry(&input.registry_path)?;
    let now = Utc::now().to_rfc3339();
    registry.updated_at = now.clone();

    let event = StrategyRegistryEvent {
        at: now.clone(),
        status: input.status,
        reason: reason.to_string(),
        evidence_paths: input.evidence_paths.clone(),
        notes: input.notes.clone(),
    };

    if let Some(entry) = registry
        .entries
        .iter_mut()
        .find(|entry| entry.strategy_id == strategy_id)
    {
        entry.status = input.status;
        entry.reason = reason.to_string();
        if input.parent_id.is_some() {
            entry.parent_id = input.parent_id.clone();
        }
        if input.artifact_path.is_some() {
            entry.artifact_path = input.artifact_path.clone();
        }
        if input.metrics_path.is_some() {
            entry.metrics_path = input.metrics_path.clone();
        }
        merge_unique_strings(&mut entry.evidence_paths, &input.evidence_paths);
        merge_unique_strings(&mut entry.notes, &input.notes);
        entry.updated_at = now.clone();
        entry.events.push(event);
    } else {
        registry.entries.push(StrategyRegistryEntry {
            strategy_id: strategy_id.to_string(),
            version_id: strategy_version_id(&input),
            parent_id: input.parent_id.clone(),
            status: input.status,
            reason: reason.to_string(),
            artifact_path: input.artifact_path.clone(),
            metrics_path: input.metrics_path.clone(),
            evidence_paths: input.evidence_paths.clone(),
            notes: input.notes.clone(),
            first_seen_at: now.clone(),
            updated_at: now,
            events: vec![event],
        });
    }

    registry
        .entries
        .sort_by(|left, right| left.strategy_id.cmp(&right.strategy_id));
    write_strategy_registry_atomic(&input.registry_path, &registry)?;
    Ok(registry)
}

pub fn export_strategy_evidence(
    input: StrategyBuilderEvidenceExportInput,
) -> Result<StrategyBuilderEvidenceExport> {
    let mut registry = read_strategy_registry(&input.registry_path)?;
    let mut copied = Vec::new();
    let mut missing = Vec::new();
    let mut rewrites: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut source_rewrites: BTreeMap<String, String> = BTreeMap::new();

    for entry in &registry.entries {
        let mut paths = Vec::new();
        if let Some(path) = &entry.artifact_path {
            paths.push(("artifact".to_string(), path.clone()));
        }
        if let Some(path) = &entry.metrics_path {
            paths.push(("metrics".to_string(), path.clone()));
        }
        for (idx, path) in entry.evidence_paths.iter().enumerate() {
            paths.push((format!("evidence_{idx:02}"), path.clone()));
        }
        for (event_idx, event) in entry.events.iter().enumerate() {
            for (evidence_idx, path) in event.evidence_paths.iter().enumerate() {
                paths.push((
                    format!("event_{event_idx:02}_evidence_{evidence_idx:02}"),
                    path.clone(),
                ));
            }
        }

        for (role, source_path) in paths {
            if source_path.trim().is_empty() {
                continue;
            }
            let source = PathBuf::from(&source_path);
            if !source.is_file() {
                missing.push(StrategyBuilderEvidenceMissing {
                    strategy_id: entry.strategy_id.clone(),
                    role,
                    source_path,
                });
                continue;
            }
            if let Some(archived) = source_rewrites.get(&source_path) {
                rewrites.insert((entry.strategy_id.clone(), role), archived.clone());
                continue;
            }

            let strategy_dir = input.out_dir.join(safe_path_component(&entry.strategy_id));
            let (archived_path, bytes, sha256) =
                archive_evidence_file(&source, &input.out_dir, &strategy_dir, &role, &source_path)
                    .with_context(|| format!("archive evidence {}", source.display()))?;
            let archived = archived_path.display().to_string();
            rewrites.insert((entry.strategy_id.clone(), role.clone()), archived.clone());
            source_rewrites
                .entry(source_path.clone())
                .or_insert_with(|| archived.clone());
            copied.push(StrategyBuilderEvidenceCopy {
                strategy_id: entry.strategy_id.clone(),
                role,
                source_path,
                archived_path: archived,
                bytes,
                sha256,
            });
        }
    }

    if input.rewrite_registry && !rewrites.is_empty() {
        let now = Utc::now().to_rfc3339();
        registry.updated_at = now.clone();
        for entry in &mut registry.entries {
            if entry.artifact_path.is_some() {
                if let Some(archived) =
                    rewrites.get(&(entry.strategy_id.clone(), "artifact".to_string()))
                {
                    entry.artifact_path = Some(archived.clone());
                }
            }
            if entry.metrics_path.is_some() {
                if let Some(archived) =
                    rewrites.get(&(entry.strategy_id.clone(), "metrics".to_string()))
                {
                    entry.metrics_path = Some(archived.clone());
                }
            }
            for (idx, path) in entry.evidence_paths.iter_mut().enumerate() {
                if let Some(archived) =
                    rewrites.get(&(entry.strategy_id.clone(), format!("evidence_{idx:02}")))
                {
                    *path = archived.clone();
                }
            }
            for event in &mut entry.events {
                for path in &mut event.evidence_paths {
                    if let Some(archived) = source_rewrites.get(path) {
                        *path = archived.clone();
                    }
                }
            }
            entry.updated_at = now.clone();
        }
        write_strategy_registry_atomic(&input.registry_path, &registry)?;
    }

    Ok(StrategyBuilderEvidenceExport {
        schema_version: 1,
        registry_path: input.registry_path.display().to_string(),
        out_dir: input.out_dir.display().to_string(),
        registry_rewritten: input.rewrite_registry && !rewrites.is_empty(),
        copied,
        missing,
    })
}

pub fn audit_strategy_registry(input: StrategyRegistryAuditInput) -> Result<StrategyRegistryAudit> {
    let registry = read_strategy_registry(&input.registry_path)?;
    let durable_prefix = input.durable_prefix.trim_end_matches('/').to_string();
    if durable_prefix.is_empty() {
        bail!("--durable-prefix must not be empty");
    }

    let mut status_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut missing_paths = Vec::new();
    let mut non_durable_paths = Vec::new();
    let mut live_candidate_count = 0_usize;

    for entry in &registry.entries {
        *status_counts
            .entry(strategy_registry_status_label(entry.status).to_string())
            .or_default() += 1;
        let blocking_live = matches!(
            entry.status,
            StrategyRegistryStatus::Active | StrategyRegistryStatus::Promoted
        );
        if blocking_live {
            live_candidate_count += 1;
        }

        for (role, path) in registry_entry_paths(entry) {
            if path.trim().is_empty() {
                continue;
            }
            if !Path::new(&path).is_file() {
                missing_paths.push(StrategyRegistryPathIssue {
                    strategy_id: entry.strategy_id.clone(),
                    status: entry.status,
                    role: role.clone(),
                    path: path.clone(),
                    blocking_live,
                    detail: "path is not readable as a file".to_string(),
                });
            }
            if !path.starts_with(&durable_prefix) {
                non_durable_paths.push(StrategyRegistryPathIssue {
                    strategy_id: entry.strategy_id.clone(),
                    status: entry.status,
                    role,
                    path,
                    blocking_live,
                    detail: format!("path is outside durable prefix `{durable_prefix}`"),
                });
            }
        }
    }

    let blocking_missing = missing_paths.iter().any(|issue| issue.blocking_live);
    let blocking_non_durable = non_durable_paths.iter().any(|issue| issue.blocking_live);
    let ok = !blocking_missing && !blocking_non_durable;
    let live_ready = ok && live_candidate_count == 1;
    let grade = if live_ready {
        "A+"
    } else if ok && missing_paths.is_empty() && non_durable_paths.is_empty() {
        "A-"
    } else if ok {
        "B+"
    } else {
        "C"
    }
    .to_string();

    let mut checks = Vec::new();
    checks.push(format!("entries={}", registry.entries.len()));
    checks.push(format!("live_candidate_count={live_candidate_count}"));
    checks.push(format!("missing_paths={}", missing_paths.len()));
    checks.push(format!("non_durable_paths={}", non_durable_paths.len()));

    let mut next_steps = Vec::new();
    if live_candidate_count == 0 {
        next_steps.push(
            "No active/promoted registry entry exists; keep live trading blocked and run fresh feed-forward search."
                .to_string(),
        );
    } else if live_candidate_count > 1 {
        next_steps.push(
            "More than one active/promoted registry entry exists; demote all but one before live."
                .to_string(),
        );
    }
    if !missing_paths.is_empty() {
        next_steps.push(
            "Run strategy-builder evidence-export and commit any missing durable evidence before promotion."
                .to_string(),
        );
    }
    if !non_durable_paths.is_empty() {
        next_steps.push(
            "Rewrite registry evidence paths to the durable promotion archive before promotion."
                .to_string(),
        );
    }
    if next_steps.is_empty() {
        next_steps.push("Registry evidence is internally promotion-ready.".to_string());
    }

    Ok(StrategyRegistryAudit {
        schema_version: 1,
        registry_path: input.registry_path.display().to_string(),
        durable_prefix,
        ok,
        live_ready,
        grade,
        entries: registry.entries.len(),
        status_counts,
        live_candidate_count,
        missing_paths,
        non_durable_paths,
        checks,
        next_steps,
    })
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
    if !(0.0..=1.0).contains(&input.tail_alpha) || input.tail_alpha <= 0.0 {
        bail!("--tail-alpha must be in (0, 1]");
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
    if !(0.0..=1.0).contains(&input.tail_alpha) || input.tail_alpha <= 0.0 {
        bail!("--tail-alpha must be in (0, 1]");
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
    if !(0.0..=1.0).contains(&input.tail_alpha) || input.tail_alpha <= 0.0 {
        bail!("--tail-alpha must be in (0, 1]");
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
    if !(0.0..=1.0).contains(&input.tail_alpha) || input.tail_alpha <= 0.0 {
        bail!("--tail-alpha must be in (0, 1]");
    }
    if input.meta_label_min_support > 0 {
        if !(0.0..=1.0).contains(&input.meta_label_alpha) || input.meta_label_alpha <= 0.0 {
            bail!("--meta-label-alpha must be in (0, 1]");
        }
        if !(0.0..=1.0).contains(&input.meta_label_max_loss_rate) {
            bail!("--meta-label-max-loss-rate must be in [0, 1]");
        }
        if input.meta_label_max_generalization_terms > CAUSAL_POLICY_DIMENSIONS.len() {
            bail!("--meta-label-max-generalization-terms is larger than the causal tag space");
        }
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

pub fn evolve_search(
    input: StrategyBuilderEvolveSearchInput,
) -> Result<StrategyBuilderEvolveSearch> {
    validate_evolve_search_input(&input)?;
    let report_set = load_selectivity_report_set(&input.report_paths, "evolve-search")?;
    let causal_input = causal_input_from_evolution(&input);
    let mut search = evolve_search_from_report_set(&report_set, &input, &causal_input)?;
    write_evolution_artifacts(&mut search, &report_set, &input)?;
    Ok(search)
}

pub fn materialize_policy_variant(
    input: StrategyBuilderMaterializePolicyVariantInput,
) -> Result<StrategyBuilderMaterializedPolicyVariant> {
    if input.rank == 0 {
        bail!("--rank must be > 0");
    }
    if input.source_report_paths.is_empty() {
        bail!("at least one --source-report is required");
    }
    let payload = std::fs::read_to_string(&input.search_path)
        .with_context(|| format!("read search artifact {}", input.search_path.display()))?;
    let search: serde_json::Value = serde_json::from_str(&payload)
        .with_context(|| format!("parse search artifact {}", input.search_path.display()))?;
    let candidate = materialize_candidate_by_rank(&search, input.rank)?;
    let source_variant = candidate
        .pointer("/genome/variant")
        .and_then(|value| value.as_str())
        .or_else(|| candidate.get("variant").and_then(|value| value.as_str()))
        .with_context(|| {
            format!(
                "candidate rank {} does not include a variant name",
                input.rank
            )
        })?
        .to_string();
    let (mut variant, source_report_path) =
        strategy_variant_from_reports(&input.source_report_paths, &source_variant)?;

    let require_tags = tags_from_json_object(candidate.pointer("/final_policy/require_tags"))
        .into_iter()
        .chain(tags_from_arg_array(
            candidate.pointer("/final_policy/harness_require_args"),
        ))
        .collect::<BTreeMap<_, _>>();
    let deny_tag_values = deny_tag_values_from_candidate(candidate)?;
    validate_causal_tag_map(&require_tags)?;
    validate_causal_tag_values(&deny_tag_values)?;
    for (dimension, value) in &require_tags {
        if deny_tag_values
            .get(dimension)
            .is_some_and(|denied| denied.contains(value))
        {
            bail!(
                "candidate rank {} both requires and denies {dimension}={value}",
                input.rank
            );
        }
    }

    let mut notes = Vec::new();
    if require_tags.is_empty() && deny_tag_values.is_empty() {
        notes.push(
            "candidate did not include final policy tags; preserving source selectivity"
                .to_string(),
        );
    } else {
        variant.selectivity =
            merge_runtime_selectivity(&variant.selectivity, &require_tags, &deny_tag_values)?;
        notes.push(
            "variant selectivity merges the source variant filter with the candidate final policy for exact replay"
                .to_string(),
        );
    }
    variant.name = format!(
        "{}_policy_rank{:03}",
        safe_path_component(&source_variant),
        input.rank
    );
    let variant_hash = stable_json_hash(&variant);
    write_json_artifact_atomic(&input.output_path, &variant)
        .with_context(|| format!("write materialized variant {}", input.output_path.display()))?;

    Ok(StrategyBuilderMaterializedPolicyVariant {
        schema_version: 1,
        rank: input.rank,
        search_path: input.search_path.display().to_string(),
        source_report_path,
        source_variant,
        output_path: input.output_path.display().to_string(),
        variant_hash,
        require_tags,
        deny_tag_values,
        selectivity: variant.selectivity,
        notes,
    })
}

pub fn materialize_sweep_variant(
    input: StrategyBuilderMaterializeSweepVariantInput,
) -> Result<StrategyBuilderMaterializedSweepVariant> {
    if input.rank == 0 {
        bail!("--rank must be > 0");
    }
    let report_path = input.report_path.display().to_string();
    let report = experiment::read_report(&report_path)
        .with_context(|| format!("load harness-sweep report {report_path}"))?;
    let variant_report = report
        .variants
        .get(input.rank - 1)
        .with_context(|| format!("report {report_path} does not contain rank {}", input.rank))?;
    let source_variant = variant_report_name(variant_report);
    let mut variant =
        serde_json::from_value::<StrategyVariant>(variant_report.strategy_params.clone())
            .with_context(|| {
                format!(
                    "report {report_path} rank {} strategy_params do not parse as StrategyVariant",
                    input.rank
                )
            })?;
    variant.name = source_variant.clone();

    let require_tags = tags_from_cli_args(&input.require_causal_tag)?;
    let deny_tag_values = tag_values_from_cli_args(&input.deny_causal_tag)?;
    validate_causal_tag_map(&require_tags)?;
    validate_causal_tag_values(&deny_tag_values)?;
    for (dimension, value) in &require_tags {
        if deny_tag_values
            .get(dimension)
            .is_some_and(|denied| denied.contains(value))
        {
            bail!(
                "rank {} both requires and denies {dimension}={value}",
                input.rank
            );
        }
    }

    let mut notes = Vec::new();
    if require_tags.is_empty() && deny_tag_values.is_empty() {
        notes.push("preserved source sweep selectivity without extra causal tags".to_string());
    } else {
        variant.selectivity =
            merge_runtime_selectivity(&variant.selectivity, &require_tags, &deny_tag_values)?;
        notes.push(
            "variant selectivity merges the source sweep row with explicit causal tags for exact replay"
                .to_string(),
        );
    }
    variant.name = format!(
        "{}_sweep_rank{:03}",
        safe_path_component(&source_variant),
        input.rank
    );
    let variant_hash = stable_json_hash(&variant);
    write_json_artifact_atomic(&input.output_path, &variant)
        .with_context(|| format!("write materialized variant {}", input.output_path.display()))?;

    Ok(StrategyBuilderMaterializedSweepVariant {
        schema_version: 1,
        rank: input.rank,
        report_path,
        source_variant,
        output_path: input.output_path.display().to_string(),
        variant_hash,
        require_tags,
        deny_tag_values,
        selectivity: variant.selectivity,
        notes,
    })
}

pub fn feature_filter_search(
    input: StrategyBuilderFeatureFilterSearchInput,
) -> Result<StrategyBuilderFeatureFilterSearch> {
    validate_feature_filter_search_input(&input)?;
    let base_payload = std::fs::read_to_string(&input.base_variant_path)
        .with_context(|| format!("read base variant {}", input.base_variant_path.display()))?;
    let base_variant: StrategyVariant = serde_json::from_str(&base_payload)
        .with_context(|| format!("parse base variant {}", input.base_variant_path.display()))?;
    let folds = load_feature_filter_folds(&input.feature_paths)?;
    let atoms = feature_filter_atoms(&folds, input.min_atom_trades, input.max_atoms);
    let drafts = feature_filter_drafts(&folds, &atoms, &base_variant.selectivity, &input)?;

    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();
    for draft in drafts {
        let key = feature_filter_draft_key(&draft);
        if !seen.insert(key) {
            continue;
        }
        let Ok(selectivity) = merge_runtime_selectivity(
            &base_variant.selectivity,
            &draft.require_tags,
            &draft.deny_tag_values,
        ) else {
            continue;
        };
        let (fitness, fold_reports) = evaluate_feature_filter_draft(&folds, &draft, &input);
        let filter_hash = stable_json_hash(&serde_json::json!({
            "require_tags": draft.require_tags,
            "deny_tag_values": draft.deny_tag_values,
        }));
        candidates.push(FeatureFilterCandidate {
            rank: 0,
            passed: fitness.passed,
            candidate_id: format!("feature_{}", &filter_hash[..16]),
            variant_path: String::new(),
            variant_hash: String::new(),
            require_tags: draft.require_tags,
            deny_tag_values: draft.deny_tag_values,
            selectivity,
            fitness,
            fold_reports,
            notes: vec![
                "feature-filter-search is static replay-row research only; exact L2 replay is required before promotion credit".to_string(),
                "candidate filters are merged into the serialized base StrategyVariant for replay".to_string(),
            ],
        });
    }
    candidates.sort_by(compare_feature_filter_candidates);
    let candidate_count = candidates.len();
    let keep = input.top.min(candidates.len());
    candidates.truncate(keep);

    std::fs::create_dir_all(&input.out_dir)
        .with_context(|| format!("create {}", input.out_dir.display()))?;
    let candidates_dir = input.out_dir.join("candidates");
    std::fs::create_dir_all(&candidates_dir)
        .with_context(|| format!("create {}", candidates_dir.display()))?;
    for (idx, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank = idx + 1;
        let mut variant = base_variant.clone();
        variant.selectivity = candidate.selectivity.clone();
        variant.name = format!(
            "{}_feature_rank{:03}",
            safe_path_component(&base_variant.name),
            idx + 1
        );
        let variant_hash = stable_json_hash(&variant);
        let rank_dir = candidates_dir.join(format!(
            "candidate_rank_{:03}_{}",
            idx + 1,
            &variant_hash[..8]
        ));
        std::fs::create_dir_all(&rank_dir)
            .with_context(|| format!("create {}", rank_dir.display()))?;
        let variant_path = rank_dir.join("variant.json");
        write_json_artifact_atomic(&variant_path, &variant)
            .with_context(|| format!("write {}", variant_path.display()))?;
        candidate.variant_path = variant_path.display().to_string();
        candidate.variant_hash = variant_hash;
        let filter_path = rank_dir.join("filter.json");
        write_json_artifact_atomic(&filter_path, candidate)
            .with_context(|| format!("write {}", filter_path.display()))?;
    }

    let ok = candidates.iter().any(|candidate| candidate.passed);
    let summary = StrategyBuilderFeatureFilterSearch {
        schema_version: 1,
        ok,
        feature_report_count: folds.len(),
        candidate_count,
        base_variant_path: input.base_variant_path.display().to_string(),
        out_dir: input.out_dir.display().to_string(),
        gates: FeatureFilterSearchGates {
            top: input.top,
            max_require_terms: input.max_require_terms,
            max_deny_terms: input.max_deny_terms,
            min_atom_trades: input.min_atom_trades,
            max_atoms: input.max_atoms,
            min_total_trades: input.min_total_trades,
            min_eligible_reports: input.min_eligible_reports,
            min_total_pnl: input.min_total_pnl,
            min_worst_report_pnl: input.min_worst_report_pnl,
        },
        candidates,
        notes: vec![
            "Reads harness-sweep trade feature JSON and proposes runtime-safe causal require/deny filters.".to_string(),
            "Static feature fitness is hypothesis context only; continuous exact replay can change trade paths.".to_string(),
            "No live parameters, registry state, or promotion artifacts are mutated.".to_string(),
        ],
    };
    let summary_path = input.out_dir.join("feature_filter_summary.json");
    write_json_artifact_atomic(&summary_path, &summary)
        .with_context(|| format!("write {}", summary_path.display()))?;
    Ok(summary)
}

fn validate_evolve_search_input(input: &StrategyBuilderEvolveSearchInput) -> Result<()> {
    if input.report_paths.len() < 2 {
        bail!("evolve-search needs at least two chronological reports");
    }
    if input.report_paths.len() <= input.min_train_reports {
        bail!(
            "report count ({}) must be greater than --min-train-reports ({})",
            input.report_paths.len(),
            input.min_train_reports
        );
    }
    if input.population == 0 {
        bail!("--population must be > 0");
    }
    if input.generations == 0 {
        bail!("--generations must be > 0");
    }
    if input.elite_count == 0 {
        bail!("--elite-count must be > 0");
    }
    if input.max_require_terms == 0 {
        bail!("--max-require-terms must be > 0");
    }
    if input.max_deny_terms > 1 {
        bail!("evolve-search only emits runtime-supported single-tag deny rules; keep --max-deny-terms <= 1");
    }
    if !(0.0..=1.0).contains(&input.tail_alpha) || input.tail_alpha <= 0.0 {
        bail!("--tail-alpha must be in (0, 1]");
    }
    if input.fold_hours <= 0 {
        bail!("--fold-hours must be > 0");
    }
    if input.window_minutes <= 0.0 {
        bail!("--window-minutes must be > 0");
    }
    if input.replay_start.is_some() != input.replay_end.is_some() {
        bail!("--replay-start and --replay-end must be provided together");
    }
    Ok(())
}

fn load_selectivity_report_set(
    report_paths: &[String],
    context: &str,
) -> Result<SelectivityReportSet> {
    let mut folds = Vec::new();
    let mut variants_by_name = BTreeMap::new();
    for report_path in report_paths {
        let report = experiment::read_report(report_path)
            .with_context(|| format!("load {context} report {report_path}"))?;
        experiment::validate_current_replay_semantics(&report)
            .with_context(|| format!("validate {context} report {report_path}"))?;
        let variants = report
            .variants
            .iter()
            .map(|variant| {
                let name = variant_report_name(variant);
                if !variants_by_name.contains_key(&name) {
                    if let Ok(mut executable) =
                        serde_json::from_value::<StrategyVariant>(variant.strategy_params.clone())
                    {
                        executable.name = name.clone();
                        variants_by_name.insert(name.clone(), executable);
                    }
                }
                let regimes = variant.diagnostics.by_regime.clone();
                SelectivityVariantFold {
                    name,
                    buckets: selectivity_buckets_for_variant(variant),
                    tagged_regimes: tagged_regimes_from_map(&regimes),
                    regimes,
                }
            })
            .collect();
        folds.push(SelectivityFold { variants });
    }
    Ok(SelectivityReportSet {
        folds,
        variants: variants_by_name,
    })
}

fn materialize_candidate_by_rank(
    search: &serde_json::Value,
    rank: usize,
) -> Result<&serde_json::Value> {
    let candidates = search
        .get("candidates")
        .and_then(|value| value.as_array())
        .context("search artifact does not include a candidates array")?;
    candidates
        .iter()
        .find(|candidate| {
            candidate
                .get("rank")
                .and_then(|value| value.as_u64())
                .is_some_and(|candidate_rank| candidate_rank == rank as u64)
        })
        .or_else(|| candidates.get(rank - 1))
        .with_context(|| format!("candidate rank {rank} was not found"))
}

fn strategy_variant_from_reports(
    report_paths: &[String],
    variant_name: &str,
) -> Result<(StrategyVariant, String)> {
    for report_path in report_paths {
        let report = experiment::read_report(report_path)
            .with_context(|| format!("load source report {report_path}"))?;
        for variant in &report.variants {
            let name = variant_report_name(variant);
            if name != variant_name {
                continue;
            }
            let mut executable = serde_json::from_value::<StrategyVariant>(
                variant.strategy_params.clone(),
            )
            .with_context(|| {
                format!(
                    "source report {report_path} variant {variant_name} strategy_params do not parse as StrategyVariant"
                )
            })?;
            executable.name = name;
            return Ok((executable, report_path.clone()));
        }
    }
    bail!("variant {variant_name} was not found in any --source-report")
}

fn deny_tag_values_from_candidate(
    candidate: &serde_json::Value,
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let mut deny_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (dimension, value) in
        tags_from_arg_array(candidate.pointer("/final_policy/harness_deny_args"))
    {
        deny_values.entry(dimension).or_default().insert(value);
    }
    if let Some(rules) = candidate
        .pointer("/final_policy/deny_rules")
        .and_then(|value| value.as_array())
    {
        for rule in rules {
            let match_tags = tags_from_json_object(rule.get("match_tags"));
            if match_tags.len() > 1 {
                bail!(
                    "candidate contains unsupported multi-term runtime deny rule: {}",
                    serde_json::to_string(&match_tags).unwrap_or_else(|_| "<invalid>".to_string())
                );
            }
            for (dimension, value) in match_tags {
                deny_values.entry(dimension).or_default().insert(value);
            }
        }
    }
    Ok(deny_values)
}

fn merge_runtime_selectivity(
    source: &SelectivityFilter,
    require_tags: &BTreeMap<String, String>,
    deny_tag_values: &BTreeMap<String, BTreeSet<String>>,
) -> Result<SelectivityFilter> {
    let mut merged = source.clone();
    for (dimension, value) in require_tags {
        insert_runtime_require_tag(&mut merged, dimension, value)?;
    }
    for (dimension, values) in deny_tag_values {
        for value in values {
            insert_runtime_deny_tag(&mut merged, dimension, value)?;
        }
    }
    Ok(merged)
}

fn insert_runtime_require_tag(
    filter: &mut SelectivityFilter,
    dimension: &str,
    value: &str,
) -> Result<()> {
    validate_causal_tag(dimension, value)?;
    if filter
        .deny_tags
        .get(dimension)
        .is_some_and(|denied| denied == value)
        || filter
            .deny_tag_values
            .get(dimension)
            .is_some_and(|denied| denied.contains(value))
    {
        bail!("runtime selectivity cannot both require and deny {dimension}={value}");
    }
    if let Some(existing) = filter.require_tags.get(dimension) {
        if existing == value {
            return Ok(());
        }
        bail!("conflicting runtime require tags for {dimension}: {existing} vs {value}");
    }
    if let Some(allowed) = filter.require_tag_values.get(dimension) {
        if !allowed.contains(value) {
            bail!("runtime require {dimension}={value} conflicts with existing allowed set");
        }
        filter.require_tag_values.remove(dimension);
    }
    filter
        .require_tags
        .insert(dimension.to_string(), value.to_string());
    Ok(())
}

fn insert_runtime_deny_tag(
    filter: &mut SelectivityFilter,
    dimension: &str,
    value: &str,
) -> Result<()> {
    validate_causal_tag(dimension, value)?;
    if filter
        .require_tags
        .get(dimension)
        .is_some_and(|required| required == value)
    {
        bail!("runtime selectivity cannot both require and deny {dimension}={value}");
    }
    if filter
        .require_tag_values
        .get(dimension)
        .is_some_and(|allowed| allowed.len() == 1 && allowed.contains(value))
    {
        bail!("runtime deny {dimension}={value} removes the only allowed require-tag value");
    }
    if filter
        .deny_tags
        .get(dimension)
        .is_some_and(|denied| denied == value)
    {
        return Ok(());
    }
    filter
        .deny_tag_values
        .entry(dimension.to_string())
        .or_default()
        .insert(value.to_string());
    Ok(())
}

fn validate_causal_tag_map(tags: &BTreeMap<String, String>) -> Result<()> {
    for (dimension, value) in tags {
        validate_causal_tag(dimension, value)?;
    }
    Ok(())
}

fn validate_causal_tag_values(tags: &BTreeMap<String, BTreeSet<String>>) -> Result<()> {
    for (dimension, values) in tags {
        for value in values {
            validate_causal_tag(dimension, value)?;
        }
    }
    Ok(())
}

fn validate_causal_tag(dimension: &str, value: &str) -> Result<()> {
    if !CAUSAL_POLICY_DIMENSIONS.contains(&dimension) {
        bail!("unsupported causal tag dimension {dimension}");
    }
    if value.trim().is_empty() {
        bail!("empty causal tag value for {dimension}");
    }
    Ok(())
}

fn validate_feature_filter_search_input(
    input: &StrategyBuilderFeatureFilterSearchInput,
) -> Result<()> {
    if input.feature_paths.is_empty() {
        bail!("feature-filter-search needs at least one --feature report");
    }
    if input.top == 0 {
        bail!("--top must be > 0");
    }
    if input.max_require_terms > CAUSAL_POLICY_DIMENSIONS.len() {
        bail!("--max-require-terms is larger than the causal tag space");
    }
    if input.max_atoms == 0 {
        bail!("--max-atoms must be > 0");
    }
    Ok(())
}

fn load_feature_filter_folds(paths: &[String]) -> Result<Vec<FeatureFilterFold>> {
    let mut folds = Vec::new();
    let mut total_rows = 0usize;
    for path in paths {
        let payload =
            std::fs::read_to_string(path).with_context(|| format!("read feature report {path}"))?;
        let report: FeatureFilterReportJson = serde_json::from_str(&payload)
            .with_context(|| format!("parse feature report {path}"))?;
        let mut rows = Vec::new();
        for row in report.rows {
            let pnl = row
                .pnl_after_fee
                .or(row.pnl)
                .with_context(|| format!("feature report {path} row missing pnl"))?;
            if !pnl.is_finite() {
                bail!("feature report {path} row has non-finite pnl");
            }
            let mut causal_tags = BTreeMap::new();
            for (dimension, value) in row.causal_tags.unwrap_or_default() {
                if CAUSAL_POLICY_DIMENSIONS.contains(&dimension.as_str()) {
                    validate_causal_tag(&dimension, &value)?;
                    causal_tags.insert(dimension, value);
                }
            }
            rows.push(FeatureFilterRow {
                pnl,
                won: row.won.unwrap_or(pnl > 0.0),
                causal_tags,
            });
        }
        total_rows += rows.len();
        folds.push(FeatureFilterFold {
            path: path.clone(),
            rows,
        });
    }
    if total_rows == 0 {
        bail!("feature-filter-search reports contain no trade rows");
    }
    Ok(folds)
}

fn feature_filter_atoms(
    folds: &[FeatureFilterFold],
    min_atom_trades: u64,
    max_atoms: usize,
) -> Vec<FeatureFilterAtom> {
    let mut stats: BTreeMap<FeatureFilterAtom, FeatureFilterAtomStats> = BTreeMap::new();
    for fold in folds {
        for row in &fold.rows {
            for (dimension, value) in &row.causal_tags {
                let entry = stats
                    .entry(FeatureFilterAtom {
                        dimension: dimension.clone(),
                        value: value.clone(),
                    })
                    .or_default();
                entry.trades += 1;
                if !row.won {
                    entry.losses += 1;
                }
                entry.pnl += row.pnl;
            }
        }
    }
    let mut atoms = stats
        .into_iter()
        .filter(|(_, stats)| stats.trades >= min_atom_trades)
        .collect::<Vec<_>>();
    atoms.sort_by(|(left_atom, left_stats), (right_atom, right_stats)| {
        right_stats
            .trades
            .cmp(&left_stats.trades)
            .then_with(|| right_stats.losses.cmp(&left_stats.losses))
            .then_with(|| f64_desc(left_stats.pnl.abs(), right_stats.pnl.abs()))
            .then_with(|| left_atom.cmp(right_atom))
    });
    atoms
        .into_iter()
        .take(max_atoms)
        .map(|(atom, _)| atom)
        .collect()
}

fn feature_filter_drafts(
    folds: &[FeatureFilterFold],
    atoms: &[FeatureFilterAtom],
    base_selectivity: &SelectivityFilter,
    input: &StrategyBuilderFeatureFilterSearchInput,
) -> Result<Vec<FeatureFilterDraft>> {
    let mut require_seeds = Vec::new();
    let mut seen_require = BTreeSet::new();
    let empty = FeatureFilterDraft {
        require_tags: BTreeMap::new(),
        deny_tag_values: BTreeMap::new(),
    };
    seen_require.insert(feature_filter_draft_key(&empty));
    require_seeds.push(empty);

    for atom in atoms {
        let current = require_seeds.clone();
        for seed in current {
            if seed.require_tags.len() >= input.max_require_terms {
                continue;
            }
            if seed.require_tags.contains_key(&atom.dimension) {
                continue;
            }
            let mut trial = seed.clone();
            trial
                .require_tags
                .insert(atom.dimension.clone(), atom.value.clone());
            if merge_runtime_selectivity(
                base_selectivity,
                &trial.require_tags,
                &trial.deny_tag_values,
            )
            .is_err()
            {
                continue;
            }
            let key = feature_filter_draft_key(&trial);
            if seen_require.insert(key) {
                require_seeds.push(trial);
            }
        }
    }

    let mut drafts = Vec::new();
    for seed in require_seeds {
        let mut current = seed.clone();
        drafts.push(current.clone());
        for _ in 0..input.max_deny_terms {
            let (current_fitness, _) = evaluate_feature_filter_draft(folds, &current, input);
            let mut best: Option<(FeatureFilterDraft, FeatureFilterFitness)> = None;
            for atom in atoms {
                if feature_filter_draft_denies(&current, &atom.dimension, &atom.value)
                    || current
                        .require_tags
                        .get(&atom.dimension)
                        .is_some_and(|required| required == &atom.value)
                {
                    continue;
                }
                let mut trial = current.clone();
                trial
                    .deny_tag_values
                    .entry(atom.dimension.clone())
                    .or_default()
                    .insert(atom.value.clone());
                if merge_runtime_selectivity(
                    base_selectivity,
                    &trial.require_tags,
                    &trial.deny_tag_values,
                )
                .is_err()
                {
                    continue;
                }
                let (fitness, _) = evaluate_feature_filter_draft(folds, &trial, input);
                if best.as_ref().is_none_or(|(_, best_fitness)| {
                    compare_feature_filter_fitness(&fitness, best_fitness) == Ordering::Less
                }) {
                    best = Some((trial, fitness));
                }
            }
            let Some((best_draft, best_fitness)) = best else {
                break;
            };
            if compare_feature_filter_fitness(&best_fitness, &current_fitness) != Ordering::Less {
                break;
            }
            current = best_draft;
            drafts.push(current.clone());
        }
    }
    Ok(drafts)
}

fn feature_filter_draft_denies(draft: &FeatureFilterDraft, dimension: &str, value: &str) -> bool {
    draft
        .deny_tag_values
        .get(dimension)
        .is_some_and(|values| values.contains(value))
}

fn feature_filter_draft_key(draft: &FeatureFilterDraft) -> String {
    serde_json::to_string(&serde_json::json!({
        "require_tags": draft.require_tags,
        "deny_tag_values": draft.deny_tag_values,
    }))
    .unwrap_or_default()
}

fn evaluate_feature_filter_draft(
    folds: &[FeatureFilterFold],
    draft: &FeatureFilterDraft,
    input: &StrategyBuilderFeatureFilterSearchInput,
) -> (FeatureFilterFitness, Vec<FeatureFilterFoldReport>) {
    let mut fold_reports = Vec::new();
    let mut fold_pnls = Vec::new();
    let mut trades = 0u64;
    let mut wins = 0u64;
    let mut losses = 0u64;
    let mut total_pnl = 0.0;
    let mut gross_profit = 0.0;
    let mut gross_loss = 0.0;
    let mut positive_trades = 0u64;
    let mut negative_trades = 0u64;

    for fold in folds {
        let mut fold_trades = 0u64;
        let mut fold_wins = 0u64;
        let mut fold_losses = 0u64;
        let mut fold_pnl = 0.0;
        for row in &fold.rows {
            if !feature_filter_row_passes(row, draft) {
                continue;
            }
            fold_trades += 1;
            trades += 1;
            if row.won {
                fold_wins += 1;
                wins += 1;
            } else {
                fold_losses += 1;
                losses += 1;
            }
            fold_pnl += row.pnl;
            total_pnl += row.pnl;
            if row.pnl > 0.0 {
                gross_profit += row.pnl;
                positive_trades += 1;
            } else if row.pnl < 0.0 {
                gross_loss += row.pnl.abs();
                negative_trades += 1;
            }
        }
        if fold_trades > 0 {
            fold_pnls.push(fold_pnl);
        }
        fold_reports.push(FeatureFilterFoldReport {
            feature_path: fold.path.clone(),
            trades: fold_trades,
            wins: fold_wins,
            losses: fold_losses,
            total_pnl: fold_pnl,
        });
    }

    let eligible_reports = fold_pnls.len();
    let worst_report_pnl = fold_pnls.iter().copied().reduce(f64::min).unwrap_or(0.0);
    let cvar_pnl = feature_filter_cvar(&fold_pnls, 0.20);
    let profit_factor = if gross_loss > 0.0 {
        gross_profit / gross_loss
    } else if gross_profit > 0.0 {
        1.0e9
    } else {
        0.0
    };
    let avg_win = if positive_trades > 0 {
        gross_profit / positive_trades as f64
    } else {
        0.0
    };
    let avg_loss = if negative_trades > 0 {
        gross_loss / negative_trades as f64
    } else {
        0.0
    };
    let payoff_ratio = if avg_loss > 0.0 {
        avg_win / avg_loss
    } else if avg_win > 0.0 {
        1.0e9
    } else {
        0.0
    };
    let wilson_win_rate_lower = wilson_lower(wins as usize, trades as usize);
    let mut failure_reasons = Vec::new();
    if trades < input.min_total_trades {
        failure_reasons.push("trades_below_gate".to_string());
    }
    if eligible_reports < input.min_eligible_reports {
        failure_reasons.push("eligible_reports_below_gate".to_string());
    }
    if total_pnl + 1e-9 < input.min_total_pnl {
        failure_reasons.push("total_pnl_below_gate".to_string());
    }
    if worst_report_pnl + 1e-9 < input.min_worst_report_pnl {
        failure_reasons.push("worst_report_pnl_below_gate".to_string());
    }
    let passed = failure_reasons.is_empty();
    (
        FeatureFilterFitness {
            passed,
            failure_reasons,
            eligible_reports,
            trades,
            wins,
            losses,
            total_pnl,
            worst_report_pnl,
            cvar_pnl,
            profit_factor,
            payoff_ratio,
            wilson_win_rate_lower,
        },
        fold_reports,
    )
}

fn feature_filter_row_passes(row: &FeatureFilterRow, draft: &FeatureFilterDraft) -> bool {
    for (dimension, value) in &draft.require_tags {
        if row.causal_tags.get(dimension) != Some(value) {
            return false;
        }
    }
    for (dimension, denied_values) in &draft.deny_tag_values {
        if row
            .causal_tags
            .get(dimension)
            .is_some_and(|value| denied_values.contains(value))
        {
            return false;
        }
    }
    true
}

fn compare_feature_filter_candidates(
    left: &FeatureFilterCandidate,
    right: &FeatureFilterCandidate,
) -> Ordering {
    compare_feature_filter_fitness(&left.fitness, &right.fitness)
        .then_with(|| left.require_tags.len().cmp(&right.require_tags.len()))
        .then_with(|| feature_filter_deny_count(left).cmp(&feature_filter_deny_count(right)))
        .then_with(|| left.candidate_id.cmp(&right.candidate_id))
}

fn compare_feature_filter_fitness(
    left: &FeatureFilterFitness,
    right: &FeatureFilterFitness,
) -> Ordering {
    right
        .passed
        .cmp(&left.passed)
        .then_with(|| left.failure_reasons.len().cmp(&right.failure_reasons.len()))
        .then_with(|| left.losses.cmp(&right.losses))
        .then_with(|| f64_desc(left.worst_report_pnl, right.worst_report_pnl))
        .then_with(|| f64_desc(left.cvar_pnl, right.cvar_pnl))
        .then_with(|| f64_desc(left.payoff_ratio, right.payoff_ratio))
        .then_with(|| f64_desc(left.profit_factor, right.profit_factor))
        .then_with(|| f64_desc(left.wilson_win_rate_lower, right.wilson_win_rate_lower))
        .then_with(|| right.eligible_reports.cmp(&left.eligible_reports))
        .then_with(|| right.trades.cmp(&left.trades))
        .then_with(|| f64_desc(left.total_pnl, right.total_pnl))
}

fn feature_filter_deny_count(candidate: &FeatureFilterCandidate) -> usize {
    candidate.deny_tag_values.values().map(BTreeSet::len).sum()
}

fn feature_filter_cvar(values: &[f64], alpha: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let tail_len = ((sorted.len() as f64) * alpha).ceil().max(1.0) as usize;
    let tail_len = tail_len.min(sorted.len());
    sorted.iter().take(tail_len).sum::<f64>() / tail_len as f64
}

fn causal_input_from_evolution(
    input: &StrategyBuilderEvolveSearchInput,
) -> StrategyBuilderCausalPolicySearchInput {
    StrategyBuilderCausalPolicySearchInput {
        report_paths: input.report_paths.clone(),
        min_train_reports: input.min_train_reports,
        min_train_trades: input.min_train_trades,
        min_oos_trades: input.min_oos_trades,
        min_oos_wilson_win_rate_lower: input.min_oos_wilson_win_rate_lower,
        min_oos_total_pnl: input.min_oos_total_pnl,
        min_oos_profitable_reports: input.min_oos_profitable_reports,
        min_oos_eligible_reports: input.min_oos_eligible_reports,
        min_worst_oos_pnl: input.min_worst_oos_pnl,
        max_require_terms: input.max_require_terms,
        max_deny_rules: input.max_deny_rules,
        max_deny_terms: input.max_deny_terms,
        min_deny_trades: input.min_deny_trades,
        min_deny_loss_pnl: input.min_deny_loss_pnl,
        min_deny_loss_reports: input.min_deny_loss_reports,
        tail_alpha: input.tail_alpha,
        min_oos_cvar_pnl: input.min_oos_cvar_pnl,
        loss_burst_lookback: input.loss_burst_lookback,
        max_loss_burst_reports: input.max_loss_burst_reports,
        tail_first_ranking: true,
        min_oos_payoff_ratio: input.min_oos_payoff_ratio,
        max_oos_worst_loss_to_avg_win: input.max_oos_worst_loss_to_avg_win,
        prior_loss_cluster_lookback: input.prior_loss_cluster_lookback,
        max_prior_loss_burst_reports: input.max_prior_loss_burst_reports,
        min_prior_payoff_ratio: input.min_prior_payoff_ratio,
        max_prior_worst_loss_to_avg_win: input.max_prior_worst_loss_to_avg_win,
        meta_label_min_support: input.meta_label_min_support,
        meta_label_alpha: input.meta_label_alpha,
        meta_label_min_quantile_pnl: input.meta_label_min_quantile_pnl,
        meta_label_max_loss_rate: input.meta_label_max_loss_rate,
        meta_label_require_supported: input.meta_label_require_supported,
        meta_label_max_generalization_terms: input.meta_label_max_generalization_terms,
        top: input.population.max(input.top).max(1),
    }
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
        let a_cvar_gate = a.fold_forward.tail.cvar_pnl >= input.min_oos_cvar_pnl;
        let b_cvar_gate = b.fold_forward.tail.cvar_pnl >= input.min_oos_cvar_pnl;
        let a_burst_gate = input.max_loss_burst_reports == 0
            || a.fold_forward.tail.max_loss_burst_reports <= input.max_loss_burst_reports;
        let b_burst_gate = input.max_loss_burst_reports == 0
            || b.fold_forward.tail.max_loss_burst_reports <= input.max_loss_burst_reports;
        b.passed
            .cmp(&a.passed)
            .then_with(|| b_trade_gate.cmp(&a_trade_gate))
            .then_with(|| b_profitable_gate.cmp(&a_profitable_gate))
            .then_with(|| b_pnl_gate.cmp(&a_pnl_gate))
            .then_with(|| b_wilson_gate.cmp(&a_wilson_gate))
            .then_with(|| b_tail_gate.cmp(&a_tail_gate))
            .then_with(|| b_cvar_gate.cmp(&a_cvar_gate))
            .then_with(|| b_burst_gate.cmp(&a_burst_gate))
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
            .then_with(|| {
                f64_desc(
                    a.fold_forward.worst_report_pnl,
                    b.fold_forward.worst_report_pnl,
                )
            })
            .then_with(|| f64_desc(a.fold_forward.tail.cvar_pnl, b.fold_forward.tail.cvar_pnl))
            .then_with(|| {
                a.fold_forward
                    .tail
                    .max_loss_burst_reports
                    .cmp(&b.fold_forward.tail.max_loss_burst_reports)
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
            "Report OOS fold-tail CVaR and recent loss-burst metrics so guarded candidates cannot hide clustered left-tail failures behind positive aggregate PnL."
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
            tail_alpha: input.tail_alpha,
            min_oos_cvar_pnl: input.min_oos_cvar_pnl,
            loss_burst_lookback: input.loss_burst_lookback,
            max_loss_burst_reports: input.max_loss_burst_reports,
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
            .then_with(|| {
                f64_desc(
                    a.fold_forward.worst_report_pnl,
                    b.fold_forward.worst_report_pnl,
                )
            })
            .then_with(|| f64_desc(a.fold_forward.tail.cvar_pnl, b.fold_forward.tail.cvar_pnl))
            .then_with(|| {
                a.fold_forward
                    .tail
                    .max_loss_burst_reports
                    .cmp(&b.fold_forward.tail.max_loss_burst_reports)
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
            "Report OOS fold-tail CVaR and recent loss-burst metrics so adaptive direction cannot pass on average PnL while hiding clustered losses."
                .to_string(),
            "Treat this as a regime-selection hypothesis; rerun any selected adaptive policy through full harness/live-replay before promotion."
                .to_string(),
        ],
        gates: AdaptiveDirectionSearchGates {
            min_train_reports: input.min_train_reports,
            min_train_trades: input.min_train_trades,
            min_oos_trades: input.min_oos_trades,
            min_oos_wilson_win_rate_lower: input.min_oos_wilson_win_rate_lower,
            min_oos_total_pnl: input.min_oos_total_pnl,
            min_oos_profitable_reports: input.min_oos_profitable_reports,
            min_worst_oos_pnl: input.min_worst_oos_pnl,
            tail_alpha: input.tail_alpha,
            min_oos_cvar_pnl: input.min_oos_cvar_pnl,
            loss_burst_lookback: input.loss_burst_lookback,
            max_loss_burst_reports: input.max_loss_burst_reports,
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
        let a_cvar_gate = a.fold_forward.tail.cvar_pnl >= input.min_oos_cvar_pnl;
        let b_cvar_gate = b.fold_forward.tail.cvar_pnl >= input.min_oos_cvar_pnl;
        let a_burst_gate = input.max_loss_burst_reports == 0
            || a.fold_forward.tail.max_loss_burst_reports <= input.max_loss_burst_reports;
        let b_burst_gate = input.max_loss_burst_reports == 0
            || b.fold_forward.tail.max_loss_burst_reports <= input.max_loss_burst_reports;
        b.passed
            .cmp(&a.passed)
            .then_with(|| b_trade_gate.cmp(&a_trade_gate))
            .then_with(|| b_profitable_gate.cmp(&a_profitable_gate))
            .then_with(|| b_pnl_gate.cmp(&a_pnl_gate))
            .then_with(|| b_wilson_gate.cmp(&a_wilson_gate))
            .then_with(|| b_tail_gate.cmp(&a_tail_gate))
            .then_with(|| b_cvar_gate.cmp(&a_cvar_gate))
            .then_with(|| b_burst_gate.cmp(&a_burst_gate))
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
            .then_with(|| {
                f64_desc(
                    a.fold_forward.worst_report_pnl,
                    b.fold_forward.worst_report_pnl,
                )
            })
            .then_with(|| f64_desc(a.fold_forward.tail.cvar_pnl, b.fold_forward.tail.cvar_pnl))
            .then_with(|| {
                a.fold_forward
                    .tail
                    .max_loss_burst_reports
                    .cmp(&b.fold_forward.tail.max_loss_burst_reports)
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
            "Report OOS fold-tail CVaR and recent loss-burst metrics so adaptive mode cannot pass on average PnL while hiding clustered losses.".to_string(),
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
            tail_alpha: input.tail_alpha,
            min_oos_cvar_pnl: input.min_oos_cvar_pnl,
            loss_burst_lookback: input.loss_burst_lookback,
            max_loss_burst_reports: input.max_loss_burst_reports,
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
        let a_eligible_gate = input.min_oos_eligible_reports == 0
            || a.fold_forward.eligible_reports >= input.min_oos_eligible_reports;
        let b_eligible_gate = input.min_oos_eligible_reports == 0
            || b.fold_forward.eligible_reports >= input.min_oos_eligible_reports;
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
        let a_cvar_gate = a.fold_forward.tail.cvar_pnl >= input.min_oos_cvar_pnl;
        let b_cvar_gate = b.fold_forward.tail.cvar_pnl >= input.min_oos_cvar_pnl;
        let a_burst_gate = input.max_loss_burst_reports == 0
            || a.fold_forward.tail.max_loss_burst_reports <= input.max_loss_burst_reports;
        let b_burst_gate = input.max_loss_burst_reports == 0
            || b.fold_forward.tail.max_loss_burst_reports <= input.max_loss_burst_reports;
        let a_payoff_gate = input.min_oos_payoff_ratio <= 0.0
            || a.fold_forward.stats.payoff_ratio >= input.min_oos_payoff_ratio;
        let b_payoff_gate = input.min_oos_payoff_ratio <= 0.0
            || b.fold_forward.stats.payoff_ratio >= input.min_oos_payoff_ratio;
        let a_worst_loss_gate = input.max_oos_worst_loss_to_avg_win <= 0.0
            || a.fold_forward.stats.worst_loss_to_avg_win <= input.max_oos_worst_loss_to_avg_win;
        let b_worst_loss_gate = input.max_oos_worst_loss_to_avg_win <= 0.0
            || b.fold_forward.stats.worst_loss_to_avg_win <= input.max_oos_worst_loss_to_avg_win;
        let gate_order = b
            .passed
            .cmp(&a.passed)
            .then_with(|| b_eligible_gate.cmp(&a_eligible_gate))
            .then_with(|| b_trade_gate.cmp(&a_trade_gate))
            .then_with(|| b_profitable_gate.cmp(&a_profitable_gate))
            .then_with(|| b_pnl_gate.cmp(&a_pnl_gate))
            .then_with(|| b_wilson_gate.cmp(&a_wilson_gate))
            .then_with(|| b_tail_gate.cmp(&a_tail_gate))
            .then_with(|| b_cvar_gate.cmp(&a_cvar_gate))
            .then_with(|| b_burst_gate.cmp(&a_burst_gate))
            .then_with(|| b_payoff_gate.cmp(&a_payoff_gate))
            .then_with(|| b_worst_loss_gate.cmp(&a_worst_loss_gate));
        if input.tail_first_ranking {
            return gate_order
                .then_with(|| {
                    a.fold_forward
                        .tail
                        .max_loss_burst_reports
                        .cmp(&b.fold_forward.tail.max_loss_burst_reports)
                })
                .then_with(|| {
                    f64_desc(
                        a.fold_forward.worst_report_pnl,
                        b.fold_forward.worst_report_pnl,
                    )
                })
                .then_with(|| f64_desc(a.fold_forward.tail.cvar_pnl, b.fold_forward.tail.cvar_pnl))
                .then_with(|| {
                    f64_asc(
                        a.fold_forward.stats.worst_loss_to_avg_win,
                        b.fold_forward.stats.worst_loss_to_avg_win,
                    )
                })
                .then_with(|| {
                    f64_desc(
                        a.fold_forward.stats.payoff_ratio,
                        b.fold_forward.stats.payoff_ratio,
                    )
                })
                .then_with(|| {
                    f64_desc(
                        a.fold_forward.stats.profit_factor,
                        b.fold_forward.stats.profit_factor,
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
                        a.fold_forward.stats.total_pnl,
                        b.fold_forward.stats.total_pnl,
                    )
                })
                .then_with(|| a.base_require.len().cmp(&b.base_require.len()))
                .then_with(|| a.variant.cmp(&b.variant))
                .then_with(|| policy_label(&a.base_require).cmp(&policy_label(&b.base_require)));
        }
        gate_order
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
            .then_with(|| f64_desc(a.fold_forward.tail.cvar_pnl, b.fold_forward.tail.cvar_pnl))
            .then_with(|| {
                a.fold_forward
                    .tail
                    .max_loss_burst_reports
                    .cmp(&b.fold_forward.tail.max_loss_burst_reports)
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
            "Report OOS fold-tail CVaR and recent loss-burst metrics so average-positive candidates with clustered drawdowns stay visible as tail risk."
                .to_string(),
            "Rank candidates by pass status, gate completion, aggregate OOS PnL, trade count, Wilson lower bound, worst fold, CVaR, loss-burst size, and policy simplicity."
                .to_string(),
            "Optional tail-first ranking prioritizes loss-burst size, worst fold, CVaR, and payoff asymmetry before aggregate PnL."
                .to_string(),
            "Optional prior-only risk gates flatten the next fold when the selected policy already shows a loss cluster or poor payoff geometry in previous folds."
                .to_string(),
            "The loss-cluster sentinel uses only the most recent configured prior window, so a policy can resume after the bad state rolls out."
                .to_string(),
            "Optional meta-label risk control checks active full-regime buckets against strictly prior outcomes before scoring the next fold."
                .to_string(),
            "Optional meta-label generalization backs off from sparse exact regimes to broader causal tag combinations, still using only prior fold outcomes."
                .to_string(),
            "Optional eligible-report coverage gate prevents thin policies from passing on one active OOS report plus abstentions."
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
            min_oos_eligible_reports: input.min_oos_eligible_reports,
            min_worst_oos_pnl: input.min_worst_oos_pnl,
            max_require_terms: input.max_require_terms,
            max_deny_rules: input.max_deny_rules,
            max_deny_terms: input.max_deny_terms,
            min_deny_trades: input.min_deny_trades,
            min_deny_loss_pnl: input.min_deny_loss_pnl,
            min_deny_loss_reports: input.min_deny_loss_reports,
            tail_alpha: input.tail_alpha,
            min_oos_cvar_pnl: input.min_oos_cvar_pnl,
            loss_burst_lookback: input.loss_burst_lookback,
            max_loss_burst_reports: input.max_loss_burst_reports,
            tail_first_ranking: input.tail_first_ranking,
            min_oos_payoff_ratio: input.min_oos_payoff_ratio,
            max_oos_worst_loss_to_avg_win: input.max_oos_worst_loss_to_avg_win,
            prior_loss_cluster_lookback: input.prior_loss_cluster_lookback,
            max_prior_loss_burst_reports: input.max_prior_loss_burst_reports,
            min_prior_payoff_ratio: input.min_prior_payoff_ratio,
            max_prior_worst_loss_to_avg_win: input.max_prior_worst_loss_to_avg_win,
            meta_label_min_support: input.meta_label_min_support,
            meta_label_alpha: input.meta_label_alpha,
            meta_label_min_quantile_pnl: input.meta_label_min_quantile_pnl,
            meta_label_max_loss_rate: input.meta_label_max_loss_rate,
            meta_label_require_supported: input.meta_label_require_supported,
            meta_label_max_generalization_terms: input.meta_label_max_generalization_terms,
        },
        candidates,
    }
}

fn evolve_search_from_report_set(
    report_set: &SelectivityReportSet,
    input: &StrategyBuilderEvolveSearchInput,
    causal_input: &StrategyBuilderCausalPolicySearchInput,
) -> Result<StrategyBuilderEvolveSearch> {
    let variants = variant_names(&report_set.folds);
    if variants.is_empty() {
        bail!("evolve-search found no variants in the input reports");
    }
    let tag_universe = causal_tag_universe(&report_set.folds);
    if tag_universe.is_empty() {
        bail!("evolve-search found no causal regime tags in the input reports");
    }
    let historical_genomes = historical_evolution_seed_genomes(report_set, input, &variants)?;

    let mut rng = StdRng::seed_from_u64(input.seed);
    let mut population = initial_evolution_population(
        report_set,
        input,
        &tag_universe,
        &variants,
        &historical_genomes,
        &mut rng,
    );
    let mut all_candidates = Vec::new();
    let mut trial_ledger = Vec::new();
    let mut generation_reports = Vec::new();

    for generation in 0..input.generations {
        let mut evaluated = population
            .iter()
            .map(|member| {
                evaluate_evolution_genome_against_variants(
                    &report_set.folds,
                    causal_input,
                    generation,
                    member.parent_hashes.clone(),
                    member.genome.clone(),
                    &report_set.variants,
                )
            })
            .collect::<Vec<_>>();
        assign_evolution_pareto_fronts(&mut evaluated);
        evaluated.sort_by(compare_evolution_candidates);
        for (idx, candidate) in evaluated.iter_mut().enumerate() {
            candidate.rank = idx + 1;
        }

        trial_ledger.extend(evaluated.iter().map(evolution_trial_ledger_row));
        all_candidates.extend(evaluated.clone());

        let survivor_limit = input
            .elite_count
            .min(evaluated.len())
            .max(1)
            .min(input.population);
        let survivor_indexes =
            evolution_diverse_candidate_indexes(&evaluated, survivor_limit, variants.len());
        let survivor_hashes = survivor_indexes
            .iter()
            .map(|idx| evaluated[*idx].genome_hash.clone())
            .collect::<Vec<_>>();
        generation_reports.push(EvolutionGeneration {
            generation,
            population_count: population.len(),
            evaluated_count: evaluated.len(),
            pareto_front_count: evaluated
                .iter()
                .map(|candidate| candidate.pareto_front)
                .collect::<BTreeSet<_>>()
                .len(),
            best_candidate_ids: evaluated
                .iter()
                .take(5)
                .map(|candidate| candidate.candidate_id.clone())
                .collect(),
            survivor_hashes: survivor_hashes.clone(),
        });

        if generation + 1 == input.generations {
            break;
        }
        population = next_evolution_population(
            &evaluated,
            input,
            &tag_universe,
            &variants,
            &report_set.variants,
            &survivor_indexes,
            &mut rng,
        );
    }

    let mut unique_candidates = unique_evolution_candidates(all_candidates);
    assign_evolution_pareto_fronts(&mut unique_candidates);
    unique_candidates.sort_by(compare_evolution_candidates);
    for (idx, candidate) in unique_candidates.iter_mut().enumerate() {
        candidate.rank = idx + 1;
    }
    let candidate_count = unique_candidates.len();
    let any_passed = unique_candidates.iter().any(|candidate| candidate.passed);
    let failure_summary = evolution_failure_summary(&unique_candidates);
    unique_candidates = select_evolution_output_candidates(&unique_candidates, input.top.max(1));

    let config = evolution_run_config(input);
    let run_hash = stable_json_hash(&config);
    let mut artifact_paths = BTreeMap::new();
    artifact_paths.insert(
        "summary".to_string(),
        input
            .out_dir
            .join("evolution_summary.json")
            .display()
            .to_string(),
    );
    artifact_paths.insert(
        "trial_ledger".to_string(),
        input
            .out_dir
            .join("trial_ledger.jsonl")
            .display()
            .to_string(),
    );
    artifact_paths.insert(
        "generations".to_string(),
        input.out_dir.join("generations").display().to_string(),
    );
    artifact_paths.insert(
        "candidates".to_string(),
        input.out_dir.join("candidates").display().to_string(),
    );

    let mut notes = vec![
        "evolve-search is offline research only; it does not mutate live parameters or registry state".to_string(),
        "static/report fitness is hypothesis context; replay is required before promotion credit".to_string(),
        "strategy-knob or selectivity counterfactuals receive no source-report fitness credit; their artifacts are replay hypotheses only".to_string(),
        "top artifacts reserve replay-hypothesis capacity for runtime counterfactuals so exact replay can feed successful variants into the next evolution run".to_string(),
        "genomes emit only runtime-supported single-tag deny filters".to_string(),
    ];
    if !input.historical_search_paths.is_empty() {
        notes.push(format!(
            "seeded {} historical genomes from {} search artifact(s)",
            historical_genomes.len(),
            input.historical_search_paths.len()
        ));
    }
    if !any_passed {
        notes.push(format!(
            "no candidate passed configured tail-first gates; top failure classes: {}",
            failure_summary
        ));
    }

    Ok(StrategyBuilderEvolveSearch {
        schema_version: 1,
        ok: any_passed,
        report_count: report_set.folds.len(),
        candidate_count,
        run: EvolutionRunManifest {
            schema_version: 1,
            run_id: format!("evo_{}", &run_hash[..16]),
            generated_at: "offline_deterministic".to_string(),
            config,
            artifact_paths,
        },
        methodology: vec![
            "Seed the population from current report variants, supplied historical search artifacts, and observed causal tag policies; bias initialization toward direction, reversion, tight/fresh/deep order-book, and maker-safe contexts.".to_string(),
            "Mutate runtime-safe StrategyVariant thresholds, execution and microstructure knobs plus causal require tags and single-tag deny vetoes from the frozen causal policy dimensions; crossover stays within one source-variant family.".to_string(),
            "Evaluate genomes with the same chronological feed-forward report mechanics as causal-policy-search; optional deny learning is prior-only. Any strategy-knob or runtime-selectivity change is marked replay-required because source reports cannot score counterfactual re-entry paths.".to_string(),
            "Apply a deterministic source-family diversity cap to elites and parent pools before mutation.".to_string(),
            "Rank candidates with NSGA-II-style non-dominated fronts and a tail-first comparator: gates, loss burst, worst fold, CVaR, payoff geometry, Wilson lower bound, coverage, median expectancy, then total PnL.".to_string(),
            "Write deterministic research artifacts only; promotion remains blocked until replay, robust-promote, zone audit, evidence export, and registry audit pass.".to_string(),
        ],
        gates: causal_policy_gates(causal_input),
        generations: generation_reports,
        candidates: unique_candidates,
        notes,
        trial_ledger,
    })
}

fn evolution_run_config(input: &StrategyBuilderEvolveSearchInput) -> EvolutionRunConfig {
    EvolutionRunConfig {
        seed: input.seed,
        population: input.population,
        generations: input.generations,
        elite_count: input.elite_count,
        top: input.top,
        report_paths: input.report_paths.clone(),
        historical_search_paths: input.historical_search_paths.clone(),
        out_dir: input.out_dir.display().to_string(),
        replay: EvolutionReplayConfig {
            start: input.replay_start.clone(),
            end: input.replay_end.clone(),
            profile: input.replay_profile.clone(),
            zone_mode: input.replay_zone_mode.clone(),
            latency_ms: input.latency_ms,
            latency_audit_json: input.latency_audit_json.clone(),
            btc_csv: input.btc_csv.clone(),
            fold_hours: input.fold_hours,
            threads: input.threads,
            window_minutes: input.window_minutes,
            atomic_parquet: input.atomic_parquet,
            execute: false,
        },
    }
}

fn causal_policy_gates(input: &StrategyBuilderCausalPolicySearchInput) -> CausalPolicySearchGates {
    CausalPolicySearchGates {
        min_train_reports: input.min_train_reports,
        min_train_trades: input.min_train_trades,
        min_oos_trades: input.min_oos_trades,
        min_oos_wilson_win_rate_lower: input.min_oos_wilson_win_rate_lower,
        min_oos_total_pnl: input.min_oos_total_pnl,
        min_oos_profitable_reports: input.min_oos_profitable_reports,
        min_oos_eligible_reports: input.min_oos_eligible_reports,
        min_worst_oos_pnl: input.min_worst_oos_pnl,
        max_require_terms: input.max_require_terms,
        max_deny_rules: input.max_deny_rules,
        max_deny_terms: input.max_deny_terms,
        min_deny_trades: input.min_deny_trades,
        min_deny_loss_pnl: input.min_deny_loss_pnl,
        min_deny_loss_reports: input.min_deny_loss_reports,
        tail_alpha: input.tail_alpha,
        min_oos_cvar_pnl: input.min_oos_cvar_pnl,
        loss_burst_lookback: input.loss_burst_lookback,
        max_loss_burst_reports: input.max_loss_burst_reports,
        tail_first_ranking: input.tail_first_ranking,
        min_oos_payoff_ratio: input.min_oos_payoff_ratio,
        max_oos_worst_loss_to_avg_win: input.max_oos_worst_loss_to_avg_win,
        prior_loss_cluster_lookback: input.prior_loss_cluster_lookback,
        max_prior_loss_burst_reports: input.max_prior_loss_burst_reports,
        min_prior_payoff_ratio: input.min_prior_payoff_ratio,
        max_prior_worst_loss_to_avg_win: input.max_prior_worst_loss_to_avg_win,
        meta_label_min_support: input.meta_label_min_support,
        meta_label_alpha: input.meta_label_alpha,
        meta_label_min_quantile_pnl: input.meta_label_min_quantile_pnl,
        meta_label_max_loss_rate: input.meta_label_max_loss_rate,
        meta_label_require_supported: input.meta_label_require_supported,
        meta_label_max_generalization_terms: input.meta_label_max_generalization_terms,
    }
}

fn causal_tag_universe(folds: &[SelectivityFold]) -> BTreeMap<String, BTreeSet<String>> {
    let allowed = CAUSAL_POLICY_DIMENSIONS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut universe: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for fold in folds {
        for variant in &fold.variants {
            for regime in &variant.tagged_regimes {
                for (dimension, value) in &regime.tags {
                    if allowed.contains(dimension.as_str()) {
                        universe
                            .entry(dimension.clone())
                            .or_default()
                            .insert(value.clone());
                    }
                }
            }
        }
    }
    universe
}

fn initial_evolution_population(
    report_set: &SelectivityReportSet,
    input: &StrategyBuilderEvolveSearchInput,
    tag_universe: &BTreeMap<String, BTreeSet<String>>,
    variants: &[String],
    historical_genomes: &[EvolutionGenome],
    rng: &mut StdRng,
) -> Vec<EvolutionPopulationMember> {
    let mut population = Vec::new();
    let mut seen = BTreeSet::new();
    let mut keys = causal_policy_candidate_keys(&report_set.folds, input.max_require_terms);
    keys.sort_by(|a, b| {
        evolution_tag_bias_score(&b.require_tags)
            .cmp(&evolution_tag_bias_score(&a.require_tags))
            .then_with(|| a.variant.cmp(&b.variant))
            .then_with(|| policy_label(&a.require_tags).cmp(&policy_label(&b.require_tags)))
    });

    let base_variant_limit = if !keys.is_empty() && input.population > 1 {
        variants.len().min((input.population / 4).max(1))
    } else {
        variants.len().min(input.population)
    };
    for variant in variants.iter().take(base_variant_limit) {
        let genome = EvolutionGenome {
            schema_version: 1,
            variant: variant.clone(),
            require_tags: BTreeMap::new(),
            deny_tags: BTreeMap::new(),
            knobs: report_set
                .variants
                .get(variant)
                .map(evolution_knobs_from_variant)
                .unwrap_or_default(),
        };
        push_unique_evolution_member(&mut population, &mut seen, genome, Vec::new());
    }

    let path_seed_limit = if input.population >= 4 {
        (input.population / 6).max(1)
    } else {
        0
    };
    let path_seeds = [(10.0, 0.06), (15.0, 0.08), (30.0, 0.10)];
    let mut path_seed_count = 0_usize;
    'variant_path_seeds: for variant in variants {
        for (lookback_seconds, max_runup) in path_seeds {
            if population.len() >= input.population || path_seed_count >= path_seed_limit {
                break 'variant_path_seeds;
            }
            let mut genome = EvolutionGenome {
                schema_version: 1,
                variant: variant.clone(),
                require_tags: BTreeMap::new(),
                deny_tags: BTreeMap::new(),
                knobs: report_set
                    .variants
                    .get(variant)
                    .map(evolution_knobs_from_variant)
                    .unwrap_or_default(),
            };
            genome.knobs.recent_mid_lookback_seconds = Some(lookback_seconds);
            genome.knobs.max_recent_mid_runup = Some(max_runup);
            normalize_evolution_knobs(&mut genome.knobs);
            if push_unique_evolution_member(&mut population, &mut seen, genome, Vec::new()) {
                path_seed_count += 1;
            }
        }
    }

    let historical_limit = if !keys.is_empty() && input.population > 2 {
        (input.population / 3).max(1)
    } else {
        input.population
    };
    for genome in historical_genomes.iter().take(historical_limit) {
        if population.len() >= input.population {
            break;
        }
        let mut genome = genome.clone();
        normalize_evolution_genome(&mut genome, tag_universe, input);
        if genome.require_tags.is_empty() && genome.deny_tags.is_empty() {
            continue;
        }
        push_unique_evolution_member(&mut population, &mut seen, genome, Vec::new());
    }

    for key in &keys {
        if population.len() >= input.population {
            break;
        }
        let genome = evolution_genome_from_key(key, &report_set.variants);
        push_unique_evolution_member(&mut population, &mut seen, genome, Vec::new());
    }

    let mut attempts = 0_usize;
    while population.len() < input.population && attempts < input.population.saturating_mul(40) {
        attempts += 1;
        let genome = if keys.is_empty() {
            let variant = variants[rng.gen_range(0..variants.len())].clone();
            EvolutionGenome {
                schema_version: 1,
                variant: variant.clone(),
                require_tags: BTreeMap::new(),
                deny_tags: BTreeMap::new(),
                knobs: report_set
                    .variants
                    .get(&variant)
                    .map(evolution_knobs_from_variant)
                    .unwrap_or_default(),
            }
        } else {
            let key = keys[rng.gen_range(0..keys.len())].clone();
            evolution_genome_from_key(&key, &report_set.variants)
        };
        let genome = mutate_evolution_genome(
            genome,
            tag_universe,
            variants,
            &report_set.variants,
            input,
            rng,
        );
        push_unique_evolution_member(&mut population, &mut seen, genome, Vec::new());
    }
    population
}

fn historical_evolution_seed_genomes(
    report_set: &SelectivityReportSet,
    input: &StrategyBuilderEvolveSearchInput,
    variants: &[String],
) -> Result<Vec<EvolutionGenome>> {
    if input.historical_search_paths.is_empty() {
        return Ok(Vec::new());
    }
    let valid_variants = variants.iter().cloned().collect::<BTreeSet<_>>();
    let mut genomes = Vec::new();
    let mut seen = BTreeSet::new();
    for path in &input.historical_search_paths {
        let payload = std::fs::read_to_string(path)
            .with_context(|| format!("read historical evolution seed artifact {path}"))?;
        let value: serde_json::Value = serde_json::from_str(&payload)
            .with_context(|| format!("parse historical evolution seed artifact {path}"))?;
        let Some(candidates) = value.get("candidates").and_then(|value| value.as_array()) else {
            continue;
        };
        for candidate in candidates {
            for genome in historical_evolution_genomes_from_candidate(
                candidate,
                report_set,
                variants,
                &valid_variants,
            ) {
                if genome.require_tags.is_empty() && genome.deny_tags.is_empty() {
                    continue;
                }
                let hash = evolution_genome_hash(&genome);
                if seen.insert(hash) {
                    genomes.push(genome);
                }
            }
        }
    }
    Ok(genomes)
}

fn historical_evolution_genomes_from_candidate(
    candidate: &serde_json::Value,
    report_set: &SelectivityReportSet,
    variants: &[String],
    valid_variants: &BTreeSet<String>,
) -> Vec<EvolutionGenome> {
    let source_variant = candidate
        .pointer("/genome/variant")
        .and_then(|value| value.as_str())
        .or_else(|| candidate.get("variant").and_then(|value| value.as_str()));
    let historical_knobs = candidate
        .pointer("/genome/knobs")
        .cloned()
        .and_then(|value| serde_json::from_value::<EvolutionStrategyKnobs>(value).ok());
    let mut require_tags = tags_from_json_object(candidate.pointer("/genome/require_tags"));
    if require_tags.is_empty() {
        require_tags = tags_from_json_object(candidate.pointer("/final_policy/require_tags"));
    }
    if require_tags.is_empty() {
        require_tags = tags_from_arg_array(candidate.pointer("/final_policy/harness_require_args"));
    }

    let mut deny_tags = tags_from_json_object(candidate.pointer("/genome/deny_tags"));
    if deny_tags.is_empty() {
        deny_tags = tags_from_arg_array(candidate.pointer("/final_policy/harness_deny_args"));
    }
    if deny_tags.is_empty() {
        deny_tags = tags_from_policy_rules(candidate.pointer("/final_policy/deny_rules"));
    }

    if require_tags.is_empty() && deny_tags.is_empty() {
        return Vec::new();
    }

    let target_variants = if let Some(variant) = source_variant {
        if valid_variants.contains(variant) {
            vec![variant.to_string()]
        } else {
            variants.to_vec()
        }
    } else {
        variants.to_vec()
    };

    target_variants
        .into_iter()
        .map(|variant| EvolutionGenome {
            schema_version: 1,
            knobs: if source_variant == Some(variant.as_str()) {
                historical_knobs.clone().unwrap_or_else(|| {
                    report_set
                        .variants
                        .get(&variant)
                        .map(evolution_knobs_from_variant)
                        .unwrap_or_default()
                })
            } else {
                report_set
                    .variants
                    .get(&variant)
                    .map(evolution_knobs_from_variant)
                    .unwrap_or_default()
            },
            variant,
            require_tags: require_tags.clone(),
            deny_tags: deny_tags.clone(),
        })
        .collect()
}

fn tags_from_json_object(value: Option<&serde_json::Value>) -> BTreeMap<String, String> {
    value
        .and_then(|value| value.as_object())
        .map(|object| {
            object
                .iter()
                .filter_map(|(dimension, value)| {
                    value
                        .as_str()
                        .map(|tag_value| (dimension.clone(), tag_value.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn tags_from_arg_array(value: Option<&serde_json::Value>) -> BTreeMap<String, String> {
    value
        .and_then(|value| value.as_array())
        .map(|args| {
            args.iter()
                .filter_map(|value| value.as_str())
                .filter_map(tag_from_arg)
                .collect()
        })
        .unwrap_or_default()
}

fn tags_from_policy_rules(value: Option<&serde_json::Value>) -> BTreeMap<String, String> {
    let Some(rules) = value.and_then(|value| value.as_array()) else {
        return BTreeMap::new();
    };
    let mut tags = BTreeMap::new();
    for rule in rules {
        let rule_tags = tags_from_json_object(rule.get("match_tags"));
        if rule_tags.len() == 1 {
            tags.extend(rule_tags);
        }
    }
    tags
}

fn tag_from_arg(raw: &str) -> Option<(String, String)> {
    let (dimension, value) = raw.split_once('=')?;
    if dimension.is_empty() || value.is_empty() {
        return None;
    }
    Some((dimension.to_string(), value.to_string()))
}

fn tags_from_cli_args(args: &[String]) -> Result<BTreeMap<String, String>> {
    let mut tags = BTreeMap::new();
    for raw in args {
        let (dimension, value) =
            tag_from_arg(raw).with_context(|| format!("invalid causal tag `{raw}`"))?;
        if let Some(existing) = tags.insert(dimension.clone(), value.clone()) {
            if existing != value {
                bail!("conflicting causal tag values for {dimension}: {existing} vs {value}");
            }
        }
    }
    Ok(tags)
}

fn tag_values_from_cli_args(args: &[String]) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let mut tags: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for raw in args {
        let (dimension, value) =
            tag_from_arg(raw).with_context(|| format!("invalid causal tag `{raw}`"))?;
        tags.entry(dimension).or_default().insert(value);
    }
    Ok(tags)
}

fn push_unique_evolution_member(
    population: &mut Vec<EvolutionPopulationMember>,
    seen: &mut BTreeSet<String>,
    genome: EvolutionGenome,
    parent_hashes: Vec<String>,
) -> bool {
    let hash = evolution_genome_hash(&genome);
    if !seen.insert(hash) {
        return false;
    }
    population.push(EvolutionPopulationMember {
        genome,
        parent_hashes,
    });
    true
}

fn evolution_genome_from_key(
    key: &CausalPolicyKey,
    variants: &BTreeMap<String, StrategyVariant>,
) -> EvolutionGenome {
    EvolutionGenome {
        schema_version: 1,
        variant: key.variant.clone(),
        require_tags: key.require_tags.clone(),
        deny_tags: BTreeMap::new(),
        knobs: variants
            .get(&key.variant)
            .map(evolution_knobs_from_variant)
            .unwrap_or_default(),
    }
}

fn evolution_knobs_from_variant(variant: &StrategyVariant) -> EvolutionStrategyKnobs {
    EvolutionStrategyKnobs {
        min_confidence: Some(variant.min_confidence),
        min_edge: Some(variant.min_edge),
        early_min_z: Some(variant.zone_config.early_min_z),
        primary_min_z: Some(variant.zone_config.primary_min_z),
        late_min_z: Some(variant.zone_config.late_min_z),
        terminal_min_z: Some(variant.zone_config.terminal_min_z),
        min_price: Some(variant.zone_config.min_price),
        max_price: Some(variant.zone_config.max_price),
        min_ev_buffer: Some(variant.zone_config.min_ev_buffer),
        settlement_guard_minutes: Some(variant.zone_config.settlement_guard_minutes),
        settlement_min_abs_move_usd: Some(variant.zone_config.settlement_min_abs_move_usd),
        min_reversion_count: Some(variant.zone_config.min_reversion_count),
        max_reversion_count: Some(variant.zone_config.max_reversion_count),
        prefer_maker: Some(variant.prefer_maker),
        max_spread: Some(variant.microstructure.max_spread),
        min_book_depth: Some(variant.microstructure.min_book_depth),
        min_book_pressure: Some(variant.microstructure.min_book_pressure),
        recent_mid_lookback_seconds: Some(variant.microstructure.recent_mid_lookback_seconds),
        max_recent_mid_runup: Some(variant.microstructure.max_recent_mid_runup),
    }
}

fn evolution_tag_bias_score(tags: &BTreeMap<String, String>) -> i32 {
    tags.iter()
        .map(|(dimension, value)| match dimension.as_str() {
            "book_spread" if value.contains("lte") || value.contains("zero") => 5,
            "book_age" if value.contains("lte") || value.contains("ms") => 5,
            "book_min_depth" => 4,
            "book_pressure" if value.contains("positive") => 4,
            "bookwalk_slippage" if value.contains("zero") || value.contains("lte") => 4,
            "book_runup" if value.contains("lte") => 5,
            "outcome_overround" if value.contains("lte") => 5,
            "btc_impulse_10s" => 2,
            "reversion" => 3,
            "confidence" | "z" | "edge" => 2,
            "utc_session" => 1,
            "utc_hour" => 1,
            "direction_utc_session" => 1,
            "direction_utc_hour" => 1,
            "direction" | "zone" => 1,
            _ => 0,
        })
        .sum()
}

fn next_evolution_population(
    evaluated: &[EvolutionCandidate],
    input: &StrategyBuilderEvolveSearchInput,
    tag_universe: &BTreeMap<String, BTreeSet<String>>,
    variants: &[String],
    variant_params: &BTreeMap<String, StrategyVariant>,
    survivor_indexes: &[usize],
    rng: &mut StdRng,
) -> Vec<EvolutionPopulationMember> {
    let mut next = Vec::new();
    let mut seen = BTreeSet::new();
    for idx in survivor_indexes {
        let candidate = &evaluated[*idx];
        push_unique_evolution_member(
            &mut next,
            &mut seen,
            candidate.genome.clone(),
            vec![candidate.genome_hash.clone()],
        );
    }

    let parent_pool_limit = evaluated.len().min((survivor_indexes.len() * 3).max(2));
    let parent_pool =
        evolution_diverse_candidate_indexes(evaluated, parent_pool_limit, variants.len())
            .into_iter()
            .map(|idx| &evaluated[idx])
            .collect::<Vec<_>>();
    let mut attempts = 0_usize;
    while next.len() < input.population && attempts < input.population.saturating_mul(60) {
        attempts += 1;
        let parent_a = parent_pool[rng.gen_range(0..parent_pool.len())];
        let same_family = parent_pool
            .iter()
            .filter(|candidate| candidate.genome.variant == parent_a.genome.variant)
            .copied()
            .collect::<Vec<_>>();
        let (genome, parent_hashes) = if same_family.len() > 1 && rng.gen_bool(0.35) {
            let parent_b = same_family[rng.gen_range(0..same_family.len())];
            (
                crossover_evolution_genome(&parent_a.genome, &parent_b.genome, input, rng),
                vec![parent_a.genome_hash.clone(), parent_b.genome_hash.clone()],
            )
        } else {
            (parent_a.genome.clone(), vec![parent_a.genome_hash.clone()])
        };
        let genome =
            mutate_evolution_genome(genome, tag_universe, variants, variant_params, input, rng);
        push_unique_evolution_member(&mut next, &mut seen, genome, parent_hashes);
    }
    next
}

fn evolution_diverse_candidate_indexes(
    evaluated: &[EvolutionCandidate],
    limit: usize,
    family_count: usize,
) -> Vec<usize> {
    let limit = limit.min(evaluated.len());
    if limit == 0 {
        return Vec::new();
    }
    let family_cap = if family_count <= 1 {
        limit
    } else {
        limit.div_ceil(2).max(1)
    };
    let mut selected = Vec::with_capacity(limit);
    let mut selected_set = BTreeSet::new();
    let mut family_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for (idx, candidate) in evaluated.iter().enumerate() {
        let count = family_counts
            .get(candidate.genome.variant.as_str())
            .copied()
            .unwrap_or_default();
        if count >= family_cap {
            continue;
        }
        selected.push(idx);
        selected_set.insert(idx);
        family_counts.insert(candidate.genome.variant.as_str(), count + 1);
        if selected.len() == limit {
            return selected;
        }
    }
    for idx in 0..evaluated.len() {
        if selected_set.insert(idx) {
            selected.push(idx);
            if selected.len() == limit {
                break;
            }
        }
    }
    selected
}

fn mutate_evolution_genome(
    mut genome: EvolutionGenome,
    tag_universe: &BTreeMap<String, BTreeSet<String>>,
    variants: &[String],
    variant_params: &BTreeMap<String, StrategyVariant>,
    input: &StrategyBuilderEvolveSearchInput,
    rng: &mut StdRng,
) -> EvolutionGenome {
    let allow_static_deny = input.max_deny_rules > 0;
    let action_count = if allow_static_deny { 7 } else { 5 };
    match rng.gen_range(0..action_count) {
        0 => {
            if let Some((dimension, value)) = random_evolution_tag(tag_universe, rng) {
                insert_bounded_tag(
                    &mut genome.require_tags,
                    dimension,
                    value,
                    input.max_require_terms,
                    rng,
                );
            }
        }
        1 => remove_random_tag(&mut genome.require_tags, rng),
        2 => {
            if variants.len() > 1 {
                genome.variant = variants[rng.gen_range(0..variants.len())].clone();
                sync_evolution_genome_knobs(&mut genome, variant_params);
            } else if let Some((dimension, value)) = random_evolution_tag(tag_universe, rng) {
                insert_bounded_tag(
                    &mut genome.require_tags,
                    dimension,
                    value,
                    input.max_require_terms,
                    rng,
                );
            }
        }
        3 => {
            if let Some((dimension, value)) = random_evolution_tag(tag_universe, rng) {
                if rng.gen_bool(0.55) {
                    insert_bounded_tag(
                        &mut genome.require_tags,
                        dimension,
                        value,
                        input.max_require_terms,
                        rng,
                    );
                } else if allow_static_deny {
                    insert_bounded_tag(
                        &mut genome.deny_tags,
                        dimension,
                        value,
                        input.max_deny_rules.max(1),
                        rng,
                    );
                }
            }
        }
        4 => mutate_evolution_knob(&mut genome.knobs, rng),
        5 => {
            if let Some((dimension, value)) = random_evolution_tag(tag_universe, rng) {
                insert_bounded_tag(
                    &mut genome.deny_tags,
                    dimension,
                    value,
                    input.max_deny_rules.max(1),
                    rng,
                );
            }
        }
        _ => remove_random_tag(&mut genome.deny_tags, rng),
    }
    normalize_evolution_genome(&mut genome, tag_universe, input);
    genome
}

fn mutate_evolution_knob(knobs: &mut EvolutionStrategyKnobs, rng: &mut StdRng) {
    match rng.gen_range(0..19) {
        0 => knobs.min_confidence = Some(random_choice_f64(rng, &[0.40, 0.50, 0.60, 0.70])),
        1 => knobs.min_edge = Some(random_choice_f64(rng, &[0.03, 0.07, 0.10, 0.15])),
        2 => knobs.early_min_z = Some(random_choice_f64(rng, &[0.50, 0.70, 0.90, 1.10, 1.50])),
        3 => knobs.primary_min_z = Some(random_choice_f64(rng, &[0.50, 0.70, 0.90, 1.10, 1.50])),
        4 => knobs.late_min_z = Some(random_choice_f64(rng, &[0.50, 0.70, 0.90, 1.10, 1.50])),
        5 => knobs.terminal_min_z = Some(random_choice_f64(rng, &[0.50, 0.70, 0.90, 1.10, 1.50])),
        6 => knobs.min_price = Some(random_choice_f64(rng, &[0.10, 0.50, 0.75])),
        7 => knobs.max_price = Some(random_choice_f64(rng, &[0.75, 0.85, 0.90])),
        8 => knobs.min_ev_buffer = Some(random_choice_f64(rng, &[-1.0, 0.0, 0.03, 0.05])),
        9 => knobs.settlement_guard_minutes = Some(random_choice_f64(rng, &[1.0, 2.0, 3.0])),
        10 => knobs.settlement_min_abs_move_usd = Some(random_choice_f64(rng, &[5.0, 10.0, 15.0])),
        11 => knobs.min_reversion_count = Some(random_choice_u64(rng, &[0, 1, 2])),
        12 => knobs.max_reversion_count = Some(random_choice_u64(rng, &[2, 3, u64::MAX])),
        13 => knobs.prefer_maker = Some(rng.gen_bool(0.5)),
        14 => knobs.max_spread = Some(random_choice_f64(rng, &[0.02, 0.03, 1.0])),
        15 => knobs.min_book_depth = Some(random_choice_f64(rng, &[0.0, 50.0, 100.0, 250.0])),
        16 => knobs.min_book_pressure = Some(random_choice_f64(rng, &[-1.0, 0.0, 0.10])),
        17 => {
            knobs.recent_mid_lookback_seconds =
                Some(random_choice_f64(rng, &[5.0, 10.0, 15.0, 30.0]))
        }
        _ => {
            knobs.max_recent_mid_runup =
                Some(random_choice_f64(rng, &[0.04, 0.06, 0.08, 0.10, 1.0]))
        }
    }
    normalize_evolution_knobs(knobs);
}

fn random_choice_f64(rng: &mut StdRng, values: &[f64]) -> f64 {
    values[rng.gen_range(0..values.len())]
}

fn random_choice_u64(rng: &mut StdRng, values: &[u64]) -> u64 {
    values[rng.gen_range(0..values.len())]
}

fn normalize_evolution_knobs(knobs: &mut EvolutionStrategyKnobs) {
    if let Some(value) = knobs.min_confidence {
        knobs.min_confidence = Some(value.clamp(0.0, 1.0));
    }
    if let Some(value) = knobs.min_edge {
        knobs.min_edge = Some(value.clamp(0.0, 1.0));
    }
    for value in [
        &mut knobs.early_min_z,
        &mut knobs.primary_min_z,
        &mut knobs.late_min_z,
        &mut knobs.terminal_min_z,
    ] {
        if let Some(z) = *value {
            *value = Some(z.clamp(0.0, 10.0));
        }
    }
    if let Some(value) = knobs.min_price {
        knobs.min_price = Some(value.clamp(0.01, 0.99));
    }
    if let Some(value) = knobs.max_price {
        knobs.max_price = Some(value.clamp(0.01, 0.99));
    }
    if let (Some(min_price), Some(max_price)) = (knobs.min_price, knobs.max_price) {
        if max_price < min_price {
            knobs.max_price = Some(min_price);
        }
    }
    if let Some(value) = knobs.min_ev_buffer {
        knobs.min_ev_buffer = Some(value.clamp(-1.0, 1.0));
    }
    if let Some(value) = knobs.settlement_guard_minutes {
        knobs.settlement_guard_minutes = Some(value.clamp(0.0, 10.0));
    }
    if let Some(value) = knobs.settlement_min_abs_move_usd {
        knobs.settlement_min_abs_move_usd = Some(value.clamp(0.0, 100.0));
    }
    if let (Some(min_reversion), Some(max_reversion)) =
        (knobs.min_reversion_count, knobs.max_reversion_count)
    {
        if max_reversion != u64::MAX && max_reversion < min_reversion {
            knobs.max_reversion_count = Some(min_reversion);
        }
    }
    if let Some(value) = knobs.max_spread {
        knobs.max_spread = Some(value.max(0.0));
    }
    if let Some(value) = knobs.min_book_depth {
        knobs.min_book_depth = Some(value.max(0.0));
    }
    if let Some(value) = knobs.min_book_pressure {
        knobs.min_book_pressure = Some(value.clamp(-1.0, 1.0));
    }
    if let Some(value) = knobs.recent_mid_lookback_seconds {
        knobs.recent_mid_lookback_seconds = Some(value.clamp(1.0, 300.0));
    }
    if let Some(value) = knobs.max_recent_mid_runup {
        knobs.max_recent_mid_runup = Some(value.clamp(0.0, 1.0));
    }
}

fn crossover_evolution_genome(
    left: &EvolutionGenome,
    right: &EvolutionGenome,
    input: &StrategyBuilderEvolveSearchInput,
    rng: &mut StdRng,
) -> EvolutionGenome {
    let mut child = left.clone();
    if left.variant != right.variant {
        return child;
    }
    child.require_tags.clear();
    child.deny_tags.clear();
    for (dimension, value) in left.require_tags.iter().chain(right.require_tags.iter()) {
        if child.require_tags.len() < input.max_require_terms && rng.gen_bool(0.65) {
            child.require_tags.insert(dimension.clone(), value.clone());
        }
    }
    for (dimension, value) in left.deny_tags.iter().chain(right.deny_tags.iter()) {
        if child.deny_tags.len() < input.max_deny_rules && rng.gen_bool(0.50) {
            child.deny_tags.insert(dimension.clone(), value.clone());
        }
    }
    macro_rules! choose_knob {
        ($field:ident) => {
            if rng.gen_bool(0.5) {
                left.knobs.$field
            } else {
                right.knobs.$field
            }
        };
    }
    child.knobs = EvolutionStrategyKnobs {
        min_confidence: choose_knob!(min_confidence),
        min_edge: choose_knob!(min_edge),
        early_min_z: choose_knob!(early_min_z),
        primary_min_z: choose_knob!(primary_min_z),
        late_min_z: choose_knob!(late_min_z),
        terminal_min_z: choose_knob!(terminal_min_z),
        min_price: choose_knob!(min_price),
        max_price: choose_knob!(max_price),
        min_ev_buffer: choose_knob!(min_ev_buffer),
        settlement_guard_minutes: choose_knob!(settlement_guard_minutes),
        settlement_min_abs_move_usd: choose_knob!(settlement_min_abs_move_usd),
        min_reversion_count: choose_knob!(min_reversion_count),
        max_reversion_count: choose_knob!(max_reversion_count),
        prefer_maker: choose_knob!(prefer_maker),
        max_spread: choose_knob!(max_spread),
        min_book_depth: choose_knob!(min_book_depth),
        min_book_pressure: choose_knob!(min_book_pressure),
        recent_mid_lookback_seconds: choose_knob!(recent_mid_lookback_seconds),
        max_recent_mid_runup: choose_knob!(max_recent_mid_runup),
    };
    normalize_evolution_knobs(&mut child.knobs);
    child
}

fn sync_evolution_genome_knobs(
    genome: &mut EvolutionGenome,
    variants: &BTreeMap<String, StrategyVariant>,
) {
    genome.knobs = variants
        .get(&genome.variant)
        .map(evolution_knobs_from_variant)
        .unwrap_or_default();
}

fn normalize_evolution_genome(
    genome: &mut EvolutionGenome,
    tag_universe: &BTreeMap<String, BTreeSet<String>>,
    input: &StrategyBuilderEvolveSearchInput,
) {
    genome.require_tags.retain(|dimension, value| {
        tag_universe
            .get(dimension)
            .is_some_and(|values| values.contains(value))
    });
    genome.deny_tags.retain(|dimension, value| {
        tag_universe
            .get(dimension)
            .is_some_and(|values| values.contains(value))
            && genome
                .require_tags
                .get(dimension)
                .is_none_or(|required| required != value)
    });
    while genome.require_tags.len() > input.max_require_terms {
        let Some(last) = genome.require_tags.keys().next_back().cloned() else {
            break;
        };
        genome.require_tags.remove(&last);
    }
    while genome.deny_tags.len() > input.max_deny_rules {
        let Some(last) = genome.deny_tags.keys().next_back().cloned() else {
            break;
        };
        genome.deny_tags.remove(&last);
    }
}

fn random_evolution_tag(
    tag_universe: &BTreeMap<String, BTreeSet<String>>,
    rng: &mut StdRng,
) -> Option<(String, String)> {
    let dimensions = tag_universe.keys().collect::<Vec<_>>();
    if dimensions.is_empty() {
        return None;
    }
    let dimension = dimensions[rng.gen_range(0..dimensions.len())];
    let values = tag_universe.get(dimension)?.iter().collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    Some((
        dimension.clone(),
        values[rng.gen_range(0..values.len())].clone(),
    ))
}

fn insert_bounded_tag(
    tags: &mut BTreeMap<String, String>,
    dimension: String,
    value: String,
    max_terms: usize,
    rng: &mut StdRng,
) {
    if max_terms == 0 {
        return;
    }
    if !tags.contains_key(&dimension) && tags.len() >= max_terms {
        remove_random_tag(tags, rng);
    }
    tags.insert(dimension, value);
}

fn remove_random_tag(tags: &mut BTreeMap<String, String>, rng: &mut StdRng) {
    if tags.is_empty() {
        return;
    }
    let keys = tags.keys().cloned().collect::<Vec<_>>();
    let key = &keys[rng.gen_range(0..keys.len())];
    tags.remove(key);
}

#[cfg(test)]
fn evaluate_evolution_genome(
    folds: &[SelectivityFold],
    input: &StrategyBuilderCausalPolicySearchInput,
    generation: usize,
    parent_hashes: Vec<String>,
    genome: EvolutionGenome,
) -> EvolutionCandidate {
    evaluate_evolution_genome_with_evidence(
        folds,
        input,
        generation,
        parent_hashes,
        genome,
        true,
        true,
    )
}

fn evaluate_evolution_genome_against_variants(
    folds: &[SelectivityFold],
    input: &StrategyBuilderCausalPolicySearchInput,
    generation: usize,
    parent_hashes: Vec<String>,
    genome: EvolutionGenome,
    variants: &BTreeMap<String, StrategyVariant>,
) -> EvolutionCandidate {
    let candidate = evaluate_causal_policy_candidate_with_fixed_denies(
        folds,
        input,
        CausalPolicyKey {
            variant: genome.variant.clone(),
            require_tags: genome.require_tags.clone(),
        },
        &genome.deny_tags,
    );
    let (replayable_policy, replay_error, static_selectivity_exact) =
        match variants.get(&genome.variant) {
            Some(source) => match runtime_selectivity_from_causal_policy(
                &source.selectivity,
                &candidate.final_policy,
            ) {
                Ok(runtime_selectivity) => (true, None, runtime_selectivity == source.selectivity),
                Err(err) => (false, Some(err.to_string()), false),
            },
            None => (false, None, false),
        };
    let static_knobs_exact = variants
        .get(&genome.variant)
        .map(evolution_knobs_from_variant)
        .is_some_and(|source| source == genome.knobs);
    let static_fitness_exact = static_knobs_exact && static_selectivity_exact;
    let mut evaluated = build_evolution_candidate(
        input,
        generation,
        parent_hashes,
        genome,
        candidate,
        replayable_policy,
        static_fitness_exact,
    );
    if let Some(err) = replay_error {
        evaluated
            .notes
            .push(format!("runtime policy is not executable: {err}"));
    }
    if let Some(source) = variants.get(&evaluated.genome.variant) {
        if source.use_perfect_fill {
            reject_evolution_execution_model(
                &mut evaluated,
                "perfect_fill_model_not_promotable",
                "perfect-fill candidates are sanity baselines only",
            );
        }
        if evaluated
            .genome
            .knobs
            .prefer_maker
            .unwrap_or(source.prefer_maker)
        {
            reject_evolution_execution_model(
                &mut evaluated,
                "synthetic_maker_fill_model_not_promotable",
                "maker hypotheses require a queue/trade-calibrated replay before promotion",
            );
        }
    }
    evaluated
}

fn reject_evolution_execution_model(
    candidate: &mut EvolutionCandidate,
    failure_reason: &str,
    note: &str,
) {
    if !candidate
        .fitness
        .failure_reasons
        .iter()
        .any(|reason| reason == failure_reason)
    {
        candidate
            .fitness
            .failure_reasons
            .push(failure_reason.to_string());
        candidate.fitness.gate_failures += 1;
    }
    candidate.fitness.passed = false;
    candidate.passed = false;
    candidate.notes.push(note.to_string());
}

#[cfg(test)]
fn evaluate_evolution_genome_with_evidence(
    folds: &[SelectivityFold],
    input: &StrategyBuilderCausalPolicySearchInput,
    generation: usize,
    parent_hashes: Vec<String>,
    genome: EvolutionGenome,
    replayable_policy: bool,
    static_fitness_exact: bool,
) -> EvolutionCandidate {
    let candidate = evaluate_causal_policy_candidate_with_fixed_denies(
        folds,
        input,
        CausalPolicyKey {
            variant: genome.variant.clone(),
            require_tags: genome.require_tags.clone(),
        },
        &genome.deny_tags,
    );
    build_evolution_candidate(
        input,
        generation,
        parent_hashes,
        genome,
        candidate,
        replayable_policy,
        static_fitness_exact,
    )
}

fn build_evolution_candidate(
    input: &StrategyBuilderCausalPolicySearchInput,
    generation: usize,
    parent_hashes: Vec<String>,
    genome: EvolutionGenome,
    candidate: CausalPolicyCandidateReport,
    replayable_policy: bool,
    static_fitness_exact: bool,
) -> EvolutionCandidate {
    let genome_hash = evolution_genome_hash(&genome);
    let fitness = evolution_fitness(&candidate, input, replayable_policy, static_fitness_exact);
    let mut notes = candidate.notes.clone();
    if !genome.deny_tags.is_empty() {
        notes.push(
            "genome includes fixed single-tag deny filters before prior-only deny learning"
                .to_string(),
        );
    }
    if !static_fitness_exact {
        notes.push(
            "strategy knobs or runtime selectivity differ from the source report; static fitness is not credited and exact replay is required"
                .to_string(),
        );
    }
    EvolutionCandidate {
        rank: 0,
        passed: fitness.passed,
        generation,
        pareto_front: usize::MAX,
        candidate_id: format!("evo_{}", &genome_hash[..16]),
        genome_hash,
        parent_hashes,
        genome,
        fitness,
        final_policy: candidate.final_policy,
        aggregate_static_final_policy: candidate.aggregate_static_final_policy,
        fold_forward: candidate.fold_forward,
        variant_path: None,
        replay_manifest_path: None,
        notes,
    }
}

fn evolution_genome_hash(genome: &EvolutionGenome) -> String {
    stable_json_hash(genome)
}

fn evolution_fitness(
    candidate: &CausalPolicyCandidateReport,
    input: &StrategyBuilderCausalPolicySearchInput,
    replayable_policy: bool,
    static_fitness_exact: bool,
) -> EvolutionFitness {
    let fold = &candidate.fold_forward;
    let stats = &fold.stats;
    let mut failure_reasons = Vec::new();
    if !replayable_policy {
        failure_reasons.push("source_variant_not_replayable".to_string());
    }
    if !static_fitness_exact {
        failure_reasons.push("report_counterfactual_requires_replay".to_string());
    }
    if stats.trades < input.min_oos_trades {
        failure_reasons.push("oos_trades_below_gate".to_string());
    }
    if stats.wilson_win_rate_lower < input.min_oos_wilson_win_rate_lower {
        failure_reasons.push("wilson_lower_below_gate".to_string());
    }
    if stats.total_pnl < input.min_oos_total_pnl {
        failure_reasons.push("total_pnl_below_gate".to_string());
    }
    if fold.profitable_reports < input.min_oos_profitable_reports {
        failure_reasons.push("profitable_reports_below_gate".to_string());
    }
    if input.min_oos_eligible_reports > 0 && fold.eligible_reports < input.min_oos_eligible_reports
    {
        failure_reasons.push("eligible_reports_below_gate".to_string());
    }
    if fold.eligible_reports == 0 {
        if fold
            .decisions
            .iter()
            .any(|decision| decision.reason == "policy_prior_stats_failed_train_gates")
        {
            failure_reasons.push("all_oos_blocked_by_prior_train_gates".to_string());
        } else if fold
            .decisions
            .iter()
            .any(|decision| decision.reason == "policy_had_no_oos_trades")
        {
            failure_reasons.push("all_oos_abstained_no_trades".to_string());
        } else {
            failure_reasons.push("no_eligible_oos_reports".to_string());
        }
    }
    if fold.worst_report_pnl < input.min_worst_oos_pnl {
        failure_reasons.push("worst_fold_below_gate".to_string());
    }
    if fold.tail.cvar_pnl < input.min_oos_cvar_pnl {
        failure_reasons.push("cvar_below_gate".to_string());
    }
    if input.max_loss_burst_reports > 0
        && fold.tail.max_loss_burst_reports > input.max_loss_burst_reports
    {
        failure_reasons.push("loss_burst_above_gate".to_string());
    }
    if input.min_oos_payoff_ratio > 0.0 && stats.payoff_ratio < input.min_oos_payoff_ratio {
        failure_reasons.push("payoff_ratio_below_gate".to_string());
    }
    if input.max_oos_worst_loss_to_avg_win > 0.0
        && stats.worst_loss_to_avg_win > input.max_oos_worst_loss_to_avg_win
    {
        failure_reasons.push("worst_loss_to_avg_win_above_gate".to_string());
    }

    EvolutionFitness {
        passed: candidate.passed && replayable_policy && static_fitness_exact,
        replayable_policy,
        static_fitness_exact,
        gate_failures: failure_reasons.len(),
        failure_reasons,
        eligible_reports: fold.eligible_reports,
        profitable_reports: fold.profitable_reports,
        losing_reports: fold.losing_reports,
        abstained_reports: fold.abstained_reports,
        trades: stats.trades,
        wins: stats.wins,
        losses: stats.losses,
        wilson_win_rate_lower: stats.wilson_win_rate_lower,
        total_pnl: stats.total_pnl,
        worst_report_pnl: fold.worst_report_pnl,
        cvar_pnl: fold.tail.cvar_pnl,
        max_loss_burst_reports: fold.tail.max_loss_burst_reports,
        worst_loss_to_avg_win: stats.worst_loss_to_avg_win,
        payoff_ratio: stats.payoff_ratio,
        profit_factor: stats.profit_factor,
        median_expectancy: median_oos_expectancy(fold),
    }
}

fn median_oos_expectancy(fold: &CausalPolicyFoldForwardReport) -> f64 {
    let mut values = fold
        .decisions
        .iter()
        .filter_map(|decision| decision.oos.as_ref())
        .filter(|stats| stats.trades > 0)
        .map(|stats| stats.avg_pnl)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    values[values.len() / 2]
}

fn assign_evolution_pareto_fronts(candidates: &mut [EvolutionCandidate]) {
    let mut remaining = (0..candidates.len()).collect::<BTreeSet<_>>();
    let mut front = 0_usize;
    while !remaining.is_empty() {
        let current = remaining.iter().copied().collect::<Vec<_>>();
        let mut non_dominated = Vec::new();
        for idx in &current {
            let dominated = current.iter().any(|other| {
                other != idx && evolution_dominates(&candidates[*other], &candidates[*idx])
            });
            if !dominated {
                non_dominated.push(*idx);
            }
        }
        if non_dominated.is_empty() {
            for idx in current {
                candidates[idx].pareto_front = front;
                remaining.remove(&idx);
            }
        } else {
            for idx in non_dominated {
                candidates[idx].pareto_front = front;
                remaining.remove(&idx);
            }
        }
        front += 1;
    }
}

fn evolution_dominates(left: &EvolutionCandidate, right: &EvolutionCandidate) -> bool {
    let checks = [
        compare_bool_obj(left.passed, right.passed),
        compare_bool_obj(
            left.fitness.replayable_policy,
            right.fitness.replayable_policy,
        ),
        compare_bool_obj(
            left.fitness.static_fitness_exact,
            right.fitness.static_fitness_exact,
        ),
        compare_low_obj(
            left.fitness.gate_failures as f64,
            right.fitness.gate_failures as f64,
        ),
        compare_low_obj(
            left.fitness.max_loss_burst_reports as f64,
            right.fitness.max_loss_burst_reports as f64,
        ),
        compare_high_obj(
            left.fitness.worst_report_pnl,
            right.fitness.worst_report_pnl,
        ),
        compare_high_obj(left.fitness.cvar_pnl, right.fitness.cvar_pnl),
        compare_low_obj(
            left.fitness.worst_loss_to_avg_win,
            right.fitness.worst_loss_to_avg_win,
        ),
        compare_high_obj(left.fitness.payoff_ratio, right.fitness.payoff_ratio),
        compare_high_obj(left.fitness.profit_factor, right.fitness.profit_factor),
        compare_high_obj(
            left.fitness.wilson_win_rate_lower,
            right.fitness.wilson_win_rate_lower,
        ),
        compare_high_obj(
            left.fitness.eligible_reports as f64,
            right.fitness.eligible_reports as f64,
        ),
        compare_high_obj(left.fitness.trades as f64, right.fitness.trades as f64),
        compare_high_obj(
            left.fitness.median_expectancy,
            right.fitness.median_expectancy,
        ),
        compare_high_obj(left.fitness.total_pnl, right.fitness.total_pnl),
    ];
    checks.iter().all(|comparison| *comparison >= 0)
        && checks.iter().any(|comparison| *comparison > 0)
}

fn compare_bool_obj(left: bool, right: bool) -> i8 {
    match (left, right) {
        (true, false) => 1,
        (false, true) => -1,
        _ => 0,
    }
}

fn compare_high_obj(left: f64, right: f64) -> i8 {
    if left > right + 1e-12 {
        1
    } else if left + 1e-12 < right {
        -1
    } else {
        0
    }
}

fn compare_low_obj(left: f64, right: f64) -> i8 {
    compare_high_obj(-left, -right)
}

fn compare_evolution_candidates(left: &EvolutionCandidate, right: &EvolutionCandidate) -> Ordering {
    left.pareto_front
        .cmp(&right.pareto_front)
        .then_with(|| right.passed.cmp(&left.passed))
        .then_with(|| {
            right
                .fitness
                .replayable_policy
                .cmp(&left.fitness.replayable_policy)
        })
        .then_with(|| {
            right
                .fitness
                .static_fitness_exact
                .cmp(&left.fitness.static_fitness_exact)
        })
        .then_with(|| left.fitness.gate_failures.cmp(&right.fitness.gate_failures))
        .then_with(|| {
            left.fitness
                .max_loss_burst_reports
                .cmp(&right.fitness.max_loss_burst_reports)
        })
        .then_with(|| {
            f64_desc(
                left.fitness.worst_report_pnl,
                right.fitness.worst_report_pnl,
            )
        })
        .then_with(|| f64_desc(left.fitness.cvar_pnl, right.fitness.cvar_pnl))
        .then_with(|| {
            f64_asc(
                left.fitness.worst_loss_to_avg_win,
                right.fitness.worst_loss_to_avg_win,
            )
        })
        .then_with(|| f64_desc(left.fitness.payoff_ratio, right.fitness.payoff_ratio))
        .then_with(|| f64_desc(left.fitness.profit_factor, right.fitness.profit_factor))
        .then_with(|| {
            f64_desc(
                left.fitness.wilson_win_rate_lower,
                right.fitness.wilson_win_rate_lower,
            )
        })
        .then_with(|| {
            right
                .fitness
                .eligible_reports
                .cmp(&left.fitness.eligible_reports)
        })
        .then_with(|| right.fitness.trades.cmp(&left.fitness.trades))
        .then_with(|| {
            f64_desc(
                left.fitness.median_expectancy,
                right.fitness.median_expectancy,
            )
        })
        .then_with(|| f64_desc(left.fitness.total_pnl, right.fitness.total_pnl))
        .then_with(|| {
            left.genome
                .require_tags
                .len()
                .cmp(&right.genome.require_tags.len())
        })
        .then_with(|| {
            left.genome
                .deny_tags
                .len()
                .cmp(&right.genome.deny_tags.len())
        })
        .then_with(|| left.genome.variant.cmp(&right.genome.variant))
        .then_with(|| left.genome_hash.cmp(&right.genome_hash))
}

fn compare_evolution_replay_hypotheses(
    left: &EvolutionCandidate,
    right: &EvolutionCandidate,
) -> Ordering {
    evolution_actionable_gate_failures(left)
        .cmp(&evolution_actionable_gate_failures(right))
        .then_with(|| {
            left.fitness
                .max_loss_burst_reports
                .cmp(&right.fitness.max_loss_burst_reports)
        })
        .then_with(|| {
            f64_desc(
                left.fitness.worst_report_pnl,
                right.fitness.worst_report_pnl,
            )
        })
        .then_with(|| f64_desc(left.fitness.cvar_pnl, right.fitness.cvar_pnl))
        .then_with(|| {
            f64_asc(
                left.fitness.worst_loss_to_avg_win,
                right.fitness.worst_loss_to_avg_win,
            )
        })
        .then_with(|| f64_desc(left.fitness.payoff_ratio, right.fitness.payoff_ratio))
        .then_with(|| f64_desc(left.fitness.profit_factor, right.fitness.profit_factor))
        .then_with(|| {
            f64_desc(
                left.fitness.wilson_win_rate_lower,
                right.fitness.wilson_win_rate_lower,
            )
        })
        .then_with(|| {
            right
                .fitness
                .eligible_reports
                .cmp(&left.fitness.eligible_reports)
        })
        .then_with(|| right.fitness.trades.cmp(&left.fitness.trades))
        .then_with(|| f64_desc(left.fitness.total_pnl, right.fitness.total_pnl))
        .then_with(|| left.genome_hash.cmp(&right.genome_hash))
}

fn evolution_actionable_gate_failures(candidate: &EvolutionCandidate) -> usize {
    candidate
        .fitness
        .failure_reasons
        .iter()
        .filter(|reason| reason.as_str() != "report_counterfactual_requires_replay")
        .count()
}

fn select_evolution_output_candidates(
    sorted: &[EvolutionCandidate],
    top: usize,
) -> Vec<EvolutionCandidate> {
    let limit = top.max(1).min(sorted.len());
    if limit == 0 {
        return Vec::new();
    }
    let mutation_quota = if limit > 1 { limit / 2 } else { 0 };
    let exact_quota = limit - mutation_quota;
    let mut selected = Vec::with_capacity(limit);
    let mut selected_hashes = BTreeSet::new();

    for candidate in sorted
        .iter()
        .filter(|candidate| candidate.fitness.static_fitness_exact)
        .take(exact_quota)
    {
        selected_hashes.insert(candidate.genome_hash.clone());
        selected.push(candidate.clone());
    }

    let mut replay_hypotheses = sorted
        .iter()
        .filter(|candidate| !candidate.fitness.static_fitness_exact)
        .cloned()
        .collect::<Vec<_>>();
    replay_hypotheses.sort_by(compare_evolution_replay_hypotheses);
    for candidate in replay_hypotheses.into_iter().take(mutation_quota) {
        if selected_hashes.insert(candidate.genome_hash.clone()) {
            selected.push(candidate);
        }
    }

    for candidate in sorted {
        if selected.len() == limit {
            break;
        }
        if selected_hashes.insert(candidate.genome_hash.clone()) {
            selected.push(candidate.clone());
        }
    }
    selected.sort_by(compare_evolution_candidates);
    for (idx, candidate) in selected.iter_mut().enumerate() {
        candidate.rank = idx + 1;
    }
    selected
}

fn unique_evolution_candidates(candidates: Vec<EvolutionCandidate>) -> Vec<EvolutionCandidate> {
    let mut by_hash: BTreeMap<String, EvolutionCandidate> = BTreeMap::new();
    for candidate in candidates {
        by_hash
            .entry(candidate.genome_hash.clone())
            .and_modify(|existing| {
                if compare_evolution_candidates(&candidate, existing) == Ordering::Less {
                    *existing = candidate.clone();
                }
            })
            .or_insert(candidate);
    }
    by_hash.into_values().collect()
}

fn evolution_trial_ledger_row(candidate: &EvolutionCandidate) -> EvolutionTrialLedgerRow {
    EvolutionTrialLedgerRow {
        generation: candidate.generation,
        candidate_id: candidate.candidate_id.clone(),
        genome_hash: candidate.genome_hash.clone(),
        parent_hashes: candidate.parent_hashes.clone(),
        passed: candidate.passed,
        replayable_policy: candidate.fitness.replayable_policy,
        static_fitness_exact: candidate.fitness.static_fitness_exact,
        gate_failures: candidate.fitness.gate_failures,
        pareto_front: candidate.pareto_front,
        failure_reasons: candidate.fitness.failure_reasons.clone(),
        eligible_reports: candidate.fitness.eligible_reports,
        abstained_reports: candidate.fitness.abstained_reports,
        trades: candidate.fitness.trades,
        total_pnl: candidate.fitness.total_pnl,
        worst_report_pnl: candidate.fitness.worst_report_pnl,
        cvar_pnl: candidate.fitness.cvar_pnl,
        max_loss_burst_reports: candidate.fitness.max_loss_burst_reports,
        payoff_ratio: candidate.fitness.payoff_ratio,
        profit_factor: candidate.fitness.profit_factor,
        wilson_win_rate_lower: candidate.fitness.wilson_win_rate_lower,
    }
}

fn evolution_failure_summary(candidates: &[EvolutionCandidate]) -> String {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for candidate in candidates {
        for reason in &candidate.fitness.failure_reasons {
            *counts.entry(reason.clone()).or_default() += 1;
        }
    }
    if counts.is_empty() {
        return "none".to_string();
    }
    let mut counts = counts.into_iter().collect::<Vec<_>>();
    counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    counts
        .into_iter()
        .take(5)
        .map(|(reason, count)| format!("{reason}={count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn write_evolution_artifacts(
    search: &mut StrategyBuilderEvolveSearch,
    report_set: &SelectivityReportSet,
    input: &StrategyBuilderEvolveSearchInput,
) -> Result<()> {
    std::fs::create_dir_all(&input.out_dir)
        .with_context(|| format!("create evolution output dir {}", input.out_dir.display()))?;
    let generations_dir = input.out_dir.join("generations");
    let candidates_dir = input.out_dir.join("candidates");
    std::fs::create_dir_all(&generations_dir)
        .with_context(|| format!("create {}", generations_dir.display()))?;
    std::fs::create_dir_all(&candidates_dir)
        .with_context(|| format!("create {}", candidates_dir.display()))?;

    for generation in &search.generations {
        let path = generations_dir.join(format!("generation_{:03}.json", generation.generation));
        write_json_artifact_atomic(&path, generation)
            .with_context(|| format!("write {}", path.display()))?;
    }

    for candidate in &mut search.candidates {
        let candidate_dir = candidates_dir.join(format!(
            "candidate_rank_{:03}_{}",
            candidate.rank,
            &candidate.genome_hash[..8]
        ));
        std::fs::create_dir_all(&candidate_dir)
            .with_context(|| format!("create {}", candidate_dir.display()))?;

        let genome_path = candidate_dir.join("genome.json");
        write_json_artifact_atomic(&genome_path, &candidate.genome)
            .with_context(|| format!("write {}", genome_path.display()))?;

        if !candidate.fitness.replayable_policy {
            candidate.notes.push(
                "variant.json omitted because the evolved policy conflicts with its source variant"
                    .to_string(),
            );
        } else if let Some(base_variant) = report_set.variants.get(&candidate.genome.variant) {
            let variant =
                executable_evolution_variant(base_variant, candidate).with_context(|| {
                    format!("materialize evolved candidate {}", candidate.candidate_id)
                })?;
            let variant_path = candidate_dir.join("variant.json");
            write_json_artifact_atomic(&variant_path, &variant)
                .with_context(|| format!("write {}", variant_path.display()))?;
            candidate.variant_path = Some(variant_path.display().to_string());
        } else {
            candidate.notes.push(
                "source reports did not include executable StrategyVariant params for variant.json"
                    .to_string(),
            );
        }

        if input.replay_start.is_some() && candidate.variant_path.is_some() {
            let replay_manifest = evolution_replay_manifest(candidate, input, &candidate_dir);
            let replay_path = candidate_dir.join("rolling_history_manifest.json");
            write_json_artifact_atomic(&replay_path, &replay_manifest)
                .with_context(|| format!("write {}", replay_path.display()))?;
            candidate.replay_manifest_path = Some(replay_path.display().to_string());
        } else if input.replay_start.is_some() {
            candidate.notes.push(
                "replay manifest omitted because no executable variant was written".to_string(),
            );
        }
    }

    let ledger_path = input.out_dir.join("trial_ledger.jsonl");
    write_jsonl_artifact_atomic(&ledger_path, &search.trial_ledger)
        .with_context(|| format!("write {}", ledger_path.display()))?;

    let summary_path = input.out_dir.join("evolution_summary.json");
    write_json_artifact_atomic(&summary_path, search)
        .with_context(|| format!("write {}", summary_path.display()))?;
    Ok(())
}

fn executable_evolution_variant(
    base_variant: &StrategyVariant,
    candidate: &EvolutionCandidate,
) -> Result<StrategyVariant> {
    let mut variant = base_variant.clone();
    variant.name = format!(
        "{}_evo_{}",
        safe_path_component(&candidate.genome.variant),
        &candidate.genome_hash[..8]
    );
    variant.selectivity =
        runtime_selectivity_from_causal_policy(&variant.selectivity, &candidate.final_policy)?;
    apply_evolution_knobs_to_variant(&mut variant, &candidate.genome.knobs);
    Ok(variant)
}

fn runtime_selectivity_from_causal_policy(
    source: &SelectivityFilter,
    policy: &CausalPolicyReport,
) -> Result<SelectivityFilter> {
    let mut require_tags = policy.require_tags.clone();
    for (dimension, value) in tags_from_cli_args(&policy.harness_require_args)? {
        if let Some(existing) = require_tags.insert(dimension.clone(), value.clone()) {
            if existing != value {
                bail!("conflicting runtime require tags for {dimension}: {existing} vs {value}");
            }
        }
    }

    let mut deny_tag_values = tag_values_from_cli_args(&policy.harness_deny_args)?;
    for rule in &policy.deny_rules {
        if rule.match_tags.len() != 1 {
            bail!(
                "evolved candidate contains unsupported multi-term runtime deny rule: {}",
                serde_json::to_string(&rule.match_tags).unwrap_or_else(|_| "<invalid>".to_string())
            );
        }
        for (dimension, value) in &rule.match_tags {
            deny_tag_values
                .entry(dimension.clone())
                .or_default()
                .insert(value.clone());
        }
    }

    validate_causal_tag_map(&require_tags)?;
    validate_causal_tag_values(&deny_tag_values)?;
    for (dimension, value) in &require_tags {
        if deny_tag_values
            .get(dimension)
            .is_some_and(|denied| denied.contains(value))
        {
            bail!("runtime selectivity cannot both require and deny {dimension}={value}");
        }
    }

    merge_runtime_selectivity(source, &require_tags, &deny_tag_values)
}

fn apply_evolution_knobs_to_variant(variant: &mut StrategyVariant, knobs: &EvolutionStrategyKnobs) {
    if let Some(value) = knobs.min_confidence {
        variant.min_confidence = value;
        variant.zone_config.early_min_confidence = value;
        variant.zone_config.late_min_confidence = value;
        variant.zone_config.terminal_min_confidence = value;
    }
    if let Some(value) = knobs.min_edge {
        variant.min_edge = value;
        variant.zone_config.early_min_edge = value;
        variant.zone_config.late_min_edge = value;
        variant.zone_config.terminal_min_edge = value;
    }
    if let Some(value) = knobs.early_min_z {
        variant.zone_config.early_min_z = value;
    }
    if let Some(value) = knobs.primary_min_z {
        variant.zone_config.primary_min_z = value;
    }
    if let Some(value) = knobs.late_min_z {
        variant.zone_config.late_min_z = value;
    }
    if let Some(value) = knobs.terminal_min_z {
        variant.zone_config.terminal_min_z = value;
    }
    if let Some(value) = knobs.min_price {
        variant.zone_config.min_price = value;
    }
    if let Some(value) = knobs.max_price {
        variant.zone_config.max_price = value;
    }
    if variant.zone_config.max_price < variant.zone_config.min_price {
        variant.zone_config.max_price = variant.zone_config.min_price;
    }
    if let Some(value) = knobs.min_ev_buffer {
        variant.zone_config.min_ev_buffer = value;
    }
    if let Some(value) = knobs.settlement_guard_minutes {
        variant.zone_config.settlement_guard_minutes = value;
    }
    if let Some(value) = knobs.settlement_min_abs_move_usd {
        variant.zone_config.settlement_min_abs_move_usd = value;
    }
    if let Some(value) = knobs.min_reversion_count {
        variant.zone_config.min_reversion_count = value;
    }
    if let Some(value) = knobs.max_reversion_count {
        variant.zone_config.max_reversion_count = value;
    }
    if variant.zone_config.max_reversion_count != u64::MAX
        && variant.zone_config.max_reversion_count < variant.zone_config.min_reversion_count
    {
        variant.zone_config.max_reversion_count = variant.zone_config.min_reversion_count;
    }
    if let Some(value) = knobs.prefer_maker {
        variant.prefer_maker = value;
    }
    if let Some(value) = knobs.max_spread {
        variant.microstructure.max_spread = value;
    }
    if let Some(value) = knobs.min_book_depth {
        variant.microstructure.min_book_depth = value;
    }
    if let Some(value) = knobs.min_book_pressure {
        variant.microstructure.min_book_pressure = value;
    }
    if let Some(value) = knobs.recent_mid_lookback_seconds {
        variant.microstructure.recent_mid_lookback_seconds = value;
    }
    if let Some(value) = knobs.max_recent_mid_runup {
        variant.microstructure.max_recent_mid_runup = value;
    }
}

fn evolution_replay_manifest(
    candidate: &EvolutionCandidate,
    input: &StrategyBuilderEvolveSearchInput,
    candidate_dir: &Path,
) -> serde_json::Value {
    let mut args = vec![
        "strategy-builder".to_string(),
        "rolling-history".to_string(),
        "--start".to_string(),
        input.replay_start.clone().unwrap_or_default(),
        "--end".to_string(),
        input.replay_end.clone().unwrap_or_default(),
        "--out-dir".to_string(),
        candidate_dir.display().to_string(),
        "--latency-ms".to_string(),
        input.latency_ms.to_string(),
        "--threads".to_string(),
        input.threads.to_string(),
        "--window-minutes".to_string(),
        input.window_minutes.to_string(),
        "--fold-hours".to_string(),
        input.fold_hours.to_string(),
        "--profile".to_string(),
        input.replay_profile.clone(),
        "--zone-mode".to_string(),
        input.replay_zone_mode.clone(),
    ];
    if let Some(path) = &input.latency_audit_json {
        args.push("--latency-audit-json".to_string());
        args.push(path.clone());
    }
    if let Some(path) = &input.btc_csv {
        args.push("--btc-csv".to_string());
        args.push(path.clone());
    }
    if input.atomic_parquet {
        args.push("--atomic-parquet".to_string());
    }
    if let Some(path) = &candidate.variant_path {
        args.push("--variant-json".to_string());
        args.push(path.clone());
    } else {
        for tag in &candidate.final_policy.harness_require_args {
            args.push("--require-causal-tag".to_string());
            args.push(tag.clone());
        }
        for tag in &candidate.final_policy.harness_deny_args {
            args.push("--deny-causal-tag".to_string());
            args.push(tag.clone());
        }
    }

    serde_json::json!({
        "schema_version": 1,
        "mode": "dry_run",
        "execute": false,
        "candidate_id": candidate.candidate_id,
        "genome_hash": candidate.genome_hash,
        "passed_static_evolution": candidate.passed,
        "variant_json": candidate.variant_path.as_deref(),
        "harness_require_args": candidate.final_policy.harness_require_args,
        "harness_deny_args": candidate.final_policy.harness_deny_args,
        "rolling_history_args": args,
        "methodology": [
            "Generated from evolve-search output only; no replay was executed.",
            "When variant_json is present, rolling-history replays that exact serialized StrategyVariant instead of expanding the profile grid.",
            "Run the args with --execute only after selecting the candidate for replay validation.",
            "Static evolution fitness is not promotion evidence."
        ],
    })
}

fn write_json_artifact_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create artifact dir {}", parent.display()))?;
        }
    }
    let mut payload = serde_json::to_vec_pretty(value).context("serialize artifact JSON")?;
    payload.push(b'\n');
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact.json");
    let tmp_path = path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
    std::fs::write(&tmp_path, payload)
        .with_context(|| format!("write artifact temp {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename artifact into {}", path.display()))?;
    Ok(())
}

fn write_jsonl_artifact_atomic<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create artifact dir {}", parent.display()))?;
        }
    }
    let mut payload = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut payload, row).context("serialize artifact JSONL row")?;
        payload.push(b'\n');
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact.jsonl");
    let tmp_path = path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
    std::fs::write(&tmp_path, payload)
        .with_context(|| format!("write artifact temp {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename artifact into {}", path.display()))?;
    Ok(())
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
    let mut eligible_report_pnls = Vec::new();
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
        eligible_report_pnls.push(fold_stats.total_pnl);
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
        tail: tail_risk_report(
            &eligible_report_pnls,
            input.tail_alpha,
            input.loss_burst_lookback,
        ),
        stats: stats_report(&oos),
        decisions,
    };
    let passed = fold_forward.stats.trades >= input.min_oos_trades
        && fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower
        && fold_forward.stats.total_pnl >= input.min_oos_total_pnl
        && fold_forward.profitable_reports >= input.min_oos_profitable_reports
        && fold_forward.worst_report_pnl >= input.min_worst_oos_pnl
        && fold_forward.tail.cvar_pnl >= input.min_oos_cvar_pnl
        && (input.max_loss_burst_reports == 0
            || fold_forward.tail.max_loss_burst_reports <= input.max_loss_burst_reports);

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
            tail_alpha: input.tail_alpha,
            min_oos_cvar_pnl: input.min_oos_cvar_pnl,
            loss_burst_lookback: input.loss_burst_lookback,
            max_loss_burst_reports: input.max_loss_burst_reports,
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
        tail_alpha: input.tail_alpha,
        min_oos_cvar_pnl: input.min_oos_cvar_pnl,
        loss_burst_lookback: input.loss_burst_lookback,
        max_loss_burst_reports: input.max_loss_burst_reports,
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
    evaluate_causal_policy_candidate_with_fixed_denies(folds, input, candidate, &BTreeMap::new())
}

fn evaluate_causal_policy_candidate_with_fixed_denies(
    folds: &[SelectivityFold],
    input: &StrategyBuilderCausalPolicySearchInput,
    candidate: CausalPolicyKey,
    fixed_deny_tags: &BTreeMap<String, String>,
) -> CausalPolicyCandidateReport {
    let mut oos = TradePnlDiagnostics::default();
    let mut eligible_reports = 0_usize;
    let mut profitable_reports = 0_usize;
    let mut losing_reports = 0_usize;
    let mut abstained_reports = 0_usize;
    let mut worst_report_pnl: Option<f64> = None;
    let mut eligible_report_pnls = Vec::new();
    let mut decisions = Vec::new();

    for idx in 0..folds.len() {
        if idx < input.min_train_reports {
            decisions.push(CausalPolicyDecisionReport {
                report_index: idx,
                train_reports: idx,
                policy: causal_policy_report(&CausalPolicy {
                    require_tags: candidate.require_tags.clone(),
                    deny_rules: fixed_causal_policy_deny_rules(
                        &folds[..idx],
                        &candidate.variant,
                        &candidate.require_tags,
                        fixed_deny_tags,
                    ),
                }),
                train: None,
                prior_tail: None,
                prior_recent_loss_reports: 0,
                meta_label: None,
                oos: None,
                reason: "insufficient_prior_reports".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        let prior_folds = &folds[..idx];
        let deny_rules = merged_causal_policy_deny_rules(
            prior_folds,
            input,
            &candidate.variant,
            &candidate.require_tags,
            fixed_deny_tags,
        );
        let policy = CausalPolicy {
            require_tags: candidate.require_tags.clone(),
            deny_rules,
        };
        let train_stats = stats_for_causal_policy(prior_folds, &candidate.variant, &policy);
        let train_reports_with_trades =
            reports_with_causal_policy_trades(prior_folds, &candidate.variant, &policy);
        let prior_policy_pnls =
            report_pnls_for_causal_policy(prior_folds, &candidate.variant, &policy);
        let prior_tail = tail_risk_report(
            &prior_policy_pnls,
            input.tail_alpha,
            input.loss_burst_lookback,
        );
        let prior_loss_cluster_lookback = effective_prior_loss_cluster_lookback(input);
        let prior_recent_loss_reports =
            recent_loss_reports(&prior_policy_pnls, prior_loss_cluster_lookback);

        if train_reports_with_trades < input.min_train_reports
            || train_stats.trades < input.min_train_trades
            || train_stats.total_pnl <= 0.0
        {
            decisions.push(CausalPolicyDecisionReport {
                report_index: idx,
                train_reports: idx,
                policy: causal_policy_report(&policy),
                train: Some(stats_report(&train_stats)),
                prior_tail: Some(prior_tail),
                prior_recent_loss_reports,
                meta_label: None,
                oos: None,
                reason: "policy_prior_stats_failed_train_gates".to_string(),
            });
            abstained_reports += 1;
            continue;
        }
        if input.max_prior_loss_burst_reports > 0
            && prior_loss_cluster_lookback > 0
            && prior_recent_loss_reports >= input.max_prior_loss_burst_reports
        {
            decisions.push(CausalPolicyDecisionReport {
                report_index: idx,
                train_reports: idx,
                policy: causal_policy_report(&policy),
                train: Some(stats_report(&train_stats)),
                prior_tail: Some(prior_tail),
                prior_recent_loss_reports,
                meta_label: None,
                oos: None,
                reason: "prior_loss_cluster_sentinel_flat".to_string(),
            });
            abstained_reports += 1;
            continue;
        }
        if input.min_prior_payoff_ratio > 0.0
            && train_stats.payoff_ratio < input.min_prior_payoff_ratio
        {
            decisions.push(CausalPolicyDecisionReport {
                report_index: idx,
                train_reports: idx,
                policy: causal_policy_report(&policy),
                train: Some(stats_report(&train_stats)),
                prior_tail: Some(prior_tail),
                prior_recent_loss_reports,
                meta_label: None,
                oos: None,
                reason: "prior_payoff_ratio_below_budget".to_string(),
            });
            abstained_reports += 1;
            continue;
        }
        if input.max_prior_worst_loss_to_avg_win > 0.0
            && train_stats.worst_loss_to_avg_win > input.max_prior_worst_loss_to_avg_win
        {
            decisions.push(CausalPolicyDecisionReport {
                report_index: idx,
                train_reports: idx,
                policy: causal_policy_report(&policy),
                train: Some(stats_report(&train_stats)),
                prior_tail: Some(prior_tail),
                prior_recent_loss_reports,
                meta_label: None,
                oos: None,
                reason: "prior_worst_loss_to_avg_win_above_budget".to_string(),
            });
            abstained_reports += 1;
            continue;
        }

        let meta_label =
            meta_label_risk_report(prior_folds, &folds[idx], &candidate.variant, &policy, input);
        if meta_label.as_ref().is_some_and(|report| report.flattened) {
            let reason = meta_label
                .as_ref()
                .map(|report| format!("meta_label_{}", report.reason))
                .unwrap_or_else(|| "meta_label_risk_gate_flat".to_string());
            decisions.push(CausalPolicyDecisionReport {
                report_index: idx,
                train_reports: idx,
                policy: causal_policy_report(&policy),
                train: Some(stats_report(&train_stats)),
                prior_tail: Some(prior_tail),
                prior_recent_loss_reports,
                meta_label,
                oos: None,
                reason,
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
                prior_tail: Some(prior_tail),
                prior_recent_loss_reports,
                meta_label,
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
        eligible_report_pnls.push(fold_stats.total_pnl);
        oos.merge_from(&fold_stats);
        decisions.push(CausalPolicyDecisionReport {
            report_index: idx,
            train_reports: idx,
            policy: causal_policy_report(&policy),
            train: Some(stats_report(&train_stats)),
            prior_tail: Some(prior_tail),
            prior_recent_loss_reports,
            meta_label,
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
        tail: tail_risk_report(
            &eligible_report_pnls,
            input.tail_alpha,
            input.loss_burst_lookback,
        ),
        stats: stats_report(&oos),
        decisions,
    };
    let passed = fold_forward.stats.trades >= input.min_oos_trades
        && fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower
        && fold_forward.stats.total_pnl >= input.min_oos_total_pnl
        && fold_forward.profitable_reports >= input.min_oos_profitable_reports
        && (input.min_oos_eligible_reports == 0
            || fold_forward.eligible_reports >= input.min_oos_eligible_reports)
        && fold_forward.worst_report_pnl >= input.min_worst_oos_pnl
        && fold_forward.tail.cvar_pnl >= input.min_oos_cvar_pnl
        && (input.max_loss_burst_reports == 0
            || fold_forward.tail.max_loss_burst_reports <= input.max_loss_burst_reports)
        && (input.min_oos_payoff_ratio <= 0.0
            || fold_forward.stats.payoff_ratio >= input.min_oos_payoff_ratio)
        && (input.max_oos_worst_loss_to_avg_win <= 0.0
            || fold_forward.stats.worst_loss_to_avg_win <= input.max_oos_worst_loss_to_avg_win);

    let final_policy = CausalPolicy {
        require_tags: candidate.require_tags.clone(),
        deny_rules: merged_causal_policy_deny_rules(
            folds,
            input,
            &candidate.variant,
            &candidate.require_tags,
            fixed_deny_tags,
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
    if !fixed_deny_tags.is_empty() {
        notes.push("fixed deny tags were supplied by an evolution genome".to_string());
    }
    if !passed {
        notes.push("candidate did not pass configured OOS gates".to_string());
    }
    if input.min_oos_eligible_reports > 0
        && fold_forward.eligible_reports < input.min_oos_eligible_reports
    {
        notes.push("candidate did not meet minimum eligible OOS report coverage".to_string());
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

fn merged_causal_policy_deny_rules(
    prior_folds: &[SelectivityFold],
    input: &StrategyBuilderCausalPolicySearchInput,
    variant: &str,
    require_tags: &BTreeMap<String, String>,
    fixed_deny_tags: &BTreeMap<String, String>,
) -> Vec<CausalPolicyRule> {
    let mut rules =
        fixed_causal_policy_deny_rules(prior_folds, variant, require_tags, fixed_deny_tags);
    let mut seen = rules
        .iter()
        .map(|rule| rule.label.clone())
        .collect::<BTreeSet<_>>();
    for rule in learn_causal_policy_deny_rules(prior_folds, input, variant, require_tags) {
        if seen.insert(rule.label.clone()) {
            rules.push(rule);
        }
    }
    rules
}

fn fixed_causal_policy_deny_rules(
    folds: &[SelectivityFold],
    variant: &str,
    require_tags: &BTreeMap<String, String>,
    fixed_deny_tags: &BTreeMap<String, String>,
) -> Vec<CausalPolicyRule> {
    fixed_deny_tags
        .iter()
        .filter_map(|(dimension, value)| {
            if !CAUSAL_POLICY_DIMENSIONS.contains(&dimension.as_str()) {
                return None;
            }
            if require_tags
                .get(dimension)
                .is_some_and(|required| required == value)
            {
                return None;
            }
            let mut match_tags = BTreeMap::new();
            match_tags.insert(dimension.clone(), value.clone());
            let mut stats = TradePnlDiagnostics::default();
            let mut reports_with_trades = 0_usize;
            for fold in folds {
                let fold_stats =
                    stats_for_causal_policy_rule_match(fold, variant, require_tags, &match_tags);
                if fold_stats.trades > 0 {
                    reports_with_trades += 1;
                }
                stats.merge_from(&fold_stats);
            }
            Some(CausalPolicyRule {
                label: policy_label(&match_tags),
                match_tags,
                reports_with_trades,
                stats,
            })
        })
        .collect()
}

fn stats_for_causal_policy_rule_match(
    fold: &SelectivityFold,
    variant: &str,
    require_tags: &BTreeMap<String, String>,
    match_tags: &BTreeMap<String, String>,
) -> TradePnlDiagnostics {
    let mut stats = TradePnlDiagnostics::default();
    let Some(variant_fold) = fold.variants.iter().find(|entry| entry.name == variant) else {
        return stats;
    };
    for regime in &variant_fold.tagged_regimes {
        if policy_tags_match(require_tags, &regime.tags)
            && policy_tags_match(match_tags, &regime.tags)
        {
            stats.merge_from(&regime.stats);
        }
    }
    stats
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

fn report_pnls_for_causal_policy(
    folds: &[SelectivityFold],
    variant: &str,
    policy: &CausalPolicy,
) -> Vec<f64> {
    folds
        .iter()
        .filter_map(|fold| {
            let stats = stats_for_causal_policy(std::slice::from_ref(fold), variant, policy);
            (stats.trades > 0).then_some(stats.total_pnl)
        })
        .collect()
}

fn meta_label_risk_report(
    prior_folds: &[SelectivityFold],
    current_fold: &SelectivityFold,
    variant: &str,
    policy: &CausalPolicy,
    input: &StrategyBuilderCausalPolicySearchInput,
) -> Option<MetaLabelRiskReport> {
    if input.meta_label_min_support == 0 {
        return None;
    }

    let active_labels = active_policy_regime_labels(current_fold, variant, policy);
    let active_buckets = active_labels.len();
    let mut buckets = Vec::new();
    let mut prior_pattern_pnls: Option<BTreeMap<String, Vec<f64>>> = None;
    let mut flattened = false;
    let mut reason = "ok".to_string();

    for label in active_labels {
        let tags = causal_tags_from_regime(&label);
        let exact_prior_pnls = prior_regime_pnls_for_policy(prior_folds, variant, policy, &label);
        let exact_report = meta_label_bucket_report(
            "exact",
            label.clone(),
            tags.clone(),
            exact_prior_pnls,
            input,
        );
        let mut has_supported_evidence = exact_report.supported;
        buckets.push(exact_report);

        if !has_supported_evidence && input.meta_label_max_generalization_terms > 0 {
            if prior_pattern_pnls.is_none() {
                prior_pattern_pnls = Some(prior_pnls_by_policy_tag_pattern(
                    prior_folds,
                    variant,
                    policy,
                    input.meta_label_max_generalization_terms,
                ));
            }
            let pattern_pnls = prior_pattern_pnls.as_ref().expect("pattern pnl cache");
            let mut seen_patterns = BTreeSet::new();
            for match_tags in
                policy_tag_combinations(&tags, input.meta_label_max_generalization_terms)
            {
                let label = policy_label(&match_tags);
                if !seen_patterns.insert(label.clone()) {
                    continue;
                }
                let prior_pnls = pattern_pnls.get(&label).cloned().unwrap_or_default();
                let report =
                    meta_label_bucket_report("generalized", label, match_tags, prior_pnls, input);
                if report.supported {
                    has_supported_evidence = true;
                    buckets.push(report);
                }
            }
        }

        if input.meta_label_require_supported && !has_supported_evidence {
            flattened = true;
            reason = "unsupported_bucket_flat".to_string();
        }
    }

    let mut supported_buckets = 0_usize;
    let mut worst_quantile_pnl: Option<f64> = None;
    let mut worst_prior_pnl: Option<f64> = None;
    let mut max_loss_rate_seen = 0.0_f64;
    for bucket in &buckets {
        if !bucket.supported {
            continue;
        }
        supported_buckets += 1;
        worst_quantile_pnl = Some(match worst_quantile_pnl {
            Some(current) => current.min(bucket.quantile_pnl),
            None => bucket.quantile_pnl,
        });
        worst_prior_pnl = Some(match worst_prior_pnl {
            Some(current) => current.min(bucket.worst_pnl),
            None => bucket.worst_pnl,
        });
        max_loss_rate_seen = max_loss_rate_seen.max(bucket.loss_rate);
        if bucket.quantile_pnl < input.meta_label_min_quantile_pnl {
            flattened = true;
            reason = "quantile_below_budget_flat".to_string();
        }
        if bucket.loss_rate > input.meta_label_max_loss_rate {
            flattened = true;
            reason = "loss_rate_above_budget_flat".to_string();
        }
    }

    if buckets.is_empty() {
        reason = "no_active_policy_regimes".to_string();
    }

    Some(MetaLabelRiskReport {
        active_buckets,
        supported_buckets,
        unsupported_buckets: buckets.len().saturating_sub(supported_buckets),
        min_support: input.meta_label_min_support,
        alpha: input.meta_label_alpha,
        min_quantile_pnl: input.meta_label_min_quantile_pnl,
        max_loss_rate: input.meta_label_max_loss_rate,
        require_supported: input.meta_label_require_supported,
        max_generalization_terms: input.meta_label_max_generalization_terms,
        worst_quantile_pnl: worst_quantile_pnl.unwrap_or(0.0),
        worst_prior_pnl: worst_prior_pnl.unwrap_or(0.0),
        max_loss_rate_seen,
        flattened,
        reason,
        buckets,
    })
}

fn meta_label_bucket_report(
    kind: &str,
    label: String,
    match_tags: BTreeMap<String, String>,
    prior_pnls: Vec<f64>,
    input: &StrategyBuilderCausalPolicySearchInput,
) -> MetaLabelBucketReport {
    let support = prior_pnls.len();
    let supported = support >= input.meta_label_min_support;
    let (loss_rate, quantile_pnl, worst_pnl) = if prior_pnls.is_empty() {
        (0.0, 0.0, 0.0)
    } else {
        let loss_rate = prior_pnls.iter().filter(|pnl| **pnl < 0.0).count() as f64 / support as f64;
        let quantile_pnl = left_tail_quantile(&prior_pnls, input.meta_label_alpha);
        let worst_pnl = prior_pnls
            .iter()
            .copied()
            .fold(f64::INFINITY, |current, pnl| current.min(pnl));
        (loss_rate, quantile_pnl, worst_pnl)
    };

    MetaLabelBucketReport {
        kind: kind.to_string(),
        label,
        match_tags,
        support,
        supported,
        loss_rate,
        quantile_pnl,
        worst_pnl,
    }
}

fn active_policy_regime_labels(
    fold: &SelectivityFold,
    variant: &str,
    policy: &CausalPolicy,
) -> Vec<String> {
    let Some(variant_fold) = fold.variants.iter().find(|entry| entry.name == variant) else {
        return Vec::new();
    };
    variant_fold
        .regimes
        .iter()
        .filter_map(|(label, stats)| {
            if stats.trades == 0 {
                return None;
            }
            let tags = causal_tags_from_regime(label);
            policy_applies_to_tags(policy, &tags).then(|| label.clone())
        })
        .collect()
}

fn prior_pnls_by_policy_tag_pattern(
    folds: &[SelectivityFold],
    variant: &str,
    policy: &CausalPolicy,
    max_terms: usize,
) -> BTreeMap<String, Vec<f64>> {
    let mut pattern_pnls = BTreeMap::new();
    for fold in folds {
        let Some(variant_fold) = fold.variants.iter().find(|entry| entry.name == variant) else {
            continue;
        };
        let mut fold_patterns: BTreeMap<String, TradePnlDiagnostics> = BTreeMap::new();
        for (label, regime_stats) in &variant_fold.regimes {
            if regime_stats.trades == 0 {
                continue;
            }
            let tags = causal_tags_from_regime(label);
            if !policy_applies_to_tags(policy, &tags) {
                continue;
            }
            for match_tags in policy_tag_combinations(&tags, max_terms) {
                let pattern = policy_label(&match_tags);
                fold_patterns
                    .entry(pattern)
                    .or_default()
                    .merge_from(regime_stats);
            }
        }
        for (pattern, stats) in fold_patterns {
            if stats.trades > 0 {
                pattern_pnls
                    .entry(pattern)
                    .or_insert_with(Vec::new)
                    .push(stats.total_pnl);
            }
        }
    }
    pattern_pnls
}

fn prior_regime_pnls_for_policy(
    folds: &[SelectivityFold],
    variant: &str,
    policy: &CausalPolicy,
    label: &str,
) -> Vec<f64> {
    folds
        .iter()
        .filter_map(|fold| {
            let variant_fold = fold.variants.iter().find(|entry| entry.name == variant)?;
            let stats = variant_fold.regimes.get(label)?;
            if stats.trades == 0 {
                return None;
            }
            let tags = causal_tags_from_regime(label);
            policy_applies_to_tags(policy, &tags).then_some(stats.total_pnl)
        })
        .collect()
}

fn policy_applies_to_tags(policy: &CausalPolicy, tags: &BTreeMap<String, String>) -> bool {
    policy_tags_match(&policy.require_tags, tags)
        && !policy
            .deny_rules
            .iter()
            .any(|rule| policy_tags_match(&rule.match_tags, tags))
}

fn left_tail_quantile(values: &[f64], alpha: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let idx = ((sorted.len() as f64) * alpha).ceil().max(1.0) as usize - 1;
    sorted[idx.min(sorted.len() - 1)]
}

fn effective_prior_loss_cluster_lookback(input: &StrategyBuilderCausalPolicySearchInput) -> usize {
    if input.prior_loss_cluster_lookback > 0 {
        input.prior_loss_cluster_lookback
    } else {
        input.loss_burst_lookback
    }
}

fn tail_risk_report(fold_pnls: &[f64], alpha: f64, loss_burst_lookback: usize) -> TailRiskReport {
    if fold_pnls.is_empty() {
        return TailRiskReport {
            alpha,
            sample_count: 0,
            tail_count: 0,
            cvar_pnl: 0.0,
            worst_pnl: 0.0,
            losing_reports: 0,
            loss_burst_lookback,
            max_loss_burst_reports: 0,
        };
    }

    let mut sorted = fold_pnls.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let tail_count = ((sorted.len() as f64) * alpha).ceil().max(1.0) as usize;
    let tail_count = tail_count.min(sorted.len());
    let cvar_pnl = sorted.iter().take(tail_count).sum::<f64>() / tail_count as f64;
    let losing_reports = fold_pnls.iter().filter(|pnl| **pnl < 0.0).count();
    let max_loss_burst_reports = max_loss_burst_reports(fold_pnls, loss_burst_lookback);

    TailRiskReport {
        alpha,
        sample_count: fold_pnls.len(),
        tail_count,
        cvar_pnl,
        worst_pnl: sorted[0],
        losing_reports,
        loss_burst_lookback,
        max_loss_burst_reports,
    }
}

fn max_loss_burst_reports(fold_pnls: &[f64], lookback: usize) -> usize {
    if lookback == 0 || fold_pnls.is_empty() {
        return 0;
    }
    let lookback = lookback.min(fold_pnls.len());
    fold_pnls
        .windows(lookback)
        .map(|window| window.iter().filter(|pnl| **pnl < 0.0).count())
        .max()
        .unwrap_or(0)
}

fn recent_loss_reports(fold_pnls: &[f64], lookback: usize) -> usize {
    if lookback == 0 || fold_pnls.is_empty() {
        return 0;
    }
    let start = fold_pnls.len().saturating_sub(lookback);
    fold_pnls[start..].iter().filter(|pnl| **pnl < 0.0).count()
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
    let mut eligible_report_pnls = Vec::new();
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
        eligible_report_pnls.push(fold_stats.total_pnl);
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
        tail: tail_risk_report(
            &eligible_report_pnls,
            input.tail_alpha,
            input.loss_burst_lookback,
        ),
        stats: stats_report(&oos),
        decisions,
    };
    let passed = fold_forward.stats.trades >= input.min_oos_trades
        && fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower
        && fold_forward.stats.total_pnl >= input.min_oos_total_pnl
        && fold_forward.profitable_reports >= input.min_oos_profitable_reports
        && fold_forward.worst_report_pnl >= input.min_worst_oos_pnl
        && fold_forward.tail.cvar_pnl >= input.min_oos_cvar_pnl
        && (input.max_loss_burst_reports == 0
            || fold_forward.tail.max_loss_burst_reports <= input.max_loss_burst_reports);

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
                "zone"
                | "price"
                | "edge"
                | "z"
                | "book_spread"
                | "book_min_depth"
                | "book_pressure"
                | "book_imbalance"
                | "bookwalk_slippage"
                | "book_age"
                | "book_runup"
                | "btc_impulse_10s"
                | "outcome_overround"
                | "utc_session"
                | "utc_hour"
                | "direction_utc_session"
                | "direction_utc_hour" => dimension.as_str(),
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

#[derive(Serialize)]
struct StrategyRegistryFingerprint<'a> {
    strategy_id: &'a str,
    parent_id: &'a Option<String>,
    artifact_path: &'a Option<String>,
    metrics_path: &'a Option<String>,
}

fn strategy_version_id(input: &StrategyRegistryMarkInput) -> String {
    let fingerprint = StrategyRegistryFingerprint {
        strategy_id: input.strategy_id.trim(),
        parent_id: &input.parent_id,
        artifact_path: &input.artifact_path,
        metrics_path: &input.metrics_path,
    };
    let hash = stable_json_hash(&fingerprint);
    format!("sv_{}", &hash[..16])
}

fn read_strategy_registry(path: &Path) -> Result<StrategyRegistry> {
    if !path.exists() {
        return Ok(StrategyRegistry {
            schema_version: 1,
            updated_at: Utc::now().to_rfc3339(),
            entries: Vec::new(),
        });
    }
    let data = std::fs::read_to_string(path)
        .with_context(|| format!("read strategy registry {}", path.display()))?;
    let registry: StrategyRegistry = serde_json::from_str(&data)
        .with_context(|| format!("parse strategy registry {}", path.display()))?;
    if registry.schema_version != 1 {
        bail!(
            "unsupported strategy registry schema_version {}; expected 1",
            registry.schema_version
        );
    }
    Ok(registry)
}

fn write_strategy_registry_atomic(path: &Path, registry: &StrategyRegistry) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create strategy registry dir {}", parent.display()))?;
        }
    }
    let mut payload = serde_json::to_vec_pretty(registry).context("serialize strategy registry")?;
    payload.push(b'\n');
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("strategy_registry.json");
    let tmp_path = path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
    std::fs::write(&tmp_path, payload)
        .with_context(|| format!("write strategy registry temp {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename strategy registry into {}", path.display()))?;
    Ok(())
}

fn archived_evidence_path(strategy_dir: &Path, role: &str, source_path: &str) -> PathBuf {
    let source = Path::new(source_path);
    let extension = source
        .extension()
        .and_then(|extension| extension.to_str())
        .filter(|extension| !extension.is_empty())
        .map(|extension| format!(".{}", safe_path_component(extension)))
        .unwrap_or_default();
    let source_hash = stable_json_hash(&source_path);
    strategy_dir.join(format!(
        "{}_{}{}",
        safe_path_component(role),
        &source_hash[..16],
        extension
    ))
}

fn archive_evidence_file(
    source: &Path,
    out_dir: &Path,
    strategy_dir: &Path,
    role: &str,
    source_path: &str,
) -> Result<(PathBuf, u64, String)> {
    std::fs::create_dir_all(out_dir)
        .with_context(|| format!("create evidence archive dir {}", out_dir.display()))?;
    let source_canonical = source
        .canonicalize()
        .with_context(|| format!("canonicalize evidence source {}", source.display()))?;
    let out_canonical = out_dir
        .canonicalize()
        .with_context(|| format!("canonicalize evidence archive dir {}", out_dir.display()))?;
    if source_canonical.starts_with(&out_canonical) {
        let (bytes, sha256) = file_sha256(source)?;
        return Ok((source.to_path_buf(), bytes, sha256));
    }

    let archived_path = archived_evidence_path(strategy_dir, role, source_path);
    let (bytes, sha256) = copy_file_atomic_with_sha256(source, &archived_path)?;
    Ok((archived_path, bytes, sha256))
}

fn copy_file_atomic_with_sha256(source: &Path, dest: &Path) -> Result<(u64, String)> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create evidence archive dir {}", parent.display()))?;
    }
    let payload =
        std::fs::read(source).with_context(|| format!("read evidence {}", source.display()))?;
    let sha256 = sha256_bytes(&payload);
    let file_name = dest
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("evidence");
    let tmp_path = dest.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
    let mut file = std::fs::File::create(&tmp_path)
        .with_context(|| format!("create evidence temp {}", tmp_path.display()))?;
    file.write_all(&payload)
        .with_context(|| format!("write evidence temp {}", tmp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync evidence temp {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, dest)
        .with_context(|| format!("rename evidence archive into {}", dest.display()))?;
    Ok((payload.len() as u64, sha256))
}

fn file_sha256(path: &Path) -> Result<(u64, String)> {
    let payload =
        std::fs::read(path).with_context(|| format!("read evidence {}", path.display()))?;
    Ok((payload.len() as u64, sha256_bytes(&payload)))
}

fn sha256_bytes(payload: &[u8]) -> String {
    hex::encode(Sha256::digest(payload))
}

fn safe_path_component(value: &str) -> String {
    let cleaned = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if cleaned.is_empty() {
        "unnamed".to_string()
    } else {
        cleaned
    }
}

fn merge_unique_strings(target: &mut Vec<String>, source: &[String]) {
    for value in source {
        if !target.iter().any(|existing| existing == value) {
            target.push(value.clone());
        }
    }
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
    "utc_session",
    "utc_hour",
    "direction_utc_session",
    "direction_utc_hour",
    "book_spread",
    "book_min_depth",
    "book_pressure",
    "book_imbalance",
    "bookwalk_slippage",
    "book_age",
    "book_runup",
    "btc_impulse_10s",
    "outcome_overround",
];

const PATTERN_GUARD_DIMENSIONS: &[&[&str]] = &[
    &[
        "zone",
        "dir",
        "price",
        "edge",
        "z",
        "conf",
        "rev",
        "min",
        "direction_utc_session",
    ],
    &[
        "zone",
        "dir",
        "price",
        "edge",
        "z",
        "conf",
        "rev",
        "min",
        "utc_session",
    ],
    &["zone", "dir", "price", "edge", "z", "conf", "rev", "min"],
    &[
        "zone",
        "dir",
        "price",
        "edge",
        "z",
        "conf",
        "direction_utc_session",
    ],
    &["zone", "dir", "price", "edge", "z", "conf", "utc_session"],
    &["zone", "dir", "price", "edge", "z", "conf", "min"],
    &["zone", "dir", "price", "z", "conf", "min"],
    &["zone", "price", "edge", "z", "conf", "vol", "utc_session"],
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
    let mut eligible_report_pnls = Vec::new();
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
        eligible_report_pnls.push(fold_stats.total_pnl);
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
        tail: tail_risk_report(
            &eligible_report_pnls,
            input.tail_alpha,
            input.loss_burst_lookback,
        ),
        stats: stats_report(&oos),
        decisions,
    };
    let passed = fold_forward.stats.trades >= input.min_oos_trades
        && fold_forward.stats.wilson_win_rate_lower >= input.min_oos_wilson_win_rate_lower
        && fold_forward.stats.total_pnl >= input.min_oos_total_pnl
        && fold_forward.profitable_reports >= input.min_oos_profitable_reports
        && fold_forward.worst_report_pnl >= input.min_worst_oos_pnl
        && fold_forward.tail.cvar_pnl >= input.min_oos_cvar_pnl
        && (input.max_loss_burst_reports == 0
            || fold_forward.tail.max_loss_burst_reports <= input.max_loss_burst_reports);

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

fn f64_asc(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
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
                position_pct: "0.025",
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
            "a_plus5m_tail_guard" => Ok(Self {
                name: "a_plus5m_tail_guard",
                conf: "0.40",
                z: "0.90,1.10",
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
                position_pct: "0.025",
                max_per_market_usd: "10",
                max_total_exposure_usd: "8",
                max_projected_stressed_drawdown_pct: "0.12",
                degraded_after_losses: "1",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "1.10",
                degraded_max_price: "0.75",
                degraded_force_taker: true,
                also_maker: false,
            }),
            "a_plus5m_tail_primary" => Ok(Self {
                name: "a_plus5m_tail_primary",
                conf: "0.40,0.50",
                z: "0.70,0.90",
                edge: "0.07",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.85,0.90",
                min_reversion_count: "1",
                max_reversion_count: "2",
                settlement_floor: "10.0",
                settlement_guard_minutes: "2.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "10",
                max_total_exposure_usd: "8",
                max_projected_stressed_drawdown_pct: "0.12",
                degraded_after_losses: "1",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "1.10",
                degraded_max_price: "0.75",
                degraded_force_taker: true,
                also_maker: false,
            }),
            "a_plus5m_tail_early_reentry" => Ok(Self {
                name: "a_plus5m_tail_early_reentry",
                conf: "0.60,0.70",
                z: "1.10,1.30",
                edge: "0.10,0.15",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.75",
                min_reversion_count: "1",
                max_reversion_count: "2",
                settlement_floor: "10.0",
                settlement_guard_minutes: "2.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "5",
                max_total_exposure_usd: "5",
                max_projected_stressed_drawdown_pct: "0.08",
                degraded_after_losses: "1",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "1.30",
                degraded_max_price: "0.65",
                degraded_force_taker: true,
                also_maker: false,
            }),
            "a_plus5m_tail_low_exposure" => Ok(Self {
                name: "a_plus5m_tail_low_exposure",
                conf: "0.50",
                z: "0.70,0.90",
                edge: "0.07",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.85,0.90",
                min_reversion_count: "1",
                max_reversion_count: "2",
                settlement_floor: "10.0",
                settlement_guard_minutes: "2.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "5",
                max_total_exposure_usd: "5",
                max_projected_stressed_drawdown_pct: "0.08",
                degraded_after_losses: "1",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "1.10",
                degraded_max_price: "0.75",
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
                "unknown strategy-builder profile `{name}`; supported profiles: guarded5m, a_plus5m, a_plus5m_regime, a_plus5m_adaptive, a_plus5m_adaptive_price, a_plus5m_ev_guard, a_plus5m_causal_guard_selected, a_plus5m_tail_guard, a_plus5m_tail_primary, a_plus5m_tail_early_reentry, a_plus5m_tail_low_exposure, a_plus5m_reversion_guard, swift5m"
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
    fn causal_regime_parser_keeps_evolution_quality_dimensions() {
        let tags = causal_tags_from_regime(
            "zone=primary|dir=up|book_runup=lte_0.02|btc_impulse_10s=8_12|outcome_overround=lte_0.02",
        );

        assert_eq!(tags.get("book_runup").map(String::as_str), Some("lte_0.02"));
        assert_eq!(
            tags.get("btc_impulse_10s").map(String::as_str),
            Some("8_12")
        );
        assert_eq!(
            tags.get("outcome_overround").map(String::as_str),
            Some("lte_0.02")
        );
    }

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
    fn multi_guard_search_reports_tail_cvar_and_loss_burst() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(8, 0, 8.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 3, 0.0, -3.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 2, 0.0, -2.0))]),
        ];

        let mut input = multi_guard_input(5);
        input.min_oos_trades = 8;
        input.min_oos_total_pnl = 0.0;
        input.min_worst_oos_pnl = -10.0;
        input.min_oos_wilson_win_rate_lower = 0.0;
        input.min_oos_profitable_reports = 1;
        input.tail_alpha = 0.50;
        input.min_oos_cvar_pnl = -2.0;
        input.loss_burst_lookback = 2;
        input.max_loss_burst_reports = 1;

        let search = multi_guard_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("multi-guard candidate");

        assert!(!search.ok);
        assert!(!candidate.passed);
        assert_eq!(candidate.fold_forward.stats.total_pnl, 3.0);
        assert_eq!(candidate.fold_forward.tail.sample_count, 3);
        assert_eq!(candidate.fold_forward.tail.tail_count, 2);
        assert_eq!(candidate.fold_forward.tail.max_loss_burst_reports, 2);
        assert!(candidate.fold_forward.tail.cvar_pnl < -2.0);
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
    fn causal_policy_min_eligible_reports_blocks_thin_oos_credit() {
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
        input.min_oos_eligible_reports = 2;
        let search = causal_policy_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.base_require.get("direction").map(String::as_str) == Some("down")
                    && candidate.base_require.get("zone").map(String::as_str) == Some("primary")
            })
            .expect("primary/down policy");

        assert_eq!(search.gates.min_oos_eligible_reports, 2);
        assert!(!search.ok);
        assert!(!candidate.passed);
        assert_eq!(candidate.fold_forward.eligible_reports, 1);
        assert_eq!(candidate.fold_forward.stats.trades, 5);
        assert_eq!(candidate.fold_forward.stats.total_pnl, 5.0);
        assert!(candidate
            .notes
            .iter()
            .any(|note| note.contains("minimum eligible OOS report coverage")));
    }

    #[test]
    fn causal_policy_search_can_select_orderbook_bucket() {
        let tight_book =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4|book_spread=lte_0.01|book_min_depth=100_250|book_pressure=positive|book_imbalance=neutral|bookwalk_slippage=zero|book_age=lte_100ms";
        let wide_book =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4|book_spread=gt_0.05|book_min_depth=10_50|book_pressure=negative|book_imbalance=negative|bookwalk_slippage=gt_0.03|book_age=5_30s";
        let folds = vec![
            selectivity_fold(vec![
                (tight_book, pnl_stats(4, 0, 4.0, 0.0)),
                (wide_book, pnl_stats(0, 2, 0.0, -6.0)),
            ]),
            selectivity_fold(vec![
                (tight_book, pnl_stats(4, 0, 4.0, 0.0)),
                (wide_book, pnl_stats(0, 2, 0.0, -6.0)),
            ]),
            selectivity_fold(vec![
                (tight_book, pnl_stats(5, 0, 5.0, 0.0)),
                (wide_book, pnl_stats(0, 2, 0.0, -8.0)),
            ]),
        ];

        let mut input = causal_policy_input(50);
        input.max_require_terms = 1;
        input.max_deny_rules = 0;
        let search = causal_policy_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate
                    .base_require
                    .get("bookwalk_slippage")
                    .map(String::as_str)
                    == Some("zero")
            })
            .expect("zero bookwalk-slippage policy");

        assert!(search.ok);
        assert!(candidate.passed);
        assert_eq!(candidate.fold_forward.stats.trades, 5);
        assert_eq!(candidate.fold_forward.stats.total_pnl, 5.0);
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
    fn causal_policy_search_reports_tail_cvar_and_loss_burst() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 4, 0.0, -8.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 4, 0.0, -6.0))]),
        ];

        let mut input = causal_policy_input(200);
        input.max_require_terms = 2;
        input.max_deny_rules = 0;
        input.min_oos_trades = 8;
        input.min_oos_total_pnl = -20.0;
        input.min_worst_oos_pnl = -10.0;
        input.min_oos_wilson_win_rate_lower = 0.0;
        input.min_oos_profitable_reports = 1;
        input.tail_alpha = 0.50;
        input.min_oos_cvar_pnl = -5.0;
        input.loss_burst_lookback = 2;
        input.max_loss_burst_reports = 1;
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
        assert_eq!(candidate.fold_forward.tail.sample_count, 4);
        assert_eq!(candidate.fold_forward.tail.tail_count, 2);
        assert_eq!(candidate.fold_forward.tail.max_loss_burst_reports, 2);
        assert!(candidate.fold_forward.tail.cvar_pnl < -5.0);
    }

    #[test]
    fn causal_policy_tail_first_ranking_prefers_cleaner_tail_over_higher_pnl() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let primary_up =
            "regime=zone=primary|dir=up|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![
                (primary_down, pnl_stats(4, 0, 4.0, 0.0)),
                (primary_up, pnl_stats(4, 0, 4.0, 0.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(4, 0, 4.0, 0.0)),
                (primary_up, pnl_stats(4, 0, 4.0, 0.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(10, 0, 10.0, 0.0)),
                (primary_up, pnl_stats(3, 0, 3.0, 0.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(0, 1, 0.0, -4.0)),
                (primary_up, pnl_stats(3, 0, 3.0, 0.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(0, 1, 0.0, -4.0)),
                (primary_up, pnl_stats(0, 1, 0.0, -1.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(20, 0, 20.0, 0.0)),
                (primary_up, pnl_stats(3, 0, 3.0, 0.0)),
            ]),
        ];

        let mut input = causal_policy_input(10);
        input.max_require_terms = 1;
        input.max_deny_rules = 0;
        input.min_oos_wilson_win_rate_lower = 0.0;
        input.min_worst_oos_pnl = -10.0;
        input.min_oos_trades = 4;
        input.loss_burst_lookback = 2;
        input.tail_first_ranking = true;
        let search = causal_policy_search_from_folds(&folds, &input);
        let top = search.candidates.first().expect("top candidate");

        assert_eq!(
            top.base_require.get("direction").map(String::as_str),
            Some("up")
        );
        assert_eq!(top.fold_forward.tail.max_loss_burst_reports, 1);
        assert_eq!(top.fold_forward.stats.total_pnl, 8.0);
    }

    #[test]
    fn causal_policy_payoff_asymmetry_gate_rejects_high_win_rate_candidate() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 1, 0.0, -5.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
        ];

        let mut input = causal_policy_input(10);
        input.max_require_terms = 1;
        input.max_deny_rules = 0;
        input.min_oos_wilson_win_rate_lower = 0.0;
        input.min_worst_oos_pnl = -10.0;
        input.min_oos_payoff_ratio = 0.5;
        input.max_oos_worst_loss_to_avg_win = 4.0;
        let search = causal_policy_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.base_require.get("direction").map(String::as_str) == Some("down")
            })
            .expect("direction/down policy");

        assert!(!search.ok);
        assert!(!candidate.passed);
        assert!(candidate.fold_forward.stats.payoff_ratio < 0.5);
        assert!(candidate.fold_forward.stats.worst_loss_to_avg_win > 4.0);
    }

    #[test]
    fn causal_policy_prior_loss_cluster_sentinel_flattens_after_warning() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 1, 0.0, -3.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 1, 0.0, -3.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 1, 0.0, -3.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
        ];

        let mut input = causal_policy_input(10);
        input.max_require_terms = 1;
        input.max_deny_rules = 0;
        input.min_oos_trades = 1;
        input.min_oos_wilson_win_rate_lower = 0.0;
        input.min_oos_total_pnl = -10.0;
        input.min_oos_profitable_reports = 0;
        input.min_worst_oos_pnl = -10.0;
        input.loss_burst_lookback = 5;
        input.max_loss_burst_reports = 2;
        input.max_prior_loss_burst_reports = 2;

        let search = causal_policy_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.base_require.get("direction").map(String::as_str) == Some("down")
            })
            .expect("direction/down policy");

        assert_eq!(
            candidate.fold_forward.decisions[4].reason,
            "prior_loss_cluster_sentinel_flat"
        );
        assert_eq!(
            candidate.fold_forward.decisions[4]
                .prior_tail
                .as_ref()
                .unwrap()
                .max_loss_burst_reports,
            2
        );
        assert_eq!(
            candidate.fold_forward.decisions[4].prior_recent_loss_reports,
            2
        );
        assert!(candidate.fold_forward.decisions[4].oos.is_none());
        assert_eq!(candidate.fold_forward.tail.max_loss_burst_reports, 2);
    }

    #[test]
    fn causal_policy_meta_label_gate_flattens_supported_bad_regime() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let late_down =
            "regime=zone=late|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![
                (primary_down, pnl_stats(6, 0, 6.0, 0.0)),
                (late_down, pnl_stats(0, 1, 0.0, -2.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(6, 0, 6.0, 0.0)),
                (late_down, pnl_stats(0, 1, 0.0, -2.0)),
            ]),
            selectivity_fold(vec![(late_down, pnl_stats(5, 0, 5.0, 0.0))]),
        ];

        let mut input = causal_policy_input(10);
        input.max_require_terms = 1;
        input.max_deny_rules = 0;
        input.min_oos_trades = 1;
        input.min_oos_wilson_win_rate_lower = 0.0;
        input.min_oos_total_pnl = -10.0;
        input.min_oos_profitable_reports = 0;
        input.min_worst_oos_pnl = -10.0;
        input.meta_label_min_support = 2;
        input.meta_label_min_quantile_pnl = 0.0;
        input.meta_label_max_loss_rate = 0.40;

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
        let meta_label = decision.meta_label.as_ref().expect("meta-label report");

        assert_eq!(decision.reason, "meta_label_loss_rate_above_budget_flat");
        assert!(meta_label.flattened);
        assert_eq!(meta_label.active_buckets, 1);
        assert_eq!(meta_label.supported_buckets, 1);
        assert_eq!(meta_label.buckets[0].support, 2);
        assert_eq!(meta_label.buckets[0].loss_rate, 1.0);
        assert!(decision.oos.is_none());
        assert_eq!(candidate.fold_forward.eligible_reports, 0);
    }

    #[test]
    fn causal_policy_meta_label_gate_uses_prior_only() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 1, 0.0, -8.0))]),
        ];

        let mut input = causal_policy_input(10);
        input.max_require_terms = 1;
        input.max_deny_rules = 0;
        input.min_oos_trades = 1;
        input.min_oos_wilson_win_rate_lower = 0.0;
        input.min_oos_total_pnl = -10.0;
        input.min_oos_profitable_reports = 0;
        input.min_worst_oos_pnl = -10.0;
        input.meta_label_min_support = 2;
        input.meta_label_min_quantile_pnl = 0.0;
        input.meta_label_max_loss_rate = 0.40;

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
        let meta_label = decision.meta_label.as_ref().expect("meta-label report");

        assert_eq!(decision.reason, "policy_selected_from_prior_causal_tags");
        assert!(!meta_label.flattened);
        assert_eq!(meta_label.buckets[0].support, 2);
        assert_eq!(meta_label.buckets[0].loss_rate, 0.0);
        assert_eq!(decision.oos.as_ref().unwrap().total_pnl, -8.0);
    }

    #[test]
    fn causal_policy_meta_label_generalizes_sparse_exact_regime() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let early_down_prior =
            "regime=zone=early|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=lt_0.50|vol=lt_0.40|rev=1_2|min=2_4";
        let early_down_new =
            "regime=zone=early|dir=down|price=0.50_0.75|edge=0.07_0.15|z=1.1_1.5|conf=lt_0.50|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![
                (primary_down, pnl_stats(8, 0, 8.0, 0.0)),
                (early_down_prior, pnl_stats(0, 1, 0.0, -3.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(8, 0, 8.0, 0.0)),
                (early_down_prior, pnl_stats(0, 1, 0.0, -3.0)),
            ]),
            selectivity_fold(vec![(early_down_new, pnl_stats(4, 0, 4.0, 0.0))]),
        ];

        let mut input = causal_policy_input(10);
        input.max_require_terms = 1;
        input.max_deny_rules = 0;
        input.min_oos_trades = 1;
        input.min_oos_wilson_win_rate_lower = 0.0;
        input.min_oos_total_pnl = -10.0;
        input.min_oos_profitable_reports = 0;
        input.min_worst_oos_pnl = -10.0;
        input.meta_label_min_support = 2;
        input.meta_label_min_quantile_pnl = 0.0;
        input.meta_label_max_loss_rate = 0.40;
        input.meta_label_max_generalization_terms = 1;

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
        let meta_label = decision.meta_label.as_ref().expect("meta-label report");

        assert_eq!(decision.reason, "meta_label_loss_rate_above_budget_flat");
        assert!(meta_label.flattened);
        assert!(meta_label.buckets.iter().any(|bucket| {
            bucket.kind == "generalized"
                && bucket.match_tags.get("zone").map(String::as_str) == Some("early")
                && bucket.support == 2
                && bucket.loss_rate == 1.0
        }));
        assert!(decision.oos.is_none());
    }

    #[test]
    fn causal_policy_prior_payoff_budget_flattens_bad_asymmetry() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(10, 0, 10.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 1, 0.0, -5.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
        ];

        let mut input = causal_policy_input(10);
        input.max_require_terms = 1;
        input.max_deny_rules = 0;
        input.min_train_trades = 1;
        input.min_prior_payoff_ratio = 0.3;

        let search = causal_policy_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.base_require.get("direction").map(String::as_str) == Some("down")
            })
            .expect("direction/down policy");

        assert_eq!(
            candidate.fold_forward.decisions[2].reason,
            "prior_payoff_ratio_below_budget"
        );
        assert!(candidate.fold_forward.decisions[2].oos.is_none());
        assert_eq!(candidate.fold_forward.eligible_reports, 0);
    }

    #[test]
    fn causal_policy_prior_worst_loss_budget_flattens_bad_asymmetry() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(10, 0, 10.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(0, 1, 0.0, -5.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
        ];

        let mut input = causal_policy_input(10);
        input.max_require_terms = 1;
        input.max_deny_rules = 0;
        input.min_train_trades = 1;
        input.max_prior_worst_loss_to_avg_win = 4.0;

        let search = causal_policy_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| {
                candidate.base_require.get("direction").map(String::as_str) == Some("down")
            })
            .expect("direction/down policy");

        assert_eq!(
            candidate.fold_forward.decisions[2].reason,
            "prior_worst_loss_to_avg_win_above_budget"
        );
        assert!(candidate.fold_forward.decisions[2].oos.is_none());
        assert_eq!(candidate.fold_forward.eligible_reports, 0);
    }

    #[test]
    fn tail_risk_report_uses_left_tail_and_windowed_bursts() {
        let report = tail_risk_report(&[4.0, -2.0, -3.0, 1.0, -1.0], 0.40, 3);

        assert_eq!(report.sample_count, 5);
        assert_eq!(report.tail_count, 2);
        assert_eq!(report.worst_pnl, -3.0);
        assert_eq!(report.cvar_pnl, -2.5);
        assert_eq!(report.losing_reports, 3);
        assert_eq!(report.max_loss_burst_reports, 2);
    }

    #[test]
    fn strategy_registry_mark_inserts_and_updates_evidence_history() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("registry.json");

        let registry = mark_strategy_version(StrategyRegistryMarkInput {
            registry_path: path.clone(),
            strategy_id: "tail_guard_v1".to_string(),
            parent_id: None,
            status: StrategyRegistryStatus::Questionable,
            reason: "profitable mean but poor tail fold".to_string(),
            artifact_path: Some("/tmp/search.json".to_string()),
            metrics_path: None,
            evidence_paths: vec!["/tmp/fold_40.json".to_string()],
            notes: vec!["needs CVaR gate".to_string()],
        })
        .unwrap();

        assert_eq!(registry.entries.len(), 1);
        assert_eq!(
            registry.entries[0].status,
            StrategyRegistryStatus::Questionable
        );
        assert_eq!(registry.entries[0].events.len(), 1);

        let registry = mark_strategy_version(StrategyRegistryMarkInput {
            registry_path: path,
            strategy_id: "tail_guard_v1".to_string(),
            parent_id: None,
            status: StrategyRegistryStatus::DeadEnd,
            reason: "failed strict CVaR and burst gates".to_string(),
            artifact_path: None,
            metrics_path: Some("/tmp/tail_metrics.json".to_string()),
            evidence_paths: vec!["/tmp/fold_41.json".to_string()],
            notes: vec!["do not promote".to_string()],
        })
        .unwrap();
        let entry = &registry.entries[0];

        assert_eq!(entry.status, StrategyRegistryStatus::DeadEnd);
        assert_eq!(entry.events.len(), 2);
        assert_eq!(entry.artifact_path.as_deref(), Some("/tmp/search.json"));
        assert_eq!(
            entry.metrics_path.as_deref(),
            Some("/tmp/tail_metrics.json")
        );
        assert!(entry
            .evidence_paths
            .iter()
            .any(|path| path == "/tmp/fold_40.json"));
        assert!(entry
            .evidence_paths
            .iter()
            .any(|path| path == "/tmp/fold_41.json"));
    }

    #[test]
    fn evidence_export_archives_existing_paths_and_rewrites_registry() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry_path = tmp.path().join("registry.json");
        let evidence_path = tmp.path().join("tail_report.json");
        let missing_path = tmp.path().join("missing_zone_audit.json");
        std::fs::write(&evidence_path, br#"{"ok":false,"pnl":-1.23}"#).unwrap();

        mark_strategy_version(StrategyRegistryMarkInput {
            registry_path: registry_path.clone(),
            strategy_id: "tail_guard_test".to_string(),
            parent_id: None,
            status: StrategyRegistryStatus::Questionable,
            reason: "temporary evidence under scratch path".to_string(),
            artifact_path: Some(evidence_path.display().to_string()),
            metrics_path: Some(missing_path.display().to_string()),
            evidence_paths: vec![evidence_path.display().to_string()],
            notes: Vec::new(),
        })
        .unwrap();

        let out_dir = tmp.path().join("durable");
        let export = export_strategy_evidence(StrategyBuilderEvidenceExportInput {
            registry_path: registry_path.clone(),
            out_dir: out_dir.clone(),
            rewrite_registry: true,
        })
        .unwrap();

        assert!(export.registry_rewritten);
        assert_eq!(export.copied.len(), 1);
        assert_eq!(export.missing.len(), 1);
        for copy in &export.copied {
            assert!(copy.archived_path.starts_with(out_dir.to_str().unwrap()));
            assert_eq!(copy.bytes, 24);
            assert_eq!(copy.sha256.len(), 64);
            assert!(std::path::Path::new(&copy.archived_path).is_file());
        }

        let registry = read_strategy_registry(&registry_path).unwrap();
        let entry = &registry.entries[0];
        assert!(entry
            .artifact_path
            .as_ref()
            .unwrap()
            .starts_with(out_dir.to_str().unwrap()));
        assert_eq!(
            entry.metrics_path.as_ref().unwrap(),
            &missing_path.display().to_string()
        );
        assert!(entry.evidence_paths[0].starts_with(out_dir.to_str().unwrap()));
        assert!(entry.events[0].evidence_paths[0].starts_with(out_dir.to_str().unwrap()));
    }

    #[test]
    fn registry_audit_requires_promoted_evidence_to_be_durable() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry_path = tmp.path().join("registry.json");
        let scratch = tmp.path().join("scratch_report.json");
        std::fs::write(&scratch, "{}").unwrap();

        mark_strategy_version(StrategyRegistryMarkInput {
            registry_path: registry_path.clone(),
            strategy_id: "scratch_promoted".to_string(),
            parent_id: None,
            status: StrategyRegistryStatus::Promoted,
            reason: "promotion points at scratch evidence".to_string(),
            artifact_path: Some(scratch.display().to_string()),
            metrics_path: None,
            evidence_paths: vec![scratch.display().to_string()],
            notes: Vec::new(),
        })
        .unwrap();

        let audit = audit_strategy_registry(StrategyRegistryAuditInput {
            registry_path,
            durable_prefix: tmp.path().join("durable").display().to_string(),
        })
        .unwrap();

        assert!(!audit.ok);
        assert!(!audit.live_ready);
        assert_eq!(audit.live_candidate_count, 1);
        assert!(audit
            .non_durable_paths
            .iter()
            .any(|issue| { issue.strategy_id == "scratch_promoted" && issue.blocking_live }));
    }

    #[test]
    fn registry_audit_marks_single_durable_promoted_entry_live_ready() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry_path = tmp.path().join("registry.json");
        let durable_dir = tmp.path().join("durable");
        std::fs::create_dir_all(&durable_dir).unwrap();
        let evidence = durable_dir.join("promotion.json");
        std::fs::write(&evidence, "{}").unwrap();

        mark_strategy_version(StrategyRegistryMarkInput {
            registry_path: registry_path.clone(),
            strategy_id: "durable_promoted".to_string(),
            parent_id: None,
            status: StrategyRegistryStatus::Promoted,
            reason: "single durable promoted entry".to_string(),
            artifact_path: Some(evidence.display().to_string()),
            metrics_path: None,
            evidence_paths: vec![evidence.display().to_string()],
            notes: Vec::new(),
        })
        .unwrap();

        let audit = audit_strategy_registry(StrategyRegistryAuditInput {
            registry_path,
            durable_prefix: durable_dir.display().to_string(),
        })
        .unwrap();

        assert!(audit.ok);
        assert!(audit.live_ready);
        assert_eq!(audit.grade, "A+");
        assert!(audit.missing_paths.is_empty());
        assert!(audit.non_durable_paths.is_empty());
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
    fn adaptive_mode_search_reports_tail_cvar_and_loss_burst() {
        let folds = vec![
            selectivity_fold(vec![("direction=up", pnl_stats(10, 0, 10.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(10, 0, 10.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(10, 0, 10.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(0, 1, 0.0, -6.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(0, 1, 0.0, -5.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(20, 0, 20.0, 0.0))]),
        ];

        let mut input = adaptive_mode_input(5);
        input.min_worst_oos_pnl = -10.0;
        input.min_oos_cvar_pnl = -10.0;
        input.loss_burst_lookback = 2;
        input.max_loss_burst_reports = 1;
        let search = adaptive_mode_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("adaptive mode candidate");

        assert!(!search.ok);
        assert!(!candidate.passed);
        assert_eq!(candidate.fold_forward.tail.sample_count, 4);
        assert_eq!(candidate.fold_forward.tail.tail_count, 1);
        assert_eq!(candidate.fold_forward.tail.max_loss_burst_reports, 2);
        assert_eq!(candidate.fold_forward.tail.cvar_pnl, -6.0);
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
    fn adaptive_direction_search_reports_tail_cvar_and_loss_burst() {
        let folds = vec![
            selectivity_fold(vec![("direction=up", pnl_stats(10, 0, 10.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(10, 0, 10.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(10, 0, 10.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(0, 1, 0.0, -6.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(0, 1, 0.0, -5.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(20, 0, 20.0, 0.0))]),
        ];

        let mut input = adaptive_input(5);
        input.min_oos_cvar_pnl = -10.0;
        input.loss_burst_lookback = 2;
        input.max_loss_burst_reports = 1;
        let search = adaptive_direction_search_from_folds(&folds, &input);
        let candidate = search
            .candidates
            .iter()
            .find(|candidate| candidate.variant == "candidate")
            .expect("adaptive candidate");

        assert!(!search.ok);
        assert!(!candidate.passed);
        assert_eq!(candidate.fold_forward.tail.sample_count, 4);
        assert_eq!(candidate.fold_forward.tail.tail_count, 1);
        assert_eq!(candidate.fold_forward.tail.max_loss_burst_reports, 2);
        assert_eq!(candidate.fold_forward.tail.cvar_pnl, -6.0);
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

    #[test]
    fn tail_guard_profile_reduces_exposure_and_tightens_after_loss() {
        let profile = StrategyBuilderProfile::from_name("a_plus5m_tail_guard").unwrap();

        assert_eq!(profile.position_pct, "0.025");
        assert_eq!(profile.max_total_exposure_usd, "8");
        assert_eq!(profile.degraded_after_losses, "1");
        assert_eq!(profile.degraded_min_z, "1.10");
        assert_eq!(profile.degraded_max_price, "0.75");
        assert!(!profile.also_maker);
    }

    #[test]
    fn tail_challenger_profiles_pin_targeted_risk_shapes() {
        let primary = StrategyBuilderProfile::from_name("a_plus5m_tail_primary").unwrap();
        assert_eq!(primary.z, "0.70,0.90");
        assert_eq!(primary.max_price, "0.85,0.90");
        assert_eq!(primary.min_reversion_count, "1");
        assert_eq!(primary.position_pct, "0.05");
        assert!(!primary.also_maker);
        assert!(primary.degraded_force_taker);

        let early = StrategyBuilderProfile::from_name("a_plus5m_tail_early_reentry").unwrap();
        assert_eq!(early.conf, "0.60,0.70");
        assert_eq!(early.z, "1.10,1.30");
        assert_eq!(early.edge, "0.10,0.15");
        assert_eq!(early.max_price, "0.75");
        assert_eq!(early.position_pct, "0.05");
        assert_eq!(early.max_per_market_usd, "5");
        assert_eq!(early.max_total_exposure_usd, "5");

        let low_exposure = StrategyBuilderProfile::from_name("a_plus5m_tail_low_exposure").unwrap();
        assert_eq!(low_exposure.conf, "0.50");
        assert_eq!(low_exposure.z, "0.70,0.90");
        assert_eq!(low_exposure.position_pct, "0.05");
        assert_eq!(low_exposure.max_per_market_usd, "5");
        assert_eq!(low_exposure.max_total_exposure_usd, "5");
        assert!(!low_exposure.also_maker);
    }

    #[test]
    fn evolution_mutation_is_deterministic_and_bounded() {
        let mut tag_universe = BTreeMap::new();
        tag_universe.insert(
            "direction".to_string(),
            ["up".to_string(), "down".to_string()].into_iter().collect(),
        );
        tag_universe.insert(
            "zone".to_string(),
            ["early".to_string(), "primary".to_string()]
                .into_iter()
                .collect(),
        );
        let variants = vec!["candidate".to_string(), "challenger".to_string()];
        let variant_params = BTreeMap::new();
        let mut input = evolve_input(5);
        input.max_require_terms = 1;
        input.max_deny_rules = 1;
        let genome = EvolutionGenome {
            schema_version: 1,
            variant: "candidate".to_string(),
            require_tags: BTreeMap::new(),
            deny_tags: BTreeMap::new(),
            knobs: EvolutionStrategyKnobs::default(),
        };

        let mut left_rng = StdRng::seed_from_u64(7);
        let mut right_rng = StdRng::seed_from_u64(7);
        let left = mutate_evolution_genome(
            genome.clone(),
            &tag_universe,
            &variants,
            &variant_params,
            &input,
            &mut left_rng,
        );
        let right = mutate_evolution_genome(
            genome,
            &tag_universe,
            &variants,
            &variant_params,
            &input,
            &mut right_rng,
        );

        assert_eq!(left, right);
        assert!(left.require_tags.len() <= 1);
        assert!(left.deny_tags.len() <= 1);
        assert_eq!(evolution_genome_hash(&left), evolution_genome_hash(&right));
    }

    #[test]
    fn evolution_variant_mutation_syncs_knob_metadata() {
        let mut variants = BTreeMap::new();
        let mut candidate = StrategyVariant::baseline();
        candidate.name = "candidate".to_string();
        candidate.min_edge = 0.07;
        candidate.prefer_maker = false;
        let mut challenger = StrategyVariant::baseline();
        challenger.name = "challenger".to_string();
        challenger.min_edge = 0.15;
        challenger.prefer_maker = true;
        variants.insert(candidate.name.clone(), candidate);
        variants.insert(challenger.name.clone(), challenger);

        let mut genome = EvolutionGenome {
            schema_version: 1,
            variant: "candidate".to_string(),
            require_tags: BTreeMap::new(),
            deny_tags: BTreeMap::new(),
            knobs: EvolutionStrategyKnobs::default(),
        };
        genome.variant = "challenger".to_string();
        sync_evolution_genome_knobs(&mut genome, &variants);

        assert_eq!(genome.knobs.min_edge, Some(0.15));
        assert_eq!(genome.knobs.prefer_maker, Some(true));
    }

    #[test]
    fn evolution_knob_mutation_is_deterministic_and_bounded() {
        let mut left = EvolutionStrategyKnobs {
            min_price: Some(0.90),
            max_price: Some(0.10),
            min_reversion_count: Some(3),
            max_reversion_count: Some(1),
            ..EvolutionStrategyKnobs::default()
        };
        let mut right = left.clone();
        let mut left_rng = StdRng::seed_from_u64(19);
        let mut right_rng = StdRng::seed_from_u64(19);

        mutate_evolution_knob(&mut left, &mut left_rng);
        mutate_evolution_knob(&mut right, &mut right_rng);

        assert_eq!(left, right);
        if let (Some(min_price), Some(max_price)) = (left.min_price, left.max_price) {
            assert!(min_price >= 0.01);
            assert!(max_price <= 0.99);
            assert!(max_price >= min_price);
        }
        if let (Some(min_reversion), Some(max_reversion)) =
            (left.min_reversion_count, left.max_reversion_count)
        {
            assert!(max_reversion == u64::MAX || max_reversion >= min_reversion);
        }
        if let Some(pressure) = left.min_book_pressure {
            assert!((-1.0..=1.0).contains(&pressure));
        }
    }

    #[test]
    fn evolution_crossover_preserves_same_family_knob_genes() {
        let input = evolve_input(5);
        let mut left = evolution_test_genome("candidate", &[("direction", "up")], &[]);
        left.knobs.min_edge = Some(0.03);
        left.knobs.primary_min_z = Some(0.90);
        let mut right = evolution_test_genome("candidate", &[("zone", "primary")], &[]);
        right.knobs.min_edge = Some(0.15);
        right.knobs.primary_min_z = Some(1.50);
        let mut rng = StdRng::seed_from_u64(23);

        let child = crossover_evolution_genome(&left, &right, &input, &mut rng);

        assert_eq!(child.variant, "candidate");
        assert!([Some(0.03), Some(0.15)].contains(&child.knobs.min_edge));
        assert!([Some(0.90), Some(1.50)].contains(&child.knobs.primary_min_z));
    }

    #[test]
    fn evolution_survivor_cap_preserves_multiple_variant_families() {
        let folds = vec![
            selectivity_fold(vec![("direction=up", pnl_stats(3, 0, 3.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(5, 0, 5.0, 0.0))]),
        ];
        let causal_input = causal_input_from_evolution(&evolve_input(5));
        let base = evaluate_evolution_genome(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            evolution_test_genome("family_a", &[("direction", "up")], &[]),
        );
        let mut evaluated = Vec::new();
        for idx in 0..4 {
            let mut candidate = base.clone();
            candidate.genome.variant = "family_a".to_string();
            candidate.genome_hash = format!("a{idx}");
            evaluated.push(candidate);
        }
        for idx in 0..2 {
            let mut candidate = base.clone();
            candidate.genome.variant = "family_b".to_string();
            candidate.genome_hash = format!("b{idx}");
            evaluated.push(candidate);
        }

        let selected = evolution_diverse_candidate_indexes(&evaluated, 4, 2);
        let families = selected
            .iter()
            .map(|idx| evaluated[*idx].genome.variant.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            families
                .iter()
                .filter(|family| **family == "family_a")
                .count(),
            2
        );
        assert_eq!(
            families
                .iter()
                .filter(|family| **family == "family_b")
                .count(),
            2
        );
    }

    #[test]
    fn evolution_output_reserves_exact_replay_hypotheses() {
        let folds = vec![
            selectivity_fold(vec![("direction=up", pnl_stats(3, 0, 3.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(5, 0, 5.0, 0.0))]),
        ];
        let causal_input = causal_input_from_evolution(&evolve_input(5));
        let base = evaluate_evolution_genome(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            evolution_test_genome("candidate", &[("direction", "up")], &[]),
        );
        let mut sorted = Vec::new();
        for idx in 0..4 {
            let mut exact = base.clone();
            exact.genome_hash = format!("exact-{idx}");
            sorted.push(exact);
        }
        for idx in 0..4 {
            let mut replay = base.clone();
            replay.genome_hash = format!("replay-{idx}");
            replay.genome.knobs.max_recent_mid_runup = Some(0.06 + idx as f64 * 0.01);
            replay.passed = false;
            replay.fitness.passed = false;
            replay.fitness.static_fitness_exact = false;
            replay.fitness.gate_failures = 1;
            replay.fitness.failure_reasons =
                vec!["report_counterfactual_requires_replay".to_string()];
            sorted.push(replay);
        }
        sorted.sort_by(compare_evolution_candidates);

        let selected = select_evolution_output_candidates(&sorted, 4);

        assert_eq!(selected.len(), 4);
        assert_eq!(
            selected
                .iter()
                .filter(|candidate| candidate.fitness.static_fitness_exact)
                .count(),
            2
        );
        assert_eq!(
            selected
                .iter()
                .filter(|candidate| !candidate.fitness.static_fitness_exact)
                .count(),
            2
        );
        assert_eq!(
            selected
                .iter()
                .map(|candidate| candidate.rank)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn evolution_materialized_variant_applies_genome_knobs() {
        let folds = vec![
            selectivity_fold(vec![("direction=up", pnl_stats(3, 0, 3.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![("direction=up", pnl_stats(5, 0, 5.0, 0.0))]),
        ];
        let causal_input = causal_input_from_evolution(&evolve_input(5));
        let mut genome = evolution_test_genome("candidate", &[("direction", "up")], &[]);
        genome.knobs = EvolutionStrategyKnobs {
            min_confidence: Some(0.70),
            min_edge: Some(0.15),
            early_min_z: Some(0.90),
            primary_min_z: Some(1.10),
            late_min_z: Some(1.50),
            terminal_min_z: Some(2.00),
            min_price: Some(0.50),
            max_price: Some(0.85),
            min_ev_buffer: Some(0.03),
            settlement_guard_minutes: Some(3.0),
            settlement_min_abs_move_usd: Some(15.0),
            min_reversion_count: Some(1),
            max_reversion_count: Some(2),
            prefer_maker: Some(true),
            max_spread: Some(0.03),
            min_book_depth: Some(100.0),
            min_book_pressure: Some(0.10),
            recent_mid_lookback_seconds: Some(15.0),
            max_recent_mid_runup: Some(0.08),
        };
        let candidate = evaluate_evolution_genome(&folds, &causal_input, 0, Vec::new(), genome);
        let base = StrategyVariant::baseline();

        let materialized = executable_evolution_variant(&base, &candidate).unwrap();

        assert_eq!(materialized.min_confidence, 0.70);
        assert_eq!(materialized.zone_config.early_min_confidence, 0.70);
        assert_eq!(materialized.min_edge, 0.15);
        assert_eq!(materialized.zone_config.early_min_edge, 0.15);
        assert_eq!(materialized.zone_config.early_min_z, 0.90);
        assert_eq!(materialized.zone_config.primary_min_z, 1.10);
        assert_eq!(materialized.zone_config.late_min_z, 1.50);
        assert_eq!(materialized.zone_config.terminal_min_z, 2.00);
        assert_eq!(materialized.zone_config.min_price, 0.50);
        assert_eq!(materialized.zone_config.max_price, 0.85);
        assert_eq!(materialized.zone_config.min_ev_buffer, 0.03);
        assert_eq!(materialized.zone_config.settlement_guard_minutes, 3.0);
        assert_eq!(materialized.zone_config.settlement_min_abs_move_usd, 15.0);
        assert_eq!(materialized.zone_config.min_reversion_count, 1);
        assert_eq!(materialized.zone_config.max_reversion_count, 2);
        assert!(materialized.prefer_maker);
        assert_eq!(materialized.microstructure.max_spread, 0.03);
        assert_eq!(materialized.microstructure.min_book_depth, 100.0);
        assert_eq!(materialized.microstructure.min_book_pressure, 0.10);
        assert_eq!(
            materialized.microstructure.recent_mid_lookback_seconds,
            15.0
        );
        assert_eq!(materialized.microstructure.max_recent_mid_runup, 0.08);
        assert_eq!(
            materialized.selectivity.require_tags.get("direction"),
            Some(&"up".to_string())
        );
    }

    #[test]
    fn evolution_materialized_variant_embeds_harness_policy_args() {
        let folds = vec![
            selectivity_fold(vec![("book_age=lte_100ms", pnl_stats(3, 0, 3.0, 0.0))]),
            selectivity_fold(vec![("book_age=lte_100ms", pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![("book_age=lte_100ms", pnl_stats(5, 0, 5.0, 0.0))]),
        ];
        let causal_input = causal_input_from_evolution(&evolve_input(5));
        let genome = evolution_test_genome("candidate", &[("book_age", "lte_100ms")], &[]);
        let mut candidate = evaluate_evolution_genome(&folds, &causal_input, 0, Vec::new(), genome);
        candidate.final_policy.require_tags.clear();
        candidate.final_policy.deny_rules.clear();
        candidate.final_policy.harness_require_args = vec![
            "book_age=lte_100ms".to_string(),
            "reversion=1_2".to_string(),
        ];
        candidate.final_policy.harness_deny_args = vec!["direction=up".to_string()];
        let base = StrategyVariant::baseline();

        let materialized = executable_evolution_variant(&base, &candidate).unwrap();

        assert_eq!(
            materialized.selectivity.require_tags.get("book_age"),
            Some(&"lte_100ms".to_string())
        );
        assert_eq!(
            materialized.selectivity.require_tags.get("reversion"),
            Some(&"1_2".to_string())
        );
        assert!(materialized
            .selectivity
            .deny_tag_values
            .get("direction")
            .is_some_and(|values| values.contains("up")));
    }

    #[test]
    fn evolution_extracts_historical_policy_seed_genome() {
        let mut variants = BTreeMap::new();
        let mut variant = StrategyVariant::baseline();
        variant.name = "candidate".to_string();
        variant.min_edge = 0.09;
        variants.insert(variant.name.clone(), variant);
        let report_set = SelectivityReportSet {
            folds: Vec::new(),
            variants,
        };
        let candidate = serde_json::json!({
            "variant": "candidate",
            "final_policy": {
                "require_tags": {"reversion": "1_2"},
                "harness_require_args": ["reversion=1_2"],
                "harness_deny_args": ["z=gte_1.5"]
            }
        });
        let valid_variants = ["candidate".to_string()].into_iter().collect();

        let genomes = historical_evolution_genomes_from_candidate(
            &candidate,
            &report_set,
            &["candidate".to_string()],
            &valid_variants,
        );
        assert_eq!(genomes.len(), 1);
        let genome = &genomes[0];

        assert_eq!(genome.variant, "candidate");
        assert_eq!(
            genome.require_tags.get("reversion").map(String::as_str),
            Some("1_2")
        );
        assert_eq!(
            genome.deny_tags.get("z").map(String::as_str),
            Some("gte_1.5")
        );
        assert_eq!(genome.knobs.min_edge, Some(0.09));
    }

    #[test]
    fn evolution_remaps_historical_policy_when_variant_is_absent() {
        let mut variants = BTreeMap::new();
        for name in ["fresh_a", "fresh_b"] {
            let mut variant = StrategyVariant::baseline();
            variant.name = name.to_string();
            variants.insert(variant.name.clone(), variant);
        }
        let report_set = SelectivityReportSet {
            folds: Vec::new(),
            variants,
        };
        let candidate = serde_json::json!({
            "variant": "old_profile",
            "final_policy": {
                "harness_require_args": ["book_age=lte_100ms"],
                "harness_deny_args": ["edge=gte_0.15"]
            }
        });
        let current_variants = vec!["fresh_a".to_string(), "fresh_b".to_string()];
        let valid_variants = current_variants.iter().cloned().collect();

        let genomes = historical_evolution_genomes_from_candidate(
            &candidate,
            &report_set,
            &current_variants,
            &valid_variants,
        );

        assert_eq!(genomes.len(), 2);
        assert_eq!(genomes[0].variant, "fresh_a");
        assert_eq!(
            genomes[1].require_tags.get("book_age").map(String::as_str),
            Some("lte_100ms")
        );
        assert_eq!(
            genomes[1].deny_tags.get("edge").map(String::as_str),
            Some("gte_0.15")
        );
    }

    #[test]
    fn evolution_tail_first_ranking_prefers_cleaner_tail_over_higher_pnl() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let primary_up =
            "regime=zone=primary|dir=up|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![
                (primary_down, pnl_stats(4, 0, 4.0, 0.0)),
                (primary_up, pnl_stats(4, 0, 4.0, 0.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(4, 0, 4.0, 0.0)),
                (primary_up, pnl_stats(4, 0, 4.0, 0.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(10, 0, 10.0, 0.0)),
                (primary_up, pnl_stats(3, 0, 3.0, 0.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(0, 1, 0.0, -4.0)),
                (primary_up, pnl_stats(3, 0, 3.0, 0.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(0, 1, 0.0, -4.0)),
                (primary_up, pnl_stats(0, 1, 0.0, -1.0)),
            ]),
            selectivity_fold(vec![
                (primary_down, pnl_stats(20, 0, 20.0, 0.0)),
                (primary_up, pnl_stats(3, 0, 3.0, 0.0)),
            ]),
        ];
        let mut input = evolve_input(10);
        input.max_deny_rules = 0;
        input.min_oos_trades = 4;
        input.min_oos_wilson_win_rate_lower = 0.0;
        input.min_worst_oos_pnl = -10.0;
        input.loss_burst_lookback = 2;
        let causal_input = causal_input_from_evolution(&input);
        let mut down = evaluate_evolution_genome(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            evolution_test_genome("candidate", &[("direction", "down")], &[]),
        );
        let mut up = evaluate_evolution_genome(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            evolution_test_genome("candidate", &[("direction", "up")], &[]),
        );
        down.pareto_front = 0;
        up.pareto_front = 0;

        assert!(down.fitness.total_pnl > up.fitness.total_pnl);
        assert_eq!(compare_evolution_candidates(&up, &down), Ordering::Less);
    }

    #[test]
    fn evolution_blocks_thin_eligible_report_candidate() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
        ];
        let mut input = evolve_input(10);
        input.max_deny_rules = 0;
        input.min_oos_eligible_reports = 2;
        let causal_input = causal_input_from_evolution(&input);
        let candidate = evaluate_evolution_genome(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            evolution_test_genome("candidate", &[("direction", "down")], &[]),
        );

        assert_eq!(candidate.fitness.eligible_reports, 1);
        assert!(!candidate.passed);
        assert!(candidate
            .fitness
            .failure_reasons
            .iter()
            .any(|reason| reason == "eligible_reports_below_gate"));
    }

    #[test]
    fn evolution_accepts_exact_no_arg_variant_for_replay() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=0.7_1.1|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
        ];
        let mut input = evolve_input(10);
        input.min_oos_trades = 5;
        input.min_oos_wilson_win_rate_lower = 0.0;
        let causal_input = causal_input_from_evolution(&input);
        let candidate = evaluate_evolution_genome(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            evolution_test_genome("candidate", &[], &[]),
        );

        assert!(candidate.fitness.replayable_policy);
        assert!(candidate.fitness.static_fitness_exact);
        assert!(candidate.passed);
    }

    #[test]
    fn evolution_requires_replay_for_mutated_strategy_knobs() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=1.1_1.5|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
        ];
        let mut input = evolve_input(10);
        input.min_oos_trades = 5;
        input.min_oos_wilson_win_rate_lower = 0.0;
        let causal_input = causal_input_from_evolution(&input);
        let mut source = StrategyVariant::baseline();
        source.name = "candidate".to_string();
        let mut genome = evolution_test_genome("candidate", &[], &[]);
        genome.knobs = evolution_knobs_from_variant(&source);
        genome.knobs.primary_min_z = Some(source.zone_config.primary_min_z + 0.20);
        let variants = [(source.name.clone(), source)].into_iter().collect();

        let candidate = evaluate_evolution_genome_against_variants(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            genome,
            &variants,
        );

        assert!(candidate.fitness.replayable_policy);
        assert!(!candidate.fitness.static_fitness_exact);
        assert!(!candidate.passed);
        assert!(candidate
            .fitness
            .failure_reasons
            .iter()
            .any(|reason| reason == "report_counterfactual_requires_replay"));
    }

    #[test]
    fn evolution_requires_replay_for_counterfactual_selectivity() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=1.1_1.5|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
        ];
        let mut input = evolve_input(10);
        input.max_deny_rules = 0;
        input.min_oos_trades = 5;
        input.min_oos_wilson_win_rate_lower = 0.0;
        let causal_input = causal_input_from_evolution(&input);
        let mut source = StrategyVariant::baseline();
        source.name = "candidate".to_string();
        let mut genome = evolution_test_genome("candidate", &[("direction", "down")], &[]);
        genome.knobs = evolution_knobs_from_variant(&source);
        let variants = [(source.name.clone(), source)].into_iter().collect();

        let candidate = evaluate_evolution_genome_against_variants(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            genome,
            &variants,
        );

        assert!(candidate.fitness.replayable_policy);
        assert!(!candidate.fitness.static_fitness_exact);
        assert!(!candidate.passed);
        assert!(candidate
            .fitness
            .failure_reasons
            .iter()
            .any(|reason| reason == "report_counterfactual_requires_replay"));
    }

    #[test]
    fn evolution_rejects_policy_conflicting_with_source_selectivity() {
        let primary_down =
            "regime=zone=primary|dir=down|price=0.75_0.90|edge=0.07_0.15|z=1.1_1.5|conf=0.50_0.70|vol=lt_0.40|rev=1_2|min=2_4";
        let folds = vec![
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(4, 0, 4.0, 0.0))]),
            selectivity_fold(vec![(primary_down, pnl_stats(5, 0, 5.0, 0.0))]),
        ];
        let mut input = evolve_input(10);
        input.min_oos_wilson_win_rate_lower = 0.0;
        let causal_input = causal_input_from_evolution(&input);
        let mut source = StrategyVariant::baseline();
        source.name = "candidate".to_string();
        source
            .selectivity
            .require_tags
            .insert("reversion".to_string(), "1_2".to_string());
        let mut genome = evolution_test_genome("candidate", &[], &[("reversion", "1_2")]);
        genome.knobs = evolution_knobs_from_variant(&source);
        let variants = [(source.name.clone(), source)].into_iter().collect();

        let candidate = evaluate_evolution_genome_against_variants(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            genome,
            &variants,
        );

        assert!(!candidate.fitness.replayable_policy);
        assert!(!candidate.fitness.static_fitness_exact);
        assert!(!candidate.passed);
        assert!(candidate
            .fitness
            .failure_reasons
            .iter()
            .any(|reason| reason == "source_variant_not_replayable"));
        assert!(candidate
            .notes
            .iter()
            .any(|note| note.contains("cannot both require and deny reversion=1_2")));
    }

    #[test]
    fn evolution_replay_manifest_is_dry_run_with_policy_args() {
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
        let mut input = evolve_input(10);
        input.replay_start = Some("2026-05-28T00:00:00Z".to_string());
        input.replay_end = Some("2026-06-10T23:00:00Z".to_string());
        input.atomic_parquet = true;
        input.latency_ms = 128;
        let causal_input = causal_input_from_evolution(&input);
        let candidate = evaluate_evolution_genome(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            evolution_test_genome("candidate", &[("direction", "down")], &[("zone", "early")]),
        );
        let manifest =
            evolution_replay_manifest(&candidate, &input, Path::new("/tmp/evo_candidate"));
        let args = manifest
            .get("rolling_history_args")
            .and_then(|value| value.as_array())
            .unwrap()
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            manifest.get("execute").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert!(!args.contains(&"--execute"));
        assert!(args.windows(2).any(|pair| pair == ["--latency-ms", "128"]));
        assert!(args.contains(&"--atomic-parquet"));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--require-causal-tag", "direction=down"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--deny-causal-tag", "zone=early"]));
    }

    #[test]
    fn evolution_replay_manifest_prefers_exact_variant_json() {
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
        let mut input = evolve_input(10);
        input.replay_start = Some("2026-05-28T00:00:00Z".to_string());
        input.replay_end = Some("2026-06-10T23:00:00Z".to_string());
        input.atomic_parquet = true;
        input.latency_ms = 128;
        let causal_input = causal_input_from_evolution(&input);
        let mut candidate = evaluate_evolution_genome(
            &folds,
            &causal_input,
            0,
            Vec::new(),
            evolution_test_genome("candidate", &[("direction", "down")], &[("zone", "early")]),
        );
        candidate.variant_path = Some("/tmp/evo_candidate/variant.json".to_string());
        let manifest =
            evolution_replay_manifest(&candidate, &input, Path::new("/tmp/evo_candidate"));
        let args = manifest
            .get("rolling_history_args")
            .and_then(|value| value.as_array())
            .unwrap()
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            manifest.get("variant_json").and_then(|v| v.as_str()),
            Some("/tmp/evo_candidate/variant.json")
        );
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--variant-json", "/tmp/evo_candidate/variant.json"]));
        assert!(!args.contains(&"--require-causal-tag"));
        assert!(!args.contains(&"--deny-causal-tag"));
        assert!(args.contains(&"--atomic-parquet"));
    }

    #[test]
    fn evolution_rejects_unsupported_multi_term_denies() {
        let mut input = evolve_input(5);
        input.max_deny_terms = 2;
        let err = validate_evolve_search_input(&input).unwrap_err();
        assert!(err.to_string().contains("single-tag deny"));
    }

    #[test]
    fn evolution_atomic_json_writer_replaces_payload() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("artifact.json");
        write_json_artifact_atomic(&path, &serde_json::json!({"version": 1})).unwrap();
        write_json_artifact_atomic(&path, &serde_json::json!({"version": 2})).unwrap();

        let value: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(value["version"], 2);
        assert!(std::fs::read_dir(tmp.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp.")));
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
            tail_alpha: 0.20,
            min_oos_cvar_pnl: -1.0e9,
            loss_burst_lookback: 0,
            max_loss_burst_reports: 0,
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
            tail_alpha: 0.20,
            min_oos_cvar_pnl: -1.0e9,
            loss_burst_lookback: 0,
            max_loss_burst_reports: 0,
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
            min_oos_eligible_reports: 0,
            min_worst_oos_pnl: 0.0,
            max_require_terms: 3,
            max_deny_rules: 1,
            max_deny_terms: 1,
            min_deny_trades: 1,
            min_deny_loss_pnl: 0.0,
            min_deny_loss_reports: 1,
            tail_alpha: 0.20,
            min_oos_cvar_pnl: -1.0e9,
            loss_burst_lookback: 0,
            max_loss_burst_reports: 0,
            tail_first_ranking: false,
            min_oos_payoff_ratio: 0.0,
            max_oos_worst_loss_to_avg_win: 0.0,
            prior_loss_cluster_lookback: 0,
            max_prior_loss_burst_reports: 0,
            min_prior_payoff_ratio: 0.0,
            max_prior_worst_loss_to_avg_win: 0.0,
            meta_label_min_support: 0,
            meta_label_alpha: 0.20,
            meta_label_min_quantile_pnl: -1.0e9,
            meta_label_max_loss_rate: 1.0,
            meta_label_require_supported: false,
            meta_label_max_generalization_terms: 0,
            top,
        }
    }

    fn evolve_input(top: usize) -> StrategyBuilderEvolveSearchInput {
        StrategyBuilderEvolveSearchInput {
            report_paths: vec![
                "fold0.json".to_string(),
                "fold1.json".to_string(),
                "fold2.json".to_string(),
            ],
            historical_search_paths: Vec::new(),
            out_dir: PathBuf::from("target/evolve-test"),
            seed: 42,
            population: 8,
            generations: 2,
            elite_count: 2,
            min_train_reports: 2,
            min_train_trades: 4,
            min_oos_trades: 5,
            min_oos_wilson_win_rate_lower: 0.50,
            min_oos_total_pnl: 0.0,
            min_oos_profitable_reports: 1,
            min_oos_eligible_reports: 0,
            min_worst_oos_pnl: 0.0,
            max_require_terms: 3,
            max_deny_rules: 1,
            max_deny_terms: 1,
            min_deny_trades: 1,
            min_deny_loss_pnl: 0.0,
            min_deny_loss_reports: 1,
            tail_alpha: 0.20,
            min_oos_cvar_pnl: -1.0e9,
            loss_burst_lookback: 0,
            max_loss_burst_reports: 0,
            min_oos_payoff_ratio: 0.0,
            max_oos_worst_loss_to_avg_win: 0.0,
            prior_loss_cluster_lookback: 0,
            max_prior_loss_burst_reports: 0,
            min_prior_payoff_ratio: 0.0,
            max_prior_worst_loss_to_avg_win: 0.0,
            meta_label_min_support: 0,
            meta_label_alpha: 0.20,
            meta_label_min_quantile_pnl: -1.0e9,
            meta_label_max_loss_rate: 1.0,
            meta_label_require_supported: false,
            meta_label_max_generalization_terms: 0,
            top,
            replay_start: None,
            replay_end: None,
            replay_profile: "a_plus5m".to_string(),
            replay_zone_mode: "early".to_string(),
            latency_ms: 128,
            latency_audit_json: None,
            btc_csv: None,
            fold_hours: 8,
            threads: 0,
            window_minutes: 5.0,
            atomic_parquet: false,
        }
    }

    fn evolution_test_genome(
        variant: &str,
        require: &[(&str, &str)],
        deny: &[(&str, &str)],
    ) -> EvolutionGenome {
        EvolutionGenome {
            schema_version: 1,
            variant: variant.to_string(),
            require_tags: require
                .iter()
                .map(|(dimension, value)| ((*dimension).to_string(), (*value).to_string()))
                .collect(),
            deny_tags: deny
                .iter()
                .map(|(dimension, value)| ((*dimension).to_string(), (*value).to_string()))
                .collect(),
            knobs: EvolutionStrategyKnobs::default(),
        }
    }

    #[test]
    fn materialize_policy_variant_writes_exact_runtime_selectivity() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut variant = StrategyVariant::baseline();
        variant.name = "candidate".to_string();
        variant
            .selectivity
            .require_tags
            .insert("reversion".to_string(), "1_2".to_string());
        let source_report = write_materialize_source_report(&tmp, &variant);
        let search_path = tmp.path().join("search.json");
        std::fs::write(
            &search_path,
            serde_json::to_vec(&serde_json::json!({
                "candidates": [{
                    "rank": 1,
                    "variant": "candidate",
                    "final_policy": {
                        "require_tags": {"direction": "up"},
                        "deny_rules": [{
                            "label": "zone=primary",
                            "match_tags": {"zone": "primary"}
                        }],
                        "harness_require_args": ["direction=up"],
                        "harness_deny_args": ["zone=primary"]
                    }
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let output_path = tmp.path().join("variant.json");

        let summary = materialize_policy_variant(StrategyBuilderMaterializePolicyVariantInput {
            search_path,
            source_report_paths: vec![source_report.display().to_string()],
            rank: 1,
            output_path: output_path.clone(),
        })
        .unwrap();
        let materialized: StrategyVariant =
            serde_json::from_slice(&std::fs::read(&output_path).unwrap()).unwrap();

        assert_eq!(summary.rank, 1);
        assert_eq!(summary.source_variant, "candidate");
        assert!(!summary.variant_hash.is_empty());
        assert!(materialized.name.ends_with("_policy_rank001"));
        assert_eq!(
            materialized.selectivity.require_tags.get("reversion"),
            Some(&"1_2".to_string())
        );
        assert_eq!(
            materialized.selectivity.require_tags.get("direction"),
            Some(&"up".to_string())
        );
        assert!(materialized
            .selectivity
            .deny_tag_values
            .get("zone")
            .is_some_and(|values| values.contains("primary")));
        assert_eq!(
            summary.selectivity.require_tags.get("reversion"),
            Some(&"1_2".to_string())
        );
    }

    #[test]
    fn materialize_policy_variant_rejects_unsupported_multi_term_deny() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut variant = StrategyVariant::baseline();
        variant.name = "candidate".to_string();
        let source_report = write_materialize_source_report(&tmp, &variant);
        let search_path = tmp.path().join("search.json");
        std::fs::write(
            &search_path,
            serde_json::to_vec(&serde_json::json!({
                "candidates": [{
                    "rank": 1,
                    "variant": "candidate",
                    "final_policy": {
                        "require_tags": {},
                        "deny_rules": [{
                            "label": "direction=down|z=gte_1.5",
                            "match_tags": {
                                "direction": "down",
                                "z": "gte_1.5"
                            }
                        }],
                        "harness_require_args": [],
                        "harness_deny_args": []
                    }
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let output_path = tmp.path().join("variant.json");

        let err = materialize_policy_variant(StrategyBuilderMaterializePolicyVariantInput {
            search_path,
            source_report_paths: vec![source_report.display().to_string()],
            rank: 1,
            output_path,
        })
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("unsupported multi-term runtime deny rule"));
    }

    #[test]
    fn materialize_sweep_variant_writes_exact_runtime_selectivity() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut variant = StrategyVariant::baseline();
        variant.name = "candidate".to_string();
        variant.prefer_maker = true;
        variant
            .selectivity
            .require_tags
            .insert("reversion".to_string(), "1_2".to_string());
        let source_report = write_materialize_source_report(&tmp, &variant);
        let output_path = tmp.path().join("variant.json");

        let summary = materialize_sweep_variant(StrategyBuilderMaterializeSweepVariantInput {
            report_path: source_report,
            rank: 1,
            output_path: output_path.clone(),
            require_causal_tag: vec!["direction=up".to_string()],
            deny_causal_tag: vec!["zone=primary".to_string()],
        })
        .unwrap();
        let materialized: StrategyVariant =
            serde_json::from_slice(&std::fs::read(&output_path).unwrap()).unwrap();

        assert_eq!(summary.rank, 1);
        assert_eq!(summary.source_variant, "candidate");
        assert!(!summary.variant_hash.is_empty());
        assert!(materialized.name.ends_with("_sweep_rank001"));
        assert!(materialized.prefer_maker);
        assert_eq!(
            materialized.selectivity.require_tags.get("reversion"),
            Some(&"1_2".to_string())
        );
        assert_eq!(
            materialized.selectivity.require_tags.get("direction"),
            Some(&"up".to_string())
        );
        assert!(materialized
            .selectivity
            .deny_tag_values
            .get("zone")
            .is_some_and(|values| values.contains("primary")));
    }

    #[test]
    fn materialize_sweep_variant_rejects_runtime_selectivity_conflict() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut variant = StrategyVariant::baseline();
        variant.name = "candidate".to_string();
        let source_report = write_materialize_source_report(&tmp, &variant);
        let output_path = tmp.path().join("variant.json");

        let err = materialize_sweep_variant(StrategyBuilderMaterializeSweepVariantInput {
            report_path: source_report,
            rank: 1,
            output_path,
            require_causal_tag: vec!["direction=up".to_string()],
            deny_causal_tag: vec!["direction=up".to_string()],
        })
        .unwrap_err();

        assert!(err.to_string().contains("both requires and denies"));
    }

    #[test]
    fn feature_filter_search_writes_ranked_exact_variants() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut variant = StrategyVariant::baseline();
        variant.name = "base_candidate".to_string();
        variant
            .selectivity
            .require_tags
            .insert("price".to_string(), "0.50_0.75".to_string());
        let base_variant_path = tmp.path().join("base_variant.json");
        write_json_artifact_atomic(&base_variant_path, &variant).unwrap();
        let feature_a = tmp.path().join("features_a.json");
        let feature_b = tmp.path().join("features_b.json");
        write_json_artifact_atomic(
            &feature_a,
            &serde_json::json!({
                "rows": [
                    {
                        "pnl_after_fee": 2.0,
                        "won": true,
                        "causal_tags": {
                            "direction": "up",
                            "zone": "early",
                            "price": "0.50_0.75",
                            "reversion": "1_2"
                        }
                    },
                    {
                        "pnl_after_fee": -3.0,
                        "won": false,
                        "causal_tags": {
                            "direction": "up",
                            "zone": "primary",
                            "price": "0.50_0.75",
                            "reversion": "1_2"
                        }
                    }
                ]
            }),
        )
        .unwrap();
        write_json_artifact_atomic(
            &feature_b,
            &serde_json::json!({
                "rows": [
                    {
                        "pnl_after_fee": 2.5,
                        "won": true,
                        "causal_tags": {
                            "direction": "up",
                            "zone": "early",
                            "price": "0.50_0.75",
                            "reversion": "1_2"
                        }
                    },
                    {
                        "pnl_after_fee": -2.0,
                        "won": false,
                        "causal_tags": {
                            "direction": "up",
                            "zone": "primary",
                            "price": "0.50_0.75",
                            "reversion": "1_2"
                        }
                    }
                ]
            }),
        )
        .unwrap();

        let summary = feature_filter_search(StrategyBuilderFeatureFilterSearchInput {
            feature_paths: vec![
                feature_a.display().to_string(),
                feature_b.display().to_string(),
            ],
            base_variant_path,
            out_dir: tmp.path().join("feature_search"),
            top: 5,
            max_require_terms: 2,
            max_deny_terms: 2,
            min_atom_trades: 1,
            max_atoms: 20,
            min_total_trades: 2,
            min_eligible_reports: 2,
            min_total_pnl: 0.0,
            min_worst_report_pnl: 0.0,
        })
        .unwrap();

        assert!(summary.ok);
        assert!(!summary.candidates.is_empty());
        let top = &summary.candidates[0];
        assert!(top.passed);
        assert_eq!(top.fitness.losses, 0);
        assert_eq!(top.fitness.eligible_reports, 2);
        assert!(top
            .deny_tag_values
            .get("zone")
            .is_some_and(|values| values.contains("primary")));
        let materialized: StrategyVariant =
            serde_json::from_slice(&std::fs::read(&top.variant_path).unwrap()).unwrap();
        assert!(materialized.name.ends_with("_feature_rank001"));
        assert_eq!(
            materialized.selectivity.require_tags.get("price"),
            Some(&"0.50_0.75".to_string())
        );
        assert!(materialized
            .selectivity
            .deny_tag_values
            .get("zone")
            .is_some_and(|values| values.contains("primary")));
        assert!(summary.out_dir.ends_with("feature_search"));
        assert!(std::path::Path::new(&top.variant_path).exists());
    }

    #[test]
    fn feature_filter_search_rejects_empty_feature_list() {
        let tmp = tempfile::TempDir::new().unwrap();
        let err = feature_filter_search(StrategyBuilderFeatureFilterSearchInput {
            feature_paths: Vec::new(),
            base_variant_path: tmp.path().join("base_variant.json"),
            out_dir: tmp.path().join("feature_search"),
            top: 5,
            max_require_terms: 2,
            max_deny_terms: 2,
            min_atom_trades: 1,
            max_atoms: 20,
            min_total_trades: 2,
            min_eligible_reports: 2,
            min_total_pnl: 0.0,
            min_worst_report_pnl: 0.0,
        })
        .unwrap_err();

        assert!(err.to_string().contains("at least one --feature report"));
    }

    fn write_materialize_source_report(
        tmp: &tempfile::TempDir,
        variant: &StrategyVariant,
    ) -> PathBuf {
        let path = tmp.path().join("source_report.json");
        let mut src = crate::data::manifest::DataSourceManifest::new("pmxt", "order_book_l2");
        src.complete = true;
        let report = crate::backtest::experiment::ExperimentReport {
            schema_version: 1,
            generated_at: "2026-05-22T00:00:00Z".to_string(),
            label: "source".to_string(),
            mode: "backtest".to_string(),
            start: "2026-05-21T00:00:00Z".to_string(),
            end: "2026-05-21T07:00:00Z".to_string(),
            bankroll_usd: 100.0,
            latency_ms: 128,
            market_catalog: crate::data::catalog::MarketCatalog::default(),
            data_manifest: crate::data::manifest::DataManifest::new(vec![src], Vec::new()),
            variants: vec![crate::backtest::experiment::VariantReport {
                strategy: crate::strategy::spec::StrategySpec::new("s", "1", "hash", "risk"),
                strategy_params: serde_json::to_value(variant).unwrap(),
                trades: 0,
                wins: 0,
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
                diagnostics: crate::backtest::resolver::BacktestDiagnostics::default(),
                win_rate: 0.0,
                total_pnl: 0.0,
                avg_pnl: 0.0,
                total_fees: 0.0,
                sharpe_like: 0.0,
                by_zone: BTreeMap::new(),
            }],
        };
        std::fs::write(&path, serde_json::to_vec(&report).unwrap()).unwrap();
        path
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
            tail_alpha: 0.20,
            min_oos_cvar_pnl: -1.0e9,
            loss_burst_lookback: 0,
            max_loss_burst_reports: 0,
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
