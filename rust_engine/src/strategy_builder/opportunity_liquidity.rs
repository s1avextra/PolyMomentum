//! Outcome-free paired-book features for liquidity-only strategy discovery.
//!
//! The source opportunity dataset supplies only immutable condition/timestamp
//! coordinates. Strategy features are reconstructed from both complementary
//! books without reading outcomes, BTC prices, volatility, or prior strategy
//! scores.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::artifact::{write_json_artifact_atomic, write_jsonl_atomic};
use crate::backtest::l2_replay::TokenBook;
use crate::backtest::pmxt::{L2EventBody, PMXTv2Loader};
use crate::data::models::Market;
use crate::execution::fees::polymarket_fee;
use crate::strategy::spec::stable_json_hash;

use super::opportunity_dataset::{load_sealed_opportunities, sha256_file, CausalOpportunity};
use super::opportunity_dataset::{read_labels, OpportunityLabelsManifest};
use super::opportunity_policy::{ExactReplayPlan, ExactReplayPlanEntry};
use super::opportunity_replay::OpportunityExactReplayReport;
use super::opportunity_table::{HashedSource, OpportunityTableManifest};

pub const OPPORTUNITY_PAIR_FEATURE_SCHEMA_VERSION: &str = "opportunity_pair_features_v1";
pub const OPPORTUNITY_LIQUIDITY_SEARCH_SCHEMA_VERSION: &str = "opportunity_liquidity_search_v1";
const LOOKBACK_MS: i64 = 15_000;
const MAXIMUM_EXECUTION_ASK: f64 = 0.99;

#[derive(Debug, Clone)]
pub struct OpportunityPairFeatureInput {
    pub dataset_seal_path: PathBuf,
    pub market_catalog_path: PathBuf,
    pub cache_dir: PathBuf,
    pub output_path: PathBuf,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct OpportunityLiquiditySearchInput {
    pub dataset_seal_path: PathBuf,
    pub labels_manifest_path: PathBuf,
    pub pair_features_manifest_path: PathBuf,
    pub output_path: PathBuf,
    pub minimum_calibration_support: usize,
    pub minimum_policy_support: usize,
    pub safety_margin: f64,
    pub latency_ms: u64,
    pub maximum_exact_replays: usize,
}

#[derive(Debug, Clone)]
pub struct OpportunityLiquidityDecisionInput {
    pub preregistration_path: PathBuf,
    pub liquidity_search_report_path: PathBuf,
    pub exact_replay_report_path: Option<PathBuf>,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PairPmxtSource {
    pub hour: String,
    pub pmxt_parquet: HashedSource,
    pub target_condition_count: usize,
    pub decoded_target_events: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityPairFeatureManifest {
    pub schema_version: String,
    pub generated_at: String,
    pub dataset_seal: HashedSource,
    pub dataset_sha256: String,
    pub market_catalog: HashedSource,
    pub output: HashedSource,
    pub source_opportunity_rows: usize,
    pub output_rows: usize,
    pub both_books_observable_rows: usize,
    pub both_lookback_books_observable_rows: usize,
    pub source_pmxt_hours: Vec<PairPmxtSource>,
    pub source_pmxt_scans: usize,
    pub lookback_ms: i64,
    pub outcome_columns_present: bool,
    pub gamma_outcome_prices_influence_output: bool,
    pub btc_or_model_features_influence_output: bool,
    pub feature_semantics: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PairBookFeature {
    pub observable: bool,
    pub book_timestamp_ms: Option<i64>,
    pub book_age_ms: Option<i64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub midpoint: Option<f64>,
    pub spread: Option<f64>,
    pub top_bid_depth_shares: Option<f64>,
    pub top_ask_depth_shares: Option<f64>,
    pub top_book_pressure: Option<f64>,
    pub stake_fully_executable: bool,
    pub fee_aware_break_even_probability: Option<f64>,
    pub fee_aware_net_win_usd: Option<f64>,
    pub fee_aware_max_loss_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OpportunityPairFeature {
    pub source_opportunity_id: String,
    pub condition_id: String,
    pub chronological_window: String,
    pub window_start_ms: i64,
    pub observed_at_ms: i64,
    pub decision_seconds: u16,
    pub up_token_id: String,
    pub down_token_id: String,
    pub up_lookback: PairBookFeature,
    pub down_lookback: PairBookFeature,
    pub up_now: PairBookFeature,
    pub down_now: PairBookFeature,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LiquidityPolicyDefinition {
    pub decision_seconds: u16,
    pub minimum_pressure_gap: f64,
    pub minimum_pressure_gap_widening_15s: f64,
    pub maximum_pair_spread: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LiquidityPolicyResult {
    pub policy_id: String,
    pub policy: LiquidityPolicyDefinition,
    pub calibration_support: usize,
    pub calibration_wins: usize,
    pub calibration_win_rate: Option<f64>,
    pub calibration_average_break_even_probability: Option<f64>,
    pub calibration_point_estimate_edge: Option<f64>,
    pub discovery_support: usize,
    pub wins: usize,
    pub losses: usize,
    pub win_rate: Option<f64>,
    pub wilson_win_rate_lower: Option<f64>,
    pub average_break_even_probability: Option<f64>,
    pub point_estimate_edge: Option<f64>,
    pub wilson_edge: Option<f64>,
    pub economic_payoff_proxy_usd: f64,
    pub average_pressure_gap: Option<f64>,
    pub average_pressure_gap_widening_15s: Option<f64>,
    pub average_absolute_midpoint_parity_residual: Option<f64>,
    pub fresh_causal_support: usize,
    pub discovery_trace_sha256: Option<String>,
    pub fresh_support_trace_sha256: Option<String>,
    pub discovery_eligible: bool,
    pub rejection_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityLiquiditySearchReport {
    pub schema_version: String,
    pub generated_at: String,
    pub family_id: String,
    pub dataset_seal: HashedSource,
    pub labels_manifest: HashedSource,
    pub paired_features: HashedSource,
    pub dataset_sha256: String,
    pub causal_feature_semantics_version: String,
    pub source_opportunity_table_reads: usize,
    pub source_paired_feature_reads: usize,
    pub in_memory_policy_evaluation_passes: usize,
    pub fresh_holdout_outcomes_accessed: bool,
    pub selection_semantics: String,
    pub policy_grid_sha256: String,
    pub policies_evaluated: usize,
    pub minimum_calibration_support: usize,
    pub minimum_policy_support: usize,
    pub safety_margin: f64,
    pub calibration_semantics: String,
    pub discovery_gate: String,
    pub exact_replay_is_research_only: bool,
    pub promotion_requires_wilson_after_exact_replay: bool,
    pub calibration_rows: usize,
    pub discovery_rows: usize,
    pub fresh_holdout_rows: usize,
    pub both_books_observable_rows: usize,
    pub eligible_policy_count: usize,
    pub top_diagnostics: Vec<LiquidityPolicyResult>,
    pub exact_replay_plan: ExactReplayPlan,
    pub verdict: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LiquidityTraceDecision {
    pub discovery_trace_sha256: String,
    pub representative_policy_id: String,
    pub decision_seconds: u16,
    pub fills: usize,
    pub wins: usize,
    pub losses: usize,
    pub point_estimate_edge: Option<f64>,
    pub wilson_edge: Option<f64>,
    pub total_pnl_usd: f64,
    pub decision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityLiquidityDecisionReport {
    pub schema_version: String,
    pub generated_at: String,
    pub family_id: String,
    pub preregistration: HashedSource,
    pub liquidity_search_report: HashedSource,
    pub exact_replay_report: Option<HashedSource>,
    pub fixed_advancement_wilson_edge: f64,
    pub maximum_exact_replays: usize,
    pub exact_replays_consumed: usize,
    pub fresh_holdout_outcomes_accessed: bool,
    pub trace_decisions: Vec<LiquidityTraceDecision>,
    pub decision: String,
    pub reason: String,
    pub terminal: bool,
    pub search_budget_exhausted: bool,
    pub more_evidence_allowed: bool,
    pub fresh_gate_opened: bool,
}

#[derive(Debug, Deserialize)]
struct LiquidityPreregistration {
    schema_version: String,
    family_id: String,
    inputs: LiquidityPreregisteredInputs,
    policy_grid_sha256: String,
    policies_evaluated: usize,
    cheap_screen: LiquidityCheapScreen,
    exact_replay_budget: LiquidityReplayBudget,
    advancement_gate: LiquidityAdvancementGate,
}

#[derive(Debug, Deserialize)]
struct LiquidityCheapScreen {
    minimum_calibration_support: usize,
    minimum_policy_support: usize,
    safety_margin: f64,
}

#[derive(Debug, Deserialize)]
struct LiquidityPreregisteredInputs {
    dataset_seal: HashedSource,
    labels_manifest: HashedSource,
    paired_features: HashedSource,
}

#[derive(Debug, Deserialize)]
struct LiquidityReplayBudget {
    maximum_unique_traces: usize,
    latency_ms: u64,
    stake_usd: f64,
    fee_rate: f64,
    additional_discovery_hours: usize,
    additional_parameter_variants: usize,
}

#[derive(Debug, Deserialize)]
struct LiquidityAdvancementGate {
    minimum_exact_replay_wilson_edge: f64,
    require_positive_exact_replay_pnl: bool,
}

#[derive(Debug, Clone)]
struct PairIdentity {
    up_token_id: String,
    down_token_id: String,
}

#[derive(Debug, Clone, Copy)]
enum TargetKind {
    Lookback,
    Now,
}

#[derive(Debug, Clone)]
struct MeasurementTarget {
    row_index: usize,
    timestamp_ms: i64,
    kind: TargetKind,
}

#[derive(Debug, Clone)]
struct PartialPairFeature {
    opportunity: CausalOpportunity,
    identity: PairIdentity,
    up_lookback: Option<PairBookFeature>,
    down_lookback: Option<PairBookFeature>,
    up_now: Option<PairBookFeature>,
    down_now: Option<PairBookFeature>,
}

#[derive(Debug, Default)]
struct LiquidityAccumulator {
    calibration_support: usize,
    calibration_wins: usize,
    calibration_break_even_sum: f64,
    discovery_ids: Vec<String>,
    discovery_tokens: BTreeMap<String, String>,
    discovery_wins: usize,
    discovery_losses: usize,
    discovery_break_even_sum: f64,
    discovery_pnl_usd: f64,
    discovery_pressure_gap_sum: f64,
    discovery_pressure_widening_sum: f64,
    discovery_parity_residual_sum: f64,
    fresh_selections: Vec<(String, String)>,
}

#[derive(Debug)]
struct FinishedLiquidityPolicy {
    result: LiquidityPolicyResult,
    discovery_ids: Vec<String>,
    discovery_tokens: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
struct LiquiditySelection<'a> {
    token_id: &'a str,
    direction: &'static str,
    book: &'a PairBookFeature,
    pressure_gap: f64,
    pressure_widening: f64,
    parity_residual: f64,
}

pub fn create_pair_features(
    input: OpportunityPairFeatureInput,
) -> Result<OpportunityPairFeatureManifest> {
    validate_feature_input(&input)?;
    let dataset_seal_sha256 = sha256_file(&input.dataset_seal_path)?;
    let (seal, opportunities) = load_sealed_opportunities(&input.dataset_seal_path)?;
    let market_catalog_sha256 = sha256_file(&input.market_catalog_path)?;
    let identities = load_pair_identities(&input.market_catalog_path)?;

    let mut opportunities_by_hour = BTreeMap::<i64, Vec<CausalOpportunity>>::new();
    for opportunity in opportunities.iter().cloned() {
        let hour_ms = opportunity.observed_at_ms.div_euclid(3_600_000) * 3_600_000;
        opportunities_by_hour
            .entry(hour_ms)
            .or_default()
            .push(opportunity);
    }

    let manifests_by_hour = load_source_manifests(&seal)?;
    let loader = PMXTv2Loader::new(&input.cache_dir);
    let mut rows = Vec::with_capacity(opportunities.len());
    let mut pmxt_sources = Vec::new();
    for (hour_ms, hour_opportunities) in opportunities_by_hour {
        let hour = DateTime::<Utc>::from_timestamp_millis(hour_ms)
            .context("paired-feature hour is outside chrono range")?;
        let hour_name = hour.to_rfc3339_opts(SecondsFormat::Secs, true);
        let source_manifest = manifests_by_hour
            .get(&hour_name)
            .with_context(|| format!("sealed opportunity manifest missing hour {hour_name}"))?;
        let pmxt_path = loader.cache_path_for_hour(hour);
        if !pmxt_path.is_file() {
            bail!(
                "paired features require cached PMXT hour at {}",
                pmxt_path.display()
            );
        }
        let pmxt_sha256 = sha256_file(&pmxt_path)?;
        if pmxt_sha256 != source_manifest.pmxt_parquet.sha256 {
            bail!("cached PMXT hash drift for sealed hour {hour_name}");
        }
        let condition_ids = hour_opportunities
            .iter()
            .map(|row| row.condition_id.clone())
            .collect::<HashSet<_>>();
        let events = loader
            .load_cached_hour(hour, Some(&condition_ids))
            .with_context(|| format!("row-filter paired-feature PMXT hour {hour_name}"))?;
        if events.is_empty() {
            bail!("paired-feature PMXT hour {hour_name} contains zero target events");
        }
        let mut hour_rows = build_hour_rows(
            hour_opportunities,
            &identities,
            &events,
            seal.stake_usd,
            seal.fee_rate,
        )?;
        rows.append(&mut hour_rows);
        pmxt_sources.push(PairPmxtSource {
            hour: hour_name,
            pmxt_parquet: HashedSource {
                path: pmxt_path.display().to_string(),
                sha256: pmxt_sha256,
            },
            target_condition_count: condition_ids.len(),
            decoded_target_events: events.len(),
        });
    }
    rows.sort_by(|left, right| {
        left.observed_at_ms
            .cmp(&right.observed_at_ms)
            .then_with(|| left.source_opportunity_id.cmp(&right.source_opportunity_id))
    });
    if rows.len() != opportunities.len() {
        bail!("paired-feature output lost source opportunity coordinates");
    }
    write_jsonl_atomic(&input.output_path, &rows)?;
    let both_books_observable_rows = rows
        .iter()
        .filter(|row| row.up_now.observable && row.down_now.observable)
        .count();
    let both_lookback_books_observable_rows = rows
        .iter()
        .filter(|row| row.up_lookback.observable && row.down_lookback.observable)
        .count();
    let manifest = OpportunityPairFeatureManifest {
        schema_version: OPPORTUNITY_PAIR_FEATURE_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        dataset_seal: HashedSource {
            path: input.dataset_seal_path.display().to_string(),
            sha256: dataset_seal_sha256,
        },
        dataset_sha256: seal.dataset_sha256,
        market_catalog: HashedSource {
            path: input.market_catalog_path.display().to_string(),
            sha256: market_catalog_sha256,
        },
        output: HashedSource {
            path: input.output_path.display().to_string(),
            sha256: sha256_file(&input.output_path)?,
        },
        source_opportunity_rows: opportunities.len(),
        output_rows: rows.len(),
        both_books_observable_rows,
        both_lookback_books_observable_rows,
        source_pmxt_scans: pmxt_sources.len(),
        source_pmxt_hours: pmxt_sources,
        lookback_ms: LOOKBACK_MS,
        outcome_columns_present: false,
        gamma_outcome_prices_influence_output: false,
        btc_or_model_features_influence_output: false,
        feature_semantics: "two complementary L2 books reconstructed causally at observed_at and observed_at-15s; source opportunity rows supply only immutable condition/timestamp/partition coordinates".to_string(),
    };
    write_json_artifact_atomic(&input.manifest_path, &manifest)?;
    Ok(manifest)
}

pub fn search_liquidity(
    input: OpportunityLiquiditySearchInput,
) -> Result<OpportunityLiquiditySearchReport> {
    validate_search_input(&input)?;
    let dataset_seal_sha256 = sha256_file(&input.dataset_seal_path)?;
    let (seal, opportunities) = load_sealed_opportunities(&input.dataset_seal_path)?;
    let opportunities_by_id = opportunities
        .iter()
        .map(|row| (row.opportunity_id.as_str(), row))
        .collect::<HashMap<_, _>>();

    let labels_manifest_sha256 = sha256_file(&input.labels_manifest_path)?;
    let labels_manifest: OpportunityLabelsManifest = serde_json::from_reader(
        File::open(&input.labels_manifest_path)
            .with_context(|| format!("open {}", input.labels_manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.labels_manifest_path.display()))?;
    if labels_manifest.dataset_seal.sha256 != dataset_seal_sha256
        || labels_manifest.dataset_sha256 != seal.dataset_sha256
        || labels_manifest.fresh_holdout_labels_present
    {
        bail!("liquidity search labels do not belong to the outcome-safe dataset");
    }
    let label_path = PathBuf::from(&labels_manifest.output.path);
    if sha256_file(&label_path)? != labels_manifest.output.sha256 {
        bail!("liquidity label table hash drifted");
    }
    let labels = read_labels(&label_path)?;
    let mut labels_by_id = HashMap::new();
    for label in &labels {
        let opportunity = opportunities_by_id
            .get(label.opportunity_id.as_str())
            .context("liquidity label references unknown opportunity")?;
        if opportunity.chronological_window == "fresh_holdout" {
            bail!("liquidity labels expose a fresh-holdout outcome");
        }
        if labels_by_id
            .insert(label.opportunity_id.as_str(), label)
            .is_some()
        {
            bail!("duplicate opportunity_id in liquidity labels");
        }
    }

    let pair_manifest_sha256 = sha256_file(&input.pair_features_manifest_path)?;
    let pair_manifest: OpportunityPairFeatureManifest = serde_json::from_reader(
        File::open(&input.pair_features_manifest_path)
            .with_context(|| format!("open {}", input.pair_features_manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.pair_features_manifest_path.display()))?;
    validate_pair_manifest(&pair_manifest, &seal.dataset_sha256, &dataset_seal_sha256)?;
    let pair_path = PathBuf::from(&pair_manifest.output.path);
    if sha256_file(&pair_path)? != pair_manifest.output.sha256 {
        bail!("paired-feature output hash drifted");
    }
    let pair_rows = read_pair_features(&pair_path)?;
    if pair_rows.len() != opportunities.len() || pair_rows.len() != pair_manifest.output_rows {
        bail!("paired features do not cover the complete sealed coordinate set");
    }
    let mut seen_ids = HashSet::new();
    for row in &pair_rows {
        let opportunity = opportunities_by_id
            .get(row.source_opportunity_id.as_str())
            .context("paired feature references unknown source opportunity")?;
        if !seen_ids.insert(row.source_opportunity_id.as_str())
            || row.condition_id != opportunity.condition_id
            || row.observed_at_ms != opportunity.observed_at_ms
            || row.chronological_window != opportunity.chronological_window
        {
            bail!("paired feature coordinate drifted from the sealed opportunity");
        }
    }

    let policies = liquidity_policy_grid();
    let policy_grid_sha256 = stable_json_hash(&policies);
    let mut evaluated = Vec::with_capacity(policies.len());
    for policy in policies {
        let mut accumulator = LiquidityAccumulator::default();
        for row in &pair_rows {
            let Some(selection) = select_liquidity(row, &policy) else {
                continue;
            };
            match row.chronological_window.as_str() {
                "older" => {
                    let Some(label) = labels_by_id.get(row.source_opportunity_id.as_str()) else {
                        continue;
                    };
                    let Some(won) =
                        selected_won(label.terminal_direction.as_str(), selection.direction)
                    else {
                        continue;
                    };
                    accumulator.calibration_support += 1;
                    accumulator.calibration_wins += usize::from(won);
                    accumulator.calibration_break_even_sum += selection
                        .book
                        .fee_aware_break_even_probability
                        .expect("selected book has break-even");
                }
                "recent_discovery" => {
                    let Some(label) = labels_by_id.get(row.source_opportunity_id.as_str()) else {
                        continue;
                    };
                    let Some(won) =
                        selected_won(label.terminal_direction.as_str(), selection.direction)
                    else {
                        continue;
                    };
                    accumulator
                        .discovery_ids
                        .push(row.source_opportunity_id.clone());
                    accumulator.discovery_tokens.insert(
                        row.source_opportunity_id.clone(),
                        selection.token_id.to_string(),
                    );
                    accumulator.discovery_wins += usize::from(won);
                    accumulator.discovery_losses += usize::from(!won);
                    accumulator.discovery_break_even_sum += selection
                        .book
                        .fee_aware_break_even_probability
                        .expect("selected book has break-even");
                    accumulator.discovery_pnl_usd += if won {
                        selection
                            .book
                            .fee_aware_net_win_usd
                            .expect("selected book has net-win economics")
                    } else {
                        -selection
                            .book
                            .fee_aware_max_loss_usd
                            .expect("selected book has max-loss economics")
                    };
                    accumulator.discovery_pressure_gap_sum += selection.pressure_gap;
                    accumulator.discovery_pressure_widening_sum += selection.pressure_widening;
                    accumulator.discovery_parity_residual_sum += selection.parity_residual;
                }
                "fresh_holdout" => accumulator.fresh_selections.push((
                    row.source_opportunity_id.clone(),
                    selection.token_id.to_string(),
                )),
                _ => bail!("paired feature has unsupported chronological partition"),
            }
        }
        evaluated.push(finish_liquidity_policy(policy, accumulator, &input));
    }

    let mut replay_groups = BTreeMap::<String, Vec<usize>>::new();
    for (index, finished) in evaluated.iter().enumerate() {
        if finished.result.discovery_eligible {
            replay_groups
                .entry(
                    finished
                        .result
                        .discovery_trace_sha256
                        .clone()
                        .expect("eligible policy has trace"),
                )
                .or_default()
                .push(index);
        }
    }
    let eligible_policy_count = replay_groups.values().map(Vec::len).sum::<usize>();
    let mut replay_entries = Vec::new();
    for (trace, indices) in replay_groups {
        let representative_index = *indices
            .iter()
            .min_by(|left, right| {
                liquidity_diagnostic_order(&evaluated[**left].result, &evaluated[**right].result)
            })
            .expect("non-empty liquidity replay group");
        let representative = &evaluated[representative_index];
        let mut equivalent_policy_ids = indices
            .iter()
            .map(|index| evaluated[*index].result.policy_id.clone())
            .collect::<Vec<_>>();
        equivalent_policy_ids.sort();
        replay_entries.push(ExactReplayPlanEntry {
            discovery_trace_sha256: trace,
            representative_policy_id: representative.result.policy_id.clone(),
            equivalent_policy_ids,
            decision_seconds: representative.result.policy.decision_seconds,
            opportunity_ids: representative.discovery_ids.clone(),
            token_overrides: representative.discovery_tokens.clone(),
            maximum_ask: MAXIMUM_EXECUTION_ASK,
            stake_usd: seal.stake_usd,
            fee_rate: seal.fee_rate,
            latency_ms: input.latency_ms,
            causal_feature_semantics_version: OPPORTUNITY_PAIR_FEATURE_SCHEMA_VERSION.to_string(),
        });
    }
    let results_by_policy = evaluated
        .iter()
        .map(|finished| (finished.result.policy_id.as_str(), &finished.result))
        .collect::<HashMap<_, _>>();
    replay_entries.sort_by(|left, right| {
        liquidity_diagnostic_order(
            results_by_policy[left.representative_policy_id.as_str()],
            results_by_policy[right.representative_policy_id.as_str()],
        )
    });
    let eligible_unique_trace_count = replay_entries.len();
    replay_entries.truncate(input.maximum_exact_replays);
    let unique_replay_count = replay_entries.len();
    let avoided_replay_count = eligible_policy_count.saturating_sub(unique_replay_count);
    let equivalence_reduction_fraction = (eligible_policy_count > 0)
        .then_some(avoided_replay_count as f64 / eligible_policy_count as f64);

    let mut diagnostics = evaluated
        .into_iter()
        .map(|finished| finished.result)
        .collect::<Vec<_>>();
    diagnostics.sort_by(liquidity_diagnostic_order);
    diagnostics.truncate(12);
    let calibration_rows = pair_rows
        .iter()
        .filter(|row| row.chronological_window == "older")
        .count();
    let discovery_rows = pair_rows
        .iter()
        .filter(|row| row.chronological_window == "recent_discovery")
        .count();
    let fresh_holdout_rows = pair_rows
        .iter()
        .filter(|row| row.chronological_window == "fresh_holdout")
        .count();
    let exact_replay_plan = ExactReplayPlan {
        status: if unique_replay_count > 0 {
            "exact_replay_plan_ready".to_string()
        } else {
            "no_policy_cleared_cheap_screen".to_string()
        },
        eligible_policy_count,
        eligible_unique_trace_count,
        maximum_replay_count: input.maximum_exact_replays,
        unique_replay_count,
        deferred_replay_count: eligible_unique_trace_count.saturating_sub(unique_replay_count),
        avoided_replay_count,
        equivalence_reduction_fraction,
        entries: replay_entries,
    };
    let report = OpportunityLiquiditySearchReport {
        schema_version: OPPORTUNITY_LIQUIDITY_SEARCH_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        family_id: "pure_cross_token_liquidity_dislocation_v1".to_string(),
        dataset_seal: HashedSource {
            path: input.dataset_seal_path.display().to_string(),
            sha256: dataset_seal_sha256,
        },
        labels_manifest: HashedSource {
            path: input.labels_manifest_path.display().to_string(),
            sha256: labels_manifest_sha256,
        },
        paired_features: HashedSource {
            path: input.pair_features_manifest_path.display().to_string(),
            sha256: pair_manifest_sha256,
        },
        dataset_sha256: seal.dataset_sha256,
        causal_feature_semantics_version: OPPORTUNITY_PAIR_FEATURE_SCHEMA_VERSION.to_string(),
        source_opportunity_table_reads: seal.entries.len(),
        source_paired_feature_reads: 1,
        in_memory_policy_evaluation_passes: 1,
        fresh_holdout_outcomes_accessed: false,
        selection_semantics: "choose Up or Down only from the sign of the cross-token top-depth pressure gap; require a bounded pair spread and a 15-second widening of that gap; no BTC path, volatility, model probability, outcome, score, or PnL enters selection".to_string(),
        policy_grid_sha256,
        policies_evaluated: liquidity_policy_grid().len(),
        minimum_calibration_support: input.minimum_calibration_support,
        minimum_policy_support: input.minimum_policy_support,
        safety_margin: input.safety_margin,
        calibration_semantics: "older outcomes test directional liquidity stability; no fitted probability model or learned threshold".to_string(),
        discovery_gate: "older support and positive point edge; recent support, positive fee-aware payoff proxy, and point edge above the fixed safety margin".to_string(),
        exact_replay_is_research_only: true,
        promotion_requires_wilson_after_exact_replay: true,
        calibration_rows,
        discovery_rows,
        fresh_holdout_rows,
        both_books_observable_rows: pair_manifest.both_books_observable_rows,
        eligible_policy_count,
        top_diagnostics: diagnostics,
        exact_replay_plan,
        verdict: if unique_replay_count > 0 {
            "bounded_exact_replay_required".to_string()
        } else {
            "family_rejected_by_cheap_screen".to_string()
        },
    };
    write_json_artifact_atomic(&input.output_path, &report)?;
    Ok(report)
}

pub fn decide_liquidity(
    input: OpportunityLiquidityDecisionInput,
) -> Result<OpportunityLiquidityDecisionReport> {
    if input.output_path == input.preregistration_path
        || input.output_path == input.liquidity_search_report_path
        || input
            .exact_replay_report_path
            .as_ref()
            .is_some_and(|path| path == &input.output_path)
    {
        bail!("liquidity-decision output must not replace an input");
    }
    let preregistration_sha256 = sha256_file(&input.preregistration_path)?;
    let preregistration: LiquidityPreregistration = serde_json::from_reader(
        File::open(&input.preregistration_path)
            .with_context(|| format!("open {}", input.preregistration_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.preregistration_path.display()))?;
    if preregistration.schema_version != "pure_cross_token_liquidity_dislocation_preregistration_v1"
        || preregistration.family_id != "pure_cross_token_liquidity_dislocation_v1"
        || preregistration.exact_replay_budget.maximum_unique_traces == 0
        || preregistration.exact_replay_budget.maximum_unique_traces > 2
        || preregistration
            .exact_replay_budget
            .additional_discovery_hours
            != 0
        || preregistration
            .exact_replay_budget
            .additional_parameter_variants
            != 0
        || (preregistration
            .advancement_gate
            .minimum_exact_replay_wilson_edge
            - 0.02)
            .abs()
            > 1e-12
        || !preregistration
            .advancement_gate
            .require_positive_exact_replay_pnl
    {
        bail!("invalid pure-liquidity family preregistration");
    }

    let search_sha256 = sha256_file(&input.liquidity_search_report_path)?;
    let search: OpportunityLiquiditySearchReport = serde_json::from_reader(
        File::open(&input.liquidity_search_report_path)
            .with_context(|| format!("open {}", input.liquidity_search_report_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.liquidity_search_report_path.display()))?;
    if search.schema_version != OPPORTUNITY_LIQUIDITY_SEARCH_SCHEMA_VERSION
        || search.family_id != preregistration.family_id
        || search.dataset_seal != preregistration.inputs.dataset_seal
        || search.labels_manifest != preregistration.inputs.labels_manifest
        || search.paired_features != preregistration.inputs.paired_features
        || search.policy_grid_sha256 != preregistration.policy_grid_sha256
        || search.policies_evaluated != preregistration.policies_evaluated
        || search.minimum_calibration_support
            != preregistration.cheap_screen.minimum_calibration_support
        || search.minimum_policy_support != preregistration.cheap_screen.minimum_policy_support
        || (search.safety_margin - preregistration.cheap_screen.safety_margin).abs() > 1e-12
        || search.fresh_holdout_outcomes_accessed
        || !search.exact_replay_is_research_only
        || !search.promotion_requires_wilson_after_exact_replay
        || search.exact_replay_plan.maximum_replay_count
            != preregistration.exact_replay_budget.maximum_unique_traces
        || search.exact_replay_plan.entries.len() != search.exact_replay_plan.unique_replay_count
        || search.exact_replay_plan.entries.iter().any(|entry| {
            entry.latency_ms != preregistration.exact_replay_budget.latency_ms
                || (entry.stake_usd - preregistration.exact_replay_budget.stake_usd).abs() > 1e-12
                || (entry.fee_rate - preregistration.exact_replay_budget.fee_rate).abs() > 1e-12
        })
    {
        bail!("liquidity search report drifted from preregistration");
    }

    let mut exact_replay_source = None;
    let mut trace_decisions = Vec::new();
    if search.exact_replay_plan.entries.is_empty() {
        if input.exact_replay_report_path.is_some() {
            bail!("cheap-screen rejection must not consume exact replay");
        }
    } else {
        let exact_path = input
            .exact_replay_report_path
            .as_ref()
            .context("eligible liquidity search requires its bounded exact replay")?;
        let exact_sha256 = sha256_file(exact_path)?;
        let exact: OpportunityExactReplayReport = serde_json::from_reader(
            File::open(exact_path).with_context(|| format!("open {}", exact_path.display()))?,
        )
        .with_context(|| format!("parse {}", exact_path.display()))?;
        if exact.policy_search_report.sha256 != search_sha256
            || exact.dataset_seal != search.dataset_seal
            || exact.labels_manifest != search.labels_manifest
            || exact.fresh_holdout_outcomes_accessed
            || exact.traces.len() != search.exact_replay_plan.entries.len()
        {
            bail!("liquidity exact replay does not match the bounded search plan");
        }
        let expected_traces = search
            .exact_replay_plan
            .entries
            .iter()
            .map(|entry| entry.discovery_trace_sha256.as_str())
            .collect::<HashSet<_>>();
        if exact
            .traces
            .iter()
            .any(|trace| !expected_traces.contains(trace.discovery_trace_sha256.as_str()))
        {
            bail!("liquidity exact replay contains an unplanned trace");
        }
        trace_decisions = exact
            .traces
            .iter()
            .map(|trace| {
                let advances = trace.wilson_edge.is_some_and(|edge| {
                    edge > preregistration
                        .advancement_gate
                        .minimum_exact_replay_wilson_edge
                }) && trace.total_pnl_usd > 0.0;
                LiquidityTraceDecision {
                    discovery_trace_sha256: trace.discovery_trace_sha256.clone(),
                    representative_policy_id: trace.representative_policy_id.clone(),
                    decision_seconds: trace.decision_seconds,
                    fills: trace.fills,
                    wins: trace.wins,
                    losses: trace.losses,
                    point_estimate_edge: trace.point_estimate_edge,
                    wilson_edge: trace.wilson_edge,
                    total_pnl_usd: trace.total_pnl_usd,
                    decision: if advances {
                        "open_fresh_gate"
                    } else {
                        "reject_trace"
                    }
                    .to_string(),
                }
            })
            .collect();
        exact_replay_source = Some(HashedSource {
            path: exact_path.display().to_string(),
            sha256: exact_sha256,
        });
    }
    let fresh_gate_opened = trace_decisions
        .iter()
        .any(|trace| trace.decision == "open_fresh_gate");
    let (decision, reason) = if fresh_gate_opened {
        (
            "advance_to_fresh_gate",
            "at least one preregistered liquidity trace cleared the fixed exact-replay Wilson edge and positive-PnL gate",
        )
    } else if search.exact_replay_plan.entries.is_empty() {
        (
            "reject_family_keep_fresh_sealed",
            "no fixed-grid liquidity policy cleared the older/recent cheap screen",
        )
    } else {
        (
            "reject_family_keep_fresh_sealed",
            "no bounded exact-replay liquidity trace cleared the fixed Wilson edge and positive-PnL gate",
        )
    };
    let report = OpportunityLiquidityDecisionReport {
        schema_version: "opportunity_liquidity_decision_v1".to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        family_id: preregistration.family_id,
        preregistration: HashedSource {
            path: input.preregistration_path.display().to_string(),
            sha256: preregistration_sha256,
        },
        liquidity_search_report: HashedSource {
            path: input.liquidity_search_report_path.display().to_string(),
            sha256: search_sha256,
        },
        exact_replay_report: exact_replay_source,
        fixed_advancement_wilson_edge: preregistration
            .advancement_gate
            .minimum_exact_replay_wilson_edge,
        maximum_exact_replays: preregistration.exact_replay_budget.maximum_unique_traces,
        exact_replays_consumed: trace_decisions.len(),
        fresh_holdout_outcomes_accessed: false,
        trace_decisions,
        decision: decision.to_string(),
        reason: reason.to_string(),
        terminal: true,
        search_budget_exhausted: true,
        more_evidence_allowed: false,
        fresh_gate_opened,
    };
    write_json_artifact_atomic(&input.output_path, &report)?;
    Ok(report)
}

fn validate_search_input(input: &OpportunityLiquiditySearchInput) -> Result<()> {
    if input.minimum_calibration_support == 0
        || input.minimum_policy_support == 0
        || input.maximum_exact_replays == 0
        || input.maximum_exact_replays > 2
    {
        bail!("liquidity supports must be positive and exact replay budget must be 1 or 2");
    }
    if !input.safety_margin.is_finite() || input.safety_margin < 0.0 {
        bail!("liquidity safety margin must be finite and non-negative");
    }
    if [
        &input.dataset_seal_path,
        &input.labels_manifest_path,
        &input.pair_features_manifest_path,
    ]
    .contains(&&input.output_path)
    {
        bail!("liquidity-search output must not replace an input");
    }
    Ok(())
}

fn validate_pair_manifest(
    manifest: &OpportunityPairFeatureManifest,
    dataset_sha256: &str,
    dataset_seal_sha256: &str,
) -> Result<()> {
    if manifest.schema_version != OPPORTUNITY_PAIR_FEATURE_SCHEMA_VERSION
        || manifest.dataset_sha256 != dataset_sha256
        || manifest.dataset_seal.sha256 != dataset_seal_sha256
        || manifest.lookback_ms != LOOKBACK_MS
        || manifest.outcome_columns_present
        || manifest.gamma_outcome_prices_influence_output
        || manifest.btc_or_model_features_influence_output
        || manifest.source_pmxt_scans != manifest.source_pmxt_hours.len()
    {
        bail!("paired-feature manifest violates the liquidity-search contract");
    }
    Ok(())
}

fn liquidity_policy_grid() -> Vec<LiquidityPolicyDefinition> {
    let mut policies = Vec::new();
    for decision_seconds in [120, 180, 240] {
        for minimum_pressure_gap in [0.50, 1.00, 1.50] {
            for minimum_pressure_gap_widening_15s in [0.00, 0.25, 0.50] {
                for maximum_pair_spread in [0.01, 0.02] {
                    policies.push(LiquidityPolicyDefinition {
                        decision_seconds,
                        minimum_pressure_gap,
                        minimum_pressure_gap_widening_15s,
                        maximum_pair_spread,
                    });
                }
            }
        }
    }
    policies
}

fn select_liquidity<'a>(
    row: &'a OpportunityPairFeature,
    policy: &LiquidityPolicyDefinition,
) -> Option<LiquiditySelection<'a>> {
    if row.decision_seconds != policy.decision_seconds
        || !row.up_now.observable
        || !row.down_now.observable
        || !row.up_lookback.observable
        || !row.down_lookback.observable
    {
        return None;
    }
    let up_pressure = row.up_now.top_book_pressure?;
    let down_pressure = row.down_now.top_book_pressure?;
    let prior_gap =
        (row.up_lookback.top_book_pressure? - row.down_lookback.top_book_pressure?).abs();
    let signed_gap = up_pressure - down_pressure;
    let pressure_gap = signed_gap.abs();
    let pressure_widening = pressure_gap - prior_gap;
    if pressure_gap + 1e-12 < policy.minimum_pressure_gap
        || pressure_widening + 1e-12 < policy.minimum_pressure_gap_widening_15s
        || row.up_now.spread?.max(row.down_now.spread?) - 1e-12 > policy.maximum_pair_spread
    {
        return None;
    }
    let (token_id, direction, book) = if signed_gap > 0.0 {
        (row.up_token_id.as_str(), "up", &row.up_now)
    } else if signed_gap < 0.0 {
        (row.down_token_id.as_str(), "down", &row.down_now)
    } else {
        return None;
    };
    if !book.stake_fully_executable
        || book.best_ask? > MAXIMUM_EXECUTION_ASK
        || book.fee_aware_break_even_probability.is_none()
        || book.fee_aware_net_win_usd.is_none()
        || book.fee_aware_max_loss_usd.is_none()
    {
        return None;
    }
    Some(LiquiditySelection {
        token_id,
        direction,
        book,
        pressure_gap,
        pressure_widening,
        parity_residual: (row.up_now.midpoint? + row.down_now.midpoint? - 1.0).abs(),
    })
}

fn selected_won(terminal_direction: &str, selected_direction: &str) -> Option<bool> {
    match terminal_direction {
        "up" | "down" => Some(terminal_direction == selected_direction),
        "tie" => None,
        _ => None,
    }
}

fn finish_liquidity_policy(
    policy: LiquidityPolicyDefinition,
    accumulator: LiquidityAccumulator,
    input: &OpportunityLiquiditySearchInput,
) -> FinishedLiquidityPolicy {
    let calibration_win_rate = ratio(
        accumulator.calibration_wins,
        accumulator.calibration_support,
    );
    let calibration_average_break_even_probability = average(
        accumulator.calibration_break_even_sum,
        accumulator.calibration_support,
    );
    let calibration_point_estimate_edge = calibration_win_rate
        .zip(calibration_average_break_even_probability)
        .map(|(win_rate, break_even)| win_rate - break_even);
    let discovery_support = accumulator.discovery_ids.len();
    let win_rate = ratio(accumulator.discovery_wins, discovery_support);
    let wilson_win_rate_lower = (discovery_support > 0)
        .then_some(wilson_lower(accumulator.discovery_wins, discovery_support));
    let average_break_even_probability =
        average(accumulator.discovery_break_even_sum, discovery_support);
    let point_estimate_edge = win_rate
        .zip(average_break_even_probability)
        .map(|(win_rate, break_even)| win_rate - break_even);
    let wilson_edge = wilson_win_rate_lower
        .zip(average_break_even_probability)
        .map(|(lower, break_even)| lower - break_even);
    let mut rejection_reasons = Vec::new();
    if accumulator.calibration_support < input.minimum_calibration_support {
        rejection_reasons.push("insufficient_older_support".to_string());
    }
    if !calibration_point_estimate_edge.is_some_and(|edge| edge > 0.0) {
        rejection_reasons.push("nonpositive_older_point_edge".to_string());
    }
    if discovery_support < input.minimum_policy_support {
        rejection_reasons.push("insufficient_recent_support".to_string());
    }
    if !point_estimate_edge.is_some_and(|edge| edge > input.safety_margin) {
        rejection_reasons.push("recent_point_edge_below_safety_margin".to_string());
    }
    if accumulator.discovery_pnl_usd <= 0.0 {
        rejection_reasons.push("nonpositive_recent_payoff_proxy".to_string());
    }
    let discovery_eligible = rejection_reasons.is_empty();
    let discovery_trace_sha256 = (!accumulator.discovery_ids.is_empty()).then(|| {
        stable_json_hash(&serde_json::json!({
            "opportunity_ids": accumulator.discovery_ids,
            "token_overrides": accumulator.discovery_tokens,
        }))
    });
    let fresh_support_trace_sha256 = (!accumulator.fresh_selections.is_empty())
        .then(|| stable_json_hash(&accumulator.fresh_selections));
    let policy_id = stable_json_hash(&policy);
    FinishedLiquidityPolicy {
        result: LiquidityPolicyResult {
            policy_id,
            policy,
            calibration_support: accumulator.calibration_support,
            calibration_wins: accumulator.calibration_wins,
            calibration_win_rate,
            calibration_average_break_even_probability,
            calibration_point_estimate_edge,
            discovery_support,
            wins: accumulator.discovery_wins,
            losses: accumulator.discovery_losses,
            win_rate,
            wilson_win_rate_lower,
            average_break_even_probability,
            point_estimate_edge,
            wilson_edge,
            economic_payoff_proxy_usd: accumulator.discovery_pnl_usd,
            average_pressure_gap: average(
                accumulator.discovery_pressure_gap_sum,
                discovery_support,
            ),
            average_pressure_gap_widening_15s: average(
                accumulator.discovery_pressure_widening_sum,
                discovery_support,
            ),
            average_absolute_midpoint_parity_residual: average(
                accumulator.discovery_parity_residual_sum,
                discovery_support,
            ),
            fresh_causal_support: accumulator.fresh_selections.len(),
            discovery_trace_sha256,
            fresh_support_trace_sha256,
            discovery_eligible,
            rejection_reasons,
        },
        discovery_ids: accumulator.discovery_ids,
        discovery_tokens: accumulator.discovery_tokens,
    }
}

fn liquidity_diagnostic_order(
    left: &LiquidityPolicyResult,
    right: &LiquidityPolicyResult,
) -> Ordering {
    right
        .discovery_eligible
        .cmp(&left.discovery_eligible)
        .then_with(|| compare_optional_desc(left.wilson_edge, right.wilson_edge))
        .then_with(|| {
            right
                .economic_payoff_proxy_usd
                .total_cmp(&left.economic_payoff_proxy_usd)
        })
        .then_with(|| right.discovery_support.cmp(&left.discovery_support))
        .then_with(|| left.policy_id.cmp(&right.policy_id))
}

fn compare_optional_desc(left: Option<f64>, right: Option<f64>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => right.total_cmp(&left),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}

fn average(sum: f64, count: usize) -> Option<f64> {
    (count > 0).then_some(sum / count as f64)
}

fn wilson_lower(wins: usize, support: usize) -> f64 {
    if support == 0 {
        return 0.0;
    }
    let z = 1.959_963_984_540_054;
    let n = support as f64;
    let probability = wins as f64 / n;
    let denominator = 1.0 + z * z / n;
    let centre = probability + z * z / (2.0 * n);
    let radius = z * ((probability * (1.0 - probability) + z * z / (4.0 * n)) / n).sqrt();
    ((centre - radius) / denominator).clamp(0.0, 1.0)
}

fn validate_feature_input(input: &OpportunityPairFeatureInput) -> Result<()> {
    if !input.cache_dir.is_dir() {
        bail!("paired features require an existing PMXT cache directory");
    }
    let outputs = [&input.output_path, &input.manifest_path];
    if outputs[0] == outputs[1]
        || outputs
            .iter()
            .any(|path| *path == &input.dataset_seal_path || *path == &input.market_catalog_path)
    {
        bail!("paired-feature outputs must not replace inputs or each other");
    }
    if input
        .output_path
        .extension()
        .and_then(|value| value.to_str())
        != Some("jsonl")
    {
        bail!("paired-feature output path must use the .jsonl extension");
    }
    Ok(())
}

fn load_source_manifests(
    seal: &super::opportunity_dataset::OpportunityDatasetSeal,
) -> Result<HashMap<String, OpportunityTableManifest>> {
    let mut manifests = HashMap::new();
    for entry in &seal.entries {
        let path = PathBuf::from(&entry.manifest.path);
        let manifest: OpportunityTableManifest = serde_json::from_reader(
            File::open(&path).with_context(|| format!("open {}", path.display()))?,
        )
        .with_context(|| format!("parse {}", path.display()))?;
        manifests.insert(entry.hour.clone(), manifest);
    }
    Ok(manifests)
}

fn load_pair_identities(path: &Path) -> Result<HashMap<String, PairIdentity>> {
    let catalog: BTreeMap<String, Market> = serde_json::from_reader(
        File::open(path).with_context(|| format!("open market catalog {}", path.display()))?,
    )
    .with_context(|| format!("parse market catalog {}", path.display()))?;
    let mut identities = HashMap::new();
    for (key, market) in catalog {
        if key != market.condition_id {
            bail!("market catalog key does not match condition_id");
        }
        let mut up = None;
        let mut down = None;
        for outcome in market.outcomes {
            if (outcome.price - 0.5).abs() > 1e-12 {
                bail!("paired features require an outcome-price-neutralized market catalog");
            }
            match outcome.name.to_ascii_lowercase().as_str() {
                "up" => up = Some(outcome.token_id),
                "down" => down = Some(outcome.token_id),
                _ => {}
            }
        }
        if let (Some(up_token_id), Some(down_token_id)) = (up, down) {
            identities.insert(
                market.condition_id,
                PairIdentity {
                    up_token_id,
                    down_token_id,
                },
            );
        }
    }
    if identities.is_empty() {
        bail!("market catalog contains no Up/Down token pairs");
    }
    Ok(identities)
}

fn build_hour_rows(
    opportunities: Vec<CausalOpportunity>,
    identities: &HashMap<String, PairIdentity>,
    events: &[crate::backtest::pmxt::L2Event],
    stake_usd: f64,
    fee_rate: f64,
) -> Result<Vec<OpportunityPairFeature>> {
    let mut partials = opportunities
        .into_iter()
        .map(|opportunity| -> Result<_> {
            let identity = identities
                .get(&opportunity.condition_id)
                .with_context(|| format!("catalog missing pair for {}", opportunity.condition_id))?
                .clone();
            if opportunity.token_id != identity.up_token_id
                && opportunity.token_id != identity.down_token_id
            {
                bail!("source opportunity token is outside its catalog pair");
            }
            Ok(PartialPairFeature {
                opportunity,
                identity,
                up_lookback: None,
                down_lookback: None,
                up_now: None,
                down_now: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut targets = partials
        .iter()
        .enumerate()
        .flat_map(|(row_index, row)| {
            [
                MeasurementTarget {
                    row_index,
                    timestamp_ms: row.opportunity.observed_at_ms - LOOKBACK_MS,
                    kind: TargetKind::Lookback,
                },
                MeasurementTarget {
                    row_index,
                    timestamp_ms: row.opportunity.observed_at_ms,
                    kind: TargetKind::Now,
                },
            ]
        })
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        left.timestamp_ms
            .cmp(&right.timestamp_ms)
            .then_with(|| left.row_index.cmp(&right.row_index))
    });

    let mut event_index = 0usize;
    let mut books = HashMap::<String, TokenBook>::new();
    for target in targets {
        let target_s = target.timestamp_ms as f64 / 1_000.0;
        while event_index < events.len() && events[event_index].timestamp_s <= target_s {
            match &events[event_index].body {
                L2EventBody::BookSnapshot(snapshot) => books
                    .entry(snapshot.token_id.clone())
                    .or_default()
                    .apply_snapshot(snapshot),
                L2EventBody::PriceChange(change) => books
                    .entry(change.token_id.clone())
                    .or_default()
                    .apply_change(change),
            }
            event_index += 1;
        }
        let row = &mut partials[target.row_index];
        let up = measure_book(
            books.get(&row.identity.up_token_id),
            target.timestamp_ms,
            stake_usd,
            fee_rate,
        );
        let down = measure_book(
            books.get(&row.identity.down_token_id),
            target.timestamp_ms,
            stake_usd,
            fee_rate,
        );
        match target.kind {
            TargetKind::Lookback => {
                row.up_lookback = Some(up);
                row.down_lookback = Some(down);
            }
            TargetKind::Now => {
                row.up_now = Some(up);
                row.down_now = Some(down);
            }
        }
    }

    partials
        .into_iter()
        .map(|row| {
            let elapsed_ms = row
                .opportunity
                .observed_at_ms
                .saturating_sub(row.opportunity.window_start_ms);
            let decision_seconds =
                u16::try_from(elapsed_ms / 1_000).context("decision offset exceeds u16")?;
            Ok(OpportunityPairFeature {
                source_opportunity_id: row.opportunity.opportunity_id,
                condition_id: row.opportunity.condition_id,
                chronological_window: row.opportunity.chronological_window,
                window_start_ms: row.opportunity.window_start_ms,
                observed_at_ms: row.opportunity.observed_at_ms,
                decision_seconds,
                up_token_id: row.identity.up_token_id,
                down_token_id: row.identity.down_token_id,
                up_lookback: row.up_lookback.context("missing Up lookback measurement")?,
                down_lookback: row
                    .down_lookback
                    .context("missing Down lookback measurement")?,
                up_now: row.up_now.context("missing Up current measurement")?,
                down_now: row.down_now.context("missing Down current measurement")?,
            })
        })
        .collect()
}

fn measure_book(
    book: Option<&TokenBook>,
    observed_at_ms: i64,
    stake_usd: f64,
    fee_rate: f64,
) -> PairBookFeature {
    let empty = || PairBookFeature {
        observable: false,
        book_timestamp_ms: None,
        book_age_ms: None,
        best_bid: None,
        best_ask: None,
        midpoint: None,
        spread: None,
        top_bid_depth_shares: None,
        top_ask_depth_shares: None,
        top_book_pressure: None,
        stake_fully_executable: false,
        fee_aware_break_even_probability: None,
        fee_aware_net_win_usd: None,
        fee_aware_max_loss_usd: None,
    };
    let Some(book) = book else { return empty() };
    if !(book.best_bid > 0.0
        && book.best_bid < 1.0
        && book.best_ask > 0.0
        && book.best_ask < 1.0
        && book.best_bid <= book.best_ask)
    {
        return empty();
    }
    let bids = book.bid_levels();
    let asks = book.ask_levels();
    let (Some((_, bid_depth)), Some((_, ask_depth))) = (bids.first(), asks.first()) else {
        return empty();
    };
    let bid_depth = *bid_depth;
    let ask_depth = *ask_depth;
    if bid_depth + ask_depth <= 0.0 {
        return empty();
    }
    let mut remaining = stake_usd;
    let mut cost = 0.0;
    let mut shares = 0.0;
    let mut fee = 0.0;
    for (price, size) in asks {
        if remaining <= 1e-9 {
            break;
        }
        let fill_cost = remaining.min(price * size);
        let fill_shares = fill_cost / price;
        cost += fill_cost;
        shares += fill_shares;
        fee += polymarket_fee(fill_shares, price, fee_rate);
        remaining -= fill_cost;
    }
    let break_even = (shares > 0.0).then_some((cost + fee) / shares);
    let net_win = (shares > 0.0).then_some(shares - cost - fee);
    let max_loss = (shares > 0.0).then_some(cost + fee);
    let timestamp_ms = (book.last_update_ts_s * 1_000.0).round() as i64;
    PairBookFeature {
        observable: true,
        book_timestamp_ms: Some(timestamp_ms),
        book_age_ms: Some(observed_at_ms.saturating_sub(timestamp_ms)),
        best_bid: Some(book.best_bid),
        best_ask: Some(book.best_ask),
        midpoint: Some((book.best_bid + book.best_ask) / 2.0),
        spread: Some(book.best_ask - book.best_bid),
        top_bid_depth_shares: Some(bid_depth),
        top_ask_depth_shares: Some(ask_depth),
        top_book_pressure: Some((bid_depth - ask_depth) / (bid_depth + ask_depth)),
        stake_fully_executable: remaining <= 1e-9,
        fee_aware_break_even_probability: break_even,
        fee_aware_net_win_usd: net_win,
        fee_aware_max_loss_usd: max_loss,
    }
}

pub(crate) fn read_pair_features(path: &Path) -> Result<Vec<OpportunityPairFeature>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read paired features {}", path.display()))?;
    let mut rows = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            bail!("paired-feature line {} is blank", index + 1);
        }
        rows.push(
            serde_json::from_str(line)
                .with_context(|| format!("parse paired-feature line {}", index + 1))?,
        );
    }
    if rows.is_empty() {
        bail!("paired-feature file contains no rows");
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::pmxt::{BookSnapshot, L2Level};

    #[test]
    fn paired_book_measurement_is_pressure_and_fee_aware() {
        let mut book = TokenBook::default();
        book.apply_snapshot(&BookSnapshot {
            market_id: "condition".to_string(),
            token_id: "up".to_string(),
            best_bid: 0.49,
            best_ask: 0.51,
            timestamp_s: 10.0,
            bids: vec![L2Level {
                price: 0.49,
                size: 30.0,
            }],
            asks: vec![L2Level {
                price: 0.51,
                size: 10.0,
            }],
        });
        let measured = measure_book(Some(&book), 10_128, 5.0, 0.07);
        assert!(measured.observable);
        assert_eq!(measured.book_age_ms, Some(128));
        assert_eq!(measured.top_book_pressure, Some(0.5));
        assert!(measured.stake_fully_executable);
        assert!(measured.fee_aware_break_even_probability.unwrap() > 0.51);
    }

    fn feature(pressure: f64, spread: f64) -> PairBookFeature {
        PairBookFeature {
            observable: true,
            book_timestamp_ms: Some(1),
            book_age_ms: Some(0),
            best_bid: Some(0.49),
            best_ask: Some(0.49 + spread),
            midpoint: Some(0.50),
            spread: Some(spread),
            top_bid_depth_shares: Some(10.0),
            top_ask_depth_shares: Some(10.0),
            top_book_pressure: Some(pressure),
            stake_fully_executable: true,
            fee_aware_break_even_probability: Some(0.51),
            fee_aware_net_win_usd: Some(4.5),
            fee_aware_max_loss_usd: Some(5.0),
        }
    }

    #[test]
    fn liquidity_selection_uses_pair_pressure_not_source_token() {
        let row = OpportunityPairFeature {
            source_opportunity_id: "source".to_string(),
            condition_id: "condition".to_string(),
            chronological_window: "recent_discovery".to_string(),
            window_start_ms: 0,
            observed_at_ms: 120_000,
            decision_seconds: 120,
            up_token_id: "up".to_string(),
            down_token_id: "down".to_string(),
            up_lookback: feature(0.1, 0.01),
            down_lookback: feature(0.0, 0.01),
            up_now: feature(-0.4, 0.01),
            down_now: feature(0.5, 0.01),
        };
        let policy = LiquidityPolicyDefinition {
            decision_seconds: 120,
            minimum_pressure_gap: 0.75,
            minimum_pressure_gap_widening_15s: 0.25,
            maximum_pair_spread: 0.02,
        };
        let selected = select_liquidity(&row, &policy).expect("liquidity selection");
        assert_eq!(selected.direction, "down");
        assert_eq!(selected.token_id, "down");
    }
}
