//! Outcome-free trade-tape directional-flow strategy family.
//!
//! The feature builder streams PMXT trade prints and quote-change arrivals at
//! sealed observation coordinates. Search direction is determined only from
//! aggressor-side trade notional across the complementary Up/Down tokens;
//! terminal labels are joined later and fresh outcomes stay sealed.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::artifact::{write_json_artifact_atomic, write_jsonl_atomic};
use crate::backtest::pmxt::{MarketFlowEvent, MarketFlowEventKind, PMXTv2Loader};
use crate::execution::fees::polymarket_fee;
use crate::strategy::spec::stable_json_hash;

use super::opportunity_dataset::{
    load_sealed_opportunities, read_labels, sha256_file, CausalOpportunity, OpportunityDatasetSeal,
    OpportunityLabelsManifest,
};
use super::opportunity_feature_store::{
    load_outcome_neutral_pairs, read_feature_store_rows, validate_outcome_free_manifest,
    FeaturePluginDescriptor, FeatureStorePmxtSource, OpportunityFeatureStoreManifest,
    OpportunityFeatureStoreRow, OutcomeNeutralPair, OPPORTUNITY_FEATURE_STORE_SCHEMA_VERSION,
};
use super::opportunity_policy::{ExactReplayPlan, ExactReplayPlanEntry};
use super::opportunity_replay::OpportunityExactReplayReport;
use super::opportunity_table::{HashedSource, OpportunityTableManifest};

pub const ORDER_FLOW_PLUGIN_ID: &str = "pmxt_trade_tape_directional_flow";
pub const ORDER_FLOW_PLUGIN_VERSION: &str = "v1";
pub const OPPORTUNITY_FLOW_SEARCH_SCHEMA_VERSION: &str = "opportunity_flow_search_v1";
const WINDOW_15S_MS: i64 = 15_000;
const WINDOW_30S_MS: i64 = 30_000;
const MAXIMUM_EXECUTION_ASK: f64 = 0.99;
const MAXIMUM_PAIR_SPREAD: f64 = 0.02;

pub type OrderFlowFeatureRow = OpportunityFeatureStoreRow<OrderFlowPairFeatures>;

#[derive(Debug, Clone)]
pub struct OrderFlowFeatureInput {
    pub dataset_seal_path: PathBuf,
    pub market_catalog_path: PathBuf,
    pub cache_dir: PathBuf,
    pub output_path: PathBuf,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct OrderFlowSearchInput {
    pub dataset_seal_path: PathBuf,
    pub labels_manifest_path: PathBuf,
    pub feature_store_manifest_path: PathBuf,
    pub output_path: PathBuf,
    pub minimum_calibration_support: usize,
    pub minimum_policy_support: usize,
    pub safety_margin: f64,
    pub latency_ms: u64,
    pub maximum_exact_replays: usize,
}

#[derive(Debug, Clone)]
pub struct OrderFlowDecisionInput {
    pub preregistration_path: PathBuf,
    pub flow_search_report_path: PathBuf,
    pub exact_replay_report_path: Option<PathBuf>,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OrderFlowWindowFeatures {
    pub quote_change_events: usize,
    pub bid_change_events: usize,
    pub ask_change_events: usize,
    pub unique_trade_prints: usize,
    pub buy_trade_prints: usize,
    pub sell_trade_prints: usize,
    pub buy_trade_notional_usd: f64,
    pub sell_trade_notional_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TokenOrderFlowFeatures {
    pub quote_observable: bool,
    pub quote_timestamp_ms: Option<i64>,
    pub quote_age_ms: Option<i64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
    pub last_trade_age_ms: Option<i64>,
    pub window_15s: OrderFlowWindowFeatures,
    pub window_30s: OrderFlowWindowFeatures,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OrderFlowPairFeatures {
    pub up: TokenOrderFlowFeatures,
    pub down: TokenOrderFlowFeatures,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderFlowPolicyDefinition {
    pub decision_seconds: u16,
    pub lookback_seconds: u16,
    pub minimum_pair_trade_prints: usize,
    pub minimum_absolute_trade_imbalance: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderFlowPolicyResult {
    pub policy_id: String,
    pub policy: OrderFlowPolicyDefinition,
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
    pub average_absolute_trade_imbalance: Option<f64>,
    pub average_pair_trade_prints: Option<f64>,
    pub fresh_causal_support: usize,
    pub discovery_trace_sha256: Option<String>,
    pub fresh_support_trace_sha256: Option<String>,
    pub discovery_eligible: bool,
    pub rejection_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityFlowSearchReport {
    pub schema_version: String,
    pub generated_at: String,
    pub family_id: String,
    pub dataset_seal: HashedSource,
    pub labels_manifest: HashedSource,
    pub feature_store: HashedSource,
    pub dataset_sha256: String,
    pub causal_feature_semantics_version: String,
    pub source_opportunity_table_reads: usize,
    pub source_feature_store_reads: usize,
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
    pub complete_pair_rows: usize,
    pub eligible_policy_count: usize,
    pub top_diagnostics: Vec<OrderFlowPolicyResult>,
    pub exact_replay_plan: ExactReplayPlan,
    pub verdict: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderFlowTraceDecision {
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
pub struct OpportunityFlowDecisionReport {
    pub schema_version: String,
    pub generated_at: String,
    pub family_id: String,
    pub preregistration: HashedSource,
    pub flow_search_report: HashedSource,
    pub exact_replay_report: Option<HashedSource>,
    pub fixed_advancement_wilson_edge: f64,
    pub maximum_exact_replays: usize,
    pub exact_replays_consumed: usize,
    pub fresh_holdout_outcomes_accessed: bool,
    pub trace_decisions: Vec<OrderFlowTraceDecision>,
    pub decision: String,
    pub reason: String,
    pub terminal: bool,
    pub search_budget_exhausted: bool,
    pub more_evidence_allowed: bool,
    pub fresh_gate_opened: bool,
}

#[derive(Debug, Clone, Serialize)]
struct OrderFlowPluginConfiguration {
    causal_windows_ms: [i64; 2],
    trade_deduplication: &'static str,
    quote_source: &'static str,
    trade_side_semantics: &'static str,
}

#[derive(Debug, Default)]
struct WindowAccumulator {
    quote_change_events: usize,
    bid_change_events: usize,
    ask_change_events: usize,
    trade_keys: HashSet<String>,
    buy_trade_prints: usize,
    sell_trade_prints: usize,
    buy_trade_notional_usd: f64,
    sell_trade_notional_usd: f64,
}

#[derive(Debug, Default)]
struct TokenFlowAccumulator {
    quote_timestamp_ms: Option<i64>,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    last_trade_timestamp_ms: Option<i64>,
    window_15s: WindowAccumulator,
    window_30s: WindowAccumulator,
}

#[derive(Debug)]
struct PartialFlowRow {
    opportunity: CausalOpportunity,
    pair: OutcomeNeutralPair,
    up: TokenFlowAccumulator,
    down: TokenFlowAccumulator,
}

type PreparedHourRows = (Vec<PartialFlowRow>, HashMap<String, Vec<usize>>);

#[derive(Debug, Default)]
struct PolicyAccumulator {
    calibration_support: usize,
    calibration_wins: usize,
    calibration_break_even_sum: f64,
    discovery_ids: Vec<String>,
    discovery_tokens: BTreeMap<String, String>,
    discovery_wins: usize,
    discovery_losses: usize,
    discovery_break_even_sum: f64,
    discovery_pnl_usd: f64,
    discovery_imbalance_sum: f64,
    discovery_trade_count_sum: usize,
    fresh_selections: Vec<(String, String)>,
}

#[derive(Debug)]
struct FinishedPolicy {
    result: OrderFlowPolicyResult,
    discovery_ids: Vec<String>,
    discovery_tokens: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
struct FlowSelection<'a> {
    token_id: &'a str,
    direction: &'static str,
    best_ask: f64,
    trade_imbalance: f64,
    pair_trade_prints: usize,
}

#[derive(Debug, Deserialize)]
struct FlowPreregistration {
    schema_version: String,
    family_id: String,
    inputs: FlowPreregisteredInputs,
    policy_grid_sha256: String,
    policies_evaluated: usize,
    cheap_screen: FlowCheapScreen,
    exact_replay_budget: FlowReplayBudget,
    advancement_gate: FlowAdvancementGate,
}

#[derive(Debug, Deserialize)]
struct FlowPreregisteredInputs {
    dataset_seal: HashedSource,
    labels_manifest: HashedSource,
    feature_store: HashedSource,
}

#[derive(Debug, Deserialize)]
struct FlowCheapScreen {
    minimum_calibration_support: usize,
    minimum_policy_support: usize,
    safety_margin: f64,
}

#[derive(Debug, Deserialize)]
struct FlowReplayBudget {
    maximum_unique_traces: usize,
    latency_ms: u64,
    stake_usd: f64,
    fee_rate: f64,
    additional_discovery_hours: usize,
    additional_parameter_variants: usize,
}

#[derive(Debug, Deserialize)]
struct FlowAdvancementGate {
    minimum_exact_replay_wilson_edge: f64,
    require_positive_exact_replay_pnl: bool,
}

pub fn create_feature_store(
    input: OrderFlowFeatureInput,
) -> Result<OpportunityFeatureStoreManifest> {
    validate_feature_input(&input)?;
    let seal_sha256 = sha256_file(&input.dataset_seal_path)?;
    let (seal, opportunities) = load_sealed_opportunities(&input.dataset_seal_path)?;
    let catalog_sha256 = sha256_file(&input.market_catalog_path)?;
    let pairs = load_outcome_neutral_pairs(&input.market_catalog_path)?;
    let source_manifests = load_source_manifests(&seal)?;
    let mut opportunities_by_hour = BTreeMap::<i64, Vec<CausalOpportunity>>::new();
    for opportunity in opportunities.iter().cloned() {
        opportunities_by_hour
            .entry(opportunity.observed_at_ms.div_euclid(3_600_000) * 3_600_000)
            .or_default()
            .push(opportunity);
    }

    let loader = PMXTv2Loader::new(&input.cache_dir);
    let mut rows = Vec::with_capacity(opportunities.len());
    let mut pmxt_sources = Vec::new();
    for (hour_ms, hour_opportunities) in opportunities_by_hour {
        let hour = DateTime::<Utc>::from_timestamp_millis(hour_ms)
            .context("order-flow hour is outside chrono range")?;
        let hour_name = hour.to_rfc3339_opts(SecondsFormat::Secs, true);
        let source_manifest = source_manifests
            .get(&hour_name)
            .with_context(|| format!("sealed opportunity manifest missing hour {hour_name}"))?;
        let pmxt_path = loader.cache_path_for_hour(hour);
        if !pmxt_path.is_file() {
            bail!(
                "order-flow features require cached PMXT hour at {}",
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
        let (mut hour_rows, by_condition) = prepare_hour_rows(hour_opportunities, &pairs)?;
        let streamed_target_events = loader
            .scan_cached_hour_market_flow(hour, &condition_ids, |event| {
                accumulate_event(event, &by_condition, &mut hour_rows)
            })
            .with_context(|| format!("stream order-flow PMXT hour {hour_name}"))?;
        if streamed_target_events == 0 {
            bail!("order-flow PMXT hour {hour_name} contains zero target events");
        }
        rows.extend(finish_hour_rows(hour_rows, seal.stake_usd, seal.fee_rate)?);
        pmxt_sources.push(FeatureStorePmxtSource {
            hour: hour_name,
            pmxt_parquet: HashedSource {
                path: pmxt_path.display().to_string(),
                sha256: pmxt_sha256,
            },
            target_condition_count: condition_ids.len(),
            streamed_target_events,
        });
    }
    rows.sort_by(|left, right| {
        left.observed_at_ms
            .cmp(&right.observed_at_ms)
            .then_with(|| left.source_opportunity_id.cmp(&right.source_opportunity_id))
    });
    if rows.len() != opportunities.len() {
        bail!("order-flow feature store lost sealed coordinates");
    }
    write_jsonl_atomic(&input.output_path, &rows)?;
    let configuration = OrderFlowPluginConfiguration {
        causal_windows_ms: [WINDOW_15S_MS, WINDOW_30S_MS],
        trade_deduplication: "transaction_hash; deterministic event tuple fallback when absent",
        quote_source: "latest valid PMXT book or price_change top-of-book at or before observed_at",
        trade_side_semantics: "PMXT aggressor side validated by prior bid/ask comparison evidence",
    };
    let complete_pair_rows = rows
        .iter()
        .filter(|row| row.features.up.quote_observable && row.features.down.quote_observable)
        .count();
    let manifest = OpportunityFeatureStoreManifest {
        schema_version: OPPORTUNITY_FEATURE_STORE_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        dataset_seal: HashedSource {
            path: input.dataset_seal_path.display().to_string(),
            sha256: seal_sha256,
        },
        dataset_sha256: seal.dataset_sha256,
        market_catalog: HashedSource {
            path: input.market_catalog_path.display().to_string(),
            sha256: catalog_sha256,
        },
        output: HashedSource {
            path: input.output_path.display().to_string(),
            sha256: sha256_file(&input.output_path)?,
        },
        plugin: FeaturePluginDescriptor {
            plugin_id: ORDER_FLOW_PLUGIN_ID.to_string(),
            plugin_version: ORDER_FLOW_PLUGIN_VERSION.to_string(),
            configuration_sha256: stable_json_hash(&configuration),
            causal_windows_ms: vec![WINDOW_15S_MS, WINDOW_30S_MS],
            payload_schema: vec![
                "up|down.quote_timestamp_ms,best_bid,best_ask,spread".to_string(),
                "up|down.window_15s|window_30s.quote_change_events,bid_change_events,ask_change_events".to_string(),
                "up|down.window_15s|window_30s.unique_trade_prints,buy_trade_prints,sell_trade_prints,buy_trade_notional_usd,sell_trade_notional_usd".to_string(),
            ],
        },
        source_opportunity_rows: opportunities.len(),
        output_rows: rows.len(),
        complete_pair_rows,
        source_pmxt_scans: pmxt_sources.len(),
        source_pmxt_hours: pmxt_sources,
        outcome_columns_present: false,
        gamma_outcome_prices_influence_output: false,
        external_price_or_model_features_influence_output: false,
        feature_semantics: "sealed coordinates plus streamed PMXT complementary-token book-top, quote-change arrivals, and unique aggressor-side trade prints over fixed trailing 15s/30s windows; no label, BTC path, volatility, model probability, prior score, or PnL enters the cache".to_string(),
    };
    write_json_artifact_atomic(&input.manifest_path, &manifest)?;
    Ok(manifest)
}

pub fn search(input: OrderFlowSearchInput) -> Result<OpportunityFlowSearchReport> {
    validate_search_input(&input)?;
    let seal_sha256 = sha256_file(&input.dataset_seal_path)?;
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
    if labels_manifest.dataset_seal.sha256 != seal_sha256
        || labels_manifest.dataset_sha256 != seal.dataset_sha256
        || labels_manifest.fresh_holdout_labels_present
    {
        bail!("order-flow labels do not belong to the outcome-safe dataset");
    }
    let labels_path = PathBuf::from(&labels_manifest.output.path);
    if sha256_file(&labels_path)? != labels_manifest.output.sha256 {
        bail!("order-flow label table hash drifted");
    }
    let labels = read_labels(&labels_path)?;
    let mut labels_by_id = HashMap::new();
    for label in &labels {
        let opportunity = opportunities_by_id
            .get(label.opportunity_id.as_str())
            .context("order-flow label references unknown opportunity")?;
        if opportunity.chronological_window == "fresh_holdout" {
            bail!("order-flow labels expose a fresh-holdout outcome");
        }
        if labels_by_id
            .insert(label.opportunity_id.as_str(), label)
            .is_some()
        {
            bail!("duplicate opportunity_id in order-flow labels");
        }
    }

    let feature_manifest_sha256 = sha256_file(&input.feature_store_manifest_path)?;
    let feature_manifest: OpportunityFeatureStoreManifest = serde_json::from_reader(
        File::open(&input.feature_store_manifest_path)
            .with_context(|| format!("open {}", input.feature_store_manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.feature_store_manifest_path.display()))?;
    validate_outcome_free_manifest(
        &feature_manifest,
        ORDER_FLOW_PLUGIN_ID,
        ORDER_FLOW_PLUGIN_VERSION,
        &seal.dataset_sha256,
        &seal_sha256,
    )?;
    let feature_path = PathBuf::from(&feature_manifest.output.path);
    if sha256_file(&feature_path)? != feature_manifest.output.sha256 {
        bail!("order-flow feature-store output hash drifted");
    }
    let feature_rows = read_feature_store_rows::<OrderFlowPairFeatures>(&feature_path)?;
    if feature_rows.len() != opportunities.len()
        || feature_rows.len() != feature_manifest.output_rows
    {
        bail!("order-flow feature store does not cover the sealed coordinate set");
    }
    let mut seen = HashSet::new();
    for row in &feature_rows {
        let opportunity = opportunities_by_id
            .get(row.source_opportunity_id.as_str())
            .context("order-flow feature references unknown opportunity")?;
        if !seen.insert(row.source_opportunity_id.as_str())
            || row.condition_id != opportunity.condition_id
            || row.observed_at_ms != opportunity.observed_at_ms
            || row.chronological_window != opportunity.chronological_window
        {
            bail!("order-flow feature coordinate drifted from sealed opportunity");
        }
    }

    let policies = order_flow_policy_grid();
    let policy_grid_sha256 = stable_json_hash(&policies);
    let mut evaluated = Vec::with_capacity(policies.len());
    for policy in policies {
        let mut accumulator = PolicyAccumulator::default();
        for row in &feature_rows {
            let Some(selection) = select_flow(row, &policy) else {
                continue;
            };
            let economics = top_quote_economics(selection.best_ask, seal.stake_usd, seal.fee_rate)?;
            match row.chronological_window.as_str() {
                "older" => {
                    let Some(label) = labels_by_id.get(row.source_opportunity_id.as_str()) else {
                        continue;
                    };
                    let Some(won) = selected_won(&label.terminal_direction, selection.direction)
                    else {
                        continue;
                    };
                    accumulator.calibration_support += 1;
                    accumulator.calibration_wins += usize::from(won);
                    accumulator.calibration_break_even_sum += economics.0;
                }
                "recent_discovery" => {
                    let Some(label) = labels_by_id.get(row.source_opportunity_id.as_str()) else {
                        continue;
                    };
                    let Some(won) = selected_won(&label.terminal_direction, selection.direction)
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
                    accumulator.discovery_break_even_sum += economics.0;
                    accumulator.discovery_pnl_usd += if won { economics.1 } else { -economics.2 };
                    accumulator.discovery_imbalance_sum += selection.trade_imbalance.abs();
                    accumulator.discovery_trade_count_sum += selection.pair_trade_prints;
                }
                "fresh_holdout" => accumulator.fresh_selections.push((
                    row.source_opportunity_id.clone(),
                    selection.token_id.to_string(),
                )),
                _ => bail!("order-flow feature has unsupported chronological partition"),
            }
        }
        evaluated.push(finish_policy(policy, accumulator, &input));
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
                        .expect("eligible order-flow policy has trace"),
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
                diagnostic_order(&evaluated[**left].result, &evaluated[**right].result)
            })
            .expect("non-empty order-flow replay group");
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
            causal_feature_semantics_version: format!(
                "{ORDER_FLOW_PLUGIN_ID}_{ORDER_FLOW_PLUGIN_VERSION}"
            ),
        });
    }
    let result_by_id = evaluated
        .iter()
        .map(|finished| (finished.result.policy_id.as_str(), &finished.result))
        .collect::<HashMap<_, _>>();
    replay_entries.sort_by(|left, right| {
        diagnostic_order(
            result_by_id[left.representative_policy_id.as_str()],
            result_by_id[right.representative_policy_id.as_str()],
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
    diagnostics.sort_by(diagnostic_order);
    diagnostics.truncate(12);
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
    let report = OpportunityFlowSearchReport {
        schema_version: OPPORTUNITY_FLOW_SEARCH_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        family_id: "trade_tape_directional_flow_v1".to_string(),
        dataset_seal: HashedSource {
            path: input.dataset_seal_path.display().to_string(),
            sha256: seal_sha256,
        },
        labels_manifest: HashedSource {
            path: input.labels_manifest_path.display().to_string(),
            sha256: labels_manifest_sha256,
        },
        feature_store: HashedSource {
            path: input.feature_store_manifest_path.display().to_string(),
            sha256: feature_manifest_sha256,
        },
        dataset_sha256: seal.dataset_sha256,
        causal_feature_semantics_version: format!("{ORDER_FLOW_PLUGIN_ID}_{ORDER_FLOW_PLUGIN_VERSION}"),
        source_opportunity_table_reads: seal.entries.len(),
        source_feature_store_reads: 1,
        in_memory_policy_evaluation_passes: 1,
        fresh_holdout_outcomes_accessed: false,
        selection_semantics: "choose Up from Up aggressive-buy + Down aggressive-sell notional, or Down from the converse; normalize by total pair trade notional; quote fields only enforce fixed executability bounds; no BTC path, volatility, model probability, depth pressure, outcome, score, or PnL enters selection".to_string(),
        policy_grid_sha256,
        policies_evaluated: order_flow_policy_grid().len(),
        minimum_calibration_support: input.minimum_calibration_support,
        minimum_policy_support: input.minimum_policy_support,
        safety_margin: input.safety_margin,
        calibration_semantics: "older outcomes test sign stability for the fixed trade-tape rules; no fitted probability or learned threshold".to_string(),
        discovery_gate: "older support and positive point edge; recent support, positive top-quote payoff proxy, and point edge above the fixed safety margin".to_string(),
        exact_replay_is_research_only: true,
        promotion_requires_wilson_after_exact_replay: true,
        calibration_rows: feature_rows.iter().filter(|row| row.chronological_window == "older").count(),
        discovery_rows: feature_rows.iter().filter(|row| row.chronological_window == "recent_discovery").count(),
        fresh_holdout_rows: feature_rows.iter().filter(|row| row.chronological_window == "fresh_holdout").count(),
        complete_pair_rows: feature_manifest.complete_pair_rows,
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

pub fn decide(input: OrderFlowDecisionInput) -> Result<OpportunityFlowDecisionReport> {
    if input.output_path == input.preregistration_path
        || input.output_path == input.flow_search_report_path
        || input
            .exact_replay_report_path
            .as_ref()
            .is_some_and(|path| path == &input.output_path)
    {
        bail!("order-flow decision output must not replace an input");
    }
    let preregistration_sha256 = sha256_file(&input.preregistration_path)?;
    let preregistration: FlowPreregistration = serde_json::from_reader(
        File::open(&input.preregistration_path)
            .with_context(|| format!("open {}", input.preregistration_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.preregistration_path.display()))?;
    if preregistration.schema_version != "trade_tape_directional_flow_preregistration_v1"
        || preregistration.family_id != "trade_tape_directional_flow_v1"
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
        bail!("invalid trade-tape directional-flow preregistration");
    }
    let search_sha256 = sha256_file(&input.flow_search_report_path)?;
    let search: OpportunityFlowSearchReport = serde_json::from_reader(
        File::open(&input.flow_search_report_path)
            .with_context(|| format!("open {}", input.flow_search_report_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.flow_search_report_path.display()))?;
    if search.schema_version != OPPORTUNITY_FLOW_SEARCH_SCHEMA_VERSION
        || search.family_id != preregistration.family_id
        || search.dataset_seal != preregistration.inputs.dataset_seal
        || search.labels_manifest != preregistration.inputs.labels_manifest
        || search.feature_store != preregistration.inputs.feature_store
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
        bail!("order-flow search report drifted from preregistration");
    }

    let mut exact_source = None;
    let mut trace_decisions = Vec::new();
    if search.exact_replay_plan.entries.is_empty() {
        if input.exact_replay_report_path.is_some() {
            bail!("cheap-screen rejection must not consume exact replay");
        }
    } else {
        let exact_path = input
            .exact_replay_report_path
            .as_ref()
            .context("eligible order-flow search requires bounded exact replay")?;
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
            bail!("order-flow exact replay does not match bounded search plan");
        }
        let expected = search
            .exact_replay_plan
            .entries
            .iter()
            .map(|entry| entry.discovery_trace_sha256.as_str())
            .collect::<HashSet<_>>();
        if exact
            .traces
            .iter()
            .any(|trace| !expected.contains(trace.discovery_trace_sha256.as_str()))
        {
            bail!("order-flow exact replay contains an unplanned trace");
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
                OrderFlowTraceDecision {
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
        exact_source = Some(HashedSource {
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
            "at least one preregistered trade-flow trace cleared the fixed exact-replay Wilson edge and positive-PnL gate",
        )
    } else if search.exact_replay_plan.entries.is_empty() {
        (
            "reject_family_keep_fresh_sealed",
            "no fixed-grid trade-flow policy cleared the older/recent cheap screen",
        )
    } else {
        (
            "reject_family_keep_fresh_sealed",
            "no bounded exact-replay trade-flow trace cleared the fixed Wilson edge and positive-PnL gate",
        )
    };
    let report = OpportunityFlowDecisionReport {
        schema_version: "opportunity_flow_decision_v1".to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        family_id: preregistration.family_id,
        preregistration: HashedSource {
            path: input.preregistration_path.display().to_string(),
            sha256: preregistration_sha256,
        },
        flow_search_report: HashedSource {
            path: input.flow_search_report_path.display().to_string(),
            sha256: search_sha256,
        },
        exact_replay_report: exact_source,
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

fn prepare_hour_rows(
    opportunities: Vec<CausalOpportunity>,
    pairs: &HashMap<String, OutcomeNeutralPair>,
) -> Result<PreparedHourRows> {
    let mut rows = Vec::with_capacity(opportunities.len());
    let mut by_condition = HashMap::<String, Vec<usize>>::new();
    for opportunity in opportunities {
        let pair = pairs
            .get(&opportunity.condition_id)
            .with_context(|| format!("catalog missing pair for {}", opportunity.condition_id))?
            .clone();
        if opportunity.token_id != pair.up_token_id && opportunity.token_id != pair.down_token_id {
            bail!("source opportunity token is outside its catalog pair");
        }
        let index = rows.len();
        by_condition
            .entry(opportunity.condition_id.clone())
            .or_default()
            .push(index);
        rows.push(PartialFlowRow {
            opportunity,
            pair,
            up: TokenFlowAccumulator::default(),
            down: TokenFlowAccumulator::default(),
        });
    }
    Ok((rows, by_condition))
}

fn accumulate_event(
    event: &MarketFlowEvent,
    by_condition: &HashMap<String, Vec<usize>>,
    rows: &mut [PartialFlowRow],
) {
    let Some(indices) = by_condition.get(&event.market_id) else {
        return;
    };
    for index in indices {
        let row = &mut rows[*index];
        if event.timestamp_ms > row.opportunity.observed_at_ms {
            continue;
        }
        let token = if event.token_id == row.pair.up_token_id {
            &mut row.up
        } else if event.token_id == row.pair.down_token_id {
            &mut row.down
        } else {
            continue;
        };
        if matches!(
            event.kind,
            MarketFlowEventKind::Book | MarketFlowEventKind::PriceChange
        ) && event.best_bid > 0.0
            && event.best_bid < 1.0
            && event.best_ask > 0.0
            && event.best_ask < 1.0
            && event.best_bid <= event.best_ask
            && token
                .quote_timestamp_ms
                .is_none_or(|timestamp| event.timestamp_ms >= timestamp)
        {
            token.quote_timestamp_ms = Some(event.timestamp_ms);
            token.best_bid = Some(event.best_bid);
            token.best_ask = Some(event.best_ask);
        }
        if event.kind == MarketFlowEventKind::Trade
            && token
                .last_trade_timestamp_ms
                .is_none_or(|timestamp| event.timestamp_ms >= timestamp)
        {
            token.last_trade_timestamp_ms = Some(event.timestamp_ms);
        }
        let age_ms = row.opportunity.observed_at_ms - event.timestamp_ms;
        if !(0..=WINDOW_30S_MS).contains(&age_ms) {
            continue;
        }
        record_window_event(&mut token.window_30s, event);
        if age_ms <= WINDOW_15S_MS {
            record_window_event(&mut token.window_15s, event);
        }
    }
}

fn record_window_event(window: &mut WindowAccumulator, event: &MarketFlowEvent) {
    match event.kind {
        MarketFlowEventKind::PriceChange => {
            window.quote_change_events += 1;
            match event.side.as_str() {
                "BUY" => window.bid_change_events += 1,
                "SELL" => window.ask_change_events += 1,
                _ => {}
            }
        }
        MarketFlowEventKind::Trade if event.price > 0.0 && event.size > 0.0 => {
            let key = event.transaction_hash.clone().unwrap_or_else(|| {
                format!(
                    "{}:{}:{}:{}:{}",
                    event.timestamp_ms,
                    event.token_id,
                    event.side,
                    event.price.to_bits(),
                    event.size.to_bits()
                )
            });
            if !window.trade_keys.insert(key) {
                return;
            }
            let notional = event.price * event.size;
            match event.side.as_str() {
                "BUY" => {
                    window.buy_trade_prints += 1;
                    window.buy_trade_notional_usd += notional;
                }
                "SELL" => {
                    window.sell_trade_prints += 1;
                    window.sell_trade_notional_usd += notional;
                }
                _ => {}
            }
        }
        _ => {}
    }
}

fn finish_hour_rows(
    rows: Vec<PartialFlowRow>,
    _stake_usd: f64,
    _fee_rate: f64,
) -> Result<Vec<OrderFlowFeatureRow>> {
    rows.into_iter()
        .map(|row| {
            let observed_at_ms = row.opportunity.observed_at_ms;
            let elapsed_ms = observed_at_ms.saturating_sub(row.opportunity.window_start_ms);
            let decision_seconds =
                u16::try_from(elapsed_ms / 1_000).context("decision offset exceeds u16")?;
            Ok(OpportunityFeatureStoreRow {
                source_opportunity_id: row.opportunity.opportunity_id,
                condition_id: row.opportunity.condition_id,
                chronological_window: row.opportunity.chronological_window,
                window_start_ms: row.opportunity.window_start_ms,
                observed_at_ms,
                decision_seconds,
                up_token_id: row.pair.up_token_id,
                down_token_id: row.pair.down_token_id,
                features: OrderFlowPairFeatures {
                    up: finish_token(row.up, observed_at_ms),
                    down: finish_token(row.down, observed_at_ms),
                },
            })
        })
        .collect()
}

fn finish_token(token: TokenFlowAccumulator, observed_at_ms: i64) -> TokenOrderFlowFeatures {
    let quote_observable =
        token.quote_timestamp_ms.is_some() && token.best_bid.is_some() && token.best_ask.is_some();
    TokenOrderFlowFeatures {
        quote_observable,
        quote_timestamp_ms: token.quote_timestamp_ms,
        quote_age_ms: token
            .quote_timestamp_ms
            .map(|timestamp| observed_at_ms.saturating_sub(timestamp)),
        best_bid: token.best_bid,
        best_ask: token.best_ask,
        spread: token
            .best_bid
            .zip(token.best_ask)
            .map(|(bid, ask)| ask - bid),
        last_trade_age_ms: token
            .last_trade_timestamp_ms
            .map(|timestamp| observed_at_ms.saturating_sub(timestamp)),
        window_15s: finish_window(token.window_15s),
        window_30s: finish_window(token.window_30s),
    }
}

fn finish_window(window: WindowAccumulator) -> OrderFlowWindowFeatures {
    OrderFlowWindowFeatures {
        quote_change_events: window.quote_change_events,
        bid_change_events: window.bid_change_events,
        ask_change_events: window.ask_change_events,
        unique_trade_prints: window.trade_keys.len(),
        buy_trade_prints: window.buy_trade_prints,
        sell_trade_prints: window.sell_trade_prints,
        buy_trade_notional_usd: window.buy_trade_notional_usd,
        sell_trade_notional_usd: window.sell_trade_notional_usd,
    }
}

fn order_flow_policy_grid() -> Vec<OrderFlowPolicyDefinition> {
    let mut policies = Vec::new();
    for decision_seconds in [120, 180, 240] {
        for lookback_seconds in [15, 30] {
            for minimum_pair_trade_prints in [25, 75, 150] {
                for minimum_absolute_trade_imbalance in [0.25, 0.50, 0.75] {
                    policies.push(OrderFlowPolicyDefinition {
                        decision_seconds,
                        lookback_seconds,
                        minimum_pair_trade_prints,
                        minimum_absolute_trade_imbalance,
                    });
                }
            }
        }
    }
    policies
}

fn select_flow<'a>(
    row: &'a OrderFlowFeatureRow,
    policy: &OrderFlowPolicyDefinition,
) -> Option<FlowSelection<'a>> {
    if row.decision_seconds != policy.decision_seconds
        || !row.features.up.quote_observable
        || !row.features.down.quote_observable
        || row.features.up.spread?.max(row.features.down.spread?) > MAXIMUM_PAIR_SPREAD + 1e-12
    {
        return None;
    }
    let (up, down) = match policy.lookback_seconds {
        15 => (&row.features.up.window_15s, &row.features.down.window_15s),
        30 => (&row.features.up.window_30s, &row.features.down.window_30s),
        _ => return None,
    };
    let pair_trade_prints = up.unique_trade_prints + down.unique_trade_prints;
    if pair_trade_prints < policy.minimum_pair_trade_prints {
        return None;
    }
    let up_evidence = up.buy_trade_notional_usd + down.sell_trade_notional_usd;
    let down_evidence = up.sell_trade_notional_usd + down.buy_trade_notional_usd;
    let total = up_evidence + down_evidence;
    if total <= 0.0 {
        return None;
    }
    let trade_imbalance = (up_evidence - down_evidence) / total;
    if trade_imbalance.abs() + 1e-12 < policy.minimum_absolute_trade_imbalance {
        return None;
    }
    let (token_id, direction, best_ask) = if trade_imbalance > 0.0 {
        (row.up_token_id.as_str(), "up", row.features.up.best_ask?)
    } else {
        (
            row.down_token_id.as_str(),
            "down",
            row.features.down.best_ask?,
        )
    };
    if !(best_ask > 0.0 && best_ask <= MAXIMUM_EXECUTION_ASK) {
        return None;
    }
    Some(FlowSelection {
        token_id,
        direction,
        best_ask,
        trade_imbalance,
        pair_trade_prints,
    })
}

fn top_quote_economics(best_ask: f64, stake_usd: f64, fee_rate: f64) -> Result<(f64, f64, f64)> {
    if !(best_ask > 0.0 && best_ask < 1.0 && stake_usd > 0.0 && fee_rate >= 0.0) {
        bail!("invalid top-quote economics inputs");
    }
    let shares = stake_usd / best_ask;
    let fee = polymarket_fee(shares, best_ask, fee_rate);
    Ok((
        (stake_usd + fee) / shares,
        shares - stake_usd - fee,
        stake_usd + fee,
    ))
}

fn finish_policy(
    policy: OrderFlowPolicyDefinition,
    accumulator: PolicyAccumulator,
    input: &OrderFlowSearchInput,
) -> FinishedPolicy {
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
    FinishedPolicy {
        result: OrderFlowPolicyResult {
            policy_id: stable_json_hash(&policy),
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
            average_absolute_trade_imbalance: average(
                accumulator.discovery_imbalance_sum,
                discovery_support,
            ),
            average_pair_trade_prints: average(
                accumulator.discovery_trade_count_sum as f64,
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

fn selected_won(terminal_direction: &str, selected_direction: &str) -> Option<bool> {
    match terminal_direction {
        "up" | "down" => Some(terminal_direction == selected_direction),
        "tie" => None,
        _ => None,
    }
}

fn diagnostic_order(left: &OrderFlowPolicyResult, right: &OrderFlowPolicyResult) -> Ordering {
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

fn validate_feature_input(input: &OrderFlowFeatureInput) -> Result<()> {
    if !input.cache_dir.is_dir() {
        bail!("order-flow features require an existing PMXT cache directory");
    }
    if input.output_path == input.manifest_path
        || input.output_path == input.dataset_seal_path
        || input.output_path == input.market_catalog_path
        || input.manifest_path == input.dataset_seal_path
        || input.manifest_path == input.market_catalog_path
    {
        bail!("order-flow outputs must not replace inputs or each other");
    }
    if input
        .output_path
        .extension()
        .and_then(|value| value.to_str())
        != Some("jsonl")
    {
        bail!("order-flow feature output path must use .jsonl");
    }
    Ok(())
}

fn validate_search_input(input: &OrderFlowSearchInput) -> Result<()> {
    if input.minimum_calibration_support == 0
        || input.minimum_policy_support == 0
        || input.maximum_exact_replays == 0
        || input.maximum_exact_replays > 2
    {
        bail!("order-flow supports must be positive and replay budget must be 1 or 2");
    }
    if !input.safety_margin.is_finite() || input.safety_margin < 0.0 {
        bail!("order-flow safety margin must be finite and non-negative");
    }
    if input.output_path == input.dataset_seal_path
        || input.output_path == input.labels_manifest_path
        || input.output_path == input.feature_store_manifest_path
    {
        bail!("order-flow search output must not replace an input");
    }
    Ok(())
}

fn load_source_manifests(
    seal: &OpportunityDatasetSeal,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn window(buy: f64, sell: f64, prints: usize) -> OrderFlowWindowFeatures {
        OrderFlowWindowFeatures {
            quote_change_events: 0,
            bid_change_events: 0,
            ask_change_events: 0,
            unique_trade_prints: prints,
            buy_trade_prints: usize::from(buy > 0.0),
            sell_trade_prints: usize::from(sell > 0.0),
            buy_trade_notional_usd: buy,
            sell_trade_notional_usd: sell,
        }
    }

    fn token(flow: OrderFlowWindowFeatures) -> TokenOrderFlowFeatures {
        TokenOrderFlowFeatures {
            quote_observable: true,
            quote_timestamp_ms: Some(1),
            quote_age_ms: Some(0),
            best_bid: Some(0.49),
            best_ask: Some(0.50),
            spread: Some(0.01),
            last_trade_age_ms: Some(0),
            window_15s: flow.clone(),
            window_30s: flow,
        }
    }

    #[test]
    fn policy_grid_is_bounded_to_fifty_four() {
        let grid = order_flow_policy_grid();
        assert_eq!(grid.len(), 54);
        assert_eq!(
            stable_json_hash(&grid),
            "e92fa9c26d82e60a678f774883676ba76a4e03c2ba855ea063f2abf7ab048093"
        );
    }

    #[test]
    fn complementary_trade_evidence_selects_up() {
        let row = OpportunityFeatureStoreRow {
            source_opportunity_id: "o".to_string(),
            condition_id: "c".to_string(),
            chronological_window: "older".to_string(),
            window_start_ms: 0,
            observed_at_ms: 120_000,
            decision_seconds: 120,
            up_token_id: "up".to_string(),
            down_token_id: "down".to_string(),
            features: OrderFlowPairFeatures {
                up: token(window(80.0, 0.0, 1)),
                down: token(window(0.0, 20.0, 1)),
            },
        };
        let selection = select_flow(
            &row,
            &OrderFlowPolicyDefinition {
                decision_seconds: 120,
                lookback_seconds: 15,
                minimum_pair_trade_prints: 2,
                minimum_absolute_trade_imbalance: 0.50,
            },
        )
        .unwrap();
        assert_eq!(selection.direction, "up");
        assert_eq!(selection.token_id, "up");
    }

    #[test]
    fn trade_prints_are_deduplicated_by_transaction_hash() {
        let event = MarketFlowEvent {
            timestamp_ms: 100,
            market_id: "c".to_string(),
            token_id: "up".to_string(),
            kind: MarketFlowEventKind::Trade,
            side: "BUY".to_string(),
            price: 0.5,
            size: 10.0,
            best_bid: 0.0,
            best_ask: 0.0,
            transaction_hash: Some("tx".to_string()),
        };
        let mut accumulator = WindowAccumulator::default();
        record_window_event(&mut accumulator, &event);
        record_window_event(&mut accumulator, &event);
        assert_eq!(accumulator.trade_keys.len(), 1);
        assert_eq!(accumulator.buy_trade_notional_usd, 5.0);
    }
}
