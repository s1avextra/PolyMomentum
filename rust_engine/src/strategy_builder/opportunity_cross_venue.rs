//! Outcome-free cross-venue lead/lag features.
//!
//! Official Binance spot one-second klines and USD-M perpetual aggregate
//! trades are normalized outside the engine into checksum-verified daily
//! tapes. This plugin joins only closed source seconds to sealed opportunity
//! coordinates and the pre-existing outcome-free complementary-book cache.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};

use crate::artifact::{write_json_artifact_atomic, write_jsonl_atomic};
use crate::strategy::spec::stable_json_hash;

use super::opportunity_dataset::{
    load_sealed_opportunities, read_labels, sha256_file, OpportunityLabelsManifest,
};
use super::opportunity_feature_store::{
    read_feature_store_rows, FeaturePluginDescriptor, OpportunityFeatureStoreRow,
};
use super::opportunity_liquidity::{
    read_pair_features, OpportunityPairFeature, OpportunityPairFeatureManifest, PairBookFeature,
    OPPORTUNITY_PAIR_FEATURE_SCHEMA_VERSION,
};
use super::opportunity_policy::{ExactReplayPlan, ExactReplayPlanEntry};
use super::opportunity_replay::{
    OpportunityExactReplayReport, OPPORTUNITY_EXACT_REPLAY_SCHEMA_VERSION,
};
use super::opportunity_table::HashedSource;

pub const CROSS_VENUE_PLUGIN_ID: &str = "binance_spot_perpetual_lead_lag";
pub const CROSS_VENUE_PLUGIN_VERSION: &str = "v1";
pub const CROSS_VENUE_FEATURE_STORE_SCHEMA_VERSION: &str =
    "opportunity_cross_venue_feature_store_v1";
const WINDOW_1S_MS: i64 = 1_000;
const WINDOW_5S_MS: i64 = 5_000;
const WINDOW_15S_MS: i64 = 15_000;
const MAXIMUM_EXECUTION_ASK: f64 = 0.99;
const MAXIMUM_PAIR_SPREAD: f64 = 0.02;
const MAXIMUM_SPOT_PERPETUAL_GAP_BPS: f64 = 2.0;
pub const CROSS_VENUE_SEARCH_SCHEMA_VERSION: &str = "opportunity_cross_venue_search_v1";
pub const CROSS_VENUE_PREREGISTRATION_SCHEMA_VERSION: &str =
    "cross_venue_lead_lag_preregistration_v1";

pub type CrossVenueFeatureRow = OpportunityFeatureStoreRow<CrossVenuePairFeatures>;

#[derive(Debug, Clone)]
pub struct CrossVenueFeatureInput {
    pub dataset_seal_path: PathBuf,
    pub paired_features_manifest_path: PathBuf,
    pub source_tape_manifest_path: PathBuf,
    pub output_path: PathBuf,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct CrossVenuePreregistrationInput {
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
pub struct CrossVenueSearchInput {
    pub preregistration_path: PathBuf,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct CrossVenueDecisionInput {
    pub preregistration_path: PathBuf,
    pub search_report_path: PathBuf,
    pub exact_replay_report_path: Option<PathBuf>,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenueFeatureStoreManifest {
    pub schema_version: String,
    pub generated_at: String,
    pub dataset_seal: HashedSource,
    pub dataset_sha256: String,
    pub paired_features: HashedSource,
    pub source_tape_manifest: HashedSource,
    pub source_tape_partitions: Vec<HashedSource>,
    pub output: HashedSource,
    pub plugin: FeaturePluginDescriptor,
    pub source_opportunity_rows: usize,
    pub output_rows: usize,
    pub complete_external_rows: usize,
    pub complete_pair_rows: usize,
    pub source_date_reads: usize,
    pub outcome_columns_present: bool,
    pub labels_or_scores_influence_output: bool,
    pub external_market_data_influence_output: bool,
    pub feature_semantics: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CrossVenueWindowFeatures {
    pub window_ms: i64,
    pub spot_return_bps: f64,
    pub perpetual_return_bps: f64,
    pub spot_perpetual_return_gap_bps: f64,
    pub spot_quote_volume_usd: f64,
    pub spot_signed_taker_quote_usd: f64,
    pub perpetual_quote_volume_usd: f64,
    pub perpetual_signed_taker_quote_usd: f64,
    pub spot_trade_count: u64,
    pub perpetual_trade_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CrossVenuePairFeatures {
    pub external_observable: bool,
    pub external_available_at_ms: Option<i64>,
    pub perpetual_price_age_ms: Option<i64>,
    pub window_1s: Option<CrossVenueWindowFeatures>,
    pub window_5s: Option<CrossVenueWindowFeatures>,
    pub window_15s: Option<CrossVenueWindowFeatures>,
    pub up_lookback_15s: PairBookFeature,
    pub down_lookback_15s: PairBookFeature,
    pub up_now: PairBookFeature,
    pub down_now: PairBookFeature,
    pub polymarket_up_midpoint_move_15s: Option<f64>,
    pub polymarket_down_midpoint_move_15s: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenuePolicyDefinition {
    pub decision_seconds: u16,
    pub lookback_seconds: u16,
    pub minimum_consensus_return_bps: f64,
    pub maximum_directional_polymarket_move_15s: f64,
    pub maximum_spot_perpetual_gap_bps: f64,
    pub maximum_pair_spread: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenuePolicyResult {
    pub policy_id: String,
    pub policy: CrossVenuePolicyDefinition,
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
    pub average_consensus_return_bps: Option<f64>,
    pub average_directional_polymarket_move_15s: Option<f64>,
    pub fresh_causal_support: usize,
    pub discovery_trace_sha256: Option<String>,
    pub fresh_support_trace_sha256: Option<String>,
    pub discovery_eligible: bool,
    pub rejection_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenueSearchReport {
    pub schema_version: String,
    pub generated_at: String,
    pub family_id: String,
    pub preregistration: HashedSource,
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
    pub calibration_rows: usize,
    pub discovery_rows: usize,
    pub fresh_holdout_rows: usize,
    pub complete_external_rows: usize,
    pub complete_pair_rows: usize,
    pub eligible_policy_count: usize,
    pub top_diagnostics: Vec<CrossVenuePolicyResult>,
    pub exact_replay_is_research_only: bool,
    pub promotion_requires_wilson_after_exact_replay: bool,
    pub exact_replay_plan: ExactReplayPlan,
    pub verdict: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenueTraceDecision {
    pub discovery_trace_sha256: String,
    pub representative_policy_id: String,
    pub fills: usize,
    pub wins: usize,
    pub losses: usize,
    pub wilson_edge: Option<f64>,
    pub total_pnl_usd: f64,
    pub decision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenueDecisionReport {
    pub schema_version: String,
    pub generated_at: String,
    pub family_id: String,
    pub preregistration: HashedSource,
    pub search_report: HashedSource,
    pub exact_replay_report: Option<HashedSource>,
    pub fixed_advancement_wilson_edge: f64,
    pub exact_replays_consumed: usize,
    pub fresh_holdout_outcomes_accessed: bool,
    pub trace_decisions: Vec<CrossVenueTraceDecision>,
    pub decision: String,
    pub reason: String,
    pub terminal: bool,
    pub search_budget_exhausted: bool,
    pub more_evidence_allowed: bool,
    pub fresh_gate_opened: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenuePreregistration {
    pub schema_version: String,
    pub generated_at: String,
    pub family_id: String,
    pub inputs: CrossVenuePreregisteredInputs,
    pub policy_grid_sha256: String,
    pub policies_evaluated: usize,
    pub policy_grid: Vec<CrossVenuePolicyDefinition>,
    pub cheap_screen: CrossVenueCheapScreen,
    pub exact_replay_budget: CrossVenueReplayBudget,
    pub advancement_gate: CrossVenueAdvancementGate,
    pub label_access_audit: LabelAccessAudit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenuePreregisteredInputs {
    pub dataset_seal: HashedSource,
    pub labels_manifest: HashedSource,
    pub feature_store: HashedSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenueCheapScreen {
    pub minimum_calibration_support: usize,
    pub minimum_policy_support: usize,
    pub safety_margin: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenueReplayBudget {
    pub maximum_unique_traces: usize,
    pub latency_ms: u64,
    pub additional_discovery_hours: usize,
    pub additional_parameter_variants: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossVenueAdvancementGate {
    pub minimum_exact_replay_wilson_edge: f64,
    pub require_positive_exact_replay_pnl: bool,
}

#[derive(Debug, Deserialize)]
struct SourceTapeManifest {
    schema_version: String,
    dataset_seal: HashedSource,
    partitions: Vec<SourceTapePartition>,
    quality: SourceTapeQuality,
    label_access_audit: LabelAccessAudit,
    status: String,
}

#[derive(Debug, Deserialize)]
struct SourceTapePartition {
    date: String,
    expected_seconds: usize,
    spot_missing_seconds: usize,
    aligned_output_rows: usize,
    output: HashedSource,
}

#[derive(Debug, Deserialize)]
struct SourceTapeQuality {
    ready_for_feature_join: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LabelAccessAudit {
    label_artifacts_read: usize,
    outcomes_read: usize,
    scores_read: usize,
    pnl_read: usize,
}

#[derive(Debug, Deserialize)]
struct TapeRow {
    second_start_ms: i64,
    available_at_ms: i64,
    spot_close: f64,
    spot_quote_volume: f64,
    spot_taker_buy_quote_volume: f64,
    spot_trade_count: u64,
    perpetual_close: Option<f64>,
    perpetual_last_trade_ms: Option<i64>,
    perpetual_quote_volume: f64,
    perpetual_taker_buy_quote_volume: f64,
    perpetual_trade_count: u64,
    perpetual_aggregate_count: u64,
}

#[derive(Debug, Serialize)]
struct CrossVenuePluginConfiguration {
    causal_windows_ms: [i64; 3],
    spot_source: &'static str,
    perpetual_source: &'static str,
    availability_semantics: &'static str,
    polymarket_source: &'static str,
}

pub fn create_feature_store(
    input: CrossVenueFeatureInput,
) -> Result<CrossVenueFeatureStoreManifest> {
    validate_distinct_paths(&input)?;
    let dataset_seal_sha256 = sha256_file(&input.dataset_seal_path)?;
    let (seal, opportunities) = load_sealed_opportunities(&input.dataset_seal_path)?;

    let paired_manifest_sha256 = sha256_file(&input.paired_features_manifest_path)?;
    let paired_manifest: OpportunityPairFeatureManifest = serde_json::from_reader(
        File::open(&input.paired_features_manifest_path)
            .with_context(|| format!("open {}", input.paired_features_manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.paired_features_manifest_path.display()))?;
    validate_paired_manifest(&paired_manifest, &seal.dataset_sha256, &dataset_seal_sha256)?;
    let paired_path = PathBuf::from(&paired_manifest.output.path);
    if sha256_file(&paired_path)? != paired_manifest.output.sha256 {
        bail!("paired-feature output hash drifted");
    }
    let paired_rows = read_pair_features(&paired_path)?;
    if paired_rows.len() != opportunities.len() || paired_rows.len() != paired_manifest.output_rows
    {
        bail!("paired features do not cover the sealed coordinate set");
    }
    let pairs_by_id = index_pairs(&opportunities, paired_rows)?;

    let tape_manifest_sha256 = sha256_file(&input.source_tape_manifest_path)?;
    let tape_manifest: SourceTapeManifest = serde_json::from_reader(
        File::open(&input.source_tape_manifest_path)
            .with_context(|| format!("open {}", input.source_tape_manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.source_tape_manifest_path.display()))?;
    validate_tape_manifest(&tape_manifest, &dataset_seal_sha256)?;
    let partitions_by_date = tape_manifest
        .partitions
        .iter()
        .map(|partition| (partition.date.as_str(), partition))
        .collect::<HashMap<_, _>>();

    let mut opportunity_indices_by_date = BTreeMap::<String, Vec<usize>>::new();
    for (index, opportunity) in opportunities.iter().enumerate() {
        let date = timestamp_date(opportunity.observed_at_ms)?;
        opportunity_indices_by_date
            .entry(date)
            .or_default()
            .push(index);
    }

    let mut rows_by_index = vec![None; opportunities.len()];
    let mut source_partitions = Vec::new();
    for (date, indices) in &opportunity_indices_by_date {
        let partition = partitions_by_date
            .get(date.as_str())
            .with_context(|| format!("source tape has no partition for {date}"))?;
        let tape_path = PathBuf::from(&partition.output.path);
        if sha256_file(&tape_path)? != partition.output.sha256 {
            bail!("source tape partition hash drifted for {date}");
        }
        let tape = read_tape(&tape_path)?;
        source_partitions.push(partition.output.clone());
        for index in indices {
            let opportunity = &opportunities[*index];
            let pair = pairs_by_id
                .get(opportunity.opportunity_id.as_str())
                .expect("validated paired feature exists");
            let external = measure_external(&tape, opportunity.observed_at_ms);
            let up_move = midpoint_move(&pair.up_lookback, &pair.up_now);
            let down_move = midpoint_move(&pair.down_lookback, &pair.down_now);
            rows_by_index[*index] = Some(CrossVenueFeatureRow {
                source_opportunity_id: opportunity.opportunity_id.clone(),
                condition_id: opportunity.condition_id.clone(),
                chronological_window: opportunity.chronological_window.clone(),
                window_start_ms: opportunity.window_start_ms,
                observed_at_ms: opportunity.observed_at_ms,
                decision_seconds: pair.decision_seconds,
                up_token_id: pair.up_token_id.clone(),
                down_token_id: pair.down_token_id.clone(),
                features: CrossVenuePairFeatures {
                    external_observable: external.0,
                    external_available_at_ms: external.1,
                    perpetual_price_age_ms: external.2,
                    window_1s: external.3,
                    window_5s: external.4,
                    window_15s: external.5,
                    up_lookback_15s: pair.up_lookback.clone(),
                    down_lookback_15s: pair.down_lookback.clone(),
                    up_now: pair.up_now.clone(),
                    down_now: pair.down_now.clone(),
                    polymarket_up_midpoint_move_15s: up_move,
                    polymarket_down_midpoint_move_15s: down_move,
                },
            });
        }
    }
    let rows = rows_by_index
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            row.with_context(|| format!("missing cross-venue feature row at index {index}"))
        })
        .collect::<Result<Vec<_>>>()?;
    let complete_external_rows = rows
        .iter()
        .filter(|row| row.features.external_observable)
        .count();
    let complete_pair_rows = rows
        .iter()
        .filter(|row| {
            row.features.up_now.observable
                && row.features.down_now.observable
                && row.features.up_lookback_15s.observable
                && row.features.down_lookback_15s.observable
        })
        .count();
    write_jsonl_atomic(&input.output_path, &rows)?;

    let configuration = CrossVenuePluginConfiguration {
        causal_windows_ms: [WINDOW_1S_MS, WINDOW_5S_MS, WINDOW_15S_MS],
        spot_source: "official Binance BTCUSDT spot one-second klines",
        perpetual_source: "official Binance BTCUSDT USD-M perpetual aggTrades",
        availability_semantics: "closed seconds only: available_at_ms <= observed_at_ms",
        polymarket_source: "outcome-free paired L2 cache at observed_at and observed_at-15s",
    };
    let manifest = CrossVenueFeatureStoreManifest {
        schema_version: CROSS_VENUE_FEATURE_STORE_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        dataset_seal: HashedSource {
            path: input.dataset_seal_path.display().to_string(),
            sha256: dataset_seal_sha256,
        },
        dataset_sha256: seal.dataset_sha256,
        paired_features: HashedSource {
            path: input.paired_features_manifest_path.display().to_string(),
            sha256: paired_manifest_sha256,
        },
        source_tape_manifest: HashedSource {
            path: input.source_tape_manifest_path.display().to_string(),
            sha256: tape_manifest_sha256,
        },
        source_tape_partitions: source_partitions,
        output: HashedSource {
            path: input.output_path.display().to_string(),
            sha256: sha256_file(&input.output_path)?,
        },
        plugin: FeaturePluginDescriptor {
            plugin_id: CROSS_VENUE_PLUGIN_ID.to_string(),
            plugin_version: CROSS_VENUE_PLUGIN_VERSION.to_string(),
            configuration_sha256: stable_json_hash(&configuration),
            causal_windows_ms: vec![WINDOW_1S_MS, WINDOW_5S_MS, WINDOW_15S_MS],
            payload_schema: vec![
                "window_1s|window_5s|window_15s spot/perpetual return, flow, volume, count"
                    .to_string(),
                "up|down paired L2 now and 15s lookback".to_string(),
                "polymarket up/down midpoint move 15s".to_string(),
            ],
        },
        source_opportunity_rows: opportunities.len(),
        output_rows: rows.len(),
        complete_external_rows,
        complete_pair_rows,
        source_date_reads: opportunity_indices_by_date.len(),
        outcome_columns_present: false,
        labels_or_scores_influence_output: false,
        external_market_data_influence_output: true,
        feature_semantics: "sealed coordinates join to fully closed Binance seconds at 1s/5s/15s and prebuilt outcome-free complementary books; no label, terminal price, strategy score, retention, or PnL input is accepted".to_string(),
    };
    write_json_artifact_atomic(&input.manifest_path, &manifest)?;
    Ok(manifest)
}

pub fn preregister(input: CrossVenuePreregistrationInput) -> Result<CrossVenuePreregistration> {
    if input.minimum_calibration_support == 0
        || input.minimum_policy_support == 0
        || input.maximum_exact_replays == 0
        || input.maximum_exact_replays > 2
        || !input.safety_margin.is_finite()
        || input.safety_margin < 0.0
        || [
            &input.dataset_seal_path,
            &input.labels_manifest_path,
            &input.feature_store_manifest_path,
        ]
        .contains(&&input.output_path)
    {
        bail!("invalid cross-venue preregistration settings");
    }
    let dataset_seal_sha256 = sha256_file(&input.dataset_seal_path)?;
    let (seal, _) = load_sealed_opportunities(&input.dataset_seal_path)?;
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
        bail!("labels manifest is incompatible with cross-venue preregistration");
    }
    let feature_manifest_sha256 = sha256_file(&input.feature_store_manifest_path)?;
    let feature_manifest = read_cross_venue_manifest(&input.feature_store_manifest_path)?;
    validate_cross_venue_manifest(
        &feature_manifest,
        &seal.dataset_sha256,
        &dataset_seal_sha256,
    )?;
    let policies = policy_grid();
    let report = CrossVenuePreregistration {
        schema_version: CROSS_VENUE_PREREGISTRATION_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        family_id: "cross_venue_lead_lag_residual_v1".to_string(),
        inputs: CrossVenuePreregisteredInputs {
            dataset_seal: HashedSource {
                path: input.dataset_seal_path.display().to_string(),
                sha256: dataset_seal_sha256,
            },
            labels_manifest: HashedSource {
                path: input.labels_manifest_path.display().to_string(),
                sha256: labels_manifest_sha256,
            },
            feature_store: HashedSource {
                path: input.feature_store_manifest_path.display().to_string(),
                sha256: feature_manifest_sha256,
            },
        },
        policy_grid_sha256: stable_json_hash(&policies),
        policies_evaluated: policies.len(),
        policy_grid: policies,
        cheap_screen: CrossVenueCheapScreen {
            minimum_calibration_support: input.minimum_calibration_support,
            minimum_policy_support: input.minimum_policy_support,
            safety_margin: input.safety_margin,
        },
        exact_replay_budget: CrossVenueReplayBudget {
            maximum_unique_traces: input.maximum_exact_replays,
            latency_ms: input.latency_ms,
            additional_discovery_hours: 0,
            additional_parameter_variants: 0,
        },
        advancement_gate: CrossVenueAdvancementGate {
            minimum_exact_replay_wilson_edge: 0.02,
            require_positive_exact_replay_pnl: true,
        },
        label_access_audit: LabelAccessAudit {
            label_artifacts_read: 0,
            outcomes_read: 0,
            scores_read: 0,
            pnl_read: 0,
        },
    };
    write_json_artifact_atomic(&input.output_path, &report)?;
    Ok(report)
}

pub fn search(input: CrossVenueSearchInput) -> Result<CrossVenueSearchReport> {
    if input.preregistration_path == input.output_path {
        bail!("cross-venue search output must not replace preregistration");
    }
    let preregistration_sha256 = sha256_file(&input.preregistration_path)?;
    let preregistration: CrossVenuePreregistration = serde_json::from_reader(
        File::open(&input.preregistration_path)
            .with_context(|| format!("open {}", input.preregistration_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.preregistration_path.display()))?;
    validate_preregistration(&preregistration)?;

    let dataset_path = PathBuf::from(&preregistration.inputs.dataset_seal.path);
    if sha256_file(&dataset_path)? != preregistration.inputs.dataset_seal.sha256 {
        bail!("cross-venue dataset seal hash drifted after preregistration");
    }
    let (seal, opportunities) = load_sealed_opportunities(&dataset_path)?;
    let opportunities_by_id = opportunities
        .iter()
        .map(|row| (row.opportunity_id.as_str(), row))
        .collect::<HashMap<_, _>>();

    let labels_manifest_path = PathBuf::from(&preregistration.inputs.labels_manifest.path);
    if sha256_file(&labels_manifest_path)? != preregistration.inputs.labels_manifest.sha256 {
        bail!("cross-venue labels manifest hash drifted after preregistration");
    }
    let labels_manifest: OpportunityLabelsManifest = serde_json::from_reader(
        File::open(&labels_manifest_path)
            .with_context(|| format!("open {}", labels_manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", labels_manifest_path.display()))?;
    if labels_manifest.dataset_seal != preregistration.inputs.dataset_seal
        || labels_manifest.dataset_sha256 != seal.dataset_sha256
        || labels_manifest.fresh_holdout_labels_present
    {
        bail!("cross-venue labels violate the sealed discovery contract");
    }
    let labels_path = PathBuf::from(&labels_manifest.output.path);
    if sha256_file(&labels_path)? != labels_manifest.output.sha256 {
        bail!("cross-venue label table hash drifted");
    }
    let labels = read_labels(&labels_path)?;
    let mut labels_by_id = HashMap::new();
    for label in &labels {
        let opportunity = opportunities_by_id
            .get(label.opportunity_id.as_str())
            .context("cross-venue label references unknown opportunity")?;
        if opportunity.chronological_window == "fresh_holdout" {
            bail!("cross-venue labels expose a fresh-holdout outcome");
        }
        if labels_by_id
            .insert(label.opportunity_id.as_str(), label)
            .is_some()
        {
            bail!("duplicate cross-venue label");
        }
    }

    let feature_manifest_path = PathBuf::from(&preregistration.inputs.feature_store.path);
    if sha256_file(&feature_manifest_path)? != preregistration.inputs.feature_store.sha256 {
        bail!("cross-venue feature manifest hash drifted after preregistration");
    }
    let feature_manifest = read_cross_venue_manifest(&feature_manifest_path)?;
    validate_cross_venue_manifest(
        &feature_manifest,
        &seal.dataset_sha256,
        &preregistration.inputs.dataset_seal.sha256,
    )?;
    let feature_path = PathBuf::from(&feature_manifest.output.path);
    if sha256_file(&feature_path)? != feature_manifest.output.sha256 {
        bail!("cross-venue feature rows hash drifted");
    }
    let feature_rows = read_feature_store_rows::<CrossVenuePairFeatures>(&feature_path)?;
    if feature_rows.len() != opportunities.len()
        || feature_rows.len() != feature_manifest.output_rows
    {
        bail!("cross-venue features do not cover the sealed dataset");
    }
    let mut feature_ids = HashSet::new();
    for row in &feature_rows {
        let opportunity = opportunities_by_id
            .get(row.source_opportunity_id.as_str())
            .context("cross-venue feature references unknown opportunity")?;
        if !feature_ids.insert(row.source_opportunity_id.as_str())
            || row.condition_id != opportunity.condition_id
            || row.observed_at_ms != opportunity.observed_at_ms
            || row.chronological_window != opportunity.chronological_window
        {
            bail!("cross-venue feature coordinate drifted or duplicated");
        }
    }

    let mut evaluated = Vec::with_capacity(preregistration.policy_grid.len());
    for policy in preregistration.policy_grid.clone() {
        let mut accumulator = PolicyAccumulator::default();
        for row in &feature_rows {
            let Some(selection) = select(row, &policy) else {
                continue;
            };
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
                    accumulator.calibration_break_even_sum += selection.break_even;
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
                    accumulator.discovery_break_even_sum += selection.break_even;
                    accumulator.discovery_pnl_usd += if won {
                        selection.net_win
                    } else {
                        -selection.max_loss
                    };
                    accumulator.consensus_return_sum += selection.consensus_return_bps;
                    accumulator.pm_move_sum += selection.directional_pm_move;
                }
                "fresh_holdout" => accumulator.fresh_selections.push((
                    row.source_opportunity_id.clone(),
                    selection.token_id.to_string(),
                )),
                _ => bail!("unsupported chronological partition in cross-venue features"),
            }
        }
        evaluated.push(finish_policy(
            policy,
            accumulator,
            &preregistration.cheap_screen,
        ));
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
                        .expect("eligible cross-venue trace exists"),
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
            .expect("non-empty replay group");
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
            latency_ms: preregistration.exact_replay_budget.latency_ms,
            causal_feature_semantics_version: format!(
                "{CROSS_VENUE_PLUGIN_ID}_{CROSS_VENUE_PLUGIN_VERSION}"
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
    replay_entries.truncate(preregistration.exact_replay_budget.maximum_unique_traces);
    let unique_replay_count = replay_entries.len();
    let avoided_replay_count = eligible_policy_count.saturating_sub(unique_replay_count);
    let exact_replay_plan = ExactReplayPlan {
        status: if unique_replay_count > 0 {
            "exact_replay_plan_ready".to_string()
        } else {
            "no_policy_cleared_cheap_screen".to_string()
        },
        eligible_policy_count,
        eligible_unique_trace_count,
        maximum_replay_count: preregistration.exact_replay_budget.maximum_unique_traces,
        unique_replay_count,
        deferred_replay_count: eligible_unique_trace_count.saturating_sub(unique_replay_count),
        avoided_replay_count,
        equivalence_reduction_fraction: (eligible_policy_count > 0)
            .then_some(avoided_replay_count as f64 / eligible_policy_count as f64),
        entries: replay_entries,
    };
    let mut diagnostics = evaluated
        .into_iter()
        .map(|finished| finished.result)
        .collect::<Vec<_>>();
    diagnostics.sort_by(diagnostic_order);
    diagnostics.truncate(12);
    let report = CrossVenueSearchReport {
        schema_version: CROSS_VENUE_SEARCH_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        family_id: preregistration.family_id,
        preregistration: HashedSource {
            path: input.preregistration_path.display().to_string(),
            sha256: preregistration_sha256,
        },
        dataset_seal: preregistration.inputs.dataset_seal,
        labels_manifest: preregistration.inputs.labels_manifest,
        feature_store: preregistration.inputs.feature_store,
        dataset_sha256: seal.dataset_sha256,
        causal_feature_semantics_version: format!(
            "{CROSS_VENUE_PLUGIN_ID}_{CROSS_VENUE_PLUGIN_VERSION}"
        ),
        source_opportunity_table_reads: seal.entries.len(),
        source_feature_store_reads: 1,
        in_memory_policy_evaluation_passes: 1,
        fresh_holdout_outcomes_accessed: false,
        selection_semantics: "choose Up/Down only when closed Binance spot and perpetual returns agree in sign and both clear the fixed horizon-specific magnitude; reject excessive spot-perpetual divergence, already-chased 15s Polymarket movement, non-executable books, and wide complementary spreads".to_string(),
        policy_grid_sha256: preregistration.policy_grid_sha256,
        policies_evaluated: preregistration.policies_evaluated,
        minimum_calibration_support: preregistration.cheap_screen.minimum_calibration_support,
        minimum_policy_support: preregistration.cheap_screen.minimum_policy_support,
        safety_margin: preregistration.cheap_screen.safety_margin,
        calibration_rows: feature_rows
            .iter()
            .filter(|row| row.chronological_window == "older")
            .count(),
        discovery_rows: feature_rows
            .iter()
            .filter(|row| row.chronological_window == "recent_discovery")
            .count(),
        fresh_holdout_rows: feature_rows
            .iter()
            .filter(|row| row.chronological_window == "fresh_holdout")
            .count(),
        complete_external_rows: feature_manifest.complete_external_rows,
        complete_pair_rows: feature_manifest.complete_pair_rows,
        eligible_policy_count,
        top_diagnostics: diagnostics,
        exact_replay_is_research_only: true,
        promotion_requires_wilson_after_exact_replay: true,
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

pub fn decide(input: CrossVenueDecisionInput) -> Result<CrossVenueDecisionReport> {
    if input.output_path == input.preregistration_path
        || input.output_path == input.search_report_path
        || input
            .exact_replay_report_path
            .as_ref()
            .is_some_and(|path| path == &input.output_path)
    {
        bail!("cross-venue decision output must not replace an input");
    }
    let preregistration_sha256 = sha256_file(&input.preregistration_path)?;
    let preregistration: CrossVenuePreregistration = serde_json::from_reader(
        File::open(&input.preregistration_path)
            .with_context(|| format!("open {}", input.preregistration_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.preregistration_path.display()))?;
    validate_preregistration(&preregistration)?;

    let search_sha256 = sha256_file(&input.search_report_path)?;
    let search: CrossVenueSearchReport = serde_json::from_reader(
        File::open(&input.search_report_path)
            .with_context(|| format!("open {}", input.search_report_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.search_report_path.display()))?;
    if search.schema_version != CROSS_VENUE_SEARCH_SCHEMA_VERSION
        || search.family_id != preregistration.family_id
        || search.preregistration.sha256 != preregistration_sha256
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
    {
        bail!("cross-venue search report drifted from preregistration");
    }

    let mut trace_decisions = Vec::new();
    let exact_source = if search.exact_replay_plan.entries.is_empty() {
        if input.exact_replay_report_path.is_some() {
            bail!("cross-venue exact replay supplied although cheap screen emitted no plan");
        }
        None
    } else {
        let exact_path = input
            .exact_replay_report_path
            .as_ref()
            .context("cross-venue exact replay is required by the bounded plan")?;
        let exact_sha256 = sha256_file(exact_path)?;
        let exact: OpportunityExactReplayReport = serde_json::from_reader(
            File::open(exact_path).with_context(|| format!("open {}", exact_path.display()))?,
        )
        .with_context(|| format!("parse {}", exact_path.display()))?;
        if exact.schema_version != OPPORTUNITY_EXACT_REPLAY_SCHEMA_VERSION
            || exact.dataset_seal != search.dataset_seal
            || exact.labels_manifest != search.labels_manifest
            || exact.policy_search_report.path != input.search_report_path.display().to_string()
            || exact.policy_search_report.sha256 != search_sha256
            || exact.fresh_holdout_outcomes_accessed
            || exact.traces.len() != search.exact_replay_plan.entries.len()
        {
            bail!("cross-venue exact replay does not match the search plan");
        }
        for trace in &exact.traces {
            let passed = trace.wilson_edge.is_some_and(|edge| {
                edge >= preregistration
                    .advancement_gate
                    .minimum_exact_replay_wilson_edge
            }) && trace.total_pnl_usd > 0.0;
            trace_decisions.push(CrossVenueTraceDecision {
                discovery_trace_sha256: trace.discovery_trace_sha256.clone(),
                representative_policy_id: trace.representative_policy_id.clone(),
                fills: trace.fills,
                wins: trace.wins,
                losses: trace.losses,
                wilson_edge: trace.wilson_edge,
                total_pnl_usd: trace.total_pnl_usd,
                decision: if passed { "advance" } else { "reject" }.to_string(),
            });
        }
        Some(HashedSource {
            path: exact_path.display().to_string(),
            sha256: exact_sha256,
        })
    };

    let fresh_gate_opened = !trace_decisions.is_empty()
        && trace_decisions
            .iter()
            .all(|trace| trace.decision == "advance");
    let (decision, reason) = if search.exact_replay_plan.entries.is_empty() {
        (
            "rejected",
            "No preregistered policy met older/recent support and point-edge gates; exact replay and fresh scoring remain closed.",
        )
    } else if fresh_gate_opened {
        (
            "advance_to_fresh_holdout",
            "Every bounded exact trace cleared the fixed Wilson-edge and positive-PnL gates; fresh outcomes may be evaluated separately.",
        )
    } else {
        (
            "rejected",
            "At least one bounded exact trace failed the fixed Wilson-edge or positive-PnL gate; fresh scoring remains closed.",
        )
    };
    let report = CrossVenueDecisionReport {
        schema_version: "cross_venue_lead_lag_decision_v1".to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        family_id: preregistration.family_id,
        preregistration: HashedSource {
            path: input.preregistration_path.display().to_string(),
            sha256: preregistration_sha256,
        },
        search_report: HashedSource {
            path: input.search_report_path.display().to_string(),
            sha256: search_sha256,
        },
        exact_replay_report: exact_source,
        fixed_advancement_wilson_edge: preregistration
            .advancement_gate
            .minimum_exact_replay_wilson_edge,
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
    consensus_return_sum: f64,
    pm_move_sum: f64,
    fresh_selections: Vec<(String, String)>,
}

struct FinishedPolicy {
    result: CrossVenuePolicyResult,
    discovery_ids: Vec<String>,
    discovery_tokens: BTreeMap<String, String>,
}

struct Selection<'a> {
    direction: &'static str,
    token_id: &'a str,
    break_even: f64,
    net_win: f64,
    max_loss: f64,
    consensus_return_bps: f64,
    directional_pm_move: f64,
}

fn policy_grid() -> Vec<CrossVenuePolicyDefinition> {
    let horizon_thresholds = [(1, [0.05, 0.25]), (5, [0.50, 1.00]), (15, [1.00, 2.00])];
    let mut policies = Vec::new();
    for decision_seconds in [120, 180, 240] {
        for (lookback_seconds, thresholds) in horizon_thresholds {
            for minimum_consensus_return_bps in thresholds {
                for maximum_directional_polymarket_move_15s in [0.03, 0.08] {
                    policies.push(CrossVenuePolicyDefinition {
                        decision_seconds,
                        lookback_seconds,
                        minimum_consensus_return_bps,
                        maximum_directional_polymarket_move_15s,
                        maximum_spot_perpetual_gap_bps: MAXIMUM_SPOT_PERPETUAL_GAP_BPS,
                        maximum_pair_spread: MAXIMUM_PAIR_SPREAD,
                    });
                }
            }
        }
    }
    policies
}

fn select<'a>(
    row: &'a CrossVenueFeatureRow,
    policy: &CrossVenuePolicyDefinition,
) -> Option<Selection<'a>> {
    if row.decision_seconds != policy.decision_seconds || !row.features.external_observable {
        return None;
    }
    let window = match policy.lookback_seconds {
        1 => row.features.window_1s.as_ref()?,
        5 => row.features.window_5s.as_ref()?,
        15 => row.features.window_15s.as_ref()?,
        _ => return None,
    };
    if window.spot_return_bps * window.perpetual_return_bps <= 0.0
        || window
            .spot_return_bps
            .abs()
            .min(window.perpetual_return_bps.abs())
            < policy.minimum_consensus_return_bps
        || window.spot_perpetual_return_gap_bps.abs() > policy.maximum_spot_perpetual_gap_bps
    {
        return None;
    }
    let upward = window.spot_return_bps > 0.0;
    let (direction, token_id, book, directional_pm_move) = if upward {
        (
            "Up",
            row.up_token_id.as_str(),
            &row.features.up_now,
            row.features.polymarket_up_midpoint_move_15s?,
        )
    } else {
        (
            "Down",
            row.down_token_id.as_str(),
            &row.features.down_now,
            row.features.polymarket_down_midpoint_move_15s?,
        )
    };
    let pair_spread = row.features.up_now.spread? + row.features.down_now.spread?;
    if !row.features.up_now.observable
        || !row.features.down_now.observable
        || !row.features.up_lookback_15s.observable
        || !row.features.down_lookback_15s.observable
        || !book.stake_fully_executable
        || book.best_ask? > MAXIMUM_EXECUTION_ASK
        || pair_spread > policy.maximum_pair_spread
        || directional_pm_move > policy.maximum_directional_polymarket_move_15s
    {
        return None;
    }
    Some(Selection {
        direction,
        token_id,
        break_even: book.fee_aware_break_even_probability?,
        net_win: book.fee_aware_net_win_usd?,
        max_loss: book.fee_aware_max_loss_usd?,
        consensus_return_bps: window
            .spot_return_bps
            .abs()
            .min(window.perpetual_return_bps.abs()),
        directional_pm_move,
    })
}

fn finish_policy(
    policy: CrossVenuePolicyDefinition,
    accumulator: PolicyAccumulator,
    gate: &CrossVenueCheapScreen,
) -> FinishedPolicy {
    let discovery_support = accumulator.discovery_wins + accumulator.discovery_losses;
    let calibration_win_rate = ratio(
        accumulator.calibration_wins,
        accumulator.calibration_support,
    );
    let calibration_break_even = average(
        accumulator.calibration_break_even_sum,
        accumulator.calibration_support,
    );
    let calibration_edge = difference(calibration_win_rate, calibration_break_even);
    let win_rate = ratio(accumulator.discovery_wins, discovery_support);
    let break_even = average(accumulator.discovery_break_even_sum, discovery_support);
    let point_edge = difference(win_rate, break_even);
    let wilson = (discovery_support > 0)
        .then(|| wilson_lower(accumulator.discovery_wins, discovery_support));
    let wilson_edge = difference(wilson, break_even);
    let discovery_trace_sha256 = (!accumulator.discovery_ids.is_empty()).then(|| {
        stable_json_hash(
            &accumulator
                .discovery_ids
                .iter()
                .map(|id| {
                    (
                        id,
                        accumulator
                            .discovery_tokens
                            .get(id)
                            .expect("discovery token exists"),
                    )
                })
                .collect::<Vec<_>>(),
        )
    });
    let fresh_support_trace_sha256 = (!accumulator.fresh_selections.is_empty())
        .then(|| stable_json_hash(&accumulator.fresh_selections));
    let mut rejection_reasons = Vec::new();
    if accumulator.calibration_support < gate.minimum_calibration_support {
        rejection_reasons.push("insufficient_older_support".to_string());
    }
    if calibration_edge.is_none_or(|edge| edge <= 0.0) {
        rejection_reasons.push("non_positive_older_point_edge".to_string());
    }
    if discovery_support < gate.minimum_policy_support {
        rejection_reasons.push("insufficient_recent_support".to_string());
    }
    if point_edge.is_none_or(|edge| edge <= gate.safety_margin) {
        rejection_reasons.push("recent_point_edge_below_safety_margin".to_string());
    }
    if accumulator.discovery_pnl_usd <= 0.0 {
        rejection_reasons.push("non_positive_top_quote_payoff_proxy".to_string());
    }
    let result = CrossVenuePolicyResult {
        policy_id: format!("cross-venue-{}", &stable_json_hash(&policy)[..12]),
        policy,
        calibration_support: accumulator.calibration_support,
        calibration_wins: accumulator.calibration_wins,
        calibration_win_rate,
        calibration_average_break_even_probability: calibration_break_even,
        calibration_point_estimate_edge: calibration_edge,
        discovery_support,
        wins: accumulator.discovery_wins,
        losses: accumulator.discovery_losses,
        win_rate,
        wilson_win_rate_lower: wilson,
        average_break_even_probability: break_even,
        point_estimate_edge: point_edge,
        wilson_edge,
        economic_payoff_proxy_usd: accumulator.discovery_pnl_usd,
        average_consensus_return_bps: average(accumulator.consensus_return_sum, discovery_support),
        average_directional_polymarket_move_15s: average(
            accumulator.pm_move_sum,
            discovery_support,
        ),
        fresh_causal_support: accumulator.fresh_selections.len(),
        discovery_trace_sha256,
        fresh_support_trace_sha256,
        discovery_eligible: rejection_reasons.is_empty(),
        rejection_reasons,
    };
    FinishedPolicy {
        result,
        discovery_ids: accumulator.discovery_ids,
        discovery_tokens: accumulator.discovery_tokens,
    }
}

fn validate_preregistration(preregistration: &CrossVenuePreregistration) -> Result<()> {
    if preregistration.schema_version != CROSS_VENUE_PREREGISTRATION_SCHEMA_VERSION
        || preregistration.family_id != "cross_venue_lead_lag_residual_v1"
        || preregistration.policy_grid != policy_grid()
        || preregistration.policy_grid_sha256 != stable_json_hash(&preregistration.policy_grid)
        || preregistration.policies_evaluated != preregistration.policy_grid.len()
        || preregistration.cheap_screen.minimum_calibration_support == 0
        || preregistration.cheap_screen.minimum_policy_support == 0
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
        || preregistration.label_access_audit
            != (LabelAccessAudit {
                label_artifacts_read: 0,
                outcomes_read: 0,
                scores_read: 0,
                pnl_read: 0,
            })
    {
        bail!("invalid or drifted cross-venue preregistration");
    }
    Ok(())
}

fn read_cross_venue_manifest(path: &Path) -> Result<CrossVenueFeatureStoreManifest> {
    serde_json::from_reader(File::open(path).with_context(|| format!("open {}", path.display()))?)
        .with_context(|| format!("parse {}", path.display()))
}

pub(crate) fn validate_cross_venue_manifest(
    manifest: &CrossVenueFeatureStoreManifest,
    dataset_sha256: &str,
    dataset_seal_sha256: &str,
) -> Result<()> {
    if manifest.schema_version != CROSS_VENUE_FEATURE_STORE_SCHEMA_VERSION
        || manifest.dataset_sha256 != dataset_sha256
        || manifest.dataset_seal.sha256 != dataset_seal_sha256
        || manifest.plugin.plugin_id != CROSS_VENUE_PLUGIN_ID
        || manifest.plugin.plugin_version != CROSS_VENUE_PLUGIN_VERSION
        || manifest.plugin.causal_windows_ms != [WINDOW_1S_MS, WINDOW_5S_MS, WINDOW_15S_MS]
        || manifest.outcome_columns_present
        || manifest.labels_or_scores_influence_output
        || !manifest.external_market_data_influence_output
        || manifest.source_date_reads != manifest.source_tape_partitions.len()
    {
        bail!("cross-venue feature manifest violates the plugin contract");
    }
    Ok(())
}

fn selected_won(terminal_direction: &str, selected_direction: &str) -> Option<bool> {
    match terminal_direction.to_ascii_lowercase().as_str() {
        "up" => Some(selected_direction == "Up"),
        "down" => Some(selected_direction == "Down"),
        _ => None,
    }
}

fn average(sum: f64, count: usize) -> Option<f64> {
    (count > 0).then_some(sum / count as f64)
}

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}

fn difference(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    Some(left? - right?)
}

fn wilson_lower(wins: usize, support: usize) -> f64 {
    if support == 0 {
        return 0.0;
    }
    let n = support as f64;
    let p = wins as f64 / n;
    let z = 1.959_963_984_540_054;
    let denominator = 1.0 + z * z / n;
    let center = p + z * z / (2.0 * n);
    let margin = z * ((p * (1.0 - p) / n) + z * z / (4.0 * n * n)).sqrt();
    (center - margin) / denominator
}

fn diagnostic_order(left: &CrossVenuePolicyResult, right: &CrossVenuePolicyResult) -> Ordering {
    right
        .discovery_eligible
        .cmp(&left.discovery_eligible)
        .then_with(|| compare_optional_desc(left.point_estimate_edge, right.point_estimate_edge))
        .then_with(|| right.discovery_support.cmp(&left.discovery_support))
        .then_with(|| left.policy_id.cmp(&right.policy_id))
}

fn compare_optional_desc(left: Option<f64>, right: Option<f64>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => right.partial_cmp(&left).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

type ExternalMeasurement = (
    bool,
    Option<i64>,
    Option<i64>,
    Option<CrossVenueWindowFeatures>,
    Option<CrossVenueWindowFeatures>,
    Option<CrossVenueWindowFeatures>,
);

fn measure_external(tape: &[TapeRow], observed_at_ms: i64) -> ExternalMeasurement {
    let end_exclusive = tape.partition_point(|row| row.available_at_ms <= observed_at_ms);
    let Some(end_index) = end_exclusive.checked_sub(1) else {
        return (false, None, None, None, None, None);
    };
    let end = &tape[end_index];
    let perpetual_age = end
        .perpetual_last_trade_ms
        .map(|timestamp| observed_at_ms.saturating_sub(timestamp));
    let one = measure_window(tape, end_index, observed_at_ms, WINDOW_1S_MS);
    let five = measure_window(tape, end_index, observed_at_ms, WINDOW_5S_MS);
    let fifteen = measure_window(tape, end_index, observed_at_ms, WINDOW_15S_MS);
    let observable = one.is_some() && five.is_some() && fifteen.is_some();
    (
        observable,
        Some(end.available_at_ms),
        perpetual_age,
        one,
        five,
        fifteen,
    )
}

fn measure_window(
    tape: &[TapeRow],
    end_index: usize,
    observed_at_ms: i64,
    window_ms: i64,
) -> Option<CrossVenueWindowFeatures> {
    let start_exclusive =
        tape.partition_point(|row| row.available_at_ms <= observed_at_ms.saturating_sub(window_ms));
    let start_index = start_exclusive.checked_sub(1)?;
    if start_index >= end_index {
        return None;
    }
    let start = &tape[start_index];
    let end = &tape[end_index];
    let perpetual_start = start.perpetual_close?;
    let perpetual_end = end.perpetual_close?;
    if start.spot_close <= 0.0 || perpetual_start <= 0.0 {
        return None;
    }
    let interval = &tape[start_index + 1..=end_index];
    let spot_quote_volume = interval.iter().map(|row| row.spot_quote_volume).sum();
    let spot_taker_buy: f64 = interval
        .iter()
        .map(|row| row.spot_taker_buy_quote_volume)
        .sum();
    let perpetual_quote_volume = interval.iter().map(|row| row.perpetual_quote_volume).sum();
    let perpetual_taker_buy: f64 = interval
        .iter()
        .map(|row| row.perpetual_taker_buy_quote_volume)
        .sum();
    let spot_return_bps = (end.spot_close / start.spot_close - 1.0) * 10_000.0;
    let perpetual_return_bps = (perpetual_end / perpetual_start - 1.0) * 10_000.0;
    Some(CrossVenueWindowFeatures {
        window_ms,
        spot_return_bps,
        perpetual_return_bps,
        spot_perpetual_return_gap_bps: spot_return_bps - perpetual_return_bps,
        spot_quote_volume_usd: spot_quote_volume,
        spot_signed_taker_quote_usd: 2.0 * spot_taker_buy - spot_quote_volume,
        perpetual_quote_volume_usd: perpetual_quote_volume,
        perpetual_signed_taker_quote_usd: 2.0 * perpetual_taker_buy - perpetual_quote_volume,
        spot_trade_count: interval.iter().map(|row| row.spot_trade_count).sum(),
        perpetual_trade_count: interval.iter().map(|row| row.perpetual_trade_count).sum(),
    })
}

fn read_tape(path: &Path) -> Result<Vec<TapeRow>> {
    let source = File::open(path).with_context(|| format!("open tape {}", path.display()))?;
    let decoder = GzDecoder::new(source);
    let mut reader = csv::Reader::from_reader(decoder);
    let mut rows = Vec::new();
    let mut previous_start = None;
    for row in reader.deserialize::<TapeRow>() {
        let row = row.with_context(|| format!("parse tape {}", path.display()))?;
        if row.available_at_ms != row.second_start_ms + 1_000
            || previous_start.is_some_and(|previous| previous >= row.second_start_ms)
            || row.perpetual_aggregate_count > row.perpetual_trade_count
        {
            bail!(
                "tape ordering or causal timestamp contract failed at {}",
                path.display()
            );
        }
        previous_start = Some(row.second_start_ms);
        rows.push(row);
    }
    if rows.is_empty() {
        bail!("source tape is empty at {}", path.display());
    }
    Ok(rows)
}

fn index_pairs(
    opportunities: &[super::opportunity_dataset::CausalOpportunity],
    pairs: Vec<OpportunityPairFeature>,
) -> Result<HashMap<String, OpportunityPairFeature>> {
    let opportunities_by_id = opportunities
        .iter()
        .map(|row| (row.opportunity_id.as_str(), row))
        .collect::<HashMap<_, _>>();
    let mut indexed = HashMap::new();
    for pair in pairs {
        let opportunity = opportunities_by_id
            .get(pair.source_opportunity_id.as_str())
            .context("paired feature references unknown opportunity")?;
        if pair.condition_id != opportunity.condition_id
            || pair.observed_at_ms != opportunity.observed_at_ms
            || pair.chronological_window != opportunity.chronological_window
            || indexed
                .insert(pair.source_opportunity_id.clone(), pair)
                .is_some()
        {
            bail!("paired-feature coordinate drifted or duplicated");
        }
    }
    Ok(indexed)
}

fn validate_paired_manifest(
    manifest: &OpportunityPairFeatureManifest,
    dataset_sha256: &str,
    dataset_seal_sha256: &str,
) -> Result<()> {
    if manifest.schema_version != OPPORTUNITY_PAIR_FEATURE_SCHEMA_VERSION
        || manifest.dataset_sha256 != dataset_sha256
        || manifest.dataset_seal.sha256 != dataset_seal_sha256
        || manifest.lookback_ms != WINDOW_15S_MS
        || manifest.outcome_columns_present
        || manifest.gamma_outcome_prices_influence_output
        || manifest.btc_or_model_features_influence_output
        || manifest.source_pmxt_scans != manifest.source_pmxt_hours.len()
    {
        bail!("paired-feature manifest violates the cross-venue contract");
    }
    Ok(())
}

fn validate_tape_manifest(manifest: &SourceTapeManifest, seal_sha256: &str) -> Result<()> {
    let audit = &manifest.label_access_audit;
    if manifest.schema_version != "binance_cross_venue_tape_v1"
        || manifest.dataset_seal.sha256 != seal_sha256
        || manifest.status != "ready"
        || !manifest.quality.ready_for_feature_join
        || audit.label_artifacts_read != 0
        || audit.outcomes_read != 0
        || audit.scores_read != 0
        || audit.pnl_read != 0
        || manifest.partitions.is_empty()
        || manifest.partitions.iter().any(|partition| {
            partition.expected_seconds != 86_400
                || partition.spot_missing_seconds != 0
                || partition.aligned_output_rows != 86_400
        })
    {
        bail!("source tape manifest violates the outcome-free causal contract");
    }
    let mut dates = HashSet::new();
    if manifest
        .partitions
        .iter()
        .any(|partition| !dates.insert(partition.date.as_str()))
    {
        bail!("source tape manifest has duplicate dates");
    }
    Ok(())
}

fn validate_distinct_paths(input: &CrossVenueFeatureInput) -> Result<()> {
    let paths = [
        &input.dataset_seal_path,
        &input.paired_features_manifest_path,
        &input.source_tape_manifest_path,
        &input.output_path,
        &input.manifest_path,
    ];
    let unique = paths.iter().collect::<HashSet<_>>();
    if unique.len() != paths.len() {
        bail!("cross-venue inputs and outputs must be distinct");
    }
    Ok(())
}

fn timestamp_date(timestamp_ms: i64) -> Result<String> {
    let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .context("observation timestamp is outside chrono range")?;
    Ok(timestamp.format("%Y-%m-%d").to_string())
}

fn midpoint_move(lookback: &PairBookFeature, now: &PairBookFeature) -> Option<f64> {
    Some(now.midpoint? - lookback.midpoint?)
}

#[cfg(test)]
fn empty_pair_book() -> PairBookFeature {
    PairBookFeature {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tape_row(second: i64, spot: f64, perpetual: f64, buy_fraction: f64) -> TapeRow {
        TapeRow {
            second_start_ms: second,
            available_at_ms: second + 1_000,
            spot_close: spot,
            spot_quote_volume: 100.0,
            spot_taker_buy_quote_volume: 100.0 * buy_fraction,
            spot_trade_count: 2,
            perpetual_close: Some(perpetual),
            perpetual_last_trade_ms: Some(second + 900),
            perpetual_quote_volume: 200.0,
            perpetual_taker_buy_quote_volume: 200.0 * buy_fraction,
            perpetual_trade_count: 3,
            perpetual_aggregate_count: 2,
        }
    }

    #[test]
    fn source_second_is_not_visible_before_close_boundary() {
        let tape = vec![
            tape_row(0, 100.0, 100.0, 0.5),
            tape_row(1_000, 101.0, 102.0, 0.75),
        ];
        let early = measure_external(&tape, 1_999);
        assert_eq!(early.1, Some(1_000));
        assert!(early.3.is_none());
        let closed = measure_external(&tape, 2_000);
        let window = closed.3.unwrap();
        assert!((window.spot_return_bps - 100.0).abs() < 1e-9);
        assert_eq!(window.spot_signed_taker_quote_usd, 50.0);
        assert_eq!(window.perpetual_signed_taker_quote_usd, 100.0);
    }

    #[test]
    fn midpoint_move_requires_both_observations() {
        let missing = empty_pair_book();
        assert_eq!(midpoint_move(&missing, &missing), None);
    }

    #[test]
    fn policy_grid_is_fixed_and_bounded() {
        let policies = policy_grid();
        assert_eq!(policies.len(), 36);
        assert_eq!(
            policies
                .iter()
                .filter(|policy| policy.lookback_seconds == 1)
                .count(),
            12
        );
        assert_eq!(
            policies
                .iter()
                .filter(|policy| policy.lookback_seconds == 5)
                .count(),
            12
        );
        assert_eq!(
            policies
                .iter()
                .filter(|policy| policy.lookback_seconds == 15)
                .count(),
            12
        );
    }
}
