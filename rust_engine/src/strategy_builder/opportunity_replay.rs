//! One-pass latency replay for the bounded opportunity-policy shortlist.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::artifact::write_json_artifact_atomic;
use crate::backtest::l2_replay::TokenBook;
use crate::backtest::pmxt::{L2EventBody, PMXTv2Loader};
use crate::execution::fees::polymarket_fee;

use super::opportunity_cross_venue::{
    validate_cross_venue_manifest, CrossVenueFeatureStoreManifest, CrossVenuePairFeatures,
    CROSS_VENUE_SEARCH_SCHEMA_VERSION,
};
use super::opportunity_dataset::{
    load_sealed_opportunities, read_labels, sha256_file, CausalOpportunity,
    OpportunityLabelsManifest,
};
use super::opportunity_feature_store::{
    read_feature_store_rows, validate_outcome_free_manifest, OpportunityFeatureStoreManifest,
};
use super::opportunity_flow::{
    OrderFlowPairFeatures, OPPORTUNITY_FLOW_SEARCH_SCHEMA_VERSION, ORDER_FLOW_PLUGIN_ID,
    ORDER_FLOW_PLUGIN_VERSION,
};
use super::opportunity_liquidity::{
    read_pair_features, OpportunityPairFeatureManifest,
    OPPORTUNITY_LIQUIDITY_SEARCH_SCHEMA_VERSION, OPPORTUNITY_PAIR_FEATURE_SCHEMA_VERSION,
};
use super::opportunity_policy::{
    ExactReplayPlan, ExactReplayPlanEntry, OPPORTUNITY_POLICY_SEARCH_SCHEMA_VERSION,
};
use super::opportunity_probability::OPPORTUNITY_PROBABILITY_SEARCH_SCHEMA_VERSION;
use super::opportunity_table::HashedSource;

pub const OPPORTUNITY_EXACT_REPLAY_SCHEMA_VERSION: &str = "opportunity_exact_replay_v1";

#[derive(Debug, Clone)]
pub struct OpportunityExactReplayInput {
    pub dataset_seal_path: PathBuf,
    pub labels_manifest_path: PathBuf,
    pub policy_search_report_path: PathBuf,
    pub cache_dir: PathBuf,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplaySourceHour {
    pub hour: String,
    pub pmxt_parquet: HashedSource,
    pub target_condition_count: usize,
    pub decoded_target_events: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityReplayFill {
    pub opportunity_id: String,
    pub observed_at_ms: i64,
    pub order_arrival_at_ms: i64,
    pub condition_id: String,
    pub token_id: String,
    pub status: String,
    pub book_timestamp_ms: Option<i64>,
    pub book_age_ms: Option<i64>,
    pub best_ask: Option<f64>,
    pub average_entry_price: Option<f64>,
    pub executable_cost_usd: Option<f64>,
    pub executable_shares: Option<f64>,
    pub taker_fee_usd: Option<f64>,
    pub fee_aware_break_even_probability: Option<f64>,
    pub won: Option<bool>,
    pub pnl_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityReplayTraceReport {
    pub discovery_trace_sha256: String,
    pub representative_policy_id: String,
    pub decision_seconds: u16,
    pub maximum_ask: f64,
    pub latency_ms: u64,
    pub requested_opportunities: usize,
    pub fills: usize,
    pub fill_rate: f64,
    pub rejection_counts: BTreeMap<String, usize>,
    pub wins: usize,
    pub losses: usize,
    pub win_rate: Option<f64>,
    pub wilson_win_rate_lower: Option<f64>,
    pub average_break_even_probability: Option<f64>,
    pub point_estimate_edge: Option<f64>,
    pub wilson_edge: Option<f64>,
    pub total_pnl_usd: f64,
    pub promotion_confidence_ready: bool,
    pub status: String,
    pub rows: Vec<OpportunityReplayFill>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityExactReplayReport {
    pub schema_version: String,
    pub generated_at: String,
    pub dataset_seal: HashedSource,
    pub labels_manifest: HashedSource,
    pub policy_search_report: HashedSource,
    pub source_pmxt_hours: Vec<ReplaySourceHour>,
    pub source_pmxt_scans: usize,
    pub duplicate_hour_scans_avoided: usize,
    pub execution_semantics: String,
    pub label_semantics: String,
    pub fresh_holdout_outcomes_accessed: bool,
    pub traces: Vec<OpportunityReplayTraceReport>,
    pub verdict: String,
}

#[derive(Debug, Clone)]
struct ReplayTarget {
    trace_index: usize,
    opportunity: CausalOpportunity,
    order_arrival_at_ms: i64,
    maximum_ask: f64,
    won: Option<bool>,
}

#[derive(Debug, Clone, Copy)]
struct BuyMeasurement {
    book_timestamp_ms: i64,
    best_ask: f64,
    average_entry_price: f64,
    cost: f64,
    shares: f64,
    fee: f64,
    break_even_probability: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct OpportunityReplayPlanSource {
    schema_version: String,
    dataset_seal: HashedSource,
    labels_manifest: HashedSource,
    dataset_sha256: String,
    fresh_holdout_outcomes_accessed: bool,
    safety_margin: f64,
    exact_replay_is_research_only: bool,
    #[serde(default)]
    paired_features: Option<HashedSource>,
    #[serde(default)]
    feature_store: Option<HashedSource>,
    exact_replay_plan: ExactReplayPlan,
}

#[derive(Debug, Clone, Copy)]
enum BuyMeasurementOutcome {
    Filled(BuyMeasurement),
    Rejected(&'static str),
}

pub fn replay(input: OpportunityExactReplayInput) -> Result<OpportunityExactReplayReport> {
    validate_input(&input)?;
    let dataset_seal_sha256 = sha256_file(&input.dataset_seal_path)?;
    let (dataset_seal, opportunities) = load_sealed_opportunities(&input.dataset_seal_path)?;
    let labels_manifest_sha256 = sha256_file(&input.labels_manifest_path)?;
    let labels_manifest: OpportunityLabelsManifest = serde_json::from_reader(
        File::open(&input.labels_manifest_path)
            .with_context(|| format!("open {}", input.labels_manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.labels_manifest_path.display()))?;
    if labels_manifest.dataset_seal.sha256 != dataset_seal_sha256
        || labels_manifest.dataset_sha256 != dataset_seal.dataset_sha256
        || labels_manifest.fresh_holdout_labels_present
    {
        bail!("label manifest does not belong to the supplied outcome-safe dataset");
    }
    let label_path = PathBuf::from(&labels_manifest.output.path);
    if sha256_file(&label_path)? != labels_manifest.output.sha256 {
        bail!("label table hash drifted");
    }
    let labels = read_labels(&label_path)?;
    let labels_by_id = labels
        .iter()
        .map(|label| (label.opportunity_id.as_str(), label))
        .collect::<HashMap<_, _>>();

    let policy_search_report_sha256 = sha256_file(&input.policy_search_report_path)?;
    let policy_search: OpportunityReplayPlanSource = serde_json::from_reader(
        File::open(&input.policy_search_report_path)
            .with_context(|| format!("open {}", input.policy_search_report_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.policy_search_report_path.display()))?;
    validate_policy_search(
        &policy_search,
        &dataset_seal_sha256,
        &labels_manifest_sha256,
        &dataset_seal.dataset_sha256,
    )?;
    let pair_tokens_by_id = validate_pair_source(&policy_search, &dataset_seal_sha256)?;
    if policy_search.exact_replay_plan.entries.iter().any(|entry| {
        (entry.stake_usd - dataset_seal.stake_usd).abs() > 1e-12
            || (entry.fee_rate - dataset_seal.fee_rate).abs() > 1e-12
    }) {
        bail!("exact-replay plan execution settings drifted from the dataset seal");
    }

    let opportunity_by_id = opportunities
        .into_iter()
        .map(|row| (row.opportunity_id.clone(), row))
        .collect::<HashMap<_, _>>();
    let mut targets_by_hour = BTreeMap::<i64, Vec<ReplayTarget>>::new();
    let mut requested_hour_reads = 0usize;
    for (trace_index, entry) in policy_search.exact_replay_plan.entries.iter().enumerate() {
        let mut seen = HashSet::new();
        let uses_complementary_tokens = matches!(
            policy_search.schema_version.as_str(),
            OPPORTUNITY_LIQUIDITY_SEARCH_SCHEMA_VERSION
                | OPPORTUNITY_FLOW_SEARCH_SCHEMA_VERSION
                | CROSS_VENUE_SEARCH_SCHEMA_VERSION
        );
        if uses_complementary_tokens
            && (entry.token_overrides.len() != entry.opportunity_ids.len()
                || entry
                    .token_overrides
                    .keys()
                    .any(|id| !entry.opportunity_ids.contains(id)))
        {
            bail!("paired exact-replay token overrides do not match opportunity IDs");
        }
        if !uses_complementary_tokens && !entry.token_overrides.is_empty() {
            bail!("token overrides are restricted to paired-token search reports");
        }
        let latency_ms = i64::try_from(entry.latency_ms).context("latency exceeds i64")?;
        let mut trace_hours = HashSet::new();
        for opportunity_id in &entry.opportunity_ids {
            if !seen.insert(opportunity_id) {
                bail!("duplicate opportunity_id in exact-replay trace");
            }
            let source_opportunity = opportunity_by_id
                .get(opportunity_id)
                .with_context(|| format!("unknown opportunity_id {opportunity_id}"))?;
            if source_opportunity.chronological_window != "recent_discovery" {
                bail!("exact-replay plan may contain only recent_discovery opportunities");
            }
            if (source_opportunity.elapsed_seconds - f64::from(entry.decision_seconds)).abs()
                > 0.001
            {
                bail!("exact-replay decision time does not match sealed opportunity");
            }
            let label = labels_by_id
                .get(opportunity_id.as_str())
                .context("exact-replay opportunity has no discovery label")?;
            let selected_token = entry
                .token_overrides
                .get(opportunity_id)
                .map(String::as_str)
                .unwrap_or(source_opportunity.token_id.as_str());
            if uses_complementary_tokens {
                let pair = pair_tokens_by_id
                    .get(opportunity_id)
                    .context("paired replay opportunity missing from feature source")?;
                if selected_token != pair.0 && selected_token != pair.1 {
                    bail!("paired replay selected a token outside the sealed pair");
                }
            }
            let won = selected_token_outcome(
                source_opportunity.token_id.as_str(),
                selected_token,
                label.won,
            );
            let mut opportunity = source_opportunity.clone();
            opportunity.token_id = selected_token.to_string();
            let hour_ms = opportunity.observed_at_ms.div_euclid(3_600_000) * 3_600_000;
            trace_hours.insert(hour_ms);
            targets_by_hour
                .entry(hour_ms)
                .or_default()
                .push(ReplayTarget {
                    trace_index,
                    opportunity: opportunity.clone(),
                    order_arrival_at_ms: opportunity.observed_at_ms + latency_ms,
                    maximum_ask: entry.maximum_ask,
                    won,
                });
        }
        requested_hour_reads += trace_hours.len();
    }

    let loader = PMXTv2Loader::new(&input.cache_dir);
    let mut source_pmxt_hours = Vec::new();
    let mut replay_rows =
        vec![Vec::<OpportunityReplayFill>::new(); policy_search.exact_replay_plan.entries.len()];
    for (hour_ms, mut targets) in targets_by_hour {
        targets.sort_by(|left, right| {
            left.order_arrival_at_ms
                .cmp(&right.order_arrival_at_ms)
                .then_with(|| left.trace_index.cmp(&right.trace_index))
                .then_with(|| {
                    left.opportunity
                        .opportunity_id
                        .cmp(&right.opportunity.opportunity_id)
                })
        });
        let hour = DateTime::<Utc>::from_timestamp_millis(hour_ms)
            .context("opportunity hour is outside chrono range")?;
        let condition_ids = targets
            .iter()
            .map(|target| target.opportunity.condition_id.clone())
            .collect::<HashSet<_>>();
        let pmxt_path = loader.cache_path_for_hour(hour);
        if !pmxt_path.is_file() {
            bail!(
                "exact replay requires cached PMXT hour at {}",
                pmxt_path.display()
            );
        }
        let pmxt_sha256 = sha256_file(&pmxt_path)?;
        let events = loader
            .load_cached_hour(hour, Some(&condition_ids))
            .with_context(|| format!("row-filter exact-replay PMXT hour {hour}"))?;
        if events.is_empty() {
            bail!("exact-replay PMXT hour {hour} contains zero target events");
        }
        source_pmxt_hours.push(ReplaySourceHour {
            hour: hour.to_rfc3339_opts(SecondsFormat::Secs, true),
            pmxt_parquet: HashedSource {
                path: pmxt_path.display().to_string(),
                sha256: pmxt_sha256,
            },
            target_condition_count: condition_ids.len(),
            decoded_target_events: events.len(),
        });

        let mut event_index = 0usize;
        let mut books = HashMap::<String, TokenBook>::new();
        for target in targets {
            let arrival_s = target.order_arrival_at_ms as f64 / 1_000.0;
            // Match the engine's pending-order semantics: an order arriving at
            // time T executes before a book event stamped exactly T.
            while event_index < events.len() && events[event_index].timestamp_s < arrival_s {
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
            let measurement = match books.get(&target.opportunity.token_id) {
                Some(book) => measure_buy(
                    book,
                    target.maximum_ask,
                    dataset_seal.stake_usd,
                    dataset_seal.fee_rate,
                )?,
                None => BuyMeasurementOutcome::Rejected("no_book_at_order_arrival"),
            };
            let won = target.won;
            let row = build_replay_row(target, measurement, won)?;
            replay_rows[row.0].push(row.1);
        }
    }
    source_pmxt_hours.sort_by(|left, right| left.hour.cmp(&right.hour));

    let traces = policy_search
        .exact_replay_plan
        .entries
        .iter()
        .cloned()
        .zip(replay_rows)
        .map(|(entry, rows)| summarize_trace(entry, rows, policy_search.safety_margin))
        .collect::<Vec<_>>();
    let verdict = if traces.iter().any(|trace| trace.promotion_confidence_ready) {
        "replay_confidence_ready_for_fresh_gate"
    } else if traces.iter().any(|trace| {
        trace.total_pnl_usd > 0.0
            && trace
                .point_estimate_edge
                .is_some_and(|edge| edge > policy_search.safety_margin)
    }) {
        "research_signal_retained_more_evidence_required"
    } else {
        "shortlist_rejected_by_latency_replay"
    };
    let source_pmxt_scans = source_pmxt_hours.len();
    let report = OpportunityExactReplayReport {
        schema_version: OPPORTUNITY_EXACT_REPLAY_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        dataset_seal: HashedSource {
            path: input.dataset_seal_path.display().to_string(),
            sha256: dataset_seal_sha256,
        },
        labels_manifest: HashedSource {
            path: input.labels_manifest_path.display().to_string(),
            sha256: labels_manifest_sha256,
        },
        policy_search_report: HashedSource {
            path: input.policy_search_report_path.display().to_string(),
            sha256: policy_search_report_sha256,
        },
        source_pmxt_hours,
        source_pmxt_scans,
        duplicate_hour_scans_avoided: requested_hour_reads.saturating_sub(source_pmxt_scans),
        execution_semantics: "one row-filtered PMXT scan per UTC hour; buy FOK at observed_at + pinned latency; pending order executes before same-timestamp book update; visible asks above maximum_ask are rejected".to_string(),
        label_semantics: labels_manifest.resolution_semantics,
        fresh_holdout_outcomes_accessed: false,
        traces,
        verdict: verdict.to_string(),
    };
    write_json_artifact_atomic(&input.output_path, &report)?;
    Ok(report)
}

fn validate_input(input: &OpportunityExactReplayInput) -> Result<()> {
    if input.output_path == input.dataset_seal_path
        || input.output_path == input.labels_manifest_path
        || input.output_path == input.policy_search_report_path
    {
        bail!("exact-replay output must not replace an input");
    }
    if !input.cache_dir.is_dir() {
        bail!("exact replay requires an existing PMXT cache directory");
    }
    Ok(())
}

fn selected_token_outcome(
    source_token: &str,
    selected_token: &str,
    source_won: Option<bool>,
) -> Option<bool> {
    source_won.map(|won| {
        if source_token == selected_token {
            won
        } else {
            !won
        }
    })
}

fn validate_pair_source(
    report: &OpportunityReplayPlanSource,
    dataset_seal_sha256: &str,
) -> Result<HashMap<String, (String, String)>> {
    if report.schema_version == OPPORTUNITY_LIQUIDITY_SEARCH_SCHEMA_VERSION {
        if report.feature_store.is_some() {
            bail!("liquidity replay report unexpectedly pins a feature store");
        }
        return validate_liquidity_pair_source(report, dataset_seal_sha256);
    }
    if report.schema_version == OPPORTUNITY_FLOW_SEARCH_SCHEMA_VERSION {
        if report.paired_features.is_some() {
            bail!("order-flow replay report unexpectedly pins liquidity features");
        }
        return validate_flow_pair_source(report, dataset_seal_sha256);
    }
    if report.schema_version == CROSS_VENUE_SEARCH_SCHEMA_VERSION {
        if report.paired_features.is_some() {
            bail!("cross-venue replay report unexpectedly pins liquidity features");
        }
        return validate_cross_venue_pair_source(report, dataset_seal_sha256);
    }
    if report.paired_features.is_some() || report.feature_store.is_some() {
        bail!("non-paired replay report unexpectedly pins paired features");
    }
    Ok(HashMap::new())
}

fn validate_liquidity_pair_source(
    report: &OpportunityReplayPlanSource,
    dataset_seal_sha256: &str,
) -> Result<HashMap<String, (String, String)>> {
    let source = report
        .paired_features
        .as_ref()
        .context("liquidity replay report does not pin paired features")?;
    let manifest_path = PathBuf::from(&source.path);
    if sha256_file(&manifest_path)? != source.sha256 {
        bail!("paired-feature manifest hash drifted");
    }
    let manifest: OpportunityPairFeatureManifest = serde_json::from_reader(
        File::open(&manifest_path).with_context(|| format!("open {}", manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", manifest_path.display()))?;
    if manifest.schema_version != OPPORTUNITY_PAIR_FEATURE_SCHEMA_VERSION
        || manifest.dataset_seal.sha256 != dataset_seal_sha256
        || manifest.dataset_sha256 != report.dataset_sha256
        || manifest.outcome_columns_present
        || manifest.gamma_outcome_prices_influence_output
        || manifest.btc_or_model_features_influence_output
    {
        bail!("paired-feature manifest is incompatible with liquidity replay");
    }
    let feature_path = PathBuf::from(&manifest.output.path);
    if sha256_file(&feature_path)? != manifest.output.sha256 {
        bail!("paired-feature output hash drifted");
    }
    let rows = read_pair_features(&feature_path)?;
    let mut pairs = HashMap::new();
    for row in rows {
        if pairs
            .insert(
                row.source_opportunity_id,
                (row.up_token_id, row.down_token_id),
            )
            .is_some()
        {
            bail!("duplicate source opportunity in paired features");
        }
    }
    Ok(pairs)
}

fn validate_flow_pair_source(
    report: &OpportunityReplayPlanSource,
    dataset_seal_sha256: &str,
) -> Result<HashMap<String, (String, String)>> {
    let source = report
        .feature_store
        .as_ref()
        .context("order-flow replay report does not pin a feature store")?;
    let manifest_path = PathBuf::from(&source.path);
    if sha256_file(&manifest_path)? != source.sha256 {
        bail!("order-flow feature-store manifest hash drifted");
    }
    let manifest: OpportunityFeatureStoreManifest = serde_json::from_reader(
        File::open(&manifest_path).with_context(|| format!("open {}", manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", manifest_path.display()))?;
    validate_outcome_free_manifest(
        &manifest,
        ORDER_FLOW_PLUGIN_ID,
        ORDER_FLOW_PLUGIN_VERSION,
        &report.dataset_sha256,
        dataset_seal_sha256,
    )?;
    let feature_path = PathBuf::from(&manifest.output.path);
    if sha256_file(&feature_path)? != manifest.output.sha256 {
        bail!("order-flow feature-store output hash drifted");
    }
    let rows = read_feature_store_rows::<OrderFlowPairFeatures>(&feature_path)?;
    let mut pairs = HashMap::new();
    for row in rows {
        if pairs
            .insert(
                row.source_opportunity_id,
                (row.up_token_id, row.down_token_id),
            )
            .is_some()
        {
            bail!("duplicate source opportunity in order-flow feature store");
        }
    }
    Ok(pairs)
}

fn validate_cross_venue_pair_source(
    report: &OpportunityReplayPlanSource,
    dataset_seal_sha256: &str,
) -> Result<HashMap<String, (String, String)>> {
    let source = report
        .feature_store
        .as_ref()
        .context("cross-venue replay report does not pin a feature store")?;
    let manifest_path = PathBuf::from(&source.path);
    if sha256_file(&manifest_path)? != source.sha256 {
        bail!("cross-venue feature-store manifest hash drifted");
    }
    let manifest: CrossVenueFeatureStoreManifest = serde_json::from_reader(
        File::open(&manifest_path).with_context(|| format!("open {}", manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", manifest_path.display()))?;
    validate_cross_venue_manifest(&manifest, &report.dataset_sha256, dataset_seal_sha256)?;
    let feature_path = PathBuf::from(&manifest.output.path);
    if sha256_file(&feature_path)? != manifest.output.sha256 {
        bail!("cross-venue feature-store output hash drifted");
    }
    let rows = read_feature_store_rows::<CrossVenuePairFeatures>(&feature_path)?;
    let mut pairs = HashMap::new();
    for row in rows {
        if pairs
            .insert(
                row.source_opportunity_id,
                (row.up_token_id, row.down_token_id),
            )
            .is_some()
        {
            bail!("duplicate source opportunity in cross-venue feature store");
        }
    }
    Ok(pairs)
}

fn validate_policy_search(
    report: &OpportunityReplayPlanSource,
    dataset_seal_sha256: &str,
    labels_manifest_sha256: &str,
    dataset_sha256: &str,
) -> Result<()> {
    if ![
        OPPORTUNITY_POLICY_SEARCH_SCHEMA_VERSION,
        OPPORTUNITY_PROBABILITY_SEARCH_SCHEMA_VERSION,
        OPPORTUNITY_LIQUIDITY_SEARCH_SCHEMA_VERSION,
        OPPORTUNITY_FLOW_SEARCH_SCHEMA_VERSION,
        CROSS_VENUE_SEARCH_SCHEMA_VERSION,
    ]
    .contains(&report.schema_version.as_str())
        || !report.exact_replay_is_research_only
        || report.fresh_holdout_outcomes_accessed
        || report.dataset_seal.sha256 != dataset_seal_sha256
        || report.labels_manifest.sha256 != labels_manifest_sha256
        || report.dataset_sha256 != dataset_sha256
    {
        bail!("policy-search report does not match the supplied sealed inputs");
    }
    if report.exact_replay_plan.entries.is_empty()
        || report.exact_replay_plan.entries.len() > report.exact_replay_plan.maximum_replay_count
        || report.exact_replay_plan.entries.len() != report.exact_replay_plan.unique_replay_count
    {
        bail!("policy-search report has no valid bounded exact-replay plan");
    }
    Ok(())
}

fn measure_buy(
    book: &TokenBook,
    maximum_ask: f64,
    stake_usd: f64,
    fee_rate: f64,
) -> Result<BuyMeasurementOutcome> {
    if !(maximum_ask.is_finite() && maximum_ask > 0.0 && maximum_ask <= 1.0) {
        bail!("maximum_ask must be in (0, 1]");
    }
    if !(stake_usd.is_finite() && stake_usd > 0.0 && fee_rate.is_finite() && fee_rate >= 0.0) {
        bail!("stake and fee settings must be finite and valid");
    }
    if !(book.best_ask > 0.0 && book.best_ask < 1.0) {
        return Ok(BuyMeasurementOutcome::Rejected("invalid_top_of_book"));
    }
    if book.best_ask > maximum_ask {
        return Ok(BuyMeasurementOutcome::Rejected("best_ask_above_price_cap"));
    }
    let mut remaining = stake_usd;
    let mut cost = 0.0;
    let mut shares = 0.0;
    let mut fee = 0.0;
    for (price, size) in book.ask_levels() {
        if price > maximum_ask || remaining <= 1e-9 {
            break;
        }
        if !(price > 0.0 && price < 1.0 && size > 0.0) {
            continue;
        }
        let fill_cost = remaining.min(price * size);
        let fill_shares = fill_cost / price;
        cost += fill_cost;
        shares += fill_shares;
        fee += polymarket_fee(fill_shares, price, fee_rate);
        remaining -= fill_cost;
    }
    if remaining > 1e-9 || shares <= 0.0 {
        return Ok(BuyMeasurementOutcome::Rejected(
            "insufficient_visible_depth_at_price_cap",
        ));
    }
    Ok(BuyMeasurementOutcome::Filled(BuyMeasurement {
        book_timestamp_ms: (book.last_update_ts_s * 1_000.0).round() as i64,
        best_ask: book.best_ask,
        average_entry_price: cost / shares,
        cost,
        shares,
        fee,
        break_even_probability: (cost + fee) / shares,
    }))
}

fn build_replay_row(
    target: ReplayTarget,
    measurement: BuyMeasurementOutcome,
    won: Option<bool>,
) -> Result<(usize, OpportunityReplayFill)> {
    let trace_index = target.trace_index;
    let BuyMeasurementOutcome::Filled(measurement) = measurement else {
        let BuyMeasurementOutcome::Rejected(status) = measurement else {
            unreachable!("matched non-fill replay result")
        };
        return Ok((
            trace_index,
            OpportunityReplayFill {
                opportunity_id: target.opportunity.opportunity_id,
                observed_at_ms: target.opportunity.observed_at_ms,
                order_arrival_at_ms: target.order_arrival_at_ms,
                condition_id: target.opportunity.condition_id,
                token_id: target.opportunity.token_id,
                status: status.to_string(),
                book_timestamp_ms: None,
                book_age_ms: None,
                best_ask: None,
                average_entry_price: None,
                executable_cost_usd: None,
                executable_shares: None,
                taker_fee_usd: None,
                fee_aware_break_even_probability: None,
                won: None,
                pnl_usd: None,
            },
        ));
    };
    let won = won.context("filled discovery opportunity has a tie label")?;
    let pnl_usd = if won {
        measurement.shares - measurement.cost - measurement.fee
    } else {
        -(measurement.cost + measurement.fee)
    };
    Ok((
        trace_index,
        OpportunityReplayFill {
            opportunity_id: target.opportunity.opportunity_id,
            observed_at_ms: target.opportunity.observed_at_ms,
            order_arrival_at_ms: target.order_arrival_at_ms,
            condition_id: target.opportunity.condition_id,
            token_id: target.opportunity.token_id,
            status: "filled".to_string(),
            book_timestamp_ms: Some(measurement.book_timestamp_ms),
            book_age_ms: Some(
                target
                    .order_arrival_at_ms
                    .saturating_sub(measurement.book_timestamp_ms),
            ),
            best_ask: Some(measurement.best_ask),
            average_entry_price: Some(measurement.average_entry_price),
            executable_cost_usd: Some(measurement.cost),
            executable_shares: Some(measurement.shares),
            taker_fee_usd: Some(measurement.fee),
            fee_aware_break_even_probability: Some(measurement.break_even_probability),
            won: Some(won),
            pnl_usd: Some(pnl_usd),
        },
    ))
}

fn summarize_trace(
    entry: ExactReplayPlanEntry,
    mut rows: Vec<OpportunityReplayFill>,
    safety_margin: f64,
) -> OpportunityReplayTraceReport {
    rows.sort_by(|left, right| {
        left.order_arrival_at_ms
            .cmp(&right.order_arrival_at_ms)
            .then_with(|| left.opportunity_id.cmp(&right.opportunity_id))
    });
    let requested_opportunities = entry.opportunity_ids.len();
    let filled = rows
        .iter()
        .filter(|row| row.status == "filled")
        .collect::<Vec<_>>();
    let fills = filled.len();
    let wins = filled.iter().filter(|row| row.won == Some(true)).count();
    let losses = fills.saturating_sub(wins);
    let win_rate = (fills > 0).then_some(wins as f64 / fills as f64);
    let wilson = (fills > 0).then_some(wilson_lower(wins, fills));
    let average_break_even_probability = (fills > 0).then(|| {
        filled
            .iter()
            .map(|row| {
                row.fee_aware_break_even_probability
                    .expect("filled row has break-even")
            })
            .sum::<f64>()
            / fills as f64
    });
    let point_estimate_edge = win_rate
        .zip(average_break_even_probability)
        .map(|(rate, break_even)| rate - break_even);
    let wilson_edge = wilson
        .zip(average_break_even_probability)
        .map(|(lower, break_even)| lower - break_even);
    let total_pnl_usd = filled
        .iter()
        .map(|row| row.pnl_usd.expect("filled row has PnL"))
        .sum::<f64>();
    let promotion_confidence_ready =
        wilson_edge.is_some_and(|edge| edge > safety_margin) && total_pnl_usd > 0.0;
    let mut rejection_counts = BTreeMap::new();
    for row in &rows {
        if row.status != "filled" {
            *rejection_counts.entry(row.status.clone()).or_insert(0) += 1;
        }
    }
    let status = if promotion_confidence_ready {
        "promotion_confidence_ready"
    } else if total_pnl_usd > 0.0 && point_estimate_edge.is_some_and(|edge| edge > safety_margin) {
        "research_signal_retained"
    } else {
        "rejected_by_latency_replay"
    };
    OpportunityReplayTraceReport {
        discovery_trace_sha256: entry.discovery_trace_sha256,
        representative_policy_id: entry.representative_policy_id,
        decision_seconds: entry.decision_seconds,
        maximum_ask: entry.maximum_ask,
        latency_ms: entry.latency_ms,
        requested_opportunities,
        fills,
        fill_rate: if requested_opportunities == 0 {
            0.0
        } else {
            fills as f64 / requested_opportunities as f64
        },
        rejection_counts,
        wins,
        losses,
        win_rate,
        wilson_win_rate_lower: wilson,
        average_break_even_probability,
        point_estimate_edge,
        wilson_edge,
        total_pnl_usd,
        promotion_confidence_ready,
        status: status.to_string(),
        rows,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::pmxt::{BookSnapshot, L2Level};

    fn book() -> TokenBook {
        let mut book = TokenBook::default();
        book.apply_snapshot(&BookSnapshot {
            market_id: "condition".to_string(),
            token_id: "token".to_string(),
            best_bid: 0.69,
            best_ask: 0.70,
            timestamp_s: 10.0,
            bids: vec![L2Level {
                price: 0.69,
                size: 20.0,
            }],
            asks: vec![
                L2Level {
                    price: 0.70,
                    size: 5.0,
                },
                L2Level {
                    price: 0.71,
                    size: 10.0,
                },
            ],
        });
        book
    }

    #[test]
    fn buy_measurement_is_fee_aware_and_price_capped() {
        let book = book();
        assert!(matches!(
            measure_buy(&book, 0.69, 5.0, 0.07).unwrap(),
            BuyMeasurementOutcome::Rejected("best_ask_above_price_cap")
        ));
        let BuyMeasurementOutcome::Filled(fill) = measure_buy(&book, 0.71, 5.0, 0.07).unwrap()
        else {
            panic!("expected fill")
        };
        assert!((fill.cost - 5.0).abs() < 1e-9);
        assert!(fill.fee > 0.0);
        assert!(fill.break_even_probability > fill.average_entry_price);
    }

    #[test]
    fn insufficient_depth_rejects_partial_fill() {
        let book = book();
        assert!(matches!(
            measure_buy(&book, 0.70, 5.0, 0.07).unwrap(),
            BuyMeasurementOutcome::Rejected("insufficient_visible_depth_at_price_cap")
        ));
    }

    #[test]
    fn complementary_token_override_inverts_binary_label() {
        assert_eq!(selected_token_outcome("up", "up", Some(true)), Some(true));
        assert_eq!(
            selected_token_outcome("up", "down", Some(true)),
            Some(false)
        );
        assert_eq!(selected_token_outcome("up", "down", None), None);
    }
}
