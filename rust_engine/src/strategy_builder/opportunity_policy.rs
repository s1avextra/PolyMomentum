//! One-pass policy discovery over sealed causal opportunities.
//!
//! This layer deliberately cannot score the fresh holdout: discovery label
//! manifests are required to physically exclude fresh rows. Exact L2 replay is
//! planned only after execution-equivalent policies have been collapsed.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::artifact::write_json_artifact_atomic;
use crate::strategy::spec::stable_json_hash;

use super::opportunity_dataset::{
    load_sealed_opportunities, read_labels, sha256_file, CausalOpportunity,
    OpportunityLabelsManifest, OPPORTUNITY_LABEL_SCHEMA_VERSION,
};
use super::opportunity_table::HashedSource;

pub const OPPORTUNITY_POLICY_SEARCH_SCHEMA_VERSION: &str = "opportunity_policy_search_v2";

#[derive(Debug, Clone)]
pub struct OpportunityPolicySearchInput {
    pub dataset_seal_path: PathBuf,
    pub labels_manifest_path: PathBuf,
    pub output_path: PathBuf,
    pub minimum_calibration_support: usize,
    pub minimum_policy_support: usize,
    pub safety_margin: f64,
    pub latency_ms: u64,
    pub maximum_exact_replays: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PolicyDefinition {
    pub decision_seconds: u16,
    pub require_aligned_path: bool,
    pub minimum_abs_move_2m_usd: u16,
    pub maximum_ask: f64,
    pub minimum_directional_distance_usd: i16,
    pub minimum_top_book_pressure: f64,
    pub direction: String,
    pub maximum_annualized_volatility: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PolicyResult {
    pub policy_id: String,
    pub policy: PolicyDefinition,
    pub raw_discovery_support: usize,
    pub calibrated_discovery_support: usize,
    pub wins: usize,
    pub losses: usize,
    pub win_rate: Option<f64>,
    pub wilson_win_rate_lower: Option<f64>,
    pub average_break_even_probability: Option<f64>,
    pub point_estimate_edge: Option<f64>,
    pub wilson_edge: Option<f64>,
    pub economic_payoff_proxy_usd: f64,
    pub fresh_causal_support: usize,
    pub fresh_calibrated_support: usize,
    pub discovery_trace_sha256: Option<String>,
    pub fresh_support_trace_sha256: Option<String>,
    pub discovery_eligible: bool,
    pub promotion_confidence_ready: bool,
    pub rejection_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CalibrationCellReport {
    pub key: String,
    pub support: usize,
    pub wins: usize,
    pub win_rate: f64,
    pub wilson_lower: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExactReplayPlanEntry {
    pub discovery_trace_sha256: String,
    pub representative_policy_id: String,
    pub equivalent_policy_ids: Vec<String>,
    pub decision_seconds: u16,
    pub opportunity_ids: Vec<String>,
    /// Optional paired-book-selected token for each source opportunity.
    /// Legacy/path/probability plans leave this empty and retain the sealed
    /// opportunity token.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub token_overrides: BTreeMap<String, String>,
    pub maximum_ask: f64,
    pub stake_usd: f64,
    pub fee_rate: f64,
    pub latency_ms: u64,
    pub causal_feature_semantics_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExactReplayPlan {
    pub status: String,
    pub eligible_policy_count: usize,
    pub eligible_unique_trace_count: usize,
    pub maximum_replay_count: usize,
    pub unique_replay_count: usize,
    pub deferred_replay_count: usize,
    pub avoided_replay_count: usize,
    pub equivalence_reduction_fraction: Option<f64>,
    pub entries: Vec<ExactReplayPlanEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityPolicySearchReport {
    pub schema_version: String,
    pub generated_at: String,
    pub dataset_seal: HashedSource,
    pub labels_manifest: HashedSource,
    pub dataset_sha256: String,
    pub causal_feature_semantics_version: String,
    pub source_opportunity_table_reads: usize,
    pub in_memory_policy_evaluation_passes: usize,
    pub fresh_holdout_outcomes_accessed: bool,
    pub policy_grid_sha256: String,
    pub policies_evaluated: usize,
    pub minimum_calibration_support: usize,
    pub minimum_policy_support: usize,
    pub safety_margin: f64,
    pub calibration_semantics: String,
    pub discovery_gate: String,
    pub exact_replay_is_research_only: bool,
    pub promotion_requires_wilson_after_exact_replay: bool,
    pub calibration_cells: Vec<CalibrationCellReport>,
    pub calibration_rows: usize,
    pub discovery_rows: usize,
    pub fresh_holdout_rows: usize,
    pub observable_rows: usize,
    pub fully_executable_rows: usize,
    pub eligible_policy_count: usize,
    pub top_diagnostics: Vec<PolicyResult>,
    pub exact_replay_plan: ExactReplayPlan,
    pub verdict: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct CalibrationKey {
    decision_seconds: u16,
    direction: String,
    move_bucket: u8,
    distance_bucket: u8,
    volatility_bucket: u8,
}

impl CalibrationKey {
    fn from_row(row: &CausalOpportunity) -> Option<Self> {
        let decision_seconds = decision_seconds(row)?;
        let absolute_move = row.move_2m_usd?.abs();
        let move_bucket = if absolute_move < 100.0 {
            0
        } else if absolute_move < 200.0 {
            1
        } else {
            2
        };
        let distance_bucket = if row.directional_distance_to_strike_usd < 0.0 {
            0
        } else if row.directional_distance_to_strike_usd < 200.0 {
            1
        } else {
            2
        };
        let volatility_bucket = if row.causal_volatility < 0.5 {
            0
        } else if row.causal_volatility < 1.0 {
            1
        } else {
            2
        };
        Some(Self {
            decision_seconds,
            direction: row.signal_direction.clone(),
            move_bucket,
            distance_bucket,
            volatility_bucket,
        })
    }

    fn stable_name(&self) -> String {
        format!(
            "t{}:{}:m{}:d{}:v{}",
            self.decision_seconds,
            self.direction,
            self.move_bucket,
            self.distance_bucket,
            self.volatility_bucket
        )
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct BinomialStats {
    support: usize,
    wins: usize,
}

#[derive(Debug, Default)]
struct PolicyAccumulator {
    raw_discovery_support: usize,
    discovery_opportunity_ids: Vec<String>,
    fresh_opportunity_ids: Vec<String>,
    wins: usize,
    losses: usize,
    break_even_sum: f64,
    economic_payoff_proxy_usd: f64,
    fresh_causal_support: usize,
}

#[derive(Debug)]
struct FinishedPolicy {
    result: PolicyResult,
    discovery_opportunity_ids: Vec<String>,
}

pub fn search(input: OpportunityPolicySearchInput) -> Result<OpportunityPolicySearchReport> {
    validate_input(&input)?;
    if input.output_path == input.dataset_seal_path
        || input.output_path == input.labels_manifest_path
    {
        bail!("policy-search output must not replace an input");
    }

    let dataset_seal_sha256 = sha256_file(&input.dataset_seal_path)?;
    let (seal, opportunities) = load_sealed_opportunities(&input.dataset_seal_path)?;
    let labels_manifest_sha256 = sha256_file(&input.labels_manifest_path)?;
    let labels_manifest: OpportunityLabelsManifest = serde_json::from_reader(
        File::open(&input.labels_manifest_path)
            .with_context(|| format!("open {}", input.labels_manifest_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.labels_manifest_path.display()))?;
    validate_labels_manifest(&labels_manifest, &seal.dataset_sha256, &dataset_seal_sha256)?;

    let label_path = PathBuf::from(&labels_manifest.output.path);
    let labels = read_labels(&label_path)?;
    let expected_fresh_rows = opportunities
        .iter()
        .filter(|row| row.chronological_window == "fresh_holdout")
        .count();
    if labels_manifest.total_opportunities != opportunities.len()
        || labels_manifest.labeled_rows != labels.len()
        || labels_manifest.fresh_holdout_rows_excluded != expected_fresh_rows
    {
        bail!("label manifest aggregate counts do not match the sealed dataset");
    }
    let opportunity_by_id = opportunities
        .iter()
        .map(|row| (row.opportunity_id.as_str(), row))
        .collect::<HashMap<_, _>>();
    let mut labels_by_id = HashMap::new();
    for label in &labels {
        let opportunity = opportunity_by_id
            .get(label.opportunity_id.as_str())
            .with_context(|| {
                format!(
                    "label references unknown opportunity {}",
                    label.opportunity_id
                )
            })?;
        if opportunity.chronological_window == "fresh_holdout" {
            bail!("discovery label table contains a fresh-holdout outcome");
        }
        if labels_by_id
            .insert(label.opportunity_id.as_str(), label)
            .is_some()
        {
            bail!("duplicate opportunity_id in label table");
        }
    }

    let mut calibration = BTreeMap::<CalibrationKey, BinomialStats>::new();
    for row in &opportunities {
        if row.chronological_window != "older" {
            continue;
        }
        let Some(label) = labels_by_id.get(row.opportunity_id.as_str()) else {
            continue;
        };
        let Some(won) = label.won else {
            continue;
        };
        let Some(key) = CalibrationKey::from_row(row) else {
            continue;
        };
        let cell = calibration.entry(key).or_default();
        cell.support += 1;
        cell.wins += usize::from(won);
    }

    let policies = policy_grid();
    let policies_evaluated = policies.len();
    let policy_grid_sha256 = stable_json_hash(&policies);
    let mut accumulators = (0..policies.len())
        .map(|_| PolicyAccumulator::default())
        .collect::<Vec<_>>();

    // This is the single discovery pass: every opportunity is visited once,
    // then fanned out across the bounded in-memory policy grid. No source table
    // is reopened per candidate.
    for row in &opportunities {
        let Some(key) = CalibrationKey::from_row(row) else {
            continue;
        };
        // Discovery is deliberately a cheap shortlist stage. Older rows must
        // establish that the coarse causal cell has enough support, but an
        // older Wilson bound does not hard-veto a possible regime change before
        // exact replay. Wilson-above-break-even remains visible on every result
        // and is required by later promotion gates.
        let calibrated = calibration
            .get(&key)
            .is_some_and(|cell| cell.support >= input.minimum_calibration_support);
        for (index, policy) in policies.iter().enumerate() {
            if !matches_policy(row, policy) {
                continue;
            }
            let accumulator = &mut accumulators[index];
            if row.chronological_window == "fresh_holdout" {
                accumulator.fresh_causal_support += 1;
                if calibrated {
                    accumulator
                        .fresh_opportunity_ids
                        .push(row.opportunity_id.clone());
                }
                continue;
            }
            if row.chronological_window != "recent_discovery" {
                continue;
            }
            accumulator.raw_discovery_support += 1;
            if !calibrated {
                continue;
            }
            let Some(label) = labels_by_id.get(row.opportunity_id.as_str()) else {
                continue;
            };
            let Some(won) = label.won else {
                continue;
            };
            accumulator
                .discovery_opportunity_ids
                .push(row.opportunity_id.clone());
            accumulator.wins += usize::from(won);
            accumulator.losses += usize::from(!won);
            accumulator.break_even_sum += row
                .fee_aware_break_even_probability
                .expect("matches_policy requires break-even economics");
            accumulator.economic_payoff_proxy_usd += if won {
                row.fee_aware_net_win_usd
                    .expect("matches_policy requires win economics")
            } else {
                -row.fee_aware_max_loss_usd
                    .expect("matches_policy requires loss economics")
            };
        }
    }

    let mut evaluated = policies
        .into_iter()
        .zip(accumulators)
        .map(|(policy, accumulator)| {
            finish_policy(
                policy,
                accumulator,
                &seal.causal_feature_semantics_version,
                seal.stake_usd,
                seal.fee_rate,
                input.latency_ms,
                input.minimum_policy_support,
                input.safety_margin,
            )
        })
        .collect::<Vec<_>>();

    let replay_groups = group_eligible_traces(&evaluated);
    let eligible_policy_count = replay_groups.values().map(Vec::len).sum::<usize>();
    let mut replay_entries = Vec::with_capacity(replay_groups.len());
    for (trace, indices) in replay_groups {
        let representative_index = *indices
            .iter()
            .min_by_key(|index| evaluated[**index].result.policy_id.as_str())
            .expect("nonempty replay group");
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
            opportunity_ids: representative.discovery_opportunity_ids.clone(),
            token_overrides: BTreeMap::new(),
            maximum_ask: representative.result.policy.maximum_ask,
            stake_usd: seal.stake_usd,
            fee_rate: seal.fee_rate,
            latency_ms: input.latency_ms,
            causal_feature_semantics_version: seal.causal_feature_semantics_version.clone(),
        });
    }
    let results_by_policy = evaluated
        .iter()
        .map(|finished| (finished.result.policy_id.as_str(), &finished.result))
        .collect::<HashMap<_, _>>();
    replay_entries.sort_by(|left, right| {
        policy_diagnostic_order(
            results_by_policy[left.representative_policy_id.as_str()],
            results_by_policy[right.representative_policy_id.as_str()],
        )
        .then_with(|| {
            left.discovery_trace_sha256
                .cmp(&right.discovery_trace_sha256)
        })
    });
    let eligible_unique_trace_count = replay_entries.len();
    replay_entries = cap_replay_entries(replay_entries, input.maximum_exact_replays);
    let unique_replay_count = replay_entries.len();
    let deferred_replay_count = eligible_unique_trace_count.saturating_sub(unique_replay_count);
    let avoided_replay_count = eligible_policy_count.saturating_sub(unique_replay_count);
    let equivalence_reduction_fraction = (eligible_policy_count > 0)
        .then_some(avoided_replay_count as f64 / eligible_policy_count as f64);

    evaluated.sort_by(|left, right| policy_diagnostic_order(&left.result, &right.result));
    let top_diagnostics = evaluated
        .into_iter()
        .take(25)
        .map(|finished| finished.result)
        .collect::<Vec<_>>();
    let calibration_cells = calibration
        .iter()
        .map(|(key, stats)| CalibrationCellReport {
            key: key.stable_name(),
            support: stats.support,
            wins: stats.wins,
            win_rate: stats.wins as f64 / stats.support as f64,
            wilson_lower: wilson_lower(stats.wins, stats.support),
        })
        .collect::<Vec<_>>();
    let calibration_rows = opportunities
        .iter()
        .filter(|row| row.chronological_window == "older")
        .count();
    let discovery_rows = opportunities
        .iter()
        .filter(|row| row.chronological_window == "recent_discovery")
        .count();
    let fresh_holdout_rows = opportunities
        .iter()
        .filter(|row| row.chronological_window == "fresh_holdout")
        .count();
    let observable_rows = opportunities
        .iter()
        .filter(|row| row.book_observable)
        .count();
    let fully_executable_rows = opportunities
        .iter()
        .filter(|row| row.stake_fully_executable)
        .count();
    let verdict = if eligible_policy_count == 0 {
        "no_candidate_survived_discovery"
    } else if equivalence_reduction_fraction.is_some_and(|value| value >= 0.80) {
        "exact_replay_plan_ready"
    } else {
        "exact_replay_plan_ready_equivalence_reduction_below_target"
    };
    let report = OpportunityPolicySearchReport {
        schema_version: OPPORTUNITY_POLICY_SEARCH_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        dataset_seal: HashedSource {
            path: input.dataset_seal_path.display().to_string(),
            sha256: dataset_seal_sha256,
        },
        labels_manifest: HashedSource {
            path: input.labels_manifest_path.display().to_string(),
            sha256: labels_manifest_sha256,
        },
        dataset_sha256: seal.dataset_sha256,
        causal_feature_semantics_version: seal.causal_feature_semantics_version,
        source_opportunity_table_reads: seal.entries.len(),
        in_memory_policy_evaluation_passes: 1,
        fresh_holdout_outcomes_accessed: false,
        policy_grid_sha256,
        policies_evaluated,
        minimum_calibration_support: input.minimum_calibration_support,
        minimum_policy_support: input.minimum_policy_support,
        safety_margin: input.safety_margin,
        calibration_semantics: "older labels establish minimum causal-cell support; older Wilson is diagnostic and cannot hard-veto a research-only exact-replay shortlist".to_string(),
        discovery_gate: "minimum support + discovery point estimate above average fee-aware break-even plus safety margin + positive payoff proxy".to_string(),
        exact_replay_is_research_only: true,
        promotion_requires_wilson_after_exact_replay: true,
        calibration_cells,
        calibration_rows,
        discovery_rows,
        fresh_holdout_rows,
        observable_rows,
        fully_executable_rows,
        eligible_policy_count,
        top_diagnostics,
        exact_replay_plan: ExactReplayPlan {
            status: if unique_replay_count == 0 {
                "empty".to_string()
            } else {
                "ready".to_string()
            },
            eligible_policy_count,
            eligible_unique_trace_count,
            maximum_replay_count: input.maximum_exact_replays,
            unique_replay_count,
            deferred_replay_count,
            avoided_replay_count,
            equivalence_reduction_fraction,
            entries: replay_entries,
        },
        verdict: verdict.to_string(),
    };
    write_json_artifact_atomic(&input.output_path, &report)?;
    Ok(report)
}

fn cap_replay_entries(
    replay_entries: Vec<ExactReplayPlanEntry>,
    maximum_exact_replays: usize,
) -> Vec<ExactReplayPlanEntry> {
    let mut selected_entries = Vec::new();
    let mut selected_traces = HashSet::new();
    let mut selected_decision_seconds = HashSet::new();
    for entry in &replay_entries {
        if selected_entries.len() >= maximum_exact_replays {
            break;
        }
        if selected_decision_seconds.insert(entry.decision_seconds) {
            selected_traces.insert(entry.discovery_trace_sha256.clone());
            selected_entries.push(entry.clone());
        }
    }
    if selected_entries.len() < maximum_exact_replays {
        for entry in replay_entries {
            if selected_entries.len() >= maximum_exact_replays {
                break;
            }
            if selected_traces.insert(entry.discovery_trace_sha256.clone()) {
                selected_entries.push(entry);
            }
        }
    }
    selected_entries
}

fn validate_input(input: &OpportunityPolicySearchInput) -> Result<()> {
    if input.minimum_calibration_support == 0 || input.minimum_policy_support == 0 {
        bail!("support floors must be positive");
    }
    if input.maximum_exact_replays == 0 {
        bail!("maximum_exact_replays must be positive");
    }
    if !input.safety_margin.is_finite() || input.safety_margin < 0.0 || input.safety_margin >= 1.0 {
        bail!("safety margin must be finite and in [0, 1)");
    }
    Ok(())
}

fn group_eligible_traces(evaluated: &[FinishedPolicy]) -> BTreeMap<String, Vec<usize>> {
    let mut groups = BTreeMap::<String, Vec<usize>>::new();
    for (index, finished) in evaluated.iter().enumerate() {
        if finished.result.discovery_eligible {
            groups
                .entry(
                    finished
                        .result
                        .discovery_trace_sha256
                        .clone()
                        .expect("eligible policy has a trace"),
                )
                .or_default()
                .push(index);
        }
    }
    groups
}

fn validate_labels_manifest(
    manifest: &OpportunityLabelsManifest,
    dataset_sha256: &str,
    dataset_seal_sha256: &str,
) -> Result<()> {
    if manifest.schema_version != OPPORTUNITY_LABEL_SCHEMA_VERSION {
        bail!("unsupported opportunity-label schema");
    }
    if manifest.dataset_sha256 != dataset_sha256 {
        bail!("label manifest belongs to a different causal dataset");
    }
    if manifest.dataset_seal.sha256 != dataset_seal_sha256 {
        bail!("label manifest does not pin the provided dataset seal");
    }
    if manifest.fresh_holdout_labels_present {
        bail!("fresh-holdout outcomes are forbidden in discovery labels");
    }
    let label_path = PathBuf::from(&manifest.output.path);
    if sha256_file(&label_path)? != manifest.output.sha256 {
        bail!("label table hash drifted");
    }
    Ok(())
}

fn policy_grid() -> Vec<PolicyDefinition> {
    let mut policies = Vec::new();
    for decision_seconds in [120, 180, 240] {
        for require_aligned_path in [false, true] {
            for minimum_abs_move_2m_usd in [0, 100, 200] {
                for maximum_ask in [0.85, 0.90, 0.95, 0.97, 1.0] {
                    for minimum_directional_distance_usd in [0, 100, 200] {
                        for minimum_top_book_pressure in [-1.0, -0.15, 0.15] {
                            for direction in ["both", "up", "down"] {
                                for maximum_annualized_volatility in [1.0, 2.0, 5.0] {
                                    policies.push(PolicyDefinition {
                                        decision_seconds,
                                        require_aligned_path,
                                        minimum_abs_move_2m_usd,
                                        maximum_ask,
                                        minimum_directional_distance_usd,
                                        minimum_top_book_pressure,
                                        direction: direction.to_string(),
                                        maximum_annualized_volatility,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    policies
}

fn decision_seconds(row: &CausalOpportunity) -> Option<u16> {
    [120u16, 180, 240]
        .into_iter()
        .find(|&value| (row.elapsed_seconds - f64::from(value)).abs() <= 0.001)
}

fn matches_policy(row: &CausalOpportunity, policy: &PolicyDefinition) -> bool {
    if decision_seconds(row) != Some(policy.decision_seconds)
        || (policy.direction != "both" && row.signal_direction != policy.direction)
        || row
            .move_2m_usd
            .is_none_or(|value| value.abs() < f64::from(policy.minimum_abs_move_2m_usd))
        || row.directional_distance_to_strike_usd
            < f64::from(policy.minimum_directional_distance_usd)
        || row.causal_volatility > policy.maximum_annualized_volatility
        || !row.book_observable
        || !row.stake_fully_executable
        || row.best_ask.is_none_or(|ask| ask > policy.maximum_ask)
        || row
            .top_book_pressure
            .is_none_or(|pressure| pressure < policy.minimum_top_book_pressure)
        || row.fee_aware_break_even_probability.is_none()
        || row.fee_aware_net_win_usd.is_none()
        || row.fee_aware_max_loss_usd.is_none()
    {
        return false;
    }
    if !policy.require_aligned_path {
        return true;
    }
    match policy.decision_seconds {
        120 => row.path_2m_aligned == Some(true),
        180 => row.path_3m_aligned == Some(true),
        240 => row.path_4m_aligned == Some(true),
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn finish_policy(
    policy: PolicyDefinition,
    accumulator: PolicyAccumulator,
    semantics_version: &str,
    stake_usd: f64,
    fee_rate: f64,
    latency_ms: u64,
    minimum_policy_support: usize,
    safety_margin: f64,
) -> FinishedPolicy {
    let policy_id = stable_json_hash(&policy);
    let support = accumulator.wins + accumulator.losses;
    let win_rate = (support > 0).then_some(accumulator.wins as f64 / support as f64);
    let wilson = (support > 0).then_some(wilson_lower(accumulator.wins, support));
    let average_break_even = (support > 0).then_some(accumulator.break_even_sum / support as f64);
    let point_estimate_edge = win_rate
        .zip(average_break_even)
        .map(|(win_rate, break_even)| win_rate - break_even);
    let wilson_edge = wilson
        .zip(average_break_even)
        .map(|(lower, break_even)| lower - break_even);
    let promotion_confidence_ready = wilson_edge.is_some_and(|edge| edge > safety_margin);
    let discovery_trace_sha256 = (!accumulator.discovery_opportunity_ids.is_empty()).then(|| {
        trace_hash(
            "discovery",
            &accumulator.discovery_opportunity_ids,
            policy.maximum_ask,
            stake_usd,
            fee_rate,
            latency_ms,
            semantics_version,
        )
    });
    let fresh_support_trace_sha256 = (!accumulator.fresh_opportunity_ids.is_empty()).then(|| {
        trace_hash(
            "fresh_support",
            &accumulator.fresh_opportunity_ids,
            policy.maximum_ask,
            stake_usd,
            fee_rate,
            latency_ms,
            semantics_version,
        )
    });
    let mut rejection_reasons = Vec::new();
    if support < minimum_policy_support {
        rejection_reasons.push("insufficient_discovery_support".to_string());
    }
    if point_estimate_edge.is_none_or(|edge| edge <= safety_margin) {
        rejection_reasons.push("point_estimate_not_above_break_even_margin".to_string());
    }
    if accumulator.economic_payoff_proxy_usd <= 0.0 {
        rejection_reasons.push("nonpositive_fee_aware_payoff_proxy".to_string());
    }
    FinishedPolicy {
        discovery_opportunity_ids: accumulator.discovery_opportunity_ids,
        result: PolicyResult {
            policy_id,
            policy,
            raw_discovery_support: accumulator.raw_discovery_support,
            calibrated_discovery_support: support,
            wins: accumulator.wins,
            losses: accumulator.losses,
            win_rate,
            wilson_win_rate_lower: wilson,
            average_break_even_probability: average_break_even,
            point_estimate_edge,
            wilson_edge,
            economic_payoff_proxy_usd: accumulator.economic_payoff_proxy_usd,
            fresh_causal_support: accumulator.fresh_causal_support,
            fresh_calibrated_support: accumulator.fresh_opportunity_ids.len(),
            discovery_trace_sha256,
            fresh_support_trace_sha256,
            discovery_eligible: rejection_reasons.is_empty(),
            promotion_confidence_ready,
            rejection_reasons,
        },
    }
}

fn trace_hash(
    partition: &str,
    opportunity_ids: &[String],
    maximum_ask: f64,
    stake_usd: f64,
    fee_rate: f64,
    latency_ms: u64,
    semantics_version: &str,
) -> String {
    stable_json_hash(&serde_json::json!({
        "partition": partition,
        "ordered_opportunity_ids": opportunity_ids,
        "execution": {
            "side": "buy",
            "maximum_ask": maximum_ask,
            "stake_usd": stake_usd,
            "latency_ms": latency_ms,
            "fee_rate": fee_rate,
            "causal_feature_semantics_version": semantics_version,
        }
    }))
}

fn policy_diagnostic_order(left: &PolicyResult, right: &PolicyResult) -> std::cmp::Ordering {
    right
        .discovery_eligible
        .cmp(&left.discovery_eligible)
        .then_with(|| {
            right
                .promotion_confidence_ready
                .cmp(&left.promotion_confidence_ready)
        })
        .then_with(|| {
            right
                .point_estimate_edge
                .unwrap_or(f64::NEG_INFINITY)
                .total_cmp(&left.point_estimate_edge.unwrap_or(f64::NEG_INFINITY))
        })
        .then_with(|| {
            right
                .economic_payoff_proxy_usd
                .total_cmp(&left.economic_payoff_proxy_usd)
        })
        .then_with(|| {
            right
                .calibrated_discovery_support
                .cmp(&left.calibrated_discovery_support)
        })
        .then_with(|| right.raw_discovery_support.cmp(&left.raw_discovery_support))
        .then_with(|| left.policy_id.cmp(&right.policy_id))
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

    #[test]
    fn grid_is_bounded_and_deterministic() {
        let first = policy_grid();
        let second = policy_grid();
        assert_eq!(first.len(), 7_290);
        assert_eq!(first, second);
        assert_eq!(stable_json_hash(&first), stable_json_hash(&second));
    }

    #[test]
    fn trace_ignores_discovery_only_thresholds_but_pins_execution() {
        let ids = vec!["a".to_string(), "b".to_string()];
        let first = trace_hash("discovery", &ids, 0.90, 5.0, 0.07, 0, "v1");
        let same = trace_hash("discovery", &ids, 0.90, 5.0, 0.07, 0, "v1");
        let different_cap = trace_hash("discovery", &ids, 0.95, 5.0, 0.07, 0, "v1");
        assert_eq!(first, same);
        assert_ne!(first, different_cap);
    }

    #[test]
    fn wilson_is_conservative() {
        assert_eq!(wilson_lower(0, 0), 0.0);
        assert!(wilson_lower(8, 10) < 0.8);
        assert!(wilson_lower(80, 100) > wilson_lower(8, 10));
    }

    #[test]
    fn point_estimate_can_shortlist_before_wilson_promotion() {
        let support = 47;
        let wins = 36;
        let average_break_even = 0.685;
        let finished = finish_policy(
            PolicyDefinition {
                decision_seconds: 120,
                require_aligned_path: false,
                minimum_abs_move_2m_usd: 0,
                maximum_ask: 0.95,
                minimum_directional_distance_usd: 0,
                minimum_top_book_pressure: -1.0,
                direction: "both".to_string(),
                maximum_annualized_volatility: 1.0,
            },
            PolicyAccumulator {
                raw_discovery_support: support,
                discovery_opportunity_ids: (0..support)
                    .map(|index| format!("id-{index}"))
                    .collect(),
                fresh_opportunity_ids: Vec::new(),
                wins,
                losses: support - wins,
                break_even_sum: average_break_even * support as f64,
                economic_payoff_proxy_usd: 1.0,
                fresh_causal_support: 0,
            },
            "v1",
            5.0,
            0.07,
            128,
            20,
            0.02,
        );

        assert!(finished.result.discovery_eligible);
        assert!(!finished.result.promotion_confidence_ready);
        assert!(finished.result.point_estimate_edge.unwrap() > 0.02);
        assert!(finished.result.wilson_edge.unwrap() < 0.0);
    }

    #[test]
    fn equivalent_discovery_predicates_collapse_before_exact_replay() {
        let ids = (0..100)
            .map(|index| format!("id-{index}"))
            .collect::<Vec<_>>();
        let evaluated = (0..100)
            .map(|index| {
                finish_policy(
                    PolicyDefinition {
                        decision_seconds: 180,
                        require_aligned_path: index % 2 == 0,
                        minimum_abs_move_2m_usd: (index % 3 * 100) as u16,
                        maximum_ask: 0.90,
                        minimum_directional_distance_usd: (index % 5 * 10) as i16,
                        minimum_top_book_pressure: -1.0 + index as f64 / 1000.0,
                        direction: "both".to_string(),
                        maximum_annualized_volatility: 1.0 + index as f64 / 100.0,
                    },
                    PolicyAccumulator {
                        raw_discovery_support: 100,
                        discovery_opportunity_ids: ids.clone(),
                        fresh_opportunity_ids: Vec::new(),
                        wins: 100,
                        losses: 0,
                        break_even_sum: 50.0,
                        economic_payoff_proxy_usd: 100.0,
                        fresh_causal_support: 0,
                    },
                    "v1",
                    5.0,
                    0.07,
                    0,
                    20,
                    0.02,
                )
            })
            .collect::<Vec<_>>();
        let groups = group_eligible_traces(&evaluated);
        assert_eq!(groups.len(), 1);
        let eligible = groups.values().map(Vec::len).sum::<usize>();
        let reduction = (eligible - groups.len()) as f64 / eligible as f64;
        assert!(reduction >= 0.80);
    }

    #[test]
    fn replay_cap_prefers_distinct_decision_times() {
        let entry = |trace: &str, decision_seconds: u16| ExactReplayPlanEntry {
            discovery_trace_sha256: trace.to_string(),
            representative_policy_id: format!("policy-{trace}"),
            equivalent_policy_ids: vec![format!("policy-{trace}")],
            decision_seconds,
            opportunity_ids: vec![format!("opportunity-{trace}")],
            token_overrides: BTreeMap::new(),
            maximum_ask: 0.95,
            stake_usd: 5.0,
            fee_rate: 0.07,
            latency_ms: 128,
            causal_feature_semantics_version: "v1".to_string(),
        };
        let selected = cap_replay_entries(
            vec![
                entry("first", 120),
                entry("second", 120),
                entry("third", 180),
            ],
            2,
        );

        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].discovery_trace_sha256, "first");
        assert_eq!(selected[1].discovery_trace_sha256, "third");
    }

    #[test]
    fn discovery_rejects_a_manifest_that_exposes_fresh_outcomes() {
        let manifest = OpportunityLabelsManifest {
            schema_version: OPPORTUNITY_LABEL_SCHEMA_VERSION.to_string(),
            generated_at: "2026-08-10T00:00:00Z".to_string(),
            dataset_seal: HashedSource {
                path: "/tmp/seal.json".to_string(),
                sha256: "seal-hash".to_string(),
            },
            dataset_sha256: "dataset-hash".to_string(),
            label_source: HashedSource {
                path: "/tmp/source.jsonl".to_string(),
                sha256: "source-hash".to_string(),
            },
            output: HashedSource {
                path: "/tmp/labels.parquet".to_string(),
                sha256: "labels-hash".to_string(),
            },
            total_opportunities: 1,
            labeled_rows: 1,
            tie_rows: 0,
            missing_label_rows: 0,
            fresh_holdout_rows_excluded: 0,
            fresh_holdout_labels_present: true,
            join_key: "opportunity_id".to_string(),
            resolution_semantics: "test".to_string(),
            resolution_rule: "close_vs_open".to_string(),
            wrong_era_rows_excluded: 0,
            settlement_tape_median_interval_ms: None,
        };
        let error = validate_labels_manifest(&manifest, "dataset-hash", "seal-hash").unwrap_err();
        assert!(error.to_string().contains("fresh-holdout outcomes"));
    }
}
