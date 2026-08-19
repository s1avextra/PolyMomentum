//! Bounded causal-probability discovery over sealed opportunity tables.
//!
//! Unlike the late-window path family, this evaluator selects contracts only
//! when a causal binary terminal-probability model exceeds the executable,
//! fee-aware break-even probability by a fixed margin. Fresh labels remain
//! physically unavailable.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::artifact::write_json_artifact_atomic;
use crate::fair_value::binary_option_price_with_rate;
use crate::strategy::spec::stable_json_hash;

use super::opportunity_dataset::{
    load_sealed_opportunities, read_labels, sha256_file, CausalOpportunity,
    OpportunityLabelsManifest, OPPORTUNITY_LABEL_SCHEMA_VERSION,
};
use super::opportunity_policy::{ExactReplayPlan, ExactReplayPlanEntry};
use super::opportunity_replay::OpportunityExactReplayReport;
use super::opportunity_table::HashedSource;

pub const OPPORTUNITY_PROBABILITY_SEARCH_SCHEMA_VERSION: &str = "opportunity_probability_search_v1";
pub const FAMILY_ID: &str = "external_causal_probability_dislocation_v1";
const PREREGISTRATION_SCHEMA_VERSION: &str =
    "external_causal_probability_dislocation_preregistration_v1";
const ADVANCEMENT_WILSON_EDGE: f64 = 0.02;

#[derive(Debug, Clone)]
pub struct OpportunityProbabilitySearchInput {
    pub dataset_seal_path: PathBuf,
    pub labels_manifest_path: PathBuf,
    pub output_path: PathBuf,
    pub minimum_calibration_support: usize,
    pub maximum_calibration_brier_score: f64,
    pub minimum_policy_support: usize,
    pub safety_margin: f64,
    pub latency_ms: u64,
    pub maximum_exact_replays: usize,
}

#[derive(Debug, Clone)]
pub struct OpportunityProbabilityDecisionInput {
    pub preregistration_path: PathBuf,
    pub probability_search_report_path: PathBuf,
    pub exact_replay_report_path: PathBuf,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProbabilityPolicyDefinition {
    pub decision_seconds: u16,
    pub volatility_scale: f64,
    pub minimum_model_edge: f64,
    pub maximum_ask: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProbabilityPolicyResult {
    pub policy_id: String,
    pub policy: ProbabilityPolicyDefinition,
    pub calibration_support: usize,
    pub calibration_wins: usize,
    pub calibration_brier_score: Option<f64>,
    pub calibration_mean_probability: Option<f64>,
    pub calibration_win_rate: Option<f64>,
    pub discovery_support: usize,
    pub wins: usize,
    pub losses: usize,
    pub win_rate: Option<f64>,
    pub wilson_win_rate_lower: Option<f64>,
    pub average_model_probability: Option<f64>,
    pub average_break_even_probability: Option<f64>,
    pub average_model_edge: Option<f64>,
    pub point_estimate_edge: Option<f64>,
    pub wilson_edge: Option<f64>,
    pub economic_payoff_proxy_usd: f64,
    pub fresh_causal_support: usize,
    pub discovery_trace_sha256: Option<String>,
    pub fresh_support_trace_sha256: Option<String>,
    pub discovery_eligible: bool,
    pub rejection_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityProbabilitySearchReport {
    pub schema_version: String,
    pub generated_at: String,
    pub family_id: String,
    pub dataset_seal: HashedSource,
    pub labels_manifest: HashedSource,
    pub dataset_sha256: String,
    pub causal_feature_semantics_version: String,
    pub source_opportunity_table_reads: usize,
    pub in_memory_policy_evaluation_passes: usize,
    pub fresh_holdout_outcomes_accessed: bool,
    pub model_semantics: String,
    pub policy_grid_sha256: String,
    pub policies_evaluated: usize,
    pub minimum_calibration_support: usize,
    pub maximum_calibration_brier_score: f64,
    pub minimum_policy_support: usize,
    pub safety_margin: f64,
    pub calibration_semantics: String,
    pub discovery_gate: String,
    pub exact_replay_is_research_only: bool,
    pub promotion_requires_wilson_after_exact_replay: bool,
    pub calibration_rows: usize,
    pub discovery_rows: usize,
    pub fresh_holdout_rows: usize,
    pub eligible_policy_count: usize,
    pub top_diagnostics: Vec<ProbabilityPolicyResult>,
    pub exact_replay_plan: ExactReplayPlan,
    pub verdict: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProbabilityTraceDecision {
    pub discovery_trace_sha256: String,
    pub representative_policy_id: String,
    pub decision_seconds: u16,
    pub maximum_ask: f64,
    pub fills: usize,
    pub wins: usize,
    pub losses: usize,
    pub point_estimate_edge: Option<f64>,
    pub wilson_edge: Option<f64>,
    pub total_pnl_usd: f64,
    pub decision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityProbabilityDecisionReport {
    pub schema_version: String,
    pub generated_at: String,
    pub family_id: String,
    pub preregistration: HashedSource,
    pub probability_search_report: HashedSource,
    pub exact_replay_report: HashedSource,
    pub fixed_advancement_wilson_edge: f64,
    pub maximum_exact_replays: usize,
    pub exact_replays_consumed: usize,
    pub fresh_holdout_outcomes_accessed: bool,
    pub trace_decisions: Vec<ProbabilityTraceDecision>,
    pub decision: String,
    pub reason: String,
    pub terminal: bool,
    pub search_budget_exhausted: bool,
    pub more_evidence_allowed: bool,
    pub fresh_gate_opened: bool,
}

#[derive(Debug, Deserialize)]
struct ProbabilityPreregistration {
    schema_version: String,
    family_id: String,
    inputs: PreregisteredInputs,
    exact_replay_budget: PreregisteredReplayBudget,
}

#[derive(Debug, Deserialize)]
struct PreregisteredInputs {
    dataset_seal: HashedSource,
    labels_manifest: HashedSource,
}

#[derive(Debug, Deserialize)]
struct PreregisteredReplayBudget {
    maximum_unique_traces: usize,
    latency_ms: u64,
    stake_usd: f64,
    fee_rate: f64,
    additional_discovery_hours: usize,
    additional_parameter_variants: usize,
}

#[derive(Debug, Default)]
struct PolicyAccumulator {
    calibration_support: usize,
    calibration_wins: usize,
    calibration_brier_sum: f64,
    calibration_probability_sum: f64,
    discovery_opportunity_ids: Vec<String>,
    discovery_wins: usize,
    discovery_losses: usize,
    discovery_probability_sum: f64,
    discovery_break_even_sum: f64,
    economic_payoff_proxy_usd: f64,
    fresh_opportunity_ids: Vec<String>,
}

#[derive(Debug)]
struct FinishedPolicy {
    result: ProbabilityPolicyResult,
    discovery_opportunity_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct SelectedOpportunity {
    model_probability: f64,
    break_even_probability: f64,
    net_win_usd: f64,
    max_loss_usd: f64,
}

pub fn search(
    input: OpportunityProbabilitySearchInput,
) -> Result<OpportunityProbabilitySearchReport> {
    validate_input(&input)?;
    if input.output_path == input.dataset_seal_path
        || input.output_path == input.labels_manifest_path
    {
        bail!("probability-search output must not replace an input");
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
    let labels = read_labels(&PathBuf::from(&labels_manifest.output.path))?;
    let opportunities_by_id = opportunities
        .iter()
        .map(|row| (row.opportunity_id.as_str(), row))
        .collect::<HashMap<_, _>>();
    let mut labels_by_id = HashMap::new();
    for label in &labels {
        let row = opportunities_by_id
            .get(label.opportunity_id.as_str())
            .with_context(|| {
                format!(
                    "label references unknown opportunity {}",
                    label.opportunity_id
                )
            })?;
        if row.chronological_window == "fresh_holdout" {
            bail!("probability-search labels contain a fresh-holdout outcome");
        }
        if labels_by_id
            .insert(label.opportunity_id.as_str(), label)
            .is_some()
        {
            bail!("duplicate opportunity_id in label table");
        }
    }
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

    let policies = policy_grid();
    let policy_grid_sha256 = stable_json_hash(&policies);
    let policies_evaluated = policies.len();
    let mut accumulators = (0..policies.len())
        .map(|_| PolicyAccumulator::default())
        .collect::<Vec<_>>();

    for row in &opportunities {
        for (index, policy) in policies.iter().enumerate() {
            let Some(selected) = select_opportunity(row, policy) else {
                continue;
            };
            let accumulator = &mut accumulators[index];
            match row.chronological_window.as_str() {
                "fresh_holdout" => accumulator
                    .fresh_opportunity_ids
                    .push(row.opportunity_id.clone()),
                "older" | "recent_discovery" => {
                    let Some(label) = labels_by_id.get(row.opportunity_id.as_str()) else {
                        continue;
                    };
                    let Some(won) = label.won else {
                        continue;
                    };
                    if row.chronological_window == "older" {
                        accumulator.calibration_support += 1;
                        accumulator.calibration_wins += usize::from(won);
                        accumulator.calibration_probability_sum += selected.model_probability;
                        let outcome = if won { 1.0 } else { 0.0 };
                        accumulator.calibration_brier_sum +=
                            (selected.model_probability - outcome).powi(2);
                    } else {
                        accumulator
                            .discovery_opportunity_ids
                            .push(row.opportunity_id.clone());
                        accumulator.discovery_wins += usize::from(won);
                        accumulator.discovery_losses += usize::from(!won);
                        accumulator.discovery_probability_sum += selected.model_probability;
                        accumulator.discovery_break_even_sum += selected.break_even_probability;
                        accumulator.economic_payoff_proxy_usd += if won {
                            selected.net_win_usd
                        } else {
                            -selected.max_loss_usd
                        };
                    }
                }
                other => bail!("unsupported chronological window {other}"),
            }
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
                &input,
            )
        })
        .collect::<Vec<_>>();

    let mut groups = BTreeMap::<String, Vec<usize>>::new();
    for (index, finished) in evaluated.iter().enumerate() {
        if finished.result.discovery_eligible {
            groups
                .entry(
                    finished
                        .result
                        .discovery_trace_sha256
                        .clone()
                        .expect("eligible probability policy has a trace"),
                )
                .or_default()
                .push(index);
        }
    }
    let eligible_policy_count = groups.values().map(Vec::len).sum::<usize>();
    let mut replay_entries = Vec::with_capacity(groups.len());
    for (trace, indices) in groups {
        let representative_index = *indices
            .iter()
            .min_by_key(|index| evaluated[**index].result.policy_id.as_str())
            .expect("nonempty probability replay group");
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
        diagnostic_order(
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
    let avoided_replay_count = eligible_policy_count.saturating_sub(unique_replay_count);
    let equivalence_reduction_fraction = (eligible_policy_count > 0)
        .then_some(avoided_replay_count as f64 / eligible_policy_count as f64);

    evaluated.sort_by(|left, right| diagnostic_order(&left.result, &right.result));
    let top_diagnostics = evaluated
        .into_iter()
        .take(20)
        .map(|finished| finished.result)
        .collect::<Vec<_>>();
    let calibration_rows = opportunities
        .iter()
        .filter(|row| row.chronological_window == "older")
        .count();
    let discovery_rows = opportunities
        .iter()
        .filter(|row| row.chronological_window == "recent_discovery")
        .count();
    let fresh_holdout_rows = expected_fresh_rows;
    let verdict = if unique_replay_count == 0 {
        "family_rejected_by_bounded_cheap_screen"
    } else {
        "bounded_exact_replay_plan_ready"
    };
    let report = OpportunityProbabilitySearchReport {
        schema_version: OPPORTUNITY_PROBABILITY_SEARCH_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        family_id: FAMILY_ID.to_string(),
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
        model_semantics: "Black-Scholes binary terminal probability at observed BTC spot and strike; zero risk-free rate; causal annualized realized volatility times a fixed scale; DOWN is one minus UP".to_string(),
        policy_grid_sha256,
        policies_evaluated,
        minimum_calibration_support: input.minimum_calibration_support,
        maximum_calibration_brier_score: input.maximum_calibration_brier_score,
        minimum_policy_support: input.minimum_policy_support,
        safety_margin: input.safety_margin,
        calibration_semantics: "older selected rows must meet the fixed support floor and Brier-score ceiling".to_string(),
        discovery_gate: "recent support + point win-rate edge above fee-aware break-even safety margin + positive fee-aware payoff proxy".to_string(),
        exact_replay_is_research_only: true,
        promotion_requires_wilson_after_exact_replay: true,
        calibration_rows,
        discovery_rows,
        fresh_holdout_rows,
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
            deferred_replay_count: eligible_unique_trace_count
                .saturating_sub(unique_replay_count),
            avoided_replay_count,
            equivalence_reduction_fraction,
            entries: replay_entries,
        },
        verdict: verdict.to_string(),
    };
    write_json_artifact_atomic(&input.output_path, &report)?;
    Ok(report)
}

pub fn decide(
    input: OpportunityProbabilityDecisionInput,
) -> Result<OpportunityProbabilityDecisionReport> {
    if input.output_path == input.preregistration_path
        || input.output_path == input.probability_search_report_path
        || input.output_path == input.exact_replay_report_path
    {
        bail!("probability-decision output must not replace an input");
    }
    let preregistration_sha256 = sha256_file(&input.preregistration_path)?;
    let preregistration: ProbabilityPreregistration = serde_json::from_reader(
        File::open(&input.preregistration_path)
            .with_context(|| format!("open {}", input.preregistration_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.preregistration_path.display()))?;
    let probability_search_sha256 = sha256_file(&input.probability_search_report_path)?;
    let probability_search: OpportunityProbabilitySearchReport = serde_json::from_reader(
        File::open(&input.probability_search_report_path)
            .with_context(|| format!("open {}", input.probability_search_report_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.probability_search_report_path.display()))?;
    let exact_replay_sha256 = sha256_file(&input.exact_replay_report_path)?;
    let exact_replay: OpportunityExactReplayReport = serde_json::from_reader(
        File::open(&input.exact_replay_report_path)
            .with_context(|| format!("open {}", input.exact_replay_report_path.display()))?,
    )
    .with_context(|| format!("parse {}", input.exact_replay_report_path.display()))?;

    if preregistration.schema_version != PREREGISTRATION_SCHEMA_VERSION
        || preregistration.family_id != FAMILY_ID
        || probability_search.schema_version != OPPORTUNITY_PROBABILITY_SEARCH_SCHEMA_VERSION
        || probability_search.family_id != FAMILY_ID
    {
        bail!("probability decision inputs belong to a different family or schema");
    }
    if preregistration.inputs.dataset_seal.sha256 != probability_search.dataset_seal.sha256
        || preregistration.inputs.labels_manifest.sha256
            != probability_search.labels_manifest.sha256
        || probability_search.fresh_holdout_outcomes_accessed
        || exact_replay.fresh_holdout_outcomes_accessed
        || exact_replay.dataset_seal.sha256 != probability_search.dataset_seal.sha256
        || exact_replay.labels_manifest.sha256 != probability_search.labels_manifest.sha256
        || exact_replay.policy_search_report.sha256 != probability_search_sha256
    {
        bail!("probability decision provenance or outcome-isolation check failed");
    }
    let budget = &preregistration.exact_replay_budget;
    if budget.maximum_unique_traces == 0
        || budget.additional_discovery_hours != 0
        || budget.additional_parameter_variants != 0
        || probability_search.exact_replay_plan.maximum_replay_count != budget.maximum_unique_traces
        || probability_search.exact_replay_plan.entries.len() > budget.maximum_unique_traces
        || exact_replay.traces.len() != probability_search.exact_replay_plan.entries.len()
        || exact_replay.traces.len() > budget.maximum_unique_traces
        || (probability_search.safety_margin - ADVANCEMENT_WILSON_EDGE).abs() > 1e-12
        || probability_search
            .exact_replay_plan
            .entries
            .iter()
            .any(|entry| {
                entry.latency_ms != budget.latency_ms
                    || (entry.stake_usd - budget.stake_usd).abs() > 1e-12
                    || (entry.fee_rate - budget.fee_rate).abs() > 1e-12
            })
    {
        bail!("probability decision budget drifted from preregistration");
    }

    let trace_decisions = exact_replay
        .traces
        .iter()
        .map(|trace| ProbabilityTraceDecision {
            discovery_trace_sha256: trace.discovery_trace_sha256.clone(),
            representative_policy_id: trace.representative_policy_id.clone(),
            decision_seconds: trace.decision_seconds,
            maximum_ask: trace.maximum_ask,
            fills: trace.fills,
            wins: trace.wins,
            losses: trace.losses,
            point_estimate_edge: trace.point_estimate_edge,
            wilson_edge: trace.wilson_edge,
            total_pnl_usd: trace.total_pnl_usd,
            decision: if trace.promotion_confidence_ready {
                "freeze_for_one_fresh_test".to_string()
            } else {
                "rejected_failed_fixed_exact_gate".to_string()
            },
        })
        .collect::<Vec<_>>();
    let fresh_gate_opened = exact_replay
        .traces
        .iter()
        .any(|trace| trace.promotion_confidence_ready);
    let (decision, reason) = if fresh_gate_opened {
        (
            "freeze_winner_for_one_fresh_test",
            "At least one bounded exact trace has positive fee-aware PnL and Wilson edge above the fixed 0.02 margin.",
        )
    } else {
        (
            "reject_family_keep_fresh_sealed",
            "Every bounded exact trace failed the fixed 0.02 Wilson edge advancement gate; preregistration forbids more hours or variants.",
        )
    };
    let report = OpportunityProbabilityDecisionReport {
        schema_version: "opportunity_probability_bounded_decision_v1".to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        family_id: FAMILY_ID.to_string(),
        preregistration: HashedSource {
            path: input.preregistration_path.display().to_string(),
            sha256: preregistration_sha256,
        },
        probability_search_report: HashedSource {
            path: input.probability_search_report_path.display().to_string(),
            sha256: probability_search_sha256,
        },
        exact_replay_report: HashedSource {
            path: input.exact_replay_report_path.display().to_string(),
            sha256: exact_replay_sha256,
        },
        fixed_advancement_wilson_edge: ADVANCEMENT_WILSON_EDGE,
        maximum_exact_replays: budget.maximum_unique_traces,
        exact_replays_consumed: exact_replay.traces.len(),
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

fn validate_input(input: &OpportunityProbabilitySearchInput) -> Result<()> {
    if input.minimum_calibration_support == 0 || input.minimum_policy_support == 0 {
        bail!("support floors must be positive");
    }
    if input.maximum_exact_replays == 0 {
        bail!("maximum_exact_replays must be positive");
    }
    if !input.maximum_calibration_brier_score.is_finite()
        || input.maximum_calibration_brier_score <= 0.0
        || input.maximum_calibration_brier_score > 1.0
    {
        bail!("maximum calibration Brier score must be in (0, 1]");
    }
    if !input.safety_margin.is_finite() || input.safety_margin < 0.0 || input.safety_margin >= 1.0 {
        bail!("safety margin must be finite and in [0, 1)");
    }
    Ok(())
}

fn validate_labels_manifest(
    manifest: &OpportunityLabelsManifest,
    dataset_sha256: &str,
    dataset_seal_sha256: &str,
) -> Result<()> {
    if manifest.schema_version != OPPORTUNITY_LABEL_SCHEMA_VERSION
        || manifest.dataset_sha256 != dataset_sha256
        || manifest.dataset_seal.sha256 != dataset_seal_sha256
        || manifest.fresh_holdout_labels_present
    {
        bail!("label manifest does not match the outcome-safe causal dataset");
    }
    if sha256_file(&PathBuf::from(&manifest.output.path))? != manifest.output.sha256 {
        bail!("label table hash drifted");
    }
    Ok(())
}

fn policy_grid() -> Vec<ProbabilityPolicyDefinition> {
    let mut policies = Vec::new();
    for decision_seconds in [120, 180, 240] {
        for volatility_scale in [0.75, 1.0, 1.25] {
            for minimum_model_edge in [0.03, 0.05, 0.08] {
                for maximum_ask in [0.85, 0.90] {
                    policies.push(ProbabilityPolicyDefinition {
                        decision_seconds,
                        volatility_scale,
                        minimum_model_edge,
                        maximum_ask,
                    });
                }
            }
        }
    }
    policies
}

fn select_opportunity(
    row: &CausalOpportunity,
    policy: &ProbabilityPolicyDefinition,
) -> Option<SelectedOpportunity> {
    if decision_seconds(row) != Some(policy.decision_seconds)
        || !row.book_observable
        || !row.stake_fully_executable
        || row.best_ask.is_none_or(|ask| ask > policy.maximum_ask)
    {
        return None;
    }
    let break_even_probability = row.fee_aware_break_even_probability?;
    let model_probability = model_probability(row, policy.volatility_scale)?;
    if model_probability < break_even_probability + policy.minimum_model_edge {
        return None;
    }
    Some(SelectedOpportunity {
        model_probability,
        break_even_probability,
        net_win_usd: row.fee_aware_net_win_usd?,
        max_loss_usd: row.fee_aware_max_loss_usd?,
    })
}

fn model_probability(row: &CausalOpportunity, volatility_scale: f64) -> Option<f64> {
    if !(volatility_scale.is_finite()
        && volatility_scale > 0.0
        && row.btc_observed.is_finite()
        && row.btc_observed > 0.0
        && row.strike_price.is_finite()
        && row.strike_price > 0.0
        && row.remaining_seconds.is_finite()
        && row.remaining_seconds > 0.0
        && row.causal_volatility.is_finite()
        && row.causal_volatility > 0.0)
    {
        return None;
    }
    let up_probability = binary_option_price_with_rate(
        row.btc_observed,
        row.strike_price,
        row.remaining_seconds / 86_400.0,
        row.causal_volatility * volatility_scale,
        0.0,
    );
    match row.signal_direction.as_str() {
        "up" => Some(up_probability),
        "down" => Some(1.0 - up_probability),
        _ => None,
    }
}

fn decision_seconds(row: &CausalOpportunity) -> Option<u16> {
    [120u16, 180, 240]
        .into_iter()
        .find(|&value| (row.elapsed_seconds - f64::from(value)).abs() <= 0.001)
}

fn finish_policy(
    policy: ProbabilityPolicyDefinition,
    accumulator: PolicyAccumulator,
    semantics_version: &str,
    stake_usd: f64,
    fee_rate: f64,
    input: &OpportunityProbabilitySearchInput,
) -> FinishedPolicy {
    let policy_id = stable_json_hash(&serde_json::json!({
        "family_id": FAMILY_ID,
        "policy": policy,
    }));
    let calibration_support = accumulator.calibration_support;
    let calibration_brier_score = (calibration_support > 0)
        .then_some(accumulator.calibration_brier_sum / calibration_support as f64);
    let calibration_mean_probability = (calibration_support > 0)
        .then_some(accumulator.calibration_probability_sum / calibration_support as f64);
    let calibration_win_rate = (calibration_support > 0)
        .then_some(accumulator.calibration_wins as f64 / calibration_support as f64);
    let discovery_support = accumulator.discovery_wins + accumulator.discovery_losses;
    let win_rate = (discovery_support > 0)
        .then_some(accumulator.discovery_wins as f64 / discovery_support as f64);
    let wilson = (discovery_support > 0)
        .then_some(wilson_lower(accumulator.discovery_wins, discovery_support));
    let average_model_probability = (discovery_support > 0)
        .then_some(accumulator.discovery_probability_sum / discovery_support as f64);
    let average_break_even_probability = (discovery_support > 0)
        .then_some(accumulator.discovery_break_even_sum / discovery_support as f64);
    let average_model_edge = average_model_probability
        .zip(average_break_even_probability)
        .map(|(model, break_even)| model - break_even);
    let point_estimate_edge = win_rate
        .zip(average_break_even_probability)
        .map(|(rate, break_even)| rate - break_even);
    let wilson_edge = wilson
        .zip(average_break_even_probability)
        .map(|(lower, break_even)| lower - break_even);
    let discovery_trace_sha256 = (!accumulator.discovery_opportunity_ids.is_empty()).then(|| {
        trace_hash(
            "discovery",
            &accumulator.discovery_opportunity_ids,
            policy.maximum_ask,
            stake_usd,
            fee_rate,
            input.latency_ms,
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
            input.latency_ms,
            semantics_version,
        )
    });
    let mut rejection_reasons = Vec::new();
    if calibration_support < input.minimum_calibration_support {
        rejection_reasons.push("insufficient_older_selected_support".to_string());
    }
    if calibration_brier_score.is_none_or(|score| score > input.maximum_calibration_brier_score) {
        rejection_reasons.push("older_brier_score_above_ceiling".to_string());
    }
    if discovery_support < input.minimum_policy_support {
        rejection_reasons.push("insufficient_recent_discovery_support".to_string());
    }
    if point_estimate_edge.is_none_or(|edge| edge <= input.safety_margin) {
        rejection_reasons.push("point_edge_not_above_break_even_margin".to_string());
    }
    if accumulator.economic_payoff_proxy_usd <= 0.0 {
        rejection_reasons.push("nonpositive_fee_aware_payoff_proxy".to_string());
    }
    FinishedPolicy {
        discovery_opportunity_ids: accumulator.discovery_opportunity_ids,
        result: ProbabilityPolicyResult {
            policy_id,
            policy,
            calibration_support,
            calibration_wins: accumulator.calibration_wins,
            calibration_brier_score,
            calibration_mean_probability,
            calibration_win_rate,
            discovery_support,
            wins: accumulator.discovery_wins,
            losses: accumulator.discovery_losses,
            win_rate,
            wilson_win_rate_lower: wilson,
            average_model_probability,
            average_break_even_probability,
            average_model_edge,
            point_estimate_edge,
            wilson_edge,
            economic_payoff_proxy_usd: accumulator.economic_payoff_proxy_usd,
            fresh_causal_support: accumulator.fresh_opportunity_ids.len(),
            discovery_trace_sha256,
            fresh_support_trace_sha256,
            discovery_eligible: rejection_reasons.is_empty(),
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

fn diagnostic_order(
    left: &ProbabilityPolicyResult,
    right: &ProbabilityPolicyResult,
) -> std::cmp::Ordering {
    right
        .discovery_eligible
        .cmp(&left.discovery_eligible)
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
        .then_with(|| right.discovery_support.cmp(&left.discovery_support))
        .then_with(|| left.policy_id.cmp(&right.policy_id))
}

fn cap_replay_entries(
    replay_entries: Vec<ExactReplayPlanEntry>,
    maximum_exact_replays: usize,
) -> Vec<ExactReplayPlanEntry> {
    let mut selected = Vec::new();
    let mut traces = HashSet::new();
    let mut decision_times = HashSet::new();
    for entry in &replay_entries {
        if selected.len() >= maximum_exact_replays {
            break;
        }
        if decision_times.insert(entry.decision_seconds) {
            traces.insert(entry.discovery_trace_sha256.clone());
            selected.push(entry.clone());
        }
    }
    if selected.len() < maximum_exact_replays {
        for entry in replay_entries {
            if selected.len() >= maximum_exact_replays {
                break;
            }
            if traces.insert(entry.discovery_trace_sha256.clone()) {
                selected.push(entry);
            }
        }
    }
    selected
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

    fn opportunity(direction: &str) -> CausalOpportunity {
        CausalOpportunity {
            opportunity_id: "opportunity".to_string(),
            condition_id: "condition".to_string(),
            token_id: "token".to_string(),
            chronological_window: "recent_discovery".to_string(),
            window_start_ms: 0,
            observed_at_ms: 120_000,
            signal_direction: direction.to_string(),
            strike_price: 100.0,
            btc_observed: 101.0,
            elapsed_seconds: 120.0,
            remaining_seconds: 180.0,
            move_2m_usd: Some(1.0),
            path_2m_aligned: None,
            path_3m_aligned: None,
            path_4m_aligned: None,
            directional_distance_to_strike_usd: 1.0,
            causal_volatility: 0.5,
            book_observable: true,
            best_ask: Some(0.50),
            top_book_pressure: None,
            stake_fully_executable: true,
            fee_aware_break_even_probability: Some(0.52),
            fee_aware_net_win_usd: Some(4.0),
            fee_aware_max_loss_usd: Some(5.0),
            btc_open: 100.0,
            partial_twap_lead_usd: None,
            twap_locked_fraction: None,
        }
    }

    #[test]
    fn grid_is_preregistered_54_policy_family() {
        let grid = policy_grid();
        assert_eq!(grid.len(), 54);
        assert_eq!(grid, policy_grid());
    }

    #[test]
    fn up_and_down_probabilities_are_complements() {
        let up = model_probability(&opportunity("up"), 1.0).unwrap();
        let down = model_probability(&opportunity("down"), 1.0).unwrap();
        assert!((up + down - 1.0).abs() < 1e-12);
        assert!(up > 0.5);
    }

    #[test]
    fn selection_uses_model_edge_and_not_path_or_pressure() {
        let mut row = opportunity("up");
        row.path_2m_aligned = Some(false);
        row.top_book_pressure = Some(-1.0);
        let policy = ProbabilityPolicyDefinition {
            decision_seconds: 120,
            volatility_scale: 1.0,
            minimum_model_edge: 0.03,
            maximum_ask: 0.85,
        };
        assert!(select_opportunity(&row, &policy).is_some());
    }
}
