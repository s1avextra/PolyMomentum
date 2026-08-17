//! Outcome-blind block allocation lock for the frozen settlement-anchor screen.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::backtest::variant_io;
use crate::strategy::spec::stable_json_hash;

pub const SETTLEMENT_ANCHOR_MECHANISM_ID: &str = "settlement_source_anchor_v1";
pub const BINARY_COMPLEMENT_MECHANISM_ID: &str = "binary_complement_coherence_v1";
pub const SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK: usize = 750;
pub const SETTLEMENT_ANCHOR_PREREGISTRATION_SHA256: &str =
    "b322a0243c148f3f70f64bbac1433707fc908ee8a60a92e3cdbed2261dcd937d";
pub const SETTLEMENT_ANCHOR_VARIANT_FILE_SHA256: &str =
    "50ffad389f08f44fe18c4914c438542f413045df869302cce81b4a621325e6cf";
pub const SETTLEMENT_ANCHOR_VARIANT_PARAMS_HASH: &str =
    "a5d67641653ae85a853aab531060a240eade257e32fd5bf0e46392c7934302d5";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementAnchorAllocationBoundary {
    AfterBinaryBlock1Failed,
    AfterBinaryBlock2Sealed,
}

impl SettlementAnchorAllocationBoundary {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AfterBinaryBlock1Failed => "after_binary_block1_failed",
            Self::AfterBinaryBlock2Sealed => "after_binary_block2_sealed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashedArtifact {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForwardCondition {
    pub condition_id: String,
    pub window_start: String,
    /// Immutable sealed capture segment/report used by breadth, fold-tail,
    /// and five-report loss-burst gates.
    pub report_id: String,
    /// Public Polymarket Chainlink price to beat. Required for settlement-
    /// anchor blocks and omitted for prior non-anchor blocks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_price_to_beat: Option<f64>,
}

/// Outcome-free, sealed condition universe used only for block allocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForwardConditionSet {
    pub schema_version: u32,
    pub mechanism_id: String,
    pub block_id: String,
    pub block_sequence: u32,
    pub sealed_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocation_boundary: Option<SettlementAnchorAllocationBoundary>,
    pub conditions: Vec<ForwardCondition>,
    pub source_artifacts: Vec<HashedArtifact>,
}

#[derive(Debug, Clone)]
pub struct SettlementAnchorAllocationLockInput {
    pub preregistration_path: String,
    pub variant_path: String,
    pub candidate_condition_set_path: String,
    pub prior_condition_set_paths: Vec<String>,
    pub report_output_path: String,
    pub trades_output_path: String,
    pub pair_audit_output_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementAnchorScoreOutputs {
    pub report_json: String,
    pub trades_json: String,
    pub pair_audit_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockedConditionSet {
    pub path: String,
    pub sha256: String,
    pub mechanism_id: String,
    pub block_id: String,
    pub block_sequence: u32,
    pub condition_count: usize,
    pub condition_ids_hash: String,
    pub report_count: usize,
    pub report_partition_hash: String,
    pub window_start: Option<String>,
    pub window_end: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllocationCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementAnchorAllocationLock {
    pub schema_version: u32,
    pub generated_at: String,
    pub mechanism_id: String,
    pub status: String,
    pub ok: bool,
    pub preregistration: HashedArtifact,
    pub frozen_variant: HashedArtifact,
    pub frozen_variant_params_hash: String,
    pub score_outputs: SettlementAnchorScoreOutputs,
    pub allocation_boundary: Option<SettlementAnchorAllocationBoundary>,
    pub candidate_condition_set: LockedConditionSet,
    pub prior_condition_sets: Vec<LockedConditionSet>,
    pub allowed_condition_ids: Vec<String>,
    pub allowed_condition_ids_hash: String,
    pub overlapping_condition_ids: Vec<String>,
    pub checks: Vec<AllocationCheck>,
    pub failure_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementAnchorAllocationEvidence {
    pub path: String,
    pub sha256: String,
    pub block_id: String,
    pub block_sequence: u32,
    pub allocation_boundary: SettlementAnchorAllocationBoundary,
    pub condition_count: usize,
    pub condition_ids_hash: String,
    pub report_count: usize,
    pub report_partition_hash: String,
    pub candidate_condition_set_sha256: String,
    pub prior_condition_set_sha256: Vec<String>,
    pub preregistration_sha256: String,
    pub frozen_variant_sha256: String,
    pub frozen_variant_params_hash: String,
    pub score_outputs: SettlementAnchorScoreOutputs,
}

#[derive(Debug)]
struct ConditionSetAnalysis {
    ids: Vec<String>,
    report_partition: Vec<(String, String)>,
    report_count: usize,
    window_start: Option<DateTime<Utc>>,
    window_end: Option<DateTime<Utc>>,
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn read_hashed(path: &str, label: &str) -> Result<(Vec<u8>, HashedArtifact)> {
    let bytes = std::fs::read(path).with_context(|| format!("read {label} {path}"))?;
    Ok((
        bytes.clone(),
        HashedArtifact {
            path: path.to_string(),
            sha256: sha256_bytes(&bytes),
        },
    ))
}

fn valid_condition_id(value: &str) -> bool {
    value
        .strip_prefix("0x")
        .is_some_and(|raw| raw.len() == 64 && raw.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn push_check(
    checks: &mut Vec<AllocationCheck>,
    name: impl Into<String>,
    passed: bool,
    detail: impl Into<String>,
) {
    checks.push(AllocationCheck {
        name: name.into(),
        passed,
        detail: detail.into(),
    });
}

fn source_integrity(set: &ForwardConditionSet) -> (bool, String) {
    if set.source_artifacts.is_empty() {
        return (false, "source_artifacts=0".to_string());
    }
    for source in &set.source_artifacts {
        let Ok(bytes) = std::fs::read(&source.path) else {
            return (false, format!("unreadable={}", source.path));
        };
        let observed = sha256_bytes(&bytes);
        if observed != source.sha256 {
            return (
                false,
                format!(
                    "hash_mismatch={} expected={} observed={observed}",
                    source.path, source.sha256
                ),
            );
        }
    }
    (true, format!("verified={}", set.source_artifacts.len()))
}

fn analyze_condition_set(
    label: &str,
    set: &ForwardConditionSet,
    checks: &mut Vec<AllocationCheck>,
) -> ConditionSetAnalysis {
    push_check(
        checks,
        format!("{label}_schema"),
        set.schema_version == 1,
        format!("schema_version={}", set.schema_version),
    );
    push_check(
        checks,
        format!("{label}_fixed_condition_count"),
        set.conditions.len() == SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK,
        format!(
            "observed={} required={SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK}",
            set.conditions.len()
        ),
    );
    let supported_mechanism = matches!(
        set.mechanism_id.as_str(),
        SETTLEMENT_ANCHOR_MECHANISM_ID | BINARY_COMPLEMENT_MECHANISM_ID
    );
    push_check(
        checks,
        format!("{label}_supported_mechanism"),
        supported_mechanism,
        set.mechanism_id.clone(),
    );
    push_check(
        checks,
        format!("{label}_block_identity"),
        !set.block_id.trim().is_empty() && (1..=2).contains(&set.block_sequence),
        format!("block_id={} sequence={}", set.block_id, set.block_sequence),
    );
    let sealed_at = DateTime::parse_from_rfc3339(&set.sealed_at)
        .map(|value| value.with_timezone(&Utc))
        .ok();
    push_check(
        checks,
        format!("{label}_sealed_at"),
        sealed_at.is_some(),
        set.sealed_at.clone(),
    );

    let ids: Vec<_> = set
        .conditions
        .iter()
        .map(|condition| condition.condition_id.clone())
        .collect();
    let unique_ids: BTreeSet<_> = ids.iter().cloned().collect();
    let ids_valid = ids
        .iter()
        .all(|condition_id| valid_condition_id(condition_id));
    push_check(
        checks,
        format!("{label}_condition_ids"),
        ids_valid && unique_ids.len() == ids.len(),
        format!(
            "valid={} unique={} total={}",
            ids_valid,
            unique_ids.len(),
            ids.len()
        ),
    );
    let report_partition: Vec<_> = set
        .conditions
        .iter()
        .map(|condition| (condition.condition_id.clone(), condition.report_id.clone()))
        .collect();
    let report_ids_valid = set
        .conditions
        .iter()
        .all(|condition| !condition.report_id.trim().is_empty());
    let mut seen_reports = BTreeSet::new();
    let mut closed_reports = BTreeSet::new();
    let mut previous_report: Option<&str> = None;
    let mut contiguous_reports = true;
    let mut report_sizes: BTreeMap<&str, usize> = BTreeMap::new();
    for condition in &set.conditions {
        *report_sizes
            .entry(condition.report_id.as_str())
            .or_default() += 1;
        if previous_report != Some(condition.report_id.as_str()) {
            if let Some(previous) = previous_report {
                closed_reports.insert(previous);
            }
            if closed_reports.contains(condition.report_id.as_str()) {
                contiguous_reports = false;
            }
            seen_reports.insert(condition.report_id.as_str());
            previous_report = Some(condition.report_id.as_str());
        }
    }
    let report_sizes_valid = report_sizes.values().all(|size| (1..=24).contains(size));
    let report_count = seen_reports.len();
    push_check(
        checks,
        format!("{label}_fixed_report_partition"),
        report_ids_valid && contiguous_reports && report_sizes_valid && report_count >= 20,
        format!(
            "nonempty={report_ids_valid} contiguous={contiguous_reports} max_24={report_sizes_valid} reports={report_count} required_min=20"
        ),
    );
    let published_prices_valid = if set.mechanism_id == SETTLEMENT_ANCHOR_MECHANISM_ID {
        set.conditions.iter().all(|condition| {
            condition
                .published_price_to_beat
                .is_some_and(|price| price.is_finite() && price > 0.0)
        })
    } else {
        true
    };
    push_check(
        checks,
        format!("{label}_published_price_to_beat"),
        published_prices_valid,
        format!(
            "required={} valid={published_prices_valid}",
            set.mechanism_id == SETTLEMENT_ANCHOR_MECHANISM_ID
        ),
    );

    let parsed_windows: Vec<_> = set
        .conditions
        .iter()
        .map(|condition| {
            DateTime::parse_from_rfc3339(&condition.window_start)
                .map(|value| value.with_timezone(&Utc))
        })
        .collect();
    let windows_valid = parsed_windows.iter().all(Result::is_ok);
    let windows: Vec<_> = parsed_windows.into_iter().filter_map(Result::ok).collect();
    let five_minute_aligned = windows
        .iter()
        .all(|window| window.timestamp().rem_euclid(300) == 0);
    let chronological = windows.windows(2).all(|pair| pair[0] < pair[1]);
    push_check(
        checks,
        format!("{label}_chronological_five_minute_windows"),
        windows_valid
            && windows.len() == set.conditions.len()
            && five_minute_aligned
            && chronological,
        format!(
            "parse_valid={windows_valid} five_minute_aligned={five_minute_aligned} strictly_increasing={chronological}"
        ),
    );
    let sealed_after_window = match (sealed_at, windows.last()) {
        (Some(sealed_at), Some(last_window)) => {
            sealed_at >= *last_window + chrono::Duration::seconds(300)
        }
        _ => false,
    };
    push_check(
        checks,
        format!("{label}_sealed_after_window"),
        sealed_after_window,
        format!("sealed_at={sealed_at:?} last_window={:?}", windows.last()),
    );
    let (sources_valid, sources_detail) = source_integrity(set);
    push_check(
        checks,
        format!("{label}_source_hashes"),
        sources_valid,
        sources_detail,
    );

    ConditionSetAnalysis {
        ids,
        report_partition,
        report_count,
        window_start: windows.first().copied(),
        window_end: windows.last().copied(),
    }
}

fn locked_condition_set(
    path: String,
    sha256: String,
    set: &ForwardConditionSet,
    analysis: &ConditionSetAnalysis,
) -> LockedConditionSet {
    let mut ids = analysis.ids.clone();
    ids.sort();
    LockedConditionSet {
        path,
        sha256,
        mechanism_id: set.mechanism_id.clone(),
        block_id: set.block_id.clone(),
        block_sequence: set.block_sequence,
        condition_count: set.conditions.len(),
        condition_ids_hash: stable_json_hash(&ids),
        report_count: analysis.report_count,
        report_partition_hash: stable_json_hash(&analysis.report_partition),
        window_start: analysis.window_start.map(|value| value.to_rfc3339()),
        window_end: analysis.window_end.map(|value| value.to_rfc3339()),
    }
}

fn build_settlement_anchor_allocation_lock_internal(
    input: SettlementAnchorAllocationLockInput,
    require_unused_score_outputs: bool,
) -> Result<SettlementAnchorAllocationLock> {
    if input.prior_condition_set_paths.is_empty() {
        bail!("settlement-anchor allocation requires at least one prior condition set");
    }
    let mut supplied_paths = input.prior_condition_set_paths.clone();
    supplied_paths.push(input.candidate_condition_set_path.clone());
    let unique_paths: BTreeSet<_> = supplied_paths.iter().collect();
    if unique_paths.len() != supplied_paths.len() {
        bail!("candidate and prior condition-set paths must be distinct");
    }

    let (preregistration_bytes, preregistration) = read_hashed(
        &input.preregistration_path,
        "settlement-anchor preregistration",
    )?;
    let preregistration_json: serde_json::Value = serde_json::from_slice(&preregistration_bytes)
        .context("parse settlement-anchor preregistration")?;
    let (variant_bytes, frozen_variant) =
        read_hashed(&input.variant_path, "settlement-anchor frozen variant")?;
    let variants = variant_io::read_variants(&input.variant_path)?;
    let frozen_variant_params_hash = variants.first().map(stable_json_hash).unwrap_or_default();

    let (candidate_bytes, candidate_artifact) = read_hashed(
        &input.candidate_condition_set_path,
        "candidate condition set",
    )?;
    let candidate: ForwardConditionSet =
        serde_json::from_slice(&candidate_bytes).context("parse candidate condition set")?;

    let mut prior_sets = Vec::with_capacity(input.prior_condition_set_paths.len());
    for path in &input.prior_condition_set_paths {
        let (bytes, artifact) = read_hashed(path, "prior condition set")?;
        let set: ForwardConditionSet = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse prior condition set {path}"))?;
        prior_sets.push((set, artifact));
    }

    let mut checks = Vec::new();
    push_check(
        &mut checks,
        "preregistration_hash",
        preregistration.sha256 == SETTLEMENT_ANCHOR_PREREGISTRATION_SHA256,
        format!(
            "observed={} required={SETTLEMENT_ANCHOR_PREREGISTRATION_SHA256}",
            preregistration.sha256
        ),
    );
    let preregistration_contract_valid = preregistration_json["mechanism_id"]
        == SETTLEMENT_ANCHOR_MECHANISM_ID
        && preregistration_json["status"]
            == "PREREGISTERED_FOR_DISJOINT_FUTURE_BLOCK_NO_ACTIVE_BLOCK_SCORE";
    push_check(
        &mut checks,
        "preregistration_contract",
        preregistration_contract_valid,
        format!(
            "mechanism_id={} status={}",
            preregistration_json["mechanism_id"], preregistration_json["status"]
        ),
    );
    let variant_valid = frozen_variant.sha256 == SETTLEMENT_ANCHOR_VARIANT_FILE_SHA256
        && variants.len() == 1
        && frozen_variant_params_hash == SETTLEMENT_ANCHOR_VARIANT_PARAMS_HASH;
    push_check(
        &mut checks,
        "frozen_variant",
        variant_valid,
        format!(
            "file_sha256={} variants={} params_hash={frozen_variant_params_hash}",
            frozen_variant.sha256,
            variants.len()
        ),
    );
    // Keep the raw bytes in scope so the exact file hash above cannot be replaced
    // by a serialization hash of the parsed variant.
    let _ = variant_bytes;
    let score_outputs = SettlementAnchorScoreOutputs {
        report_json: input.report_output_path,
        trades_json: input.trades_output_path,
        pair_audit_json: input.pair_audit_output_path,
    };
    let output_paths_error = require_unused_score_outputs
        .then(|| settlement_anchor_score_output_paths_error(&score_outputs))
        .flatten();
    push_check(
        &mut checks,
        "single_use_score_outputs",
        output_paths_error.is_none(),
        output_paths_error.unwrap_or_else(|| {
            format!(
                "report={} trades={} pair_audit={}",
                score_outputs.report_json, score_outputs.trades_json, score_outputs.pair_audit_json
            )
        }),
    );

    let candidate_analysis = analyze_condition_set("candidate", &candidate, &mut checks);
    let mut prior_analyses = Vec::with_capacity(prior_sets.len());
    for (index, (set, _)) in prior_sets.iter().enumerate() {
        prior_analyses.push(analyze_condition_set(
            &format!("prior_{}", index + 1),
            set,
            &mut checks,
        ));
    }

    let candidate_contract = candidate.mechanism_id == SETTLEMENT_ANCHOR_MECHANISM_ID
        && (1..=2).contains(&candidate.block_sequence)
        && candidate.allocation_boundary.is_some();
    push_check(
        &mut checks,
        "candidate_contract",
        candidate_contract,
        format!(
            "mechanism_id={} sequence={} boundary={:?}",
            candidate.mechanism_id, candidate.block_sequence, candidate.allocation_boundary
        ),
    );

    let mut binary_sequences = Vec::new();
    let mut anchor_sequences = Vec::new();
    for (set, _) in &prior_sets {
        match set.mechanism_id.as_str() {
            BINARY_COMPLEMENT_MECHANISM_ID => binary_sequences.push(set.block_sequence),
            SETTLEMENT_ANCHOR_MECHANISM_ID => anchor_sequences.push(set.block_sequence),
            _ => {}
        }
    }
    binary_sequences.sort_unstable();
    anchor_sequences.sort_unstable();
    let expected_binary_sequences = match candidate.allocation_boundary {
        Some(SettlementAnchorAllocationBoundary::AfterBinaryBlock1Failed) => vec![1],
        Some(SettlementAnchorAllocationBoundary::AfterBinaryBlock2Sealed) => vec![1, 2],
        None => Vec::new(),
    };
    let expected_anchor_sequences: Vec<u32> = (1..candidate.block_sequence).collect();
    let prior_boundaries_valid = prior_sets.iter().all(|(set, _)| {
        if set.mechanism_id == BINARY_COMPLEMENT_MECHANISM_ID {
            set.allocation_boundary.is_none()
        } else if set.mechanism_id == SETTLEMENT_ANCHOR_MECHANISM_ID {
            set.allocation_boundary == candidate.allocation_boundary
        } else {
            false
        }
    });
    let prior_shape_valid = binary_sequences == expected_binary_sequences
        && anchor_sequences == expected_anchor_sequences
        && prior_sets.len() == binary_sequences.len() + anchor_sequences.len()
        && prior_boundaries_valid;
    push_check(
        &mut checks,
        "complete_prior_block_shape",
        prior_shape_valid,
        format!(
            "binary={binary_sequences:?} required={expected_binary_sequences:?} anchor={anchor_sequences:?} required={expected_anchor_sequences:?} boundaries_valid={prior_boundaries_valid}"
        ),
    );

    let mut block_ids = BTreeSet::new();
    let block_ids_unique = std::iter::once(&candidate)
        .chain(prior_sets.iter().map(|(set, _)| set))
        .all(|set| block_ids.insert(set.block_id.clone()));
    push_check(
        &mut checks,
        "block_ids_unique",
        block_ids_unique,
        format!("unique={} total={}", block_ids.len(), prior_sets.len() + 1),
    );

    let prior_windows: Vec<_> = prior_analyses
        .iter()
        .filter_map(|analysis| Some((analysis.window_start?, analysis.window_end?)))
        .collect();
    let candidate_after_priors = match (
        candidate_analysis.window_start,
        prior_windows.iter().map(|(_, end)| *end).max(),
    ) {
        (Some(candidate_start), Some(latest_prior_end)) => candidate_start > latest_prior_end,
        _ => false,
    };
    push_check(
        &mut checks,
        "candidate_strictly_after_all_prior_blocks",
        candidate_after_priors,
        format!(
            "candidate_start={:?} latest_prior_end={:?}",
            candidate_analysis.window_start,
            prior_windows.iter().map(|(_, end)| *end).max()
        ),
    );

    let mut chronological_windows = prior_windows.clone();
    chronological_windows.sort_by_key(|(start, _)| *start);
    let prior_windows_disjoint = chronological_windows
        .windows(2)
        .all(|pair| pair[0].1 < pair[1].0);
    push_check(
        &mut checks,
        "prior_windows_chronological_and_disjoint",
        prior_windows.len() == prior_sets.len() && prior_windows_disjoint,
        format!(
            "valid_windows={} prior_sets={} non_overlapping={prior_windows_disjoint}",
            prior_windows.len(),
            prior_sets.len()
        ),
    );

    let mut id_owners: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for condition_id in &candidate_analysis.ids {
        id_owners
            .entry(condition_id.clone())
            .or_default()
            .push(candidate.block_id.clone());
    }
    for ((set, _), analysis) in prior_sets.iter().zip(&prior_analyses) {
        for condition_id in &analysis.ids {
            id_owners
                .entry(condition_id.clone())
                .or_default()
                .push(set.block_id.clone());
        }
    }
    let overlapping_condition_ids: Vec<_> = id_owners
        .iter()
        .filter(|(_, owners)| owners.len() > 1)
        .map(|(condition_id, _)| condition_id.clone())
        .collect();
    push_check(
        &mut checks,
        "all_condition_sets_disjoint",
        overlapping_condition_ids.is_empty(),
        format!("overlap_count={}", overlapping_condition_ids.len()),
    );

    let mut allowed_condition_ids = candidate_analysis.ids.clone();
    allowed_condition_ids.sort();
    let allowed_condition_ids_hash = stable_json_hash(&allowed_condition_ids);
    let failure_reasons: Vec<_> = checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| format!("check_failed:{}", check.name))
        .collect();
    let ok = failure_reasons.is_empty();

    let candidate_condition_set = locked_condition_set(
        candidate_artifact.path,
        candidate_artifact.sha256,
        &candidate,
        &candidate_analysis,
    );
    let prior_condition_sets = prior_sets
        .into_iter()
        .zip(prior_analyses)
        .map(|((set, artifact), analysis)| {
            locked_condition_set(artifact.path, artifact.sha256, &set, &analysis)
        })
        .collect();

    Ok(SettlementAnchorAllocationLock {
        schema_version: 1,
        generated_at: Utc::now().to_rfc3339(),
        mechanism_id: SETTLEMENT_ANCHOR_MECHANISM_ID.to_string(),
        status: if ok {
            "BLOCK_ALLOCATION_LOCKED_EVALUATION_ALLOWED"
        } else {
            "REJECT_BLOCK_ALLOCATION"
        }
        .to_string(),
        ok,
        preregistration,
        frozen_variant,
        frozen_variant_params_hash,
        score_outputs,
        allocation_boundary: candidate.allocation_boundary,
        candidate_condition_set,
        prior_condition_sets,
        allowed_condition_ids,
        allowed_condition_ids_hash,
        overlapping_condition_ids,
        checks,
        failure_reasons,
    })
}

pub fn build_settlement_anchor_allocation_lock(
    input: SettlementAnchorAllocationLockInput,
) -> Result<SettlementAnchorAllocationLock> {
    build_settlement_anchor_allocation_lock_internal(input, true)
}

fn lock_substance_hash(lock: &SettlementAnchorAllocationLock) -> String {
    let mut lock = lock.clone();
    lock.generated_at.clear();
    stable_json_hash(&lock)
}

pub fn validate_settlement_anchor_allocation_lock(
    path: impl AsRef<Path>,
    supplied_condition_ids: &[String],
    report_output_path: &str,
    trades_output_path: &str,
) -> Result<SettlementAnchorAllocationEvidence> {
    let path = path.as_ref();
    let bytes = std::fs::read(path)
        .with_context(|| format!("read settlement-anchor allocation lock {}", path.display()))?;
    let lock: SettlementAnchorAllocationLock = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse settlement-anchor allocation lock {}", path.display()))?;
    if lock.schema_version != 1
        || lock.mechanism_id != SETTLEMENT_ANCHOR_MECHANISM_ID
        || lock.status != "BLOCK_ALLOCATION_LOCKED_EVALUATION_ALLOWED"
        || !lock.ok
        || lock.checks.iter().any(|check| !check.passed)
        || !lock.failure_reasons.is_empty()
        || !lock.overlapping_condition_ids.is_empty()
    {
        bail!("allocation lock is not a passing settlement-anchor lock");
    }

    let rebuilt = build_settlement_anchor_allocation_lock(SettlementAnchorAllocationLockInput {
        preregistration_path: lock.preregistration.path.clone(),
        variant_path: lock.frozen_variant.path.clone(),
        candidate_condition_set_path: lock.candidate_condition_set.path.clone(),
        prior_condition_set_paths: lock
            .prior_condition_sets
            .iter()
            .map(|set| set.path.clone())
            .collect(),
        report_output_path: lock.score_outputs.report_json.clone(),
        trades_output_path: lock.score_outputs.trades_json.clone(),
        pair_audit_output_path: lock.score_outputs.pair_audit_json.clone(),
    })?;
    if !rebuilt.ok || lock_substance_hash(&lock) != lock_substance_hash(&rebuilt) {
        bail!("allocation lock contents or pinned condition-set sources have drifted");
    }

    let supplied: Vec<_> = supplied_condition_ids
        .iter()
        .map(|condition_id| condition_id.trim().to_string())
        .collect();
    if supplied.iter().any(|condition_id| condition_id.is_empty()) {
        bail!("settlement-anchor condition allowlist contains an empty ID");
    }
    let supplied_unique: BTreeSet<_> = supplied.iter().cloned().collect();
    if supplied_unique.len() != supplied.len() {
        bail!("settlement-anchor condition allowlist contains duplicate IDs");
    }
    let mut supplied_sorted = supplied;
    supplied_sorted.sort();
    if supplied_sorted != lock.allowed_condition_ids {
        bail!("settlement-anchor condition allowlist does not exactly match the allocation lock");
    }
    if stable_json_hash(&supplied_sorted) != lock.allowed_condition_ids_hash {
        bail!("settlement-anchor condition allowlist hash does not match the allocation lock");
    }
    if report_output_path != lock.score_outputs.report_json
        || trades_output_path != lock.score_outputs.trades_json
    {
        bail!("settlement-anchor score output paths do not match the allocation lock");
    }

    let allocation_boundary = lock
        .allocation_boundary
        .context("passing allocation lock is missing its boundary")?;
    Ok(SettlementAnchorAllocationEvidence {
        path: path.display().to_string(),
        sha256: sha256_bytes(&bytes),
        block_id: lock.candidate_condition_set.block_id,
        block_sequence: lock.candidate_condition_set.block_sequence,
        allocation_boundary,
        condition_count: lock.allowed_condition_ids.len(),
        condition_ids_hash: lock.allowed_condition_ids_hash,
        report_count: lock.candidate_condition_set.report_count,
        report_partition_hash: lock.candidate_condition_set.report_partition_hash.clone(),
        candidate_condition_set_sha256: lock.candidate_condition_set.sha256,
        prior_condition_set_sha256: lock
            .prior_condition_sets
            .iter()
            .map(|set| set.sha256.clone())
            .collect(),
        preregistration_sha256: lock.preregistration.sha256,
        frozen_variant_sha256: lock.frozen_variant.sha256,
        frozen_variant_params_hash: lock.frozen_variant_params_hash,
        score_outputs: lock.score_outputs,
    })
}

pub fn revalidate_settlement_anchor_allocation_lock(
    path: impl AsRef<Path>,
) -> Result<SettlementAnchorAllocationLock> {
    let path = path.as_ref();
    let bytes = std::fs::read(path)
        .with_context(|| format!("read settlement-anchor allocation lock {}", path.display()))?;
    let lock: SettlementAnchorAllocationLock = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse settlement-anchor allocation lock {}", path.display()))?;
    if lock.schema_version != 1
        || lock.mechanism_id != SETTLEMENT_ANCHOR_MECHANISM_ID
        || lock.status != "BLOCK_ALLOCATION_LOCKED_EVALUATION_ALLOWED"
        || !lock.ok
        || lock.checks.iter().any(|check| !check.passed)
        || !lock.failure_reasons.is_empty()
        || !lock.overlapping_condition_ids.is_empty()
    {
        bail!("allocation lock is not a passing settlement-anchor lock");
    }
    let rebuilt = build_settlement_anchor_allocation_lock_internal(
        SettlementAnchorAllocationLockInput {
            preregistration_path: lock.preregistration.path.clone(),
            variant_path: lock.frozen_variant.path.clone(),
            candidate_condition_set_path: lock.candidate_condition_set.path.clone(),
            prior_condition_set_paths: lock
                .prior_condition_sets
                .iter()
                .map(|set| set.path.clone())
                .collect(),
            report_output_path: lock.score_outputs.report_json.clone(),
            trades_output_path: lock.score_outputs.trades_json.clone(),
            pair_audit_output_path: lock.score_outputs.pair_audit_json.clone(),
        },
        false,
    )?;
    if !rebuilt.ok || lock_substance_hash(&lock) != lock_substance_hash(&rebuilt) {
        bail!("allocation lock contents or pinned condition-set sources have drifted");
    }
    Ok(lock)
}

pub fn settlement_anchor_output_paths_error(
    report_path: &str,
    trades_path: &str,
) -> Option<String> {
    if Path::new(report_path) == Path::new(trades_path) {
        return Some("settlement-anchor report and trade outputs must be distinct".to_string());
    }
    let existing: Vec<_> = [report_path, trades_path]
        .into_iter()
        .filter(|path| PathBuf::from(path).exists())
        .collect();
    if !existing.is_empty() {
        return Some(format!(
            "settlement-anchor score outputs already exist and cannot be reused: {}",
            existing.join(",")
        ));
    }
    None
}

fn settlement_anchor_score_output_paths_error(
    outputs: &SettlementAnchorScoreOutputs,
) -> Option<String> {
    let paths = [
        outputs.report_json.as_str(),
        outputs.trades_json.as_str(),
        outputs.pair_audit_json.as_str(),
    ];
    let unique: BTreeSet<_> = paths.into_iter().collect();
    if unique.len() != paths.len() {
        return Some(
            "settlement-anchor report, trade, and pair-audit outputs must be distinct".to_string(),
        );
    }
    let existing: Vec<_> = paths
        .into_iter()
        .filter(|path| PathBuf::from(path).exists())
        .collect();
    if !existing.is_empty() {
        return Some(format!(
            "settlement-anchor score outputs already exist and cannot be reused: {}",
            existing.join(",")
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry_path(name: &str) -> String {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../deploy/promotions/evidence/strategy_registry")
            .join(name)
            .display()
            .to_string()
    }

    fn condition_id(index: usize) -> String {
        format!("0x{index:064x}")
    }

    #[allow(clippy::too_many_arguments)]
    fn write_condition_set(
        temp: &tempfile::TempDir,
        filename: &str,
        mechanism_id: &str,
        block_id: &str,
        block_sequence: u32,
        boundary: Option<SettlementAnchorAllocationBoundary>,
        first_index: usize,
        start: DateTime<Utc>,
    ) -> String {
        let source_path = temp.path().join(format!("{filename}.source.json"));
        std::fs::write(&source_path, b"{\"sealed\":true}\n").unwrap();
        let source_sha256 = sha256_bytes(&std::fs::read(&source_path).unwrap());
        let set = ForwardConditionSet {
            schema_version: 1,
            mechanism_id: mechanism_id.to_string(),
            block_id: block_id.to_string(),
            block_sequence,
            sealed_at: (start + chrono::Duration::days(5)).to_rfc3339(),
            allocation_boundary: boundary,
            conditions: (0..SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK)
                .map(|offset| ForwardCondition {
                    condition_id: condition_id(first_index + offset + 1),
                    window_start: (start + chrono::Duration::seconds(offset as i64 * 300))
                        .to_rfc3339(),
                    report_id: format!("{block_id}-segment-{:03}", offset / 24 + 1),
                    published_price_to_beat: (mechanism_id == SETTLEMENT_ANCHOR_MECHANISM_ID)
                        .then_some(100_000.0 + offset as f64),
                })
                .collect(),
            source_artifacts: vec![HashedArtifact {
                path: source_path.display().to_string(),
                sha256: source_sha256,
            }],
        };
        let path = temp.path().join(filename);
        std::fs::write(&path, serde_json::to_vec_pretty(&set).unwrap()).unwrap();
        path.display().to_string()
    }

    fn fixture(
        candidate_first_index: usize,
        boundary: SettlementAnchorAllocationBoundary,
    ) -> (
        tempfile::TempDir,
        SettlementAnchorAllocationLockInput,
        Vec<String>,
    ) {
        let temp = tempfile::TempDir::new().unwrap();
        let prior_start = "2026-07-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let prior = write_condition_set(
            &temp,
            "binary-block-1.json",
            BINARY_COMPLEMENT_MECHANISM_ID,
            "binary-block-1",
            1,
            None,
            0,
            prior_start,
        );
        let candidate = write_condition_set(
            &temp,
            "anchor-block-1.json",
            SETTLEMENT_ANCHOR_MECHANISM_ID,
            "anchor-block-1",
            1,
            Some(boundary),
            candidate_first_index,
            prior_start
                + chrono::Duration::seconds(SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK as i64 * 300),
        );
        let candidate_set: ForwardConditionSet =
            serde_json::from_slice(&std::fs::read(&candidate).unwrap()).unwrap();
        let ids = candidate_set
            .conditions
            .into_iter()
            .map(|condition| condition.condition_id)
            .collect();
        let input = SettlementAnchorAllocationLockInput {
            preregistration_path: registry_path(
                "20260721_settlement_source_anchor_preregistration.json",
            ),
            variant_path: registry_path("20260721_settlement_source_anchor_baseline_variant.json"),
            candidate_condition_set_path: candidate,
            prior_condition_set_paths: vec![prior],
            report_output_path: temp.path().join("score-report.json").display().to_string(),
            trades_output_path: temp.path().join("score-trades.json").display().to_string(),
            pair_audit_output_path: temp
                .path()
                .join("score-pair-audit.json")
                .display()
                .to_string(),
        };
        (temp, input, ids)
    }

    #[test]
    fn allocation_lock_passes_exact_disjoint_750_condition_block() {
        let (temp, input, mut ids) = fixture(
            SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK,
            SettlementAnchorAllocationBoundary::AfterBinaryBlock1Failed,
        );
        let lock = build_settlement_anchor_allocation_lock(input).unwrap();
        assert!(lock.ok);
        assert_eq!(lock.status, "BLOCK_ALLOCATION_LOCKED_EVALUATION_ALLOWED");
        assert_eq!(lock.allowed_condition_ids.len(), 750);
        assert!(lock.overlapping_condition_ids.is_empty());
        assert!(lock.checks.iter().all(|check| check.passed));

        let lock_path = temp.path().join("allocation-lock.json");
        std::fs::write(&lock_path, serde_json::to_vec_pretty(&lock).unwrap()).unwrap();
        ids.reverse();
        let evidence = validate_settlement_anchor_allocation_lock(
            &lock_path,
            &ids,
            &lock.score_outputs.report_json,
            &lock.score_outputs.trades_json,
        )
        .unwrap();
        assert_eq!(evidence.condition_count, 750);
        assert_eq!(evidence.condition_ids_hash, lock.allowed_condition_ids_hash);
        let error = validate_settlement_anchor_allocation_lock(
            &lock_path,
            &ids,
            "/tmp/different-score-report.json",
            &lock.score_outputs.trades_json,
        )
        .unwrap_err();
        assert!(error.to_string().contains("output paths"));
    }

    #[test]
    fn allocation_lock_rejects_condition_overlap() {
        let (_temp, input, _) = fixture(
            0,
            SettlementAnchorAllocationBoundary::AfterBinaryBlock1Failed,
        );
        let lock = build_settlement_anchor_allocation_lock(input).unwrap();
        assert!(!lock.ok);
        assert_eq!(lock.overlapping_condition_ids.len(), 750);
        assert!(lock
            .failure_reasons
            .contains(&"check_failed:all_condition_sets_disjoint".to_string()));
    }

    #[test]
    fn allocation_lock_rejects_incomplete_binary_boundary() {
        let (_temp, input, _) = fixture(
            SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK,
            SettlementAnchorAllocationBoundary::AfterBinaryBlock2Sealed,
        );
        let lock = build_settlement_anchor_allocation_lock(input).unwrap();
        assert!(!lock.ok);
        assert!(lock
            .failure_reasons
            .contains(&"check_failed:complete_prior_block_shape".to_string()));
    }

    #[test]
    fn allocation_lock_rejects_nonchronological_or_unsealed_candidate() {
        let (_temp, input, _) = fixture(
            SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK,
            SettlementAnchorAllocationBoundary::AfterBinaryBlock1Failed,
        );
        let mut candidate: ForwardConditionSet =
            serde_json::from_slice(&std::fs::read(&input.candidate_condition_set_path).unwrap())
                .unwrap();
        candidate.conditions[749].window_start = candidate.conditions[748].window_start.clone();
        candidate.sealed_at = candidate.conditions[0].window_start.clone();
        std::fs::write(
            &input.candidate_condition_set_path,
            serde_json::to_vec_pretty(&candidate).unwrap(),
        )
        .unwrap();

        let lock = build_settlement_anchor_allocation_lock(input).unwrap();
        assert!(!lock.ok);
        assert!(lock
            .failure_reasons
            .contains(&"check_failed:candidate_chronological_five_minute_windows".to_string()));
        assert!(lock
            .failure_reasons
            .contains(&"check_failed:candidate_sealed_after_window".to_string()));
    }

    #[test]
    fn allocation_lock_rejects_mutable_report_partition_or_missing_public_strike() {
        let (_temp, input, _) = fixture(
            SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK,
            SettlementAnchorAllocationBoundary::AfterBinaryBlock1Failed,
        );
        let mut candidate: ForwardConditionSet =
            serde_json::from_slice(&std::fs::read(&input.candidate_condition_set_path).unwrap())
                .unwrap();
        for condition in &mut candidate.conditions {
            condition.report_id = "one-retrofitted-report".to_string();
        }
        candidate.conditions[0].published_price_to_beat = None;
        std::fs::write(
            &input.candidate_condition_set_path,
            serde_json::to_vec_pretty(&candidate).unwrap(),
        )
        .unwrap();

        let lock = build_settlement_anchor_allocation_lock(input).unwrap();
        assert!(!lock.ok);
        assert!(lock
            .failure_reasons
            .contains(&"check_failed:candidate_fixed_report_partition".to_string()));
        assert!(lock
            .failure_reasons
            .contains(&"check_failed:candidate_published_price_to_beat".to_string()));
    }

    #[test]
    fn allocation_lock_validation_detects_condition_set_tampering() {
        let (temp, input, ids) = fixture(
            SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK,
            SettlementAnchorAllocationBoundary::AfterBinaryBlock1Failed,
        );
        let candidate_path = input.candidate_condition_set_path.clone();
        let lock = build_settlement_anchor_allocation_lock(input).unwrap();
        let lock_path = temp.path().join("allocation-lock.json");
        std::fs::write(&lock_path, serde_json::to_vec_pretty(&lock).unwrap()).unwrap();

        let mut candidate: ForwardConditionSet =
            serde_json::from_slice(&std::fs::read(&candidate_path).unwrap()).unwrap();
        candidate.conditions[0].condition_id = condition_id(9_999_999);
        std::fs::write(
            &candidate_path,
            serde_json::to_vec_pretty(&candidate).unwrap(),
        )
        .unwrap();

        let error = validate_settlement_anchor_allocation_lock(
            &lock_path,
            &ids,
            &lock.score_outputs.report_json,
            &lock.score_outputs.trades_json,
        )
        .unwrap_err();
        assert!(error.to_string().contains("drifted"));
    }

    #[test]
    fn allocation_lock_validation_rejects_duplicate_or_different_allowlist() {
        let (temp, input, mut ids) = fixture(
            SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK,
            SettlementAnchorAllocationBoundary::AfterBinaryBlock1Failed,
        );
        let lock = build_settlement_anchor_allocation_lock(input).unwrap();
        let lock_path = temp.path().join("allocation-lock.json");
        std::fs::write(&lock_path, serde_json::to_vec_pretty(&lock).unwrap()).unwrap();

        ids[1] = ids[0].clone();
        let error = validate_settlement_anchor_allocation_lock(
            &lock_path,
            &ids,
            &lock.score_outputs.report_json,
            &lock.score_outputs.trades_json,
        )
        .unwrap_err();
        assert!(error.to_string().contains("duplicate"));
    }

    #[test]
    fn settlement_anchor_outputs_are_single_use_and_distinct() {
        let temp = tempfile::TempDir::new().unwrap();
        let report = temp.path().join("report.json");
        let trades = temp.path().join("trades.json");
        let pair_audit = temp.path().join("pair-audit.json");
        assert!(settlement_anchor_output_paths_error(
            report.to_str().unwrap(),
            trades.to_str().unwrap()
        )
        .is_none());
        assert!(settlement_anchor_output_paths_error(
            report.to_str().unwrap(),
            report.to_str().unwrap()
        )
        .unwrap()
        .contains("distinct"));
        std::fs::write(&report, b"{}").unwrap();
        assert!(settlement_anchor_output_paths_error(
            report.to_str().unwrap(),
            trades.to_str().unwrap()
        )
        .unwrap()
        .contains("already exist"));

        let outputs = SettlementAnchorScoreOutputs {
            report_json: temp.path().join("new-report.json").display().to_string(),
            trades_json: temp.path().join("new-trades.json").display().to_string(),
            pair_audit_json: pair_audit.display().to_string(),
        };
        assert!(settlement_anchor_score_output_paths_error(&outputs).is_none());
        let mut duplicate = outputs.clone();
        duplicate.pair_audit_json = duplicate.trades_json.clone();
        assert!(settlement_anchor_score_output_paths_error(&duplicate)
            .unwrap()
            .contains("distinct"));
        std::fs::write(&pair_audit, b"{}").unwrap();
        assert!(settlement_anchor_score_output_paths_error(&outputs)
            .unwrap()
            .contains("already exist"));
    }
}
