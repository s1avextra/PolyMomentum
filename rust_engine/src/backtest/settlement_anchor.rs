//! Outcome-blind source coverage and paired scoring for settlement anchoring.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::backtest::allocation_lock::{
    revalidate_settlement_anchor_allocation_lock, AllocationCheck, ForwardConditionSet,
    HashedArtifact, SettlementAnchorAllocationEvidence, SettlementAnchorAllocationLock,
    SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK, SETTLEMENT_ANCHOR_MECHANISM_ID,
    SETTLEMENT_ANCHOR_VARIANT_PARAMS_HASH,
};
use crate::backtest::btc_history::BTCHistory;
use crate::backtest::experiment::{
    ExperimentReport, VariantReport, CURRENT_REPLAY_SEMANTICS_VERSION,
};
use crate::data::manifest::DataSourceManifest;
use crate::strategy::spec::stable_json_hash;

pub const OFFICIAL_OPEN_MAX_AGE_MS: i64 = 2_000;
pub const OFFICIAL_CURRENT_MAX_AGE_MS: i64 = 10_000;
pub const OFFICIAL_PRIMARY_START_MS: i64 = 120_000;
pub const OFFICIAL_PRIMARY_END_MS: i64 = 180_000;
pub const PUBLISHED_PRICE_MAX_DIFFERENCE_USD: f64 = 0.01;
pub const MIN_OFFICIAL_ANCHOR_COVERAGE: f64 = 0.95;

#[derive(Debug, Clone)]
pub struct SettlementAnchorSourceAuditInput {
    pub condition_set_path: String,
    pub fair_value_btc_csv_path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementAnchorSourceCoverageRow {
    pub condition_id: String,
    pub report_id: String,
    pub window_start: String,
    pub published_price_to_beat: Option<f64>,
    pub official_open_price: Option<f64>,
    pub official_open_age_ms: Option<i64>,
    pub published_price_difference_usd: Option<f64>,
    pub primary_seconds_checked: usize,
    pub maximum_primary_current_age_ms: Option<i64>,
    pub open_fresh: bool,
    pub primary_current_fresh: bool,
    pub published_price_matches: bool,
    pub source_covered: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementAnchorSourceCoverageCounts {
    pub conditions: usize,
    pub reports: usize,
    pub open_fresh_conditions: usize,
    pub primary_current_fresh_conditions: usize,
    pub source_covered_conditions: usize,
    pub published_price_comparisons: usize,
    pub published_price_mismatches: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementAnchorSourceAudit {
    pub schema_version: u32,
    pub generated_at: String,
    pub mechanism_id: String,
    pub status: String,
    pub ok: bool,
    pub condition_set: HashedArtifact,
    pub fair_value_btc_csv: HashedArtifact,
    pub fair_value_source_kind: String,
    pub current_max_age_ms: i64,
    pub open_max_age_ms: i64,
    pub primary_window_start_ms: i64,
    pub primary_window_end_ms: i64,
    pub published_price_max_difference_usd: f64,
    pub minimum_official_anchor_coverage: f64,
    pub counts: SettlementAnchorSourceCoverageCounts,
    pub official_anchor_coverage: f64,
    pub maximum_published_price_difference_usd: Option<f64>,
    pub rows: Vec<SettlementAnchorSourceCoverageRow>,
    pub checks: Vec<AllocationCheck>,
    pub failure_reasons: Vec<String>,
    pub blindness: BTreeMap<String, bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementAnchorSourceEvidence {
    pub path: String,
    pub sha256: String,
    pub condition_set_sha256: String,
    pub fair_value_btc_csv_sha256: String,
    pub condition_count: usize,
    pub report_count: usize,
    pub source_covered_conditions: usize,
    pub official_anchor_coverage: f64,
    pub maximum_published_price_difference_usd: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct SettlementAnchorPairAuditInput {
    pub allocation_lock_path: String,
    pub source_audit_path: String,
    pub fair_value_btc_csv_path: String,
    pub baseline_report_path: String,
    pub baseline_trades_path: String,
    pub official_report_path: String,
    pub official_trades_path: String,
    pub output_path: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementAnchorBlockMetrics {
    pub trades: usize,
    pub wins: usize,
    pub losses: usize,
    pub win_rate: f64,
    pub wilson_95_lower: f64,
    pub fee_inclusive_pnl_usd: f64,
    pub gross_win_pnl_usd: f64,
    pub gross_loss_pnl_usd: f64,
    pub profit_factor: f64,
    pub average_win_pnl_usd: f64,
    pub average_loss_pnl_usd: f64,
    pub payoff_ratio: f64,
    pub worst_loss_over_average_win: f64,
    pub maximum_drawdown_usd: f64,
    pub reports: usize,
    pub eligible_reports: usize,
    pub profitable_reports: usize,
    pub worst_fold_pnl_usd: f64,
    pub left_tail_cvar_20_usd: f64,
    pub maximum_losing_reports_in_any_five_report_window: usize,
    pub first_half_fee_inclusive_pnl_usd: f64,
    pub second_half_fee_inclusive_pnl_usd: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementAnchorDecisionAttribution {
    pub baseline_conditions: usize,
    pub official_conditions: usize,
    pub baseline_only_conditions: usize,
    pub official_only_conditions: usize,
    pub both_conditions: usize,
    pub neither_conditions: usize,
    pub direction_changes: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementAnchorPairAudit {
    pub schema_version: u32,
    pub generated_at: String,
    pub mechanism_id: String,
    pub status: String,
    pub ok: bool,
    pub allocation_lock: HashedArtifact,
    pub source_audit: HashedArtifact,
    pub baseline_report: HashedArtifact,
    pub baseline_trades: HashedArtifact,
    pub official_report: HashedArtifact,
    pub official_trades: HashedArtifact,
    pub score_output_path: String,
    pub block_id: String,
    pub condition_ids_hash: String,
    pub report_partition_hash: String,
    pub source_coverage: SettlementAnchorSourceEvidence,
    pub baseline_metrics: SettlementAnchorBlockMetrics,
    pub official_metrics: SettlementAnchorBlockMetrics,
    pub attribution: SettlementAnchorDecisionAttribution,
    pub parity_checks: Vec<AllocationCheck>,
    pub absolute_gates: Vec<AllocationCheck>,
    pub failure_reasons: Vec<String>,
    pub decision: BTreeMap<String, bool>,
}

#[derive(Debug, Clone, Deserialize)]
struct HarnessSweepTradeReport {
    schema_version: u32,
    mode: String,
    start: String,
    end: String,
    bankroll_usd: f64,
    max_total_exposure_usd: f64,
    latency_ms: u64,
    window_minutes: Option<f64>,
    continuous: bool,
    #[serde(default)]
    settlement_anchor_allocation: Option<SettlementAnchorAllocationEvidence>,
    #[serde(default)]
    settlement_anchor_source: Option<SettlementAnchorSourceEvidence>,
    variants: Vec<HarnessSweepTradeVariant>,
}

#[derive(Debug, Clone, Deserialize)]
struct HarnessSweepTradeVariant {
    strategy_name: String,
    risk_profile: String,
    strategy_params: serde_json::Value,
    summary: serde_json::Value,
    trades: Vec<PairTradeRow>,
    unresolved_fills: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct PairTradeRow {
    fill: PairTradeFill,
    won: bool,
    pnl_after_fee: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct PairTradeFill {
    order: PairTradeOrder,
    fill_timestamp_s: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct PairTradeOrder {
    condition_id: String,
    token_id: String,
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn sha256_file(path: impl AsRef<Path>) -> Result<String> {
    use std::io::Read;

    let path = path.as_ref();
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("open pinned input {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("hash pinned input {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn read_hashed(path: &str, label: &str) -> Result<(Vec<u8>, HashedArtifact)> {
    let bytes = std::fs::read(path).with_context(|| format!("read {label} {path}"))?;
    let artifact = HashedArtifact {
        path: path.to_string(),
        sha256: sha256_bytes(&bytes),
    };
    Ok((bytes, artifact))
}

fn check(
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

pub fn settlement_anchor_source_audit(
    input: SettlementAnchorSourceAuditInput,
) -> Result<SettlementAnchorSourceAudit> {
    let (condition_set_bytes, condition_set_artifact) =
        read_hashed(&input.condition_set_path, "settlement-anchor condition set")?;
    let condition_set: ForwardConditionSet = serde_json::from_slice(&condition_set_bytes)
        .context("parse settlement-anchor condition set")?;
    let (_, fair_value_btc_csv) = read_hashed(
        &input.fair_value_btc_csv_path,
        "official fair-value BTC CSV",
    )?;
    let mut fair_value_btc = BTCHistory::new();
    fair_value_btc.load_csv(&input.fair_value_btc_csv_path)?;

    let mut checks = Vec::new();
    check(
        &mut checks,
        "condition_set_contract",
        condition_set.schema_version == 1
            && condition_set.mechanism_id == SETTLEMENT_ANCHOR_MECHANISM_ID
            && condition_set.conditions.len() == SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK,
        format!(
            "schema={} mechanism={} conditions={}",
            condition_set.schema_version,
            condition_set.mechanism_id,
            condition_set.conditions.len()
        ),
    );
    let unique_condition_ids: BTreeSet<_> = condition_set
        .conditions
        .iter()
        .map(|condition| condition.condition_id.as_str())
        .collect();
    check(
        &mut checks,
        "condition_ids_unique",
        unique_condition_ids.len() == condition_set.conditions.len(),
        format!(
            "unique={} total={}",
            unique_condition_ids.len(),
            condition_set.conditions.len()
        ),
    );
    let report_ids: BTreeSet<_> = condition_set
        .conditions
        .iter()
        .map(|condition| condition.report_id.as_str())
        .collect();
    check(
        &mut checks,
        "report_partition_present",
        report_ids.len() >= 20
            && condition_set
                .conditions
                .iter()
                .all(|condition| !condition.report_id.trim().is_empty()),
        format!("reports={} required_min=20", report_ids.len()),
    );
    check(
        &mut checks,
        "official_source_kind",
        fair_value_btc.source_kind() == "chainlink_btc_usd_data_stream",
        fair_value_btc.source_kind().to_string(),
    );

    let mut rows = Vec::with_capacity(condition_set.conditions.len());
    for condition in &condition_set.conditions {
        let start = DateTime::parse_from_rfc3339(&condition.window_start)
            .with_context(|| {
                format!(
                    "parse window_start {} for {}",
                    condition.window_start, condition.condition_id
                )
            })?
            .with_timezone(&Utc)
            .timestamp_millis();
        let open = fair_value_btc.price_and_age_at_with_max_age(start, OFFICIAL_OPEN_MAX_AGE_MS);
        let official_open_price = open.map(|(price, _)| price);
        let official_open_age_ms = open.map(|(_, age_ms)| age_ms);
        let open_fresh = open.is_some();

        let mut primary_seconds_checked = 0usize;
        let mut maximum_primary_current_age_ms: Option<i64> = None;
        let mut primary_current_fresh = true;
        let mut offset_ms = OFFICIAL_PRIMARY_START_MS;
        while offset_ms <= OFFICIAL_PRIMARY_END_MS {
            match fair_value_btc
                .price_and_age_at_with_max_age(start + offset_ms, OFFICIAL_CURRENT_MAX_AGE_MS)
            {
                Some((_, age_ms)) => {
                    primary_seconds_checked += 1;
                    maximum_primary_current_age_ms = Some(
                        maximum_primary_current_age_ms
                            .map_or(age_ms, |observed| observed.max(age_ms)),
                    );
                }
                None => {
                    primary_current_fresh = false;
                    break;
                }
            }
            offset_ms += 1_000;
        }
        primary_current_fresh &= primary_seconds_checked
            == ((OFFICIAL_PRIMARY_END_MS - OFFICIAL_PRIMARY_START_MS) / 1_000 + 1) as usize;

        let published_price_difference_usd = official_open_price
            .zip(condition.published_price_to_beat)
            .map(|(official, published)| (official - published).abs());
        let published_price_matches = published_price_difference_usd
            .is_some_and(|difference| difference <= PUBLISHED_PRICE_MAX_DIFFERENCE_USD);
        let source_covered = open_fresh && primary_current_fresh;
        rows.push(SettlementAnchorSourceCoverageRow {
            condition_id: condition.condition_id.clone(),
            report_id: condition.report_id.clone(),
            window_start: condition.window_start.clone(),
            published_price_to_beat: condition.published_price_to_beat,
            official_open_price,
            official_open_age_ms,
            published_price_difference_usd,
            primary_seconds_checked,
            maximum_primary_current_age_ms,
            open_fresh,
            primary_current_fresh,
            published_price_matches,
            source_covered,
        });
    }

    let counts = SettlementAnchorSourceCoverageCounts {
        conditions: rows.len(),
        reports: report_ids.len(),
        open_fresh_conditions: rows.iter().filter(|row| row.open_fresh).count(),
        primary_current_fresh_conditions: rows
            .iter()
            .filter(|row| row.primary_current_fresh)
            .count(),
        source_covered_conditions: rows.iter().filter(|row| row.source_covered).count(),
        published_price_comparisons: rows
            .iter()
            .filter(|row| row.published_price_difference_usd.is_some())
            .count(),
        published_price_mismatches: rows
            .iter()
            .filter(|row| {
                row.published_price_difference_usd
                    .is_some_and(|difference| difference > PUBLISHED_PRICE_MAX_DIFFERENCE_USD)
            })
            .count(),
    };
    let official_anchor_coverage = if counts.conditions == 0 {
        0.0
    } else {
        counts.source_covered_conditions as f64 / counts.conditions as f64
    };
    let maximum_published_price_difference_usd = rows
        .iter()
        .filter_map(|row| row.published_price_difference_usd)
        .reduce(f64::max);
    check(
        &mut checks,
        "minimum_official_anchor_coverage",
        official_anchor_coverage >= MIN_OFFICIAL_ANCHOR_COVERAGE,
        format!(
            "observed={official_anchor_coverage:.9} required={MIN_OFFICIAL_ANCHOR_COVERAGE:.9} covered={} total={}",
            counts.source_covered_conditions, counts.conditions
        ),
    );
    check(
        &mut checks,
        "published_price_to_beat_reproduction",
        counts.published_price_comparisons == counts.open_fresh_conditions
            && counts.published_price_mismatches == 0,
        format!(
            "comparisons={} open_fresh={} mismatches={} max_difference={maximum_published_price_difference_usd:?}",
            counts.published_price_comparisons,
            counts.open_fresh_conditions,
            counts.published_price_mismatches
        ),
    );

    let failure_reasons: Vec<_> = checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| format!("check_failed:{}", check.name))
        .collect();
    let ok = failure_reasons.is_empty();
    Ok(SettlementAnchorSourceAudit {
        schema_version: 1,
        generated_at: Utc::now().to_rfc3339(),
        mechanism_id: SETTLEMENT_ANCHOR_MECHANISM_ID.to_string(),
        status: if ok {
            "OFFICIAL_ANCHOR_SOURCE_COVERAGE_PASS"
        } else {
            "REJECT_BLOCK_SOURCE_COVERAGE"
        }
        .to_string(),
        ok,
        condition_set: condition_set_artifact,
        fair_value_btc_csv,
        fair_value_source_kind: fair_value_btc.source_kind().to_string(),
        current_max_age_ms: OFFICIAL_CURRENT_MAX_AGE_MS,
        open_max_age_ms: OFFICIAL_OPEN_MAX_AGE_MS,
        primary_window_start_ms: OFFICIAL_PRIMARY_START_MS,
        primary_window_end_ms: OFFICIAL_PRIMARY_END_MS,
        published_price_max_difference_usd: PUBLISHED_PRICE_MAX_DIFFERENCE_USD,
        minimum_official_anchor_coverage: MIN_OFFICIAL_ANCHOR_COVERAGE,
        counts,
        official_anchor_coverage,
        maximum_published_price_difference_usd,
        rows,
        checks,
        failure_reasons,
        blindness: BTreeMap::from([
            ("terminal_labels_loaded".to_string(), false),
            ("strategy_outcomes_loaded".to_string(), false),
            ("strategy_metrics_loaded".to_string(), false),
        ]),
    })
}

pub fn validate_settlement_anchor_source_audit(
    audit_path: impl AsRef<Path>,
    expected_condition_set_sha256: &str,
    fair_value_btc_csv_path: &str,
) -> Result<SettlementAnchorSourceEvidence> {
    let audit_path = audit_path.as_ref();
    let audit_bytes = std::fs::read(audit_path).with_context(|| {
        format!(
            "read settlement-anchor source audit {}",
            audit_path.display()
        )
    })?;
    let audit: SettlementAnchorSourceAudit =
        serde_json::from_slice(&audit_bytes).with_context(|| {
            format!(
                "parse settlement-anchor source audit {}",
                audit_path.display()
            )
        })?;
    if audit.schema_version != 1
        || audit.mechanism_id != SETTLEMENT_ANCHOR_MECHANISM_ID
        || audit.status != "OFFICIAL_ANCHOR_SOURCE_COVERAGE_PASS"
        || !audit.ok
        || audit.checks.iter().any(|check| !check.passed)
        || !audit.failure_reasons.is_empty()
    {
        anyhow::bail!("source audit is not a passing settlement-anchor audit");
    }
    if audit.condition_set.sha256 != expected_condition_set_sha256 {
        anyhow::bail!("source audit condition set does not match the allocation lock");
    }
    if sha256_file(&audit.condition_set.path)? != audit.condition_set.sha256 {
        anyhow::bail!("source audit condition-set hash has drifted");
    }
    if sha256_file(fair_value_btc_csv_path)? != audit.fair_value_btc_csv.sha256 {
        anyhow::bail!("source audit fair-value BTC CSV hash has drifted");
    }
    if audit.fair_value_btc_csv.path != fair_value_btc_csv_path {
        anyhow::bail!("source audit fair-value BTC CSV path does not match the evaluator input");
    }
    let row_ids: BTreeSet<_> = audit
        .rows
        .iter()
        .map(|row| row.condition_id.as_str())
        .collect();
    let row_reports: BTreeSet<_> = audit
        .rows
        .iter()
        .map(|row| row.report_id.as_str())
        .collect();
    if audit.counts.conditions != SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK
        || audit.rows.len() != audit.counts.conditions
        || row_ids.len() != audit.counts.conditions
        || row_reports.len() != audit.counts.reports
        || audit.counts.reports < 20
        || audit.counts.source_covered_conditions > audit.counts.conditions
        || audit.official_anchor_coverage < MIN_OFFICIAL_ANCHOR_COVERAGE
        || (audit.official_anchor_coverage
            - audit.counts.source_covered_conditions as f64 / audit.counts.conditions as f64)
            .abs()
            > 1e-12
        || audit.counts.published_price_mismatches != 0
        || audit.blindness.values().any(|accessed| *accessed)
    {
        anyhow::bail!("source audit counts, coverage, price reproduction, or blindness drifted");
    }
    Ok(SettlementAnchorSourceEvidence {
        path: audit_path.display().to_string(),
        sha256: sha256_bytes(&audit_bytes),
        condition_set_sha256: audit.condition_set.sha256,
        fair_value_btc_csv_sha256: audit.fair_value_btc_csv.sha256,
        condition_count: audit.counts.conditions,
        report_count: audit.counts.reports,
        source_covered_conditions: audit.counts.source_covered_conditions,
        official_anchor_coverage: audit.official_anchor_coverage,
        maximum_published_price_difference_usd: audit.maximum_published_price_difference_usd,
    })
}

fn finite_ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator > 1e-12 {
        numerator / denominator
    } else if numerator > 0.0 {
        999.0
    } else {
        0.0
    }
}

fn wilson_lower(wins: usize, trades: usize) -> f64 {
    if trades == 0 {
        return 0.0;
    }
    let n = trades as f64;
    let probability = wins as f64 / n;
    let z = 1.96_f64;
    let z2 = z * z;
    let denominator = 1.0 + z2 / n;
    let centre = probability + z2 / (2.0 * n);
    let margin = z
        * ((probability * (1.0 - probability) + z2 / (4.0 * n)) / n)
            .max(0.0)
            .sqrt();
    ((centre - margin) / denominator).clamp(0.0, 1.0)
}

fn trade_metrics(
    trades: &[PairTradeRow],
    condition_set: &ForwardConditionSet,
) -> SettlementAnchorBlockMetrics {
    let condition_index: BTreeMap<_, _> = condition_set
        .conditions
        .iter()
        .enumerate()
        .map(|(index, condition)| {
            (
                condition.condition_id.as_str(),
                (index, condition.report_id.as_str()),
            )
        })
        .collect();
    let mut report_order = Vec::new();
    let mut seen_reports = BTreeSet::new();
    for condition in &condition_set.conditions {
        if seen_reports.insert(condition.report_id.as_str()) {
            report_order.push(condition.report_id.as_str());
        }
    }
    let mut report_pnl: BTreeMap<&str, f64> = report_order
        .iter()
        .map(|report_id| (*report_id, 0.0))
        .collect();
    let mut report_trades: BTreeMap<&str, usize> = report_order
        .iter()
        .map(|report_id| (*report_id, 0))
        .collect();
    let mut first_half_pnl = 0.0;
    let mut second_half_pnl = 0.0;
    let mut chronological_pnl = Vec::with_capacity(trades.len());
    for trade in trades {
        if let Some((index, report_id)) =
            condition_index.get(trade.fill.order.condition_id.as_str())
        {
            *report_pnl.entry(report_id).or_default() += trade.pnl_after_fee;
            *report_trades.entry(report_id).or_default() += 1;
            if *index < SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK / 2 {
                first_half_pnl += trade.pnl_after_fee;
            } else {
                second_half_pnl += trade.pnl_after_fee;
            }
        }
        chronological_pnl.push((trade.fill.fill_timestamp_s, trade.pnl_after_fee));
    }
    chronological_pnl.sort_by(|left, right| {
        left.0
            .partial_cmp(&right.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let wins = trades.iter().filter(|trade| trade.won).count();
    let losses = trades.len().saturating_sub(wins);
    let gross_win_pnl: f64 = trades
        .iter()
        .filter(|trade| trade.won)
        .map(|trade| trade.pnl_after_fee)
        .sum();
    let gross_loss_pnl: f64 = trades
        .iter()
        .filter(|trade| !trade.won)
        .map(|trade| trade.pnl_after_fee)
        .sum();
    let total_pnl = gross_win_pnl + gross_loss_pnl;
    let average_win = if wins == 0 {
        0.0
    } else {
        gross_win_pnl / wins as f64
    };
    let average_loss = if losses == 0 {
        0.0
    } else {
        gross_loss_pnl / losses as f64
    };
    let worst_loss = trades
        .iter()
        .filter(|trade| !trade.won)
        .map(|trade| trade.pnl_after_fee)
        .reduce(f64::min)
        .unwrap_or(0.0);

    let ordered_report_pnls: Vec<_> = report_order
        .iter()
        .map(|report_id| report_pnl.get(report_id).copied().unwrap_or(0.0))
        .collect();
    let eligible_report_pnls: Vec<_> = report_order
        .iter()
        .filter(|report_id| report_trades.get(*report_id).copied().unwrap_or(0) > 0)
        .map(|report_id| report_pnl.get(report_id).copied().unwrap_or(0.0))
        .collect();
    let profitable_reports = eligible_report_pnls
        .iter()
        .filter(|pnl| **pnl > 0.0)
        .count();
    let worst_fold_pnl = ordered_report_pnls
        .iter()
        .copied()
        .reduce(f64::min)
        .unwrap_or(0.0);
    let mut cvar_values = eligible_report_pnls.clone();
    cvar_values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let cvar_count = ((cvar_values.len() as f64 * 0.20).ceil() as usize)
        .max(1)
        .min(cvar_values.len().max(1));
    let left_tail_cvar = if cvar_values.is_empty() {
        0.0
    } else {
        cvar_values.iter().take(cvar_count).sum::<f64>() / cvar_count as f64
    };
    let maximum_losing_reports_in_five = ordered_report_pnls
        .windows(5)
        .map(|window| window.iter().filter(|pnl| **pnl < 0.0).count())
        .max()
        .unwrap_or_else(|| ordered_report_pnls.iter().filter(|pnl| **pnl < 0.0).count());

    let mut cumulative: f64 = 0.0;
    let mut peak: f64 = 0.0;
    let mut maximum_drawdown: f64 = 0.0;
    for (_, pnl) in chronological_pnl {
        cumulative += pnl;
        peak = peak.max(cumulative);
        maximum_drawdown = maximum_drawdown.max(peak - cumulative);
    }

    SettlementAnchorBlockMetrics {
        trades: trades.len(),
        wins,
        losses,
        win_rate: if trades.is_empty() {
            0.0
        } else {
            wins as f64 / trades.len() as f64
        },
        wilson_95_lower: wilson_lower(wins, trades.len()),
        fee_inclusive_pnl_usd: total_pnl,
        gross_win_pnl_usd: gross_win_pnl,
        gross_loss_pnl_usd: gross_loss_pnl,
        profit_factor: finite_ratio(gross_win_pnl, gross_loss_pnl.abs()),
        average_win_pnl_usd: average_win,
        average_loss_pnl_usd: average_loss,
        payoff_ratio: finite_ratio(average_win, average_loss.abs()),
        worst_loss_over_average_win: finite_ratio(worst_loss.abs(), average_win),
        maximum_drawdown_usd: maximum_drawdown,
        reports: report_order.len(),
        eligible_reports: eligible_report_pnls.len(),
        profitable_reports,
        worst_fold_pnl_usd: worst_fold_pnl,
        left_tail_cvar_20_usd: left_tail_cvar,
        maximum_losing_reports_in_any_five_report_window: maximum_losing_reports_in_five,
        first_half_fee_inclusive_pnl_usd: first_half_pnl,
        second_half_fee_inclusive_pnl_usd: second_half_pnl,
    }
}

fn manifest_source<'a>(report: &'a ExperimentReport, name: &str) -> Option<&'a DataSourceManifest> {
    report
        .data_manifest
        .sources
        .iter()
        .find(|source| source.name == name)
}

fn pinned_file_source_valid(source: &DataSourceManifest) -> bool {
    let Some(path) = source.path.as_deref() else {
        return false;
    };
    let Some(expected_hash) = source.checksum_sha256.as_deref() else {
        return false;
    };
    sha256_file(path)
        .map(|actual_hash| actual_hash == expected_hash)
        .unwrap_or(false)
}

fn pinned_pmxt_source_valid(source: &DataSourceManifest) -> bool {
    let Some(encoded) = source.metadata.get("input_artifacts_json") else {
        return false;
    };
    let Ok(artifacts) = serde_json::from_str::<Vec<HashedArtifact>>(encoded) else {
        return false;
    };
    let unique_paths: BTreeSet<_> = artifacts.iter().map(|artifact| &artifact.path).collect();
    let aggregate_hash = stable_json_hash(&artifacts);
    !artifacts.is_empty()
        && unique_paths.len() == artifacts.len()
        && source.complete
        && source.checksum_sha256.as_deref() == Some(aggregate_hash.as_str())
        && source
            .metadata
            .get("input_artifacts_hash")
            .map(String::as_str)
            == Some(aggregate_hash.as_str())
        && source
            .metadata
            .get("input_artifact_count")
            .and_then(|count| count.parse::<usize>().ok())
            == Some(artifacts.len())
        && artifacts.iter().all(|artifact| {
            sha256_file(&artifact.path)
                .map(|actual_hash| actual_hash == artifact.sha256)
                .unwrap_or(false)
        })
}

fn json_number(value: &serde_json::Value, field: &str) -> Option<f64> {
    value.get(field).and_then(serde_json::Value::as_f64)
}

fn approximately_equal(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-9 * left.abs().max(right.abs()).max(1.0)
}

fn trade_summary_matches(
    variant: &HarnessSweepTradeVariant,
    metrics: &SettlementAnchorBlockMetrics,
) -> bool {
    variant.summary["trades"].as_u64() == Some(metrics.trades as u64)
        && variant.summary["wins"].as_u64() == Some(metrics.wins as u64)
        && variant.summary["losses"].as_u64() == Some(metrics.losses as u64)
        && json_number(&variant.summary, "total_pnl")
            .is_some_and(|value| approximately_equal(value, metrics.fee_inclusive_pnl_usd))
        && variant.summary["unresolved_fills"].as_u64()
            == Some(variant.unresolved_fills.len() as u64)
}

fn experiment_summary_matches(
    variant: &VariantReport,
    metrics: &SettlementAnchorBlockMetrics,
) -> bool {
    variant.trades == metrics.trades
        && variant.wins == metrics.wins
        && variant.losses == metrics.losses
        && variant.unresolved_fills == 0
        && approximately_equal(variant.total_pnl, metrics.fee_inclusive_pnl_usd)
        && approximately_equal(
            variant.avg_pnl,
            if metrics.trades == 0 {
                0.0
            } else {
                metrics.fee_inclusive_pnl_usd / metrics.trades as f64
            },
        )
}

pub fn settlement_anchor_pair_audit(
    input: SettlementAnchorPairAuditInput,
) -> Result<SettlementAnchorPairAudit> {
    let (allocation_bytes, allocation_artifact) = read_hashed(
        &input.allocation_lock_path,
        "settlement-anchor allocation lock",
    )?;
    let allocation_lock: SettlementAnchorAllocationLock =
        revalidate_settlement_anchor_allocation_lock(&input.allocation_lock_path)?;
    if sha256_bytes(&allocation_bytes) != allocation_artifact.sha256 {
        anyhow::bail!("allocation-lock byte hash changed while reading");
    }
    let (_, source_audit_artifact) =
        read_hashed(&input.source_audit_path, "settlement-anchor source audit")?;
    let source_coverage = validate_settlement_anchor_source_audit(
        &input.source_audit_path,
        &allocation_lock.candidate_condition_set.sha256,
        &input.fair_value_btc_csv_path,
    )?;
    let (condition_set_bytes, _) = read_hashed(
        &allocation_lock.candidate_condition_set.path,
        "locked candidate condition set",
    )?;
    let condition_set: ForwardConditionSet = serde_json::from_slice(&condition_set_bytes)
        .context("parse locked candidate condition set")?;
    let frozen_variant_bytes =
        std::fs::read(&allocation_lock.frozen_variant.path).with_context(|| {
            format!(
                "read frozen settlement-anchor variant {}",
                allocation_lock.frozen_variant.path
            )
        })?;
    let frozen_variant_json: serde_json::Value = serde_json::from_slice(&frozen_variant_bytes)
        .context("parse frozen settlement-anchor variant JSON")?;
    let frozen_variant_params = match frozen_variant_json {
        serde_json::Value::Array(values) if values.len() == 1 => values[0].clone(),
        value @ serde_json::Value::Object(_) => value,
        _ => anyhow::bail!("frozen settlement-anchor variant must be one JSON object"),
    };

    let (baseline_report_bytes, baseline_report_artifact) =
        read_hashed(&input.baseline_report_path, "baseline experiment report")?;
    let baseline_report: ExperimentReport = serde_json::from_slice(&baseline_report_bytes)
        .context("parse baseline experiment report")?;
    let (official_report_bytes, official_report_artifact) = read_hashed(
        &input.official_report_path,
        "official-anchor experiment report",
    )?;
    let official_report: ExperimentReport = serde_json::from_slice(&official_report_bytes)
        .context("parse official-anchor experiment report")?;
    let (baseline_trades_bytes, baseline_trades_artifact) =
        read_hashed(&input.baseline_trades_path, "baseline trade report")?;
    let baseline_trades: HarnessSweepTradeReport =
        serde_json::from_slice(&baseline_trades_bytes).context("parse baseline trade report")?;
    let (official_trades_bytes, official_trades_artifact) =
        read_hashed(&input.official_trades_path, "official-anchor trade report")?;
    let official_trades: HarnessSweepTradeReport = serde_json::from_slice(&official_trades_bytes)
        .context("parse official-anchor trade report")?;

    let mut parity_checks = Vec::new();
    check(
        &mut parity_checks,
        "official_outputs_match_allocation_lock",
        input.official_report_path == allocation_lock.score_outputs.report_json
            && input.official_trades_path == allocation_lock.score_outputs.trades_json
            && input.output_path == allocation_lock.score_outputs.pair_audit_json
            && !Path::new(&input.output_path).exists(),
        format!(
            "report={} locked_report={} trades={} locked_trades={} pair_audit={} locked_pair_audit={} unused={}",
            input.official_report_path,
            allocation_lock.score_outputs.report_json,
            input.official_trades_path,
            allocation_lock.score_outputs.trades_json,
            input.output_path,
            allocation_lock.score_outputs.pair_audit_json,
            !Path::new(&input.output_path).exists()
        ),
    );
    check(
        &mut parity_checks,
        "baseline_outputs_distinct",
        input.baseline_report_path != input.official_report_path
            && input.baseline_trades_path != input.official_trades_path
            && input.baseline_report_path != input.baseline_trades_path,
        format!(
            "baseline_report={} baseline_trades={}",
            input.baseline_report_path, input.baseline_trades_path
        ),
    );
    let expected_ids: BTreeSet<_> = allocation_lock
        .allowed_condition_ids
        .iter()
        .cloned()
        .collect();
    let baseline_catalog_ids: BTreeSet<_> = baseline_report
        .market_catalog
        .markets
        .keys()
        .cloned()
        .collect();
    let official_catalog_ids: BTreeSet<_> = official_report
        .market_catalog
        .markets
        .keys()
        .cloned()
        .collect();
    check(
        &mut parity_checks,
        "exact_locked_condition_universe",
        baseline_catalog_ids == expected_ids && official_catalog_ids == expected_ids,
        format!(
            "locked={} baseline={} official={}",
            expected_ids.len(),
            baseline_catalog_ids.len(),
            official_catalog_ids.len()
        ),
    );
    let report_envelopes_match = baseline_report.schema_version == 1
        && official_report.schema_version == 1
        && baseline_report.mode == "backtest"
        && official_report.mode == "backtest"
        && baseline_report.start == official_report.start
        && baseline_report.end == official_report.end
        && approximately_equal(baseline_report.bankroll_usd, official_report.bankroll_usd)
        && baseline_report.latency_ms == official_report.latency_ms
        && baseline_report.market_catalog == official_report.market_catalog
        && baseline_report.variants.len() == 1
        && official_report.variants.len() == 1;
    check(
        &mut parity_checks,
        "experiment_envelopes_match",
        report_envelopes_match,
        format!(
            "start={}=={} end={}=={} latency={}=={} variants={}=={}",
            baseline_report.start,
            official_report.start,
            baseline_report.end,
            official_report.end,
            baseline_report.latency_ms,
            official_report.latency_ms,
            baseline_report.variants.len(),
            official_report.variants.len()
        ),
    );
    let report_params_match = baseline_report
        .variants
        .first()
        .zip(official_report.variants.first())
        .is_some_and(|(baseline, official)| {
            baseline.strategy == official.strategy
                && baseline.strategy_params == official.strategy_params
                && baseline.strategy_params == frozen_variant_params
                && baseline.strategy.params_hash == SETTLEMENT_ANCHOR_VARIANT_PARAMS_HASH
        });
    check(
        &mut parity_checks,
        "frozen_strategy_params_match",
        report_params_match,
        format!("required_hash={SETTLEMENT_ANCHOR_VARIANT_PARAMS_HASH}"),
    );
    let common_source_names = [
        "pmxt_v2_archive",
        "btc_price_tape",
        "btc_settlement_price_tape",
    ];
    let common_sources_match = common_source_names.iter().all(|name| {
        manifest_source(&baseline_report, name) == manifest_source(&official_report, name)
            && manifest_source(&baseline_report, name).is_some()
    });
    check(
        &mut parity_checks,
        "non_fair_data_sources_match",
        common_sources_match,
        common_source_names.join(","),
    );
    let non_fair_file_sources_pinned =
        ["btc_price_tape", "btc_settlement_price_tape"]
            .iter()
            .all(|name| {
                manifest_source(&baseline_report, name).is_some_and(|source| {
                    source.complete
                        && source.path.as_ref().is_some_and(|path| !path.is_empty())
                        && source
                            .checksum_sha256
                            .as_ref()
                            .is_some_and(|hash| hash.len() == 64)
                })
            });
    check(
        &mut parity_checks,
        "non_fair_file_sources_hash_pinned",
        non_fair_file_sources_pinned,
        "btc_price_tape,btc_settlement_price_tape require path+sha256",
    );
    let pinned_inputs_revalidate = manifest_source(&baseline_report, "pmxt_v2_archive")
        .is_some_and(pinned_pmxt_source_valid)
        && ["btc_price_tape", "btc_settlement_price_tape"]
            .iter()
            .all(|name| {
                manifest_source(&baseline_report, name).is_some_and(pinned_file_source_valid)
            });
    check(
        &mut parity_checks,
        "pinned_replay_inputs_revalidate",
        pinned_inputs_revalidate,
        "PMXT artifact list and non-fair CSV byte hashes must still match",
    );
    let baseline_fair = manifest_source(&baseline_report, "btc_fair_value_price_tape");
    let official_fair = manifest_source(&official_report, "btc_fair_value_price_tape");
    let fair_source_contract = baseline_fair.is_none()
        && official_fair.is_some_and(|source| {
            source.complete
                && source.path.as_deref() == Some(input.fair_value_btc_csv_path.as_str())
                && source.checksum_sha256.as_deref()
                    == Some(source_coverage.fair_value_btc_csv_sha256.as_str())
                && source.metadata.get("source_kind").map(String::as_str)
                    == Some("chainlink_btc_usd_data_stream")
                && source.metadata.get("role").map(String::as_str)
                    == Some("fair_value_spot_and_strike_only")
                && source
                    .metadata
                    .get("current_max_age_ms")
                    .map(String::as_str)
                    == Some("10000")
                && source.metadata.get("open_max_age_ms").map(String::as_str) == Some("2000")
        });
    check(
        &mut parity_checks,
        "only_official_report_has_fair_value_source",
        fair_source_contract,
        format!(
            "baseline_present={} official_present={}",
            baseline_fair.is_some(),
            official_fair.is_some()
        ),
    );
    let baseline_source_names: BTreeSet<_> = baseline_report
        .data_manifest
        .sources
        .iter()
        .map(|source| source.name.as_str())
        .collect();
    let official_source_names: BTreeSet<_> = official_report
        .data_manifest
        .sources
        .iter()
        .map(|source| source.name.as_str())
        .collect();
    let expected_baseline_source_names: BTreeSet<_> = common_source_names.into_iter().collect();
    let expected_official_source_names: BTreeSet<_> = common_source_names
        .into_iter()
        .chain([
            "btc_fair_value_price_tape",
            "settlement_anchor_allocation_lock",
            "settlement_anchor_source_audit",
        ])
        .collect();
    check(
        &mut parity_checks,
        "exact_manifest_source_sets",
        baseline_source_names == expected_baseline_source_names
            && official_source_names == expected_official_source_names,
        format!("baseline={baseline_source_names:?} official={official_source_names:?}"),
    );
    let official_allocation_source =
        manifest_source(&official_report, "settlement_anchor_allocation_lock");
    let official_source_audit = manifest_source(&official_report, "settlement_anchor_source_audit");
    let embedded_evidence_valid = official_allocation_source.is_some_and(|source| {
        source.checksum_sha256.as_deref() == Some(allocation_artifact.sha256.as_str())
            && source
                .metadata
                .get("condition_ids_hash")
                .map(String::as_str)
                == Some(allocation_lock.allowed_condition_ids_hash.as_str())
            && source
                .metadata
                .get("report_partition_hash")
                .map(String::as_str)
                == Some(
                    allocation_lock
                        .candidate_condition_set
                        .report_partition_hash
                        .as_str(),
                )
            && source.metadata.get("pair_audit_output").map(String::as_str)
                == Some(allocation_lock.score_outputs.pair_audit_json.as_str())
    }) && official_source_audit.is_some_and(|source| {
        source.checksum_sha256.as_deref() == Some(source_audit_artifact.sha256.as_str())
            && source
                .metadata
                .get("fair_value_btc_csv_sha256")
                .map(String::as_str)
                == Some(source_coverage.fair_value_btc_csv_sha256.as_str())
    });
    check(
        &mut parity_checks,
        "official_report_embeds_locked_evidence",
        embedded_evidence_valid,
        format!(
            "allocation_present={} source_audit_present={}",
            official_allocation_source.is_some(),
            official_source_audit.is_some()
        ),
    );
    let manifest_hashes_valid = baseline_report.data_manifest.compute_hash()
        == baseline_report.data_manifest.manifest_hash
        && official_report.data_manifest.compute_hash()
            == official_report.data_manifest.manifest_hash;
    check(
        &mut parity_checks,
        "data_manifest_hashes_valid",
        manifest_hashes_valid,
        format!(
            "baseline={} official={}",
            baseline_report.data_manifest.manifest_hash,
            official_report.data_manifest.manifest_hash
        ),
    );
    let replay_semantics_valid = common_source_names.first().is_some_and(|name| {
        manifest_source(&official_report, name).is_some_and(|source| {
            source
                .metadata
                .get("replay_semantics_version")
                .and_then(|value| value.parse::<u32>().ok())
                == Some(CURRENT_REPLAY_SEMANTICS_VERSION)
                && source.metadata.get("taker_fill_model").map(String::as_str)
                    == Some("max_share_budget_optimized_visible_l2_bookwalk_with_fok_limit")
                && source.metadata.get("decision_edge").map(String::as_str)
                    == Some("fair_minus_executable_vwap_minus_effective_entry_fee")
        })
    });
    check(
        &mut parity_checks,
        "exact_replay_semantics",
        replay_semantics_valid,
        format!("required_version={CURRENT_REPLAY_SEMANTICS_VERSION}"),
    );
    let trade_envelopes_match = baseline_trades.schema_version == 1
        && official_trades.schema_version == 1
        && baseline_trades.mode == "harness_sweep_trades"
        && official_trades.mode == "harness_sweep_trades"
        && baseline_trades.start == official_trades.start
        && baseline_trades.end == official_trades.end
        && approximately_equal(baseline_trades.bankroll_usd, official_trades.bankroll_usd)
        && approximately_equal(
            baseline_trades.max_total_exposure_usd,
            official_trades.max_total_exposure_usd,
        )
        && baseline_trades.latency_ms == official_trades.latency_ms
        && baseline_trades.window_minutes == official_trades.window_minutes
        && baseline_trades.continuous
        && official_trades.continuous
        && baseline_trades.variants.len() == 1
        && official_trades.variants.len() == 1;
    check(
        &mut parity_checks,
        "trade_report_envelopes_match",
        trade_envelopes_match,
        format!(
            "latency={}=={} variants={}=={}",
            baseline_trades.latency_ms,
            official_trades.latency_ms,
            baseline_trades.variants.len(),
            official_trades.variants.len()
        ),
    );
    let baseline_variant = baseline_trades.variants.first();
    let official_variant = official_trades.variants.first();
    let trade_params_match =
        baseline_variant
            .zip(official_variant)
            .is_some_and(|(baseline, official)| {
                baseline.strategy_name == official.strategy_name
                    && baseline.risk_profile == official.risk_profile
                    && baseline.strategy_params == official.strategy_params
                    && baseline.strategy_params == frozen_variant_params
            });
    check(
        &mut parity_checks,
        "trade_report_strategy_params_match",
        trade_params_match,
        format!("required_hash={SETTLEMENT_ANCHOR_VARIANT_PARAMS_HASH}"),
    );
    let report_trade_strategy_identity = baseline_report
        .variants
        .first()
        .zip(baseline_variant)
        .zip(official_report.variants.first().zip(official_variant))
        .is_some_and(
            |(
                (baseline_report_variant, baseline_trade_variant),
                (official_report_variant, official_trade_variant),
            )| {
                baseline_report_variant.strategy.name == baseline_trade_variant.strategy_name
                    && baseline_report_variant.strategy.risk_profile
                        == baseline_trade_variant.risk_profile
                    && official_report_variant.strategy.name == official_trade_variant.strategy_name
                    && official_report_variant.strategy.risk_profile
                        == official_trade_variant.risk_profile
            },
        );
    check(
        &mut parity_checks,
        "report_and_trade_strategy_identity_match",
        report_trade_strategy_identity,
        "strategy name and risk profile must match across report/trade artifacts",
    );
    let official_trade_evidence_valid = official_trades
        .settlement_anchor_allocation
        .as_ref()
        .zip(official_trades.settlement_anchor_source.as_ref())
        .is_some_and(|(allocation, source)| {
            allocation.sha256 == allocation_artifact.sha256
                && allocation.condition_ids_hash == allocation_lock.allowed_condition_ids_hash
                && allocation.score_outputs == allocation_lock.score_outputs
                && source.sha256 == source_audit_artifact.sha256
                && source.condition_set_sha256 == allocation_lock.candidate_condition_set.sha256
        });
    check(
        &mut parity_checks,
        "official_trade_report_embeds_locked_evidence",
        official_trade_evidence_valid,
        format!(
            "allocation_present={} source_present={}",
            official_trades.settlement_anchor_allocation.is_some(),
            official_trades.settlement_anchor_source.is_some()
        ),
    );
    check(
        &mut parity_checks,
        "baseline_has_no_official_anchor_evidence",
        baseline_trades.settlement_anchor_allocation.is_none()
            && baseline_trades.settlement_anchor_source.is_none(),
        format!(
            "allocation_present={} source_present={}",
            baseline_trades.settlement_anchor_allocation.is_some(),
            baseline_trades.settlement_anchor_source.is_some()
        ),
    );

    let baseline_rows = baseline_variant
        .map(|variant| variant.trades.as_slice())
        .unwrap_or_default();
    let official_rows = official_variant
        .map(|variant| variant.trades.as_slice())
        .unwrap_or_default();
    let baseline_metrics = trade_metrics(baseline_rows, &condition_set);
    let official_metrics = trade_metrics(official_rows, &condition_set);
    let baseline_conditions: BTreeSet<_> = baseline_rows
        .iter()
        .map(|trade| trade.fill.order.condition_id.clone())
        .collect();
    let official_conditions: BTreeSet<_> = official_rows
        .iter()
        .map(|trade| trade.fill.order.condition_id.clone())
        .collect();
    let trade_conditions_valid = baseline_conditions.is_subset(&expected_ids)
        && official_conditions.is_subset(&expected_ids)
        && baseline_conditions.len() == baseline_rows.len()
        && official_conditions.len() == official_rows.len();
    check(
        &mut parity_checks,
        "trade_condition_ids_locked_and_unique",
        trade_conditions_valid,
        format!(
            "baseline_unique={} baseline_trades={} official_unique={} official_trades={}",
            baseline_conditions.len(),
            baseline_rows.len(),
            official_conditions.len(),
            official_rows.len()
        ),
    );
    let pnl_signs_valid = baseline_rows
        .iter()
        .chain(official_rows)
        .all(|trade| trade.won == (trade.pnl_after_fee > 0.0));
    check(
        &mut parity_checks,
        "resolved_win_and_pnl_sign_consistent",
        pnl_signs_valid,
        "won must equal positive fee-inclusive pnl",
    );
    let summaries_valid = baseline_variant.is_some_and(|variant| {
        trade_summary_matches(variant, &baseline_metrics) && variant.unresolved_fills.is_empty()
    }) && official_variant.is_some_and(|variant| {
        trade_summary_matches(variant, &official_metrics) && variant.unresolved_fills.is_empty()
    });
    check(
        &mut parity_checks,
        "trade_summaries_recompute_and_no_unresolved_fills",
        summaries_valid,
        format!(
            "baseline_unresolved={} official_unresolved={}",
            baseline_variant.map_or(0, |variant| variant.unresolved_fills.len()),
            official_variant.map_or(0, |variant| variant.unresolved_fills.len())
        ),
    );
    let experiment_summaries_valid = baseline_report
        .variants
        .first()
        .is_some_and(|variant| experiment_summary_matches(variant, &baseline_metrics))
        && official_report
            .variants
            .first()
            .is_some_and(|variant| experiment_summary_matches(variant, &official_metrics));
    check(
        &mut parity_checks,
        "experiment_summaries_recompute_from_trade_rows",
        experiment_summaries_valid,
        format!(
            "baseline_trades={} official_trades={}",
            baseline_metrics.trades, official_metrics.trades
        ),
    );

    let condition_open_ms: BTreeMap<_, _> = condition_set
        .conditions
        .iter()
        .filter_map(|condition| {
            DateTime::parse_from_rfc3339(&condition.window_start)
                .ok()
                .map(|window| (condition.condition_id.as_str(), window.timestamp_millis()))
        })
        .collect();
    let trade_rows_match_catalog_and_primary_window =
        |rows: &[PairTradeRow], report: &ExperimentReport| {
            rows.iter().all(|trade| {
                report
                    .market_catalog
                    .token_to_condition
                    .get(&trade.fill.order.token_id)
                    == Some(&trade.fill.order.condition_id)
                    && trade.fill.fill_timestamp_s.is_finite()
                    && condition_open_ms
                        .get(trade.fill.order.condition_id.as_str())
                        .is_some_and(|open_ms| {
                            let fill_ms = trade.fill.fill_timestamp_s * 1_000.0;
                            fill_ms >= (*open_ms + OFFICIAL_PRIMARY_START_MS) as f64
                                && fill_ms <= (*open_ms + OFFICIAL_PRIMARY_END_MS) as f64
                        })
            })
        };
    let catalog_and_fill_window_valid = baseline_report.market_catalog.is_complete()
        && official_report.market_catalog.is_complete()
        && trade_rows_match_catalog_and_primary_window(baseline_rows, &baseline_report)
        && trade_rows_match_catalog_and_primary_window(official_rows, &official_report);
    check(
        &mut parity_checks,
        "trade_tokens_and_fill_times_match_locked_primary_windows",
        catalog_and_fill_window_valid,
        format!(
            "baseline_complete={} official_complete={} primary_ms={}..={}",
            baseline_report.market_catalog.is_complete(),
            official_report.market_catalog.is_complete(),
            OFFICIAL_PRIMARY_START_MS,
            OFFICIAL_PRIMARY_END_MS
        ),
    );

    let baseline_tokens: BTreeMap<_, _> = baseline_rows
        .iter()
        .map(|trade| {
            (
                trade.fill.order.condition_id.as_str(),
                trade.fill.order.token_id.as_str(),
            )
        })
        .collect();
    let official_tokens: BTreeMap<_, _> = official_rows
        .iter()
        .map(|trade| {
            (
                trade.fill.order.condition_id.as_str(),
                trade.fill.order.token_id.as_str(),
            )
        })
        .collect();
    let both_conditions: BTreeSet<_> = baseline_conditions
        .intersection(&official_conditions)
        .cloned()
        .collect();
    let attribution = SettlementAnchorDecisionAttribution {
        baseline_conditions: baseline_conditions.len(),
        official_conditions: official_conditions.len(),
        baseline_only_conditions: baseline_conditions.difference(&official_conditions).count(),
        official_only_conditions: official_conditions.difference(&baseline_conditions).count(),
        both_conditions: both_conditions.len(),
        neither_conditions: expected_ids
            .len()
            .saturating_sub(baseline_conditions.union(&official_conditions).count()),
        direction_changes: both_conditions
            .iter()
            .filter(|condition_id| {
                baseline_tokens.get(condition_id.as_str())
                    != official_tokens.get(condition_id.as_str())
            })
            .count(),
    };

    let mut absolute_gates = Vec::new();
    let mut gate = |name: &str, passed: bool, detail: String| {
        check(&mut absolute_gates, name, passed, detail);
    };
    gate(
        "official_anchor_coverage",
        source_coverage.official_anchor_coverage >= MIN_OFFICIAL_ANCHOR_COVERAGE,
        format!(
            "observed={:.9} required={MIN_OFFICIAL_ANCHOR_COVERAGE:.9}",
            source_coverage.official_anchor_coverage
        ),
    );
    gate(
        "minimum_candidate_trades",
        official_metrics.trades >= 80,
        format!("observed={} required=80", official_metrics.trades),
    );
    gate(
        "wilson_95_lower_bound",
        official_metrics.wilson_95_lower >= 0.70,
        format!(
            "observed={:.9} required=0.70",
            official_metrics.wilson_95_lower
        ),
    );
    gate(
        "positive_fee_inclusive_pnl",
        official_metrics.fee_inclusive_pnl_usd > 0.0,
        format!(
            "observed={:.9} required>0",
            official_metrics.fee_inclusive_pnl_usd
        ),
    );
    gate(
        "minimum_profit_factor",
        official_metrics.profit_factor >= 1.20,
        format!(
            "observed={:.9} required=1.20",
            official_metrics.profit_factor
        ),
    );
    gate(
        "minimum_payoff_ratio",
        official_metrics.payoff_ratio >= 0.30,
        format!(
            "observed={:.9} required=0.30",
            official_metrics.payoff_ratio
        ),
    );
    gate(
        "minimum_profitable_reports",
        official_metrics.profitable_reports >= 20,
        format!(
            "observed={} required=20",
            official_metrics.profitable_reports
        ),
    );
    gate(
        "minimum_eligible_reports",
        official_metrics.eligible_reports >= 20,
        format!("observed={} required=20", official_metrics.eligible_reports),
    );
    gate(
        "minimum_worst_fold_pnl",
        official_metrics.worst_fold_pnl_usd >= -13.0,
        format!(
            "observed={:.9} required=-13.0",
            official_metrics.worst_fold_pnl_usd
        ),
    );
    gate(
        "minimum_left_tail_cvar_20",
        official_metrics.left_tail_cvar_20_usd >= -8.0,
        format!(
            "observed={:.9} required=-8.0",
            official_metrics.left_tail_cvar_20_usd
        ),
    );
    gate(
        "maximum_losing_reports_in_any_five",
        official_metrics.maximum_losing_reports_in_any_five_report_window <= 2,
        format!(
            "observed={} required_max=2",
            official_metrics.maximum_losing_reports_in_any_five_report_window
        ),
    );
    gate(
        "maximum_worst_loss_over_average_win",
        official_metrics.worst_loss_over_average_win <= 3.5,
        format!(
            "observed={:.9} required_max=3.5",
            official_metrics.worst_loss_over_average_win
        ),
    );
    gate(
        "positive_first_half_pnl",
        official_metrics.first_half_fee_inclusive_pnl_usd > 0.0,
        format!(
            "observed={:.9} required>0",
            official_metrics.first_half_fee_inclusive_pnl_usd
        ),
    );
    gate(
        "positive_second_half_pnl",
        official_metrics.second_half_fee_inclusive_pnl_usd > 0.0,
        format!(
            "observed={:.9} required>0",
            official_metrics.second_half_fee_inclusive_pnl_usd
        ),
    );
    gate(
        "minimum_replay_latency_ms",
        official_report.latency_ms >= 202 && official_trades.latency_ms >= 202,
        format!(
            "report={} trades={} required=202",
            official_report.latency_ms, official_trades.latency_ms
        ),
    );
    gate(
        "exact_non_fair_parity",
        parity_checks.iter().all(|check| check.passed),
        format!(
            "passing={} total={}",
            parity_checks.iter().filter(|check| check.passed).count(),
            parity_checks.len()
        ),
    );

    let failure_reasons: Vec<_> = parity_checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| format!("parity_failed:{}", check.name))
        .chain(
            absolute_gates
                .iter()
                .filter(|gate| !gate.passed)
                .map(|gate| format!("gate_failed:{}", gate.name)),
        )
        .collect();
    let ok = failure_reasons.is_empty();
    Ok(SettlementAnchorPairAudit {
        schema_version: 1,
        generated_at: Utc::now().to_rfc3339(),
        mechanism_id: SETTLEMENT_ANCHOR_MECHANISM_ID.to_string(),
        status: if ok {
            "BLOCK_ABSOLUTE_A_PLUS_GATES_PASS_REPLICATION_REQUIRED"
        } else {
            "REJECT_SETTLEMENT_ANCHOR_FAMILY_NO_NEIGHBOR_SEARCH"
        }
        .to_string(),
        ok,
        allocation_lock: allocation_artifact,
        source_audit: source_audit_artifact,
        baseline_report: baseline_report_artifact,
        baseline_trades: baseline_trades_artifact,
        official_report: official_report_artifact,
        official_trades: official_trades_artifact,
        score_output_path: input.output_path,
        block_id: allocation_lock.candidate_condition_set.block_id,
        condition_ids_hash: allocation_lock.allowed_condition_ids_hash,
        report_partition_hash: allocation_lock
            .candidate_condition_set
            .report_partition_hash,
        source_coverage,
        baseline_metrics,
        official_metrics,
        attribution,
        parity_checks,
        absolute_gates,
        failure_reasons,
        decision: BTreeMap::from([
            ("block_absolute_gates_pass".to_string(), ok),
            ("second_disjoint_block_required".to_string(), ok),
            ("runtime_implementation_authorized".to_string(), false),
            ("paper_or_live_trading_authorized".to_string(), false),
            ("profitability_claim".to_string(), false),
            ("a_plus_claim".to_string(), false),
        ]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::allocation_lock::{
        build_settlement_anchor_allocation_lock, validate_settlement_anchor_allocation_lock,
        ForwardCondition, SettlementAnchorAllocationBoundary, SettlementAnchorAllocationLockInput,
        BINARY_COMPLEMENT_MECHANISM_ID,
    };
    use crate::backtest::experiment::VariantReport;
    use crate::data::catalog::{CatalogMarket, MarketCatalog};
    use crate::data::manifest::{DataManifest, DataSourceManifest};
    use crate::strategy::spec::StrategySpec;
    use std::io::Write;

    fn condition_id(index: usize) -> String {
        format!("0x{index:064x}")
    }

    fn fixture(
        missing_conditions: &BTreeSet<usize>,
        mismatched_condition: Option<usize>,
    ) -> (tempfile::TempDir, SettlementAnchorSourceAuditInput) {
        let temp = tempfile::TempDir::new().unwrap();
        let start = "2026-07-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let conditions: Vec<_> = (0..SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK)
            .map(|index| ForwardCondition {
                condition_id: condition_id(index + 1),
                window_start: (start + chrono::Duration::seconds(index as i64 * 300)).to_rfc3339(),
                report_id: format!("segment-{:03}", index / 24 + 1),
                published_price_to_beat: Some(
                    100_000.0
                        + index as f64
                        + if mismatched_condition == Some(index) {
                            1.0
                        } else {
                            0.0
                        },
                ),
            })
            .collect();
        let source_path = temp.path().join("capture-source.json");
        std::fs::write(&source_path, b"{\"sealed\":true}\n").unwrap();
        let condition_set = ForwardConditionSet {
            schema_version: 1,
            mechanism_id: SETTLEMENT_ANCHOR_MECHANISM_ID.to_string(),
            block_id: "anchor-block-1".to_string(),
            block_sequence: 1,
            sealed_at: (start + chrono::Duration::days(5)).to_rfc3339(),
            allocation_boundary: Some(SettlementAnchorAllocationBoundary::AfterBinaryBlock1Failed),
            conditions,
            source_artifacts: vec![HashedArtifact {
                path: source_path.display().to_string(),
                sha256: sha256_bytes(&std::fs::read(&source_path).unwrap()),
            }],
        };
        let condition_set_path = temp.path().join("condition-set.json");
        std::fs::write(
            &condition_set_path,
            serde_json::to_vec_pretty(&condition_set).unwrap(),
        )
        .unwrap();

        let csv_path = temp.path().join("chainlink.csv");
        let mut csv = std::fs::File::create(&csv_path).unwrap();
        writeln!(csv, "timestamp_ms,source,price").unwrap();
        for index in 0..SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK {
            if missing_conditions.contains(&index) {
                continue;
            }
            let open_ms = start.timestamp_millis() + index as i64 * 300_000;
            let price = 100_000.0 + index as f64;
            writeln!(csv, "{open_ms},chainlink_btc_usd_data_stream,{price}").unwrap();
            for offset in (OFFICIAL_PRIMARY_START_MS..=OFFICIAL_PRIMARY_END_MS).step_by(10_000) {
                writeln!(
                    csv,
                    "{},chainlink_btc_usd_data_stream,{price}",
                    open_ms + offset
                )
                .unwrap();
            }
        }
        (
            temp,
            SettlementAnchorSourceAuditInput {
                condition_set_path: condition_set_path.display().to_string(),
                fair_value_btc_csv_path: csv_path.display().to_string(),
            },
        )
    }

    fn registry_path(name: &str) -> String {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../deploy/promotions/evidence/strategy_registry")
            .join(name)
            .display()
            .to_string()
    }

    fn complete_source(name: &str, kind: &str, path: &Path) -> DataSourceManifest {
        let mut source = DataSourceManifest::new(name, kind);
        source.path = Some(path.display().to_string());
        source.row_count = Some(10_000);
        source.checksum_sha256 = Some(sha256_bytes(&std::fs::read(path).unwrap()));
        source.complete = true;
        source
    }

    fn experiment_variant(
        strategy_params: &serde_json::Value,
        trades: usize,
        wins: usize,
        losses: usize,
        total_pnl: f64,
    ) -> VariantReport {
        VariantReport {
            strategy: StrategySpec::new(
                "candle_momentum",
                "1",
                SETTLEMENT_ANCHOR_VARIANT_PARAMS_HASH,
                "fixture-risk",
            ),
            strategy_params: strategy_params.clone(),
            trades,
            wins,
            losses,
            unresolved_fills: 0,
            execution_attempts: trades,
            fills_success: trades,
            fills_failed: 0,
            fill_rate: 1.0,
            reject_reasons: BTreeMap::new(),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: Default::default(),
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
            total_fees: 0.0,
            sharpe_like: 0.0,
            by_zone: BTreeMap::new(),
        }
    }

    struct PairFixture {
        _temp: tempfile::TempDir,
        input: SettlementAnchorPairAuditInput,
    }

    fn pair_fixture() -> PairFixture {
        let (temp, source_input) = fixture(&BTreeSet::new(), None);
        let candidate: ForwardConditionSet =
            serde_json::from_slice(&std::fs::read(&source_input.condition_set_path).unwrap())
                .unwrap();
        let candidate_start =
            DateTime::parse_from_rfc3339(&candidate.conditions.first().unwrap().window_start)
                .unwrap()
                .with_timezone(&Utc);
        let prior_start = candidate_start
            - chrono::Duration::seconds(SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK as i64 * 300);
        let prior_source_path = temp.path().join("prior-source.json");
        std::fs::write(&prior_source_path, b"{\"sealed\":true}\n").unwrap();
        let prior = ForwardConditionSet {
            schema_version: 1,
            mechanism_id: BINARY_COMPLEMENT_MECHANISM_ID.to_string(),
            block_id: "binary-block-1".to_string(),
            block_sequence: 1,
            sealed_at: candidate_start.to_rfc3339(),
            allocation_boundary: None,
            conditions: (0..SETTLEMENT_ANCHOR_CONDITIONS_PER_BLOCK)
                .map(|index| ForwardCondition {
                    condition_id: condition_id(10_001 + index),
                    window_start: (prior_start + chrono::Duration::seconds(index as i64 * 300))
                        .to_rfc3339(),
                    report_id: format!("binary-segment-{:03}", index / 24 + 1),
                    published_price_to_beat: None,
                })
                .collect(),
            source_artifacts: vec![HashedArtifact {
                path: prior_source_path.display().to_string(),
                sha256: sha256_bytes(&std::fs::read(&prior_source_path).unwrap()),
            }],
        };
        let prior_path = temp.path().join("prior-condition-set.json");
        std::fs::write(&prior_path, serde_json::to_vec_pretty(&prior).unwrap()).unwrap();

        let official_report_path = temp.path().join("official-report.json");
        let official_trades_path = temp.path().join("official-trades.json");
        let pair_audit_path = temp.path().join("pair-audit.json");
        let lock = build_settlement_anchor_allocation_lock(SettlementAnchorAllocationLockInput {
            preregistration_path: registry_path(
                "20260721_settlement_source_anchor_preregistration.json",
            ),
            variant_path: registry_path("20260721_settlement_source_anchor_baseline_variant.json"),
            candidate_condition_set_path: source_input.condition_set_path.clone(),
            prior_condition_set_paths: vec![prior_path.display().to_string()],
            report_output_path: official_report_path.display().to_string(),
            trades_output_path: official_trades_path.display().to_string(),
            pair_audit_output_path: pair_audit_path.display().to_string(),
        })
        .unwrap();
        assert!(lock.ok, "{:#?}", lock.failure_reasons);
        let lock_path = temp.path().join("allocation-lock.json");
        std::fs::write(&lock_path, serde_json::to_vec_pretty(&lock).unwrap()).unwrap();
        let allocation_evidence = validate_settlement_anchor_allocation_lock(
            &lock_path,
            &lock.allowed_condition_ids,
            official_report_path.to_str().unwrap(),
            official_trades_path.to_str().unwrap(),
        )
        .unwrap();

        let source_audit = settlement_anchor_source_audit(source_input.clone()).unwrap();
        assert!(source_audit.ok);
        let source_audit_path = temp.path().join("source-audit.json");
        std::fs::write(
            &source_audit_path,
            serde_json::to_vec_pretty(&source_audit).unwrap(),
        )
        .unwrap();
        let source_evidence = validate_settlement_anchor_source_audit(
            &source_audit_path,
            &lock.candidate_condition_set.sha256,
            &source_input.fair_value_btc_csv_path,
        )
        .unwrap();

        let frozen_variant_path =
            registry_path("20260721_settlement_source_anchor_baseline_variant.json");
        let strategy_params: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&frozen_variant_path).unwrap()).unwrap();
        let signal_path = temp.path().join("signal.csv");
        let settlement_path = temp.path().join("settlement.csv");
        std::fs::write(&signal_path, b"timestamp_ms,source,price\n1,fixture,1\n").unwrap();
        std::fs::write(
            &settlement_path,
            b"timestamp_ms,source,price\n1,fixture,1\n",
        )
        .unwrap();
        let cache_path = temp.path().join("pmxt-cache");
        std::fs::create_dir(&cache_path).unwrap();
        let pmxt_input_path = cache_path.join("fixture.events.bin.gz");
        std::fs::write(&pmxt_input_path, b"fixture-pmxt-events").unwrap();

        let first =
            DateTime::parse_from_rfc3339(&candidate.conditions.first().unwrap().window_start)
                .unwrap()
                .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339(&candidate.conditions.last().unwrap().window_start)
            .unwrap()
            .with_timezone(&Utc)
            + chrono::Duration::seconds(300);
        let start_text = first.to_rfc3339();
        let end_text = end.to_rfc3339();

        let mut catalog = MarketCatalog::default();
        for (index, condition) in candidate.conditions.iter().enumerate() {
            let up_token = format!("up-{index}");
            let down_token = format!("down-{index}");
            catalog
                .token_to_condition
                .insert(up_token.clone(), condition.condition_id.clone());
            catalog
                .token_to_condition
                .insert(down_token.clone(), condition.condition_id.clone());
            catalog.markets.insert(
                condition.condition_id.clone(),
                CatalogMarket {
                    condition_id: condition.condition_id.clone(),
                    question: format!("BTC up or down {index}"),
                    slug: format!("btc-{index}"),
                    asset: "BTC".to_string(),
                    window_description: "5m".to_string(),
                    end_date: (DateTime::parse_from_rfc3339(&condition.window_start)
                        .unwrap()
                        .with_timezone(&Utc)
                        + chrono::Duration::seconds(300))
                    .to_rfc3339(),
                    up_token_id: up_token,
                    down_token_id: down_token,
                    neg_risk: false,
                    liquidity: 1_000.0,
                    volume: 1_000.0,
                },
            );
        }

        let mut pmxt = DataSourceManifest::new("pmxt_v2_archive", "pmxt_v2");
        pmxt.path = Some(cache_path.display().to_string());
        pmxt.start = Some(start_text.clone());
        pmxt.end = Some(end_text.clone());
        pmxt.row_count = Some(10_000);
        pmxt.complete = true;
        let pmxt_artifacts = vec![HashedArtifact {
            path: pmxt_input_path.display().to_string(),
            sha256: sha256_bytes(&std::fs::read(&pmxt_input_path).unwrap()),
        }];
        let pmxt_artifacts_hash = stable_json_hash(&pmxt_artifacts);
        pmxt.checksum_sha256 = Some(pmxt_artifacts_hash.clone());
        pmxt.metadata.insert(
            "input_artifact_count".to_string(),
            pmxt_artifacts.len().to_string(),
        );
        pmxt.metadata
            .insert("input_artifacts_hash".to_string(), pmxt_artifacts_hash);
        pmxt.metadata.insert(
            "input_artifacts_json".to_string(),
            serde_json::to_string(&pmxt_artifacts).unwrap(),
        );
        pmxt.metadata.insert(
            "replay_semantics_version".to_string(),
            CURRENT_REPLAY_SEMANTICS_VERSION.to_string(),
        );
        pmxt.metadata.insert(
            "taker_fill_model".to_string(),
            "max_share_budget_optimized_visible_l2_bookwalk_with_fok_limit".to_string(),
        );
        pmxt.metadata.insert(
            "decision_edge".to_string(),
            "fair_minus_executable_vwap_minus_effective_entry_fee".to_string(),
        );
        let mut signal = complete_source("btc_price_tape", "external_price", &signal_path);
        signal.start = Some(start_text.clone());
        signal.end = Some(end_text.clone());
        signal
            .metadata
            .insert("source_kind".to_string(), "fixture_signal".to_string());
        signal
            .metadata
            .insert("role".to_string(), "causal_signal".to_string());
        let mut settlement = complete_source(
            "btc_settlement_price_tape",
            "external_price",
            &settlement_path,
        );
        settlement.start = Some(start_text.clone());
        settlement.end = Some(end_text.clone());
        settlement
            .metadata
            .insert("source_kind".to_string(), "fixture_settlement".to_string());
        settlement
            .metadata
            .insert("role".to_string(), "market_resolution".to_string());
        let common_sources = vec![pmxt, signal, settlement];

        let mut fair = complete_source(
            "btc_fair_value_price_tape",
            "external_price",
            Path::new(&source_input.fair_value_btc_csv_path),
        );
        fair.start = Some(start_text.clone());
        fair.end = Some(end_text.clone());
        fair.metadata.insert(
            "source_kind".to_string(),
            "chainlink_btc_usd_data_stream".to_string(),
        );
        fair.metadata.insert(
            "role".to_string(),
            "fair_value_spot_and_strike_only".to_string(),
        );
        fair.metadata.insert(
            "current_max_age_ms".to_string(),
            OFFICIAL_CURRENT_MAX_AGE_MS.to_string(),
        );
        fair.metadata.insert(
            "open_max_age_ms".to_string(),
            OFFICIAL_OPEN_MAX_AGE_MS.to_string(),
        );
        let mut allocation_source = DataSourceManifest::new(
            "settlement_anchor_allocation_lock",
            "forward_condition_allocation",
        );
        allocation_source.path = Some(allocation_evidence.path.clone());
        allocation_source.row_count = Some(allocation_evidence.condition_count as u64);
        allocation_source.checksum_sha256 = Some(allocation_evidence.sha256.clone());
        allocation_source.complete = true;
        allocation_source.metadata.insert(
            "condition_ids_hash".to_string(),
            allocation_evidence.condition_ids_hash.clone(),
        );
        allocation_source.metadata.insert(
            "report_partition_hash".to_string(),
            allocation_evidence.report_partition_hash.clone(),
        );
        allocation_source.metadata.insert(
            "pair_audit_output".to_string(),
            allocation_evidence.score_outputs.pair_audit_json.clone(),
        );
        let mut source_audit_source =
            DataSourceManifest::new("settlement_anchor_source_audit", "official_anchor_coverage");
        source_audit_source.path = Some(source_evidence.path.clone());
        source_audit_source.row_count = Some(source_evidence.condition_count as u64);
        source_audit_source.checksum_sha256 = Some(source_evidence.sha256.clone());
        source_audit_source.complete = true;
        source_audit_source.metadata.insert(
            "fair_value_btc_csv_sha256".to_string(),
            source_evidence.fair_value_btc_csv_sha256.clone(),
        );

        let trade_indices: Vec<_> = (0..32)
            .flat_map(|report_index| (0..3).map(move |offset| report_index * 24 + offset))
            .collect();
        let trade_rows: Vec<_> = trade_indices
            .iter()
            .map(|index| {
                let condition = &candidate.conditions[*index];
                let open = DateTime::parse_from_rfc3339(&condition.window_start)
                    .unwrap()
                    .with_timezone(&Utc);
                serde_json::json!({
                    "fill": {
                        "order": {
                            "condition_id": condition.condition_id,
                            "token_id": format!("up-{index}"),
                        },
                        "fill_timestamp_s": open.timestamp() as f64 + 150.0,
                    },
                    "won": true,
                    "pnl_after_fee": 1.0,
                })
            })
            .collect();
        let trades = trade_rows.len();
        let variant = experiment_variant(&strategy_params, trades, trades, 0, trades as f64);
        let baseline_report = ExperimentReport {
            schema_version: 1,
            generated_at: Utc::now().to_rfc3339(),
            label: "baseline".to_string(),
            mode: "backtest".to_string(),
            start: start_text.clone(),
            end: end_text.clone(),
            bankroll_usd: 100.0,
            latency_ms: 202,
            market_catalog: catalog.clone(),
            data_manifest: DataManifest::new(common_sources.clone(), Vec::new()),
            variants: vec![variant.clone()],
        };
        let mut official_sources = common_sources;
        official_sources.extend([fair, allocation_source, source_audit_source]);
        let official_report = ExperimentReport {
            label: "official".to_string(),
            data_manifest: DataManifest::new(official_sources, Vec::new()),
            ..baseline_report.clone()
        };
        let baseline_report_path = temp.path().join("baseline-report.json");
        std::fs::write(
            &baseline_report_path,
            serde_json::to_vec_pretty(&baseline_report).unwrap(),
        )
        .unwrap();
        std::fs::write(
            &official_report_path,
            serde_json::to_vec_pretty(&official_report).unwrap(),
        )
        .unwrap();

        let trade_variant = serde_json::json!({
            "strategy_name": "candle_momentum",
            "risk_profile": "fixture-risk",
            "strategy_params": strategy_params,
            "summary": {
                "trades": trades,
                "wins": trades,
                "losses": 0,
                "total_pnl": trades as f64,
                "unresolved_fills": 0,
            },
            "trades": trade_rows,
            "unresolved_fills": [],
        });
        let baseline_trades = serde_json::json!({
            "schema_version": 1,
            "mode": "harness_sweep_trades",
            "start": start_text,
            "end": end_text,
            "bankroll_usd": 100.0,
            "max_total_exposure_usd": 100.0,
            "latency_ms": 202,
            "window_minutes": 5.0,
            "continuous": true,
            "settlement_anchor_allocation": null,
            "settlement_anchor_source": null,
            "variants": [trade_variant],
        });
        let mut official_trades = baseline_trades.clone();
        official_trades["settlement_anchor_allocation"] =
            serde_json::to_value(&allocation_evidence).unwrap();
        official_trades["settlement_anchor_source"] =
            serde_json::to_value(&source_evidence).unwrap();
        let baseline_trades_path = temp.path().join("baseline-trades.json");
        std::fs::write(
            &baseline_trades_path,
            serde_json::to_vec_pretty(&baseline_trades).unwrap(),
        )
        .unwrap();
        std::fs::write(
            &official_trades_path,
            serde_json::to_vec_pretty(&official_trades).unwrap(),
        )
        .unwrap();

        PairFixture {
            input: SettlementAnchorPairAuditInput {
                allocation_lock_path: lock_path.display().to_string(),
                source_audit_path: source_audit_path.display().to_string(),
                fair_value_btc_csv_path: source_input.fair_value_btc_csv_path,
                baseline_report_path: baseline_report_path.display().to_string(),
                baseline_trades_path: baseline_trades_path.display().to_string(),
                official_report_path: official_report_path.display().to_string(),
                official_trades_path: official_trades_path.display().to_string(),
                output_path: pair_audit_path.display().to_string(),
            },
            _temp: temp,
        }
    }

    #[test]
    fn source_audit_passes_full_causal_coverage_and_published_prices() {
        let (temp, input) = fixture(&BTreeSet::new(), None);
        let fair_value_path = input.fair_value_btc_csv_path.clone();
        let audit = settlement_anchor_source_audit(input).unwrap();
        assert!(audit.ok);
        assert_eq!(audit.counts.conditions, 750);
        assert_eq!(audit.counts.reports, 32);
        assert_eq!(audit.counts.source_covered_conditions, 750);
        assert_eq!(audit.counts.published_price_mismatches, 0);
        assert_eq!(audit.official_anchor_coverage, 1.0);
        assert_eq!(audit.maximum_published_price_difference_usd, Some(0.0));
        assert!(audit.blindness.values().all(|accessed| !accessed));

        let audit_path = temp.path().join("source-audit.json");
        std::fs::write(&audit_path, serde_json::to_vec_pretty(&audit).unwrap()).unwrap();
        let evidence = validate_settlement_anchor_source_audit(
            &audit_path,
            &audit.condition_set.sha256,
            &fair_value_path,
        )
        .unwrap();
        assert_eq!(evidence.condition_count, 750);
        assert_eq!(evidence.report_count, 32);
        assert_eq!(evidence.official_anchor_coverage, 1.0);
    }

    #[test]
    fn source_audit_rejects_coverage_below_frozen_95_percent() {
        let missing: BTreeSet<_> = (0..38).collect();
        let (_temp, input) = fixture(&missing, None);
        let audit = settlement_anchor_source_audit(input).unwrap();
        assert!(!audit.ok);
        assert_eq!(audit.counts.source_covered_conditions, 712);
        assert!(audit.official_anchor_coverage < MIN_OFFICIAL_ANCHOR_COVERAGE);
        assert!(audit
            .failure_reasons
            .contains(&"check_failed:minimum_official_anchor_coverage".to_string()));
    }

    #[test]
    fn source_audit_rejects_published_price_mismatch() {
        let (_temp, input) = fixture(&BTreeSet::new(), Some(17));
        let audit = settlement_anchor_source_audit(input).unwrap();
        assert!(!audit.ok);
        assert_eq!(audit.counts.published_price_mismatches, 1);
        assert!(audit
            .failure_reasons
            .contains(&"check_failed:published_price_to_beat_reproduction".to_string()));
    }

    #[test]
    fn paired_audit_passes_recomputed_absolute_gates_but_never_claims_a_plus() {
        let fixture = pair_fixture();
        let audit = settlement_anchor_pair_audit(fixture.input).unwrap();
        assert!(audit.ok, "{:#?}", audit.failure_reasons);
        assert_eq!(
            audit.status,
            "BLOCK_ABSOLUTE_A_PLUS_GATES_PASS_REPLICATION_REQUIRED"
        );
        assert_eq!(audit.official_metrics.trades, 96);
        assert_eq!(audit.official_metrics.eligible_reports, 32);
        assert_eq!(audit.official_metrics.profitable_reports, 32);
        assert!(audit.parity_checks.iter().all(|check| check.passed));
        assert!(audit.absolute_gates.iter().all(|gate| gate.passed));
        assert!(audit.decision["second_disjoint_block_required"]);
        assert!(!audit.decision["profitability_claim"]);
        assert!(!audit.decision["a_plus_claim"]);
    }

    #[test]
    fn paired_audit_rejects_non_fair_source_hash_drift() {
        let fixture = pair_fixture();
        let mut report: ExperimentReport =
            serde_json::from_slice(&std::fs::read(&fixture.input.official_report_path).unwrap())
                .unwrap();
        let signal = report
            .data_manifest
            .sources
            .iter_mut()
            .find(|source| source.name == "btc_price_tape")
            .unwrap();
        signal.checksum_sha256 = Some("f".repeat(64));
        report.data_manifest.manifest_hash = report.data_manifest.compute_hash();
        std::fs::write(
            &fixture.input.official_report_path,
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();

        let audit = settlement_anchor_pair_audit(fixture.input).unwrap();
        assert!(!audit.ok);
        assert!(audit
            .failure_reasons
            .contains(&"parity_failed:non_fair_data_sources_match".to_string()));
    }

    #[test]
    fn paired_audit_rejects_reuse_of_its_locked_output_path() {
        let fixture = pair_fixture();
        std::fs::write(&fixture.input.output_path, b"{}\n").unwrap();

        let audit = settlement_anchor_pair_audit(fixture.input).unwrap();
        assert!(!audit.ok);
        assert!(audit
            .failure_reasons
            .contains(&"parity_failed:official_outputs_match_allocation_lock".to_string()));
    }

    #[test]
    fn paired_audit_recomputes_and_rejects_negative_second_half() {
        let fixture = pair_fixture();
        let mut trades: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&fixture.input.official_trades_path).unwrap())
                .unwrap();
        trades["variants"][0]["trades"][48]["won"] = serde_json::json!(false);
        trades["variants"][0]["trades"][48]["pnl_after_fee"] = serde_json::json!(-50.0);
        trades["variants"][0]["summary"]["wins"] = serde_json::json!(95);
        trades["variants"][0]["summary"]["losses"] = serde_json::json!(1);
        trades["variants"][0]["summary"]["total_pnl"] = serde_json::json!(45.0);
        std::fs::write(
            &fixture.input.official_trades_path,
            serde_json::to_vec_pretty(&trades).unwrap(),
        )
        .unwrap();

        let mut report: ExperimentReport =
            serde_json::from_slice(&std::fs::read(&fixture.input.official_report_path).unwrap())
                .unwrap();
        report.variants[0] =
            experiment_variant(&report.variants[0].strategy_params, 96, 95, 1, 45.0);
        std::fs::write(
            &fixture.input.official_report_path,
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();

        let audit = settlement_anchor_pair_audit(fixture.input).unwrap();
        assert!(!audit.ok);
        assert!(audit.parity_checks.iter().all(|check| check.passed));
        assert!(audit
            .absolute_gates
            .iter()
            .find(|gate| gate.name == "positive_second_half_pnl")
            .is_some_and(|gate| !gate.passed));
        assert!(audit.official_metrics.second_half_fee_inclusive_pnl_usd < 0.0);
    }
}
