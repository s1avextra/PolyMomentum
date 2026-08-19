//! Locked one-shot fresh-holdout gate — the promotion endgame.
//!
//! Consumes the sealed dataset's `fresh_holdout` rows EXACTLY ONCE for one
//! frozen policy. Ordering is crash-safe fail-closed: the consumed marker
//! is written (atomically, refusing overwrite) BEFORE any fresh outcome is
//! computed, so no code path — including a crash mid-run — can leave the
//! fresh block openable a second time. Fresh labels are computed in memory
//! and never written to disk; only the aggregate verdict is persisted.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::artifact::write_json_artifact_atomic;
use crate::backtest::btc_history::BTCHistory;

use super::opportunity_dataset::{
    load_sealed_opportunities, sha256_file, CausalOpportunity,
    RESOLUTION_RULE_CHANGE_AMBIGUOUS_END_MS,
};

#[derive(Debug, Clone)]
pub struct FreshGateInput {
    pub dataset_seal_path: PathBuf,
    pub settlement_tape_path: PathBuf,
    pub policy_path: PathBuf,
    pub preregistration_path: PathBuf,
    pub consumed_dir: PathBuf,
    pub output_path: PathBuf,
}

/// The frozen policy being tested. Field semantics match the discovery
/// screen exactly; the file's sha256 is pinned into the verdict.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrozenPolicy {
    pub family: String,
    pub decision_seconds: i64,
    pub lock_strength_min: f64,
    pub ask_cap: f64,
    pub min_lock_fraction: f64,
    /// Wilson-edge margin the fresh block must clear (the +0.02 stage).
    pub advancement_margin: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshGateVerdict {
    pub schema_version: u32,
    pub generated_at: String,
    pub dataset_sha256: String,
    pub policy_sha256: String,
    pub preregistration_sha256: String,
    pub policy: FrozenPolicy,
    pub fresh_rows_total: usize,
    pub fresh_rows_selected: usize,
    pub lead_side_mismatch_rows: usize,
    pub wins: usize,
    pub win_rate: Option<f64>,
    pub avg_break_even: Option<f64>,
    pub wilson_lower: Option<f64>,
    pub wilson_edge: Option<f64>,
    pub fee_aware_payoff_usd: f64,
    pub verdict: String,
    pub fresh_outcomes_written_to_disk: bool,
}

fn wilson_lower(wins: usize, n: usize) -> f64 {
    if n == 0 {
        return 0.0;
    }
    let z = 1.959_963_984_540_054_f64;
    let n_f = n as f64;
    let p = wins as f64 / n_f;
    let denominator = 1.0 + z * z / n_f;
    let centre = p + z * z / (2.0 * n_f);
    let radius = z * ((p * (1.0 - p) + z * z / (4.0 * n_f)) / n_f).sqrt();
    ((centre - radius) / denominator).clamp(0.0, 1.0)
}

fn policy_selects(op: &CausalOpportunity, policy: &FrozenPolicy) -> PolicyDecision {
    let decision_s = op.elapsed_seconds.round() as i64;
    if decision_s != policy.decision_seconds {
        return PolicyDecision::OutOfScope;
    }
    let Some(lock_fraction) = op.twap_locked_fraction else {
        return PolicyDecision::OutOfScope;
    };
    if lock_fraction + 1e-9 < policy.min_lock_fraction {
        return PolicyDecision::OutOfScope;
    }
    let Some(lead) = op.partial_twap_lead_usd else {
        return PolicyDecision::OutOfScope;
    };
    let sigma_tail = op.btc_open
        * op.causal_volatility
        * (op.remaining_seconds / 31_536_000.0).sqrt();
    if sigma_tail <= 0.0 || lead.abs() / sigma_tail < policy.lock_strength_min {
        return PolicyDecision::OutOfScope;
    }
    let lead_direction = if lead > 0.0 { "up" } else { "down" };
    if lead_direction != op.signal_direction {
        return PolicyDecision::LeadSideMismatch;
    }
    if !op.book_observable || !op.stake_fully_executable {
        return PolicyDecision::OutOfScope;
    }
    match op.best_ask {
        Some(ask) if ask <= policy.ask_cap + 1e-9 => PolicyDecision::Selected,
        _ => PolicyDecision::OutOfScope,
    }
}

enum PolicyDecision {
    Selected,
    LeadSideMismatch,
    OutOfScope,
}

pub fn run_fresh_gate(input: FreshGateInput) -> Result<FreshGateVerdict> {
    let dataset_sha256 = sha256_file(&input.dataset_seal_path)?;
    let policy_sha256 = sha256_file(&input.policy_path)?;
    let preregistration_sha256 = sha256_file(&input.preregistration_path)?;
    let policy: FrozenPolicy = serde_json::from_reader(
        std::fs::File::open(&input.policy_path)
            .with_context(|| format!("open policy {}", input.policy_path.display()))?,
    )
    .context("parse frozen policy")?;
    if !(policy.advancement_margin.is_finite() && policy.advancement_margin >= 0.0) {
        bail!("advancement_margin must be a non-negative finite number");
    }

    // One-shot enforcement, keyed by the DATASET: a sealed fresh block can
    // be opened once, for one policy, ever. Marker lands before outcomes.
    std::fs::create_dir_all(&input.consumed_dir)
        .with_context(|| format!("create consumed dir {}", input.consumed_dir.display()))?;
    let marker_path = input
        .consumed_dir
        .join(format!("{dataset_sha256}.fresh_consumed.json"));
    if marker_path.exists() {
        bail!(
            "fresh holdout for dataset {dataset_sha256} was already consumed \
             (marker: {}); a sealed fresh block can never be reopened",
            marker_path.display(),
        );
    }
    write_consumed_marker_exclusive(
        &marker_path,
        &dataset_sha256,
        &policy_sha256,
        &preregistration_sha256,
    )?;

    // Only now do fresh outcomes get computed — in memory.
    let (_seal, opportunities) = load_sealed_opportunities(&input.dataset_seal_path)?;
    let mut tape = BTCHistory::new();
    let rows = tape
        .load_csv(&input.settlement_tape_path)
        .context("load settlement tape")?;
    if rows == 0 {
        bail!("settlement tape is empty");
    }

    let fresh: Vec<&CausalOpportunity> = opportunities
        .iter()
        .filter(|op| op.chronological_window == "fresh_holdout")
        .collect();
    let mut selected = Vec::new();
    let mut mismatches = 0usize;
    for op in &fresh {
        if op.window_start_ms < RESOLUTION_RULE_CHANGE_AMBIGUOUS_END_MS {
            bail!(
                "fresh row {} predates the TWAP era; refusing a mixed-rule fresh gate",
                op.opportunity_id
            );
        }
        match policy_selects(op, &policy) {
            PolicyDecision::Selected => selected.push(*op),
            PolicyDecision::LeadSideMismatch => mismatches += 1,
            PolicyDecision::OutOfScope => {}
        }
    }

    let mut wins = 0usize;
    let mut payoff = 0.0_f64;
    let mut break_evens = Vec::new();
    for op in &selected {
        let window_seconds = op.elapsed_seconds + op.remaining_seconds;
        let close_ms = op.window_start_ms + (window_seconds * 1000.0).round() as i64;
        let twap = tape
            .twap_between(op.window_start_ms, close_ms)
            .with_context(|| {
                format!(
                    "settlement tape cannot resolve fresh window starting {}",
                    op.window_start_ms
                )
            })?;
        // Official rule: TWAP >= open resolves Up (ties Up).
        let outcome = if twap >= op.btc_open { "up" } else { "down" };
        let won = outcome == op.signal_direction;
        if won {
            wins += 1;
            payoff += op.fee_aware_net_win_usd.unwrap_or(0.0);
        } else {
            payoff -= op.fee_aware_max_loss_usd.unwrap_or(0.0);
        }
        if let Some(be) = op.fee_aware_break_even_probability {
            break_evens.push(be);
        }
    }

    let n = selected.len();
    let win_rate = (n > 0).then(|| wins as f64 / n as f64);
    let avg_break_even = (!break_evens.is_empty())
        .then(|| break_evens.iter().sum::<f64>() / break_evens.len() as f64);
    let wl = (n > 0).then(|| wilson_lower(wins, n));
    let wilson_edge = match (wl, avg_break_even) {
        (Some(lower), Some(be)) => Some(lower - be),
        _ => None,
    };
    let verdict = match wilson_edge {
        None => "fresh_no_selected_rows".to_string(),
        Some(edge) if edge > policy.advancement_margin && payoff > 0.0 => {
            "fresh_gate_passed".to_string()
        }
        Some(_) => "fresh_gate_failed".to_string(),
    };

    let result = FreshGateVerdict {
        schema_version: 1,
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        dataset_sha256,
        policy_sha256,
        preregistration_sha256,
        policy,
        fresh_rows_total: fresh.len(),
        fresh_rows_selected: n,
        lead_side_mismatch_rows: mismatches,
        wins,
        win_rate,
        avg_break_even,
        wilson_lower: wl,
        wilson_edge,
        fee_aware_payoff_usd: payoff,
        verdict,
        fresh_outcomes_written_to_disk: false,
    };
    write_json_artifact_atomic(&input.output_path, &result)?;
    Ok(result)
}

/// Exclusive-create marker write: `create_new` guarantees that two
/// concurrent invocations cannot both claim the fresh block.
fn write_consumed_marker_exclusive(
    path: &Path,
    dataset_sha256: &str,
    policy_sha256: &str,
    preregistration_sha256: &str,
) -> Result<()> {
    let payload = serde_json::json!({
        "schema_version": 1,
        "consumed_at": Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        "dataset_sha256": dataset_sha256,
        "policy_sha256": policy_sha256,
        "preregistration_sha256": preregistration_sha256,
    });
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("exclusive-create consumed marker {}", path.display()))?;
    use std::io::Write;
    file.write_all(serde_json::to_string_pretty(&payload)?.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> FrozenPolicy {
        FrozenPolicy {
            family: "partial_twap_lock_v2".to_string(),
            decision_seconds: 240,
            lock_strength_min: 0.25,
            ask_cap: 0.90,
            min_lock_fraction: 0.6,
            advancement_margin: 0.02,
        }
    }

    fn fresh_op(id: &str, lead: f64, ask: f64) -> CausalOpportunity {
        CausalOpportunity {
            opportunity_id: id.to_string(),
            condition_id: "0xc".to_string(),
            token_id: "t".to_string(),
            chronological_window: "fresh_holdout".to_string(),
            window_start_ms: RESOLUTION_RULE_CHANGE_AMBIGUOUS_END_MS + 3_600_000,
            observed_at_ms: RESOLUTION_RULE_CHANGE_AMBIGUOUS_END_MS + 3_840_000,
            signal_direction: if lead > 0.0 { "up" } else { "down" }.to_string(),
            strike_price: 100_000.0,
            btc_observed: 100_000.0 + lead,
            elapsed_seconds: 240.0,
            remaining_seconds: 60.0,
            move_2m_usd: None,
            path_2m_aligned: None,
            path_3m_aligned: None,
            path_4m_aligned: None,
            directional_distance_to_strike_usd: lead,
            causal_volatility: 0.09,
            book_observable: true,
            best_ask: Some(ask),
            top_book_pressure: None,
            stake_fully_executable: true,
            fee_aware_break_even_probability: Some(ask + 0.07 * ask * (1.0 - ask)),
            fee_aware_net_win_usd: Some(5.0 * (1.0 / ask - 1.0)),
            fee_aware_max_loss_usd: Some(5.0),
            btc_open: 100_000.0,
            partial_twap_lead_usd: Some(lead),
            twap_locked_fraction: Some(0.8),
        }
    }

    #[test]
    fn policy_selection_matches_screen_semantics() {
        let p = policy();
        // Strong up-lead, cheap ask -> selected.
        assert!(matches!(
            policy_selects(&fresh_op("a", 50.0, 0.50), &p),
            PolicyDecision::Selected
        ));
        // Lead favours the OTHER side -> mismatch, not silently dropped.
        let mut wrong_side = fresh_op("b", 50.0, 0.50);
        wrong_side.signal_direction = "down".to_string();
        assert!(matches!(
            policy_selects(&wrong_side, &p),
            PolicyDecision::LeadSideMismatch
        ));
        // Ask above cap -> out of scope.
        assert!(matches!(
            policy_selects(&fresh_op("c", 50.0, 0.95), &p),
            PolicyDecision::OutOfScope
        ));
        // Tiny lead below the lock floor -> out of scope.
        assert!(matches!(
            policy_selects(&fresh_op("d", 0.01, 0.50), &p),
            PolicyDecision::OutOfScope
        ));
    }

    #[test]
    fn consumed_marker_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("abc.fresh_consumed.json");
        write_consumed_marker_exclusive(&marker, "abc", "p", "r").unwrap();
        let second = write_consumed_marker_exclusive(&marker, "abc", "p", "r");
        assert!(second.is_err(), "second claim must fail via create_new");
    }

    #[test]
    fn wilson_lower_matches_production_constant() {
        // Same value the policy/replay layers produce for 25/32.
        let lower = wilson_lower(25, 32);
        assert!((lower - 0.6180).abs() < 0.01, "got {lower}");
    }
}
