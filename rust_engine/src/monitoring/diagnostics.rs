//! Session diagnostics for the production loop.

use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct SessionDiagnostics {
    pub schema_version: u32,
    pub path: String,
    pub ok: bool,
    pub mode: Option<String>,
    pub promotion_status: Option<String>,
    pub promotion_strategy_hash: Option<String>,
    pub promotion_source_report_hash: Option<String>,
    pub promotion_data_manifest_hash: Option<String>,
    pub release_manifest_seen: bool,
    pub total_events: u64,
    pub malformed_lines: u64,
    pub event_counts: BTreeMap<String, u64>,
    pub signals: SignalDiagnostics,
    pub orders: OrderDiagnostics,
    pub resolutions: ResolutionDiagnostics,
    pub oracle: OracleDiagnostics,
    pub risk: RiskDiagnostics,
    pub system: SystemDiagnostics,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionComparison {
    pub schema_version: u32,
    pub ok: bool,
    pub left_path: String,
    pub right_path: String,
    pub left_mode: Option<String>,
    pub right_mode: Option<String>,
    pub left_promotion_strategy_hash: Option<String>,
    pub right_promotion_strategy_hash: Option<String>,
    pub event_count_delta: BTreeMap<String, i64>,
    pub mismatches: Vec<String>,
    pub left: SessionDiagnostics,
    pub right: SessionDiagnostics,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SignalDiagnostics {
    pub evaluations: u64,
    pub skips: u64,
    pub decision_trades: u64,
    pub execution_attempted: u64,
    pub traded_true: u64,
    pub missing_replay_fields: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OrderDiagnostics {
    pub placed: u64,
    pub filled: u64,
    pub rejected: u64,
    pub missing_intent_id: u64,
    pub placed_missing_state: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ResolutionDiagnostics {
    pub resolved: u64,
    pub wins: u64,
    pub losses: u64,
    pub total_pnl: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OracleDiagnostics {
    pub checks: u64,
    pub disagreements: u64,
    pub corrections: u64,
    pub total_pnl_delta: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RiskDiagnostics {
    pub snapshots: u64,
    pub first_bankroll: Option<f64>,
    pub last_bankroll: Option<f64>,
    pub first_realized_pnl: Option<f64>,
    pub last_realized_pnl: Option<f64>,
    pub first_wins: Option<u64>,
    pub first_losses: Option<u64>,
    pub last_wins: Option<u64>,
    pub last_losses: Option<u64>,
    pub max_positions: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SystemDiagnostics {
    pub errors: u64,
    pub fatal_errors: u64,
    pub first_errors: Vec<String>,
}

pub fn analyze_session(path: impl AsRef<Path>) -> Result<SessionDiagnostics> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)
        .with_context(|| format!("open session log {}", path.display()))?;
    let reader = std::io::BufReader::new(file);

    let mut out = SessionDiagnostics {
        schema_version: 1,
        path: path.display().to_string(),
        ok: false,
        mode: None,
        promotion_status: None,
        promotion_strategy_hash: None,
        promotion_source_report_hash: None,
        promotion_data_manifest_hash: None,
        release_manifest_seen: false,
        total_events: 0,
        malformed_lines: 0,
        event_counts: BTreeMap::new(),
        signals: SignalDiagnostics::default(),
        orders: OrderDiagnostics::default(),
        resolutions: ResolutionDiagnostics::default(),
        oracle: OracleDiagnostics::default(),
        risk: RiskDiagnostics::default(),
        system: SystemDiagnostics::default(),
        warnings: Vec::new(),
    };

    for line in reader.lines() {
        let line = line.with_context(|| format!("read session log {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(&line) else {
            out.malformed_lines += 1;
            continue;
        };
        out.total_events += 1;
        let cat = v.get("cat").and_then(|x| x.as_str()).unwrap_or("unknown");
        let ty = v.get("type").and_then(|x| x.as_str()).unwrap_or("unknown");
        *out.event_counts.entry(format!("{cat}.{ty}")).or_insert(0) += 1;

        match (cat, ty) {
            ("system", "release_manifest") => record_release_manifest(&mut out, &v),
            ("system", "error") => record_system_error(&mut out, &v),
            ("signal", "evaluation") => record_signal_evaluation(&mut out, &v),
            ("signal", "skip") => out.signals.skips += 1,
            ("order", "placed") => record_order_placed(&mut out, &v),
            ("order", "filled") => record_order_filled(&mut out, &v),
            ("order", "rejected") => out.orders.rejected += 1,
            ("resolution", "resolved") => record_resolution(&mut out, &v),
            ("oracle", "resolution") => record_oracle_resolution(&mut out, &v),
            ("oracle", "correction") => record_oracle_correction(&mut out, &v),
            ("risk", "state") => record_risk_state(&mut out, &v),
            _ => {}
        }
    }

    finalize(&mut out);
    Ok(out)
}

pub fn compare_sessions(
    left_path: impl AsRef<Path>,
    right_path: impl AsRef<Path>,
) -> Result<SessionComparison> {
    let left = analyze_session(left_path)?;
    let right = analyze_session(right_path)?;
    let mut mismatches = Vec::new();
    if !left.ok {
        mismatches.push("left session diagnostics are not ok".to_string());
    }
    if !right.ok {
        mismatches.push("right session diagnostics are not ok".to_string());
    }
    if left.promotion_strategy_hash != right.promotion_strategy_hash {
        mismatches.push("promotion strategy hash differs".to_string());
    }
    if left.promotion_source_report_hash != right.promotion_source_report_hash {
        mismatches.push("promotion source report hash differs".to_string());
    }
    if left.promotion_data_manifest_hash != right.promotion_data_manifest_hash {
        mismatches.push("promotion data manifest hash differs".to_string());
    }

    let mut keys: Vec<String> = left
        .event_counts
        .keys()
        .chain(right.event_counts.keys())
        .cloned()
        .collect();
    keys.sort();
    keys.dedup();
    let event_count_delta = keys
        .into_iter()
        .map(|key| {
            let l = *left.event_counts.get(&key).unwrap_or(&0) as i64;
            let r = *right.event_counts.get(&key).unwrap_or(&0) as i64;
            (key, r - l)
        })
        .collect();

    Ok(SessionComparison {
        schema_version: 1,
        ok: mismatches.is_empty(),
        left_path: left.path.clone(),
        right_path: right.path.clone(),
        left_mode: left.mode.clone(),
        right_mode: right.mode.clone(),
        left_promotion_strategy_hash: left.promotion_strategy_hash.clone(),
        right_promotion_strategy_hash: right.promotion_strategy_hash.clone(),
        event_count_delta,
        mismatches,
        left,
        right,
    })
}

fn record_release_manifest(out: &mut SessionDiagnostics, v: &Value) {
    out.release_manifest_seen = true;
    out.mode = v
        .get("mode")
        .and_then(|x| x.as_str())
        .map(ToString::to_string);
    out.promotion_status = v
        .get("promotion")
        .and_then(|p| p.get("status"))
        .and_then(|x| x.as_str())
        .map(ToString::to_string);
    out.promotion_strategy_hash = v
        .get("promotion")
        .and_then(|p| p.get("strategy"))
        .and_then(|s| s.get("params_hash"))
        .and_then(|x| x.as_str())
        .map(ToString::to_string);
    out.promotion_source_report_hash = v
        .get("promotion")
        .and_then(|p| p.get("source_report_hash"))
        .and_then(|x| x.as_str())
        .map(ToString::to_string);
    out.promotion_data_manifest_hash = v
        .get("promotion")
        .and_then(|p| p.get("data_manifest_hash"))
        .and_then(|x| x.as_str())
        .map(ToString::to_string);
}

fn record_system_error(out: &mut SessionDiagnostics, v: &Value) {
    out.system.errors += 1;
    if !v
        .get("recoverable")
        .and_then(|x| x.as_bool())
        .unwrap_or(true)
    {
        out.system.fatal_errors += 1;
    }
    if out.system.first_errors.len() < 10 {
        let component = v
            .get("component")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown");
        let error = v.get("error").and_then(|x| x.as_str()).unwrap_or("");
        out.system
            .first_errors
            .push(format!("{component}: {error}"));
    }
}

fn record_signal_evaluation(out: &mut SessionDiagnostics, v: &Value) {
    out.signals.evaluations += 1;
    if v.get("decision_trade")
        .and_then(|x| x.as_bool())
        .unwrap_or(false)
    {
        out.signals.decision_trades += 1;
    }
    if v.get("execution_attempted")
        .and_then(|x| x.as_bool())
        .unwrap_or(false)
    {
        out.signals.execution_attempted += 1;
    }
    if v.get("traded").and_then(|x| x.as_bool()).unwrap_or(false) {
        out.signals.traded_true += 1;
    }
    for key in ["decision_trade", "execution_attempted", "traded"] {
        if v.get(key).is_none() {
            out.signals.missing_replay_fields += 1;
            break;
        }
    }
}

fn record_order_placed(out: &mut SessionDiagnostics, v: &Value) {
    out.orders.placed += 1;
    if missing_string(v, "intent_id") {
        out.orders.missing_intent_id += 1;
    }
    if missing_string(v, "state") {
        out.orders.placed_missing_state += 1;
    }
}

fn record_order_filled(out: &mut SessionDiagnostics, v: &Value) {
    out.orders.filled += 1;
    if missing_string(v, "intent_id") {
        out.orders.missing_intent_id += 1;
    }
}

fn record_resolution(out: &mut SessionDiagnostics, v: &Value) {
    out.resolutions.resolved += 1;
    if v.get("won").and_then(|x| x.as_bool()).unwrap_or(false) {
        out.resolutions.wins += 1;
    } else {
        out.resolutions.losses += 1;
    }
    out.resolutions.total_pnl += v.get("pnl").and_then(|x| x.as_f64()).unwrap_or(0.0);
}

fn record_oracle_resolution(out: &mut SessionDiagnostics, v: &Value) {
    out.oracle.checks += 1;
    if !v.get("agreed").and_then(|x| x.as_bool()).unwrap_or(true) {
        out.oracle.disagreements += 1;
    }
}

fn record_oracle_correction(out: &mut SessionDiagnostics, v: &Value) {
    out.oracle.corrections += 1;
    out.oracle.total_pnl_delta += v
        .get("pnl_delta")
        .and_then(|x| x.as_f64())
        .unwrap_or(0.0);
}

fn record_risk_state(out: &mut SessionDiagnostics, v: &Value) {
    out.risk.snapshots += 1;
    let bankroll = v.get("bankroll").and_then(|x| x.as_f64());
    let realized_pnl = v.get("realized_pnl").and_then(|x| x.as_f64());
    let wins = v.get("wins").and_then(|x| x.as_u64());
    let losses = v.get("losses").and_then(|x| x.as_u64());
    let positions = v.get("positions").and_then(|x| x.as_u64()).unwrap_or(0);

    if out.risk.first_bankroll.is_none() {
        out.risk.first_bankroll = bankroll;
        out.risk.first_realized_pnl = realized_pnl;
        out.risk.first_wins = wins;
        out.risk.first_losses = losses;
    }
    out.risk.last_bankroll = bankroll;
    out.risk.last_realized_pnl = realized_pnl;
    out.risk.last_wins = wins;
    out.risk.last_losses = losses;
    out.risk.max_positions = out.risk.max_positions.max(positions);
}

fn missing_string(v: &Value, key: &str) -> bool {
    v.get(key)
        .and_then(|x| x.as_str())
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
}

fn finalize(out: &mut SessionDiagnostics) {
    if !out.release_manifest_seen {
        out.warnings
            .push("missing system.release_manifest event".to_string());
    }
    if out.promotion_status.as_deref() == Some("invalid") {
        out.warnings
            .push("release manifest marks promotion artifact invalid".to_string());
    }
    if out.malformed_lines > 0 {
        out.warnings
            .push(format!("{} malformed JSONL line(s)", out.malformed_lines));
    }
    if out.signals.missing_replay_fields > 0 {
        out.warnings.push(format!(
            "{} signal evaluation(s) missing replay fields",
            out.signals.missing_replay_fields
        ));
    }
    if out.orders.missing_intent_id > 0 {
        out.warnings.push(format!(
            "{} order event(s) missing intent_id",
            out.orders.missing_intent_id
        ));
    }
    if out.orders.placed_missing_state > 0 {
        out.warnings.push(format!(
            "{} placed order event(s) missing state",
            out.orders.placed_missing_state
        ));
    }
    if out.orders.rejected > 0 {
        out.warnings
            .push(format!("{} rejected order event(s)", out.orders.rejected));
    }
    if out.resolutions.resolved > out.orders.filled {
        out.warnings.push(format!(
            "{} resolution event(s) but only {} filled order event(s); session may include restored paper state",
            out.resolutions.resolved, out.orders.filled
        ));
    }
    if out.oracle.disagreements > 0 {
        out.warnings.push(format!(
            "{} oracle disagreement(s) between local resolution and Polymarket",
            out.oracle.disagreements
        ));
    }
    if out
        .risk
        .first_realized_pnl
        .map(|pnl| pnl.abs() > 1e-9)
        .unwrap_or(false)
    {
        out.warnings.push(format!(
            "session starts with non-zero realized PnL {:.4}; state was not a clean baseline",
            out.risk.first_realized_pnl.unwrap_or(0.0)
        ));
    }
    let first_wins = out.risk.first_wins.unwrap_or(0);
    let first_losses = out.risk.first_losses.unwrap_or(0);
    if first_wins > 0 || first_losses > 0 {
        out.warnings.push(format!(
            "session starts with existing paper results wins={} losses={}; state was not a clean baseline",
            first_wins, first_losses
        ));
    }
    if out.system.fatal_errors > 0 {
        out.warnings
            .push(format!("{} fatal system error(s)", out.system.fatal_errors));
    }
    if out.signals.evaluations == 0 {
        out.warnings
            .push("no signal evaluations captured; diagnostic run may be too short".to_string());
    }

    out.ok = out.release_manifest_seen
        && out.malformed_lines == 0
        && out.signals.missing_replay_fields == 0
        && out.orders.missing_intent_id == 0
        && out.orders.placed_missing_state == 0
        && out.orders.rejected == 0
        && out.resolutions.resolved <= out.orders.filled
        && out.oracle.disagreements == 0
        && out
            .risk
            .first_realized_pnl
            .map(|pnl| pnl.abs() <= 1e-9)
            .unwrap_or(true)
        && first_wins == 0
        && first_losses == 0
        && out.system.fatal_errors == 0;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn session_diagnostics_accepts_current_schema() {
        let tmp = TempDir::new().unwrap();
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
                "cat": "signal",
                "type": "evaluation",
                "decision_trade": true,
                "execution_attempted": true,
                "traded": false
            }),
            serde_json::json!({
                "cat": "order",
                "type": "placed",
                "intent_id": "intent_1",
                "state": "acked"
            }),
            serde_json::json!({
                "cat": "order",
                "type": "filled",
                "intent_id": "intent_1"
            }),
        ];
        let payload = lines
            .into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, payload).unwrap();

        let diag = analyze_session(&path).unwrap();

        assert!(diag.ok, "{:?}", diag.warnings);
        assert_eq!(diag.mode.as_deref(), Some("paper"));
        assert_eq!(diag.promotion_strategy_hash.as_deref(), Some("strategy"));
        assert_eq!(diag.orders.placed, 1);
        assert_eq!(diag.signals.decision_trades, 1);
    }

    #[test]
    fn session_diagnostics_flags_restored_resolution_and_oracle_disagreement() {
        let tmp = TempDir::new().unwrap();
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
                "cat": "order",
                "type": "filled",
                "intent_id": "intent_1"
            }),
            serde_json::json!({
                "cat": "resolution",
                "type": "resolved",
                "won": true,
                "pnl": 1.0
            }),
            serde_json::json!({
                "cat": "resolution",
                "type": "resolved",
                "won": false,
                "pnl": -1.0
            }),
            serde_json::json!({
                "cat": "oracle",
                "type": "resolution",
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

        let diag = analyze_session(&path).unwrap();

        assert!(!diag.ok);
        assert_eq!(diag.resolutions.resolved, 2);
        assert_eq!(diag.oracle.disagreements, 1);
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("restored paper state")));
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("oracle disagreement")));
    }

    #[test]
    fn session_diagnostics_flags_nonzero_starting_risk_state() {
        let tmp = TempDir::new().unwrap();
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
                "cat": "risk",
                "type": "state",
                "bankroll": 125.0,
                "realized_pnl": 25.0,
                "wins": 2,
                "losses": 1,
                "positions": 0
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

        let diag = analyze_session(&path).unwrap();

        assert!(!diag.ok);
        assert_eq!(diag.risk.first_realized_pnl, Some(25.0));
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("non-zero realized PnL")));
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("existing paper results")));
    }

    #[test]
    fn session_diagnostics_flags_legacy_order_schema() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("session.jsonl");
        std::fs::write(
            &path,
            serde_json::json!({
                "cat": "order",
                "type": "placed",
                "order_id": "old"
            })
            .to_string(),
        )
        .unwrap();

        let diag = analyze_session(&path).unwrap();

        assert!(!diag.ok);
        assert_eq!(diag.orders.missing_intent_id, 1);
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("missing system.release_manifest")));
    }

    #[test]
    fn compare_sessions_requires_same_promotion_identity() {
        let tmp = TempDir::new().unwrap();
        let left = tmp.path().join("left.jsonl");
        let right = tmp.path().join("right.jsonl");
        let session = |hash: &str| {
            [
                serde_json::json!({
                    "cat": "system",
                    "type": "release_manifest",
                    "mode": "paper",
                    "promotion": {
                        "status": "ok",
                        "source_report_hash": "report",
                        "data_manifest_hash": "data",
                        "strategy": {"params_hash": hash}
                    }
                }),
                serde_json::json!({
                    "cat": "signal",
                    "type": "evaluation",
                    "decision_trade": false,
                    "execution_attempted": false,
                    "traded": false
                }),
            ]
            .into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n")
        };
        std::fs::write(&left, session("a")).unwrap();
        std::fs::write(&right, session("b")).unwrap();

        let comparison = compare_sessions(&left, &right).unwrap();

        assert!(!comparison.ok);
        assert!(comparison
            .mismatches
            .iter()
            .any(|m| m.contains("promotion strategy hash differs")));
    }
}
