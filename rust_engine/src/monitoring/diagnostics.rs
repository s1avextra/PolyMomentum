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
    pub skip_reasons: BTreeMap<String, u64>,
    pub decision_trades: u64,
    pub execution_attempted: u64,
    pub traded_true: u64,
    pub missing_replay_fields: u64,
    pub evals_with_book_spread: u64,
    pub evals_with_book_depth: u64,
    pub max_book_spread: Option<f64>,
    pub max_book_bid_depth: Option<f64>,
    pub max_book_ask_depth: Option<f64>,
    pub max_abs_book_pressure: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OrderDiagnostics {
    pub placed: u64,
    pub filled: u64,
    pub rejected: u64,
    pub missing_intent_id: u64,
    pub placed_missing_state: u64,
    pub submit_latency_samples: u64,
    pub avg_submit_latency_ms: Option<f64>,
    pub max_submit_latency_ms: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ResolutionDiagnostics {
    pub resolved: u64,
    pub wins: u64,
    pub losses: u64,
    pub total_pnl: f64,
    pub near_threshold: u64,
    pub min_abs_btc_move: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OracleDiagnostics {
    pub checks: u64,
    pub disagreements: u64,
    pub actionable_disagreements: u64,
    pub below_floor_disagreements: u64,
    pub ties: u64,
    pub corrections: u64,
    pub total_pnl_delta: f64,
    pub first_disagreements: Vec<String>,
    pub disagreement_min_abs_move: Option<f64>,
    pub disagreement_max_abs_move: Option<f64>,
    pub actionable_disagreement_min_abs_move: Option<f64>,
    pub actionable_disagreement_max_abs_move: Option<f64>,
    pub below_floor_disagreement_min_abs_move: Option<f64>,
    pub below_floor_disagreement_max_abs_move: Option<f64>,
    pub tie_min_abs_move: Option<f64>,
    pub tie_max_abs_move: Option<f64>,
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
    pub breaker_events: u64,
    pub breaker_tripped: bool,
    pub last_breaker_state: Option<String>,
    pub last_breaker_reason: Option<String>,
    pub last_breaker_peak_pnl: Option<f64>,
    pub last_breaker_open_exposure: Option<f64>,
    pub last_breaker_stressed_pnl: Option<f64>,
    pub last_breaker_realized_drawdown: Option<f64>,
    pub last_breaker_realized_drawdown_pct: Option<f64>,
    pub last_breaker_stressed_drawdown: Option<f64>,
    pub last_breaker_stressed_drawdown_pct: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SystemDiagnostics {
    pub errors: u64,
    pub fatal_errors: u64,
    pub first_errors: Vec<String>,
    pub runtime_strategy_seen: bool,
    pub runtime_strategy_source: Option<String>,
    pub runtime_strategy_hash: Option<String>,
    pub runtime_strategy_risk_profile: Option<String>,
    pub settlement_alignment_ready: Option<bool>,
    pub settlement_cutoff_minutes: Option<f64>,
    pub settlement_guard_minutes: Option<f64>,
    pub settlement_min_abs_move_usd: Option<f64>,
    pub settlement_sigma_buffer: Option<f64>,
    pub microstructure_max_spread: Option<f64>,
    pub microstructure_min_book_depth: Option<f64>,
    pub microstructure_min_book_pressure: Option<f64>,
    pub cycle_samples: u64,
    pub avg_cycle_ms: Option<f64>,
    pub max_cycle_ms: Option<f64>,
    pub price_snapshots: u64,
    pub avg_price_staleness_ms: Option<f64>,
    pub max_price_staleness_ms: Option<f64>,
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
            ("system", "runtime_strategy") => record_runtime_strategy(&mut out, &v),
            ("system", "cycle") => record_system_cycle(&mut out, &v),
            ("system", "error") => record_system_error(&mut out, &v),
            ("price", "snapshot") => record_price_snapshot(&mut out, &v),
            ("signal", "evaluation") => record_signal_evaluation(&mut out, &v),
            ("signal", "skip") => record_signal_skip(&mut out, &v),
            ("order", "placed") => record_order_placed(&mut out, &v),
            ("order", "filled") => record_order_filled(&mut out, &v),
            ("order", "rejected") => out.orders.rejected += 1,
            ("resolution", "resolved") => record_resolution(&mut out, &v),
            ("oracle", "resolution") => record_oracle_resolution(&mut out, &v),
            ("oracle", "correction") => record_oracle_correction(&mut out, &v),
            ("risk", "state") => record_risk_state(&mut out, &v),
            ("risk", "breaker") => record_breaker_state(&mut out, &v),
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
    if left.system.runtime_strategy_hash.is_some()
        && right.system.runtime_strategy_hash.is_some()
        && left.system.runtime_strategy_hash != right.system.runtime_strategy_hash
    {
        mismatches.push("runtime strategy hash differs".to_string());
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

fn record_runtime_strategy(out: &mut SessionDiagnostics, v: &Value) {
    out.system.runtime_strategy_seen = true;
    out.system.runtime_strategy_source = v
        .get("source")
        .and_then(|x| x.as_str())
        .map(ToString::to_string);
    out.system.runtime_strategy_hash = v
        .get("strategy")
        .and_then(|s| s.get("params_hash"))
        .and_then(|x| x.as_str())
        .map(ToString::to_string);
    out.system.runtime_strategy_risk_profile = v
        .get("strategy")
        .and_then(|s| s.get("risk_profile"))
        .and_then(|x| x.as_str())
        .map(ToString::to_string);
    out.system.settlement_alignment_ready = v
        .get("settlement_alignment_ready")
        .and_then(|x| x.as_bool());
    out.system.settlement_cutoff_minutes =
        v.get("settlement_cutoff_minutes").and_then(|x| x.as_f64());
    out.system.settlement_guard_minutes =
        v.get("settlement_guard_minutes").and_then(|x| x.as_f64());
    out.system.settlement_min_abs_move_usd = v
        .get("settlement_min_abs_move_usd")
        .and_then(|x| x.as_f64());
    out.system.settlement_sigma_buffer = v.get("settlement_sigma_buffer").and_then(|x| x.as_f64());
    let micro = v.get("microstructure");
    out.system.microstructure_max_spread = micro
        .and_then(|m| m.get("max_spread"))
        .and_then(|x| x.as_f64());
    out.system.microstructure_min_book_depth = micro
        .and_then(|m| m.get("min_book_depth"))
        .and_then(|x| x.as_f64());
    out.system.microstructure_min_book_pressure = micro
        .and_then(|m| m.get("min_book_pressure"))
        .and_then(|x| x.as_f64());
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

fn record_signal_skip(out: &mut SessionDiagnostics, v: &Value) {
    out.signals.skips += 1;
    let reason = v
        .get("reason")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    *out.signals.skip_reasons.entry(reason).or_insert(0) += 1;
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
    if let Some(spread) = finite_f64(v, "book_spread") {
        update_max(&mut out.signals.max_book_spread, spread.abs());
        if spread.abs() > 0.0 {
            out.signals.evals_with_book_spread += 1;
        }
    }
    let bid_depth = finite_f64(v, "book_bid_depth").unwrap_or(0.0).max(0.0);
    let ask_depth = finite_f64(v, "book_ask_depth").unwrap_or(0.0).max(0.0);
    update_max(&mut out.signals.max_book_bid_depth, bid_depth);
    update_max(&mut out.signals.max_book_ask_depth, ask_depth);
    if bid_depth > 0.0 && ask_depth > 0.0 {
        out.signals.evals_with_book_depth += 1;
    }
    if let Some(pressure) = finite_f64(v, "book_pressure") {
        update_max(&mut out.signals.max_abs_book_pressure, pressure.abs());
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
    if let Some(latency_ms) = finite_f64(v, "submit_latency_ms") {
        out.orders.submit_latency_samples += 1;
        update_avg(
            &mut out.orders.avg_submit_latency_ms,
            out.orders.submit_latency_samples,
            latency_ms.max(0.0),
        );
        update_max(&mut out.orders.max_submit_latency_ms, latency_ms.max(0.0));
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
    if let Some(btc_move) = v.get("btc_move").and_then(|x| x.as_f64()) {
        let abs_move = btc_move.abs();
        out.resolutions.min_abs_btc_move = Some(
            out.resolutions
                .min_abs_btc_move
                .map(|existing| existing.min(abs_move))
                .unwrap_or(abs_move),
        );
        if abs_move < SETTLEMENT_BASIS_WARN_BTC {
            out.resolutions.near_threshold += 1;
        }
    }
}

fn record_oracle_resolution(out: &mut SessionDiagnostics, v: &Value) {
    out.oracle.checks += 1;
    let abs_move = oracle_abs_move(v);
    if !v.get("agreed").and_then(|x| x.as_bool()).unwrap_or(true) {
        out.oracle.disagreements += 1;
        let below_floor = match (abs_move, out.system.settlement_min_abs_move_usd) {
            (Some(abs_move), Some(floor)) if floor > 0.0 => abs_move < floor,
            _ => false,
        };
        if let Some(abs_move) = abs_move {
            update_min(&mut out.oracle.disagreement_min_abs_move, abs_move);
            update_max(&mut out.oracle.disagreement_max_abs_move, abs_move);
            if below_floor {
                update_min(
                    &mut out.oracle.below_floor_disagreement_min_abs_move,
                    abs_move,
                );
                update_max(
                    &mut out.oracle.below_floor_disagreement_max_abs_move,
                    abs_move,
                );
            } else {
                update_min(
                    &mut out.oracle.actionable_disagreement_min_abs_move,
                    abs_move,
                );
                update_max(
                    &mut out.oracle.actionable_disagreement_max_abs_move,
                    abs_move,
                );
            }
        }
        if below_floor {
            out.oracle.below_floor_disagreements += 1;
        } else {
            out.oracle.actionable_disagreements += 1;
        }
        if out.oracle.first_disagreements.len() < 10 {
            let cid = v.get("cid").and_then(|x| x.as_str()).unwrap_or("unknown");
            let ours = v
                .get("our_actual")
                .and_then(|x| x.as_str())
                .unwrap_or("unknown");
            let polymarket = v
                .get("polymarket_actual")
                .and_then(|x| x.as_str())
                .unwrap_or("unknown");
            let move_detail = abs_move
                .map(|m| format!(" abs_move={m:.2}"))
                .unwrap_or_default();
            let class_detail = if below_floor {
                " class=below_settlement_floor"
            } else {
                " class=actionable"
            };
            out.oracle.first_disagreements.push(format!(
                "{cid}: ours={ours} polymarket={polymarket}{move_detail}{class_detail}"
            ));
        }
    }
    if v.get("polymarket_actual")
        .and_then(|x| x.as_str())
        .map(|s| s == "tie")
        .unwrap_or(false)
    {
        out.oracle.ties += 1;
        if let Some(abs_move) = abs_move {
            update_min(&mut out.oracle.tie_min_abs_move, abs_move);
            update_max(&mut out.oracle.tie_max_abs_move, abs_move);
        }
    }
}

fn oracle_abs_move(v: &Value) -> Option<f64> {
    let open = v.get("our_open_btc").and_then(|x| x.as_f64())?;
    let close = v.get("our_close_btc").and_then(|x| x.as_f64())?;
    Some((close - open).abs())
}

fn update_min(slot: &mut Option<f64>, value: f64) {
    *slot = Some(slot.map(|existing| existing.min(value)).unwrap_or(value));
}

fn update_max(slot: &mut Option<f64>, value: f64) {
    *slot = Some(slot.map(|existing| existing.max(value)).unwrap_or(value));
}

fn record_oracle_correction(out: &mut SessionDiagnostics, v: &Value) {
    out.oracle.corrections += 1;
    out.oracle.total_pnl_delta += v.get("pnl_delta").and_then(|x| x.as_f64()).unwrap_or(0.0);
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

fn record_breaker_state(out: &mut SessionDiagnostics, v: &Value) {
    out.risk.breaker_events += 1;
    let state = v
        .get("state")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    let reason = v
        .get("reason")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    if matches!(state.as_str(), "tripped" | "restored_tripped") {
        out.risk.breaker_tripped = true;
    }
    out.risk.last_breaker_state = Some(state);
    out.risk.last_breaker_reason = Some(reason);
    out.risk.last_breaker_peak_pnl = v.get("peak_pnl").and_then(|x| x.as_f64());
    out.risk.last_breaker_open_exposure = v.get("open_exposure").and_then(|x| x.as_f64());
    out.risk.last_breaker_stressed_pnl = v.get("stressed_pnl").and_then(|x| x.as_f64());
    out.risk.last_breaker_realized_drawdown = v.get("realized_drawdown").and_then(|x| x.as_f64());
    out.risk.last_breaker_realized_drawdown_pct =
        v.get("realized_drawdown_pct").and_then(|x| x.as_f64());
    out.risk.last_breaker_stressed_drawdown = v.get("stressed_drawdown").and_then(|x| x.as_f64());
    out.risk.last_breaker_stressed_drawdown_pct =
        v.get("stressed_drawdown_pct").and_then(|x| x.as_f64());
}

fn missing_string(v: &Value, key: &str) -> bool {
    v.get(key)
        .and_then(|x| x.as_str())
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
}

fn finite_f64(v: &Value, key: &str) -> Option<f64> {
    let value = v.get(key).and_then(|x| x.as_f64())?;
    value.is_finite().then_some(value)
}

fn update_avg(avg: &mut Option<f64>, samples: u64, value: f64) {
    if samples == 0 {
        return;
    }
    *avg = Some(match *avg {
        Some(current) => current + (value - current) / samples as f64,
        None => value,
    });
}

fn record_system_cycle(out: &mut SessionDiagnostics, v: &Value) {
    if let Some(cycle_ms) = finite_f64(v, "cycle_ms") {
        out.system.cycle_samples += 1;
        update_avg(
            &mut out.system.avg_cycle_ms,
            out.system.cycle_samples,
            cycle_ms.max(0.0),
        );
        update_max(&mut out.system.max_cycle_ms, cycle_ms.max(0.0));
    }
}

fn record_price_snapshot(out: &mut SessionDiagnostics, v: &Value) {
    out.system.price_snapshots += 1;
    if let Some(staleness_ms) = finite_f64(v, "staleness_ms") {
        update_avg(
            &mut out.system.avg_price_staleness_ms,
            out.system.price_snapshots,
            staleness_ms.max(0.0),
        );
        update_max(
            &mut out.system.max_price_staleness_ms,
            staleness_ms.max(0.0),
        );
    }
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
    if out.oracle.actionable_disagreements > 0 {
        let move_range = oracle_move_range(
            out.oracle.actionable_disagreement_min_abs_move,
            out.oracle.actionable_disagreement_max_abs_move,
        );
        out.warnings.push(format!(
            "{} actionable oracle disagreement(s) between local resolution and Polymarket{}",
            out.oracle.actionable_disagreements, move_range
        ));
    }
    if out.oracle.below_floor_disagreements > 0 {
        let move_range = oracle_move_range(
            out.oracle.below_floor_disagreement_min_abs_move,
            out.oracle.below_floor_disagreement_max_abs_move,
        );
        let floor = out
            .system
            .settlement_min_abs_move_usd
            .map(|v| format!(" below configured ${v:.2} settlement floor"))
            .unwrap_or_else(|| " below configured settlement floor".to_string());
        out.warnings.push(format!(
            "{} below-floor oracle disagreement(s){}{}; excluded from executable settlement gate",
            out.oracle.below_floor_disagreements, move_range, floor
        ));
    }
    let unresolved_oracle_disagreements = out
        .oracle
        .actionable_disagreements
        .saturating_sub(out.oracle.corrections);
    if unresolved_oracle_disagreements > 0 {
        out.warnings.push(format!(
            "{} actionable oracle disagreement(s) have no recorded PnL correction",
            unresolved_oracle_disagreements
        ));
    }
    if out.oracle.ties > 0 {
        let move_range =
            oracle_move_range(out.oracle.tie_min_abs_move, out.oracle.tie_max_abs_move);
        out.warnings.push(format!(
            "{} Polymarket tie resolution(s){}; tie risk must be investigated before live promotion",
            out.oracle.ties, move_range
        ));
    }
    if out.system.settlement_alignment_ready == Some(false) {
        out.warnings.push(
            "settlement alignment is not verified; runtime is settlement-shadow gated".to_string(),
        );
    }
    if out.resolutions.near_threshold > 0 {
        out.warnings.push(format!(
            "{} resolution(s) within ${:.2} BTC of the candle threshold; settlement-basis risk is elevated",
            out.resolutions.near_threshold, SETTLEMENT_BASIS_WARN_BTC
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
    if out.risk.breaker_tripped {
        let reason = out.risk.last_breaker_reason.as_deref().unwrap_or("unknown");
        out.warnings.push(format!(
            "circuit breaker is tripped (reason={reason}); no new paper/live trades will be evaluated"
        ));
    }
    if out.signals.evaluations == 0 {
        if out.risk.breaker_tripped {
            out.warnings.push(
                "no signal evaluations captured because the circuit breaker is tripped".to_string(),
            );
        } else {
            out.warnings.push(
                "no signal evaluations captured; diagnostic run may be too short".to_string(),
            );
        }
    } else if out.signals.evals_with_book_spread == 0 {
        out.warnings.push(
            "no signal evaluation carried non-zero book spread; CLOB feed health is unproven"
                .to_string(),
        );
    }
    if let Some(max_cycle_ms) = out.system.max_cycle_ms {
        if max_cycle_ms > 100.0 {
            out.warnings.push(format!(
                "max decision-cycle latency {:.1}ms exceeds 100ms loop budget",
                max_cycle_ms
            ));
        }
    }
    if let Some(max_staleness_ms) = out.system.max_price_staleness_ms {
        if max_staleness_ms > 2000.0 {
            out.warnings.push(format!(
                "max price-feed staleness {:.1}ms exceeds 2000ms threshold",
                max_staleness_ms
            ));
        }
    }
    if let Some(max_submit_latency_ms) = out.orders.max_submit_latency_ms {
        if max_submit_latency_ms > 1000.0 {
            out.warnings.push(format!(
                "max order-submit latency {:.1}ms exceeds 1000ms threshold",
                max_submit_latency_ms
            ));
        }
    }

    out.ok = out.release_manifest_seen
        && out.malformed_lines == 0
        && out.signals.missing_replay_fields == 0
        && out.orders.missing_intent_id == 0
        && out.orders.placed_missing_state == 0
        && out.orders.rejected == 0
        && out.resolutions.resolved <= out.orders.filled
        && unresolved_oracle_disagreements == 0
        && out.oracle.ties == 0
        && !out.risk.breaker_tripped
        && out.system.fatal_errors == 0;
}

const SETTLEMENT_BASIS_WARN_BTC: f64 = 5.0;

fn oracle_move_range(min: Option<f64>, max: Option<f64>) -> String {
    match (min, max) {
        (Some(min), Some(max)) if (max - min).abs() <= f64::EPSILON => {
            format!(" at local |BTC move| ${min:.2}")
        }
        (Some(min), Some(max)) => format!(" at local |BTC move| range ${min:.2}-${max:.2}"),
        _ => String::new(),
    }
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
                "cat": "system",
                "type": "runtime_strategy",
                "source": "promotion:/tmp/promotion.json+settlement_floor",
                "strategy": {
                    "name": "candle_momentum",
                    "version": "1",
                    "params_hash": "runtime_strategy",
                    "risk_profile": "test-risk"
                },
                "settlement_alignment_ready": false,
                "settlement_cutoff_minutes": 1.5,
                "settlement_guard_minutes": 5.0,
                "settlement_min_abs_move_usd": 25.0,
                "settlement_sigma_buffer": 0.2
            }),
            serde_json::json!({
                "cat": "signal",
                "type": "evaluation",
                "decision_trade": true,
                "execution_attempted": true,
                "traded": false
            }),
            serde_json::json!({
                "cat": "system",
                "type": "cycle",
                "cycle_ms": 7.5,
                "contracts": 42
            }),
            serde_json::json!({
                "cat": "price",
                "type": "snapshot",
                "staleness_ms": 123.4
            }),
            serde_json::json!({
                "cat": "order",
                "type": "placed",
                "intent_id": "intent_1",
                "state": "acked",
                "submit_latency_ms": 12.5
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
        assert_eq!(
            diag.system.runtime_strategy_hash.as_deref(),
            Some("runtime_strategy")
        );
        assert_eq!(diag.system.settlement_cutoff_minutes, Some(1.5));
        assert_eq!(diag.system.settlement_guard_minutes, Some(5.0));
        assert_eq!(diag.system.settlement_alignment_ready, Some(false));
        assert_eq!(diag.orders.placed, 1);
        assert_eq!(diag.orders.max_submit_latency_ms, Some(12.5));
        assert_eq!(diag.system.max_cycle_ms, Some(7.5));
        assert_eq!(diag.system.max_price_staleness_ms, Some(123.4));
        assert_eq!(diag.signals.decision_trades, 1);
    }

    #[test]
    fn session_diagnostics_summarizes_signal_feed_health() {
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
                "type": "skip",
                "reason": "low_edge_fast"
            }),
            serde_json::json!({
                "cat": "signal",
                "type": "skip",
                "reason": "low_edge_fast"
            }),
            serde_json::json!({
                "cat": "signal",
                "type": "skip",
                "reason": "settlement_alignment_unverified_fast"
            }),
            serde_json::json!({
                "cat": "signal",
                "type": "evaluation",
                "decision_trade": false,
                "execution_attempted": false,
                "traded": false,
                "book_spread": 0.03,
                "book_pressure": -0.25,
                "book_bid_depth": 120.5,
                "book_ask_depth": 90.25
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
        assert_eq!(diag.signals.skips, 3);
        assert_eq!(
            diag.signals.skip_reasons.get("low_edge_fast").copied(),
            Some(2)
        );
        assert_eq!(
            diag.signals
                .skip_reasons
                .get("settlement_alignment_unverified_fast")
                .copied(),
            Some(1)
        );
        assert_eq!(diag.signals.evals_with_book_spread, 1);
        assert_eq!(diag.signals.evals_with_book_depth, 1);
        assert_eq!(diag.signals.max_book_spread, Some(0.03));
        assert_eq!(diag.signals.max_book_bid_depth, Some(120.5));
        assert_eq!(diag.signals.max_book_ask_depth, Some(90.25));
        assert_eq!(diag.signals.max_abs_book_pressure, Some(0.25));
        assert!(!diag.warnings.iter().any(|w| w.contains("CLOB feed health")));
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
                "cid": "0xabc",
                "our_actual": "up",
                "polymarket_actual": "down",
                "our_open_btc": 100000.0,
                "our_close_btc": 100028.64,
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
        assert_eq!(
            diag.oracle.first_disagreements,
            vec!["0xabc: ours=up polymarket=down abs_move=28.64 class=actionable"]
        );
        assert_eq!(diag.oracle.actionable_disagreements, 1);
        assert_eq!(diag.oracle.below_floor_disagreements, 0);
        assert!((diag.oracle.disagreement_min_abs_move.unwrap() - 28.64).abs() < 1e-9);
        assert!((diag.oracle.disagreement_max_abs_move.unwrap() - 28.64).abs() < 1e-9);
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("restored paper state")));
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("oracle disagreement")));
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("no recorded PnL correction")));
    }

    #[test]
    fn session_diagnostics_excludes_below_floor_oracle_disagreement_from_gate() {
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
                "cat": "system",
                "type": "runtime_strategy",
                "source": "promotion:/tmp/promotion.json+settlement_floor",
                "strategy": {
                    "name": "candle_momentum",
                    "version": "1",
                    "params_hash": "runtime_strategy",
                    "risk_profile": "test-risk"
                },
                "settlement_alignment_ready": false,
                "settlement_cutoff_minutes": 1.5,
                "settlement_guard_minutes": 5.0,
                "settlement_min_abs_move_usd": 25.0,
                "settlement_sigma_buffer": 0.2
            }),
            serde_json::json!({
                "cat": "oracle",
                "type": "resolution",
                "cid": "0xsmall",
                "our_actual": "up",
                "polymarket_actual": "down",
                "our_open_btc": 100000.0,
                "our_close_btc": 100011.61,
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

        assert!(diag.ok, "{:?}", diag.warnings);
        assert_eq!(diag.oracle.disagreements, 1);
        assert_eq!(diag.oracle.actionable_disagreements, 0);
        assert_eq!(diag.oracle.below_floor_disagreements, 1);
        assert_eq!(
            diag.oracle.first_disagreements,
            vec!["0xsmall: ours=up polymarket=down abs_move=11.61 class=below_settlement_floor"]
        );
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("below-floor oracle disagreement")));
        assert!(!diag
            .warnings
            .iter()
            .any(|w| w.contains("actionable oracle disagreement")));
    }

    #[test]
    fn session_diagnostics_warns_on_nonzero_starting_risk_state() {
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

        assert!(diag.ok);
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
    fn session_diagnostics_flags_breaker_and_tie_resolution() {
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
                "type": "breaker",
                "state": "tripped",
                "reason": "oracle_tie",
                "wins": 0,
                "losses": 1,
                "realized_pnl": -7.5
            }),
            serde_json::json!({
                "cat": "oracle",
                "type": "resolution",
                "our_open_btc": 100000.0,
                "our_close_btc": 100028.64,
                "polymarket_actual": "tie",
                "agreed": false
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
        assert_eq!(diag.oracle.ties, 1);
        assert!((diag.oracle.tie_min_abs_move.unwrap() - 28.64).abs() < 1e-9);
        assert!((diag.oracle.tie_max_abs_move.unwrap() - 28.64).abs() < 1e-9);
        assert!(diag.risk.breaker_tripped);
        assert_eq!(diag.risk.last_breaker_reason.as_deref(), Some("oracle_tie"));
        assert!(diag.warnings.iter().any(|w| w.contains("tie resolution")));
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("circuit breaker is tripped")));
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("because the circuit breaker is tripped")));
    }

    #[test]
    fn session_diagnostics_flags_tight_settlement_basis() {
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
            serde_json::json!({
                "cat": "resolution",
                "type": "resolved",
                "won": true,
                "pnl": 1.0,
                "btc_move": 3.39
            }),
        ];
        let payload = lines
            .into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, payload).unwrap();

        let diag = analyze_session(&path).unwrap();

        assert!(diag.ok);
        assert_eq!(diag.resolutions.near_threshold, 1);
        assert_eq!(diag.resolutions.min_abs_btc_move, Some(3.39));
        assert!(diag
            .warnings
            .iter()
            .any(|w| w.contains("settlement-basis risk")));
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
