//! Causality audit for session JSONL logs.
//!
//! This gate is intentionally mechanical: every executable order must prove
//! that signal source time <= decision time <= order time <= fill time <
//! market end, and every recorded resolution must occur after market end.

use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct CausalityAuditConfig {
    pub max_clock_skew_s: f64,
    pub max_post_end_fill_s: f64,
    pub min_order_timings: u64,
    pub min_resolution_timings: u64,
}

impl Default for CausalityAuditConfig {
    fn default() -> Self {
        Self {
            max_clock_skew_s: 0.5,
            max_post_end_fill_s: 0.0,
            min_order_timings: 0,
            min_resolution_timings: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CausalityAudit {
    pub schema_version: u32,
    pub path: String,
    pub ok: bool,
    pub config: CausalityAuditConfig,
    pub total_events: u64,
    pub malformed_lines: u64,
    pub order_timings: u64,
    pub order_placed: u64,
    pub order_filled: u64,
    pub resolution_timings: u64,
    pub missing_timing_for_fills: u64,
    pub violations: Vec<CausalityViolation>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CausalityViolation {
    pub code: String,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u64>,
}

#[derive(Debug, Clone)]
struct OrderTiming {
    intent_id: String,
    condition_id: String,
    signal_source_ts_s: f64,
    decision_ts_s: f64,
    order_ts_s: f64,
    market_start_ts_s: f64,
    market_end_ts_s: f64,
    line: u64,
}

#[derive(Debug, Clone)]
struct FillEvent {
    intent_id: String,
    fill_time_s: f64,
    line: u64,
}

pub fn audit_session(
    path: impl AsRef<Path>,
    config: CausalityAuditConfig,
) -> Result<CausalityAudit> {
    let path = path.as_ref();
    let file = std::fs::File::open(path)
        .with_context(|| format!("open session log {}", path.display()))?;
    let reader = std::io::BufReader::new(file);

    let mut out = CausalityAudit {
        schema_version: 1,
        path: path.display().to_string(),
        ok: false,
        config,
        total_events: 0,
        malformed_lines: 0,
        order_timings: 0,
        order_placed: 0,
        order_filled: 0,
        resolution_timings: 0,
        missing_timing_for_fills: 0,
        violations: Vec::new(),
        warnings: Vec::new(),
    };
    let mut order_timings: BTreeMap<String, OrderTiming> = BTreeMap::new();
    let mut fills = Vec::new();

    for (idx, line) in reader.lines().enumerate() {
        let line_no = (idx + 1) as u64;
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
        match (cat, ty) {
            ("causality", "order_timing") => {
                out.order_timings += 1;
                match parse_order_timing(&v, line_no) {
                    Ok(timing) => {
                        validate_order_timing(&mut out, &timing);
                        if order_timings
                            .insert(timing.intent_id.clone(), timing.clone())
                            .is_some()
                        {
                            violation(
                                &mut out,
                                "duplicate_order_timing",
                                "duplicate causality.order_timing for intent",
                                Some(timing.intent_id),
                                Some(timing.condition_id),
                                Some(line_no),
                            );
                        }
                    }
                    Err(detail) => violation(
                        &mut out,
                        "invalid_order_timing",
                        detail,
                        string_field(&v, "intent_id"),
                        string_field(&v, "condition_id"),
                        Some(line_no),
                    ),
                }
            }
            ("causality", "resolution_timing") => {
                out.resolution_timings += 1;
                validate_resolution_timing(&mut out, &v, line_no);
            }
            ("order", "placed") => {
                out.order_placed += 1;
                if string_field(&v, "intent_id").is_none() {
                    violation(
                        &mut out,
                        "order_placed_missing_intent",
                        "order.placed event has no intent_id",
                        None,
                        None,
                        Some(line_no),
                    );
                }
            }
            ("order", "filled") => {
                out.order_filled += 1;
                match parse_fill_event(&v, line_no) {
                    Ok(fill) => fills.push(fill),
                    Err(detail) => violation(
                        &mut out,
                        "invalid_fill_event",
                        detail,
                        string_field(&v, "intent_id"),
                        None,
                        Some(line_no),
                    ),
                }
            }
            _ => {}
        }
    }

    if out.malformed_lines > 0 {
        let detail = format!("{} malformed JSONL line(s)", out.malformed_lines);
        violation(&mut out, "malformed_lines", detail, None, None, None);
    }
    if out.order_placed > 0 && out.order_timings == 0 {
        violation(
            &mut out,
            "missing_order_timing",
            "session placed orders but recorded no causality.order_timing events",
            None,
            None,
            None,
        );
    }
    if out.order_timings < out.config.min_order_timings {
        let detail = format!(
            "order_timings {} below minimum {}",
            out.order_timings, out.config.min_order_timings
        );
        violation(
            &mut out,
            "insufficient_order_timings",
            detail,
            None,
            None,
            None,
        );
    }
    if out.resolution_timings < out.config.min_resolution_timings {
        let detail = format!(
            "resolution_timings {} below minimum {}",
            out.resolution_timings, out.config.min_resolution_timings
        );
        violation(
            &mut out,
            "insufficient_resolution_timings",
            detail,
            None,
            None,
            None,
        );
    }

    for fill in fills {
        match order_timings.get(&fill.intent_id) {
            Some(timing) => validate_fill(&mut out, &fill, timing),
            None => {
                out.missing_timing_for_fills += 1;
                violation(
                    &mut out,
                    "fill_missing_order_timing",
                    "order.filled has no matching causality.order_timing",
                    Some(fill.intent_id),
                    None,
                    Some(fill.line),
                );
            }
        }
    }

    if out.total_events == 0 {
        out.warnings.push("session log is empty".to_string());
    }
    if out.order_placed == 0 && out.order_filled == 0 {
        out.warnings.push(
            "no executable orders in session; causality audit only checked resolutions".to_string(),
        );
    }
    out.ok = out.violations.is_empty();
    Ok(out)
}

fn parse_order_timing(v: &Value, line: u64) -> std::result::Result<OrderTiming, String> {
    let timing = OrderTiming {
        intent_id: required_string(v, "intent_id")?,
        condition_id: required_string(v, "condition_id")?,
        signal_source_ts_s: required_f64(v, "signal_source_ts_s")?,
        decision_ts_s: required_f64(v, "decision_ts_s")?,
        order_ts_s: required_f64(v, "order_ts_s")?,
        market_start_ts_s: required_f64(v, "market_start_ts_s")?,
        market_end_ts_s: required_f64(v, "market_end_ts_s")?,
        line,
    };
    if timing.intent_id.is_empty() {
        return Err("intent_id is empty".to_string());
    }
    if timing.condition_id.is_empty() {
        return Err("condition_id is empty".to_string());
    }
    Ok(timing)
}

fn parse_fill_event(v: &Value, line: u64) -> std::result::Result<FillEvent, String> {
    Ok(FillEvent {
        intent_id: required_string(v, "intent_id")?,
        fill_time_s: required_f64(v, "fill_time_s")?,
        line,
    })
}

fn validate_order_timing(out: &mut CausalityAudit, timing: &OrderTiming) {
    let skew = out.config.max_clock_skew_s;
    if timing.market_start_ts_s > timing.market_end_ts_s {
        violation(
            out,
            "market_window_inverted",
            format!(
                "market_start_ts_s {:.6} after market_end_ts_s {:.6}",
                timing.market_start_ts_s, timing.market_end_ts_s
            ),
            Some(timing.intent_id.clone()),
            Some(timing.condition_id.clone()),
            Some(timing.line),
        );
    }
    if timing.signal_source_ts_s > timing.decision_ts_s + skew {
        violation(
            out,
            "future_signal_source",
            format!(
                "signal_source_ts_s {:.6} after decision_ts_s {:.6}",
                timing.signal_source_ts_s, timing.decision_ts_s
            ),
            Some(timing.intent_id.clone()),
            Some(timing.condition_id.clone()),
            Some(timing.line),
        );
    }
    if timing.decision_ts_s + skew < timing.market_start_ts_s {
        violation(
            out,
            "decision_before_market_start",
            format!(
                "decision_ts_s {:.6} before market_start_ts_s {:.6}",
                timing.decision_ts_s, timing.market_start_ts_s
            ),
            Some(timing.intent_id.clone()),
            Some(timing.condition_id.clone()),
            Some(timing.line),
        );
    }
    if timing.decision_ts_s - skew >= timing.market_end_ts_s {
        violation(
            out,
            "decision_after_market_end",
            format!(
                "decision_ts_s {:.6} at/after market_end_ts_s {:.6}",
                timing.decision_ts_s, timing.market_end_ts_s
            ),
            Some(timing.intent_id.clone()),
            Some(timing.condition_id.clone()),
            Some(timing.line),
        );
    }
    if timing.order_ts_s + skew < timing.decision_ts_s {
        violation(
            out,
            "order_before_decision",
            format!(
                "order_ts_s {:.6} before decision_ts_s {:.6}",
                timing.order_ts_s, timing.decision_ts_s
            ),
            Some(timing.intent_id.clone()),
            Some(timing.condition_id.clone()),
            Some(timing.line),
        );
    }
    if timing.order_ts_s - skew >= timing.market_end_ts_s {
        violation(
            out,
            "order_after_market_end",
            format!(
                "order_ts_s {:.6} at/after market_end_ts_s {:.6}",
                timing.order_ts_s, timing.market_end_ts_s
            ),
            Some(timing.intent_id.clone()),
            Some(timing.condition_id.clone()),
            Some(timing.line),
        );
    }
}

fn validate_fill(out: &mut CausalityAudit, fill: &FillEvent, timing: &OrderTiming) {
    let skew = out.config.max_clock_skew_s;
    if fill.fill_time_s + skew < 0.0 {
        violation(
            out,
            "negative_fill_time",
            format!("fill_time_s {:.6} is negative", fill.fill_time_s),
            Some(fill.intent_id.clone()),
            Some(timing.condition_id.clone()),
            Some(fill.line),
        );
        return;
    }
    let fill_ts = timing.order_ts_s + fill.fill_time_s;
    if fill_ts + skew < timing.order_ts_s {
        violation(
            out,
            "fill_before_order",
            format!(
                "fill_ts_s {:.6} before order_ts_s {:.6}",
                fill_ts, timing.order_ts_s
            ),
            Some(fill.intent_id.clone()),
            Some(timing.condition_id.clone()),
            Some(fill.line),
        );
    }
    if fill_ts > timing.market_end_ts_s + out.config.max_post_end_fill_s + skew {
        violation(
            out,
            "fill_after_market_end",
            format!(
                "fill_ts_s {:.6} after market_end_ts_s {:.6}",
                fill_ts, timing.market_end_ts_s
            ),
            Some(fill.intent_id.clone()),
            Some(timing.condition_id.clone()),
            Some(fill.line),
        );
    }
}

fn validate_resolution_timing(out: &mut CausalityAudit, v: &Value, line: u64) {
    let condition_id = string_field(v, "condition_id");
    let resolution_ts_s = match required_f64(v, "resolution_ts_s") {
        Ok(ts) => ts,
        Err(detail) => {
            violation(
                out,
                "invalid_resolution_timing",
                detail,
                None,
                condition_id,
                Some(line),
            );
            return;
        }
    };
    let market_end_ts_s = match required_f64(v, "market_end_ts_s") {
        Ok(ts) => ts,
        Err(detail) => {
            violation(
                out,
                "invalid_resolution_timing",
                detail,
                None,
                condition_id,
                Some(line),
            );
            return;
        }
    };
    if resolution_ts_s + out.config.max_clock_skew_s < market_end_ts_s {
        violation(
            out,
            "resolution_before_market_end",
            format!(
                "resolution_ts_s {:.6} before market_end_ts_s {:.6}",
                resolution_ts_s, market_end_ts_s
            ),
            None,
            condition_id,
            Some(line),
        );
    }
}

fn required_string(v: &Value, key: &str) -> std::result::Result<String, String> {
    v.get(key)
        .and_then(|x| x.as_str())
        .map(str::to_string)
        .ok_or_else(|| format!("missing or non-string {key}"))
}

fn required_f64(v: &Value, key: &str) -> std::result::Result<f64, String> {
    let x = v
        .get(key)
        .and_then(|x| x.as_f64())
        .ok_or_else(|| format!("missing or non-number {key}"))?;
    if !x.is_finite() {
        return Err(format!("{key} is not finite"));
    }
    Ok(x)
}

fn string_field(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(str::to_string)
}

fn violation(
    out: &mut CausalityAudit,
    code: impl Into<String>,
    detail: impl Into<String>,
    intent_id: Option<String>,
    condition_id: Option<String>,
    line: Option<u64>,
) {
    out.violations.push(CausalityViolation {
        code: code.into(),
        detail: detail.into(),
        intent_id,
        condition_id,
        line,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn write_session(lines: Vec<Value>) -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("session.jsonl");
        let payload = lines
            .into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, payload).unwrap();
        (tmp, path)
    }

    fn order_timing() -> Value {
        json!({
            "cat": "causality",
            "type": "order_timing",
            "intent_id": "intent_1",
            "condition_id": "0xabc",
            "token_id": "token",
            "signal_source_ts_s": 100.0,
            "decision_ts_s": 100.0,
            "order_ts_s": 100.05,
            "market_start_ts_s": 0.0,
            "market_end_ts_s": 300.0
        })
    }

    #[test]
    fn accepts_causal_order_fill_and_resolution() {
        let (_tmp, path) = write_session(vec![
            order_timing(),
            json!({
                "cat": "order",
                "type": "placed",
                "intent_id": "intent_1"
            }),
            json!({
                "cat": "order",
                "type": "filled",
                "intent_id": "intent_1",
                "fill_time_s": 0.05
            }),
            json!({
                "cat": "causality",
                "type": "resolution_timing",
                "condition_id": "0xabc",
                "market_end_ts_s": 300.0,
                "resolution_ts_s": 301.0
            }),
        ]);

        let audit = audit_session(path, CausalityAuditConfig::default()).unwrap();

        assert!(audit.ok, "{:?}", audit.violations);
        assert_eq!(audit.order_timings, 1);
        assert_eq!(audit.order_filled, 1);
    }

    #[test]
    fn rejects_future_signal_source() {
        let mut timing = order_timing();
        timing["signal_source_ts_s"] = json!(101.0);
        let (_tmp, path) = write_session(vec![timing]);

        let audit = audit_session(path, CausalityAuditConfig::default()).unwrap();

        assert!(!audit.ok);
        assert!(audit
            .violations
            .iter()
            .any(|v| v.code == "future_signal_source"));
    }

    #[test]
    fn rejects_fill_after_market_end() {
        let (_tmp, path) = write_session(vec![
            order_timing(),
            json!({
                "cat": "order",
                "type": "filled",
                "intent_id": "intent_1",
                "fill_time_s": 250.0
            }),
        ]);

        let audit = audit_session(path, CausalityAuditConfig::default()).unwrap();

        assert!(!audit.ok);
        assert!(audit
            .violations
            .iter()
            .any(|v| v.code == "fill_after_market_end"));
    }

    #[test]
    fn rejects_filled_order_without_timing() {
        let (_tmp, path) = write_session(vec![json!({
            "cat": "order",
            "type": "filled",
            "intent_id": "intent_1",
            "fill_time_s": 0.0
        })]);

        let audit = audit_session(path, CausalityAuditConfig::default()).unwrap();

        assert!(!audit.ok);
        assert_eq!(audit.missing_timing_for_fills, 1);
    }
}
