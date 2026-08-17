//! Offline parity validation for recorded runtime decision events.

use std::io::BufRead;
use std::path::Path;

use anyhow::{Context, Result};

use crate::backtest::experiment::PromotionArtifact;
use crate::backtest::strategies::{SelectivityFilter, StrategyVariant};
use crate::strategy::decision::{
    decide_candle_trade, DecisionResult, ZoneConfig, DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_EDGE,
};
use crate::strategy::momentum::MomentumSignal;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplayValidationSummary {
    pub total: u64,
    pub mismatches: u64,
    pub mismatch_pct: f64,
}

/// Re-evaluate every recorded signal event using the runtime strategy settings
/// embedded in the session log.
pub fn validate_replay(path: impl AsRef<Path>) -> Result<ReplayValidationSummary> {
    let path = path.as_ref();
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = std::io::BufReader::new(file);
    let mut total = 0u64;
    let mut mismatches = 0u64;
    let mut validation = ReplayValidationConfig::default();

    for (line_number, line) in reader.lines().enumerate() {
        let line = line.with_context(|| {
            format!(
                "read {} line {}",
                path.display(),
                line_number.saturating_add(1)
            )
        })?;
        let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        if is_event(&event, "system", "runtime_strategy") {
            validation.apply_runtime_strategy_event(&event);
            continue;
        }
        if !is_event(&event, "signal", "evaluation") {
            continue;
        }

        total += 1;
        let signal = signal_from_event(&event);
        let decision = decide_candle_trade(
            &signal,
            signal.minutes_elapsed,
            signal.minutes_remaining,
            signal.minutes_elapsed + signal.minutes_remaining,
            f64_field(&event, "up_price").unwrap_or(0.5),
            f64_field(&event, "down_price").unwrap_or(0.5),
            signal.current_price,
            signal.open_price,
            f64_field(&event, "implied_vol").unwrap_or(0.5),
            validation.min_confidence,
            validation.min_edge,
            validation.skip_dead_zone,
            &validation.zone_config,
            f64_field(&event, "cross_boost").unwrap_or(0.0),
        );
        let traded = match decision {
            DecisionResult::Trade(decision) => validation
                .selectivity
                .reject_reason(&decision.regime)
                .is_none(),
            DecisionResult::Skip(_) => false,
        };
        let expected_trade = traded && validation.settlement_alignment_ready;
        let logged_trade = event
            .get("decision_trade")
            .and_then(serde_json::Value::as_bool)
            .or_else(|| event.get("traded").and_then(serde_json::Value::as_bool))
            .unwrap_or(false);
        mismatches += u64::from(expected_trade != logged_trade);
    }

    let mismatch_pct = if total == 0 {
        0.0
    } else {
        100.0 * mismatches as f64 / total as f64
    };
    Ok(ReplayValidationSummary {
        total,
        mismatches,
        mismatch_pct,
    })
}

fn is_event(event: &serde_json::Value, category: &str, event_type: &str) -> bool {
    event.get("cat").and_then(serde_json::Value::as_str) == Some(category)
        && event.get("type").and_then(serde_json::Value::as_str) == Some(event_type)
}

fn signal_from_event(event: &serde_json::Value) -> MomentumSignal {
    MomentumSignal {
        direction: event
            .get("dir")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("up")
            .to_string(),
        confidence: f64_field(event, "conf").unwrap_or(0.0),
        price_change: f64_field(event, "chg").unwrap_or(0.0),
        price_change_pct: f64_field(event, "chg_pct").unwrap_or(0.0),
        consistency: f64_field(event, "cons").unwrap_or(0.0),
        minutes_elapsed: f64_field(event, "elapsed_min").unwrap_or(0.0),
        minutes_remaining: f64_field(event, "remaining_min").unwrap_or(0.0),
        current_price: f64_field(event, "px").unwrap_or(0.0),
        open_price: f64_field(event, "open").unwrap_or(0.0),
        z_score: f64_field(event, "z").unwrap_or(0.0),
        reversion_count: u32_field(event, "reversion_count").unwrap_or(0),
        directional_impulse_10s_bps: f64_field(event, "directional_impulse_10s_bps"),
        article_path_2m: event
            .get("article_path_2m")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        article_path_3m: event
            .get("article_path_3m")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        article_path_4m: event
            .get("article_path_4m")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        article_move_2m_usd: f64_field(event, "article_move_2m_usd"),
    }
}

#[derive(Debug, Clone)]
struct ReplayValidationConfig {
    zone_config: ZoneConfig,
    min_confidence: f64,
    min_edge: f64,
    skip_dead_zone: bool,
    selectivity: SelectivityFilter,
    settlement_alignment_ready: bool,
}

impl Default for ReplayValidationConfig {
    fn default() -> Self {
        Self {
            zone_config: ZoneConfig::default(),
            min_confidence: DEFAULT_MIN_CONFIDENCE,
            min_edge: DEFAULT_MIN_EDGE,
            skip_dead_zone: true,
            selectivity: SelectivityFilter::default(),
            settlement_alignment_ready: true,
        }
    }
}

impl ReplayValidationConfig {
    fn apply_runtime_strategy_event(&mut self, event: &serde_json::Value) {
        if let Some(source) = event.get("source").and_then(serde_json::Value::as_str) {
            if let Some(path) = promotion_path_from_runtime_source(source) {
                if let Some(variant) = load_promotion_variant(path) {
                    self.apply_variant(variant);
                }
            }
        }
        if let Some(zone_config) = event.get("zone_config") {
            if let Ok(config) = serde_json::from_value::<ZoneConfig>(zone_config.clone()) {
                self.zone_config = config;
            }
        }
        if let Some(value) = f64_field(event, "settlement_cutoff_minutes") {
            self.zone_config.settlement_cutoff_minutes = value;
        }
        if let Some(value) = f64_field(event, "settlement_guard_minutes") {
            self.zone_config.settlement_guard_minutes = value;
        }
        if let Some(value) = f64_field(event, "settlement_min_abs_move_usd") {
            self.zone_config.settlement_min_abs_move_usd = value;
        }
        if let Some(value) = f64_field(event, "settlement_sigma_buffer") {
            self.zone_config.settlement_sigma_buffer = value;
        }
        if let Some(value) = f64_field(event, "min_confidence") {
            self.min_confidence = value;
        }
        if let Some(value) = f64_field(event, "min_edge") {
            self.min_edge = value;
        }
        if let Some(value) = event
            .get("skip_dead_zone")
            .and_then(serde_json::Value::as_bool)
        {
            self.skip_dead_zone = value;
        }
        if let Some(selectivity) = event.get("selectivity") {
            if let Ok(filter) = serde_json::from_value::<SelectivityFilter>(selectivity.clone()) {
                self.selectivity = filter;
            }
        }
        if let Some(value) = event
            .get("settlement_alignment_ready")
            .and_then(serde_json::Value::as_bool)
        {
            self.settlement_alignment_ready = value;
        }
    }

    fn apply_variant(&mut self, variant: StrategyVariant) {
        self.zone_config = variant.zone_config;
        self.min_confidence = variant.min_confidence;
        self.min_edge = variant.min_edge;
        self.skip_dead_zone = variant.skip_dead_zone;
        self.selectivity = variant.selectivity;
    }
}

fn promotion_path_from_runtime_source(source: &str) -> Option<&str> {
    let path = source.strip_prefix("promotion:")?;
    Some(path.split('+').next().unwrap_or(path))
}

fn load_promotion_variant(path: &str) -> Option<StrategyVariant> {
    let text = std::fs::read_to_string(path).ok()?;
    let artifact: PromotionArtifact = serde_json::from_str(&text).ok()?;
    serde_json::from_value(artifact.strategy_params).ok()
}

fn f64_field(event: &serde_json::Value, key: &str) -> Option<f64> {
    event.get(key).and_then(serde_json::Value::as_f64)
}

fn u32_field(event: &serde_json::Value, key: &str) -> Option<u32> {
    event
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_reports_logged_decision_drift() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("session.jsonl");
        std::fs::write(
            &path,
            concat!(
                "not-json\n",
                "{\"cat\":\"signal\",\"type\":\"evaluation\",\"conf\":0.0,\"traded\":false}\n",
                "{\"cat\":\"signal\",\"type\":\"evaluation\",\"conf\":0.0,\"traded\":true}\n"
            ),
        )
        .unwrap();

        let summary = validate_replay(&path).unwrap();

        assert_eq!(summary.total, 2);
        assert_eq!(summary.mismatches, 1);
        assert_eq!(summary.mismatch_pct, 50.0);
    }

    #[test]
    fn runtime_event_updates_inline_strategy() {
        let zone_config = ZoneConfig {
            min_ev_buffer: 0.12,
            settlement_min_abs_move_usd: 25.0,
            ..ZoneConfig::default()
        };
        let event = serde_json::json!({
            "zone_config": zone_config,
            "min_confidence": 0.42,
            "min_edge": 0.03,
            "skip_dead_zone": false,
            "settlement_alignment_ready": false
        });
        let mut config = ReplayValidationConfig::default();

        config.apply_runtime_strategy_event(&event);

        assert_eq!(config.zone_config.min_ev_buffer, 0.12);
        assert_eq!(config.zone_config.settlement_min_abs_move_usd, 25.0);
        assert_eq!(config.min_confidence, 0.42);
        assert_eq!(config.min_edge, 0.03);
        assert!(!config.skip_dead_zone);
        assert!(!config.settlement_alignment_ready);
    }

    #[test]
    fn promotion_source_path_ignores_suffix_flags() {
        assert_eq!(
            promotion_path_from_runtime_source("promotion:/tmp/promotion.json+settlement_floor"),
            Some("/tmp/promotion.json")
        );
        assert_eq!(promotion_path_from_runtime_source("settings"), None);
    }

    #[test]
    fn logged_reversion_count_is_bounded_to_u32() {
        let event = serde_json::json!({ "reversion_count": 3 });
        assert_eq!(u32_field(&event, "reversion_count"), Some(3));

        let too_large = serde_json::json!({ "reversion_count": u64::from(u32::MAX) + 1 });
        assert_eq!(u32_field(&too_large, "reversion_count"), None);
        assert_eq!(u32_field(&serde_json::json!({}), "reversion_count"), None);
    }
}
