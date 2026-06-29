//! Pure decision function for candle trading.
//!
//! Same logic used in live and backtest. Mirrors
//! `src/polymomentum/crypto/decision.py` for parity.

use serde::{Deserialize, Serialize};

use crate::fair_value::binary_option_price_with_rate;
use crate::strategy::momentum::MomentumSignal;

pub const DEFAULT_MIN_CONFIDENCE: f64 = 0.60;
pub const DEFAULT_MIN_EDGE: f64 = 0.07;
pub const DEFAULT_DEAD_ZONE_LO: f64 = 0.80;
pub const DEFAULT_DEAD_ZONE_HI: f64 = 0.90;
pub const DEFAULT_MIN_PRICE: f64 = 0.10;
pub const DEFAULT_MAX_PRICE: f64 = 0.90;
pub const DEFAULT_EDGE_CAP: f64 = 0.25;
pub const DEFAULT_SETTLEMENT_CUTOFF_MINUTES: f64 = 0.30;
pub const DEFAULT_SETTLEMENT_GUARD_MINUTES: f64 = 1.0;
pub const DEFAULT_SETTLEMENT_MIN_ABS_MOVE_USD: f64 = 10.0;
pub const DEFAULT_SETTLEMENT_SIGMA_BUFFER: f64 = 0.0;
pub const DEFAULT_MIN_REVERSION_COUNT: u64 = 0;
pub const DEFAULT_MAX_REVERSION_COUNT: u64 = u64::MAX;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleDecision {
    pub direction: String,
    pub confidence: f64,
    pub z_score: f64,
    pub zone: String,
    pub fair_value: f64,
    pub market_price: f64,
    pub edge: f64,
    pub minutes_remaining: f64,
    pub yes_no_vig: f64,
    #[serde(default)]
    pub regime: DecisionRegime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionRegime {
    pub zone: String,
    pub direction: String,
    pub price_bucket: String,
    pub edge_bucket: String,
    pub z_bucket: String,
    pub confidence_bucket: String,
    pub volatility_bucket: String,
    pub reversion_bucket: String,
    pub reversion_count: u32,
    pub minutes_remaining_bucket: String,
}

impl Default for DecisionRegime {
    fn default() -> Self {
        Self {
            zone: "unknown".to_string(),
            direction: "unknown".to_string(),
            price_bucket: "unknown".to_string(),
            edge_bucket: "unknown".to_string(),
            z_bucket: "unknown".to_string(),
            confidence_bucket: "unknown".to_string(),
            volatility_bucket: "unknown".to_string(),
            reversion_bucket: "unknown".to_string(),
            reversion_count: 0,
            minutes_remaining_bucket: "unknown".to_string(),
        }
    }
}

impl DecisionRegime {
    pub fn from_decision_inputs(
        zone: &str,
        signal: &MomentumSignal,
        market_price: f64,
        edge: f64,
        implied_vol: f64,
        minutes_remaining: f64,
    ) -> Self {
        Self {
            zone: zone.to_string(),
            direction: signal.direction.clone(),
            price_bucket: bucket_market_price(market_price),
            edge_bucket: bucket_edge(edge),
            z_bucket: bucket_z(signal.z_score),
            confidence_bucket: bucket_confidence(signal.confidence),
            volatility_bucket: bucket_implied_vol(implied_vol),
            reversion_bucket: bucket_reversions(signal.reversion_count),
            reversion_count: signal.reversion_count,
            minutes_remaining_bucket: bucket_minutes_remaining(minutes_remaining),
        }
    }

    pub fn key(&self) -> String {
        format!(
            "zone={}|dir={}|price={}|edge={}|z={}|conf={}|vol={}|rev={}|min={}",
            self.zone,
            self.direction,
            self.price_bucket,
            self.edge_bucket,
            self.z_bucket,
            self.confidence_bucket,
            self.volatility_bucket,
            self.reversion_bucket,
            self.minutes_remaining_bucket
        )
    }

    pub fn causal_tags(&self) -> Vec<(String, String)> {
        vec![
            ("regime".to_string(), self.key()),
            ("zone".to_string(), self.zone.clone()),
            ("direction".to_string(), self.direction.clone()),
            ("price".to_string(), self.price_bucket.clone()),
            ("edge".to_string(), self.edge_bucket.clone()),
            ("z".to_string(), self.z_bucket.clone()),
            ("confidence".to_string(), self.confidence_bucket.clone()),
            ("volatility".to_string(), self.volatility_bucket.clone()),
            ("reversion".to_string(), self.reversion_bucket.clone()),
            (
                "minutes_remaining".to_string(),
                self.minutes_remaining_bucket.clone(),
            ),
        ]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkipReason {
    pub reason: String,
    pub zone: String,
    pub detail: String,
}

impl SkipReason {
    pub fn new(reason: &str, zone: &str, detail: impl Into<String>) -> Self {
        Self {
            reason: reason.to_string(),
            zone: zone.to_string(),
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ZoneConfig {
    pub early_min_confidence: f64,
    pub early_min_z: f64,
    pub early_min_edge: f64,
    pub primary_min_z: f64,
    pub late_min_confidence: f64,
    pub late_min_z: f64,
    pub late_min_edge: f64,
    pub terminal_min_confidence: f64,
    pub terminal_min_z: f64,
    pub terminal_min_edge: f64,
    pub dead_zone_lo: f64,
    pub dead_zone_hi: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub edge_cap: f64,
    pub min_ev_buffer: f64,
    #[serde(
        default = "default_settlement_cutoff_minutes",
        skip_serializing_if = "is_default_settlement_cutoff_minutes"
    )]
    pub settlement_cutoff_minutes: f64,
    #[serde(default = "default_settlement_guard_minutes")]
    pub settlement_guard_minutes: f64,
    #[serde(default = "default_settlement_min_abs_move_usd")]
    pub settlement_min_abs_move_usd: f64,
    #[serde(default = "default_settlement_sigma_buffer")]
    pub settlement_sigma_buffer: f64,
    #[serde(
        default = "default_min_reversion_count",
        skip_serializing_if = "is_default_min_reversion_count"
    )]
    pub min_reversion_count: u64,
    #[serde(
        default = "default_max_reversion_count",
        skip_serializing_if = "is_default_max_reversion_count"
    )]
    pub max_reversion_count: u64,
}

fn default_settlement_guard_minutes() -> f64 {
    DEFAULT_SETTLEMENT_GUARD_MINUTES
}

fn default_settlement_cutoff_minutes() -> f64 {
    DEFAULT_SETTLEMENT_CUTOFF_MINUTES
}

fn is_default_settlement_cutoff_minutes(v: &f64) -> bool {
    (*v - DEFAULT_SETTLEMENT_CUTOFF_MINUTES).abs() <= f64::EPSILON
}

fn default_settlement_min_abs_move_usd() -> f64 {
    DEFAULT_SETTLEMENT_MIN_ABS_MOVE_USD
}

fn default_settlement_sigma_buffer() -> f64 {
    DEFAULT_SETTLEMENT_SIGMA_BUFFER
}

fn default_max_reversion_count() -> u64 {
    DEFAULT_MAX_REVERSION_COUNT
}

fn default_min_reversion_count() -> u64 {
    DEFAULT_MIN_REVERSION_COUNT
}

fn is_default_min_reversion_count(v: &u64) -> bool {
    *v == DEFAULT_MIN_REVERSION_COUNT
}

fn is_default_max_reversion_count(v: &u64) -> bool {
    *v == DEFAULT_MAX_REVERSION_COUNT
}

fn bucket_market_price(price: f64) -> String {
    if !price.is_finite() {
        "unknown".to_string()
    } else if price < 0.25 {
        "lt_0.25".to_string()
    } else if price < 0.50 {
        "0.25_0.50".to_string()
    } else if price < 0.75 {
        "0.50_0.75".to_string()
    } else if price < 0.90 {
        "0.75_0.90".to_string()
    } else {
        "gte_0.90".to_string()
    }
}

fn bucket_edge(edge: f64) -> String {
    if !edge.is_finite() {
        "unknown".to_string()
    } else if edge < 0.03 {
        "lt_0.03".to_string()
    } else if edge < 0.07 {
        "0.03_0.07".to_string()
    } else if edge < 0.15 {
        "0.07_0.15".to_string()
    } else {
        "gte_0.15".to_string()
    }
}

fn bucket_z(z: f64) -> String {
    let z = z.abs();
    if !z.is_finite() {
        "unknown".to_string()
    } else if z < 0.7 {
        "lt_0.7".to_string()
    } else if z < 1.1 {
        "0.7_1.1".to_string()
    } else if z < 1.5 {
        "1.1_1.5".to_string()
    } else {
        "gte_1.5".to_string()
    }
}

fn bucket_confidence(confidence: f64) -> String {
    if !confidence.is_finite() {
        "unknown".to_string()
    } else if confidence < 0.50 {
        "lt_0.50".to_string()
    } else if confidence < 0.70 {
        "0.50_0.70".to_string()
    } else if confidence < 0.85 {
        "0.70_0.85".to_string()
    } else {
        "gte_0.85".to_string()
    }
}

fn bucket_implied_vol(implied_vol: f64) -> String {
    if !implied_vol.is_finite() {
        "unknown".to_string()
    } else if implied_vol < 0.40 {
        "lt_0.40".to_string()
    } else if implied_vol < 0.80 {
        "0.40_0.80".to_string()
    } else if implied_vol < 1.20 {
        "0.80_1.20".to_string()
    } else {
        "gte_1.20".to_string()
    }
}

fn bucket_reversions(reversion_count: u32) -> String {
    match reversion_count {
        0 => "0".to_string(),
        1 | 2 => "1_2".to_string(),
        _ => "gte_3".to_string(),
    }
}

fn bucket_minutes_remaining(minutes_remaining: f64) -> String {
    if !minutes_remaining.is_finite() {
        "unknown".to_string()
    } else if minutes_remaining <= 1.0 {
        "lte_1".to_string()
    } else if minutes_remaining <= 2.0 {
        "1_2".to_string()
    } else if minutes_remaining <= 4.0 {
        "2_4".to_string()
    } else {
        "gt_4".to_string()
    }
}

impl Default for ZoneConfig {
    fn default() -> Self {
        Self {
            early_min_confidence: 0.55,
            early_min_z: 2.0,
            early_min_edge: 0.03,
            primary_min_z: 1.0,
            late_min_confidence: 0.65,
            late_min_z: 0.5,
            late_min_edge: 0.08,
            terminal_min_confidence: 0.55,
            terminal_min_z: 0.3,
            terminal_min_edge: 0.03,
            dead_zone_lo: DEFAULT_DEAD_ZONE_LO,
            dead_zone_hi: DEFAULT_DEAD_ZONE_HI,
            min_price: DEFAULT_MIN_PRICE,
            max_price: DEFAULT_MAX_PRICE,
            edge_cap: DEFAULT_EDGE_CAP,
            min_ev_buffer: 0.05,
            settlement_cutoff_minutes: DEFAULT_SETTLEMENT_CUTOFF_MINUTES,
            settlement_guard_minutes: DEFAULT_SETTLEMENT_GUARD_MINUTES,
            settlement_min_abs_move_usd: DEFAULT_SETTLEMENT_MIN_ABS_MOVE_USD,
            settlement_sigma_buffer: DEFAULT_SETTLEMENT_SIGMA_BUFFER,
            min_reversion_count: DEFAULT_MIN_REVERSION_COUNT,
            max_reversion_count: DEFAULT_MAX_REVERSION_COUNT,
        }
    }
}

impl ZoneConfig {
    pub fn from_settings(s: &crate::config::Settings) -> Self {
        Self {
            early_min_confidence: s.candle_zone_early_min_confidence,
            early_min_z: s.candle_zone_early_min_z,
            early_min_edge: s.candle_zone_early_min_edge,
            primary_min_z: s.candle_zone_primary_min_z,
            late_min_confidence: s.candle_zone_late_min_confidence,
            late_min_z: s.candle_zone_late_min_z,
            late_min_edge: s.candle_zone_late_min_edge,
            terminal_min_confidence: s.candle_zone_terminal_min_confidence,
            terminal_min_z: s.candle_zone_terminal_min_z,
            terminal_min_edge: s.candle_zone_terminal_min_edge,
            dead_zone_lo: s.candle_dead_zone_lo,
            dead_zone_hi: s.candle_dead_zone_hi,
            min_price: s.candle_min_price,
            max_price: s.candle_max_price,
            edge_cap: s.candle_edge_cap,
            min_ev_buffer: s.candle_min_ev_buffer,
            settlement_cutoff_minutes: s.candle_settlement_cutoff_minutes,
            settlement_guard_minutes: s.candle_settlement_guard_minutes,
            settlement_min_abs_move_usd: s.candle_settlement_min_abs_move_usd,
            settlement_sigma_buffer: s.candle_settlement_sigma_buffer,
            min_reversion_count: DEFAULT_MIN_REVERSION_COUNT,
            max_reversion_count: DEFAULT_MAX_REVERSION_COUNT,
        }
    }

    /// Apply runtime safety floors from settings without relaxing a promoted
    /// strategy artifact. This keeps ops able to tighten settlement-basis risk
    /// while preserving the artifact hash gate.
    pub fn apply_settings_safety_floor(&mut self, s: &crate::config::Settings) -> bool {
        let mut changed = false;

        changed |= tighten_min(
            &mut self.early_min_confidence,
            s.candle_runtime_min_confidence_floor,
        );
        changed |= tighten_min(
            &mut self.late_min_confidence,
            s.candle_runtime_min_confidence_floor,
        );
        changed |= tighten_min(
            &mut self.terminal_min_confidence,
            s.candle_runtime_min_confidence_floor,
        );
        changed |= tighten_min(&mut self.early_min_z, s.candle_runtime_min_z_floor);
        changed |= tighten_min(&mut self.primary_min_z, s.candle_runtime_min_z_floor);
        changed |= tighten_min(&mut self.late_min_z, s.candle_runtime_min_z_floor);
        changed |= tighten_min(&mut self.terminal_min_z, s.candle_runtime_min_z_floor);
        changed |= tighten_min(&mut self.early_min_edge, s.candle_runtime_min_edge_floor);
        changed |= tighten_min(&mut self.late_min_edge, s.candle_runtime_min_edge_floor);
        changed |= tighten_min(&mut self.terminal_min_edge, s.candle_runtime_min_edge_floor);
        changed |= tighten_min(
            &mut self.min_ev_buffer,
            s.candle_runtime_min_ev_buffer_floor,
        );
        changed |= tighten_min(&mut self.min_price, s.candle_runtime_min_price_floor);
        changed |= tighten_max(&mut self.max_price, s.candle_runtime_max_price_ceiling);
        if self.min_price > self.max_price {
            self.min_price = self.max_price;
            changed = true;
        }

        if s.candle_settlement_cutoff_minutes.is_finite()
            && s.candle_settlement_cutoff_minutes >= 0.0
            && self.settlement_cutoff_minutes < s.candle_settlement_cutoff_minutes
        {
            self.settlement_cutoff_minutes = s.candle_settlement_cutoff_minutes;
            changed = true;
        }
        if s.candle_settlement_guard_minutes.is_finite()
            && s.candle_settlement_guard_minutes >= 0.0
            && self.settlement_guard_minutes < s.candle_settlement_guard_minutes
        {
            self.settlement_guard_minutes = s.candle_settlement_guard_minutes;
            changed = true;
        }
        if s.candle_settlement_min_abs_move_usd.is_finite()
            && s.candle_settlement_min_abs_move_usd >= 0.0
            && self.settlement_min_abs_move_usd < s.candle_settlement_min_abs_move_usd
        {
            self.settlement_min_abs_move_usd = s.candle_settlement_min_abs_move_usd;
            changed = true;
        }
        if s.candle_settlement_sigma_buffer.is_finite()
            && s.candle_settlement_sigma_buffer >= 0.0
            && self.settlement_sigma_buffer < s.candle_settlement_sigma_buffer
        {
            self.settlement_sigma_buffer = s.candle_settlement_sigma_buffer;
            changed = true;
        }

        changed
    }
}

fn tighten_min(slot: &mut f64, floor: f64) -> bool {
    if floor.is_finite() && *slot < floor {
        *slot = floor;
        true
    } else {
        false
    }
}

fn tighten_max(slot: &mut f64, ceiling: f64) -> bool {
    if ceiling.is_finite() && ceiling >= 0.0 && *slot > ceiling {
        *slot = ceiling;
        true
    } else {
        false
    }
}

pub fn zone_for(elapsed_pct: f64) -> &'static str {
    if elapsed_pct < 0.40 {
        "early"
    } else if elapsed_pct < 0.80 {
        "primary"
    } else if elapsed_pct < 0.95 {
        "late"
    } else {
        "terminal"
    }
}

pub fn zone_thresholds(
    zone: &str,
    min_confidence: f64,
    min_edge: f64,
    cfg: &ZoneConfig,
) -> (f64, f64, f64) {
    match zone {
        "early" => (
            cfg.early_min_confidence,
            cfg.early_min_z,
            cfg.early_min_edge,
        ),
        "primary" => (min_confidence, cfg.primary_min_z, min_edge),
        "terminal" => (
            cfg.terminal_min_confidence,
            cfg.terminal_min_z,
            cfg.terminal_min_edge,
        ),
        _ => (
            cfg.late_min_confidence,
            cfg.late_min_z,
            min_edge.max(cfg.late_min_edge),
        ),
    }
}

fn remaining_sigma_usd(btc_price: f64, implied_vol: f64, minutes_remaining: f64) -> f64 {
    if btc_price <= 0.0
        || implied_vol <= 0.0
        || minutes_remaining <= 0.0
        || !btc_price.is_finite()
        || !implied_vol.is_finite()
        || !minutes_remaining.is_finite()
    {
        return 0.0;
    }
    let minutes_per_year = 365.0 * 24.0 * 60.0;
    btc_price * implied_vol * (minutes_remaining / minutes_per_year).sqrt()
}

pub fn settlement_guard_buffer_usd(
    cfg: &ZoneConfig,
    btc_price: f64,
    implied_vol: f64,
    minutes_remaining: f64,
) -> f64 {
    let sigma_buffer = cfg.settlement_sigma_buffer
        * remaining_sigma_usd(btc_price, implied_vol, minutes_remaining);
    cfg.settlement_min_abs_move_usd.max(sigma_buffer).max(0.0)
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum DecisionResult {
    Trade(CandleDecision),
    Skip(SkipReason),
}

#[allow(clippy::too_many_arguments)]
pub fn decide_candle_trade(
    signal: &MomentumSignal,
    minutes_elapsed: f64,
    minutes_remaining: f64,
    window_minutes: f64,
    up_price: f64,
    down_price: f64,
    btc_price: f64,
    open_btc: f64,
    implied_vol: f64,
    min_confidence: f64,
    min_edge: f64,
    skip_dead_zone: bool,
    zone_config: &ZoneConfig,
    cross_asset_boost: f64,
) -> DecisionResult {
    let cfg = zone_config;

    // 4-zone entry timing
    let elapsed_pct = if window_minutes > 0.0 {
        minutes_elapsed / window_minutes
    } else {
        1.0
    };
    let zone = zone_for(elapsed_pct);
    let (mut z_min_conf, mut z_min_z, z_min_edge) =
        zone_thresholds(zone, min_confidence, min_edge, cfg);

    if minutes_remaining <= cfg.settlement_cutoff_minutes {
        return DecisionResult::Skip(SkipReason::new(
            "settlement_cutoff",
            zone,
            format!(
                "{:.2} <= {:.2}",
                minutes_remaining, cfg.settlement_cutoff_minutes
            ),
        ));
    }

    if cfg.settlement_guard_minutes > 0.0
        && minutes_remaining <= cfg.settlement_guard_minutes
        && btc_price.is_finite()
        && open_btc.is_finite()
        && open_btc > 0.0
    {
        let threshold_distance = (btc_price - open_btc).abs();
        let required_distance =
            settlement_guard_buffer_usd(cfg, btc_price, implied_vol, minutes_remaining);
        if threshold_distance < required_distance {
            return DecisionResult::Skip(SkipReason::new(
                "settlement_margin",
                zone,
                format!("distance={threshold_distance:.2}<required={required_distance:.2}"),
            ));
        }
    }

    if cross_asset_boost > 0.0 {
        z_min_conf = (z_min_conf - cross_asset_boost).max(0.40);
        z_min_z = (z_min_z - cross_asset_boost).max(0.1);
    }

    if u64::from(signal.reversion_count) < cfg.min_reversion_count {
        return DecisionResult::Skip(SkipReason::new(
            "low_reversion_count",
            zone,
            format!("{} < {}", signal.reversion_count, cfg.min_reversion_count),
        ));
    }

    if u64::from(signal.reversion_count) > cfg.max_reversion_count {
        return DecisionResult::Skip(SkipReason::new(
            "high_reversion_count",
            zone,
            format!("{} > {}", signal.reversion_count, cfg.max_reversion_count),
        ));
    }

    if signal.confidence < z_min_conf {
        return DecisionResult::Skip(SkipReason::new(
            "low_confidence",
            zone,
            format!("{:.2} < {:.2}", signal.confidence, z_min_conf),
        ));
    }

    if signal.z_score < z_min_z {
        return DecisionResult::Skip(SkipReason::new(
            "low_z_score",
            zone,
            format!("{:.2} < {:.2}", signal.z_score, z_min_z),
        ));
    }

    if skip_dead_zone
        && signal.confidence >= cfg.dead_zone_lo
        && signal.confidence < cfg.dead_zone_hi
    {
        return DecisionResult::Skip(SkipReason::new("dead_zone_80_90", zone, ""));
    }

    let market_price = if signal.direction == "up" {
        up_price
    } else {
        down_price
    };

    if market_price < cfg.min_price || market_price > cfg.max_price {
        return DecisionResult::Skip(SkipReason::new(
            "price_out_of_range",
            zone,
            format!("{:.2}", market_price),
        ));
    }

    if signal.confidence < market_price + cfg.min_ev_buffer {
        return DecisionResult::Skip(SkipReason::new(
            "negative_ev",
            zone,
            format!(
                "conf={:.2}<price={:.2}+{:.2}",
                signal.confidence, market_price, cfg.min_ev_buffer
            ),
        ));
    }

    let yes_no_vig = up_price + down_price - 1.0;

    let days_remaining = minutes_remaining / 1440.0;
    let raw_fair =
        binary_option_price_with_rate(btc_price, open_btc, days_remaining, implied_vol, 0.05);
    let fair_value = if signal.direction == "up" {
        raw_fair
    } else {
        1.0 - raw_fair
    };
    let edge = fair_value - market_price;

    if zone != "terminal" && edge > cfg.edge_cap {
        return DecisionResult::Skip(SkipReason::new(
            "edge_too_high_stale",
            zone,
            format!("{:.2}", edge),
        ));
    }

    if edge < z_min_edge {
        return DecisionResult::Skip(SkipReason::new(
            "low_edge",
            zone,
            format!("{:.3} < {:.3}", edge, z_min_edge),
        ));
    }

    let regime = DecisionRegime::from_decision_inputs(
        zone,
        signal,
        market_price,
        edge,
        implied_vol,
        minutes_remaining,
    );

    DecisionResult::Trade(CandleDecision {
        direction: signal.direction.clone(),
        confidence: signal.confidence,
        z_score: signal.z_score,
        zone: zone.to_string(),
        fair_value,
        market_price,
        edge,
        minutes_remaining,
        yes_no_vig,
        regime,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_signal(confidence: f64, z: f64, direction: &str) -> MomentumSignal {
        MomentumSignal {
            direction: direction.to_string(),
            confidence,
            price_change: 100.0,
            price_change_pct: 0.001,
            consistency: 0.8,
            minutes_elapsed: 4.0,
            minutes_remaining: 1.0,
            current_price: 70_100.0,
            open_price: 70_000.0,
            z_score: z,
            reversion_count: 1,
        }
    }

    #[test]
    fn skips_low_confidence() {
        let sig = mk_signal(0.40, 1.5, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig,
            4.0,
            1.0,
            5.0,
            0.5,
            0.5,
            70_100.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            true,
            &cfg,
            0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "low_confidence"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn skips_dead_zone() {
        let sig = mk_signal(0.85, 1.5, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig,
            4.0,
            1.0,
            5.0,
            0.5,
            0.5,
            70_100.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            true,
            &cfg,
            0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "dead_zone_80_90"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn skips_price_out_of_range() {
        let sig = mk_signal(0.95, 1.5, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig,
            4.0,
            1.0,
            5.0,
            0.95,
            0.05,
            70_100.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            true,
            &cfg,
            0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "price_out_of_range"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn skips_settlement_cutoff() {
        // The terminal seconds are where local exchange mid and official
        // Polymarket settlement can disagree, so the shared decision path
        // refuses new entries there.
        let sig = mk_signal(0.75, 2.0, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig,
            4.95,
            0.05,
            5.0,
            0.30,
            0.70,
            70_500.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            true,
            &cfg,
            0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "settlement_cutoff"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn skips_inside_settlement_margin() {
        let sig = mk_signal(0.95, 2.0, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig,
            4.2,
            0.8,
            5.0,
            0.40,
            0.60,
            70_002.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            true,
            &cfg,
            0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "settlement_margin"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn skips_high_reversion_count() {
        let mut sig = mk_signal(0.75, 2.0, "up");
        sig.reversion_count = 3;
        let cfg = ZoneConfig {
            max_reversion_count: 2,
            ..ZoneConfig::default()
        };
        let r = decide_candle_trade(
            &sig,
            4.0,
            1.0,
            5.0,
            0.30,
            0.70,
            70_100.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            true,
            &cfg,
            0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "high_reversion_count"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn skips_low_reversion_count() {
        let mut sig = mk_signal(0.75, 2.0, "up");
        sig.reversion_count = 0;
        let cfg = ZoneConfig {
            min_reversion_count: 1,
            max_reversion_count: 2,
            ..ZoneConfig::default()
        };
        let r = decide_candle_trade(
            &sig,
            4.0,
            1.0,
            5.0,
            0.30,
            0.70,
            70_100.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            true,
            &cfg,
            0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "low_reversion_count"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn settlement_guard_uses_volatility_buffer() {
        let cfg = ZoneConfig {
            settlement_sigma_buffer: 0.15,
            ..ZoneConfig::default()
        };
        let low_vol = settlement_guard_buffer_usd(&cfg, 70_000.0, 0.10, 2.0);
        let high_vol = settlement_guard_buffer_usd(&cfg, 70_000.0, 0.80, 2.0);

        assert!(low_vol >= DEFAULT_SETTLEMENT_MIN_ABS_MOVE_USD);
        assert!(high_vol > low_vol);
    }

    #[test]
    fn settings_safety_floor_only_tightens_settlement_params() {
        let mut cfg = ZoneConfig {
            settlement_cutoff_minutes: 0.1,
            settlement_guard_minutes: 0.5,
            settlement_min_abs_move_usd: 2.0,
            settlement_sigma_buffer: 0.1,
            ..ZoneConfig::default()
        };
        let mut settings = crate::config::Settings::from_env();
        settings.candle_settlement_cutoff_minutes = 1.5;
        settings.candle_settlement_guard_minutes = 5.0;
        settings.candle_settlement_min_abs_move_usd = 25.0;
        settings.candle_settlement_sigma_buffer = 0.2;
        settings.candle_runtime_min_confidence_floor = 0.7;
        settings.candle_runtime_min_z_floor = 1.5;
        settings.candle_runtime_min_edge_floor = 0.09;
        settings.candle_runtime_min_ev_buffer_floor = 0.08;
        settings.candle_runtime_min_price_floor = 0.2;
        settings.candle_runtime_max_price_ceiling = 0.75;

        assert!(cfg.apply_settings_safety_floor(&settings));
        assert_eq!(cfg.early_min_confidence, 0.7);
        assert_eq!(cfg.late_min_confidence, 0.7);
        assert_eq!(cfg.terminal_min_confidence, 0.7);
        assert_eq!(cfg.early_min_z, 2.0);
        assert_eq!(cfg.primary_min_z, 1.5);
        assert_eq!(cfg.early_min_z, 2.0);
        assert_eq!(cfg.early_min_edge, 0.09);
        assert_eq!(cfg.late_min_edge, 0.09);
        assert_eq!(cfg.terminal_min_edge, 0.09);
        assert_eq!(cfg.min_ev_buffer, 0.08);
        assert_eq!(cfg.min_price, 0.2);
        assert_eq!(cfg.max_price, 0.75);
        assert_eq!(cfg.settlement_cutoff_minutes, 1.5);
        assert_eq!(cfg.settlement_guard_minutes, 5.0);
        assert_eq!(cfg.settlement_min_abs_move_usd, 25.0);
        assert_eq!(cfg.settlement_sigma_buffer, 0.2);

        settings.candle_settlement_cutoff_minutes = 0.3;
        settings.candle_settlement_guard_minutes = 1.0;
        settings.candle_settlement_min_abs_move_usd = 10.0;
        settings.candle_settlement_sigma_buffer = 0.0;
        settings.candle_runtime_min_confidence_floor = 0.5;
        settings.candle_runtime_min_z_floor = 0.5;
        settings.candle_runtime_min_edge_floor = 0.03;
        settings.candle_runtime_min_ev_buffer_floor = -1.0;
        settings.candle_runtime_min_price_floor = 0.1;
        settings.candle_runtime_max_price_ceiling = 0.9;

        assert!(!cfg.apply_settings_safety_floor(&settings));
        assert_eq!(cfg.early_min_confidence, 0.7);
        assert_eq!(cfg.primary_min_z, 1.5);
        assert_eq!(cfg.early_min_edge, 0.09);
        assert_eq!(cfg.min_ev_buffer, 0.08);
        assert_eq!(cfg.min_price, 0.2);
        assert_eq!(cfg.max_price, 0.75);
        assert_eq!(cfg.settlement_cutoff_minutes, 1.5);
        assert_eq!(cfg.settlement_guard_minutes, 5.0);
        assert_eq!(cfg.settlement_min_abs_move_usd, 25.0);
        assert_eq!(cfg.settlement_sigma_buffer, 0.2);
    }

    #[test]
    fn configurable_cutoff_skips_late_entries() {
        let sig = mk_signal(0.95, 2.0, "up");
        let cfg = ZoneConfig {
            settlement_cutoff_minutes: 1.5,
            ..ZoneConfig::default()
        };
        let r = decide_candle_trade(
            &sig,
            4.0,
            1.0,
            5.0,
            0.30,
            0.70,
            70_500.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            true,
            &cfg,
            0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "settlement_cutoff"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn rejects_negative_ev() {
        // confidence ~ market_price → negative_ev gate
        let sig = mk_signal(0.65, 1.5, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig,
            4.0,
            1.0,
            5.0,
            0.65,
            0.35,
            70_100.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            true,
            &cfg,
            0.0,
        );
        match r {
            DecisionResult::Skip(s) => assert_eq!(s.reason, "negative_ev"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn decision_regime_uses_only_pre_trade_inputs() {
        let sig = mk_signal(0.76, 1.3, "down");
        let regime = DecisionRegime::from_decision_inputs("terminal", &sig, 0.42, 0.09, 0.75, 0.8);

        assert_eq!(regime.zone, "terminal");
        assert_eq!(regime.direction, "down");
        assert_eq!(regime.price_bucket, "0.25_0.50");
        assert_eq!(regime.edge_bucket, "0.07_0.15");
        assert_eq!(regime.z_bucket, "1.1_1.5");
        assert_eq!(regime.confidence_bucket, "0.70_0.85");
        assert_eq!(regime.volatility_bucket, "0.40_0.80");
        assert_eq!(regime.reversion_bucket, "1_2");
        assert_eq!(regime.minutes_remaining_bucket, "lte_1");
        assert!(regime.key().contains("zone=terminal"));
    }
}
