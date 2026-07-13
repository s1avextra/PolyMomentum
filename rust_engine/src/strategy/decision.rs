//! Pure decision function for candle trading.
//!
//! Same logic used in live and backtest. Mirrors
//! `src/polymomentum/crypto/decision.py` for parity.

use serde::{Deserialize, Serialize};

use crate::data::models::DEFAULT_CRYPTO_TAKER_FEE_RATE;
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
    #[serde(default)]
    pub gross_edge: f64,
    #[serde(default)]
    pub entry_fee_per_share: f64,
    /// Expected edge after entry fee, before settlement payoff.
    pub edge: f64,
    pub minutes_remaining: f64,
    pub yes_no_vig: f64,
    #[serde(default)]
    pub regime: DecisionRegime,
}

impl CandleDecision {
    /// Revalidate a taker decision at the visible-depth VWAP and FOK limit.
    /// This keeps top-of-book signals from passing gates that their executable
    /// order would fail after walking the book.
    pub fn reprice_for_taker_execution(
        &mut self,
        execution_vwap: f64,
        worst_price: f64,
        entry_fee_rate: f64,
        min_edge: f64,
        cfg: &ZoneConfig,
    ) -> Result<(), SkipReason> {
        if !(execution_vwap.is_finite()
            && worst_price.is_finite()
            && entry_fee_rate.is_finite()
            && min_edge.is_finite()
            && execution_vwap > 0.0
            && execution_vwap <= 1.0
            && worst_price + 1e-12 >= execution_vwap
            && worst_price <= 1.0
            && entry_fee_rate >= 0.0)
        {
            return Err(SkipReason::new(
                "invalid_execution_quote",
                &self.zone,
                format!("vwap={execution_vwap} worst={worst_price} fee_rate={entry_fee_rate}"),
            ));
        }
        if execution_vwap < cfg.min_price || worst_price > cfg.max_price {
            return Err(SkipReason::new(
                "execution_price_out_of_range",
                &self.zone,
                format!(
                    "vwap={execution_vwap:.4} worst={worst_price:.4} range=[{:.4},{:.4}]",
                    cfg.min_price, cfg.max_price
                ),
            ));
        }

        let gross_edge = self.fair_value - execution_vwap;
        let entry_fee_per_share = entry_fee_rate * execution_vwap * (1.0 - execution_vwap);
        let edge = gross_edge - entry_fee_per_share;
        if edge < cfg.min_ev_buffer {
            return Err(SkipReason::new(
                "execution_negative_ev",
                &self.zone,
                format!("{edge:.4} < {:.4}", cfg.min_ev_buffer),
            ));
        }
        if self.zone != "terminal" && gross_edge > cfg.edge_cap {
            return Err(SkipReason::new(
                "execution_edge_too_high_stale",
                &self.zone,
                format!("{gross_edge:.4} > {:.4}", cfg.edge_cap),
            ));
        }
        let (_, _, required_edge) = zone_thresholds(&self.zone, 0.0, min_edge, cfg);
        if edge < required_edge {
            return Err(SkipReason::new(
                "execution_low_edge",
                &self.zone,
                format!("{edge:.4} < {required_edge:.4}"),
            ));
        }

        self.market_price = execution_vwap;
        self.gross_edge = gross_edge;
        self.entry_fee_per_share = entry_fee_per_share;
        self.edge = edge;
        self.regime.price_bucket = bucket_market_price(execution_vwap);
        self.regime.edge_bucket = bucket_edge(edge);
        Ok(())
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utc_hour_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utc_session_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub book_spread_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub book_min_depth_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub book_pressure_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub book_imbalance_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bookwalk_slippage_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub book_age_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub book_runup_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directional_impulse_10s_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_overround_bucket: Option<String>,
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
            utc_hour_bucket: None,
            utc_session_bucket: None,
            book_spread_bucket: None,
            book_min_depth_bucket: None,
            book_pressure_bucket: None,
            book_imbalance_bucket: None,
            bookwalk_slippage_bucket: None,
            book_age_bucket: None,
            book_runup_bucket: None,
            directional_impulse_10s_bucket: None,
            outcome_overround_bucket: None,
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
            utc_hour_bucket: None,
            utc_session_bucket: None,
            book_spread_bucket: None,
            book_min_depth_bucket: None,
            book_pressure_bucket: None,
            book_imbalance_bucket: None,
            bookwalk_slippage_bucket: None,
            book_age_bucket: None,
            book_runup_bucket: None,
            directional_impulse_10s_bucket: signal
                .directional_impulse_10s_bps
                .map(bucket_directional_impulse_bps),
            outcome_overround_bucket: None,
        }
    }

    pub fn key(&self) -> String {
        let mut parts = vec![
            format!("zone={}", self.zone),
            format!("dir={}", self.direction),
            format!("price={}", self.price_bucket),
            format!("edge={}", self.edge_bucket),
            format!("z={}", self.z_bucket),
            format!("conf={}", self.confidence_bucket),
            format!("vol={}", self.volatility_bucket),
            format!("rev={}", self.reversion_bucket),
            format!("min={}", self.minutes_remaining_bucket),
        ];
        if let Some(bucket) = &self.utc_session_bucket {
            parts.push(format!("utc_session={bucket}"));
            parts.push(format!(
                "direction_utc_session={}_{}",
                self.direction, bucket
            ));
        }
        if let Some(bucket) = &self.utc_hour_bucket {
            parts.push(format!("utc_hour={bucket}"));
            parts.push(format!("direction_utc_hour={}_{}", self.direction, bucket));
        }
        if let Some(bucket) = &self.book_spread_bucket {
            parts.push(format!("book_spread={bucket}"));
        }
        if let Some(bucket) = &self.book_min_depth_bucket {
            parts.push(format!("book_min_depth={bucket}"));
        }
        if let Some(bucket) = &self.book_pressure_bucket {
            parts.push(format!("book_pressure={bucket}"));
        }
        if let Some(bucket) = &self.book_imbalance_bucket {
            parts.push(format!("book_imbalance={bucket}"));
        }
        if let Some(bucket) = &self.bookwalk_slippage_bucket {
            parts.push(format!("bookwalk_slippage={bucket}"));
        }
        if let Some(bucket) = &self.book_age_bucket {
            parts.push(format!("book_age={bucket}"));
        }
        if let Some(bucket) = &self.book_runup_bucket {
            parts.push(format!("book_runup={bucket}"));
        }
        if let Some(bucket) = &self.directional_impulse_10s_bucket {
            parts.push(format!("btc_impulse_10s={bucket}"));
        }
        if let Some(bucket) = &self.outcome_overround_bucket {
            parts.push(format!("outcome_overround={bucket}"));
        }
        parts.join("|")
    }

    pub fn causal_tags(&self) -> Vec<(String, String)> {
        let mut tags = vec![
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
        ];
        if let Some(bucket) = &self.utc_session_bucket {
            tags.push(("utc_session".to_string(), bucket.clone()));
            tags.push((
                "direction_utc_session".to_string(),
                format!("{}_{}", self.direction, bucket),
            ));
        }
        if let Some(bucket) = &self.utc_hour_bucket {
            tags.push(("utc_hour".to_string(), bucket.clone()));
            tags.push((
                "direction_utc_hour".to_string(),
                format!("{}_{}", self.direction, bucket),
            ));
        }
        if let Some(bucket) = &self.book_spread_bucket {
            tags.push(("book_spread".to_string(), bucket.clone()));
        }
        if let Some(bucket) = &self.book_min_depth_bucket {
            tags.push(("book_min_depth".to_string(), bucket.clone()));
        }
        if let Some(bucket) = &self.book_pressure_bucket {
            tags.push(("book_pressure".to_string(), bucket.clone()));
        }
        if let Some(bucket) = &self.book_imbalance_bucket {
            tags.push(("book_imbalance".to_string(), bucket.clone()));
        }
        if let Some(bucket) = &self.bookwalk_slippage_bucket {
            tags.push(("bookwalk_slippage".to_string(), bucket.clone()));
        }
        if let Some(bucket) = &self.book_age_bucket {
            tags.push(("book_age".to_string(), bucket.clone()));
        }
        if let Some(bucket) = &self.book_runup_bucket {
            tags.push(("book_runup".to_string(), bucket.clone()));
        }
        if let Some(bucket) = &self.directional_impulse_10s_bucket {
            tags.push(("btc_impulse_10s".to_string(), bucket.clone()));
        }
        if let Some(bucket) = &self.outcome_overround_bucket {
            tags.push(("outcome_overround".to_string(), bucket.clone()));
        }
        tags
    }

    pub fn attach_time_inputs(&mut self, timestamp_s: f64) {
        if !timestamp_s.is_finite() || timestamp_s < 0.0 {
            return;
        }
        let hour = ((timestamp_s.floor() as i64).rem_euclid(86_400) / 3_600) as u8;
        self.utc_hour_bucket = Some(format!("{hour:02}"));
        self.utc_session_bucket = Some(bucket_utc_session(hour));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn attach_orderbook_inputs(
        &mut self,
        best_bid: f64,
        best_ask: f64,
        spread: f64,
        bid_depth: f64,
        ask_depth: f64,
        pressure: f64,
        imbalance: f64,
    ) {
        if !best_bid.is_finite()
            || !best_ask.is_finite()
            || best_bid <= 0.0
            || best_ask <= 0.0
            || best_bid >= best_ask
        {
            return;
        }
        self.book_spread_bucket = Some(bucket_book_spread(spread));
        self.book_min_depth_bucket = Some(bucket_book_min_depth(bid_depth.min(ask_depth)));
        self.book_pressure_bucket = Some(bucket_signed_book_value(pressure));
        self.book_imbalance_bucket = Some(bucket_signed_book_value(imbalance));
    }

    pub fn attach_orderbook_quality_inputs(
        &mut self,
        bookwalk_slippage: Option<f64>,
        book_age_ms: Option<f64>,
    ) {
        if let Some(slippage) = bookwalk_slippage {
            self.bookwalk_slippage_bucket = Some(bucket_bookwalk_slippage(slippage));
        }
        if let Some(age_ms) = book_age_ms {
            self.book_age_bucket = Some(bucket_book_age_ms(age_ms));
        }
    }

    pub fn attach_orderbook_path_inputs(&mut self, recent_mid_runup: Option<f64>) {
        if let Some(runup) = recent_mid_runup {
            self.book_runup_bucket = Some(bucket_book_runup(runup));
        }
    }

    pub fn attach_outcome_market_inputs(&mut self, yes_no_overround: f64) {
        if yes_no_overround.is_finite() {
            self.outcome_overround_bucket = Some(bucket_outcome_overround(yes_no_overround));
        }
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

fn bucket_book_spread(spread: f64) -> String {
    if !spread.is_finite() || spread < 0.0 {
        "unknown".to_string()
    } else if spread <= 0.01 {
        "lte_0.01".to_string()
    } else if spread <= 0.03 {
        "0.01_0.03".to_string()
    } else if spread <= 0.05 {
        "0.03_0.05".to_string()
    } else {
        "gt_0.05".to_string()
    }
}

fn bucket_book_min_depth(depth: f64) -> String {
    if !depth.is_finite() || depth < 0.0 {
        "unknown".to_string()
    } else if depth < 10.0 {
        "lt_10".to_string()
    } else if depth < 50.0 {
        "10_50".to_string()
    } else if depth < 100.0 {
        "50_100".to_string()
    } else if depth < 250.0 {
        "100_250".to_string()
    } else {
        "gte_250".to_string()
    }
}

fn bucket_signed_book_value(value: f64) -> String {
    if !value.is_finite() {
        "unknown".to_string()
    } else if value <= -0.50 {
        "strong_negative".to_string()
    } else if value < -0.15 {
        "negative".to_string()
    } else if value <= 0.15 {
        "neutral".to_string()
    } else if value < 0.50 {
        "positive".to_string()
    } else {
        "strong_positive".to_string()
    }
}

fn bucket_bookwalk_slippage(slippage: f64) -> String {
    if !slippage.is_finite() || slippage < 0.0 {
        "unknown".to_string()
    } else if slippage <= 0.0001 {
        "zero".to_string()
    } else if slippage <= 0.005 {
        "lte_0.005".to_string()
    } else if slippage <= 0.01 {
        "0.005_0.01".to_string()
    } else if slippage <= 0.03 {
        "0.01_0.03".to_string()
    } else {
        "gt_0.03".to_string()
    }
}

fn bucket_book_age_ms(age_ms: f64) -> String {
    if !age_ms.is_finite() || age_ms < 0.0 {
        "unknown".to_string()
    } else if age_ms <= 100.0 {
        "lte_100ms".to_string()
    } else if age_ms <= 500.0 {
        "100_500ms".to_string()
    } else if age_ms <= 1_000.0 {
        "500_1000ms".to_string()
    } else if age_ms <= 5_000.0 {
        "1_5s".to_string()
    } else if age_ms <= 30_000.0 {
        "5_30s".to_string()
    } else {
        "gt_30s".to_string()
    }
}

fn bucket_book_runup(runup: f64) -> String {
    if !runup.is_finite() || runup < 0.0 {
        "unknown".to_string()
    } else if runup <= 0.02 {
        "lte_0.02".to_string()
    } else if runup <= 0.05 {
        "0.02_0.05".to_string()
    } else if runup <= 0.08 {
        "0.05_0.08".to_string()
    } else if runup <= 0.12 {
        "0.08_0.12".to_string()
    } else {
        "gt_0.12".to_string()
    }
}

fn bucket_directional_impulse_bps(impulse_bps: f64) -> String {
    if !impulse_bps.is_finite() {
        "unknown".to_string()
    } else if impulse_bps <= 0.0 {
        "opposite_or_flat".to_string()
    } else if impulse_bps <= 2.0 {
        "0_2".to_string()
    } else if impulse_bps <= 5.0 {
        "2_5".to_string()
    } else if impulse_bps <= 8.0 {
        "5_8".to_string()
    } else if impulse_bps <= 12.0 {
        "8_12".to_string()
    } else {
        "gt_12".to_string()
    }
}

fn bucket_outcome_overround(overround: f64) -> String {
    if !overround.is_finite() {
        "unknown".to_string()
    } else if overround < -0.01 {
        "negative".to_string()
    } else if overround <= 0.02 {
        "lte_0.02".to_string()
    } else if overround <= 0.05 {
        "0.02_0.05".to_string()
    } else if overround <= 0.10 {
        "0.05_0.10".to_string()
    } else if overround <= 0.25 {
        "0.10_0.25".to_string()
    } else {
        "gt_0.25".to_string()
    }
}

fn bucket_utc_session(hour: u8) -> String {
    match hour {
        0..=3 => "00_03".to_string(),
        4..=7 => "04_07".to_string(),
        8..=11 => "08_11".to_string(),
        12..=15 => "12_15".to_string(),
        16..=19 => "16_19".to_string(),
        _ => "20_23".to_string(),
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
    decide_candle_trade_with_fee(
        signal,
        minutes_elapsed,
        minutes_remaining,
        window_minutes,
        up_price,
        down_price,
        btc_price,
        open_btc,
        implied_vol,
        DEFAULT_CRYPTO_TAKER_FEE_RATE,
        min_confidence,
        min_edge,
        skip_dead_zone,
        zone_config,
        cross_asset_boost,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn decide_candle_trade_with_fee(
    signal: &MomentumSignal,
    minutes_elapsed: f64,
    minutes_remaining: f64,
    window_minutes: f64,
    up_price: f64,
    down_price: f64,
    btc_price: f64,
    open_btc: f64,
    implied_vol: f64,
    entry_fee_rate: f64,
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
    if !minutes_elapsed.is_finite()
        || !minutes_remaining.is_finite()
        || !window_minutes.is_finite()
        || !up_price.is_finite()
        || !down_price.is_finite()
        || !btc_price.is_finite()
        || !open_btc.is_finite()
        || !implied_vol.is_finite()
        || !entry_fee_rate.is_finite()
        || !signal.confidence.is_finite()
        || !signal.z_score.is_finite()
        || window_minutes <= 0.0
        || minutes_remaining <= 0.0
        || !(0.0..=1.0).contains(&up_price)
        || up_price == 0.0
        || !(0.0..=1.0).contains(&down_price)
        || down_price == 0.0
        || btc_price <= 0.0
        || open_btc <= 0.0
        || implied_vol <= 0.0
        || entry_fee_rate < 0.0
        || !matches!(signal.direction.as_str(), "up" | "down")
    {
        return DecisionResult::Skip(SkipReason::new(
            "invalid_decision_input",
            zone,
            "non-finite or out-of-domain decision input",
        ));
    }
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

    let yes_no_vig = up_price + down_price - 1.0;

    let days_remaining = minutes_remaining / 1440.0;
    let raw_fair =
        binary_option_price_with_rate(btc_price, open_btc, days_remaining, implied_vol, 0.05);
    let fair_value = if signal.direction == "up" {
        raw_fair
    } else {
        1.0 - raw_fair
    };
    let gross_edge = fair_value - market_price;
    let entry_fee_per_share = entry_fee_rate * market_price * (1.0 - market_price);
    let edge = gross_edge - entry_fee_per_share;

    if edge < cfg.min_ev_buffer {
        return DecisionResult::Skip(SkipReason::new(
            "negative_ev",
            zone,
            format!(
                "fair={fair_value:.3}<price={market_price:.3}+fee={entry_fee_per_share:.4}+buffer={:.3}",
                cfg.min_ev_buffer,
            ),
        ));
    }

    if zone != "terminal" && gross_edge > cfg.edge_cap {
        return DecisionResult::Skip(SkipReason::new(
            "edge_too_high_stale",
            zone,
            format!("{:.2}", gross_edge),
        ));
    }

    if edge < z_min_edge {
        return DecisionResult::Skip(SkipReason::new(
            "low_edge",
            zone,
            format!("{:.3} < {:.3}", edge, z_min_edge),
        ));
    }

    let mut regime = DecisionRegime::from_decision_inputs(
        zone,
        signal,
        market_price,
        edge,
        implied_vol,
        minutes_remaining,
    );
    regime.attach_outcome_market_inputs(yes_no_vig);

    DecisionResult::Trade(CandleDecision {
        direction: signal.direction.clone(),
        confidence: signal.confidence,
        z_score: signal.z_score,
        zone: zone.to_string(),
        fair_value,
        market_price,
        gross_edge,
        entry_fee_per_share,
        edge,
        minutes_remaining,
        yes_no_vig,
        regime,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_decision() -> CandleDecision {
        CandleDecision {
            direction: "up".to_string(),
            confidence: 0.9,
            z_score: 2.0,
            zone: "primary".to_string(),
            fair_value: 0.80,
            market_price: 0.50,
            gross_edge: 0.30,
            entry_fee_per_share: 0.0175,
            edge: 0.2825,
            minutes_remaining: 2.0,
            yes_no_vig: 0.0,
            regime: DecisionRegime::default(),
        }
    }

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
            directional_impulse_10s_bps: None,
        }
    }

    #[test]
    fn taker_reprice_uses_executable_vwap_and_fee() {
        let mut decision = sample_decision();
        decision
            .reprice_for_taker_execution(0.70, 0.75, 0.07, 0.07, &ZoneConfig::default())
            .expect("executable edge should pass");
        assert!((decision.market_price - 0.70).abs() < 1e-12);
        assert!((decision.entry_fee_per_share - 0.0147).abs() < 1e-12);
        assert!((decision.edge - 0.0853).abs() < 1e-12);
    }

    #[test]
    fn taker_reprice_rejects_limit_above_max_price() {
        let mut decision = sample_decision();
        let skip = decision
            .reprice_for_taker_execution(0.70, 0.91, 0.07, 0.07, &ZoneConfig::default())
            .expect_err("FOK limit above max price must fail");
        assert_eq!(skip.reason, "execution_price_out_of_range");
    }

    #[test]
    fn taker_reprice_tolerates_sub_epsilon_vwap_rounding() {
        let mut decision = sample_decision();
        decision.fair_value = 0.95;
        let cfg = ZoneConfig {
            min_ev_buffer: 0.0,
            ..ZoneConfig::default()
        };
        decision
            .reprice_for_taker_execution(0.7900000000000001, 0.79, 0.0, 0.0, &cfg)
            .expect("tick-rounded limit should cover epsilon-only VWAP drift");
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
    fn rejects_negative_fair_value_ev_even_with_high_confidence() {
        let sig = mk_signal(0.95, 1.5, "up");
        let cfg = ZoneConfig::default();
        let r = decide_candle_trade(
            &sig,
            4.0,
            1.0,
            5.0,
            0.65,
            0.35,
            69_990.0,
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
    fn fair_value_ev_does_not_treat_confidence_as_probability() {
        let sig = mk_signal(0.65, 1.5, "up");
        let cfg = ZoneConfig {
            edge_cap: 1.0,
            ..ZoneConfig::default()
        };
        let r = decide_candle_trade(
            &sig,
            4.0,
            1.0,
            5.0,
            0.65,
            0.35,
            70_500.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            true,
            &cfg,
            0.0,
        );
        assert!(matches!(r, DecisionResult::Trade(_)));
    }

    #[test]
    fn entry_ev_gate_uses_fee_adjusted_edge() {
        let sig = mk_signal(0.95, 1.5, "up");
        let cfg = ZoneConfig {
            min_ev_buffer: 0.05,
            early_min_edge: 0.0,
            late_min_edge: 0.0,
            terminal_min_edge: 0.0,
            primary_min_z: 0.0,
            late_min_z: 0.0,
            edge_cap: 1.0,
            ..ZoneConfig::default()
        };
        let fair = binary_option_price_with_rate(70_050.0, 70_000.0, 1.0 / 1440.0, 0.5, 0.05);
        let market = fair - 0.06;
        let without_fee = decide_candle_trade_with_fee(
            &sig,
            4.0,
            1.0,
            5.0,
            market,
            1.0 - market,
            70_050.0,
            70_000.0,
            0.5,
            0.0,
            DEFAULT_MIN_CONFIDENCE,
            0.0,
            false,
            &cfg,
            0.0,
        );
        let with_fee = decide_candle_trade_with_fee(
            &sig,
            4.0,
            1.0,
            5.0,
            market,
            1.0 - market,
            70_050.0,
            70_000.0,
            0.5,
            0.07,
            DEFAULT_MIN_CONFIDENCE,
            0.0,
            false,
            &cfg,
            0.0,
        );

        assert!(matches!(without_fee, DecisionResult::Trade(_)));
        assert!(matches!(
            with_fee,
            DecisionResult::Skip(SkipReason { reason, .. }) if reason == "negative_ev"
        ));
    }

    #[test]
    fn non_finite_decision_input_fails_closed() {
        let sig = mk_signal(0.95, 1.5, "up");
        let result = decide_candle_trade(
            &sig,
            4.0,
            1.0,
            5.0,
            f64::NAN,
            0.5,
            70_050.0,
            70_000.0,
            0.5,
            DEFAULT_MIN_CONFIDENCE,
            DEFAULT_MIN_EDGE,
            false,
            &ZoneConfig::default(),
            0.0,
        );

        assert!(matches!(
            result,
            DecisionResult::Skip(SkipReason { reason, .. }) if reason == "invalid_decision_input"
        ));
    }

    #[test]
    fn decision_regime_uses_only_pre_trade_inputs() {
        let mut sig = mk_signal(0.76, 1.3, "down");
        sig.directional_impulse_10s_bps = Some(8.2);
        let mut regime =
            DecisionRegime::from_decision_inputs("terminal", &sig, 0.42, 0.09, 0.75, 0.8);
        regime.attach_outcome_market_inputs(0.80);

        assert_eq!(regime.zone, "terminal");
        assert_eq!(regime.direction, "down");
        assert_eq!(regime.price_bucket, "0.25_0.50");
        assert_eq!(regime.edge_bucket, "0.07_0.15");
        assert_eq!(regime.z_bucket, "1.1_1.5");
        assert_eq!(regime.confidence_bucket, "0.70_0.85");
        assert_eq!(regime.volatility_bucket, "0.40_0.80");
        assert_eq!(regime.reversion_bucket, "1_2");
        assert_eq!(regime.minutes_remaining_bucket, "lte_1");
        assert_eq!(
            regime.directional_impulse_10s_bucket.as_deref(),
            Some("8_12")
        );
        assert_eq!(regime.outcome_overround_bucket.as_deref(), Some("gt_0.25"));
        assert!(regime.key().contains("zone=terminal"));
        assert!(regime.key().contains("btc_impulse_10s=8_12"));
        assert!(regime.key().contains("outcome_overround=gt_0.25"));
        assert!(!regime.key().contains("book_"));
        assert!(!regime.key().contains("utc_hour="));

        let mut with_time = regime.clone();
        with_time.attach_time_inputs(16.0 * 3_600.0 + 15.0);
        assert_eq!(with_time.utc_hour_bucket.as_deref(), Some("16"));
        assert_eq!(with_time.utc_session_bucket.as_deref(), Some("16_19"));
        assert!(with_time.key().contains("utc_session=16_19"));
        assert!(with_time.key().contains("utc_hour=16"));
        assert!(with_time.key().contains("direction_utc_session=down_16_19"));
        assert!(with_time.key().contains("direction_utc_hour=down_16"));
        assert!(with_time
            .causal_tags()
            .contains(&("utc_session".to_string(), "16_19".to_string())));
        assert!(with_time
            .causal_tags()
            .contains(&("utc_hour".to_string(), "16".to_string())));
        assert!(with_time
            .causal_tags()
            .contains(&("direction_utc_hour".to_string(), "down_16".to_string())));

        let mut invalid_book = regime.clone();
        invalid_book.attach_orderbook_inputs(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
        assert_eq!(invalid_book.key(), regime.key());

        let mut with_book = regime.clone();
        with_book.attach_orderbook_inputs(0.50, 0.52, 0.02, 200.0, 70.0, 0.20, -0.48);
        assert_eq!(with_book.book_spread_bucket.as_deref(), Some("0.01_0.03"));
        assert_eq!(with_book.book_min_depth_bucket.as_deref(), Some("50_100"));
        assert_eq!(with_book.book_pressure_bucket.as_deref(), Some("positive"));
        assert_eq!(with_book.book_imbalance_bucket.as_deref(), Some("negative"));
        with_book.attach_orderbook_quality_inputs(Some(0.012), Some(750.0));
        assert_eq!(
            with_book.bookwalk_slippage_bucket.as_deref(),
            Some("0.01_0.03")
        );
        assert_eq!(with_book.book_age_bucket.as_deref(), Some("500_1000ms"));
        assert!(with_book.key().contains("book_spread=0.01_0.03"));
        assert!(with_book.key().contains("bookwalk_slippage=0.01_0.03"));
        assert!(with_book
            .causal_tags()
            .contains(&("book_min_depth".to_string(), "50_100".to_string())));
        assert!(with_book
            .causal_tags()
            .contains(&("book_age".to_string(), "500_1000ms".to_string())));
    }
}
