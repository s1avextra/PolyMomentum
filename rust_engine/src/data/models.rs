//! Domain models — parsed Polymarket markets and outcomes.
//!
//! L2 book state for live trading lives in `polymarket_ws::TokenBookState`.

use serde::{Deserialize, Serialize};

pub const DEFAULT_CRYPTO_TAKER_FEE_RATE: f64 = 0.07;
pub const DEFAULT_MAKER_FEE_RATE: f64 = 0.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Outcome {
    pub token_id: String,
    pub name: String,
    pub price: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Market {
    pub condition_id: String,
    pub question: String,
    pub slug: String,
    pub outcomes: Vec<Outcome>,
    pub tags: Vec<String>,
    pub category: String,
    pub active: bool,
    pub closed: bool,
    pub volume: f64,
    pub liquidity: f64,
    pub end_date: String,
    pub event_slug: String,
    pub event_id: String,
    pub event_title: String,
    pub group_slug: String,
    pub neg_risk: bool,
    pub neg_risk_augmented: bool,
    pub minimum_tick_size: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fees_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub taker_fee_rate: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maker_fee_rate: Option<f64>,
}

impl Market {
    pub fn effective_taker_fee_rate(&self, fallback: f64) -> f64 {
        if self.fees_enabled == Some(false) {
            return 0.0;
        }
        self.taker_fee_rate
            .filter(|rate| rate.is_finite() && *rate >= 0.0)
            .or_else(|| category_taker_fee_rate(&self.category))
            .unwrap_or(fallback)
    }

    pub fn effective_maker_fee_rate(&self, fallback: f64) -> f64 {
        if self.fees_enabled == Some(false) {
            return 0.0;
        }
        self.maker_fee_rate
            .filter(|rate| rate.is_finite() && *rate >= 0.0)
            .unwrap_or(fallback)
    }
}

pub fn category_taker_fee_rate(category: &str) -> Option<f64> {
    match category.trim().to_ascii_lowercase().as_str() {
        "crypto" | "cryptocurrency" | "cryptocurrencies" => Some(0.07),
        "sports" => Some(0.03),
        "finance" | "politics" | "mentions" | "tech" | "technology" => Some(0.04),
        "economics" | "culture" | "weather" | "other" | "general" | "other / general" => Some(0.05),
        "geopolitics" | "geopolitical" | "world events" => Some(0.0),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_fee_rates_match_polymarket_schedule() {
        assert_eq!(category_taker_fee_rate("Crypto"), Some(0.07));
        assert_eq!(category_taker_fee_rate("Sports"), Some(0.03));
        assert_eq!(category_taker_fee_rate("Tech"), Some(0.04));
        assert_eq!(category_taker_fee_rate("Weather"), Some(0.05));
        assert_eq!(category_taker_fee_rate("Geopolitics"), Some(0.0));
        assert_eq!(category_taker_fee_rate(""), None);
    }

    #[test]
    fn explicit_market_fee_overrides_category_and_disabled_fees_override_all() {
        let mut market = Market {
            category: "Crypto".to_string(),
            taker_fee_rate: Some(0.03),
            maker_fee_rate: Some(0.01),
            ..Default::default()
        };
        assert_eq!(market.effective_taker_fee_rate(0.99), 0.03);
        assert_eq!(
            market.effective_maker_fee_rate(DEFAULT_MAKER_FEE_RATE),
            0.01
        );

        market.fees_enabled = Some(false);
        assert_eq!(market.effective_taker_fee_rate(0.99), 0.0);
        assert_eq!(market.effective_maker_fee_rate(0.99), 0.0);
    }
}
