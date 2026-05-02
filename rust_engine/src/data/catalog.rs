//! Canonical market/token catalog snapshots.
//!
//! The catalog is the small, deterministic lookup layer that lets a backtest
//! report say exactly which markets and token IDs it used.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::data::scanner::CandleContract;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogMarket {
    pub condition_id: String,
    pub question: String,
    pub slug: String,
    pub asset: String,
    pub window_description: String,
    pub end_date: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub neg_risk: bool,
    pub liquidity: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MarketCatalog {
    pub markets: BTreeMap<String, CatalogMarket>,
    pub token_to_condition: BTreeMap<String, String>,
}

impl MarketCatalog {
    pub fn from_candle_contracts(contracts: &[CandleContract]) -> Self {
        let mut markets = BTreeMap::new();
        let mut token_to_condition = BTreeMap::new();
        for c in contracts {
            let condition_id = c.market.condition_id.clone();
            if !c.up_token_id.is_empty() {
                token_to_condition.insert(c.up_token_id.clone(), condition_id.clone());
            }
            if !c.down_token_id.is_empty() {
                token_to_condition.insert(c.down_token_id.clone(), condition_id.clone());
            }
            markets.insert(
                condition_id.clone(),
                CatalogMarket {
                    condition_id,
                    question: c.market.question.clone(),
                    slug: c.market.slug.clone(),
                    asset: c.asset.clone(),
                    window_description: c.window_description.clone(),
                    end_date: c.end_date.clone(),
                    up_token_id: c.up_token_id.clone(),
                    down_token_id: c.down_token_id.clone(),
                    neg_risk: c.market.neg_risk,
                    liquidity: c.liquidity,
                    volume: c.volume,
                },
            );
        }
        Self {
            markets,
            token_to_condition,
        }
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub fn token_count(&self) -> usize {
        self.token_to_condition.len()
    }

    pub fn assets(&self) -> Vec<String> {
        let mut set = BTreeSet::new();
        for m in self.markets.values() {
            set.insert(m.asset.clone());
        }
        set.into_iter().collect()
    }

    pub fn missing_required_tokens(&self) -> Vec<String> {
        let mut missing = Vec::new();
        for (cid, m) in &self.markets {
            if m.up_token_id.is_empty() {
                missing.push(format!("{cid}:up"));
            }
            if m.down_token_id.is_empty() {
                missing.push(format!("{cid}:down"));
            }
        }
        missing
    }

    pub fn is_complete(&self) -> bool {
        !self.markets.is_empty() && self.missing_required_tokens().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::models::{Market, Outcome};

    fn contract() -> CandleContract {
        CandleContract {
            market: Market {
                condition_id: "0xabc".to_string(),
                question: "Bitcoin Up or Down - test".to_string(),
                slug: "btc-test".to_string(),
                outcomes: vec![
                    Outcome {
                        token_id: "up-token".to_string(),
                        name: "Up".to_string(),
                        price: 0.5,
                    },
                    Outcome {
                        token_id: "down-token".to_string(),
                        name: "Down".to_string(),
                        price: 0.5,
                    },
                ],
                tags: Vec::new(),
                category: String::new(),
                active: true,
                closed: false,
                volume: 1000.0,
                liquidity: 500.0,
                end_date: "2026-05-01T00:00:00Z".to_string(),
                event_slug: String::new(),
                event_id: String::new(),
                event_title: String::new(),
                group_slug: String::new(),
                neg_risk: false,
                neg_risk_augmented: false,
                minimum_tick_size: None,
            },
            up_token_id: "up-token".to_string(),
            down_token_id: "down-token".to_string(),
            up_price: 0.5,
            down_price: 0.5,
            end_date: "2026-05-01T00:00:00Z".to_string(),
            hours_left: 0.0,
            volume: 1000.0,
            liquidity: 500.0,
            window_description: "test".to_string(),
            asset: "BTC".to_string(),
        }
    }

    #[test]
    fn builds_token_lookup() {
        let catalog = MarketCatalog::from_candle_contracts(&[contract()]);
        assert_eq!(catalog.market_count(), 1);
        assert_eq!(catalog.token_count(), 2);
        assert_eq!(catalog.token_to_condition["up-token"], "0xabc");
        assert_eq!(catalog.assets(), vec!["BTC".to_string()]);
        assert!(catalog.is_complete());
    }

    #[test]
    fn detects_missing_token() {
        let mut c = contract();
        c.down_token_id.clear();
        let catalog = MarketCatalog::from_candle_contracts(&[c]);
        assert!(!catalog.is_complete());
        assert_eq!(
            catalog.missing_required_tokens(),
            vec!["0xabc:down".to_string()]
        );
    }
}
