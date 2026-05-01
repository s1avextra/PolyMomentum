//! Stage-neutral strategy and order contracts.
//!
//! Backtest, paper, and live should all speak these types at the strategy
//! boundary. Execution adapters may add venue-specific fields later, but a
//! strategy signal must become the same order intent in every mode.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::strategy::decision::CandleDecision;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategySpec {
    pub name: String,
    pub version: String,
    pub params_hash: String,
    pub risk_profile: String,
}

impl StrategySpec {
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        params_hash: impl Into<String>,
        risk_profile: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            params_hash: params_hash.into(),
            risk_profile: risk_profile.into(),
        }
    }

    pub fn from_serializable_params<T: Serialize>(
        name: impl Into<String>,
        version: impl Into<String>,
        params: &T,
        risk_profile: impl Into<String>,
    ) -> Self {
        let params_hash = stable_json_hash(params);
        Self::new(name, version, params_hash, risk_profile)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Signal {
    pub market_id: String,
    pub token_id: String,
    pub direction: String,
    pub fair_price: f64,
    pub edge: f64,
    pub confidence: f64,
    pub diagnostics: serde_json::Value,
}

impl Signal {
    pub fn from_candle_decision(
        market_id: impl Into<String>,
        token_id: impl Into<String>,
        decision: &CandleDecision,
        diagnostics: serde_json::Value,
    ) -> Self {
        Self {
            market_id: market_id.into(),
            token_id: token_id.into(),
            direction: decision.direction.clone(),
            fair_price: decision.fair_value,
            edge: decision.edge,
            confidence: decision.confidence,
            diagnostics,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub intent_id: String,
    pub strategy: StrategySpec,
    pub market_id: String,
    pub token_id: String,
    pub side: String,
    pub order_type: String,
    pub limit_price: Option<f64>,
    pub size: f64,
    pub reason: String,
}

impl OrderIntent {
    #[allow(clippy::too_many_arguments)]
    pub fn deterministic(
        strategy: StrategySpec,
        signal: &Signal,
        side: impl Into<String>,
        order_type: impl Into<String>,
        limit_price: Option<f64>,
        size: f64,
        reason: impl Into<String>,
        uniqueness_key: impl AsRef<str>,
    ) -> Self {
        let side = side.into();
        let order_type = order_type.into();
        let reason = reason.into();
        let id_payload = serde_json::json!({
            "strategy": &strategy,
            "market_id": signal.market_id,
            "token_id": signal.token_id,
            "side": side,
            "order_type": order_type,
            "limit_price": limit_price,
            "size": size,
            "reason": reason,
            "uniqueness_key": uniqueness_key.as_ref(),
        });
        let intent_id = format!("intent_{}", stable_json_hash(&id_payload));
        Self {
            intent_id,
            strategy,
            market_id: signal.market_id.clone(),
            token_id: signal.token_id.clone(),
            side,
            order_type,
            limit_price,
            size,
            reason,
        }
    }
}

pub fn stable_json_hash<T: Serialize>(value: &T) -> String {
    let payload = serde_json::to_vec(value).expect("serializable strategy contract");
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decision() -> CandleDecision {
        CandleDecision {
            direction: "up".to_string(),
            confidence: 0.72,
            z_score: 1.4,
            zone: "terminal".to_string(),
            fair_value: 0.66,
            market_price: 0.58,
            edge: 0.08,
            minutes_remaining: 0.5,
            yes_no_vig: 0.01,
        }
    }

    #[test]
    fn strategy_params_hash_is_stable() {
        let spec_a = StrategySpec::from_serializable_params(
            "candle_momentum",
            "1",
            &serde_json::json!({"z": 0.3, "edge": 0.05}),
            "micro",
        );
        let spec_b = StrategySpec::from_serializable_params(
            "candle_momentum",
            "1",
            &serde_json::json!({"z": 0.3, "edge": 0.05}),
            "micro",
        );
        assert_eq!(spec_a.params_hash, spec_b.params_hash);
        assert_eq!(spec_a.params_hash.len(), 64);
    }

    #[test]
    fn candle_decision_maps_to_signal() {
        let d = decision();
        let sig = Signal::from_candle_decision(
            "0xabc",
            "token-up",
            &d,
            serde_json::json!({"zone": d.zone}),
        );
        assert_eq!(sig.market_id, "0xabc");
        assert_eq!(sig.token_id, "token-up");
        assert_eq!(sig.direction, "up");
        assert_eq!(sig.fair_price, 0.66);
        assert_eq!(sig.edge, 0.08);
    }

    #[test]
    fn deterministic_order_intent_ids_match_for_same_payload() {
        let spec = StrategySpec::from_serializable_params(
            "candle_momentum",
            "1",
            &serde_json::json!({"edge": 0.05}),
            "micro",
        );
        let signal =
            Signal::from_candle_decision("0xabc", "token-up", &decision(), serde_json::json!({}));
        let a = OrderIntent::deterministic(
            spec.clone(),
            &signal,
            "buy",
            "market",
            None,
            10.0,
            "terminal edge",
            "1700000000.0",
        );
        let b = OrderIntent::deterministic(
            spec,
            &signal,
            "buy",
            "market",
            None,
            10.0,
            "terminal edge",
            "1700000000.0",
        );
        assert_eq!(a.intent_id, b.intent_id);
        assert!(a.intent_id.starts_with("intent_"));
    }
}
