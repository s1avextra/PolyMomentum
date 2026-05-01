//! Shared order lifecycle state machine for backtest, paper, and live.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::strategy::spec::OrderIntent;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderState {
    IntentCreated,
    RiskAccepted,
    Submitted,
    Acked,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
    Settled,
}

impl OrderState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IntentCreated => "intent_created",
            Self::RiskAccepted => "risk_accepted",
            Self::Submitted => "submitted",
            Self::Acked => "acked",
            Self::PartiallyFilled => "partially_filled",
            Self::Filled => "filled",
            Self::Canceled => "canceled",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
            Self::Settled => "settled",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Rejected | Self::Expired | Self::Settled
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedOrder {
    pub intent: OrderIntent,
    pub state: OrderState,
    pub venue_order_id: Option<String>,
    pub requested_size: f64,
    pub filled_size: f64,
    pub avg_fill_price: f64,
    pub total_fees: f64,
    pub reject_reason: Option<String>,
    pub created_ts: f64,
    pub updated_ts: f64,
}

impl ManagedOrder {
    pub fn fill_pct(&self) -> f64 {
        if self.requested_size > 0.0 {
            (self.filled_size / self.requested_size).clamp(0.0, 1.0)
        } else {
            0.0
        }
    }
}

#[derive(Debug, Default)]
pub struct OrderManager {
    orders: BTreeMap<String, ManagedOrder>,
}

impl OrderManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_intent(&mut self, intent: OrderIntent, ts: f64) -> Result<&ManagedOrder, String> {
        if self.orders.contains_key(&intent.intent_id) {
            return Err(format!("duplicate intent_id {}", intent.intent_id));
        }
        let order = ManagedOrder {
            requested_size: intent.size,
            intent,
            state: OrderState::IntentCreated,
            venue_order_id: None,
            filled_size: 0.0,
            avg_fill_price: 0.0,
            total_fees: 0.0,
            reject_reason: None,
            created_ts: ts,
            updated_ts: ts,
        };
        let key = order.intent.intent_id.clone();
        self.orders.insert(key.clone(), order);
        self.orders
            .get(&key)
            .ok_or_else(|| "inserted order missing".to_string())
    }

    pub fn risk_accept(&mut self, intent_id: &str, ts: f64) -> Result<&ManagedOrder, String> {
        self.transition(intent_id, OrderState::RiskAccepted, ts)
    }

    pub fn submit(
        &mut self,
        intent_id: &str,
        venue_order_id: Option<String>,
        ts: f64,
    ) -> Result<&ManagedOrder, String> {
        self.transition(intent_id, OrderState::Submitted, ts)?;
        if let Some(id) = venue_order_id {
            self.orders
                .get_mut(intent_id)
                .expect("order exists after transition")
                .venue_order_id = Some(id);
        }
        self.orders
            .get(intent_id)
            .ok_or_else(|| format!("unknown intent_id {intent_id}"))
    }

    pub fn ack(
        &mut self,
        intent_id: &str,
        venue_order_id: Option<String>,
        ts: f64,
    ) -> Result<&ManagedOrder, String> {
        self.transition(intent_id, OrderState::Acked, ts)?;
        if let Some(id) = venue_order_id {
            self.orders
                .get_mut(intent_id)
                .expect("order exists after transition")
                .venue_order_id = Some(id);
        }
        self.orders
            .get(intent_id)
            .ok_or_else(|| format!("unknown intent_id {intent_id}"))
    }

    pub fn fill(
        &mut self,
        intent_id: &str,
        fill_size: f64,
        fill_price: f64,
        fee: f64,
        ts: f64,
    ) -> Result<&ManagedOrder, String> {
        if fill_size <= 0.0 {
            return Err("fill_size must be positive".to_string());
        }
        let order = self
            .orders
            .get_mut(intent_id)
            .ok_or_else(|| format!("unknown intent_id {intent_id}"))?;
        if order.state.is_terminal() && order.state != OrderState::Filled {
            return Err(format!("cannot fill terminal order in {}", order.state.as_str()));
        }
        let prev_notional = order.avg_fill_price * order.filled_size;
        let new_filled = (order.filled_size + fill_size).min(order.requested_size);
        let applied_size = new_filled - order.filled_size;
        let new_notional = prev_notional + applied_size * fill_price;
        order.filled_size = new_filled;
        order.avg_fill_price = if order.filled_size > 0.0 {
            new_notional / order.filled_size
        } else {
            0.0
        };
        order.total_fees += fee;
        order.updated_ts = ts;
        order.state = if order.filled_size + f64::EPSILON >= order.requested_size {
            OrderState::Filled
        } else {
            OrderState::PartiallyFilled
        };
        Ok(order)
    }

    pub fn reject(
        &mut self,
        intent_id: &str,
        reason: impl Into<String>,
        ts: f64,
    ) -> Result<&ManagedOrder, String> {
        self.transition(intent_id, OrderState::Rejected, ts)?;
        self.orders
            .get_mut(intent_id)
            .expect("order exists after transition")
            .reject_reason = Some(reason.into());
        self.orders
            .get(intent_id)
            .ok_or_else(|| format!("unknown intent_id {intent_id}"))
    }

    pub fn get(&self, intent_id: &str) -> Option<&ManagedOrder> {
        self.orders.get(intent_id)
    }

    fn transition(
        &mut self,
        intent_id: &str,
        next: OrderState,
        ts: f64,
    ) -> Result<&ManagedOrder, String> {
        let order = self
            .orders
            .get_mut(intent_id)
            .ok_or_else(|| format!("unknown intent_id {intent_id}"))?;
        if order.state.is_terminal() {
            return Err(format!(
                "cannot transition terminal order {} -> {}",
                order.state.as_str(),
                next.as_str()
            ));
        }
        order.state = next;
        order.updated_ts = ts;
        Ok(order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::spec::{OrderIntent, Signal, StrategySpec};

    fn intent(size: f64) -> OrderIntent {
        let strategy = StrategySpec::new("test", "1", "hash", "risk");
        let signal = Signal {
            market_id: "0xabc".to_string(),
            token_id: "tok".to_string(),
            direction: "up".to_string(),
            fair_price: 0.6,
            edge: 0.1,
            confidence: 0.7,
            diagnostics: serde_json::json!({}),
        };
        OrderIntent::deterministic(strategy, &signal, "buy", "market", None, size, "test", "1")
    }

    #[test]
    fn manages_ack_and_full_fill() {
        let mut manager = OrderManager::new();
        let intent = intent(10.0);
        let id = intent.intent_id.clone();
        manager.create_intent(intent, 1.0).unwrap();
        manager.risk_accept(&id, 1.1).unwrap();
        manager.submit(&id, Some("paper-1".to_string()), 1.2).unwrap();
        manager.ack(&id, Some("paper-1".to_string()), 1.3).unwrap();
        let order = manager.fill(&id, 10.0, 0.5, 0.01, 1.4).unwrap();
        assert_eq!(order.state, OrderState::Filled);
        assert_eq!(order.fill_pct(), 1.0);
        assert_eq!(order.venue_order_id.as_deref(), Some("paper-1"));
    }

    #[test]
    fn partial_fill_then_fill_updates_average_price() {
        let mut manager = OrderManager::new();
        let intent = intent(10.0);
        let id = intent.intent_id.clone();
        manager.create_intent(intent, 1.0).unwrap();
        manager.risk_accept(&id, 1.1).unwrap();
        manager.submit(&id, None, 1.2).unwrap();
        manager.ack(&id, None, 1.3).unwrap();
        let partial = manager.fill(&id, 4.0, 0.50, 0.0, 1.4).unwrap();
        assert_eq!(partial.state, OrderState::PartiallyFilled);
        let full = manager.fill(&id, 6.0, 0.60, 0.0, 1.5).unwrap();
        assert_eq!(full.state, OrderState::Filled);
        assert!((full.avg_fill_price - 0.56).abs() < 1e-9);
    }

    #[test]
    fn reject_is_terminal() {
        let mut manager = OrderManager::new();
        let intent = intent(10.0);
        let id = intent.intent_id.clone();
        manager.create_intent(intent, 1.0).unwrap();
        manager.reject(&id, "no liquidity", 1.1).unwrap();
        let err = manager.risk_accept(&id, 1.2).unwrap_err();
        assert!(err.contains("terminal"));
    }
}
