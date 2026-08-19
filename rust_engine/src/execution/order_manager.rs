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

    pub fn restore(&mut self, order: ManagedOrder) -> Result<&ManagedOrder, String> {
        let intent_id = order.intent.intent_id.clone();
        if self.orders.contains_key(&intent_id) {
            return Err(format!("duplicate intent_id {intent_id}"));
        }
        if !matches!(
            order.state,
            OrderState::Submitted | OrderState::Acked | OrderState::PartiallyFilled
        ) {
            return Err(format!("cannot restore order in {}", order.state.as_str()));
        }
        if order
            .venue_order_id
            .as_deref()
            .is_none_or(|id| id.trim().is_empty())
        {
            return Err("restored order requires venue_order_id".to_string());
        }
        if !order.requested_size.is_finite()
            || order.requested_size <= 0.0
            || !order.filled_size.is_finite()
            || order.filled_size < 0.0
            || order.filled_size - order.requested_size > 1e-9
            || !order.total_fees.is_finite()
            || order.total_fees < 0.0
            || (order.intent.size - order.requested_size).abs() > 1e-9
        {
            return Err("restored order economics are invalid".to_string());
        }
        if order.filled_size > 0.0
            && (!order.avg_fill_price.is_finite() || !(0.0..=1.0).contains(&order.avg_fill_price))
        {
            return Err("restored order fill price is invalid".to_string());
        }
        self.orders.insert(intent_id.clone(), order);
        self.orders
            .get(&intent_id)
            .ok_or_else(|| "restored order missing".to_string())
    }

    pub fn get(&self, intent_id: &str) -> Option<&ManagedOrder> {
        self.orders.get(intent_id)
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
        if !matches!(order.state, OrderState::Acked | OrderState::PartiallyFilled) {
            return Err(format!("cannot fill order in {}", order.state.as_str()));
        }
        let remaining_size = order.requested_size - order.filled_size;
        if remaining_size <= f64::EPSILON {
            return Err("order already fully filled".to_string());
        }
        if fill_size - remaining_size > 1e-9 {
            return Err(format!(
                "fill_size {} exceeds remaining_size {}",
                fill_size, remaining_size
            ));
        }
        let prev_notional = order.avg_fill_price * order.filled_size;
        let new_notional = prev_notional + fill_size * fill_price;
        order.filled_size += fill_size;
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

    pub fn cancel(&mut self, intent_id: &str, ts: f64) -> Result<&ManagedOrder, String> {
        self.transition(intent_id, OrderState::Canceled, ts)
    }

    pub fn intent_id_for_venue_order_id(&self, venue_order_id: &str) -> Option<String> {
        self.orders.iter().find_map(|(intent_id, order)| {
            (order.venue_order_id.as_deref() == Some(venue_order_id)).then(|| intent_id.clone())
        })
    }

    pub fn reconcile_live_by_venue_order_id(
        &mut self,
        venue_order_id: &str,
        ts: f64,
    ) -> Result<&ManagedOrder, String> {
        let intent_id = self
            .intent_id_for_venue_order_id(venue_order_id)
            .ok_or_else(|| format!("unknown venue_order_id {venue_order_id}"))?;
        let state = self
            .orders
            .get(&intent_id)
            .ok_or_else(|| format!("unknown intent_id {intent_id}"))?
            .state;
        if matches!(
            state,
            OrderState::IntentCreated | OrderState::RiskAccepted | OrderState::Submitted
        ) {
            self.ack(&intent_id, Some(venue_order_id.to_string()), ts)
        } else {
            self.orders
                .get(&intent_id)
                .ok_or_else(|| format!("unknown intent_id {intent_id}"))
        }
    }

    pub fn fill_by_venue_order_id(
        &mut self,
        venue_order_id: &str,
        fill_size: f64,
        fill_price: f64,
        fee: f64,
        ts: f64,
    ) -> Result<&ManagedOrder, String> {
        let intent_id = self
            .intent_id_for_venue_order_id(venue_order_id)
            .ok_or_else(|| format!("unknown venue_order_id {venue_order_id}"))?;
        self.fill(&intent_id, fill_size, fill_price, fee, ts)
    }

    pub fn reject_by_venue_order_id(
        &mut self,
        venue_order_id: &str,
        reason: impl Into<String>,
        ts: f64,
    ) -> Result<&ManagedOrder, String> {
        let intent_id = self
            .intent_id_for_venue_order_id(venue_order_id)
            .ok_or_else(|| format!("unknown venue_order_id {venue_order_id}"))?;
        self.reject(&intent_id, reason, ts)
    }

    pub fn cancel_by_venue_order_id(
        &mut self,
        venue_order_id: &str,
        ts: f64,
    ) -> Result<&ManagedOrder, String> {
        let intent_id = self
            .intent_id_for_venue_order_id(venue_order_id)
            .ok_or_else(|| format!("unknown venue_order_id {venue_order_id}"))?;
        self.cancel(&intent_id, ts)
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
        if !transition_allowed(order.state, next) {
            return Err(format!(
                "illegal order transition {} -> {}",
                order.state.as_str(),
                next.as_str()
            ));
        }
        order.state = next;
        order.updated_ts = ts;
        Ok(order)
    }
}

fn transition_allowed(current: OrderState, next: OrderState) -> bool {
    use OrderState::*;
    matches!(
        (current, next),
        (IntentCreated, RiskAccepted)
            | (IntentCreated, Rejected)
            | (RiskAccepted, Submitted)
            | (RiskAccepted, Rejected)
            | (RiskAccepted, Canceled)
            | (Submitted, Acked)
            | (Submitted, Rejected)
            | (Submitted, Canceled)
            | (Submitted, Expired)
            | (Acked, Rejected)
            | (Acked, Canceled)
            | (Acked, Expired)
            | (PartiallyFilled, Canceled)
            | (PartiallyFilled, Expired)
    )
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
        manager
            .submit(&id, Some("paper-1".to_string()), 1.2)
            .unwrap();
        manager.ack(&id, Some("paper-1".to_string()), 1.3).unwrap();
        let order = manager.fill(&id, 10.0, 0.5, 0.01, 1.4).unwrap();
        assert_eq!(order.state, OrderState::Filled);
        assert_eq!(order.fill_pct(), 1.0);
        assert_eq!(order.venue_order_id.as_deref(), Some("paper-1"));
    }

    #[test]
    fn restores_a_persisted_nonterminal_order() {
        let mut manager = OrderManager::new();
        let intent = intent(10.0);
        let order = ManagedOrder {
            intent: intent.clone(),
            state: OrderState::Submitted,
            venue_order_id: Some("0xorder".to_string()),
            requested_size: 10.0,
            filled_size: 0.0,
            avg_fill_price: 0.0,
            total_fees: 0.0,
            reject_reason: None,
            created_ts: 1.0,
            updated_ts: 1.1,
        };
        manager.restore(order).unwrap();
        assert_eq!(
            manager.intent_id_for_venue_order_id("0xorder"),
            Some(intent.intent_id)
        );
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
    fn fill_requires_ack_and_cannot_overfill_or_duplicate() {
        let mut manager = OrderManager::new();
        let intent = intent(10.0);
        let id = intent.intent_id.clone();
        manager.create_intent(intent, 1.0).unwrap();
        manager.risk_accept(&id, 1.1).unwrap();
        manager
            .submit(&id, Some("paper-1".to_string()), 1.2)
            .unwrap();

        let err = manager.fill(&id, 1.0, 0.5, 0.0, 1.25).unwrap_err();
        assert!(err.contains("submitted"));

        manager.ack(&id, Some("paper-1".to_string()), 1.3).unwrap();
        let err = manager.fill(&id, 11.0, 0.5, 0.0, 1.35).unwrap_err();
        assert!(err.contains("exceeds remaining_size"));

        manager.fill(&id, 10.0, 0.5, 0.0, 1.4).unwrap();
        let err = manager.fill(&id, 1.0, 0.5, 0.0, 1.5).unwrap_err();
        assert!(err.contains("filled"));
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

    #[test]
    fn illegal_lifecycle_transitions_are_rejected() {
        let mut manager = OrderManager::new();
        let intent = intent(10.0);
        let id = intent.intent_id.clone();
        manager.create_intent(intent, 1.0).unwrap();

        let err = manager
            .submit(&id, Some("paper-1".to_string()), 1.1)
            .unwrap_err();
        assert!(err.contains("illegal order transition"));

        manager.risk_accept(&id, 1.2).unwrap();
        manager
            .submit(&id, Some("paper-1".to_string()), 1.3)
            .unwrap();
        let err = manager.risk_accept(&id, 1.4).unwrap_err();
        assert!(err.contains("illegal order transition"));
    }

    #[test]
    fn reconciles_by_venue_order_id() {
        let mut manager = OrderManager::new();
        let intent = intent(10.0);
        let id = intent.intent_id.clone();
        manager.create_intent(intent, 1.0).unwrap();
        manager.risk_accept(&id, 1.1).unwrap();
        manager
            .submit(&id, Some("0xvenue".to_string()), 1.2)
            .unwrap();
        manager
            .reconcile_live_by_venue_order_id("0xvenue", 1.3)
            .unwrap();
        let order = manager
            .fill_by_venue_order_id("0xvenue", 4.0, 0.5, 0.01, 1.4)
            .unwrap();
        assert_eq!(order.state, OrderState::PartiallyFilled);
        assert_eq!(order.filled_size, 4.0);
        assert_eq!(
            manager.intent_id_for_venue_order_id("0xvenue").as_deref(),
            Some(id.as_str())
        );
    }
}
