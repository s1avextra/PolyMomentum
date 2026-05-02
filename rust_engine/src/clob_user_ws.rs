//! Authenticated Polymarket CLOB user WebSocket feed.
//!
//! This channel is the live reconciliation source for order/trade events. It
//! does not place or cancel orders; it only subscribes with CLOB API
//! credentials and emits parsed events for the live pipeline to reconcile.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Notify, RwLock};
use tokio::time::{timeout, Instant};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

pub const POLYMARKET_USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const PING_INTERVAL: Duration = Duration::from_secs(10);
const STALE_AFTER: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct UserChannelAuth {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

impl UserChannelAuth {
    pub fn new(api_key: String, secret: String, passphrase: String) -> Self {
        Self {
            api_key,
            secret,
            passphrase,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct SubscribeMsg {
    auth: SubscribeAuth,
    #[serde(rename = "type")]
    msg_type: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    markets: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SubscribeAuth {
    #[serde(rename = "apiKey")]
    api_key: String,
    secret: String,
    passphrase: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UserEvent {
    Order(UserOrderEvent),
    Trade(UserTradeEvent),
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct UserOrderEvent {
    #[serde(default)]
    pub event_type: String,
    #[serde(default, rename = "type")]
    pub event_kind: String,
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub original_size: String,
    #[serde(default)]
    pub size_matched: String,
    #[serde(default)]
    pub price: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub created_at: serde_json::Value,
}

impl UserOrderEvent {
    pub fn timestamp_s(&self) -> f64 {
        parse_ts_s(&self.timestamp)
            .or_else(|| parse_value_ts_s(&self.created_at))
            .unwrap_or(0.0)
    }

    pub fn original_size(&self) -> f64 {
        parse_number(&self.original_size)
    }

    pub fn size_matched(&self) -> f64 {
        parse_number(&self.size_matched)
    }

    pub fn is_canceled(&self) -> bool {
        let kind = self.event_kind.to_ascii_uppercase();
        let status = self.status.to_ascii_uppercase();
        kind == "CANCELLATION" || status.contains("CANCELED") || status.contains("CANCELLED")
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct UserTradeEvent {
    #[serde(default)]
    pub event_type: String,
    #[serde(default, rename = "type")]
    pub event_kind: String,
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub taker_order_id: String,
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub size: String,
    #[serde(default)]
    pub price: String,
    #[serde(default)]
    pub fee_rate_bps: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub matchtime: String,
    #[serde(default)]
    pub maker_orders: Vec<UserMakerOrder>,
}

impl UserTradeEvent {
    pub fn timestamp_s(&self) -> f64 {
        parse_ts_s(&self.timestamp)
            .or_else(|| parse_ts_s(&self.matchtime))
            .unwrap_or(0.0)
    }

    pub fn size(&self) -> f64 {
        parse_number(&self.size)
    }

    pub fn price(&self) -> f64 {
        parse_number(&self.price)
    }

    pub fn fee(&self) -> f64 {
        self.size() * self.price() * parse_number(&self.fee_rate_bps) / 10_000.0
    }

    pub fn is_fill_status(&self) -> bool {
        matches!(
            self.status.to_ascii_uppercase().as_str(),
            "MATCHED"
                | "MINED"
                | "CONFIRMED"
                | "TRADE_STATUS_MATCHED"
                | "TRADE_STATUS_MINED"
                | "TRADE_STATUS_CONFIRMED"
        )
    }

    pub fn is_failed(&self) -> bool {
        matches!(
            self.status.to_ascii_uppercase().as_str(),
            "FAILED" | "TRADE_STATUS_FAILED"
        )
    }

    pub fn candidate_order_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        if !self.taker_order_id.is_empty() {
            ids.push(self.taker_order_id.clone());
        }
        for maker in &self.maker_orders {
            if !maker.order_id.is_empty() && !ids.iter().any(|id| id == &maker.order_id) {
                ids.push(maker.order_id.clone());
            }
        }
        ids
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct UserMakerOrder {
    #[serde(default)]
    pub order_id: String,
    #[serde(default)]
    pub matched_amount: String,
    #[serde(default)]
    pub price: String,
}

#[derive(Deserialize)]
struct EventKind {
    event_type: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

pub fn parse_user_events_text(text: &str) -> Vec<UserEvent> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return Vec::new();
    }
    if trimmed.starts_with('[') {
        let Ok(values) = serde_json::from_str::<Vec<serde_json::Value>>(trimmed) else {
            return Vec::new();
        };
        values
            .into_iter()
            .filter_map(parse_user_event_value)
            .collect()
    } else {
        serde_json::from_str::<serde_json::Value>(trimmed)
            .ok()
            .and_then(parse_user_event_value)
            .into_iter()
            .collect()
    }
}

fn parse_user_event_value(value: serde_json::Value) -> Option<UserEvent> {
    let kind: EventKind = serde_json::from_value(value.clone()).ok()?;
    let event_type = kind.event_type.unwrap_or_default().to_ascii_lowercase();
    let ty = kind.kind.unwrap_or_default().to_ascii_uppercase();
    if event_type == "order" || matches!(ty.as_str(), "PLACEMENT" | "UPDATE" | "CANCELLATION") {
        serde_json::from_value(value).ok().map(UserEvent::Order)
    } else if event_type == "trade" || ty == "TRADE" {
        serde_json::from_value(value).ok().map(UserEvent::Trade)
    } else {
        None
    }
}

pub async fn polymarket_user_feed(
    auth: UserChannelAuth,
    tracked_markets: Arc<RwLock<Vec<String>>>,
    resubscribe: Arc<Notify>,
    tx: mpsc::Sender<UserEvent>,
) {
    let mut backoff = Duration::from_millis(500);
    loop {
        let markets = tracked_markets.read().await.clone();
        let session = run_session(&auth, markets, &resubscribe, &tx).await;
        match session {
            Ok(()) => backoff = Duration::from_millis(500),
            Err(e) => {
                tracing::warn!(error = %e, "clob_user_ws session error; backing off");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

async fn run_session(
    auth: &UserChannelAuth,
    markets: Vec<String>,
    resubscribe: &Arc<Notify>,
    tx: &mpsc::Sender<UserEvent>,
) -> Result<(), String> {
    let (ws, _) = connect_async(POLYMARKET_USER_WS_URL)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let (mut write, mut read) = ws.split();
    let sub = SubscribeMsg {
        auth: SubscribeAuth {
            api_key: auth.api_key.clone(),
            secret: auth.secret.clone(),
            passphrase: auth.passphrase.clone(),
        },
        msg_type: "user",
        markets,
    };
    let payload = serde_json::to_string(&sub).map_err(|e| format!("encode: {e}"))?;
    write
        .send(Message::Text(payload.into()))
        .await
        .map_err(|e| format!("send sub: {e}"))?;

    let mut last_msg = Instant::now();
    let mut ping_timer = tokio::time::interval(PING_INTERVAL);
    ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = resubscribe.notified() => {
                tracing::info!("clob_user_ws resubscribe requested");
                return Ok(());
            }
            _ = ping_timer.tick() => {
                if last_msg.elapsed() > STALE_AFTER {
                    return Err("stale (no frames)".to_string());
                }
                let _ = write.send(Message::Text("{}".into())).await;
            }
            msg = timeout(Duration::from_secs(60), read.next()) => {
                match msg {
                    Ok(Some(Ok(m))) => {
                        last_msg = Instant::now();
                        if let Ok(text) = m.into_text() {
                            for event in parse_user_events_text(&text) {
                                tx.send(event).await.map_err(|_| "event receiver dropped".to_string())?;
                            }
                        }
                    }
                    Ok(Some(Err(e))) => return Err(format!("ws read: {e}")),
                    Ok(None) => return Err("ws closed".to_string()),
                    Err(_) => return Err("read timeout".to_string()),
                }
            }
        }
    }
}

fn parse_number(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(0.0)
}

fn parse_ts_s(s: &str) -> Option<f64> {
    let v = s.parse::<f64>().ok()?;
    (v > 0.0).then_some(v)
}

fn parse_value_ts_s(v: &serde_json::Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_json_uses_user_auth_shape() {
        let msg = SubscribeMsg {
            auth: SubscribeAuth {
                api_key: "key".to_string(),
                secret: "secret".to_string(),
                passphrase: "pass".to_string(),
            },
            msg_type: "user",
            markets: vec!["0xabc".to_string()],
        };
        let json = serde_json::to_value(msg).unwrap();
        assert_eq!(json["type"], "user");
        assert_eq!(json["auth"]["apiKey"], "key");
        assert_eq!(json["markets"][0], "0xabc");
    }

    #[test]
    fn parses_order_event() {
        let text = r#"{
          "event_type": "order",
          "id": "0xorder",
          "market": "0xmarket",
          "asset_id": "123",
          "side": "SELL",
          "original_size": "10",
          "size_matched": "4",
          "price": "0.57",
          "type": "UPDATE",
          "status": "LIVE",
          "timestamp": "1672290687"
        }"#;
        let events = parse_user_events_text(text);
        assert_eq!(events.len(), 1);
        let UserEvent::Order(order) = &events[0] else {
            panic!("order event")
        };
        assert_eq!(order.id, "0xorder");
        assert_eq!(order.size_matched(), 4.0);
        assert!(!order.is_canceled());
    }

    #[test]
    fn parses_trade_event_candidate_order_ids() {
        let text = r#"[{
          "event_type": "trade",
          "type": "TRADE",
          "id": "trade-1",
          "taker_order_id": "0xtaker",
          "price": "0.57",
          "size": "10",
          "fee_rate_bps": "30",
          "status": "MATCHED",
          "timestamp": "1672290701",
          "maker_orders": [{"order_id": "0xmaker", "matched_amount": "10", "price": "0.57"}]
        }]"#;
        let events = parse_user_events_text(text);
        assert_eq!(events.len(), 1);
        let UserEvent::Trade(trade) = &events[0] else {
            panic!("trade event")
        };
        assert!(trade.is_fill_status());
        assert_eq!(
            trade.candidate_order_ids(),
            vec!["0xtaker".to_string(), "0xmaker".to_string()]
        );
        assert!((trade.fee() - 0.0171).abs() < 1e-9);
    }
}
