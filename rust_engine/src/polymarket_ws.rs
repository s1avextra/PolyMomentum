//! Polymarket L2 book WebSocket feed.
//!
//! Subscribes to real-time order book updates for active candle token_ids.
//! Stores full L2 (bids + asks per token) so paper fills can walk the same
//! book the live exchange would see — closing the price-staleness gap that
//! drove the 2026-04-26 Rust port.
//!
//! Resubscribes whenever the tracked-tokens list changes (i.e. the cycle
//! loop's contract refresh injects a new candle window). Prior version
//! waited for a disconnect to reload the list.
//!
//! WS protocol:
//!   Subscribe: {"type":"market","assets_ids":[...]}
//!   Snapshot:  {"event_type":"book","asset_id":..., "bids":[...], "asks":[...]}
//!   Tick:      {"event_type":"price_change","price_changes":[...]}
//!
//! The parser also accepts the legacy `{"type": ..., "data": ...}` envelope so
//! old captures remain replayable in tests and diagnostics.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock};
use tokio::time::{timeout, Instant};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_INTERVAL: Duration = Duration::from_secs(10);
const STALE_AFTER: Duration = Duration::from_secs(60);
const MID_HISTORY_MAX_AGE_US: u64 = 300_000_000;
const MID_HISTORY_MIN_STEP_US: u64 = 100_000;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenBookState {
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid: f64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    /// Polymarket source event time, not local receipt time.
    pub last_update_us: u64,
    /// Latest market-channel tick-size value for this token.
    #[serde(default)]
    pub tick_size: Option<f64>,
    /// Source event time for `tick_size`; kept separate from book freshness.
    #[serde(default)]
    pub tick_update_us: u64,
    #[serde(skip)]
    pub mid_history: VecDeque<(f64, f64)>,
}

pub type SharedBookState = Arc<RwLock<HashMap<String, TokenBookState>>>;

pub fn new_shared_book() -> SharedBookState {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Notifier handed back to the cycle loop. Call `notify_one` after mutating
/// `tracked_tokens` to make the WS feed reconnect with the new subscription.
pub fn new_subscription_notify() -> Arc<Notify> {
    Arc::new(Notify::new())
}

#[derive(Serialize)]
struct SubscribeMsg {
    #[serde(rename = "type")]
    msg_type: &'static str,
    assets_ids: Vec<String>,
}

#[derive(Deserialize)]
struct BookSnapshot {
    asset_id: Option<String>,
    bids: Option<Vec<RawLevel>>,
    asks: Option<Vec<RawLevel>>,
}

#[derive(Deserialize)]
struct PriceChange {
    asset_id: Option<String>,
    changes: Option<Vec<ChangeEntry>>,
    price_changes: Option<Vec<ChangeEntry>>,
}

#[derive(Deserialize)]
struct ChangeEntry {
    asset_id: Option<String>,
    price: String,
    side: String, // "BUY" or "SELL"
    size: String,
    best_bid: Option<String>,
    best_ask: Option<String>,
}

#[derive(Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

fn parse_levels(raw: &Option<Vec<RawLevel>>, descending: bool) -> Vec<BookLevel> {
    let Some(levels) = raw else { return Vec::new() };
    let mut out: Vec<BookLevel> = levels
        .iter()
        .filter_map(|l| {
            let p = l.price.parse::<f64>().ok()?;
            let s = l.size.parse::<f64>().ok()?;
            if p > 0.0 {
                Some(BookLevel { price: p, size: s })
            } else {
                None
            }
        })
        .collect();
    if descending {
        out.sort_by(|a, b| {
            b.price
                .partial_cmp(&a.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    } else {
        out.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    out
}

/// Diagnostic counters for the market ws intake (cumulative since start).
pub static WS_APPLIED_BOOK: AtomicU64 = AtomicU64::new(0);
pub static WS_APPLIED_PRICE_CHANGE: AtomicU64 = AtomicU64::new(0);
pub static WS_APPLIED_BEST_BID_ASK: AtomicU64 = AtomicU64::new(0);
pub static WS_DROPPED_NO_TIMESTAMP: AtomicU64 = AtomicU64::new(0);
pub static WS_DROPPED_PARSE: AtomicU64 = AtomicU64::new(0);
pub static WS_DROPPED_STALE_ORDER: AtomicU64 = AtomicU64::new(0);
pub static WS_FRAMES: AtomicU64 = AtomicU64::new(0);

pub fn ws_counters() -> [u64; 7] {
    [
        WS_FRAMES.load(Ordering::Relaxed),
        WS_APPLIED_BOOK.load(Ordering::Relaxed),
        WS_APPLIED_PRICE_CHANGE.load(Ordering::Relaxed),
        WS_APPLIED_BEST_BID_ASK.load(Ordering::Relaxed),
        WS_DROPPED_NO_TIMESTAMP.load(Ordering::Relaxed),
        WS_DROPPED_PARSE.load(Ordering::Relaxed),
        WS_DROPPED_STALE_ORDER.load(Ordering::Relaxed),
    ]
}

fn source_timestamp_us(data: &serde_json::Value) -> Option<u64> {
    fn float_ms_to_us(timestamp_ms: f64) -> Option<u64> {
        let timestamp_us = timestamp_ms * 1_000.0;
        if !timestamp_us.is_finite() || timestamp_us <= 0.0 || timestamp_us > u64::MAX as f64 {
            return None;
        }
        Some(timestamp_us.round() as u64)
    }

    let parsed = match data.get("timestamp")? {
        serde_json::Value::String(raw) => raw
            .parse::<u64>()
            .ok()
            .and_then(|timestamp_ms| timestamp_ms.checked_mul(1_000))
            .or_else(|| raw.parse::<f64>().ok().and_then(float_ms_to_us)),
        serde_json::Value::Number(number) => number
            .as_u64()
            .and_then(|timestamp_ms| timestamp_ms.checked_mul(1_000))
            .or_else(|| number.as_f64().and_then(float_ms_to_us)),
        _ => None,
    }?;
    Some(clamp_timestamp_us_to_local(parsed))
}

/// A single venue message whose timestamp is mis-scaled (e.g. already in
/// microseconds) would set `last_update_us` far into the future and make the
/// monotonicity guard reject every later legitimate update for that token
/// until restart. Freshness is judged against the local clock, so clamp any
/// implausible venue timestamp to "received now" instead of trusting it.
fn clamp_timestamp_us_to_local(timestamp_us: u64) -> u64 {
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(timestamp_us);
    let lower = now_us.saturating_sub(60 * 60 * 1_000_000);
    let upper = now_us.saturating_add(10 * 1_000_000);
    if (lower..=upper).contains(&timestamp_us) {
        timestamp_us
    } else {
        now_us
    }
}

fn record_mid_history(state: &mut TokenBookState, timestamp_us: u64) {
    if state.mid <= 0.0 || !state.mid.is_finite() {
        return;
    }
    if state.mid_history.back().is_some_and(|(last_ts, _)| {
        timestamp_us as f64 / 1_000_000.0 - *last_ts < MID_HISTORY_MIN_STEP_US as f64 / 1_000_000.0
    }) {
        return;
    }
    let timestamp_s = timestamp_us as f64 / 1_000_000.0;
    state.mid_history.push_back((timestamp_s, state.mid));
    let cutoff = timestamp_us.saturating_sub(MID_HISTORY_MAX_AGE_US) as f64 / 1_000_000.0;
    while state
        .mid_history
        .front()
        .is_some_and(|(ts, _)| *ts < cutoff)
    {
        state.mid_history.pop_front();
    }
}

fn apply_price_change(state: &mut TokenBookState, changes: &[ChangeEntry]) {
    for ch in changes {
        let Ok(price) = ch.price.parse::<f64>() else {
            continue;
        };
        let Ok(size) = ch.size.parse::<f64>() else {
            continue;
        };
        let descending = matches!(ch.side.as_str(), "BUY");
        let levels = if descending {
            &mut state.bids
        } else {
            &mut state.asks
        };
        if let Some(idx) = levels.iter().position(|l| (l.price - price).abs() < 1e-9) {
            if size <= 0.0 {
                levels.remove(idx);
            } else {
                levels[idx].size = size;
            }
        } else if size > 0.0 {
            levels.push(BookLevel { price, size });
            if descending {
                levels.sort_by(|a, b| {
                    b.price
                        .partial_cmp(&a.price)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            } else {
                levels.sort_by(|a, b| {
                    a.price
                        .partial_cmp(&b.price)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }
    }
    state.best_bid = state.bids.first().map(|l| l.price).unwrap_or(0.0);
    state.best_ask = state.asks.first().map(|l| l.price).unwrap_or(0.0);
    state.mid = if state.best_bid > 0.0 && state.best_ask > 0.0 {
        (state.best_bid + state.best_ask) / 2.0
    } else if state.best_bid > 0.0 {
        state.best_bid
    } else {
        state.best_ask
    };
}

/// Run the Polymarket WS feed.
///
/// `tracked_tokens` is read on each (re)connect; mutate it from outside and
/// call `resubscribe.notify_one()` to force a reconnect with the new set.
/// Wholesale-replace one token's ladder from the venue's REST `/book`
/// response, bypassing the per-token monotonicity guard.
///
/// The guard is what let a corrupted ladder stay live: a delta gap under an
/// update storm (2026-08-26: the mirror held asks the venue had cancelled a
/// minute earlier and missed the real 0.47-0.55 ladder) leaves
/// `last_update_us` fresh - later deltas at other prices keep stamping it -
/// while the levels are wrong, so no timestamp check can detect the state.
/// The REST body is the venue's authoritative current book; replacing with it
/// heals any gap. `last_update_us` takes the snapshot's own venue timestamp,
/// so deltas newer than the snapshot keep applying on top.
///
/// Returns (previous_best_ask, new_best_ask) for divergence accounting.
pub async fn overwrite_book_from_rest(
    book_state: &SharedBookState,
    body: &serde_json::Value,
) -> Option<(f64, f64)> {
    let asset_id = body.get("asset_id")?.as_str()?.to_string();
    let to_levels = |key: &str| -> Option<Vec<RawLevel>> {
        Some(
            body.get(key)?
                .as_array()?
                .iter()
                .filter_map(|l| {
                    Some(RawLevel {
                        price: l.get("price")?.as_str()?.to_string(),
                        size: l.get("size")?.as_str()?.to_string(),
                    })
                })
                .collect(),
        )
    };
    let bids = parse_levels(&to_levels("bids"), true);
    let asks = parse_levels(&to_levels("asks"), false);
    let timestamp_us = body
        .get("timestamp")
        .and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .or_else(|| v.as_u64())
        })
        .and_then(|ms| ms.checked_mul(1_000))
        .map(clamp_timestamp_us_to_local)?;
    let best_bid = bids.first().map(|l| l.price).unwrap_or(0.0);
    let best_ask = asks.first().map(|l| l.price).unwrap_or(0.0);
    let mut map = book_state.write().await;
    let state = map.entry(asset_id).or_default();
    let previous_best_ask = state.best_ask;
    state.best_bid = best_bid;
    state.best_ask = best_ask;
    state.mid = if best_bid > 0.0 && best_ask > 0.0 {
        (best_bid + best_ask) / 2.0
    } else if best_bid > 0.0 {
        best_bid
    } else {
        best_ask
    };
    state.bids = bids;
    state.asks = asks;
    state.last_update_us = timestamp_us;
    record_mid_history(state, timestamp_us);
    Some((previous_best_ask, best_ask))
}

pub async fn polymarket_book_feed(
    book_state: SharedBookState,
    tracked_tokens: Arc<RwLock<Vec<String>>>,
    resubscribe: Arc<Notify>,
) {
    let mut backoff = Duration::from_millis(500);

    loop {
        let ids = tracked_tokens.read().await.clone();
        if ids.is_empty() {
            tokio::select! {
                _ = resubscribe.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
            continue;
        }

        let session = run_session(&book_state, ids, &resubscribe).await;
        match session {
            Ok(()) => {
                backoff = Duration::from_millis(500);
            }
            Err(e) => {
                tracing::warn!(error = %e, "polymarket_ws session error; backing off");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

async fn run_session(
    book_state: &SharedBookState,
    ids: Vec<String>,
    resubscribe: &Arc<Notify>,
) -> Result<(), String> {
    let (ws, _) = connect_async(POLYMARKET_WS_URL)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let (mut write, mut read) = ws.split();
    let sub = SubscribeMsg {
        msg_type: "market",
        assets_ids: ids,
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
                tracing::info!("polymarket_ws resubscribe requested");
                return Ok(());
            }
            _ = ping_timer.tick() => {
                if last_msg.elapsed() > STALE_AFTER {
                    return Err("stale (no frames)".into());
                }
                let _ = write.send(Message::Ping(Vec::new().into())).await;
            }
            msg = timeout(Duration::from_secs(60), read.next()) => {
                match msg {
                    Ok(Some(Ok(m))) => {
                        last_msg = Instant::now();
                        handle_frame(book_state, m).await;
                    }
                    Ok(Some(Err(e))) => return Err(format!("ws read: {e}")),
                    Ok(None) => return Err("ws closed".into()),
                    Err(_) => return Err("read timeout".into()),
                }
            }
        }
    }
}

async fn handle_frame(book_state: &SharedBookState, m: Message) {
    WS_FRAMES.fetch_add(1, Ordering::Relaxed);
    let Ok(text) = m.into_text() else { return };
    // Polymarket sometimes sends arrays of messages.
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    if trimmed.starts_with('[') {
        let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(trimmed) else {
            return;
        };
        for msg in arr {
            apply_message_value(book_state, msg).await;
        }
    } else {
        let Ok(msg) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            return;
        };
        apply_message_value(book_state, msg).await;
    }
}

async fn apply_message_value(book_state: &SharedBookState, msg: serde_json::Value) {
    let msg_type = msg
        .get("event_type")
        .or_else(|| msg.get("type"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let Some(msg_type) = msg_type else { return };
    let data = msg.get("data").cloned().unwrap_or(msg);
    apply_typed_message(book_state, &msg_type, data).await;
}

async fn apply_typed_message(book_state: &SharedBookState, t: &str, data: serde_json::Value) {
    if !matches!(
        t,
        "book" | "price_change" | "best_bid_ask" | "tick_size_change"
    ) {
        return;
    }
    let Some(timestamp_us) = source_timestamp_us(&data) else {
        WS_DROPPED_NO_TIMESTAMP.fetch_add(1, Ordering::Relaxed);
        return;
    };

    match t {
        "tick_size_change" => {
            let asset_id = data
                .get("asset_id")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned);
            let tick_size = data.get("new_tick_size").and_then(parse_positive_price);
            let (Some(asset_id), Some(tick_size)) = (asset_id, tick_size) else {
                return;
            };
            let mut map = book_state.write().await;
            let state = map.entry(asset_id).or_default();
            if timestamp_us < state.tick_update_us {
                return;
            }
            state.tick_size = Some(tick_size);
            state.tick_update_us = timestamp_us;
        }
        "book" => {
            let Ok(snap): Result<BookSnapshot, _> = serde_json::from_value(data) else {
                WS_DROPPED_PARSE.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let Some(asset_id) = snap.asset_id else {
                WS_DROPPED_PARSE.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let bids = parse_levels(&snap.bids, true);
            let asks = parse_levels(&snap.asks, false);
            let best_bid = bids.first().map(|l| l.price).unwrap_or(0.0);
            let best_ask = asks.first().map(|l| l.price).unwrap_or(0.0);
            let mid = if best_bid > 0.0 && best_ask > 0.0 {
                (best_bid + best_ask) / 2.0
            } else if best_bid > 0.0 {
                best_bid
            } else {
                best_ask
            };
            let mut map = book_state.write().await;
            let state = map.entry(asset_id).or_default();
            if timestamp_us < state.last_update_us {
                WS_DROPPED_STALE_ORDER.fetch_add(1, Ordering::Relaxed);
                return;
            }
            state.best_bid = best_bid;
            state.best_ask = best_ask;
            state.mid = mid;
            state.bids = bids;
            state.asks = asks;
            state.last_update_us = timestamp_us;
            record_mid_history(state, timestamp_us);
            WS_APPLIED_BOOK.fetch_add(1, Ordering::Relaxed);
        }
        "price_change" => {
            let Ok(pc): Result<PriceChange, _> = serde_json::from_value(data) else {
                WS_DROPPED_PARSE.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let changes = pc.price_changes.or(pc.changes).unwrap_or_default();
            if changes.is_empty() {
                return;
            }
            WS_APPLIED_PRICE_CHANGE.fetch_add(1, Ordering::Relaxed);
            let mut map = book_state.write().await;
            for ch in changes {
                let Some(asset_id) = ch.asset_id.clone().or_else(|| pc.asset_id.clone()) else {
                    continue;
                };
                let entry = map.entry(asset_id).or_default();
                if timestamp_us < entry.last_update_us {
                    continue;
                }
                apply_price_change(entry, std::slice::from_ref(&ch));
                if let Some(best_bid) = ch.best_bid.as_deref().and_then(|v| v.parse::<f64>().ok()) {
                    entry.best_bid = best_bid;
                }
                if let Some(best_ask) = ch.best_ask.as_deref().and_then(|v| v.parse::<f64>().ok()) {
                    entry.best_ask = best_ask;
                }
                entry.mid = if entry.best_bid > 0.0 && entry.best_ask > 0.0 {
                    (entry.best_bid + entry.best_ask) / 2.0
                } else if entry.best_bid > 0.0 {
                    entry.best_bid
                } else {
                    entry.best_ask
                };
                entry.last_update_us = timestamp_us;
                record_mid_history(entry, timestamp_us);
            }
        }
        "best_bid_ask" => {
            let asset_id = data
                .get("asset_id")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned);
            let best_bid = data
                .get("best_bid")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            let best_ask = data
                .get("best_ask")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0);
            let Some(asset_id) = asset_id else { return };
            let mut map = book_state.write().await;
            let entry = map.entry(asset_id).or_default();
            if timestamp_us < entry.last_update_us {
                WS_DROPPED_STALE_ORDER.fetch_add(1, Ordering::Relaxed);
                return;
            }
            entry.best_bid = best_bid;
            entry.best_ask = best_ask;
            entry.mid = if best_bid > 0.0 && best_ask > 0.0 {
                (best_bid + best_ask) / 2.0
            } else if best_bid > 0.0 {
                best_bid
            } else {
                best_ask
            };
            entry.last_update_us = timestamp_us;
            record_mid_history(entry, timestamp_us);
            WS_APPLIED_BEST_BID_ASK.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

fn parse_positive_price(value: &serde_json::Value) -> Option<f64> {
    let parsed = match value {
        serde_json::Value::String(raw) => raw.parse::<f64>().ok(),
        serde_json::Value::Number(number) => number.as_f64(),
        _ => None,
    }?;
    (parsed.is_finite() && parsed > 0.0 && parsed < 1.0).then_some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_book_snapshot() {
        let levels = Some(vec![
            RawLevel {
                price: "0.51".into(),
                size: "100".into(),
            },
            RawLevel {
                price: "0.50".into(),
                size: "200".into(),
            },
        ]);
        let bids = parse_levels(&levels, true);
        assert_eq!(bids.len(), 2);
        assert!((bids[0].price - 0.51).abs() < 1e-9);
        assert!((bids[1].price - 0.50).abs() < 1e-9);
    }

    #[test]
    fn applies_price_change_inserts_and_removes() {
        let mut s = TokenBookState {
            bids: vec![BookLevel {
                price: 0.50,
                size: 100.0,
            }],
            asks: vec![BookLevel {
                price: 0.52,
                size: 50.0,
            }],
            ..TokenBookState::default()
        };
        let changes = vec![
            ChangeEntry {
                asset_id: None,
                price: "0.51".into(),
                side: "BUY".into(),
                size: "150".into(),
                best_bid: None,
                best_ask: None,
            },
            ChangeEntry {
                asset_id: None,
                price: "0.52".into(),
                side: "SELL".into(),
                size: "0".into(),
                best_bid: None,
                best_ask: None,
            },
        ];
        apply_price_change(&mut s, &changes);
        assert_eq!(s.bids.len(), 2);
        assert!((s.best_bid - 0.51).abs() < 1e-9);
        assert!(s.asks.is_empty());
    }

    #[test]
    fn subscription_uses_current_market_channel_shape() {
        let sub = SubscribeMsg {
            msg_type: "market",
            assets_ids: vec!["token-a".into()],
        };
        let json = serde_json::to_value(&sub).unwrap();
        assert_eq!(json["type"], "market");
        assert_eq!(json["assets_ids"][0], "token-a");
        assert!(json.get("channel").is_none());
    }

    #[tokio::test]
    async fn applies_current_flat_book_snapshot() {
        let book = new_shared_book();
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "book",
                "timestamp": "2000",
                "asset_id": "token-a",
                "bids": [{"price": "0.48", "size": "30"}],
                "asks": [{"price": "0.52", "size": "25"}],
            }),
        )
        .await;
        let state = book.read().await;
        let token = state.get("token-a").unwrap();
        assert!((token.best_bid - 0.48).abs() < 1e-9);
        assert!((token.best_ask - 0.52).abs() < 1e-9);
    }

    #[tokio::test]
    async fn applies_current_flat_price_change() {
        let book = new_shared_book();
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "price_change",
                "timestamp": 2001,
                "price_changes": [{
                    "asset_id": "token-a",
                    "price": "0.51",
                    "size": "10",
                    "side": "BUY",
                    "best_bid": "0.51",
                    "best_ask": "0.53"
                }],
            }),
        )
        .await;
        let state = book.read().await;
        let token = state.get("token-a").unwrap();
        assert!((token.best_bid - 0.51).abs() < 1e-9);
        assert!((token.best_ask - 0.53).abs() < 1e-9);
    }

    /// Millisecond timestamp within the plausibility window of
    /// `clamp_timestamp_us_to_local`, offset back from now by `back_ms`.
    fn recent_ms(back_ms: u64) -> u64 {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        now_ms - back_ms
    }

    #[tokio::test]
    async fn applies_current_best_bid_ask() {
        let ts_ms = recent_ms(2_000);
        let book = new_shared_book();
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "best_bid_ask",
                "timestamp": (ts_ms as f64) + 0.5,
                "asset_id": "token-a",
                "best_bid": "0.49",
                "best_ask": "0.51"
            }),
        )
        .await;
        let state = book.read().await;
        let token = state.get("token-a").unwrap();
        assert!((token.mid - 0.50).abs() < 1e-9);
        assert_eq!(token.last_update_us, ts_ms * 1_000 + 500);
    }

    #[tokio::test]
    async fn implausible_timestamp_is_clamped_and_cannot_poison_the_book() {
        let book = new_shared_book();
        // Microsecond-scaled venue timestamp: without the clamp this would
        // set last_update_us ~1000x into the future and freeze the token.
        let us_scaled = recent_ms(0) * 1_000;
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "book",
                "timestamp": us_scaled.to_string(),
                "asset_id": "token-a",
                "bids": [{"price": "0.20", "size": "30"}],
                "asks": [{"price": "0.80", "size": "25"}],
            }),
        )
        .await;
        let ts_ms = recent_ms(0) + 5;
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "book",
                "timestamp": ts_ms.to_string(),
                "asset_id": "token-a",
                "bids": [{"price": "0.48", "size": "30"}],
                "asks": [{"price": "0.52", "size": "25"}],
            }),
        )
        .await;
        let state = book.read().await;
        let token = state.get("token-a").unwrap();
        // The later legitimate update must win despite the poisoned first one.
        assert!((token.best_bid - 0.48).abs() < 1e-9);
        assert_eq!(token.last_update_us, ts_ms * 1_000);
    }

    #[tokio::test]
    async fn applies_latest_tick_size_change_without_refreshing_book_age() {
        let book_ms = recent_ms(30_000);
        let tick_new_ms = recent_ms(10_000);
        let tick_stale_ms = recent_ms(20_000);
        let book = new_shared_book();
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "book",
                "timestamp": book_ms.to_string(),
                "asset_id": "token-a",
                "bids": [{"price": "0.95", "size": "30"}],
                "asks": [{"price": "0.96", "size": "25"}],
            }),
        )
        .await;
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "tick_size_change",
                "timestamp": tick_new_ms.to_string(),
                "asset_id": "token-a",
                "market": "condition-a",
                "old_tick_size": "0.01",
                "new_tick_size": "0.001"
            }),
        )
        .await;
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "tick_size_change",
                "timestamp": tick_stale_ms.to_string(),
                "asset_id": "token-a",
                "market": "condition-a",
                "old_tick_size": "0.001",
                "new_tick_size": "0.01"
            }),
        )
        .await;

        let state = book.read().await;
        let token = state.get("token-a").unwrap();
        assert_eq!(token.tick_size, Some(0.001));
        assert_eq!(token.tick_update_us, tick_new_ms * 1_000);
        assert_eq!(token.last_update_us, book_ms * 1_000);
    }

    #[tokio::test]
    async fn stale_snapshot_cannot_overwrite_newer_book() {
        let newer_ms = recent_ms(5_000);
        let stale_ms = recent_ms(15_000);
        let book = new_shared_book();
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "book",
                "timestamp": newer_ms.to_string(),
                "asset_id": "token-a",
                "bids": [{"price": "0.48", "size": "30"}],
                "asks": [{"price": "0.52", "size": "25"}],
            }),
        )
        .await;
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "book",
                "timestamp": stale_ms.to_string(),
                "asset_id": "token-a",
                "bids": [{"price": "0.20", "size": "30"}],
                "asks": [{"price": "0.80", "size": "25"}],
            }),
        )
        .await;

        let state = book.read().await;
        let token = state.get("token-a").unwrap();
        assert!((token.best_bid - 0.48).abs() < 1e-9);
        assert!((token.best_ask - 0.52).abs() < 1e-9);
        assert_eq!(token.last_update_us, newer_ms * 1_000);
    }

    #[tokio::test]
    async fn missing_source_timestamp_is_ignored() {
        let book = new_shared_book();
        apply_message_value(
            &book,
            serde_json::json!({
                "event_type": "book",
                "asset_id": "token-a",
                "bids": [{"price": "0.48", "size": "30"}],
                "asks": [{"price": "0.52", "size": "25"}],
            }),
        )
        .await;

        assert!(book.read().await.is_empty());
    }
}
