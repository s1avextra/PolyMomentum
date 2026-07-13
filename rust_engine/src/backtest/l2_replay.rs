//! Event-driven L2 replay engine.
//!
//! Walks PMXT v2 events for a token universe, maintains a per-token in-memory
//! book, and runs a strategy callback on every event. Pending orders fire
//! after a configurable static latency window — and **before** the
//! current event's update is applied, to prevent lookahead from
//! same-instant book changes.

use std::collections::{BTreeMap, VecDeque};

use crate::backtest::fill_model::{
    BookWalkTaker, FillReason, FillResult, Maker, OneTickTaker, OrderType, Perfect, Side,
};
use crate::backtest::pmxt::{L2Event, L2EventBody};
use crate::execution::fees::polymarket_fee;

#[derive(Debug, Clone, Copy)]
pub struct StaticLatencyConfig {
    /// Time it takes for an order to reach the book after the strategy fires.
    pub insert_ms: u64,
}

impl Default for StaticLatencyConfig {
    fn default() -> Self {
        // Conservative Dublin-VPS-to-Polymarket-CLOB round trip.
        Self { insert_ms: 50 }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TokenBook {
    pub bids: BTreeMap<u64, f64>, // price * 1e9 → size, sorted ascending
    pub asks: BTreeMap<u64, f64>,
    pub best_bid: f64,
    pub best_ask: f64,
    pub last_update_ts_s: f64,
}

fn key(p: f64) -> u64 {
    (p * 1e9).round() as u64
}

impl TokenBook {
    pub fn apply_snapshot(&mut self, snap: &crate::backtest::pmxt::BookSnapshot) {
        self.bids.clear();
        self.asks.clear();
        for lv in &snap.bids {
            if lv.size > 0.0 {
                self.bids.insert(key(lv.price), lv.size);
            }
        }
        for lv in &snap.asks {
            if lv.size > 0.0 {
                self.asks.insert(key(lv.price), lv.size);
            }
        }
        self.best_bid = if snap.best_bid > 0.0 {
            snap.best_bid
        } else {
            self.bids
                .keys()
                .next_back()
                .map(|k| *k as f64 / 1e9)
                .unwrap_or(0.0)
        };
        self.best_ask = if snap.best_ask > 0.0 {
            snap.best_ask
        } else {
            self.asks
                .keys()
                .next()
                .map(|k| *k as f64 / 1e9)
                .unwrap_or(0.0)
        };
        self.last_update_ts_s = snap.timestamp_s;
    }

    pub fn apply_change(&mut self, chg: &crate::backtest::pmxt::PriceChange) {
        let side = if chg.change_side.is_empty() {
            &chg.side
        } else {
            &chg.change_side
        };
        let book = if side.eq_ignore_ascii_case("buy") || side.eq_ignore_ascii_case("b") {
            &mut self.bids
        } else {
            &mut self.asks
        };
        if chg.change_size <= 0.0 {
            book.remove(&key(chg.change_price));
        } else {
            book.insert(key(chg.change_price), chg.change_size);
        }

        self.best_bid = self
            .bids
            .keys()
            .next_back()
            .map(|top| *top as f64 / 1e9)
            .unwrap_or(0.0);
        self.best_ask = self
            .asks
            .keys()
            .next()
            .map(|top| *top as f64 / 1e9)
            .unwrap_or(0.0);
        if chg.best_bid > 0.0 {
            self.best_bid = chg.best_bid;
        }
        if chg.best_ask > 0.0 {
            self.best_ask = chg.best_ask;
        }
        self.last_update_ts_s = chg.timestamp_s;
    }

    pub fn mid(&self) -> f64 {
        if self.best_bid > 0.0 && self.best_ask > 0.0 {
            (self.best_bid + self.best_ask) / 2.0
        } else {
            0.0
        }
    }

    pub fn ask_levels(&self) -> Vec<(f64, f64)> {
        self.asks
            .iter()
            .map(|(k, s)| (*k as f64 / 1e9, *s))
            .collect()
    }

    pub fn bid_levels(&self) -> Vec<(f64, f64)> {
        self.bids
            .iter()
            .rev()
            .map(|(k, s)| (*k as f64 / 1e9, *s))
            .collect()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BacktestOrder {
    pub intent_id: String,
    pub timestamp_s: f64,
    pub condition_id: String, // for resolver linkage
    pub token_id: String,
    pub side: String, // "buy" or "sell"
    pub size: f64,
    pub order_type: String, // "market" or "limit"
    pub limit_price: Option<f64>,
    pub fee_rate: f64,
    pub maker_fee_rate: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BacktestFill {
    pub order: BacktestOrder,
    pub fill_timestamp_s: f64,
    pub fill_price: f64,
    pub filled_size: f64,
    pub cost: f64,
    pub fee: f64,
    pub slippage: f64,
    pub book_age_ms: f64,
    pub success: bool,
    pub reason: String,
}

/// Strategy callback: called on every event after the per-tick book has
/// been updated (and any due fills flushed). Should return any orders the
/// strategy wants to place at this instant.
pub trait Strategy {
    fn needs_l2_history(&self) -> bool {
        true
    }

    fn on_fills(&mut self, _fills: &[BacktestFill]) {}

    fn on_event(
        &mut self,
        timestamp_s: f64,
        token_id: &str,
        book: &TokenBook,
        history: &L2MidHistory,
    ) -> Vec<BacktestOrder>;
}

pub type L2MidHistory = BTreeMap<String, VecDeque<(f64, f64)>>;

/// Pluggable fill model. Each variant of this enum is called from the
/// backtest engine when an order's latency window has elapsed.
pub enum FillModel {
    /// Executable taker behavior using only visible L2 depth.
    BookWalkTaker(BookWalkTaker),
    /// Probabilistic post-only-style maker. Limit orders must rest, crossing
    /// limits reject, and unfilled maker quotes do not auto-fallback to taker.
    /// Maker fills use `maker_fee_rate`; explicit market orders still use
    /// taker fallback semantics and `fee_rate`.
    Maker(Box<Maker>),
    /// Touch fill, no slippage. Sanity baseline only — not realistic.
    Perfect(Perfect),
}

impl FillModel {
    pub fn fill(
        &mut self,
        order_key: &str,
        side: Side,
        size: f64,
        book: &TokenBook,
        order_type: OrderType,
        limit_price: Option<f64>,
    ) -> FillResult {
        match self {
            FillModel::BookWalkTaker(m) => match order_type {
                OrderType::Market => {
                    let bids = book.bid_levels();
                    let asks = book.ask_levels();
                    m.fill(side, size, &bids, &asks, limit_price)
                }
                OrderType::Limit => OneTickTaker {
                    tick_size: m.tick_size,
                }
                .fill(
                    side,
                    size,
                    book.best_bid,
                    book.best_ask,
                    order_type,
                    limit_price,
                ),
            },
            FillModel::Maker(m) => m.fill_with_key(
                side,
                size,
                book.best_bid,
                book.best_ask,
                order_type,
                limit_price,
                Some(order_key),
            ),
            FillModel::Perfect(m) => m.fill(side, size, book.best_bid, book.best_ask),
        }
    }
}

pub struct L2BacktestEngine {
    fill_model: FillModel,
    latency: StaticLatencyConfig,
    history_window_seconds: f64,

    books: BTreeMap<String, TokenBook>,
    history: L2MidHistory,
    pending_orders: Vec<BacktestOrder>,
    pub fills: Vec<BacktestFill>,
    pub event_count: u64,
}

impl L2BacktestEngine {
    pub fn new(fill_model: FillModel, latency: StaticLatencyConfig) -> Self {
        Self {
            fill_model,
            latency,
            history_window_seconds: 300.0,
            books: BTreeMap::new(),
            history: BTreeMap::new(),
            pending_orders: Vec::new(),
            fills: Vec::new(),
            event_count: 0,
        }
    }

    /// Replay `events` (assumed sorted by timestamp). Drives the strategy.
    pub fn replay<S: Strategy>(
        &mut self,
        events: impl IntoIterator<Item = L2Event>,
        strategy: &mut S,
        default_fee_rate: f64,
    ) {
        let needs_l2_history = strategy.needs_l2_history();
        for event in events {
            self.event_count += 1;
            let token_id = match &event.body {
                L2EventBody::BookSnapshot(s) => s.token_id.as_str(),
                L2EventBody::PriceChange(c) => c.token_id.as_str(),
            };
            if token_id.is_empty() {
                continue;
            }

            // Flush due fills BEFORE applying the new event — same lookahead
            // guard as the Python engine.
            let fills = self.flush_pending_orders(event.timestamp_s);
            if !fills.is_empty() {
                strategy.on_fills(&fills);
            }

            let mid = {
                let book = self.books.entry(token_id.to_string()).or_default();
                match &event.body {
                    L2EventBody::BookSnapshot(s) => book.apply_snapshot(s),
                    L2EventBody::PriceChange(c) => book.apply_change(c),
                }
                book.mid()
            };
            if needs_l2_history && mid > 0.0 {
                self.record_history(token_id, event.timestamp_s, mid);
            }
            let book = self
                .books
                .get(token_id)
                .expect("book inserted before strategy callback");
            let new_orders = strategy.on_event(event.timestamp_s, token_id, book, &self.history);
            for mut order in new_orders {
                if order.fee_rate == 0.0 {
                    order.fee_rate = default_fee_rate;
                }
                self.pending_orders.push(order);
            }
        }
    }

    fn record_history(&mut self, token_id: &str, ts: f64, mid: f64) {
        let entry = self.history.entry(token_id.to_string()).or_default();
        if entry.back().is_some_and(|(last_ts, _)| ts - *last_ts < 0.1) {
            return;
        }
        entry.push_back((ts, mid));
        let cutoff = ts - self.history_window_seconds;
        while entry
            .front()
            .is_some_and(|(point_ts, _)| *point_ts < cutoff)
        {
            entry.pop_front();
        }
    }

    fn flush_pending_orders(&mut self, current_ts: f64) -> Vec<BacktestFill> {
        let mut emitted = Vec::new();
        if self.pending_orders.is_empty() {
            return emitted;
        }
        let latency_s = self.latency.insert_ms as f64 / 1000.0;
        let mut still_pending: Vec<BacktestOrder> = Vec::new();
        let drained: Vec<BacktestOrder> = self.pending_orders.drain(..).collect();
        for order in drained {
            if current_ts - order.timestamp_s < latency_s {
                still_pending.push(order);
                continue;
            }
            let fill_ts = order.timestamp_s + latency_s;

            // Snapshot the book state we need so we don't hold the borrow.
            let book_snapshot = self.books.get(&order.token_id).cloned();
            let Some(book) = book_snapshot else {
                let fill = BacktestFill {
                    order,
                    fill_timestamp_s: fill_ts,
                    fill_price: 0.0,
                    filled_size: 0.0,
                    cost: 0.0,
                    fee: 0.0,
                    slippage: 0.0,
                    book_age_ms: 0.0,
                    success: false,
                    reason: "no book at fill time".to_string(),
                };
                self.fills.push(fill.clone());
                emitted.push(fill);
                continue;
            };
            if book.best_bid <= 0.0 || book.best_ask <= 0.0 {
                let fill = BacktestFill {
                    order,
                    fill_timestamp_s: fill_ts,
                    fill_price: 0.0,
                    filled_size: 0.0,
                    cost: 0.0,
                    fee: 0.0,
                    slippage: 0.0,
                    book_age_ms: 0.0,
                    success: false,
                    reason: "no book at fill time".to_string(),
                };
                self.fills.push(fill.clone());
                emitted.push(fill);
                continue;
            }

            let side = match Side::parse(&order.side) {
                Some(s) => s,
                None => {
                    let fill = BacktestFill {
                        order,
                        fill_timestamp_s: fill_ts,
                        fill_price: 0.0,
                        filled_size: 0.0,
                        cost: 0.0,
                        fee: 0.0,
                        slippage: 0.0,
                        book_age_ms: 0.0,
                        success: false,
                        reason: "invalid side".to_string(),
                    };
                    self.fills.push(fill.clone());
                    emitted.push(fill);
                    continue;
                }
            };
            let order_type = match order.order_type.as_str() {
                "limit" => OrderType::Limit,
                _ => OrderType::Market,
            };
            let fill_key = maker_fill_key(&order);
            let result: FillResult = self.fill_model.fill(
                &fill_key,
                side,
                order.size,
                &book,
                order_type,
                order.limit_price,
            );
            let book_age_ms = ((fill_ts - book.last_update_ts_s) * 1000.0).max(0.0);
            if !result.success {
                let fill = BacktestFill {
                    order,
                    fill_timestamp_s: fill_ts,
                    fill_price: 0.0,
                    filled_size: 0.0,
                    cost: 0.0,
                    fee: 0.0,
                    slippage: 0.0,
                    book_age_ms,
                    success: false,
                    reason: result.reason.as_str().to_string(),
                };
                self.fills.push(fill.clone());
                emitted.push(fill);
                continue;
            }
            let effective_rate = if matches!(result.reason, FillReason::MakerFill) {
                order.maker_fee_rate
            } else {
                order.fee_rate
            };
            let fee = polymarket_fee(result.filled_size, result.fill_price, effective_rate);
            let fill = BacktestFill {
                order,
                fill_timestamp_s: fill_ts,
                fill_price: result.fill_price,
                filled_size: result.filled_size,
                cost: result.fill_cost,
                fee,
                slippage: result.slippage_per_share,
                book_age_ms,
                success: true,
                reason: result.reason.as_str().to_string(),
            };
            self.fills.push(fill.clone());
            emitted.push(fill);
        }
        self.pending_orders = still_pending;
        emitted
    }

    pub fn summary(&self) -> Summary {
        let successful: Vec<&BacktestFill> = self.fills.iter().filter(|f| f.success).collect();
        let total_cost: f64 = successful.iter().map(|f| f.cost.abs()).sum();
        let total_fees: f64 = successful.iter().map(|f| f.fee).sum();
        let avg_slip = if successful.is_empty() {
            0.0
        } else {
            successful.iter().map(|f| f.slippage).sum::<f64>() / successful.len() as f64
        };
        let avg_book_age = if successful.is_empty() {
            0.0
        } else {
            successful.iter().map(|f| f.book_age_ms).sum::<f64>() / successful.len() as f64
        };
        Summary {
            events_processed: self.event_count,
            fills_total: self.fills.len() as u64,
            fills_success: successful.len() as u64,
            fills_failed: (self.fills.len() - successful.len()) as u64,
            total_cost,
            total_fees,
            avg_slippage: avg_slip,
            avg_book_age_ms: avg_book_age,
            tokens_tracked: self.books.len() as u64,
        }
    }
}

fn maker_fill_key(order: &BacktestOrder) -> String {
    format!(
        "{}:{:.6}:{}:{}:{}:{:.8}:{:.8}",
        order.condition_id,
        order.timestamp_s,
        order.token_id,
        order.side,
        order.order_type,
        order.limit_price.unwrap_or(0.0),
        order.size,
    )
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Summary {
    pub events_processed: u64,
    pub fills_total: u64,
    pub fills_success: u64,
    pub fills_failed: u64,
    pub total_cost: f64,
    pub total_fees: f64,
    pub avg_slippage: f64,
    pub avg_book_age_ms: f64,
    pub tokens_tracked: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::fill_model::{Maker, DEFAULT_TICK};
    use crate::backtest::pmxt::{BookSnapshot, L2Level, PriceChange};
    use crate::data::models::DEFAULT_CRYPTO_TAKER_FEE_RATE;

    struct NoopStrategy;
    impl Strategy for NoopStrategy {
        fn on_event(
            &mut self,
            _ts: f64,
            _tok: &str,
            _book: &TokenBook,
            _history: &L2MidHistory,
        ) -> Vec<BacktestOrder> {
            Vec::new()
        }
    }

    fn snap_event(token: &str, ts: f64, bid: f64, ask: f64) -> L2Event {
        L2Event {
            timestamp_s: ts,
            market_id: "m".to_string(),
            body: L2EventBody::BookSnapshot(BookSnapshot {
                market_id: "m".to_string(),
                token_id: token.to_string(),
                best_bid: bid,
                best_ask: ask,
                timestamp_s: ts,
                bids: vec![L2Level {
                    price: bid,
                    size: 100.0,
                }],
                asks: vec![L2Level {
                    price: ask,
                    size: 100.0,
                }],
            }),
        }
    }

    #[test]
    fn empty_replay_produces_no_fills() {
        let mut e = L2BacktestEngine::new(
            FillModel::BookWalkTaker(BookWalkTaker::default()),
            StaticLatencyConfig::default(),
        );
        let mut s = NoopStrategy;
        e.replay(
            std::iter::empty::<L2Event>(),
            &mut s,
            DEFAULT_CRYPTO_TAKER_FEE_RATE,
        );
        assert_eq!(e.fills.len(), 0);
    }

    #[test]
    fn book_snapshot_updates_top_of_book() {
        let mut e = L2BacktestEngine::new(
            FillModel::BookWalkTaker(BookWalkTaker::default()),
            StaticLatencyConfig::default(),
        );
        let mut s = NoopStrategy;
        e.replay(
            vec![snap_event("t", 1.0, 0.50, 0.52)],
            &mut s,
            DEFAULT_CRYPTO_TAKER_FEE_RATE,
        );
        let book = e.books.get("t").unwrap();
        assert!((book.best_bid - 0.50).abs() < 1e-9);
        assert!((book.best_ask - 0.52).abs() < 1e-9);
    }

    #[test]
    fn removing_last_level_clears_replay_touch() {
        let mut book = TokenBook::default();
        if let L2EventBody::BookSnapshot(snapshot) = snap_event("t", 1.0, 0.50, 0.52).body {
            book.apply_snapshot(&snapshot);
        }
        book.apply_change(&PriceChange {
            market_id: "m".to_string(),
            token_id: "t".to_string(),
            side: "sell".to_string(),
            change_side: "sell".to_string(),
            change_price: 0.52,
            change_size: 0.0,
            best_bid: 0.0,
            best_ask: 0.0,
            timestamp_s: 1.1,
        });

        assert_eq!(book.best_ask, 0.0);
        assert!(book.asks.is_empty());
    }

    struct OneShotBuy {
        fired: bool,
    }
    impl Strategy for OneShotBuy {
        fn on_event(
            &mut self,
            ts: f64,
            tok: &str,
            book: &TokenBook,
            _h: &L2MidHistory,
        ) -> Vec<BacktestOrder> {
            if self.fired || book.best_ask <= 0.0 {
                return Vec::new();
            }
            self.fired = true;
            vec![BacktestOrder {
                intent_id: "test-intent".into(),
                timestamp_s: ts,
                condition_id: "c".into(),
                token_id: tok.into(),
                side: "buy".into(),
                size: 10.0,
                order_type: "market".into(),
                limit_price: None,
                fee_rate: DEFAULT_CRYPTO_TAKER_FEE_RATE,
                maker_fee_rate: 0.0,
            }]
        }
    }

    #[test]
    fn order_fires_after_latency_window() {
        let mut e = L2BacktestEngine::new(
            FillModel::BookWalkTaker(BookWalkTaker::default()),
            StaticLatencyConfig { insert_ms: 50 },
        );
        let mut s = OneShotBuy { fired: false };
        let events = vec![
            snap_event("t", 1.0, 0.50, 0.52),  // strategy fires here (ts=1.0)
            snap_event("t", 1.04, 0.50, 0.52), // 40ms later — still within latency, no fill
            snap_event("t", 1.10, 0.50, 0.52), // 100ms after order — past 50ms, flush should fire
        ];
        e.replay(events, &mut s, DEFAULT_CRYPTO_TAKER_FEE_RATE);
        assert_eq!(e.fills.len(), 1);
        let f = &e.fills[0];
        assert!(f.success);
        assert!((f.fill_price - 0.52).abs() < 1e-9); // ten shares fit at the visible ask
        assert!((f.fill_timestamp_s - 1.05).abs() < 1e-9);
    }

    #[test]
    fn l2_history_uses_live_ten_hertz_cadence() {
        let mut engine = L2BacktestEngine::new(
            FillModel::BookWalkTaker(BookWalkTaker::default()),
            StaticLatencyConfig { insert_ms: 50 },
        );
        engine.record_history("t", 1.0, 0.50);
        engine.record_history("t", 1.05, 0.60);
        engine.record_history("t", 1.10, 0.70);

        let history = engine.history.get("t").unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history.front(), Some(&(1.0, 0.50)));
        assert_eq!(history.back(), Some(&(1.10, 0.70)));
    }

    struct FillHookStrategy {
        inner: OneShotBuy,
        seen_fills: usize,
    }

    impl Strategy for FillHookStrategy {
        fn on_fills(&mut self, fills: &[BacktestFill]) {
            self.seen_fills += fills.len();
        }

        fn on_event(
            &mut self,
            ts: f64,
            tok: &str,
            book: &TokenBook,
            h: &L2MidHistory,
        ) -> Vec<BacktestOrder> {
            self.inner.on_event(ts, tok, book, h)
        }
    }

    #[test]
    fn strategy_receives_fill_callbacks_after_latency_flush() {
        let mut e = L2BacktestEngine::new(
            FillModel::BookWalkTaker(BookWalkTaker::default()),
            StaticLatencyConfig { insert_ms: 50 },
        );
        let mut s = FillHookStrategy {
            inner: OneShotBuy { fired: false },
            seen_fills: 0,
        };
        e.replay(
            vec![
                snap_event("t", 1.0, 0.50, 0.52),
                snap_event("t", 1.10, 0.50, 0.52),
            ],
            &mut s,
            DEFAULT_CRYPTO_TAKER_FEE_RATE,
        );
        assert_eq!(e.fills.len(), 1);
        assert_eq!(s.seen_fills, 1);
    }

    struct OneShotMakerBuy {
        fired: bool,
    }
    impl Strategy for OneShotMakerBuy {
        fn on_event(
            &mut self,
            ts: f64,
            tok: &str,
            book: &TokenBook,
            _h: &L2MidHistory,
        ) -> Vec<BacktestOrder> {
            if self.fired || book.best_ask <= 0.0 {
                return Vec::new();
            }
            self.fired = true;
            vec![BacktestOrder {
                intent_id: "maker-intent".into(),
                timestamp_s: ts,
                condition_id: "c".into(),
                token_id: tok.into(),
                side: "buy".into(),
                size: 10.0,
                order_type: "limit".into(),
                limit_price: Some(book.best_ask - DEFAULT_TICK),
                fee_rate: DEFAULT_CRYPTO_TAKER_FEE_RATE,
                maker_fee_rate: 0.0,
            }]
        }
    }

    #[test]
    fn maker_limit_rejects_if_quote_crosses_during_latency() {
        let mut e = L2BacktestEngine::new(
            FillModel::Maker(Box::new(Maker::new(1.0, DEFAULT_TICK, Some(42)))),
            StaticLatencyConfig { insert_ms: 50 },
        );
        let mut s = OneShotMakerBuy { fired: false };
        let events = vec![
            snap_event("t", 1.0, 0.50, 0.52),
            snap_event("t", 1.04, 0.50, 0.51),
            snap_event("t", 1.10, 0.50, 0.51),
        ];
        e.replay(events, &mut s, DEFAULT_CRYPTO_TAKER_FEE_RATE);

        assert_eq!(e.fills.len(), 1);
        let f = &e.fills[0];
        assert!(!f.success);
        assert_eq!(f.reason, "post_only_cross");
    }
}
