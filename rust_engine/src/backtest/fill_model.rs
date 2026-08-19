//! Backtest fill models for synthetic order book fills.
//!
//! Models:
//! - [`BookWalkTaker`]  — executable VWAP across visible L2 depth
//! - [`OneTickTaker`]   — synthetic fallback for crossed limit orders
//! - [`Maker`]          — probabilistic resting limit fill, no auto-fallback
//! - [`Perfect`]        — touch fill, no slippage (sanity baseline)
//!
//! All return `FillResult` with `success=false` and a `reason` when the
//! input is invalid; never panic on bad books.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sha2::{Digest, Sha256};

pub const DEFAULT_TICK: f64 = 0.01;

#[derive(Debug, Clone, Copy)]
pub struct FillResult {
    pub filled_size: f64,
    pub fill_price: f64,
    pub fill_cost: f64, // signed: positive for buy, negative for sell
    pub slippage_per_share: f64,
    pub success: bool,
    pub reason: FillReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillReason {
    None,
    Empty,
    Invalid,
    InsufficientDepth,
    LimitNotCrossed,
    LimitMissingPrice,
    MakerFill,
    TakerFallback,
    PostOnlyCross,
    MakerUnfilled,
}

impl FillReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            FillReason::None => "",
            FillReason::Empty => "size <= 0",
            FillReason::Invalid => "invalid book",
            FillReason::InsufficientDepth => "insufficient_depth",
            FillReason::LimitNotCrossed => "limit not crossed",
            FillReason::LimitMissingPrice => "limit price required",
            FillReason::MakerFill => "maker_fill",
            FillReason::TakerFallback => "taker_fallback",
            FillReason::PostOnlyCross => "post_only_cross",
            FillReason::MakerUnfilled => "maker_unfilled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn parse(s: &str) -> Option<Side> {
        match s.to_lowercase().as_str() {
            "buy" | "b" => Some(Side::Buy),
            "sell" | "s" => Some(Side::Sell),
            _ => None,
        }
    }

    fn cost_sign(&self) -> f64 {
        match self {
            Side::Buy => 1.0,
            Side::Sell => -1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Market,
    Limit,
}

fn one_tick_adverse_price(side: Side, best_bid: f64, best_ask: f64, tick_size: f64) -> f64 {
    let p = match side {
        Side::Buy => best_ask + tick_size,
        Side::Sell => best_bid - tick_size,
    };
    p.clamp(tick_size, 1.0 - tick_size)
}

pub fn round_price_to_tick(price: f64, tick_size: f64) -> f64 {
    let tick = tick_size.max(0.0001);
    ((price / tick).round() * tick).clamp(tick, 1.0 - tick)
}

pub fn ceil_buy_price_to_tick(price: f64, tick_size: f64) -> f64 {
    let tick = tick_size.max(0.0001);
    ((price / tick - 1e-12).ceil() * tick).clamp(tick, 1.0 - tick)
}

pub fn resting_limit_price(
    side: Side,
    best_bid: f64,
    best_ask: f64,
    tick_size: f64,
) -> Option<f64> {
    if best_bid <= 0.0 || best_ask <= 0.0 || best_bid >= best_ask {
        return None;
    }
    let tick = tick_size.max(0.0001);
    let price = match side {
        Side::Buy => best_ask - tick,
        Side::Sell => best_bid + tick,
    };
    Some(round_price_to_tick(price, tick))
}

/// Synthetic-book taker fill model. Market orders pay touch + 1 tick adverse;
/// limit orders that cross fill at touch.
#[derive(Debug, Clone, Copy)]
pub struct OneTickTaker {
    pub tick_size: f64,
}

impl Default for OneTickTaker {
    fn default() -> Self {
        Self {
            tick_size: DEFAULT_TICK,
        }
    }
}

impl OneTickTaker {
    pub fn fill(
        &self,
        side: Side,
        size: f64,
        best_bid: f64,
        best_ask: f64,
        order_type: OrderType,
        limit_price: Option<f64>,
    ) -> FillResult {
        if size <= 0.0 || !size.is_finite() {
            return failed(FillReason::Empty);
        }
        if !valid_binary_book(best_bid, best_ask) {
            return failed(FillReason::Invalid);
        }

        let (fill_price, slippage) = match order_type {
            OrderType::Limit => {
                let Some(lp) = limit_price else {
                    return failed(FillReason::LimitMissingPrice);
                };
                if !lp.is_finite() || !(0.0..=1.0).contains(&lp) {
                    return failed(FillReason::Invalid);
                }
                match side {
                    Side::Buy if lp >= best_ask => (best_ask, 0.0),
                    Side::Sell if lp <= best_bid => (best_bid, 0.0),
                    _ => return failed(FillReason::LimitNotCrossed),
                }
            }
            OrderType::Market => {
                let p = one_tick_adverse_price(side, best_bid, best_ask, self.tick_size);
                let touch = match side {
                    Side::Buy => best_ask,
                    Side::Sell => best_bid,
                };
                (p, (p - touch).abs())
            }
        };

        FillResult {
            filled_size: size,
            fill_price,
            fill_cost: fill_price * size * side.cost_sign(),
            slippage_per_share: slippage,
            success: true,
            reason: FillReason::None,
        }
    }
}

/// Walks real L2 depth (bid/ask vectors) and rejects orders that exceed the
/// visible book. Inventing liquidity beyond the last level understates tail
/// execution risk.
#[derive(Debug, Clone, Copy)]
pub struct BookWalkTaker {
    pub tick_size: f64,
}

impl Default for BookWalkTaker {
    fn default() -> Self {
        Self {
            tick_size: DEFAULT_TICK,
        }
    }
}

impl BookWalkTaker {
    /// `bids` must be sorted descending by price, `asks` ascending.
    pub fn fill(
        &self,
        side: Side,
        size: f64,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        limit_price: Option<f64>,
    ) -> FillResult {
        if size <= 0.0 || !size.is_finite() {
            return failed(FillReason::Empty);
        }
        let levels: &[(f64, f64)] = match side {
            Side::Buy => asks,
            Side::Sell => bids,
        };
        if levels.is_empty() {
            return failed(FillReason::Empty);
        }
        if limit_price.is_some_and(|price| !price.is_finite() || !(0.0..=1.0).contains(&price)) {
            return failed(FillReason::Invalid);
        }

        let mut remaining = size;
        let mut total_cost = 0.0;
        let mut touch = None;
        for &(price, avail) in levels {
            if remaining <= 0.0 {
                break;
            }
            if !price.is_finite()
                || !avail.is_finite()
                || !(0.0..=1.0).contains(&price)
                || price == 0.0
                || avail <= 0.0
            {
                continue;
            }
            if limit_price.is_some_and(|limit| match side {
                Side::Buy => price > limit + 1e-12,
                Side::Sell => price + 1e-12 < limit,
            }) {
                break;
            }
            touch.get_or_insert(price);
            let take = remaining.min(avail);
            total_cost += take * price;
            remaining -= take;
        }
        if remaining > 1e-9 || touch.is_none() {
            return failed(FillReason::InsufficientDepth);
        }

        let vwap = total_cost / size;
        let slippage = (vwap - touch.expect("validated visible touch")).abs();
        FillResult {
            filled_size: size,
            fill_price: vwap,
            fill_cost: vwap * size * side.cost_sign(),
            slippage_per_share: slippage,
            success: true,
            reason: FillReason::None,
        }
    }
}

/// Post-only-style maker fill. Limit orders must rest; if they would cross
/// the visible touch they reject instead of silently becoming takers.
pub struct Maker {
    pub fill_prob: f64,
    pub tick_size: f64,
    seed: Option<u64>,
    rng: StdRng,
}

impl Maker {
    pub fn new(fill_prob: f64, tick_size: f64, seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };
        Self {
            fill_prob,
            tick_size,
            seed,
            rng,
        }
    }

    #[allow(dead_code)]
    pub fn fill(
        &mut self,
        side: Side,
        size: f64,
        best_bid: f64,
        best_ask: f64,
        order_type: OrderType,
        limit_price: Option<f64>,
    ) -> FillResult {
        self.fill_with_key(
            side,
            size,
            best_bid,
            best_ask,
            order_type,
            limit_price,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fill_with_key(
        &mut self,
        side: Side,
        size: f64,
        best_bid: f64,
        best_ask: f64,
        order_type: OrderType,
        limit_price: Option<f64>,
        deterministic_key: Option<&str>,
    ) -> FillResult {
        if size <= 0.0 || !size.is_finite() {
            return failed(FillReason::Empty);
        }
        if !valid_binary_book(best_bid, best_ask) {
            return failed(FillReason::Invalid);
        }

        if matches!(order_type, OrderType::Market) {
            let fill_price = one_tick_adverse_price(side, best_bid, best_ask, self.tick_size);
            let touch = match side {
                Side::Buy => best_ask,
                Side::Sell => best_bid,
            };
            FillResult {
                filled_size: size,
                fill_price,
                fill_cost: fill_price * size * side.cost_sign(),
                slippage_per_share: (fill_price - touch).abs(),
                success: true,
                reason: FillReason::TakerFallback,
            }
        } else {
            let Some(lp) = limit_price else {
                return failed(FillReason::LimitMissingPrice);
            };
            if !lp.is_finite() || !(0.0..=1.0).contains(&lp) {
                return failed(FillReason::Invalid);
            }
            let eps = 1e-9;
            match side {
                Side::Buy if lp >= best_ask - eps => return failed(FillReason::PostOnlyCross),
                Side::Sell if lp <= best_bid + eps => return failed(FillReason::PostOnlyCross),
                Side::Buy if lp + eps < best_bid => return failed(FillReason::MakerUnfilled),
                Side::Sell if lp - eps > best_ask => return failed(FillReason::MakerUnfilled),
                _ => {}
            }

            let draw = match (self.seed, deterministic_key) {
                (Some(seed), Some(key)) => deterministic_unit_interval(seed, key),
                _ => self.rng.gen::<f64>(),
            };
            if draw >= self.fill_prob {
                return failed(FillReason::MakerUnfilled);
            }

            let touch = match side {
                Side::Buy => best_ask,
                Side::Sell => best_bid,
            };
            let improvement = (touch - lp).abs();
            FillResult {
                filled_size: size,
                fill_price: lp,
                fill_cost: lp * size * side.cost_sign(),
                slippage_per_share: -improvement,
                success: true,
                reason: FillReason::MakerFill,
            }
        }
    }
}

fn deterministic_unit_interval(seed: u64, key: &str) -> f64 {
    let mut hasher = Sha256::new();
    hasher.update(seed.to_le_bytes());
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let n = u64::from_le_bytes(bytes);
    (n as f64) / ((u64::MAX as f64) + 1.0)
}

/// Sanity baseline — fills at touch with zero slippage.
#[derive(Debug, Clone, Copy, Default)]
pub struct Perfect;

impl Perfect {
    pub fn fill(&self, side: Side, size: f64, best_bid: f64, best_ask: f64) -> FillResult {
        if size <= 0.0 || !size.is_finite() {
            return failed(FillReason::Empty);
        }
        if !valid_binary_book(best_bid, best_ask) {
            return failed(FillReason::Invalid);
        }
        let price = match side {
            Side::Buy => best_ask,
            Side::Sell => best_bid,
        };
        FillResult {
            filled_size: size,
            fill_price: price,
            fill_cost: price * size * side.cost_sign(),
            slippage_per_share: 0.0,
            success: true,
            reason: FillReason::None,
        }
    }
}

fn valid_binary_book(best_bid: f64, best_ask: f64) -> bool {
    best_bid.is_finite()
        && best_ask.is_finite()
        && best_bid > 0.0
        && best_ask <= 1.0
        && best_bid < best_ask
}

fn failed(reason: FillReason) -> FillResult {
    FillResult {
        filled_size: 0.0,
        fill_price: 0.0,
        fill_cost: 0.0,
        slippage_per_share: 0.0,
        success: false,
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_tick_taker_market_buy_pays_one_tick_adverse() {
        let f = OneTickTaker::default();
        let r = f.fill(Side::Buy, 10.0, 0.50, 0.52, OrderType::Market, None);
        assert!(r.success);
        assert!((r.fill_price - 0.53).abs() < 1e-9);
        assert!((r.slippage_per_share - 0.01).abs() < 1e-9);
    }

    #[test]
    fn one_tick_taker_limit_not_crossed_fails() {
        let f = OneTickTaker::default();
        let r = f.fill(Side::Buy, 10.0, 0.50, 0.52, OrderType::Limit, Some(0.51));
        assert!(!r.success);
        assert_eq!(r.reason, FillReason::LimitNotCrossed);
    }

    #[test]
    fn one_tick_taker_invalid_book_fails() {
        let f = OneTickTaker::default();
        let r = f.fill(Side::Buy, 10.0, 0.0, 0.0, OrderType::Market, None);
        assert!(!r.success);
        assert_eq!(r.reason, FillReason::Invalid);
    }

    #[test]
    fn book_walk_vwaps_across_levels() {
        let f = BookWalkTaker::default();
        let asks = vec![(0.50, 100.0), (0.60, 50.0)];
        let r = f.fill(Side::Buy, 130.0, &[], &asks, None);
        assert!(r.success);
        let expected_vwap = (0.50 * 100.0 + 0.60 * 30.0) / 130.0;
        assert!((r.fill_price - expected_vwap).abs() < 1e-9);
    }

    #[test]
    fn book_walk_rejects_insufficient_visible_depth() {
        let f = BookWalkTaker::default();
        let asks = vec![(0.50, 50.0)];
        let r = f.fill(Side::Buy, 100.0, &[], &asks, None);
        assert!(!r.success);
        assert_eq!(r.reason, FillReason::InsufficientDepth);
    }

    #[test]
    fn book_walk_skips_invalid_levels_without_poisoning_slippage() {
        let f = BookWalkTaker::default();
        let asks = vec![(f64::NAN, 10.0), (0.50, 10.0)];
        let r = f.fill(Side::Buy, 5.0, &[], &asks, None);

        assert!(r.success);
        assert_eq!(r.fill_price, 0.50);
        assert_eq!(r.slippage_per_share, 0.0);
    }

    #[test]
    fn book_walk_respects_fok_limit() {
        let f = BookWalkTaker::default();
        let asks = vec![(0.50, 2.0), (0.60, 10.0)];
        let r = f.fill(Side::Buy, 3.0, &[], &asks, Some(0.50));
        assert!(!r.success);
        assert_eq!(r.reason, FillReason::InsufficientDepth);
    }

    #[test]
    fn fill_models_reject_non_finite_books_and_sizes() {
        assert!(
            !OneTickTaker::default()
                .fill(Side::Buy, f64::NAN, 0.50, 0.52, OrderType::Market, None,)
                .success
        );
        assert!(!Perfect.fill(Side::Buy, 1.0, f64::NAN, 0.52).success);
    }

    #[test]
    fn perfect_fills_at_touch() {
        let f = Perfect;
        let r = f.fill(Side::Buy, 10.0, 0.50, 0.52);
        assert!(r.success);
        assert!((r.fill_price - 0.52).abs() < 1e-9);
        assert_eq!(r.slippage_per_share, 0.0);
    }

    #[test]
    fn maker_with_seed_is_deterministic() {
        let mut a = Maker::new(0.65, DEFAULT_TICK, Some(42));
        let mut b = Maker::new(0.65, DEFAULT_TICK, Some(42));
        let ra = a.fill(Side::Buy, 1.0, 0.50, 0.52, OrderType::Limit, Some(0.51));
        let rb = b.fill(Side::Buy, 1.0, 0.50, 0.52, OrderType::Limit, Some(0.51));
        assert!((ra.fill_price - rb.fill_price).abs() < 1e-12);
        assert_eq!(ra.reason, rb.reason);
    }

    #[test]
    fn seeded_maker_fill_key_is_independent_of_call_order() {
        let mut a = Maker::new(0.65, DEFAULT_TICK, Some(42));
        let a_first = a.fill_with_key(
            Side::Buy,
            1.0,
            0.50,
            0.52,
            OrderType::Limit,
            Some(0.51),
            Some("order-a"),
        );
        let a_second = a.fill_with_key(
            Side::Buy,
            1.0,
            0.50,
            0.52,
            OrderType::Limit,
            Some(0.51),
            Some("order-b"),
        );

        let mut b = Maker::new(0.65, DEFAULT_TICK, Some(42));
        let b_second = b.fill_with_key(
            Side::Buy,
            1.0,
            0.50,
            0.52,
            OrderType::Limit,
            Some(0.51),
            Some("order-b"),
        );
        let b_first = b.fill_with_key(
            Side::Buy,
            1.0,
            0.50,
            0.52,
            OrderType::Limit,
            Some(0.51),
            Some("order-a"),
        );

        assert_eq!(a_first.success, b_first.success);
        assert_eq!(a_first.reason, b_first.reason);
        assert_eq!(a_second.success, b_second.success);
        assert_eq!(a_second.reason, b_second.reason);
    }

    #[test]
    fn maker_limit_crossing_rejects_post_only() {
        let mut maker = Maker::new(1.0, DEFAULT_TICK, Some(42));
        let r = maker.fill(Side::Buy, 1.0, 0.50, 0.52, OrderType::Limit, Some(0.52));
        assert!(!r.success);
        assert_eq!(r.reason, FillReason::PostOnlyCross);
    }

    #[test]
    fn maker_limit_fills_at_resting_limit_when_probability_hits() {
        let mut maker = Maker::new(1.0, DEFAULT_TICK, Some(42));
        let r = maker.fill(Side::Buy, 1.0, 0.50, 0.52, OrderType::Limit, Some(0.51));
        assert!(r.success);
        assert_eq!(r.reason, FillReason::MakerFill);
        assert!((r.fill_price - 0.51).abs() < 1e-9);
        assert!(r.slippage_per_share < 0.0);
    }

    #[test]
    fn maker_limit_can_remain_unfilled() {
        let mut maker = Maker::new(0.0, DEFAULT_TICK, Some(42));
        let r = maker.fill(Side::Buy, 1.0, 0.50, 0.52, OrderType::Limit, Some(0.51));
        assert!(!r.success);
        assert_eq!(r.reason, FillReason::MakerUnfilled);
    }

    #[test]
    fn resting_limit_quotes_one_tick_inside_touch() {
        let buy = resting_limit_price(Side::Buy, 0.50, 0.52, DEFAULT_TICK).unwrap();
        let sell = resting_limit_price(Side::Sell, 0.50, 0.52, DEFAULT_TICK).unwrap();
        assert!((buy - 0.51).abs() < 1e-9);
        assert!((sell - 0.51).abs() < 1e-9);
    }
}
