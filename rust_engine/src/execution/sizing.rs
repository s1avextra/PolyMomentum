//! Shared order sizing helpers for Polymarket-style share quantities.

pub const SHARE_SIZE_QUANTUM: f64 = 0.01;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BuyBookQuote {
    pub shares: f64,
    pub spend: f64,
    pub vwap: f64,
    pub worst_price: f64,
    pub slippage_per_share: f64,
    pub depth_limited: bool,
}

pub fn floor_share_size(shares: f64) -> f64 {
    if !(shares.is_finite() && shares > 0.0) {
        return 0.0;
    }
    (shares / SHARE_SIZE_QUANTUM).floor() * SHARE_SIZE_QUANTUM
}

pub fn shares_from_budget(budget_usd: f64, price: f64, min_order_size_shares: f64) -> Option<f64> {
    if !(budget_usd.is_finite() && budget_usd > 0.0 && price.is_finite() && price > 0.0) {
        return None;
    }
    let shares = floor_share_size(budget_usd / price);
    if shares <= 0.0 {
        return None;
    }
    if shares + 1e-9 < min_order_size_shares.max(0.0) {
        return None;
    }
    Some(shares)
}

/// Size a buy against visible asks without exceeding `budget_usd`.
/// Levels must be in ascending price order. Invalid or unsorted books fail
/// closed so live FOK limits and replay book walks use the same quote.
pub fn buy_book_quote_from_budget(
    budget_usd: f64,
    asks: &[(f64, f64)],
    min_order_size_shares: f64,
    tick_size: f64,
) -> Option<BuyBookQuote> {
    if !(budget_usd.is_finite() && budget_usd > 0.0 && tick_size.is_finite() && tick_size > 0.0)
        || asks.is_empty()
    {
        return None;
    }
    let mut previous_price = 0.0;
    let mut cumulative_depth = 0.0;
    let mut shares = 0.0;
    let mut limit_price = 0.0;
    let mut depth_limited = false;
    for &(price, size) in asks {
        if !(price.is_finite() && size.is_finite() && price > 0.0 && price <= 1.0 && size > 0.0)
            || price + 1e-12 < previous_price
        {
            return None;
        }
        previous_price = price;
        cumulative_depth += size;
        if !cumulative_depth.is_finite() {
            return None;
        }

        let candidate_limit = ((price / tick_size - 1e-12).ceil() * tick_size).min(1.0);
        if candidate_limit + 1e-12 < price {
            return None;
        }
        let budget_limited_shares = budget_usd / candidate_limit;
        let candidate_shares = floor_share_size(cumulative_depth.min(budget_limited_shares));
        if candidate_shares > shares + 1e-9 {
            shares = candidate_shares;
            limit_price = candidate_limit;
            depth_limited = cumulative_depth + 1e-9 < budget_limited_shares;
        }
    }
    if shares <= 0.0 || shares + 1e-9 < min_order_size_shares.max(0.0) {
        return None;
    }

    let touch = asks.first()?.0;
    let mut remaining_shares = shares;
    let mut spend = 0.0;
    let mut worst_price = 0.0;
    for &(price, size) in asks {
        if remaining_shares <= 1e-9 {
            break;
        }
        let take = size.min(remaining_shares);
        spend += take * price;
        remaining_shares -= take;
        worst_price = price;
    }
    if remaining_shares > 1e-9
        || spend > budget_usd + 1e-8
        || shares * limit_price > budget_usd + 1e-8
    {
        return None;
    }
    let vwap = spend / shares;
    Some(BuyBookQuote {
        shares,
        spend,
        vwap,
        worst_price: limit_price.max(worst_price),
        slippage_per_share: (vwap - touch).max(0.0),
        depth_limited,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floors_to_polymarket_share_precision() {
        assert!((floor_share_size(3.456) - 3.45).abs() < 1e-12);
        assert_eq!(floor_share_size(0.009), 0.0);
    }

    #[test]
    fn rejects_orders_below_live_minimum() {
        assert_eq!(shares_from_budget(2.0, 0.50, 5.0), None);
        assert_eq!(shares_from_budget(2.5, 0.50, 5.0), Some(5.0));
    }

    #[test]
    fn book_quote_sizes_to_budget_and_reports_fok_limit() {
        let quote = buy_book_quote_from_budget(3.0, &[(0.50, 4.0), (0.60, 10.0)], 1.0, 0.01)
            .expect("visible quote");
        assert_eq!(quote.shares, 5.0);
        assert!((quote.spend - 2.6).abs() < 1e-12);
        assert!((quote.vwap - 0.52).abs() < 1e-12);
        assert_eq!(quote.worst_price, 0.60);
        assert!(!quote.depth_limited);
    }

    #[test]
    fn book_quote_uses_only_visible_depth() {
        let quote = buy_book_quote_from_budget(10.0, &[(0.50, 3.0)], 1.0, 0.01)
            .expect("depth-limited quote");
        assert_eq!(quote.shares, 3.0);
        assert_eq!(quote.spend, 1.5);
        assert!(quote.depth_limited);
    }

    #[test]
    fn book_quote_does_not_let_high_price_dust_raise_the_fok_limit() {
        let quote = buy_book_quote_from_budget(50.10, &[(0.50, 100.0), (0.90, 1.0)], 1.0, 0.01)
            .expect("low-price depth should remain executable");

        assert_eq!(quote.shares, 100.0);
        assert_eq!(quote.spend, 50.0);
        assert_eq!(quote.worst_price, 0.50);
        assert!(quote.depth_limited);
        assert!(quote.shares * quote.worst_price <= 50.10);
    }

    #[test]
    fn book_quote_rejects_unsorted_or_non_finite_levels() {
        assert!(buy_book_quote_from_budget(10.0, &[(0.60, 1.0), (0.50, 1.0)], 1.0, 0.01).is_none());
        assert!(buy_book_quote_from_budget(10.0, &[(f64::NAN, 1.0)], 1.0, 0.01).is_none());
    }
}
