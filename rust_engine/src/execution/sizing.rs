//! Shared order sizing helpers for Polymarket-style share quantities.

pub const SHARE_SIZE_QUANTUM: f64 = 0.01;

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
}
