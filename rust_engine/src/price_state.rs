//! Shared multi-source price aggregation state.
//!
//! Lives in the library so both binaries (`polymomentum-engine` and the
//! legacy `polymomentum-legacy`) and the `exchange` module can share it.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PRICE_HISTORY_MAX_AGE_S: f64 = 3_600.0;
const PRICE_HISTORY_MIN_STEP_S: f64 = 0.05;

#[derive(Debug, Clone)]
pub struct PriceState {
    pub prices: HashMap<String, f64>,
    pub last_update: Instant,
    pub mid_price: f64,
    pub spread: f64,
    pub implied_vol: f64,
    pub source_timestamps: HashMap<String, Instant>,
    reference_prices: HashMap<String, f64>,
    reference_timestamps: HashMap<String, Instant>,
    reference_observed_at_ms: HashMap<String, i64>,
    pub alt_prices: HashMap<String, HashMap<String, f64>>,
    pub alt_mid: HashMap<String, f64>,
    pub alt_timestamps: HashMap<String, Instant>,
    price_history: VecDeque<(f64, f64)>,
    reference_history: HashMap<String, VecDeque<(f64, f64)>>,
    alt_history: HashMap<String, VecDeque<(f64, f64)>>,
}

impl Default for PriceState {
    fn default() -> Self {
        Self::new()
    }
}

impl PriceState {
    pub fn new() -> Self {
        Self {
            prices: HashMap::new(),
            last_update: Instant::now(),
            mid_price: 0.0,
            spread: 0.0,
            implied_vol: 0.50,
            source_timestamps: HashMap::new(),
            reference_prices: HashMap::new(),
            reference_timestamps: HashMap::new(),
            reference_observed_at_ms: HashMap::new(),
            alt_prices: HashMap::new(),
            alt_mid: HashMap::new(),
            alt_timestamps: HashMap::new(),
            price_history: VecDeque::new(),
            reference_history: HashMap::new(),
            alt_history: HashMap::new(),
        }
    }

    pub fn update_reference_at(&mut self, source: &str, price: f64, observed_at_ms: i64) {
        if price <= 0.0 || !price.is_finite() {
            return;
        }
        if observed_at_ms <= 0
            || self
                .reference_observed_at_ms
                .get(source)
                .is_some_and(|previous| observed_at_ms <= *previous)
        {
            return;
        }
        self.reference_prices.insert(source.to_string(), price);
        self.reference_timestamps
            .insert(source.to_string(), Instant::now());
        self.reference_observed_at_ms
            .insert(source.to_string(), observed_at_ms);
        record_history(
            self.reference_history
                .entry(source.to_string())
                .or_default(),
            observed_at_ms as f64 / 1_000.0,
            price,
        );
    }

    pub fn fresh_source_price(&self, source: &str, max_age: Duration) -> Option<f64> {
        let updated = self.reference_timestamps.get(source)?;
        if updated.elapsed() > max_age {
            return None;
        }
        let observed_at_ms = *self.reference_observed_at_ms.get(source)?;
        let max_age_ms = i64::try_from(max_age.as_millis()).unwrap_or(i64::MAX);
        let now_ms = now_millis();
        let observation_age_ms = now_ms.saturating_sub(observed_at_ms);
        if observation_age_ms > max_age_ms || observed_at_ms > now_ms.saturating_add(max_age_ms) {
            return None;
        }
        self.reference_prices
            .get(source)
            .copied()
            .filter(|price| *price > 0.0)
    }

    pub fn reference_price_near_seconds(
        &self,
        source: &str,
        target_s: f64,
        max_distance_s: f64,
    ) -> Option<f64> {
        self.reference_history
            .get(source)?
            .iter()
            .filter_map(|(ts, price)| {
                let distance = (*ts - target_s).abs();
                (distance <= max_distance_s).then_some((distance, *price))
            })
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, price)| price)
    }

    pub fn update(&mut self, source: &str, price: f64) {
        if price <= 0.0 || !price.is_finite() {
            return;
        }
        self.prices.insert(source.to_string(), price);
        self.source_timestamps
            .insert(source.to_string(), Instant::now());
        self.last_update = Instant::now();

        let now = Instant::now();
        let live: Vec<f64> = self
            .prices
            .iter()
            .filter(|(src, _)| {
                self.source_timestamps
                    .get(*src)
                    .map(|t| now.duration_since(*t).as_secs() < 10)
                    .unwrap_or(false)
            })
            .map(|(_, p)| *p)
            .collect();

        if !live.is_empty() {
            self.mid_price = live.iter().sum::<f64>() / live.len() as f64;
            let min = live.iter().cloned().fold(f64::MAX, f64::min);
            let max = live.iter().cloned().fold(f64::MIN, f64::max);
            self.spread = max - min;
            record_history(&mut self.price_history, now_seconds(), self.mid_price);
        }
    }

    pub fn update_alt(&mut self, asset: &str, source: &str, price: f64) {
        if price <= 0.0 || !price.is_finite() {
            return;
        }
        let key = format!("{asset}:{source}");
        self.alt_timestamps.insert(key, Instant::now());

        let sources = self.alt_prices.entry(asset.to_string()).or_default();
        sources.insert(source.to_string(), price);

        let now = Instant::now();
        let live: Vec<f64> = sources
            .iter()
            .filter(|(src, _)| {
                let key = format!("{asset}:{src}");
                self.alt_timestamps
                    .get(&key)
                    .map(|t| now.duration_since(*t).as_secs() < 10)
                    .unwrap_or(false)
            })
            .map(|(_, p)| *p)
            .collect();

        if !live.is_empty() {
            let mid = live.iter().sum::<f64>() / live.len() as f64;
            self.alt_mid.insert(asset.to_string(), mid);
            record_history(
                self.alt_history.entry(asset.to_string()).or_default(),
                now_seconds(),
                mid,
            );
        }
    }

    pub fn price_near_seconds(
        &self,
        asset: &str,
        target_s: f64,
        max_distance_s: f64,
    ) -> Option<f64> {
        let history = if asset == "BTC" {
            &self.price_history
        } else {
            self.alt_history.get(asset)?
        };
        history
            .iter()
            .filter_map(|(ts, price)| {
                let distance = (*ts - target_s).abs();
                (distance <= max_distance_s).then_some((distance, *price))
            })
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, price)| price)
    }

    pub fn n_live_sources(&self) -> usize {
        let now = Instant::now();
        self.source_timestamps
            .values()
            .filter(|t| now.duration_since(**t).as_secs() < 10)
            .count()
    }
}

fn record_history(history: &mut VecDeque<(f64, f64)>, ts_s: f64, price: f64) {
    if price <= 0.0 {
        return;
    }
    if history
        .back()
        .map(|(last_ts, _)| ts_s - *last_ts < PRICE_HISTORY_MIN_STEP_S)
        .unwrap_or(false)
    {
        return;
    }
    history.push_back((ts_s, price));
    let cutoff = ts_s - PRICE_HISTORY_MAX_AGE_S;
    while history
        .front()
        .map(|(old_ts, _)| *old_ts < cutoff)
        .unwrap_or(false)
    {
        history.pop_front();
    }
}

fn now_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_history_returns_nearest_price_inside_tolerance() {
        let mut ps = PriceState::new();
        record_history(&mut ps.price_history, 100.0, 10.0);
        record_history(&mut ps.price_history, 101.0, 11.0);
        record_history(&mut ps.price_history, 103.0, 13.0);

        assert_eq!(ps.price_near_seconds("BTC", 100.8, 1.0), Some(11.0));
        assert_eq!(ps.price_near_seconds("BTC", 105.0, 1.0), None);
    }

    #[test]
    fn price_history_retains_only_recent_window() {
        let mut history = VecDeque::new();
        record_history(&mut history, 0.0, 10.0);
        record_history(&mut history, PRICE_HISTORY_MAX_AGE_S + 1.0, 11.0);

        assert_eq!(history.len(), 1);
        assert_eq!(
            history.front().copied(),
            Some((PRICE_HISTORY_MAX_AGE_S + 1.0, 11.0))
        );
    }

    #[test]
    fn settlement_reference_does_not_move_the_execution_aggregate() {
        let mut ps = PriceState::new();
        ps.update("binance", 100.0);
        ps.update_reference_at("chainlink_settlement", 101.0, now_millis());
        ps.update("bybit", 102.0);

        assert_eq!(ps.mid_price, 101.0);
        assert_eq!(ps.n_live_sources(), 2);
        assert_eq!(ps.prices.len(), 2);
        assert_eq!(
            ps.fresh_source_price("chainlink_settlement", Duration::from_secs(1)),
            Some(101.0)
        );
        assert_eq!(
            ps.reference_price_near_seconds("chainlink_settlement", now_seconds(), 1.0),
            Some(101.0)
        );
    }

    #[test]
    fn settlement_reference_freshness_uses_observation_time() {
        let mut ps = PriceState::new();
        ps.update_reference_at("chainlink_settlement", 101.0, now_millis() - 20_000);

        assert_eq!(
            ps.fresh_source_price("chainlink_settlement", Duration::from_secs(10)),
            None
        );
    }

    #[test]
    fn out_of_order_settlement_reference_cannot_roll_back_price() {
        let now_ms = now_millis();
        let mut ps = PriceState::new();
        ps.update_reference_at("chainlink_settlement", 101.0, now_ms - 1_000);
        ps.update_reference_at("chainlink_settlement", 99.0, now_ms - 2_000);

        assert_eq!(
            ps.fresh_source_price("chainlink_settlement", Duration::from_secs(10)),
            Some(101.0)
        );
        assert_eq!(
            ps.reference_price_near_seconds(
                "chainlink_settlement",
                (now_ms - 1_000) as f64 / 1_000.0,
                0.1,
            ),
            Some(101.0)
        );
    }
}
