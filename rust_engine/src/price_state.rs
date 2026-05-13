//! Shared multi-source price aggregation state.
//!
//! Lives in the library so both binaries (`polymomentum-engine` and the
//! legacy `polymomentum-legacy`) and the `exchange` module can share it.

use std::collections::{HashMap, VecDeque};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    pub alt_prices: HashMap<String, HashMap<String, f64>>,
    pub alt_mid: HashMap<String, f64>,
    pub alt_timestamps: HashMap<String, Instant>,
    price_history: VecDeque<(f64, f64)>,
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
            alt_prices: HashMap::new(),
            alt_mid: HashMap::new(),
            alt_timestamps: HashMap::new(),
            price_history: VecDeque::new(),
            alt_history: HashMap::new(),
        }
    }

    pub fn update(&mut self, source: &str, price: f64) {
        if price <= 0.0 {
            return;
        }
        self.prices.insert(source.to_string(), price);
        self.source_timestamps.insert(source.to_string(), Instant::now());
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
        if price <= 0.0 {
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
            .min_by(|a, b| {
                a.0.partial_cmp(&b.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
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
        assert_eq!(history.front().copied(), Some((PRICE_HISTORY_MAX_AGE_S + 1.0, 11.0)));
    }
}
