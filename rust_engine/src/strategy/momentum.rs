//! BTC/ETH/etc momentum detector.
//!
//! Volatility-normalized signal: z-score = price move / (σ × √window).
//! Volatility is estimated via fast (~15min) and slow (~4h) EWMA of squared
//! log returns, with a configurable floor.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MomentumSignal {
    pub direction: String,
    pub confidence: f64,
    pub price_change: f64,
    pub price_change_pct: f64,
    pub consistency: f64,
    pub minutes_elapsed: f64,
    pub minutes_remaining: f64,
    pub current_price: f64,
    pub open_price: f64,
    pub z_score: f64,
    pub reversion_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directional_impulse_10s_bps: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
pub struct MomentumConfig {
    pub noise_z_threshold: f64,
    pub fast_vol_half_life_min: f64,
    pub slow_vol_half_life_min: f64,
    pub floor_vol: f64,
    pub max_ticks: usize,
}

impl Default for MomentumConfig {
    fn default() -> Self {
        Self {
            noise_z_threshold: 0.3,
            fast_vol_half_life_min: 15.0,
            slow_vol_half_life_min: 240.0,
            floor_vol: 0.10,
            max_ticks: 5000,
        }
    }
}

pub struct MomentumDetector {
    cfg: MomentumConfig,
    ticks: VecDeque<(f64, f64)>, // (ts, price)
    window_opens: HashMap<String, f64>,
    seed_vol: f64,
    fast_tau_s: f64,
    slow_tau_s: f64,
    fast_var: f64,
    slow_var: f64,
    ewma_warmed: bool,
}

const SECONDS_PER_YEAR: f64 = 365.25 * 86400.0;

pub fn annualized_realized_vol<I>(samples: I, min_returns: usize) -> Option<f64>
where
    I: IntoIterator<Item = (f64, f64)>,
{
    let mut previous: Option<(f64, f64)> = None;
    let mut count = 0_usize;
    let mut mean = 0.0;
    let mut m2 = 0.0;
    let mut total_dt = 0.0;
    for (timestamp_s, price) in samples {
        if let Some((previous_ts, previous_price)) = previous {
            let dt = timestamp_s - previous_ts;
            if dt > 0.0 && previous_price > 0.0 && price > 0.0 {
                let value = (price / previous_price).ln();
                count += 1;
                let delta = value - mean;
                mean += delta / count as f64;
                m2 += delta * (value - mean);
                total_dt += dt;
            }
        }
        previous = Some((timestamp_s, price));
    }
    if count < min_returns || total_dt <= 0.0 {
        return None;
    }
    let variance = m2 / count as f64;
    let average_dt = total_dt / count as f64;
    Some(
        (variance / average_dt * SECONDS_PER_YEAR)
            .sqrt()
            .clamp(0.05, 5.0),
    )
}

impl MomentumDetector {
    pub fn new(seed_vol: Option<f64>, cfg: MomentumConfig) -> Self {
        let seed = seed_vol.unwrap_or(0.50);
        let seed = if seed > 0.0 { seed } else { 0.50 };
        let fast_tau_s = cfg.fast_vol_half_life_min * 60.0 / std::f64::consts::LN_2;
        let slow_tau_s = cfg.slow_vol_half_life_min * 60.0 / std::f64::consts::LN_2;
        Self {
            cfg,
            ticks: VecDeque::with_capacity(5000),
            window_opens: HashMap::new(),
            seed_vol: seed,
            fast_tau_s,
            slow_tau_s,
            fast_var: 0.0,
            slow_var: 0.0,
            ewma_warmed: false,
        }
    }

    pub fn realized_vol(&self) -> f64 {
        if !self.ewma_warmed {
            return self.seed_vol;
        }
        let v = (self.fast_var.max(0.0) * SECONDS_PER_YEAR).sqrt();
        v.clamp(self.cfg.floor_vol, 5.0)
    }

    pub fn slow_realized_vol(&self) -> f64 {
        if !self.ewma_warmed {
            return self.seed_vol;
        }
        let v = (self.slow_var.max(0.0) * SECONDS_PER_YEAR).sqrt();
        v.clamp(self.cfg.floor_vol, 5.0)
    }

    pub fn rolling_realized_vol(&self, lookback_seconds: f64) -> Option<f64> {
        if lookback_seconds <= 0.0 || !lookback_seconds.is_finite() {
            return None;
        }
        let cutoff = self.ticks.back()?.0 - lookback_seconds;
        annualized_realized_vol(
            self.ticks
                .iter()
                .copied()
                .filter(|(timestamp_s, _)| *timestamp_s >= cutoff),
            20,
        )
    }

    pub fn set_realized_vol(&mut self, vol: f64) {
        if vol > 0.0 && !self.ewma_warmed {
            self.seed_vol = vol;
        }
    }

    pub fn add_tick(&mut self, price: f64, timestamp: Option<f64>) {
        let ts = timestamp.unwrap_or_else(now_ts);
        if let Some(&(last_ts, last_price)) = self.ticks.back() {
            let dt = ts - last_ts;
            if dt > 0.0 && last_price > 0.0 && price > 0.0 {
                let log_return = (price / last_price).ln();
                let r2_rate = (log_return * log_return) / dt;
                if self.ewma_warmed {
                    let fast_alpha = 1.0 - (-dt / self.fast_tau_s).exp();
                    let slow_alpha = 1.0 - (-dt / self.slow_tau_s).exp();
                    self.fast_var = (1.0 - fast_alpha) * self.fast_var + fast_alpha * r2_rate;
                    self.slow_var = (1.0 - slow_alpha) * self.slow_var + slow_alpha * r2_rate;
                } else {
                    self.fast_var = r2_rate;
                    self.slow_var = r2_rate;
                    self.ewma_warmed = true;
                }
            }
        }
        if self.ticks.len() == self.cfg.max_ticks {
            self.ticks.pop_front();
        }
        self.ticks.push_back((ts, price));
    }

    pub fn set_window_open(&mut self, contract_id: &str, price: f64) {
        self.window_opens.insert(contract_id.to_string(), price);
    }

    pub fn get_open_price(&self, contract_id: &str) -> Option<f64> {
        self.window_opens.get(contract_id).copied()
    }

    pub fn evict_stale_windows(&mut self, active: &HashSet<String>) -> usize {
        let stale: Vec<String> = self
            .window_opens
            .keys()
            .filter(|k| !active.contains(*k))
            .cloned()
            .collect();
        let n = stale.len();
        for s in stale {
            self.window_opens.remove(&s);
        }
        n
    }

    pub fn detect(
        &mut self,
        contract_id: &str,
        window_start_ago_minutes: f64,
        minutes_remaining: f64,
        current_price: f64,
        now_ts_override: Option<f64>,
    ) -> Option<MomentumSignal> {
        if self.ticks.is_empty() || minutes_remaining <= 0.0 {
            return None;
        }
        let now = now_ts_override.unwrap_or_else(now_ts);
        let window_start = now - window_start_ago_minutes * 60.0;

        let open_price = match self.window_opens.get(contract_id).copied() {
            Some(p) => p,
            None => {
                let mut found = None;
                for &(ts, price) in self.ticks.iter() {
                    if ts >= window_start {
                        found = Some(price);
                        break;
                    }
                }
                if let Some(p) = found {
                    self.window_opens.insert(contract_id.to_string(), p);
                }
                found?
            }
        };

        if open_price <= 0.0 {
            return None;
        }

        let price_change = current_price - open_price;
        let price_change_pct = price_change / open_price;
        let direction = if price_change >= 0.0 { "up" } else { "down" };
        let directional_impulse_10s_bps =
            self.directional_impulse_bps(direction, current_price, now, 10.0);

        // Walk ticks newest → oldest, stop at window_start.
        let mut recent: Vec<(f64, f64)> = Vec::new();
        for &(ts, p) in self.ticks.iter().rev() {
            if ts < window_start {
                break;
            }
            recent.push((ts, p));
        }
        if recent.len() < 3 {
            return None;
        }
        recent.reverse();

        let mut consistent = 0;
        let mut reversion_count = 0u32;
        let mut prev_side: Option<bool> = None;
        for i in 1..recent.len() {
            let tick_dir = recent[i].1 - recent[i - 1].1;
            let agrees = match direction {
                "up" => tick_dir >= 0.0,
                _ => tick_dir <= 0.0,
            };
            if agrees {
                consistent += 1;
            }
            let curr_side = recent[i].1 >= open_price;
            if let Some(prev) = prev_side {
                if curr_side != prev {
                    reversion_count += 1;
                }
            }
            prev_side = Some(curr_side);
        }
        let consistency = consistent as f64 / (recent.len() - 1).max(1) as f64;

        let minutes_elapsed = window_start_ago_minutes;
        let total_window = minutes_elapsed + minutes_remaining;
        let current_vol = self.realized_vol();
        let mut sigma_window = open_price * current_vol * (total_window / 525_600.0).sqrt();
        if sigma_window < 1.0 {
            sigma_window = 1.0;
        }
        let z_score = price_change.abs() / sigma_window;

        let time_factor = if total_window > 0.0 {
            (minutes_elapsed / total_window).min(1.0)
        } else {
            0.0
        };
        let reversion_penalty = (1.0 - reversion_count as f64 * 0.05).max(0.0);
        let z_factor = (z_score / 3.0).min(1.0);

        let mut confidence =
            0.35 * time_factor + 0.35 * z_factor + 0.15 * consistency + 0.15 * reversion_penalty;
        confidence = confidence.clamp(0.10, 0.95);

        if minutes_remaining < 1.0 && z_score > 0.5 {
            confidence = (confidence + 0.10 * z_score.min(2.0)).min(0.95);
        } else if minutes_remaining < 2.0 && z_score > 1.0 {
            confidence = (confidence + 0.05 * z_score.min(3.0)).min(0.95);
        }

        if z_score < self.cfg.noise_z_threshold {
            confidence *= 0.4;
        }

        Some(MomentumSignal {
            direction: direction.to_string(),
            confidence,
            price_change,
            price_change_pct,
            consistency,
            minutes_elapsed,
            minutes_remaining,
            current_price,
            open_price,
            z_score,
            reversion_count,
            directional_impulse_10s_bps,
        })
    }

    pub fn directional_impulse_bps(
        &self,
        direction: &str,
        current_price: f64,
        now_ts: f64,
        lookback_seconds: f64,
    ) -> Option<f64> {
        if current_price <= 0.0
            || !current_price.is_finite()
            || !now_ts.is_finite()
            || lookback_seconds <= 0.0
        {
            return None;
        }
        let target_ts = now_ts - lookback_seconds;
        let &(history_ts, history_price) = self
            .ticks
            .iter()
            .rev()
            .find(|(ts, price)| *ts <= target_ts && *price > 0.0 && price.is_finite())?;
        if target_ts - history_ts > 2.0 {
            return None;
        }
        let raw_bps = (current_price - history_price) / current_price * 10_000.0;
        Some(if direction == "down" {
            -raw_bps
        } else {
            raw_bps
        })
    }
}

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_with_no_ticks_is_none() {
        let mut det = MomentumDetector::new(None, MomentumConfig::default());
        assert!(det.detect("c", 1.0, 4.0, 70_000.0, Some(0.0)).is_none());
    }

    #[test]
    fn upward_move_produces_up_signal() {
        let mut det = MomentumDetector::new(Some(0.5), MomentumConfig::default());
        let t0 = 1_700_000_000.0;
        // Add 60 ticks across a 5-min window, monotonically rising.
        for i in 0..60 {
            det.add_tick(70_000.0 + i as f64 * 5.0, Some(t0 + i as f64 * 5.0));
        }
        let now = t0 + 300.0;
        let sig = det.detect("c", 5.0, 0.5, 70_000.0 + 60.0 * 5.0, Some(now));
        let sig = sig.expect("signal");
        assert_eq!(sig.direction, "up");
        assert!(sig.consistency > 0.5);
        assert!(sig.confidence > 0.0);
    }

    #[test]
    fn directional_impulse_is_signed_to_the_trade_direction() {
        let mut det = MomentumDetector::new(None, MomentumConfig::default());
        for second in 0..=10 {
            det.add_tick(100.0 + second as f64, Some(second as f64));
        }

        let up = det
            .directional_impulse_bps("up", 110.0, 10.0, 10.0)
            .unwrap();
        let down = det
            .directional_impulse_bps("down", 110.0, 10.0, 10.0)
            .unwrap();
        assert!((up - 909.090909).abs() < 1e-5);
        assert!((down + 909.090909).abs() < 1e-5);
    }

    #[test]
    fn rolling_realized_vol_uses_only_causal_lookback_ticks() {
        let mut detector = MomentumDetector::new(None, MomentumConfig::default());
        for second in 0..=60 {
            let price = 100.0 * (1.0 + (second as f64 * 0.0001));
            detector.add_tick(price, Some(second as f64));
        }

        let volatility = detector.rolling_realized_vol(30.0).unwrap();
        assert!(volatility.is_finite());
        assert!((0.05..=5.0).contains(&volatility));
        assert_eq!(detector.rolling_realized_vol(0.0), None);
    }

    #[test]
    fn evict_stale_windows() {
        let mut det = MomentumDetector::new(None, MomentumConfig::default());
        det.set_window_open("a", 1.0);
        det.set_window_open("b", 2.0);
        det.set_window_open("c", 3.0);
        let mut active = HashSet::new();
        active.insert("a".to_string());
        let n = det.evict_stale_windows(&active);
        assert_eq!(n, 2);
        assert!(det.get_open_price("a").is_some());
        assert!(det.get_open_price("b").is_none());
    }
}
