//! Circuit breaker — drawdown + win-rate guard for paper/live trading.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct BreakerConfig {
    pub min_trades: u32,
    pub min_win_rate: f64,
    pub max_drawdown_pct: f64,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            min_trades: 20,
            min_win_rate: 0.65,
            max_drawdown_pct: 0.30,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
pub struct BreakerState {
    pub wins: u64,
    pub losses: u64,
    pub realized_pnl: f64,
    pub peak_pnl: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BreakerMetrics {
    pub total_trades: u64,
    pub win_rate: f64,
    pub realized_drawdown: f64,
    pub realized_drawdown_pct: f64,
    pub open_exposure: f64,
    pub stressed_pnl: f64,
    pub stressed_drawdown: f64,
    pub stressed_drawdown_pct: f64,
}

impl BreakerState {
    pub fn record_resolution(&mut self, won: bool, pnl: f64) {
        if won {
            self.wins += 1;
        } else {
            self.losses += 1;
        }
        self.realized_pnl += pnl;
        if self.realized_pnl > self.peak_pnl {
            self.peak_pnl = self.realized_pnl;
        }
    }

    pub fn correct_resolution(&mut self, provisional_won: bool, final_won: bool, pnl_delta: f64) {
        if provisional_won != final_won {
            if provisional_won {
                self.wins = self.wins.saturating_sub(1);
            } else {
                self.losses = self.losses.saturating_sub(1);
            }
            if final_won {
                self.wins += 1;
            } else {
                self.losses += 1;
            }
        }
        self.realized_pnl += pnl_delta;
        if self.realized_pnl > self.peak_pnl {
            self.peak_pnl = self.realized_pnl;
        }
    }

    pub fn metrics(&self, open_exposure: f64, initial_bankroll: f64) -> BreakerMetrics {
        let total = self.wins + self.losses;
        let win_rate = if total > 0 { self.wins as f64 / total as f64 } else { 0.0 };
        let initial_bankroll = initial_bankroll.max(1.0);
        let realized_drawdown = (self.peak_pnl - self.realized_pnl).max(0.0);
        let realized_drawdown_pct = if self.peak_pnl > 0.0 {
            realized_drawdown / self.peak_pnl
        } else if self.realized_pnl < 0.0 {
            self.realized_pnl.abs() / initial_bankroll
        } else {
            0.0
        };
        let open_exposure = open_exposure.max(0.0);
        let stressed_pnl = self.realized_pnl - open_exposure;
        let stressed_drawdown = (self.peak_pnl - stressed_pnl).max(0.0);
        let stressed_drawdown_pct = if self.peak_pnl > 0.0 {
            stressed_drawdown / self.peak_pnl
        } else if stressed_pnl < 0.0 {
            stressed_pnl.abs() / initial_bankroll
        } else {
            0.0
        };

        BreakerMetrics {
            total_trades: total,
            win_rate,
            realized_drawdown,
            realized_drawdown_pct,
            open_exposure,
            stressed_pnl,
            stressed_drawdown,
            stressed_drawdown_pct,
        }
    }

    /// Should we trip the breaker now?
    ///
    /// Realized drawdown is the primary circuit breaker. Open exposure is
    /// still stress-tested, but bounded paper/live positions are not treated
    /// as realized losses while stressed PnL remains positive.
    pub fn should_trip(&self, cfg: &BreakerConfig, open_exposure: f64, initial_bankroll: f64) -> Option<&'static str> {
        let metrics = self.metrics(open_exposure, initial_bankroll);
        if metrics.total_trades < cfg.min_trades as u64 {
            return None;
        }
        if metrics.win_rate < cfg.min_win_rate {
            return Some("win_rate_low");
        }
        if metrics.realized_drawdown_pct > cfg.max_drawdown_pct {
            return Some("realized_drawdown");
        };
        if metrics.stressed_pnl < 0.0 && metrics.stressed_drawdown_pct > cfg.max_drawdown_pct {
            return Some("open_exposure_stress");
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_trip_below_min_trades() {
        let mut s = BreakerState::default();
        for _ in 0..10 {
            s.record_resolution(false, -1.0);
        }
        assert!(s.should_trip(&BreakerConfig::default(), 0.0, 100.0).is_none());
    }

    #[test]
    fn breaker_state_json_round_trip_preserves_metrics() {
        let mut s = BreakerState::default();
        s.record_resolution(true, 12.5);
        s.record_resolution(false, -4.25);

        let raw = serde_json::to_string(&s).unwrap();
        let restored: BreakerState = serde_json::from_str(&raw).unwrap();

        assert_eq!(restored.wins, 1);
        assert_eq!(restored.losses, 1);
        assert_eq!(restored.realized_pnl, 8.25);
        assert_eq!(restored.peak_pnl, 12.5);
    }

    #[test]
    fn trips_on_low_win_rate() {
        let mut s = BreakerState::default();
        for _ in 0..30 {
            s.record_resolution(false, -1.0);
        }
        assert_eq!(
            s.should_trip(&BreakerConfig::default(), 0.0, 100.0),
            Some("win_rate_low"),
        );
    }

    #[test]
    fn trips_on_drawdown() {
        let mut s = BreakerState::default();
        for _ in 0..20 {
            s.record_resolution(true, 5.0);
        }
        for _ in 0..10 {
            s.record_resolution(false, -10.0);
        }
        let trip = s.should_trip(&BreakerConfig::default(), 0.0, 100.0);
        assert_eq!(trip, Some("realized_drawdown"));
    }

    #[test]
    fn does_not_trip_on_positive_open_exposure_stress() {
        let mut s = BreakerState::default();
        for _ in 0..30 {
            s.record_resolution(true, 1.0);
        }
        let trip = s.should_trip(&BreakerConfig::default(), 20.0, 100.0);
        assert_eq!(trip, None);
    }

    #[test]
    fn trips_on_negative_open_exposure_stress() {
        let mut s = BreakerState::default();
        for _ in 0..30 {
            s.record_resolution(true, 1.0);
        }
        let trip = s.should_trip(&BreakerConfig::default(), 50.0, 100.0);
        assert_eq!(trip, Some("open_exposure_stress"));
    }

    #[test]
    fn correction_moves_win_to_loss_and_adjusts_pnl() {
        let mut s = BreakerState::default();
        s.record_resolution(true, 10.0);
        s.correct_resolution(true, false, -15.0);

        assert_eq!(s.wins, 0);
        assert_eq!(s.losses, 1);
        assert_eq!(s.realized_pnl, -5.0);
        assert_eq!(s.peak_pnl, 10.0);
    }
}
