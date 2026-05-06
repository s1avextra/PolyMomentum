//! Circuit breaker — drawdown + win-rate guard for paper/live trading.

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

#[derive(Debug, Clone, Copy, Default)]
pub struct BreakerState {
    pub wins: u64,
    pub losses: u64,
    pub realized_pnl: f64,
    pub peak_pnl: f64,
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

    /// Should we trip the breaker now?
    ///
    /// `open_exposure` is the total $ at risk in open paper positions —
    /// folded into effective PnL so a sudden bankroll concentration trips
    /// the breaker without waiting for resolution.
    pub fn should_trip(&self, cfg: &BreakerConfig, open_exposure: f64, initial_bankroll: f64) -> Option<&'static str> {
        let total = self.wins + self.losses;
        if total < cfg.min_trades as u64 {
            return None;
        }
        let win_rate = self.wins as f64 / total as f64;
        if win_rate < cfg.min_win_rate {
            return Some("win_rate_low");
        }
        let effective_pnl = self.realized_pnl - open_exposure;
        let drawdown = self.peak_pnl - effective_pnl;
        let dd_pct = if self.peak_pnl > 0.0 {
            drawdown / self.peak_pnl
        } else {
            effective_pnl.abs() / initial_bankroll.max(1.0)
        };
        if dd_pct > cfg.max_drawdown_pct {
            return Some("drawdown");
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
        // Build up to peak
        for _ in 0..30 {
            s.record_resolution(true, 1.0);
        }
        // Then take a big drawdown via open exposure
        let trip = s.should_trip(&BreakerConfig::default(), 50.0, 100.0);
        assert_eq!(trip, Some("drawdown"));
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
