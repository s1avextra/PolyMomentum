//! Circuit breaker — drawdown + win-rate guard for paper/live trading.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct BreakerConfig {
    pub min_trades: u32,
    pub min_win_rate: f64,
    pub max_drawdown_pct: f64,
    /// Absolute session-loss floor as a fraction of initial bankroll. Trips at
    /// ANY trade count — the first live session of a new strategy is exactly
    /// when the model is most likely wrong, so this must not wait for
    /// `min_trades` resolutions. 0 disables.
    pub max_session_loss_pct: f64,
    /// Consecutive-loss streak limit, also ungated by `min_trades`.
    /// At a promoted 65% win rate, 8 straight losses has p ≈ 2e-4 under the
    /// model — a "model is wrong" signal, not variance. 0 disables.
    pub max_consecutive_losses: u32,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            min_trades: 20,
            min_win_rate: 0.65,
            max_drawdown_pct: 0.30,
            max_session_loss_pct: 0.20,
            max_consecutive_losses: 8,
        }
    }
}

impl BreakerConfig {
    pub fn from_settings(settings: &crate::config::Settings) -> Self {
        Self {
            min_trades: settings.candle_breaker_min_trades.max(1) as u32,
            min_win_rate: settings.candle_breaker_min_win_rate,
            max_drawdown_pct: settings.candle_breaker_max_drawdown_pct,
            max_session_loss_pct: settings.candle_breaker_max_session_loss_pct,
            max_consecutive_losses: settings.candle_breaker_max_consecutive_losses.max(0) as u32,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
pub struct BreakerState {
    pub wins: u64,
    pub losses: u64,
    pub realized_pnl: f64,
    pub peak_pnl: f64,
    /// Current losing streak. `serde(default)` keeps previously persisted
    /// state (which lacks the field) loadable; the streak then restarts at 0.
    #[serde(default)]
    pub consecutive_losses: u64,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
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
            self.consecutive_losses = 0;
        } else {
            self.losses += 1;
            self.consecutive_losses += 1;
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
            // Oracle corrections arrive out of order, so the exact streak is
            // unrecoverable; adjust conservatively in the correction's
            // direction rather than replaying history.
            if final_won {
                self.consecutive_losses = 0;
            } else {
                self.consecutive_losses += 1;
            }
        }
        self.realized_pnl += pnl_delta;
        if self.realized_pnl > self.peak_pnl {
            self.peak_pnl = self.realized_pnl;
        }
    }

    pub fn metrics(&self, open_exposure: f64, initial_bankroll: f64) -> BreakerMetrics {
        let total = self.wins + self.losses;
        let win_rate = if total > 0 {
            self.wins as f64 / total as f64
        } else {
            0.0
        };
        let initial_bankroll = initial_bankroll.max(1.0);
        let realized_drawdown = (self.peak_pnl - self.realized_pnl).max(0.0);
        let peak_equity = (initial_bankroll + self.peak_pnl).max(initial_bankroll);
        let realized_drawdown_pct = if realized_drawdown > 0.0 {
            realized_drawdown / peak_equity
        } else if self.realized_pnl < 0.0 {
            self.realized_pnl.abs() / initial_bankroll
        } else {
            0.0
        };
        let open_exposure = open_exposure.max(0.0);
        let stressed_pnl = self.realized_pnl - open_exposure;
        let stressed_drawdown = (self.peak_pnl - stressed_pnl).max(0.0);
        let stressed_drawdown_pct = if stressed_drawdown > 0.0 {
            stressed_drawdown / peak_equity
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
    /// Loss-magnitude checks (session-loss floor, consecutive losses,
    /// drawdown, exposure stress) fire at ANY trade count — 19 max-size
    /// losses in a row must not be survivable just because `min_trades`
    /// hasn't been reached. Only the win-rate check stays behind
    /// `min_trades`, because a rate is meaningless on a tiny sample while
    /// a loss magnitude is not.
    pub fn should_trip(
        &self,
        cfg: &BreakerConfig,
        open_exposure: f64,
        initial_bankroll: f64,
    ) -> Option<&'static str> {
        let metrics = self.metrics(open_exposure, initial_bankroll);
        if cfg.max_session_loss_pct > 0.0
            && self.realized_pnl <= -cfg.max_session_loss_pct * initial_bankroll.max(1.0)
        {
            return Some("session_loss_floor");
        }
        if cfg.max_consecutive_losses > 0
            && self.consecutive_losses >= cfg.max_consecutive_losses as u64
        {
            return Some("consecutive_losses");
        }
        if metrics.realized_drawdown_pct > cfg.max_drawdown_pct {
            return Some("realized_drawdown");
        };
        if metrics.stressed_pnl < 0.0 && metrics.stressed_drawdown_pct > cfg.max_drawdown_pct {
            return Some("open_exposure_stress");
        }
        if metrics.total_trades < cfg.min_trades as u64 {
            return None;
        }
        if metrics.win_rate < cfg.min_win_rate {
            return Some("win_rate_low");
        }
        None
    }

    /// Remaining exposure headroom before projected stressed drawdown crosses
    /// `max_projected_stressed_drawdown_pct`.
    ///
    /// This is a feed-forward sizing guard: it uses only realized breaker
    /// state plus currently open exposure. Callers can cap a new order by the
    /// returned value to slow down before the hard breaker trips.
    pub fn stressed_drawdown_exposure_headroom(
        &self,
        open_exposure: f64,
        initial_bankroll: f64,
        max_projected_stressed_drawdown_pct: f64,
    ) -> Option<f64> {
        if !(max_projected_stressed_drawdown_pct.is_finite()
            && max_projected_stressed_drawdown_pct > 0.0)
        {
            return None;
        }
        let initial_bankroll = initial_bankroll.max(1.0);
        let peak_equity = (initial_bankroll + self.peak_pnl).max(initial_bankroll);
        let max_stressed_drawdown = max_projected_stressed_drawdown_pct * peak_equity;
        let current_stressed_pnl = self.realized_pnl - open_exposure.max(0.0);
        let current_stressed_drawdown = (self.peak_pnl - current_stressed_pnl).max(0.0);
        Some((max_stressed_drawdown - current_stressed_drawdown).max(0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn win_rate_check_stays_gated_below_min_trades() {
        let mut s = BreakerState::default();
        // 3W/4L alternating: WR 43% < 65%, but only 7 trades, tiny loss,
        // streak ≤ 2 — no loss-magnitude check applies, and the win-rate
        // check must stay silent below min_trades.
        for i in 0..7 {
            s.record_resolution(i % 2 == 0, if i % 2 == 0 { 0.4 } else { -0.5 });
        }
        assert!(s
            .should_trip(&BreakerConfig::default(), 0.0, 100.0)
            .is_none());
    }

    #[test]
    fn session_loss_floor_trips_at_any_trade_count() {
        let mut s = BreakerState::default();
        // Two max-size losses on a $100 bankroll: -21% crosses the 20% floor
        // long before min_trades=20 resolutions exist.
        s.record_resolution(false, -10.5);
        s.record_resolution(false, -10.5);
        assert_eq!(
            s.should_trip(&BreakerConfig::default(), 0.0, 100.0),
            Some("session_loss_floor"),
        );
    }

    #[test]
    fn consecutive_losses_trip_before_min_trades() {
        let mut s = BreakerState::default();
        for _ in 0..8 {
            s.record_resolution(false, -1.0);
        }
        assert_eq!(
            s.should_trip(&BreakerConfig::default(), 0.0, 100.0),
            Some("consecutive_losses"),
        );
    }

    #[test]
    fn win_resets_consecutive_loss_streak() {
        let mut s = BreakerState::default();
        for _ in 0..7 {
            s.record_resolution(false, -1.0);
        }
        s.record_resolution(true, 1.0);
        assert_eq!(s.consecutive_losses, 0);
        assert!(s
            .should_trip(&BreakerConfig::default(), 0.0, 100.0)
            .is_none());
    }

    #[test]
    fn legacy_persisted_state_without_streak_field_still_loads() {
        let raw = r#"{"wins":3,"losses":2,"realized_pnl":1.5,"peak_pnl":2.0}"#;
        let restored: BreakerState = serde_json::from_str(raw).unwrap();
        assert_eq!(restored.wins, 3);
        assert_eq!(restored.consecutive_losses, 0);
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
        // 12W/18L alternating with small stakes: no floor (-4.2%), no streak,
        // no drawdown breach — only the 40% win rate over 30 trades trips.
        for i in 0..30 {
            s.record_resolution(i % 5 < 2, if i % 5 < 2 { 0.4 } else { -0.5 });
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
        // Draw down 45% from peak equity via 3L+1W cycles so the streak
        // stays below the consecutive-loss limit and pnl stays positive
        // (no session floor) — isolating the drawdown check.
        for _ in 0..3 {
            for _ in 0..3 {
                s.record_resolution(false, -10.0);
            }
            s.record_resolution(true, 0.1);
        }
        let trip = s.should_trip(&BreakerConfig::default(), 0.0, 100.0);
        assert_eq!(trip, Some("realized_drawdown"));
    }

    #[test]
    fn profitable_giveback_uses_equity_drawdown_not_pnl_peak() {
        let s = BreakerState {
            realized_pnl: 39.1973,
            peak_pnl: 67.8299,
            wins: 26,
            losses: 12,
            ..Default::default()
        };

        let metrics = s.metrics(0.0, 100.0);

        assert!(metrics.realized_drawdown_pct < 0.30);
        assert_eq!(s.should_trip(&BreakerConfig::default(), 0.0, 100.0), None);
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
    fn stressed_drawdown_headroom_caps_projected_exposure_feed_forward() {
        let s = BreakerState {
            peak_pnl: 20.0,
            realized_pnl: 5.0,
            ..Default::default()
        };

        let headroom = s
            .stressed_drawdown_exposure_headroom(5.0, 100.0, 0.25)
            .unwrap();

        // Peak equity is 120, so 25% allows 30 of stressed drawdown. Current
        // stressed drawdown is peak 20 - stressed pnl 0 = 20, leaving 10.
        assert!((headroom - 10.0).abs() < 1e-9);
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
