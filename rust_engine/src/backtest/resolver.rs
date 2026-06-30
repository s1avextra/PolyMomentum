//! Resolve backtest fills against actual BTC outcomes.
//!
//! After `L2BacktestEngine` runs, each fill is a hypothetical entry in some
//! candle window. This module pairs each fill with the BTC open/close prices
//! at that window and computes realized P&L.

use std::collections::BTreeMap;

use crate::backtest::btc_history::BTCHistory;
use crate::backtest::l2_replay::BacktestFill;
use crate::live::breaker::{BreakerMetrics, BreakerState};
use crate::strategy::decision::CandleDecision;
#[cfg(test)]
use crate::strategy::decision::DecisionRegime;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResolvedTrade {
    pub fill: BacktestFill,
    pub decision: CandleDecision,
    pub open_btc: f64,
    pub close_btc: f64,
    #[serde(default)]
    pub local_direction: String,
    pub actual_direction: String,
    #[serde(default)]
    pub resolution_source: String,
    #[serde(default)]
    pub resolution_disagreed: bool,
    pub won: bool,
    /// Realized P&L *before* fees: (1 - fill_price) * size on win, -fill_price * size on loss.
    pub pnl: f64,
    /// Realized P&L net of fees.
    pub pnl_after_fee: f64,
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct BacktestResults {
    pub trades: Vec<ResolvedTrade>,
    pub unresolved_fills: Vec<BacktestFill>,
    #[serde(default)]
    pub breaker: BacktestBreakerReport,
    #[serde(default)]
    pub diagnostics: BacktestDiagnostics,
    #[serde(default)]
    pub execution_attempts: usize,
    #[serde(default)]
    pub fills_success: usize,
    #[serde(default)]
    pub fills_failed: usize,
    #[serde(default)]
    pub reject_reasons: BTreeMap<String, usize>,
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct BacktestDiagnostics {
    pub events_seen: u64,
    pub events_for_known_token: u64,
    pub skipped_resolved: u64,
    pub skipped_too_early: u64,
    pub skipped_no_btc: u64,
    pub skipped_no_signal: u64,
    pub skipped_decision: u64,
    pub skipped_throttled: u64,
    #[serde(default)]
    pub breaker_paused_events: u64,
    #[serde(default)]
    pub adaptive_rearms: u64,
    #[serde(default)]
    pub trade_pnl: TradePnlDiagnostics,
    #[serde(default)]
    pub by_regime: BTreeMap<String, TradePnlDiagnostics>,
    #[serde(default)]
    pub by_causal_bucket: BTreeMap<String, TradePnlDiagnostics>,
    pub skip_reasons: BTreeMap<String, u64>,
}

impl BacktestDiagnostics {
    pub fn merge_from(&mut self, other: Self) {
        self.events_seen += other.events_seen;
        self.events_for_known_token += other.events_for_known_token;
        self.skipped_resolved += other.skipped_resolved;
        self.skipped_too_early += other.skipped_too_early;
        self.skipped_no_btc += other.skipped_no_btc;
        self.skipped_no_signal += other.skipped_no_signal;
        self.skipped_decision += other.skipped_decision;
        self.skipped_throttled += other.skipped_throttled;
        self.breaker_paused_events += other.breaker_paused_events;
        self.adaptive_rearms += other.adaptive_rearms;
        self.trade_pnl.merge_from(&other.trade_pnl);
        merge_trade_pnl_maps(&mut self.by_regime, &other.by_regime);
        merge_trade_pnl_maps(&mut self.by_causal_bucket, &other.by_causal_bucket);
        for (reason, count) in other.skip_reasons {
            *self.skip_reasons.entry(reason).or_insert(0) += count;
        }
    }
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct TradePnlDiagnostics {
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub win_rate: f64,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub gross_win_pnl: f64,
    pub gross_loss_pnl: f64,
    pub avg_win_pnl: f64,
    pub avg_loss_pnl: f64,
    pub max_win_pnl: f64,
    pub max_loss_pnl: f64,
    pub profit_factor: f64,
    pub payoff_ratio: f64,
    pub worst_loss_to_avg_win: f64,
}

impl TradePnlDiagnostics {
    pub fn add_trade(&mut self, t: &ResolvedTrade) {
        self.trades += 1;
        self.total_pnl += t.pnl_after_fee;
        if t.won {
            if self.wins == 0 || t.pnl_after_fee > self.max_win_pnl {
                self.max_win_pnl = t.pnl_after_fee;
            }
            self.wins += 1;
            self.gross_win_pnl += t.pnl_after_fee;
        } else {
            if self.losses == 0 || t.pnl_after_fee < self.max_loss_pnl {
                self.max_loss_pnl = t.pnl_after_fee;
            }
            self.losses += 1;
            self.gross_loss_pnl += t.pnl_after_fee;
        }
        self.recompute();
    }

    pub fn merge_from(&mut self, other: &Self) {
        if other.trades == 0 {
            return;
        }
        if self.wins == 0 || (other.wins > 0 && other.max_win_pnl > self.max_win_pnl) {
            self.max_win_pnl = other.max_win_pnl;
        }
        if self.losses == 0 || (other.losses > 0 && other.max_loss_pnl < self.max_loss_pnl) {
            self.max_loss_pnl = other.max_loss_pnl;
        }
        self.trades += other.trades;
        self.wins += other.wins;
        self.losses += other.losses;
        self.total_pnl += other.total_pnl;
        self.gross_win_pnl += other.gross_win_pnl;
        self.gross_loss_pnl += other.gross_loss_pnl;
        self.recompute();
    }

    fn recompute(&mut self) {
        let resolved = self.wins + self.losses;
        self.win_rate = if resolved == 0 {
            0.0
        } else {
            self.wins as f64 / resolved as f64
        };
        self.avg_pnl = if self.trades == 0 {
            0.0
        } else {
            self.total_pnl / self.trades as f64
        };
        self.avg_win_pnl = if self.wins == 0 {
            0.0
        } else {
            self.gross_win_pnl / self.wins as f64
        };
        self.avg_loss_pnl = if self.losses == 0 {
            0.0
        } else {
            self.gross_loss_pnl / self.losses as f64
        };
        let abs_loss = self.gross_loss_pnl.abs();
        self.profit_factor = finite_ratio(self.gross_win_pnl, abs_loss);
        self.payoff_ratio = finite_ratio(self.avg_win_pnl, self.avg_loss_pnl.abs());
        self.worst_loss_to_avg_win = finite_ratio(self.max_loss_pnl.abs(), self.avg_win_pnl);
    }
}

fn finite_ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator > 1e-12 {
        numerator / denominator
    } else if numerator > 0.0 {
        999.0
    } else {
        0.0
    }
}

fn merge_trade_pnl_maps(
    dest: &mut BTreeMap<String, TradePnlDiagnostics>,
    src: &BTreeMap<String, TradePnlDiagnostics>,
) {
    for (key, stats) in src {
        dest.entry(key.clone()).or_default().merge_from(stats);
    }
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct BacktestBreakerReport {
    pub tripped: bool,
    pub reason: Option<String>,
    pub tripped_at_s: Option<f64>,
    pub state: BreakerState,
    pub metrics: BreakerMetrics,
}

impl BacktestBreakerReport {
    pub fn from_state(
        state: BreakerState,
        open_exposure: f64,
        initial_bankroll: f64,
        tripped: bool,
        reason: Option<String>,
        tripped_at_s: Option<f64>,
    ) -> Self {
        Self {
            tripped,
            reason,
            tripped_at_s,
            state,
            metrics: state.metrics(open_exposure, initial_bankroll),
        }
    }

    pub fn has_state(&self) -> bool {
        self.tripped
            || self.reason.is_some()
            || self.tripped_at_s.is_some()
            || self.state.wins + self.state.losses > 0
            || self.state.realized_pnl.abs() > 1e-9
            || self.state.peak_pnl.abs() > 1e-9
    }
}

impl BacktestResults {
    pub fn from_fills(fills: &[BacktestFill]) -> Self {
        let execution_attempts = fills.len();
        let fills_success = fills.iter().filter(|f| f.success).count();
        let fills_failed = execution_attempts.saturating_sub(fills_success);
        let mut reject_reasons = BTreeMap::new();
        for fill in fills.iter().filter(|f| !f.success) {
            let reason = if fill.reason.trim().is_empty() {
                "unknown".to_string()
            } else {
                fill.reason.clone()
            };
            *reject_reasons.entry(reason).or_insert(0) += 1;
        }
        Self {
            execution_attempts,
            fills_success,
            fills_failed,
            reject_reasons,
            ..Self::default()
        }
    }

    pub fn merge_from(&mut self, other: Self) {
        self.trades.extend(other.trades);
        self.unresolved_fills.extend(other.unresolved_fills);
        if other.breaker.has_state() || !self.breaker.has_state() {
            self.breaker = other.breaker;
        }
        self.diagnostics.merge_from(other.diagnostics);
        self.execution_attempts += other.execution_attempts;
        self.fills_success += other.fills_success;
        self.fills_failed += other.fills_failed;
        for (reason, count) in other.reject_reasons {
            *self.reject_reasons.entry(reason).or_insert(0) += count;
        }
    }

    pub fn fill_rate(&self) -> f64 {
        if self.execution_attempts == 0 {
            0.0
        } else {
            self.fills_success as f64 / self.execution_attempts as f64
        }
    }

    pub fn n_trades(&self) -> usize {
        self.trades.len()
    }

    pub fn n_wins(&self) -> usize {
        self.trades.iter().filter(|t| t.won).count()
    }

    pub fn n_losses(&self) -> usize {
        self.trades.len() - self.n_wins()
    }

    pub fn win_rate(&self) -> f64 {
        if self.trades.is_empty() {
            0.0
        } else {
            self.n_wins() as f64 / self.trades.len() as f64
        }
    }

    pub fn total_pnl(&self) -> f64 {
        self.trades.iter().map(|t| t.pnl_after_fee).sum()
    }

    pub fn total_fees(&self) -> f64 {
        self.trades.iter().map(|t| t.fill.fee).sum()
    }

    pub fn avg_pnl(&self) -> f64 {
        if self.trades.is_empty() {
            0.0
        } else {
            self.total_pnl() / self.trades.len() as f64
        }
    }

    pub fn sharpe(&self) -> f64 {
        if self.trades.len() < 2 {
            return 0.0;
        }
        let pnls: Vec<f64> = self.trades.iter().map(|t| t.pnl_after_fee).collect();
        let mean = pnls.iter().sum::<f64>() / pnls.len() as f64;
        let var = pnls.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / pnls.len() as f64;
        let std = var.sqrt();
        if std > 0.0 {
            mean / std
        } else {
            0.0
        }
    }

    pub fn by_zone(&self) -> BTreeMap<String, ZoneBucket> {
        let mut out: BTreeMap<String, ZoneBucket> = BTreeMap::new();
        for t in &self.trades {
            let bucket = out.entry(t.decision.zone.clone()).or_default();
            bucket.add(t);
        }
        out
    }

    pub fn pnl_diagnostics(&self) -> TradePnlDiagnostics {
        let mut out = TradePnlDiagnostics::default();
        for t in &self.trades {
            out.add_trade(t);
        }
        out
    }

    pub fn by_regime(&self) -> BTreeMap<String, TradePnlDiagnostics> {
        let mut out: BTreeMap<String, TradePnlDiagnostics> = BTreeMap::new();
        for t in &self.trades {
            out.entry(t.decision.regime.key()).or_default().add_trade(t);
        }
        out
    }

    pub fn by_causal_bucket(&self) -> BTreeMap<String, TradePnlDiagnostics> {
        let mut out: BTreeMap<String, TradePnlDiagnostics> = BTreeMap::new();
        for t in &self.trades {
            for (name, value) in t.decision.regime.causal_tags() {
                out.entry(format!("{name}={value}"))
                    .or_default()
                    .add_trade(t);
            }
        }
        out
    }
}

#[derive(Debug, Default, Clone)]
pub struct ZoneBucket {
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub pnl: f64,
}

impl ZoneBucket {
    fn add(&mut self, t: &ResolvedTrade) {
        self.trades += 1;
        if t.won {
            self.wins += 1;
        } else {
            self.losses += 1;
        }
        self.pnl += t.pnl_after_fee;
    }

    pub fn win_rate(&self) -> f64 {
        let r = self.wins + self.losses;
        if r == 0 {
            0.0
        } else {
            self.wins as f64 / r as f64
        }
    }
}

/// Window descriptor used by the resolver to look up BTC prices.
#[derive(Debug, Clone)]
pub struct CandleWindow {
    pub condition_id: String,
    /// Start of the window in seconds since epoch.
    pub open_ts_s: f64,
    /// Close of the window in seconds since epoch.
    pub close_ts_s: f64,
    /// Polymarket terminal outcome when historical metadata already has it.
    pub official_direction: Option<String>,
}

/// Resolve a list of fills + decisions against the BTC tape. Each fill must
/// carry a `condition_id` (via `BacktestFill.order.condition_id`) that can be
/// looked up in `windows`.
pub fn resolve_fills(
    fills: &[BacktestFill],
    decisions: &[CandleDecision],
    windows: &[CandleWindow],
    btc_history: &BTCHistory,
) -> BacktestResults {
    assert_eq!(
        fills.len(),
        decisions.len(),
        "resolve_fills: fills and decisions must align 1:1"
    );

    let window_by_cid: BTreeMap<String, &CandleWindow> = windows
        .iter()
        .map(|w| (w.condition_id.clone(), w))
        .collect();

    let mut results = BacktestResults::from_fills(fills);
    for (fill, decision) in fills.iter().zip(decisions) {
        if !fill.success {
            continue;
        }
        let cid = match fill.order.condition_id.as_str() {
            "" => {
                results.unresolved_fills.push(fill.clone());
                continue;
            }
            c => c,
        };
        let Some(window) = window_by_cid.get(cid) else {
            results.unresolved_fills.push(fill.clone());
            continue;
        };

        let open_btc = btc_history.price_at_seconds(window.open_ts_s);
        let close_btc = btc_history.price_at_seconds(window.close_ts_s);
        if open_btc <= 0.0 || close_btc <= 0.0 {
            results.unresolved_fills.push(fill.clone());
            continue;
        }

        let local_actual = if close_btc >= open_btc { "up" } else { "down" };
        let (actual, resolution_source) = match window.official_direction.as_deref() {
            Some(direction) => (direction, "polymarket_terminal"),
            None => (local_actual, "btc_close"),
        };
        let won = decision.direction == actual;
        let pnl = if won {
            (1.0 - fill.fill_price) * fill.filled_size
        } else {
            -fill.fill_price * fill.filled_size
        };
        let pnl_after_fee = pnl - fill.fee;

        results.trades.push(ResolvedTrade {
            fill: fill.clone(),
            decision: decision.clone(),
            open_btc,
            close_btc,
            local_direction: local_actual.to_string(),
            actual_direction: actual.to_string(),
            resolution_source: resolution_source.to_string(),
            resolution_disagreed: actual != local_actual,
            won,
            pnl,
            pnl_after_fee,
        });
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::l2_replay::{BacktestFill, BacktestOrder};
    use crate::data::models::DEFAULT_CRYPTO_TAKER_FEE_RATE;

    fn mk_history() -> BTCHistory {
        let mut h = BTCHistory::default();
        // Build a tick stream from 70_000 to 70_100 over 600 s.
        for i in 0..=600 {
            h.timestamps_ms.push(1_700_000_000_000 + i * 1000);
            h.prices.push(70_000.0 + (i as f64 / 600.0) * 100.0);
        }
        h
    }

    fn mk_decision(direction: &str) -> CandleDecision {
        CandleDecision {
            direction: direction.to_string(),
            confidence: 0.7,
            z_score: 1.5,
            zone: "terminal".to_string(),
            fair_value: 0.6,
            market_price: 0.4,
            edge: 0.2,
            minutes_remaining: 0.05,
            yes_no_vig: 0.0,
            regime: DecisionRegime::default(),
        }
    }

    fn mk_fill(cid: &str, fill_price: f64, size: f64, fee: f64) -> BacktestFill {
        BacktestFill {
            order: BacktestOrder {
                intent_id: "test-intent".to_string(),
                timestamp_s: 1_700_000_000.0,
                condition_id: cid.to_string(),
                token_id: "tok".to_string(),
                side: "buy".to_string(),
                size,
                order_type: "market".to_string(),
                limit_price: None,
                fee_rate: DEFAULT_CRYPTO_TAKER_FEE_RATE,
                maker_fee_rate: 0.0,
            },
            fill_timestamp_s: 1_700_000_001.0,
            fill_price,
            filled_size: size,
            cost: fill_price * size,
            fee,
            slippage: 0.01,
            book_age_ms: 0.0,
            success: true,
            reason: "".to_string(),
        }
    }

    fn mk_failed_fill(reason: &str) -> BacktestFill {
        let mut fill = mk_fill("c1", 0.40, 10.0, 0.0);
        fill.success = false;
        fill.filled_size = 0.0;
        fill.cost = 0.0;
        fill.reason = reason.to_string();
        fill
    }

    fn mk_window(cid: &str, official_direction: Option<&str>) -> CandleWindow {
        CandleWindow {
            condition_id: cid.into(),
            open_ts_s: 1_700_000_000.0,
            close_ts_s: 1_700_000_300.0,
            official_direction: official_direction.map(str::to_string),
        }
    }

    #[test]
    fn resolves_winning_up_trade() {
        let h = mk_history();
        let windows = vec![mk_window("c1", None)];
        let fills = vec![mk_fill("c1", 0.40, 10.0, 0.10)];
        let decisions = vec![mk_decision("up")];
        let res = resolve_fills(&fills, &decisions, &windows, &h);
        assert_eq!(res.n_trades(), 1);
        let t = &res.trades[0];
        assert!(t.won);
        assert_eq!(t.local_direction, "up");
        assert_eq!(t.actual_direction, "up");
        assert_eq!(t.resolution_source, "btc_close");
        assert!(!t.resolution_disagreed);
        // pnl = (1 - 0.40) * 10 = 6.0; minus fee 0.10 = 5.90
        assert!((t.pnl_after_fee - 5.9).abs() < 1e-9);
    }

    #[test]
    fn resolves_losing_down_trade() {
        let h = mk_history();
        let windows = vec![mk_window("c1", None)];
        let fills = vec![mk_fill("c1", 0.40, 10.0, 0.10)];
        let decisions = vec![mk_decision("down")]; // BTC went up, we predicted down → loss
        let res = resolve_fills(&fills, &decisions, &windows, &h);
        assert_eq!(res.n_trades(), 1);
        let t = &res.trades[0];
        assert!(!t.won);
        // pnl = -0.40 * 10 = -4.0; minus fee 0.10 = -4.10
        assert!((t.pnl_after_fee + 4.1).abs() < 1e-9);
    }

    #[test]
    fn official_terminal_outcome_overrides_btc_close() {
        let h = mk_history();
        let windows = vec![mk_window("c1", Some("down"))];
        let fills = vec![mk_fill("c1", 0.40, 10.0, 0.10)];
        let decisions = vec![mk_decision("down")];

        let res = resolve_fills(&fills, &decisions, &windows, &h);

        assert_eq!(res.n_trades(), 1);
        let t = &res.trades[0];
        assert!(t.won);
        assert_eq!(t.local_direction, "up");
        assert_eq!(t.actual_direction, "down");
        assert_eq!(t.resolution_source, "polymarket_terminal");
        assert!(t.resolution_disagreed);
        assert!((t.pnl_after_fee - 5.9).abs() < 1e-9);
    }

    #[test]
    fn unknown_window_marks_fill_unresolved() {
        let h = mk_history();
        let fills = vec![mk_fill("missing", 0.40, 10.0, 0.10)];
        let decisions = vec![mk_decision("up")];
        let res = resolve_fills(&fills, &decisions, &[], &h);
        assert_eq!(res.n_trades(), 0);
        assert_eq!(res.unresolved_fills.len(), 1);
    }

    #[test]
    fn failed_execution_attempt_is_not_unresolved_exposure() {
        let h = mk_history();
        let windows = vec![mk_window("c1", None)];
        let fills = vec![mk_failed_fill("maker_unfilled")];
        let decisions = vec![mk_decision("up")];

        let res = resolve_fills(&fills, &decisions, &windows, &h);

        assert_eq!(res.n_trades(), 0);
        assert_eq!(res.fills_failed, 1);
        assert_eq!(res.reject_reasons.get("maker_unfilled"), Some(&1));
        assert!(res.unresolved_fills.is_empty());
    }

    #[test]
    fn pnl_diagnostics_preserve_win_loss_asymmetry() {
        let h = mk_history();
        let windows = vec![mk_window("c1", None)];
        let fills = vec![
            mk_fill("c1", 0.40, 10.0, 0.10),
            mk_fill("c1", 0.40, 10.0, 0.10),
        ];
        let decisions = vec![mk_decision("up"), mk_decision("down")];

        let res = resolve_fills(&fills, &decisions, &windows, &h);
        let pnl = res.pnl_diagnostics();

        assert_eq!(pnl.trades, 2);
        assert_eq!(pnl.wins, 1);
        assert_eq!(pnl.losses, 1);
        assert!((pnl.gross_win_pnl - 5.9).abs() < 1e-9);
        assert!((pnl.gross_loss_pnl + 4.1).abs() < 1e-9);
        assert!((pnl.payoff_ratio - (5.9 / 4.1)).abs() < 1e-9);
    }

    #[test]
    fn causal_bucket_diagnostics_use_decision_regime() {
        let h = mk_history();
        let windows = vec![mk_window("c1", None)];
        let fills = vec![mk_fill("c1", 0.40, 10.0, 0.10)];
        let mut decision = mk_decision("up");
        decision.regime = DecisionRegime {
            zone: "terminal".to_string(),
            direction: "up".to_string(),
            price_bucket: "0.25_0.50".to_string(),
            edge_bucket: "0.07_0.15".to_string(),
            z_bucket: "1.1_1.5".to_string(),
            confidence_bucket: "0.70_0.85".to_string(),
            volatility_bucket: "0.40_0.80".to_string(),
            reversion_bucket: "1_2".to_string(),
            reversion_count: 1,
            minutes_remaining_bucket: "lte_1".to_string(),
            ..DecisionRegime::default()
        };
        decision
            .regime
            .attach_orderbook_inputs(0.49, 0.51, 0.02, 125.0, 80.0, 0.20, -0.22);

        let res = resolve_fills(&fills, &[decision], &windows, &h);
        let by_regime = res.by_regime();
        let by_bucket = res.by_causal_bucket();

        assert_eq!(by_regime.len(), 1);
        assert!(by_regime
            .keys()
            .next()
            .unwrap()
            .contains("book_spread=0.01_0.03"));
        assert_eq!(by_bucket["price=0.25_0.50"].trades, 1);
        assert_eq!(by_bucket["reversion=1_2"].wins, 1);
        assert_eq!(by_bucket["book_min_depth=50_100"].trades, 1);
        assert_eq!(by_bucket["book_pressure=positive"].wins, 1);
    }
}
