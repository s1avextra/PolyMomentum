//! Backtest harness — full A/B runner over PMXT v2 hours.
//!
//! Pipeline per strategy variant:
//!   1. Reset L2 engine + book state
//!   2. Replay each requested PMXT v2 hour
//!   3. Apply momentum + decision on each book update against BTC tape
//!   4. Resolve fills against actual BTC outcomes
//!   5. Aggregate
//!
//! Outputs a per-variant `BacktestResults` you can format with
//! [`render_table`].

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rayon::prelude::*;

use crate::artifact::write_json_atomic;
use crate::backtest::btc_history::BTCHistory;
use crate::backtest::fill_model::{
    resting_limit_price, BookWalkTaker, Maker, Perfect, Side, DEFAULT_TICK,
};
use crate::backtest::l2_replay::{
    BacktestOrder, FillModel, L2BacktestEngine, L2MidHistory, StaticLatencyConfig, Strategy,
    TokenBook,
};
use crate::backtest::pmxt::{L2Event, PMXTv2Loader};
use crate::backtest::resolver::{
    resolve_fills, BacktestBreakerReport, BacktestDiagnostics, BacktestResults, CandleWindow,
};
use crate::backtest::strategies::StrategyVariant;
use crate::data::scanner::CandleContract;
use crate::execution::sizing::{
    buy_book_quote_from_budget, sell_book_quote_for_size, shares_from_budget, BuyBookQuote,
    SellBookQuote,
};
use crate::live::breaker::{BreakerConfig, BreakerState};
use crate::strategy::decision::{evaluate_candle_trade_with_fee, CandleDecision, DecisionResult};
use crate::strategy::microstructure::{
    binary_complement_microstructure, bookwalk_buy_slippage as bookwalk_buy_slippage_from_levels,
    recent_mid_logit_change, recent_mid_runup, BookLevelView, BookMicrostructure,
};
use crate::strategy::momentum::{MomentumConfig, MomentumDetector};
use crate::strategy::spec::{OrderIntent, Signal, StrategySpec};

const DEFAULT_EXPOSURE_RATIO: f64 = 0.80;

#[derive(Debug, Clone)]
pub struct CandleUniverse {
    pub contracts: Vec<CandleContract>,
}

impl CandleUniverse {
    pub fn condition_id_set(&self) -> HashSet<String> {
        self.contracts
            .iter()
            .map(|c| c.market.condition_id.clone())
            .collect()
    }

    pub fn condition_id_set_for_hour(&self, hour: DateTime<Utc>) -> HashSet<String> {
        let hour_start_s = hour.timestamp() as f64;
        let hour_end_s = hour_start_s + 3600.0;
        self.contracts
            .iter()
            .filter_map(|c| {
                let Some(close_s) = chrono::DateTime::parse_from_rfc3339(&c.end_date)
                    .ok()
                    .map(|d| d.timestamp() as f64)
                else {
                    // Preserve the old broad-filter behavior for malformed
                    // metadata instead of silently dropping the contract.
                    return Some(c.market.condition_id.clone());
                };
                let window_minutes =
                    crate::live::window::estimate_window_minutes(&c.window_description);
                let window_minutes = if window_minutes > 0.0 {
                    window_minutes
                } else {
                    60.0
                };
                let open_s = close_s - window_minutes * 60.0;
                if close_s > hour_start_s && open_s < hour_end_s {
                    Some(c.market.condition_id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn windows(&self) -> Vec<CandleWindow> {
        self.contracts
            .iter()
            .map(|c| {
                let close = chrono::DateTime::parse_from_rfc3339(&c.end_date)
                    .ok()
                    .map(|d| d.timestamp() as f64)
                    .unwrap_or(0.0);
                let window_minutes =
                    crate::live::window::estimate_window_minutes(&c.window_description);
                let window_minutes = if window_minutes > 0.0 {
                    window_minutes
                } else {
                    60.0
                };
                CandleWindow {
                    condition_id: c.market.condition_id.clone(),
                    open_ts_s: close - window_minutes * 60.0,
                    close_ts_s: close,
                    official_direction: c.terminal_direction(),
                }
            })
            .collect()
    }

    /// `token_id → CandleContract` lookup so the `BacktestStrategy` can
    /// resolve which contract owns each tick it sees.
    pub fn by_token_id(&self) -> BTreeMap<String, CandleContract> {
        let mut m = BTreeMap::new();
        for c in &self.contracts {
            if !c.up_token_id.is_empty() {
                m.insert(c.up_token_id.clone(), c.clone());
            }
            if !c.down_token_id.is_empty() {
                m.insert(c.down_token_id.clone(), c.clone());
            }
        }
        m
    }

    fn runtime_by_token_id(&self) -> BTreeMap<String, CandleRuntimeContract> {
        let mut m = BTreeMap::new();
        for c in &self.contracts {
            let rc = CandleRuntimeContract::from_contract(c);
            if !c.up_token_id.is_empty() {
                m.insert(c.up_token_id.clone(), rc.clone());
            }
            if !c.down_token_id.is_empty() {
                m.insert(c.down_token_id.clone(), rc.clone());
            }
        }
        m
    }
}

#[derive(Debug, Clone)]
struct CandleRuntimeContract {
    contract: CandleContract,
    close_ts_s: f64,
    open_ts_s: f64,
    window_minutes: f64,
    official_direction: Option<String>,
}

impl CandleRuntimeContract {
    fn from_contract(contract: &CandleContract) -> Self {
        let close_ts_s = chrono::DateTime::parse_from_rfc3339(&contract.end_date)
            .ok()
            .map(|d| d.timestamp() as f64)
            .unwrap_or(0.0);
        let window_minutes =
            crate::live::window::estimate_window_minutes(&contract.window_description);
        let window_minutes = if window_minutes > 0.0 {
            window_minutes
        } else {
            60.0
        };
        Self {
            contract: contract.clone(),
            close_ts_s,
            open_ts_s: close_ts_s - window_minutes * 60.0,
            window_minutes,
            official_direction: contract.terminal_direction(),
        }
    }
}

/// Deterministic one-Hz calibration sample captured after every non-edge
/// strategy gate has passed. Repeated rows from the same condition share one
/// terminal label and must be condition-weighted during model fitting.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CalibrationOpportunity {
    pub condition_id: String,
    pub token_id: String,
    pub decision_timestamp_s: f64,
    pub sampling_second: i64,
    pub evaluation_result: String,
    pub btc_price: f64,
    pub open_btc: f64,
    pub observed_volatility: f64,
    pub decision_volatility: f64,
    pub entry_fee_rate: f64,
    pub up_price: f64,
    pub down_price: f64,
    pub signal_price_change_pct: f64,
    pub directional_impulse_10s_bps: Option<f64>,
    pub token_mid: Option<f64>,
    #[serde(default)]
    pub opposite_token_id: String,
    #[serde(default)]
    pub market_tick_size: f64,
    #[serde(default)]
    pub chosen_microprice: Option<f64>,
    #[serde(default)]
    pub opposite_mid: Option<f64>,
    #[serde(default)]
    pub opposite_microprice: Option<f64>,
    #[serde(default)]
    pub complement_mid_sum_residual: Option<f64>,
    #[serde(default)]
    pub complement_microprice_sum_residual: Option<f64>,
    pub token_logit_change_5s: Option<f64>,
    pub token_logit_change_30s: Option<f64>,
    pub token_logit_change_60s: Option<f64>,
    pub directional_btc_return_bps_5s: Option<f64>,
    pub directional_btc_return_bps_30s: Option<f64>,
    pub directional_btc_return_bps_60s: Option<f64>,
    pub reversion_count: u32,
    pub actual_direction: String,
    pub won: bool,
    pub resolution_source: String,
    pub settlement_open_btc: f64,
    pub settlement_close_btc: f64,
    pub decision: CandleDecision,
}

/// Strategy adapter: glues the live decision logic onto the L2 backtest engine.
pub struct CandleBacktestStrategy {
    variant: StrategyVariant,
    strategy_spec: StrategySpec,
    universe_by_token: BTreeMap<String, CandleRuntimeContract>,
    books: BTreeMap<String, TokenBook>,
    momentum: MomentumDetector,
    bankroll_usd: f64,
    max_total_exposure_usd: f64,
    min_order_size_shares: f64,
    /// Causal exchange/proxy tape used for momentum and realized volatility.
    btc_history: Arc<BTCHistory>,
    /// Outcome tape used only for market resolution and realized breaker PnL.
    settlement_btc_history: Arc<BTCHistory>,
    breaker_cfg: BreakerConfig,
    breaker_state: BreakerState,
    breaker_tripped: bool,
    breaker_reason: Option<String>,
    breaker_tripped_at_s: Option<f64>,
    adaptive_rearm_after_s: Option<f64>,
    submitted_positions: BTreeMap<String, BacktestOpenPosition>,
    /// Exit intent ID -> entry intent ID for in-flight FOK sell attempts.
    submitted_exits: BTreeMap<String, String>,
    open_positions: BTreeMap<String, BacktestOpenPosition>,
    pub decisions: Vec<CandleDecision>,
    capture_calibration_opportunities: bool,
    pub calibration_opportunities: Vec<CalibrationOpportunity>,
    last_calibration_second_by_condition: HashMap<String, i64>,
    /// Per-condition_id flag so we only enter once per market.
    traded: HashSet<String>,
    /// Live scans roughly every 100ms, so historical L2 bursts should not
    /// trigger unlimited strategy decisions inside a single live-cycle bucket.
    last_eval_bucket_by_token: HashMap<String, i64>,
    /// Last timestamp we fed into the detector — throttle add_tick to once
    /// per second to match live cadence (otherwise the 5k-tick deque rolls
    /// over in seconds at ~870 events/s and we lose window history).
    last_tick_ts_s: f64,
    // Diagnostic counters.
    pub events_seen: u64,
    pub events_for_known_token: u64,
    pub skipped_resolved: u64,
    pub skipped_too_early: u64,
    pub skipped_no_btc: u64,
    pub skipped_no_signal: u64,
    pub skipped_decision: u64,
    pub skipped_throttled: u64,
    pub breaker_paused_events: u64,
    pub adaptive_rearms: u64,
    pub exit_signals: u64,
    pub exit_fills: u64,
    pub exit_failures: u64,
    pub skip_reasons: BTreeMap<String, u64>,
}

#[derive(Debug, Clone)]
struct BacktestOpenPosition {
    condition_id: String,
    token_id: String,
    direction: String,
    open_btc: f64,
    settlement_open_btc: f64,
    close_ts_s: f64,
    official_direction: Option<String>,
    entry_timestamp_s: f64,
    entry_price: f64,
    size: f64,
    fee: f64,
    exit_fee_rate: f64,
    exit_pending: bool,
    last_exit_attempt_ts_s: Option<f64>,
}

impl CandleBacktestStrategy {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_breaker(
        variant: StrategyVariant,
        universe: &CandleUniverse,
        bankroll_usd: f64,
        max_total_exposure_usd: f64,
        min_order_size_shares: f64,
        btc_history: Arc<BTCHistory>,
        breaker: BacktestBreakerReport,
        breaker_cfg: BreakerConfig,
        adaptive_rearm_after_s: Option<f64>,
    ) -> Self {
        Self::new_with_breaker_and_settlement_history(
            variant,
            universe,
            bankroll_usd,
            max_total_exposure_usd,
            min_order_size_shares,
            Arc::clone(&btc_history),
            btc_history,
            breaker,
            breaker_cfg,
            adaptive_rearm_after_s,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_breaker_and_settlement_history(
        variant: StrategyVariant,
        universe: &CandleUniverse,
        bankroll_usd: f64,
        max_total_exposure_usd: f64,
        min_order_size_shares: f64,
        btc_history: Arc<BTCHistory>,
        settlement_btc_history: Arc<BTCHistory>,
        breaker: BacktestBreakerReport,
        breaker_cfg: BreakerConfig,
        adaptive_rearm_after_s: Option<f64>,
    ) -> Self {
        let mom_cfg = MomentumConfig {
            noise_z_threshold: 0.3,
            ..Default::default()
        };
        let strategy_spec = StrategySpec::from_serializable_params(
            "candle_momentum",
            "1",
            &variant,
            variant.risk_profile(),
        );
        Self {
            variant,
            strategy_spec,
            universe_by_token: universe.runtime_by_token_id(),
            books: BTreeMap::new(),
            momentum: MomentumDetector::new(None, mom_cfg),
            bankroll_usd,
            max_total_exposure_usd,
            min_order_size_shares,
            btc_history,
            settlement_btc_history,
            breaker_cfg,
            breaker_state: breaker.state,
            breaker_tripped: breaker.tripped,
            breaker_reason: breaker.reason,
            breaker_tripped_at_s: breaker.tripped_at_s,
            adaptive_rearm_after_s,
            submitted_positions: BTreeMap::new(),
            submitted_exits: BTreeMap::new(),
            open_positions: BTreeMap::new(),
            decisions: Vec::new(),
            capture_calibration_opportunities: false,
            calibration_opportunities: Vec::new(),
            last_calibration_second_by_condition: HashMap::new(),
            traded: HashSet::new(),
            last_eval_bucket_by_token: HashMap::new(),
            last_tick_ts_s: 0.0,
            events_seen: 0,
            events_for_known_token: 0,
            skipped_resolved: 0,
            skipped_too_early: 0,
            skipped_no_btc: 0,
            skipped_no_signal: 0,
            skipped_decision: 0,
            skipped_throttled: 0,
            breaker_paused_events: 0,
            adaptive_rearms: 0,
            exit_signals: 0,
            exit_fills: 0,
            exit_failures: 0,
            skip_reasons: BTreeMap::new(),
        }
    }

    fn enable_calibration_opportunity_capture(&mut self) {
        self.capture_calibration_opportunities = true;
    }

    fn fresh_ask(&self, token_id: &str, now_ts: f64) -> Option<f64> {
        self.books
            .get(token_id)
            .filter(|b| now_ts - b.last_update_ts_s <= 30.0)
            .and_then(|b| (b.best_ask > 0.0).then_some(b.best_ask))
    }

    fn microstructure_for_token(&self, token_id: &str, now_ts: f64) -> BookMicrostructure {
        self.books
            .get(token_id)
            .filter(|b| now_ts - b.last_update_ts_s <= 30.0)
            .map(backtest_microstructure)
            .unwrap_or_default()
    }

    fn book_age_ms_for_token(&self, token_id: &str, now_ts: f64) -> Option<f64> {
        self.books
            .get(token_id)
            .map(|b| ((now_ts - b.last_update_ts_s) * 1000.0).max(0.0))
            .filter(|age_ms| *age_ms <= 30_000.0)
    }

    fn bookwalk_buy_slippage_for_token(&self, token_id: &str, size: f64) -> Option<f64> {
        let book = self.books.get(token_id)?;
        bookwalk_buy_slippage(book, size)
    }

    fn buy_book_quote_for_token(
        &self,
        token_id: &str,
        now_ts: f64,
        budget_usd: f64,
    ) -> Option<BuyBookQuote> {
        let asks = self
            .books
            .get(token_id)
            .filter(|book| now_ts - book.last_update_ts_s <= 30.0)?
            .ask_levels();
        buy_book_quote_from_budget(budget_usd, &asks, self.min_order_size_shares, DEFAULT_TICK)
    }

    fn sell_book_quote_for_token(
        &self,
        token_id: &str,
        now_ts: f64,
        size: f64,
    ) -> Option<SellBookQuote> {
        let bids = self
            .books
            .get(token_id)
            .filter(|book| now_ts - book.last_update_ts_s <= 30.0)?
            .bid_levels();
        sell_book_quote_for_size(size, &bids, DEFAULT_TICK)
    }

    fn maybe_exit_orders(
        &mut self,
        timestamp_s: f64,
        token_id: &str,
        btc: f64,
    ) -> Vec<BacktestOrder> {
        let cfg = self.variant.exit;
        if cfg.is_disabled() || btc <= 0.0 {
            return Vec::new();
        }

        let candidate =
            self.open_positions
                .iter()
                .find_map(|(entry_intent_id, pos)| {
                    if pos.token_id != token_id || pos.exit_pending {
                        return None;
                    }
                    if pos.last_exit_attempt_ts_s.is_some_and(|last| {
                        timestamp_s - last < cfg.retry_cooldown_seconds.max(0.0)
                    }) {
                        return None;
                    }
                    let basis_exit = cfg.should_exit(
                        &pos.direction,
                        pos.open_btc,
                        btc,
                        pos.entry_timestamp_s,
                        timestamp_s,
                        pos.close_ts_s,
                    );
                    basis_exit.then(|| (entry_intent_id.clone(), pos.clone()))
                });
        let Some((entry_intent_id, pos)) = candidate else {
            return Vec::new();
        };

        let Some(quote) = self.sell_book_quote_for_token(token_id, timestamp_s, pos.size) else {
            if let Some(open) = self.open_positions.get_mut(&entry_intent_id) {
                open.last_exit_attempt_ts_s = Some(timestamp_s);
            }
            *self
                .skip_reasons
                .entry("exit_visible_bid_depth_unavailable".to_string())
                .or_insert(0) += 1;
            return Vec::new();
        };

        let exit_intent_id = format!("{entry_intent_id}:settlement_basis_exit:{timestamp_s:.6}");
        if let Some(open) = self.open_positions.get_mut(&entry_intent_id) {
            open.exit_pending = true;
            open.last_exit_attempt_ts_s = Some(timestamp_s);
        }
        self.submitted_exits
            .insert(exit_intent_id.clone(), entry_intent_id);
        self.exit_signals += 1;

        vec![BacktestOrder {
            intent_id: exit_intent_id,
            timestamp_s,
            condition_id: pos.condition_id,
            token_id: pos.token_id,
            side: "sell".to_string(),
            size: quote.shares,
            order_type: "market".to_string(),
            limit_price: Some(quote.worst_price),
            fee_rate: pos.exit_fee_rate,
            maker_fee_rate: 0.0,
        }]
    }

    fn open_exposure(&self) -> f64 {
        self.open_positions
            .values()
            .map(|p| p.entry_price * p.size)
            .sum()
    }

    fn submitted_exposure(&self) -> f64 {
        self.submitted_positions
            .values()
            .map(|p| p.entry_price * p.size)
            .sum()
    }

    fn active_bankroll(&self) -> f64 {
        (self.bankroll_usd + self.breaker_state.realized_pnl).max(0.0)
    }

    fn exposure_cap(&self) -> f64 {
        let ratio_cap = self.active_bankroll() * DEFAULT_EXPOSURE_RATIO;
        if self.max_total_exposure_usd > 0.0 {
            ratio_cap.min(self.max_total_exposure_usd)
        } else {
            ratio_cap
        }
    }

    fn position_budget_before_stress(&self, used_exposure: f64) -> (f64, f64) {
        let active_bankroll = self.active_bankroll();
        let base_position = (active_bankroll * self.variant.position_pct)
            .min(self.variant.max_per_market_usd)
            .min(active_bankroll);
        let exposure_available = (self.exposure_cap() - used_exposure.max(0.0)).max(0.0);
        (base_position.min(exposure_available), exposure_available)
    }

    fn settle_due_positions(&mut self, timestamp_s: f64) {
        let due: Vec<String> = self
            .open_positions
            .iter()
            .filter(|(_, p)| p.close_ts_s <= timestamp_s)
            .map(|(intent_id, _)| intent_id.clone())
            .collect();
        for intent_id in due {
            let Some(pos) = self.open_positions.remove(&intent_id) else {
                continue;
            };
            let close_btc = self.settlement_btc_history.price_at_seconds(pos.close_ts_s);
            if close_btc <= 0.0 || pos.settlement_open_btc <= 0.0 {
                self.open_positions.insert(intent_id, pos);
                continue;
            }
            let local_actual = if close_btc >= pos.settlement_open_btc {
                "up"
            } else {
                "down"
            };
            let actual = pos.official_direction.as_deref().unwrap_or(local_actual);
            let won = pos.direction == actual;
            let pnl = paper_outcome_pnl(won, pos.entry_price, pos.size, pos.fee);
            self.breaker_state.record_resolution(won, pnl);
            self.trip_breaker_if_needed(pos.close_ts_s);
        }
        self.trip_breaker_if_needed(timestamp_s);
    }

    fn settle_all_positions(&mut self) {
        self.settle_due_positions(f64::MAX);
    }

    fn trip_breaker_if_needed(&mut self, timestamp_s: f64) {
        if self.breaker_tripped {
            return;
        }
        if let Some(reason) = self.breaker_state.should_trip(
            &self.breaker_cfg,
            self.open_exposure(),
            self.bankroll_usd.max(1.0),
        ) {
            self.breaker_tripped = true;
            self.breaker_reason = Some(reason.to_string());
            self.breaker_tripped_at_s = Some(timestamp_s);
        }
    }

    fn maybe_rearm_adaptive_health(&mut self, timestamp_s: f64) {
        let Some(cooldown_s) = self.adaptive_rearm_after_s else {
            return;
        };
        if !self.breaker_tripped {
            return;
        }
        if self.breaker_reason.as_deref() != Some("win_rate_low") {
            return;
        }
        let Some(tripped_at_s) = self.breaker_tripped_at_s else {
            return;
        };
        if timestamp_s - tripped_at_s < cooldown_s {
            return;
        }
        if !self.open_positions.is_empty()
            || !self.submitted_positions.is_empty()
            || !self.submitted_exits.is_empty()
        {
            return;
        }

        self.breaker_state = BreakerState::default();
        self.breaker_tripped = false;
        self.breaker_reason = None;
        self.breaker_tripped_at_s = None;
        self.adaptive_rearms += 1;
        *self
            .skip_reasons
            .entry("adaptive_health_rearm".to_string())
            .or_insert(0) += 1;
    }

    pub fn breaker_report(&self) -> BacktestBreakerReport {
        BacktestBreakerReport::from_state(
            self.breaker_state,
            self.open_exposure(),
            self.bankroll_usd.max(1.0),
            self.breaker_tripped,
            self.breaker_reason.clone(),
            self.breaker_tripped_at_s,
        )
    }

    pub fn diagnostics(&self) -> BacktestDiagnostics {
        BacktestDiagnostics {
            events_seen: self.events_seen,
            events_for_known_token: self.events_for_known_token,
            skipped_resolved: self.skipped_resolved,
            skipped_too_early: self.skipped_too_early,
            skipped_no_btc: self.skipped_no_btc,
            skipped_no_signal: self.skipped_no_signal,
            skipped_decision: self.skipped_decision,
            skipped_throttled: self.skipped_throttled,
            breaker_paused_events: self.breaker_paused_events,
            adaptive_rearms: self.adaptive_rearms,
            exit_signals: self.exit_signals,
            exit_fills: self.exit_fills,
            exit_failures: self.exit_failures,
            skip_reasons: self.skip_reasons.clone(),
            ..BacktestDiagnostics::default()
        }
    }
}

impl Strategy for CandleBacktestStrategy {
    fn needs_l2_history(&self) -> bool {
        self.capture_calibration_opportunities || self.variant.microstructure.is_path_active()
    }

    fn on_fills(&mut self, fills: &[crate::backtest::l2_replay::BacktestFill]) {
        for fill in fills {
            if fill.order.side.eq_ignore_ascii_case("sell") {
                let Some(entry_intent_id) = self.submitted_exits.remove(&fill.order.intent_id)
                else {
                    continue;
                };
                if !fill.success {
                    if let Some(pos) = self.open_positions.get_mut(&entry_intent_id) {
                        pos.exit_pending = false;
                    }
                    self.exit_failures += 1;
                    continue;
                }
                let Some(pos) = self.open_positions.remove(&entry_intent_id) else {
                    self.exit_failures += 1;
                    continue;
                };
                let pnl =
                    (fill.fill_price - pos.entry_price) * fill.filled_size - pos.fee - fill.fee;
                self.breaker_state.record_resolution(pnl > 0.0, pnl);
                self.exit_fills += 1;
                self.trip_breaker_if_needed(fill.fill_timestamp_s);
                continue;
            }

            let Some(mut pos) = self.submitted_positions.remove(&fill.order.intent_id) else {
                continue;
            };
            if !fill.success {
                continue;
            }
            pos.entry_price = fill.fill_price;
            pos.size = fill.filled_size;
            pos.fee = fill.fee;
            pos.entry_timestamp_s = fill.fill_timestamp_s;
            self.open_positions
                .insert(fill.order.intent_id.clone(), pos);
            self.settle_due_positions(fill.fill_timestamp_s);
        }
    }

    fn on_event(
        &mut self,
        timestamp_s: f64,
        token_id: &str,
        book: &TokenBook,
        history: &L2MidHistory,
    ) -> Vec<BacktestOrder> {
        self.events_seen += 1;
        let btc = self.btc_history.price_at_seconds(timestamp_s);
        if btc > 0.0 && timestamp_s - self.last_tick_ts_s >= 1.0 {
            self.momentum.add_tick(btc, Some(timestamp_s));
            self.last_tick_ts_s = timestamp_s;
        }
        self.books.insert(token_id.to_string(), book.clone());
        self.settle_due_positions(timestamp_s);
        self.maybe_rearm_adaptive_health(timestamp_s);
        let exit_orders = self.maybe_exit_orders(timestamp_s, token_id, btc);
        if !exit_orders.is_empty() {
            return exit_orders;
        }
        if self.breaker_tripped {
            self.breaker_paused_events += 1;
            return Vec::new();
        }
        let Some(runtime) = self.universe_by_token.get(token_id) else {
            return Vec::new();
        };
        self.events_for_known_token += 1;
        let contract = &runtime.contract;
        let cid = contract.market.condition_id.as_str();
        if self.traded.contains(cid) {
            return Vec::new();
        }

        let minutes_remaining = (runtime.close_ts_s - timestamp_s) / 60.0;
        if minutes_remaining <= 0.083 || minutes_remaining > 30.0 {
            self.skipped_resolved += 1;
            return Vec::new();
        }
        let minutes_elapsed = runtime.window_minutes - minutes_remaining;
        if minutes_elapsed < 0.5 {
            self.skipped_too_early += 1;
            return Vec::new();
        }
        let eval_bucket = (timestamp_s * 10.0).floor() as i64;
        if self
            .last_eval_bucket_by_token
            .get(token_id)
            .copied()
            .is_some_and(|last| last == eval_bucket)
        {
            self.skipped_throttled += 1;
            return Vec::new();
        }
        self.last_eval_bucket_by_token
            .insert(token_id.to_string(), eval_bucket);

        if btc <= 0.0 {
            self.skipped_no_btc += 1;
            return Vec::new();
        }

        if self.momentum.get_open_price(cid).is_none() {
            let open_btc = self.btc_history.price_at_seconds(runtime.open_ts_s);
            if open_btc <= 0.0 {
                self.skipped_no_btc += 1;
                return Vec::new();
            }
            self.momentum.set_window_open(cid, open_btc);
        }

        let signal = match self.momentum.detect(
            cid,
            minutes_elapsed,
            minutes_remaining,
            btc,
            Some(timestamp_s),
        ) {
            Some(s) => s,
            None => {
                self.skipped_no_signal += 1;
                return Vec::new();
            }
        };

        let (Some(up_price), Some(down_price)) = (
            self.fresh_ask(&contract.up_token_id, timestamp_s),
            self.fresh_ask(&contract.down_token_id, timestamp_s),
        ) else {
            self.skipped_decision += 1;
            *self
                .skip_reasons
                .entry("fresh_outcome_book_unavailable".to_string())
                .or_insert(0) += 1;
            return Vec::new();
        };

        let observed_vol = self
            .btc_history
            .realized_vol_at((timestamp_s * 1000.0) as i64, 3600.0);
        let implied_vol = self.variant.decision_volatility(observed_vol);
        let used_exposure = self.open_exposure() + self.submitted_exposure();
        let breaker_metrics = self
            .breaker_state
            .metrics(used_exposure, self.bankroll_usd.max(1.0));
        let effective_zone_config = self.variant.effective_zone_config(
            self.breaker_state.losses,
            breaker_metrics.realized_drawdown_pct,
        );
        let prefer_maker = self.variant.effective_prefer_maker(
            self.breaker_state.losses,
            breaker_metrics.realized_drawdown_pct,
        );
        let fee_rate = contract
            .market
            .effective_taker_fee_rate(self.variant.default_fee_rate);
        let maker_fee_rate = contract
            .market
            .effective_maker_fee_rate(self.variant.maker_fee_rate);
        let entry_fee_rate = if prefer_maker {
            maker_fee_rate
        } else {
            fee_rate
        };
        let evaluation = evaluate_candle_trade_with_fee(
            &signal,
            minutes_elapsed,
            minutes_remaining,
            runtime.window_minutes,
            up_price,
            down_price,
            btc,
            signal.open_price,
            implied_vol,
            entry_fee_rate,
            self.variant.min_confidence,
            self.variant.min_edge,
            self.variant.skip_dead_zone,
            &effective_zone_config,
            0.0,
            self.capture_calibration_opportunities,
        );

        if self.capture_calibration_opportunities {
            let sampling_second = timestamp_s.floor() as i64;
            let already_captured = self
                .last_calibration_second_by_condition
                .get(cid)
                .is_some_and(|last| *last == sampling_second);
            if !already_captured {
                let candidate = match &evaluation.result {
                    DecisionResult::Trade(decision) => Some(decision),
                    DecisionResult::Skip(_) => evaluation.opportunity.as_ref(),
                };
                if let Some(candidate) = candidate {
                    let mut candidate = candidate.clone();
                    candidate.regime.attach_time_inputs(timestamp_s);
                    let candidate_token = if candidate.direction == "up" {
                        contract.up_token_id.as_str()
                    } else {
                        contract.down_token_id.as_str()
                    };
                    let opposite_token = if candidate.direction == "up" {
                        contract.down_token_id.as_str()
                    } else {
                        contract.up_token_id.as_str()
                    };
                    let candidate_micro =
                        self.microstructure_for_token(candidate_token, timestamp_s);
                    let opposite_micro = self.microstructure_for_token(opposite_token, timestamp_s);
                    let complement =
                        binary_complement_microstructure(&candidate_micro, &opposite_micro);
                    candidate.regime.attach_orderbook_inputs(
                        candidate_micro.best_bid,
                        candidate_micro.best_ask,
                        candidate_micro.spread,
                        candidate_micro.bid_depth,
                        candidate_micro.ask_depth,
                        candidate_micro.pressure,
                        candidate_micro.imbalance,
                    );
                    candidate.regime.attach_orderbook_quality_inputs(
                        None,
                        self.book_age_ms_for_token(candidate_token, timestamp_s),
                    );
                    let recent_runup = history.get(candidate_token).and_then(|points| {
                        recent_mid_runup(
                            points,
                            timestamp_s,
                            self.variant.microstructure.recent_mid_lookback_seconds,
                        )
                    });
                    candidate.regime.attach_orderbook_path_inputs(recent_runup);
                    let token_mid = (candidate_micro.best_bid > 0.0
                        && candidate_micro.best_ask > candidate_micro.best_bid)
                        .then_some((candidate_micro.best_bid + candidate_micro.best_ask) / 2.0);
                    let token_logit_change = |lookback_seconds| {
                        history.get(candidate_token).and_then(|points| {
                            recent_mid_logit_change(points, timestamp_s, lookback_seconds)
                        })
                    };
                    let directional_btc_return = |lookback_seconds| {
                        directional_log_return_bps(
                            btc,
                            self.btc_history
                                .price_at_seconds(timestamp_s - lookback_seconds),
                            &candidate.direction,
                        )
                    };
                    let token_logit_change_5s = token_logit_change(5.0);
                    let token_logit_change_30s = token_logit_change(30.0);
                    let token_logit_change_60s = token_logit_change(60.0);
                    let directional_btc_return_bps_5s = directional_btc_return(5.0);
                    let directional_btc_return_bps_30s = directional_btc_return(30.0);
                    let directional_btc_return_bps_60s = directional_btc_return(60.0);

                    let settlement_open_btc = self
                        .settlement_btc_history
                        .price_at_seconds(runtime.open_ts_s);
                    let settlement_close_btc = self
                        .settlement_btc_history
                        .price_at_seconds(runtime.close_ts_s);
                    let resolved = runtime
                        .official_direction
                        .as_ref()
                        .map(|direction| (direction.clone(), "polymarket_terminal".to_string()))
                        .or_else(|| {
                            (settlement_open_btc > 0.0 && settlement_close_btc > 0.0).then(|| {
                                let direction = if settlement_close_btc >= settlement_open_btc {
                                    "up"
                                } else {
                                    "down"
                                };
                                (direction.to_string(), "settlement_btc_tape".to_string())
                            })
                        });
                    if let Some((actual_direction, resolution_source)) = resolved {
                        let evaluation_result = match &evaluation.result {
                            DecisionResult::Trade(_) => "edge_pass".to_string(),
                            DecisionResult::Skip(skip) => skip.reason.clone(),
                        };
                        self.last_calibration_second_by_condition
                            .insert(cid.to_string(), sampling_second);
                        self.calibration_opportunities.push(CalibrationOpportunity {
                            condition_id: cid.to_string(),
                            token_id: candidate_token.to_string(),
                            decision_timestamp_s: timestamp_s,
                            sampling_second,
                            evaluation_result,
                            btc_price: btc,
                            open_btc: signal.open_price,
                            observed_volatility: observed_vol,
                            decision_volatility: implied_vol,
                            entry_fee_rate,
                            up_price,
                            down_price,
                            signal_price_change_pct: signal.price_change_pct,
                            directional_impulse_10s_bps: signal.directional_impulse_10s_bps,
                            token_mid,
                            opposite_token_id: opposite_token.to_string(),
                            market_tick_size: contract
                                .market
                                .minimum_tick_size
                                .filter(|tick| tick.is_finite() && *tick > 0.0)
                                .unwrap_or(DEFAULT_TICK),
                            chosen_microprice: complement.map(|paired| paired.chosen_microprice),
                            opposite_mid: complement.map(|paired| paired.opposite_mid),
                            opposite_microprice: complement
                                .map(|paired| paired.opposite_microprice),
                            complement_mid_sum_residual: complement
                                .map(|paired| paired.mid_sum_residual),
                            complement_microprice_sum_residual: complement
                                .map(|paired| paired.microprice_sum_residual),
                            token_logit_change_5s,
                            token_logit_change_30s,
                            token_logit_change_60s,
                            directional_btc_return_bps_5s,
                            directional_btc_return_bps_30s,
                            directional_btc_return_bps_60s,
                            reversion_count: signal.reversion_count,
                            won: candidate.direction == actual_direction,
                            actual_direction,
                            resolution_source,
                            settlement_open_btc,
                            settlement_close_btc,
                            decision: candidate,
                        });
                    }
                }
            }
        }

        let mut decision = match evaluation.result {
            DecisionResult::Trade(d) => d,
            DecisionResult::Skip(skip) => {
                self.skipped_decision += 1;
                let key = format!("{}_{}", skip.reason, skip.zone);
                *self.skip_reasons.entry(key).or_insert(0) += 1;
                return Vec::new();
            }
        };
        decision.regime.attach_time_inputs(timestamp_s);
        let traded_token = if decision.direction == "up" {
            contract.up_token_id.as_str()
        } else {
            contract.down_token_id.as_str()
        };
        let micro = self.microstructure_for_token(traded_token, timestamp_s);
        decision.regime.attach_orderbook_inputs(
            micro.best_bid,
            micro.best_ask,
            micro.spread,
            micro.bid_depth,
            micro.ask_depth,
            micro.pressure,
            micro.imbalance,
        );
        let recent_runup = history.get(traded_token).and_then(|points| {
            recent_mid_runup(
                points,
                timestamp_s,
                self.variant.microstructure.recent_mid_lookback_seconds,
            )
        });
        decision.regime.attach_orderbook_path_inputs(recent_runup);
        let (mut position, exposure_available) = self.position_budget_before_stress(used_exposure);
        if let Some(stress_headroom) = self.breaker_state.stressed_drawdown_exposure_headroom(
            used_exposure,
            self.bankroll_usd.max(1.0),
            self.variant.max_projected_stressed_drawdown_pct,
        ) {
            position = position.min(stress_headroom);
        }
        let market_price = decision.market_price;
        let pending_limit_price = if prefer_maker {
            resting_limit_price(Side::Buy, micro.best_bid, micro.best_ask, DEFAULT_TICK)
        } else {
            None
        };
        let taker_quote = (!prefer_maker && position >= 1.0)
            .then(|| self.buy_book_quote_for_token(traded_token, timestamp_s, position))
            .flatten();
        if let Some(quote) = taker_quote {
            if let Err(skip) = decision.reprice_for_taker_execution(
                quote.vwap,
                quote.worst_price,
                fee_rate,
                self.variant.min_edge,
                &effective_zone_config,
            ) {
                self.skipped_decision += 1;
                let key = format!("{}_{}", skip.reason, decision.zone);
                *self.skip_reasons.entry(key).or_insert(0) += 1;
                return Vec::new();
            }
        }
        let estimated_sizing_price = if prefer_maker {
            pending_limit_price
        } else {
            None
        };
        let estimated_size = taker_quote.map(|quote| quote.shares).or_else(|| {
            if position < 1.0 {
                return None;
            }
            estimated_sizing_price
                .filter(|price| *price > 0.0)
                .and_then(|price| shares_from_budget(position, price, self.min_order_size_shares))
        });
        decision.regime.attach_orderbook_quality_inputs(
            taker_quote
                .map(|quote| quote.slippage_per_share)
                .or_else(|| {
                    estimated_size
                        .and_then(|size| self.bookwalk_buy_slippage_for_token(traded_token, size))
                }),
            self.book_age_ms_for_token(traded_token, timestamp_s),
        );
        if let Some(reason) = self.variant.selectivity.reject_reason(&decision.regime) {
            self.skipped_decision += 1;
            let key = format!("{}_{}", reason, decision.zone);
            *self.skip_reasons.entry(key).or_insert(0) += 1;
            return Vec::new();
        }

        if let Err(skip) = self
            .variant
            .microstructure
            .check_recent_mid_path(recent_runup)
        {
            self.skipped_decision += 1;
            let key = format!("{}_{}", skip.reason, decision.zone);
            *self.skip_reasons.entry(key).or_insert(0) += 1;
            return Vec::new();
        }

        if let Err(skip) = micro.check_long_entry(&self.variant.microstructure) {
            self.skipped_decision += 1;
            let key = format!("{}_{}", skip.reason, decision.zone);
            *self.skip_reasons.entry(key).or_insert(0) += 1;
            return Vec::new();
        }
        if position < 1.0 {
            self.skipped_decision += 1;
            let reason = if self.active_bankroll() < 1.0 {
                "bankroll_depleted"
            } else if exposure_available < 1.0 {
                "exposure_cap"
            } else {
                "stress_drawdown_cap"
            };
            let key = format!("{}_{}", reason, decision.zone);
            *self.skip_reasons.entry(key).or_insert(0) += 1;
            return Vec::new();
        }
        if market_price <= 0.0 {
            return Vec::new();
        }
        let (order_type, limit_price, sizing_price, size) = if prefer_maker {
            let Some(lp) = pending_limit_price else {
                self.skipped_decision += 1;
                let key = format!("maker_invalid_book_{}", decision.zone);
                *self.skip_reasons.entry(key).or_insert(0) += 1;
                return Vec::new();
            };
            let Some(size) = shares_from_budget(position, lp, self.min_order_size_shares) else {
                self.skipped_decision += 1;
                let key = format!("min_order_size_{}", decision.zone);
                *self.skip_reasons.entry(key).or_insert(0) += 1;
                return Vec::new();
            };
            ("limit", Some(lp), lp, size)
        } else {
            let Some(quote) = taker_quote else {
                self.skipped_decision += 1;
                let key = format!("taker_visible_depth_unavailable_{}", decision.zone);
                *self.skip_reasons.entry(key).or_insert(0) += 1;
                return Vec::new();
            };
            ("market", Some(quote.worst_price), quote.vwap, quote.shares)
        };
        if sizing_price <= 0.0 {
            return Vec::new();
        }
        self.traded.insert(cid.to_string());
        self.decisions.push(decision.clone());

        let signal_contract = Signal::from_candle_decision(
            cid.to_string(),
            traded_token.to_string(),
            &decision,
            serde_json::json!({
                "zone": decision.zone,
                "z_score": decision.z_score,
                "minutes_remaining": decision.minutes_remaining,
                "market_price": decision.market_price,
                "timestamp_s": timestamp_s,
            }),
        );
        let intent = OrderIntent::deterministic(
            self.strategy_spec.clone(),
            &signal_contract,
            "buy",
            order_type,
            limit_price,
            size,
            "candle_momentum_decision",
            format!("{cid}:{timestamp_s:.6}:{traded_token}"),
        );
        self.submitted_positions.insert(
            intent.intent_id.clone(),
            BacktestOpenPosition {
                condition_id: cid.to_string(),
                token_id: traded_token.to_string(),
                direction: decision.direction.clone(),
                open_btc: signal.open_price,
                settlement_open_btc: self
                    .settlement_btc_history
                    .price_at_seconds(runtime.open_ts_s),
                close_ts_s: runtime.close_ts_s,
                official_direction: runtime.official_direction.clone(),
                entry_timestamp_s: timestamp_s,
                entry_price: limit_price.unwrap_or(sizing_price),
                size,
                fee: 0.0,
                exit_fee_rate: fee_rate,
                exit_pending: false,
                last_exit_attempt_ts_s: None,
            },
        );
        vec![BacktestOrder {
            intent_id: intent.intent_id,
            timestamp_s,
            condition_id: cid.to_string(),
            token_id: traded_token.to_string(),
            side: "buy".into(),
            size,
            order_type: order_type.into(),
            limit_price,
            fee_rate,
            maker_fee_rate,
        }]
    }
}

fn backtest_microstructure(book: &TokenBook) -> BookMicrostructure {
    let bids: Vec<BookLevelView> = book
        .bid_levels()
        .into_iter()
        .map(|(price, size)| BookLevelView { price, size })
        .collect();
    let asks: Vec<BookLevelView> = book
        .ask_levels()
        .into_iter()
        .map(|(price, size)| BookLevelView { price, size })
        .collect();
    BookMicrostructure::from_levels_with_top(book.best_bid, book.best_ask, &bids, &asks, 3)
}

fn directional_log_return_bps(current_price: f64, past_price: f64, direction: &str) -> Option<f64> {
    if !current_price.is_finite()
        || !past_price.is_finite()
        || current_price <= 0.0
        || past_price <= 0.0
    {
        return None;
    }
    let direction_sign = match direction {
        "up" => 1.0,
        "down" => -1.0,
        _ => return None,
    };
    Some(direction_sign * (current_price / past_price).ln() * 10_000.0)
}

fn bookwalk_buy_slippage(book: &TokenBook, size: f64) -> Option<f64> {
    let asks = book
        .ask_levels()
        .into_iter()
        .map(|(price, size)| BookLevelView { price, size })
        .collect::<Vec<_>>();
    bookwalk_buy_slippage_from_levels(&asks, size, DEFAULT_TICK)
}

fn paper_outcome_pnl(won: bool, entry_price: f64, size: f64, fee: f64) -> f64 {
    let gross = if won {
        (1.0 - entry_price) * size
    } else {
        -entry_price * size
    };
    gross - fee
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HarnessRun {
    pub variant: StrategyVariant,
    pub results: BacktestResults,
    pub calibration_opportunities: Vec<CalibrationOpportunity>,
}

pub struct HarnessConfig {
    pub hours: Vec<DateTime<Utc>>,
    pub universe: CandleUniverse,
    pub btc_history: Arc<BTCHistory>,
    pub settlement_btc_history: Arc<BTCHistory>,
    pub bankroll_usd: f64,
    pub max_total_exposure_usd: f64,
    pub min_order_size_shares: f64,
    pub cache_dir: PathBuf,
    pub latency: StaticLatencyConfig,
    pub breaker_cfg: BreakerConfig,
    /// Offline-only model of adaptive health recovery. A value of `Some(n)`
    /// lets a backtest resume after a `win_rate_low` breaker has cooled down
    /// for `n` seconds and there is no open/in-flight exposure. Drawdown and
    /// exposure trips stay hard stops. Promotion gates reject candidates that
    /// require a rearm, so this is diagnostic rather than a live shortcut.
    pub adaptive_rearm_after_s: Option<f64>,
    /// Optional shared distilled-cache directory. When set, the harness
    /// checks `<dir>/<hour>.v1.candles.jsonl.gz` BEFORE the per-tenant
    /// sidecar and the parquet. The shared-cache writer is `polymomentum-
    /// engine distill`. See cross_bot_distilled_cache_response.md.
    pub shared_distilled_dir: Option<PathBuf>,
    /// Fail instead of falling back to a sidecar, parquet, or network download
    /// when the requested shared distilled hour is missing, corrupt, or has no
    /// events for the selected conditions. Used for exact recorded-capture
    /// replay; false preserves the shared-cache fallback contract.
    pub require_shared_distilled: bool,
    /// Variant-fan-out parallelism. `None` → use rayon's global pool
    /// (defaults to `num_cpus`). `Some(1)` → serial (matches the
    /// pre-rayon behavior bit-for-bit). `Some(n>1)` → cap at `n`.
    /// Honors `RAYON_NUM_THREADS` env var when this is `None`.
    pub threads: Option<usize>,
    /// Optional pause/resume checkpoint dir. When set:
    ///   - Existing `<dir>/<hour>.json` files are loaded into the running
    ///     accumulator and those hours are SKIPPED in this invocation.
    ///   - After each hour completes, its results are written atomically
    ///     to `<dir>/<hour>.json` (tmp + rename).
    ///   - Before starting each hour, the harness checks for `<dir>/PAUSE`.
    ///     If present, it stops cleanly after writing the previous hour's
    ///     checkpoint and returns the partial results.
    pub checkpoint_dir: Option<PathBuf>,
    /// External stop flag (SIGINT/SIGTERM). Same effect as `<dir>/PAUSE`:
    /// checked between hours; on set, the harness returns whatever it has
    /// so far. Persists checkpoints first if `checkpoint_dir` is set.
    pub stop_flag: Option<Arc<AtomicBool>>,
    /// Preserve strategy, fill-model, order-book, and breaker state across
    /// requested hours. This mirrors live/live-replay semantics and avoids
    /// hour-boundary double entries.
    pub continuous: bool,
    /// Capture one pre-edge calibration opportunity per condition-second.
    /// Requires continuous mode so sampling state cannot reset at hour edges.
    pub capture_calibration_opportunities: bool,
    /// Delete an hourly parquet after this process downloaded, loaded, and
    /// replayed it. Pre-existing cached parquets are never removed.
    pub delete_downloaded_parquet_after_hour: bool,
}

struct ContinuousVariantState {
    variant: StrategyVariant,
    engine: L2BacktestEngine,
    strategy: CandleBacktestStrategy,
}

/// Run every variant over the requested hours. Streams one hour at a time
/// (the parquet expansion is huge — ~500 MB / hour in memory). For each
/// hour, all variants replay in parallel against a shared `Arc<Vec<L2Event>>`
/// — variant-fan-out is the natural unit of parallelism since each variant
/// is independent (its own engine, strategy, RNG seed). Per-variant
/// `BacktestResults` are then merged sequentially.
///
/// Determinism: each variant has its own `maker_seed`, so results are
/// independent of thread count. Output `runs` Vec is in the same order as
/// the input `variants` Vec regardless of thread count (rayon's
/// `par_iter().map().collect()` preserves source order).
///
/// Pause/resume: when `cfg.checkpoint_dir` is set, per-hour result files
/// are loaded on entry (skipping those hours) and written atomically as
/// each hour completes. A `<dir>/PAUSE` sentinel file or a triggered
/// `cfg.stop_flag` causes a clean exit between hours; partial results
/// returned cover hours processed so far (including any loaded from disk).
pub async fn run_harness(
    cfg: &HarnessConfig,
    variants: &[StrategyVariant],
) -> Result<Vec<HarnessRun>> {
    if cfg.capture_calibration_opportunities && !cfg.continuous {
        anyhow::bail!("calibration opportunity capture requires continuous harness mode");
    }
    if cfg.require_shared_distilled && cfg.shared_distilled_dir.is_none() {
        anyhow::bail!("required shared distilled replay needs PMXT_DISTILLED_DIR");
    }
    if cfg.continuous {
        return run_harness_continuous(cfg, variants).await;
    }

    let loader = PMXTv2Loader::new(&cfg.cache_dir);
    let all_condition_ids = cfg.universe.condition_id_set();
    let windows = cfg.universe.windows();

    // Optional bounded rayon pool. `None` → use the global pool (which respects
    // `RAYON_NUM_THREADS` env var; default is num_cpus). `Some(n)` → build a
    // local pool with exactly n threads.
    let local_pool = match cfg.threads {
        Some(n) if n > 0 => Some(
            rayon::ThreadPoolBuilder::new()
                .num_threads(n)
                .thread_name(|i| format!("harness-{i}"))
                .build()
                .map_err(|e| anyhow::anyhow!("rayon ThreadPoolBuilder: {e}"))?,
        ),
        _ => None,
    };
    let effective_threads = local_pool
        .as_ref()
        .map(|p| p.current_num_threads())
        .unwrap_or_else(rayon::current_num_threads);

    // Per-variant accumulator (merged sequentially after each hour's parallel
    // block). Index-aligned with `variants`.
    let mut variant_state: Vec<BacktestResults> = (0..variants.len())
        .map(|_| BacktestResults::default())
        .collect();

    // Load any existing per-hour checkpoints. Hours found on disk skip the
    // replay; their per-variant results are merged into `variant_state`.
    let mut hours_done: HashSet<DateTime<Utc>> = HashSet::new();
    if let Some(dir) = &cfg.checkpoint_dir {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("create checkpoint dir {}", dir.display()))?;
        let loaded = load_existing_checkpoints(dir, variants)?;
        for (h, per_variant) in loaded {
            for (acc, hour_res) in variant_state.iter_mut().zip(per_variant) {
                acc.merge_from(hour_res);
            }
            hours_done.insert(h);
        }
        if !hours_done.is_empty() {
            eprintln!(
                "harness: resumed {} checkpointed hour(s) from {}",
                hours_done.len(),
                dir.display(),
            );
            tracing::info!(
                resumed = hours_done.len(),
                dir = %dir.display(),
                "resumed from checkpoint",
            );
        }
    }

    let total_hours = cfg.hours.len();
    let already = hours_done.len();
    tracing::info!(
        variants = variants.len(),
        threads = effective_threads,
        hours = total_hours,
        already_done = already,
        remaining = total_hours.saturating_sub(already),
        "harness starting parallel variant fan-out",
    );

    let mut paused_at: Option<DateTime<Utc>> = None;
    for (i, &h) in cfg.hours.iter().enumerate() {
        // Pre-hour pause check: PAUSE sentinel OR stop_flag set.
        if should_pause(cfg) {
            paused_at = Some(h);
            tracing::warn!(
                hour = %h,
                completed = i,
                remaining = total_hours - i,
                "pause requested — exiting cleanly between hours",
            );
            break;
        }
        if hours_done.contains(&h) {
            eprintln!(
                "harness: hour {}/{} {} skipped (checkpoint exists)",
                i + 1,
                total_hours,
                h,
            );
            tracing::info!(hour = %h, "skipped (checkpoint exists)");
            continue;
        }

        let load_t0 = std::time::Instant::now();
        let hour_filter = cfg.universe.condition_id_set_for_hour(h);
        eprintln!(
            "harness: hour {}/{} {} loading {} overlapping condition_id(s) ({} total)",
            i + 1,
            total_hours,
            h,
            hour_filter.len(),
            all_condition_ids.len(),
        );

        // Reader fallback chain: shared distilled → per-tenant sidecar → parquet.
        let mut events_vec: Vec<L2Event> = Vec::new();
        let mut source = "sidecar_or_cached_parquet";
        let mut downloaded_hour = false;
        if let Some(shared_dir) = &cfg.shared_distilled_dir {
            let path = crate::backtest::distill::shared_cache_path_for_hour(shared_dir, h);
            if path.exists() {
                match crate::backtest::distill::read_distilled(&path) {
                    Ok((mut shared_events, _)) => {
                        shared_events.retain(|e| hour_filter.contains(&e.market_id));
                        events_vec = shared_events;
                        source = "shared_distilled";
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, ?path, "shared distilled cache unreadable; falling back");
                    }
                }
            }
        }
        if events_vec.is_empty() && cfg.require_shared_distilled {
            let shared_dir = cfg
                .shared_distilled_dir
                .as_ref()
                .expect("validated required shared distilled directory");
            let path = crate::backtest::distill::shared_cache_path_for_hour(shared_dir, h);
            anyhow::bail!(
                "required shared distilled hour {} is missing, unreadable, or has no events for the selected conditions: {}",
                h,
                path.display()
            );
        }
        if events_vec.is_empty() {
            match loader.load_with_sidecar(h, &hour_filter) {
                Ok(events) => {
                    events_vec = events;
                }
                Err(cache_err) => {
                    let (_, did_download) = loader.download_hour_with_status(h, false).await?;
                    downloaded_hour = did_download;
                    source = "parquet";
                    events_vec = loader.load_with_sidecar(h, &hour_filter).with_context(|| {
                        format!(
                            "load PMXT hour {} after download; initial cache load failed: {cache_err:#}",
                            h
                        )
                    })?;
                }
            }
        }
        events_vec.sort_by(|a, b| {
            a.timestamp_s
                .partial_cmp(&b.timestamp_s)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let load_ms = load_t0.elapsed().as_millis() as u64;
        // Wrap in Arc so all variant tasks share the same buffer (no per-task
        // copy of the ~16 MB event vector).
        let events: Arc<Vec<L2Event>> = Arc::new(events_vec);
        eprintln!(
            "harness: hour {}/{} {} loaded {} event(s) from {} in {:.2}s",
            i + 1,
            total_hours,
            h,
            events.len(),
            source,
            load_ms as f64 / 1000.0,
        );
        tracing::info!(
            hour = %h,
            events = events.len(),
            cids = hour_filter.len(),
            elapsed_ms = load_ms,
            source,
            "L2 events loaded",
        );

        let replay_t0 = std::time::Instant::now();
        let starting_breakers: Vec<BacktestBreakerReport> = variant_state
            .iter()
            .map(|state| state.breaker.clone())
            .collect();
        let run = |(idx, v): (usize, &StrategyVariant)| -> BacktestResults {
            let fm = build_fill_model(v);
            let mut engine = L2BacktestEngine::new(fm, cfg.latency);
            let mut strategy = CandleBacktestStrategy::new_with_breaker_and_settlement_history(
                v.clone(),
                &cfg.universe,
                cfg.bankroll_usd,
                cfg.max_total_exposure_usd,
                cfg.min_order_size_shares,
                Arc::clone(&cfg.btc_history),
                Arc::clone(&cfg.settlement_btc_history),
                starting_breakers[idx].clone(),
                cfg.breaker_cfg,
                cfg.adaptive_rearm_after_s,
            );
            engine.replay(events.iter().cloned(), &mut strategy, v.default_fee_rate);

            let mut top_skips: Vec<(&String, &u64)> = strategy.skip_reasons.iter().collect();
            top_skips.sort_by(|a, b| b.1.cmp(a.1));
            let top: Vec<String> = top_skips
                .iter()
                .take(5)
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            tracing::debug!(
                variant = %v.name,
                hour = %h,
                events_seen = strategy.events_seen,
                skipped_decision = strategy.skipped_decision,
                top_skips = top.join(" | "),
                "strategy diagnostic",
            );

            strategy.settle_all_positions();
            let breaker = strategy.breaker_report();
            let diagnostics = strategy.diagnostics();
            let decisions = strategy.decisions;
            let mut results = resolve_fills(
                &engine.fills,
                &decisions,
                &windows,
                &cfg.settlement_btc_history,
            );
            results.breaker = breaker;
            results.diagnostics = diagnostics;
            results
        };
        let per_variant: Vec<BacktestResults> = if let Some(pool) = &local_pool {
            pool.install(|| variants.par_iter().enumerate().map(run).collect())
        } else {
            variants.par_iter().enumerate().map(run).collect()
        };

        // Persist this hour's per-variant results BEFORE merging — so an
        // interrupt mid-merge doesn't desync disk vs in-memory state.
        if let Some(dir) = &cfg.checkpoint_dir {
            write_hour_checkpoint(dir, h, variants, &per_variant)?;
        }

        // Merge sequentially. Index-aligned with `variants`, so this preserves
        // the input order regardless of thread count.
        for (acc, hour_res) in variant_state.iter_mut().zip(per_variant) {
            acc.merge_from(hour_res);
        }
        hours_done.insert(h);
        let replay_ms = replay_t0.elapsed().as_millis() as u64;
        eprintln!(
            "harness: hour {}/{} {} replayed {} variant(s) in {:.2}s (done {}/{})",
            i + 1,
            total_hours,
            h,
            variants.len(),
            replay_ms as f64 / 1000.0,
            hours_done.len(),
            total_hours,
        );
        tracing::info!(
            hour = %h,
            replay_ms = replay_ms,
            variants = variants.len(),
            threads = effective_threads,
            done = hours_done.len(),
            total = total_hours,
            "variants replayed",
        );
        if cfg.delete_downloaded_parquet_after_hour && downloaded_hour {
            loader.remove_cached_hour_parquet(h)?;
            eprintln!(
                "harness: hour {}/{} {} deleted downloaded parquet",
                i + 1,
                total_hours,
                h
            );
        }
    }

    if let Some(h) = paused_at {
        tracing::info!(
            paused_before = %h,
            done = hours_done.len(),
            total = total_hours,
            "harness paused — re-run with the same --checkpoint to resume",
        );
    }

    Ok(variants
        .iter()
        .cloned()
        .zip(variant_state)
        .map(|(variant, results)| HarnessRun {
            variant,
            results,
            calibration_opportunities: Vec::new(),
        })
        .collect())
}

async fn run_harness_continuous(
    cfg: &HarnessConfig,
    variants: &[StrategyVariant],
) -> Result<Vec<HarnessRun>> {
    if cfg.checkpoint_dir.is_some() {
        anyhow::bail!("continuous harness mode does not support --checkpoint yet");
    }

    let loader = PMXTv2Loader::new(&cfg.cache_dir);
    let all_condition_ids = cfg.universe.condition_id_set();
    let windows = cfg.universe.windows();

    let local_pool = match cfg.threads {
        Some(n) if n > 0 => Some(
            rayon::ThreadPoolBuilder::new()
                .num_threads(n)
                .thread_name(|i| format!("harness-continuous-{i}"))
                .build()
                .map_err(|e| anyhow::anyhow!("rayon ThreadPoolBuilder: {e}"))?,
        ),
        _ => None,
    };

    let mut states: Vec<ContinuousVariantState> = variants
        .iter()
        .cloned()
        .map(|variant| {
            let engine = L2BacktestEngine::new(build_fill_model(&variant), cfg.latency);
            let mut strategy = CandleBacktestStrategy::new_with_breaker_and_settlement_history(
                variant.clone(),
                &cfg.universe,
                cfg.bankroll_usd,
                cfg.max_total_exposure_usd,
                cfg.min_order_size_shares,
                Arc::clone(&cfg.btc_history),
                Arc::clone(&cfg.settlement_btc_history),
                BacktestBreakerReport::default(),
                cfg.breaker_cfg,
                cfg.adaptive_rearm_after_s,
            );
            if cfg.capture_calibration_opportunities {
                strategy.enable_calibration_opportunity_capture();
            }
            ContinuousVariantState {
                variant,
                engine,
                strategy,
            }
        })
        .collect();

    let total_hours = cfg.hours.len();
    for (i, &h) in cfg.hours.iter().enumerate() {
        if should_pause(cfg) {
            tracing::warn!(
                hour = %h,
                completed = i,
                remaining = total_hours - i,
                "pause requested — exiting continuous harness between hours",
            );
            break;
        }

        let load_t0 = std::time::Instant::now();
        let hour_filter = cfg.universe.condition_id_set_for_hour(h);
        eprintln!(
            "harness-continuous: hour {}/{} {} loading {} overlapping condition_id(s) ({} total)",
            i + 1,
            total_hours,
            h,
            hour_filter.len(),
            all_condition_ids.len(),
        );

        let mut events_vec: Vec<L2Event> = Vec::new();
        let mut source = "sidecar_or_cached_parquet";
        let mut downloaded_hour = false;
        if let Some(shared_dir) = &cfg.shared_distilled_dir {
            let path = crate::backtest::distill::shared_cache_path_for_hour(shared_dir, h);
            if path.exists() {
                match crate::backtest::distill::read_distilled(&path) {
                    Ok((mut shared_events, _)) => {
                        shared_events.retain(|e| hour_filter.contains(&e.market_id));
                        events_vec = shared_events;
                        source = "shared_distilled";
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, ?path, "shared distilled cache unreadable; falling back");
                    }
                }
            }
        }
        if events_vec.is_empty() && cfg.require_shared_distilled {
            let shared_dir = cfg
                .shared_distilled_dir
                .as_ref()
                .expect("validated required shared distilled directory");
            let path = crate::backtest::distill::shared_cache_path_for_hour(shared_dir, h);
            anyhow::bail!(
                "required shared distilled hour {} is missing, unreadable, or has no events for the selected conditions: {}",
                h,
                path.display()
            );
        }
        if events_vec.is_empty() {
            match loader.load_with_sidecar(h, &hour_filter) {
                Ok(events) => {
                    events_vec = events;
                }
                Err(cache_err) => {
                    let (_, did_download) = loader.download_hour_with_status(h, false).await?;
                    downloaded_hour = did_download;
                    source = "parquet";
                    events_vec = loader.load_with_sidecar(h, &hour_filter).with_context(|| {
                        format!(
                            "load PMXT hour {} after download; initial cache load failed: {cache_err:#}",
                            h
                        )
                    })?;
                }
            }
        }
        events_vec.sort_by(|a, b| {
            a.timestamp_s
                .partial_cmp(&b.timestamp_s)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let load_ms = load_t0.elapsed().as_millis() as u64;
        let events: Arc<Vec<L2Event>> = Arc::new(events_vec);
        eprintln!(
            "harness-continuous: hour {}/{} {} loaded {} event(s) from {} in {:.2}s",
            i + 1,
            total_hours,
            h,
            events.len(),
            source,
            load_ms as f64 / 1000.0,
        );

        let replay_t0 = std::time::Instant::now();
        let replay_state = |state: &mut ContinuousVariantState| {
            state.engine.replay(
                events.iter().cloned(),
                &mut state.strategy,
                state.variant.default_fee_rate,
            );
        };
        if let Some(pool) = &local_pool {
            pool.install(|| states.par_iter_mut().for_each(replay_state));
        } else {
            states.par_iter_mut().for_each(replay_state);
        }
        let replay_ms = replay_t0.elapsed().as_millis() as u64;
        eprintln!(
            "harness-continuous: hour {}/{} {} replayed {} variant(s) in {:.2}s (done {}/{})",
            i + 1,
            total_hours,
            h,
            states.len(),
            replay_ms as f64 / 1000.0,
            i + 1,
            total_hours,
        );
        if cfg.delete_downloaded_parquet_after_hour && downloaded_hour {
            loader.remove_cached_hour_parquet(h)?;
            eprintln!(
                "harness-continuous: hour {}/{} {} deleted downloaded parquet",
                i + 1,
                total_hours,
                h
            );
        }
    }

    if cfg.capture_calibration_opportunities {
        for state in &states {
            if !state.strategy.traded.is_empty() {
                anyhow::bail!(
                    "calibration capture variant {} submitted {} trade(s); use an impossible final edge threshold to avoid truncating later opportunities",
                    state.variant.name,
                    state.strategy.traded.len(),
                );
            }
        }
    }

    Ok(states
        .into_iter()
        .map(|mut state| {
            state.strategy.settle_all_positions();
            let breaker = state.strategy.breaker_report();
            let diagnostics = state.strategy.diagnostics();
            let calibration_opportunities = state.strategy.calibration_opportunities;
            let decisions = state.strategy.decisions;
            let mut results = resolve_fills(
                &state.engine.fills,
                &decisions,
                &windows,
                &cfg.settlement_btc_history,
            );
            results.breaker = breaker;
            results.diagnostics = diagnostics;
            HarnessRun {
                variant: state.variant,
                results,
                calibration_opportunities,
            }
        })
        .collect())
}

/// Pause sentinel: `<checkpoint_dir>/PAUSE` exists, OR the `stop_flag` was
/// triggered (typically by a SIGINT handler installed by the CLI).
fn should_pause(cfg: &HarnessConfig) -> bool {
    if let Some(flag) = &cfg.stop_flag {
        if flag.load(Ordering::Relaxed) {
            return true;
        }
    }
    if let Some(dir) = &cfg.checkpoint_dir {
        if dir.join("PAUSE").exists() {
            return true;
        }
    }
    false
}

/// Per-hour checkpoint envelope. Each `<dir>/<hour>.json` contains one of
/// these. The `variant_names` field is a sanity check: on resume we refuse
/// to load a checkpoint whose variant set doesn't match the current run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct HourCheckpoint {
    hour: DateTime<Utc>,
    variant_names: Vec<String>,
    per_variant: Vec<BacktestResults>,
}

fn checkpoint_filename(hour: DateTime<Utc>) -> String {
    format!("{}.json", hour.format("%Y-%m-%dT%H"))
}

/// Atomic write: tmp + rename. Matches the rule-9 multi-tenant convention
/// (`*.tmp.<pid>` + `rename(2)`).
fn write_hour_checkpoint(
    dir: &Path,
    hour: DateTime<Utc>,
    variants: &[StrategyVariant],
    per_variant: &[BacktestResults],
) -> Result<()> {
    let envelope = HourCheckpoint {
        hour,
        variant_names: variants.iter().map(|v| v.name.clone()).collect(),
        per_variant: per_variant.to_vec(),
    };
    let final_path = dir.join(checkpoint_filename(hour));
    write_json_atomic(&final_path, &envelope, false)
        .with_context(|| format!("write checkpoint {}", final_path.display()))
}

/// Read every `<hour>.json` in `dir` and merge into a `(hour → per_variant)`
/// map. Files whose `variant_names` don't match the current `variants` are
/// rejected (returns an error) — so resuming with a different grid fails
/// loudly instead of producing garbage.
fn load_existing_checkpoints(
    dir: &Path,
    variants: &[StrategyVariant],
) -> Result<BTreeMap<DateTime<Utc>, Vec<BacktestResults>>> {
    let mut out: BTreeMap<DateTime<Utc>, Vec<BacktestResults>> = BTreeMap::new();
    let expected_names: Vec<String> = variants.iter().map(|v| v.name.clone()).collect();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(out), // no dir or unreadable → treat as empty
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.ends_with(".json") || name.contains(".tmp.") {
            continue;
        }
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "skip unreadable checkpoint");
                continue;
            }
        };
        let envelope: HourCheckpoint = match serde_json::from_str(&text) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "skip malformed checkpoint");
                continue;
            }
        };
        if envelope.variant_names != expected_names {
            return Err(anyhow::anyhow!(
                "checkpoint {} has different variant grid than this run \
                 ({} variants vs {}); re-run without --resume or pick a fresh --checkpoint dir",
                path.display(),
                envelope.variant_names.len(),
                expected_names.len(),
            ));
        }
        out.insert(envelope.hour, envelope.per_variant);
    }
    Ok(out)
}

/// Build the engine's fill model from a strategy variant. `prefer_maker` →
/// post-only-style probabilistic Maker; otherwise visible-depth BookWalkTaker.
pub(crate) fn build_fill_model(v: &StrategyVariant) -> FillModel {
    if v.prefer_maker {
        FillModel::Maker(Box::new(Maker::new(
            v.maker_fill_prob,
            crate::backtest::fill_model::DEFAULT_TICK,
            v.maker_seed,
        )))
    } else if v.use_perfect_fill {
        FillModel::Perfect(Perfect)
    } else {
        FillModel::BookWalkTaker(BookWalkTaker::default())
    }
}

pub fn render_table(runs: &[HarnessRun]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    writeln!(
        &mut out,
        "{:<24} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>9} {:>11} {:>9} {:>5}",
        "variant",
        "trades",
        "att",
        "fill%",
        "fails",
        "wins",
        "losses",
        "PnL",
        "PnL/trade",
        "fees",
        "brk"
    )
    .unwrap();
    writeln!(&mut out, "{}", "─".repeat(116)).unwrap();
    let mut sorted = runs.to_vec();
    sorted.sort_by(|a, b| {
        b.results
            .total_pnl()
            .partial_cmp(&a.results.total_pnl())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for r in &sorted {
        writeln!(
            &mut out,
            "{:<24} {:>7} {:>7} {:>6.1}% {:>7} {:>7} {:>7} {:>+8.2} {:>+10.3} {:>9.4} {:>5}",
            r.variant.name,
            r.results.n_trades(),
            r.results.execution_attempts,
            100.0 * r.results.fill_rate(),
            r.results.fills_failed,
            r.results.n_wins(),
            r.results.n_losses(),
            r.results.total_pnl(),
            r.results.avg_pnl(),
            r.results.total_fees(),
            if r.results.breaker.tripped {
                "yes"
            } else {
                "no"
            },
        )
        .unwrap();
    }
    out
}

pub fn render_zone_breakdown(runs: &[HarnessRun]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let mut sorted = runs.to_vec();
    sorted.sort_by(|a, b| {
        b.results
            .total_pnl()
            .partial_cmp(&a.results.total_pnl())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for r in &sorted {
        let zones = r.results.by_zone();
        if zones.is_empty() {
            continue;
        }
        writeln!(&mut out, "\n{} — by zone", r.variant.name).unwrap();
        writeln!(
            &mut out,
            "  {:<10} {:>7} {:>7} {:>7} {:>7} {:>9}",
            "zone", "trades", "wins", "losses", "WR%", "PnL"
        )
        .unwrap();
        for (zone, stats) in &zones {
            writeln!(
                &mut out,
                "  {:<10} {:>7} {:>7} {:>7} {:>6.1}% {:>+8.2}",
                zone,
                stats.trades,
                stats.wins,
                stats.losses,
                100.0 * stats.win_rate(),
                stats.pnl
            )
            .unwrap();
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::strategies::StrategyVariant;
    use crate::data::models::{Market, Outcome};
    use crate::data::scanner::CandleContract;

    #[test]
    fn bookwalk_slippage_walks_visible_ask_depth() {
        let mut book = TokenBook::default();
        book.asks.insert(500_000_000, 100.0);
        book.asks.insert(600_000_000, 50.0);

        let slippage = bookwalk_buy_slippage(&book, 130.0).unwrap();
        let expected_vwap = (0.50 * 100.0 + 0.60 * 30.0) / 130.0;
        assert!((slippage - (expected_vwap - 0.50)).abs() < 1e-9);
    }

    #[test]
    fn directional_log_return_aligns_with_candidate_direction() {
        let up = directional_log_return_bps(101.0, 100.0, "up").unwrap();
        let down = directional_log_return_bps(99.0, 100.0, "down").unwrap();

        assert!(up > 0.0);
        assert!(down > 0.0);
        assert_eq!(directional_log_return_bps(101.0, 100.0, "sideways"), None);
        assert_eq!(directional_log_return_bps(0.0, 100.0, "up"), None);
    }

    #[test]
    fn historical_gamma_price_is_not_an_l2_fallback() {
        let (cfg, variants) = synthetic_cfg();
        let strategy = CandleBacktestStrategy::new_with_breaker(
            variants[0].clone(),
            &cfg.universe,
            100.0,
            80.0,
            0.0,
            cfg.btc_history,
            BacktestBreakerReport::default(),
            BreakerConfig::default(),
            None,
        );

        assert_eq!(strategy.fresh_ask("1", 1_745_654_400.0), None);
    }

    /// Build a tiny synthetic universe + history for the parallel-vs-serial
    /// determinism test. We don't need the harness to find any trades — we
    /// just need it to run end-to-end and produce per-variant results we can
    /// compare across thread counts.
    fn synthetic_cfg() -> (HarnessConfig, Vec<StrategyVariant>) {
        let contract = CandleContract {
            market: Market {
                condition_id: "0xabc".into(),
                question: "BTC Up or Down - test".into(),
                end_date: "2026-04-26T08:30:00Z".into(),
                outcomes: vec![
                    Outcome {
                        name: "Up".into(),
                        price: 0.5,
                        token_id: "1".into(),
                    },
                    Outcome {
                        name: "Down".into(),
                        price: 0.5,
                        token_id: "2".into(),
                    },
                ],
                ..Default::default()
            },
            asset: "BTC".into(),
            window_description: "8:00AM-8:30AM ET".into(),
            up_token_id: "1".into(),
            down_token_id: "2".into(),
            end_date: "2026-04-26T08:30:00Z".into(),
            hours_left: 0.0,
            up_price: 0.5,
            down_price: 0.5,
            volume: 0.0,
            liquidity: 0.0,
        };
        let universe = CandleUniverse {
            contracts: vec![contract],
        };

        let mut btc = BTCHistory::default();
        // 60 evenly spaced 1-second ticks around the synthetic window.
        let base_ms = 1745654400000_i64; // 2026-04-26T08:00:00Z
        for i in 0..1800 {
            btc.timestamps_ms.push(base_ms + i * 1000);
            btc.prices.push(50000.0 + (i as f64).sin() * 10.0);
        }

        let variants = vec![
            StrategyVariant::baseline(),
            StrategyVariant::loose_smoke(),
            StrategyVariant::loose_maker(),
        ];

        let cfg = HarnessConfig {
            hours: vec![], // empty hours -> the loop is a no-op, but the parallel
            // setup code still runs (pool build, universe prep).
            universe,
            settlement_btc_history: Arc::new(btc.clone()),
            btc_history: Arc::new(btc),
            bankroll_usd: 100.0,
            max_total_exposure_usd: 80.0,
            min_order_size_shares: 0.0,
            cache_dir: PathBuf::from("/tmp"),
            latency: StaticLatencyConfig::default(),
            breaker_cfg: BreakerConfig::default(),
            adaptive_rearm_after_s: None,
            shared_distilled_dir: None,
            require_shared_distilled: false,
            threads: None,
            checkpoint_dir: None,
            stop_flag: None,
            continuous: false,
            capture_calibration_opportunities: false,
            delete_downloaded_parquet_after_hour: false,
        };
        (cfg, variants)
    }

    #[tokio::test]
    async fn empty_hours_returns_empty_state_per_variant() {
        let (cfg, variants) = synthetic_cfg();
        let runs = run_harness(&cfg, &variants).await.unwrap();
        assert_eq!(runs.len(), variants.len());
        // Order preserved: variant[i] in == variant[i] out.
        for (run, v) in runs.iter().zip(&variants) {
            assert_eq!(run.variant.name, v.name);
            assert_eq!(run.results.n_trades(), 0);
        }
    }

    #[tokio::test]
    async fn calibration_capture_requires_continuous_mode() {
        let (mut cfg, variants) = synthetic_cfg();
        cfg.capture_calibration_opportunities = true;

        let error = run_harness(&cfg, &variants).await.unwrap_err();

        assert!(error
            .to_string()
            .contains("calibration opportunity capture requires continuous"));
    }

    #[tokio::test]
    async fn required_shared_distilled_never_falls_back() {
        let (mut cfg, variants) = synthetic_cfg();
        let tmp = tempfile::TempDir::new().unwrap();
        cfg.hours = vec![DateTime::parse_from_rfc3339("2026-04-26T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc)];
        cfg.shared_distilled_dir = Some(tmp.path().to_path_buf());
        cfg.require_shared_distilled = true;

        let error = run_harness(&cfg, &variants).await.unwrap_err();

        assert!(error.to_string().contains("required shared distilled hour"));
    }

    #[tokio::test]
    async fn required_shared_distilled_requires_configured_directory() {
        let (mut cfg, variants) = synthetic_cfg();
        cfg.require_shared_distilled = true;

        let error = run_harness(&cfg, &variants).await.unwrap_err();

        assert!(error.to_string().contains("needs PMXT_DISTILLED_DIR"));
    }

    #[tokio::test]
    async fn thread_count_does_not_change_output_order() {
        // Same synthetic cfg; verify result order is variant-stable for both
        // serial (threads=1) and parallel (threads=4).
        let (mut cfg, variants) = synthetic_cfg();
        cfg.threads = Some(1);
        let serial = run_harness(&cfg, &variants).await.unwrap();
        cfg.threads = Some(4);
        let parallel = run_harness(&cfg, &variants).await.unwrap();
        assert_eq!(serial.len(), parallel.len());
        for (s, p) in serial.iter().zip(&parallel) {
            assert_eq!(s.variant.name, p.variant.name);
            assert_eq!(s.results.n_trades(), p.results.n_trades());
        }
    }

    #[test]
    fn condition_id_set_for_hour_filters_to_overlapping_windows() {
        let (cfg, _) = synthetic_cfg();
        let active_hour = chrono::DateTime::parse_from_rfc3339("2026-04-26T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let inactive_hour = chrono::DateTime::parse_from_rfc3339("2026-04-26T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let active = cfg.universe.condition_id_set_for_hour(active_hour);
        assert!(active.contains("0xabc"));
        let inactive = cfg.universe.condition_id_set_for_hour(inactive_hour);
        assert!(!inactive.contains("0xabc"));
    }

    #[test]
    fn traded_markets_do_not_stop_global_btc_sampling() {
        let (cfg, variants) = synthetic_cfg();
        let mut strategy = CandleBacktestStrategy::new_with_breaker(
            variants[0].clone(),
            &cfg.universe,
            100.0,
            80.0,
            0.0,
            cfg.btc_history,
            BacktestBreakerReport::default(),
            BreakerConfig::default(),
            None,
        );
        strategy.traded.insert("0xabc".to_string());
        let history = L2MidHistory::default();
        let book = TokenBook::default();
        let base_s = 1_745_654_400.0;

        for second in 0..30 {
            strategy.on_event(base_s + f64::from(second), "1", &book, &history);
        }

        assert!(strategy.momentum.rolling_realized_vol(30.0).is_some());
    }

    #[test]
    fn calibration_capture_requests_l2_path_history() {
        let (cfg, variants) = synthetic_cfg();
        let mut strategy = CandleBacktestStrategy::new_with_breaker(
            variants[0].clone(),
            &cfg.universe,
            100.0,
            80.0,
            0.0,
            cfg.btc_history,
            BacktestBreakerReport::default(),
            BreakerConfig::default(),
            None,
        );

        assert!(!strategy.needs_l2_history());
        strategy.capture_calibration_opportunities = true;
        assert!(strategy.needs_l2_history());
    }

    #[test]
    fn position_budget_uses_active_bankroll_after_realized_pnl() {
        let (cfg, mut variants) = synthetic_cfg();
        let mut variant = variants.remove(0);
        variant.position_pct = 0.05;
        variant.max_per_market_usd = 20.0;
        let strategy = CandleBacktestStrategy::new_with_breaker(
            variant,
            &cfg.universe,
            100.0,
            80.0,
            0.0,
            cfg.btc_history,
            BacktestBreakerReport {
                state: BreakerState {
                    realized_pnl: -20.0,
                    ..Default::default()
                },
                ..Default::default()
            },
            BreakerConfig::default(),
            None,
        );

        assert!((strategy.active_bankroll() - 80.0).abs() < 1e-9);
        let (budget, available) = strategy.position_budget_before_stress(0.0);
        assert!((budget - 4.0).abs() < 1e-9);
        assert!((available - 64.0).abs() < 1e-9);
    }

    #[test]
    fn settlement_basis_exit_submits_sell_and_realizes_executable_pnl() {
        let (cfg, mut variants) = synthetic_cfg();
        let mut variant = variants.remove(0);
        variant.exit.settlement_basis_enabled = true;
        let mut strategy = CandleBacktestStrategy::new_with_breaker(
            variant,
            &cfg.universe,
            100.0,
            80.0,
            0.0,
            cfg.btc_history,
            BacktestBreakerReport::default(),
            BreakerConfig::default(),
            None,
        );
        strategy.open_positions.insert(
            "entry".to_string(),
            BacktestOpenPosition {
                condition_id: "0xabc".to_string(),
                token_id: "1".to_string(),
                direction: "up".to_string(),
                open_btc: 100.0,
                settlement_open_btc: 100.0,
                close_ts_s: 60.0,
                official_direction: None,
                entry_timestamp_s: 0.0,
                entry_price: 0.80,
                size: 10.0,
                fee: 0.05,
                exit_fee_rate: 0.0,
                exit_pending: false,
                last_exit_attempt_ts_s: None,
            },
        );
        strategy.books.insert(
            "1".to_string(),
            TokenBook {
                bids: BTreeMap::from([(400_000_000, 20.0)]),
                asks: BTreeMap::from([(420_000_000, 20.0)]),
                best_bid: 0.40,
                best_ask: 0.42,
                last_update_ts_s: 30.0,
            },
        );

        let orders = strategy.maybe_exit_orders(30.0, "1", 99.0);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].side, "sell");
        assert_eq!(orders[0].limit_price, Some(0.40));
        assert!(strategy.open_positions["entry"].exit_pending);

        strategy.on_fills(&[crate::backtest::l2_replay::BacktestFill {
            order: orders[0].clone(),
            fill_timestamp_s: 31.0,
            fill_price: 0.40,
            filled_size: 10.0,
            cost: -4.0,
            fee: 0.02,
            slippage: 0.0,
            book_age_ms: 1_000.0,
            success: true,
            reason: "".to_string(),
        }]);

        assert!(strategy.open_positions.is_empty());
        assert!(strategy.submitted_exits.is_empty());
        assert_eq!(strategy.breaker_state.losses, 1);
        assert!((strategy.breaker_state.realized_pnl + 4.07).abs() < 1e-9);
        let diagnostics = strategy.diagnostics();
        assert_eq!(diagnostics.exit_signals, 1);
        assert_eq!(diagnostics.exit_fills, 1);
        assert_eq!(diagnostics.exit_failures, 0);
    }

    fn mk_test_fill(
        intent_id: &str,
        fill_timestamp_s: f64,
    ) -> crate::backtest::l2_replay::BacktestFill {
        crate::backtest::l2_replay::BacktestFill {
            order: BacktestOrder {
                intent_id: intent_id.to_string(),
                timestamp_s: fill_timestamp_s - 0.05,
                condition_id: "cid".to_string(),
                token_id: "tok".to_string(),
                side: "buy".to_string(),
                size: 10.0,
                order_type: "market".to_string(),
                limit_price: None,
                fee_rate: 0.0,
                maker_fee_rate: 0.0,
            },
            fill_timestamp_s,
            fill_price: 0.5,
            filled_size: 10.0,
            cost: 5.0,
            fee: 0.0,
            slippage: 0.0,
            book_age_ms: 0.0,
            success: true,
            reason: "taker".to_string(),
        }
    }

    #[test]
    fn backtest_strategy_trips_shared_breaker_on_settled_losses() {
        let (cfg, variants) = synthetic_cfg();
        let mut btc = BTCHistory::default();
        btc.timestamps_ms.extend([10_000, 20_000]);
        btc.prices.extend([90.0, 90.0]);
        let breaker_cfg = BreakerConfig {
            min_trades: 2,
            min_win_rate: 0.75,
            max_drawdown_pct: 0.30,
        };
        let mut strategy = CandleBacktestStrategy::new_with_breaker(
            variants[0].clone(),
            &cfg.universe,
            100.0,
            80.0,
            0.0,
            Arc::new(btc),
            BacktestBreakerReport::default(),
            breaker_cfg,
            None,
        );

        for (intent_id, close_ts_s) in [("loss-1", 10.0), ("loss-2", 20.0)] {
            strategy.submitted_positions.insert(
                intent_id.to_string(),
                BacktestOpenPosition {
                    condition_id: "cid".to_string(),
                    token_id: "token".to_string(),
                    direction: "up".to_string(),
                    open_btc: 100.0,
                    settlement_open_btc: 100.0,
                    close_ts_s,
                    official_direction: None,
                    entry_timestamp_s: close_ts_s - 1.0,
                    entry_price: 0.0,
                    size: 0.0,
                    fee: 0.0,
                    exit_fee_rate: 0.0,
                    exit_pending: false,
                    last_exit_attempt_ts_s: None,
                },
            );
            strategy.on_fills(&[mk_test_fill(intent_id, close_ts_s - 1.0)]);
        }

        strategy.settle_due_positions(21.0);
        let report = strategy.breaker_report();
        assert!(report.tripped);
        assert_eq!(report.reason.as_deref(), Some("win_rate_low"));
        assert_eq!(report.state.losses, 2);
    }

    #[test]
    fn breaker_resolution_uses_settlement_tape_not_signal_tape() {
        let (cfg, variants) = synthetic_cfg();
        let mut signal_btc = BTCHistory::default();
        signal_btc.timestamps_ms.extend([0, 20_000]);
        signal_btc.prices.extend([100.0, 110.0]);
        let mut settlement_btc = BTCHistory::default();
        settlement_btc.timestamps_ms.extend([0, 20_000]);
        settlement_btc.prices.extend([100.0, 90.0]);
        let mut strategy = CandleBacktestStrategy::new_with_breaker_and_settlement_history(
            variants[0].clone(),
            &cfg.universe,
            100.0,
            80.0,
            0.0,
            Arc::new(signal_btc),
            Arc::new(settlement_btc),
            BacktestBreakerReport::default(),
            BreakerConfig::default(),
            None,
        );
        strategy.open_positions.insert(
            "down-winner".to_string(),
            BacktestOpenPosition {
                condition_id: "cid".to_string(),
                token_id: "token".to_string(),
                direction: "down".to_string(),
                open_btc: 100.0,
                settlement_open_btc: 100.0,
                close_ts_s: 20.0,
                official_direction: None,
                entry_timestamp_s: 10.0,
                entry_price: 0.50,
                size: 10.0,
                fee: 0.0,
                exit_fee_rate: 0.0,
                exit_pending: false,
                last_exit_attempt_ts_s: None,
            },
        );

        strategy.settle_due_positions(20.0);

        let report = strategy.breaker_report();
        assert_eq!(report.state.wins, 1);
        assert_eq!(report.state.losses, 0);
    }

    #[test]
    fn adaptive_rearm_only_resets_win_rate_breaker_after_cooldown() {
        let (cfg, variants) = synthetic_cfg();
        let breaker_state = BreakerState {
            losses: 2,
            realized_pnl: -10.0,
            ..Default::default()
        };
        let mut strategy = CandleBacktestStrategy::new_with_breaker(
            variants[0].clone(),
            &cfg.universe,
            100.0,
            80.0,
            0.0,
            Arc::new(BTCHistory::default()),
            BacktestBreakerReport {
                tripped: true,
                reason: Some("win_rate_low".to_string()),
                tripped_at_s: Some(100.0),
                state: breaker_state,
                ..BacktestBreakerReport::default()
            },
            BreakerConfig::default(),
            Some(60.0),
        );

        strategy.maybe_rearm_adaptive_health(159.0);
        assert!(strategy.breaker_report().tripped);
        assert_eq!(strategy.diagnostics().adaptive_rearms, 0);

        strategy.maybe_rearm_adaptive_health(161.0);
        let report = strategy.breaker_report();
        assert!(!report.tripped);
        assert_eq!(report.state.wins + report.state.losses, 0);
        assert_eq!(strategy.diagnostics().adaptive_rearms, 1);
        assert_eq!(
            strategy
                .diagnostics()
                .skip_reasons
                .get("adaptive_health_rearm"),
            Some(&1)
        );
    }

    #[test]
    fn adaptive_rearm_leaves_drawdown_breaker_tripped() {
        let (cfg, variants) = synthetic_cfg();
        let mut strategy = CandleBacktestStrategy::new_with_breaker(
            variants[0].clone(),
            &cfg.universe,
            100.0,
            80.0,
            0.0,
            Arc::new(BTCHistory::default()),
            BacktestBreakerReport {
                tripped: true,
                reason: Some("realized_drawdown".to_string()),
                tripped_at_s: Some(100.0),
                ..BacktestBreakerReport::default()
            },
            BreakerConfig::default(),
            Some(60.0),
        );

        strategy.maybe_rearm_adaptive_health(1_000.0);
        assert!(strategy.breaker_report().tripped);
        assert_eq!(
            strategy.breaker_report().reason.as_deref(),
            Some("realized_drawdown")
        );
        assert_eq!(strategy.diagnostics().adaptive_rearms, 0);
    }

    #[test]
    fn checkpoint_roundtrip_atomic() {
        let tmp = tempfile::TempDir::new().unwrap();
        let variants = vec![StrategyVariant::baseline(), StrategyVariant::loose_smoke()];
        let per_variant = vec![BacktestResults::default(), BacktestResults::default()];
        let h = chrono::DateTime::parse_from_rfc3339("2026-04-23T05:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        write_hour_checkpoint(tmp.path(), h, &variants, &per_variant).unwrap();

        // File exists, no leftover tmp
        let written = tmp.path().join("2026-04-23T05.json");
        assert!(written.exists(), "atomic write should leave the final file");
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no .tmp file should remain after rename"
        );

        // Reload and verify hour is present
        let loaded = load_existing_checkpoints(tmp.path(), &variants).unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded.contains_key(&h));
    }

    #[test]
    fn checkpoint_rejects_grid_mismatch() {
        let tmp = tempfile::TempDir::new().unwrap();
        let written_with = vec![StrategyVariant::baseline(), StrategyVariant::loose_smoke()];
        let h = chrono::DateTime::parse_from_rfc3339("2026-04-23T05:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        write_hour_checkpoint(
            tmp.path(),
            h,
            &written_with,
            &vec![BacktestResults::default(); 2],
        )
        .unwrap();

        // Try to load with a different variant set — should fail loudly
        let resume_with = vec![StrategyVariant::baseline()];
        let err = load_existing_checkpoints(tmp.path(), &resume_with).unwrap_err();
        assert!(
            err.to_string().contains("different variant grid"),
            "expected grid-mismatch error, got: {err}"
        );
    }

    #[tokio::test]
    async fn pause_sentinel_short_circuits_remaining_hours() {
        let tmp = tempfile::TempDir::new().unwrap();
        // Create the PAUSE sentinel BEFORE running — the harness should bail
        // at the first hour boundary (no replay, no checkpoint write).
        std::fs::File::create(tmp.path().join("PAUSE")).unwrap();

        let (mut cfg, variants) = synthetic_cfg();
        // Give the harness some hypothetical hours so the loop body would
        // normally run. Without parquets these would error during load, so
        // pause must short-circuit before any download attempt.
        cfg.hours = vec![chrono::DateTime::parse_from_rfc3339("2026-04-23T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)];
        cfg.checkpoint_dir = Some(tmp.path().to_path_buf());

        let runs = run_harness(&cfg, &variants).await.unwrap();
        // No hours processed → empty per-variant state.
        for run in &runs {
            assert_eq!(run.results.n_trades(), 0);
        }
        // No <hour>.json should have been written.
        let json_files: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".json"))
            .collect();
        assert!(
            json_files.is_empty(),
            "PAUSE before any work → no checkpoint files"
        );
    }
}
