//! Cached-data replay of the live decision loop.
//!
//! This is the bridge between the fast research harness and the production
//! runtime: PMXT L2 events become the market-book feed, a cached BTC tape
//! becomes the exchange-price feed, and the output is a normal session JSONL.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::backtest::btc_history::BTCHistory;
use crate::backtest::harness::{build_fill_model, CandleUniverse};
use crate::backtest::l2_replay::{
    BacktestOrder, L2BacktestEngine, StaticLatencyConfig, Strategy, TokenBook,
};
use crate::backtest::pmxt::{L2Event, PMXTv2Loader};
use crate::backtest::strategies::StrategyVariant;
use crate::config::Settings;
use crate::data::scanner::CandleContract;
use crate::execution::order_manager::OrderManager;
use crate::monitoring::session::{OrderFilled, OrderPlaced, SessionMonitor, SignalEvaluation};
use crate::release::ReleaseManifest;
use crate::strategy::decision::{decide_candle_trade, DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_EDGE};
use crate::strategy::decision::{CandleDecision, DecisionResult, ZoneConfig};
use crate::strategy::microstructure::{BookLevelView, BookMicrostructure, MicrostructureConfig};
use crate::strategy::momentum::{MomentumConfig, MomentumDetector};
use crate::strategy::spec::{stable_json_hash, OrderIntent, Signal, StrategySpec};

#[derive(Clone)]
pub struct LiveReplayConfig {
    pub hours: Vec<DateTime<Utc>>,
    pub universe: CandleUniverse,
    pub btc_history: Arc<BTCHistory>,
    pub bankroll_usd: f64,
    pub cache_dir: PathBuf,
    pub session_log_dir: PathBuf,
    pub latency: StaticLatencyConfig,
    pub shared_distilled_dir: Option<PathBuf>,
    pub strategy: ReplayStrategy,
}

#[derive(Debug, Clone)]
pub struct ReplayStrategy {
    pub variant: StrategyVariant,
    pub strategy_spec: StrategySpec,
    pub source: String,
}

impl ReplayStrategy {
    pub fn load(settings: &Settings) -> Result<Self> {
        let path = settings.promotion_artifact_path.trim();
        if path.is_empty() {
            return Ok(Self::from_settings(settings));
        }

        let artifact = crate::backtest::experiment::read_promotion(path)
            .with_context(|| format!("load promotion artifact {path}"))?;
        if artifact.selected_strategy.name != "candle_momentum" {
            bail!(
                "unsupported promoted strategy {}",
                artifact.selected_strategy.name
            );
        }
        let mut variant: StrategyVariant = serde_json::from_value(artifact.strategy_params.clone())
            .context("parse promoted strategy_params as StrategyVariant")?;
        let params_hash = stable_json_hash(&variant);
        if params_hash != artifact.selected_strategy.params_hash {
            bail!(
                "promotion artifact hash mismatch: strategy_params hash {} != selected_strategy hash {}",
                params_hash,
                artifact.selected_strategy.params_hash
            );
        }
        let safety_floor_applied = variant.zone_config.apply_settings_safety_floor(settings);
        let mut strategy_spec = artifact.selected_strategy;
        let mut source = format!("promotion:{path}");
        if safety_floor_applied {
            strategy_spec = StrategySpec::from_serializable_params(
                strategy_spec.name.clone(),
                strategy_spec.version.clone(),
                &variant,
                format!(
                    "{};settlement_floor cutoff_min={:.2},guard_min={:.2},min_abs_usd={:.2},sigma_buffer={:.2}",
                    strategy_spec.risk_profile,
                    variant.zone_config.settlement_cutoff_minutes,
                    variant.zone_config.settlement_guard_minutes,
                    variant.zone_config.settlement_min_abs_move_usd,
                    variant.zone_config.settlement_sigma_buffer,
                ),
            );
            source = format!("{source}+settlement_floor");
        }

        Ok(Self {
            variant,
            strategy_spec,
            source,
        })
    }

    pub fn from_settings(settings: &Settings) -> Self {
        let zone_config = ZoneConfig::from_settings(settings);
        let mut variant = StrategyVariant::baseline();
        variant.name = "settings_live_replay".to_string();
        variant.zone_config = zone_config.clone();
        variant.skip_dead_zone = settings.candle_skip_dead_zone;
        variant.min_confidence = DEFAULT_MIN_CONFIDENCE;
        variant.min_edge = DEFAULT_MIN_EDGE;
        variant.position_pct = settings.candle_position_pct;
        variant.max_per_market_usd = settings.max_position_per_market_usd;
        variant.prefer_maker = settings.candle_prefer_maker;
        variant.default_fee_rate = 0.072;
        variant.maker_fee_rate = 0.0;
        variant.microstructure = MicrostructureConfig::disabled();

        let params = serde_json::json!({
            "zone_config": zone_config,
            "skip_dead_zone": settings.candle_skip_dead_zone,
            "min_confidence": DEFAULT_MIN_CONFIDENCE,
            "min_edge": DEFAULT_MIN_EDGE,
            "position_pct": settings.candle_position_pct,
            "max_per_market_usd": settings.max_position_per_market_usd,
            "prefer_maker": settings.candle_prefer_maker,
            "default_fee_rate": 0.072,
            "microstructure": MicrostructureConfig::disabled(),
        });
        let strategy_spec = StrategySpec::from_serializable_params(
            "candle_momentum",
            "1",
            &params,
            format!(
                "position_pct={:.4};max_per_market_usd={:.2}",
                settings.candle_position_pct, settings.max_position_per_market_usd
            ),
        );

        Self {
            variant,
            strategy_spec,
            source: "settings".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LiveReplayReport {
    pub schema_version: u32,
    pub session_id: String,
    pub session_path: String,
    pub summary_path: String,
    pub hours: Vec<String>,
    pub contracts: usize,
    pub events_loaded: usize,
    pub events_processed: u64,
    pub orders_submitted: usize,
    pub fills_success: usize,
    pub fills_failed: usize,
    pub resolutions: usize,
    pub total_pnl: f64,
    pub oracle_checks: usize,
    pub strategy: String,
    pub strategy_source: String,
}

pub async fn run_live_replay(
    cfg: LiveReplayConfig,
    settings: &Settings,
) -> Result<LiveReplayReport> {
    let monitor = Arc::new(SessionMonitor::open(&cfg.session_log_dir)?);
    monitor.record_release_manifest(&ReleaseManifest::capture(
        settings,
        crate::config::RuntimeMode::Paper,
    ));
    monitor.record_runtime_strategy(
        &cfg.strategy.source,
        &cfg.strategy.strategy_spec,
        &cfg.strategy.variant.zone_config,
    );

    let loader = PMXTv2Loader::new(&cfg.cache_dir);
    let token_filter = cfg.universe.condition_id_set();
    let mut all_events = Vec::new();
    for &hour in &cfg.hours {
        eprintln!("live-replay: loading PMXT hour {hour}");
        let mut events = load_replay_hour(
            &loader,
            hour,
            &token_filter,
            cfg.shared_distilled_dir.as_ref(),
        )
        .with_context(|| format!("load replay hour {hour}"))?;
        all_events.append(&mut events);
    }
    all_events.sort_by(|a, b| {
        a.timestamp_s
            .partial_cmp(&b.timestamp_s)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let events_loaded = all_events.len();

    let mut strategy = LiveReplayStrategy::new(
        cfg.strategy.clone(),
        &cfg.universe,
        cfg.bankroll_usd,
        cfg.btc_history.clone(),
        monitor.clone(),
    );
    let mut engine = L2BacktestEngine::new(build_fill_model(&cfg.strategy.variant), cfg.latency);
    engine.replay(
        all_events,
        &mut strategy,
        cfg.strategy.variant.default_fee_rate,
    );
    strategy.record_fills(&engine.fills).await;
    monitor.save_summary()?;

    let summary = engine.summary();
    let lifecycle = strategy.lifecycle;
    Ok(LiveReplayReport {
        schema_version: 1,
        session_id: monitor.session_id().to_string(),
        session_path: monitor.events_path().display().to_string(),
        summary_path: monitor.summary_path().display().to_string(),
        hours: cfg.hours.iter().map(|h| h.to_rfc3339()).collect(),
        contracts: cfg.universe.contracts.len(),
        events_loaded,
        events_processed: summary.events_processed,
        orders_submitted: strategy.orders_submitted,
        fills_success: summary.fills_success as usize,
        fills_failed: summary.fills_failed as usize,
        resolutions: lifecycle.resolutions,
        total_pnl: lifecycle.realized_pnl,
        oracle_checks: lifecycle.oracle_checks,
        strategy: cfg.strategy.variant.name,
        strategy_source: cfg.strategy.source,
    })
}

fn load_replay_hour(
    loader: &PMXTv2Loader,
    hour: DateTime<Utc>,
    token_filter: &HashSet<String>,
    shared_distilled_dir: Option<&PathBuf>,
) -> Result<Vec<L2Event>> {
    if let Some(shared_dir) = shared_distilled_dir {
        let path = crate::backtest::distill::shared_cache_path_for_hour(shared_dir, hour);
        if path.exists() {
            match crate::backtest::distill::read_distilled(&path) {
                Ok((mut events, _)) => {
                    events.retain(|e| token_filter.contains(&e.market_id));
                    return Ok(events);
                }
                Err(e) => {
                    tracing::warn!(error = %e, ?path, "shared distilled cache unreadable; falling back");
                }
            }
        }
    }
    loader.load_with_sidecar(hour, token_filter)
}

struct LiveReplayStrategy {
    replay_strategy: ReplayStrategy,
    universe_by_token: BTreeMap<String, CandleContract>,
    books: BTreeMap<String, TokenBook>,
    momentum: MomentumDetector,
    order_manager: OrderManager,
    bankroll_usd: f64,
    btc_history: Arc<BTCHistory>,
    monitor: Arc<SessionMonitor>,
    traded: HashSet<String>,
    replay_positions: BTreeMap<String, ReplayPosition>,
    lifecycle: ReplayLifecycle,
    last_tick_ts_s: f64,
    orders_submitted: usize,
}

#[derive(Debug, Clone)]
struct ReplayPosition {
    condition_id: String,
    direction: String,
    open_btc: f64,
    close_ts_s: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct ReplayLifecycle {
    realized_pnl: f64,
    wins: u64,
    losses: u64,
    resolutions: usize,
    oracle_checks: usize,
}

impl LiveReplayStrategy {
    fn new(
        replay_strategy: ReplayStrategy,
        universe: &CandleUniverse,
        bankroll_usd: f64,
        btc_history: Arc<BTCHistory>,
        monitor: Arc<SessionMonitor>,
    ) -> Self {
        Self {
            replay_strategy,
            universe_by_token: universe.by_token_id(),
            books: BTreeMap::new(),
            momentum: MomentumDetector::new(
                None,
                MomentumConfig {
                    noise_z_threshold: 0.3,
                    ..Default::default()
                },
            ),
            order_manager: OrderManager::new(),
            bankroll_usd,
            btc_history,
            monitor,
            traded: HashSet::new(),
            replay_positions: BTreeMap::new(),
            lifecycle: ReplayLifecycle::default(),
            last_tick_ts_s: 0.0,
            orders_submitted: 0,
        }
    }

    async fn record_fills(&mut self, fills: &[crate::backtest::l2_replay::BacktestFill]) {
        self.record_risk_state(0.0, 0);
        for fill in fills {
            let order_id = replay_order_id(&fill.order.intent_id);
            if fill.success {
                let limit_price = fill
                    .order
                    .limit_price
                    .unwrap_or(fill.fill_price - fill.slippage);
                self.monitor.record_order_filled(&OrderFilled {
                    intent_id: fill.order.intent_id.clone(),
                    order_id,
                    filled: fill.filled_size,
                    requested: fill.order.size,
                    fill_pct: if fill.order.size > 0.0 {
                        (fill.filled_size / fill.order.size).clamp(0.0, 1.0)
                    } else {
                        0.0
                    },
                    fill_price: fill.fill_price,
                    limit_price,
                    slippage: fill.slippage,
                    slippage_bps: if limit_price > 0.0 {
                        fill.slippage / limit_price * 10_000.0
                    } else {
                        0.0
                    },
                    fill_time_s: (fill.fill_timestamp_s - fill.order.timestamp_s).max(0.0),
                    fee: fill.fee,
                    n_trades: 1,
                });
                self.record_open_position(fill);
                self.record_resolution(fill);
            } else {
                self.monitor.record_order_rejected(
                    &fill.order.token_id,
                    &fill.reason,
                    fill.order.limit_price.unwrap_or(0.0),
                    fill.order.size,
                );
            }
        }
    }

    fn record_open_position(&self, fill: &crate::backtest::l2_replay::BacktestFill) {
        let exposure = fill.cost.abs();
        self.monitor.record_risk_state(
            self.bankroll_usd + self.lifecycle.realized_pnl,
            exposure,
            (self.bankroll_usd + self.lifecycle.realized_pnl - exposure).max(0.0),
            1,
            self.lifecycle.realized_pnl,
            self.lifecycle.wins,
            self.lifecycle.losses,
        );
    }

    fn record_resolution(&mut self, fill: &crate::backtest::l2_replay::BacktestFill) {
        let Some(pos) = self.replay_positions.get(&fill.order.intent_id).cloned() else {
            self.monitor.record_error(
                "live_replay_resolution",
                "missing replay position for fill intent",
                true,
            );
            return;
        };
        let close_btc = self.btc_history.price_at_seconds(pos.close_ts_s);
        if close_btc <= 0.0 || pos.open_btc <= 0.0 {
            self.monitor.record_error(
                "live_replay_resolution",
                "missing BTC open/close price for replay fill",
                true,
            );
            return;
        }

        let actual = if close_btc >= pos.open_btc { "up" } else { "down" };
        let won = actual == pos.direction;
        let pnl = paper_outcome_pnl(won, fill.fill_price, fill.filled_size, fill.fee);
        self.monitor.record_resolution(
            &pos.condition_id,
            &pos.direction,
            actual,
            won,
            pnl,
            fill.fill_price,
            pos.open_btc,
            close_btc,
        );

        let outcome_prices = replay_outcome_prices(actual);
        self.monitor.record_oracle_resolution(
            &pos.condition_id,
            actual,
            pos.open_btc,
            close_btc,
            actual,
            &outcome_prices,
            true,
            true,
            0.0,
        );

        self.lifecycle.resolutions += 1;
        self.lifecycle.oracle_checks += 1;
        self.lifecycle.realized_pnl += pnl;
        if won {
            self.lifecycle.wins += 1;
        } else {
            self.lifecycle.losses += 1;
        }
        self.record_risk_state(0.0, 0);
    }

    fn record_risk_state(&self, exposure: f64, positions: u64) {
        let bankroll = self.bankroll_usd + self.lifecycle.realized_pnl;
        self.monitor.record_risk_state(
            bankroll,
            exposure,
            (bankroll - exposure).max(0.0),
            positions,
            self.lifecycle.realized_pnl,
            self.lifecycle.wins,
            self.lifecycle.losses,
        );
    }

    fn fresh_ask(&self, token_id: &str, now_ts: f64, fallback: f64) -> f64 {
        self.books
            .get(token_id)
            .filter(|b| now_ts - b.last_update_ts_s <= 30.0)
            .and_then(|b| (b.best_ask > 0.0).then_some(b.best_ask))
            .unwrap_or(fallback)
    }

    fn record_skip(
        &self,
        timestamp_s: f64,
        contract: &CandleContract,
        signal: &crate::strategy::momentum::MomentumSignal,
        up_price: f64,
        down_price: f64,
        implied_vol: f64,
        zone: String,
        reason: String,
        detail: String,
    ) {
        let aggregate = format!("{reason}_{zone}");
        self.monitor
            .record_signal_skip(&contract.market.condition_id, &aggregate);
        self.monitor.record_signal_evaluation(&SignalEvaluation {
            ts_ms: (timestamp_s * 1000.0) as i64,
            cid: short_cid(&contract.market.condition_id),
            asset: contract.asset.clone(),
            open: signal.open_price,
            px: signal.current_price,
            chg: signal.price_change,
            chg_pct: signal.price_change_pct,
            cons: signal.consistency,
            z: signal.z_score,
            conf: signal.confidence,
            elapsed_min: signal.minutes_elapsed,
            remaining_min: signal.minutes_remaining,
            dir: signal.direction.clone(),
            vol_fast: implied_vol,
            vol_slow: implied_vol,
            implied_vol,
            cross_boost: 0.0,
            up_price,
            down_price,
            book_spread: 0.0,
            book_pressure: 0.0,
            book_bid_depth: 0.0,
            book_ask_depth: 0.0,
            zone,
            fair: 0.0,
            edge: 0.0,
            decision_trade: false,
            execution_attempted: false,
            traded: false,
            skip_reason: Some(reason),
            skip_detail: Some(detail),
        });
    }
}

impl Strategy for LiveReplayStrategy {
    fn on_event(
        &mut self,
        timestamp_s: f64,
        token_id: &str,
        book: &TokenBook,
        _history: &BTreeMap<String, Vec<(f64, f64)>>,
    ) -> Vec<BacktestOrder> {
        self.books.insert(token_id.to_string(), book.clone());
        let Some(contract) = self.universe_by_token.get(token_id).cloned() else {
            return Vec::new();
        };
        let cid = contract.market.condition_id.clone();
        if self.traded.contains(&cid) {
            return Vec::new();
        }

        let close = chrono::DateTime::parse_from_rfc3339(&contract.end_date)
            .ok()
            .map(|d| d.timestamp() as f64)
            .unwrap_or(0.0);
        let window_minutes =
            crate::live::window::estimate_window_minutes(&contract.window_description);
        if window_minutes <= 0.0 {
            self.monitor.record_signal_skip(&cid, "window_parse_failed");
            return Vec::new();
        }
        let minutes_remaining = (close - timestamp_s) / 60.0;
        if minutes_remaining <= 0.083 || minutes_remaining > 30.0 {
            return Vec::new();
        }
        let minutes_elapsed = window_minutes - minutes_remaining;
        if minutes_elapsed < 0.5 {
            return Vec::new();
        }

        let asset_price = self.btc_history.price_at_seconds(timestamp_s);
        if asset_price <= 0.0 {
            return Vec::new();
        }
        if timestamp_s - self.last_tick_ts_s >= 1.0 {
            self.momentum.add_tick(asset_price, Some(timestamp_s));
            self.last_tick_ts_s = timestamp_s;
        }
        if self.momentum.get_open_price(&cid).is_none() {
            let open_ts_s = close - window_minutes * 60.0;
            let open_btc = self.btc_history.price_at_seconds(open_ts_s);
            if open_btc <= 0.0 {
                return Vec::new();
            }
            self.momentum.set_window_open(&cid, open_btc);
        }
        let Some(signal) = self.momentum.detect(
            &cid,
            minutes_elapsed,
            minutes_remaining,
            asset_price,
            Some(timestamp_s),
        ) else {
            return Vec::new();
        };

        let up_price = self.fresh_ask(&contract.up_token_id, timestamp_s, contract.up_price);
        let down_price = self.fresh_ask(&contract.down_token_id, timestamp_s, contract.down_price);
        let implied_vol = self
            .btc_history
            .realized_vol_at((timestamp_s * 1000.0) as i64, 3600.0);
        let variant = &self.replay_strategy.variant;
        let decision = match decide_candle_trade(
            &signal,
            minutes_elapsed,
            minutes_remaining,
            window_minutes,
            up_price,
            down_price,
            asset_price,
            signal.open_price,
            implied_vol,
            variant.min_confidence,
            variant.min_edge,
            variant.skip_dead_zone,
            &variant.zone_config,
            0.0,
        ) {
            DecisionResult::Trade(decision) => decision,
            DecisionResult::Skip(skip) => {
                self.record_skip(
                    timestamp_s,
                    &contract,
                    &signal,
                    up_price,
                    down_price,
                    implied_vol,
                    skip.zone,
                    skip.reason,
                    skip.detail,
                );
                return Vec::new();
            }
        };

        let traded_token = if decision.direction == "up" {
            contract.up_token_id.clone()
        } else {
            contract.down_token_id.clone()
        };
        let Some(traded_book) = self
            .books
            .get(&traded_token)
            .or_else(|| self.books.get(token_id))
        else {
            return Vec::new();
        };
        let micro = replay_microstructure(traded_book);
        if let Err(skip) = micro.check_long_entry(&variant.microstructure) {
            self.record_skip(
                timestamp_s,
                &contract,
                &signal,
                up_price,
                down_price,
                implied_vol,
                decision.zone,
                skip.reason,
                skip.detail,
            );
            return Vec::new();
        }

        let order = self.build_order(
            timestamp_s,
            &contract,
            &signal,
            &decision,
            &traded_token,
            up_price,
            down_price,
            implied_vol,
            &micro,
        );
        self.orders_submitted += 1;
        self.traded.insert(cid);
        vec![order]
    }
}

impl LiveReplayStrategy {
    fn build_order(
        &mut self,
        timestamp_s: f64,
        contract: &CandleContract,
        signal: &crate::strategy::momentum::MomentumSignal,
        decision: &CandleDecision,
        traded_token: &str,
        up_price: f64,
        down_price: f64,
        implied_vol: f64,
        micro: &BookMicrostructure,
    ) -> BacktestOrder {
        let variant = &self.replay_strategy.variant;
        let position = (self.bankroll_usd * variant.position_pct).min(variant.max_per_market_usd);
        let size = (position / decision.market_price).round().max(1.0);
        let order_signal = Signal::from_candle_decision(
            contract.market.condition_id.clone(),
            traded_token.to_string(),
            decision,
            serde_json::json!({
                "mode": "live_replay",
                "zone": decision.zone,
                "market_price": decision.market_price,
                "timestamp_s": timestamp_s,
            }),
        );
        let order_type = if variant.prefer_maker {
            "limit"
        } else {
            "market"
        };
        let limit_price = variant.prefer_maker.then_some(decision.market_price);
        let intent = OrderIntent::deterministic(
            self.replay_strategy.strategy_spec.clone(),
            &order_signal,
            "buy",
            order_type,
            limit_price,
            size,
            "live_replay_candle_momentum_decision",
            format!(
                "{}:{timestamp_s:.6}:{traded_token}",
                contract.market.condition_id
            ),
        );
        let order_id = replay_order_id(&intent.intent_id);
        let close_ts_s = chrono::DateTime::parse_from_rfc3339(&contract.end_date)
            .ok()
            .map(|d| d.timestamp() as f64)
            .unwrap_or(0.0);
        self.replay_positions.insert(
            intent.intent_id.clone(),
            ReplayPosition {
                condition_id: contract.market.condition_id.clone(),
                direction: decision.direction.clone(),
                open_btc: signal.open_price,
                close_ts_s,
            },
        );
        let _ = self
            .order_manager
            .create_intent(intent.clone(), timestamp_s);
        let _ = self
            .order_manager
            .risk_accept(&intent.intent_id, timestamp_s);
        let _ = self
            .order_manager
            .submit(&intent.intent_id, Some(order_id.clone()), timestamp_s);
        self.monitor.record_signal_evaluation(&SignalEvaluation {
            ts_ms: (timestamp_s * 1000.0) as i64,
            cid: short_cid(&contract.market.condition_id),
            asset: contract.asset.clone(),
            open: signal.open_price,
            px: signal.current_price,
            chg: signal.price_change,
            chg_pct: signal.price_change_pct,
            cons: signal.consistency,
            z: decision.z_score,
            conf: decision.confidence,
            elapsed_min: signal.minutes_elapsed,
            remaining_min: decision.minutes_remaining,
            dir: decision.direction.clone(),
            vol_fast: implied_vol,
            vol_slow: implied_vol,
            implied_vol,
            cross_boost: 0.0,
            up_price,
            down_price,
            book_spread: micro.spread,
            book_pressure: micro.pressure,
            book_bid_depth: micro.bid_depth,
            book_ask_depth: micro.ask_depth,
            zone: decision.zone.clone(),
            fair: decision.fair_value,
            edge: decision.edge,
            decision_trade: true,
            execution_attempted: true,
            traded: false,
            skip_reason: None,
            skip_detail: None,
        });
        self.monitor.record_order_placed(&OrderPlaced {
            intent_id: intent.intent_id.clone(),
            token_id: short_cid(traded_token),
            side: "BUY".to_string(),
            state: "submitted".to_string(),
            price: decision.market_price,
            live_price: decision.market_price,
            size,
            order_value: decision.market_price * size,
            order_id,
            book_best_ask: decision.market_price,
            book_ask_depth: 0.0,
            book_bid_depth: 0.0,
            balance_usd: self.bankroll_usd,
        });

        BacktestOrder {
            intent_id: intent.intent_id,
            timestamp_s,
            condition_id: contract.market.condition_id.clone(),
            token_id: traded_token.to_string(),
            side: "buy".to_string(),
            size,
            order_type: order_type.to_string(),
            limit_price,
            fee_rate: variant.default_fee_rate,
            maker_fee_rate: variant.maker_fee_rate,
        }
    }
}

fn paper_outcome_pnl(won: bool, entry_price: f64, size: f64, fee: f64) -> f64 {
    if won {
        (1.0 - entry_price) * size - fee
    } else {
        -entry_price * size - fee
    }
}

fn replay_outcome_prices(actual: &str) -> [f64; 2] {
    if actual == "up" {
        [1.0, 0.0]
    } else {
        [0.0, 1.0]
    }
}

fn replay_microstructure(book: &TokenBook) -> BookMicrostructure {
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
    BookMicrostructure::from_levels(&bids, &asks, 3)
}

fn replay_order_id(intent_id: &str) -> String {
    format!("replay-{}", short_cid(intent_id))
}

fn short_cid(s: &str) -> String {
    if s.len() <= 16 {
        s.to_string()
    } else {
        s[..16].to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn replay_strategy_from_settings_matches_live_defaults() {
        let settings = Settings::from_env();
        let replay = ReplayStrategy::from_settings(&settings);

        assert_eq!(replay.variant.min_confidence, DEFAULT_MIN_CONFIDENCE);
        assert_eq!(replay.variant.min_edge, DEFAULT_MIN_EDGE);
        assert_eq!(replay.variant.position_pct, settings.candle_position_pct);
        assert_eq!(
            replay.variant.max_per_market_usd,
            settings.max_position_per_market_usd
        );

        let tmp = TempDir::new().unwrap();
        let monitor = Arc::new(SessionMonitor::open(tmp.path()).unwrap());
        assert!(monitor.events_path().exists());
    }

    #[test]
    fn replay_strategy_loads_promotion_artifact() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("promotion.json");
        let variant = StrategyVariant::maker_first();
        let strategy_spec = StrategySpec::from_serializable_params(
            "candle_momentum",
            "1",
            &variant,
            format!(
                "position_pct={:.4};max_per_market_usd={:.2}",
                variant.position_pct, variant.max_per_market_usd
            ),
        );
        let artifact = crate::backtest::experiment::PromotionArtifact {
            schema_version: 1,
            created_at: "2026-05-06T00:00:00Z".to_string(),
            source_report_hash: "source".to_string(),
            source_label: "unit".to_string(),
            source_window: "2026-04-25T10:00:00Z..2026-04-25T10:00:00Z".to_string(),
            selected_strategy: strategy_spec.clone(),
            strategy_params: serde_json::to_value(&variant).unwrap(),
            data_manifest_hash: "manifest".to_string(),
            market_count: 1,
            trades: 30,
            win_rate: 0.7,
            total_pnl: 10.0,
            avg_pnl: 0.33,
            total_fees: 0.0,
            sharpe_like: 1.0,
            dominant_zone: None,
            dominant_zone_trade_share: None,
            risk_notes: Vec::new(),
            promotion_gate: crate::backtest::experiment::PromotionGate::default(),
        };
        std::fs::write(&path, serde_json::to_vec(&artifact).unwrap()).unwrap();

        let mut settings = Settings::from_env();
        settings.promotion_artifact_path = path.display().to_string();
        settings.candle_settlement_cutoff_minutes = 0.30;
        settings.candle_settlement_guard_minutes = 1.0;
        settings.candle_settlement_min_abs_move_usd = 10.0;
        settings.candle_settlement_sigma_buffer = 0.0;
        let replay = ReplayStrategy::load(&settings).unwrap();

        assert_eq!(replay.strategy_spec.params_hash, strategy_spec.params_hash);
        assert_eq!(replay.variant.name, "maker_first");
        assert!(replay.variant.prefer_maker);
        assert!(replay.source.starts_with("promotion:"));
    }

    #[test]
    fn replay_strategy_applies_same_settlement_safety_floor_as_live() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("promotion.json");
        let mut variant = StrategyVariant::maker_first();
        variant.zone_config.settlement_cutoff_minutes = 0.1;
        variant.zone_config.settlement_guard_minutes = 0.5;
        variant.zone_config.settlement_min_abs_move_usd = 2.0;
        variant.zone_config.settlement_sigma_buffer = 0.0;
        let strategy_spec = StrategySpec::from_serializable_params(
            "candle_momentum",
            "1",
            &variant,
            format!(
                "position_pct={:.4};max_per_market_usd={:.2}",
                variant.position_pct, variant.max_per_market_usd
            ),
        );
        let artifact = crate::backtest::experiment::PromotionArtifact {
            schema_version: 1,
            created_at: "2026-05-06T00:00:00Z".to_string(),
            source_report_hash: "source".to_string(),
            source_label: "unit".to_string(),
            source_window: "2026-04-25T10:00:00Z..2026-04-25T10:00:00Z".to_string(),
            selected_strategy: strategy_spec.clone(),
            strategy_params: serde_json::to_value(&variant).unwrap(),
            data_manifest_hash: "manifest".to_string(),
            market_count: 1,
            trades: 30,
            win_rate: 0.7,
            total_pnl: 10.0,
            avg_pnl: 0.33,
            total_fees: 0.0,
            sharpe_like: 1.0,
            dominant_zone: None,
            dominant_zone_trade_share: None,
            risk_notes: Vec::new(),
            promotion_gate: crate::backtest::experiment::PromotionGate::default(),
        };
        std::fs::write(&path, serde_json::to_vec(&artifact).unwrap()).unwrap();

        let mut settings = Settings::from_env();
        settings.promotion_artifact_path = path.display().to_string();
        settings.candle_settlement_cutoff_minutes = 1.5;
        settings.candle_settlement_guard_minutes = 5.0;
        settings.candle_settlement_min_abs_move_usd = 25.0;
        settings.candle_settlement_sigma_buffer = 0.2;
        let replay = ReplayStrategy::load(&settings).unwrap();

        assert_eq!(replay.variant.zone_config.settlement_cutoff_minutes, 1.5);
        assert_eq!(replay.variant.zone_config.settlement_guard_minutes, 5.0);
        assert_eq!(replay.variant.zone_config.settlement_min_abs_move_usd, 25.0);
        assert_eq!(replay.variant.zone_config.settlement_sigma_buffer, 0.2);
        assert_ne!(replay.strategy_spec.params_hash, strategy_spec.params_hash);
        assert!(replay.source.ends_with("+settlement_floor"));
    }
}
