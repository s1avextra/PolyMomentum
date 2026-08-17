//! Live (paper or live) candle trading pipeline.
//!
//! Translates `src/polymomentum/crypto/candle_pipeline.py::CandlePipeline`
//! to async Rust:
//!
//! - 8-exchange BTC + ETH/SOL spot WS aggregator (already in `exchange.rs`)
//! - Polymarket WS L2 books (already in `polymarket_ws.rs`)
//! - Gamma REST contract refresh (every 2 min)
//! - 10 Hz cycle loop: per-contract evaluation + decision
//! - Paper resolution loop (BTC tape vs window close)
//! - CTF oracle verification loop
//! - Risk + monitoring + breaker

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tokio::time::sleep;

use crate::backtest::fill_model::{
    ceil_buy_price_to_tick, resting_limit_price, Side, DEFAULT_TICK,
};
use crate::backtest::strategies::{SelectivityFilter, StrategyVariant};
use crate::clob::{create_shared_client, SharedClobClient, SubmitFailureKind};
use crate::clob_user_ws::{
    parse_rest_trade_events, polymarket_user_feed, UserChannelAuth, UserEvent,
};
use crate::config::{RuntimeMode, Settings};
use crate::data::ctf::{CtfReader, Resolution};
use crate::data::gamma::GammaClient;
use crate::data::models::{DEFAULT_CRYPTO_TAKER_FEE_RATE, DEFAULT_MAKER_FEE_RATE};
use crate::data::scanner::{scan_candle_markets, CandleContract};
use crate::execution::fees::polymarket_fee;
use crate::execution::order_manager::{ManagedOrder, OrderManager, OrderState};
use crate::execution::sizing::{buy_book_quote_from_budget, shares_from_budget, BuyBookQuote};
use crate::live::breaker::{BreakerConfig, BreakerState};
use crate::live::paper_fill::{simulate_paper_fill, PaperFillCfg};
use crate::live::window::{
    btc_updown_slug_step_seconds, btc_updown_slugs_for_live_horizon, estimate_window_minutes,
};
use crate::monitoring::alerter::Alerter;
use crate::monitoring::session::{
    OrderFilled, OrderReconciled, OrderTiming, ResolutionTiming, SessionMonitor, SignalEvaluation,
};
use crate::polymarket_ws::{
    new_shared_book, new_subscription_notify, polymarket_book_feed, SharedBookState,
};
use crate::price_state::PriceState;
use crate::release::ReleaseManifest;
use crate::risk::manager::{RiskConfig, RiskManager, TradeRecord};
use crate::strategy::decision::{
    decide_candle_trade_with_fee, DecisionResult, ZoneConfig, DEFAULT_MIN_CONFIDENCE,
    DEFAULT_MIN_EDGE,
};
use crate::strategy::microstructure::{
    apply_causal_dynamic_tick_transition, bookwalk_buy_slippage, recent_mid_runup, BookLevelView,
    BookMicrostructure, MicrostructureConfig,
};
use crate::strategy::momentum::{MomentumConfig, MomentumDetector};
use crate::strategy::spec::{stable_json_hash, OrderIntent, Signal, StrategySpec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Paper,
    Live,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::Paper => "paper",
            Mode::Live => "live",
        }
    }

    pub fn from_runtime_mode(mode: RuntimeMode) -> Self {
        match mode {
            RuntimeMode::Paper => Self::Paper,
            RuntimeMode::Live => Self::Live,
        }
    }

    pub fn runtime_mode(&self) -> RuntimeMode {
        match self {
            Self::Paper => RuntimeMode::Paper,
            Self::Live => RuntimeMode::Live,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaperPosition {
    direction: String,
    entry_price: f64,
    fee: f64,
    size: f64,
    open_btc: f64,
    end_time: f64,
    asset: String,
    contract_id: String,
    #[serde(default)]
    event_id: String,
    #[serde(default)]
    shadow: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingLivePosition {
    position: PaperPosition,
    entry_fee_rate: f64,
    #[serde(default)]
    recovery_misses: u32,
}

impl PendingLivePosition {
    fn fill_fee(&self, size: f64, price: f64) -> Option<f64> {
        if !size.is_finite()
            || size <= 0.0
            || !price.is_finite()
            || !(0.0..=1.0).contains(&price)
            || !self.entry_fee_rate.is_finite()
            || !(0.0..=1.0).contains(&self.entry_fee_rate)
        {
            return None;
        }
        Some(polymarket_fee(size, price, self.entry_fee_rate))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveOrderJournalEntry {
    order: ManagedOrder,
    pending: PendingLivePosition,
    #[serde(default)]
    reconciled_trade_ids: Vec<String>,
}

impl LiveOrderJournalEntry {
    fn validate(&self, intent_id: &str) -> Result<(), String> {
        if self.order.intent.intent_id != intent_id {
            return Err("journal key does not match order intent_id".to_string());
        }
        if self.order.intent.market_id != self.pending.position.contract_id {
            return Err("journal order market does not match pending contract".to_string());
        }
        let remaining = self.order.requested_size - self.order.filled_size;
        if !remaining.is_finite()
            || remaining <= 0.0
            || (remaining - self.pending.position.size).abs() > 1e-9
            || !self.pending.position.entry_price.is_finite()
            || !(0.0..=1.0).contains(&self.pending.position.entry_price)
            || !self.pending.position.open_btc.is_finite()
            || self.pending.position.open_btc <= 0.0
            || !self.pending.position.end_time.is_finite()
            || !self.pending.entry_fee_rate.is_finite()
            || !(0.0..=1.0).contains(&self.pending.entry_fee_rate)
        {
            return Err("journal pending economics are invalid".to_string());
        }
        Ok(())
    }
}

impl PaperPosition {
    fn to_json(&self) -> serde_json::Value {
        json!({
            "direction": self.direction,
            "entry_price": self.entry_price,
            "fee": self.fee,
            "size": self.size,
            "open_btc": self.open_btc,
            "end_time": self.end_time,
            "asset": self.asset,
            "contract_id": self.contract_id,
            "event_id": self.event_id,
            "shadow": self.shadow,
        })
    }

    fn from_json(cid: String, v: &serde_json::Value) -> Option<Self> {
        Some(Self {
            direction: v.get("direction")?.as_str()?.to_string(),
            entry_price: v.get("entry_price")?.as_f64()?,
            fee: v.get("fee").and_then(|x| x.as_f64()).unwrap_or(0.0),
            size: v.get("size")?.as_f64()?,
            open_btc: v.get("open_btc")?.as_f64()?,
            end_time: v.get("end_time")?.as_f64()?,
            asset: v
                .get("asset")
                .and_then(|x| x.as_str())
                .unwrap_or("BTC")
                .to_string(),
            contract_id: cid,
            event_id: v
                .get("event_id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            shadow: v.get("shadow").and_then(|x| x.as_bool()).unwrap_or(false),
        })
    }
}

fn outcome_idx_for_direction(direction: &str) -> i64 {
    if direction.eq_ignore_ascii_case("down") {
        1
    } else {
        0
    }
}

fn live_position_from_fill(
    template: &PaperPosition,
    order: &ManagedOrder,
) -> Option<PaperPosition> {
    if !(order.filled_size > 0.0 && order.avg_fill_price > 0.0) {
        return None;
    }
    let mut pos = template.clone();
    pos.entry_price = order.avg_fill_price;
    pos.size = order.filled_size;
    pos.fee = order.total_fees;
    Some(pos)
}

#[derive(Debug, Clone)]
struct OraclePending {
    our_actual: String,
    our_open_btc: f64,
    our_close_btc: f64,
    end_time: f64,
    attempts: u32,
    direction: Option<String>,
    entry_price: Option<f64>,
    fee: Option<f64>,
    size: Option<f64>,
    provisional_won: Option<bool>,
    provisional_pnl: Option<f64>,
    pnl_recorded: bool,
    shadow: bool,
}

impl OraclePending {
    fn to_json(&self) -> serde_json::Value {
        json!({
            "our_actual": self.our_actual,
            "our_open_btc": self.our_open_btc,
            "our_close_btc": self.our_close_btc,
            "end_time": self.end_time,
            "attempts": self.attempts,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "fee": self.fee,
            "size": self.size,
            "provisional_won": self.provisional_won,
            "provisional_pnl": self.provisional_pnl,
            "pnl_recorded": self.pnl_recorded,
            "shadow": self.shadow,
        })
    }

    fn from_json(v: &serde_json::Value) -> Option<Self> {
        let shadow = v.get("shadow").and_then(|x| x.as_bool()).unwrap_or(false);
        let pnl_recorded = v
            .get("pnl_recorded")
            .and_then(|x| x.as_bool())
            .unwrap_or_else(|| !shadow && v.get("provisional_pnl").is_some());
        Some(Self {
            our_actual: v.get("our_actual")?.as_str()?.to_string(),
            our_open_btc: v.get("our_open_btc")?.as_f64()?,
            our_close_btc: v.get("our_close_btc")?.as_f64()?,
            end_time: v.get("end_time")?.as_f64()?,
            attempts: v.get("attempts").and_then(|x| x.as_u64()).unwrap_or(0) as u32,
            direction: v
                .get("direction")
                .and_then(|x| x.as_str())
                .map(ToString::to_string),
            entry_price: v.get("entry_price").and_then(|x| x.as_f64()),
            fee: v.get("fee").and_then(|x| x.as_f64()),
            size: v.get("size").and_then(|x| x.as_f64()),
            provisional_won: v.get("provisional_won").and_then(|x| x.as_bool()),
            provisional_pnl: v.get("provisional_pnl").and_then(|x| x.as_f64()),
            pnl_recorded,
            shadow,
        })
    }

    fn oracle_pnl(&self, polymarket_actual: &str) -> Option<(bool, f64, bool, f64)> {
        let direction = self.direction.as_deref()?;
        let entry_price = self.entry_price?;
        let size = self.size?;
        let fee = self.fee.unwrap_or(0.0);
        let provisional_won = self.provisional_won?;
        let provisional_pnl = self.provisional_pnl?;
        let (final_won, final_pnl) = match polymarket_actual {
            "up" | "down" => {
                let won = polymarket_actual == direction;
                (won, paper_outcome_pnl(won, entry_price, size, fee))
            }
            "tie" => (false, (0.5 - entry_price) * size - fee),
            _ => return None,
        };
        Some((final_won, final_pnl, provisional_won, provisional_pnl))
    }
}

fn paper_outcome_pnl(won: bool, entry_price: f64, size: f64, fee: f64) -> f64 {
    if won {
        (1.0 - entry_price) * size - fee
    } else {
        -entry_price * size - fee
    }
}

fn paper_position_exposure(pos: &PaperPosition) -> f64 {
    (pos.entry_price * pos.size + pos.fee).max(0.0)
}

fn pending_resolution_exposure(entry: &OraclePending) -> f64 {
    if entry.pnl_recorded {
        return 0.0;
    }
    match (entry.entry_price, entry.size) {
        (Some(entry_price), Some(size)) if entry_price > 0.0 && size > 0.0 => {
            (entry_price * size + entry.fee.unwrap_or(0.0)).max(0.0)
        }
        _ => 0.0,
    }
}

fn pending_requires_realization(entry: &OraclePending) -> bool {
    if entry.pnl_recorded {
        return false;
    }
    if entry.shadow {
        return entry.entry_price.is_some()
            && entry.size.unwrap_or(0.0) > 0.0
            && entry.provisional_won.is_some()
            && entry.provisional_pnl.is_some();
    }
    true
}

fn permanent_live_order_reject_reason(message: &str) -> Option<&'static str> {
    let msg = message.to_ascii_lowercase();
    if msg.contains("not enough balance")
        || msg.contains("balance is not enough")
        || msg.contains("insufficient balance")
        || msg.contains("allowance")
    {
        Some("live_balance_allowance_reject")
    } else if msg.contains("post-only") && (msg.contains("cross") || msg.contains("match")) {
        Some("live_post_only_cross_reject")
    } else if msg.contains("marketable") && msg.contains("min size") {
        Some("live_marketable_min_size_reject")
    } else if msg.contains("invalid price") || msg.contains("tick") {
        Some("live_order_shape_reject")
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RestOrderSnapshot {
    status: String,
    size_matched: f64,
}

impl RestOrderSnapshot {
    fn is_terminal_without_more_fills(&self) -> bool {
        matches!(
            self.status.to_ascii_uppercase().as_str(),
            "CANCELED" | "CANCELLED" | "INVALID" | "CANCELED_MARKET_RESOLVED"
        )
    }
}

fn parse_rest_order_snapshot(value: &serde_json::Value) -> Result<RestOrderSnapshot, String> {
    let order = value.get("data").unwrap_or(value);
    let status = order
        .get("status")
        .and_then(|raw| raw.as_str())
        .unwrap_or_default()
        .trim()
        .to_string();
    if status.is_empty() {
        return Err("REST order response has no status".to_string());
    }
    let size_matched = order
        .get("size_matched")
        .or_else(|| order.get("sizeMatched"))
        .and_then(json_number)
        .unwrap_or(0.0);
    if !size_matched.is_finite() || size_matched < 0.0 {
        return Err("REST order size_matched is invalid".to_string());
    }
    Ok(RestOrderSnapshot {
        status,
        size_matched,
    })
}

fn json_number(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse::<f64>().ok()))
}

fn is_rest_not_found(error: &str) -> bool {
    error.starts_with("HTTP 404:") || error.starts_with("HTTP 404 ")
}

#[derive(Debug, Clone)]
struct RuntimeStrategy {
    strategy_spec: StrategySpec,
    zone_config: ZoneConfig,
    skip_dead_zone: bool,
    min_confidence: f64,
    min_edge: f64,
    decision_volatility_floor: f64,
    position_pct: f64,
    max_per_market_usd: f64,
    max_projected_stressed_drawdown_pct: f64,
    degraded_after_losses: u64,
    degraded_after_drawdown_pct: f64,
    degraded_min_z: f64,
    degraded_max_price: f64,
    degraded_force_taker: bool,
    prefer_maker: bool,
    default_fee_rate: f64,
    maker_fee_rate: f64,
    microstructure: MicrostructureConfig,
    selectivity: SelectivityFilter,
    source: String,
}

impl RuntimeStrategy {
    fn load(settings: &Settings) -> Result<Self> {
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
        if !variant.exit.is_disabled() {
            bail!("promoted strategy enables an exit lifecycle that live runtime does not yet implement");
        }
        let params_hash = stable_json_hash(&variant);
        if params_hash != artifact.selected_strategy.params_hash {
            bail!(
                "promotion artifact hash mismatch: strategy_params hash {} != selected_strategy hash {}",
                params_hash,
                artifact.selected_strategy.params_hash
            );
        }
        let zone_before = variant.zone_config;
        let zone_floor_applied = variant.zone_config.apply_settings_safety_floor(settings);
        let settlement_floor_applied = settlement_fields_changed(zone_before, variant.zone_config);
        let decision_floor_applied =
            zone_floor_applied && !zones_equal_except_settlement(zone_before, variant.zone_config);
        let runtime_floor_applied = apply_runtime_variant_safety_floor(&mut variant, settings);
        let mut strategy_spec = artifact.selected_strategy;
        let mut source = format!("promotion:{path}");
        if zone_floor_applied || runtime_floor_applied {
            strategy_spec = StrategySpec::from_serializable_params(
                strategy_spec.name.clone(),
                strategy_spec.version.clone(),
                &variant,
                format!(
                    "{};runtime_floor conf>={:.2},z>={:.2},edge>={:.2},ev>={:.2},price=[{:.2},{:.2}],micro_spread<={:.3},micro_depth>={:.2},micro_pressure>={:.2},settlement cutoff_min={:.2},guard_min={:.2},min_abs_usd={:.2},sigma_buffer={:.2}",
                    strategy_spec.risk_profile,
                    variant.min_confidence,
                    variant.zone_config.primary_min_z,
                    variant.min_edge,
                    variant.zone_config.min_ev_buffer,
                    variant.zone_config.min_price,
                    variant.zone_config.max_price,
                    variant.microstructure.max_spread,
                    variant.microstructure.min_book_depth,
                    variant.microstructure.min_book_pressure,
                    variant.zone_config.settlement_cutoff_minutes,
                    variant.zone_config.settlement_guard_minutes,
                    variant.zone_config.settlement_min_abs_move_usd,
                    variant.zone_config.settlement_sigma_buffer,
                ),
            );
            if settlement_floor_applied {
                source = format!("{source}+settlement_floor");
            }
            if decision_floor_applied || runtime_floor_applied {
                source = format!("{source}+runtime_floor");
            }
        }
        Ok(Self {
            strategy_spec,
            zone_config: variant.zone_config,
            skip_dead_zone: variant.skip_dead_zone,
            min_confidence: variant.min_confidence,
            min_edge: variant.min_edge,
            decision_volatility_floor: variant.decision_volatility_floor,
            position_pct: variant.position_pct,
            max_per_market_usd: variant.max_per_market_usd,
            max_projected_stressed_drawdown_pct: variant.max_projected_stressed_drawdown_pct,
            degraded_after_losses: variant.degraded_after_losses,
            degraded_after_drawdown_pct: variant.degraded_after_drawdown_pct,
            degraded_min_z: variant.degraded_min_z,
            degraded_max_price: variant.degraded_max_price,
            degraded_force_taker: variant.degraded_force_taker,
            prefer_maker: variant.prefer_maker,
            default_fee_rate: variant.default_fee_rate,
            maker_fee_rate: variant.maker_fee_rate,
            microstructure: variant.microstructure,
            selectivity: variant.selectivity,
            source,
        })
    }

    fn from_settings(settings: &Settings) -> Self {
        let zone_config = ZoneConfig::from_settings(settings);
        let mut microstructure = MicrostructureConfig::disabled();
        microstructure.apply_safety_floor(
            settings.candle_microstructure_max_spread,
            settings.candle_microstructure_min_book_depth,
            settings.candle_microstructure_min_book_pressure,
        );
        let params = json!({
            "zone_config": zone_config,
            "skip_dead_zone": settings.candle_skip_dead_zone,
            "min_confidence": DEFAULT_MIN_CONFIDENCE,
            "min_edge": DEFAULT_MIN_EDGE,
            "decision_volatility_floor": 0.0,
            "position_pct": settings.candle_position_pct,
            "max_per_market_usd": settings.max_position_per_market_usd,
            "max_projected_stressed_drawdown_pct": settings.candle_max_projected_stressed_drawdown_pct,
            "degraded_after_losses": 0,
            "degraded_after_drawdown_pct": 0.0,
            "degraded_min_z": 0.0,
            "degraded_max_price": 0.0,
            "degraded_force_taker": false,
            "prefer_maker": settings.candle_prefer_maker,
            "default_fee_rate": DEFAULT_CRYPTO_TAKER_FEE_RATE,
            "maker_fee_rate": DEFAULT_MAKER_FEE_RATE,
            "microstructure": microstructure,
        });
        Self {
            strategy_spec: StrategySpec::from_serializable_params(
                "candle_momentum",
                "1",
                &params,
                format!(
                    "position_pct={:.4};max_per_market_usd={:.2};stress_dd_cap={:.4}",
                    settings.candle_position_pct,
                    settings.max_position_per_market_usd,
                    settings.candle_max_projected_stressed_drawdown_pct
                ),
            ),
            zone_config,
            skip_dead_zone: settings.candle_skip_dead_zone,
            min_confidence: DEFAULT_MIN_CONFIDENCE,
            min_edge: DEFAULT_MIN_EDGE,
            decision_volatility_floor: 0.0,
            position_pct: settings.candle_position_pct,
            max_per_market_usd: settings.max_position_per_market_usd,
            max_projected_stressed_drawdown_pct: settings
                .candle_max_projected_stressed_drawdown_pct,
            degraded_after_losses: 0,
            degraded_after_drawdown_pct: 0.0,
            degraded_min_z: 0.0,
            degraded_max_price: 0.0,
            degraded_force_taker: false,
            prefer_maker: settings.candle_prefer_maker,
            default_fee_rate: DEFAULT_CRYPTO_TAKER_FEE_RATE,
            maker_fee_rate: DEFAULT_MAKER_FEE_RATE,
            microstructure,
            selectivity: SelectivityFilter::default(),
            source: "settings".to_string(),
        }
    }

    fn degraded_execution_active(&self, losses: u64, realized_drawdown_pct: f64) -> bool {
        self.degraded_after_losses > 0
            && losses >= self.degraded_after_losses
            && realized_drawdown_pct.is_finite()
            && realized_drawdown_pct >= self.degraded_after_drawdown_pct.max(0.0)
    }

    fn effective_zone_config(&self, losses: u64, realized_drawdown_pct: f64) -> ZoneConfig {
        let mut cfg = self.zone_config;
        if self.degraded_execution_active(losses, realized_drawdown_pct)
            && self.degraded_min_z.is_finite()
            && self.degraded_min_z > 0.0
        {
            cfg.early_min_z = cfg.early_min_z.max(self.degraded_min_z);
            cfg.primary_min_z = cfg.primary_min_z.max(self.degraded_min_z);
            cfg.late_min_z = cfg.late_min_z.max(self.degraded_min_z);
            cfg.terminal_min_z = cfg.terminal_min_z.max(self.degraded_min_z);
        }
        if self.degraded_execution_active(losses, realized_drawdown_pct)
            && self.degraded_max_price.is_finite()
            && self.degraded_max_price > 0.0
        {
            cfg.max_price = cfg.max_price.min(self.degraded_max_price);
            if cfg.min_price > cfg.max_price {
                cfg.min_price = cfg.max_price;
            }
        }
        cfg
    }

    fn effective_prefer_maker(&self, losses: u64, realized_drawdown_pct: f64) -> bool {
        if self.degraded_force_taker
            && self.degraded_execution_active(losses, realized_drawdown_pct)
        {
            false
        } else {
            self.prefer_maker
        }
    }

    fn decision_volatility(&self, observed_volatility: f64) -> f64 {
        crate::backtest::strategies::decision_volatility_with_floor(
            observed_volatility,
            self.decision_volatility_floor,
        )
    }
}

fn apply_runtime_variant_safety_floor(variant: &mut StrategyVariant, settings: &Settings) -> bool {
    let mut changed = false;
    if settings.candle_runtime_min_confidence_floor.is_finite()
        && variant.min_confidence < settings.candle_runtime_min_confidence_floor
    {
        variant.min_confidence = settings.candle_runtime_min_confidence_floor;
        changed = true;
    }
    if settings.candle_runtime_min_edge_floor.is_finite()
        && variant.min_edge < settings.candle_runtime_min_edge_floor
    {
        variant.min_edge = settings.candle_runtime_min_edge_floor;
        changed = true;
    }
    changed |= variant.microstructure.apply_safety_floor(
        settings.candle_microstructure_max_spread,
        settings.candle_microstructure_min_book_depth,
        settings.candle_microstructure_min_book_pressure,
    );
    changed
}

fn settlement_fields_changed(before: ZoneConfig, after: ZoneConfig) -> bool {
    before.settlement_cutoff_minutes != after.settlement_cutoff_minutes
        || before.settlement_guard_minutes != after.settlement_guard_minutes
        || before.settlement_min_abs_move_usd != after.settlement_min_abs_move_usd
        || before.settlement_sigma_buffer != after.settlement_sigma_buffer
}

fn zones_equal_except_settlement(before: ZoneConfig, after: ZoneConfig) -> bool {
    let mut before = before;
    before.settlement_cutoff_minutes = after.settlement_cutoff_minutes;
    before.settlement_guard_minutes = after.settlement_guard_minutes;
    before.settlement_min_abs_move_usd = after.settlement_min_abs_move_usd;
    before.settlement_sigma_buffer = after.settlement_sigma_buffer;
    before.early_min_confidence == after.early_min_confidence
        && before.early_min_z == after.early_min_z
        && before.early_min_edge == after.early_min_edge
        && before.primary_min_z == after.primary_min_z
        && before.late_min_confidence == after.late_min_confidence
        && before.late_min_z == after.late_min_z
        && before.late_min_edge == after.late_min_edge
        && before.terminal_min_confidence == after.terminal_min_confidence
        && before.terminal_min_z == after.terminal_min_z
        && before.terminal_min_edge == after.terminal_min_edge
        && before.dead_zone_lo == after.dead_zone_lo
        && before.dead_zone_hi == after.dead_zone_hi
        && before.min_price == after.min_price
        && before.max_price == after.max_price
        && before.edge_cap == after.edge_cap
        && before.min_ev_buffer == after.min_ev_buffer
        && before.min_reversion_count == after.min_reversion_count
        && before.max_reversion_count == after.max_reversion_count
}

pub struct Pipeline {
    settings: Settings,
    mode: Mode,
    release_manifest: ReleaseManifest,
    runtime_strategy: RuntimeStrategy,
    risk: RiskManager,
    order_manager: Mutex<OrderManager>,
    clob: Option<SharedClobClient>,
    monitor: Arc<SessionMonitor>,
    alerter: Alerter,
    gamma: GammaClient,
    ctf: CtfReader,
    breaker_cfg: BreakerConfig,
    momentum: Mutex<HashMap<String, MomentumDetector>>,
    contracts: RwLock<Vec<CandleContract>>,
    traded: Mutex<HashSet<String>>,
    live_pending_positions: Mutex<HashMap<String, PendingLivePosition>>,
    paper_positions: Mutex<HashMap<String, PaperPosition>>,
    oracle_pending: Mutex<HashMap<String, OraclePending>>,
    breaker: Mutex<BreakerState>,
    /// Cumulative realized live PnL from PRIOR sessions (meta key
    /// `live_cumulative_realized_pnl`). Survives the bankroll-actualization
    /// breaker reset so live loss streaks accumulate across restarts;
    /// current-session PnL lives in the breaker state and is added on check.
    live_loss_ledger_prior: f64,
    breaker_tripped: Mutex<bool>,
    breaker_trip_reason: Mutex<Option<String>>,
    breaker_tripped_at_s: Mutex<Option<i64>>,
    price_state: Arc<RwLock<PriceState>>,
    book_state: SharedBookState,
    tracked_tokens: Arc<RwLock<Vec<String>>>,
    resub_notify: Arc<Notify>,
    tracked_markets: Arc<RwLock<Vec<String>>>,
    user_resub_notify: Arc<Notify>,
    reconciled_trade_ids: Mutex<HashSet<String>>,
    live_recovery_ready: Mutex<bool>,
    stop: Arc<Notify>,
    kill_switch_path: PathBuf,
    cycle_count: Mutex<u64>,
}

impl Pipeline {
    pub async fn new(settings: Settings, mode: Mode) -> Result<Arc<Self>> {
        let release_manifest = ReleaseManifest::capture(&settings, mode.runtime_mode());
        let runtime_strategy = RuntimeStrategy::load(&settings)?;
        let bankroll = if matches!(mode, Mode::Paper) {
            settings.simulated_bankroll_usd()
        } else if settings.bankroll_usd > 0.0 {
            settings.bankroll_usd
        } else {
            // Fall back to wallet detection if private key set
            try_wallet_bankroll(&settings).await.unwrap_or(0.0)
        };
        let risk_cfg = RiskConfig {
            initial_bankroll: bankroll,
            max_total_exposure_override: settings.max_total_exposure_usd,
            max_per_market_override: runtime_strategy.max_per_market_usd,
            actualize_on_open: matches!(mode, Mode::Live),
            ..Default::default()
        };
        let risk = RiskManager::open(&settings.state_db_path, risk_cfg).await?;
        if matches!(mode, Mode::Paper) && settings.candle_simulated_balance_reset_on_start {
            risk.reset_simulated_session(bankroll).await?;
            tracing::info!(bankroll, "simulated paper/shadow state reset on startup");
        }

        let monitor = Arc::new(SessionMonitor::open(&settings.session_log_dir)?);
        let alerter = Alerter::from_env();
        let gamma = GammaClient::new(&settings.poly_gamma_url);
        let ctf = CtfReader::new(&settings.polygon_rpc_url);
        let breaker_cfg = BreakerConfig::from_settings(&settings);

        // Restore breaker + paper positions + oracle pending
        let mut breaker_tripped = matches!(
            risk.get_meta("candle_breaker_tripped").await?.as_deref(),
            Some("1")
        );
        let mut breaker_trip_reason = risk.get_meta("candle_breaker_reason").await?;
        let mut breaker_tripped_at_s = risk
            .get_meta("candle_breaker_tripped_at")
            .await?
            .and_then(|raw| raw.parse::<i64>().ok());
        let mut breaker_state = match risk.get_meta("candle_breaker_state").await? {
            Some(raw) => match serde_json::from_str::<BreakerState>(&raw) {
                Ok(state) => state,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to restore candle breaker metrics");
                    BreakerState::default()
                }
            },
            None => BreakerState::default(),
        };
        let mut paper_positions = HashMap::new();
        for (cid, payload) in risk.load_paper_positions().await.unwrap_or_default() {
            if let Some(pp) = PaperPosition::from_json(cid.clone(), &payload) {
                paper_positions.insert(cid, pp);
            }
        }
        let mut oracle_pending = HashMap::new();
        for (cid, payload) in risk.load_oracle_pending().await.unwrap_or_default() {
            if let Some(op) = OraclePending::from_json(&payload) {
                oracle_pending.insert(cid, op);
            }
        }
        if !paper_positions.is_empty() {
            tracing::info!(n = paper_positions.len(), "restored paper positions");
        }
        if !oracle_pending.is_empty() {
            tracing::info!(n = oracle_pending.len(), "restored oracle-pending");
        }
        let raw_live_journal = risk
            .load_live_pending_orders()
            .await
            .context("load live pending-order journal")?;
        if matches!(mode, Mode::Paper) && !raw_live_journal.is_empty() {
            bail!(
                "paper startup blocked: {} unresolved live order(s) require authenticated live recovery",
                raw_live_journal.len()
            );
        }
        let mut order_manager = OrderManager::new();
        let mut live_pending_positions = HashMap::new();
        let mut reconciled_trade_ids = HashSet::new();
        let mut traded = HashSet::new();
        for (intent_id, payload) in raw_live_journal {
            let entry: LiveOrderJournalEntry = serde_json::from_value(payload)
                .with_context(|| format!("decode live order journal {intent_id}"))?;
            entry
                .validate(&intent_id)
                .map_err(anyhow::Error::msg)
                .with_context(|| format!("validate live order journal {intent_id}"))?;
            order_manager
                .restore(entry.order)
                .map_err(anyhow::Error::msg)
                .with_context(|| format!("restore live order {intent_id}"))?;
            traded.insert(entry.pending.position.contract_id.clone());
            reconciled_trade_ids.extend(entry.reconciled_trade_ids);
            live_pending_positions.insert(intent_id, entry.pending);
        }
        if !live_pending_positions.is_empty() {
            tracing::warn!(
                n = live_pending_positions.len(),
                "restored unresolved live orders; new trading locked pending REST recovery"
            );
        }
        if risk.actualizes_on_open().await
            && (breaker_state.wins > 0
                || breaker_state.losses > 0
                || breaker_state.realized_pnl.abs() > 1e-9
                || breaker_state.peak_pnl.abs() > 1e-9
                || breaker_tripped)
        {
            tracing::info!(
                wins = breaker_state.wins,
                losses = breaker_state.losses,
                pnl = breaker_state.realized_pnl,
                "resetting breaker session after bankroll actualization"
            );
            // Fold the finished session's realized PnL into the cross-restart
            // live loss ledger BEFORE the reset wipes it. The ledger meta key
            // is deliberately absent from the delete list below.
            if mode == Mode::Live && breaker_state.realized_pnl.abs() > 1e-9 {
                let prior: f64 = risk
                    .get_meta("live_cumulative_realized_pnl")
                    .await?
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let updated = prior + breaker_state.realized_pnl;
                risk.set_meta("live_cumulative_realized_pnl", &format!("{updated:.6}"))
                    .await?;
                tracing::info!(
                    prior,
                    session = breaker_state.realized_pnl,
                    cumulative = updated,
                    "live loss ledger updated across restart"
                );
            }
            for key in [
                "candle_breaker_tripped",
                "candle_breaker_state",
                "candle_breaker_reason",
                "candle_breaker_tripped_at",
            ] {
                if let Err(e) = risk.delete_meta(key).await {
                    tracing::warn!(error = %e, key, "delete candle breaker key failed");
                }
            }
            breaker_tripped = false;
            breaker_state = BreakerState::default();
            breaker_trip_reason = None;
            breaker_tripped_at_s = None;
            monitor.record_breaker_state(
                "session_actualized",
                "bankroll_actualized_on_restart",
                0,
                0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            );
        }
        if should_reset_paper_breaker_on_start(
            mode,
            settings.candle_paper_breaker_reset_on_start,
            breaker_tripped,
            breaker_state,
            paper_positions.is_empty(),
            oracle_pending.is_empty(),
        ) {
            tracing::warn!(
                wins = breaker_state.wins,
                losses = breaker_state.losses,
                pnl = breaker_state.realized_pnl,
                "resetting paper breaker state on startup"
            );
            if let Err(e) = risk.delete_meta("candle_breaker_tripped").await {
                tracing::warn!(error = %e, "delete candle breaker flag failed");
            }
            if let Err(e) = risk.delete_meta("candle_breaker_state").await {
                tracing::warn!(error = %e, "delete candle breaker state failed");
            }
            if let Err(e) = risk.delete_meta("candle_breaker_reason").await {
                tracing::warn!(error = %e, "delete candle breaker reason failed");
            }
            if let Err(e) = risk.delete_meta("candle_breaker_tripped_at").await {
                tracing::warn!(error = %e, "delete candle breaker timestamp failed");
            }
            breaker_tripped = false;
            breaker_state = BreakerState::default();
            breaker_trip_reason = None;
            breaker_tripped_at_s = None;
            monitor.record_breaker_state(
                "paper_reset_on_start",
                "configured_session_reset",
                0,
                0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            );
        }
        let restored_open_exposure: f64 = paper_positions
            .values()
            .map(paper_position_exposure)
            .sum::<f64>()
            + oracle_pending
                .values()
                .map(pending_resolution_exposure)
                .sum::<f64>()
            + live_pending_positions
                .values()
                .map(|pending| paper_position_exposure(&pending.position))
                .sum::<f64>();

        // Cross-restart live loss ledger: prior-session cumulative realized
        // PnL. Checked together with current-session breaker PnL so live
        // losses cannot be laundered by restarting the process.
        let live_loss_ledger_prior: f64 = risk
            .get_meta("live_cumulative_realized_pnl")
            .await?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        if mode == Mode::Live {
            let cap_pct = settings.candle_live_max_cumulative_loss_pct;
            let bankroll_floor = risk.initial_bankroll().await.max(1.0);
            if cap_pct > 0.0 && live_loss_ledger_prior <= -cap_pct * bankroll_floor {
                bail!(
                    "live start blocked: cumulative live loss ledger {live_loss_ledger_prior:.2} \
                     breaches -{:.0}% of bankroll {bankroll_floor:.2}; clear \
                     live_cumulative_realized_pnl meta only after an explicit post-mortem",
                    cap_pct * 100.0,
                );
            }
        }

        let mut momentum_map = HashMap::new();
        let mom_cfg = MomentumConfig {
            noise_z_threshold: settings.candle_noise_z_threshold,
            ..Default::default()
        };
        momentum_map.insert("BTC".to_string(), MomentumDetector::new(None, mom_cfg));

        if matches!(mode, Mode::Live) && crate::signing::CLOB_ORDER_SIGNING_VERSION != 2 {
            bail!(
                "live CLOB order placement blocked: compiled signer is V{} but live mode requires CLOB V2 signing",
                crate::signing::CLOB_ORDER_SIGNING_VERSION
            );
        }
        if matches!(mode, Mode::Live) && !settings.clob_v2_ready {
            bail!(
                "live CLOB order placement blocked: set CLOB_V2_READY=1 only after V2 signing and reconciliation are verified"
            );
        }
        if matches!(mode, Mode::Live) && !settings.live_reconciliation_ready {
            bail!(
                "live CLOB order placement blocked: set POLYMOMENTUM_LIVE_RECONCILIATION_READY=1 only after user-channel/REST reconciliation is verified"
            );
        }

        // Initialize CLOB client only in live mode and only if API creds present.
        let clob = if matches!(mode, Mode::Live)
            && !settings.poly_api_key.is_empty()
            && !settings.private_key.is_empty()
        {
            let client = create_shared_client(
                &settings.poly_base_url,
                &settings.poly_api_key,
                &settings.poly_api_secret,
                &settings.poly_api_passphrase,
            )
            .map_err(anyhow::Error::msg)?;
            client
                .write()
                .await
                .set_signing_key(&settings.private_key)
                .map_err(anyhow::Error::msg)?;
            client.write().await.warm_connection().await;
            tracing::info!("CLOB direct order placement ENABLED (live mode)");
            Some(client)
        } else {
            None
        };
        if matches!(mode, Mode::Live) && !live_pending_positions.is_empty() && clob.is_none() {
            bail!("live startup blocked: unresolved live orders require CLOB L2 credentials");
        }

        let live_recovery_ready = live_pending_positions.is_empty();

        let p = Arc::new(Self {
            kill_switch_path: PathBuf::from(&settings.kill_switch_path),
            settings,
            mode,
            release_manifest,
            runtime_strategy,
            risk,
            order_manager: Mutex::new(order_manager),
            clob,
            monitor,
            alerter,
            gamma,
            ctf,
            breaker_cfg,
            momentum: Mutex::new(momentum_map),
            contracts: RwLock::new(Vec::new()),
            traded: Mutex::new(traded),
            live_pending_positions: Mutex::new(live_pending_positions),
            paper_positions: Mutex::new(paper_positions),
            oracle_pending: Mutex::new(oracle_pending),
            breaker: Mutex::new(breaker_state),
            live_loss_ledger_prior,
            breaker_tripped: Mutex::new(breaker_tripped),
            breaker_trip_reason: Mutex::new(breaker_trip_reason),
            breaker_tripped_at_s: Mutex::new(breaker_tripped_at_s),
            price_state: Arc::new(RwLock::new(PriceState::new())),
            book_state: new_shared_book(),
            tracked_tokens: Arc::new(RwLock::new(Vec::new())),
            resub_notify: new_subscription_notify(),
            tracked_markets: Arc::new(RwLock::new(Vec::new())),
            user_resub_notify: new_subscription_notify(),
            reconciled_trade_ids: Mutex::new(reconciled_trade_ids),
            live_recovery_ready: Mutex::new(live_recovery_ready),
            stop: Arc::new(Notify::new()),
            cycle_count: Mutex::new(0),
        });
        if breaker_tripped && !p.maybe_rearm_paper_breaker().await {
            let metrics = breaker_state.metrics(
                restored_open_exposure,
                p.risk.initial_bankroll().await.max(1.0),
            );
            p.monitor.record_breaker_state(
                "restored_tripped",
                "state_db",
                breaker_state.wins,
                breaker_state.losses,
                breaker_state.realized_pnl,
                breaker_state.peak_pnl,
                metrics.open_exposure,
                metrics.stressed_pnl,
                metrics.realized_drawdown,
                metrics.realized_drawdown_pct,
                metrics.stressed_drawdown,
                metrics.stressed_drawdown_pct,
            );
        }

        Ok(p)
    }

    /// Hand back the cancellation token so a signal handler can request shutdown.
    pub fn stop_token(&self) -> Arc<Notify> {
        self.stop.clone()
    }

    pub async fn run(self: &Arc<Self>) -> Result<()> {
        self.monitor.record_release_manifest(&self.release_manifest);
        self.monitor.record_runtime_strategy(
            &self.runtime_strategy.source,
            &self.runtime_strategy.strategy_spec,
            &self.runtime_strategy.zone_config,
            self.runtime_strategy.min_confidence,
            self.runtime_strategy.min_edge,
            self.runtime_strategy.skip_dead_zone,
            &self.runtime_strategy.microstructure,
            &self.runtime_strategy.selectivity,
            self.settings.candle_settlement_alignment_ready,
        );
        tracing::info!(
            mode = self.mode.as_str(),
            venue = self.release_manifest.venue.as_str(),
            git_sha = self.release_manifest.git_sha,
            config_hash = self.release_manifest.config_hash,
            strategy_source = %self.runtime_strategy.source,
            strategy_hash = %self.runtime_strategy.strategy_spec.params_hash,
            "candle.start"
        );
        if self.alerter.enabled() {
            let _ = self
                .alerter
                .send(
                    "info",
                    "PolyMomentum Rust starting",
                    &format!("mode={}", self.mode.as_str()),
                )
                .await;
        }

        // Spawn exchange feeds (BTC: binance/bybit/okx; ETH+SOL: alts; Deribit IV)
        spawn_exchange_feeds(self.price_state.clone());

        // Polymarket WS book feed
        {
            let bs = self.book_state.clone();
            let tt = self.tracked_tokens.clone();
            let nt = self.resub_notify.clone();
            tokio::spawn(async move {
                polymarket_book_feed(bs, tt, nt).await;
            });
        }
        if matches!(self.mode, Mode::Live) && self.settings.live_reconciliation_ready {
            let auth = UserChannelAuth::new(
                self.settings.poly_api_key.clone(),
                self.settings.poly_api_secret.clone(),
                self.settings.poly_api_passphrase.clone(),
            );
            let markets = self.tracked_markets.clone();
            let notify = self.user_resub_notify.clone();
            let (tx, mut rx) = mpsc::channel(1024);
            tokio::spawn(async move {
                polymarket_user_feed(auth, markets, notify, tx).await;
            });
            let p = self.clone();
            tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    if let Err(e) = p.handle_user_event(event).await {
                        tracing::warn!(error = %e, "CLOB user event reconciliation failed");
                    }
                }
            });
        }
        if let Some(clob) = self.clob.clone() {
            let recovery_pipeline = self.clone();
            tokio::spawn(async move {
                recovery_pipeline.live_recovery_loop().await;
            });
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_secs(5)).await;
                    match clob.read().await.post_heartbeat().await {
                        Ok(_) => tracing::debug!("CLOB heartbeat acknowledged"),
                        Err(e) => tracing::warn!(error = %e, "CLOB heartbeat failed"),
                    }
                }
            });
        }

        // First contract refresh
        if let Err(e) = self.refresh_contracts().await {
            tracing::warn!(error = %e, "initial contract refresh failed");
        }

        // Wait for first BTC price. This must remain cancellable; diagnostics
        // runs often expose feed/network problems before the first tick.
        let wait_started = Instant::now();
        loop {
            if self.price_state.read().await.mid_price > 0.0 {
                break;
            }
            if wait_started.elapsed() > Duration::from_secs(30) {
                let msg = "no BTC price within 30s startup timeout";
                self.monitor.record_error("startup_price_wait", msg, false);
                anyhow::bail!(msg);
            }
            let stop = self.stop.clone();
            tokio::select! {
                _ = stop.notified() => {
                    tracing::info!("startup price wait interrupted");
                    if let Err(e) = self.monitor.save_summary() {
                        tracing::warn!(error = %e, "save summary failed");
                    }
                    return Ok(());
                }
                _ = sleep(Duration::from_millis(100)) => {}
            }
        }

        let scan = {
            let p = self.clone();
            tokio::spawn(async move { p.scan_loop().await })
        };
        let refresh = {
            let p = self.clone();
            tokio::spawn(async move { p.contract_refresh_loop().await })
        };
        let resolve = {
            let p = self.clone();
            tokio::spawn(async move { p.paper_resolution_loop().await })
        };
        let oracle = {
            let p = self.clone();
            tokio::spawn(async move { p.oracle_verification_loop().await })
        };
        let monitor = {
            let p = self.clone();
            tokio::spawn(async move { p.monitoring_loop().await })
        };

        let stop = self.stop.clone();
        stop.notified().await;
        scan.abort();
        refresh.abort();
        resolve.abort();
        oracle.abort();
        monitor.abort();

        if let Err(e) = self.monitor.save_summary() {
            tracing::warn!(error = %e, "save summary failed");
        }
        if self.alerter.enabled() {
            let bs = self.breaker.lock().await;
            let _ = self
                .alerter
                .send(
                    "warning",
                    "PolyMomentum Rust stopped",
                    &format!(
                        "wins={} losses={} pnl=${:.2}",
                        bs.wins, bs.losses, bs.realized_pnl
                    ),
                )
                .await;
        }
        Ok(())
    }

    async fn live_recovery_loop(self: Arc<Self>) {
        loop {
            match self.reconcile_live_orders_once().await {
                Ok(()) => {
                    let was_locked = {
                        let mut ready = self.live_recovery_ready.lock().await;
                        let was_locked = !*ready;
                        *ready = true;
                        was_locked
                    };
                    if was_locked {
                        tracing::info!("authenticated live-order recovery lock released");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "authenticated live-order recovery pass failed");
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    }

    async fn reconcile_live_orders_once(&self) -> Result<()> {
        let Some(clob) = self.clob.clone() else {
            return Ok(());
        };
        let orders: Vec<ManagedOrder> = {
            let manager = self.order_manager.lock().await;
            self.live_pending_positions
                .lock()
                .await
                .keys()
                .map(|intent_id| {
                    manager.get(intent_id).cloned().ok_or_else(|| {
                        anyhow::anyhow!("pending order {intent_id} has no lifecycle")
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };

        for order in orders {
            let venue_order_id = order
                .venue_order_id
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("live order has no venue lookup id"))?;
            let after = (order.created_ts.floor() as i64 - 1).max(0).to_string();
            let trades_json = {
                let client = clob.read().await;
                client
                    .get_trades(&[
                        ("market", order.intent.market_id.as_str()),
                        ("after", after.as_str()),
                    ])
                    .await
                    .map_err(anyhow::Error::msg)?
            };
            for trade in parse_rest_trade_events(&trades_json).map_err(anyhow::Error::msg)? {
                self.handle_user_event_from(UserEvent::Trade(trade), "clob_rest")
                    .await?;
            }

            if !self
                .live_pending_positions
                .lock()
                .await
                .contains_key(&order.intent.intent_id)
            {
                continue;
            }

            let order_json = {
                let client = clob.read().await;
                client.get_order(venue_order_id).await
            };
            match order_json {
                Ok(value) => {
                    let snapshot = parse_rest_order_snapshot(&value).map_err(anyhow::Error::msg)?;
                    let current = self
                        .order_manager
                        .lock()
                        .await
                        .get(&order.intent.intent_id)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("recovered order lifecycle disappeared"))?;
                    if snapshot.size_matched - current.filled_size > 1e-9
                        && snapshot.is_terminal_without_more_fills()
                    {
                        tracing::error!(
                            intent_id = %order.intent.intent_id,
                            venue_order_id = %short_cid(venue_order_id),
                            venue_size_matched = snapshot.size_matched,
                            confirmed_size = current.filled_size,
                            "REST terminal order is missing confirmed trade evidence"
                        );
                        self.trip_breaker("live_rest_missing_confirmed_trade").await;
                        self.stop.notify_one();
                        bail!("terminal REST order has unmatched confirmed exposure");
                    }
                    if snapshot.is_terminal_without_more_fills() {
                        {
                            let mut manager = self.order_manager.lock().await;
                            manager
                                .cancel(&order.intent.intent_id, nonzero_ts_or_now(0.0))
                                .map_err(anyhow::Error::msg)?;
                        }
                        self.live_pending_positions
                            .lock()
                            .await
                            .remove(&order.intent.intent_id);
                        self.require_live_journal("REST terminal order reconciliation")
                            .await?;
                        self.monitor.record_order_reconciled(&OrderReconciled {
                            intent_id: order.intent.intent_id.clone(),
                            order_id: venue_order_id.to_string(),
                            source: "clob_rest.order".to_string(),
                            venue_state: snapshot.status,
                            filled: current.filled_size,
                            requested: current.requested_size,
                            fill_price: current.avg_fill_price,
                            fee: current.total_fees,
                            detail: "terminal_without_unconfirmed_fill".to_string(),
                        });
                    } else {
                        let changed = {
                            let mut pending = self.live_pending_positions.lock().await;
                            pending
                                .get_mut(&order.intent.intent_id)
                                .is_some_and(|pending| {
                                    let changed = pending.recovery_misses != 0;
                                    pending.recovery_misses = 0;
                                    changed
                                })
                        };
                        if changed {
                            self.require_live_journal("REST order recovery reset")
                                .await?;
                        }
                    }
                }
                Err(error) if is_rest_not_found(&error) => {
                    self.handle_missing_rest_order(&order).await?;
                }
                Err(error) => return Err(anyhow::anyhow!(error)),
            }
        }
        Ok(())
    }

    async fn handle_missing_rest_order(&self, order: &ManagedOrder) -> Result<()> {
        let misses = {
            let mut pending = self.live_pending_positions.lock().await;
            let Some(pending) = pending.get_mut(&order.intent.intent_id) else {
                return Ok(());
            };
            pending.recovery_misses = pending.recovery_misses.saturating_add(1);
            pending.recovery_misses
        };
        self.require_live_journal("REST order-not-found recovery")
            .await?;
        let age_s = (nonzero_ts_or_now(0.0) - order.created_ts).max(0.0);
        if misses < 3 || age_s < 30.0 {
            return Ok(());
        }
        let current = self
            .order_manager
            .lock()
            .await
            .get(&order.intent.intent_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing REST order lifecycle disappeared"))?;
        if current.state != OrderState::Submitted || current.filled_size > 1e-9 {
            self.trip_breaker("live_rest_order_disappeared").await;
            self.stop.notify_one();
            bail!("acked or partially filled live order returned repeated REST 404");
        }
        {
            let mut manager = self.order_manager.lock().await;
            manager
                .reject(
                    &order.intent.intent_id,
                    "pre-submission journal entry not found by venue",
                    nonzero_ts_or_now(0.0),
                )
                .map_err(anyhow::Error::msg)?;
        }
        self.live_pending_positions
            .lock()
            .await
            .remove(&order.intent.intent_id);
        self.require_live_journal("abandoned pre-submit journal entry")
            .await?;
        tracing::warn!(
            intent_id = %order.intent.intent_id,
            order_id = ?order.venue_order_id,
            misses,
            age_s,
            "cleared signed order that was never accepted by venue"
        );
        Ok(())
    }

    async fn handle_user_event(&self, event: UserEvent) -> Result<()> {
        self.handle_user_event_from(event, "clob_user_ws").await
    }

    async fn handle_user_event_from(&self, event: UserEvent, source: &str) -> Result<()> {
        match event {
            UserEvent::Order(order) => {
                if order.id.is_empty() {
                    return Ok(());
                }
                let ts = nonzero_ts_or_now(order.timestamp_s());
                let (reconciled, canceled_intent_id, missing_fill_evidence) = {
                    let mut orders = self.order_manager.lock().await;
                    let intent_id = orders.intent_id_for_venue_order_id(&order.id);
                    let missing_fill_evidence = intent_id
                        .as_deref()
                        .and_then(|id| orders.get(id))
                        .is_some_and(|managed| {
                            order.is_canceled() && order.size_matched() - managed.filled_size > 1e-9
                        });
                    let res = if missing_fill_evidence {
                        Err(format!(
                            "venue cancellation reports {} matched but only {} confirmed locally",
                            order.size_matched(),
                            intent_id
                                .as_deref()
                                .and_then(|id| orders.get(id))
                                .map(|managed| managed.filled_size)
                                .unwrap_or(0.0)
                        ))
                    } else if order.is_canceled() {
                        orders.cancel_by_venue_order_id(&order.id, ts)
                    } else {
                        orders.reconcile_live_by_venue_order_id(&order.id, ts)
                    };
                    match res {
                        Ok(o) => (
                            Some(OrderReconciled {
                                intent_id: o.intent.intent_id.clone(),
                                order_id: order.id.clone(),
                                source: format!("{source}.order"),
                                venue_state: if order.is_canceled() {
                                    "canceled".to_string()
                                } else {
                                    order.status.clone()
                                },
                                filled: o.filled_size.max(order.size_matched()),
                                requested: o.requested_size.max(order.original_size()),
                                fill_price: order.price.parse::<f64>().unwrap_or(0.0),
                                fee: o.total_fees,
                                detail: order.event_kind.clone(),
                            }),
                            order.is_canceled().then(|| o.intent.intent_id.clone()),
                            false,
                        ),
                        Err(e) => {
                            tracing::debug!(order_id = %short_cid(&order.id), error = %e, "unmatched user-channel order event");
                            (None, None, missing_fill_evidence)
                        }
                    }
                };
                if missing_fill_evidence {
                    tracing::error!(
                        order_id = %short_cid(&order.id),
                        venue_size_matched = order.size_matched(),
                        "terminal order state arrived before confirmed trade evidence"
                    );
                    self.trip_breaker("live_terminal_order_missing_confirmed_trade")
                        .await;
                    self.stop.notify_one();
                    return Ok(());
                }
                if let Some(ref evt) = reconciled {
                    self.monitor.record_order_reconciled(evt);
                }
                if let Some(intent_id) = canceled_intent_id {
                    self.live_pending_positions.lock().await.remove(&intent_id);
                }
                if reconciled.is_some() {
                    self.require_live_journal("user order reconciliation")
                        .await?;
                }
            }
            UserEvent::Trade(trade) => {
                if trade.id.is_empty() {
                    return Ok(());
                }
                if trade.is_fill_status() && !trade.is_confirmed_fill() {
                    tracing::debug!(
                        trade_id = %trade.id,
                        status = %trade.status,
                        "user-channel trade awaiting terminal confirmation"
                    );
                    return Ok(());
                }
                if !trade.is_confirmed_fill() && !trade.is_failed() {
                    return Ok(());
                }
                {
                    let mut seen = self.reconciled_trade_ids.lock().await;
                    if !seen.insert(trade.id.clone()) {
                        return Ok(());
                    }
                }
                let ts = nonzero_ts_or_now(trade.timestamp_s());
                for fill in trade.candidate_order_fills() {
                    let intent_id = {
                        self.order_manager
                            .lock()
                            .await
                            .intent_id_for_venue_order_id(&fill.order_id)
                    };
                    let Some(intent_id) = intent_id else {
                        continue;
                    };
                    let fill_fee = if trade.is_failed() {
                        0.0
                    } else {
                        let fee = self
                            .live_pending_positions
                            .lock()
                            .await
                            .get(&intent_id)
                            .and_then(|pending| pending.fill_fee(fill.size, fill.price));
                        let Some(fee) = fee else {
                            tracing::error!(
                                intent_id = %intent_id,
                                order_id = %short_cid(&fill.order_id),
                                trade_id = %trade.id,
                                "live fill fee schedule unavailable or fill economics invalid"
                            );
                            self.trip_breaker("live_fill_fee_unavailable").await;
                            self.stop.notify_one();
                            return Ok(());
                        };
                        fee
                    };
                    let outcome = {
                        let mut orders = self.order_manager.lock().await;
                        if trade.is_failed() {
                            match orders.reject_by_venue_order_id(
                                &fill.order_id,
                                "clob trade failed",
                                ts,
                            ) {
                                Ok(o) => Some((o.clone(), false)),
                                Err(_) => None,
                            }
                        } else {
                            if let Err(error) =
                                orders.reconcile_live_by_venue_order_id(&fill.order_id, ts)
                            {
                                tracing::debug!(%error, "fill pre-ack reconciliation skipped");
                            }
                            match orders.fill_by_venue_order_id(
                                &fill.order_id,
                                fill.size,
                                fill.price,
                                fill_fee,
                                ts,
                            ) {
                                Ok(o) => Some((o.clone(), true)),
                                Err(_) => None,
                            }
                        }
                    };
                    let Some((order, filled)) = outcome else {
                        continue;
                    };
                    self.monitor.record_order_reconciled(&OrderReconciled {
                        intent_id: order.intent.intent_id.clone(),
                        order_id: fill.order_id.clone(),
                        source: format!("{source}.trade"),
                        venue_state: trade.status.clone(),
                        filled: order.filled_size,
                        requested: order.requested_size,
                        fill_price: fill.price,
                        fee: order.total_fees,
                        detail: trade.id.clone(),
                    });
                    if filled {
                        self.monitor.record_order_filled(&OrderFilled {
                            intent_id: order.intent.intent_id.clone(),
                            order_id: fill.order_id,
                            filled: fill.size,
                            requested: order.requested_size,
                            fill_pct: order.fill_pct(),
                            fill_price: fill.price,
                            cost: fill.size * fill.price,
                            limit_price: order.intent.limit_price.unwrap_or(fill.price),
                            slippage: 0.0,
                            slippage_bps: 0.0,
                            fill_time_s: (ts - order.created_ts).max(0.0),
                            fee: fill_fee,
                            n_trades: 1,
                        });
                        self.record_live_fill_position(&order, ts, fill.size, fill.price)
                            .await?;
                    } else {
                        self.live_pending_positions
                            .lock()
                            .await
                            .remove(&order.intent.intent_id);
                        self.require_live_journal("failed trade reconciliation")
                            .await?;
                        self.monitor.record_order_rejected(
                            &trade.asset_id,
                            "clob trade failed",
                            fill.price,
                            fill.size,
                        );
                        self.trip_breaker("live_trade_failed").await;
                        self.stop.notify_one();
                    }
                    return Ok(());
                }
                tracing::debug!(trade_id = %trade.id, "user-channel trade did not match a managed order");
            }
        }
        Ok(())
    }

    async fn record_live_fill_position(
        &self,
        order: &ManagedOrder,
        ts: f64,
        fill_size: f64,
        fill_price: f64,
    ) -> Result<()> {
        if !matches!(self.mode, Mode::Live) {
            return Ok(());
        }
        let template = {
            self.live_pending_positions
                .lock()
                .await
                .get(&order.intent.intent_id)
                .cloned()
        };
        let Some(pending) = template else {
            tracing::warn!(
                intent_id = %order.intent.intent_id,
                venue_order_id = ?order.venue_order_id,
                "live fill has no pending position metadata; cannot attach PnL lifecycle"
            );
            return Ok(());
        };
        let Some(position) = live_position_from_fill(&pending.position, order) else {
            return Ok(());
        };
        {
            let mut positions = self.paper_positions.lock().await;
            positions.insert(position.contract_id.clone(), position.clone());
        }
        self.persist_paper_positions().await;
        self.risk
            .record_trade(TradeRecord {
                timestamp: ts,
                market_condition_id: position.contract_id.clone(),
                outcome_idx: outcome_idx_for_direction(&position.direction),
                side: "buy".to_string(),
                size: fill_size,
                price: fill_price,
                cost: fill_size * fill_price,
                event_id: position.event_id.clone(),
                pnl: 0.0,
                paper: false,
            })
            .await?;
        let remaining_size = (order.requested_size - order.filled_size).max(0.0);
        if matches!(order.state, OrderState::Filled) || remaining_size <= 1e-9 {
            self.live_pending_positions
                .lock()
                .await
                .remove(&order.intent.intent_id);
        } else {
            let mut pending = self.live_pending_positions.lock().await;
            if let Some(p) = pending.get_mut(&order.intent.intent_id) {
                p.position.size = remaining_size;
            }
        }
        self.require_live_journal("confirmed fill reconciliation")
            .await?;
        tracing::info!(
            cid = short_cid(&position.contract_id),
            size = position.size,
            entry_price = position.entry_price,
            fee = position.fee,
            "live fill attached to resolution lifecycle"
        );
        Ok(())
    }

    pub async fn refresh_contracts(&self) -> Result<()> {
        let markets = self.fetch_live_contract_markets().await?;
        let mut contracts = scan_candle_markets(&markets, 1.0, 50.0);
        if self.settings.candle_window_minutes > 0.0 {
            let before = contracts.len();
            let target = self.settings.candle_window_minutes;
            contracts.retain(|c| {
                let minutes = estimate_window_minutes(&c.window_description);
                (minutes - target).abs() < 0.05
            });
            tracing::info!(
                target,
                kept = contracts.len(),
                before,
                "candle.window_filter"
            );
        }

        let active_cids: HashSet<String> = contracts
            .iter()
            .map(|c| c.market.condition_id.clone())
            .collect();
        {
            let mut traded = self.traded.lock().await;
            traded.retain(|c| active_cids.contains(c));
        }
        {
            let mut moms = self.momentum.lock().await;
            for det in moms.values_mut() {
                det.evict_stale_windows(&active_cids);
            }
        }

        // Update token subscriptions
        let token_ids: Vec<String> = contracts
            .iter()
            .flat_map(|c| {
                vec![c.up_token_id.clone(), c.down_token_id.clone()]
                    .into_iter()
                    .filter(|s| !s.is_empty())
            })
            .collect();
        {
            let mut tt = self.tracked_tokens.write().await;
            *tt = token_ids;
        }
        self.resub_notify.notify_one();
        let market_ids: Vec<String> = contracts
            .iter()
            .map(|c| c.market.condition_id.clone())
            .filter(|s| !s.is_empty())
            .collect();
        {
            let mut tm = self.tracked_markets.write().await;
            *tm = market_ids;
        }
        self.user_resub_notify.notify_one();

        let n = contracts.len();
        *self.contracts.write().await = contracts;
        tracing::info!(contracts = n, "candle.scan");
        Ok(())
    }

    async fn fetch_live_contract_markets(&self) -> Result<Vec<crate::data::models::Market>> {
        if !self.settings.candle_cross_asset_enabled {
            if let Some(step_s) = btc_updown_slug_step_seconds(self.settings.candle_window_minutes)
            {
                let slugs = btc_updown_slugs_for_live_horizon(Utc::now(), step_s, 45);
                tracing::info!(slugs = slugs.len(), step_s, "candle.slug_discovery");
                return self.gamma.fetch_markets_by_slugs(&slugs, false).await;
            }
        }

        self.gamma.fetch_markets_by_end_date(3.0, 0.0).await
    }

    async fn contract_refresh_loop(self: Arc<Self>) {
        loop {
            sleep(Duration::from_secs(120)).await;
            if let Err(e) = self.refresh_contracts().await {
                tracing::warn!(error = %e, "refresh failed");
                self.monitor
                    .record_error("contract_refresh", &e.to_string(), true);
            }
        }
    }

    async fn scan_loop(self: Arc<Self>) {
        let mut last_btc = 0.0;
        let mut unchanged = 0u32;
        let mut last_momentum_tick_ts_s = 0.0;
        loop {
            let cycle_start = Instant::now();
            {
                let mut c = self.cycle_count.lock().await;
                *c += 1;
            }

            let ps = self.price_state.read().await.clone();
            let btc = if self.settings.candle_settlement_alignment_ready {
                ps.fresh_source_price("chainlink_settlement", Duration::from_secs(10))
                    .unwrap_or(0.0)
            } else {
                ps.mid_price
            };
            if btc <= 0.0 {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            // Skip evaluation if BTC unchanged (with periodic forced refresh
            // every 10 cycles to catch zone transitions).
            if (btc - last_btc).abs() < 1e-9 {
                unchanged += 1;
                if unchanged < 10 {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                unchanged = 0;
            } else {
                unchanged = 0;
            }
            last_btc = btc;

            // Keep detector sampling aligned with harness/live-replay cadence.
            let momentum_tick_ts_s = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs_f64())
                .unwrap_or(0.0);
            let should_tick_momentum = momentum_tick_ts_s - last_momentum_tick_ts_s >= 1.0;
            if should_tick_momentum {
                let mut moms = self.momentum.lock().await;
                let det = moms.entry("BTC".to_string()).or_insert_with(|| {
                    MomentumDetector::new(
                        Some(ps.implied_vol),
                        MomentumConfig {
                            noise_z_threshold: self.settings.candle_noise_z_threshold,
                            ..Default::default()
                        },
                    )
                });
                det.add_tick(btc, Some(momentum_tick_ts_s));
                det.set_realized_vol(ps.implied_vol);
                last_momentum_tick_ts_s = momentum_tick_ts_s;
            }

            // Tick alts (ETH/SOL) if cross-asset is enabled — feed their WS
            // prices (`ps.alt_mid`) into per-asset momentum detectors.
            if should_tick_momentum && self.settings.candle_cross_asset_enabled {
                let mut moms = self.momentum.lock().await;
                for asset in ["ETH", "SOL"] {
                    if let Some(&alt_price) = ps.alt_mid.get(asset) {
                        if alt_price > 0.0 {
                            let det = moms.entry(asset.to_string()).or_insert_with(|| {
                                MomentumDetector::new(
                                    Some(ps.implied_vol),
                                    MomentumConfig {
                                        noise_z_threshold: self.settings.candle_noise_z_threshold,
                                        ..Default::default()
                                    },
                                )
                            });
                            det.add_tick(alt_price, Some(momentum_tick_ts_s));
                            det.set_realized_vol(ps.implied_vol);
                        }
                    }
                }
            }

            // Kill switch
            if self.kill_switch_active() {
                self.trip_breaker("kill_switch").await;
                self.stop.notify_one();
                return;
            }

            // Eager breaker check (every cycle)
            {
                let bs = *self.breaker.lock().await;
                let open_exposure = self.open_position_exposure().await;
                let breaker_bankroll = self.risk.initial_bankroll().await.max(1.0);
                if let Some(reason) =
                    self.breaker_trip_reason_for(&bs, open_exposure, breaker_bankroll)
                {
                    self.trip_breaker(reason).await;
                }
            }
            if *self.breaker_tripped.lock().await {
                if self.maybe_rearm_paper_breaker().await {
                    continue;
                }
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let now = Utc::now();
            let now_ts = now.timestamp() as f64;

            let books = self.book_state.read().await.clone();
            let contracts = self.contracts.read().await.clone();
            let mut traded_windows: HashSet<String> = HashSet::new();
            let traded_set = self.traded.lock().await.clone();

            for c in contracts.iter() {
                let cid = c.market.condition_id.clone();
                if traded_set.contains(&cid) {
                    continue;
                }

                let Ok(end) = parse_end(&c.end_date) else {
                    continue;
                };
                let minutes_left = (end - now).num_seconds() as f64 / 60.0;
                if minutes_left <= 0.083 || minutes_left > 30.0 {
                    continue;
                }
                if traded_windows.contains(&c.end_date) {
                    continue;
                }
                let window_minutes = estimate_window_minutes(&c.window_description);
                if window_minutes <= 0.0 {
                    self.monitor.record_signal_skip(&cid, "window_parse_failed");
                    continue;
                }
                let minutes_elapsed = (window_minutes - minutes_left).max(0.0);
                if minutes_elapsed < 0.5 {
                    continue;
                }

                let asset_price = if c.asset == "BTC" {
                    btc
                } else {
                    ps.alt_mid.get(&c.asset).copied().unwrap_or(0.0)
                };
                if asset_price <= 0.0 {
                    continue;
                }

                let Some((up_price, down_price)) = pick_book_prices(c, &books, now_ts) else {
                    self.monitor
                        .record_signal_skip(&cid, "fresh_outcome_book_unavailable");
                    continue;
                };

                // Detect momentum for the contract's own asset
                let (signal, observed_vol) = {
                    let mut moms = self.momentum.lock().await;
                    let det = moms.entry(c.asset.clone()).or_insert_with(|| {
                        MomentumDetector::new(
                            Some(ps.implied_vol),
                            MomentumConfig {
                                noise_z_threshold: self.settings.candle_noise_z_threshold,
                                ..Default::default()
                            },
                        )
                    });
                    if det.get_open_price(&cid).is_none() {
                        let open_ts = end.timestamp() as f64 - window_minutes * 60.0;
                        let open_price = if c.asset == "BTC"
                            && self.settings.candle_settlement_alignment_ready
                        {
                            ps.reference_price_near_seconds("chainlink_settlement", open_ts, 2.0)
                        } else {
                            ps.price_near_seconds(&c.asset, open_ts, 2.0)
                        };
                        if let Some(open_price) = open_price {
                            det.set_window_open(&cid, open_price);
                        }
                    }
                    let signal = det.detect(&cid, minutes_elapsed, minutes_left, asset_price, None);
                    if signal.is_none() && det.get_open_price(&cid).is_none() {
                        self.monitor
                            .record_signal_skip(&cid, "open_price_unavailable");
                    }
                    let observed_vol = det.rolling_realized_vol(3_600.0).unwrap_or(0.50);
                    (signal, observed_vol)
                };
                let Some(signal) = signal else { continue };
                let decision_vol = self.runtime_strategy.decision_volatility(observed_vol);

                let open_exposure = self.open_position_exposure().await;
                let breaker_state = *self.breaker.lock().await;
                let breaker_metrics = breaker_state
                    .metrics(open_exposure, self.risk.initial_bankroll().await.max(1.0));
                let effective_zone_config = self.runtime_strategy.effective_zone_config(
                    breaker_state.losses,
                    breaker_metrics.realized_drawdown_pct,
                );
                let prefer_maker = self.settings.live_allow_maker_orders
                    && self.runtime_strategy.effective_prefer_maker(
                        breaker_state.losses,
                        breaker_metrics.realized_drawdown_pct,
                    )
                    && crate::strategy::decision::zone_for(minutes_elapsed / window_minutes)
                        != "terminal";
                let entry_fee_rate = if prefer_maker {
                    c.market
                        .effective_maker_fee_rate(self.runtime_strategy.maker_fee_rate)
                } else {
                    c.market
                        .effective_taker_fee_rate(self.runtime_strategy.default_fee_rate)
                };
                let decision = decide_candle_trade_with_fee(
                    &signal,
                    minutes_elapsed,
                    minutes_left,
                    window_minutes,
                    up_price,
                    down_price,
                    asset_price,
                    signal.open_price,
                    decision_vol,
                    entry_fee_rate,
                    self.runtime_strategy.min_confidence,
                    self.runtime_strategy.min_edge,
                    self.runtime_strategy.skip_dead_zone,
                    &effective_zone_config,
                    0.0, // cross-asset boost not yet wired
                );
                let signal_token_id = if signal.direction == "up" {
                    &c.up_token_id
                } else {
                    &c.down_token_id
                };
                let signal_micro = live_microstructure(signal_token_id, &books, now_ts);

                let (vol_fast, vol_slow) = {
                    let moms = self.momentum.lock().await;
                    moms.get(&c.asset)
                        .map(|d| (d.realized_vol(), d.slow_realized_vol()))
                        .unwrap_or((ps.implied_vol, ps.implied_vol))
                };
                let eval_ts_ms = (SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0)) as i64;

                match decision {
                    DecisionResult::Skip(skip) => {
                        let aggregate = format!("{}_{}", skip.reason, skip.zone);
                        self.monitor.record_signal_skip(&cid, &aggregate);
                        self.monitor.record_signal_evaluation(&SignalEvaluation {
                            ts_ms: eval_ts_ms,
                            cid: short_cid(&cid),
                            asset: c.asset.clone(),
                            open: signal.open_price,
                            px: signal.current_price,
                            chg: signal.price_change,
                            chg_pct: signal.price_change_pct,
                            cons: signal.consistency,
                            z: signal.z_score,
                            conf: signal.confidence,
                            reversion_count: signal.reversion_count,
                            elapsed_min: signal.minutes_elapsed,
                            remaining_min: signal.minutes_remaining,
                            dir: signal.direction.clone(),
                            vol_fast,
                            vol_slow,
                            implied_vol: decision_vol,
                            cross_boost: 0.0,
                            up_price,
                            down_price,
                            book_spread: signal_micro.spread,
                            book_pressure: signal_micro.pressure,
                            book_bid_depth: signal_micro.bid_depth,
                            book_ask_depth: signal_micro.ask_depth,
                            zone: skip.zone.clone(),
                            fair: 0.0,
                            edge: 0.0,
                            decision_trade: false,
                            execution_attempted: false,
                            traded: false,
                            skip_reason: Some(skip.reason),
                            skip_detail: Some(skip.detail),
                        });
                    }
                    DecisionResult::Trade(mut decision) => {
                        let traded_token_id = if decision.direction == "up" {
                            &c.up_token_id
                        } else {
                            &c.down_token_id
                        };
                        let micro = live_microstructure(traded_token_id, &books, now_ts);
                        decision.regime.attach_time_inputs(now_ts);
                        decision.regime.attach_orderbook_inputs(
                            micro.best_bid,
                            micro.best_ask,
                            micro.spread,
                            micro.bid_depth,
                            micro.ask_depth,
                            micro.pressure,
                            micro.imbalance,
                        );
                        let mut estimated_position = self.risk.effective_bankroll().await
                            * self.runtime_strategy.position_pct;
                        let max_per_market = self.risk.max_per_market().await;
                        let available = self
                            .risk
                            .available_capital_for_exposure(open_exposure)
                            .await;
                        estimated_position = estimated_position.min(max_per_market).min(available);
                        if let Some(stress_headroom) = breaker_state
                            .stressed_drawdown_exposure_headroom(
                                open_exposure,
                                self.risk.initial_bankroll().await.max(1.0),
                                self.runtime_strategy.max_projected_stressed_drawdown_pct,
                            )
                        {
                            estimated_position = estimated_position.min(stress_headroom);
                        }
                        let market_tick = live_market_tick_size(
                            c.market.minimum_tick_size,
                            [&c.up_token_id, &c.down_token_id],
                            &books,
                        );
                        let taker_quote = (!prefer_maker && estimated_position >= 1.0)
                            .then(|| {
                                live_buy_book_quote(
                                    traded_token_id,
                                    &books,
                                    estimated_position,
                                    self.settings.live_min_order_size_shares,
                                    market_tick,
                                )
                            })
                            .flatten();
                        let mut execution_quote_skip = None;
                        if !prefer_maker {
                            match taker_quote {
                                Some(quote) => {
                                    if let Err(skip) = decision.reprice_for_taker_execution(
                                        quote.vwap,
                                        quote.worst_price,
                                        entry_fee_rate,
                                        self.runtime_strategy.min_edge,
                                        &effective_zone_config,
                                    ) {
                                        execution_quote_skip = Some((skip.reason, skip.detail));
                                    }
                                }
                                None => {
                                    execution_quote_skip = Some((
                                        "taker_visible_depth_unavailable".to_string(),
                                        "budget-aware visible L2 quote unavailable".to_string(),
                                    ));
                                }
                            }
                        }
                        let estimated_sizing_price = if prefer_maker {
                            resting_limit_price(
                                Side::Buy,
                                micro.best_bid,
                                micro.best_ask,
                                market_tick,
                            )
                        } else {
                            None
                        };
                        let estimated_size = taker_quote.map(|quote| quote.shares).or_else(|| {
                            estimated_sizing_price
                                .filter(|price| *price > 0.0 && estimated_position >= 1.0)
                                .and_then(|price| {
                                    shares_from_budget(
                                        estimated_position,
                                        price,
                                        self.settings.live_min_order_size_shares,
                                    )
                                })
                        });
                        decision.regime.attach_orderbook_quality_inputs(
                            taker_quote
                                .map(|quote| quote.slippage_per_share)
                                .or_else(|| {
                                    estimated_size.and_then(|size| {
                                        live_bookwalk_buy_slippage(traded_token_id, &books, size)
                                    })
                                }),
                            live_book_age_ms(traded_token_id, &books, now_ts),
                        );
                        let recent_runup = live_recent_mid_runup(
                            traded_token_id,
                            &books,
                            now_ts,
                            self.runtime_strategy
                                .microstructure
                                .recent_mid_lookback_seconds,
                        );
                        decision.regime.attach_orderbook_path_inputs(recent_runup);
                        if execution_quote_skip.is_none() {
                            if let Some(reason) = self
                                .runtime_strategy
                                .selectivity
                                .reject_reason(&decision.regime)
                            {
                                let aggregate = format!("{}_{}", reason, decision.zone);
                                self.monitor.record_signal_skip(&cid, &aggregate);
                                self.monitor.record_signal_evaluation(&SignalEvaluation {
                                    ts_ms: eval_ts_ms,
                                    cid: short_cid(&cid),
                                    asset: c.asset.clone(),
                                    open: signal.open_price,
                                    px: signal.current_price,
                                    chg: signal.price_change,
                                    chg_pct: signal.price_change_pct,
                                    cons: signal.consistency,
                                    z: signal.z_score,
                                    conf: signal.confidence,
                                    reversion_count: signal.reversion_count,
                                    elapsed_min: signal.minutes_elapsed,
                                    remaining_min: signal.minutes_remaining,
                                    dir: signal.direction.clone(),
                                    vol_fast,
                                    vol_slow,
                                    implied_vol: decision_vol,
                                    cross_boost: 0.0,
                                    up_price,
                                    down_price,
                                    book_spread: signal_micro.spread,
                                    book_pressure: signal_micro.pressure,
                                    book_bid_depth: signal_micro.bid_depth,
                                    book_ask_depth: signal_micro.ask_depth,
                                    zone: decision.zone.clone(),
                                    fair: decision.fair_value,
                                    edge: decision.edge,
                                    decision_trade: false,
                                    execution_attempted: false,
                                    traded: false,
                                    skip_reason: Some(reason),
                                    skip_detail: Some(
                                        "causal selectivity filter rejected the decision"
                                            .to_string(),
                                    ),
                                });
                                continue;
                            }
                        }
                        let execution_guard_skip = execution_quote_skip
                            .or_else(|| {
                                self.runtime_strategy
                                    .microstructure
                                    .check_recent_mid_path(recent_runup)
                                    .err()
                                    .map(|skip| (skip.reason, skip.detail))
                            })
                            .or_else(|| {
                                micro
                                    .check_long_entry(&self.runtime_strategy.microstructure)
                                    .err()
                                    .map(|skip| (skip.reason, skip.detail))
                            })
                            .or_else(|| {
                                (prefer_maker
                                    && resting_limit_price(
                                        Side::Buy,
                                        micro.best_bid,
                                        micro.best_ask,
                                        market_tick,
                                    )
                                    .is_none())
                                .then(|| {
                                    (
                                        "maker_invalid_book".to_string(),
                                        format!(
                                            "bid={:.4} ask={:.4}",
                                            micro.best_bid, micro.best_ask
                                        ),
                                    )
                                })
                            });
                        if let Some((reason, detail)) = execution_guard_skip {
                            let aggregate = format!("{}_{}", reason, decision.zone);
                            self.monitor.record_signal_skip(&cid, &aggregate);
                            self.monitor.record_signal_evaluation(&SignalEvaluation {
                                ts_ms: eval_ts_ms,
                                cid: short_cid(&cid),
                                asset: c.asset.clone(),
                                open: signal.open_price,
                                px: signal.current_price,
                                chg: signal.price_change,
                                chg_pct: signal.price_change_pct,
                                cons: signal.consistency,
                                z: signal.z_score,
                                conf: signal.confidence,
                                reversion_count: signal.reversion_count,
                                elapsed_min: signal.minutes_elapsed,
                                remaining_min: signal.minutes_remaining,
                                dir: signal.direction.clone(),
                                vol_fast,
                                vol_slow,
                                implied_vol: decision_vol,
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
                                execution_attempted: false,
                                traded: false,
                                skip_reason: Some(reason),
                                skip_detail: Some(detail),
                            });
                            continue;
                        }
                        if !self.settings.candle_settlement_alignment_ready {
                            let reason = "settlement_alignment_unverified".to_string();
                            let aggregate = format!("{}_{}", reason, decision.zone);
                            self.monitor.record_signal_skip(&cid, &aggregate);
                            self.monitor.record_signal_evaluation(&SignalEvaluation {
                                ts_ms: eval_ts_ms,
                                cid: short_cid(&cid),
                                asset: c.asset.clone(),
                                open: signal.open_price,
                                px: signal.current_price,
                                chg: signal.price_change,
                                chg_pct: signal.price_change_pct,
                                cons: signal.consistency,
                                z: signal.z_score,
                                conf: signal.confidence,
                                reversion_count: signal.reversion_count,
                                elapsed_min: signal.minutes_elapsed,
                                remaining_min: signal.minutes_remaining,
                                dir: signal.direction.clone(),
                                vol_fast,
                                vol_slow,
                                implied_vol: decision_vol,
                                cross_boost: 0.0,
                                up_price,
                                down_price,
                                book_spread: signal_micro.spread,
                                book_pressure: signal_micro.pressure,
                                book_bid_depth: signal_micro.bid_depth,
                                book_ask_depth: signal_micro.ask_depth,
                                zone: decision.zone.clone(),
                                fair: decision.fair_value,
                                edge: decision.edge,
                                decision_trade: false,
                                execution_attempted: false,
                                traded: false,
                                skip_reason: Some(reason),
                                skip_detail: Some(
                                    "CANDLE_SETTLEMENT_ALIGNMENT_READY=false".to_string(),
                                ),
                            });
                            let entry_price = if decision.direction == "up" {
                                up_price
                            } else {
                                down_price
                            };
                            let mut position = self.risk.effective_bankroll().await
                                * self.runtime_strategy.position_pct;
                            let max_per_market = self.risk.max_per_market().await;
                            let avail = self
                                .risk
                                .available_capital_for_exposure(open_exposure)
                                .await;
                            position = position.min(max_per_market).min(avail);
                            if let Some(stress_headroom) = breaker_state
                                .stressed_drawdown_exposure_headroom(
                                    open_exposure,
                                    self.risk.initial_bankroll().await.max(1.0),
                                    self.runtime_strategy.max_projected_stressed_drawdown_pct,
                                )
                            {
                                position = position.min(stress_headroom);
                            }
                            if position < 1.0 {
                                continue;
                            }
                            let taker_fee_rate = c
                                .market
                                .effective_taker_fee_rate(self.runtime_strategy.default_fee_rate);
                            let cfg = PaperFillCfg {
                                prefer_maker: self.runtime_strategy.effective_prefer_maker(
                                    breaker_state.losses,
                                    breaker_metrics.realized_drawdown_pct,
                                ),
                                default_taker_rate: taker_fee_rate,
                                min_order_size_shares: self.settings.live_min_order_size_shares,
                                ..Default::default()
                            };
                            let Some(fill) = simulate_paper_fill(entry_price, position, &cfg)
                            else {
                                continue;
                            };
                            let shadow_position = PaperPosition {
                                direction: decision.direction.clone(),
                                entry_price: fill.fill_price,
                                fee: fill.fee,
                                size: fill.shares,
                                open_btc: signal.open_price,
                                end_time: end.timestamp() as f64,
                                asset: c.asset.clone(),
                                contract_id: c.market.condition_id.clone(),
                                event_id: c.market.event_id.clone(),
                                shadow: true,
                            };
                            let inserted = {
                                let mut positions = self.paper_positions.lock().await;
                                if positions.contains_key(&cid) {
                                    false
                                } else {
                                    positions.insert(cid.clone(), shadow_position);
                                    true
                                }
                            };
                            if inserted {
                                self.traded.lock().await.insert(cid.clone());
                                self.persist_paper_positions().await;
                            }
                            continue;
                        }
                        traded_windows.insert(c.end_date.clone());
                        self.monitor.record_signal_evaluation(&SignalEvaluation {
                            ts_ms: eval_ts_ms,
                            cid: short_cid(&cid),
                            asset: c.asset.clone(),
                            open: signal.open_price,
                            px: signal.current_price,
                            chg: signal.price_change,
                            chg_pct: signal.price_change_pct,
                            cons: signal.consistency,
                            z: signal.z_score,
                            conf: signal.confidence,
                            reversion_count: signal.reversion_count,
                            elapsed_min: signal.minutes_elapsed,
                            remaining_min: signal.minutes_remaining,
                            dir: signal.direction.clone(),
                            vol_fast,
                            vol_slow,
                            implied_vol: decision_vol,
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
                        if let Err(e) = self
                            .execute_trade(c, &signal, &decision, &micro, taker_quote, market_tick)
                            .await
                        {
                            tracing::warn!(error = %e, "execute_trade failed");
                            self.monitor
                                .record_error("execute_trade", &e.to_string(), true);
                        }
                    }
                }
            }

            let cycle_ms = cycle_start.elapsed().as_secs_f64() * 1000.0;
            let cycle = *self.cycle_count.lock().await;
            if cycle % 30 == 0 {
                self.monitor.record_cycle(cycle, cycle_ms, contracts.len());
                let top = self.monitor.top_skip_reasons(5);
                tracing::info!(
                    cycle,
                    btc,
                    cycle_ms = cycle_ms,
                    contracts = contracts.len(),
                    top_skips = ?top,
                    "candle.cycle"
                );
            }

            let elapsed_ms = cycle_start.elapsed().as_millis() as u64;
            if elapsed_ms < 100 {
                sleep(Duration::from_millis(100 - elapsed_ms)).await;
            }
        }
    }

    async fn execute_trade(
        self: &Arc<Self>,
        contract: &CandleContract,
        signal: &crate::strategy::momentum::MomentumSignal,
        decision: &crate::strategy::decision::CandleDecision,
        micro: &BookMicrostructure,
        taker_quote: Option<BuyBookQuote>,
        market_tick: f64,
    ) -> Result<()> {
        let bankroll = self.risk.effective_bankroll().await;
        let mut position = bankroll * self.runtime_strategy.position_pct;

        let max_per_market = self.risk.max_per_market().await;
        let open_exposure = self.open_position_exposure().await;
        let avail = self
            .risk
            .available_capital_for_exposure(open_exposure)
            .await;
        position = position.min(max_per_market).min(avail);
        let breaker_state = *self.breaker.lock().await;
        let breaker_bankroll = self.risk.initial_bankroll().await.max(1.0);
        let breaker_metrics = breaker_state.metrics(open_exposure, breaker_bankroll);
        let prefer_maker = self.settings.live_allow_maker_orders
            && self.runtime_strategy.effective_prefer_maker(
                breaker_state.losses,
                breaker_metrics.realized_drawdown_pct,
            )
            && decision.zone != "terminal";
        let mut stress_capped = false;
        if let Some(stress_headroom) = breaker_state.stressed_drawdown_exposure_headroom(
            open_exposure,
            breaker_bankroll,
            self.runtime_strategy.max_projected_stressed_drawdown_pct,
        ) {
            position = position.min(stress_headroom);
            stress_capped = true;
        }
        if 0.0 < position && position < 1.0 && avail >= 1.0 && !stress_capped {
            position = 1.0;
        }
        if position < 1.0 {
            return Ok(());
        }

        let token_id = if signal.direction == "up" {
            &contract.up_token_id
        } else {
            &contract.down_token_id
        };
        let end_ts = parse_end(&contract.end_date)?.timestamp() as f64;
        let window_minutes =
            crate::live::window::estimate_window_minutes(&contract.window_description)
                .max(signal.minutes_elapsed + signal.minutes_remaining);
        let market_start_ts_s = end_ts - window_minutes * 60.0;
        let decision_ts_s = end_ts - signal.minutes_remaining * 60.0;
        let market_price = decision.market_price;

        match self.mode {
            Mode::Paper => {
                let taker_fee_rate = contract
                    .market
                    .effective_taker_fee_rate(self.runtime_strategy.default_fee_rate);
                let cfg = PaperFillCfg {
                    prefer_maker,
                    default_taker_rate: taker_fee_rate,
                    min_order_size_shares: self.settings.live_min_order_size_shares,
                    ..Default::default()
                };
                let Some(fill) = simulate_paper_fill(market_price, position, &cfg) else {
                    return Ok(());
                };
                let expected_edge_value =
                    fill.shares * (decision.fair_value - fill.fill_price) - fill.fee;
                let now_ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                let order_id = format!(
                    "paper-{}-{}",
                    short_cid(&contract.market.condition_id),
                    (now_ts * 1000.0) as u64
                );
                let order_signal = Signal::from_candle_decision(
                    contract.market.condition_id.clone(),
                    token_id.clone(),
                    decision,
                    serde_json::json!({
                        "mode": self.mode.as_str(),
                        "zone": decision.zone.clone(),
                        "market_price": market_price,
                    }),
                );
                let intent = OrderIntent::deterministic(
                    self.runtime_strategy.strategy_spec.clone(),
                    &order_signal,
                    "buy",
                    "market",
                    None,
                    fill.shares,
                    "paper_candle_momentum_decision",
                    format!("{}:{now_ts:.6}:{}", contract.market.condition_id, token_id),
                );
                let ack_state = {
                    let mut orders = self.order_manager.lock().await;
                    orders
                        .create_intent(intent.clone(), now_ts)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    orders
                        .risk_accept(&intent.intent_id, now_ts)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    orders
                        .submit(&intent.intent_id, Some(order_id.clone()), now_ts)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    let acked = orders
                        .ack(&intent.intent_id, Some(order_id.clone()), now_ts)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    let ack_state = acked.state.as_str().to_string();
                    orders
                        .fill(
                            &intent.intent_id,
                            fill.shares,
                            fill.fill_price,
                            fill.fee,
                            now_ts,
                        )
                        .map_err(|e| anyhow::anyhow!(e))?;
                    ack_state
                };
                let order_value = fill.fill_price * fill.shares;
                tracing::info!(
                    direction = %signal.direction,
                    cost = order_value,
                    fee = fill.fee,
                    expected_ev = expected_edge_value,
                    edge = decision.edge,
                    minutes_left = signal.minutes_remaining,
                    "candle.trade.paper"
                );
                self.monitor.record_order_timing(&OrderTiming {
                    intent_id: intent.intent_id.clone(),
                    condition_id: contract.market.condition_id.clone(),
                    token_id: token_id.clone(),
                    source: self.mode.as_str().to_string(),
                    signal_source_ts_s: decision_ts_s,
                    decision_ts_s,
                    order_ts_s: now_ts,
                    market_start_ts_s,
                    market_end_ts_s: end_ts,
                    latency_model_ms: Some(0.0),
                });
                self.monitor
                    .record_order_placed(&crate::monitoring::session::OrderPlaced {
                        intent_id: intent.intent_id.clone(),
                        token_id: short_cid(token_id),
                        side: "BUY".into(),
                        state: ack_state,
                        price: fill.fill_price,
                        live_price: market_price,
                        size: fill.shares,
                        order_value,
                        order_id: short_cid(&order_id),
                        book_best_ask: market_price,
                        book_ask_depth: 0.0,
                        book_bid_depth: 0.0,
                        balance_usd: bankroll,
                        submit_latency_ms: Some(0.0),
                    });
                self.monitor
                    .record_order_filled(&crate::monitoring::session::OrderFilled {
                        intent_id: intent.intent_id,
                        order_id: short_cid(&order_id),
                        filled: fill.shares,
                        requested: fill.shares,
                        fill_pct: 1.0,
                        fill_price: fill.fill_price,
                        cost: order_value,
                        limit_price: market_price,
                        slippage: fill.fill_price - market_price,
                        slippage_bps: if market_price > 0.0 {
                            (fill.fill_price - market_price) / market_price * 10_000.0
                        } else {
                            0.0
                        },
                        fill_time_s: 0.0,
                        fee: fill.fee,
                        n_trades: 1,
                    });
                self.traded
                    .lock()
                    .await
                    .insert(contract.market.condition_id.clone());
                self.risk
                    .record_trade(TradeRecord {
                        timestamp: now_ts,
                        market_condition_id: contract.market.condition_id.clone(),
                        outcome_idx: 0,
                        side: "buy".into(),
                        size: fill.shares,
                        price: fill.fill_price,
                        cost: order_value,
                        event_id: contract.market.event_id.clone(),
                        pnl: 0.0,
                        paper: true,
                    })
                    .await?;

                let pp = PaperPosition {
                    direction: signal.direction.clone(),
                    entry_price: fill.fill_price,
                    fee: fill.fee,
                    size: fill.shares,
                    open_btc: signal.open_price,
                    end_time: end_ts,
                    asset: contract.asset.clone(),
                    contract_id: contract.market.condition_id.clone(),
                    event_id: contract.market.event_id.clone(),
                    shadow: false,
                };
                self.paper_positions
                    .lock()
                    .await
                    .insert(contract.market.condition_id.clone(), pp);
                self.persist_paper_positions().await;
                Ok(())
            }
            Mode::Live => {
                if !*self.live_recovery_ready.lock().await {
                    tracing::warn!("live order skipped: authenticated recovery lock is active");
                    return Ok(());
                }
                let Some(clob) = self.clob.clone() else {
                    tracing::error!(
                        "live mode but no CLOB client (missing api keys / private key)"
                    );
                    return Ok(());
                };
                let min_order_size = self.settings.live_min_order_size_shares.max(0.0);
                let (limit_price, shares) = if prefer_maker {
                    let Some(price) =
                        resting_limit_price(Side::Buy, micro.best_bid, micro.best_ask, market_tick)
                    else {
                        tracing::warn!(
                            best_bid = micro.best_bid,
                            best_ask = micro.best_ask,
                            "live maker order skipped: invalid visible book"
                        );
                        return Ok(());
                    };
                    let Some(shares) = shares_from_budget(position, price, min_order_size) else {
                        tracing::warn!(
                            min_order_size,
                            limit_price = price,
                            position,
                            "live order skipped: below configured minimum order size"
                        );
                        return Ok(());
                    };
                    (price, shares)
                } else {
                    let Some(quote) = taker_quote else {
                        tracing::warn!("live taker order skipped: visible L2 quote unavailable");
                        return Ok(());
                    };
                    let limit_price = ceil_buy_price_to_tick(quote.worst_price, market_tick);
                    if limit_price * quote.shares > position + 1e-8 {
                        tracing::warn!(
                            limit_price,
                            shares = quote.shares,
                            position,
                            "live taker order skipped: FOK worst-case cost exceeds risk budget"
                        );
                        return Ok(());
                    }
                    (limit_price, quote.shares)
                };
                let neg_risk = contract.market.neg_risk;
                let order_signal = Signal::from_candle_decision(
                    contract.market.condition_id.clone(),
                    token_id.clone(),
                    decision,
                    serde_json::json!({
                        "mode": self.mode.as_str(),
                        "zone": decision.zone.clone(),
                        "market_price": market_price,
                    }),
                );
                let intent = OrderIntent::deterministic(
                    self.runtime_strategy.strategy_spec.clone(),
                    &order_signal,
                    "buy",
                    if prefer_maker { "limit" } else { "market" },
                    Some(limit_price),
                    shares,
                    "live_candle_momentum_decision",
                    format!(
                        "{}:{limit_price:.4}:{shares:.4}",
                        contract.market.condition_id
                    ),
                );
                let entry_fee_rate = if prefer_maker {
                    contract
                        .market
                        .effective_maker_fee_rate(self.runtime_strategy.maker_fee_rate)
                } else {
                    contract
                        .market
                        .effective_taker_fee_rate(self.runtime_strategy.default_fee_rate)
                };
                let pending_position = PendingLivePosition {
                    position: PaperPosition {
                        direction: signal.direction.clone(),
                        entry_price: limit_price,
                        fee: 0.0,
                        size: shares,
                        open_btc: signal.open_price,
                        end_time: end_ts,
                        asset: contract.asset.clone(),
                        contract_id: contract.market.condition_id.clone(),
                        event_id: contract.market.event_id.clone(),
                        shadow: false,
                    },
                    entry_fee_rate,
                    recovery_misses: 0,
                };

                let prepared = if prefer_maker {
                    clob.read().await.prepare_maker_order(
                        token_id,
                        limit_price,
                        shares,
                        "BUY",
                        neg_risk,
                        market_tick,
                    )
                } else {
                    clob.read().await.prepare_taker_order(
                        token_id,
                        limit_price,
                        shares,
                        "BUY",
                        neg_risk,
                        market_tick,
                    )
                }
                .map_err(anyhow::Error::msg)?;
                let expected_order_id = prepared.expected_order_id().to_string();

                let t_start = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                if !self
                    .traded
                    .lock()
                    .await
                    .insert(contract.market.condition_id.clone())
                {
                    return Ok(());
                }
                {
                    let mut orders = self.order_manager.lock().await;
                    orders
                        .create_intent(intent.clone(), t_start)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    orders
                        .risk_accept(&intent.intent_id, t_start)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    orders
                        .submit(&intent.intent_id, Some(expected_order_id.clone()), t_start)
                        .map_err(|e| anyhow::anyhow!(e))?;
                }
                self.live_pending_positions
                    .lock()
                    .await
                    .insert(intent.intent_id.clone(), pending_position);
                self.require_live_journal("pre-submit order journal")
                    .await?;
                self.monitor.record_order_timing(&OrderTiming {
                    intent_id: intent.intent_id.clone(),
                    condition_id: contract.market.condition_id.clone(),
                    token_id: token_id.clone(),
                    source: self.mode.as_str().to_string(),
                    signal_source_ts_s: decision_ts_s,
                    decision_ts_s,
                    order_ts_s: t_start,
                    market_start_ts_s,
                    market_end_ts_s: end_ts,
                    latency_model_ms: None,
                });
                let result = clob.write().await.submit_prepared_order(prepared).await;
                let submit_latency_s = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0)
                    - t_start;

                match result {
                    Ok(receipt) => {
                        let id_matches_expected = receipt.id_matches_expected();
                        let response_expected_order_id = receipt.expected_order_id.clone();
                        let order_id = receipt.order_id;
                        let ack_state = {
                            let mut orders = self.order_manager.lock().await;
                            let order = if id_matches_expected {
                                orders.reconcile_live_by_venue_order_id(
                                    &order_id,
                                    t_start + submit_latency_s,
                                )
                            } else {
                                orders.ack(
                                    &intent.intent_id,
                                    Some(order_id.clone()),
                                    t_start + submit_latency_s,
                                )
                            };
                            order
                                .map_err(|e| anyhow::anyhow!(e))?
                                .state
                                .as_str()
                                .to_string()
                        };
                        self.require_live_journal("order acknowledgement").await?;
                        let order_value = limit_price * shares;
                        self.monitor.record_order_placed(
                            &crate::monitoring::session::OrderPlaced {
                                intent_id: intent.intent_id,
                                token_id: short_cid(token_id),
                                side: "BUY".into(),
                                state: ack_state,
                                price: limit_price,
                                live_price: market_price,
                                size: shares,
                                order_value,
                                order_id: short_cid(&order_id),
                                book_best_ask: micro.best_ask,
                                book_ask_depth: micro.ask_depth,
                                book_bid_depth: micro.bid_depth,
                                balance_usd: self.risk.effective_bankroll().await,
                                submit_latency_ms: Some(submit_latency_s * 1000.0),
                            },
                        );
                        tracing::info!(
                            order_id = short_cid(&order_id),
                            cost = order_value,
                            submit_latency_s,
                            "candle.trade.live.accepted_unconfirmed"
                        );
                        if !id_matches_expected {
                            tracing::error!(
                                expected_order_id = %short_cid(&response_expected_order_id),
                                actual_order_id = %short_cid(&order_id),
                                "CLOB response order id differs from precomputed EIP-712 hash"
                            );
                            self.trip_breaker("live_order_hash_mismatch").await;
                            self.stop.notify_one();
                        }
                    }
                    Err(e) => {
                        let truncated = if e.message.len() > 200 {
                            &e.message[..200]
                        } else {
                            e.message.as_str()
                        };
                        if e.kind == SubmitFailureKind::DefinitiveReject {
                            {
                                let mut orders = self.order_manager.lock().await;
                                let _ = orders.reject(
                                    &intent.intent_id,
                                    truncated,
                                    t_start + submit_latency_s,
                                );
                            }
                            self.live_pending_positions
                                .lock()
                                .await
                                .remove(&intent.intent_id);
                            self.require_live_journal("definitive order rejection")
                                .await?;
                            self.monitor.record_order_rejected(
                                token_id,
                                truncated,
                                limit_price,
                                shares,
                            );
                            if let Some(reason) = permanent_live_order_reject_reason(truncated) {
                                self.trip_breaker(reason).await;
                                self.stop.notify_one();
                            }
                            tracing::warn!(error = %truncated, "candle.trade.live.rejected");
                        } else {
                            *self.live_recovery_ready.lock().await = false;
                            tracing::error!(
                                error = %truncated,
                                order_id = %short_cid(&expected_order_id),
                                "candle.trade.live.submit_ambiguous; exposure retained for REST recovery"
                            );
                        }
                    }
                }
                Ok(())
            }
        }
    }

    async fn open_position_exposure(&self) -> f64 {
        let paper_exposure: f64 = self
            .paper_positions
            .lock()
            .await
            .values()
            .map(paper_position_exposure)
            .sum();
        let pending_resolution_exposure: f64 = self
            .oracle_pending
            .lock()
            .await
            .values()
            .map(pending_resolution_exposure)
            .sum();
        let pending_order_exposure: f64 = self
            .live_pending_positions
            .lock()
            .await
            .values()
            .map(|pending| paper_position_exposure(&pending.position))
            .sum();
        paper_exposure + pending_resolution_exposure + pending_order_exposure
    }

    async fn paper_resolution_loop(self: Arc<Self>) {
        loop {
            let near_resolution = {
                let pp = self.paper_positions.lock().await;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                pp.values()
                    .any(|p| (p.end_time - now) > 0.0 && (p.end_time - now) < 15.0)
            };
            sleep(Duration::from_secs(if near_resolution { 1 } else { 5 })).await;

            let positions = self.paper_positions.lock().await.clone();
            if positions.is_empty() {
                continue;
            }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);

            let ps = self.price_state.read().await.clone();
            let btc = ps.mid_price;
            if btc <= 0.0 {
                continue;
            }

            let mut resolved: Vec<String> = Vec::new();
            for (cid, pos) in positions.iter() {
                if now < pos.end_time {
                    continue;
                }
                let close_price = ps
                    .price_near_seconds(&pos.asset, pos.end_time, 2.0)
                    .unwrap_or_else(|| {
                        if pos.asset == "BTC" {
                            btc
                        } else {
                            ps.alt_mid.get(&pos.asset).copied().unwrap_or(btc)
                        }
                    });
                let actual = if close_price >= pos.open_btc {
                    "up"
                } else {
                    "down"
                };
                let won = actual == pos.direction;
                let pnl = paper_outcome_pnl(won, pos.entry_price, pos.size, pos.fee);
                if pos.shadow {
                    self.monitor.record_resolution_timing(&ResolutionTiming {
                        condition_id: cid.clone(),
                        source: "paper_shadow".to_string(),
                        market_end_ts_s: pos.end_time,
                        resolution_ts_s: now,
                    });
                    self.monitor.record_shadow_resolution(
                        cid,
                        &pos.direction,
                        actual,
                        pos.open_btc,
                        close_price,
                    );
                    self.oracle_pending.lock().await.insert(
                        cid.clone(),
                        OraclePending {
                            our_actual: actual.to_string(),
                            our_open_btc: pos.open_btc,
                            our_close_btc: close_price,
                            end_time: pos.end_time,
                            attempts: 0,
                            direction: Some(pos.direction.clone()),
                            entry_price: Some(pos.entry_price),
                            fee: Some(pos.fee),
                            size: Some(pos.size),
                            provisional_won: Some(won),
                            provisional_pnl: Some(pnl),
                            pnl_recorded: false,
                            shadow: true,
                        },
                    );
                    tracing::info!(
                        cid = short_cid(cid),
                        predicted = %pos.direction,
                        actual,
                        "candle.shadow.resolved"
                    );
                    resolved.push(cid.clone());
                    continue;
                }
                self.monitor.record_resolution_timing(&ResolutionTiming {
                    condition_id: cid.clone(),
                    source: self.mode.as_str().to_string(),
                    market_end_ts_s: pos.end_time,
                    resolution_ts_s: now,
                });
                self.monitor.record_resolution(
                    cid,
                    &pos.direction,
                    actual,
                    won,
                    pnl,
                    pos.entry_price,
                    pos.open_btc,
                    close_price,
                    "local_close",
                    false,
                );

                self.oracle_pending.lock().await.insert(
                    cid.clone(),
                    OraclePending {
                        our_actual: actual.to_string(),
                        our_open_btc: pos.open_btc,
                        our_close_btc: close_price,
                        end_time: pos.end_time,
                        attempts: 0,
                        direction: Some(pos.direction.clone()),
                        entry_price: Some(pos.entry_price),
                        fee: Some(pos.fee),
                        size: Some(pos.size),
                        provisional_won: Some(won),
                        provisional_pnl: Some(pnl),
                        pnl_recorded: false,
                        shadow: false,
                    },
                );

                tracing::info!(
                    cid = short_cid(cid),
                    predicted = %pos.direction,
                    actual,
                    won,
                    pnl,
                    "candle.resolved"
                );
                resolved.push(cid.clone());
            }

            if !resolved.is_empty() {
                let mut pp = self.paper_positions.lock().await;
                for cid in &resolved {
                    pp.remove(cid);
                }
                drop(pp);
                self.persist_paper_positions().await;
                self.persist_oracle_pending().await;

                // Post-resolution breaker check
                let bs = *self.breaker.lock().await;
                let open_exp = self.open_position_exposure().await;
                let breaker_bankroll = self.risk.initial_bankroll().await.max(1.0);
                if let Some(reason) =
                    self.breaker_trip_reason_for(&bs, open_exp, breaker_bankroll)
                {
                    self.trip_breaker(reason).await;
                    self.stop.notify_one();
                }
            }
        }
    }

    async fn oracle_verification_loop(self: Arc<Self>) {
        const MAX_ATTEMPTS: u32 = 120;
        loop {
            sleep(Duration::from_secs(60)).await;
            let pending = self.oracle_pending.lock().await.clone();
            if pending.is_empty() {
                continue;
            }
            let mut to_remove: Vec<String> = Vec::new();
            for (cid, mut entry) in pending {
                entry.attempts += 1;
                let result = self.ctf.get_resolution(&cid).await;
                match result {
                    Ok((Resolution::NotResolved, _)) => {
                        if entry.attempts >= MAX_ATTEMPTS {
                            if pending_requires_realization(&entry) {
                                let msg =
                                    format!("{} attempts for {}", entry.attempts, short_cid(&cid));
                                self.monitor
                                    .record_error("ctf_unresolved_timeout", &msg, false);
                                self.trip_breaker("oracle_unresolved_timeout").await;
                                self.stop.notify_one();
                                self.oracle_pending.lock().await.insert(cid, entry);
                            } else {
                                to_remove.push(cid.clone());
                            }
                        } else {
                            self.oracle_pending.lock().await.insert(cid, entry);
                        }
                    }
                    Ok((res, [n0, n1])) => {
                        let is_tie = matches!(res, Resolution::Tie);
                        let res_str = res.as_str();
                        let agreed = res_str == entry.our_actual;
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs_f64())
                            .unwrap_or(0.0);
                        let delay = now - entry.end_time;
                        self.monitor.record_oracle_resolution(
                            &cid,
                            &entry.our_actual,
                            entry.our_open_btc,
                            entry.our_close_btc,
                            res_str,
                            &[n0 as f64, n1 as f64],
                            true,
                            agreed,
                            delay,
                        );
                        if let Some((final_won, final_pnl, provisional_won, provisional_pnl)) =
                            entry.oracle_pnl(res_str)
                        {
                            if entry.pnl_recorded {
                                let pnl_delta = final_pnl - provisional_pnl;
                                if !agreed && pnl_delta.abs() > 1e-9 {
                                    if let Err(e) = self.risk.record_pnl(pnl_delta).await {
                                        tracing::warn!(
                                            cid = short_cid(&cid),
                                            error = %e,
                                            "oracle pnl correction failed"
                                        );
                                        let msg = format!("{}: {}", short_cid(&cid), e);
                                        self.monitor.record_error(
                                            "oracle_pnl_correction",
                                            &msg,
                                            false,
                                        );
                                        self.trip_breaker("oracle_pnl_correction_failed").await;
                                        self.stop.notify_one();
                                        self.oracle_pending.lock().await.insert(cid, entry);
                                        continue;
                                    } else {
                                        let mut bs = self.breaker.lock().await;
                                        bs.correct_resolution(
                                            provisional_won,
                                            final_won,
                                            pnl_delta,
                                        );
                                        drop(bs);
                                        self.persist_breaker_state().await;
                                        self.monitor.record_oracle_correction(
                                            &cid,
                                            entry.direction.as_deref().unwrap_or("unknown"),
                                            &entry.our_actual,
                                            res_str,
                                            provisional_won,
                                            final_won,
                                            provisional_pnl,
                                            final_pnl,
                                        );
                                    }
                                }
                            } else if let Err(e) = self.risk.record_pnl(final_pnl).await {
                                tracing::warn!(
                                    cid = short_cid(&cid),
                                    error = %e,
                                    "oracle pnl realization failed"
                                );
                                let msg = format!("{}: {}", short_cid(&cid), e);
                                self.monitor
                                    .record_error("oracle_pnl_realization", &msg, false);
                                self.trip_breaker("oracle_pnl_realization_failed").await;
                                self.stop.notify_one();
                                self.oracle_pending.lock().await.insert(cid, entry);
                                continue;
                            } else {
                                self.risk.record_fees(entry.fee.unwrap_or(0.0)).await;
                                let mut bs = self.breaker.lock().await;
                                bs.record_resolution(final_won, final_pnl);
                                drop(bs);
                                self.persist_breaker_state().await;
                                let source = if entry.shadow { "ctf_shadow" } else { "ctf" };
                                self.monitor.record_realized_resolution(
                                    &cid, res_str, final_won, final_pnl, source,
                                );
                                tracing::info!(
                                    cid = short_cid(&cid),
                                    pnl = final_pnl,
                                    won = final_won,
                                    "candle.oracle.realized"
                                );
                                if !agreed {
                                    self.monitor.record_oracle_correction(
                                        &cid,
                                        entry.direction.as_deref().unwrap_or("unknown"),
                                        &entry.our_actual,
                                        res_str,
                                        provisional_won,
                                        final_won,
                                        provisional_pnl,
                                        final_pnl,
                                    );
                                }
                            }
                            if !agreed {
                                tracing::warn!(
                                    cid = short_cid(&cid),
                                    ours = %entry.our_actual,
                                    polymarket = res_str,
                                    "candle.oracle.disagreement"
                                );
                            } else {
                                tracing::info!(cid = short_cid(&cid), "candle.oracle.agreed");
                            }
                        } else if pending_requires_realization(&entry) {
                            let msg = format!(
                                "{} missing fill economics for real oracle PnL",
                                short_cid(&cid)
                            );
                            self.monitor
                                .record_error("oracle_pnl_missing_fields", &msg, false);
                            self.trip_breaker("oracle_pnl_missing_fields").await;
                            self.stop.notify_one();
                            self.oracle_pending.lock().await.insert(cid, entry);
                            continue;
                        } else if agreed {
                            tracing::info!(cid = short_cid(&cid), "candle.oracle.agreed");
                        } else {
                            tracing::warn!(
                                cid = short_cid(&cid),
                                ours = %entry.our_actual,
                                polymarket = res_str,
                                "candle.oracle.disagreement"
                            );
                        }
                        let bs = *self.breaker.lock().await;
                        let open_exposure = (self.open_position_exposure().await
                            - pending_resolution_exposure(&entry))
                        .max(0.0);
                        let breaker_bankroll = self.risk.initial_bankroll().await.max(1.0);
                        if let Some(reason) =
                            self.breaker_trip_reason_for(&bs, open_exposure, breaker_bankroll)
                        {
                            self.trip_breaker(reason).await;
                            self.stop.notify_one();
                        }
                        if is_tie {
                            if entry.shadow {
                                tracing::warn!(cid = short_cid(&cid), "candle.oracle.shadow_tie");
                            } else {
                                self.trip_breaker("oracle_tie").await;
                                self.stop.notify_one();
                            }
                        }
                        to_remove.push(cid);
                    }
                    Err(e) => {
                        tracing::warn!(cid = short_cid(&cid), error = %e, "ctf read failed");
                        if entry.attempts >= MAX_ATTEMPTS {
                            if pending_requires_realization(&entry) {
                                let msg = format!(
                                    "{} attempts for {}: {}",
                                    entry.attempts,
                                    short_cid(&cid),
                                    e
                                );
                                self.monitor
                                    .record_error("ctf_unresolved_timeout", &msg, false);
                                self.trip_breaker("oracle_unresolved_timeout").await;
                                self.stop.notify_one();
                                self.oracle_pending.lock().await.insert(cid, entry);
                            } else {
                                to_remove.push(cid.clone());
                            }
                        } else {
                            self.oracle_pending.lock().await.insert(cid, entry);
                        }
                    }
                }
            }
            if !to_remove.is_empty() {
                let mut op = self.oracle_pending.lock().await;
                for cid in &to_remove {
                    op.remove(cid);
                }
                drop(op);
                self.persist_oracle_pending().await;
            }
        }
    }

    async fn monitoring_loop(self: Arc<Self>) {
        let mut prev_sources: HashSet<String> = HashSet::new();
        loop {
            sleep(Duration::from_secs(15)).await;
            let ps = self.price_state.read().await.clone();
            let n_sources = ps.n_live_sources();
            let staleness_ms = if let Some(t) = ps.source_timestamps.values().max() {
                t.elapsed().as_millis() as f64
            } else {
                0.0
            };
            let sources: HashMap<String, f64> =
                ps.prices.iter().map(|(k, v)| (k.clone(), *v)).collect();
            self.monitor.record_price_snapshot(
                ps.mid_price,
                n_sources,
                ps.spread,
                staleness_ms,
                &sources,
            );
            let current: HashSet<String> = sources.keys().cloned().collect();
            for src in prev_sources.difference(&current) {
                self.monitor.record_error("source_dropout", src, true);
            }
            prev_sources = current;

            let bs = *self.breaker.lock().await;
            let bankroll = self.risk.effective_bankroll().await;
            let starting_bankroll = self.risk.initial_bankroll().await;
            let exposure = self.open_position_exposure().await;
            let avail = self.risk.available_capital_for_exposure(exposure).await;
            let realized_pnl = self.risk.total_pnl().await;
            let n_paper = self.paper_positions.lock().await.len() as u64;
            self.monitor.record_risk_state(
                starting_bankroll,
                bankroll,
                exposure,
                avail,
                n_paper,
                realized_pnl,
                bs.wins,
                bs.losses,
            );
        }
    }

    async fn trip_breaker(&self, reason: &str) {
        let mut tripped = self.breaker_tripped.lock().await;
        if *tripped {
            return;
        }
        *tripped = true;
        let tripped_at = unix_now_s();
        *self.breaker_trip_reason.lock().await = Some(reason.to_string());
        *self.breaker_tripped_at_s.lock().await = Some(tripped_at);
        let _ = self.risk.set_meta("candle_breaker_tripped", "1").await;
        let _ = self.risk.set_meta("candle_breaker_reason", reason).await;
        let _ = self
            .risk
            .set_meta("candle_breaker_tripped_at", &tripped_at.to_string())
            .await;
        self.persist_breaker_state().await;
        let bs = *self.breaker.lock().await;
        let open_exposure = self.open_position_exposure().await;
        let metrics = bs.metrics(open_exposure, self.risk.initial_bankroll().await.max(1.0));
        self.monitor.record_breaker_state(
            "tripped",
            reason,
            bs.wins,
            bs.losses,
            bs.realized_pnl,
            bs.peak_pnl,
            metrics.open_exposure,
            metrics.stressed_pnl,
            metrics.realized_drawdown,
            metrics.realized_drawdown_pct,
            metrics.stressed_drawdown,
            metrics.stressed_drawdown_pct,
        );
        tracing::warn!(
            reason,
            wins = bs.wins,
            losses = bs.losses,
            pnl = bs.realized_pnl,
            open_exposure = metrics.open_exposure,
            stressed_pnl = metrics.stressed_pnl,
            "candle.circuit_breaker.tripped"
        );
        let _ = self
            .alerter
            .send(
                "critical",
                "PolyMomentum circuit breaker",
                &format!(
                    "reason={reason} wins={} losses={} pnl=${:.2}",
                    bs.wins, bs.losses, bs.realized_pnl
                ),
            )
            .await;
    }

    async fn maybe_rearm_paper_breaker(&self) -> bool {
        let breaker_tripped = *self.breaker_tripped.lock().await;
        if !breaker_tripped {
            return false;
        }
        let breaker_state = *self.breaker.lock().await;
        let paper_positions_empty = self.paper_positions.lock().await.is_empty();
        let oracle_pending_empty = self.oracle_pending.lock().await.is_empty();
        let trip_reason = self.breaker_trip_reason.lock().await.clone();
        let tripped_at_s = *self.breaker_tripped_at_s.lock().await;
        let Some(rearm_reason) = paper_breaker_rearm_reason(
            self.mode,
            self.settings.candle_paper_breaker_auto_rearm_secs,
            breaker_tripped,
            breaker_state,
            paper_positions_empty,
            oracle_pending_empty,
            trip_reason.as_deref(),
            tripped_at_s,
            unix_now_s(),
            &self.breaker_cfg,
            self.risk.initial_bankroll().await.max(1.0),
        ) else {
            return false;
        };

        tracing::warn!(
            reason = rearm_reason,
            wins = breaker_state.wins,
            losses = breaker_state.losses,
            pnl = breaker_state.realized_pnl,
            "rearming paper circuit breaker"
        );
        self.clear_breaker_state(rearm_reason).await;
        true
    }

    async fn clear_breaker_state(&self, reason: &str) {
        *self.breaker_tripped.lock().await = false;
        *self.breaker.lock().await = BreakerState::default();
        *self.breaker_trip_reason.lock().await = None;
        *self.breaker_tripped_at_s.lock().await = None;
        for key in [
            "candle_breaker_tripped",
            "candle_breaker_state",
            "candle_breaker_reason",
            "candle_breaker_tripped_at",
        ] {
            if let Err(e) = self.risk.delete_meta(key).await {
                tracing::warn!(key, error = %e, "delete breaker metadata failed");
            }
        }
        self.monitor.record_breaker_state(
            "paper_rearmed",
            reason,
            0,
            0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        );
    }

    fn kill_switch_active(&self) -> bool {
        self.kill_switch_path.exists()
    }

    async fn persist_paper_positions(&self) {
        let pp = self.paper_positions.lock().await.clone();
        let entries: Vec<(String, serde_json::Value)> =
            pp.into_iter().map(|(k, v)| (k, v.to_json())).collect();
        if let Err(e) = self.risk.save_paper_positions(&entries).await {
            tracing::warn!(error = %e, "persist paper positions failed");
        }
    }

    async fn persist_live_pending_orders(&self) -> Result<()> {
        let orders = self.order_manager.lock().await;
        let pending = self.live_pending_positions.lock().await;
        let reconciled_trade_ids: Vec<String> = self
            .reconciled_trade_ids
            .lock()
            .await
            .iter()
            .cloned()
            .collect();
        let mut entries = Vec::with_capacity(pending.len());
        for (intent_id, pending_position) in pending.iter() {
            let order = orders
                .get(intent_id)
                .ok_or_else(|| anyhow::anyhow!("pending live order {intent_id} has no lifecycle"))?
                .clone();
            let entry = LiveOrderJournalEntry {
                order,
                pending: pending_position.clone(),
                reconciled_trade_ids: reconciled_trade_ids.clone(),
            };
            entry
                .validate(intent_id)
                .map_err(anyhow::Error::msg)
                .with_context(|| format!("validate live order journal {intent_id}"))?;
            entries.push((
                intent_id.clone(),
                serde_json::to_value(entry).context("serialize live order journal")?,
            ));
        }
        drop(pending);
        drop(orders);
        self.risk
            .save_live_pending_orders(&entries)
            .await
            .context("persist live pending-order journal")
    }

    async fn require_live_journal(&self, operation: &str) -> Result<()> {
        if let Err(error) = self.persist_live_pending_orders().await {
            tracing::error!(%error, operation, "live order journal failed; stopping live runtime");
            self.trip_breaker("live_order_journal_failure").await;
            self.stop.notify_one();
            return Err(error);
        }
        Ok(())
    }

    /// Breaker check plus the cross-restart cumulative live-loss cap. The
    /// ledger holds prior sessions' realized PnL; adding the current breaker
    /// PnL makes the cap restart-proof (a live trip cannot be cleared by
    /// bouncing the process).
    fn breaker_trip_reason_for(
        &self,
        bs: &BreakerState,
        open_exposure: f64,
        bankroll: f64,
    ) -> Option<&'static str> {
        if let Some(reason) = bs.should_trip(&self.breaker_cfg, open_exposure, bankroll) {
            return Some(reason);
        }
        if self.mode == Mode::Live {
            let cap_pct = self.settings.candle_live_max_cumulative_loss_pct;
            if cap_pct > 0.0
                && self.live_loss_ledger_prior + bs.realized_pnl <= -cap_pct * bankroll.max(1.0)
            {
                return Some("live_cumulative_loss");
            }
        }
        None
    }

    async fn persist_breaker_state(&self) {
        let bs = *self.breaker.lock().await;
        match serde_json::to_string(&bs) {
            Ok(payload) => {
                if let Err(e) = self.risk.set_meta("candle_breaker_state", &payload).await {
                    tracing::warn!(error = %e, "persist breaker state failed");
                }
            }
            Err(e) => tracing::warn!(error = %e, "serialize breaker state failed"),
        }
    }

    async fn persist_oracle_pending(&self) {
        let op = self.oracle_pending.lock().await.clone();
        let entries: Vec<(String, serde_json::Value)> =
            op.into_iter().map(|(k, v)| (k, v.to_json())).collect();
        if let Err(e) = self.risk.save_oracle_pending(&entries).await {
            tracing::warn!(error = %e, "persist oracle pending failed");
        }
    }
}

fn pick_book_prices(
    contract: &CandleContract,
    books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
    now_ts: f64,
) -> Option<(f64, f64)> {
    let up = books.get(&contract.up_token_id).and_then(|b| {
        if live_book_age_seconds(now_ts, b.last_update_us).is_some() && b.best_ask > 0.0 {
            Some(b.best_ask)
        } else {
            None
        }
    })?;
    let down = books.get(&contract.down_token_id).and_then(|b| {
        if live_book_age_seconds(now_ts, b.last_update_us).is_some() && b.best_ask > 0.0 {
            Some(b.best_ask)
        } else {
            None
        }
    })?;
    Some((up, down))
}

fn live_microstructure(
    token_id: &str,
    books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
    now_ts: f64,
) -> BookMicrostructure {
    let Some(book) = books.get(token_id) else {
        return BookMicrostructure::default();
    };
    if live_book_age_seconds(now_ts, book.last_update_us).is_none() {
        return BookMicrostructure::default();
    }
    let bids: Vec<BookLevelView> = book
        .bids
        .iter()
        .map(|l| BookLevelView {
            price: l.price,
            size: l.size,
        })
        .collect();
    let asks: Vec<BookLevelView> = book
        .asks
        .iter()
        .map(|l| BookLevelView {
            price: l.price,
            size: l.size,
        })
        .collect();
    BookMicrostructure::from_levels_with_top(book.best_bid, book.best_ask, &bids, &asks, 3)
}

fn live_book_age_seconds(now_ts: f64, last_update_us: u64) -> Option<f64> {
    let age = now_ts - last_update_us as f64 / 1_000_000.0;
    (age.is_finite() && (0.0..30.0).contains(&age)).then_some(age)
}

fn live_market_tick_size(
    metadata_tick_size: Option<f64>,
    outcome_token_ids: [&str; 2],
    books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
) -> f64 {
    let mut tick_size = metadata_tick_size
        .filter(|tick| tick.is_finite() && *tick > 0.0 && *tick < 1.0)
        .unwrap_or(DEFAULT_TICK);
    let mut latest_feed_tick: Option<(u64, f64)> = None;
    for token_id in outcome_token_ids {
        let Some(book) = books.get(token_id) else {
            continue;
        };
        if let Some(feed_tick) = book
            .tick_size
            .filter(|tick| tick.is_finite() && *tick > 0.0 && *tick < 1.0)
        {
            latest_feed_tick = match latest_feed_tick {
                Some((timestamp_us, current)) if timestamp_us > book.tick_update_us => {
                    Some((timestamp_us, current))
                }
                Some((timestamp_us, current)) if timestamp_us == book.tick_update_us => {
                    Some((timestamp_us, current.min(feed_tick)))
                }
                _ => Some((book.tick_update_us, feed_tick)),
            };
        }
    }
    if let Some((_, feed_tick)) = latest_feed_tick {
        tick_size = feed_tick;
    }
    for token_id in outcome_token_ids {
        if let Some(book) = books.get(token_id) {
            tick_size =
                apply_causal_dynamic_tick_transition(tick_size, book.best_bid, book.best_ask);
        }
    }
    tick_size
}

fn live_book_age_ms(
    token_id: &str,
    books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
    now_ts: f64,
) -> Option<f64> {
    books
        .get(token_id)
        .and_then(|book| live_book_age_seconds(now_ts, book.last_update_us))
        .map(|age| age * 1_000.0)
}

fn live_bookwalk_buy_slippage(
    token_id: &str,
    books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
    size: f64,
) -> Option<f64> {
    let asks = books
        .get(token_id)?
        .asks
        .iter()
        .map(|level| BookLevelView {
            price: level.price,
            size: level.size,
        })
        .collect::<Vec<_>>();
    bookwalk_buy_slippage(&asks, size, crate::backtest::fill_model::DEFAULT_TICK)
}

fn live_buy_book_quote(
    token_id: &str,
    books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
    budget_usd: f64,
    min_order_size_shares: f64,
    tick_size: f64,
) -> Option<BuyBookQuote> {
    let asks = books
        .get(token_id)?
        .asks
        .iter()
        .map(|level| (level.price, level.size))
        .collect::<Vec<_>>();
    buy_book_quote_from_budget(budget_usd, &asks, min_order_size_shares, tick_size)
}

fn live_recent_mid_runup(
    token_id: &str,
    books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
    now_ts: f64,
    lookback_seconds: f64,
) -> Option<f64> {
    recent_mid_runup(&books.get(token_id)?.mid_history, now_ts, lookback_seconds)
}

fn parse_end(s: &str) -> Result<DateTime<Utc>> {
    let normalized = s.replace('Z', "+00:00");
    Ok(DateTime::parse_from_rfc3339(&normalized)?.with_timezone(&Utc))
}

fn short_cid(s: &str) -> String {
    if s.len() <= 16 {
        s.to_string()
    } else {
        s[..16].to_string()
    }
}

fn nonzero_ts_or_now(ts: f64) -> f64 {
    if ts > 0.0 {
        ts
    } else {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }
}

async fn try_wallet_bankroll(settings: &Settings) -> Option<f64> {
    if settings.private_key.is_empty() {
        return None;
    }
    let r =
        crate::data::wallet::WalletReader::new(&settings.polygon_rpc_url, &settings.private_key)
            .ok()?;
    let b = r.fetch_balances().await.ok()?;
    if b.pusd > 0.0 {
        tracing::info!(
            address = b.address,
            pusd = b.pusd,
            "auto-detected CLOB V2 bankroll"
        );
        Some(b.pusd)
    } else if b.usdc_e > 0.0 {
        tracing::warn!(
            address = b.address,
            usdc_e = b.usdc_e,
            "USDC.e balance detected but CLOB V2 live bankroll requires pUSD"
        );
        None
    } else {
        None
    }
}

fn spawn_exchange_feeds(state: Arc<RwLock<PriceState>>) {
    use crate::exchange;

    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::binance_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::chainlink_settlement_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::bybit_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::okx_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    // Alts
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::binance_alt_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                exchange::bybit_alt_feed(s.clone()).await;
                sleep(Duration::from_secs(3)).await;
            }
        });
    }
    // Deribit IV
    {
        let s = state.clone();
        tokio::spawn(async move {
            loop {
                if let Some(iv) = exchange::fetch_deribit_iv().await {
                    s.write().await.implied_vol = iv;
                }
                sleep(Duration::from_secs(60)).await;
            }
        });
    }
}

fn should_reset_paper_breaker_on_start(
    mode: Mode,
    enabled: bool,
    breaker_tripped: bool,
    breaker_state: BreakerState,
    paper_positions_empty: bool,
    oracle_pending_empty: bool,
) -> bool {
    matches!(mode, Mode::Paper)
        && enabled
        && paper_positions_empty
        && oracle_pending_empty
        && (breaker_tripped || breaker_state.wins + breaker_state.losses > 0)
}

#[allow(clippy::too_many_arguments)]
fn paper_breaker_rearm_reason(
    mode: Mode,
    auto_rearm_secs: i64,
    breaker_tripped: bool,
    breaker_state: BreakerState,
    paper_positions_empty: bool,
    oracle_pending_empty: bool,
    trip_reason: Option<&str>,
    tripped_at_s: Option<i64>,
    now_s: i64,
    cfg: &BreakerConfig,
    initial_bankroll: f64,
) -> Option<&'static str> {
    if !matches!(mode, Mode::Paper)
        || !breaker_tripped
        || !paper_positions_empty
        || !oracle_pending_empty
        || matches!(trip_reason, Some("kill_switch" | "oracle_tie"))
    {
        return None;
    }
    if breaker_state
        .should_trip(cfg, 0.0, initial_bankroll)
        .is_none()
    {
        return Some("paper_policy_clear");
    }
    if auto_rearm_secs >= 0 {
        let elapsed = tripped_at_s
            .map(|tripped_at| now_s.saturating_sub(tripped_at))
            .unwrap_or(0);
        if elapsed >= auto_rearm_secs {
            return Some("paper_cooldown_elapsed");
        }
    }
    None
}

fn unix_now_s() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::experiment::{
        PromotionArtifact, PromotionGate, CURRENT_INVENTORY_MODEL_VERSION,
    };
    use tempfile::TempDir;

    #[test]
    fn live_market_tick_uses_latest_feed_change_and_causal_book_fallback() {
        let mut books = HashMap::new();
        books.insert(
            "up".to_string(),
            crate::polymarket_ws::TokenBookState {
                best_bid: 0.95,
                best_ask: 0.96,
                tick_size: Some(0.001),
                tick_update_us: 20,
                ..Default::default()
            },
        );
        books.insert(
            "down".to_string(),
            crate::polymarket_ws::TokenBookState {
                best_bid: 0.04,
                best_ask: 0.05,
                tick_size: Some(0.01),
                tick_update_us: 10,
                ..Default::default()
            },
        );

        assert_eq!(
            live_market_tick_size(Some(0.01), ["up", "down"], &books),
            0.001
        );

        books.get_mut("up").unwrap().tick_size = None;
        books.get_mut("down").unwrap().tick_size = None;
        books.get_mut("up").unwrap().best_ask = 0.97;
        assert_eq!(
            live_market_tick_size(Some(0.01), ["up", "down"], &books),
            0.001
        );
    }

    #[test]
    fn live_book_quality_populates_runtime_selectivity_inputs() {
        let token_id = "token".to_string();
        let mut books = HashMap::new();
        books.insert(
            token_id.clone(),
            crate::polymarket_ws::TokenBookState {
                best_bid: 0.79,
                best_ask: 0.80,
                mid: 0.795,
                asks: vec![
                    crate::polymarket_ws::BookLevel {
                        price: 0.80,
                        size: 5.0,
                    },
                    crate::polymarket_ws::BookLevel {
                        price: 0.82,
                        size: 10.0,
                    },
                ],
                last_update_us: 99_950_000,
                mid_history: std::collections::VecDeque::from([
                    (85.0, 0.78),
                    (90.0, 0.79),
                    (95.0, 0.80),
                    (99.95, 0.795),
                ]),
                ..crate::polymarket_ws::TokenBookState::default()
            },
        );

        let age = live_book_age_ms(&token_id, &books, 100.0);
        let slippage = live_bookwalk_buy_slippage(&token_id, &books, 10.0);
        let runup = live_recent_mid_runup(&token_id, &books, 100.0, 15.0);
        assert!((age.unwrap() - 50.0).abs() < 1e-9);
        assert_eq!(live_book_age_ms(&token_id, &books, 99.0), None);
        assert!((slippage.unwrap() - 0.01).abs() < 1e-9);
        assert!((runup.unwrap() - 0.015).abs() < 1e-9);

        let mut regime = crate::strategy::decision::DecisionRegime::default();
        regime.attach_orderbook_quality_inputs(slippage, age);
        regime.attach_orderbook_path_inputs(runup);
        let mut filter = SelectivityFilter::default();
        filter
            .require_tags
            .insert("book_age".to_string(), "lte_100ms".to_string());
        assert!(filter.reject_reason(&regime).is_none());
        assert!(regime
            .causal_tags()
            .contains(&("book_runup".to_string(), "lte_0.02".to_string())));
    }

    fn promotion_for_variant(variant: &StrategyVariant) -> PromotionArtifact {
        let spec =
            StrategySpec::from_serializable_params("candle_momentum", "1", variant, "test-risk");
        PromotionArtifact {
            schema_version: 1,
            inventory_model_version: CURRENT_INVENTORY_MODEL_VERSION,
            created_at: "2026-05-01T00:00:00Z".to_string(),
            source_report_hash: "report-hash".to_string(),
            source_label: "unit".to_string(),
            source_window: "a..b".to_string(),
            selected_strategy: spec,
            strategy_params: serde_json::to_value(variant).unwrap(),
            data_manifest_hash: "manifest-hash".to_string(),
            market_count: 1,
            trades: 30,
            win_rate: 0.6,
            total_pnl: 1.0,
            avg_pnl: 0.03,
            total_fees: 0.1,
            sharpe_like: 1.0,
            dominant_zone: Some("primary".to_string()),
            dominant_zone_trade_share: Some(0.5),
            risk_notes: Vec::new(),
            promotion_gate: PromotionGate::default(),
            robust_diagnostics: None,
        }
    }

    #[test]
    fn runtime_strategy_uses_promoted_variant() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("promotion.json");
        let mut variant = StrategyVariant::loose_maker();
        variant.decision_volatility_floor = 0.80;
        let artifact = promotion_for_variant(&variant);
        std::fs::write(&path, serde_json::to_vec(&artifact).unwrap()).unwrap();
        let mut settings = Settings::from_env();
        settings.promotion_artifact_path = path.display().to_string();
        settings.candle_settlement_cutoff_minutes = 0.30;
        settings.candle_settlement_guard_minutes = 1.0;
        settings.candle_settlement_min_abs_move_usd = 10.0;
        settings.candle_settlement_sigma_buffer = 0.0;

        let runtime = RuntimeStrategy::load(&settings).unwrap();

        assert_eq!(runtime.strategy_spec, artifact.selected_strategy);
        assert!(runtime.prefer_maker);
        assert_eq!(runtime.min_confidence, variant.min_confidence);
        assert_eq!(runtime.min_edge, variant.min_edge);
        assert_eq!(runtime.max_per_market_usd, variant.max_per_market_usd);
        assert_eq!(runtime.decision_volatility(0.35), 0.80);
        assert_eq!(runtime.decision_volatility(0.90), 0.90);
        assert!(runtime.decision_volatility(f64::NAN).is_nan());
    }

    #[test]
    fn paper_breaker_reset_on_start_is_paper_only_and_requires_no_open_state() {
        let mut state = BreakerState::default();
        state.record_resolution(false, -5.0);

        assert!(should_reset_paper_breaker_on_start(
            Mode::Paper,
            true,
            true,
            state,
            true,
            true
        ));
        assert!(!should_reset_paper_breaker_on_start(
            Mode::Live,
            true,
            true,
            state,
            true,
            true
        ));
        assert!(!should_reset_paper_breaker_on_start(
            Mode::Paper,
            true,
            true,
            state,
            false,
            true
        ));
        assert!(!should_reset_paper_breaker_on_start(
            Mode::Paper,
            true,
            true,
            state,
            true,
            false
        ));
        assert!(!should_reset_paper_breaker_on_start(
            Mode::Paper,
            false,
            true,
            state,
            true,
            true
        ));
    }

    #[test]
    fn paper_breaker_auto_rearms_flat_policy_clear_only_in_paper() {
        let state = BreakerState {
            realized_pnl: 39.1973,
            peak_pnl: 67.8299,
            wins: 26,
            losses: 12,
            ..Default::default()
        };

        assert_eq!(
            paper_breaker_rearm_reason(
                Mode::Paper,
                300,
                true,
                state,
                true,
                true,
                Some("realized_drawdown"),
                Some(1_000),
                1_001,
                &BreakerConfig::default(),
                100.0,
            ),
            Some("paper_policy_clear")
        );
        assert_eq!(
            paper_breaker_rearm_reason(
                Mode::Live,
                300,
                true,
                state,
                true,
                true,
                Some("realized_drawdown"),
                Some(1_000),
                1_001,
                &BreakerConfig::default(),
                100.0,
            ),
            None
        );
    }

    #[test]
    fn paper_breaker_auto_rearm_respects_hard_reasons_and_open_state() {
        let mut state = BreakerState::default();
        for _ in 0..30 {
            state.record_resolution(false, -1.0);
        }

        assert_eq!(
            paper_breaker_rearm_reason(
                Mode::Paper,
                300,
                true,
                state,
                true,
                true,
                Some("win_rate_low"),
                Some(1_000),
                1_301,
                &BreakerConfig::default(),
                100.0,
            ),
            Some("paper_cooldown_elapsed")
        );
        assert_eq!(
            paper_breaker_rearm_reason(
                Mode::Paper,
                300,
                true,
                state,
                false,
                true,
                Some("win_rate_low"),
                Some(1_000),
                1_301,
                &BreakerConfig::default(),
                100.0,
            ),
            None
        );
        assert_eq!(
            paper_breaker_rearm_reason(
                Mode::Paper,
                300,
                true,
                state,
                true,
                true,
                Some("kill_switch"),
                Some(1_000),
                1_301,
                &BreakerConfig::default(),
                100.0,
            ),
            None
        );
    }

    #[test]
    fn runtime_strategy_applies_settlement_safety_floor_to_promotion() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("promotion.json");
        let mut variant = StrategyVariant::loose_maker();
        variant.zone_config.settlement_cutoff_minutes = 0.1;
        variant.zone_config.settlement_guard_minutes = 0.5;
        variant.zone_config.settlement_min_abs_move_usd = 2.0;
        variant.zone_config.settlement_sigma_buffer = 0.0;
        let artifact = promotion_for_variant(&variant);
        std::fs::write(&path, serde_json::to_vec(&artifact).unwrap()).unwrap();
        let mut settings = Settings::from_env();
        settings.promotion_artifact_path = path.display().to_string();
        settings.candle_settlement_cutoff_minutes = 1.5;
        settings.candle_settlement_guard_minutes = 5.0;
        settings.candle_settlement_min_abs_move_usd = 25.0;
        settings.candle_settlement_sigma_buffer = 0.2;
        settings.candle_runtime_min_confidence_floor = 0.7;
        settings.candle_runtime_min_edge_floor = 0.09;
        settings.candle_microstructure_max_spread = 0.02;
        settings.candle_microstructure_min_book_depth = 20.0;
        settings.candle_microstructure_min_book_pressure = 0.0;

        let runtime = RuntimeStrategy::load(&settings).unwrap();

        assert_eq!(runtime.zone_config.settlement_cutoff_minutes, 1.5);
        assert_eq!(runtime.zone_config.settlement_guard_minutes, 5.0);
        assert_eq!(runtime.zone_config.settlement_min_abs_move_usd, 25.0);
        assert_eq!(runtime.zone_config.settlement_sigma_buffer, 0.2);
        assert_eq!(runtime.min_confidence, 0.7);
        assert_eq!(runtime.min_edge, 0.09);
        assert_eq!(runtime.microstructure.max_spread, 0.02);
        assert_eq!(runtime.microstructure.min_book_depth, 20.0);
        assert_eq!(runtime.microstructure.min_book_pressure, 0.0);
        assert_ne!(
            runtime.strategy_spec.params_hash,
            artifact.selected_strategy.params_hash
        );
        assert!(runtime.source.ends_with("+settlement_floor+runtime_floor"));
    }

    #[test]
    fn runtime_strategy_rejects_tampered_params() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("promotion.json");
        let variant = StrategyVariant::loose_maker();
        let mut artifact = promotion_for_variant(&variant);
        artifact.strategy_params["min_edge"] = serde_json::json!(0.99);
        std::fs::write(&path, serde_json::to_vec(&artifact).unwrap()).unwrap();
        let mut settings = Settings::from_env();
        settings.promotion_artifact_path = path.display().to_string();

        let err = RuntimeStrategy::load(&settings).unwrap_err();

        assert!(err.to_string().contains("hash mismatch"));
    }

    #[test]
    fn permanent_live_order_rejects_are_fail_closed() {
        assert_eq!(
            permanent_live_order_reject_reason(
                "400 Bad Request: {\"error\":\"not enough balance / allowance\"}"
            ),
            Some("live_balance_allowance_reject")
        );
        assert_eq!(
            permanent_live_order_reject_reason("invalid post-only order: order crosses book"),
            Some("live_post_only_cross_reject")
        );
        assert_eq!(
            permanent_live_order_reject_reason(
                "invalid amount for a marketable BUY order ($0.05), min size: $1"
            ),
            Some("live_marketable_min_size_reject")
        );
        assert_eq!(
            permanent_live_order_reject_reason("invalid price: not on tick size"),
            Some("live_order_shape_reject")
        );
        assert_eq!(permanent_live_order_reject_reason("network timeout"), None);
    }

    #[test]
    fn oracle_pnl_treats_polymarket_tie_as_half_redemption() {
        let pending = OraclePending {
            our_actual: "up".to_string(),
            our_open_btc: 100.0,
            our_close_btc: 110.0,
            end_time: 1.0,
            attempts: 0,
            direction: Some("up".to_string()),
            entry_price: Some(0.42),
            fee: Some(0.01),
            size: Some(10.0),
            provisional_won: Some(true),
            provisional_pnl: Some(5.79),
            pnl_recorded: false,
            shadow: false,
        };

        let (final_won, final_pnl, provisional_won, provisional_pnl) =
            pending.oracle_pnl("tie").unwrap();

        assert!(!final_won);
        assert!(provisional_won);
        assert!((final_pnl - 0.79).abs() < 1e-9);
        assert!((provisional_pnl - 5.79).abs() < 1e-9);
    }

    #[test]
    fn pending_resolution_exposure_counts_unrealized_entries_until_recorded() {
        let mut pending = OraclePending {
            our_actual: "up".to_string(),
            our_open_btc: 100.0,
            our_close_btc: 110.0,
            end_time: 1.0,
            attempts: 0,
            direction: Some("up".to_string()),
            entry_price: Some(0.42),
            fee: Some(0.01),
            size: Some(10.0),
            provisional_won: Some(true),
            provisional_pnl: Some(5.79),
            pnl_recorded: false,
            shadow: false,
        };

        assert!((pending_resolution_exposure(&pending) - 4.21).abs() < 1e-9);
        pending.pnl_recorded = true;
        assert_eq!(pending_resolution_exposure(&pending), 0.0);
        pending.pnl_recorded = false;
        pending.shadow = true;
        assert!((pending_resolution_exposure(&pending) - 4.21).abs() < 1e-9);
        pending.pnl_recorded = true;
        assert_eq!(pending_resolution_exposure(&pending), 0.0);
    }

    #[test]
    fn paper_position_shadow_flag_round_trips_with_legacy_default() {
        let pos = PaperPosition {
            direction: "up".to_string(),
            entry_price: 0.42,
            fee: 0.0,
            size: 0.0,
            open_btc: 100.0,
            end_time: 10.0,
            asset: "BTC".to_string(),
            contract_id: "cid".to_string(),
            event_id: "event".to_string(),
            shadow: true,
        };
        let encoded = pos.to_json();
        let decoded = PaperPosition::from_json("cid".to_string(), &encoded).unwrap();
        assert!(decoded.shadow);

        let mut legacy = encoded;
        legacy.as_object_mut().unwrap().remove("shadow");
        legacy.as_object_mut().unwrap().remove("event_id");
        let decoded_legacy = PaperPosition::from_json("cid".to_string(), &legacy).unwrap();
        assert!(!decoded_legacy.shadow);
        assert!(decoded_legacy.event_id.is_empty());
    }

    #[test]
    fn live_position_from_fill_uses_actual_fill_economics() {
        let template = PaperPosition {
            direction: "up".to_string(),
            entry_price: 0.60,
            fee: 0.0,
            size: 0.0,
            open_btc: 100.0,
            end_time: 10.0,
            asset: "BTC".to_string(),
            contract_id: "cid".to_string(),
            event_id: "event".to_string(),
            shadow: false,
        };
        let intent = OrderIntent {
            intent_id: "intent-1".to_string(),
            strategy: StrategySpec::new("test", "1", "hash", "risk"),
            market_id: "cid".to_string(),
            token_id: "tok".to_string(),
            side: "buy".to_string(),
            order_type: "market".to_string(),
            limit_price: Some(0.60),
            size: 10.0,
            reason: "test".to_string(),
        };
        let order = ManagedOrder {
            intent,
            state: OrderState::PartiallyFilled,
            venue_order_id: Some("0xorder".to_string()),
            requested_size: 10.0,
            filled_size: 4.0,
            avg_fill_price: 0.57,
            total_fees: 0.00735,
            reject_reason: None,
            created_ts: 1.0,
            updated_ts: 2.0,
        };

        let pos = live_position_from_fill(&template, &order).unwrap();

        assert_eq!(pos.size, 4.0);
        assert_eq!(pos.entry_price, 0.57);
        assert_eq!(pos.fee, 0.00735);
        assert!(
            (paper_outcome_pnl(true, pos.entry_price, pos.size, pos.fee) - 1.71265).abs() < 1e-9
        );
    }

    #[test]
    fn pending_live_position_prices_current_taker_fee_and_validates_fill() {
        let pending = PendingLivePosition {
            position: PaperPosition {
                direction: "up".to_string(),
                entry_price: 0.57,
                fee: 0.0,
                size: 10.0,
                open_btc: 100.0,
                end_time: 10.0,
                asset: "BTC".to_string(),
                contract_id: "cid".to_string(),
                event_id: "event".to_string(),
                shadow: false,
            },
            entry_fee_rate: 0.07,
            recovery_misses: 0,
        };
        assert_eq!(pending.fill_fee(10.0, 0.57), Some(0.17157));
        assert_eq!(pending.fill_fee(0.0, 0.57), None);
        assert_eq!(pending.fill_fee(10.0, 1.1), None);

        let maker = PendingLivePosition {
            entry_fee_rate: 0.0,
            ..pending
        };
        assert_eq!(maker.fill_fee(10.0, 0.57), Some(0.0));
    }

    #[test]
    fn parses_rest_terminal_order_economics() {
        let snapshot = parse_rest_order_snapshot(&serde_json::json!({
            "id": "0xorder",
            "status": "CANCELED",
            "size_matched": "4.25"
        }))
        .unwrap();
        assert_eq!(snapshot.size_matched, 4.25);
        assert!(snapshot.is_terminal_without_more_fills());

        let live = parse_rest_order_snapshot(&serde_json::json!({
            "data": {"status": "LIVE", "sizeMatched": 0}
        }))
        .unwrap();
        assert!(!live.is_terminal_without_more_fills());
        assert!(parse_rest_order_snapshot(&serde_json::json!({"size_matched": "1"})).is_err());
    }

    #[test]
    fn recognizes_only_explicit_rest_not_found_errors() {
        assert!(is_rest_not_found("HTTP 404: not found"));
        assert!(is_rest_not_found("HTTP 404 Not Found: missing"));
        assert!(!is_rest_not_found("HTTP 500: upstream failed"));
        assert!(!is_rest_not_found("Request failed: timeout"));
    }
}
