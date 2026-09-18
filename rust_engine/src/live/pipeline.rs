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
use crate::strategy::momentum::{MomentumConfig, MomentumDetector, MomentumSignal};
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
        // Already realized (possibly in a previous process lifetime): never
        // replay PnL into risk/breaker/ledger accounting.
        if self.pnl_recorded {
            return None;
        }
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

/// Shares the band sub-book settles for a resolved entry: the book's own
/// fill first, then the fill size persisted with the pending entry, and
/// only then the process-local trade_log ring (0 after a restart).
fn settled_band_qty(book_fill: Option<f64>, pending_size: Option<f64>, ring_size: f64) -> f64 {
    book_fill
        .or(pending_size.filter(|s| *s > 0.0))
        .unwrap_or(ring_size)
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

/// Decide from one venue status-page payload whether new entries must be
/// suspended. `Err` when the body is not JSON (caller keeps the previous
/// flag - fail-open); `Ok(None)` when the venue looks clear; `Ok(Some(reason))`
/// when `activeIncidents` carries an unresolved MAJOROUTAGE / PARTIALOUTAGE /
/// DEGRADEDPERFORMANCE incident or `activeMaintenances` carries an INPROGRESS
/// maintenance (SCHEDULED / NOTSTARTEDYET do not trip). The reason names the
/// kind (`incident` / `maintenance`) so the alert tells them apart. Missing or
/// mis-shaped lists count as clear.
fn venue_status_verdict(body: &str) -> Result<Option<String>, serde_json::Error> {
    let body: serde_json::Value = serde_json::from_str(body)?;
    fn status_is(entry: &serde_json::Value, want: &str) -> bool {
        entry
            .get("status")
            .and_then(|v| v.as_str())
            .is_some_and(|st| st.eq_ignore_ascii_case(want))
    }
    fn active_names(
        body: &serde_json::Value,
        key: &str,
        active: impl Fn(&serde_json::Value) -> bool,
    ) -> Vec<String> {
        body.get(key)
            .and_then(|v| v.as_array())
            .map(|list| {
                list.iter()
                    .filter(|entry| active(entry))
                    .map(|entry| {
                        entry
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unnamed")
                            .to_string()
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
    let incidents = active_names(&body, "activeIncidents", |i| {
        let impact = i.get("impact").and_then(|v| v.as_str()).unwrap_or("");
        !status_is(i, "RESOLVED")
            && matches!(
                impact,
                "MAJOROUTAGE" | "PARTIALOUTAGE" | "DEGRADEDPERFORMANCE"
            )
    });
    let maintenances = active_names(&body, "activeMaintenances", |m| status_is(m, "INPROGRESS"));
    let mut parts = Vec::new();
    if !incidents.is_empty() {
        parts.push(format!("incident \u{00b7} {}", incidents.join("; ")));
    }
    if !maintenances.is_empty() {
        parts.push(format!("maintenance \u{00b7} {}", maintenances.join("; ")));
    }
    Ok((!parts.is_empty()).then(|| parts.join(" \u{00b7} ")))
}

/// Frozen band family: preregistered 2026-08-19, fresh-gate PASS 2026-08-24
/// (`docs/signal_favorite_band_official_v1_preregistration_2026-08-19.md`).
pub const BAND_FAMILY: &str = "signal_favorite_band_official_v1";

/// The frozen band policy. Field set and order are part of the promotion
/// contract: `stable_json_hash` of this struct must equal the artifact's
/// `selected_strategy.params_hash`, so changing anything here invalidates
/// every existing band promotion artifact (by design).
/// Venue floor: below ~$5 the 5-share minimum order cannot be met across the
/// upper band, so the compounding target never sizes under this.
pub const BAND_MIN_STAKE_USD: f64 = 5.0;
/// Capture span after each anchor second: the first cycle inside
/// [anchor, anchor + span) records the anchor, later cycles never re-record.
const BAND_ANCHOR_SPAN_S: f64 = 4.0;

fn serde_f64_is_zero(v: &f64) -> bool {
    *v == 0.0
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BandPolicyParams {
    pub family: String,
    /// Seconds after window open at which the entry attempt window begins.
    /// The signal is LATCHED on the first cycle at or after this second
    /// (`band_latch_decision`, ahead of every cycle gate): direction, or
    /// no-signal for the whole window, from that one look at btc vs open -
    /// the point rule the mechanism was validated on. Later cycles never
    /// re-read the margin, and a first look more than 2 s late is a missed
    /// decision (no-signal), never a late one
    /// (docs/risk_book_v2/margin_latch_study_2026-09-03.md).
    ///
    /// "This second" is the true instant open + decision_seconds on a
    /// sub-second elapsed (`band_window_elapsed_s`), not the loop's
    /// integer-second arithmetic that read 240 from 239.001 s on; and both
    /// prices are Binance's own last tick at or before the open and at or
    /// before that instant (`band_basis_prices`, tolerance 2 s) - the 1 s
    /// close series the margin studies and `fresh_gate_public_v1` sample.
    /// The composite exchange mid is the fallback only when Binance has no
    /// tick at either instant, never a mix; the basis used is recorded on
    /// the window's detail / evaluation record and its `band_anchor`
    /// (study doc, basis parity addendum 2026-09-08).
    pub decision_seconds: f64,
    /// Patient-entry span: attempts run in [decision, decision+entry_window)
    /// on the latched side only, waiting for an executable in-band ask.
    pub entry_window_seconds: f64,
    /// Exclusive lower bound on the budget-aware execution VWAP.
    pub ask_floor: f64,
    /// Inclusive upper bound; enforced on the FOK worst price, which is
    /// stricter than the replay's average-price bound. Break-even at the
    /// 0.07 taker fee is 0.9063 at 0.90, 0.9157 at 0.91 and 0.9252 at 0.92;
    /// of the entry-level Wilson lower bounds measured so far (0.886, 0.919,
    /// 0.921) only 0.921 clears 0.9157 and none clears 0.9252, so
    /// `band_policy_margin50_cap91.json` pins 0.91 as the largest cap any
    /// measured bound clears (direction memo 2026-09-07, gap 3).
    pub ask_cap: f64,
    /// Upper cap on the per-trade stake in USD.
    pub stake_usd: f64,
    /// Minimum |decision price - window open| in USD before an entry may be
    /// attempted. Margin study 2026-09-01: signal accuracy is 98.6-100% at
    /// |margin| >= $50 across 700+ fresh windows including chop, and 72-78%
    /// (below break-even) under $25 - without this floor the band trades
    /// noise at favorite prices. Checked once, at the latch; 0 disables the
    /// floor (legacy artifacts) but the direction is still latched.
    /// skip_serializing_if keeps the canonical serialization - and thus the
    /// params hash - of zero-floor legacy artifacts byte-identical, so
    /// rollback to an older artifact never fails the release hash check.
    #[serde(default, skip_serializing_if = "serde_f64_is_zero")]
    pub min_decision_margin_usd: f64,
    /// Fraction of effective bankroll (allocation + realized PnL) staked per
    /// trade — the compounding knob. Default 1.0 keeps older fixed-stake
    /// artifacts (stake_usd then binds via the cap).
    #[serde(default = "default_band_position_pct")]
    pub position_pct: f64,
}

fn default_band_position_pct() -> f64 {
    1.0
}

impl BandPolicyParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.family != BAND_FAMILY {
            return Err(format!("family {} != {BAND_FAMILY}", self.family));
        }
        if !(self.decision_seconds > 0.0 && self.decision_seconds < 300.0) {
            return Err(format!("decision_seconds {} out of (0,300)", self.decision_seconds));
        }
        if !(self.entry_window_seconds > 0.0
            && self.decision_seconds + self.entry_window_seconds <= 300.0)
        {
            return Err(format!(
                "entry window [{}, {}) exceeds the 5m window",
                self.decision_seconds,
                self.decision_seconds + self.entry_window_seconds
            ));
        }
        if !(0.0 < self.ask_floor && self.ask_floor < self.ask_cap && self.ask_cap <= 1.0) {
            return Err(format!(
                "band bounds ({}, {}] invalid",
                self.ask_floor, self.ask_cap
            ));
        }
        if !(self.stake_usd >= 1.0) {
            return Err(format!("stake_usd {} below venue minimum", self.stake_usd));
        }
        if !(self.position_pct > 0.0 && self.position_pct <= 1.0) {
            return Err(format!("position_pct {} out of (0,1]", self.position_pct));
        }
        Ok(())
    }

    /// Compounding per-trade target: a fraction of effective bankroll,
    /// floored at the venue minimum and capped at stake_usd. Availability,
    /// exposure, and stress caps still apply on top.
    pub fn target_stake(&self, effective_bankroll: f64) -> f64 {
        (effective_bankroll * self.position_pct).clamp(BAND_MIN_STAKE_USD, self.stake_usd)
    }

    /// Entry attempts run while elapsed is in [decision, decision+window).
    pub fn in_entry_window(&self, elapsed_s: f64) -> bool {
        elapsed_s >= self.decision_seconds
            && elapsed_s < self.decision_seconds + self.entry_window_seconds
    }

    /// Frozen band gate on the executable quote: VWAP strictly above the
    /// floor, FOK worst price within the cap (stricter than the replay's
    /// average-price bound).
    pub fn quote_clears_band(&self, vwap: f64, worst_price: f64) -> bool {
        vwap > self.ask_floor && worst_price <= self.ask_cap
    }
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
    band: Option<BandPolicyParams>,
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
        if artifact.selected_strategy.name == BAND_FAMILY {
            let params: BandPolicyParams =
                serde_json::from_value(artifact.strategy_params.clone())
                    .context("parse promoted strategy_params as BandPolicyParams")?;
            params.validate().map_err(anyhow::Error::msg)?;
            let params_hash = stable_json_hash(&params);
            if params_hash != artifact.selected_strategy.params_hash {
                bail!(
                    "promotion artifact hash mismatch: strategy_params hash {} != selected_strategy hash {}",
                    params_hash,
                    artifact.selected_strategy.params_hash
                );
            }
            return Ok(Self::band_runtime(
                settings,
                artifact.selected_strategy,
                params,
                format!("promotion:{path}"),
            ));
        }
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
            band: None,
            source,
        })
    }

    /// Runtime for the frozen band family. The candle_momentum knobs below
    /// are inert placeholders: the band branch in `scan_loop` bypasses the
    /// candle decision path entirely. Sizing intent: position_pct=1.0 with
    /// the $-stake enforced by MAX_POSITION_PER_MARKET_USD /
    /// MAX_TOTAL_EXPOSURE_USD so `execute_trade`'s existing chain yields
    /// min(bankroll, stake). stress cap 1.0 = wallet-bounded canary; the
    /// operative loss brakes are the session-loss floor and the
    /// consecutive-losses breaker, both of which stay active.
    fn band_runtime(
        settings: &Settings,
        strategy_spec: StrategySpec,
        params: BandPolicyParams,
        source: String,
    ) -> Self {
        Self {
            strategy_spec,
            zone_config: ZoneConfig::from_settings(settings),
            skip_dead_zone: false,
            min_confidence: 1.0,
            min_edge: 0.0,
            decision_volatility_floor: 0.0,
            position_pct: 1.0,
            max_per_market_usd: params.stake_usd,
            max_projected_stressed_drawdown_pct: 1.0,
            degraded_after_losses: 0,
            degraded_after_drawdown_pct: 0.0,
            degraded_min_z: 0.0,
            degraded_max_price: 0.0,
            degraded_force_taker: false,
            prefer_maker: false,
            default_fee_rate: DEFAULT_CRYPTO_TAKER_FEE_RATE,
            maker_fee_rate: DEFAULT_MAKER_FEE_RATE,
            microstructure: MicrostructureConfig::disabled(),
            selectivity: SelectivityFilter::default(),
            band: Some(params),
            source,
        }
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
            band: None,
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
    /// `live_cumulative_realized_pnl`), stored as f64 bits. Survives the
    /// bankroll-actualization breaker reset so live loss streaks accumulate
    /// across restarts; current-session PnL lives in the breaker state and
    /// is added on check. The operator re-arm advances it in-process when
    /// it folds a parked session (read via `live_loss_ledger_prior()`).
    live_loss_ledger_prior: std::sync::atomic::AtomicU64,
    breaker_tripped: Mutex<bool>,
    breaker_trip_reason: Mutex<Option<String>>,
    /// Unix seconds of the last "halted" heartbeat log (rate limiter).
    halted_log_s: std::sync::atomic::AtomicU64,
    /// Ring of recent trades for the operator bot (/trades).
    trade_log: Mutex<std::collections::VecDeque<TradeLogRecord>>,
    /// RiskBook v2 shadow ledger (event-sourced postings). Observes every
    /// fill/settlement in parallel with the driving v1 book; never drives
    /// decisions until the RISK_BOOK cutover.
    book_v2: Option<crate::risk::book_v2::BookV2>,
    /// RISK_BOOK=v2 (phase 0 cutover): `book_v2`'s band sub-book drives
    /// band sizing and the cumulative money floor. Position/exposure
    /// accounting and the breaker session stats stay on v1 in both modes.
    risk_book_v2: bool,
    /// The money floor's base under v2 in micro-USD (u64::MAX = unset):
    /// the band equity at the cross-restart ledger's origin (meta
    /// `v2_floor_base_equity`, set when the ledger is fresh and kept while
    /// it accumulates, so a restart after a partial drawdown does not
    /// shrink the floor), lowered by wallet withdrawals, never raised by
    /// deposits (only the ledger post-mortem re-bases).
    v2_floor_base_micro: std::sync::atomic::AtomicU64,
    /// v2 wallet reconciliation state (see `reconcile_v2_wallet`).
    v2_reconcile: Mutex<WalletReconcile>,
    breaker_tripped_at_s: Mutex<Option<i64>>,
    price_state: Arc<RwLock<PriceState>>,
    book_state: SharedBookState,
    /// Unix seconds of the last "decision feed unavailable" record (rate limit).
    last_btc_stall_log_s: std::sync::atomic::AtomicU64,
    /// (cid, reason) pairs whose detailed band-skip record was already
    /// written this process lifetime (one detail record per window+reason).
    band_detail_logged: Mutex<HashSet<String>>,
    /// (cid, anchor second) pairs already captured as `band_anchor` events;
    /// pruned to the live contract list so it cannot grow unbounded.
    band_anchor_logged: Mutex<HashSet<(String, u32)>>,
    /// Per-window band decision (`BandLatch`) with the window end (unix s),
    /// made by `latch_band_decision` on the first cycle inside the entry
    /// window and never revisited; pruned to windows still open at each new
    /// latch (never to the contract list: a refresh that transiently omits
    /// the live market must not re-open a decided window).
    band_latch: Mutex<HashMap<String, (BandLatch, i64)>>,
    /// Latest on-chain pUSD reading in micro-USD and its unix-seconds
    /// timestamp. The wallet is shared with a peer bot, so the balance can
    /// drop under our stake between cycles; the band path consults this to
    /// skip gracefully instead of collecting a venue insufficient-balance
    /// reject (which would trip the breaker as a permanent reason).
    last_wallet_pusd_micro: std::sync::atomic::AtomicU64,
    last_wallet_read_s: std::sync::atomic::AtomicU64,
    tracked_tokens: Arc<RwLock<Vec<String>>>,
    resub_notify: Arc<Notify>,
    /// Set while the venue's own status page reports an active incident.
    /// Gates NEW entries only; exits, reconciliation and settlement never
    /// depend on it.
    venue_incident: Arc<std::sync::atomic::AtomicBool>,
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
        let mut risk_cfg = RiskConfig {
            initial_bankroll: bankroll,
            max_total_exposure_override: settings.max_total_exposure_usd,
            max_per_market_override: runtime_strategy.max_per_market_usd,
            actualize_on_open: matches!(mode, Mode::Live),
            ..Default::default()
        };
        if runtime_strategy.band.is_some() {
            // Wallet-bounded band canary: the frozen $-stake must survive a
            // bankroll of the same magnitude, so the $ overrides above are
            // the operative caps; fractional ratios would silently shrink
            // the stake below the promoted policy. Loss brakes (session
            // floor, consecutive losses) are unaffected.
            risk_cfg.exposure_ratio = 1.0;
            risk_cfg.max_per_market_ratio = 1.0;
        }
        // An explicit BANKROLL_USD is an operator allocation (e.g. a fixed
        // slice of a wallet shared with a peer bot) and must not be
        // overridden by a persisted baseline from earlier auto-detection.
        risk_cfg.pin_initial_bankroll = !matches!(mode, Mode::Paper) && settings.bankroll_usd > 0.0;
        let risk = RiskManager::open(&settings.state_db_path, risk_cfg).await?;
        if matches!(mode, Mode::Paper) && settings.candle_simulated_balance_reset_on_start {
            risk.reset_simulated_session(bankroll).await?;
            tracing::info!(bankroll, "simulated paper/shadow state reset on startup");
        }

        let monitor = Arc::new(SessionMonitor::open(&settings.session_log_dir)?);
        // Unit tests build live pipelines through the trip and held-restart
        // paths, which notify: never from the process environment there.
        let alerter = if cfg!(test) {
            Alerter::disabled()
        } else {
            Alerter::from_env()
        };
        let gamma = GammaClient::new(&settings.poly_gamma_url);
        let ctf = CtfReader::new(&settings.polygon_rpc_url);
        let breaker_cfg = if runtime_strategy.band.is_some() {
            // Band stopping policy, reformatted ground-up (2026-09-01).
            // Trading halts for exactly three families of reasons:
            //   1. MONEY  - restart-proof cumulative floor
            //              (live_cumulative_loss: ledger + session <=
            //              -CANDLE_LIVE_MAX_CUMULATIVE_LOSS_PCT x base) and
            //              the consecutive-loss streak below;
            //   2. BUGS   - band_exposure_anomaly (exposure beyond what
            //              sizing could commit) and the accounting-integrity
            //              trips (fee/journal/oracle failures);
            //   3. OPERATOR - kill switch and telegram /stop.
            // Removed from halting: session_loss_floor (the cumulative
            // floor already includes the session - profits are risk capital
            // under compounding), realized_drawdown (peak-relative: could
            // halt while net POSITIVE), win_rate_low (indirect proxy; the
            // direct money measures bind first).
            BreakerConfig {
                min_trades: u32::MAX,
                min_win_rate: 0.0,
                max_drawdown_pct: f64::INFINITY,
                max_session_loss_pct: 0.0,
                max_consecutive_losses: settings.candle_breaker_max_consecutive_losses.max(1)
                    as u32,
            }
        } else {
            BreakerConfig::from_settings(&settings)
        };

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
        // A persisted money, accounting-integrity or operator trip must
        // survive process restarts - the actualization reset below would
        // otherwise fold the session and re-arm trading on every bounce
        // (observed live 2026-08-31 17:59 and 2026-09-01 13:21: trading
        // resumed unattended after the 5-loss streak). The process starts
        // parked; only the operator's post-mortem (/start, or clearing the
        // candle_breaker_* meta keys) re-arms it. A trip without a persisted
        // reason holds too (fail-closed). Live only: paper has no operator
        // bot to /start and clears its own trips below.
        let trip_held = matches!(mode, Mode::Live)
            && breaker_tripped
            && breaker_trip_reason
                .as_deref()
                .map_or(true, breaker_trip_holds_across_restart);
        if trip_held {
            // The session stays, so the v1 base must not absorb it as well:
            // reverse the open-time bankroll actualization, or the floor
            // and the status equity would count the session twice and
            // move on every bounce.
            risk.undo_actualization().await?;
            let reason = breaker_trip_reason.as_deref().unwrap_or("unknown");
            let ledger = risk
                .get_meta("candle_breaker_ledger")
                .await?
                .unwrap_or_else(|| "n/a".to_string());
            tracing::warn!(
                reason,
                tripped_at = ?breaker_tripped_at_s,
                %ledger,
                wins = breaker_state.wins,
                losses = breaker_state.losses,
                pnl = breaker_state.realized_pnl,
                "candle.halted.restored breaker trip persisted across restart; starting parked (operator /start only after the post-mortem)"
            );
            alerter
                .notify(&format!(
                    "\u{26d4} still HALTED after restart \u{00b7} {reason} \u{00b7} ledger {ledger} \u{00b7} /start only after the post-mortem"
                ))
                .await;
        }
        if !trip_held
            && risk.actualizes_on_open().await
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
            for key in CANDLE_BREAKER_META_KEYS {
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
            for key in CANDLE_BREAKER_META_KEYS {
                if let Err(e) = risk.delete_meta(key).await {
                    tracing::warn!(error = %e, key, "delete candle breaker key failed");
                }
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

        let book_v2 = if matches!(mode, Mode::Live) {
            match crate::risk::book_v2::BookV2::open(book_v2_path(&settings)) {
                Ok(book) => Some(book),
                Err(error) => {
                    tracing::warn!(%error, "book_v2 shadow unavailable");
                    None
                }
            }
        } else {
            None
        };
        // RISK_BOOK=v2 (live only; paper has no wallet to anchor to): the
        // band sub-book must exist and be seeded from the on-chain wallet
        // before anything sizes on it. Its equity at this start is the
        // money floor's base for the session (0.6 x real equity, never
        // the pinned BANKROLL_USD).
        let risk_book_v2 = matches!(mode, Mode::Live) && settings.risk_book_v2();
        if matches!(mode, Mode::Live) && !matches!(settings.risk_book.as_str(), "v1" | "v2") {
            bail!("RISK_BOOK must be v1 or v2 (got {:?})", settings.risk_book);
        }
        // Cross-restart live loss ledger: prior-session cumulative realized
        // PnL. Checked together with current-session breaker PnL so live
        // losses cannot be laundered by restarting the process.
        let live_loss_ledger_prior: f64 = risk
            .get_meta("live_cumulative_realized_pnl")
            .await?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let v2_floor_base = if risk_book_v2 {
            let Some(book) = book_v2.as_ref() else {
                bail!(
                    "RISK_BOOK=v2: book_v2.db unavailable; cannot size on the wallet-anchored book"
                );
            };
            if book.is_empty().await? {
                if seed_book_v2_from_wallet(book, &settings).await.is_none() {
                    bail!("RISK_BOOK=v2: book_v2 is empty and the wallet is unreadable; cannot seed");
                }
            } else {
                // The wallet may have moved while the process was down (a
                // withdrawal, a purged FOK that filled on-chain, the funding
                // the resume procedure calls for): the book follows it now,
                // ahead of the session-start snapshot, or the floor base
                // and the first readings' sizing would be a stale book's.
                // An unreadable wallet leaves the book standing (the live
                // preflight read the wallet seconds ago).
                match read_wallet_pusd(&settings).await {
                    Some(pusd) if pusd > 0.0 => {
                        let equity = book.equity_for("band").await?;
                        let diff = pusd - equity;
                        if diff.abs() > settings.risk_book_reconcile_tolerance_usd {
                            let posted =
                                post_wallet_adjustment(book, diff, pusd, "wallet_reconcile_start")
                                    .await;
                            tracing::warn!(
                                wallet = pusd,
                                book = equity,
                                adjustment = diff,
                                posted,
                                "book_v2 wallet_reconcile at start: band sub-book follows the wallet"
                            );
                        }
                    }
                    _ => tracing::warn!(
                        "book_v2 start: wallet unreadable or empty; session-start equity is the book's"
                    ),
                }
            }
            let equity = book.equity_for("band").await?;
            risk.set_meta("v2_session_start_equity", &format!("{equity:.6}"))
                .await?;
            // The floor's base is the equity at the ledger's origin: taken
            // when the ledger is fresh (absent or zero: the operator's
            // post-mortem reset, or the first v2 start), kept while it
            // accumulates. Re-reading the drawn-down equity on every restart
            // would compare losses against a base that already excludes
            // them (a benign restart after losing 19 of 50 read a floor of
            // -18.6 and blocked the start).
            let ledger_fresh = live_loss_ledger_prior.abs() <= 1e-9;
            let base = match risk
                .get_meta("v2_floor_base_equity")
                .await?
                .and_then(|s| s.parse::<f64>().ok())
            {
                Some(base) if !ledger_fresh => base,
                _ => {
                    risk.set_meta("v2_floor_base_equity", &format!("{equity:.6}"))
                        .await?;
                    equity
                }
            };
            tracing::info!(
                equity,
                floor_base = base,
                "risk_book v2 drives band sizing and the money floor"
            );
            Some(base)
        } else {
            None
        };
        if mode == Mode::Live {
            let cap_pct = settings.candle_live_max_cumulative_loss_pct;
            let bankroll_floor = match v2_floor_base {
                Some(base) => base.max(1.0),
                None => risk.initial_bankroll().await.max(1.0),
            };
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
                &settings.poly_funder,
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
            live_loss_ledger_prior: std::sync::atomic::AtomicU64::new(
                live_loss_ledger_prior.to_bits(),
            ),
            breaker_tripped: Mutex::new(breaker_tripped),
            breaker_trip_reason: Mutex::new(breaker_trip_reason),
            halted_log_s: std::sync::atomic::AtomicU64::new(0),
            trade_log: Mutex::new(std::collections::VecDeque::new()),
            book_v2,
            risk_book_v2,
            v2_floor_base_micro: std::sync::atomic::AtomicU64::new(
                v2_floor_base.map_or(u64::MAX, usd_to_micro),
            ),
            v2_reconcile: Mutex::new(WalletReconcile::default()),
            breaker_tripped_at_s: Mutex::new(breaker_tripped_at_s),
            price_state: Arc::new(RwLock::new(PriceState::new())),
            book_state: new_shared_book(),
            last_btc_stall_log_s: std::sync::atomic::AtomicU64::new(0),
            band_detail_logged: Mutex::new(HashSet::new()),
            band_anchor_logged: Mutex::new(HashSet::new()),
            band_latch: Mutex::new(HashMap::new()),
            last_wallet_pusd_micro: std::sync::atomic::AtomicU64::new(u64::MAX),
            last_wallet_read_s: std::sync::atomic::AtomicU64::new(0),
            tracked_tokens: Arc::new(RwLock::new(Vec::new())),
            resub_notify: new_subscription_notify(),
            venue_incident: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
        {
            // Startup is not a money event: no operator push, log only.
        }
        if let Some(book) = &self.book_v2 {
            if book.is_empty().await.unwrap_or(false)
                && seed_book_v2_from_wallet(book, &self.settings)
                    .await
                    .is_none()
            {
                // Trading must not depend on the shadow: seed retries at
                // the next start instead of blocking this one. (Under
                // RISK_BOOK=v2 the seed already happened in `new`, or the
                // start was refused.)
                tracing::warn!("book_v2 seed deferred: wallet unreadable");
            }
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
        // REST snapshot reconciliation for the active window's pair. The ws
        // mirror is delta-maintained, and a single missed delta corrupts the
        // ladder invisibly (freshness checks pass because later deltas keep
        // stamping the token). Re-anchoring on the venue's authoritative
        // /book bounds staleness by construction: at most one refresh
        // interval, whatever happens to the delta stream.
        {
            let p = self.clone();
            tokio::spawn(async move {
                p.book_snapshot_reconciliation_loop().await;
            });
        }
        // Venue incident gate: both live losses to date clustered around
        // venue-side events (post-maintenance backend mix, "delayed open
        // order read responses"). A half-working venue produces data that
        // passes local checks while behaving abnormally, so while the
        // venue's own status page reports an active incident we stop
        // OPENING positions. Fail-open on fetch errors - the status page
        // being unreachable is not evidence the venue is broken, and the
        // book/arbiter guards keep carrying data quality.
        if !self.settings.poly_status_url.trim().is_empty() {
            let p = self.clone();
            tokio::spawn(async move {
                p.venue_status_loop().await;
            });
        }
        // Operator bot: minimal telegram command surface (/status /trades
        // /balance /stop /start) served from inside the live process, so it
        // sees real state and /start can un-park the actual breaker.
        if matches!(self.mode, Mode::Live) {
            let p = self.clone();
            tokio::spawn(async move {
                p.operator_bot_loop().await;
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
            self.alerter
                .notify(&format!(
                    "\u{25a0} stopped \u{00b7} session {}-{} \u{00b7} {}{:.2}",
                    bs.wins,
                    bs.losses,
                    if bs.realized_pnl >= 0.0 { "+$" } else { "-$" },
                    bs.realized_pnl.abs()
                ))
                .await;
        }
        Ok(())
    }

    /// Maker timeout sweep: request venue cancels for resting GTC orders
    /// older than `CANDLE_MAKER_TIMEOUT_S`. The cancel is only an ACTION —
    /// no local lifecycle state is mutated from its response; authoritative
    /// truth arrives via the user channel and the REST reconciliation pass
    /// that runs immediately after in the same recovery tick. An ambiguous
    /// cancel therefore leaves the order treated as possibly-live, exactly
    /// like an ambiguous submit.
    async fn sweep_resting_maker_orders(&self) {
        let Some(clob) = self.clob.clone() else {
            return;
        };
        let timeout_s = self.settings.candle_maker_timeout_s.max(1.0);
        let now = nonzero_ts_or_now(0.0);
        let stale: Vec<(String, String)> = {
            let manager = self.order_manager.lock().await;
            self.live_pending_positions
                .lock()
                .await
                .keys()
                .filter_map(|intent_id| {
                    let order = manager.get(intent_id)?;
                    crate::execution::order_manager::resting_timeout_candidate(
                        order, now, timeout_s,
                    )
                    .map(|venue_id| (intent_id.clone(), venue_id.to_string()))
                })
                .collect()
        };
        for (intent_id, venue_order_id) in stale {
            let outcome = clob.write().await.cancel_order(&venue_order_id).await;
            match outcome {
                Ok(_) => tracing::info!(
                    %intent_id,
                    order_id = %venue_order_id,
                    timeout_s,
                    "resting maker order cancel accepted; awaiting authoritative reconciliation"
                ),
                Err(error) if error.kind == SubmitFailureKind::DefinitiveReject => {
                    tracing::warn!(
                        %intent_id,
                        order_id = %venue_order_id,
                        error = %error.message,
                        "venue refused maker-timeout cancel (likely already terminal); REST pass will resolve"
                    );
                }
                Err(error) => tracing::warn!(
                    %intent_id,
                    order_id = %venue_order_id,
                    error = %error.message,
                    "ambiguous maker-timeout cancel; order treated as possibly live"
                ),
            }
        }
    }

    async fn live_recovery_loop(self: Arc<Self>) {
        let mut consecutive_failures: u32 = 0;
        loop {
            self.sweep_resting_maker_orders().await;
            match self.reconcile_live_orders_once().await {
                Ok(()) => {
                    consecutive_failures = 0;
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
                    consecutive_failures += 1;
                    // A persistently failing reconciliation keeps the order
                    // path locked; surface it beyond the tracing log so a
                    // stuck canary is visible in the session record.
                    if consecutive_failures == 6 {
                        self.monitor.record_error(
                            "live_recovery_stuck",
                            &format!("{consecutive_failures} consecutive failures: {error}"),
                            true,
                        );
                    }
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

    /// True when the PUBLIC data-api shows an executed trade by our maker in
    /// this market at/after the order's creation. Independent evidence
    /// channel: the venue purges terminal FOK orders from authenticated REST
    /// within seconds while its authed /trades indexing can lag past 30s
    /// (observed live 2026-08-25: fill at :10, still unindexed at :38),
    /// whereas the public feed showed the fill immediately.
    /// Poll the venue status page and raise/lower the incident flag.
    async fn venue_status_loop(&self) {
        let url = self.settings.poly_status_url.trim().to_string();
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(c) => c,
            Err(_) => return,
        };
        loop {
            let verdict: Option<Option<String>> = async {
                let text = client
                    .get(&url)
                    .header("User-Agent", "polymomentum/1.0")
                    .send()
                    .await
                    .ok()?
                    .text()
                    .await
                    .ok()?;
                venue_status_verdict(&text).ok()
            }
            .await;
            if let Some(reason) = verdict {
                let degraded = reason.is_some();
                let was = self
                    .venue_incident
                    .swap(degraded, std::sync::atomic::Ordering::Relaxed);
                if degraded != was {
                    if let Some(reason) = reason {
                        tracing::warn!(reason = %reason, "venue status page reports an active incident or maintenance; new entries suspended");
                        self.monitor.record_error("venue_incident_gate", &reason, true);
                        if self.alerter.enabled() {
                            self.alerter
                                .notify(&format!("\u{23f8} venue {reason} \u{00b7} entries suspended"))
                                .await;
                        }
                    } else {
                        tracing::info!("venue status page clear; entries resume");
                        if self.alerter.enabled() {
                            self.alerter
                                .notify("\u{25b6} venue clear \u{00b7} entries resume")
                                .await;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    /// Every few seconds, re-anchor the active btc-updown window's two books
    /// on the venue's REST `/book`. Logs when the mirror had diverged so gap
    /// frequency becomes measurable instead of invisible.
    async fn book_snapshot_reconciliation_loop(&self) {
        const REFRESH: Duration = Duration::from_secs(4);
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_millis(2500))
            .build()
        {
            Ok(c) => c,
            Err(_) => return,
        };
        loop {
            tokio::time::sleep(REFRESH).await;
            let now_s = nonzero_ts_or_now(0.0);
            let pair: Option<(String, String, String)> = {
                self.contracts
                    .read()
                    .await
                    .iter()
                    .filter_map(|c| {
                        let end_ts = parse_end(&c.end_date).ok()?.timestamp() as f64;
                        let remaining = end_ts - now_s;
                        (0.0..=300.0).contains(&remaining).then(|| {
                            (
                                c.market.condition_id.clone(),
                                c.up_token_id.clone(),
                                c.down_token_id.clone(),
                            )
                        })
                    })
                    .next()
            };
            let Some((cid, up, down)) = pair else { continue };
            for token in [up, down] {
                if token.is_empty() {
                    continue;
                }
                let url = format!(
                    "{}/book?token_id={token}",
                    self.settings.poly_base_url
                );
                let body: Option<serde_json::Value> = match client.get(&url).send().await {
                    Ok(resp) => resp.json().await.ok(),
                    Err(_) => None,
                };
                let Some(body) = body else { continue };
                if let Some((prev_ask, rest_ask)) =
                    crate::polymarket_ws::overwrite_book_from_rest(&self.book_state, &body)
                        .await
                {
                    if prev_ask > 0.0 && rest_ask > 0.0 && (prev_ask - rest_ask).abs() > 0.02 {
                        tracing::warn!(
                            token = %short_cid(&token),
                            prev_ask,
                            rest_ask,
                            "ws book had diverged from venue REST; ladder re-anchored"
                        );
                        self.monitor
                            .record_signal_skip(&cid, "book_resync_divergence");
                    }
                }
            }
        }
    }

    async fn public_fill_evidence(&self, market: &str, created_ts: f64) -> Option<bool> {
        self.public_fill_details(market, "", created_ts)
            .await
            .map(|d| d.is_some())
    }

    /// Aggregated public-data-api trade rows matching our order: same maker,
    /// same market, BUY side, at/after order creation; token filter optional.
    /// Outer None = channel unavailable; Some(None) = positively no fill.
    async fn public_fill_details(
        &self,
        market: &str,
        token_id: &str,
        created_ts: f64,
    ) -> Option<Option<PublicFillDetails>> {
        // The public feed indexes trades under the MAKER address, which is
        // the deposit wallet in the 1271 flow - not the signing EOA. Using
        // the EOA here made every filled order look unfilled and the
        // runtime discarded real positions as killed FOKs (observed live
        // 2026-08-26 10:11: "FOK purged; no public fill within 120s").
        let maker = if self.settings.poly_funder.trim().is_empty() {
            let key = crate::signing::parse_private_key(&self.settings.private_key)?;
            format!("0x{}", hex::encode(crate::signing::address_from_key(&key)))
        } else {
            self.settings.poly_funder.trim().to_ascii_lowercase()
        };
        let url = format!(
            "https://data-api.polymarket.com/trades?user={maker}&market={market}&limit=20"
        );
        let resp = reqwest::Client::new()
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .ok()?;
        let rows: serde_json::Value = resp.json().await.ok()?;
        let cutoff = created_ts - 2.0;
        let mut size_sum = 0.0f64;
        let mut notional = 0.0f64;
        let mut latest_ts = 0.0f64;
        for t in rows.as_array()? {
            let ts = t
                .get("timestamp")
                .and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
                .map(|v| v as f64);
            let Some(ts) = ts.filter(|ts| *ts >= cutoff) else {
                continue;
            };
            if !token_id.is_empty()
                && t.get("asset").and_then(|v| v.as_str()) != Some(token_id)
            {
                continue;
            }
            if t.get("side").and_then(|v| v.as_str()) != Some("BUY") {
                continue;
            }
            let price = t.get("price").and_then(json_number);
            let size = t.get("size").and_then(json_number);
            if let (Some(price), Some(size)) = (price, size) {
                if price > 0.0 && size > 0.0 {
                    size_sum += size;
                    notional += price * size;
                    latest_ts = latest_ts.max(ts);
                }
            }
        }
        if size_sum <= 0.0 {
            return Some(None);
        }
        Some(Some(PublicFillDetails {
            size: size_sum,
            price: notional / size_sum,
            ts: latest_ts,
        }))
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
        let is_fok = current.intent.order_type == "market";
        if current.state != OrderState::Submitted || current.filled_size > 1e-9 {
            if is_fok && current.filled_size > 1e-9 {
                // In-process fills exist and the venue purged the terminal
                // FOK from REST: nothing is unaccounted. Terminalize quietly.
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
                self.persist_live_pending_orders().await?;
                self.monitor.record_order_reconciled(&OrderReconciled {
                    intent_id: order.intent.intent_id.clone(),
                    order_id: current
                        .venue_order_id
                        .clone()
                        .unwrap_or_default(),
                    source: "clob_rest.purged_terminal_fok".to_string(),
                    venue_state: "purged".to_string(),
                    filled: current.filled_size,
                    requested: current.requested_size,
                    fill_price: current.avg_fill_price,
                    fee: current.total_fees,
                    detail: "venue purged filled FOK from REST".to_string(),
                });
                return Ok(());
            }
            if is_fok {
                // Acked FOK, no in-process fills, REST purged it. Consult the
                // independent public feed before любое fail-closed действие.
                match self
                    .public_fill_details(
                        &order.intent.market_id,
                        &order.intent.token_id,
                        order.created_ts,
                    )
                    .await
                {
                    Some(Some(details)) => {
                        // The fill happened; authed indexing is lagging.
                        // Give it 90s, then book the position from the
                        // public record itself - a real fill must never
                        // wait forever on a channel that may simply not
                        // cover this maker (observed: the lost -$5.18
                        // position existed only in public data).
                        if age_s < 90.0 {
                            if let Some(pending) = self
                                .live_pending_positions
                                .lock()
                                .await
                                .get_mut(&order.intent.intent_id)
                            {
                                pending.recovery_misses = 0;
                            }
                            tracing::info!(
                                intent_id = %order.intent.intent_id,
                                "public feed confirms fill; waiting for authed indexing"
                            );
                            return Ok(());
                        }
                        let ts = nonzero_ts_or_now(0.0);
                        let fee = self
                            .live_pending_positions
                            .lock()
                            .await
                            .get(&order.intent.intent_id)
                            .and_then(|pending| pending.fill_fee(details.size, details.price))
                            .unwrap_or(0.0);
                        let booked = {
                            let mut orders = self.order_manager.lock().await;
                            orders
                                .fill(
                                    &order.intent.intent_id,
                                    details.size,
                                    details.price,
                                    fee,
                                    ts,
                                )
                                .map(|o| o.clone())
                        };
                        let booked = match booked {
                            Ok(o) => o,
                            Err(error) => {
                                tracing::error!(
                                    intent_id = %order.intent.intent_id,
                                    %error,
                                    "public-evidence fill booking failed"
                                );
                                return Ok(());
                            }
                        };
                        self.monitor.record_order_reconciled(&OrderReconciled {
                            intent_id: order.intent.intent_id.clone(),
                            order_id: booked
                                .venue_order_id
                                .clone()
                                .unwrap_or_default(),
                            source: "public_data_api.trade".to_string(),
                            venue_state: "MINED".to_string(),
                            filled: booked.filled_size,
                            requested: booked.requested_size,
                            fill_price: details.price,
                            fee,
                            detail: format!(
                                "booked from public evidence after authed indexing lag; venue_ts={:.0}",
                                details.ts
                            ),
                        });
                        self.monitor.record_order_filled(&OrderFilled {
                            intent_id: order.intent.intent_id.clone(),
                            order_id: booked.venue_order_id.clone().unwrap_or_default(),
                            filled: details.size,
                            requested: booked.requested_size,
                            fill_pct: booked.fill_pct(),
                            fill_price: details.price,
                            cost: details.size * details.price,
                            limit_price: booked.intent.limit_price.unwrap_or(details.price),
                            slippage: 0.0,
                            slippage_bps: 0.0,
                            fill_time_s: (ts - booked.created_ts).max(0.0),
                            fee,
                            n_trades: 1,
                        });
                        self.record_live_fill_position(&booked, ts, details.size, details.price)
                            .await?;
                        return Ok(());
                    }
                    // Only a POSITIVE "no fill" answer may retire the order;
                    // an unavailable evidence channel keeps it pending.
                    Some(None) if age_s >= 120.0 && misses >= 24 => {
                        // 2+ minutes, no public fill, REST purged: the FOK was
                        // killed. Resolve as reject and release the window.
                        {
                            let mut manager = self.order_manager.lock().await;
                            manager
                                .reject(
                                    &order.intent.intent_id,
                                    "FOK purged by venue with no public fill evidence",
                                    nonzero_ts_or_now(0.0),
                                )
                                .map_err(anyhow::Error::msg)?;
                        }
                        self.live_pending_positions
                            .lock()
                            .await
                            .remove(&order.intent.intent_id);
                        self.persist_live_pending_orders().await?;
                        self.traded.lock().await.remove(&order.intent.market_id);
                        self.monitor.record_order_rejected(
                            &order.intent.token_id,
                            "FOK purged; no public fill within 120s",
                            current.intent.limit_price.unwrap_or(0.0),
                            current.requested_size,
                        );
                        return Ok(());
                    }
                    _ => return Ok(()), // keep polling until the age/miss floor
                }
            }
            self.trip_breaker("live_rest_order_disappeared").await;
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
        {
            let mut log = self.trade_log.lock().await;
            log.push_back(TradeLogRecord {
                ts,
                cid: position.contract_id.clone(),
                price: fill_price,
                size: fill_size,
                cost: fill_size * fill_price,
                outcome: None,
            });
            while log.len() > 30 {
                log.pop_front();
            }
        }
        if let Some(book) = &self.book_v2 {
            let _ = book
                .post(
                    ts,
                    crate::risk::book_v2::PostingKind::Fill,
                    "band",
                    &position.contract_id,
                    -(fill_size * fill_price + position.fee),
                    fill_size,
                    fill_price,
                    "",
                    &format!("{}::fill", order.intent.intent_id),
                )
                .await;
            // Parallel sizing emulation: what the Kelly-lower policy would
            // have staked on THIS fill, compounding on its own sub-book
            // equity (skips price buckets it refuses). Lets the operator
            // watch both equity curves before switching policies.
            if let Some(band) = self.runtime_strategy.band.as_ref() {
                let sim = crate::risk::book_v2::KELLY_SIM_STRATEGY;
                let sim_equity = book.equity_for(sim).await.unwrap_or(0.0);
                match crate::risk::book_v2::kelly_lo_stake(
                    fill_price,
                    sim_equity,
                    band.stake_usd,
                ) {
                    Some(stake) if fill_size * fill_price > 0.0 => {
                        let scale = stake / (fill_size * fill_price);
                        let _ = book
                            .post(
                                ts,
                                crate::risk::book_v2::PostingKind::Fill,
                                sim,
                                &position.contract_id,
                                -(stake + position.fee * scale),
                                stake / fill_price,
                                fill_price,
                                "",
                                &format!("{}::simfill", order.intent.intent_id),
                            )
                            .await;
                    }
                    _ => {
                        self.monitor.record_v2_shadow(
                            "sim_skip",
                            serde_json::json!({
                                "cid": short_cid(&position.contract_id),
                                "price": fill_price,
                                "sim_equity": sim_equity,
                            }),
                        );
                    }
                }
            }
        }
        if self.alerter.enabled() {
            self.alerter
                .notify(&format!(
                    "\u{25b6} BUY {:.2} @ {:.2} \u{00b7} ${:.2}",
                    fill_size,
                    fill_price,
                    fill_size * fill_price
                ))
                .await;
        }
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
        let tokens_changed = {
            let mut tt = self.tracked_tokens.write().await;
            let changed = *tt != token_ids;
            *tt = token_ids.clone();
            changed
        };
        // Reconnecting drops every book until the venue resends snapshots, so
        // only churn the subscription when the token set actually changed.
        if tokens_changed {
            self.resub_notify.notify_one();
            // Evict books for tokens we no longer track: the map otherwise
            // grows without bound (~26 tokens/hour) and is cloned every cycle.
            let keep: HashSet<&String> = token_ids.iter().collect();
            self.book_state
                .write()
                .await
                .retain(|token, _| keep.contains(token));
            // Refresh the offline cid->window mapping alongside.
            let rows: Vec<serde_json::Value> = contracts
                .iter()
                .map(|c| {
                    serde_json::json!({
                        "cid": short_cid(&c.market.condition_id),
                        "end_date": c.end_date,
                        "asset": c.asset,
                        "up_token": short_cid(&c.up_token_id),
                        "down_token": short_cid(&c.down_token_id),
                    })
                })
                .collect();
            self.monitor.record_contract_map(&rows);
        }
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

            // Kill switch must stay effective even while the price feed is
            // down, so check it before any early continue. The trip parks
            // the loop below (never returns): the process must stay alive
            // for the halted heartbeat and the operator bot.
            if self.kill_switch_active() {
                self.trip_breaker("kill_switch").await;
            }
            if *self.breaker_tripped.lock().await {
                if self.maybe_rearm_paper_breaker().await {
                    continue;
                }
                // A tripped breaker parks the cycle loop while the process
                // stays alive (oracle settlement continues) - which looked
                // exactly like a healthy service from outside (observed
                // live 2026-08-31: 8 hours "active" with zero cycles).
                // A halt must be LOUD: heartbeat the halted state into the
                // journal and session every 5 minutes - ahead of the price
                // read, so the heartbeat does not depend on a live decision
                // feed (a stalled feed otherwise hid the halt behind
                // decision_feed_unavailable for the whole outage).
                let now_s = nonzero_ts_or_now(0.0);
                let last = self
                    .halted_log_s
                    .load(std::sync::atomic::Ordering::Relaxed);
                if now_s as u64 >= last + 300 {
                    self.halted_log_s
                        .store(now_s as u64, std::sync::atomic::Ordering::Relaxed);
                    let reason = self
                        .breaker_trip_reason
                        .lock()
                        .await
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    tracing::warn!(
                        %reason,
                        "candle.halted breaker tripped; cycle loop parked (operator action required)"
                    );
                    self.monitor.record_error("breaker_halted", &reason, true);
                }
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let ps = self.price_state.read().await.clone();
            let btc = if self.settings.candle_settlement_alignment_ready {
                ps.fresh_source_price("chainlink_settlement", Duration::from_secs(10))
                    .unwrap_or(0.0)
            } else {
                ps.mid_price
            };
            if btc <= 0.0 {
                // A dead decision feed halts trading fail-closed, but must be
                // visible: record once per minute rather than per cycle.
                let now_s = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let last = self.last_btc_stall_log_s.swap(now_s, std::sync::atomic::Ordering::Relaxed);
                if now_s.saturating_sub(last) >= 60 {
                    tracing::warn!("decision price feed unavailable (btc<=0); trading paused");
                    self.monitor
                        .record_error("decision_feed_unavailable", "btc<=0", true);
                } else {
                    self.last_btc_stall_log_s
                        .store(last, std::sync::atomic::Ordering::Relaxed);
                }
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
            }

            // Eager breaker check (every cycle)
            {
                let bs = *self.breaker.lock().await;
                let open_exposure = self.breaker_stress_exposure().await;
                let breaker_bankroll = self.breaker_bankroll().await;
                if let Some(reason) =
                    self.breaker_trip_reason_for(&bs, open_exposure, breaker_bankroll)
                {
                    self.trip_breaker(reason).await;
                }
            }
            // A trip parks at the top of the next cycle.
            if *self.breaker_tripped.lock().await {
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
                // Money-free challenger capture. Deliberately ahead of the
                // traded skip: a window the champion entered at 240s still
                // yields its 270s anchor. Observation only.
                self.capture_band_anchors(c, &contracts, &books, &ps, now, now_ts)
                    .await;
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

                // The band's one look at the decision second, ahead of the
                // asset-price and fresh-book gates below: a gate failing on
                // the decision cycle must not defer the look to a later,
                // uncontrolled second (the multiple-looks rule the latch
                // removes; 2026-09-02 18:14). A decision-feed stall above
                // that spans the decision second is a missed decision. The
                // elapsed is sub-second here: the loop's integer-second
                // `minutes_elapsed` rounds up and would take the look at
                // 239.x s (study doc, basis parity addendum).
                self.latch_band_decision(
                    c,
                    &ps,
                    now,
                    end,
                    window_minutes,
                    band_window_elapsed_s(end, now, window_minutes),
                )
                .await;

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

                // Frozen band family replaces the candle_momentum evaluation
                // entirely; the promoted mechanism has no confidence/z/zone
                // model to consult.
                if let Some(band) = self.runtime_strategy.band.clone() {
                    match self
                        .evaluate_band_opportunity(
                            &band,
                            c,
                            &cid,
                            &books,
                            &ps,
                            btc,
                            now_ts,
                            window_minutes,
                            minutes_elapsed,
                            minutes_left,
                            up_price,
                            down_price,
                        )
                        .await
                    {
                        Ok(true) => {
                            traded_windows.insert(c.end_date.clone());
                        }
                        Ok(false) => {}
                        Err(e) => {
                            self.monitor
                                .record_error("band_evaluation", &e.to_string(), true);
                            tracing::warn!(error = %e, cid = %short_cid(&cid), "band evaluation failed");
                        }
                    }
                    continue;
                }

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
                let books_total = books.len();
                let books_fresh = books
                    .values()
                    .filter(|b| live_book_age_seconds(now_ts, b.last_update_us).is_some())
                    .count();
                // Ages for the most time-advanced (decision-relevant) contract.
                let active_ages = contracts
                    .iter()
                    .filter_map(|c| {
                        let end = parse_end(&c.end_date).ok()?;
                        let minutes_left = (end - now).num_seconds() as f64 / 60.0;
                        (0.0..5.0).contains(&minutes_left).then(|| {
                            let age = |tid: &str| {
                                books
                                    .get(tid)
                                    .map(|b| now_ts - b.last_update_us as f64 / 1e6)
                                    .map(|a| format!("{a:.1}"))
                                    .unwrap_or_else(|| "absent".to_string())
                            };
                            format!(
                                "{}:up={},down={}",
                                short_cid(&c.market.condition_id),
                                age(&c.up_token_id),
                                age(&c.down_token_id)
                            )
                        })
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                let ws = crate::polymarket_ws::ws_counters();
                if cycle % 300 == 0 {
                    self.monitor.record_ws_health(&ws, books_total, books_fresh);
                }
                tracing::info!(
                    cycle,
                    btc,
                    cycle_ms = cycle_ms,
                    contracts = contracts.len(),
                    books_total,
                    books_fresh,
                    active_ages = %active_ages,
                    ws_frames = ws[0],
                    ws_book = ws[1],
                    ws_pc = ws[2],
                    ws_bba = ws[3],
                    ws_drop_ts = ws[4],
                    ws_drop_parse = ws[5],
                    ws_drop_stale = ws[6],
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

    /// Writes the detailed band-skip record once per (window, reason); the
    /// aggregate skip counter still counts every cycle. Returns quickly on
    /// repeats. The set is capped as a leak guard.
    async fn band_skip_with_detail(&self, cid: &str, reason: &str, detail: String) {
        self.monitor.record_signal_skip(cid, reason);
        let key = format!("{cid}:{reason}");
        let mut logged = self.band_detail_logged.lock().await;
        if logged.len() > 8192 {
            logged.clear();
        }
        if logged.insert(key) {
            drop(logged);
            self.monitor.record_band_skip_detail(cid, reason, &detail);
        }
    }

    /// Records one `band_anchor` event per (window, anchor second): the
    /// latch's margin basis at that anchor's instant (`band_basis_prices`:
    /// Binance's own ticks, composite fallback, the series used recorded
    /// as `basis`), the window open on the same basis, the live sizing
    /// target and the budget-aware quote on both sides, so challenger rules
    /// can be scored offline against the champion on the quotes that
    /// actually existed. At the decision anchor the latch and the anchor
    /// read the same cycle's series, so the race and the engine agree by
    /// construction. Reads the cycle's own book/price snapshot only: never
    /// orders, never mutates risk or breaker state, and every lookup
    /// degrades to null rather than failing.
    ///
    /// `stake_usd` is null exactly when this cycle's champion could not
    /// trade for reasons that are not the rule's: no fresh best ask on both
    /// sides (`pick_book_prices`, the `fresh_outcome_book_unavailable` skip)
    /// or the live sizing policy declining the bucket (kelly_lo, the
    /// `kelly_no_edge_bucket` skip); the offline scorer treats null as "no
    /// trade" for every rule. The quote budget is the sizing target BEFORE
    /// the per-market / available-capital / stress caps
    /// `evaluate_band_opportunity` applies, so the anchor is a rule-vs-rule
    /// comparison on a common budget, not a replica of the live champion:
    /// a capital-bound window (`band_no_capital`, or a smaller live quote)
    /// is not reproducible from the record.
    ///
    /// Band engine only: the legacy candle path seeds the shared per-asset
    /// open cache from the chainlink settlement reference, and an anchor
    /// margin of exchange mid minus chainlink open is the cross-basis sign
    /// inversion the band avoids by design (2026-08-25 09:15 window).
    async fn capture_band_anchors(
        &self,
        c: &CandleContract,
        contracts: &[CandleContract],
        books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
        ps: &PriceState,
        now: DateTime<Utc>,
        now_ts: f64,
    ) {
        let anchors = &self.settings.band_anchor_seconds;
        if anchors.is_empty() || self.runtime_strategy.band.is_none() {
            return;
        }
        let Some((elapsed_s, open_ts)) = band_anchor_elapsed(c, now) else {
            return;
        };
        let cid = c.market.condition_id.as_str();
        // Parity with the latch: an anchor whose Binance sample is not final
        // yet waits, uncaptured, for the covering tick the latch waits for
        // (the deferral bound sits inside the capture span), so the decision
        // anchor and the decision read the same series.
        if anchors
            .iter()
            .any(|&a| elapsed_s >= a && binance_sample_pending(ps, open_ts + a, a, elapsed_s))
        {
            return;
        }
        let due = {
            let mut seen = self.band_anchor_logged.lock().await;
            let due = due_band_anchors(&mut seen, anchors, cid, elapsed_s);
            if !due.is_empty() {
                seen.retain(|(k, _)| contracts.iter().any(|c| c.market.condition_id == *k));
            }
            due
        };
        if due.is_empty() {
            return;
        }

        // Same composite open lookup as the latch's fallback (the cached
        // per-window open, else the exchange-mid tick nearest the open),
        // read-only so the capture never seeds the champion's cache.
        let composite_open = {
            let cached = self
                .momentum
                .lock()
                .await
                .get(&c.asset)
                .and_then(|det| det.get_open_price(cid));
            cached.or_else(|| ps.price_near_seconds(&c.asset, open_ts, 2.0))
        };

        // Live sizing target, exactly as evaluate_band_opportunity sizes
        // (favorite from both fresh asks); the quote budget falls back to
        // the frozen target so a no-edge Kelly bucket still yields a quote.
        let band = self.runtime_strategy.band.as_ref();
        let bankroll = self.band_sizing_equity().await;
        let stake = band.and_then(|band| {
            let (up, down) = pick_book_prices(c, books, now_ts)?;
            if self.settings.band_sizing == "kelly_lo" {
                crate::risk::book_v2::kelly_lo_stake(up.max(down), bankroll, band.stake_usd)
            } else {
                Some(band.target_stake(bankroll))
            }
        });
        let quote_budget = stake.or_else(|| band.map(|band| band.target_stake(bankroll)));
        let market_tick = live_market_tick_size(
            c.market.minimum_tick_size,
            [&c.up_token_id, &c.down_token_id],
            books,
        );
        let side = |token_id: &str| {
            let book = books.get(token_id);
            let quote = quote_budget.and_then(|budget| {
                live_buy_book_quote(
                    token_id,
                    books,
                    budget,
                    self.settings.live_min_order_size_shares,
                    market_tick,
                )
            });
            json!({
                "best_ask": book.map(|b| b.best_ask).filter(|a| *a > 0.0).map(|a| round_to(a, 4)),
                "book_age_s": book
                    .and_then(|b| live_book_age_seconds(now_ts, b.last_update_us))
                    .map(|a| round_to(a, 4)),
                "vwap": quote.as_ref().map(|q| round_to(q.vwap, 4)),
                "worst": quote.as_ref().map(|q| round_to(q.worst_price, 4)),
                "shares": quote.as_ref().map(|q| round_to(q.shares, 4)),
            })
        };
        let up = side(&c.up_token_id);
        let down = side(&c.down_token_id);
        let pair_sum = match (up["best_ask"].as_f64(), down["best_ask"].as_f64()) {
            (Some(a), Some(b)) => Some(round_to(a + b, 4)),
            _ => None,
        };
        for anchor in due {
            let (basis, open, btc) =
                band_basis_prices(ps, open_ts, open_ts + f64::from(anchor), composite_open);
            let margin = open.filter(|_| btc > 0.0).map(|open| btc - open);
            let direction = margin.and_then(|m| {
                if m > 0.0 {
                    Some("up")
                } else if m < 0.0 {
                    Some("down")
                } else {
                    None
                }
            });
            self.monitor.record_band_anchor(json!({
                "cid": short_cid(cid),
                "anchor_s": anchor,
                "elapsed_s": round_to(elapsed_s, 2),
                "basis": basis,
                "btc": (btc > 0.0).then(|| round_to(btc, 4)),
                "open": open.map(|o| round_to(o, 4)),
                "margin": margin.map(|m| round_to(m, 4)),
                "direction": direction,
                "stake_usd": stake.map(|s| round_to(s, 4)),
                "quote_budget_usd": quote_budget.map(|b| round_to(b, 4)),
                "up": up.clone(),
                "down": down.clone(),
                "pair_sum": pair_sum,
            }));
        }
    }

    /// The band's ONE look at the decision second: on the first cycle with
    /// the sub-second `elapsed_s` inside the entry window, decide the window
    /// from the margin basis (`band_basis_prices`: Binance's own tick at or
    /// before the decision instant vs its tick at or before the open;
    /// composite mid only as a whole-window fallback) via
    /// `band_latch_decision` and latch it. The
    /// cycle loop calls this AHEAD of its asset-price and fresh-book gates
    /// and this never consults the venue-incident flag or the books: a
    /// window whose decision cycle is blocked by a gate is still decided on
    /// that cycle (mid 0 latches no-signal), not on whatever later cycle
    /// first clears it (the 2026-09-02 18:14 loss re-created under a
    /// gate-deferred latch). Later cycles return at the map lookup; the
    /// no-signal reason is the window's one signal detail record. The map
    /// is pruned to windows still open at each new latch, so a contract
    /// refresh that transiently omits the live market cannot re-open a
    /// decided window.
    async fn latch_band_decision(
        &self,
        c: &CandleContract,
        ps: &PriceState,
        now: DateTime<Utc>,
        end: DateTime<Utc>,
        window_minutes: f64,
        elapsed_s: f64,
    ) {
        let Some(band) = self.runtime_strategy.band.as_ref() else {
            return;
        };
        if c.asset != "BTC" || (window_minutes - 5.0).abs() > 0.01 {
            return;
        }
        if !band.in_entry_window(elapsed_s) {
            return;
        }
        let cid = c.market.condition_id.as_str();
        if self.band_latch.lock().await.contains_key(cid) {
            return;
        }
        let open_ts = end.timestamp() as f64 - window_minutes * 60.0;
        let decision_ts = open_ts + band.decision_seconds;
        // Binance's tick at or before the decision instant is final only
        // once a tick at or after it has arrived (one in-order stream). The
        // ticker runs at 1 Hz and the first in-window cycle lands ~50-100 ms
        // after the instant, so on ordinary feed latency that tick is still
        // in flight and the look would read the tick one second older,
        // labelled basis=binance. While the sample is pending the look
        // waits (`binance_sample_pending`: bounded, inside the missed
        // tolerance, so a stalled ticker still decides on what it has - at
        // or before the instant, never a later look).
        if binance_sample_pending(ps, decision_ts, band.decision_seconds, elapsed_s) {
            return;
        }

        // The band's signal basis is Binance's own tick series on both ends
        // of the sign comparison - the instrument the mechanism was
        // validated with (Binance 1 s closes; 93.2% WR at the fresh gate).
        // The composite exchange mid used until 2026-09-08 differed from it
        // by a median 3.1 / p90 10.6 USD at the decision second and flipped
        // the $50 floor in 8/204 windows (direction memo 2026-09-07, gap
        // 2); it remains the fallback when Binance has no tick at the open
        // or the decision instant, never mixed with it. The chainlink
        // point-sample basis was observed live to lag whipsaws by tens of
        // dollars at the window open, inverting the sign (2026-08-25 09:15
        // window: chainlink-basis "up" vs Binance-basis+official "down").
        // The settlement-alignment attestation governs the legacy candle
        // path only; the band ignores it by design.
        // Composite window open via the shared per-window store (the
        // fallback basis; the anchor capture reads the same store).
        let composite_open = {
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
            if det.get_open_price(cid).is_none() {
                if let Some(open_price) = ps.price_near_seconds(&c.asset, open_ts, 2.0) {
                    det.set_window_open(cid, open_price);
                }
            }
            det.get_open_price(cid)
        };
        let (basis, open_price, btc) = band_basis_prices(ps, open_ts, decision_ts, composite_open);
        let latch = band_latch_decision(band, elapsed_s, open_price, btc, basis);
        {
            let now_s = now.timestamp();
            let mut latches = self.band_latch.lock().await;
            latches.retain(|_, (_, end_s)| *end_s > now_s);
            latches.insert(cid.to_string(), (latch, end.timestamp()));
        }
        if let BandLatch::NoSignal(reason) = latch {
            let detail = match (reason, open_price) {
                ("band_decision_missed", _) => format!(
                    "elapsed_s={elapsed_s:.2} decision_s={:.0} tolerance_s={BAND_DECISION_TOLERANCE_S:.0}",
                    band.decision_seconds
                ),
                ("band_mid_unavailable", _) => format!("basis={basis}"),
                (_, None) => format!(
                    "open_ts={open_ts:.0} basis={basis} tolerance_s={BAND_BASIS_TOLERANCE_S:.1}"
                ),
                (_, Some(open)) if btc == open => format!("btc=open={btc:.2} basis={basis}"),
                (_, Some(open)) => format!(
                    "margin={:+.0} floor={:.0} btc={btc:.0} open={open:.0} basis={basis}",
                    btc - open,
                    band.min_decision_margin_usd
                ),
            };
            self.band_skip_with_detail(cid, reason, detail).await;
        }
    }

    /// One evaluation cycle of the frozen band policy for one contract.
    /// Returns Ok(true) iff an order attempt was handed to `execute_trade`
    /// (the caller then marks the window as consumed for this cycle).
    ///
    /// Entry semantics mirror the promoted replay: the signal (direction,
    /// or no-signal for the window) was latched by `latch_band_decision`
    /// from the one look at the decision second and is never re-read;
    /// attempts then run on every cycle with elapsed in [decision,
    /// decision+entry_window) on the latched side, and the first cycle
    /// whose budget-aware quote clears the band gate places the order
    /// (`execute_trade`'s traded-set makes entries one-shot per market).
    /// The settlement-alignment attestation is not consulted here:
    /// the band's signal source is the exchange mid (per preregistration)
    /// and its outcomes settle on official resolutions, not on a feed proxy.
    #[allow(clippy::too_many_arguments)]
    async fn evaluate_band_opportunity(
        self: &Arc<Self>,
        band: &BandPolicyParams,
        c: &CandleContract,
        cid: &str,
        books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
        ps: &PriceState,
        _decision_feed_btc: f64,
        now_ts: f64,
        window_minutes: f64,
        minutes_elapsed: f64,
        minutes_left: f64,
        up_price: f64,
        down_price: f64,
    ) -> Result<bool> {
        if c.asset != "BTC" || (window_minutes - 5.0).abs() > 0.01 {
            return Ok(false);
        }
        if !band.in_entry_window(minutes_elapsed * 60.0) {
            return Ok(false);
        }
        // The window was decided by `latch_band_decision` ahead of the cycle
        // gates; this path only consults the latch. A no-signal window is
        // never re-evaluated, a latched direction is never re-derived from a
        // later mid, and no latch (the loop never took the look) is fail
        // closed - the margin is not read here.
        let latched = self
            .band_latch
            .lock()
            .await
            .get(cid)
            .map(|(latch, _)| *latch);
        let (direction, open_price, decision_btc, basis) = match latched {
            Some(BandLatch::Signal {
                direction,
                open,
                btc,
                basis,
            }) => (direction, open, btc, basis),
            Some(BandLatch::NoSignal(_)) | None => return Ok(false),
        };
        // Money moves on the validated instrument only. A window decided on
        // the composite fallback keeps its latch, detail and anchor for the
        // race but is never traded: the composite mid flipped the $50 floor
        // against Binance in 8/204 windows, both live losses among them
        // (direction memo 2026-09-07, gap 2), and a degraded-feed sample is
        // not the series the rule was validated on.
        if basis != "binance" {
            self.band_skip_with_detail(
                cid,
                "band_basis_unvalidated",
                format!("basis={basis} open={open_price:.2} btc={decision_btc:.2}"),
            )
            .await;
            return Ok(false);
        }
        if self
            .venue_incident
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.band_skip_with_detail(
                &c.market.condition_id,
                "venue_incident",
                "venue status page reports an active incident; entries suspended".to_string(),
            )
            .await;
            return Ok(false);
        }

        // This cycle's composite mid is an entry-time feed-health gate only.
        // The evaluation record and the entry's signal fields carry the
        // latched decision sample (open, price and basis from
        // `band_basis_prices`), never a cross-basis margin against the mid.
        if ps.mid_price <= 0.0 {
            self.band_skip_with_detail(cid, "band_mid_unavailable", String::new())
                .await;
            return Ok(false);
        }
        let btc = decision_btc;

        let (vol_fast, vol_slow) = {
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
            (det.realized_vol(), det.slow_realized_vol())
        };
        let token_id = if direction == "up" {
            &c.up_token_id
        } else {
            &c.down_token_id
        };

        // Budget estimate mirrors execute_trade's sizing chain, additionally
        // capped at the frozen stake so the quote never exceeds it.
        let open_exposure = self.open_position_exposure().await;
        let breaker_state = *self.breaker.lock().await;
        let bankroll = self.band_sizing_equity().await;
        if self.risk_book_v2 && bankroll <= 0.0 {
            // Fail closed: an unreadable or empty wallet-anchored book must
            // not fall through to the venue-minimum stake.
            self.band_skip_with_detail(
                cid,
                "v2_equity_unavailable",
                format!("equity={bankroll:.2}"),
            )
            .await;
            return Ok(false);
        }
        let per_market = self.risk.max_per_market().await;
        let available = self
            .risk
            .available_capital_for_exposure(open_exposure)
            .await;
        let favorite_price = up_price.max(down_price);
        let target = if self.settings.band_sizing == "kelly_lo" {
            // Operator-approved 2026-09-01 (docs/risk_book_v2/): half-Kelly
            // on the Wilson LOWER bound per price bucket; the <=0.70 bucket's
            // edge CI straddles break-even, so it does not trade. One day of
            // live A/B: the sim book beat the live book by \$2.87 and dodged
            // 3 of the 5 streak losses.
            match crate::risk::book_v2::kelly_lo_stake(
                favorite_price,
                bankroll,
                band.stake_usd,
            ) {
                Some(stake) => stake,
                None => {
                    self.band_skip_with_detail(
                        cid,
                        "kelly_no_edge_bucket",
                        format!("favorite={favorite_price:.2} equity={bankroll:.2}"),
                    )
                    .await;
                    return Ok(false);
                }
            }
        } else {
            band.target_stake(bankroll)
        };
        let mut estimated_position = target.min(per_market).min(available);
        // RiskBook v2 sizing shadow: log what the Kelly-lower policy would
        // stake here (docs/risk_book_v2/sizing_study_2026-09-01.md). Uses
        // the favorite price as the bucket proxy at evaluation time.
        {
            let v2 = crate::risk::book_v2::kelly_lo_stake(
                up_price.max(down_price),
                bankroll,
                band.stake_usd,
            );
            self.monitor.record_v2_shadow(
                "sizing",
                serde_json::json!({
                    "cid": short_cid(&c.market.condition_id),
                    // Which book `equity` (and so `v1_stake`) came from.
                    "risk_book": if self.risk_book_v2 { "v2" } else { "v1" },
                    "favorite_price": up_price.max(down_price),
                    "equity": bankroll,
                    "v1_stake": estimated_position,
                    "v2_stake": v2,
                }),
            );
        }
        if let Some(stress_headroom) = breaker_state.stressed_drawdown_exposure_headroom(
            open_exposure,
            self.risk.initial_bankroll().await.max(1.0),
            self.runtime_strategy.max_projected_stressed_drawdown_pct,
        ) {
            estimated_position = estimated_position.min(stress_headroom);
        }
        if estimated_position < 1.0 {
            self.band_skip_with_detail(
                cid,
                "band_no_capital",
                format!(
                    "estimated={estimated_position:.2} bankroll={bankroll:.2} per_market={per_market:.2} available={available:.2} exposure={open_exposure:.2}"
                ),
            )
            .await;
            return Ok(false);
        }
        // Shared-wallet guard: the peer bot can drain pUSD below our stake
        // between cycles. A recent on-chain reading below stake+fees means
        // the venue would reject with an insufficient-balance permanent
        // reason and trip the breaker - skip gracefully instead.
        {
            let read_s = self
                .last_wallet_read_s
                .load(std::sync::atomic::Ordering::Relaxed);
            let now_s = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            if read_s > 0 && now_s.saturating_sub(read_s) < 900 {
                let pusd = self
                    .last_wallet_pusd_micro
                    .load(std::sync::atomic::Ordering::Relaxed) as f64
                    / 1_000_000.0;
                if pusd < estimated_position + 0.6 {
                    self.band_skip_with_detail(
                        cid,
                        "band_wallet_low",
                        format!(
                            "pusd={pusd:.2} needed={:.2} read_age_s={}",
                            estimated_position + 0.6,
                            now_s.saturating_sub(read_s)
                        ),
                    )
                    .await;
                    return Ok(false);
                }
            }
        }

        let market_tick = live_market_tick_size(
            c.market.minimum_tick_size,
            [&c.up_token_id, &c.down_token_id],
            books,
        );
        let Some(quote) = live_buy_book_quote(
            token_id,
            books,
            estimated_position,
            self.settings.live_min_order_size_shares,
            market_tick,
        ) else {
            // Decompose why no executable quote exists so offline analysis
            // can tell book absence from staleness from thin asks from a
            // budget-below-minimum-shares constraint.
            let detail = match books.get(token_id) {
                None => "book_absent".to_string(),
                Some(b) => {
                    let age = now_ts - b.last_update_us as f64 / 1e6;
                    let min_cost =
                        self.settings.live_min_order_size_shares * b.best_ask.max(0.0);
                    format!(
                        "age_s={age:.1} ask_levels={} best_ask={:.3} budget={estimated_position:.2} min_shares_cost={min_cost:.2}",
                        b.asks.len(),
                        b.best_ask
                    )
                }
            };
            self.band_skip_with_detail(cid, "band_quote_unavailable", detail)
                .await;
            return Ok(false);
        };

        // Both sides must be fresh and coherent. A binary pair's asks sum
        // to roughly 1 plus two spreads; a far larger sum means at least one
        // side is stale (live incident 2026-08-26: our 0.71/0.40 while the
        // venue had 0.415/0.545 - the stale side made the underdog look like
        // the favourite and the entry landed outside the band).
        {
            let complement = if direction == "up" {
                &c.down_token_id
            } else {
                &c.up_token_id
            };
            let other = books
                .get(complement)
                .filter(|b| live_book_age_seconds(now_ts, b.last_update_us).is_some())
                .map(|b| b.best_ask)
                .unwrap_or(0.0);
            let pair_sum = quote.vwap + other;
            if other <= 0.0 || !(0.90..=1.10).contains(&pair_sum) {
                self.band_skip_with_detail(
                    cid,
                    "band_pair_incoherent",
                    format!(
                        "side={:.4} complement={other:.4} sum={pair_sum:.4} dir={direction}",
                        quote.vwap
                    ),
                )
                .await;
                return Ok(false);
            }
        }

        if !band.quote_clears_band(quote.vwap, quote.worst_price) {
            let bound = if quote.vwap <= band.ask_floor { "low" } else { "high" };
            self.band_skip_with_detail(
                cid,
                "band_price_out_of_range",
                format!(
                    "bound={bound} vwap={:.4} worst={:.4} shares={:.2} dir={direction} btc={btc:.2} open={open_price:.2} basis={basis}",
                    quote.vwap, quote.worst_price, quote.shares
                ),
            )
            .await;
            return Ok(false);
        }

        let micro = live_microstructure(token_id, books, now_ts);
        let entry_fee_rate = c
            .market
            .effective_taker_fee_rate(self.runtime_strategy.default_fee_rate);
        let signal = MomentumSignal {
            direction: direction.to_string(),
            confidence: 1.0,
            price_change: btc - open_price,
            price_change_pct: if open_price > 0.0 {
                (btc - open_price) / open_price * 100.0
            } else {
                0.0
            },
            consistency: 1.0,
            minutes_elapsed,
            minutes_remaining: minutes_left,
            current_price: btc,
            open_price,
            z_score: 0.0,
            reversion_count: 0,
            directional_impulse_10s_bps: None,
            article_path_2m: None,
            article_path_3m: None,
            article_path_4m: None,
            article_move_2m_usd: None,
        };
        // fair_value = execution VWAP: the band mechanism carries no model
        // fair value; its edge evidence lives in the strategy registry.
        let mut decision = crate::strategy::decision::CandleDecision {
            direction: direction.to_string(),
            confidence: 1.0,
            z_score: 0.0,
            zone: "band".to_string(),
            fair_value: quote.vwap,
            market_price: quote.vwap,
            gross_edge: 0.0,
            entry_fee_per_share: entry_fee_rate * quote.vwap * (1.0 - quote.vwap),
            edge: 0.0,
            minutes_remaining: minutes_left,
            yes_no_vig: (up_price + down_price - 1.0).max(0.0),
            regime: Default::default(),
        };
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

        let eval_ts_ms = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)) as i64;
        self.monitor.record_signal_evaluation(&SignalEvaluation {
            ts_ms: eval_ts_ms,
            cid: short_cid(cid),
            asset: c.asset.clone(),
            open: open_price,
            px: btc,
            chg: signal.price_change,
            chg_pct: signal.price_change_pct,
            cons: 1.0,
            z: 0.0,
            conf: 1.0,
            reversion_count: 0,
            elapsed_min: minutes_elapsed,
            remaining_min: minutes_left,
            dir: direction.to_string(),
            vol_fast,
            vol_slow,
            implied_vol: ps.implied_vol,
            cross_boost: 0.0,
            up_price,
            down_price,
            book_spread: micro.spread,
            book_pressure: micro.pressure,
            book_bid_depth: micro.bid_depth,
            book_ask_depth: micro.ask_depth,
            zone: "band".to_string(),
            fair: decision.fair_value,
            edge: 0.0,
            decision_trade: true,
            execution_attempted: true,
            traded: false,
            skip_reason: None,
            // Not a skip: carries the executable quote (and the latched
            // margin basis) so entered windows keep their decision-time
            // quote alongside the eventual fill record.
            skip_detail: Some(format!(
                "quote vwap={:.4} worst={:.4} shares={:.2} basis={basis}",
                quote.vwap, quote.worst_price, quote.shares
            )),
        });
        let submitted = self
            .execute_trade(c, &signal, &decision, &micro, Some(quote), market_tick)
            .await?;
        if !submitted {
            // The evaluation said trade but execute_trade skipped internally;
            // make the divergence visible instead of leaving traded:false
            // ambiguous in the session log.
            self.monitor.record_signal_skip(cid, "band_execute_skipped");
        }
        Ok(submitted)
    }

    async fn execute_trade(
        self: &Arc<Self>,
        contract: &CandleContract,
        signal: &crate::strategy::momentum::MomentumSignal,
        decision: &crate::strategy::decision::CandleDecision,
        micro: &BookMicrostructure,
        taker_quote: Option<BuyBookQuote>,
        market_tick: f64,
    ) -> Result<bool> {
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
            return Ok(false);
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
                    return Ok(false);
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
                Ok(true)
            }
            Mode::Live => {
                if !*self.live_recovery_ready.lock().await {
                    tracing::warn!("live order skipped: authenticated recovery lock is active");
                    return Ok(false);
                }
                let Some(clob) = self.clob.clone() else {
                    tracing::error!(
                        "live mode but no CLOB client (missing api keys / private key)"
                    );
                    return Ok(false);
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
                        return Ok(false);
                    };
                    let Some(shares) = shares_from_budget(position, price, min_order_size) else {
                        tracing::warn!(
                            min_order_size,
                            limit_price = price,
                            position,
                            "live order skipped: below configured minimum order size"
                        );
                        return Ok(false);
                    };
                    (price, shares)
                } else {
                    let Some(quote) = taker_quote else {
                        tracing::warn!("live taker order skipped: visible L2 quote unavailable");
                        return Ok(false);
                    };
                    // Re-read the book immediately before signing. At the
                    // 240s decision the favourite reprices within seconds
                    // (measured 2026-08-26: 0.52 -> 0.64 -> 0.97 across one
                    // window), so a quote taken even a second earlier can be
                    // stale. Live incident: quoted 0.71, executed 0.47 -
                    // an entry below the band floor, outside the validated
                    // mechanism, which lost. Fail closed on any drift that
                    // would move the entry off the decision price or out of
                    // the band.
                    if let Some(band) = self.runtime_strategy.band.as_ref() {
                        // The venue's public REST book is the arbiter, not our
                        // ws mirror: a delta gap under an update storm leaves
                        // the mirror internally fresh but wrong (2026-08-26
                        // incident: mirror said 0.71 while the venue traded
                        // 0.47-0.56 for 10+ seconds, and the FOK - a spend
                        // with no lower price bound - filled below the band
                        // floor and lost). One 50ms GET per attempt is free at
                        // our one-order-per-window cadence. Fail closed on any
                        // fetch error: no authoritative price, no order.
                        let ask_now = match venue_rest_best_ask(
                            &self.settings.poly_base_url,
                            token_id,
                        )
                        .await
                        {
                            Some(ask) if ask > 0.0 => ask,
                            _ => {
                                tracing::warn!(
                                    "live taker order skipped: venue REST book unavailable"
                                );
                                self.monitor.record_signal_skip(
                                    &contract.market.condition_id,
                                    "band_rest_book_unavailable",
                                );
                                return Ok(false);
                            }
                        };
                        let drift = (ask_now - quote.vwap).abs();
                        if !band.quote_clears_band(ask_now, ask_now) || drift > 2.0 * market_tick {
                            tracing::warn!(
                                quoted = quote.vwap,
                                ask_now,
                                drift,
                                "live taker order skipped: venue REST book disagrees with decision quote"
                            );
                            self.monitor.record_signal_skip(
                                &contract.market.condition_id,
                                "band_price_moved_presubmit",
                            );
                            return Ok(false);
                        }
                    }
                    let limit_price = ceil_buy_price_to_tick(quote.worst_price, market_tick);
                    if limit_price * quote.shares > position + 1e-8 {
                        tracing::warn!(
                            limit_price,
                            shares = quote.shares,
                            position,
                            "live taker order skipped: FOK worst-case cost exceeds risk budget"
                        );
                        return Ok(false);
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
                    return Ok(false);
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
                            } else {
                                // A transient FOK kill (book moved in flight)
                                // must not burn the window: the band entry
                                // window may still be open, so allow a retry.
                                self.traded
                                    .lock()
                                    .await
                                    .remove(&contract.market.condition_id);
                            }
                            tracing::warn!(error = %truncated, "candle.trade.live.rejected");
                            return Ok(false);
                        } else {
                            *self.live_recovery_ready.lock().await = false;
                            self.monitor
                                .record_error("live_submit_ambiguous", truncated, false);
                            tracing::error!(
                                error = %truncated,
                                order_id = %short_cid(&expected_order_id),
                                "candle.trade.live.submit_ambiguous; exposure retained for REST recovery"
                            );
                        }
                    }
                }
                Ok(true)
            }
        }
    }

    /// Exposure the breaker's stress projection should count: positions in
    /// still-open windows plus unreconciled order reservations. Expired
    /// windows awaiting the oracle are EXCLUDED - they are sunk, cannot be
    /// exited (hold-to-expiry, market closed), and their outcome is already
    /// covered by the absolute-dollar stops. Counting them made venue
    /// resolution lag look like an exposure spike: 2026-08-31 17:59 a 20-min
    /// oracle delay overlapped two windows and stress-tripped a freshly
    /// actualized session (peak 0) at 53%, halting the bot for 8 hours.
    async fn breaker_stress_exposure(&self) -> f64 {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut total = 0.0f64;
        let now_s = nonzero_ts_or_now(0.0);
        for (cid, end_time, exposure) in self
            .paper_positions
            .lock()
            .await
            .iter()
            .map(|(cid, p)| (cid.clone(), p.end_time, paper_position_exposure(p)))
            .collect::<Vec<_>>()
        {
            if now_s < end_time && seen.insert(cid) {
                total += exposure;
            }
        }
        for (cid, exposure) in self
            .live_pending_positions
            .lock()
            .await
            .values()
            .map(|pending| {
                (
                    pending.position.contract_id.clone(),
                    paper_position_exposure(&pending.position),
                )
            })
            .collect::<Vec<_>>()
        {
            if seen.insert(cid) {
                total += exposure;
            }
        }
        total
    }

    /// Sums open exposure counting each contract ONCE across the three
    /// lifecycle maps. A trade moves pending-order -> position -> oracle
    /// pending, and the handoffs hold awaits between insert and remove, so
    /// a per-cycle breaker check can land while a trade sits in TWO maps.
    /// Live incident 2026-08-31 13:09: a $7.91 fill counted twice pushed
    /// stressed_pnl negative for ~3ms and false-tripped
    /// "open_exposure_stress" mid-win-streak, halting the bot for 4 hours.
    async fn open_position_exposure(&self) -> f64 {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut total = 0.0f64;
        for (cid, exposure) in self
            .paper_positions
            .lock()
            .await
            .iter()
            .map(|(cid, p)| (cid.clone(), paper_position_exposure(p)))
            .collect::<Vec<_>>()
        {
            if seen.insert(cid) {
                total += exposure;
            }
        }
        for (cid, exposure) in self
            .oracle_pending
            .lock()
            .await
            .iter()
            .map(|(cid, e)| (cid.clone(), pending_resolution_exposure(e)))
            .collect::<Vec<_>>()
        {
            if seen.insert(cid) {
                total += exposure;
            }
        }
        for (cid, exposure) in self
            .live_pending_positions
            .lock()
            .await
            .values()
            .map(|pending| {
                (
                    pending.position.contract_id.clone(),
                    paper_position_exposure(&pending.position),
                )
            })
            .collect::<Vec<_>>()
        {
            if seen.insert(cid) {
                total += exposure;
            }
        }
        total
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
                let open_exp = self.breaker_stress_exposure().await;
                let breaker_bankroll = self.breaker_bankroll().await;
                if let Some(reason) =
                    self.breaker_trip_reason_for(&bs, open_exp, breaker_bankroll)
                {
                    self.trip_breaker(reason).await;
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
            let parked = *self.breaker_tripped.lock().await;
            for (cid, mut entry) in pending {
                // A timed-out entry, or one whose realization tripped, has
                // recorded its error and tripped the breaker (the trip sites
                // exhaust `attempts`); while parked it waits for the operator
                // instead of being re-queried every pass (which re-recorded
                // the error ~1440x/day per stuck entry across a multi-day
                // hold). The first pass after /start re-tries and, still
                // failing, re-trips.
                if parked && entry.attempts >= MAX_ATTEMPTS {
                    continue;
                }
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
                                        entry.attempts = entry.attempts.max(MAX_ATTEMPTS);
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
                                entry.attempts = entry.attempts.max(MAX_ATTEMPTS);
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
                                let settled_qty = {
                                    let mut log = self.trade_log.lock().await;
                                    match log
                                        .iter_mut()
                                        .rev()
                                        .find(|r| r.cid == cid && r.outcome.is_none())
                                    {
                                        Some(rec) => {
                                            rec.outcome = Some((final_won, final_pnl));
                                            rec.size
                                        }
                                        None => 0.0,
                                    }
                                };
                                if let Some(book) = &self.book_v2 {
                                    // The book's own fill is the durable
                                    // record; the trade_log ring is
                                    // process-local (empty after a restart)
                                    // and once settled a win at payout 0
                                    // under the permanent key.
                                    let band_qty = settled_band_qty(
                                        book.fill_qty("band", &cid).await.ok().flatten(),
                                        entry.size,
                                        settled_qty,
                                    );
                                    let payout = if final_won { band_qty } else { 0.0 };
                                    let _ = book
                                        .post(
                                            nonzero_ts_or_now(0.0),
                                            crate::risk::book_v2::PostingKind::Settlement,
                                            "band",
                                            &cid,
                                            payout,
                                            band_qty,
                                            0.0,
                                            if final_won { "won" } else { "lost" },
                                            &format!("{cid}::settle"),
                                        )
                                        .await;
                                    let sim = crate::risk::book_v2::KELLY_SIM_STRATEGY;
                                    if let Ok(Some(sim_qty)) = book.fill_qty(sim, &cid).await
                                    {
                                        let sim_payout =
                                            if final_won { sim_qty } else { 0.0 };
                                        let _ = book
                                            .post(
                                                nonzero_ts_or_now(0.0),
                                                crate::risk::book_v2::PostingKind::Settlement,
                                                sim,
                                                &cid,
                                                sim_payout,
                                                sim_qty,
                                                0.0,
                                                if final_won { "won" } else { "lost" },
                                                &format!("{cid}::simsettle"),
                                            )
                                            .await;
                                    }
                                }
                                if self.alerter.enabled() {
                                    let bs = *self.breaker.lock().await;
                                    let equity =
                                        self.risk.initial_bankroll().await + bs.realized_pnl;
                                    self.alerter
                                        .notify(&format!(
                                            "{} {}{:.2} \u{00b7} equity ${equity:.2} \u{00b7} session {}-{}",
                                            if final_won { "\u{2705}" } else { "\u{274c}" },
                                            if final_pnl >= 0.0 { "+$" } else { "-$" },
                                            final_pnl.abs(),
                                            bs.wins,
                                            bs.losses
                                        ))
                                        .await;
                                }
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
                            // Realization is durable from this point: mark and
                            // persist BEFORE the breaker-trip check below, so a
                            // trip/abort between realization and the batched
                            // removal cannot replay this PnL on the next boot.
                            entry.pnl_recorded = true;
                            self.oracle_pending
                                .lock()
                                .await
                                .insert(cid.clone(), entry.clone());
                            self.persist_oracle_pending().await;
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
                            entry.attempts = entry.attempts.max(MAX_ATTEMPTS);
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
                        // Oracle-pending exposure is excluded from the stress
                        // measure, so the just-resolved entry needs no manual
                        // subtraction here.
                        let open_exposure = self.breaker_stress_exposure().await;
                        let breaker_bankroll = self.breaker_bankroll().await;
                        if let Some(reason) =
                            self.breaker_trip_reason_for(&bs, open_exposure, breaker_bankroll)
                        {
                            self.trip_breaker(reason).await;
                        }
                        if is_tie {
                            if entry.shadow {
                                tracing::warn!(cid = short_cid(&cid), "candle.oracle.shadow_tie");
                            } else {
                                self.trip_breaker("oracle_tie").await;
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
        let mut tick: u64 = 0;
        loop {
            sleep(Duration::from_secs(15)).await;
            tick += 1;
            // On-chain balance timeline: best-effort, every ~5 minutes.
            if tick % 20 == 1 && !self.settings.private_key.is_empty() {
                if let Ok(reader) = crate::data::wallet::WalletReader::for_funder(
                    &self.settings.polygon_rpc_url,
                    &self.settings.private_key,
                    &self.settings.poly_funder,
                ) {
                    if let Ok(b) = reader.fetch_balances().await {
                        self.monitor.record_wallet_snapshot(b.pusd, b.usdc_e, b.pol);
                        let micro = (b.pusd.max(0.0) * 1_000_000.0).round() as u64;
                        self.last_wallet_pusd_micro
                            .store(micro, std::sync::atomic::Ordering::Relaxed);
                        let now_s = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        self.last_wallet_read_s
                            .store(now_s, std::sync::atomic::Ordering::Relaxed);
                        if let Some(book) = &self.book_v2 {
                            let v2_equity = book.equity().await.unwrap_or(f64::NAN);
                            let v2_open = book.open_cost().await.unwrap_or(f64::NAN);
                            let bs = *self.breaker.lock().await;
                            let v1_equity =
                                self.risk.initial_bankroll().await + bs.realized_pnl;
                            let sim_equity = book
                                .equity_for(crate::risk::book_v2::KELLY_SIM_STRATEGY)
                                .await
                                .unwrap_or(f64::NAN);
                            let band_equity = book.equity_for("band").await.unwrap_or(f64::NAN);
                            self.monitor.record_v2_shadow(
                                "reconcile",
                                serde_json::json!({
                                    "wallet": b.pusd,
                                    "v1_equity": v1_equity,
                                    "v2_equity": band_equity,
                                    "v2_open_cost": v2_open,
                                    "v2_minus_wallet": band_equity - b.pusd,
                                    "v2_minus_v1": band_equity - v1_equity,
                                    "kelly_sim_equity": sim_equity,
                                    "kelly_sim_vs_live": sim_equity - band_equity,
                                }),
                            );
                            let _ = v2_equity;
                        }
                        if self.risk_book_v2 {
                            self.reconcile_v2_wallet(b.pusd).await;
                        }
                    }
                }
            }
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
            let v2_equity = match &self.book_v2 {
                Some(book) => book.equity_for("band").await.ok(),
                None => None,
            };
            self.monitor.record_risk_state(
                starting_bankroll,
                bankroll,
                exposure,
                avail,
                n_paper,
                realized_pnl,
                bs.wins,
                bs.losses,
                Some(crate::monitoring::session::RiskBookState {
                    driver: if self.risk_book_v2 { "v2" } else { "v1" },
                    v1_equity: starting_bankroll + bs.realized_pnl,
                    v2_equity,
                }),
            );
        }
    }

    /// Trip the circuit breaker: a HOLD, not an exit.
    ///
    /// The process stays alive with the cycle loop parked (`scan_loop`
    /// skips evaluation and orders while `breaker_tripped`, heartbeating
    /// `candle.halted` every 5 minutes); open positions and oracle-pending
    /// entries keep reconciling and settling, and the operator bot keeps
    /// serving `/status` and `/start`. Callers must NOT follow a trip with
    /// `stop.notify_one()`: exiting handed the halt to systemd, whose
    /// restart re-based the session and resumed trading unattended (live
    /// 2026-08-31 17:59 and 2026-09-01 13:21, after the 5-loss streak).
    ///
    /// The trip is persisted in `state.db` meta (`CANDLE_BREAKER_META_KEYS`:
    /// flag, reason, unix timestamp, breaker session state, and the
    /// cumulative live loss ledger at the trip). A restart while tripped
    /// for a reason that `breaker_trip_holds_across_restart` accepts
    /// (money, accounting integrity, operator /stop) starts parked again:
    /// the bankroll-actualization reset in `Pipeline::new` leaves the keys
    /// alone, and only `operator_rearm` (/start) or the operator's
    /// documented post-mortem clears them.
    async fn trip_breaker(&self, reason: &str) {
        let mut tripped = self.breaker_tripped.lock().await;
        let mut trip_reason = self.breaker_trip_reason.lock().await;
        let upgraded_from = if *tripped {
            // Already parked. A re-fire (the eager check, the kill-switch
            // file) is silent. A reason that holds across restarts arriving
            // while parked under one that does not (kill switch, a
            // session-relative brake) upgrades the park: the persisted
            // reason decides whether the next restart starts parked or
            // re-bases and trades, and an accounting-integrity failure
            // settled inside a kill-switch park otherwise left no trace.
            let held = trip_reason
                .as_deref()
                .map_or(true, breaker_trip_holds_across_restart);
            if held || !breaker_trip_holds_across_restart(reason) {
                return;
            }
            trip_reason.replace(reason.to_string())
        } else {
            *tripped = true;
            *trip_reason = Some(reason.to_string());
            None
        };
        drop(trip_reason);
        let tripped_at = unix_now_s();
        *self.breaker_tripped_at_s.lock().await = Some(tripped_at);
        let bs = *self.breaker.lock().await;
        let ledger = self.live_loss_ledger_prior() + bs.realized_pnl;
        for (key, value) in [
            ("candle_breaker_tripped", "1".to_string()),
            ("candle_breaker_reason", reason.to_string()),
            ("candle_breaker_tripped_at", tripped_at.to_string()),
            ("candle_breaker_ledger", format!("{ledger:.6}")),
        ] {
            if let Err(e) = self.risk.set_meta(key, &value).await {
                tracing::warn!(error = %e, key, "persist candle breaker trip failed");
            }
        }
        self.persist_breaker_state().await;
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
            upgraded_from = ?upgraded_from,
            wins = bs.wins,
            losses = bs.losses,
            pnl = bs.realized_pnl,
            open_exposure = metrics.open_exposure,
            stressed_pnl = metrics.stressed_pnl,
            "candle.circuit_breaker.tripped"
        );
        if reason != "operator_stop" {
            let was = upgraded_from
                .as_deref()
                .map(|from| format!(" (was {from})"))
                .unwrap_or_default();
            self.alerter
                .notify(&format!(
                    "\u{26d4} HALTED \u{00b7} {reason}{was} \u{00b7} session {}-{} {}{:.2} \u{00b7} /start to resume",
                    bs.wins,
                    bs.losses,
                    if bs.realized_pnl >= 0.0 { "+$" } else { "-$" },
                    bs.realized_pnl.abs()
                ))
                .await;
        }
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
        for key in CANDLE_BREAKER_META_KEYS {
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

    async fn operator_status_text(&self) -> String {
        let bs = *self.breaker.lock().await;
        let tripped = *self.breaker_tripped.lock().await;
        let reason = self
            .breaker_trip_reason
            .lock()
            .await
            .clone()
            .unwrap_or_default();
        let bankroll = self.risk.initial_bankroll().await;
        let v1_equity = bankroll + bs.realized_pnl;
        let v2_equity = match &self.book_v2 {
            Some(book) => book.equity_for("band").await.ok(),
            None => None,
        };
        let equity = if self.risk_book_v2 {
            v2_equity.unwrap_or(f64::NAN)
        } else {
            v1_equity
        };
        let cumulative = self.live_loss_ledger_prior() + bs.realized_pnl;
        let stop_at =
            -self.settings.candle_live_max_cumulative_loss_pct * self.breaker_bankroll().await;
        let venue = self
            .venue_incident
            .load(std::sync::atomic::Ordering::Relaxed);
        let state = if self.kill_switch_active() {
            "\u{26d4} kill switch".to_string()
        } else if tripped {
            format!("\u{23f9} halted ({reason})")
        } else if venue {
            "\u{23f8} waiting out venue incident".to_string()
        } else {
            "\u{25b6} trading".to_string()
        };
        let wallet_micro = self
            .last_wallet_pusd_micro
            .load(std::sync::atomic::Ordering::Relaxed);
        let position = {
            let positions = self.paper_positions.lock().await;
            positions
                .values()
                .next()
                .map(|p| format!("{:.2} @ {:.2} (${:.2})", p.size, p.entry_price, p.size * p.entry_price))
        };
        let awaiting = self.oracle_pending.lock().await.len();
        let mut out = format!(
            "{state}\nwallet ${:.2} \u{00b7} equity ${equity:.2}\nsession {}-{} {}{:.2}\nledger {}{:.2} (stop at {stop_at:.2}) \u{00b7} loss streak {}",
            wallet_micro as f64 / 1e6,
            bs.wins,
            bs.losses,
            if bs.realized_pnl >= 0.0 { "+$" } else { "-$" },
            bs.realized_pnl.abs(),
            if cumulative >= 0.0 { "+$" } else { "-$" },
            cumulative.abs(),
            bs.consecutive_losses,
        );
        match position {
            Some(p) => out.push_str(&format!("\nposition {p}")),
            None => out.push_str("\nno open position"),
        }
        out.push_str(&format!(
            "\nbook {} drives sizing \u{00b7} v1 ${v1_equity:.2} \u{00b7} v2 {}",
            if self.risk_book_v2 { "v2" } else { "v1" },
            match v2_equity {
                Some(v) => format!("${v:.2}"),
                None => "n/a".to_string(),
            }
        ));
        if let Some(book) = &self.book_v2 {
            if let (Ok(live), Ok(sim)) = (
                book.equity_for("band").await,
                book.equity_for(crate::risk::book_v2::KELLY_SIM_STRATEGY).await,
            ) {
                out.push_str(&format!(
                    "\nkelly-sim ${sim:.2} vs live ${live:.2} ({}{:.2})",
                    if sim >= live { "+" } else { "-" },
                    (sim - live).abs()
                ));
            }
        }
        if awaiting > 0 {
            out.push_str(&format!(" \u{00b7} awaiting oracle: {awaiting}"));
        }
        out
    }

    async fn operator_trades_text(&self) -> String {
        let log = self.trade_log.lock().await;
        if log.is_empty() {
            return "no trades this session".to_string();
        }
        let mut out = String::new();
        for r in log.iter().rev().take(10) {
            let t = chrono::DateTime::from_timestamp(r.ts as i64, 0)
                .map(|d| d.format("%H:%M").to_string())
                .unwrap_or_default();
            let outcome = match r.outcome {
                Some((true, pnl)) => format!("\u{2705} +${pnl:.2}"),
                Some((false, pnl)) => format!("\u{274c} -${:.2}", pnl.abs()),
                None => "\u{23f3} open".to_string(),
            };
            out.push_str(&format!(
                "{t} \u{00b7} {:.2} @ {:.2} \u{00b7} {outcome}\n",
                r.size, r.price
            ));
        }
        out.trim_end().to_string()
    }

    async fn operator_balance_text(&self) -> String {
        let bs = *self.breaker.lock().await;
        let equity = self.risk.initial_bankroll().await + bs.realized_pnl;
        match crate::data::wallet::WalletReader::for_funder(
            &self.settings.polygon_rpc_url,
            &self.settings.private_key,
            &self.settings.poly_funder,
        ) {
            Ok(reader) => match reader.fetch_balances().await {
                Ok(b) => format!(
                    "on-chain ${:.2} \u{00b7} equity ${equity:.2}\nledger {}{:.2}",
                    b.pusd,
                    if self.live_loss_ledger_prior() + bs.realized_pnl >= 0.0 { "+$" } else { "-$" },
                    (self.live_loss_ledger_prior() + bs.realized_pnl).abs()
                ),
                Err(e) => format!("on-chain read failed: {e:#}"),
            },
            Err(e) => format!("wallet reader unavailable: {e:#}"),
        }
    }

    /// Operator /stop: park trading in place. Positions keep settling, the
    /// process stays alive (so /start still works), and the parked state
    /// survives restarts. The kill-switch file remains the out-of-band hard
    /// stop for when telegram itself is unavailable.
    async fn operator_stop(&self) {
        self.trip_breaker("operator_stop").await;
    }

    /// Operator /start: clear a parked/tripped breaker in-memory AND in the
    /// persisted meta - the sqlite-only clearing we did by hand never
    /// affected the running process.
    ///
    /// The parked session's realized PnL is folded into the cross-restart
    /// loss ledger first: the restart path only folds re-based (non-held)
    /// trips, so this is where a held trip's losses reach the money floor
    /// (without it every /start handed back a full floor of headroom). A
    /// ledger that still breaches the floor parks again at once; only the
    /// documented ledger post-mortem clears that. `Err` carries the reply
    /// when trading stays halted.
    async fn operator_rearm(&self) -> Result<(), String> {
        let bs = *self.breaker.lock().await;
        if self.mode == Mode::Live && bs.realized_pnl.abs() > 1e-9 {
            let prior = self.live_loss_ledger_prior();
            let updated = prior + bs.realized_pnl;
            if let Err(e) = self
                .risk
                .set_meta("live_cumulative_realized_pnl", &format!("{updated:.6}"))
                .await
            {
                tracing::warn!(error = %e, "live loss ledger write failed; staying halted");
                return Err(format!("still halted \u{00b7} ledger write failed: {e:#}"));
            }
            self.live_loss_ledger_prior
                .store(updated.to_bits(), std::sync::atomic::Ordering::Relaxed);
            tracing::info!(
                prior,
                session = bs.realized_pnl,
                cumulative = updated,
                "live loss ledger updated on operator re-arm"
            );
        }
        *self.breaker_tripped.lock().await = false;
        *self.breaker.lock().await = BreakerState::default();
        *self.breaker_trip_reason.lock().await = None;
        for key in CANDLE_BREAKER_META_KEYS {
            let _ = self.risk.delete_meta(key).await;
        }
        let bankroll = self.breaker_bankroll().await;
        if let Some(reason) = self.cumulative_loss_trip(&BreakerState::default(), bankroll) {
            self.trip_breaker(reason).await;
            return Err(format!(
                "\u{23f9} still halted \u{00b7} ledger {:+.2} breaches stop at {:.2} \u{00b7} clear live_cumulative_realized_pnl only after the post-mortem",
                self.live_loss_ledger_prior(),
                -self.settings.candle_live_max_cumulative_loss_pct * bankroll
            ));
        }
        tracing::warn!("operator re-armed trading via telegram");
        self.monitor
            .record_error("operator_rearm", "trading re-armed via telegram", true);
        Ok(())
    }

    async fn operator_bot_loop(&self) {
        let Some(client) = crate::monitoring::telegram::TelegramClient::from_env() else {
            return;
        };
        let _ = client.set_operator_commands().await;
        let mut offset: Option<i64> = None;
        loop {
            let updates = match client.get_updates(offset, 25).await {
                Ok(u) => u,
                Err(e) => {
                    // A second consumer (legacy monitor) causes conflicts;
                    // back off rather than fight it.
                    tracing::debug!(error = %e, "telegram getUpdates failed");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
            };
            for update in updates {
                if let Some(id) = update.get("update_id").and_then(|v| v.as_i64()) {
                    offset = Some(id + 1);
                }
                let (chat_id, command, callback_id) =
                    if let Some(cb) = update.get("callback_query") {
                        (
                            cb.pointer("/message/chat/id").and_then(|v| v.as_i64()),
                            cb.get("data").and_then(|v| v.as_str()).unwrap_or(""),
                            cb.get("id").and_then(|v| v.as_str()),
                        )
                    } else if let Some(msg) = update.get("message") {
                        (
                            msg.pointer("/chat/id").and_then(|v| v.as_i64()),
                            msg.get("text").and_then(|v| v.as_str()).unwrap_or(""),
                            None,
                        )
                    } else {
                        (None, "", None)
                    };
                let authorized = chat_id.is_some_and(|id| client.is_allowed_chat(id));
                if let Some(cb_id) = callback_id {
                    let _ = client.answer_callback_query(cb_id, "").await;
                }
                if !authorized || command.is_empty() {
                    continue;
                }
                let command = command.split('@').next().unwrap_or(command).trim();
                let halted = *self.breaker_tripped.lock().await || self.kill_switch_active();
                let keyboard = crate::monitoring::telegram::minimal_keyboard(halted);
                let reply = match command {
                    "/status" | "pm:status" => self.operator_status_text().await,
                    "/trades" | "pm:trades" => self.operator_trades_text().await,
                    "/balance" | "pm:wallet" => self.operator_balance_text().await,
                    "/stop" | "pm:stop" | "pm:terminate" => {
                        self.operator_stop().await;
                        "\u{23f9} halted \u{00b7} open positions settle normally \u{00b7} /start to resume"
                            .to_string()
                    }
                    "/start" | "pm:start" => {
                        if self.kill_switch_active() {
                            format!(
                                "kill switch file is present ({}); remove it on the host first",
                                self.settings.kill_switch_path
                            )
                        } else {
                            match self.operator_rearm().await {
                                Ok(()) => "\u{25b6} trading re-armed".to_string(),
                                Err(reply) => reply,
                            }
                        }
                    }
                    _ => continue,
                };
                let halted_after =
                    *self.breaker_tripped.lock().await || self.kill_switch_active();
                let keyboard = if halted_after != halted {
                    crate::monitoring::telegram::minimal_keyboard(halted_after)
                } else {
                    keyboard
                };
                if let Err(e) = client.send_message(&reply, Some(keyboard)).await {
                    tracing::warn!(error = %e, "telegram reply failed");
                }
            }
        }
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
            // The fraction-of-bankroll stress rule contradicts the band
            // sizing contract by construction: sizing MUST stake the venue
            // minimum ($5), while 30% of an actualized sub-$16.7 bankroll is
            // less than that - so after any drawdown every single entry
            // "over-stresses" and halts the bot (death loop observed live
            // 2026-09-01 02:19: one $5.00 position, bankroll ~$13.8, trip).
            // A brake and the sizing it polices must agree: for the band the
            // stress rule becomes an ANOMALY bound - exposure is tripworthy
            // only if it exceeds what sizing could legitimately commit
            // (~one stake), which still catches double-entry bugs.
            if reason == "open_exposure_stress" {
                if let Some(band) = self.runtime_strategy.band.as_ref() {
                    if band_exposure_within_contract(
                        band,
                        bs.realized_pnl,
                        bankroll,
                        open_exposure,
                    ) {
                        return self.cumulative_loss_trip(bs, bankroll);
                    }
                    return Some("band_exposure_anomaly");
                }
            }
            return Some(reason);
        }
        self.cumulative_loss_trip(bs, bankroll)
    }

    /// Equity the band sizing compounds on: the v2 band sub-book under
    /// RISK_BOOK=v2 (0.0 when unreadable, which sizes to nothing), else
    /// the v1 effective bankroll (pinned base + session PnL).
    async fn band_sizing_equity(&self) -> f64 {
        if self.risk_book_v2 {
            match &self.book_v2 {
                Some(book) => book.equity_for("band").await.unwrap_or(0.0),
                None => 0.0,
            }
        } else {
            self.risk.effective_bankroll().await
        }
    }

    /// Base for the breaker's bankroll-relative rules and the cumulative
    /// money floor: under RISK_BOOK=v2 the band equity at the ledger's
    /// origin (wallet-anchored; `v2_floor_base_micro`), else the v1
    /// baseline (BANKROLL_USD pinned, actualized on restart).
    async fn breaker_bankroll(&self) -> f64 {
        if self.risk_book_v2 {
            let micro = self
                .v2_floor_base_micro
                .load(std::sync::atomic::Ordering::Relaxed);
            if micro == u64::MAX {
                1.0
            } else {
                (micro as f64 / 1_000_000.0).max(1.0)
            }
        } else {
            self.risk.initial_bankroll().await.max(1.0)
        }
    }

    /// RISK_BOOK=v2 wallet reconciliation: the book follows the wallet.
    /// |wallet - book| above the tolerance on two consecutive readings is a
    /// deposit, a withdrawal or an untracked fill: one `adjustment` posting
    /// (`wallet_reconcile`) brings the band sub-book to the wallet. A
    /// difference still above the halt tolerance on the reading after that
    /// adjustment means the book cannot be reconciled: `accounting_drift`
    /// trips the breaker (a money trip; holds across restarts). Readings
    /// are ~5 min apart, so the two-reading rule outlasts the observed
    /// auto-redeem lag on settled wins (< one reading, 2026-09-01/02).
    async fn reconcile_v2_wallet(&self, pusd: f64) {
        let Some(book) = &self.book_v2 else {
            return;
        };
        let Ok(equity) = book.equity_for("band").await else {
            return;
        };
        let diff = pusd - equity;
        let action = wallet_reconcile_step(
            &mut *self.v2_reconcile.lock().await,
            diff,
            self.settings.risk_book_reconcile_tolerance_usd,
            self.settings.risk_book_drift_halt_usd,
        );
        match action {
            ReconcileAction::Adjust | ReconcileAction::Reverse => {
                let posted = post_wallet_adjustment(book, diff, pusd, "wallet_reconcile").await;
                // Money that left the wallet is no longer at risk: a
                // withdrawal (or an untracked loss) lowers the floor's
                // base by the same amount. A deposit never raises it -
                // only the ledger post-mortem re-bases. A reversal gives
                // back what the reversed adjustment took (a lagged redeem
                // is not a withdrawal) and never raises it otherwise.
                let base_delta = match action {
                    ReconcileAction::Adjust if diff < 0.0 => diff,
                    ReconcileAction::Reverse if diff > 0.0 => diff,
                    _ => 0.0,
                };
                if base_delta != 0.0 {
                    let base = (self.breaker_bankroll().await + base_delta).max(0.0);
                    self.v2_floor_base_micro
                        .store(usd_to_micro(base), std::sync::atomic::Ordering::Relaxed);
                    if let Err(e) = self
                        .risk
                        .set_meta("v2_floor_base_equity", &format!("{base:.6}"))
                        .await
                    {
                        tracing::warn!(error = %e, "persist v2 floor base failed");
                    }
                }
                tracing::warn!(
                    wallet = pusd,
                    book = equity,
                    adjustment = diff,
                    posted,
                    "book_v2 wallet_reconcile: band sub-book follows the wallet"
                );
                self.monitor.record_v2_shadow(
                    "wallet_reconcile",
                    serde_json::json!({
                        "wallet": pusd,
                        "book": equity,
                        "adjustment": diff,
                        "posted": posted,
                    }),
                );
                self.alerter
                    .notify(&format!(
                        "\u{1f4d2} wallet_reconcile {}{:.2} \u{00b7} book ${equity:.2} \u{2192} wallet ${pusd:.2}",
                        if diff >= 0.0 { "+$" } else { "-$" },
                        diff.abs()
                    ))
                    .await;
            }
            ReconcileAction::Drift => {
                tracing::error!(
                    wallet = pusd,
                    book = equity,
                    diff,
                    "book_v2 cannot be reconciled to the wallet after an adjustment"
                );
                self.monitor.record_v2_shadow(
                    "accounting_drift",
                    serde_json::json!({ "wallet": pusd, "book": equity, "diff": diff }),
                );
                self.trip_breaker("accounting_drift").await;
            }
            ReconcileAction::Ok | ReconcileAction::Watch => {}
        }
    }

    /// Prior-session cumulative realized live PnL (see the field).
    fn live_loss_ledger_prior(&self) -> f64 {
        f64::from_bits(
            self.live_loss_ledger_prior
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    fn cumulative_loss_trip(&self, bs: &BreakerState, bankroll: f64) -> Option<&'static str> {
        if self.mode == Mode::Live {
            let cap_pct = self.settings.candle_live_max_cumulative_loss_pct;
            if cap_pct > 0.0
                && self.live_loss_ledger_prior() + bs.realized_pnl <= -cap_pct * bankroll.max(1.0)
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

/// True when open exposure is within what the band sizing contract could
/// legitimately commit: one target stake (venue-min floored) plus headroom
/// for fees. Exposure beyond that indicates a double-entry style bug and
/// remains tripworthy.
fn band_exposure_within_contract(
    band: &BandPolicyParams,
    realized_pnl: f64,
    bankroll: f64,
    open_exposure: f64,
) -> bool {
    let equity = (bankroll + realized_pnl).max(1.0);
    open_exposure <= band.target_stake(equity) * 1.5 + 1.0
}

/// Seconds since the window open at `now`, sub-second. The cycle loop's
/// own `minutes_left` truncates with chrono `num_seconds()`, so its elapsed
/// rounds UP to the next whole second and reads 240 from true 239.001 s
/// on; the band's decision second and its anchors are real instants (the
/// studies sample Binance's close of the second ending at open + 240), so
/// the latch and the anchor capture use this instead.
fn band_window_elapsed_s(end: DateTime<Utc>, now: DateTime<Utc>, window_minutes: f64) -> f64 {
    window_minutes * 60.0 - (end - now).num_milliseconds() as f64 / 1_000.0
}

/// Tolerance for the basis lookups: the last Binance tick at or before an
/// instant must be no older than this (the ticker runs at 1 Hz).
const BAND_BASIS_TOLERANCE_S: f64 = 2.0;

/// The band's margin basis at one instant of a window: Binance's own last
/// tick at or before the window open and at or before `target_s`
/// (`PriceState::source_price_at_or_before`, tolerance
/// `BAND_BASIS_TOLERANCE_S`) - the 1 s close series the margin studies and
/// `fresh_gate_public_v1` sample - and, only when Binance has no tick at
/// either instant, the composite exchange mid against `composite_open`
/// (the cached per-window open, else the composite tick nearest the open).
/// Both ends always come from one series: a Binance decision price against
/// a composite open would be the cross-basis margin the band avoids by
/// design. Returns (basis, open, decision price); the composite price is 0
/// when no live source exists, which the latch turns into
/// `band_mid_unavailable`.
fn band_basis_prices(
    ps: &PriceState,
    open_ts: f64,
    target_s: f64,
    composite_open: Option<f64>,
) -> (&'static str, Option<f64>, f64) {
    let binance = |at: f64| ps.source_price_at_or_before("binance", at, BAND_BASIS_TOLERANCE_S);
    match (binance(open_ts), binance(target_s)) {
        (Some(open), Some(btc)) => ("binance", Some(open), btc),
        _ => ("composite", composite_open, ps.mid_price),
    }
}

/// Anchor gating for one contract: BTC 5-minute windows only, and only once
/// the window is open (the contract list runs up to an hour ahead, and a
/// not-yet-open window must not satisfy a `0` anchor). Returns the
/// sub-second elapsed since the window open (`band_window_elapsed_s`, the
/// latch's clock) and the open timestamp.
fn band_anchor_elapsed(c: &CandleContract, now: DateTime<Utc>) -> Option<(f64, f64)> {
    if c.asset != "BTC" {
        return None;
    }
    let end = parse_end(&c.end_date).ok()?;
    let window_minutes = estimate_window_minutes(&c.window_description);
    if (window_minutes - 5.0).abs() > 0.01 {
        return None;
    }
    let elapsed_s = band_window_elapsed_s(end, now, window_minutes);
    if elapsed_s < 0.0 {
        return None;
    }
    Some((elapsed_s, end.timestamp() as f64 - window_minutes * 60.0))
}

/// Anchors due for `cid` at `elapsed_s` that were not captured yet, marked
/// captured on return. The cycle loop calls this BEFORE its traded-set
/// skip, so the traded state is never consulted here.
fn due_band_anchors(
    seen: &mut HashSet<(String, u32)>,
    anchors: &[f64],
    cid: &str,
    elapsed_s: f64,
) -> Vec<u32> {
    anchors
        .iter()
        .filter(|&&a| elapsed_s >= a && elapsed_s < a + BAND_ANCHOR_SPAN_S)
        .map(|&a| a as u32)
        .filter(|&a| seen.insert((cid.to_string(), a)))
        .collect()
}

/// Per-window band decision, made on the first cycle inside the entry
/// window and never revisited (`band_latch_decision`).
#[derive(Debug, Clone, Copy, PartialEq)]
enum BandLatch {
    /// The window never trades: the decision cycle came too late, or at
    /// that cycle the mid or the open was unavailable, btc == open, or
    /// |margin| was below the floor. Payload: the skip reason, recorded
    /// once as the window's detail.
    NoSignal(&'static str),
    /// Momentum side at the decision second, the window open it was
    /// measured against, the decision price, and the series both came from
    /// (`band_basis_prices`); later cycles wait for an in-band ask on this
    /// side.
    Signal {
        direction: &'static str,
        open: f64,
        btc: f64,
        basis: &'static str,
    },
}

/// Cycles run every ~100 ms on a sub-second elapsed, so a window's first
/// in-window cycle lands within ~100 ms after the decision second (plus
/// cycle overrun). A first look later than this means the loop was not
/// evaluating across the decision second (decision feed stall, restart, the
/// window absent from a contract refresh): the decision is MISSED, never
/// taken late, because a late look samples a second the rule was not
/// validated on.
const BAND_DECISION_TOLERANCE_S: f64 = 2.0;

/// How long past the decision second the look may wait for Binance's tick
/// at or after the instant (which makes the at-or-before sample final);
/// under the missed tolerance so the bounded look still lands in time.
/// The sample itself never moves: it is the tick at or before the instant.
const BAND_DECISION_DEFER_S: f64 = 1.0;

/// True while Binance's sample at `instant_s` is not final: the feed is
/// live there (a tick within the basis tolerance at or before it) but no
/// tick at or after it has arrived yet, and `elapsed_s` is still inside the
/// deferral bound after `anchor_s` (the window second the instant belongs
/// to). A dead or stalled feed never defers.
fn binance_sample_pending(ps: &PriceState, instant_s: f64, anchor_s: f64, elapsed_s: f64) -> bool {
    elapsed_s < anchor_s + BAND_DECISION_DEFER_S
        && ps
            .source_price_at_or_before("binance", instant_s, BAND_BASIS_TOLERANCE_S)
            .is_some()
        && !ps
            .source_latest_ts("binance")
            .is_some_and(|ts| ts >= instant_s)
}

/// The band's ONE look at the decision second. The rule was validated on a
/// single sample per window, |close(open + decision_s) - open| >= floor,
/// while re-reading the mid every cycle across the entry window and firing
/// on the first crossing is a multiple-looks rule with a lower accuracy on
/// a smaller margin (scripts/margin_latch_study.py; live loss 2026-09-02
/// 18:14: -15 at the decision cycle, -50 on a flash dip at 247 s, -9 at
/// 270 s, resolved against). Taken by `latch_band_decision` ahead of every
/// cycle gate, so a gate failing on the decision cycle latches no-signal
/// (mid 0 -> `band_mid_unavailable`) rather than deferring the look; a look
/// later than the tolerance is `band_decision_missed`. With floor 0 (legacy
/// artifacts) the direction is still fixed at that one look.
/// docs/risk_book_v2/margin_latch_study_2026-09-03.md.
fn band_latch_decision(
    band: &BandPolicyParams,
    elapsed_s: f64,
    open: Option<f64>,
    btc: f64,
    basis: &'static str,
) -> BandLatch {
    if elapsed_s > band.decision_seconds + BAND_DECISION_TOLERANCE_S {
        return BandLatch::NoSignal("band_decision_missed");
    }
    if btc <= 0.0 {
        return BandLatch::NoSignal("band_mid_unavailable");
    }
    let Some(open) = open else {
        return BandLatch::NoSignal("band_open_price_unavailable");
    };
    if btc == open {
        return BandLatch::NoSignal("band_no_direction");
    }
    if band.min_decision_margin_usd > 0.0 && (btc - open).abs() < band.min_decision_margin_usd {
        return BandLatch::NoSignal("band_margin_below_floor");
    }
    BandLatch::Signal {
        direction: if btc > open { "up" } else { "down" },
        open,
        btc,
        basis,
    }
}

fn round_to(x: f64, places: i32) -> f64 {
    let scale = 10f64.powi(places);
    (x * scale).round() / scale
}

fn live_book_age_seconds(now_ts: f64, last_update_us: u64) -> Option<f64> {
    let age = now_ts - last_update_us as f64 / 1_000_000.0;
    // Venue timestamps run slightly ahead of the local clock at times; a
    // small negative age is clock skew, not staleness (observed live:
    // -0.4..-0.6s on the most active pair, which silently rejected every
    // decision-time book). Intake clamps timestamps to now+10s.
    (age.is_finite() && (-10.0..30.0).contains(&age)).then_some(age.max(0.0))
}

#[derive(Debug, Clone)]
struct TradeLogRecord {
    ts: f64,
    cid: String,
    price: f64,
    size: f64,
    cost: f64,
    outcome: Option<(bool, f64)>, // (won, pnl) once settled
}

#[derive(Debug, Clone, Copy)]
struct PublicFillDetails {
    size: f64,
    price: f64,
    ts: f64,
}

/// Best ask from the venue's public REST book. Authoritative pre-submit
/// price check for the band path; short timeout so a venue hiccup costs one
/// retry cycle, not the whole entry window.
async fn venue_rest_best_ask(base_url: &str, token_id: &str) -> Option<f64> {
    let url = format!("{base_url}/book?token_id={token_id}");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(1500))
        .build()
        .ok()?;
    let body: serde_json::Value = client.get(&url).send().await.ok()?.json().await.ok()?;
    body.get("asks")?
        .as_array()?
        .iter()
        .filter_map(|level| level.get("price")?.as_str()?.parse::<f64>().ok())
        .filter(|price| price.is_finite() && *price > 0.0)
        .fold(None, |low: Option<f64>, price| {
            Some(low.map_or(price, |l| l.min(price)))
        })
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

/// RiskBook v2 ledger next to the v1 state db.
pub fn book_v2_path(settings: &Settings) -> PathBuf {
    std::path::Path::new(&settings.state_db_path)
        .parent()
        .map(|d| d.join("book_v2.db"))
        .unwrap_or_else(|| PathBuf::from("book_v2.db"))
}

/// Seed an empty v2 book from the WALLET, not from v1: v1's pinned-base
/// equity is not wallet-anchored (that is the disease v2 cures), and
/// seeding from it imported the discrepancy into the cure (observed live
/// 2026-09-01: v2 opened $6.27 under chain). Both sub-books get the SAME
/// opening balance at the SAME instant so their curves differ only by
/// policy. Returns the opening balance, None when the wallet is unreadable.
/// The funder wallet's pUSD, None when the reader or the RPC fails.
async fn read_wallet_pusd(settings: &Settings) -> Option<f64> {
    Some(
        crate::data::wallet::WalletReader::for_funder(
            &settings.polygon_rpc_url,
            &settings.private_key,
            &settings.poly_funder,
        )
        .ok()?
        .fetch_balances()
        .await
        .ok()?
        .pusd,
    )
}

/// One `adjustment` posting per sub-book bringing the band book to the
/// wallet by `diff`; the kelly sim receives the same deposit/withdrawal so
/// the two curves keep differing by policy only. One posting per (note,
/// second, wallet reading): a replay of the same reading is a no-op, a new
/// reading in the same second is a new event. Returns whether the band
/// posting was new.
async fn post_wallet_adjustment(
    book: &crate::risk::book_v2::BookV2,
    diff: f64,
    pusd: f64,
    note: &str,
) -> bool {
    let ts = nonzero_ts_or_now(0.0);
    let key = format!("{note}::{}::{}", ts as i64, usd_to_micro(pusd));
    let mut posted = false;
    for (strategy, key) in [
        ("band", key.clone()),
        (crate::risk::book_v2::KELLY_SIM_STRATEGY, format!("{key}::sim")),
    ] {
        match book
            .post(
                ts,
                crate::risk::book_v2::PostingKind::Adjustment,
                strategy,
                "",
                diff,
                0.0,
                0.0,
                note,
                &key,
            )
            .await
        {
            Ok(new) => posted |= new && strategy == "band",
            Err(e) => tracing::warn!(error = %e, strategy, note, "book_v2 adjustment failed"),
        }
    }
    posted
}

async fn seed_book_v2_from_wallet(
    book: &crate::risk::book_v2::BookV2,
    settings: &Settings,
) -> Option<f64> {
    let opening = read_wallet_pusd(settings).await?;
    let ts = nonzero_ts_or_now(0.0);
    for (strategy, key, note) in [
        ("band", "import_wallet", "opening balance = on-chain wallet"),
        (
            crate::risk::book_v2::KELLY_SIM_STRATEGY,
            "import_sim",
            "kelly-sim opening balance = on-chain wallet",
        ),
    ] {
        let _ = book
            .post(
                ts,
                crate::risk::book_v2::PostingKind::Import,
                strategy,
                "",
                opening,
                0.0,
                0.0,
                note,
                key,
            )
            .await;
    }
    tracing::info!(opening, "book_v2 seeded from wallet");
    Some(opening)
}

fn usd_to_micro(usd: f64) -> u64 {
    (usd.max(0.0) * 1_000_000.0).round() as u64
}

/// State of the v2 wallet reconciliation (`Pipeline::reconcile_v2_wallet`).
#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct WalletReconcile {
    /// Consecutive readings with |wallet - book| above the tolerance.
    streak: u32,
    /// The previous reading posted an adjustment: this reading verifies it.
    adjusted: bool,
    /// The amount that adjustment posted (a reversal of it on the
    /// verifying reading is the wallet catching up, not drift).
    last_adjustment: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileAction {
    /// Within tolerance.
    Ok,
    /// First reading outside the tolerance; wait for confirmation.
    Watch,
    /// Second consecutive reading outside: post the difference.
    Adjust,
    /// The reading after an adjustment reversed it (the wallet caught up
    /// with the book after all): post the reversal, undo what the
    /// adjustment did to the floor base.
    Reverse,
    /// Still beyond the halt tolerance right after an adjustment.
    Drift,
}

fn wallet_reconcile_step(
    state: &mut WalletReconcile,
    diff: f64,
    tolerance: f64,
    halt: f64,
) -> ReconcileAction {
    let verifying = std::mem::replace(&mut state.adjusted, false);
    if verifying && diff.abs() > halt {
        // The difference reversed the adjustment just posted: the wallet
        // caught up with the book after all (an auto-redeem landing on the
        // reading after two lagged ones; the smallest win payout, $5 at
        // the cap, already exceeds the default halt). Post the reversal,
        // no drift.
        if (diff + state.last_adjustment).abs() <= tolerance {
            state.streak = 0;
            state.adjusted = true;
            state.last_adjustment = diff;
            return ReconcileAction::Reverse;
        }
        *state = WalletReconcile::default();
        return ReconcileAction::Drift;
    }
    if diff.abs() <= tolerance {
        state.streak = 0;
        return ReconcileAction::Ok;
    }
    state.streak += 1;
    if state.streak < 2 {
        return ReconcileAction::Watch;
    }
    state.streak = 0;
    state.adjusted = true;
    state.last_adjustment = diff;
    ReconcileAction::Adjust
}

async fn try_wallet_bankroll(settings: &Settings) -> Option<f64> {
    if settings.private_key.is_empty() {
        return None;
    }
    let r = crate::data::wallet::WalletReader::for_funder(
        &settings.polygon_rpc_url,
        &settings.private_key,
        &settings.poly_funder,
    )
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

/// `state.db` meta keys that carry a breaker trip across restarts (see
/// `Pipeline::trip_breaker`). Cleared together by the operator re-arm, the
/// paper re-arms and the non-held actualization reset; never touched by a
/// restart that holds the trip.
const CANDLE_BREAKER_META_KEYS: [&str; 5] = [
    "candle_breaker_tripped",
    "candle_breaker_state",
    "candle_breaker_reason",
    "candle_breaker_tripped_at",
    "candle_breaker_ledger",
];

/// Whether a persisted breaker trip survives a process restart parked
/// (see `Pipeline::trip_breaker`). Money trips (`live_cumulative_loss`,
/// `consecutive_losses`, `accounting_drift`), bug trips (`band_exposure_anomaly` and the
/// accounting-integrity family: fee, journal, oracle and order-evidence
/// failures) and the operator's `/stop` hold. The kill switch does not: its
/// file is the persistent state and re-trips on the first cycle. The
/// session-relative brakes (`open_exposure_stress`, `session_loss_floor`,
/// `realized_drawdown`, `win_rate_low`) keep their pre-existing behaviour:
/// the bankroll actualization re-bases the session on restart. Fail-closed:
/// any reason not listed here holds.
fn breaker_trip_holds_across_restart(reason: &str) -> bool {
    !matches!(
        reason,
        "kill_switch"
            | "open_exposure_stress"
            | "session_loss_floor"
            | "realized_drawdown"
            | "win_rate_low"
    )
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
        // Small negative age is venue clock skew, tolerated and clamped to 0.
        assert_eq!(live_book_age_ms(&token_id, &books, 99.0), Some(0.0));
        // A far-future timestamp is implausible and still rejected.
        assert_eq!(live_book_age_ms(&token_id, &books, 85.0), None);
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

    fn band_params() -> BandPolicyParams {
        BandPolicyParams {
            family: BAND_FAMILY.to_string(),
            decision_seconds: 240.0,
            entry_window_seconds: 30.0,
            ask_floor: 0.55,
            ask_cap: 0.92,
            stake_usd: 5.0,
            min_decision_margin_usd: 0.0,
            position_pct: 1.0,
        }
    }

    #[test]
    fn band_stopping_policy_halts_only_on_money_and_streak() {
        // The ground-up band policy: session floor, peak drawdown, and
        // win-rate must NEVER halt; only the streak (and, in the pipeline,
        // the cumulative floor / anomaly / integrity trips) may.
        let cfg = BreakerConfig {
            min_trades: u32::MAX,
            min_win_rate: 0.0,
            max_drawdown_pct: f64::INFINITY,
            max_session_loss_pct: 0.0,
            max_consecutive_losses: 5,
        };
        let mut s = BreakerState::default();
        // A brutal but streak-free session: would have tripped
        // session_loss_floor (-20 < -0.6*19) and realized_drawdown
        // (peak 12 -> deep negative is far beyond 30% of peak equity).
        s.record_resolution(true, 12.0);
        for _ in 0..4 {
            s.record_resolution(false, -5.0);
        }
        s.record_resolution(true, 0.5);
        for _ in 0..4 {
            s.record_resolution(false, -5.0);
        }
        assert!(s.realized_pnl < -19.0);
        assert_eq!(s.should_trip(&cfg, 5.0, 19.0), None);
        // The streak still halts.
        s.record_resolution(false, -5.0);
        assert_eq!(
            s.should_trip(&cfg, 5.0, 19.0),
            Some("consecutive_losses")
        );
    }

    #[test]
    fn band_stress_override_death_loop_regression() {
        // Live 2026-09-01 02:19: bankroll actualized to ~$13.8 after a loss,
        // a single venue-minimum $5.00 entry exceeded 30% of bankroll and
        // open_exposure_stress halted the bot on EVERY entry. The sizing
        // contract makes that exposure legitimate; only exposure beyond
        // ~one stake indicates an actual bug.
        let band = BandPolicyParams {
            family: BAND_FAMILY.to_string(),
            decision_seconds: 240.0,
            entry_window_seconds: 30.0,
            ask_floor: 0.55,
            ask_cap: 0.92,
            stake_usd: 25.0,
            min_decision_margin_usd: 0.0,
            position_pct: 0.25,
        };
        // one $5 position on a drawn-down bankroll: legitimate
        assert!(band_exposure_within_contract(&band, 0.0, 13.8, 5.0));
        // fee dust on top: still legitimate
        assert!(band_exposure_within_contract(&band, 0.0, 13.8, 5.2));
        // two concurrent stakes (double-entry bug): anomaly
        assert!(!band_exposure_within_contract(&band, 0.0, 13.8, 10.0));
        // compounded equity raises the legitimate bound with the stake
        assert!(band_exposure_within_contract(&band, 12.3, 19.0, 7.9));
    }

    #[test]
    fn band_target_stake_compounds_between_floor_and_cap() {
        let mut p = band_params();
        p.stake_usd = 25.0;
        p.position_pct = 0.25;
        assert_eq!(p.target_stake(20.0), 5.0); // starting equity -> canary size
        assert_eq!(p.target_stake(10.0), 5.0); // drawdown floors at venue min
        assert_eq!(p.target_stake(40.0), 10.0); // compounds with realized PnL
        assert_eq!(p.target_stake(200.0), 25.0); // capped until next review
        // Legacy fixed-stake artifact (pct defaults to 1.0): cap binds.
        let legacy: BandPolicyParams =
            serde_json::from_value(serde_json::to_value(band_params()).unwrap()).unwrap();
        assert_eq!(legacy.target_stake(20.0), 5.0);

        let mut bad = band_params();
        bad.position_pct = 0.0;
        assert!(bad.validate().is_err());
    }

    fn band_anchor_contract(cid: &str, end_date: &str) -> CandleContract {
        CandleContract {
            market: crate::data::models::Market {
                condition_id: cid.to_string(),
                ..Default::default()
            },
            up_token_id: "up".to_string(),
            down_token_id: "down".to_string(),
            up_price: 0.5,
            down_price: 0.5,
            end_date: end_date.to_string(),
            hours_left: 0.0,
            volume: 0.0,
            liquidity: 0.0,
            window_description: "Bitcoin Up or Down - April 4, 3:45AM-3:50AM ET".to_string(),
            asset: "BTC".to_string(),
        }
    }

    #[test]
    fn band_anchor_recorded_once_per_cid_and_anchor() {
        let anchors = [180.0, 210.0, 240.0, 270.0];
        let mut seen = HashSet::new();
        // First cycle inside the [240, 244) span records the anchor.
        assert_eq!(
            due_band_anchors(&mut seen, &anchors, "cid-a", 240.0),
            vec![240]
        );
        // Later cycles in the same span, or after it, never re-record.
        assert!(due_band_anchors(&mut seen, &anchors, "cid-a", 241.5).is_empty());
        assert!(due_band_anchors(&mut seen, &anchors, "cid-a", 243.99).is_empty());
        assert!(due_band_anchors(&mut seen, &anchors, "cid-a", 250.0).is_empty());
        // A missed span is not back-filled; the next anchor is independent.
        assert_eq!(
            due_band_anchors(&mut seen, &anchors, "cid-a", 270.0),
            vec![270]
        );
        assert!(due_band_anchors(&mut seen, &anchors, "cid-a", 272.0).is_empty());
        // Another window has its own (cid, anchor) keys.
        assert_eq!(
            due_band_anchors(&mut seen, &anchors, "cid-b", 241.0),
            vec![240]
        );
        // Span bounds: inclusive start, exclusive end; empty list disables.
        assert!(due_band_anchors(&mut seen, &anchors, "cid-c", 239.99).is_empty());
        assert!(due_band_anchors(&mut seen, &anchors, "cid-c", 244.0).is_empty());
        assert!(due_band_anchors(&mut seen, &[], "cid-d", 240.0).is_empty());
        assert_eq!(seen.len(), 3);
    }

    #[test]
    fn band_anchor_traded_window_still_gets_later_anchors() {
        let cid = "0xabc".to_string();
        let end = parse_end("2026-09-02T10:05:00Z").unwrap();
        let c = band_anchor_contract(&cid, "2026-09-02T10:05:00Z");
        let anchors = [240.0, 270.0];
        let mut seen = HashSet::new();
        let mut traded_set: HashSet<String> = HashSet::new();

        // 240s: champion decision second.
        let (elapsed_s, open_ts) =
            band_anchor_elapsed(&c, end - chrono::Duration::seconds(60)).unwrap();
        assert_eq!(elapsed_s, 240.0);
        assert_eq!(open_ts, end.timestamp() as f64 - 300.0);
        assert_eq!(
            due_band_anchors(&mut seen, &anchors, &cid, elapsed_s),
            vec![240]
        );

        // The champion fills and the loop adds the cid to `traded`. The
        // anchor capture runs BEFORE the traded-set skip and keys on
        // (cid, anchor), so the 270s anchor is still captured.
        traded_set.insert(cid.clone());
        let (elapsed_s, _) = band_anchor_elapsed(&c, end - chrono::Duration::seconds(29)).unwrap();
        assert_eq!(elapsed_s, 271.0);
        assert!(traded_set.contains(&cid));
        assert_eq!(
            due_band_anchors(&mut seen, &anchors, &cid, elapsed_s),
            vec![270]
        );

        // A window that has not opened yet is not anchored, even for a `0`
        // anchor: the contract list runs up to an hour ahead.
        assert!(band_anchor_elapsed(&c, end - chrono::Duration::seconds(301)).is_none());
        assert!(band_anchor_elapsed(&c, end - chrono::Duration::minutes(50)).is_none());
        // (due_band_anchors alone would fire a `0` anchor at elapsed 0.)
        assert_eq!(
            due_band_anchors(&mut HashSet::new(), &[0.0, 240.0], "future", 0.0),
            vec![0]
        );
        let (elapsed_s, _) = band_anchor_elapsed(&c, end - chrono::Duration::seconds(300)).unwrap();
        assert_eq!(elapsed_s, 0.0);
        let (elapsed_s, _) = band_anchor_elapsed(&c, end - chrono::Duration::seconds(299)).unwrap();
        assert!((elapsed_s - 1.0).abs() < 1e-9);
        assert_eq!(
            due_band_anchors(&mut HashSet::new(), &[0.0, 240.0], &cid, elapsed_s),
            vec![0]
        );

        // Only BTC 5-minute windows are anchored.
        let mut eth = c.clone();
        eth.asset = "ETH".to_string();
        assert!(band_anchor_elapsed(&eth, end - chrono::Duration::seconds(60)).is_none());
        let mut m15 = c.clone();
        m15.window_description = "Bitcoin Up or Down - April 4, 3:45AM-4:00AM ET".to_string();
        assert!(band_anchor_elapsed(&m15, end - chrono::Duration::seconds(60)).is_none());
        let mut bad_end = c.clone();
        bad_end.end_date = "not-a-date".to_string();
        assert!(band_anchor_elapsed(&bad_end, end).is_none());
    }

    #[test]
    fn band_latch_decision_is_the_point_rule() {
        let mut band = band_params();
        band.min_decision_margin_usd = 50.0;
        let open = 110_000.0;
        let signal = |direction: &'static str, btc: f64| BandLatch::Signal {
            direction,
            open,
            btc,
            basis: "binance",
        };
        let at = |elapsed_s: f64, open: Option<f64>, btc: f64| {
            band_latch_decision(&band, elapsed_s, open, btc, "binance")
        };

        // The 2026-09-02 18:14 samples: the decision second says no, the
        // 247 s flash dip on its own would say DOWN - it is the latch, not
        // the sample, that keeps the window closed (the async tests below).
        assert_eq!(
            at(240.0, Some(open), open - 15.0),
            BandLatch::NoSignal("band_margin_below_floor")
        );
        assert_eq!(
            at(240.0, Some(open), open - 50.24),
            signal("down", open - 50.24)
        );
        // A look later than the tolerance is a missed decision, never a
        // late one; the tolerance itself is inclusive.
        assert_eq!(
            at(247.0, Some(open), open - 50.24),
            BandLatch::NoSignal("band_decision_missed")
        );
        assert_eq!(
            at(242.0, Some(open), open + 60.0),
            signal("up", open + 60.0)
        );
        assert_eq!(
            at(242.01, Some(open), open + 60.0),
            BandLatch::NoSignal("band_decision_missed")
        );
        // Remaining no-signal cases, the floor bound, and the legacy floor
        // 0: the direction is still fixed at the one look.
        assert_eq!(
            at(240.0, Some(open), 0.0),
            BandLatch::NoSignal("band_mid_unavailable")
        );
        assert_eq!(
            at(240.0, None, open),
            BandLatch::NoSignal("band_open_price_unavailable")
        );
        assert_eq!(
            at(240.0, Some(open), open),
            BandLatch::NoSignal("band_no_direction")
        );
        assert_eq!(
            at(240.0, Some(open), open + 49.99),
            BandLatch::NoSignal("band_margin_below_floor")
        );
        assert_eq!(
            at(240.0, Some(open), open + 50.0),
            signal("up", open + 50.0)
        );
        assert_eq!(
            band_latch_decision(&band_params(), 240.0, Some(open), open - 0.5, "composite"),
            BandLatch::Signal {
                direction: "down",
                open,
                btc: open - 0.5,
                basis: "composite",
            }
        );
    }

    #[test]
    fn band_window_elapsed_is_sub_second() {
        use chrono::Timelike;
        let end = Utc::now().with_nanosecond(0).unwrap() + chrono::Duration::seconds(120);
        let at = |before_end_ms: i64| end - chrono::Duration::milliseconds(before_end_ms);
        assert!((band_window_elapsed_s(end, at(60_400), 5.0) - 239.6).abs() < 1e-9);
        assert!((band_window_elapsed_s(end, at(59_950), 5.0) - 240.05).abs() < 1e-9);
        assert!((band_window_elapsed_s(end, at(60_000), 5.0) - 240.0).abs() < 1e-9);
        // The loop's own arithmetic (integer seconds left) already reads
        // 240 at true 239.6 s - the look the latch no longer takes.
        let loop_elapsed = 5.0 - (end - at(60_400)).num_seconds() as f64 / 60.0;
        assert!((loop_elapsed * 60.0 - 240.0).abs() < 1e-9);
        // The anchor clock is the same one.
        let c = band_anchor_contract(
            "0xelapsed",
            &end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        let (elapsed_s, _) = band_anchor_elapsed(&c, at(60_400)).unwrap();
        assert!((elapsed_s - 239.6).abs() < 1e-9);
        assert!(due_band_anchors(&mut HashSet::new(), &[240.0], "0xelapsed", elapsed_s).is_empty());
        let (elapsed_s, _) = band_anchor_elapsed(&c, at(59_950)).unwrap();
        assert_eq!(
            due_band_anchors(&mut HashSet::new(), &[240.0], "0xelapsed", elapsed_s),
            vec![240]
        );
    }

    #[test]
    fn band_basis_prefers_binance_and_never_mixes() {
        let now_ms = Utc::now().timestamp_millis();
        let open_ts = (now_ms - 300_000) as f64 / 1_000.0;
        let decision_ts = open_ts + 240.0;
        let mut ps = PriceState::new();
        ps.mid_price = 110_040.0;
        // No Binance series at all: composite, with the caller's open.
        assert_eq!(
            band_basis_prices(&ps, open_ts, decision_ts, Some(110_000.0)),
            ("composite", Some(110_000.0), 110_040.0)
        );
        // Binance at the open only: still composite (no cross-basis margin).
        // (`update_at` also feeds the composite; the test pins the mid.)
        ps.update_at("binance", 109_990.0, (open_ts * 1_000.0) as i64 - 300);
        ps.mid_price = 110_040.0;
        assert_eq!(
            band_basis_prices(&ps, open_ts, decision_ts, Some(110_000.0)),
            ("composite", Some(110_000.0), 110_040.0)
        );
        // Binance at both instants: its own ticks, the composite ignored.
        ps.update_at("binance", 110_060.0, (decision_ts * 1_000.0) as i64 - 300);
        ps.mid_price = 110_040.0;
        assert_eq!(
            band_basis_prices(&ps, open_ts, decision_ts, Some(110_000.0)),
            ("binance", Some(109_990.0), 110_060.0)
        );
        // A tick AFTER the decision instant is never the decision price.
        ps.update_at("binance", 120_000.0, (decision_ts * 1_000.0) as i64 + 400);
        ps.mid_price = 110_040.0;
        assert_eq!(
            band_basis_prices(&ps, open_ts, decision_ts, None),
            ("binance", Some(109_990.0), 110_060.0)
        );
    }

    /// Settings for a band artifact under `tmp` (state db and session log
    /// in the temp dir; the log is read back for record assertions).
    fn band_test_settings(tmp: &TempDir, params: &BandPolicyParams) -> Settings {
        let path = tmp.path().join("band_promotion.json");
        std::fs::write(&path, serde_json::to_vec(&band_promotion(params)).unwrap()).unwrap();
        let mut settings = Settings::from_env();
        settings.promotion_artifact_path = path.display().to_string();
        settings.state_db_path = tmp.path().join("state.db").display().to_string();
        settings.session_log_dir = tmp.path().join("sessions").display().to_string();
        settings.band_sizing = "pct".to_string();
        settings.band_anchor_seconds = Vec::new();
        settings.live_min_order_size_shares = 5.0;
        settings
    }

    /// Paper `Pipeline` on a floor-50 band artifact under `tmp`.
    async fn band_test_pipeline(tmp: &TempDir) -> (Arc<Pipeline>, BandPolicyParams) {
        let mut params = band_params();
        params.min_decision_margin_usd = 50.0;
        let settings = band_test_settings(tmp, &params);
        let p = Pipeline::new(settings, Mode::Paper).await.unwrap();
        (p, params)
    }

    /// Live-mode settings without venue credentials: no CLOB client and no
    /// wallet read (BANKROLL_USD pinned), so `Pipeline::new` runs the live
    /// restart path (bankroll actualization, ledger fold, trip hold) offline.
    fn band_live_test_settings(tmp: &TempDir) -> Settings {
        let mut settings = band_test_settings(tmp, &band_params());
        settings.bankroll_usd = 19.0;
        settings.clob_v2_ready = true;
        settings.live_reconciliation_ready = true;
        settings.poly_api_key = String::new();
        settings.private_key = String::new();
        settings
    }

    /// `true` when nothing has requested process stop (a `Notify` keeps a
    /// permit from an unobserved `notify_one`, so a prior request resolves
    /// immediately).
    async fn stop_not_requested(p: &Pipeline) -> bool {
        tokio::time::timeout(Duration::from_millis(10), p.stop.notified())
            .await
            .is_err()
    }

    async fn breaker_meta_present(p: &Pipeline) -> Vec<&'static str> {
        let mut present = Vec::new();
        for key in CANDLE_BREAKER_META_KEYS {
            if p.risk.get_meta(key).await.unwrap().is_some() {
                present.push(key);
            }
        }
        present
    }

    fn band_test_book(best_ask: f64, now_ts: f64) -> crate::polymarket_ws::TokenBookState {
        crate::polymarket_ws::TokenBookState {
            best_bid: best_ask - 0.01,
            best_ask,
            bids: vec![crate::polymarket_ws::BookLevel {
                price: best_ask - 0.01,
                size: 500.0,
            }],
            asks: vec![crate::polymarket_ws::BookLevel {
                price: best_ask,
                size: 500.0,
            }],
            last_update_us: (now_ts * 1e6) as u64,
            ..Default::default()
        }
    }

    fn band_test_books(
        up_ask: f64,
        down_ask: f64,
        now_ts: f64,
    ) -> HashMap<String, crate::polymarket_ws::TokenBookState> {
        HashMap::from([
            ("up".to_string(), band_test_book(up_ask, now_ts)),
            ("down".to_string(), band_test_book(down_ask, now_ts)),
        ])
    }

    /// `band_skip_detail` reasons recorded for `cid`, in session-log order.
    fn band_skip_details(p: &Pipeline, cid: &str) -> Vec<String> {
        std::fs::read_to_string(p.monitor.events_path())
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|v| v["type"] == "band_skip_detail" && v["cid"] == short_cid(cid))
            .map(|v| v["reason"].as_str().unwrap().to_string())
            .collect()
    }

    /// Session records of `kind` for `cid`, in log order.
    fn band_records_of(p: &Pipeline, kind: &str, cid: &str) -> Vec<serde_json::Value> {
        std::fs::read_to_string(p.monitor.events_path())
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|v| v["type"] == kind && v["cid"] == short_cid(cid))
            .collect()
    }

    async fn band_latch_of(p: &Pipeline, cid: &str) -> Option<BandLatch> {
        p.band_latch.lock().await.get(cid).map(|(latch, _)| *latch)
    }

    /// One cycle of the loop's band path for `c` at `now`, in the loop's
    /// order: the latch step (ahead of the loop's book gate, which this
    /// harness does not replay - the ordering in `scan_loop` is verified by
    /// reading), then the evaluation on the cycle's books.
    async fn band_cycle(
        p: &Arc<Pipeline>,
        band: &BandPolicyParams,
        c: &CandleContract,
        ps: &PriceState,
        now: DateTime<Utc>,
        books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
    ) -> bool {
        let end = parse_end(&c.end_date).unwrap();
        let minutes_left = (end - now).num_seconds() as f64 / 60.0;
        let minutes_elapsed = 5.0 - minutes_left;
        p.latch_band_decision(c, ps, now, end, 5.0, band_window_elapsed_s(end, now, 5.0))
            .await;
        let (up, down) = pick_book_prices(c, books, now.timestamp() as f64).unwrap();
        p.evaluate_band_opportunity(
            band,
            c,
            &c.market.condition_id,
            books,
            ps,
            ps.mid_price,
            now.timestamp() as f64,
            5.0,
            minutes_elapsed,
            minutes_left,
            up,
            down,
        )
        .await
        .unwrap()
    }

    fn band_window_ending(cid: &str, end: DateTime<Utc>) -> CandleContract {
        band_anchor_contract(cid, &end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
    }

    #[tokio::test]
    async fn band_latch_taken_at_decision_second_ahead_of_gates() {
        // Live 2026-09-02 18:14 UTC: margin -15 at the decision cycle
        // (band_margin_below_floor), a composite-feed flash dip to -50.24
        // at 247 s bought DOWN at 0.91, the price reverted (-9 at 270 s)
        // and the window resolved UP. The decision is one look per window,
        // taken on the decision cycle even when an entry gate is closed.
        let tmp = TempDir::new().unwrap();
        let (p, band) = band_test_pipeline(&tmp).await;
        let open = 110_000.0;
        let t0 = Utc::now();
        let cid = "0xlatch-incident";
        let c = band_window_ending(cid, t0 + chrono::Duration::seconds(60));
        p.momentum
            .lock()
            .await
            .get_mut("BTC")
            .unwrap()
            .set_window_open(cid, open);
        let mut ps = PriceState::new();

        // 240 s, venue incident flagged: the latch is taken regardless.
        p.venue_incident
            .store(true, std::sync::atomic::Ordering::Relaxed);
        ps.mid_price = open - 15.0;
        let books = band_test_books(0.10, 0.91, t0.timestamp() as f64);
        assert!(!band_cycle(&p, &band, &c, &ps, t0, &books).await);
        assert_eq!(
            band_latch_of(&p, cid).await,
            Some(BandLatch::NoSignal("band_margin_below_floor"))
        );
        assert_eq!(band_skip_details(&p, cid), vec!["band_margin_below_floor"]);

        // 247 s, incident cleared, the flash dip, a fresh in-band DOWN ask:
        // the latch is consulted, not the margin. No trade, no new detail.
        p.venue_incident
            .store(false, std::sync::atomic::Ordering::Relaxed);
        let t1 = t0 + chrono::Duration::seconds(7);
        ps.mid_price = open - 50.24;
        let books = band_test_books(0.10, 0.91, t1.timestamp() as f64);
        assert!(!band_cycle(&p, &band, &c, &ps, t1, &books).await);
        assert_eq!(
            band_latch_of(&p, cid).await,
            Some(BandLatch::NoSignal("band_margin_below_floor"))
        );
        assert!(p.traded.lock().await.is_empty());
        assert!(p.paper_positions.lock().await.is_empty());
        assert_eq!(band_skip_details(&p, cid), vec!["band_margin_below_floor"]);

        // Mid unavailable on the decision cycle latches no-signal; the
        // dip seven seconds later cannot re-open the window.
        let cid = "0xlatch-mid0";
        let c = band_window_ending(cid, t0 + chrono::Duration::seconds(60));
        p.momentum
            .lock()
            .await
            .get_mut("BTC")
            .unwrap()
            .set_window_open(cid, open);
        ps.mid_price = 0.0;
        let books = band_test_books(0.10, 0.91, t0.timestamp() as f64);
        assert!(!band_cycle(&p, &band, &c, &ps, t0, &books).await);
        assert_eq!(
            band_latch_of(&p, cid).await,
            Some(BandLatch::NoSignal("band_mid_unavailable"))
        );
        ps.mid_price = open - 50.24;
        let books = band_test_books(0.10, 0.91, t1.timestamp() as f64);
        assert!(!band_cycle(&p, &band, &c, &ps, t1, &books).await);
        assert_eq!(
            band_latch_of(&p, cid).await,
            Some(BandLatch::NoSignal("band_mid_unavailable"))
        );
        assert_eq!(band_skip_details(&p, cid), vec!["band_mid_unavailable"]);

        // A first look at 247 s (loop not running across 240 s) is a
        // missed decision, not a late one: no trade on the dip.
        let cid = "0xlatch-missed";
        let c = band_window_ending(cid, t0 + chrono::Duration::seconds(60));
        p.momentum
            .lock()
            .await
            .get_mut("BTC")
            .unwrap()
            .set_window_open(cid, open);
        assert!(!band_cycle(&p, &band, &c, &ps, t1, &books).await);
        assert_eq!(
            band_latch_of(&p, cid).await,
            Some(BandLatch::NoSignal("band_decision_missed"))
        );
        assert_eq!(band_skip_details(&p, cid), vec!["band_decision_missed"]);

        // The evaluation never decides on its own: with no latch it is
        // fail closed even on a clean signal and an in-band ask.
        let cid = "0xlatch-none";
        let c = band_window_ending(cid, t0 + chrono::Duration::seconds(60));
        ps.mid_price = open + 60.0;
        let books = band_test_books(0.90, 0.11, t0.timestamp() as f64);
        let (up, down) = pick_book_prices(&c, &books, t0.timestamp() as f64).unwrap();
        let traded = p
            .evaluate_band_opportunity(
                &band,
                &c,
                cid,
                &books,
                &ps,
                ps.mid_price,
                t0.timestamp() as f64,
                5.0,
                4.0,
                1.0,
                up,
                down,
            )
            .await
            .unwrap();
        assert!(!traded);
        assert_eq!(band_latch_of(&p, cid).await, None);
        assert!(band_skip_details(&p, cid).is_empty());
        assert!(p.traded.lock().await.is_empty());
    }

    /// The latch fires on the first cycle at or after the TRUE decision
    /// second: a cycle at 239.6 s (which the loop's integer-second elapsed
    /// already calls 240) takes no look; 240.05 s does.
    #[tokio::test]
    async fn band_latch_waits_for_the_true_decision_second() {
        use chrono::Timelike;
        let tmp = TempDir::new().unwrap();
        let (p, band) = band_test_pipeline(&tmp).await;
        let open = 110_000.0;
        let end = Utc::now().with_nanosecond(0).unwrap() + chrono::Duration::seconds(120);
        let cid = "0xtrue-240";
        let c = band_window_ending(cid, end);
        p.momentum
            .lock()
            .await
            .get_mut("BTC")
            .unwrap()
            .set_window_open(cid, open);
        let mut ps = PriceState::new();
        ps.mid_price = open + 60.0;

        // 239.6 s: no look, no latch, no detail - and the evaluation stays
        // fail closed on the missing latch.
        let t0 = end - chrono::Duration::milliseconds(60_400);
        let books = band_test_books(0.94, 0.07, t0.timestamp() as f64);
        assert!(!band_cycle(&p, &band, &c, &ps, t0, &books).await);
        assert_eq!(band_latch_of(&p, cid).await, None);
        assert!(band_skip_details(&p, cid).is_empty());

        // 240.05 s: the look, latched UP on the (fallback) composite basis
        // - recorded, never traded (the money path wants Binance's series).
        let t1 = end - chrono::Duration::milliseconds(59_950);
        let books = band_test_books(0.94, 0.07, t1.timestamp() as f64);
        assert!(!band_cycle(&p, &band, &c, &ps, t1, &books).await);
        assert_eq!(
            band_latch_of(&p, cid).await,
            Some(BandLatch::Signal {
                direction: "up",
                open,
                btc: open + 60.0,
                basis: "composite",
            })
        );
        assert_eq!(band_skip_details(&p, cid), vec!["band_basis_unvalidated"]);
    }

    /// The look waits (bounded) for Binance's tick at or after the decision
    /// instant, which makes the at-or-before sample final; a ticker that
    /// never gets past the instant is sampled at the deferral bound on the
    /// tick it has. Both windows sit in the past so their instants fall
    /// inside the tick clamp window.
    #[tokio::test]
    async fn band_latch_waits_for_the_covering_binance_tick() {
        use chrono::Timelike;
        let tmp = TempDir::new().unwrap();
        let (p, band) = band_test_pipeline(&tmp).await;
        let open = 110_000.0;
        let mut ps = PriceState::new();
        ps.mid_price = open - 500.0;
        let end1 = Utc::now().with_nanosecond(0).unwrap() - chrono::Duration::seconds(400);
        let open1_ms = (end1 - chrono::Duration::seconds(300)).timestamp_millis();
        let dec1_ms = open1_ms + 240_000;
        let cid1 = "0xcover-late";
        let c1 = band_window_ending(cid1, end1);
        let at = |end: DateTime<Utc>, ms_after_decision: i64| {
            end - chrono::Duration::milliseconds(60_000 - ms_after_decision)
        };
        let cycle = |c: &CandleContract, ps: &PriceState, now: DateTime<Utc>| {
            let books = band_test_books(0.94, 0.07, now.timestamp() as f64);
            let p = p.clone();
            let band = band.clone();
            let c = c.clone();
            let ps = ps.clone();
            async move { band_cycle(&p, &band, &c, &ps, now, &books).await }
        };

        // Open tick, then the tick one second before the instant: the
        // at-or-before tick is still in flight at 240.05 s - no look yet.
        ps.update_at("binance", open, open1_ms - 300);
        ps.update_at("binance", open + 40.0, dec1_ms - 1_050);
        assert!(!cycle(&c1, &ps, at(end1, 50)).await);
        assert_eq!(band_latch_of(&p, cid1).await, None);
        assert!(band_skip_details(&p, cid1).is_empty());
        // The at-or-before tick lands; still nothing past the instant.
        ps.update_at("binance", open + 60.0, dec1_ms - 50);
        assert!(!cycle(&c1, &ps, at(end1, 400)).await);
        assert_eq!(band_latch_of(&p, cid1).await, None);
        // The covering tick lands: decided on the at-or-before tick, never
        // on the later one.
        ps.update_at("binance", open - 500.0, dec1_ms + 500);
        assert!(!cycle(&c1, &ps, at(end1, 600)).await);
        assert_eq!(
            band_latch_of(&p, cid1).await,
            Some(BandLatch::Signal {
                direction: "up",
                open,
                btc: open + 60.0,
                basis: "binance",
            })
        );

        // Next window: Binance live at the instant but never past it. The
        // look waits to the deferral bound, then samples what it has.
        let end2 = end1 + chrono::Duration::seconds(300);
        let open2_ms = end1.timestamp_millis();
        let dec2_ms = open2_ms + 240_000;
        let cid2 = "0xcover-stalled";
        let c2 = band_window_ending(cid2, end2);
        ps.update_at("binance", open, open2_ms - 300);
        ps.update_at("binance", open - 55.0, dec2_ms - 1_050);
        assert!(!cycle(&c2, &ps, at(end2, 50)).await);
        assert_eq!(band_latch_of(&p, cid2).await, None);
        assert!(!cycle(&c2, &ps, at(end2, 950)).await);
        assert_eq!(band_latch_of(&p, cid2).await, None);
        assert!(!cycle(&c2, &ps, at(end2, 1_050)).await);
        assert_eq!(
            band_latch_of(&p, cid2).await,
            Some(BandLatch::Signal {
                direction: "down",
                open,
                btc: open - 55.0,
                basis: "binance",
            })
        );
    }

    /// The latch reads Binance's own ticks at or before the open and at or
    /// before the decision instant; when Binance has no tick at either it
    /// falls back to the composite mid on BOTH ends; the basis used is on
    /// the window's detail, its evaluation record and its decision anchor.
    #[tokio::test]
    async fn band_latch_reads_the_binance_basis_and_records_the_fallback() {
        use chrono::Timelike;
        let tmp = TempDir::new().unwrap();
        let mut params = band_params();
        params.min_decision_margin_usd = 50.0;
        let mut settings = band_test_settings(&tmp, &params);
        settings.band_anchor_seconds = vec![240.0];
        let p = Pipeline::new(settings, Mode::Paper).await.unwrap();
        let band = params;
        let open = 110_000.0;
        let mut ps = PriceState::new();
        // Window 1 ended 100 s ago (its instants sit inside the tick clamp
        // window). Binance: 0.3 s before the open, 0.3 s before the
        // decision instant, and a large tick 0.4 s AFTER it that must not
        // be the decision price. The cached composite open is 100 higher
        // and the composite mid says DOWN: with Binance present neither
        // is read.
        let end1 = Utc::now().with_nanosecond(0).unwrap() - chrono::Duration::seconds(100);
        let open1_ms = (end1 - chrono::Duration::seconds(300)).timestamp_millis();
        ps.update_at("binance", open, open1_ms - 300);
        ps.update_at("binance", open + 60.0, open1_ms + 240_000 - 300);
        ps.update_at("binance", open - 200.0, open1_ms + 240_000 + 400);
        // Window 2 (the next one): a Binance tick at its open only - the
        // feed stalled before its decision instant. Whole-window fallback.
        let end2 = end1 + chrono::Duration::seconds(300);
        ps.update_at("binance", open + 5.0, end1.timestamp_millis() - 300);
        // The composite mid says DOWN by 60 on every cycle below (pinned
        // after the ticks: `update_at` feeds the composite too).
        ps.mid_price = open - 60.0;
        {
            let mut moms = p.momentum.lock().await;
            let det = moms.get_mut("BTC").unwrap();
            det.set_window_open("0xbasis-binance", open + 100.0);
            det.set_window_open("0xbasis-partial", open);
        }

        // Anchor capture, then latch and evaluation: the loop's order.
        let cycle = |cid: &'static str, end: DateTime<Utc>| {
            let p = p.clone();
            let band = band.clone();
            let ps = ps.clone();
            async move {
                let c = band_window_ending(cid, end);
                let now = end - chrono::Duration::milliseconds(59_950);
                let now_ts = now.timestamp() as f64;
                let books = band_test_books(0.94, 0.07, now_ts);
                p.capture_band_anchors(&c, std::slice::from_ref(&c), &books, &ps, now, now_ts)
                    .await;
                assert!(!band_cycle(&p, &band, &c, &ps, now, &books).await);
                let mut details = band_records_of(&p, "band_skip_detail", cid);
                let mut anchors = band_records_of(&p, "band_anchor", cid);
                assert_eq!((details.len(), anchors.len()), (1, 1), "{cid}");
                (
                    band_latch_of(&p, cid).await,
                    details.remove(0),
                    anchors.remove(0),
                )
            }
        };

        let (latch, detail, anchor) = cycle("0xbasis-binance", end1).await;
        assert_eq!(
            latch,
            Some(BandLatch::Signal {
                direction: "up",
                open,
                btc: open + 60.0,
                basis: "binance",
            })
        );
        assert_eq!(detail["reason"], "band_price_out_of_range");
        let text = detail["detail"].as_str().unwrap();
        assert!(
            text.contains("basis=binance")
                && text.contains("btc=110060.00")
                && text.contains("open=110000.00"),
            "{text}"
        );
        assert_eq!(anchor["basis"], "binance");
        assert_eq!(anchor["btc"], open + 60.0);
        assert_eq!(anchor["open"], open);
        assert_eq!(anchor["margin"], 60.0);
        assert_eq!(anchor["direction"], "up");

        let (latch, detail, anchor) = cycle("0xbasis-partial", end2).await;
        assert_eq!(
            latch,
            Some(BandLatch::Signal {
                direction: "down",
                open,
                btc: open - 60.0,
                basis: "composite",
            })
        );
        // Recorded on the fallback basis, never traded on it.
        assert_eq!(detail["reason"], "band_basis_unvalidated");
        assert!(detail["detail"]
            .as_str()
            .unwrap()
            .contains("basis=composite"));
        assert_eq!(anchor["basis"], "composite");
        assert_eq!(anchor["btc"], open - 60.0);
        assert_eq!(anchor["open"], open);
        assert_eq!(anchor["direction"], "down");

        // A no-signal latch carries the basis on its one detail too.
        let cid = "0xbasis-floor";
        let end3 = end2 + chrono::Duration::seconds(300);
        let c = band_window_ending(cid, end3);
        p.momentum
            .lock()
            .await
            .get_mut("BTC")
            .unwrap()
            .set_window_open(cid, open - 45.0);
        let now = end3 - chrono::Duration::milliseconds(59_950);
        let books = band_test_books(0.94, 0.07, now.timestamp() as f64);
        assert!(!band_cycle(&p, &band, &c, &ps, now, &books).await);
        assert_eq!(
            band_latch_of(&p, cid).await,
            Some(BandLatch::NoSignal("band_margin_below_floor"))
        );
        let details = band_records_of(&p, "band_skip_detail", cid);
        assert_eq!(details.len(), 1);
        let text = details[0]["detail"].as_str().unwrap();
        assert!(
            text.contains("margin=-15") && text.contains("basis=composite"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn band_latch_signal_keeps_side_and_waits_for_in_band_ask() {
        let tmp = TempDir::new().unwrap();
        let (p, band) = band_test_pipeline(&tmp).await;
        let open = 110_000.0;
        let t0 = Utc::now();
        let cid = "0xlatch-up";
        let c = band_window_ending(cid, t0 + chrono::Duration::seconds(60));
        p.momentum
            .lock()
            .await
            .get_mut("BTC")
            .unwrap()
            .set_window_open(cid, open);
        let mut ps = PriceState::new();
        // Binance at the open, at the decision instant and past it (the
        // money path trades the validated series only). Instants come from
        // the contract's whole-second end.
        let end = parse_end(&c.end_date).unwrap();
        let open_ms = (end - chrono::Duration::seconds(300)).timestamp_millis();
        let decision_ms = (end - chrono::Duration::seconds(60)).timestamp_millis();
        ps.update_at("binance", open, open_ms - 300);
        ps.update_at("binance", open + 60.0, decision_ms - 300);
        ps.update_at("binance", open + 61.0, decision_ms + 400);
        let signal_up = Some(BandLatch::Signal {
            direction: "up",
            open,
            btc: open + 60.0,
            basis: "binance",
        });

        // 240 s: UP by 60, but the UP ask is above the cap - wait.
        ps.mid_price = open + 60.0;
        let books = band_test_books(0.94, 0.07, t0.timestamp() as f64);
        assert!(!band_cycle(&p, &band, &c, &ps, t0, &books).await);
        assert_eq!(band_latch_of(&p, cid).await, signal_up);
        assert_eq!(band_skip_details(&p, cid), vec!["band_price_out_of_range"]);

        // 250 s: the mid is now BELOW the open. The side stays UP (and the
        // latched open), and the band gate still governs the entry.
        let t1 = t0 + chrono::Duration::seconds(10);
        ps.mid_price = open - 60.0;
        let books = band_test_books(0.94, 0.07, t1.timestamp() as f64);
        assert!(!band_cycle(&p, &band, &c, &ps, t1, &books).await);
        assert_eq!(band_latch_of(&p, cid).await, signal_up);
        assert!(p.traded.lock().await.is_empty());

        // 255 s: an in-band UP ask - the patient entry fires on the
        // latched side, not on this cycle's (DOWN) sign.
        let t2 = t0 + chrono::Duration::seconds(15);
        let books = band_test_books(0.90, 0.11, t2.timestamp() as f64);
        assert!(band_cycle(&p, &band, &c, &ps, t2, &books).await);
        assert!(p.traded.lock().await.contains(cid));
        let positions = p.paper_positions.lock().await;
        let pp = positions.get(cid).unwrap();
        assert_eq!(pp.direction, "up");
        assert_eq!(pp.open_btc, open);
    }

    #[tokio::test]
    async fn band_latch_pruned_to_open_windows() {
        let tmp = TempDir::new().unwrap();
        let (p, band) = band_test_pipeline(&tmp).await;
        let t0 = Utc::now();
        let mut ps = PriceState::new();
        ps.mid_price = 110_000.0;
        let books = |now: DateTime<Utc>| band_test_books(0.50, 0.52, now.timestamp() as f64);
        // Open unavailable: every window latches no-signal; only the map
        // membership matters here.
        let a = band_window_ending("0xprune-a", t0 + chrono::Duration::seconds(60));
        assert!(!band_cycle(&p, &band, &a, &ps, t0, &books(t0)).await);
        let t1 = t0 + chrono::Duration::seconds(30);
        let b = band_window_ending("0xprune-b", t1 + chrono::Duration::seconds(60));
        assert!(!band_cycle(&p, &band, &b, &ps, t1, &books(t1)).await);
        assert_eq!(p.band_latch.lock().await.len(), 2);

        // A new latch after window a has ended prunes a and keeps b: the
        // prune keys on the window end, never on the contract list, so a
        // refresh that omits a live market cannot re-open its window.
        let t2 = t0 + chrono::Duration::seconds(61);
        let c = band_window_ending("0xprune-c", t2 + chrono::Duration::seconds(60));
        assert!(!band_cycle(&p, &band, &c, &ps, t2, &books(t2)).await);
        let keys: HashSet<String> = p.band_latch.lock().await.keys().cloned().collect();
        assert_eq!(
            keys,
            HashSet::from(["0xprune-b".to_string(), "0xprune-c".to_string()])
        );
    }

    #[tokio::test]
    async fn breaker_trip_parks_scan_loop_without_stopping() {
        let tmp = TempDir::new().unwrap();
        let (p, _) = band_test_pipeline(&tmp).await;
        // A live decision price, so the loop reaches the breaker check (a
        // dead feed short-circuits ahead of it).
        p.price_state.write().await.mid_price = 60_000.0;
        // oracle_tie: an accounting trip the paper policy never re-arms.
        p.trip_breaker("oracle_tie").await;
        assert!(stop_not_requested(&p).await, "a trip must not request stop");
        let scan = {
            let p = p.clone();
            tokio::spawn(async move { p.scan_loop().await })
        };
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(
            !scan.is_finished(),
            "a trip parks the loop; it must not end"
        );
        assert!(stop_not_requested(&p).await);
        assert!(*p.breaker_tripped.lock().await);
        assert!(
            *p.cycle_count.lock().await >= 2,
            "the parked loop keeps cycling (heartbeat, settlement stay alive)"
        );
        let heartbeats = std::fs::read_to_string(p.monitor.events_path())
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|v| v["type"] == "error" && v["component"] == "breaker_halted")
            .count();
        assert_eq!(heartbeats, 1, "candle.halted heartbeat once per 5 minutes");
        scan.abort();
    }

    #[tokio::test]
    async fn kill_switch_parks_scan_loop_instead_of_returning() {
        let tmp = TempDir::new().unwrap();
        let mut params = band_params();
        params.min_decision_margin_usd = 50.0;
        let mut settings = band_test_settings(&tmp, &params);
        let kill = tmp.path().join("KILL");
        settings.kill_switch_path = kill.display().to_string();
        let p = Pipeline::new(settings, Mode::Paper).await.unwrap();
        std::fs::write(&kill, "").unwrap();
        // No decision price: exercises the check that runs ahead of the
        // dead-feed early continue.
        let scan = {
            let p = p.clone();
            tokio::spawn(async move { p.scan_loop().await })
        };
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(
            !scan.is_finished(),
            "kill switch parks the loop; it must not return"
        );
        assert!(stop_not_requested(&p).await);
        assert!(*p.breaker_tripped.lock().await);
        assert_eq!(
            p.breaker_trip_reason.lock().await.as_deref(),
            Some("kill_switch")
        );
        assert!(*p.cycle_count.lock().await >= 2);
        scan.abort();
    }

    #[test]
    fn money_accounting_and_operator_trips_hold_across_restart() {
        for reason in [
            "live_cumulative_loss",
            "consecutive_losses",
            "band_exposure_anomaly",
            "live_fill_fee_unavailable",
            "live_order_journal_failure",
            "live_trade_failed",
            "live_order_hash_mismatch",
            "live_rest_missing_confirmed_trade",
            "live_rest_order_disappeared",
            "live_terminal_order_missing_confirmed_trade",
            "live_balance_allowance_reject",
            "accounting_drift",
            "oracle_tie",
            "oracle_unresolved_timeout",
            "oracle_pnl_missing_fields",
            "oracle_pnl_realization_failed",
            "oracle_pnl_correction_failed",
            "operator_stop",
            "some_future_reason",
        ] {
            assert!(breaker_trip_holds_across_restart(reason), "{reason}");
        }
        for reason in [
            "kill_switch",
            "open_exposure_stress",
            "session_loss_floor",
            "realized_drawdown",
            "win_rate_low",
        ] {
            assert!(!breaker_trip_holds_across_restart(reason), "{reason}");
        }
    }

    #[tokio::test]
    async fn persisted_money_trip_holds_across_restart_until_operator_rearm() {
        let tmp = TempDir::new().unwrap();
        let mut settings = band_live_test_settings(&tmp);
        // The live canary's floor (0.6 x $19 = -11.40), not the env default.
        settings.candle_live_max_cumulative_loss_pct = 0.6;
        let p = Pipeline::new(settings.clone(), Mode::Live).await.unwrap();
        {
            let mut bs = p.breaker.lock().await;
            bs.losses = 5;
            bs.realized_pnl = -5.02;
        }
        // The v1 book carries the same session loss (as `record_pnl` does
        // at realization), so the restart has something to actualize.
        p.risk.record_pnl(-5.02).await.unwrap();
        p.trip_breaker("live_cumulative_loss").await;
        assert!(stop_not_requested(&p).await);
        assert_eq!(
            p.risk
                .get_meta("candle_breaker_ledger")
                .await
                .unwrap()
                .as_deref(),
            Some("-5.020000"),
            "ledger figure persisted at the trip"
        );
        drop(p);

        // Restart: the actualization path must neither fold the session nor
        // clear the trip.
        let p = Pipeline::new(settings.clone(), Mode::Live).await.unwrap();
        assert!(*p.breaker_tripped.lock().await, "restart must start parked");
        assert_eq!(
            p.breaker_trip_reason.lock().await.as_deref(),
            Some("live_cumulative_loss")
        );
        assert_eq!(
            p.breaker.lock().await.losses,
            5,
            "session retained, not re-based"
        );
        assert_eq!(
            p.risk
                .get_meta("live_cumulative_realized_pnl")
                .await
                .unwrap(),
            None,
            "held trip is not folded into the ledger"
        );
        assert_eq!(breaker_meta_present(&p).await, CANDLE_BREAKER_META_KEYS);
        // The v1 base did not absorb the retained session either: the
        // floor and the status equity are the ones the trip fired on.
        assert!((p.breaker_bankroll().await - 19.0).abs() < 1e-9);
        assert!((p.risk.effective_bankroll().await - 13.98).abs() < 1e-9);
        let status = p.operator_status_text().await;
        assert!(status.contains("stop at -11.40"), "{status}");
        assert!(status.contains("v1 $13.98"), "{status}");
        drop(p);

        // A second bounce does not re-arm either.
        let p = Pipeline::new(settings.clone(), Mode::Live).await.unwrap();
        assert!(*p.breaker_tripped.lock().await);
        assert!((p.breaker_bankroll().await - 19.0).abs() < 1e-9);

        // Only the operator's re-arm clears it, in memory and in the db -
        // and the held session reaches the cross-restart ledger there.
        p.operator_rearm().await.expect("ledger -5.02 is above the -11.40 floor");
        assert!(!*p.breaker_tripped.lock().await);
        assert!(p.breaker_trip_reason.lock().await.is_none());
        assert!(breaker_meta_present(&p).await.is_empty());
        assert_eq!(
            p.risk
                .get_meta("live_cumulative_realized_pnl")
                .await
                .unwrap()
                .as_deref(),
            Some("-5.020000"),
            "re-arm folds the held session into the ledger"
        );
        assert!((p.live_loss_ledger_prior() + 5.02).abs() < 1e-9);
        assert!(p.breaker.lock().await.realized_pnl.abs() < 1e-9);
        drop(p);
        let p = Pipeline::new(settings, Mode::Live).await.unwrap();
        assert!(!*p.breaker_tripped.lock().await);
        assert!((p.live_loss_ledger_prior() + 5.02).abs() < 1e-9);

        // A held session that still breaches the floor cannot be /start-ed
        // away: the fold lands, the trip re-fires at once.
        p.breaker.lock().await.realized_pnl = -6.50;
        p.trip_breaker("live_cumulative_loss").await;
        let err = p.operator_rearm().await.expect_err("ledger -11.52 breaches -11.40");
        assert!(err.contains("still halted"), "{err}");
        assert!(*p.breaker_tripped.lock().await);
        assert!((p.live_loss_ledger_prior() + 11.52).abs() < 1e-9);
        assert_eq!(
            p.risk
                .get_meta("live_cumulative_realized_pnl")
                .await
                .unwrap()
                .as_deref(),
            Some("-11.520000")
        );
    }



    #[tokio::test]
    async fn non_money_trip_is_rebased_on_restart_as_before() {
        let tmp = TempDir::new().unwrap();
        let settings = band_live_test_settings(&tmp);
        let p = Pipeline::new(settings.clone(), Mode::Live).await.unwrap();
        p.breaker.lock().await.realized_pnl = -1.0;
        p.trip_breaker("open_exposure_stress").await;
        assert!(stop_not_requested(&p).await, "no trip exits the process");
        assert!(*p.breaker_tripped.lock().await);
        assert_eq!(breaker_meta_present(&p).await, CANDLE_BREAKER_META_KEYS);
        drop(p);

        let p = Pipeline::new(settings, Mode::Live).await.unwrap();
        assert!(
            !*p.breaker_tripped.lock().await,
            "actualization re-bases the session"
        );
        assert!(p.breaker_trip_reason.lock().await.is_none());
        assert!(breaker_meta_present(&p).await.is_empty());
        assert_eq!(
            p.risk
                .get_meta("live_cumulative_realized_pnl")
                .await
                .unwrap()
                .as_deref(),
            Some("-1.000000"),
            "session folded into the cross-restart ledger"
        );
        assert!((p.live_loss_ledger_prior() + 1.0).abs() < 1e-9);
    }

    /// Live v2 settings; the wallet is unreadable offline, so the book is
    /// seeded here (both sub-books, as `seed_book_v2_from_wallet` does).
    async fn band_live_v2_settings(tmp: &TempDir, opening: f64) -> Settings {
        let mut settings = band_live_test_settings(tmp);
        settings.risk_book = "v2".to_string();
        // The live canary's floor (deploy/band-canary.env), not the 0.5
        // env default.
        settings.candle_live_max_cumulative_loss_pct = 0.6;
        let book = crate::risk::book_v2::BookV2::open(book_v2_path(&settings)).unwrap();
        for (strategy, key) in [
            ("band", "import_wallet"),
            (crate::risk::book_v2::KELLY_SIM_STRATEGY, "import_sim"),
        ] {
            book.post(
                1.0,
                crate::risk::book_v2::PostingKind::Import,
                strategy,
                "",
                opening,
                0.0,
                0.0,
                "test opening",
                key,
            )
            .await
            .unwrap();
        }
        settings
    }

    async fn v2_posting_count(p: &Pipeline) -> usize {
        let conn = rusqlite::Connection::open(book_v2_path(&p.settings)).unwrap();
        conn.query_row("SELECT COUNT(*) FROM v2_postings", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap() as usize
    }

    #[test]
    fn wallet_reconcile_step_adjusts_on_two_readings_and_drifts_after() {
        use ReconcileAction::*;
        let mut st = WalletReconcile::default();
        let step = |st: &mut WalletReconcile, diff: f64| wallet_reconcile_step(st, diff, 0.5, 5.0);
        // Within tolerance: nothing, and a lone outlier resets.
        assert_eq!(step(&mut st, 0.3), Ok);
        assert_eq!(step(&mut st, 10.0), Watch);
        assert_eq!(step(&mut st, 0.1), Ok);
        assert_eq!(step(&mut st, 10.0), Watch);
        // Second consecutive reading outside: post once.
        assert_eq!(step(&mut st, 10.0), Adjust);
        // Reconciled: back within tolerance, no further posting.
        assert_eq!(step(&mut st, 0.0), Ok);
        assert_eq!(step(&mut st, 0.0), Ok);
        // A residual under the halt tolerance right after an adjustment is
        // a new watch, not a drift.
        assert_eq!(step(&mut st, -3.0), Watch);
        assert_eq!(step(&mut st, -3.0), Adjust);
        assert_eq!(step(&mut st, 4.9), Watch);
        assert_eq!(step(&mut st, 4.9), Adjust);
        // Still beyond the halt tolerance after the adjustment: drift.
        assert_eq!(step(&mut st, 6.0), Drift);
        assert_eq!(st, WalletReconcile::default());
        // Sign is irrelevant: a withdrawal reconciles the same way.
        assert_eq!(step(&mut st, -6.0), Watch);
        assert_eq!(step(&mut st, -6.0), Adjust);
        assert_eq!(step(&mut st, -6.0), Drift);
        // A settled win the wallet has not redeemed yet: the book is
        // ahead by the payout for two readings, the adjustment follows the
        // wallet, then the redeem lands and the difference reverses the
        // adjustment. That is the wallet catching up, posted back, not
        // drift (the default halt is under the smallest payout).
        assert_eq!(step(&mut st, -5.49), Watch);
        assert_eq!(step(&mut st, -5.49), Adjust);
        assert_eq!(step(&mut st, 5.49), Reverse);
        assert_eq!(step(&mut st, 0.0), Ok);
        assert!(st.streak == 0 && !st.adjusted);
        // A same-sign residual after an adjustment is still drift.
        assert_eq!(step(&mut st, 7.0), Watch);
        assert_eq!(step(&mut st, 7.0), Adjust);
        assert_eq!(step(&mut st, 7.0), Drift);
    }

    #[tokio::test]
    async fn risk_book_v2_sizes_and_floors_on_book_equity() {
        let tmp = TempDir::new().unwrap();
        let settings = band_live_v2_settings(&tmp, 7.8).await;
        let p = Pipeline::new(settings, Mode::Live).await.unwrap();
        assert!(p.risk_book_v2);
        // v1 still pins BANKROLL_USD; sizing and the floor read the book.
        assert!((p.risk.effective_bankroll().await - 19.0).abs() < 1e-9);
        assert!((p.band_sizing_equity().await - 7.8).abs() < 1e-9);
        assert!((p.breaker_bankroll().await - 7.8).abs() < 1e-9);
        assert_eq!(
            p.risk
                .get_meta("v2_session_start_equity")
                .await
                .unwrap()
                .as_deref(),
            Some("7.800000")
        );
        // Money floor: 0.6 x $7.80 = -$4.68 on real equity (v1's pinned
        // base would allow -$11.40).
        let mut bs = BreakerState {
            losses: 1,
            realized_pnl: -4.70,
            ..Default::default()
        };
        assert_eq!(
            p.breaker_trip_reason_for(&bs, 0.0, p.breaker_bankroll().await),
            Some("live_cumulative_loss")
        );
        bs.realized_pnl = -4.60;
        assert_eq!(
            p.breaker_trip_reason_for(&bs, 0.0, p.breaker_bankroll().await),
            None
        );
        let status = p.operator_status_text().await;
        assert!(
            status.contains("book v2 drives sizing \u{00b7} v1 $19.00 \u{00b7} v2 $7.80"),
            "{status}"
        );
        assert!(status.contains("stop at -4.68"), "{status}");
    }

    #[tokio::test]
    async fn risk_book_v1_is_unchanged_when_unset() {
        let tmp = TempDir::new().unwrap();
        let mut settings = band_live_test_settings(&tmp);
        settings.candle_live_max_cumulative_loss_pct = 0.6;
        assert_eq!(settings.risk_book, "v1");
        let p = Pipeline::new(settings, Mode::Live).await.unwrap();
        assert!(!p.risk_book_v2);
        assert!((p.band_sizing_equity().await - 19.0).abs() < 1e-9);
        assert!((p.breaker_bankroll().await - 19.0).abs() < 1e-9);
        assert_eq!(
            p.risk.get_meta("v2_session_start_equity").await.unwrap(),
            None
        );
        let bs = BreakerState {
            losses: 1,
            realized_pnl: -4.70,
            ..Default::default()
        };
        assert_eq!(
            p.breaker_trip_reason_for(&bs, 0.0, p.breaker_bankroll().await),
            None
        );
        let status = p.operator_status_text().await;
        assert!(status.contains("book v1 drives sizing"), "{status}");
        assert!(status.contains("stop at -11.40"), "{status}");
    }

    #[tokio::test]
    async fn risk_book_v2_start_blocked_on_real_equity_floor() {
        let tmp = TempDir::new().unwrap();
        let settings = band_live_v2_settings(&tmp, 7.8).await;
        {
            let p = Pipeline::new(settings.clone(), Mode::Live).await.unwrap();
            p.risk
                .set_meta("live_cumulative_realized_pnl", "-5.02")
                .await
                .unwrap();
        }
        // -5.02 breaches 0.6 x $7.80 under v2 ...
        let err = Pipeline::new(settings.clone(), Mode::Live)
            .await
            .err()
            .expect("v2 start must be blocked")
            .to_string();
        assert!(err.contains("live start blocked"), "{err}");
        assert!(err.contains("bankroll 7.80"), "{err}");
        // ... but not 0.6 x the pinned $19 under v1 (pre-existing behaviour).
        let mut v1 = settings;
        v1.risk_book = "v1".to_string();
        assert!(Pipeline::new(v1, Mode::Live).await.is_ok());
    }

    /// The floor's base is the equity at the ledger's origin, not the
    /// drawn-down equity at each restart: losing $3 of $7.80 and bouncing
    /// keeps the floor at -4.68 (start allowed) instead of re-basing to
    /// 0.6 x 4.80 = -2.88 and blocking it. Only the ledger reset re-bases.
    #[tokio::test]
    async fn risk_book_v2_floor_base_survives_restarts_until_the_ledger_resets() {
        use crate::risk::book_v2::PostingKind;
        let tmp = TempDir::new().unwrap();
        let settings = band_live_v2_settings(&tmp, 7.8).await;
        {
            let p = Pipeline::new(settings.clone(), Mode::Live).await.unwrap();
            assert!((p.breaker_bankroll().await - 7.8).abs() < 1e-9);
            assert_eq!(
                p.risk.get_meta("v2_floor_base_equity").await.unwrap().as_deref(),
                Some("7.800000")
            );
            // A lost $3 stake: the book and the session carry it.
            let book = p.book_v2.as_ref().unwrap();
            book.post(2.0, PostingKind::Fill, "band", "0xl1", -3.0, 3.5, 0.857, "", "l1::fill")
                .await
                .unwrap();
            book.post(3.0, PostingKind::Settlement, "band", "0xl1", 0.0, 3.5, 0.0, "lost", "0xl1::settle")
                .await
                .unwrap();
            {
                let mut bs = p.breaker.lock().await;
                bs.losses = 1;
                bs.realized_pnl = -3.0;
            }
            p.persist_breaker_state().await;
        }
        // Restart: the session folds into the ledger (-3), the book reads
        // 4.80, the floor base stays 7.80.
        let p = Pipeline::new(settings.clone(), Mode::Live).await.unwrap();
        assert!((p.live_loss_ledger_prior() + 3.0).abs() < 1e-9);
        assert!((p.band_sizing_equity().await - 4.8).abs() < 1e-9);
        assert!((p.breaker_bankroll().await - 7.8).abs() < 1e-9);
        let status = p.operator_status_text().await;
        assert!(status.contains("stop at -4.68"), "{status}");
        // The operator's post-mortem zeroes the ledger: the next start
        // re-bases on the equity it finds.
        p.risk
            .set_meta("live_cumulative_realized_pnl", "0")
            .await
            .unwrap();
        drop(p);
        let p = Pipeline::new(settings, Mode::Live).await.unwrap();
        assert!((p.breaker_bankroll().await - 4.8).abs() < 1e-9);
        assert_eq!(
            p.risk.get_meta("v2_floor_base_equity").await.unwrap().as_deref(),
            Some("4.800000")
        );
        assert!(p.operator_status_text().await.contains("stop at -2.88"));
    }

    #[test]
    fn settled_band_qty_prefers_durable_records() {
        assert_eq!(settled_band_qty(Some(5.48), Some(5.48), 0.0), 5.48);
        assert_eq!(settled_band_qty(None, Some(5.48), 0.0), 5.48);
        assert_eq!(settled_band_qty(None, Some(0.0), 7.0), 7.0);
        assert_eq!(settled_band_qty(None, None, 0.0), 0.0);
    }

    #[tokio::test]
    async fn risk_book_v2_refuses_to_start_unseeded_or_misconfigured() {
        let tmp = TempDir::new().unwrap();
        let mut settings = band_live_test_settings(&tmp);
        settings.risk_book = "v2".to_string();
        let err = Pipeline::new(settings.clone(), Mode::Live)
            .await
            .err()
            .expect("empty book without a wallet must not start")
            .to_string();
        assert!(err.contains("cannot seed"), "{err}");
        settings.risk_book = "v3".to_string();
        let err = Pipeline::new(settings, Mode::Live)
            .await
            .err()
            .expect("unknown RISK_BOOK must not start")
            .to_string();
        assert!(err.contains("RISK_BOOK must be v1 or v2"), "{err}");
    }

    #[tokio::test]
    async fn risk_book_v2_deposit_adjusts_once_and_drift_trips() {
        let tmp = TempDir::new().unwrap();
        let settings = band_live_v2_settings(&tmp, 7.8).await;
        let p = Pipeline::new(settings, Mode::Live).await.unwrap();
        let before = v2_posting_count(&p).await;
        // A $10 deposit: first reading watches, second posts once.
        p.reconcile_v2_wallet(17.8).await;
        assert_eq!(v2_posting_count(&p).await, before);
        assert!((p.band_sizing_equity().await - 7.8).abs() < 1e-9);
        p.reconcile_v2_wallet(17.8).await;
        // One posting per sub-book: the kelly sim receives the deposit too,
        // so the two curves keep differing by policy only.
        assert_eq!(v2_posting_count(&p).await, before + 2);
        assert!((p.band_sizing_equity().await - 17.8).abs() < 1e-9);
        let book = p.book_v2.as_ref().unwrap();
        let sim = crate::risk::book_v2::KELLY_SIM_STRATEGY;
        assert!((book.equity_for(sim).await.unwrap() - 17.8).abs() < 1e-9);
        // Idempotent: the reconciled book posts nothing more.
        p.reconcile_v2_wallet(17.8).await;
        p.reconcile_v2_wallet(17.8).await;
        assert_eq!(v2_posting_count(&p).await, before + 2);
        assert!(!*p.breaker_tripped.lock().await);
        let records = std::fs::read_to_string(p.monitor.events_path()).unwrap();
        assert_eq!(
            records.matches("\"wallet_reconcile\"").count(),
            1,
            "{records}"
        );
        // The money floor base stays the session-start equity: a deposit
        // does not launder the ledger.
        assert!((p.breaker_bankroll().await - 7.8).abs() < 1e-9);
        // A settled win the wallet redeems late: two lagged readings post
        // the payout out of the book (and off the base), the redeem then
        // reverses both - no drift, base restored (readings differ by a
        // few cents so the per-reading postings do not collide in-second).
        p.reconcile_v2_wallet(12.31).await;
        p.reconcile_v2_wallet(12.31).await;
        assert_eq!(v2_posting_count(&p).await, before + 4);
        assert!((p.breaker_bankroll().await - 2.31).abs() < 1e-9);
        p.reconcile_v2_wallet(17.9).await;
        assert_eq!(v2_posting_count(&p).await, before + 6);
        assert!(!*p.breaker_tripped.lock().await, "a lagged redeem is not drift");
        assert!((p.band_sizing_equity().await - 17.9).abs() < 1e-9);
        assert!((p.breaker_bankroll().await - 7.9).abs() < 1e-9);
        p.reconcile_v2_wallet(17.9).await;
        // A withdrawal lowers it by the same amount: the floor is measured
        // on money still in the wallet.
        p.reconcile_v2_wallet(12.9).await;
        p.reconcile_v2_wallet(12.9).await;
        assert_eq!(v2_posting_count(&p).await, before + 8);
        assert!((p.band_sizing_equity().await - 12.9).abs() < 1e-9);
        assert!((p.breaker_bankroll().await - 2.9).abs() < 1e-9);
        assert_eq!(
            p.risk
                .get_meta("v2_floor_base_equity")
                .await
                .unwrap()
                .as_deref(),
            Some("2.900000")
        );
        p.reconcile_v2_wallet(12.9).await;
        p.reconcile_v2_wallet(12.9).await;
        // Drift: the reading after an adjustment is still far off.
        p.reconcile_v2_wallet(30.0).await;
        p.reconcile_v2_wallet(30.0).await;
        assert_eq!(v2_posting_count(&p).await, before + 10);
        p.reconcile_v2_wallet(40.0).await;
        assert!(*p.breaker_tripped.lock().await);
        assert_eq!(
            p.breaker_trip_reason.lock().await.as_deref(),
            Some("accounting_drift")
        );
        assert!(stop_not_requested(&p).await, "a trip parks, never exits");
    }

    fn band_promotion(params: &BandPolicyParams) -> PromotionArtifact {
        PromotionArtifact {
            schema_version: 1,
            inventory_model_version: CURRENT_INVENTORY_MODEL_VERSION,
            created_at: "2026-08-24T00:00:00Z".to_string(),
            source_report_hash: "gate-hash".to_string(),
            source_label: "fresh_gate_public_v1_20260821".to_string(),
            source_window: "2026-08-19T09:00:00Z..2026-08-24T02:05:00Z".to_string(),
            selected_strategy: StrategySpec::new(
                BAND_FAMILY,
                "1",
                stable_json_hash(params),
                "band-test",
            ),
            strategy_params: serde_json::to_value(params).unwrap(),
            data_manifest_hash: "fill-hash".to_string(),
            market_count: 222,
            trades: 222,
            win_rate: 0.9324,
            total_pnl: 1.0,
            avg_pnl: 0.0045,
            total_fees: 0.1,
            sharpe_like: 0.0,
            dominant_zone: Some("band".to_string()),
            dominant_zone_trade_share: Some(1.0),
            risk_notes: Vec::new(),
            promotion_gate: PromotionGate::default(),
            robust_diagnostics: None,
        }
    }

    #[test]
    fn band_policy_bounds_match_frozen_semantics() {
        let p = band_params();
        assert!(p.validate().is_ok());
        // Entry window [240, 270): first attempt at 240s, none at 270s.
        assert!(!p.in_entry_window(239.999));
        assert!(p.in_entry_window(240.0));
        assert!(p.in_entry_window(269.999));
        assert!(!p.in_entry_window(270.0));
        // Band (0.55, 0.92]: floor exclusive on VWAP, cap inclusive on worst.
        assert!(!p.quote_clears_band(0.55, 0.55));
        assert!(p.quote_clears_band(0.5501, 0.5501));
        assert!(p.quote_clears_band(0.91, 0.92));
        assert!(!p.quote_clears_band(0.91, 0.9201));
        assert!(p.quote_clears_band(0.92, 0.92));
        assert!(!p.quote_clears_band(0.93, 0.93));
    }

    #[test]
    fn band_policy_validate_rejects_bad_shapes() {
        let mut p = band_params();
        p.family = "other".to_string();
        assert!(p.validate().is_err());

        let mut p = band_params();
        p.decision_seconds = 300.0;
        assert!(p.validate().is_err());

        let mut p = band_params();
        p.entry_window_seconds = 61.0;
        assert!(p.validate().is_err(), "entry window may not cross expiry");

        let mut p = band_params();
        p.ask_floor = 0.92;
        assert!(p.validate().is_err());

        let mut p = band_params();
        p.stake_usd = 0.5;
        assert!(p.validate().is_err());
    }

    #[test]
    fn band_params_deny_unknown_fields() {
        let mut v = serde_json::to_value(band_params()).unwrap();
        v.as_object_mut()
            .unwrap()
            .insert("lock_strength_min".to_string(), serde_json::json!(0.5));
        assert!(serde_json::from_value::<BandPolicyParams>(v).is_err());
    }

    #[test]
    fn runtime_strategy_loads_band_promotion() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("band_promotion.json");
        let params = band_params();
        let artifact = band_promotion(&params);
        std::fs::write(&path, serde_json::to_vec(&artifact).unwrap()).unwrap();
        let mut settings = Settings::from_env();
        settings.promotion_artifact_path = path.display().to_string();

        let runtime = RuntimeStrategy::load(&settings).unwrap();

        assert_eq!(runtime.band.as_ref(), Some(&params));
        assert_eq!(runtime.strategy_spec, artifact.selected_strategy);
        assert!(!runtime.prefer_maker);
        assert_eq!(runtime.position_pct, 1.0);
        assert_eq!(runtime.max_projected_stressed_drawdown_pct, 1.0);
        assert_eq!(runtime.degraded_after_losses, 0);
    }

    #[test]
    fn runtime_strategy_rejects_tampered_band_params() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("band_promotion.json");
        let params = band_params();
        let mut artifact = band_promotion(&params);
        // Tamper with the frozen band after hashing: must be refused.
        artifact.strategy_params["ask_cap"] = serde_json::json!(0.95);
        std::fs::write(&path, serde_json::to_vec(&artifact).unwrap()).unwrap();
        let mut settings = Settings::from_env();
        settings.promotion_artifact_path = path.display().to_string();

        let err = RuntimeStrategy::load(&settings).unwrap_err().to_string();
        assert!(err.contains("hash mismatch"), "unexpected error: {err}");
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

    #[test]
    fn venue_status_incident_only_suspends() {
        let body = r#"{"activeIncidents":[{"name":"Delayed order reads","status":"INVESTIGATING","impact":"DEGRADEDPERFORMANCE"}],"activeMaintenances":[]}"#;
        assert_eq!(
            venue_status_verdict(body).unwrap().as_deref(),
            Some("incident \u{00b7} Delayed order reads")
        );
        // Resolved or low-impact incidents do not trip.
        let quiet = r#"{"activeIncidents":[
            {"name":"Old","status":"RESOLVED","impact":"MAJOROUTAGE"},
            {"name":"Notice","status":"MONITORING","impact":"OPERATIONAL"}
        ],"activeMaintenances":[]}"#;
        assert_eq!(venue_status_verdict(quiet).unwrap(), None);
    }

    #[test]
    fn venue_status_maintenance_only_suspends_when_in_progress() {
        // Live payload shape observed 2026-09-02.
        let body = r#"{"activeIncidents":[],"activeMaintenances":[{"name":"Scheduled Emergency CLOB maintenance","status":"INPROGRESS","start":"2026-09-02T01:30:00.000Z","duration":150}]}"#;
        assert_eq!(
            venue_status_verdict(body).unwrap().as_deref(),
            Some("maintenance \u{00b7} Scheduled Emergency CLOB maintenance")
        );
        for status in ["SCHEDULED", "NOTSTARTEDYET", "COMPLETED"] {
            let body = format!(
                r#"{{"activeIncidents":[],"activeMaintenances":[{{"name":"Later","status":"{status}","duration":150}}]}}"#
            );
            assert_eq!(venue_status_verdict(&body).unwrap(), None, "{status}");
        }
    }

    #[test]
    fn venue_status_incident_and_maintenance_both_named() {
        let body = r#"{"activeIncidents":[{"name":"API errors","status":"IDENTIFIED","impact":"PARTIALOUTAGE"}],"activeMaintenances":[{"name":"CLOB maintenance","status":"INPROGRESS"}]}"#;
        assert_eq!(
            venue_status_verdict(body).unwrap().as_deref(),
            Some("incident \u{00b7} API errors \u{00b7} maintenance \u{00b7} CLOB maintenance")
        );
    }

    #[test]
    fn venue_status_clear_when_nothing_active() {
        assert_eq!(
            venue_status_verdict(r#"{"activeIncidents":[],"activeMaintenances":[]}"#).unwrap(),
            None
        );
        assert_eq!(
            venue_status_verdict(r#"{"page":{"name":"Polymarket"}}"#).unwrap(),
            None
        );
    }

    #[test]
    fn venue_status_malformed_payload_fails_open() {
        // Not JSON at all (an HTML error page, empty body): no verdict, the
        // loop keeps whatever flag it had.
        assert!(venue_status_verdict("<html>502 Bad Gateway</html>").is_err());
        assert!(venue_status_verdict("").is_err());
        // JSON of the wrong shape never reads as an outage.
        for body in [
            r#"[]"#,
            r#"{"activeIncidents":"MAJOROUTAGE","activeMaintenances":"INPROGRESS"}"#,
            r#"{"activeIncidents":[1,"x",null],"activeMaintenances":[{"status":7},{"name":"n"}]}"#,
        ] {
            assert_eq!(venue_status_verdict(body).unwrap(), None, "{body}");
        }
        // An entry missing its name still trips and is labelled.
        assert_eq!(
            venue_status_verdict(r#"{"activeMaintenances":[{"status":"inprogress"}]}"#)
                .unwrap()
                .as_deref(),
            Some("maintenance \u{00b7} unnamed")
        );
    }
}
