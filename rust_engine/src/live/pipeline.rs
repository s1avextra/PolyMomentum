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
use serde_json::json;
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tokio::time::sleep;

use crate::backtest::strategies::StrategyVariant;
use crate::clob::{create_shared_client, SharedClobClient};
use crate::clob_user_ws::{polymarket_user_feed, UserChannelAuth, UserEvent};
use crate::config::{RuntimeMode, Settings};
use crate::data::ctf::{CtfReader, Resolution};
use crate::data::gamma::GammaClient;
use crate::data::scanner::{scan_candle_markets, CandleContract};
use crate::execution::order_manager::OrderManager;
use crate::live::breaker::{BreakerConfig, BreakerState};
use crate::live::paper_fill::{simulate_paper_fill, PaperFillCfg};
use crate::live::window::estimate_window_minutes;
use crate::monitoring::alerter::Alerter;
use crate::monitoring::session::{OrderFilled, OrderReconciled, SessionMonitor, SignalEvaluation};
use crate::polymarket_ws::{
    new_shared_book, new_subscription_notify, polymarket_book_feed, SharedBookState,
};
use crate::price_state::PriceState;
use crate::release::ReleaseManifest;
use crate::risk::manager::{RiskConfig, RiskManager, TradeRecord};
use crate::strategy::decision::{
    decide_candle_trade, DecisionResult, DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_EDGE,
    ZoneConfig,
};
use crate::strategy::microstructure::{
    BookLevelView, BookMicrostructure, MicrostructureConfig,
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

#[derive(Debug, Clone)]
struct PaperPosition {
    direction: String,
    entry_price: f64,
    fee: f64,
    size: f64,
    open_btc: f64,
    end_time: f64,
    asset: String,
    contract_id: String,
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
        })
    }
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
        })
    }

    fn from_json(v: &serde_json::Value) -> Option<Self> {
        Some(Self {
            our_actual: v.get("our_actual")?.as_str()?.to_string(),
            our_open_btc: v.get("our_open_btc")?.as_f64()?,
            our_close_btc: v.get("our_close_btc")?.as_f64()?,
            end_time: v.get("end_time")?.as_f64()?,
            attempts: v
                .get("attempts")
                .and_then(|x| x.as_u64())
                .unwrap_or(0) as u32,
            direction: v
                .get("direction")
                .and_then(|x| x.as_str())
                .map(ToString::to_string),
            entry_price: v.get("entry_price").and_then(|x| x.as_f64()),
            fee: v.get("fee").and_then(|x| x.as_f64()),
            size: v.get("size").and_then(|x| x.as_f64()),
            provisional_won: v.get("provisional_won").and_then(|x| x.as_bool()),
            provisional_pnl: v.get("provisional_pnl").and_then(|x| x.as_f64()),
        })
    }

    fn oracle_pnl(&self, polymarket_actual: &str) -> Option<(bool, f64, bool, f64)> {
        let direction = self.direction.as_deref()?;
        let entry_price = self.entry_price?;
        let size = self.size?;
        let fee = self.fee.unwrap_or(0.0);
        let provisional_won = self.provisional_won?;
        let provisional_pnl = self.provisional_pnl?;
        let final_won = match polymarket_actual {
            "up" | "down" => polymarket_actual == direction,
            "tie" => false,
            _ => return None,
        };
        let final_pnl = paper_outcome_pnl(final_won, entry_price, size, fee);
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

#[derive(Debug, Clone)]
struct RuntimeStrategy {
    strategy_spec: StrategySpec,
    zone_config: ZoneConfig,
    skip_dead_zone: bool,
    min_confidence: f64,
    min_edge: f64,
    position_pct: f64,
    max_per_market_usd: f64,
    prefer_maker: bool,
    default_fee_rate: f64,
    microstructure: MicrostructureConfig,
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
        let variant: StrategyVariant = serde_json::from_value(artifact.strategy_params.clone())
            .context("parse promoted strategy_params as StrategyVariant")?;
        let params_hash = stable_json_hash(&variant);
        if params_hash != artifact.selected_strategy.params_hash {
            bail!(
                "promotion artifact hash mismatch: strategy_params hash {} != selected_strategy hash {}",
                params_hash,
                artifact.selected_strategy.params_hash
            );
        }
        Ok(Self {
            strategy_spec: artifact.selected_strategy,
            zone_config: variant.zone_config,
            skip_dead_zone: variant.skip_dead_zone,
            min_confidence: variant.min_confidence,
            min_edge: variant.min_edge,
            position_pct: variant.position_pct,
            max_per_market_usd: variant.max_per_market_usd,
            prefer_maker: variant.prefer_maker,
            default_fee_rate: variant.default_fee_rate,
            microstructure: variant.microstructure,
            source: format!("promotion:{path}"),
        })
    }

    fn from_settings(settings: &Settings) -> Self {
        let zone_config = ZoneConfig::from_settings(settings);
        let params = json!({
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
        Self {
            strategy_spec: StrategySpec::from_serializable_params(
                "candle_momentum",
                "1",
                &params,
                format!(
                    "position_pct={:.4};max_per_market_usd={:.2}",
                    settings.candle_position_pct, settings.max_position_per_market_usd
                ),
            ),
            zone_config,
            skip_dead_zone: settings.candle_skip_dead_zone,
            min_confidence: DEFAULT_MIN_CONFIDENCE,
            min_edge: DEFAULT_MIN_EDGE,
            position_pct: settings.candle_position_pct,
            max_per_market_usd: settings.max_position_per_market_usd,
            prefer_maker: settings.candle_prefer_maker,
            default_fee_rate: 0.072,
            microstructure: MicrostructureConfig::disabled(),
            source: "settings".to_string(),
        }
    }
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
    paper_positions: Mutex<HashMap<String, PaperPosition>>,
    oracle_pending: Mutex<HashMap<String, OraclePending>>,
    breaker: Mutex<BreakerState>,
    breaker_tripped: Mutex<bool>,
    price_state: Arc<RwLock<PriceState>>,
    book_state: SharedBookState,
    tracked_tokens: Arc<RwLock<Vec<String>>>,
    resub_notify: Arc<Notify>,
    tracked_markets: Arc<RwLock<Vec<String>>>,
    user_resub_notify: Arc<Notify>,
    reconciled_trade_ids: Mutex<HashSet<String>>,
    stop: Arc<Notify>,
    kill_switch_path: PathBuf,
    cycle_count: Mutex<u64>,
}

impl Pipeline {
    pub async fn new(settings: Settings, mode: Mode) -> Result<Arc<Self>> {
        let release_manifest = ReleaseManifest::capture(&settings, mode.runtime_mode());
        let runtime_strategy = RuntimeStrategy::load(&settings)?;
        let bankroll = if settings.bankroll_usd > 0.0 {
            settings.bankroll_usd
        } else {
            // Fall back to wallet detection if private key set
            try_wallet_bankroll(&settings).await.unwrap_or(0.0)
        };
        let risk_cfg = RiskConfig {
            initial_bankroll: bankroll,
            max_per_market_override: runtime_strategy.max_per_market_usd,
            ..Default::default()
        };
        let risk = RiskManager::open(&settings.state_db_path, risk_cfg).await?;

        let monitor = Arc::new(SessionMonitor::open(&settings.session_log_dir)?);
        let alerter = Alerter::new(std::env::var("SLACK_WEBHOOK_URL").ok());
        let gamma = GammaClient::new(&settings.poly_gamma_url);
        let ctf = CtfReader::new(&settings.polygon_rpc_url);
        let breaker_cfg = BreakerConfig {
            min_trades: settings.candle_breaker_min_trades.max(1) as u32,
            min_win_rate: settings.candle_breaker_min_win_rate,
            max_drawdown_pct: settings.candle_breaker_max_drawdown_pct,
        };

        // Restore breaker + paper positions + oracle pending
        let breaker_tripped = matches!(
            risk.get_meta("candle_breaker_tripped").await?.as_deref(),
            Some("1")
        );
        let breaker_state = match risk.get_meta("candle_breaker_state").await? {
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
        let restored_open_exposure: f64 = paper_positions
            .values()
            .map(|p| p.entry_price * p.size)
            .sum();

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
            );
            client.write().await.set_signing_key(&settings.private_key);
            client.write().await.warm_connection().await;
            tracing::info!("CLOB direct order placement ENABLED (live mode)");
            Some(client)
        } else {
            None
        };

        let p = Arc::new(Self {
            kill_switch_path: PathBuf::from(&settings.kill_switch_path),
            settings,
            mode,
            release_manifest,
            runtime_strategy,
            risk,
            order_manager: Mutex::new(OrderManager::new()),
            clob,
            monitor,
            alerter,
            gamma,
            ctf,
            breaker_cfg,
            momentum: Mutex::new(momentum_map),
            contracts: RwLock::new(Vec::new()),
            traded: Mutex::new(HashSet::new()),
            paper_positions: Mutex::new(paper_positions),
            oracle_pending: Mutex::new(oracle_pending),
            breaker: Mutex::new(breaker_state),
            breaker_tripped: Mutex::new(breaker_tripped),
            price_state: Arc::new(RwLock::new(PriceState::new())),
            book_state: new_shared_book(),
            tracked_tokens: Arc::new(RwLock::new(Vec::new())),
            resub_notify: new_subscription_notify(),
            tracked_markets: Arc::new(RwLock::new(Vec::new())),
            user_resub_notify: new_subscription_notify(),
            reconciled_trade_ids: Mutex::new(HashSet::new()),
            stop: Arc::new(Notify::new()),
            cycle_count: Mutex::new(0),
        });
        if breaker_tripped {
            let metrics = breaker_state.metrics(
                restored_open_exposure,
                p.settings.bankroll_usd.max(1.0),
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
                .send("info", "PolyMomentum Rust starting", &format!("mode={}", self.mode.as_str()))
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

    async fn handle_user_event(&self, event: UserEvent) -> Result<()> {
        match event {
            UserEvent::Order(order) => {
                if order.id.is_empty() {
                    return Ok(());
                }
                let ts = nonzero_ts_or_now(order.timestamp_s());
                let reconciled = {
                    let mut orders = self.order_manager.lock().await;
                    let res = if order.is_canceled() {
                        orders.cancel_by_venue_order_id(&order.id, ts)
                    } else {
                        orders.reconcile_live_by_venue_order_id(&order.id, ts)
                    };
                    match res {
                        Ok(o) => Some(OrderReconciled {
                            intent_id: o.intent.intent_id.clone(),
                            order_id: order.id.clone(),
                            source: "clob_user_ws.order".to_string(),
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
                        Err(e) => {
                            tracing::debug!(order_id = %short_cid(&order.id), error = %e, "unmatched user-channel order event");
                            None
                        }
                    }
                };
                if let Some(evt) = reconciled {
                    self.monitor.record_order_reconciled(&evt);
                }
            }
            UserEvent::Trade(trade) => {
                if trade.id.is_empty() {
                    return Ok(());
                }
                if !trade.is_fill_status() && !trade.is_failed() {
                    return Ok(());
                }
                {
                    let mut seen = self.reconciled_trade_ids.lock().await;
                    if !seen.insert(trade.id.clone()) {
                        return Ok(());
                    }
                }
                let ts = nonzero_ts_or_now(trade.timestamp_s());
                for order_id in trade.candidate_order_ids() {
                    let outcome = {
                        let mut orders = self.order_manager.lock().await;
                        if trade.is_failed() {
                            match orders.reject_by_venue_order_id(
                                &order_id,
                                "clob trade failed",
                                ts,
                            ) {
                                Ok(o) => Some((o.clone(), false)),
                                Err(_) => None,
                            }
                        } else {
                            match orders.fill_by_venue_order_id(
                                &order_id,
                                trade.size(),
                                trade.price(),
                                trade.fee(),
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
                        order_id: order_id.clone(),
                        source: "clob_user_ws.trade".to_string(),
                        venue_state: trade.status.clone(),
                        filled: order.filled_size,
                        requested: order.requested_size,
                        fill_price: trade.price(),
                        fee: order.total_fees,
                        detail: trade.id.clone(),
                    });
                    if filled {
                        self.monitor.record_order_filled(&OrderFilled {
                            intent_id: order.intent.intent_id.clone(),
                            order_id,
                            filled: trade.size(),
                            requested: order.requested_size,
                            fill_pct: order.fill_pct(),
                            fill_price: trade.price(),
                            limit_price: order.intent.limit_price.unwrap_or(trade.price()),
                            slippage: 0.0,
                            slippage_bps: 0.0,
                            fill_time_s: (ts - order.created_ts).max(0.0),
                            fee: trade.fee(),
                            n_trades: 1,
                        });
                    } else {
                        self.monitor.record_order_rejected(
                            &trade.asset_id,
                            "clob trade failed",
                            trade.price(),
                            trade.size(),
                        );
                    }
                    return Ok(());
                }
                tracing::debug!(trade_id = %trade.id, "user-channel trade did not match a managed order");
            }
        }
        Ok(())
    }

    pub async fn refresh_contracts(&self) -> Result<()> {
        let markets = self
            .gamma
            .fetch_markets_by_end_date(3.0, 0.0)
            .await?;
        let contracts = scan_candle_markets(&markets, 1.0, 50.0);

        let active_cids: HashSet<String> =
            contracts.iter().map(|c| c.market.condition_id.clone()).collect();
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

    async fn contract_refresh_loop(self: Arc<Self>) {
        loop {
            sleep(Duration::from_secs(120)).await;
            if let Err(e) = self.refresh_contracts().await {
                tracing::warn!(error = %e, "refresh failed");
                self.monitor.record_error("contract_refresh", &e.to_string(), true);
            }
        }
    }

    async fn scan_loop(self: Arc<Self>) {
        let mut last_btc = 0.0;
        let mut unchanged = 0u32;
        loop {
            let cycle_start = Instant::now();
            {
                let mut c = self.cycle_count.lock().await;
                *c += 1;
            }

            let ps = self.price_state.read().await.clone();
            let btc = ps.mid_price;
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

            // Tick the BTC momentum detector
            {
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
                det.add_tick(btc, None);
                det.set_realized_vol(ps.implied_vol);
            }

            // Tick alts (ETH/SOL) if cross-asset is enabled — feed their WS
            // prices (`ps.alt_mid`) into per-asset momentum detectors.
            if self.settings.candle_cross_asset_enabled {
                let mut moms = self.momentum.lock().await;
                for asset in ["ETH", "SOL"] {
                    if let Some(&alt_price) = ps.alt_mid.get(asset) {
                        if alt_price > 0.0 {
                            let det = moms.entry(asset.to_string()).or_insert_with(|| {
                                MomentumDetector::new(
                                    Some(ps.implied_vol),
                                    MomentumConfig {
                                        noise_z_threshold: self
                                            .settings
                                            .candle_noise_z_threshold,
                                        ..Default::default()
                                    },
                                )
                            });
                            det.add_tick(alt_price, None);
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
                let open_exposure: f64 = self
                    .paper_positions
                    .lock()
                    .await
                    .values()
                    .map(|p| p.entry_price * p.size)
                    .sum();
                if let Some(reason) = bs.should_trip(
                    &self.breaker_cfg,
                    open_exposure,
                    self.settings.bankroll_usd.max(1.0),
                ) {
                    self.trip_breaker(reason).await;
                }
            }
            if *self.breaker_tripped.lock().await {
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

                let Ok(end) = parse_end(&c.end_date) else { continue };
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

                // Pull real-time best up/down from the WS book if fresh,
                // otherwise fall back to the scanner snapshot.
                let (up_price, down_price) = pick_book_prices(c, &books, now_ts);

                // Detect momentum for the contract's own asset
                let signal = {
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
                        det.set_window_open(&cid, asset_price);
                    }
                    det.detect(&cid, minutes_elapsed, minutes_left, asset_price, None)
                };
                let Some(signal) = signal else { continue };

                let decision = decide_candle_trade(
                    &signal,
                    minutes_elapsed,
                    minutes_left,
                    window_minutes,
                    up_price,
                    down_price,
                    asset_price,
                    signal.open_price,
                    ps.implied_vol,
                    self.runtime_strategy.min_confidence,
                    self.runtime_strategy.min_edge,
                    self.runtime_strategy.skip_dead_zone,
                    &self.runtime_strategy.zone_config,
                    0.0, // cross-asset boost not yet wired
                );

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
                            elapsed_min: signal.minutes_elapsed,
                            remaining_min: signal.minutes_remaining,
                            dir: signal.direction.clone(),
                            vol_fast,
                            vol_slow,
                            implied_vol: ps.implied_vol,
                            cross_boost: 0.0,
                            up_price,
                            down_price,
                            book_spread: 0.0,
                            book_pressure: 0.0,
                            book_bid_depth: 0.0,
                            book_ask_depth: 0.0,
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
                    DecisionResult::Trade(decision) => {
                        let traded_token_id = if decision.direction == "up" {
                            &c.up_token_id
                        } else {
                            &c.down_token_id
                        };
                        let micro =
                            live_microstructure(traded_token_id, &books, now_ts);
                        if let Err(skip) =
                            micro.check_long_entry(&self.runtime_strategy.microstructure)
                        {
                            let aggregate = format!("{}_{}", skip.reason, decision.zone);
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
                                elapsed_min: signal.minutes_elapsed,
                                remaining_min: signal.minutes_remaining,
                                dir: signal.direction.clone(),
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
                                zone: decision.zone.clone(),
                                fair: decision.fair_value,
                                edge: decision.edge,
                                decision_trade: true,
                                execution_attempted: false,
                                traded: false,
                                skip_reason: Some(skip.reason),
                                skip_detail: Some(skip.detail),
                            });
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
                            elapsed_min: signal.minutes_elapsed,
                            remaining_min: signal.minutes_remaining,
                            dir: signal.direction.clone(),
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
                            zone: decision.zone.clone(),
                            fair: decision.fair_value,
                            edge: decision.edge,
                            decision_trade: true,
                            execution_attempted: true,
                            traded: false,
                            skip_reason: None,
                            skip_detail: None,
                        });
                        if let Err(e) = self.execute_trade(c, &signal, &decision, &ps).await {
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
        ps: &PriceState,
    ) -> Result<()> {
        let bankroll = self.risk.effective_bankroll().await;
        let mut position = bankroll * self.runtime_strategy.position_pct;

        // Volatility regime sizing
        let vol_ratio = if ps.implied_vol > 0.0 {
            ps.implied_vol / 0.50
        } else {
            1.0
        };
        if vol_ratio > 2.5 {
            position *= self.settings.candle_vol_extreme_multiplier;
        } else if vol_ratio > 1.5 {
            position *= self.settings.candle_vol_high_multiplier;
        }

        let max_per_market = self.risk.max_per_market().await;
        let avail = self.risk.available_capital().await;
        position = position.min(max_per_market).min(avail);
        if 0.0 < position && position < 1.0 && avail >= 1.0 {
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
        let market_price = decision.market_price;

        match self.mode {
            Mode::Paper => {
                let cfg = PaperFillCfg {
                    prefer_maker: self.runtime_strategy.prefer_maker,
                    default_taker_rate: self.runtime_strategy.default_fee_rate,
                    ..Default::default()
                };
                let Some(fill) = simulate_paper_fill(market_price, position, &cfg) else {
                    return Ok(());
                };
                let expected_profit = fill.shares * (decision.fair_value - fill.fill_price) - fill.fee;
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
                    &decision,
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
                        .fill(&intent.intent_id, fill.shares, fill.fill_price, fill.fee, now_ts)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    ack_state
                };
                let order_value = fill.fill_price * fill.shares;
                tracing::info!(
                    direction = %signal.direction,
                    cost = position,
                    fee = fill.fee,
                    profit = expected_profit,
                    edge = decision.edge,
                    minutes_left = signal.minutes_remaining,
                    "candle.trade.paper"
                );
                self.monitor.record_order_placed(&crate::monitoring::session::OrderPlaced {
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
                });
                self.monitor.record_order_filled(&crate::monitoring::session::OrderFilled {
                    intent_id: intent.intent_id,
                    order_id: short_cid(&order_id),
                    filled: fill.shares,
                    requested: fill.shares,
                    fill_pct: 1.0,
                    fill_price: fill.fill_price,
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
                self.traded.lock().await.insert(contract.market.condition_id.clone());
                self.risk
                    .record_trade(TradeRecord {
                        timestamp: now_ts,
                        market_condition_id: contract.market.condition_id.clone(),
                        outcome_idx: 0,
                        side: "buy".into(),
                        size: fill.shares,
                        price: fill.fill_price,
                        cost: position,
                        event_id: contract.market.event_id.clone(),
                        pnl: 0.0,
                        paper: true,
                    })
                    .await?;

                let end_ts = parse_end(&contract.end_date)?.timestamp() as f64;
                let pp = PaperPosition {
                    direction: signal.direction.clone(),
                    entry_price: fill.fill_price,
                    fee: fill.fee,
                    size: fill.shares,
                    open_btc: signal.open_price,
                    end_time: end_ts,
                    asset: contract.asset.clone(),
                    contract_id: contract.market.condition_id.clone(),
                };
                self.paper_positions
                    .lock()
                    .await
                    .insert(contract.market.condition_id.clone(), pp);
                self.persist_paper_positions().await;
                Ok(())
            }
            Mode::Live => {
                let Some(clob) = self.clob.clone() else {
                    tracing::error!("live mode but no CLOB client (missing api keys / private key)");
                    return Ok(());
                };
                // Round to the market's advertised tick and keep a sane fallback
                // for legacy metadata that does not include minimum_tick_size.
                let tick = contract.market.minimum_tick_size.unwrap_or(0.01).max(0.0001);
                let limit_price = ((market_price / tick).round() * tick).clamp(tick, 1.0 - tick);
                let shares = (position / limit_price).round().max(1.0);
                let zone = decision.zone.as_str();
                let prefer_maker = self.runtime_strategy.prefer_maker && zone != "terminal";
                let neg_risk = contract.market.neg_risk;
                let order_signal = Signal::from_candle_decision(
                    contract.market.condition_id.clone(),
                    token_id.clone(),
                    &decision,
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
                    format!("{}:{limit_price:.4}:{shares:.4}", contract.market.condition_id),
                );

                let t_start = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                {
                    let mut orders = self.order_manager.lock().await;
                    orders
                        .create_intent(intent.clone(), t_start)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    orders
                        .risk_accept(&intent.intent_id, t_start)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    orders
                        .submit(&intent.intent_id, None, t_start)
                        .map_err(|e| anyhow::anyhow!(e))?;
                }
                let result = if prefer_maker {
                    let maker = clob
                        .write()
                        .await
                        .place_maker_order(token_id, limit_price, shares, "BUY", neg_risk, tick)
                        .await;
                    if maker.is_err() {
                        tracing::warn!("CLOB maker rejected; taker fallback");
                        clob.write()
                            .await
                            .place_taker_order(token_id, limit_price, shares, "BUY", neg_risk, tick)
                            .await
                    } else {
                        maker
                    }
                } else {
                    clob.write()
                        .await
                        .place_taker_order(token_id, limit_price, shares, "BUY", neg_risk, tick)
                        .await
                };
                let submit_latency_s = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0)
                    - t_start;

                match result {
                    Ok(order_id) => {
                        let ack_state = {
                            let mut orders = self.order_manager.lock().await;
                            orders
                                .ack(&intent.intent_id, Some(order_id.clone()), t_start + submit_latency_s)
                                .map_err(|e| anyhow::anyhow!(e))?
                                .state
                                .as_str()
                                .to_string()
                        };
                        let order_value = limit_price * shares;
                        self.monitor.record_order_placed(&crate::monitoring::session::OrderPlaced {
                            intent_id: intent.intent_id,
                            token_id: short_cid(token_id),
                            side: "BUY".into(),
                            state: ack_state,
                            price: limit_price,
                            live_price: market_price,
                            size: shares,
                            order_value,
                            order_id: short_cid(&order_id),
                            book_best_ask: market_price,
                            book_ask_depth: 0.0,
                            book_bid_depth: 0.0,
                            balance_usd: self.risk.effective_bankroll().await,
                        });
                        tracing::info!(
                            order_id = short_cid(&order_id),
                            cost = order_value,
                            submit_latency_s,
                            "candle.trade.live.accepted_unconfirmed"
                        );
                    }
                    Err(e) => {
                        let truncated = if e.len() > 200 { &e[..200] } else { e.as_str() };
                        {
                            let mut orders = self.order_manager.lock().await;
                            let _ = orders.reject(&intent.intent_id, truncated, t_start + submit_latency_s);
                        }
                        self.monitor
                            .record_order_rejected(token_id, truncated, limit_price, shares);
                        tracing::warn!(error = %truncated, "candle.trade.live.failed");
                    }
                }
                Ok(())
            }
        }
    }

    async fn paper_resolution_loop(self: Arc<Self>) {
        loop {
            let near_resolution = {
                let pp = self.paper_positions.lock().await;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                pp.values().any(|p| (p.end_time - now) > 0.0 && (p.end_time - now) < 15.0)
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
                let close_price = if pos.asset == "BTC" {
                    btc
                } else {
                    ps.alt_mid.get(&pos.asset).copied().unwrap_or(btc)
                };
                let actual = if close_price >= pos.open_btc { "up" } else { "down" };
                let won = actual == pos.direction;
                let pnl = paper_outcome_pnl(won, pos.entry_price, pos.size, pos.fee);
                self.risk.record_pnl(pnl).await.ok();
                self.risk.record_fees(pos.fee).await;
                let mut bs = self.breaker.lock().await;
                bs.record_resolution(won, pnl);
                drop(bs);
                self.persist_breaker_state().await;

                self.monitor.record_resolution(
                    cid,
                    &pos.direction,
                    actual,
                    won,
                    pnl,
                    pos.entry_price,
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
                let open_exp: f64 = self
                    .paper_positions
                    .lock()
                    .await
                    .values()
                    .map(|p| p.entry_price * p.size)
                    .sum();
                if let Some(reason) = bs.should_trip(
                    &self.breaker_cfg,
                    open_exp,
                    self.settings.bankroll_usd.max(1.0),
                ) {
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
                            to_remove.push(cid.clone());
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
                        if !agreed {
                            if let Some((final_won, final_pnl, provisional_won, provisional_pnl)) =
                                entry.oracle_pnl(res_str)
                            {
                                let pnl_delta = final_pnl - provisional_pnl;
                                if pnl_delta.abs() > 1e-9 {
                                    if let Err(e) = self.risk.record_pnl(pnl_delta).await {
                                        tracing::warn!(
                                            cid = short_cid(&cid),
                                            error = %e,
                                            "oracle pnl correction failed"
                                        );
                                    } else {
                                        let mut bs = self.breaker.lock().await;
                                        bs.correct_resolution(provisional_won, final_won, pnl_delta);
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
                            }
                            tracing::warn!(
                                cid = short_cid(&cid),
                                ours = %entry.our_actual,
                                polymarket = res_str,
                                "candle.oracle.disagreement"
                            );
                        } else {
                            tracing::info!(cid = short_cid(&cid), "candle.oracle.agreed");
                        }
                        if is_tie {
                            self.trip_breaker("oracle_tie").await;
                            self.stop.notify_one();
                        }
                        to_remove.push(cid);
                    }
                    Err(e) => {
                        tracing::warn!(cid = short_cid(&cid), error = %e, "ctf read failed");
                        if entry.attempts >= MAX_ATTEMPTS {
                            to_remove.push(cid.clone());
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
            let sources: HashMap<String, f64> = ps
                .prices
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect();
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
            let exposure = self.risk.total_exposure().await;
            let avail = self.risk.available_capital().await;
            let n_paper = self.paper_positions.lock().await.len() as u64;
            self.monitor.record_risk_state(
                bankroll,
                exposure,
                avail,
                n_paper,
                bs.realized_pnl,
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
        let _ = self.risk.set_meta("candle_breaker_tripped", "1").await;
        self.persist_breaker_state().await;
        let bs = *self.breaker.lock().await;
        let open_exposure: f64 = self
            .paper_positions
            .lock()
            .await
            .values()
            .map(|p| p.entry_price * p.size)
            .sum();
        let metrics = bs.metrics(open_exposure, self.settings.bankroll_usd.max(1.0));
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
                &format!("reason={reason} wins={} losses={} pnl=${:.2}", bs.wins, bs.losses, bs.realized_pnl),
            )
            .await;
    }

    fn kill_switch_active(&self) -> bool {
        self.kill_switch_path.exists()
    }

    async fn persist_paper_positions(&self) {
        let pp = self.paper_positions.lock().await.clone();
        let entries: Vec<(String, serde_json::Value)> = pp
            .into_iter()
            .map(|(k, v)| (k, v.to_json()))
            .collect();
        if let Err(e) = self.risk.save_paper_positions(&entries).await {
            tracing::warn!(error = %e, "persist paper positions failed");
        }
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
        let entries: Vec<(String, serde_json::Value)> = op
            .into_iter()
            .map(|(k, v)| (k, v.to_json()))
            .collect();
        if let Err(e) = self.risk.save_oracle_pending(&entries).await {
            tracing::warn!(error = %e, "persist oracle pending failed");
        }
    }
}

fn pick_book_prices(
    contract: &CandleContract,
    books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
    now_ts: f64,
) -> (f64, f64) {
    let up = books
        .get(&contract.up_token_id)
        .and_then(|b| {
            let age = now_ts - b.last_update_us as f64 / 1_000_000.0;
            if age < 30.0 && b.best_ask > 0.0 {
                Some(b.best_ask)
            } else {
                None
            }
        })
        .unwrap_or(contract.up_price);
    let down = books
        .get(&contract.down_token_id)
        .and_then(|b| {
            let age = now_ts - b.last_update_us as f64 / 1_000_000.0;
            if age < 30.0 && b.best_ask > 0.0 {
                Some(b.best_ask)
            } else {
                None
            }
        })
        .unwrap_or(contract.down_price);
    (up, down)
}

fn live_microstructure(
    token_id: &str,
    books: &HashMap<String, crate::polymarket_ws::TokenBookState>,
    now_ts: f64,
) -> BookMicrostructure {
    let Some(book) = books.get(token_id) else {
        return BookMicrostructure::default();
    };
    let age = now_ts - book.last_update_us as f64 / 1_000_000.0;
    if age >= 30.0 {
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
    BookMicrostructure::from_levels(&bids, &asks, 3)
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
    let r = crate::data::wallet::WalletReader::new(&settings.polygon_rpc_url, &settings.private_key)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::experiment::{PromotionArtifact, PromotionGate};
    use tempfile::TempDir;

    fn promotion_for_variant(variant: &StrategyVariant) -> PromotionArtifact {
        let spec = StrategySpec::from_serializable_params(
            "candle_momentum",
            "1",
            variant,
            "test-risk",
        );
        PromotionArtifact {
            schema_version: 1,
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
        }
    }

    #[test]
    fn runtime_strategy_uses_promoted_variant() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("promotion.json");
        let variant = StrategyVariant::loose_maker();
        let artifact = promotion_for_variant(&variant);
        std::fs::write(&path, serde_json::to_vec(&artifact).unwrap()).unwrap();
        let mut settings = Settings::from_env();
        settings.promotion_artifact_path = path.display().to_string();

        let runtime = RuntimeStrategy::load(&settings).unwrap();

        assert_eq!(runtime.strategy_spec, artifact.selected_strategy);
        assert!(runtime.prefer_maker);
        assert_eq!(runtime.min_confidence, variant.min_confidence);
        assert_eq!(runtime.min_edge, variant.min_edge);
        assert_eq!(runtime.max_per_market_usd, variant.max_per_market_usd);
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
    fn oracle_pnl_treats_polymarket_tie_as_loss() {
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
        };

        let (final_won, final_pnl, provisional_won, provisional_pnl) =
            pending.oracle_pnl("tie").unwrap();

        assert!(!final_won);
        assert!(provisional_won);
        assert!((final_pnl - -4.21).abs() < 1e-9);
        assert!((provisional_pnl - 5.79).abs() < 1e-9);
    }
}
