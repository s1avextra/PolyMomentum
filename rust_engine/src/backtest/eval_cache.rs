//! One-pass replay-grade evaluation cache for strategy search.
//!
//! This emits the same compact `signal.evaluation` rows consumed by
//! `polymomentum-engine sweep`, plus synthetic `resolution.resolved` rows for
//! every candle in the requested universe. It lets broad parameter searches run
//! against cached PMXT data without replaying raw L2 once per variant.

use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::json;

use crate::backtest::btc_history::BTCHistory;
use crate::backtest::distill;
use crate::backtest::fill_model::Perfect;
use crate::backtest::harness::CandleUniverse;
use crate::backtest::l2_replay::{
    BacktestOrder, FillModel, L2BacktestEngine, StaticLatencyConfig, Strategy, TokenBook,
};
use crate::backtest::pmxt::PMXTv2Loader;
use crate::data::scanner::CandleContract;
use crate::monitoring::session::SignalEvaluation;
use crate::strategy::decision::decide_candle_trade;
use crate::strategy::microstructure::{BookLevelView, BookMicrostructure};
use crate::strategy::momentum::{MomentumConfig, MomentumDetector};

#[derive(Clone)]
pub struct EvalCacheConfig {
    pub hours: Vec<DateTime<Utc>>,
    pub universe: CandleUniverse,
    pub btc_history: Arc<BTCHistory>,
    pub cache_dir: PathBuf,
    pub shared_distilled_dir: Option<PathBuf>,
    pub output: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvalCacheSummary {
    pub output: String,
    pub hours: usize,
    pub contracts: usize,
    pub events_loaded: u64,
    pub evaluations: u64,
    pub resolutions: u64,
}

pub struct EvalCacheGenerator {
    universe_by_token: BTreeMap<String, EvalRuntimeContract>,
    books: BTreeMap<String, TokenBook>,
    momentum: MomentumDetector,
    btc_history: Arc<BTCHistory>,
    rows: Vec<serde_json::Value>,
    last_eval_bucket_by_token: BTreeMap<String, i64>,
    last_tick_ts_s: f64,
    evaluations: u64,
}

#[derive(Debug, Clone)]
struct EvalRuntimeContract {
    contract: CandleContract,
    close_ts_s: f64,
    open_ts_s: f64,
    window_minutes: f64,
}

impl EvalRuntimeContract {
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
        }
    }
}

impl EvalCacheGenerator {
    pub fn new(universe: &CandleUniverse, btc_history: Arc<BTCHistory>) -> Self {
        let mut universe_by_token = BTreeMap::new();
        for contract in &universe.contracts {
            let runtime = EvalRuntimeContract::from_contract(contract);
            if !contract.up_token_id.is_empty() {
                universe_by_token.insert(contract.up_token_id.clone(), runtime.clone());
            }
            if !contract.down_token_id.is_empty() {
                universe_by_token.insert(contract.down_token_id.clone(), runtime.clone());
            }
        }
        Self {
            universe_by_token,
            books: BTreeMap::new(),
            momentum: MomentumDetector::new(
                None,
                MomentumConfig {
                    noise_z_threshold: 0.3,
                    ..Default::default()
                },
            ),
            btc_history,
            rows: Vec::new(),
            last_eval_bucket_by_token: BTreeMap::new(),
            last_tick_ts_s: 0.0,
            evaluations: 0,
        }
    }

    fn into_rows(self) -> Vec<serde_json::Value> {
        self.rows
    }

    fn fresh_ask(&self, token_id: &str, now_ts: f64, fallback: f64) -> f64 {
        self.books
            .get(token_id)
            .filter(|b| now_ts - b.last_update_ts_s <= 30.0 && b.best_ask > 0.0)
            .map(|b| b.best_ask)
            .unwrap_or(fallback)
    }

    fn microstructure_for_token(&self, token_id: &str, now_ts: f64) -> BookMicrostructure {
        self.books
            .get(token_id)
            .filter(|b| now_ts - b.last_update_ts_s <= 30.0)
            .map(book_microstructure)
            .unwrap_or_default()
    }

    #[allow(clippy::too_many_arguments)]
    fn record_evaluation(
        &mut self,
        timestamp_s: f64,
        runtime: &EvalRuntimeContract,
        signal: &crate::strategy::momentum::MomentumSignal,
        up_price: f64,
        down_price: f64,
        implied_vol: f64,
        micro: &BookMicrostructure,
    ) {
        let decision = decide_candle_trade(
            signal,
            signal.minutes_elapsed,
            signal.minutes_remaining,
            runtime.window_minutes,
            up_price,
            down_price,
            signal.current_price,
            signal.open_price,
            implied_vol,
            0.0,
            0.0,
            false,
            &crate::strategy::decision::ZoneConfig {
                early_min_confidence: 0.0,
                late_min_confidence: 0.0,
                terminal_min_confidence: 0.0,
                early_min_z: 0.0,
                primary_min_z: 0.0,
                late_min_z: 0.0,
                terminal_min_z: 0.0,
                early_min_edge: -1.0,
                late_min_edge: -1.0,
                terminal_min_edge: -1.0,
                min_ev_buffer: -1.0,
                ..Default::default()
            },
            0.0,
        );
        let (zone, fair, edge, decision_trade, skip_reason, skip_detail) = match decision {
            crate::strategy::decision::DecisionResult::Trade(d) => {
                (d.zone, d.fair_value, d.edge, true, None, None)
            }
            crate::strategy::decision::DecisionResult::Skip(s) => {
                (s.zone, 0.0, 0.0, false, Some(s.reason), Some(s.detail))
            }
        };
        let mut row = serde_json::to_value(SignalEvaluation {
            ts_ms: (timestamp_s * 1000.0) as i64,
            cid: runtime.contract.market.condition_id.clone(),
            asset: runtime.contract.asset.clone(),
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
            book_spread: micro.spread,
            book_pressure: micro.pressure,
            book_bid_depth: micro.bid_depth,
            book_ask_depth: micro.ask_depth,
            zone,
            fair,
            edge,
            decision_trade,
            execution_attempted: false,
            traded: false,
            skip_reason,
            skip_detail,
        })
        .expect("serialize signal evaluation");
        row["cat"] = json!("signal");
        row["type"] = json!("evaluation");
        self.rows.push(row);
        self.evaluations += 1;
    }
}

impl Strategy for EvalCacheGenerator {
    fn needs_l2_history(&self) -> bool {
        false
    }

    fn on_event(
        &mut self,
        timestamp_s: f64,
        token_id: &str,
        book: &TokenBook,
        _history: &BTreeMap<String, Vec<(f64, f64)>>,
    ) -> Vec<BacktestOrder> {
        self.books.insert(token_id.to_string(), book.clone());
        let Some(runtime) = self.universe_by_token.get(token_id).cloned() else {
            return Vec::new();
        };
        let minutes_remaining = (runtime.close_ts_s - timestamp_s) / 60.0;
        if minutes_remaining <= 0.083 || minutes_remaining > 30.0 {
            return Vec::new();
        }
        let minutes_elapsed = runtime.window_minutes - minutes_remaining;
        if minutes_elapsed < 0.5 {
            return Vec::new();
        }
        let eval_bucket = (timestamp_s * 10.0).floor() as i64;
        if self
            .last_eval_bucket_by_token
            .get(token_id)
            .copied()
            .is_some_and(|last| last == eval_bucket)
        {
            return Vec::new();
        }
        self.last_eval_bucket_by_token
            .insert(token_id.to_string(), eval_bucket);

        let btc = self.btc_history.price_at_seconds(timestamp_s);
        if btc <= 0.0 {
            return Vec::new();
        }
        if timestamp_s - self.last_tick_ts_s >= 1.0 {
            self.momentum.add_tick(btc, Some(timestamp_s));
            self.last_tick_ts_s = timestamp_s;
        }
        let cid = runtime.contract.market.condition_id.as_str();
        if self.momentum.get_open_price(cid).is_none() {
            let open_btc = self.btc_history.price_at_seconds(runtime.open_ts_s);
            if open_btc <= 0.0 {
                return Vec::new();
            }
            self.momentum.set_window_open(cid, open_btc);
        }
        let Some(signal) = self.momentum.detect(
            cid,
            minutes_elapsed,
            minutes_remaining,
            btc,
            Some(timestamp_s),
        ) else {
            return Vec::new();
        };
        let up_price = self.fresh_ask(
            &runtime.contract.up_token_id,
            timestamp_s,
            runtime.contract.up_price,
        );
        let down_price = self.fresh_ask(
            &runtime.contract.down_token_id,
            timestamp_s,
            runtime.contract.down_price,
        );
        let signal_token = if signal.direction == "up" {
            &runtime.contract.up_token_id
        } else {
            &runtime.contract.down_token_id
        };
        let micro = self.microstructure_for_token(signal_token, timestamp_s);
        let implied_vol = self
            .btc_history
            .realized_vol_at((timestamp_s * 1000.0) as i64, 3600.0);
        self.record_evaluation(
            timestamp_s,
            &runtime,
            &signal,
            up_price,
            down_price,
            implied_vol,
            &micro,
        );
        Vec::new()
    }
}

pub fn write_eval_cache(cfg: EvalCacheConfig) -> Result<EvalCacheSummary> {
    let loader = PMXTv2Loader::new(&cfg.cache_dir);
    let mut generator = EvalCacheGenerator::new(&cfg.universe, Arc::clone(&cfg.btc_history));
    let mut events_loaded = 0_u64;
    for (idx, hour) in cfg.hours.iter().enumerate() {
        let hour_filter = cfg.universe.condition_id_set_for_hour(*hour);
        if hour_filter.is_empty() {
            continue;
        }
        eprintln!(
            "eval-cache: hour {}/{} {} loading {} overlapping condition_id(s)",
            idx + 1,
            cfg.hours.len(),
            hour,
            hour_filter.len(),
        );
        let mut events =
            load_events_for_hour(&loader, *hour, &hour_filter, &cfg.shared_distilled_dir)?;
        events.sort_by(|a, b| {
            a.timestamp_s
                .partial_cmp(&b.timestamp_s)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        events_loaded += events.len() as u64;
        let mut engine = L2BacktestEngine::new(
            FillModel::Perfect(Perfect),
            StaticLatencyConfig { insert_ms: 0 },
        );
        engine.replay(events, &mut generator, 0.0);
    }
    let evaluations = generator.evaluations;
    let mut rows = generator.into_rows();
    let resolutions = append_resolution_rows(&mut rows, &cfg.universe, &cfg.btc_history);
    write_jsonl_atomic(&cfg.output, rows.iter())?;
    Ok(EvalCacheSummary {
        output: cfg.output.display().to_string(),
        hours: cfg.hours.len(),
        contracts: cfg.universe.contracts.len(),
        events_loaded,
        evaluations,
        resolutions,
    })
}

fn load_events_for_hour(
    loader: &PMXTv2Loader,
    hour: DateTime<Utc>,
    hour_filter: &HashSet<String>,
    shared_distilled_dir: &Option<PathBuf>,
) -> Result<Vec<crate::backtest::pmxt::L2Event>> {
    if let Some(shared_dir) = shared_distilled_dir {
        let path = distill::shared_cache_path_for_hour(shared_dir, hour);
        if path.exists() {
            match distill::read_distilled(&path) {
                Ok((mut events, _)) => {
                    events.retain(|e| hour_filter.contains(&e.market_id));
                    return Ok(events);
                }
                Err(e) => {
                    tracing::warn!(error = %e, path = %path.display(), "shared distilled cache unreadable; falling back");
                }
            }
        }
    }
    loader.load_with_sidecar(hour, hour_filter)
}

fn append_resolution_rows(
    rows: &mut Vec<serde_json::Value>,
    universe: &CandleUniverse,
    btc: &BTCHistory,
) -> u64 {
    let mut count = 0_u64;
    for contract in &universe.contracts {
        let runtime = EvalRuntimeContract::from_contract(contract);
        let open_btc = btc.price_at_seconds(runtime.open_ts_s);
        let close_btc = btc.price_at_seconds(runtime.close_ts_s);
        if open_btc <= 0.0 || close_btc <= 0.0 {
            continue;
        }
        let actual = if (close_btc - open_btc).abs() <= f64::EPSILON {
            "tie"
        } else if close_btc > open_btc {
            "up"
        } else {
            "down"
        };
        rows.push(json!({
            "cat": "resolution",
            "type": "resolved",
            "cid": contract.market.condition_id,
            "predicted": actual,
            "actual": actual,
            "won": true,
            "pnl": 0.0,
            "entry_price": 0.0,
            "open_btc": open_btc,
            "close_btc": close_btc,
            "btc_move": close_btc - open_btc,
        }));
        count += 1;
    }
    count
}

fn write_jsonl_atomic<'a>(
    path: &Path,
    rows: impl Iterator<Item = &'a serde_json::Value>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create eval-cache dir {}", parent.display()))?;
    }
    let tmp = path.with_extension(format!(
        "{}.tmp.{}",
        path.extension().and_then(|e| e.to_str()).unwrap_or("jsonl"),
        std::process::id()
    ));
    {
        let mut file =
            std::fs::File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        for row in rows {
            serde_json::to_writer(&mut file, row)
                .with_context(|| format!("write {}", tmp.display()))?;
            file.write_all(b"\n")
                .with_context(|| format!("write newline {}", tmp.display()))?;
        }
        file.flush()
            .with_context(|| format!("flush {}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn book_microstructure(book: &TokenBook) -> BookMicrostructure {
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
