//! Causal, outcome-free opportunity-table export for strategy discovery.
//!
//! The exporter consumes a strict JSONL signal contract and one already-cached
//! PMXT hour. It never downloads data, reads resolutions, or computes PnL.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, Duration, SecondsFormat, Timelike, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::artifact::write_json_artifact_atomic;
use crate::backtest::l2_replay::TokenBook;
use crate::backtest::pmxt::{L2Event, L2EventBody, PMXTv2Loader};
use crate::execution::fees::polymarket_fee;
use crate::strategy::spec::stable_json_hash;

pub const OPPORTUNITY_TABLE_SCHEMA_VERSION: &str = "opportunity_table_v1";
pub const CAUSAL_FEATURE_SEMANTICS_VERSION: &str = "late_window_causal_features_v1";

#[derive(Debug, Clone)]
pub struct OpportunityTableInput {
    pub hour: String,
    pub signals_path: PathBuf,
    pub cache_dir: PathBuf,
    pub output_path: PathBuf,
    pub manifest_path: PathBuf,
    pub stake_usd: f64,
    pub fee_rate: f64,
    pub max_rows: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SignalDirection {
    Up,
    Down,
}

impl SignalDirection {
    fn sign(self) -> f64 {
        match self {
            Self::Up => 1.0,
            Self::Down => -1.0,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CausalSignal {
    pub(crate) condition_id: String,
    pub(crate) token_id: String,
    pub(crate) chronological_window: String,
    pub(crate) window_start: String,
    pub(crate) market_close: String,
    pub(crate) observed_at: String,
    pub(crate) signal_direction: SignalDirection,
    pub(crate) strike_price: f64,
    pub(crate) btc_open: f64,
    pub(crate) btc_60s: Option<f64>,
    pub(crate) btc_120s: Option<f64>,
    pub(crate) btc_180s: Option<f64>,
    pub(crate) btc_240s: Option<f64>,
    pub(crate) btc_observed: f64,
    pub(crate) causal_volatility: f64,
}

#[derive(Debug, Clone)]
struct ValidatedSignal {
    raw: CausalSignal,
    window_start: DateTime<Utc>,
    market_close: DateTime<Utc>,
    observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityRow {
    pub opportunity_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub chronological_window: String,
    pub window_start_ms: i64,
    pub market_close_ms: i64,
    pub observed_at_ms: i64,
    pub signal_direction: String,
    pub strike_price: f64,
    pub btc_open: f64,
    pub btc_60s: Option<f64>,
    pub btc_120s: Option<f64>,
    pub btc_180s: Option<f64>,
    pub btc_240s: Option<f64>,
    pub btc_observed: f64,
    pub causal_volatility: f64,
    pub elapsed_seconds: f64,
    pub remaining_seconds: f64,
    pub move_2m_usd: Option<f64>,
    pub move_2m_aligned: Option<bool>,
    pub path_2m_aligned: Option<bool>,
    pub path_3m_aligned: Option<bool>,
    pub path_4m_aligned: Option<bool>,
    pub signed_distance_to_strike_usd: f64,
    pub directional_distance_to_strike_usd: f64,
    pub book_observable: bool,
    pub book_reason: Option<String>,
    pub book_timestamp_ms: Option<i64>,
    pub book_age_ms: Option<i64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
    pub top_bid_depth_shares: Option<f64>,
    pub top_ask_depth_shares: Option<f64>,
    pub top_book_pressure: Option<f64>,
    pub visible_ask_notional_usd: Option<f64>,
    pub stake_fully_executable: bool,
    pub executable_cost_usd: Option<f64>,
    pub executable_shares: Option<f64>,
    pub average_entry_price: Option<f64>,
    pub taker_fee_usd: Option<f64>,
    pub fee_aware_break_even_probability: Option<f64>,
    pub fee_aware_net_win_usd: Option<f64>,
    pub fee_aware_max_loss_usd: Option<f64>,
    pub loss_recovery_wins: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HashedSource {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityTableManifest {
    pub schema_version: String,
    pub causal_feature_semantics_version: String,
    pub generated_at: String,
    pub hour: String,
    pub signals: HashedSource,
    pub pmxt_parquet: HashedSource,
    pub output: HashedSource,
    pub row_count: usize,
    pub observable_book_rows: usize,
    pub missing_book_rows: usize,
    pub stake_usd: f64,
    pub fee_rate: f64,
    pub outcome_columns_present: bool,
    pub single_pmxt_rowfiltered_scan: bool,
    pub timestamp_semantics: String,
}

pub fn create(input: OpportunityTableInput) -> Result<OpportunityTableManifest> {
    validate_policy(&input)?;
    let hour = parse_hour(&input.hour)?;
    let signals_sha256 = sha256_file(&input.signals_path)?;
    let signals = read_signals(&input.signals_path, hour, input.max_rows)?;
    let condition_ids = signals
        .iter()
        .map(|signal| signal.raw.condition_id.clone())
        .collect::<HashSet<_>>();

    if !input.cache_dir.is_dir() {
        bail!(
            "measurement-only opportunity table requires an existing cache directory at {}",
            input.cache_dir.display()
        );
    }
    let loader = PMXTv2Loader::new(&input.cache_dir);
    let pmxt_path = loader.cache_path_for_hour(hour);
    if input.output_path == pmxt_path || input.manifest_path == pmxt_path {
        bail!("output paths must never replace the source PMXT parquet");
    }
    if !pmxt_path.is_file() {
        bail!(
            "measurement-only opportunity table requires cached PMXT hour at {}",
            pmxt_path.display()
        );
    }
    let pmxt_sha256 = sha256_file(&pmxt_path)?;
    let events = loader
        .load_cached_hour(hour, Some(&condition_ids))
        .context("row-filter cached PMXT hour")?;
    if events.is_empty() {
        bail!(
            "PMXT hour {} contains zero events for the {} target condition IDs; reject this hour as an upstream coverage gap",
            hour.to_rfc3339_opts(SecondsFormat::Secs, true),
            condition_ids.len()
        );
    }
    let rows = build_rows_from_events(
        signals,
        &events,
        &signals_sha256,
        &pmxt_sha256,
        input.stake_usd,
        input.fee_rate,
    );
    write_parquet_atomic(&input.output_path, &rows)?;
    let output_sha256 = sha256_file(&input.output_path)?;
    let observable_book_rows = rows.iter().filter(|row| row.book_observable).count();
    let manifest = OpportunityTableManifest {
        schema_version: OPPORTUNITY_TABLE_SCHEMA_VERSION.to_string(),
        causal_feature_semantics_version: CAUSAL_FEATURE_SEMANTICS_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        hour: hour.to_rfc3339_opts(SecondsFormat::Secs, true),
        signals: HashedSource {
            path: input.signals_path.display().to_string(),
            sha256: signals_sha256,
        },
        pmxt_parquet: HashedSource {
            path: pmxt_path.display().to_string(),
            sha256: pmxt_sha256,
        },
        output: HashedSource {
            path: input.output_path.display().to_string(),
            sha256: output_sha256,
        },
        row_count: rows.len(),
        observable_book_rows,
        missing_book_rows: rows.len() - observable_book_rows,
        stake_usd: input.stake_usd,
        fee_rate: input.fee_rate,
        outcome_columns_present: false,
        single_pmxt_rowfiltered_scan: true,
        timestamp_semantics: "PMXT exchange timestamp; events at or before observed_at only"
            .to_string(),
    };
    write_json_artifact_atomic(&input.manifest_path, &manifest)?;
    Ok(manifest)
}

fn validate_policy(input: &OpportunityTableInput) -> Result<()> {
    if !input.stake_usd.is_finite() || input.stake_usd <= 0.0 {
        bail!("stake_usd must be finite and positive");
    }
    if !input.fee_rate.is_finite() || input.fee_rate < 0.0 {
        bail!("fee_rate must be finite and non-negative");
    }
    if input.max_rows == 0 {
        bail!("max_rows must be positive");
    }
    if input.output_path == input.manifest_path {
        bail!("output and manifest paths must differ");
    }
    if input.output_path == input.signals_path || input.manifest_path == input.signals_path {
        bail!("output paths must never replace the causal signal source");
    }
    if input
        .output_path
        .extension()
        .and_then(|value| value.to_str())
        != Some("parquet")
    {
        bail!("output path must use the .parquet extension");
    }
    Ok(())
}

fn parse_hour(raw: &str) -> Result<DateTime<Utc>> {
    let parsed = DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("parse --hour {raw}"))?
        .with_timezone(&Utc);
    if parsed.minute() != 0 || parsed.second() != 0 || parsed.nanosecond() != 0 {
        bail!("--hour must identify the start of one UTC hour");
    }
    Ok(parsed)
}

fn parse_timestamp(field: &str, raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("parse {field} timestamp {raw}"))
        .map(|value| value.with_timezone(&Utc))
}

fn read_signals(path: &Path, hour: DateTime<Utc>, max_rows: usize) -> Result<Vec<ValidatedSignal>> {
    let file = File::open(path).with_context(|| format!("open signals {}", path.display()))?;
    let mut signals = Vec::new();
    let mut unique = HashSet::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("read signals line {}", index + 1))?;
        if line.trim().is_empty() {
            bail!("signals line {} is blank", index + 1);
        }
        if signals.len() >= max_rows {
            bail!("signals exceed --max-rows {max_rows}");
        }
        let raw: CausalSignal = serde_json::from_str(&line)
            .with_context(|| format!("parse strict causal signal line {}", index + 1))?;
        let signal = validate_signal(raw, hour)
            .with_context(|| format!("validate causal signal line {}", index + 1))?;
        let key = (
            signal.raw.condition_id.clone(),
            signal.raw.token_id.clone(),
            signal.observed_at.timestamp_millis(),
        );
        if !unique.insert(key) {
            bail!(
                "duplicate condition/token/observed_at signal at line {}",
                index + 1
            );
        }
        signals.push(signal);
    }
    if signals.is_empty() {
        bail!("signals file contains no rows");
    }
    signals.sort_by(|left, right| {
        left.observed_at
            .cmp(&right.observed_at)
            .then_with(|| left.raw.condition_id.cmp(&right.raw.condition_id))
            .then_with(|| left.raw.token_id.cmp(&right.raw.token_id))
    });
    Ok(signals)
}

fn validate_signal(raw: CausalSignal, hour: DateTime<Utc>) -> Result<ValidatedSignal> {
    if raw.condition_id.trim().is_empty() || raw.token_id.trim().is_empty() {
        bail!("condition_id and token_id must be non-empty");
    }
    if !matches!(
        raw.chronological_window.as_str(),
        "older" | "recent_discovery" | "fresh_holdout"
    ) {
        bail!("chronological_window is not allowlisted");
    }
    let window_start = parse_timestamp("window_start", &raw.window_start)?;
    let market_close = parse_timestamp("market_close", &raw.market_close)?;
    let observed_at = parse_timestamp("observed_at", &raw.observed_at)?;
    if market_close <= window_start || observed_at < window_start || observed_at > market_close {
        bail!("timestamps must satisfy window_start <= observed_at <= market_close");
    }
    if observed_at < hour || observed_at >= hour + Duration::hours(1) {
        bail!("observed_at is outside the requested UTC hour");
    }
    validate_positive("strike_price", raw.strike_price)?;
    validate_positive("btc_open", raw.btc_open)?;
    validate_positive("btc_observed", raw.btc_observed)?;
    for (name, value) in [
        ("btc_60s", raw.btc_60s),
        ("btc_120s", raw.btc_120s),
        ("btc_180s", raw.btc_180s),
        ("btc_240s", raw.btc_240s),
    ] {
        if let Some(value) = value {
            validate_positive(name, value)?;
        }
    }
    if !raw.causal_volatility.is_finite() || raw.causal_volatility < 0.0 {
        bail!("causal_volatility must be finite and non-negative");
    }
    let elapsed = (observed_at - window_start).num_milliseconds() as f64 / 1_000.0;
    for (offset, value) in [
        (60.0, raw.btc_60s),
        (120.0, raw.btc_120s),
        (180.0, raw.btc_180s),
        (240.0, raw.btc_240s),
    ] {
        if elapsed + 1e-9 < offset && value.is_some() {
            bail!("BTC checkpoint at {offset:.0}s is future information at observed_at");
        }
    }
    Ok(ValidatedSignal {
        raw,
        window_start,
        market_close,
        observed_at,
    })
}

fn validate_positive(name: &str, value: f64) -> Result<()> {
    if !value.is_finite() || value <= 0.0 {
        bail!("{name} must be finite and positive");
    }
    Ok(())
}

fn build_rows_from_events(
    signals: Vec<ValidatedSignal>,
    events: &[L2Event],
    signals_sha256: &str,
    pmxt_sha256: &str,
    stake_usd: f64,
    fee_rate: f64,
) -> Vec<OpportunityRow> {
    let mut event_index = 0usize;
    let mut books = HashMap::<String, TokenBook>::new();
    let mut rows = Vec::with_capacity(signals.len());
    for signal in signals {
        let observed_s = signal.observed_at.timestamp_millis() as f64 / 1_000.0;
        while event_index < events.len() && events[event_index].timestamp_s <= observed_s {
            match &events[event_index].body {
                L2EventBody::BookSnapshot(snapshot) => {
                    books
                        .entry(snapshot.token_id.clone())
                        .or_default()
                        .apply_snapshot(snapshot);
                }
                L2EventBody::PriceChange(change) => {
                    books
                        .entry(change.token_id.clone())
                        .or_default()
                        .apply_change(change);
                }
            }
            event_index += 1;
        }
        let token_id = signal.raw.token_id.clone();
        rows.push(build_row(
            signal,
            books.get(&token_id),
            signals_sha256,
            pmxt_sha256,
            stake_usd,
            fee_rate,
        ));
    }
    rows
}

fn build_row(
    signal: ValidatedSignal,
    book: Option<&TokenBook>,
    signals_sha256: &str,
    pmxt_sha256: &str,
    stake_usd: f64,
    fee_rate: f64,
) -> OpportunityRow {
    let direction = signal.raw.signal_direction;
    let elapsed_seconds =
        (signal.observed_at - signal.window_start).num_milliseconds() as f64 / 1_000.0;
    let remaining_seconds =
        (signal.market_close - signal.observed_at).num_milliseconds() as f64 / 1_000.0;
    let move_2m_usd = signal.raw.btc_120s.map(|value| value - signal.raw.btc_open);
    let move_2m_aligned = move_2m_usd.map(|value| value * direction.sign() > 0.0);
    let path_2m_aligned = path_alignment(
        [
            Some(signal.raw.btc_open),
            signal.raw.btc_60s,
            signal.raw.btc_120s,
        ],
        direction,
    );
    let path_3m_aligned = path_alignment(
        [
            Some(signal.raw.btc_open),
            signal.raw.btc_60s,
            signal.raw.btc_120s,
            signal.raw.btc_180s,
        ],
        direction,
    );
    let path_4m_aligned = path_alignment(
        [
            Some(signal.raw.btc_open),
            signal.raw.btc_60s,
            signal.raw.btc_120s,
            signal.raw.btc_180s,
            signal.raw.btc_240s,
        ],
        direction,
    );
    let signed_distance = signal.raw.btc_observed - signal.raw.strike_price;
    let opportunity_id = stable_json_hash(&serde_json::json!({
        "schema_version": OPPORTUNITY_TABLE_SCHEMA_VERSION,
        "condition_id": &signal.raw.condition_id,
        "token_id": &signal.raw.token_id,
        "observed_at_ms": signal.observed_at.timestamp_millis(),
        "signals_sha256": signals_sha256,
        "pmxt_sha256": pmxt_sha256,
    }));
    let book_metrics = measure_book(book, signal.observed_at, stake_usd, fee_rate);

    OpportunityRow {
        opportunity_id,
        condition_id: signal.raw.condition_id,
        token_id: signal.raw.token_id,
        chronological_window: signal.raw.chronological_window,
        window_start_ms: signal.window_start.timestamp_millis(),
        market_close_ms: signal.market_close.timestamp_millis(),
        observed_at_ms: signal.observed_at.timestamp_millis(),
        signal_direction: direction.as_str().to_string(),
        strike_price: signal.raw.strike_price,
        btc_open: signal.raw.btc_open,
        btc_60s: signal.raw.btc_60s,
        btc_120s: signal.raw.btc_120s,
        btc_180s: signal.raw.btc_180s,
        btc_240s: signal.raw.btc_240s,
        btc_observed: signal.raw.btc_observed,
        causal_volatility: signal.raw.causal_volatility,
        elapsed_seconds,
        remaining_seconds,
        move_2m_usd,
        move_2m_aligned,
        path_2m_aligned,
        path_3m_aligned,
        path_4m_aligned,
        signed_distance_to_strike_usd: signed_distance,
        directional_distance_to_strike_usd: signed_distance * direction.sign(),
        book_observable: book_metrics.observable,
        book_reason: book_metrics.reason,
        book_timestamp_ms: book_metrics.timestamp_ms,
        book_age_ms: book_metrics.age_ms,
        best_bid: book_metrics.best_bid,
        best_ask: book_metrics.best_ask,
        spread: book_metrics.spread,
        top_bid_depth_shares: book_metrics.top_bid_depth,
        top_ask_depth_shares: book_metrics.top_ask_depth,
        top_book_pressure: book_metrics.top_book_pressure,
        visible_ask_notional_usd: book_metrics.visible_ask_notional,
        stake_fully_executable: book_metrics.stake_fully_executable,
        executable_cost_usd: book_metrics.executable_cost,
        executable_shares: book_metrics.executable_shares,
        average_entry_price: book_metrics.average_entry_price,
        taker_fee_usd: book_metrics.taker_fee,
        fee_aware_break_even_probability: book_metrics.break_even_probability,
        fee_aware_net_win_usd: book_metrics.net_win,
        fee_aware_max_loss_usd: book_metrics.max_loss,
        loss_recovery_wins: book_metrics.loss_recovery_wins,
    }
}

fn path_alignment<const N: usize>(
    points: [Option<f64>; N],
    direction: SignalDirection,
) -> Option<bool> {
    let points = points.into_iter().collect::<Option<Vec<_>>>()?;
    let all_up = points.windows(2).all(|pair| pair[1] > pair[0]);
    let all_down = points.windows(2).all(|pair| pair[1] < pair[0]);
    Some(match direction {
        SignalDirection::Up => all_up,
        SignalDirection::Down => all_down,
    })
}

#[derive(Debug, Default)]
struct BookMetrics {
    observable: bool,
    reason: Option<String>,
    timestamp_ms: Option<i64>,
    age_ms: Option<i64>,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    spread: Option<f64>,
    top_bid_depth: Option<f64>,
    top_ask_depth: Option<f64>,
    top_book_pressure: Option<f64>,
    visible_ask_notional: Option<f64>,
    stake_fully_executable: bool,
    executable_cost: Option<f64>,
    executable_shares: Option<f64>,
    average_entry_price: Option<f64>,
    taker_fee: Option<f64>,
    break_even_probability: Option<f64>,
    net_win: Option<f64>,
    max_loss: Option<f64>,
    loss_recovery_wins: Option<f64>,
}

fn measure_book(
    book: Option<&TokenBook>,
    observed_at: DateTime<Utc>,
    stake_usd: f64,
    fee_rate: f64,
) -> BookMetrics {
    let Some(book) = book else {
        return BookMetrics {
            reason: Some("no_book_before_observed_at".to_string()),
            ..Default::default()
        };
    };
    let asks = book.ask_levels();
    let bids = book.bid_levels();
    if !(book.best_bid > 0.0
        && book.best_bid < 1.0
        && book.best_ask > 0.0
        && book.best_ask < 1.0
        && book.best_bid <= book.best_ask)
    {
        return BookMetrics {
            reason: Some("invalid_top_of_book".to_string()),
            ..Default::default()
        };
    }
    if asks.is_empty() {
        return BookMetrics {
            reason: Some("no_visible_ask_depth".to_string()),
            ..Default::default()
        };
    }
    let visible_ask_notional = asks.iter().map(|(price, size)| price * size).sum::<f64>();
    let top_bid_depth = bids.first().map(|(_, size)| *size);
    let top_ask_depth = asks.first().map(|(_, size)| *size);
    let top_book_pressure = match (top_bid_depth, top_ask_depth) {
        (Some(bid), Some(ask)) if bid + ask > 0.0 => Some((bid - ask) / (bid + ask)),
        _ => None,
    };
    let mut remaining = stake_usd;
    let mut cost = 0.0;
    let mut shares = 0.0;
    let mut fee = 0.0;
    for (price, size) in &asks {
        if remaining <= 1e-9 {
            break;
        }
        let fill_cost = remaining.min(price * size);
        let fill_shares = fill_cost / price;
        cost += fill_cost;
        shares += fill_shares;
        fee += polymarket_fee(fill_shares, *price, fee_rate);
        remaining -= fill_cost;
    }
    let average_entry_price = (shares > 0.0).then_some(cost / shares);
    let break_even_probability = (shares > 0.0).then_some((cost + fee) / shares);
    let net_win = (shares > 0.0).then_some(shares - cost - fee);
    let max_loss = (shares > 0.0).then_some(cost + fee);
    let loss_recovery_wins = match (net_win, max_loss) {
        (Some(win), Some(loss)) if win > 0.0 => Some(loss / win),
        _ => None,
    };
    let timestamp_ms = (book.last_update_ts_s * 1_000.0).round() as i64;
    BookMetrics {
        observable: true,
        reason: None,
        timestamp_ms: Some(timestamp_ms),
        age_ms: Some(observed_at.timestamp_millis().saturating_sub(timestamp_ms)),
        best_bid: Some(book.best_bid),
        best_ask: Some(book.best_ask),
        spread: Some(book.best_ask - book.best_bid),
        top_bid_depth,
        top_ask_depth,
        top_book_pressure,
        visible_ask_notional: Some(visible_ask_notional),
        stake_fully_executable: remaining <= 1e-9,
        executable_cost: (shares > 0.0).then_some(cost),
        executable_shares: (shares > 0.0).then_some(shares),
        average_entry_price,
        taker_fee: (shares > 0.0).then_some(fee),
        break_even_probability,
        net_win,
        max_loss,
        loss_recovery_wins,
    }
}

fn write_parquet_atomic(path: &Path, rows: &[OpportunityRow]) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("opportunities.parquet");
    let temporary = path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
    let result = (|| -> Result<()> {
        let schema = opportunity_schema();
        let batch = opportunity_batch(schema.clone(), rows)?;
        let file =
            File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        std::fs::rename(&temporary, path)
            .with_context(|| format!("rename {} to {}", temporary.display(), path.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn opportunity_schema() -> Arc<Schema> {
    let fields = vec![
        Field::new("opportunity_id", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, false),
        Field::new("token_id", DataType::Utf8, false),
        Field::new("chronological_window", DataType::Utf8, false),
        Field::new("window_start_ms", DataType::Int64, false),
        Field::new("market_close_ms", DataType::Int64, false),
        Field::new("observed_at_ms", DataType::Int64, false),
        Field::new("signal_direction", DataType::Utf8, false),
        Field::new("strike_price", DataType::Float64, false),
        Field::new("btc_open", DataType::Float64, false),
        Field::new("btc_60s", DataType::Float64, true),
        Field::new("btc_120s", DataType::Float64, true),
        Field::new("btc_180s", DataType::Float64, true),
        Field::new("btc_240s", DataType::Float64, true),
        Field::new("btc_observed", DataType::Float64, false),
        Field::new("causal_volatility", DataType::Float64, false),
        Field::new("elapsed_seconds", DataType::Float64, false),
        Field::new("remaining_seconds", DataType::Float64, false),
        Field::new("move_2m_usd", DataType::Float64, true),
        Field::new("move_2m_aligned", DataType::Boolean, true),
        Field::new("path_2m_aligned", DataType::Boolean, true),
        Field::new("path_3m_aligned", DataType::Boolean, true),
        Field::new("path_4m_aligned", DataType::Boolean, true),
        Field::new("signed_distance_to_strike_usd", DataType::Float64, false),
        Field::new(
            "directional_distance_to_strike_usd",
            DataType::Float64,
            false,
        ),
        Field::new("book_observable", DataType::Boolean, false),
        Field::new("book_reason", DataType::Utf8, true),
        Field::new("book_timestamp_ms", DataType::Int64, true),
        Field::new("book_age_ms", DataType::Int64, true),
        Field::new("best_bid", DataType::Float64, true),
        Field::new("best_ask", DataType::Float64, true),
        Field::new("spread", DataType::Float64, true),
        Field::new("top_bid_depth_shares", DataType::Float64, true),
        Field::new("top_ask_depth_shares", DataType::Float64, true),
        Field::new("top_book_pressure", DataType::Float64, true),
        Field::new("visible_ask_notional_usd", DataType::Float64, true),
        Field::new("stake_fully_executable", DataType::Boolean, false),
        Field::new("executable_cost_usd", DataType::Float64, true),
        Field::new("executable_shares", DataType::Float64, true),
        Field::new("average_entry_price", DataType::Float64, true),
        Field::new("taker_fee_usd", DataType::Float64, true),
        Field::new("fee_aware_break_even_probability", DataType::Float64, true),
        Field::new("fee_aware_net_win_usd", DataType::Float64, true),
        Field::new("fee_aware_max_loss_usd", DataType::Float64, true),
        Field::new("loss_recovery_wins", DataType::Float64, true),
    ];
    Arc::new(Schema::new(fields))
}

fn opportunity_batch(schema: Arc<Schema>, rows: &[OpportunityRow]) -> Result<RecordBatch> {
    let strings = |value: fn(&OpportunityRow) -> &str| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(rows.iter().map(value)))
    };
    let optional_strings = |value: fn(&OpportunityRow) -> Option<&str>| -> ArrayRef {
        Arc::new(StringArray::from(
            rows.iter().map(value).collect::<Vec<_>>(),
        ))
    };
    let i64s = |value: fn(&OpportunityRow) -> i64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(rows.iter().map(value)))
    };
    let optional_i64s = |value: fn(&OpportunityRow) -> Option<i64>| -> ArrayRef {
        Arc::new(Int64Array::from(rows.iter().map(value).collect::<Vec<_>>()))
    };
    let floats = |value: fn(&OpportunityRow) -> f64| -> ArrayRef {
        Arc::new(Float64Array::from_iter_values(rows.iter().map(value)))
    };
    let optional_floats = |value: fn(&OpportunityRow) -> Option<f64>| -> ArrayRef {
        Arc::new(Float64Array::from(
            rows.iter().map(value).collect::<Vec<_>>(),
        ))
    };
    let bools = |value: fn(&OpportunityRow) -> bool| -> ArrayRef {
        Arc::new(BooleanArray::from(
            rows.iter().map(value).collect::<Vec<_>>(),
        ))
    };
    let optional_bools = |value: fn(&OpportunityRow) -> Option<bool>| -> ArrayRef {
        Arc::new(BooleanArray::from(
            rows.iter().map(value).collect::<Vec<_>>(),
        ))
    };
    RecordBatch::try_new(
        schema,
        vec![
            strings(|r| &r.opportunity_id),
            strings(|r| &r.condition_id),
            strings(|r| &r.token_id),
            strings(|r| &r.chronological_window),
            i64s(|r| r.window_start_ms),
            i64s(|r| r.market_close_ms),
            i64s(|r| r.observed_at_ms),
            strings(|r| &r.signal_direction),
            floats(|r| r.strike_price),
            floats(|r| r.btc_open),
            optional_floats(|r| r.btc_60s),
            optional_floats(|r| r.btc_120s),
            optional_floats(|r| r.btc_180s),
            optional_floats(|r| r.btc_240s),
            floats(|r| r.btc_observed),
            floats(|r| r.causal_volatility),
            floats(|r| r.elapsed_seconds),
            floats(|r| r.remaining_seconds),
            optional_floats(|r| r.move_2m_usd),
            optional_bools(|r| r.move_2m_aligned),
            optional_bools(|r| r.path_2m_aligned),
            optional_bools(|r| r.path_3m_aligned),
            optional_bools(|r| r.path_4m_aligned),
            floats(|r| r.signed_distance_to_strike_usd),
            floats(|r| r.directional_distance_to_strike_usd),
            bools(|r| r.book_observable),
            optional_strings(|r| r.book_reason.as_deref()),
            optional_i64s(|r| r.book_timestamp_ms),
            optional_i64s(|r| r.book_age_ms),
            optional_floats(|r| r.best_bid),
            optional_floats(|r| r.best_ask),
            optional_floats(|r| r.spread),
            optional_floats(|r| r.top_bid_depth_shares),
            optional_floats(|r| r.top_ask_depth_shares),
            optional_floats(|r| r.top_book_pressure),
            optional_floats(|r| r.visible_ask_notional_usd),
            bools(|r| r.stake_fully_executable),
            optional_floats(|r| r.executable_cost_usd),
            optional_floats(|r| r.executable_shares),
            optional_floats(|r| r.average_entry_price),
            optional_floats(|r| r.taker_fee_usd),
            optional_floats(|r| r.fee_aware_break_even_probability),
            optional_floats(|r| r.fee_aware_net_win_usd),
            optional_floats(|r| r.fee_aware_max_loss_usd),
            optional_floats(|r| r.loss_recovery_wins),
        ],
    )
    .context("build opportunity-table record batch")
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("open {} for hashing", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::pmxt::{BookSnapshot, L2Level, PriceChange};
    use crate::strategy::momentum::{MomentumConfig, MomentumDetector};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn signal(observed_at: &str) -> CausalSignal {
        CausalSignal {
            condition_id: "condition-a".to_string(),
            token_id: "token-up".to_string(),
            chronological_window: "recent_discovery".to_string(),
            window_start: "2026-08-01T12:00:00Z".to_string(),
            market_close: "2026-08-01T12:05:00Z".to_string(),
            observed_at: observed_at.to_string(),
            signal_direction: SignalDirection::Up,
            strike_price: 100_050.0,
            btc_open: 100_000.0,
            btc_60s: Some(100_050.0),
            btc_120s: Some(100_150.0),
            btc_180s: Some(100_250.0),
            btc_240s: None,
            btc_observed: 100_250.0,
            causal_volatility: 0.001,
        }
    }

    fn snapshot(timestamp_s: f64) -> L2Event {
        L2Event {
            timestamp_s,
            market_id: "condition-a".to_string(),
            body: L2EventBody::BookSnapshot(BookSnapshot {
                market_id: "condition-a".to_string(),
                token_id: "token-up".to_string(),
                best_bid: 0.54,
                best_ask: 0.56,
                timestamp_s,
                bids: vec![L2Level {
                    price: 0.54,
                    size: 20.0,
                }],
                asks: vec![
                    L2Level {
                        price: 0.56,
                        size: 5.0,
                    },
                    L2Level {
                        price: 0.57,
                        size: 10.0,
                    },
                ],
            }),
        }
    }

    #[test]
    fn strict_signal_contract_rejects_outcomes_and_future_checkpoints() {
        let hour = parse_hour("2026-08-01T12:00:00Z").unwrap();
        let with_outcome = serde_json::json!({
            "condition_id":"c", "token_id":"t",
            "chronological_window":"recent_discovery",
            "window_start":"2026-08-01T12:00:00Z",
            "market_close":"2026-08-01T12:05:00Z",
            "observed_at":"2026-08-01T12:01:00Z",
            "signal_direction":"up", "strike_price":1.0,
            "btc_open":1.0, "btc_60s":1.1, "btc_120s":null,
            "btc_180s":null, "btc_240s":null, "btc_observed":1.1,
            "causal_volatility":0.1, "outcome":"UP"
        });
        assert!(serde_json::from_value::<CausalSignal>(with_outcome).is_err());

        let mut future = signal("2026-08-01T12:01:00Z");
        future.btc_120s = Some(100_150.0);
        assert!(validate_signal(future, hour)
            .unwrap_err()
            .to_string()
            .contains("future information"));
    }

    #[test]
    fn output_policy_never_allows_signal_source_replacement() {
        let input = OpportunityTableInput {
            hour: "2026-08-01T12:00:00Z".to_string(),
            signals_path: PathBuf::from("signals.jsonl"),
            cache_dir: PathBuf::from("cache"),
            output_path: PathBuf::from("signals.jsonl"),
            manifest_path: PathBuf::from("manifest.json"),
            stake_usd: 5.0,
            fee_rate: 0.07,
            max_rows: 100,
        };
        assert!(validate_policy(&input)
            .unwrap_err()
            .to_string()
            .contains("never replace"));
    }

    #[test]
    fn causal_row_matches_runtime_features_token_book_and_fee_math() {
        let hour = parse_hour("2026-08-01T12:00:00Z").unwrap();
        let validated = validate_signal(signal("2026-08-01T12:03:00Z"), hour).unwrap();
        let start = validated.window_start.timestamp() as f64;
        let mut detector = MomentumDetector::new(Some(0.5), MomentumConfig::default());
        detector.add_tick(100_000.0, Some(start));
        detector.add_tick(100_050.0, Some(start + 60.0));
        detector.add_tick(100_150.0, Some(start + 120.0));
        detector.add_tick(100_250.0, Some(start + 180.0));
        detector.set_window_open("condition-a", 100_000.0);
        let runtime_signal = detector
            .detect("condition-a", 3.0, 2.0, 100_250.0, Some(start + 180.0))
            .unwrap();
        let events = vec![
            snapshot(start + 170.0),
            L2Event {
                timestamp_s: start + 181.0,
                market_id: "condition-a".to_string(),
                body: L2EventBody::PriceChange(PriceChange {
                    market_id: "condition-a".to_string(),
                    token_id: "token-up".to_string(),
                    side: "SELL".to_string(),
                    best_bid: 0.54,
                    best_ask: 0.55,
                    timestamp_s: start + 181.0,
                    change_price: 0.55,
                    change_size: 100.0,
                    change_side: "SELL".to_string(),
                }),
            },
        ];
        let rows = build_rows_from_events(vec![validated], &events, "signals", "pmxt", 5.0, 0.07);
        let row = &rows[0];
        assert!(row.book_observable);
        assert_eq!(row.best_ask, Some(0.56));
        assert_eq!(row.top_ask_depth_shares, Some(5.0));
        assert_eq!(row.top_book_pressure, Some(0.6));
        assert_eq!(row.path_3m_aligned, Some(true));
        assert_eq!(row.path_2m_aligned, Some(true));
        assert_eq!(row.move_2m_aligned, Some(true));
        assert_eq!(
            row.path_3m_aligned,
            runtime_signal
                .article_path_3m
                .as_deref()
                .map(|path| path == runtime_signal.direction.as_str())
        );
        assert_eq!(row.move_2m_usd, runtime_signal.article_move_2m_usd);
        assert!(row.stake_fully_executable);
        let expected_shares = 5.0 + 2.2 / 0.57;
        let expected_fee = polymarket_fee(5.0, 0.56, 0.07) + polymarket_fee(2.2 / 0.57, 0.57, 0.07);
        assert!((row.executable_shares.unwrap() - expected_shares).abs() < 1e-9);
        assert!((row.taker_fee_usd.unwrap() - expected_fee).abs() < 1e-9);
        assert_eq!(row.book_age_ms, Some(10_000));
    }

    #[test]
    fn parquet_bytes_and_opportunity_ids_are_stable() {
        let hour = parse_hour("2026-08-01T12:00:00Z").unwrap();
        let validated = validate_signal(signal("2026-08-01T12:03:00Z"), hour).unwrap();
        let start = validated.window_start.timestamp() as f64;
        let rows = build_rows_from_events(
            vec![validated],
            &[snapshot(start + 170.0)],
            "signals",
            "pmxt",
            5.0,
            0.07,
        );
        let temp = tempfile::TempDir::new().unwrap();
        let first = temp.path().join("first.parquet");
        let second = temp.path().join("second.parquet");
        write_parquet_atomic(&first, &rows).unwrap();
        write_parquet_atomic(&second, &rows).unwrap();
        assert_eq!(sha256_file(&first).unwrap(), sha256_file(&second).unwrap());
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(first).unwrap())
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
            1
        );
        assert_eq!(rows[0].opportunity_id.len(), 64);
    }
}
