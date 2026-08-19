//! Historical BTC price replay with causality-guaranteed lookup.
//!
//! Two ingestion paths:
//!   - `load_csv` — read a Binance kline CSV (timestamp,open,high,low,close,volume)
//!     or a collector tick CSV (timestamp_ms,price,...). For klines we shift
//!     the stored timestamp to `open_time + interval` so a query at T cannot
//!     return a close price that wasn't yet observable at T.
//!   - `load_from_binance` — pull kline windows from the Binance public REST.
//!
//! Lookup is `O(log n)` via binary search over the sorted timestamp vector.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;

#[derive(Default, Clone)]
pub struct BTCHistory {
    pub(crate) timestamps_ms: Vec<i64>, // sorted ascending
    pub(crate) prices: Vec<f64>,
    source_kind: String,
}

impl BTCHistory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn n_ticks(&self) -> usize {
        self.timestamps_ms.len()
    }

    pub fn source_kind(&self) -> &str {
        if self.source_kind.is_empty() {
            "unknown"
        } else {
            &self.source_kind
        }
    }

    pub fn first_timestamp_ms(&self) -> i64 {
        self.timestamps_ms.first().copied().unwrap_or(0)
    }

    pub fn last_timestamp_ms(&self) -> i64 {
        self.timestamps_ms.last().copied().unwrap_or(0)
    }

    pub fn median_interval_ms(&self, start_ms: i64, end_ms: i64) -> Option<i64> {
        let mut intervals = self.intervals_in_range(start_ms, end_ms);
        if intervals.is_empty() {
            return None;
        }
        intervals.sort_unstable();
        Some(intervals[intervals.len() / 2])
    }

    pub fn max_gap_ms(&self, start_ms: i64, end_ms: i64) -> Option<i64> {
        self.intervals_in_range(start_ms, end_ms).into_iter().max()
    }

    fn intervals_in_range(&self, start_ms: i64, end_ms: i64) -> Vec<i64> {
        if self.timestamps_ms.len() < 2 || end_ms < start_ms {
            return Vec::new();
        }
        let mut lo = self.timestamps_ms.partition_point(|&ts| ts < start_ms);
        lo = lo.saturating_sub(1);
        let hi = self.timestamps_ms.partition_point(|&ts| ts <= end_ms);
        self.timestamps_ms[lo..hi]
            .windows(2)
            .filter_map(|pair| {
                let gap = pair[1] - pair[0];
                (gap > 0).then_some(gap)
            })
            .collect()
    }

    /// Load a CSV. Auto-detects schema:
    ///   - Binance kline:    `timestamp,open,high,low,close,volume` (kline open_time)
    ///   - Collector ticks:  `timestamp_ms,...,price,...` (observation time)
    ///
    /// Klines are stored at `open_time + interval` so a `price_at(T)` query can
    /// only ever return a close that was observable at T.
    pub fn load_csv(&mut self, path: impl AsRef<Path>) -> Result<usize> {
        let path = path.as_ref();
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(path)
            .with_context(|| format!("open csv {}", path.display()))?;
        let headers: Vec<String> = reader.headers()?.iter().map(|s| s.to_string()).collect();
        if headers.is_empty() {
            return Ok(0);
        }

        let lower: Vec<String> = headers.iter().map(|h| h.to_lowercase()).collect();
        let source_idx = lower.iter().position(|header| header == "source");
        let (ts_idx, price_idx, is_kline) =
            if lower.contains(&"timestamp".to_string()) && lower.contains(&"close".to_string()) {
                (
                    lower.iter().position(|h| h == "timestamp").unwrap(),
                    lower.iter().position(|h| h == "close").unwrap(),
                    true,
                )
            } else if lower.contains(&"timestamp_ms".to_string())
                && lower.contains(&"price".to_string())
            {
                (
                    lower.iter().position(|h| h == "timestamp_ms").unwrap(),
                    lower.iter().position(|h| h == "price").unwrap(),
                    false,
                )
            } else if lower.contains(&"timestamp".to_string()) && lower.len() >= 6 {
                (
                    lower.iter().position(|h| h == "timestamp").unwrap(),
                    4,
                    true,
                )
            } else {
                anyhow::bail!("unknown CSV schema in {}: {:?}", path.display(), headers);
            };

        let mut raw: Vec<(i64, f64)> = Vec::new();
        let mut source_values = BTreeSet::new();
        for rec in reader.records() {
            let rec = match rec {
                Ok(r) => r,
                Err(_) => continue,
            };
            if rec.len() <= ts_idx.max(price_idx) {
                continue;
            }
            let ts = match rec.get(ts_idx).and_then(|s| s.parse::<f64>().ok()) {
                Some(v) => v as i64,
                None => continue,
            };
            let price = match rec.get(price_idx).and_then(|s| s.parse::<f64>().ok()) {
                Some(v) if v > 0.0 && v.is_finite() => v,
                _ => continue,
            };
            if let Some(source) = source_idx.and_then(|index| rec.get(index)) {
                let source = source.trim().to_ascii_lowercase();
                if !source.is_empty() {
                    source_values.insert(source);
                }
            }
            raw.push((ts, price));
        }
        if raw.is_empty() {
            return Ok(0);
        }
        raw.sort_by_key(|r| r.0);

        let added = raw.len();
        let loaded_source_kind = if is_kline {
            "binance_btcusdt_klines"
        } else {
            classify_btc_source_values(&source_values)
        };
        self.merge_source_kind(loaded_source_kind);
        if is_kline {
            // Detect interval as the smallest positive gap; treat that as the
            // bar width. Storing at open_time + interval guarantees no
            // lookahead.
            let mut interval_ms = 1000_i64;
            let mut min_diff = i64::MAX;
            for w in raw.windows(2) {
                let diff = w[1].0 - w[0].0;
                if diff > 0 && diff < min_diff {
                    min_diff = diff;
                }
            }
            if min_diff != i64::MAX {
                interval_ms = min_diff;
            }
            for (ts, p) in raw {
                self.timestamps_ms.push(ts + interval_ms);
                self.prices.push(p);
            }
        } else {
            for (ts, p) in raw {
                self.timestamps_ms.push(ts);
                self.prices.push(p);
            }
        }
        self.dedupe_and_sort();
        Ok(added)
    }

    /// Pull klines from Binance public REST. Stores at `close_time` so the
    /// causality contract holds.
    pub async fn load_from_binance(
        &mut self,
        start_ms: i64,
        end_ms: i64,
        symbol: &str,
        interval: &str,
    ) -> Result<usize> {
        let client = Client::builder().timeout(Duration::from_secs(15)).build()?;
        let mut cursor = start_ms;
        let mut added = 0usize;
        while cursor < end_ms {
            let resp = client
                .get("https://api.binance.com/api/v3/klines")
                .query(&[
                    ("symbol", symbol),
                    ("interval", interval),
                    ("startTime", &cursor.to_string()),
                    ("endTime", &end_ms.to_string()),
                    ("limit", "1000"),
                ])
                .send()
                .await;
            let resp = match resp {
                Ok(r) if r.status().is_success() => r,
                Ok(r) => {
                    tracing::warn!(status = %r.status(), "Binance kline non-2xx");
                    break;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Binance kline error");
                    break;
                }
            };
            let klines: Value = resp.json().await?;
            let arr = match klines.as_array() {
                Some(a) if !a.is_empty() => a,
                _ => break,
            };
            let mut last_open = cursor;
            for k in arr {
                let kk = match k.as_array() {
                    Some(a) if a.len() >= 7 => a,
                    _ => continue,
                };
                let close_time = kk[6].as_i64().unwrap_or(0);
                let close = kk[4]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| kk[4].as_f64())
                    .unwrap_or(0.0);
                if close <= 0.0 || close_time <= 0 {
                    continue;
                }
                self.timestamps_ms.push(close_time);
                self.prices.push(close);
                added += 1;
                last_open = kk[0].as_i64().unwrap_or(close_time);
            }
            if last_open <= cursor {
                break;
            }
            cursor = last_open + 1;
            if arr.len() < 1000 {
                break;
            }
        }
        if added > 0 {
            self.merge_source_kind("binance_btcusdt_klines");
            self.dedupe_and_sort();
        }
        Ok(added)
    }

    fn dedupe_and_sort(&mut self) {
        if self.timestamps_ms.len() != self.prices.len() {
            return;
        }
        let mut paired: Vec<(i64, f64)> = self
            .timestamps_ms
            .drain(..)
            .zip(self.prices.drain(..))
            .collect();
        paired.sort_by_key(|p| p.0);
        let mut seen: BTreeMap<i64, f64> = BTreeMap::new();
        for (ts, p) in paired {
            seen.entry(ts).or_insert(p);
        }
        self.timestamps_ms.clear();
        self.prices.clear();
        for (ts, p) in seen {
            self.timestamps_ms.push(ts);
            self.prices.push(p);
        }
    }

    fn merge_source_kind(&mut self, loaded: &str) {
        if self.source_kind.is_empty() || self.source_kind == "unknown" {
            self.source_kind = loaded.to_string();
        } else if self.source_kind != loaded {
            self.source_kind = "multi_source_proxy".to_string();
        }
    }

    /// Most recent observable price at time T. Returns 0 if no data is
    /// available at or before T.
    pub fn price_at(&self, timestamp_ms: i64) -> f64 {
        if self.timestamps_ms.is_empty() {
            return 0.0;
        }
        let idx = match self.timestamps_ms.binary_search(&timestamp_ms) {
            Ok(i) => i,
            Err(0) => return 0.0,
            Err(i) => i - 1,
        };
        self.prices[idx]
    }

    pub fn price_at_seconds(&self, timestamp_s: f64) -> f64 {
        self.price_at((timestamp_s * 1000.0) as i64)
    }

    /// Most recent observable price at or before `timestamp_ms`, provided it
    /// is no older than `max_age_ms`. This is the fail-closed lookup used by
    /// source-specific fair-value anchors; it never reads a future tick.
    pub fn price_at_with_max_age(&self, timestamp_ms: i64, max_age_ms: i64) -> Option<f64> {
        self.price_and_age_at_with_max_age(timestamp_ms, max_age_ms)
            .map(|(price, _)| price)
    }

    /// Price plus causal observation age at `timestamp_ms`. This exposes the
    /// exact freshness used by source-coverage evidence without allowing a
    /// future tick or a separate lookup rule.
    pub fn price_and_age_at_with_max_age(
        &self,
        timestamp_ms: i64,
        max_age_ms: i64,
    ) -> Option<(f64, i64)> {
        if self.timestamps_ms.is_empty() || max_age_ms < 0 {
            return None;
        }
        let idx = match self.timestamps_ms.binary_search(&timestamp_ms) {
            Ok(i) => i,
            Err(0) => return None,
            Err(i) => i - 1,
        };
        let age_ms = timestamp_ms.saturating_sub(self.timestamps_ms[idx]);
        (age_ms <= max_age_ms).then_some((self.prices[idx], age_ms))
    }

    /// Time-weighted average price over `[start_ms, end_ms]`, the resolution
    /// quantity of post-2026-08-08 Polymarket candle markets ("TWAP of the
    /// time range vs the price at the beginning of that range").
    ///
    /// Each observed tick's price is weighted by the time it remained the
    /// latest observation inside the window (step-function integral). The
    /// price in force at `start_ms` comes from the causal `price_at` lookup
    /// (latest tick at-or-before start). Fail-closed: returns `None` when the
    /// window is empty/inverted, when no price exists at the window start, or
    /// when the tape ends before `end_ms` (a partial tape must not
    /// silently produce a full-window TWAP).
    pub fn twap_between(&self, start_ms: i64, end_ms: i64) -> Option<f64> {
        if end_ms <= start_ms || self.timestamps_ms.is_empty() {
            return None;
        }
        if *self.timestamps_ms.last().unwrap() < end_ms {
            return None;
        }
        self.partial_twap(start_ms, end_ms, end_ms)
    }

    /// Causal partial TWAP: the time-weighted average of `[start_ms, as_of_ms]`
    /// for a window ending at `end_ms`, using only ticks observable at
    /// `as_of_ms`. This is the strategy-side feature: at `as_of_ms` the final
    /// window TWAP is already `(as_of - start) / (end - start)` determined by
    /// this value. Returns `None` if no price exists at the window start or
    /// `as_of_ms <= start_ms`.
    pub fn partial_twap(&self, start_ms: i64, as_of_ms: i64, end_ms: i64) -> Option<f64> {
        if as_of_ms <= start_ms || end_ms < as_of_ms {
            return None;
        }
        // Price in force at window start (causal: latest tick <= start).
        let start_idx = match self.timestamps_ms.binary_search(&start_ms) {
            Ok(i) => i,
            Err(0) => return None,
            Err(i) => i - 1,
        };
        let mut integral = 0.0_f64;
        let mut cur_price = self.prices[start_idx];
        let mut cur_ts = start_ms;
        for i in (start_idx + 1)..self.timestamps_ms.len() {
            let ts = self.timestamps_ms[i];
            if ts >= as_of_ms {
                break;
            }
            if ts <= start_ms {
                // Duplicate-at-start ticks: latest one wins, no elapsed time.
                cur_price = self.prices[i];
                continue;
            }
            integral += cur_price * (ts - cur_ts) as f64;
            cur_price = self.prices[i];
            cur_ts = ts;
        }
        integral += cur_price * (as_of_ms - cur_ts) as f64;
        Some(integral / (as_of_ms - start_ms) as f64)
    }

    /// (open, high, low, close) over `[start_ms, end_ms]`. Returns zeros if
    /// the window has no ticks.
    #[cfg(test)]
    pub fn range_at(&self, start_ms: i64, end_ms: i64) -> (f64, f64, f64, f64) {
        let lo = self.timestamps_ms.partition_point(|&t| t < start_ms);
        let hi = self.timestamps_ms.partition_point(|&t| t <= end_ms);
        if lo >= hi {
            return (0.0, 0.0, 0.0, 0.0);
        }
        let prices = &self.prices[lo..hi];
        let mut high = prices[0];
        let mut low = prices[0];
        for &p in prices {
            if p > high {
                high = p;
            }
            if p < low {
                low = p;
            }
        }
        (prices[0], high, low, *prices.last().unwrap())
    }

    /// Annualized realized volatility from log returns in the lookback window.
    /// Returns 0.50 if there's insufficient data, mirroring the Python default.
    pub fn realized_vol_at(&self, timestamp_ms: i64, lookback_seconds: f64) -> f64 {
        if self.timestamps_ms.len() < 50 {
            return 0.50;
        }
        let cutoff_lo = timestamp_ms - (lookback_seconds * 1000.0) as i64;
        let cutoff_hi = timestamp_ms;
        let lo = self.timestamps_ms.partition_point(|&t| t < cutoff_lo);
        let hi = self.timestamps_ms.partition_point(|&t| t <= cutoff_hi);
        if hi.saturating_sub(lo) < 30 {
            return 0.50;
        }
        let window_ts = &self.timestamps_ms[lo..hi];
        let window_p = &self.prices[lo..hi];

        crate::strategy::momentum::annualized_realized_vol(
            window_ts
                .iter()
                .zip(window_p.iter())
                .map(|(timestamp_ms, price)| (*timestamp_ms as f64 / 1000.0, *price)),
            20,
        )
        .unwrap_or(0.50)
    }
}

pub fn classify_btc_source_values(source_values: &BTreeSet<String>) -> &'static str {
    const CHAINLINK_SOURCES: &[&str] = &[
        "chainlink_btc_usd_data_stream",
        "crypto_prices_chainlink",
        "polymarket_rtds_chainlink_btc_usd",
    ];
    const BINANCE_SOURCES: &[&str] = &[
        "binance",
        "binance_btcusdt_klines",
        "binance_btcusdt_rtds",
        "crypto_prices",
    ];
    const BINANCE_RTDS_SOURCES: &[&str] = &["binance_btcusdt_rtds", "crypto_prices"];
    if !source_values.is_empty()
        && source_values
            .iter()
            .all(|source| CHAINLINK_SOURCES.contains(&source.as_str()))
    {
        "chainlink_btc_usd_data_stream"
    } else if !source_values.is_empty()
        && source_values
            .iter()
            .all(|source| BINANCE_RTDS_SOURCES.contains(&source.as_str()))
    {
        "binance_btcusdt_rtds"
    } else if !source_values.is_empty()
        && source_values
            .iter()
            .all(|source| BINANCE_SOURCES.contains(&source.as_str()))
    {
        "binance_btcusdt_klines"
    } else if source_values.len() > 1 {
        "multi_source_proxy"
    } else {
        "csv_unclassified"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_csv(rows: &[&str]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        for r in rows {
            writeln!(f, "{}", r).unwrap();
        }
        f
    }

    fn tape(points: &[(i64, f64)]) -> BTCHistory {
        let mut h = BTCHistory::default();
        for (ts, px) in points {
            h.timestamps_ms.push(*ts);
            h.prices.push(*px);
        }
        h
    }

    #[test]
    fn twap_weights_prices_by_time_in_force() {
        // Price 100 for 60s, then 200 for 240s of a 300s window:
        // TWAP = (100*60 + 200*240) / 300 = 180.
        let h = tape(&[(0, 100.0), (60_000, 200.0)]);
        // Tape must extend to end for the full-window TWAP.
        let h_full = tape(&[(0, 100.0), (60_000, 200.0), (300_000, 200.0)]);
        assert!(h.twap_between(0, 300_000).is_none(), "tape ends early: fail closed");
        let twap = h_full.twap_between(0, 300_000).unwrap();
        assert!((twap - 180.0).abs() < 1e-9, "got {twap}");
    }

    #[test]
    fn twap_fail_closed_without_price_at_window_start() {
        let h = tape(&[(120_000, 100.0), (300_000, 100.0)]);
        assert!(h.twap_between(0, 300_000).is_none());
    }

    #[test]
    fn partial_twap_uses_only_observable_ticks() {
        // Ticks at 0(100) and 240s(400). Partial at 240s must NOT see the
        // 240s tick's future duration: window [0,240) is all at 100 except
        // nothing — the 240s tick arrives exactly at as_of and is excluded.
        let h = tape(&[(0, 100.0), (240_000, 400.0), (300_000, 400.0)]);
        let partial = h.partial_twap(0, 240_000, 300_000).unwrap();
        assert!((partial - 100.0).abs() < 1e-9, "got {partial}");
        // The final TWAP blends the last minute at 400:
        // (100*240 + 400*60)/300 = 160.
        let full = h.twap_between(0, 300_000).unwrap();
        assert!((full - 160.0).abs() < 1e-9, "got {full}");
        // Determinism bound: |full - partial * t/T| <= remaining weight * max move.
        let locked = partial * 240.0 / 300.0; // 80
        assert!(full >= locked, "final TWAP can never fall below the locked component");
    }

    #[test]
    fn twap_vs_close_disagree_on_asymmetric_cross() {
        // Path: open 100, jumps to 110 after 1s, stays 269s, crashes to 90
        // for the last 30s. Close(90) < open(100) => close-vs-open says DOWN.
        // TWAP = (100*1 + 110*269 + 90*30)/300 ≈ 107.97 > 100 => TWAP says UP.
        // This is exactly the disagreement class that stalled the canary.
        let h = tape(&[(0, 100.0), (1_000, 110.0), (270_000, 90.0), (300_000, 90.0)]);
        let twap = h.twap_between(0, 300_000).unwrap();
        let close = h.price_at(300_000);
        let open = h.price_at(0);
        assert!(twap >= open, "TWAP rule: up ({twap} vs {open})");
        assert!(close < open, "close rule: down ({close} vs {open})");
    }

    #[test]
    fn empty_history_returns_zero() {
        let h = BTCHistory::new();
        assert_eq!(h.price_at(0), 0.0);
        assert_eq!(h.price_at(1_700_000_000_000), 0.0);
        assert_eq!(h.price_at_with_max_age(1_700_000_000_000, 10_000), None);
    }

    #[test]
    fn max_age_lookup_is_causal_and_fail_closed() {
        let mut h = BTCHistory::new();
        h.timestamps_ms.extend([1_000, 3_000, 6_000]);
        h.prices.extend([100.0, 101.0, 102.0]);

        assert_eq!(h.price_at_with_max_age(2_000, 1_000), Some(100.0));
        assert_eq!(h.price_at_with_max_age(2_000, 999), None);
        assert_eq!(h.price_at_with_max_age(3_000, 0), Some(101.0));
        assert_eq!(h.price_at_with_max_age(500, 10_000), None);
        assert_eq!(h.price_at_with_max_age(5_000, -1), None);
        assert_eq!(h.price_at_with_max_age(5_000, 10_000), Some(101.0));
        assert_eq!(
            h.price_and_age_at_with_max_age(5_000, 10_000),
            Some((101.0, 2_000))
        );
    }

    #[test]
    fn binance_kline_csv_shifts_to_close_time() {
        // Binance kline rows: open_time, open, high, low, close, volume
        // Stored at open_time + interval so a query exactly at open_time
        // returns 0 (the kline isn't observable yet).
        let f = write_csv(&[
            "timestamp,open,high,low,close,volume",
            "1700000000000,70000,70010,69990,70005,1.0",
            "1700000060000,70005,70015,69995,70010,1.0",
            "1700000120000,70010,70020,70000,70015,1.0",
        ]);
        let mut h = BTCHistory::new();
        h.load_csv(f.path()).unwrap();
        assert_eq!(h.source_kind(), "binance_btcusdt_klines");

        // Interval = 60_000 ms. Stored timestamps = open_time + 60_000.
        // Query at exact open_time => returns 0 (no kline closed yet).
        assert_eq!(h.price_at(1_700_000_000_000), 0.0);
        // Query at close_time of first kline => returns its close.
        assert!((h.price_at(1_700_000_060_000) - 70005.0).abs() < 1e-9);
        // Query 1ms before close => still 0 (causality).
        assert_eq!(h.price_at(1_700_000_059_999), 0.0);
    }

    #[test]
    fn collector_tick_csv_uses_observation_time() {
        let f = write_csv(&[
            "timestamp_ms,source,price",
            "1700000000000,binance,70000",
            "1700000001000,bybit,70010",
            "1700000002000,okx,70020",
        ]);
        let mut h = BTCHistory::new();
        h.load_csv(f.path()).unwrap();
        assert_eq!(h.timestamps_ms.len(), h.prices.len());
        assert_eq!(h.n_ticks(), 3);
        assert_eq!(h.price_at(1_699_999_999_000), 0.0);
        assert!((h.price_at(1_700_000_001_500) - 70010.0).abs() < 1e-9);
        assert!((h.price_at(1_700_000_010_000) - 70020.0).abs() < 1e-9);
    }

    #[test]
    fn collector_tick_csv_rejects_non_finite_prices() {
        let f = write_csv(&[
            "timestamp_ms,source,price",
            "1700000000000,binance,70000",
            "1700000001000,binance,inf",
            "1700000002000,binance,70020",
        ]);
        let mut h = BTCHistory::new();
        h.load_csv(f.path()).unwrap();

        assert_eq!(h.timestamps_ms.len(), h.prices.len());
        assert_eq!(h.n_ticks(), 2);
        assert_eq!(h.price_at(1_700_000_001_000), 70000.0);
        assert_eq!(h.price_at(1_700_000_002_000), 70020.0);
    }

    #[test]
    fn collector_tick_csv_records_chainlink_provenance() {
        let f = write_csv(&[
            "timestamp_ms,source,price",
            "1700000000000,chainlink_btc_usd_data_stream,70000",
            "1700000001000,chainlink_btc_usd_data_stream,70010",
        ]);
        let mut h = BTCHistory::new();
        h.load_csv(f.path()).unwrap();

        assert_eq!(h.source_kind(), "chainlink_btc_usd_data_stream");
    }

    #[test]
    fn collector_tick_csv_preserves_binance_rtds_provenance() {
        let f = write_csv(&[
            "timestamp_ms,source,price",
            "1700000000000,binance_btcusdt_rtds,70000",
            "1700000001000,binance_btcusdt_rtds,70010",
        ]);
        let mut h = BTCHistory::new();
        h.load_csv(f.path()).unwrap();

        assert_eq!(h.source_kind(), "binance_btcusdt_rtds");
    }

    #[test]
    fn realized_vol_returns_default_below_threshold() {
        let mut h = BTCHistory::new();
        // Just a couple of ticks — below the 50 min threshold.
        h.timestamps_ms = vec![1, 2, 3];
        h.prices = vec![100.0, 101.0, 99.0];
        assert!((h.realized_vol_at(3, 3600.0) - 0.50).abs() < 1e-9);
    }

    #[test]
    fn tape_cadence_reports_internal_gap() {
        let mut h = BTCHistory::new();
        h.timestamps_ms = vec![1_000, 2_000, 3_000, 15_000, 16_000];
        h.prices = vec![100.0, 101.0, 102.0, 103.0, 104.0];

        assert_eq!(h.median_interval_ms(1_000, 16_000), Some(1_000));
        assert_eq!(h.max_gap_ms(1_000, 16_000), Some(12_000));
    }

    #[test]
    fn range_at_returns_ohlc() {
        let f = write_csv(&[
            "timestamp_ms,source,price",
            "1700000000000,b,100",
            "1700000001000,b,105",
            "1700000002000,b,95",
            "1700000003000,b,102",
        ]);
        let mut h = BTCHistory::new();
        h.load_csv(f.path()).unwrap();
        let (o, hi, lo, c) = h.range_at(1_700_000_000_000, 1_700_000_003_000);
        assert!((o - 100.0).abs() < 1e-9);
        assert!((hi - 105.0).abs() < 1e-9);
        assert!((lo - 95.0).abs() < 1e-9);
        assert!((c - 102.0).abs() < 1e-9);
    }
}
