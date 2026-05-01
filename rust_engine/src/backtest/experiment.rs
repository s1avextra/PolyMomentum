//! Reproducible experiment reports for harness runs.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::backtest::harness::{HarnessConfig, HarnessRun};
use crate::data::catalog::MarketCatalog;
use crate::data::manifest::{DataManifest, DataSourceManifest};
use crate::strategy::spec::StrategySpec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentReport {
    pub schema_version: u32,
    pub generated_at: String,
    pub label: String,
    pub mode: String,
    pub start: String,
    pub end: String,
    pub bankroll_usd: f64,
    pub latency_ms: u64,
    pub market_catalog: MarketCatalog,
    pub data_manifest: DataManifest,
    pub variants: Vec<VariantReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantReport {
    pub strategy: StrategySpec,
    pub trades: usize,
    pub wins: usize,
    pub losses: usize,
    pub unresolved_fills: usize,
    pub win_rate: f64,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub total_fees: f64,
    pub sharpe_like: f64,
    pub by_zone: BTreeMap<String, ZoneReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneReport {
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub win_rate: f64,
    pub pnl: f64,
}

impl ExperimentReport {
    pub fn from_harness(
        label: impl Into<String>,
        cfg: &HarnessConfig,
        runs: &[HarnessRun],
    ) -> Self {
        let market_catalog = MarketCatalog::from_candle_contracts(&cfg.universe.contracts);
        let data_manifest = harness_data_manifest(cfg, &market_catalog);
        let start = cfg
            .hours
            .first()
            .map(|h| h.to_rfc3339())
            .unwrap_or_default();
        let end = cfg.hours.last().map(|h| h.to_rfc3339()).unwrap_or_default();
        let mut variants: Vec<VariantReport> = runs
            .iter()
            .map(|run| VariantReport::from_run(run))
            .collect();
        variants.sort_by(|a, b| {
            b.total_pnl
                .partial_cmp(&a.total_pnl)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Self {
            schema_version: 1,
            generated_at: Utc::now().to_rfc3339(),
            label: label.into(),
            mode: "backtest".to_string(),
            start,
            end,
            bankroll_usd: cfg.bankroll_usd,
            latency_ms: cfg.latency.insert_ms,
            market_catalog,
            data_manifest,
            variants,
        }
    }
}

impl VariantReport {
    fn from_run(run: &HarnessRun) -> Self {
        let strategy = StrategySpec::from_serializable_params(
            "candle_momentum",
            "1",
            &run.variant,
            format!(
                "position_pct={:.4};max_per_market_usd={:.2}",
                run.variant.position_pct, run.variant.max_per_market_usd
            ),
        );
        let by_zone = run
            .results
            .by_zone()
            .into_iter()
            .map(|(zone, bucket)| {
                (
                    zone,
                    ZoneReport {
                        trades: bucket.trades,
                        wins: bucket.wins,
                        losses: bucket.losses,
                        win_rate: bucket.win_rate(),
                        pnl: bucket.pnl,
                    },
                )
            })
            .collect();
        Self {
            strategy,
            trades: run.results.n_trades(),
            wins: run.results.n_wins(),
            losses: run.results.n_losses(),
            unresolved_fills: run.results.unresolved_fills.len(),
            win_rate: run.results.win_rate(),
            total_pnl: run.results.total_pnl(),
            avg_pnl: run.results.avg_pnl(),
            total_fees: run.results.total_fees(),
            sharpe_like: run.results.sharpe(),
            by_zone,
        }
    }
}

fn harness_data_manifest(cfg: &HarnessConfig, catalog: &MarketCatalog) -> DataManifest {
    let start = cfg.hours.first().map(|h| h.to_rfc3339());
    let end = cfg.hours.last().map(|h| h.to_rfc3339());

    let mut pmxt = DataSourceManifest::new("pmxt_v2_archive", "order_book_l2");
    pmxt.path = Some(cfg.cache_dir.display().to_string());
    pmxt.start = start.clone();
    pmxt.end = end.clone();
    pmxt.row_count = Some(cfg.hours.len() as u64);
    pmxt.complete = !cfg.hours.is_empty() && catalog.is_complete();
    pmxt.metadata
        .insert("hours".to_string(), cfg.hours.len().to_string());
    pmxt.metadata.insert(
        "market_count".to_string(),
        catalog.market_count().to_string(),
    );
    pmxt.metadata
        .insert("token_count".to_string(), catalog.token_count().to_string());
    pmxt.metadata
        .insert("assets".to_string(), catalog.assets().join(","));
    if let Some(shared) = &cfg.shared_distilled_dir {
        pmxt.metadata.insert(
            "shared_distilled_dir".to_string(),
            shared.display().to_string(),
        );
    }

    let mut btc = DataSourceManifest::new("btc_price_tape", "external_price");
    btc.start = start;
    btc.end = end;
    btc.row_count = Some(cfg.btc_history.n_ticks() as u64);
    btc.complete = cfg.btc_history.n_ticks() >= 50;
    btc.metadata
        .insert("symbol".to_string(), "BTCUSDT".to_string());

    let mut notes = Vec::new();
    let missing = catalog.missing_required_tokens();
    if !missing.is_empty() {
        notes.push(format!("missing required token ids: {}", missing.join(",")));
    }
    DataManifest::new(vec![pmxt, btc], notes)
}

pub fn write_report_atomic(path: impl AsRef<Path>, report: &ExperimentReport) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create report dir {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(report).context("serialize ExperimentReport")?;
    let tmp = path.with_extension(format!(
        "{}.tmp.{}",
        path.extension().and_then(|s| s.to_str()).unwrap_or("json"),
        std::process::id()
    ));
    std::fs::write(&tmp, payload)
        .with_context(|| format!("write tmp experiment report {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::btc_history::BTCHistory;
    use crate::backtest::harness::{CandleUniverse, HarnessConfig};
    use crate::backtest::l2_replay::StaticLatencyConfig;
    use crate::backtest::resolver::BacktestResults;
    use crate::backtest::strategies::StrategyVariant;
    use crate::data::models::{Market, Outcome};
    use crate::data::scanner::CandleContract;

    fn cfg() -> HarnessConfig {
        let mut btc = BTCHistory::default();
        for i in 0..60 {
            btc.timestamps_ms.push(1_700_000_000_000 + i * 1000);
            btc.prices.push(70_000.0 + i as f64);
        }
        HarnessConfig {
            hours: vec![chrono::DateTime::parse_from_rfc3339("2026-05-01T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc)],
            universe: CandleUniverse {
                contracts: vec![CandleContract {
                    market: Market {
                        condition_id: "0xabc".to_string(),
                        question: "Bitcoin Up or Down - test".to_string(),
                        slug: "btc-test".to_string(),
                        outcomes: vec![
                            Outcome {
                                token_id: "up-token".to_string(),
                                name: "Up".to_string(),
                                price: 0.5,
                            },
                            Outcome {
                                token_id: "down-token".to_string(),
                                name: "Down".to_string(),
                                price: 0.5,
                            },
                        ],
                        tags: Vec::new(),
                        category: String::new(),
                        active: true,
                        closed: false,
                        volume: 1000.0,
                        liquidity: 500.0,
                        end_date: "2026-05-01T00:05:00Z".to_string(),
                        event_slug: String::new(),
                        event_id: String::new(),
                        event_title: String::new(),
                        group_slug: String::new(),
                        neg_risk: false,
                        neg_risk_augmented: false,
                    },
                    up_token_id: "up-token".to_string(),
                    down_token_id: "down-token".to_string(),
                    up_price: 0.5,
                    down_price: 0.5,
                    end_date: "2026-05-01T00:05:00Z".to_string(),
                    hours_left: 0.0,
                    volume: 1000.0,
                    liquidity: 500.0,
                    window_description: "test".to_string(),
                    asset: "BTC".to_string(),
                }],
            },
            btc_history: std::sync::Arc::new(btc),
            bankroll_usd: 100.0,
            cache_dir: std::path::PathBuf::from("/tmp/pmxt"),
            latency: StaticLatencyConfig { insert_ms: 50 },
            shared_distilled_dir: None,
            threads: Some(1),
            checkpoint_dir: None,
            stop_flag: None,
        }
    }

    #[test]
    fn report_contains_manifest_and_sorted_variant() {
        let cfg = cfg();
        let runs = vec![HarnessRun {
            variant: StrategyVariant::baseline(),
            results: BacktestResults::default(),
        }];
        let report = ExperimentReport::from_harness("test", &cfg, &runs);
        assert_eq!(report.mode, "backtest");
        assert_eq!(report.market_catalog.market_count(), 1);
        assert!(report.data_manifest.complete);
        assert_eq!(report.variants.len(), 1);
        assert_eq!(report.variants[0].strategy.name, "candle_momentum");
    }
}
