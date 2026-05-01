//! Reproducible experiment reports for harness runs.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
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
    #[serde(default)]
    pub strategy_params: serde_json::Value,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionGate {
    pub min_trades: usize,
    pub min_win_rate: f64,
    pub min_total_pnl: f64,
    pub require_complete_data: bool,
}

impl Default for PromotionGate {
    fn default() -> Self {
        Self {
            min_trades: 30,
            min_win_rate: 0.0,
            min_total_pnl: 0.0,
            require_complete_data: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionArtifact {
    pub schema_version: u32,
    pub created_at: String,
    pub source_report_hash: String,
    pub source_label: String,
    pub source_window: String,
    pub selected_strategy: StrategySpec,
    #[serde(default)]
    pub strategy_params: serde_json::Value,
    pub data_manifest_hash: String,
    pub market_count: usize,
    pub trades: usize,
    pub win_rate: f64,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub total_fees: f64,
    pub risk_notes: Vec<String>,
    pub promotion_gate: PromotionGate,
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
            strategy_params: serde_json::to_value(&run.variant).unwrap_or(serde_json::Value::Null),
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

impl PromotionArtifact {
    pub fn from_report(report: &ExperimentReport, gate: PromotionGate) -> Result<Self> {
        if gate.require_complete_data && !report.data_manifest.complete {
            bail!("promotion rejected: data manifest is incomplete");
        }
        let selected = report
            .variants
            .iter()
            .max_by(|a, b| {
                a.total_pnl
                    .partial_cmp(&b.total_pnl)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .context("promotion rejected: report has no variants")?;
        if selected.strategy_params.is_null() {
            bail!(
                "promotion rejected: selected variant lacks strategy_params; regenerate the report"
            );
        }
        if selected.trades < gate.min_trades {
            bail!(
                "promotion rejected: trades {} below minimum {}",
                selected.trades,
                gate.min_trades
            );
        }
        if selected.win_rate < gate.min_win_rate {
            bail!(
                "promotion rejected: win_rate {:.4} below minimum {:.4}",
                selected.win_rate,
                gate.min_win_rate
            );
        }
        if selected.total_pnl < gate.min_total_pnl {
            bail!(
                "promotion rejected: total_pnl {:.4} below minimum {:.4}",
                selected.total_pnl,
                gate.min_total_pnl
            );
        }

        let mut risk_notes = Vec::new();
        if selected.unresolved_fills > 0 {
            risk_notes.push(format!(
                "selected variant has {} unresolved fills",
                selected.unresolved_fills
            ));
        }
        risk_notes.extend(report.data_manifest.notes.iter().cloned());

        Ok(Self {
            schema_version: 1,
            created_at: Utc::now().to_rfc3339(),
            source_report_hash: crate::strategy::spec::stable_json_hash(report),
            source_label: report.label.clone(),
            source_window: format!("{}..{}", report.start, report.end),
            selected_strategy: selected.strategy.clone(),
            strategy_params: selected.strategy_params.clone(),
            data_manifest_hash: report.data_manifest.manifest_hash.clone(),
            market_count: report.market_catalog.market_count(),
            trades: selected.trades,
            win_rate: selected.win_rate,
            total_pnl: selected.total_pnl,
            avg_pnl: selected.avg_pnl,
            total_fees: selected.total_fees,
            risk_notes,
            promotion_gate: gate,
        })
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

pub fn read_report(path: impl AsRef<Path>) -> Result<ExperimentReport> {
    let path = path.as_ref();
    let payload =
        std::fs::read(path).with_context(|| format!("read experiment report {}", path.display()))?;
    serde_json::from_slice(&payload)
        .with_context(|| format!("parse experiment report {}", path.display()))
}

pub fn read_promotion(path: impl AsRef<Path>) -> Result<PromotionArtifact> {
    let path = path.as_ref();
    let payload = std::fs::read(path)
        .with_context(|| format!("read promotion artifact {}", path.display()))?;
    serde_json::from_slice(&payload)
        .with_context(|| format!("parse promotion artifact {}", path.display()))
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

pub fn write_promotion_atomic(
    path: impl AsRef<Path>,
    artifact: &PromotionArtifact,
) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create promotion dir {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(artifact).context("serialize PromotionArtifact")?;
    let tmp = path.with_extension(format!(
        "{}.tmp.{}",
        path.extension().and_then(|s| s.to_str()).unwrap_or("json"),
        std::process::id()
    ));
    std::fs::write(&tmp, payload)
        .with_context(|| format!("write tmp promotion artifact {}", tmp.display()))?;
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

    #[test]
    fn promotion_selects_best_passing_variant() {
        let cfg = cfg();
        let mut worse = VariantReport::from_run(&HarnessRun {
            variant: StrategyVariant::baseline(),
            results: BacktestResults::default(),
        });
        worse.trades = 30;
        worse.win_rate = 0.4;
        worse.total_pnl = 1.0;
        worse.avg_pnl = 0.03;
        let mut better = worse.clone();
        better.strategy.risk_profile = "better".to_string();
        better.total_pnl = 2.0;
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants = vec![worse, better];

        let artifact =
            PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap();

        assert_eq!(artifact.selected_strategy.risk_profile, "better");
        assert_eq!(artifact.trades, 30);
        assert_eq!(artifact.data_manifest_hash, report.data_manifest.manifest_hash);
    }

    #[test]
    fn promotion_rejects_incomplete_data() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.data_manifest.complete = false;
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 30,
            wins: 20,
            losses: 10,
            unresolved_fills: 0,
            win_rate: 0.66,
            total_pnl: 1.0,
            avg_pnl: 0.03,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: BTreeMap::new(),
        });

        let err = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap_err();

        assert!(err.to_string().contains("data manifest is incomplete"));
    }

    #[test]
    fn promotion_rejects_gate_failures() {
        let cfg = cfg();
        let mut report = ExperimentReport::from_harness("test", &cfg, &[]);
        report.variants.push(VariantReport {
            strategy: StrategySpec::new("s", "1", "hash", "risk"),
            strategy_params: serde_json::json!({"name": "test"}),
            trades: 5,
            wins: 3,
            losses: 2,
            unresolved_fills: 0,
            win_rate: 0.60,
            total_pnl: 1.0,
            avg_pnl: 0.20,
            total_fees: 0.0,
            sharpe_like: 1.0,
            by_zone: BTreeMap::new(),
        });

        let err = PromotionArtifact::from_report(&report, PromotionGate::default()).unwrap_err();

        assert!(err.to_string().contains("trades 5 below minimum"));
    }
}
