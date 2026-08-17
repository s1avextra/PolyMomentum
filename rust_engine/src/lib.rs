//! PolyMomentum engine library.
//!
//! Single-binary Rust trading engine for Polymarket Up/Down crypto candle
//! markets. Modules:
//!
//! - `config` — environment-driven settings
//! - `data` — market discovery, catalog, settlement, wallet, and source manifests
//! - `strategy` and `fair_value` — signals, trade decisions, and pricing
//! - `execution` and `risk` — order lifecycle, fees, sizing, and persisted risk state
//! - `monitoring` — session diagnostics, causality checks, alerts, and staleness
//! - `live` — paper/live runtime and replay parity
//! - `backtest`, `sweep`, and `strategy_builder` — replay-first research and promotion gates
//! - `polymarket_ws`, `exchange`, and `price_state` — market-data plumbing
//! - `clob` and `signing` — authenticated CLOB execution

#![forbid(unsafe_code)]

pub mod artifact;
pub mod backtest;
pub mod clob;
pub mod clob_user_ws;
pub mod config;
pub mod data;
pub mod exchange;
pub mod execution;
pub mod fair_value;
pub mod live;
pub mod monitoring;
pub mod polymarket_ws;
pub mod price_state;
pub mod release;
pub mod risk;
pub mod signing;
pub mod strategy;
pub mod strategy_builder;
pub mod sweep;

pub use fair_value::norm_cdf;
