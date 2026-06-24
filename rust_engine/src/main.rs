//! polymomentum-engine: unified Rust binary.
//!
//! Subcommands:
//!   live                              — main runtime (paper/live)
//!   scan                              — Gamma + scanner smoke test
//!   wallet                            — print wallet balances
//!   ctf <condition_id>                — read on-chain CTF resolution
//!   validate-replay <session.jsonl>   — replay-validator (parity check vs decision function)
//!
//! Environment-driven configuration. See `src/config.rs` for the full list of
//! variables; the runtime reads `.env` from the working directory if present.

mod backtest;
mod clob;
mod clob_user_ws;
mod config;
mod data;
mod exchange;
mod execution;
mod fair_value;
mod live;
mod monitoring;
mod polymarket_ws;
mod price_state;
mod release;
mod risk;
mod signing;
mod strategy;
mod strategy_builder;
mod sweep;

use anyhow::Context;
use clap::{Parser, Subcommand};
use config::RuntimeMode;

#[derive(Parser, Debug)]
#[command(
    name = "polymomentum-engine",
    version,
    about = "PolyMomentum Rust trading engine"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Override log level (e.g. info, debug, trace)
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log: String,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the candle trading runtime
    Live {
        /// Paper or live mode (live requires explicit confirmation flag)
        #[arg(long, value_enum, default_value_t = RuntimeMode::Paper)]
        mode: RuntimeMode,
        /// Allow live mode (default: paper-only safeguard).
        #[arg(long)]
        i_understand_live: bool,
        /// Promotion artifact JSON to bind this runtime to a backtested variant.
        #[arg(long)]
        promotion_artifact: Option<String>,
        /// Permit a stale promotion artifact only for paper-mode research diagnostics.
        #[arg(long)]
        allow_stale_research_artifact: bool,
    },
    /// Replay the live decision/order diagnostics loop from cached PMXT + BTC data.
    LiveReplay {
        /// Inclusive UTC start hour (RFC3339), e.g. 2026-04-25T10:00:00Z.
        #[arg(long)]
        start: String,
        /// Inclusive UTC end hour. Defaults to `start`.
        #[arg(long)]
        end: Option<String>,
        /// PMXT v2 cache directory.
        #[arg(long)]
        cache_dir: Option<String>,
        /// BTC tick/kline CSV used as the virtual exchange price feed. If omitted, pull Binance
        /// public klines for the required replay range.
        #[arg(long)]
        btc_csv: Option<String>,
        /// Replay bankroll used for sizing.
        #[arg(long, default_value_t = 100.0)]
        bankroll: f64,
        /// Simulated insert latency in milliseconds.
        #[arg(long, default_value_t = 50)]
        latency_ms: u64,
        /// Output session JSONL directory. Defaults to SESSION_LOG_DIR.
        #[arg(long)]
        session_log_dir: Option<String>,
        /// Permit downloading missing PMXT hours. Default is cache-only.
        #[arg(long, default_value_t = false)]
        allow_download: bool,
        /// Delete raw PMXT parquet files downloaded by this live-replay after completion.
        #[arg(long, default_value_t = false)]
        delete_after_process: bool,
        /// Permit Gamma fetches for missing historical metadata.
        #[arg(long, default_value_t = false)]
        allow_gamma_fetch: bool,
        /// Cap the BTC candle universe for short resource-friendly diagnostics.
        #[arg(long)]
        max_contracts: Option<usize>,
        /// Restrict the candle universe to one window length, e.g. 5 for 5-minute candles.
        #[arg(long)]
        window_minutes: Option<f64>,
        /// Promotion artifact JSON to replay the same strategy as paper/live.
        #[arg(long)]
        promotion_artifact: Option<String>,
        /// Treat settlement alignment as verified for this offline replay.
        #[arg(long, default_value_t = false)]
        settlement_alignment_ready: bool,
        /// Write the live-replay report JSON to this path in addition to stdout.
        #[arg(long)]
        report_json: Option<String>,
    },
    /// Run startup checks without opening market-data or order connections.
    Preflight {
        /// Paper or live mode to validate.
        #[arg(long, value_enum, default_value_t = RuntimeMode::Paper)]
        mode: RuntimeMode,
        /// Required when validating the live startup path.
        #[arg(long)]
        i_understand_live: bool,
        /// Promotion artifact JSON to validate.
        #[arg(long)]
        promotion_artifact: Option<String>,
        /// Permit a stale promotion artifact only for paper-mode research diagnostics.
        #[arg(long)]
        allow_stale_research_artifact: bool,
    },
    /// Print the release manifest used in preflight and session logs.
    ReleaseManifest {
        /// Paper or live mode to include in the manifest.
        #[arg(long, value_enum, default_value_t = RuntimeMode::Paper)]
        mode: RuntimeMode,
        /// Promotion artifact JSON to include in the manifest.
        #[arg(long)]
        promotion_artifact: Option<String>,
    },
    /// Smoke-test scanner: fetch candle markets, print summary.
    Scan {
        #[arg(long, default_value_t = 2.0)]
        max_hours: f64,
        #[arg(long, default_value_t = 100.0)]
        min_liquidity: f64,
    },
    /// Print wallet balances (pUSD, USDC diagnostics, POL).
    Wallet {
        /// Emit machine-readable JSON including live_ready.
        #[arg(long)]
        json: bool,
    },
    /// Read-only CLOB diagnostics. These do not place orders.
    Clob {
        #[command(subcommand)]
        command: ClobCommand,
    },
    /// Experiment report utilities for promoting backtests toward paper/live.
    Experiment {
        #[command(subcommand)]
        command: ExperimentCommand,
    },
    /// Analyze runtime diagnostics from session JSONL logs.
    Diagnostics {
        #[command(subcommand)]
        command: DiagnosticsCommand,
    },
    /// Telegram operator monitor: read-only status cards and callbacks.
    Telegram {
        #[command(subcommand)]
        command: TelegramCommand,
    },
    /// Plan and audit the full backtest → promotion → replay strategy-builder loop.
    StrategyBuilder {
        #[command(subcommand)]
        command: StrategyBuilderCommand,
    },
    /// Read CTF resolution for a condition_id.
    Ctf { condition_id: String },
    /// Validate a paper session JSONL replays clean against the decision function.
    ValidateReplay { path: String },
    /// Distill a parquet hour into the shared candles-only JSONL.gz format
    /// (v1 schema; see docs/cross_bot_distilled_cache_response.md). Output
    /// is shareable with polyarbitrage on the multi-tenant VPS.
    Distill {
        /// Path to the source parquet (e.g. polymarket_orderbook_2026-04-26T08.parquet).
        #[arg(long)]
        input: String,
        /// Output path. If omitted, derived from --input + the v1 naming.
        #[arg(long)]
        output: Option<String>,
        /// Path to a file containing candle condition_ids, one per line OR
        /// comma-separated. If omitted, the binary auto-discovers via Gamma.
        #[arg(long)]
        candle_cids: Option<String>,
        /// Override the hour for auto-discovery (defaults to parsing the
        /// hour out of the parquet filename).
        #[arg(long)]
        hour: Option<String>,
    },
    /// Pre-download PMXT v2 archives for a UTC hour range so subsequent
    /// `harness` runs are offline-fast.
    PmxtDownload {
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: Option<String>,
        #[arg(long)]
        cache_dir: Option<String>,
    },
    /// Print PMXT v2 archive metadata for a given hour: distinct
    /// condition_ids, sample IDs, total event count.
    PmxtInfo {
        #[arg(long)]
        hour: String,
        #[arg(long)]
        cache_dir: Option<String>,
        #[arg(long, default_value_t = 5)]
        sample: usize,
    },
    /// Sweep a parameter grid through the full L2-backtest harness. Generates
    /// cartesian product of confidence × z × edge × ev × {taker, maker} —
    /// runs every cell against the same hours and ranks by PnL.
    HarnessSweep {
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: Option<String>,
        #[arg(long, default_value_t = 100.0)]
        bankroll: f64,
        #[arg(long)]
        cache_dir: Option<String>,
        #[arg(long)]
        btc_csv: Option<String>,
        #[arg(long, default_value_t = 50)]
        latency_ms: u64,
        /// Comma-separated confidence thresholds.
        #[arg(long, default_value = "0.30,0.40,0.50,0.60")]
        conf: String,
        /// Comma-separated z-score thresholds.
        #[arg(long, default_value = "0.20,0.50,1.00")]
        z: String,
        /// Comma-separated edge thresholds.
        #[arg(long, default_value = "0.00,0.03,0.07")]
        edge: String,
        /// Comma-separated EV buffers (negative disables the EV gate).
        #[arg(long, default_value = "-1.0,0.05")]
        ev_buffer: String,
        /// Comma-separated minimum executable token prices.
        #[arg(long, default_value = "0.10")]
        min_price: String,
        /// Comma-separated maximum executable token prices.
        #[arg(long, default_value = "0.90")]
        max_price: String,
        /// Comma-separated hard settlement cutoffs in minutes.
        #[arg(long, default_value = "0.30")]
        settlement_cutoff_minutes: String,
        /// Comma-separated settlement floors in USD for the final-window guard.
        #[arg(long, default_value = "10.0")]
        settlement_floor: String,
        /// Comma-separated final-window settlement guard lengths in minutes.
        #[arg(long, default_value = "1.0")]
        settlement_guard_minutes: String,
        /// Comma-separated volatility-scaled settlement buffer multipliers.
        #[arg(long, default_value = "0.0")]
        settlement_sigma_buffer: String,
        /// Comma-separated max reversion counts; 0 disables the cap.
        #[arg(long, default_value = "0")]
        max_reversion_count: String,
        /// Comma-separated min reversion counts; 0 disables the floor.
        #[arg(long, default_value = "0")]
        min_reversion_count: String,
        /// Comma-separated executable spread ceilings for microstructure gates.
        #[arg(long, default_value = "1.0")]
        micro_max_spread: String,
        /// Comma-separated minimum thinner-side book depth gates.
        #[arg(long, default_value = "0.0")]
        micro_min_depth: String,
        /// Comma-separated minimum microprice pressure gates.
        #[arg(long, default_value = "-1.0")]
        micro_min_pressure: String,
        /// Fraction of bankroll to risk per attempted candle trade.
        #[arg(long, default_value_t = 0.10)]
        position_pct: f64,
        /// Hard USD cap per market for attempted candle trades.
        #[arg(long, default_value_t = 20.0)]
        max_per_market_usd: f64,
        /// Hard USD cap across unresolved candle exposure.
        #[arg(long)]
        max_total_exposure_usd: Option<f64>,
        /// Soft cap on projected stressed drawdown before adding a new order; 0 disables.
        #[arg(long, default_value = "0.0")]
        max_projected_stressed_drawdown_pct: String,
        /// Comma-separated realized loss counts that activate degraded execution fallback; 0 disables.
        #[arg(long, default_value = "0")]
        degraded_after_losses: String,
        /// Comma-separated realized drawdown fractions needed for degraded execution fallback.
        #[arg(long, default_value = "0.0")]
        degraded_after_drawdown_pct: String,
        /// Comma-separated z-score floors while degraded execution fallback is active.
        #[arg(long, default_value = "0.0")]
        degraded_min_z: String,
        /// Comma-separated max executable token prices while degraded execution fallback is active; 0 disables.
        #[arg(long, default_value = "0.0")]
        degraded_max_price: String,
        /// Force taker execution while degraded execution fallback is active.
        #[arg(long, default_value_t = false)]
        degraded_force_taker: bool,
        /// Include both maker and taker fill model variants per cell.
        #[arg(long, default_value_t = true)]
        also_maker: bool,
        /// Restrict the sweep grid to maker fill variants only.
        #[arg(long, default_value_t = false)]
        maker_only: bool,
        /// Restrict grid to one timing zone: all, early, primary, late, terminal.
        #[arg(long, default_value = "all")]
        zone_mode: String,
        /// Restrict the sweep grid to taker fill variants only.
        #[arg(long, default_value_t = false)]
        taker_only: bool,
        /// Show top N variants in the report.
        #[arg(long, default_value_t = 20)]
        top: usize,
        /// Variant-fan-out thread count. 0 → rayon's default (num_cpus, also
        /// honors `RAYON_NUM_THREADS`). 1 → serial. Use small N on the VPS
        /// (e.g. 1) per CLAUDE.md rule 5; full N=num_cpus on a dev box.
        #[arg(long, default_value_t = 0)]
        threads: usize,
        /// Pause/resume checkpoint dir. Per-hour `<hour>.json` files are
        /// written after each hour completes; touch `<dir>/PAUSE` (or send
        /// SIGINT) for a clean exit between hours. Re-run with the same
        /// `--checkpoint` to resume; pass `--resume` to acknowledge the
        /// existing state.
        #[arg(long)]
        checkpoint: Option<String>,
        /// Acknowledge an existing checkpoint dir and continue. Without
        /// this flag, a non-empty checkpoint dir aborts the run to avoid
        /// silently mixing two runs' results.
        #[arg(long, default_value_t = false)]
        resume: bool,
        /// Write a reproducible JSON experiment report to this path.
        #[arg(long)]
        report_json: Option<String>,
        /// Write full resolved trades for every sweep variant to this path.
        #[arg(long)]
        trades_json: Option<String>,
        /// Write compact per-trade causal features for every sweep variant to this path.
        #[arg(long)]
        trade_features_json: Option<String>,
        /// Require causal decision tags before order creation, e.g. direction=down. Repeat or comma-separate.
        #[arg(long)]
        require_causal_tag: Vec<String>,
        /// Deny causal decision tags before order creation, e.g. direction=up. Repeat or comma-separate.
        #[arg(long)]
        deny_causal_tag: Vec<String>,
        /// Restrict the candle universe to one window length, e.g. 5 for 5-minute candles.
        #[arg(long)]
        window_minutes: Option<f64>,
        /// Offline diagnostic only: let `win_rate_low` pause for N minutes,
        /// then resume if no exposure is open. Candidates that need this
        /// are still rejected by promotion gates.
        #[arg(long, default_value_t = 0.0)]
        adaptive_health_rearm_minutes: f64,
        /// Preserve strategy/fill/book state across hours to mirror live-replay.
        #[arg(long, default_value_t = false)]
        continuous: bool,
        /// Download/replay/delete each PMXT parquet hour inside the harness loop.
        #[arg(long, default_value_t = false)]
        atomic_parquet: bool,
    },
    /// Run the full L2-backtest harness over PMXT v2 archives. Loads candle
    /// markets from Gamma, downloads/streams the requested UTC hours,
    /// replays them through each strategy variant, resolves against the
    /// actual BTC tape, and prints per-variant P&L.
    Harness {
        /// Inclusive UTC start hour (RFC3339), e.g. 2026-04-26T10:00:00Z.
        #[arg(long)]
        start: String,
        /// Inclusive UTC end hour. Defaults to `start` (single hour).
        #[arg(long)]
        end: Option<String>,
        /// Bankroll used to size hypothetical trades.
        #[arg(long, default_value_t = 100.0)]
        bankroll: f64,
        /// Hard USD cap across unresolved candle exposure.
        #[arg(long)]
        max_total_exposure_usd: Option<f64>,
        /// PMXT v2 cache directory (otherwise env, shared VPS cache, then local fallback).
        #[arg(long)]
        cache_dir: Option<String>,
        /// BTC kline CSV (Binance format) used for the tape. If omitted, the
        /// harness pulls 1m klines from Binance's public REST.
        #[arg(long)]
        btc_csv: Option<String>,
        /// Insert latency in ms (strategy → fill).
        #[arg(long, default_value_t = 50)]
        latency_ms: u64,
        /// Variant-fan-out thread count (see harness-sweep --threads).
        #[arg(long, default_value_t = 0)]
        threads: usize,
        /// Pause/resume checkpoint dir. Per-hour `<hour>.json` files are
        /// written after each hour completes; touch `<dir>/PAUSE` (or send
        /// SIGINT) for a clean exit between hours.
        #[arg(long)]
        checkpoint: Option<String>,
        /// Acknowledge an existing checkpoint dir and continue. Without
        /// this flag, a non-empty checkpoint dir aborts the run to avoid
        /// silently mixing two runs' results.
        #[arg(long, default_value_t = false)]
        resume: bool,
        /// Cap the BTC candle universe for short resource-friendly diagnostics.
        #[arg(long)]
        max_contracts: Option<usize>,
        /// Restrict the candle universe to one window length, e.g. 5 for 5-minute candles.
        #[arg(long)]
        window_minutes: Option<f64>,
        /// Permit archive-wide condition-id scans and Gamma fetches for missing historical metadata.
        #[arg(long, default_value_t = false)]
        allow_gamma_fetch: bool,
        /// Fetch/write Gamma metadata and exit before any PMXT replay.
        #[arg(long, default_value_t = false)]
        metadata_only: bool,
        /// Write a reproducible JSON experiment report to this path.
        #[arg(long)]
        report_json: Option<String>,
        /// Offline diagnostic only: let `win_rate_low` pause for N minutes,
        /// then resume if no exposure is open. Candidates that need this
        /// are still rejected by promotion gates.
        #[arg(long, default_value_t = 0.0)]
        adaptive_health_rearm_minutes: f64,
        /// Preserve strategy/fill/book state across hours to mirror live-replay.
        #[arg(long, default_value_t = false)]
        continuous: bool,
        /// Download/replay/delete each PMXT parquet hour inside the harness loop.
        #[arg(long, default_value_t = false)]
        atomic_parquet: bool,
    },
    /// Replay one or more captured session JSONLs through a grid of strategy
    /// variants and report synthetic P&L per variant.
    Sweep {
        /// Path(s) to session_*.jsonl files. Repeat the flag for multiple.
        #[arg(long)]
        session: Vec<String>,
        /// Bankroll used to size hypothetical trades.
        #[arg(long, default_value_t = 100.0)]
        bankroll: f64,
        /// Fraction of bankroll risked per hypothetical trade.
        #[arg(long, default_value_t = 0.10)]
        position_pct: f64,
        /// Maximum USD risked per hypothetical market.
        #[arg(long, default_value_t = 20.0)]
        max_per_market_usd: f64,
        /// Minimum trades for a variant before its numbers are considered
        /// statistically meaningful.
        #[arg(long, default_value_t = 30)]
        min_trades: u64,
        /// Show per-zone breakdown for each strategy.
        #[arg(long, default_value_t = false)]
        zones: bool,
        /// Run a cartesian parameter grid instead of the built-in named variants.
        #[arg(long, default_value_t = false)]
        grid: bool,
        /// Comma-separated confidence thresholds for --grid.
        #[arg(long, default_value = "0.20,0.25,0.30,0.35")]
        conf: String,
        /// Comma-separated z-score thresholds for --grid.
        #[arg(long, default_value = "0.0,0.5")]
        z: String,
        /// Comma-separated edge thresholds for --grid.
        #[arg(long, default_value = "0.00,0.02,0.05")]
        edge: String,
        /// Comma-separated EV buffers for --grid (negative disables the EV gate).
        #[arg(long, default_value = "-1.0,0.02")]
        ev_buffer: String,
        /// Comma-separated minimum executable token prices for --grid.
        #[arg(long, default_value = "0.10")]
        min_price: String,
        /// Comma-separated maximum executable token prices for --grid.
        #[arg(long, default_value = "0.75")]
        max_price: String,
        /// Comma-separated hard settlement cutoffs in minutes for --grid.
        #[arg(long, default_value = "0.30")]
        settlement_cutoff_minutes: String,
        /// Comma-separated settlement floors in USD for --grid.
        #[arg(long, default_value = "25.0")]
        settlement_floor: String,
        /// Comma-separated settlement guard lengths in minutes for --grid.
        #[arg(long, default_value = "5.0")]
        settlement_guard_minutes: String,
        /// Comma-separated volatility-scaled settlement buffers for --grid.
        #[arg(long, default_value = "0.20")]
        settlement_sigma_buffer: String,
        /// Comma-separated max reversion counts for --grid; 0 disables the cap.
        #[arg(long, default_value = "0")]
        max_reversion_count: String,
        /// Comma-separated executable spread ceilings for --grid.
        #[arg(long, default_value = "0.02")]
        micro_max_spread: String,
        /// Comma-separated thinner-side book depth gates for --grid.
        #[arg(long, default_value = "20.0")]
        micro_min_depth: String,
        /// Comma-separated minimum microprice pressure gates for --grid.
        #[arg(long, default_value = "0.0")]
        micro_min_pressure: String,
        /// Include both maker and taker fill variants for --grid.
        #[arg(long, default_value_t = true)]
        also_maker: bool,
        /// Restrict --grid to one timing zone: all, early, primary, late, terminal.
        #[arg(long, default_value = "all")]
        zone_mode: String,
        /// Show only the top N variants after ranking by PnL.
        #[arg(long, default_value_t = 20)]
        top: usize,
        /// Write sorted sweep results to JSON.
        #[arg(long)]
        report_json: Option<String>,
    },
    /// Generate replay-grade signal/resolution JSONL from cached PMXT once.
    EvalCache {
        /// Inclusive UTC start hour (RFC3339).
        #[arg(long)]
        start: String,
        /// Inclusive UTC end hour. Defaults to `start`.
        #[arg(long)]
        end: Option<String>,
        /// PMXT v2 cache directory.
        #[arg(long)]
        cache_dir: Option<String>,
        /// BTC kline CSV used as the virtual exchange price feed.
        #[arg(long)]
        btc_csv: Option<String>,
        /// Output JSONL path for signal.evaluation + resolution.resolved rows.
        #[arg(long)]
        output: String,
        /// Restrict the candle universe to one window length, e.g. 5.
        #[arg(long)]
        window_minutes: Option<f64>,
        /// Permit Gamma fetches for missing historical metadata.
        #[arg(long, default_value_t = false)]
        allow_gamma_fetch: bool,
    },
    /// Run unit + integration tests embedded in the binary.
    SelfTest,
}

#[derive(Subcommand, Debug)]
enum ClobCommand {
    /// CLOB health check.
    Ok,
    /// CLOB server time.
    Time,
    /// Fetch an order book by outcome token ID.
    Book { token_id: String },
    /// Fetch the current buy/sell price for an outcome token.
    Price {
        token_id: String,
        #[arg(long, default_value = "BUY")]
        side: String,
    },
    /// Fetch midpoint for an outcome token.
    Midpoint { token_id: String },
    /// Fetch spread for an outcome token.
    Spread { token_id: String },
    /// Fetch minimum tick size for an outcome token.
    TickSize { token_id: String },
    /// Fetch fee rate in basis points for an outcome token.
    FeeRate { token_id: String },
    /// Check whether the token's market is negative-risk.
    NegRisk { token_id: String },
    /// Fetch CLOB market metadata by condition ID.
    Market { condition_id: String },
    /// Fetch authenticated open orders for reconciliation diagnostics.
    Orders {
        #[arg(long)]
        market: Option<String>,
        #[arg(long)]
        asset_id: Option<String>,
        #[arg(long)]
        next_cursor: Option<String>,
    },
    /// Fetch one authenticated order by order hash.
    Order { order_id: String },
    /// Fetch authenticated user trades for reconciliation diagnostics.
    Trades {
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        market: Option<String>,
        #[arg(long)]
        asset_id: Option<String>,
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        before: Option<String>,
        #[arg(long)]
        next_cursor: Option<String>,
    },
    /// Send the authenticated CLOB heartbeat used by live order safety.
    Heartbeat,
}

#[derive(Subcommand, Debug)]
enum ExperimentCommand {
    /// Audit timing-zone concentration for a comparable variant across reports.
    ZoneAudit {
        /// Input JSON generated by harness or harness-sweep --report-json.
        /// Repeat once per independent window.
        #[arg(long, required = true)]
        report: Vec<String>,
        /// Optional strategy params_hash to audit. Defaults to the aggregate top-PnL variant.
        #[arg(long)]
        params_hash: Option<String>,
        /// Maximum share of selected aggregate trades allowed from one timing zone.
        #[arg(long, default_value_t = 0.70)]
        max_zone_trade_share: f64,
        /// Optional minimum trades required in each active aggregate timing zone. 0 disables.
        #[arg(long, default_value_t = 0)]
        min_zone_trades: u64,
        /// Optional minimum PnL required in each active aggregate timing zone.
        #[arg(long)]
        min_zone_pnl: Option<f64>,
    },
    /// Promote the best passing backtest variant into a deployable artifact.
    Promote {
        /// Input JSON generated by harness or harness-sweep --report-json.
        #[arg(long)]
        report: String,
        /// Output promotion artifact JSON path.
        #[arg(long)]
        output: String,
        /// Minimum selected-variant trade count.
        #[arg(long, default_value_t = 30)]
        min_trades: usize,
        /// Minimum selected-variant loss count, guarding against lossless tiny samples.
        #[arg(long, default_value_t = 1)]
        min_losses: usize,
        /// Minimum number of timing zones with at least one selected-variant trade.
        #[arg(long, default_value_t = 2)]
        min_zone_count: usize,
        /// Minimum selected-variant win rate, e.g. 0.52.
        #[arg(long, default_value_t = 0.0)]
        min_win_rate: f64,
        /// Minimum Wilson 95% lower bound for selected-variant win rate.
        #[arg(long, default_value_t = 0.0)]
        min_wilson_win_rate_lower: f64,
        /// Minimum selected-variant total PnL.
        #[arg(long, default_value_t = 0.0)]
        min_total_pnl: f64,
        /// Minimum selected-variant Sharpe-like score.
        #[arg(long, default_value_t = 0.0)]
        min_sharpe_like: f64,
        /// Maximum unresolved fills allowed in the selected variant.
        #[arg(long, default_value_t = 0)]
        max_unresolved_fills: usize,
        /// Maximum non-passive failed execution attempts allowed in the selected variant.
        #[arg(long, default_value_t = 0)]
        max_failed_fills: usize,
        /// Maximum passive maker non-fills/post-only rejects allowed in the selected variant.
        #[arg(long, default_value_t = 0)]
        max_passive_failed_fills: usize,
        /// Minimum selected-variant fill rate across execution attempts.
        #[arg(long, default_value_t = 0.0)]
        min_fill_rate: f64,
        /// Maximum share of selected trades allowed from one timing zone.
        #[arg(long, default_value_t = 0.70)]
        max_zone_trade_share: f64,
        /// Permit promotion when the data manifest is incomplete.
        #[arg(long, default_value_t = false)]
        allow_incomplete_data: bool,
    },
    /// Promote the best variant that passes aggregate gates across reports.
    AggregatePromote {
        /// Input JSON generated by harness or harness-sweep --report-json.
        /// Repeat once per out-of-sample window.
        #[arg(long, required = true)]
        report: Vec<String>,
        /// Output promotion artifact JSON path.
        #[arg(long)]
        output: String,
        /// Minimum aggregate trade count.
        #[arg(long, default_value_t = 90)]
        min_trades: usize,
        /// Minimum aggregate loss count, guarding against lossless tiny samples.
        #[arg(long, default_value_t = 1)]
        min_losses: usize,
        /// Minimum number of timing zones with at least one selected aggregate trade.
        #[arg(long, default_value_t = 2)]
        min_zone_count: usize,
        /// Minimum aggregate win rate, e.g. 0.52.
        #[arg(long, default_value_t = 0.0)]
        min_win_rate: f64,
        /// Minimum Wilson 95% lower bound for selected aggregate win rate.
        #[arg(long, default_value_t = 0.0)]
        min_wilson_win_rate_lower: f64,
        /// Minimum aggregate total PnL.
        #[arg(long, default_value_t = 0.0)]
        min_total_pnl: f64,
        /// Minimum aggregate Sharpe-like score.
        #[arg(long, default_value_t = 0.0)]
        min_sharpe_like: f64,
        /// Maximum unresolved fills allowed in the selected aggregate variant.
        #[arg(long, default_value_t = 0)]
        max_unresolved_fills: usize,
        /// Maximum non-passive failed execution attempts allowed in the selected aggregate variant.
        #[arg(long, default_value_t = 0)]
        max_failed_fills: usize,
        /// Maximum passive maker non-fills/post-only rejects allowed in the selected aggregate variant.
        #[arg(long, default_value_t = 0)]
        max_passive_failed_fills: usize,
        /// Minimum selected aggregate fill rate across execution attempts.
        #[arg(long, default_value_t = 0.0)]
        min_fill_rate: f64,
        /// Maximum share of selected aggregate trades allowed from one timing zone.
        #[arg(long, default_value_t = 0.70)]
        max_zone_trade_share: f64,
        /// Minimum number of reports/windows required.
        #[arg(long, default_value_t = 3)]
        min_reports: usize,
        /// Minimum selected-variant profitable reports/windows required.
        #[arg(long, default_value_t = 2)]
        min_profitable_reports: usize,
        /// Minimum selected-variant trades required in each daily report.
        #[arg(long, default_value_t = 10)]
        min_daily_trades: usize,
        /// Minimum selected-variant PnL required in each daily report.
        #[arg(long, default_value_t = 0.0)]
        min_daily_pnl: f64,
        /// Optional selected-variant daily loss cap; 0 disables it.
        #[arg(long, default_value_t = 0.0)]
        max_daily_loss: f64,
        /// Permit promotion when any data manifest is incomplete.
        #[arg(long, default_value_t = false)]
        allow_incomplete_data: bool,
    },
    /// Promote by robust score, rejecting isolated parameter spikes and high-PBO searches.
    RobustPromote {
        /// Input JSON generated by harness or harness-sweep --report-json.
        /// Repeat once per independent window.
        #[arg(long, required = true)]
        report: Vec<String>,
        /// Output promotion artifact JSON path.
        #[arg(long)]
        output: String,
        /// Minimum aggregate trade count.
        #[arg(long, default_value_t = 90)]
        min_trades: usize,
        /// Minimum aggregate loss count, guarding against lossless tiny samples.
        #[arg(long, default_value_t = 1)]
        min_losses: usize,
        /// Minimum number of timing zones with at least one selected aggregate trade.
        #[arg(long, default_value_t = 2)]
        min_zone_count: usize,
        /// Minimum aggregate win rate, e.g. 0.52.
        #[arg(long, default_value_t = 0.0)]
        min_win_rate: f64,
        /// Minimum Wilson 95% lower bound for selected aggregate win rate.
        #[arg(long, default_value_t = 0.0)]
        min_wilson_win_rate_lower: f64,
        /// Minimum aggregate total PnL.
        #[arg(long, default_value_t = 0.0)]
        min_total_pnl: f64,
        /// Minimum aggregate Sharpe-like score.
        #[arg(long, default_value_t = 0.0)]
        min_sharpe_like: f64,
        /// Maximum unresolved fills allowed in the selected aggregate variant.
        #[arg(long, default_value_t = 0)]
        max_unresolved_fills: usize,
        /// Maximum non-passive failed execution attempts allowed in the selected aggregate variant.
        #[arg(long, default_value_t = 0)]
        max_failed_fills: usize,
        /// Maximum passive maker non-fills/post-only rejects allowed in the selected aggregate variant.
        #[arg(long, default_value_t = 0)]
        max_passive_failed_fills: usize,
        /// Minimum selected aggregate fill rate across execution attempts.
        #[arg(long, default_value_t = 0.0)]
        min_fill_rate: f64,
        /// Maximum share of selected aggregate trades allowed from one timing zone.
        #[arg(long, default_value_t = 0.70)]
        max_zone_trade_share: f64,
        /// Minimum number of reports/windows required.
        #[arg(long, default_value_t = 3)]
        min_reports: usize,
        /// Minimum selected-variant profitable reports/windows required.
        #[arg(long, default_value_t = 2)]
        min_profitable_reports: usize,
        /// Minimum selected-variant trades required in each daily report.
        #[arg(long, default_value_t = 10)]
        min_daily_trades: usize,
        /// Minimum selected-variant PnL required in each daily report.
        #[arg(long, default_value_t = 0.0)]
        min_daily_pnl: f64,
        /// Optional selected-variant daily loss cap; 0 disables it.
        #[arg(long, default_value_t = 0.0)]
        max_daily_loss: f64,
        /// Minimum count of nearby parameter variants required around the selected point.
        #[arg(long, default_value_t = 2)]
        min_neighbor_count: usize,
        /// Minimum active neighbor-window observations required around the selected point.
        #[arg(long, default_value_t = 0)]
        min_neighbor_observations: usize,
        /// Minimum share of neighbor-window observations with positive PnL.
        #[arg(long, default_value_t = 0.60)]
        min_neighbor_positive_rate: f64,
        /// Maximum estimated probability of backtest overfitting for the searched family.
        #[arg(long, default_value_t = 0.50)]
        max_pbo: f64,
        /// Minimum median out-of-sample percentile across combinatorial purged splits.
        #[arg(long, default_value_t = 0.0)]
        min_median_oos_percentile: f64,
        /// Minimum selected-variant PnL in its worst window.
        #[arg(long, default_value_t = 0.0)]
        min_worst_window_pnl: f64,
        /// Minimum robust score; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        min_robust_score: f64,
        /// Minimum aggregate profit factor; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        min_profit_factor: f64,
        /// Minimum aggregate average-win / average-loss payoff ratio; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        min_payoff_ratio: f64,
        /// Maximum aggregate worst-loss / average-win ratio; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        max_worst_loss_to_avg_win: f64,
        /// Minimum trades in a causal bucket before bucket-PnL veto applies; 0 disables.
        #[arg(long, default_value_t = 0)]
        min_causal_bucket_trades: u64,
        /// Minimum PnL required for causal buckets at/above --min-causal-bucket-trades.
        #[arg(long, default_value_t = 0.0)]
        min_causal_bucket_pnl: f64,
        /// Permit promotion when any data manifest is incomplete.
        #[arg(long, default_value_t = false)]
        allow_incomplete_data: bool,
    },
}

#[derive(Subcommand, Debug)]
enum DiagnosticsCommand {
    /// Analyze one session_*.jsonl file and print a machine-readable report.
    Session {
        /// Path to a session JSONL file.
        path: String,
    },
    /// Compare two session JSONLs for promotion identity and schema health.
    Compare {
        /// First session JSONL, typically paper.
        #[arg(long)]
        left: String,
        /// Second session JSONL, typically live or a later paper run.
        #[arg(long)]
        right: String,
    },
    /// Audit timestamp causality for signal, order, fill, and resolution events.
    Causality {
        /// Path to a session JSONL file.
        path: String,
        /// Maximum tolerated clock skew between related timestamps.
        #[arg(long, default_value_t = 0.5)]
        max_clock_skew_s: f64,
        /// Maximum tolerated fill delay after market end.
        #[arg(long, default_value_t = 0.0)]
        max_post_end_fill_s: f64,
        /// Minimum executable order timing records required.
        #[arg(long, default_value_t = 0)]
        min_order_timings: u64,
        /// Minimum resolution timing records required.
        #[arg(long, default_value_t = 0)]
        min_resolution_timings: u64,
    },
    /// Detect whether the deployed strategy is going stale from resolved outcomes.
    Staleness {
        /// Path to a session JSONL file.
        path: String,
        /// Minimum resolved outcomes before stale can become a hard verdict.
        #[arg(long, default_value_t = 30)]
        min_outcomes: usize,
        /// Minimum tail window used for change detection.
        #[arg(long, default_value_t = 10)]
        min_recent_window: usize,
        /// Minimum acceptable recent win rate.
        #[arg(long, default_value_t = 0.55)]
        min_recent_win_rate: f64,
        /// False-positive budget for the adaptive window drift test.
        #[arg(long, default_value_t = 0.01)]
        delta: f64,
    },
}

#[derive(Subcommand, Debug)]
enum TelegramCommand {
    /// Validate Telegram credentials and optionally register commands/send a card.
    Probe {
        /// Register /status, /stale, /preflight, /wallet, /help.
        #[arg(long, default_value_t = false)]
        set_commands: bool,
        /// Send the current status card to TELEGRAM_CHAT_ID.
        #[arg(long, default_value_t = false)]
        send_status: bool,
    },
    /// Send or print the current operator status card.
    Status {
        /// Use a specific soak report JSON instead of the newest one.
        #[arg(long)]
        soak_report: Option<String>,
        /// Send to TELEGRAM_CHAT_ID. Without this flag the card is printed.
        #[arg(long, default_value_t = false)]
        send: bool,
    },
    /// Run a read-only interactive long-poll loop for commands and buttons.
    Poll {
        /// Process one long-poll response and exit.
        #[arg(long, default_value_t = false)]
        once: bool,
        /// Long-poll timeout in seconds.
        #[arg(long, default_value_t = 30)]
        timeout_s: u64,
    },
}

#[derive(Subcommand, Debug)]
enum StrategyBuilderCommand {
    /// Print a reproducible strategy-builder plan using the current 5-minute candidate loop.
    Plan {
        /// Inclusive UTC start hour (RFC3339).
        #[arg(long)]
        start: String,
        /// Inclusive UTC end hour. Defaults to `start`.
        #[arg(long)]
        end: Option<String>,
        /// Output directory for reports, checkpoints, replay sessions, and promotion artifacts.
        #[arg(long, default_value = "logs/strategy_builder")]
        out_dir: String,
        /// PMXT v2 cache directory.
        #[arg(long)]
        cache_dir: Option<String>,
        /// BTC tick/kline CSV used as the virtual exchange price feed.
        #[arg(long)]
        btc_csv: Option<String>,
        /// Replay/backtest bankroll used for sizing.
        #[arg(long, default_value_t = 100.0)]
        bankroll: f64,
        /// Simulated insert latency in milliseconds.
        #[arg(long, default_value_t = 50)]
        latency_ms: u64,
        /// Variant-fan-out thread count for harness-sweep.
        #[arg(long, default_value_t = 0)]
        threads: usize,
        /// Candle frame length to isolate.
        #[arg(long, default_value_t = 5.0)]
        window_minutes: f64,
        /// Feed-forward fold length in inclusive UTC hours.
        #[arg(long, default_value_t = 24)]
        fold_hours: i64,
        /// Builder profile. Currently `guarded5m` is the production default.
        #[arg(long, default_value = "guarded5m")]
        profile: String,
        /// Restrict strategy-builder sweeps to one timing zone: all, early, primary, late, terminal.
        #[arg(long, default_value = "all")]
        zone_mode: String,
        /// Override promotion artifact output path.
        #[arg(long)]
        promotion_output: Option<String>,
    },
    /// Audit experiment reports, a promotion artifact, and replay/paper sessions as one gate.
    Audit {
        /// Input JSON generated by harness or harness-sweep --report-json.
        #[arg(long)]
        report: Vec<String>,
        /// Diagnostic JSON generated by harness-sweep with --adaptive-health-rearm-minutes.
        #[arg(long)]
        adaptive_report: Vec<String>,
        /// Promotion artifact JSON selected for paper/live.
        #[arg(long)]
        promotion_artifact: Option<String>,
        /// Replay or paper session JSONL to validate. Repeat for multiple windows.
        #[arg(long)]
        replay_session: Vec<String>,
        /// Minimum aggregate trade count.
        #[arg(long, default_value_t = 750)]
        min_trades: usize,
        /// Minimum win rate.
        #[arg(long, default_value_t = 0.63)]
        min_win_rate: f64,
        /// Minimum Wilson 95% lower bound for win rate.
        #[arg(long, default_value_t = 0.60)]
        min_wilson_win_rate_lower: f64,
        /// Minimum total PnL.
        #[arg(long, default_value_t = 250.0)]
        min_total_pnl: f64,
        /// Minimum shadow resolutions expected in each replay session.
        #[arg(long, default_value_t = 1)]
        min_shadow_resolutions: u64,
        /// Minimum daily/holdout research reports required for an A+ audit.
        #[arg(long, default_value_t = 3)]
        min_research_reports: usize,
        /// Minimum replay or paper sessions required for an A+ audit.
        #[arg(long, default_value_t = 1)]
        min_replay_sessions: usize,
        /// Minimum shadow resolutions per replay/paper session required for A+.
        #[arg(long, default_value_t = 50)]
        a_plus_min_shadow_resolutions: u64,
    },
    /// Search causal bucket selectivity rules with feed-forward OOS scoring.
    SelectivitySearch {
        /// Input JSON generated by harness or harness-sweep --report-json.
        #[arg(long, required = true, num_args = 1..)]
        report: Vec<String>,
        /// Write the JSON search artifact to this path.
        #[arg(long)]
        output: Option<String>,
        /// Minimum prior reports required before a rule can score the next fold.
        #[arg(long, default_value_t = 2)]
        min_train_reports: usize,
        /// Minimum prior trades required before a rule can score the next fold.
        #[arg(long, default_value_t = 20)]
        min_train_trades: u64,
        /// Minimum feed-forward OOS trades required to pass.
        #[arg(long, default_value_t = 30)]
        min_oos_trades: u64,
        /// Minimum Wilson 95% lower bound on feed-forward OOS win rate.
        #[arg(long, default_value_t = 0.60)]
        min_oos_wilson_win_rate_lower: f64,
        /// Minimum feed-forward OOS total PnL.
        #[arg(long, default_value_t = 0.0)]
        min_oos_total_pnl: f64,
        /// Minimum number of profitable OOS reports.
        #[arg(long, default_value_t = 1)]
        min_oos_profitable_reports: usize,
        /// Minimum worst OOS report PnL.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        min_worst_oos_pnl: f64,
        /// Show top N candidates.
        #[arg(long, default_value_t = 25)]
        top: usize,
    },
    /// Execute a storage-bounded rolling PMXT backtest loop, one fold cache at a time.
    RollingHistory {
        /// Inclusive UTC start hour (RFC3339).
        #[arg(long)]
        start: String,
        /// Inclusive UTC end hour (RFC3339).
        #[arg(long)]
        end: String,
        /// Output directory for compact reports, artifacts, and run manifest.
        #[arg(long)]
        out_dir: String,
        /// Root for per-fold temporary PMXT caches. Defaults to <out-dir>/cache.
        #[arg(long)]
        cache_root: Option<String>,
        /// BTC tick/kline CSV used as the virtual exchange price feed.
        #[arg(long)]
        btc_csv: Option<String>,
        /// Replay/backtest bankroll used for sizing.
        #[arg(long, default_value_t = 100.0)]
        bankroll: f64,
        /// Simulated insert latency in milliseconds.
        #[arg(long, default_value_t = 50)]
        latency_ms: u64,
        /// Variant-fan-out thread count for harness-sweep.
        #[arg(long, default_value_t = 0)]
        threads: usize,
        /// Candle frame length to isolate.
        #[arg(long, default_value_t = 5.0)]
        window_minutes: f64,
        /// Fold length in inclusive UTC hours.
        #[arg(long, default_value_t = 8)]
        fold_hours: i64,
        /// Limit number of folds for bounded smoke runs.
        #[arg(long)]
        max_folds: Option<usize>,
        /// Rolling lab profile, e.g. a_plus5m, a_plus5m_causal_guard, or a_plus5m_reversion_guard.
        #[arg(long, default_value = "a_plus5m")]
        profile: String,
        /// Require causal decision tags in each fold's harness-sweep, e.g. direction=down.
        #[arg(long)]
        require_causal_tag: Vec<String>,
        /// Deny causal decision tags in each fold's harness-sweep, e.g. direction=up.
        #[arg(long)]
        deny_causal_tag: Vec<String>,
        /// Restrict sweeps to one timing zone: all, early, primary, late, terminal.
        #[arg(long, default_value = "early")]
        zone_mode: String,
        /// Override promotion artifact output path.
        #[arg(long)]
        promotion_output: Option<String>,
        /// Execute the generated loop. Without this flag, prints a dry-run manifest.
        #[arg(long, default_value_t = false)]
        execute: bool,
        /// Delete each per-fold cache after its compact report is written.
        #[arg(long, default_value_t = false)]
        delete_after_process: bool,
        /// Within each fold, keep at most one downloaded raw PMXT parquet at a time.
        #[arg(long, default_value_t = false)]
        atomic_parquet: bool,
        /// Probe PMXT archive-hour availability before generating folds.
        #[arg(long, default_value_t = false)]
        preflight_pmxt_hours: bool,
        /// With PMXT preflight, stop at the first missing hour instead of failing.
        #[arg(long, default_value_t = false)]
        stop_at_first_missing_hour: bool,
        /// Drop a final fold that is shorter than --fold-hours.
        #[arg(long, default_value_t = false)]
        require_full_folds: bool,
        /// Minimum trades required in every fold during robust promotion.
        #[arg(long, default_value_t = 20)]
        min_fold_trades: usize,
        /// Minimum target PMXT events required in each fold before treating it as strategy evidence. 0 disables.
        #[arg(long, default_value_t = 1)]
        min_fold_target_events: u64,
        /// Minimum top-variant trades required in each fold before treating it as strategy evidence. Defaults to --min-fold-trades; 0 disables.
        #[arg(long)]
        min_fold_top_trades: Option<usize>,
        /// Minimum aggregate trades required by robust-promote. Defaults to --min-fold-trades × fold count.
        #[arg(long)]
        min_promotion_trades: Option<usize>,
        /// Minimum trades per report required by robust-promote. Defaults to --min-fold-trades.
        #[arg(long)]
        min_promotion_daily_trades: Option<usize>,
        /// Minimum profitable reports required by robust-promote. Defaults to fold count.
        #[arg(long)]
        min_promotion_profitable_reports: Option<usize>,
        /// Minimum aggregate losses required by robust-promote. Defaults to 5.
        #[arg(long)]
        min_promotion_losses: Option<usize>,
        /// Abort before the next fold if cache-root size exceeds this budget. 0 disables.
        #[arg(long, default_value_t = 0.0)]
        max_cache_gb: f64,
        /// Minimum active neighbor-window observations passed to robust-promote.
        #[arg(long)]
        min_neighbor_observations: Option<usize>,
        /// Minimum neighbor positive rate passed to robust-promote.
        #[arg(long, default_value_t = 0.60)]
        min_neighbor_positive_rate: f64,
        /// Maximum PBO passed to robust-promote.
        #[arg(long, default_value_t = 0.50)]
        max_pbo: f64,
        /// Minimum median OOS percentile passed to robust-promote.
        #[arg(long, default_value_t = 0.80)]
        min_median_oos_percentile: f64,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_tracing(&cli.log);
    let settings = config::Settings::from_env();

    match cli.command {
        Command::Live {
            mode,
            i_understand_live,
            promotion_artifact,
            allow_stale_research_artifact,
        } => {
            let mut settings = settings.clone();
            apply_promotion_override(&mut settings, promotion_artifact);
            apply_stale_research_override(&mut settings, allow_stale_research_artifact);
            let preflight = run_startup_preflight(&settings, mode, i_understand_live).await;
            if !preflight.ok {
                eprintln!("preflight failed: {}", preflight.failure_summary());
                std::process::exit(2);
            }
            let m = live::pipeline::Mode::from_runtime_mode(mode);
            let pipeline = live::pipeline::Pipeline::new(settings.clone(), m).await;
            match pipeline {
                Ok(p) => {
                    install_signal_handlers(p.stop_token());
                    if let Err(e) = p.run().await {
                        tracing::error!(error = %e, "pipeline exited with error");
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("pipeline init failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        Command::LiveReplay {
            start,
            end,
            cache_dir,
            btc_csv,
            bankroll,
            latency_ms,
            session_log_dir,
            allow_download,
            delete_after_process,
            allow_gamma_fetch,
            max_contracts,
            window_minutes,
            promotion_artifact,
            settlement_alignment_ready,
            report_json,
        } => {
            let mut settings = settings.clone();
            apply_promotion_override(&mut settings, promotion_artifact);
            if settlement_alignment_ready {
                settings.candle_settlement_alignment_ready = true;
            }
            cmd_live_replay(
                &settings,
                &start,
                end.as_deref(),
                cache_dir.as_deref(),
                btc_csv.as_deref(),
                bankroll,
                latency_ms,
                session_log_dir.as_deref(),
                allow_download,
                delete_after_process,
                allow_gamma_fetch,
                max_contracts,
                window_minutes,
                report_json.as_deref(),
            )
            .await;
        }
        Command::Preflight {
            mode,
            i_understand_live,
            promotion_artifact,
            allow_stale_research_artifact,
        } => {
            let mut settings = settings.clone();
            apply_promotion_override(&mut settings, promotion_artifact);
            apply_stale_research_override(&mut settings, allow_stale_research_artifact);
            let report = run_startup_preflight(&settings, mode, i_understand_live).await;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serialize preflight report")
            );
            if !report.ok {
                std::process::exit(2);
            }
        }
        Command::ReleaseManifest {
            mode,
            promotion_artifact,
        } => {
            let mut settings = settings.clone();
            apply_promotion_override(&mut settings, promotion_artifact);
            let manifest = release::ReleaseManifest::capture(&settings, mode);
            println!(
                "{}",
                serde_json::to_string_pretty(&manifest).expect("serialize release manifest")
            );
        }
        Command::Scan {
            max_hours,
            min_liquidity,
        } => {
            cmd_scan(&settings, max_hours, min_liquidity).await;
        }
        Command::Wallet { json } => cmd_wallet(&settings, json).await,
        Command::Clob { command } => cmd_clob(&settings, command).await,
        Command::Experiment { command } => cmd_experiment(command),
        Command::Diagnostics { command } => cmd_diagnostics(command),
        Command::Telegram { command } => cmd_telegram(&settings, command).await,
        Command::StrategyBuilder { command } => cmd_strategy_builder(command).await,
        Command::Ctf { condition_id } => cmd_ctf(&settings, &condition_id).await,
        Command::ValidateReplay { path } => cmd_validate_replay(&path).await,
        Command::Sweep {
            session,
            bankroll,
            position_pct,
            max_per_market_usd,
            min_trades,
            zones,
            grid,
            conf,
            z,
            edge,
            ev_buffer,
            min_price,
            max_price,
            settlement_cutoff_minutes,
            settlement_floor,
            settlement_guard_minutes,
            settlement_sigma_buffer,
            max_reversion_count,
            micro_max_spread,
            micro_min_depth,
            micro_min_pressure,
            also_maker,
            zone_mode,
            top,
            report_json,
        } => {
            let grid_config = if grid {
                let Some(zone_mode) = sweep::strategy::ZoneMode::parse(&zone_mode) else {
                    eprintln!("--zone-mode must be one of: all, early, primary, late, terminal");
                    std::process::exit(2);
                };
                Some(sweep::strategy::GridConfig {
                    conf: parse_csv_floats(&conf),
                    z: parse_csv_floats(&z),
                    edge: parse_csv_floats(&edge),
                    ev_buffer: parse_csv_floats(&ev_buffer),
                    min_price: parse_csv_floats(&min_price),
                    max_price: parse_csv_floats(&max_price),
                    settlement_cutoff_minutes: parse_csv_floats(&settlement_cutoff_minutes),
                    settlement_min_abs_move_usd: parse_csv_floats(&settlement_floor),
                    settlement_guard_minutes: parse_csv_floats(&settlement_guard_minutes),
                    settlement_sigma_buffer: parse_csv_floats(&settlement_sigma_buffer),
                    max_reversion_count: parse_csv_u64s(&max_reversion_count),
                    micro_max_spread: parse_csv_floats(&micro_max_spread),
                    micro_min_depth: parse_csv_floats(&micro_min_depth),
                    micro_min_pressure: parse_csv_floats(&micro_min_pressure),
                    also_maker,
                    zone_mode,
                })
            } else {
                None
            };
            cmd_sweep(
                &session,
                bankroll,
                position_pct,
                max_per_market_usd,
                min_trades,
                zones,
                grid_config,
                top,
                report_json.as_deref(),
            );
        }
        Command::EvalCache {
            start,
            end,
            cache_dir,
            btc_csv,
            output,
            window_minutes,
            allow_gamma_fetch,
        } => {
            cmd_eval_cache(
                &settings,
                &start,
                end.as_deref(),
                cache_dir.as_deref(),
                btc_csv.as_deref(),
                &output,
                window_minutes,
                allow_gamma_fetch,
            )
            .await;
        }
        Command::PmxtInfo {
            hour,
            cache_dir,
            sample,
        } => {
            cmd_pmxt_info(&hour, cache_dir.as_deref(), sample).await;
        }
        Command::PmxtDownload {
            start,
            end,
            cache_dir,
        } => {
            cmd_pmxt_download(&start, end.as_deref(), cache_dir.as_deref()).await;
        }
        Command::Distill {
            input,
            output,
            candle_cids,
            hour,
        } => {
            cmd_distill(
                &settings,
                &input,
                output.as_deref(),
                candle_cids.as_deref(),
                hour.as_deref(),
            )
            .await;
        }
        Command::HarnessSweep {
            start,
            end,
            bankroll,
            cache_dir,
            btc_csv,
            latency_ms,
            conf,
            z,
            edge,
            ev_buffer,
            min_price,
            max_price,
            settlement_cutoff_minutes,
            settlement_floor,
            settlement_guard_minutes,
            settlement_sigma_buffer,
            max_reversion_count,
            min_reversion_count,
            micro_max_spread,
            micro_min_depth,
            micro_min_pressure,
            position_pct,
            max_per_market_usd,
            max_total_exposure_usd,
            max_projected_stressed_drawdown_pct,
            degraded_after_losses,
            degraded_after_drawdown_pct,
            degraded_min_z,
            degraded_max_price,
            degraded_force_taker,
            also_maker,
            maker_only,
            zone_mode,
            taker_only,
            top,
            threads,
            checkpoint,
            resume,
            report_json,
            trades_json,
            trade_features_json,
            require_causal_tag,
            deny_causal_tag,
            window_minutes,
            adaptive_health_rearm_minutes,
            continuous,
            atomic_parquet,
        } => {
            let conf = parse_csv_floats(&conf);
            let zs = parse_csv_floats(&z);
            let edges = parse_csv_floats(&edge);
            let evs = parse_csv_floats(&ev_buffer);
            let min_prices = parse_csv_floats(&min_price);
            let max_prices = parse_csv_floats(&max_price);
            let settlement_cutoffs = parse_csv_floats(&settlement_cutoff_minutes);
            let settlement_floors = parse_csv_floats(&settlement_floor);
            let settlement_guards = parse_csv_floats(&settlement_guard_minutes);
            let settlement_sigmas = parse_csv_floats(&settlement_sigma_buffer);
            let max_reversion_count = parse_csv_u64s(&max_reversion_count);
            let min_reversion_count = parse_csv_u64s(&min_reversion_count);
            let micro_spreads = parse_csv_floats(&micro_max_spread);
            let micro_depths = parse_csv_floats(&micro_min_depth);
            let micro_pressures = parse_csv_floats(&micro_min_pressure);
            let stress_drawdown_caps = parse_csv_floats(&max_projected_stressed_drawdown_pct);
            let degraded_after_losses = parse_csv_u64s(&degraded_after_losses);
            let degraded_drawdowns = parse_csv_floats(&degraded_after_drawdown_pct);
            let degraded_min_z = parse_csv_floats(&degraded_min_z);
            let degraded_max_price = parse_csv_floats(&degraded_max_price);
            let Some(zone_mode) = backtest::sweep::ZoneMode::parse(&zone_mode) else {
                eprintln!("--zone-mode must be one of: all, early, primary, late, terminal");
                std::process::exit(2);
            };
            cmd_harness_sweep(
                &settings,
                &start,
                end.as_deref(),
                bankroll,
                cache_dir.as_deref(),
                btc_csv.as_deref(),
                latency_ms,
                conf,
                zs,
                edges,
                evs,
                min_prices,
                max_prices,
                settlement_cutoffs,
                settlement_floors,
                settlement_guards,
                settlement_sigmas,
                max_reversion_count,
                min_reversion_count,
                micro_spreads,
                micro_depths,
                micro_pressures,
                position_pct,
                max_per_market_usd,
                max_total_exposure_usd.unwrap_or(settings.max_total_exposure_usd),
                stress_drawdown_caps,
                degraded_after_losses,
                degraded_drawdowns,
                degraded_min_z,
                degraded_max_price,
                degraded_force_taker,
                also_maker,
                maker_only,
                zone_mode,
                taker_only,
                top,
                threads,
                checkpoint.as_deref(),
                resume,
                report_json.as_deref(),
                trades_json.as_deref(),
                trade_features_json.as_deref(),
                require_causal_tag,
                deny_causal_tag,
                window_minutes,
                adaptive_health_rearm_minutes,
                continuous,
                atomic_parquet,
            )
            .await;
        }
        Command::Harness {
            start,
            end,
            bankroll,
            max_total_exposure_usd,
            cache_dir,
            btc_csv,
            latency_ms,
            threads,
            checkpoint,
            resume,
            max_contracts,
            window_minutes,
            allow_gamma_fetch,
            metadata_only,
            report_json,
            adaptive_health_rearm_minutes,
            continuous,
            atomic_parquet,
        } => {
            cmd_harness(
                &settings,
                &start,
                end.as_deref(),
                bankroll,
                max_total_exposure_usd.unwrap_or(settings.max_total_exposure_usd),
                cache_dir.as_deref(),
                btc_csv.as_deref(),
                latency_ms,
                threads,
                checkpoint.as_deref(),
                resume,
                max_contracts,
                window_minutes,
                allow_gamma_fetch,
                metadata_only,
                report_json.as_deref(),
                adaptive_health_rearm_minutes,
                continuous,
                atomic_parquet,
            )
            .await;
        }
        Command::SelfTest => {
            println!("self-test: this binary's tests run via `cargo test`. ok.");
        }
    }
}

fn apply_promotion_override(settings: &mut config::Settings, path: Option<String>) {
    if let Some(path) = path {
        settings.promotion_artifact_path = path;
    }
}

fn apply_stale_research_override(settings: &mut config::Settings, allow: bool) {
    if allow {
        settings.allow_stale_research_artifact = true;
    }
}

async fn cmd_strategy_builder(command: StrategyBuilderCommand) {
    match command {
        StrategyBuilderCommand::Plan {
            start,
            end,
            out_dir,
            cache_dir,
            btc_csv,
            bankroll,
            latency_ms,
            threads,
            window_minutes,
            fold_hours,
            profile,
            zone_mode,
            promotion_output,
        } => {
            let plan =
                match strategy_builder::build_plan(strategy_builder::StrategyBuilderPlanInput {
                    start,
                    end,
                    out_dir: std::path::PathBuf::from(out_dir),
                    cache_dir,
                    btc_csv,
                    bankroll,
                    latency_ms,
                    threads,
                    window_minutes,
                    fold_hours,
                    profile,
                    zone_mode,
                    promotion_output,
                }) {
                    Ok(plan) => plan,
                    Err(e) => {
                        eprintln!("strategy-builder plan failed: {e:#}");
                        std::process::exit(2);
                    }
                };
            println!(
                "{}",
                serde_json::to_string_pretty(&plan).expect("serialize strategy-builder plan")
            );
        }
        StrategyBuilderCommand::Audit {
            report,
            adaptive_report,
            promotion_artifact,
            replay_session,
            min_trades,
            min_win_rate,
            min_wilson_win_rate_lower,
            min_total_pnl,
            min_shadow_resolutions,
            min_research_reports,
            min_replay_sessions,
            a_plus_min_shadow_resolutions,
        } => {
            let audit = strategy_builder::audit(strategy_builder::StrategyBuilderAuditInput {
                report_paths: report,
                adaptive_report_paths: adaptive_report,
                promotion_artifact,
                replay_sessions: replay_session,
                min_trades,
                min_win_rate,
                min_wilson_win_rate_lower,
                min_total_pnl,
                min_shadow_resolutions,
                min_research_reports,
                min_replay_sessions,
                a_plus_min_shadow_resolutions,
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&audit).expect("serialize strategy-builder audit")
            );
            if !audit.ok {
                std::process::exit(2);
            }
        }
        StrategyBuilderCommand::SelectivitySearch {
            report,
            output,
            min_train_reports,
            min_train_trades,
            min_oos_trades,
            min_oos_wilson_win_rate_lower,
            min_oos_total_pnl,
            min_oos_profitable_reports,
            min_worst_oos_pnl,
            top,
        } => {
            let search = match strategy_builder::selectivity_search(
                strategy_builder::StrategyBuilderSelectivitySearchInput {
                    report_paths: report,
                    min_train_reports,
                    min_train_trades,
                    min_oos_trades,
                    min_oos_wilson_win_rate_lower,
                    min_oos_total_pnl,
                    min_oos_profitable_reports,
                    min_worst_oos_pnl,
                    top,
                },
            ) {
                Ok(search) => search,
                Err(e) => {
                    eprintln!("strategy-builder selectivity-search failed: {e:#}");
                    std::process::exit(2);
                }
            };
            let json = serde_json::to_string_pretty(&search)
                .expect("serialize strategy-builder selectivity search");
            if let Some(output) = output {
                let path = std::path::PathBuf::from(output);
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        if let Err(e) = std::fs::create_dir_all(parent) {
                            eprintln!(
                                "strategy-builder selectivity-search output mkdir failed: {e:#}"
                            );
                            std::process::exit(2);
                        }
                    }
                }
                let file_name = path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "selectivity_search.json".to_string());
                let tmp_path =
                    path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
                if let Err(e) = std::fs::write(&tmp_path, format!("{json}\n")) {
                    eprintln!("strategy-builder selectivity-search output write failed: {e:#}");
                    std::process::exit(2);
                }
                if let Err(e) = std::fs::rename(&tmp_path, &path) {
                    eprintln!("strategy-builder selectivity-search output rename failed: {e:#}");
                    std::process::exit(2);
                }
            }
            println!("{json}");
        }
        StrategyBuilderCommand::RollingHistory {
            start,
            end,
            out_dir,
            cache_root,
            btc_csv,
            bankroll,
            latency_ms,
            threads,
            window_minutes,
            fold_hours,
            max_folds,
            profile,
            require_causal_tag,
            deny_causal_tag,
            zone_mode,
            promotion_output,
            execute,
            delete_after_process,
            atomic_parquet,
            preflight_pmxt_hours,
            stop_at_first_missing_hour,
            require_full_folds,
            min_fold_trades,
            min_fold_target_events,
            min_fold_top_trades,
            min_promotion_trades,
            min_promotion_daily_trades,
            min_promotion_profitable_reports,
            min_promotion_losses,
            max_cache_gb,
            min_neighbor_observations,
            min_neighbor_positive_rate,
            max_pbo,
            min_median_oos_percentile,
        } => {
            let input = RollingHistoryInput {
                start,
                end,
                out_dir: std::path::PathBuf::from(out_dir),
                cache_root: cache_root.map(std::path::PathBuf::from),
                btc_csv,
                bankroll,
                latency_ms,
                threads,
                window_minutes,
                fold_hours,
                max_folds,
                profile,
                require_causal_tag,
                deny_causal_tag,
                zone_mode,
                promotion_output: promotion_output.map(std::path::PathBuf::from),
                execute,
                delete_after_process,
                atomic_parquet,
                preflight_pmxt_hours,
                stop_at_first_missing_hour,
                require_full_folds,
                min_fold_trades,
                min_fold_target_events,
                min_fold_top_trades,
                min_promotion_trades,
                min_promotion_daily_trades,
                min_promotion_profitable_reports,
                min_promotion_losses,
                max_cache_gb,
                min_neighbor_observations,
                min_neighbor_positive_rate,
                max_pbo,
                min_median_oos_percentile,
            };
            match run_rolling_history(input).await {
                Ok(summary) => println!(
                    "{}",
                    serde_json::to_string_pretty(&summary)
                        .expect("serialize rolling history summary")
                ),
                Err(e) => {
                    eprintln!("rolling-history failed: {e:#}");
                    std::process::exit(2);
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RollingHistoryInput {
    start: String,
    end: String,
    out_dir: std::path::PathBuf,
    cache_root: Option<std::path::PathBuf>,
    btc_csv: Option<String>,
    bankroll: f64,
    latency_ms: u64,
    threads: usize,
    window_minutes: f64,
    fold_hours: i64,
    max_folds: Option<usize>,
    profile: String,
    require_causal_tag: Vec<String>,
    deny_causal_tag: Vec<String>,
    zone_mode: String,
    promotion_output: Option<std::path::PathBuf>,
    execute: bool,
    delete_after_process: bool,
    preflight_pmxt_hours: bool,
    stop_at_first_missing_hour: bool,
    require_full_folds: bool,
    min_fold_trades: usize,
    min_fold_target_events: u64,
    min_fold_top_trades: Option<usize>,
    min_promotion_trades: Option<usize>,
    min_promotion_daily_trades: Option<usize>,
    min_promotion_profitable_reports: Option<usize>,
    min_promotion_losses: Option<usize>,
    max_cache_gb: f64,
    min_neighbor_observations: Option<usize>,
    min_neighbor_positive_rate: f64,
    max_pbo: f64,
    min_median_oos_percentile: f64,
    atomic_parquet: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RollingHistoryProfile {
    name: String,
    conf: String,
    z: String,
    edge: String,
    ev_buffer: String,
    min_price: String,
    max_price: String,
    settlement_cutoff_minutes: String,
    settlement_floor: String,
    settlement_guard_minutes: String,
    settlement_sigma_buffer: String,
    min_reversion_count: String,
    max_reversion_count: String,
    micro_max_spread: String,
    micro_min_depth: String,
    micro_min_pressure: String,
    position_pct: String,
    max_per_market_usd: String,
    max_total_exposure_usd: String,
    max_projected_stressed_drawdown_pct: String,
    degraded_after_losses: String,
    degraded_after_drawdown_pct: String,
    degraded_min_z: String,
    degraded_max_price: String,
    degraded_force_taker: bool,
    taker_only: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RollingFoldSummary {
    index: usize,
    start: String,
    end: String,
    cache_dir: String,
    hydrate_report: String,
    sweep_report: String,
    hydrate_args: Vec<String>,
    sweep_args: Vec<String>,
    cache_deleted: bool,
    coverage: Option<RollingFoldCoverage>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RollingFoldCoverage {
    status: String,
    reason: Option<String>,
    target_events: u64,
    target_events_per_hour: f64,
    top_trades: usize,
    top_variant: Option<String>,
    top_variant_pnl: Option<f64>,
    min_target_events: u64,
    min_top_trades: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RollingCoveragePolicy {
    min_fold_target_events: u64,
    min_fold_top_trades: usize,
}

async fn run_rolling_history(input: RollingHistoryInput) -> anyhow::Result<serde_json::Value> {
    use anyhow::{bail, Context};
    use chrono::{DateTime, Duration as ChronoDuration, Utc};

    let start = DateTime::parse_from_rfc3339(&input.start)
        .context("--start must be RFC3339")?
        .with_timezone(&Utc);
    let requested_end = DateTime::parse_from_rfc3339(&input.end)
        .context("--end must be RFC3339")?
        .with_timezone(&Utc);
    if requested_end < start {
        bail!("--end must be >= --start");
    }
    if input.fold_hours <= 0 {
        bail!("--fold-hours must be > 0");
    }
    if input.min_fold_trades == 0 {
        bail!("--min-fold-trades must be > 0");
    }
    let min_fold_top_trades = input.min_fold_top_trades.unwrap_or(input.min_fold_trades);
    if input.window_minutes <= 0.0 {
        bail!("--window-minutes must be > 0");
    }
    if input.max_cache_gb < 0.0 || !input.max_cache_gb.is_finite() {
        bail!("--max-cache-gb must be finite and non-negative");
    }

    let profile = rolling_history_profile(&input.profile)?;
    let zone_mode = backtest::sweep::ZoneMode::parse(&input.zone_mode)
        .with_context(|| format!("unknown --zone-mode `{}`", input.zone_mode))?;
    let out_dir = input.out_dir;
    let cache_root = input.cache_root.unwrap_or_else(|| out_dir.join("cache"));
    let reports_dir = out_dir.join("reports");
    let hydrate_dir = out_dir.join("hydrate_reports");
    let promotions_dir = out_dir.join("promotions");
    let preflight_enabled = input.preflight_pmxt_hours || input.stop_at_first_missing_hour;
    let mut end = requested_end;
    let mut archive_preflight = serde_json::Value::Null;
    if preflight_enabled {
        let (effective_end, preflight) = preflight_pmxt_hours(
            start,
            requested_end,
            &cache_root,
            input.stop_at_first_missing_hour,
        )
        .await?;
        end = effective_end;
        archive_preflight = preflight;
    }
    let mut partial_final_fold_dropped = false;
    if input.require_full_folds {
        let available_hours = (end - start).num_hours() + 1;
        let full_hours = (available_hours / input.fold_hours) * input.fold_hours;
        if full_hours <= 0 {
            bail!(
                "no complete {}h fold available between {} and {}",
                input.fold_hours,
                start.to_rfc3339(),
                end.to_rfc3339()
            );
        }
        let full_fold_end = start + ChronoDuration::hours(full_hours - 1);
        if full_fold_end < end {
            partial_final_fold_dropped = true;
            end = full_fold_end;
        }
    }
    let promotion_output = input.promotion_output.unwrap_or_else(|| {
        promotions_dir.join(format!(
            "rolling_{}_{}_robust.json",
            start.format("%Y%m%dT%H%M%SZ"),
            end.format("%Y%m%dT%H%M%SZ")
        ))
    });

    let mut folds = Vec::new();
    let mut fold_start = start;
    let fold_span = ChronoDuration::hours(input.fold_hours);
    while fold_start <= end {
        let fold_end = (fold_start + fold_span - ChronoDuration::hours(1)).min(end);
        folds.push((fold_start, fold_end));
        fold_start = fold_end + ChronoDuration::hours(1);
        if let Some(max_folds) = input.max_folds {
            if folds.len() >= max_folds {
                break;
            }
        }
    }
    if folds.is_empty() {
        bail!("no folds generated");
    }

    let exe = std::env::current_exe().context("locate current executable")?;
    let mut fold_summaries = Vec::new();
    let mut sweep_reports = Vec::new();
    let coverage_policy = RollingCoveragePolicy {
        min_fold_target_events: input.min_fold_target_events,
        min_fold_top_trades,
    };

    let build_summary = |promotion_status: &str,
                         promotion_error: Option<String>,
                         fold_summaries: &[RollingFoldSummary],
                         promote_args: &[String]| {
        serde_json::json!({
            "schema_version": 1,
            "mode": if input.execute { "executed" } else { "dry_run" },
            "promotion_status": promotion_status,
            "promotion_error": promotion_error,
            "profile": profile,
            "fold_hours": input.fold_hours,
            "min_fold_trades": input.min_fold_trades,
            "coverage_policy": coverage_policy,
            "promotion_policy": {
                "min_trades": input
                    .min_promotion_trades
                    .unwrap_or(input.min_fold_trades * fold_summaries.len()),
                "min_daily_trades": input
                    .min_promotion_daily_trades
                    .unwrap_or(input.min_fold_trades),
                "min_profitable_reports": input
                    .min_promotion_profitable_reports
                    .unwrap_or(fold_summaries.len()),
                "min_losses": input.min_promotion_losses.unwrap_or(5),
                "min_neighbor_observations": input.min_neighbor_observations.unwrap_or(0),
            },
            "window_minutes": input.window_minutes,
            "delete_after_process": input.delete_after_process,
            "atomic_parquet": input.atomic_parquet,
            "require_full_folds": input.require_full_folds,
            "partial_final_fold_dropped": partial_final_fold_dropped,
            "requested_start": start.to_rfc3339(),
            "requested_end": requested_end.to_rfc3339(),
            "effective_end": end.to_rfc3339(),
            "archive_preflight": archive_preflight,
            "cache_root": cache_root.display().to_string(),
            "out_dir": out_dir.display().to_string(),
            "folds": fold_summaries,
            "promotion_output": promotion_output.display().to_string(),
            "promotion_args": promote_args,
            "storage_policy": if input.atomic_parquet {
                "per-fold cache is session-owned; atomic_parquet downloads one raw PMXT hour at a time and deletes only parquets downloaded by this process after replay; delete_after_process removes fold_* dirs under cache_root after report write"
            } else {
                "per-fold cache is session-owned; delete_after_process removes only fold_* dirs under cache_root after report write"
            },
        })
    };

    for (idx, (fold_start, fold_end)) in folds.iter().copied().enumerate() {
        let mut coverage = None;
        if input.execute {
            std::fs::create_dir_all(&reports_dir)
                .with_context(|| format!("create {}", reports_dir.display()))?;
            std::fs::create_dir_all(&hydrate_dir)
                .with_context(|| format!("create {}", hydrate_dir.display()))?;
            std::fs::create_dir_all(&promotions_dir)
                .with_context(|| format!("create {}", promotions_dir.display()))?;
            std::fs::create_dir_all(&cache_root)
                .with_context(|| format!("create {}", cache_root.display()))?;
            enforce_cache_budget(&cache_root, input.max_cache_gb)?;
        }

        let stamp = format!(
            "{}_{}",
            fold_start.format("%Y%m%dT%H%M%SZ"),
            fold_end.format("%Y%m%dT%H%M%SZ")
        );
        let fold_cache = cache_root.join(format!("fold_{:03}_{stamp}", idx + 1));
        let hydrate_report = hydrate_dir.join(format!("fold_{:03}_{stamp}_hydrate.json", idx + 1));
        let sweep_report = reports_dir.join(format!("fold_{:03}_{stamp}_sweep.json", idx + 1));

        let mut hydrate_args = vec![
            "harness".to_string(),
            "--start".to_string(),
            fold_start.to_rfc3339(),
            "--end".to_string(),
            fold_end.to_rfc3339(),
            "--cache-dir".to_string(),
            fold_cache.display().to_string(),
            "--bankroll".to_string(),
            cli_money_arg(input.bankroll),
            "--max-total-exposure-usd".to_string(),
            profile.max_total_exposure_usd.clone(),
            "--latency-ms".to_string(),
            input.latency_ms.to_string(),
            "--threads".to_string(),
            input.threads.to_string(),
            "--max-contracts".to_string(),
            "1".to_string(),
            "--window-minutes".to_string(),
            cli_float_arg(input.window_minutes),
            "--allow-gamma-fetch".to_string(),
            "--metadata-only".to_string(),
            "--continuous".to_string(),
            "--report-json".to_string(),
            hydrate_report.display().to_string(),
        ];
        if input.atomic_parquet {
            hydrate_args.push("--atomic-parquet".to_string());
        }
        if let Some(btc_csv) = &input.btc_csv {
            hydrate_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
        }

        let mut sweep_args = vec![
            "harness-sweep".to_string(),
            "--start".to_string(),
            fold_start.to_rfc3339(),
            "--end".to_string(),
            fold_end.to_rfc3339(),
            "--cache-dir".to_string(),
            fold_cache.display().to_string(),
            "--bankroll".to_string(),
            cli_money_arg(input.bankroll),
            "--position-pct".to_string(),
            profile.position_pct.clone(),
            "--max-total-exposure-usd".to_string(),
            profile.max_total_exposure_usd.clone(),
            "--max-per-market-usd".to_string(),
            profile.max_per_market_usd.clone(),
            "--max-projected-stressed-drawdown-pct".to_string(),
            profile.max_projected_stressed_drawdown_pct.clone(),
            "--degraded-after-losses".to_string(),
            profile.degraded_after_losses.clone(),
            "--degraded-after-drawdown-pct".to_string(),
            profile.degraded_after_drawdown_pct.clone(),
            "--degraded-min-z".to_string(),
            profile.degraded_min_z.clone(),
            "--degraded-max-price".to_string(),
            profile.degraded_max_price.clone(),
            "--conf".to_string(),
            profile.conf.clone(),
            "--z".to_string(),
            profile.z.clone(),
            "--edge".to_string(),
            profile.edge.clone(),
            format!("--ev-buffer={}", profile.ev_buffer),
            "--min-price".to_string(),
            profile.min_price.clone(),
            "--max-price".to_string(),
            profile.max_price.clone(),
            "--settlement-cutoff-minutes".to_string(),
            profile.settlement_cutoff_minutes.clone(),
            "--settlement-floor".to_string(),
            profile.settlement_floor.clone(),
            "--settlement-guard-minutes".to_string(),
            profile.settlement_guard_minutes.clone(),
            "--settlement-sigma-buffer".to_string(),
            profile.settlement_sigma_buffer.clone(),
            "--min-reversion-count".to_string(),
            profile.min_reversion_count.clone(),
            "--max-reversion-count".to_string(),
            profile.max_reversion_count.clone(),
            "--micro-max-spread".to_string(),
            profile.micro_max_spread.clone(),
            "--micro-min-depth".to_string(),
            profile.micro_min_depth.clone(),
            format!("--micro-min-pressure={}", profile.micro_min_pressure),
            "--zone-mode".to_string(),
            zone_mode_string(zone_mode).to_string(),
            "--top".to_string(),
            "20".to_string(),
            "--threads".to_string(),
            input.threads.to_string(),
            "--window-minutes".to_string(),
            cli_float_arg(input.window_minutes),
            "--continuous".to_string(),
            "--report-json".to_string(),
            sweep_report.display().to_string(),
        ];
        for tag in &input.require_causal_tag {
            sweep_args.push("--require-causal-tag".to_string());
            sweep_args.push(tag.clone());
        }
        for tag in &input.deny_causal_tag {
            sweep_args.push("--deny-causal-tag".to_string());
            sweep_args.push(tag.clone());
        }
        if !profile.taker_only {
            sweep_args.push("--also-maker".to_string());
        }
        if profile.degraded_force_taker {
            sweep_args.push("--degraded-force-taker".to_string());
        }
        if profile.taker_only {
            sweep_args.push("--taker-only".to_string());
        }
        if input.atomic_parquet {
            sweep_args.push("--atomic-parquet".to_string());
        }
        if let Some(btc_csv) = &input.btc_csv {
            sweep_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
        }

        if input.execute {
            let fold_result =
                run_child(&exe, &hydrate_args).and_then(|_| run_child(&exe, &sweep_args));
            if let Err(err) = fold_result {
                if input.delete_after_process && fold_cache.exists() {
                    delete_fold_cache(&cache_root, &fold_cache).with_context(|| {
                        format!(
                            "fold {} failed, then cleanup failed for {}",
                            idx + 1,
                            fold_cache.display()
                        )
                    })?;
                }
                return Err(err).with_context(|| {
                    format!(
                        "fold {} failed for {} through {}",
                        idx + 1,
                        fold_start.to_rfc3339(),
                        fold_end.to_rfc3339()
                    )
                });
            }
            let fold_hours = (fold_end - fold_start).num_hours() + 1;
            let fold_coverage = sweep_report_coverage(
                &sweep_report,
                fold_hours,
                input.min_fold_target_events,
                min_fold_top_trades,
            )?;
            if let Some(reason) = fold_coverage.reason.clone() {
                let mut cache_deleted = false;
                if input.delete_after_process && fold_cache.exists() {
                    delete_fold_cache(&cache_root, &fold_cache).with_context(|| {
                        format!(
                            "fold {} was coverage-limited, then cleanup failed for {}",
                            idx + 1,
                            fold_cache.display()
                        )
                    })?;
                    cache_deleted = true;
                }
                fold_summaries.push(RollingFoldSummary {
                    index: idx + 1,
                    start: fold_start.to_rfc3339(),
                    end: fold_end.to_rfc3339(),
                    cache_dir: fold_cache.display().to_string(),
                    hydrate_report: hydrate_report.display().to_string(),
                    sweep_report: sweep_report.display().to_string(),
                    hydrate_args,
                    sweep_args,
                    cache_deleted,
                    coverage: Some(fold_coverage),
                });
                std::fs::create_dir_all(&out_dir)
                    .with_context(|| format!("create {}", out_dir.display()))?;
                let manifest = out_dir.join("rolling_history_manifest.json");
                let message = format!(
                    "fold {} for {} through {} is coverage-limited: {}; treat this as data coverage failure, not strategy evidence",
                    idx + 1,
                    fold_start.to_rfc3339(),
                    fold_end.to_rfc3339(),
                    reason
                );
                let summary = build_summary(
                    "coverage_limited",
                    Some(message.clone()),
                    &fold_summaries,
                    &[],
                );
                write_json_atomic(&manifest, &summary, false)
                    .with_context(|| format!("write {}", manifest.display()))?;
                anyhow::bail!("{message}");
            }
            coverage = Some(fold_coverage);
        }

        let mut cache_deleted = false;
        if input.execute && input.delete_after_process {
            delete_fold_cache(&cache_root, &fold_cache)?;
            cache_deleted = true;
        }

        sweep_reports.push(sweep_report.clone());
        fold_summaries.push(RollingFoldSummary {
            index: idx + 1,
            start: fold_start.to_rfc3339(),
            end: fold_end.to_rfc3339(),
            cache_dir: fold_cache.display().to_string(),
            hydrate_report: hydrate_report.display().to_string(),
            sweep_report: sweep_report.display().to_string(),
            hydrate_args,
            sweep_args,
            cache_deleted,
            coverage,
        });
    }

    let min_promotion_trades = input
        .min_promotion_trades
        .unwrap_or(input.min_fold_trades * fold_summaries.len());
    let min_promotion_daily_trades = input
        .min_promotion_daily_trades
        .unwrap_or(input.min_fold_trades);
    let min_promotion_profitable_reports = input
        .min_promotion_profitable_reports
        .unwrap_or(fold_summaries.len());
    let min_promotion_losses = input.min_promotion_losses.unwrap_or(5);
    let min_neighbor_observations = input.min_neighbor_observations.unwrap_or(0);
    let mut promote_args = vec![
        "experiment".to_string(),
        "robust-promote".to_string(),
        "--output".to_string(),
        promotion_output.display().to_string(),
        "--min-reports".to_string(),
        fold_summaries.len().to_string(),
        "--min-profitable-reports".to_string(),
        min_promotion_profitable_reports.to_string(),
        "--min-trades".to_string(),
        min_promotion_trades.to_string(),
        "--min-losses".to_string(),
        min_promotion_losses.to_string(),
        "--min-zone-count".to_string(),
        if zone_mode == backtest::sweep::ZoneMode::All {
            "2".to_string()
        } else {
            "1".to_string()
        },
        "--max-zone-trade-share".to_string(),
        if zone_mode == backtest::sweep::ZoneMode::All {
            "0.70".to_string()
        } else {
            "1.0".to_string()
        },
        "--min-win-rate".to_string(),
        "0.70".to_string(),
        "--min-wilson-win-rate-lower".to_string(),
        "0.60".to_string(),
        "--min-total-pnl".to_string(),
        "0".to_string(),
        "--max-passive-failed-fills".to_string(),
        (80 * fold_summaries.len()).to_string(),
        "--min-fill-rate".to_string(),
        "0.55".to_string(),
        "--min-daily-trades".to_string(),
        min_promotion_daily_trades.to_string(),
        "--min-daily-pnl".to_string(),
        "0".to_string(),
        "--min-neighbor-count".to_string(),
        "2".to_string(),
        "--min-neighbor-observations".to_string(),
        min_neighbor_observations.to_string(),
        "--min-neighbor-positive-rate".to_string(),
        input.min_neighbor_positive_rate.to_string(),
        "--max-pbo".to_string(),
        input.max_pbo.to_string(),
        "--min-median-oos-percentile".to_string(),
        input.min_median_oos_percentile.to_string(),
        "--min-worst-window-pnl".to_string(),
        "0".to_string(),
        "--min-profit-factor".to_string(),
        "1.20".to_string(),
        "--min-payoff-ratio".to_string(),
        "0.20".to_string(),
        "--max-worst-loss-to-avg-win".to_string(),
        "6.0".to_string(),
        "--min-causal-bucket-trades".to_string(),
        "10".to_string(),
        "--min-causal-bucket-pnl".to_string(),
        "0".to_string(),
    ];
    for report in &sweep_reports {
        promote_args.extend(["--report".to_string(), report.display().to_string()]);
    }

    if input.execute {
        std::fs::create_dir_all(&out_dir)
            .with_context(|| format!("create {}", out_dir.display()))?;
        let manifest = out_dir.join("rolling_history_manifest.json");
        let pending_summary =
            build_summary("promotion_pending", None, &fold_summaries, &promote_args);
        write_json_atomic(&manifest, &pending_summary, false)
            .with_context(|| format!("write {}", manifest.display()))?;
        if let Err(err) = run_child(&exe, &promote_args) {
            let failed_summary = build_summary(
                "promotion_failed",
                Some(err.to_string()),
                &fold_summaries,
                &promote_args,
            );
            write_json_atomic(&manifest, &failed_summary, false)
                .with_context(|| format!("write {}", manifest.display()))?;
            return Err(err);
        }
        let summary = build_summary("promotion_passed", None, &fold_summaries, &promote_args);
        write_json_atomic(&manifest, &summary, false)
            .with_context(|| format!("write {}", manifest.display()))?;
        return Ok(summary);
    }

    let summary = build_summary("dry_run", None, &fold_summaries, &promote_args);
    Ok(summary)
}

async fn preflight_pmxt_hours(
    start: chrono::DateTime<chrono::Utc>,
    requested_end: chrono::DateTime<chrono::Utc>,
    cache_root: &std::path::Path,
    stop_at_first_missing_hour: bool,
) -> anyhow::Result<(chrono::DateTime<chrono::Utc>, serde_json::Value)> {
    use anyhow::{bail, Context};
    use chrono::Duration as ChronoDuration;

    let loader = backtest::pmxt::PMXTv2Loader::new(cache_root);
    let mut cur = start;
    let mut checked_hours = 0_u64;
    let mut available_hours = 0_u64;
    let mut cached_hours = 0_u64;
    let mut remote_available_hours = 0_u64;
    let mut missing_hour = None;
    let mut effective_end = requested_end;

    while cur <= requested_end {
        checked_hours += 1;
        if loader.is_cached(cur) {
            available_hours += 1;
            cached_hours += 1;
        } else if loader
            .remote_hour_available(cur)
            .await
            .with_context(|| format!("preflight PMXT hour {}", cur.to_rfc3339()))?
        {
            available_hours += 1;
            remote_available_hours += 1;
        } else {
            missing_hour = Some(cur);
            if stop_at_first_missing_hour {
                effective_end = cur - ChronoDuration::hours(1);
                break;
            }
            bail!(
                "PMXT hour {} is missing; pass --stop-at-first-missing-hour to run only the contiguous available prefix",
                cur.to_rfc3339()
            );
        }
        cur += ChronoDuration::hours(1);
    }

    if effective_end < start {
        let missing = missing_hour
            .map(|h| h.to_rfc3339())
            .unwrap_or_else(|| start.to_rfc3339());
        bail!(
            "no available PMXT hours from {}; first missing hour {}",
            start,
            missing
        );
    }

    let preflight = serde_json::json!({
        "enabled": true,
        "requested_start": start.to_rfc3339(),
        "requested_end": requested_end.to_rfc3339(),
        "effective_end": effective_end.to_rfc3339(),
        "checked_hours": checked_hours,
        "available_hours": available_hours,
        "cached_hours": cached_hours,
        "remote_available_hours": remote_available_hours,
        "missing_hour": missing_hour.map(|h| h.to_rfc3339()),
        "stopped_at_first_missing_hour": stop_at_first_missing_hour && missing_hour.is_some(),
    });
    Ok((effective_end, preflight))
}

fn rolling_history_profile(name: &str) -> anyhow::Result<RollingHistoryProfile> {
    let profile = match name {
        "a_plus5m" | "highz5m" => RollingHistoryProfile {
            name: name.to_string(),
            conf: "0.30,0.35,0.40".to_string(),
            z: "0.50,0.70,0.90,1.10".to_string(),
            edge: "0.03".to_string(),
            ev_buffer: "-1.0".to_string(),
            min_price: "0.10".to_string(),
            max_price: "0.75,0.90".to_string(),
            settlement_cutoff_minutes: "0.30".to_string(),
            settlement_floor: "10.0".to_string(),
            settlement_guard_minutes: "1.0".to_string(),
            settlement_sigma_buffer: "0.0".to_string(),
            min_reversion_count: "0".to_string(),
            max_reversion_count: "0".to_string(),
            micro_max_spread: "1.0".to_string(),
            micro_min_depth: "0.0".to_string(),
            micro_min_pressure: "-1.0".to_string(),
            position_pct: "0.05".to_string(),
            max_per_market_usd: "20".to_string(),
            max_total_exposure_usd: "15".to_string(),
            max_projected_stressed_drawdown_pct: "0.24".to_string(),
            degraded_after_losses: "0".to_string(),
            degraded_after_drawdown_pct: "0.0".to_string(),
            degraded_min_z: "0.0".to_string(),
            degraded_max_price: "0.0".to_string(),
            degraded_force_taker: false,
            taker_only: false,
        },
        "a_plus5m_regime" => {
            let mut profile = rolling_history_profile("a_plus5m")?;
            profile.name = name.to_string();
            profile.max_projected_stressed_drawdown_pct = "0.12,0.16,0.24".to_string();
            profile
        }
        "a_plus5m_adaptive" => {
            let mut profile = rolling_history_profile("a_plus5m")?;
            profile.name = name.to_string();
            profile.degraded_after_losses = "1,2".to_string();
            profile.degraded_after_drawdown_pct = "0.0".to_string();
            profile.degraded_min_z = "0.90".to_string();
            profile.degraded_max_price = "0.0".to_string();
            profile.degraded_force_taker = true;
            profile
        }
        "a_plus5m_adaptive_price" => {
            let mut profile = rolling_history_profile("a_plus5m")?;
            profile.name = name.to_string();
            profile.degraded_after_losses = "1,2".to_string();
            profile.degraded_after_drawdown_pct = "0.0".to_string();
            profile.degraded_min_z = "0.90".to_string();
            profile.degraded_max_price = "0.75,0.90".to_string();
            profile.degraded_force_taker = true;
            profile
        }
        "a_plus5m_ev_guard" => {
            let mut profile = rolling_history_profile("a_plus5m")?;
            profile.name = name.to_string();
            profile.conf = "0.40".to_string();
            profile.edge = "0.03,0.07".to_string();
            profile.ev_buffer = "0.05".to_string();
            profile.max_price = "0.90".to_string();
            profile.degraded_after_losses = "2".to_string();
            profile.degraded_after_drawdown_pct = "0.0".to_string();
            profile.degraded_min_z = "0.90".to_string();
            profile.degraded_max_price = "0.0".to_string();
            profile.degraded_force_taker = true;
            profile
        }
        "a_plus5m_causal_guard" => {
            let mut profile = rolling_history_profile("a_plus5m_adaptive")?;
            profile.name = name.to_string();
            profile.edge = "0.07,0.10".to_string();
            profile.max_price = "0.75,0.85".to_string();
            profile.settlement_cutoff_minutes = "2.0".to_string();
            profile.settlement_guard_minutes = "2.0".to_string();
            profile.max_reversion_count = "2".to_string();
            profile
        }
        "a_plus5m_causal_guard_selected" => {
            let mut profile = rolling_history_profile("a_plus5m_causal_guard")?;
            profile.name = name.to_string();
            profile.conf = "0.40".to_string();
            profile.z = "0.90".to_string();
            profile.edge = "0.07".to_string();
            profile.max_price = "0.85".to_string();
            profile.degraded_after_losses = "2".to_string();
            profile.degraded_force_taker = true;
            profile.taker_only = true;
            profile
        }
        "a_plus5m_reversion_guard" => {
            let mut profile = rolling_history_profile("a_plus5m_causal_guard")?;
            profile.name = name.to_string();
            profile.conf = "0.50".to_string();
            profile.min_price = "0.75".to_string();
            profile.max_price = "0.85".to_string();
            profile.min_reversion_count = "1".to_string();
            profile.max_reversion_count = "2".to_string();
            profile
        }
        "a_plus5m_down_reversion_guard" => {
            let mut profile = rolling_history_profile("a_plus5m_reversion_guard")?;
            profile.name = name.to_string();
            profile.conf = "0.60".to_string();
            profile.z = "0.80".to_string();
            profile.edge = "0.09".to_string();
            profile.degraded_after_losses = "2".to_string();
            profile.degraded_max_price = "0.0".to_string();
            profile.taker_only = true;
            profile
        }
        "a_plus5m_down_reversion_guard_neighbors" => {
            let mut profile = rolling_history_profile("a_plus5m_down_reversion_guard")?;
            profile.name = name.to_string();
            profile.z = "0.70,0.80".to_string();
            profile.edge = "0.05,0.07,0.09".to_string();
            profile
        }
        "a_plus5m_down_reversion_guard_confidence" => {
            let mut profile = rolling_history_profile("a_plus5m_down_reversion_guard")?;
            profile.name = name.to_string();
            profile.conf = "0.60,0.70".to_string();
            profile.z = "0.70,0.80".to_string();
            profile.edge = "0.07,0.09".to_string();
            profile
        }
        other => anyhow::bail!("unknown rolling-history profile `{other}`"),
    };
    Ok(profile)
}

fn cli_money_arg(value: f64) -> String {
    format!("{value:.2}")
}

fn cli_float_arg(value: f64) -> String {
    if (value.fract()).abs() <= f64::EPSILON {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn zone_mode_string(mode: backtest::sweep::ZoneMode) -> &'static str {
    match mode {
        backtest::sweep::ZoneMode::All => "all",
        backtest::sweep::ZoneMode::Early => "early",
        backtest::sweep::ZoneMode::Primary => "primary",
        backtest::sweep::ZoneMode::Late => "late",
        backtest::sweep::ZoneMode::Terminal => "terminal",
    }
}

fn run_child(exe: &std::path::Path, args: &[String]) -> anyhow::Result<()> {
    use anyhow::Context;

    let status = std::process::Command::new(exe)
        .args(args)
        .status()
        .with_context(|| format!("run {} {}", exe.display(), args.join(" ")))?;
    if !status.success() {
        anyhow::bail!("child command failed with {status}: {}", args.join(" "));
    }
    Ok(())
}

fn enforce_cache_budget(cache_root: &std::path::Path, max_cache_gb: f64) -> anyhow::Result<()> {
    if max_cache_gb <= 0.0 {
        return Ok(());
    }
    let bytes = dir_size_bytes(cache_root)?;
    let budget = (max_cache_gb * 1024.0 * 1024.0 * 1024.0) as u64;
    if bytes > budget {
        anyhow::bail!(
            "cache root {} is {:.2} GiB, above budget {:.2} GiB",
            cache_root.display(),
            bytes as f64 / 1024.0 / 1024.0 / 1024.0,
            max_cache_gb
        );
    }
    Ok(())
}

fn dir_size_bytes(path: &std::path::Path) -> anyhow::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total += dir_size_bytes(&entry.path())?;
        } else {
            total += meta.len();
        }
    }
    Ok(total)
}

fn sweep_report_coverage(
    path: &std::path::Path,
    fold_hours: i64,
    min_target_events: u64,
    min_top_trades: usize,
) -> anyhow::Result<RollingFoldCoverage> {
    let report = backtest::experiment::read_report(path)
        .with_context(|| format!("read sweep report {}", path.display()))?;
    Ok(sweep_report_coverage_from_report(
        &report,
        fold_hours,
        min_target_events,
        min_top_trades,
    ))
}

fn sweep_report_coverage_from_report(
    report: &backtest::experiment::ExperimentReport,
    fold_hours: i64,
    min_target_events: u64,
    min_top_trades: usize,
) -> RollingFoldCoverage {
    let target_events = report
        .variants
        .iter()
        .map(|variant| variant.diagnostics.events_seen)
        .max()
        .unwrap_or(0);
    let top_variant = report.variants.iter().max_by(|a, b| {
        a.trades
            .cmp(&b.trades)
            .then_with(|| a.total_pnl.total_cmp(&b.total_pnl))
    });
    let top_trades = top_variant.map(|v| v.trades).unwrap_or(0);
    let top_variant_name = top_variant.map(variant_report_name);
    let top_variant_pnl = top_variant.map(|v| v.total_pnl);
    let fold_hours = fold_hours.max(1) as f64;
    let target_events_per_hour = target_events as f64 / fold_hours;

    let mut reasons = Vec::new();
    if min_target_events > 0 && target_events < min_target_events {
        reasons.push(format!(
            "target_events {} below minimum {}",
            target_events, min_target_events
        ));
    }
    if min_top_trades > 0 && top_trades < min_top_trades {
        reasons.push(format!(
            "top variant trades {} below minimum {}",
            top_trades, min_top_trades
        ));
    }
    let reason = if reasons.is_empty() {
        None
    } else {
        Some(reasons.join("; "))
    };
    RollingFoldCoverage {
        status: if reason.is_some() {
            "coverage_limited".to_string()
        } else {
            "ok".to_string()
        },
        reason,
        target_events,
        target_events_per_hour,
        top_trades,
        top_variant: top_variant_name,
        top_variant_pnl,
        min_target_events,
        min_top_trades,
    }
}

fn variant_report_name(variant: &backtest::experiment::VariantReport) -> String {
    variant
        .strategy_params
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or(&variant.strategy.name)
        .to_string()
}

fn delete_fold_cache(
    cache_root: &std::path::Path,
    fold_cache: &std::path::Path,
) -> anyhow::Result<()> {
    let root = cache_root.canonicalize()?;
    let target = fold_cache.canonicalize()?;
    if target == root || !target.starts_with(&root) {
        anyhow::bail!(
            "refusing to delete {}; it is not a child of cache root {}",
            target.display(),
            root.display()
        );
    }
    let Some(name) = target.file_name().and_then(|n| n.to_str()) else {
        anyhow::bail!("refusing to delete cache with invalid final path component");
    };
    if !name.starts_with("fold_") {
        anyhow::bail!("refusing to delete non-fold cache {}", target.display());
    }
    std::fs::remove_dir_all(&target)?;
    Ok(())
}

fn filter_contracts_by_window_minutes(
    contracts: &mut Vec<data::scanner::CandleContract>,
    target_minutes: Option<f64>,
    label: &str,
) {
    let Some(target) = target_minutes else {
        return;
    };
    if target <= 0.0 {
        eprintln!("--window-minutes must be > 0");
        std::process::exit(2);
    }
    let before = contracts.len();
    contracts.retain(|c| {
        let minutes = live::window::estimate_window_minutes(&c.window_description);
        (minutes - target).abs() < 1e-6
    });
    eprintln!(
        "{label}: window_minutes={target} kept {}/{} contract(s)",
        contracts.len(),
        before
    );
    tracing::info!(
        label,
        target_minutes = target,
        before,
        kept = contracts.len(),
        "window length filter",
    );
}

fn gamma_market_has_terminal_outcome(market: &data::models::Market) -> bool {
    market.outcomes.len() == 2
        && (market.outcomes.iter().any(|o| o.price >= 0.99)
            || ((market.outcomes[0].price - market.outcomes[1].price).abs() <= 1e-9
                && market.outcomes[0].price > 0.0))
}

fn gamma_market_needs_refresh(market: &data::models::Market) -> bool {
    let ended = chrono::DateTime::parse_from_rfc3339(&market.end_date)
        .map(|d| d.with_timezone(&chrono::Utc) < chrono::Utc::now())
        .unwrap_or(false);
    ended && !gamma_market_has_terminal_outcome(market)
}

fn write_json_atomic<T: serde::Serialize>(
    path: impl AsRef<std::path::Path>,
    value: &T,
    pretty: bool,
) -> std::io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let payload = if pretty {
        serde_json::to_vec_pretty(value)
    } else {
        serde_json::to_vec(value)
    }
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("payload.json");
    let tmp = path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
    std::fs::write(&tmp, payload)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

async fn run_startup_preflight(
    settings: &config::Settings,
    mode: RuntimeMode,
    i_understand_live: bool,
) -> release::PreflightReport {
    let mut report = release::run_preflight(settings, mode, i_understand_live);
    if mode.is_live() {
        let check = live_wallet_preflight_check(settings).await;
        report.checks.push(check);
        report.ok = !report
            .checks
            .iter()
            .any(|c| c.status == release::CheckStatus::Fail);
    }
    report
}

async fn live_wallet_preflight_check(settings: &config::Settings) -> release::PreflightCheck {
    if settings.private_key.is_empty() {
        return release::PreflightCheck {
            name: "live_wallet",
            status: release::CheckStatus::Fail,
            detail: "PRIVATE_KEY not set; cannot verify wallet live_ready".to_string(),
        };
    }
    match data::wallet::WalletReader::new(&settings.polygon_rpc_url, &settings.private_key) {
        Ok(reader) => match reader.fetch_balances().await {
            Ok(balances) => {
                let configured_budget = live_configured_order_budget_usd(settings, &balances);
                let min_order_budget = live_min_order_budget_usd(settings);
                let required = live_required_wallet_usd(settings, &balances);
                let budget_ready = live_wallet_covers_budget(&balances, required);
                let config_ready = configured_budget + 1e-9 >= min_order_budget;
                let status = if balances.live_ready() && budget_ready && config_ready {
                    release::CheckStatus::Ok
                } else {
                    release::CheckStatus::Fail
                };
                release::PreflightCheck {
                    name: "live_wallet",
                    status,
                    detail: live_wallet_preflight_detail(
                        &balances,
                        configured_budget,
                        min_order_budget,
                        required,
                    ),
                }
            }
            Err(e) => release::PreflightCheck {
                name: "live_wallet",
                status: release::CheckStatus::Fail,
                detail: format!("wallet fetch failed: {e}"),
            },
        },
        Err(e) => release::PreflightCheck {
            name: "live_wallet",
            status: release::CheckStatus::Fail,
            detail: format!("wallet init failed: {e}"),
        },
    }
}

fn live_configured_order_budget_usd(
    settings: &config::Settings,
    balances: &data::wallet::WalletBalances,
) -> f64 {
    let bankroll = if settings.bankroll_usd > 0.0 {
        settings.bankroll_usd
    } else {
        balances.pusd
    };
    if bankroll <= 0.0 {
        return 0.0;
    }

    let vol_multiplier = settings
        .candle_vol_high_multiplier
        .max(settings.candle_vol_extreme_multiplier)
        .max(1.0);
    let mut position = bankroll * settings.candle_position_pct.max(0.0) * vol_multiplier;
    if settings.max_position_per_market_usd > 0.0 {
        position = position.min(settings.max_position_per_market_usd);
    }
    position = position.min(bankroll);
    if 0.0 < position && position < 1.0 && bankroll >= 1.0 {
        position = 1.0;
    }
    position
}

fn live_min_order_budget_usd(settings: &config::Settings) -> f64 {
    let max_price = settings.candle_max_price.clamp(0.01, 0.99);
    settings.live_min_order_size_shares.max(1.0) * max_price
}

fn live_required_wallet_usd(
    settings: &config::Settings,
    balances: &data::wallet::WalletBalances,
) -> f64 {
    let raw_required = live_configured_order_budget_usd(settings, balances)
        .max(live_min_order_budget_usd(settings))
        .max(1.0);
    raw_required * settings.live_order_budget_buffer.max(1.0)
}

fn live_wallet_covers_budget(balances: &data::wallet::WalletBalances, required_usd: f64) -> bool {
    let required = required_usd.max(1.0);
    let eps = 1e-9;
    balances.pusd + eps >= required
        && balances.pusd_allowance_exchange + eps >= required
        && balances.pusd_allowance_neg_risk_exchange + eps >= required
        && balances.pol >= 0.01
}

fn live_wallet_preflight_detail(
    balances: &data::wallet::WalletBalances,
    configured_budget_usd: f64,
    min_order_budget_usd: f64,
    required_usd: f64,
) -> String {
    let base = balances.live_ready_detail();
    let budget_ready = live_wallet_covers_budget(balances, required_usd);
    let config_ready = configured_budget_usd + 1e-9 >= min_order_budget_usd;
    format!(
        "{}; configured live order budget {}: configured=${:.2}, min_order_floor=${:.2}, requires pUSD and both CTF Exchange V2 allowances >= ${:.2}",
        base,
        if budget_ready && config_ready { "ok" } else { "not ready" },
        configured_budget_usd,
        min_order_budget_usd,
        required_usd.max(1.0)
    )
}

fn install_signal_handlers(stop: std::sync::Arc<tokio::sync::Notify>) {
    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM");
        let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("install SIGINT");
        tokio::select! {
            _ = term.recv() => tracing::info!("SIGTERM received, shutting down"),
            _ = int.recv() => tracing::info!("SIGINT received, shutting down"),
        }
        stop.notify_one();
    });
}

fn init_tracing(level: &str) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

#[allow(clippy::too_many_arguments)]
async fn cmd_live_replay(
    settings: &config::Settings,
    start: &str,
    end: Option<&str>,
    cache_dir: Option<&str>,
    btc_csv: Option<&str>,
    bankroll: f64,
    latency_ms: u64,
    session_log_dir: Option<&str>,
    allow_download: bool,
    delete_after_process: bool,
    allow_gamma_fetch: bool,
    max_contracts: Option<usize>,
    window_minutes: Option<f64>,
    report_json: Option<&str>,
) {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};

    let start_dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(start) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--start must be RFC3339: {e}");
            std::process::exit(2);
        }
    };
    let end_dt = match end {
        Some(e) => match DateTime::parse_from_rfc3339(e) {
            Ok(d) => d.with_timezone(&Utc),
            Err(err) => {
                eprintln!("--end must be RFC3339: {err}");
                std::process::exit(2);
            }
        },
        None => start_dt,
    };
    if end_dt < start_dt {
        eprintln!("--end must be >= --start");
        std::process::exit(2);
    }

    let mut hours = Vec::new();
    let mut cur = start_dt;
    while cur <= end_dt {
        hours.push(cur);
        cur += ChronoDuration::hours(1);
    }

    let mut parquet_cleanup = SessionOwnedParquetCleanup::new(delete_after_process);
    let cache_dir_path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(backtest::pmxt::PMXTv2Loader::default_cache_dir);
    let loader = backtest::pmxt::PMXTv2Loader::new(&cache_dir_path);
    for &h in &hours {
        if allow_download {
            eprintln!("live-replay: ensuring PMXT archive hour {h}");
            match loader.download_hour_with_status(h, false).await {
                Ok((path, downloaded)) => {
                    if downloaded {
                        parquet_cleanup.push(path);
                    }
                }
                Err(e) => {
                    eprintln!("download {h} failed: {e}");
                    live_replay_exit(1, &mut parquet_cleanup);
                }
            }
        } else if !loader.is_cached(h) {
            eprintln!(
                "PMXT hour {h} is not cached in {}; pass --allow-download to fetch it",
                cache_dir_path.display()
            );
            live_replay_exit(1, &mut parquet_cleanup);
        }
    }

    let gamma_cache_path = cache_dir_path.join("gamma_market_cache.json");
    let mut cached_markets: std::collections::BTreeMap<String, data::models::Market> =
        match std::fs::read_to_string(&gamma_cache_path) {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(_) => Default::default(),
        };
    if allow_gamma_fetch {
        let gamma = data::gamma::GammaClient::new(&settings.poly_gamma_url);
        let new_markets = match fetch_gamma_historical_markets_for_window(
            &gamma,
            start_dt,
            end_dt,
            window_minutes,
            "live-replay",
        )
        .await
        {
            Ok(markets) => markets,
            Err(e) => {
                eprintln!("Gamma historical metadata lookup failed: {e}");
                live_replay_exit(1, &mut parquet_cleanup);
            }
        };
        let fetched = new_markets.len();
        let candle_markets = data::scanner::scan_candle_markets_for_backtest(&new_markets, 0.0);
        let mut merged = 0usize;
        for contract in candle_markets {
            if contract.asset != "BTC" {
                continue;
            }
            if !window_minutes
                .map(|target| {
                    (live::window::estimate_window_minutes(&contract.window_description) - target)
                        .abs()
                        <= 1e-6
                })
                .unwrap_or(true)
            {
                continue;
            }
            if cached_markets
                .get(&contract.market.condition_id)
                .map(gamma_market_needs_refresh)
                .unwrap_or(true)
            {
                merged += 1;
            }
            cached_markets.insert(
                contract.market.condition_id.clone(),
                contract.market.clone(),
            );
        }
        eprintln!(
            "live-replay: fetched {fetched} historical market(s), merged {merged} BTC candle market(s)"
        );
        if merged > 0 {
            if let Err(e) = write_json_atomic(&gamma_cache_path, &cached_markets, false) {
                eprintln!(
                    "write Gamma cache {} failed: {e}",
                    gamma_cache_path.display()
                );
                live_replay_exit(1, &mut parquet_cleanup);
            }
        }
    } else {
        eprintln!(
            "live-replay: using cached Gamma metadata from {}",
            gamma_cache_path.display()
        );
    }
    if cached_markets.is_empty() {
        eprintln!(
            "live-replay has no cached Gamma metadata at {}; pass --allow-gamma-fetch to build it",
            gamma_cache_path.display()
        );
        live_replay_exit(1, &mut parquet_cleanup);
    }

    let markets: Vec<data::models::Market> = cached_markets.values().cloned().collect();
    let mut contracts = data::scanner::scan_candle_markets_for_backtest(&markets, 0.0);
    contracts.retain(|c| c.asset == "BTC");
    filter_contracts_by_window_minutes(&mut contracts, window_minutes, "live-replay");
    let start_ts = start_dt.timestamp() as f64;
    let end_ts = end_dt.timestamp() as f64 + 3600.0;
    contracts.retain(|c| {
        let close_t = chrono::DateTime::parse_from_rfc3339(&c.end_date)
            .map(|d| d.timestamp() as f64)
            .unwrap_or(0.0);
        let window_minutes = live::window::estimate_window_minutes(&c.window_description);
        let window_minutes = if window_minutes > 0.0 {
            window_minutes
        } else {
            60.0
        };
        let open_t = close_t - window_minutes * 60.0;
        close_t > start_ts && open_t < end_ts
    });
    if contracts.is_empty() {
        eprintln!(
            "live-replay found no BTC candle contracts in [{start}, {}]",
            end.unwrap_or(start)
        );
        live_replay_exit(1, &mut parquet_cleanup);
    }
    contracts.sort_by(|a, b| {
        a.end_date
            .cmp(&b.end_date)
            .then_with(|| a.market.condition_id.cmp(&b.market.condition_id))
    });
    if let Some(limit) = max_contracts {
        contracts.truncate(limit);
    }
    if contracts.is_empty() {
        eprintln!("live-replay --max-contracts must be greater than zero");
        live_replay_exit(2, &mut parquet_cleanup);
    }
    eprintln!("live-replay: BTC candle contracts={}", contracts.len());
    let universe = backtest::harness::CandleUniverse { contracts };
    let (btc_required_start_ms, btc_required_end_ms) = btc_required_range_ms(
        &universe,
        start_dt.timestamp_millis(),
        (end_dt + ChronoDuration::hours(1)).timestamp_millis(),
    );

    let mut btc = backtest::btc_history::BTCHistory::new();
    if let Some(path) = btc_csv {
        if let Err(e) = btc.load_csv(path) {
            eprintln!("BTC CSV load failed: {e}");
            live_replay_exit(1, &mut parquet_cleanup);
        }
    } else {
        let pad_ms = 3_600_000;
        let start_ms = btc_required_start_ms - pad_ms;
        let end_ms = btc_required_end_ms + pad_ms;
        match btc
            .load_from_binance(start_ms, end_ms, "BTCUSDT", "1s")
            .await
        {
            Ok(n) if n > 100 => tracing::info!(rows = n, interval = "1s", "BTC klines"),
            _ => {
                btc = backtest::btc_history::BTCHistory::new();
                if let Err(e) = btc
                    .load_from_binance(start_ms, end_ms, "BTCUSDT", "1m")
                    .await
                {
                    eprintln!("Binance fetch failed: {e}");
                    live_replay_exit(1, &mut parquet_cleanup);
                }
            }
        }
    }
    ensure_btc_history_covers_or_cleanup(
        "live-replay",
        &btc,
        btc_required_start_ms,
        btc_required_end_ms,
        &mut parquet_cleanup,
    );

    let shared_dir = std::env::var("PMXT_DISTILLED_DIR")
        .ok()
        .or_else(|| {
            let p = std::path::PathBuf::from(backtest::distill::SHARED_CACHE_DIR);
            if p.exists() {
                Some(backtest::distill::SHARED_CACHE_DIR.to_string())
            } else {
                None
            }
        })
        .map(std::path::PathBuf::from);
    let session_log_dir = session_log_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(&settings.session_log_dir));
    let cfg = live::replay::LiveReplayConfig {
        hours,
        universe,
        btc_history: std::sync::Arc::new(btc),
        bankroll_usd: bankroll,
        max_total_exposure_usd: settings.max_total_exposure_usd,
        min_order_size_shares: settings.live_min_order_size_shares,
        cache_dir: cache_dir_path,
        session_log_dir,
        latency: backtest::l2_replay::StaticLatencyConfig {
            insert_ms: latency_ms,
        },
        shared_distilled_dir: shared_dir,
        strategy: match live::replay::ReplayStrategy::load(settings) {
            Ok(strategy) => strategy,
            Err(e) => {
                eprintln!("live-replay strategy load failed: {e:#}");
                live_replay_exit(2, &mut parquet_cleanup);
            }
        },
    };
    match live::replay::run_live_replay(cfg, settings).await {
        Ok(report) => {
            if let Some(path) = report_json {
                if let Err(e) = write_json_atomic(path, &report, true) {
                    eprintln!("write live-replay report {path}: {e}");
                    live_replay_exit(1, &mut parquet_cleanup);
                }
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serialize live replay report")
            );
            parquet_cleanup.cleanup_best_effort();
        }
        Err(e) => {
            eprintln!("live-replay failed: {e:?}");
            live_replay_exit(1, &mut parquet_cleanup);
        }
    }
}

struct SessionOwnedParquetCleanup {
    enabled: bool,
    paths: Vec<std::path::PathBuf>,
}

impl SessionOwnedParquetCleanup {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            paths: Vec::new(),
        }
    }

    fn push(&mut self, path: std::path::PathBuf) {
        if self.enabled {
            self.paths.push(path);
        }
    }

    fn cleanup_best_effort(&mut self) {
        if !self.enabled {
            return;
        }
        for path in self.paths.drain(..) {
            match std::fs::remove_file(&path) {
                Ok(()) => eprintln!("live-replay: deleted downloaded parquet {}", path.display()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => eprintln!(
                    "live-replay: failed to delete downloaded parquet {}: {err}",
                    path.display()
                ),
            }
        }
    }
}

fn live_replay_exit(code: i32, cleanup: &mut SessionOwnedParquetCleanup) -> ! {
    cleanup.cleanup_best_effort();
    std::process::exit(code);
}

fn ensure_btc_history_covers_or_cleanup(
    label: &str,
    btc: &backtest::btc_history::BTCHistory,
    required_start_ms: i64,
    required_end_ms: i64,
    cleanup: &mut SessionOwnedParquetCleanup,
) {
    if let Some(message) =
        btc_history_coverage_error(label, btc, required_start_ms, required_end_ms)
    {
        eprintln!("{message}");
        live_replay_exit(1, cleanup);
    }
}

async fn cmd_scan(s: &config::Settings, max_hours: f64, min_liquidity: f64) {
    let client = data::gamma::GammaClient::new(&s.poly_gamma_url);
    match client
        .fetch_markets_by_end_date(max_hours, min_liquidity)
        .await
    {
        Ok(markets) => {
            let contracts = data::scanner::scan_candle_markets(&markets, max_hours, min_liquidity);
            println!(
                "markets={} candle_contracts={}",
                markets.len(),
                contracts.len()
            );
            for c in contracts.iter().take(20) {
                println!(
                    "  {asset:5} {hours:5.2}h {q}",
                    asset = c.asset,
                    hours = c.hours_left,
                    q = c.market.question,
                );
            }
        }
        Err(e) => {
            eprintln!("scan failed: {e}");
            std::process::exit(1);
        }
    }
}

async fn cmd_wallet(s: &config::Settings, json: bool) {
    if s.private_key.is_empty() {
        eprintln!("PRIVATE_KEY not set");
        std::process::exit(1);
    }
    match data::wallet::WalletReader::new(&s.polygon_rpc_url, &s.private_key) {
        Ok(reader) => match reader.fetch_balances().await {
            Ok(b) => {
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "address": b.address,
                            "pusd": b.pusd,
                            "usdc_e": b.usdc_e,
                            "usdc_native": b.usdc_native,
                            "stable_total": b.total_stable_diagnostics,
                            "pusd_allowance_exchange": b.pusd_allowance_exchange,
                            "pusd_allowance_neg_risk_exchange": b.pusd_allowance_neg_risk_exchange,
                            "usdc_e_allowance_onramp": b.usdc_e_allowance_onramp,
                            "pol": b.pol,
                            "live_ready": b.live_ready(),
                            "detail": b.live_ready_detail(),
                        }))
                        .expect("serialize wallet")
                    );
                    return;
                }
                println!("address      {}", b.address);
                println!("pusd         ${:.2}", b.pusd);
                println!("usdc_e       ${:.2}", b.usdc_e);
                println!("usdc_native  ${:.2}", b.usdc_native);
                println!("stable_total ${:.2}", b.total_stable_diagnostics);
                println!(
                    "pusd_allow   ${:.2} CTF Exchange V2",
                    b.pusd_allowance_exchange
                );
                println!(
                    "pusd_allow   ${:.2} Neg Risk CTF Exchange V2",
                    b.pusd_allowance_neg_risk_exchange
                );
                println!(
                    "usdc_e_allow ${:.2} Collateral Onramp",
                    b.usdc_e_allowance_onramp
                );
                println!("pol          {:.4}", b.pol);
                println!(
                    "live_ready   {}",
                    if b.live_ready() {
                        "yes"
                    } else {
                        "no (needs pUSD, both CTF Exchange V2 pUSD allowances, and >=0.01 POL)"
                    }
                );
            }
            Err(e) => {
                eprintln!("wallet fetch failed: {e}");
                std::process::exit(1);
            }
        },
        Err(e) => {
            eprintln!("wallet init failed: {e}");
            std::process::exit(1);
        }
    }
}

async fn cmd_clob(s: &config::Settings, command: ClobCommand) {
    let mut client = clob::ClobClient::new(
        &s.poly_base_url,
        &s.poly_api_key,
        &s.poly_api_secret,
        &s.poly_api_passphrase,
    );
    if !s.private_key.is_empty() {
        client.set_signing_key(&s.private_key);
    }
    let result = match command {
        ClobCommand::Ok => client.get_ok().await,
        ClobCommand::Time => client.get_server_time().await,
        ClobCommand::Book { token_id } => client.get_book(&token_id).await,
        ClobCommand::Price { token_id, side } => {
            client
                .get_price(&token_id, &side.to_ascii_uppercase())
                .await
        }
        ClobCommand::Midpoint { token_id } => client.get_midpoint(&token_id).await,
        ClobCommand::Spread { token_id } => client.get_spread(&token_id).await,
        ClobCommand::TickSize { token_id } => client.get_tick_size(&token_id).await,
        ClobCommand::FeeRate { token_id } => client.get_fee_rate_bps(&token_id).await,
        ClobCommand::NegRisk { token_id } => client.get_neg_risk(&token_id).await,
        ClobCommand::Market { condition_id } => client.get_market(&condition_id).await,
        ClobCommand::Orders {
            market,
            asset_id,
            next_cursor,
        } => {
            let mut params = Vec::new();
            if let Some(v) = &market {
                params.push(("market", v.as_str()));
            }
            if let Some(v) = &asset_id {
                params.push(("asset_id", v.as_str()));
            }
            if let Some(v) = &next_cursor {
                params.push(("next_cursor", v.as_str()));
            }
            client.get_user_orders(&params).await
        }
        ClobCommand::Order { order_id } => client.get_order(&order_id).await,
        ClobCommand::Trades {
            id,
            market,
            asset_id,
            after,
            before,
            next_cursor,
        } => {
            let mut params = Vec::new();
            if let Some(v) = &id {
                params.push(("id", v.as_str()));
            }
            if let Some(v) = &market {
                params.push(("market", v.as_str()));
            }
            if let Some(v) = &asset_id {
                params.push(("asset_id", v.as_str()));
            }
            if let Some(v) = &after {
                params.push(("after", v.as_str()));
            }
            if let Some(v) = &before {
                params.push(("before", v.as_str()));
            }
            if let Some(v) = &next_cursor {
                params.push(("next_cursor", v.as_str()));
            }
            client.get_trades(&params).await
        }
        ClobCommand::Heartbeat => client.post_heartbeat().await,
    };
    match result {
        Ok(v) => println!(
            "{}",
            serde_json::to_string_pretty(&v).unwrap_or_else(|_| v.to_string())
        ),
        Err(e) => {
            eprintln!("clob diagnostic failed: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_experiment(command: ExperimentCommand) {
    match command {
        ExperimentCommand::ZoneAudit {
            report,
            params_hash,
            max_zone_trade_share,
            min_zone_trades,
            min_zone_pnl,
        } => {
            let mut reports = Vec::new();
            for path in &report {
                match backtest::experiment::read_report(path) {
                    Ok(report_doc) => reports.push(report_doc),
                    Err(e) => {
                        eprintln!("read experiment report {path} failed: {e}");
                        std::process::exit(1);
                    }
                }
            }
            let audit = match backtest::experiment::zone_concentration_audit(
                &reports,
                params_hash.as_deref(),
                max_zone_trade_share,
                min_zone_trades,
                min_zone_pnl,
            ) {
                Ok(audit) => audit,
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&audit).expect("serialize zone concentration audit")
            );
        }
        ExperimentCommand::Promote {
            report,
            output,
            min_trades,
            min_losses,
            min_zone_count,
            min_win_rate,
            min_wilson_win_rate_lower,
            min_total_pnl,
            min_sharpe_like,
            max_unresolved_fills,
            max_failed_fills,
            max_passive_failed_fills,
            min_fill_rate,
            max_zone_trade_share,
            allow_incomplete_data,
        } => {
            let report_doc = match backtest::experiment::read_report(&report) {
                Ok(report_doc) => report_doc,
                Err(e) => {
                    eprintln!("read experiment report failed: {e}");
                    std::process::exit(1);
                }
            };
            let gate = backtest::experiment::PromotionGate {
                min_trades,
                min_losses,
                min_zone_count,
                min_win_rate,
                min_wilson_win_rate_lower,
                min_total_pnl,
                min_sharpe_like,
                max_unresolved_fills,
                max_failed_fills,
                max_passive_failed_fills,
                min_fill_rate,
                max_zone_trade_share,
                require_complete_data: !allow_incomplete_data,
            };
            let artifact =
                match backtest::experiment::PromotionArtifact::from_report(&report_doc, gate) {
                    Ok(artifact) => artifact,
                    Err(e) => {
                        eprintln!("{e}");
                        std::process::exit(2);
                    }
                };
            if let Err(e) = backtest::experiment::write_promotion_atomic(&output, &artifact) {
                eprintln!("write promotion artifact failed: {e}");
                std::process::exit(1);
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "output": output,
                    "strategy": artifact.selected_strategy,
                    "trades": artifact.trades,
                    "win_rate": artifact.win_rate,
                    "total_pnl": artifact.total_pnl,
                    "sharpe_like": artifact.sharpe_like,
                    "dominant_zone": artifact.dominant_zone,
                    "dominant_zone_trade_share": artifact.dominant_zone_trade_share,
                    "data_manifest_hash": artifact.data_manifest_hash,
                    "source_report_hash": artifact.source_report_hash,
                }))
                .expect("serialize promotion summary")
            );
        }
        ExperimentCommand::AggregatePromote {
            report,
            output,
            min_trades,
            min_losses,
            min_zone_count,
            min_win_rate,
            min_wilson_win_rate_lower,
            min_total_pnl,
            min_sharpe_like,
            max_unresolved_fills,
            max_failed_fills,
            max_passive_failed_fills,
            min_fill_rate,
            max_zone_trade_share,
            min_reports,
            min_profitable_reports,
            min_daily_trades,
            min_daily_pnl,
            max_daily_loss,
            allow_incomplete_data,
        } => {
            let mut reports = Vec::new();
            for path in &report {
                match backtest::experiment::read_report(path) {
                    Ok(report_doc) => reports.push(report_doc),
                    Err(e) => {
                        eprintln!("read experiment report {path} failed: {e}");
                        std::process::exit(1);
                    }
                }
            }
            let gate = backtest::experiment::PromotionGate {
                min_trades,
                min_losses,
                min_zone_count,
                min_win_rate,
                min_wilson_win_rate_lower,
                min_total_pnl,
                min_sharpe_like,
                max_unresolved_fills,
                max_failed_fills,
                max_passive_failed_fills,
                min_fill_rate,
                max_zone_trade_share,
                require_complete_data: !allow_incomplete_data,
            };
            let multi_gate = backtest::experiment::MultiReportPromotionGate {
                min_reports,
                min_profitable_reports,
                min_daily_trades,
                min_daily_pnl,
                max_daily_loss,
            };
            let artifact = match backtest::experiment::PromotionArtifact::from_reports(
                &reports, gate, multi_gate,
            ) {
                Ok(artifact) => artifact,
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            };
            if let Err(e) = backtest::experiment::write_promotion_atomic(&output, &artifact) {
                eprintln!("write promotion artifact failed: {e}");
                std::process::exit(1);
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "output": output,
                    "strategy": artifact.selected_strategy,
                    "trades": artifact.trades,
                    "win_rate": artifact.win_rate,
                    "total_pnl": artifact.total_pnl,
                    "sharpe_like": artifact.sharpe_like,
                    "dominant_zone": artifact.dominant_zone,
                    "dominant_zone_trade_share": artifact.dominant_zone_trade_share,
                    "data_manifest_hash": artifact.data_manifest_hash,
                    "source_report_hash": artifact.source_report_hash,
                    "risk_notes": artifact.risk_notes,
                }))
                .expect("serialize aggregate promotion summary")
            );
        }
        ExperimentCommand::RobustPromote {
            report,
            output,
            min_trades,
            min_losses,
            min_zone_count,
            min_win_rate,
            min_wilson_win_rate_lower,
            min_total_pnl,
            min_sharpe_like,
            max_unresolved_fills,
            max_failed_fills,
            max_passive_failed_fills,
            min_fill_rate,
            max_zone_trade_share,
            min_reports,
            min_profitable_reports,
            min_daily_trades,
            min_daily_pnl,
            max_daily_loss,
            min_neighbor_count,
            min_neighbor_observations,
            min_neighbor_positive_rate,
            max_pbo,
            min_median_oos_percentile,
            min_worst_window_pnl,
            min_robust_score,
            min_profit_factor,
            min_payoff_ratio,
            max_worst_loss_to_avg_win,
            min_causal_bucket_trades,
            min_causal_bucket_pnl,
            allow_incomplete_data,
        } => {
            let mut reports = Vec::new();
            for path in &report {
                match backtest::experiment::read_report(path) {
                    Ok(report_doc) => reports.push(report_doc),
                    Err(e) => {
                        eprintln!("read experiment report {path} failed: {e}");
                        std::process::exit(1);
                    }
                }
            }
            let gate = backtest::experiment::PromotionGate {
                min_trades,
                min_losses,
                min_zone_count,
                min_win_rate,
                min_wilson_win_rate_lower,
                min_total_pnl,
                min_sharpe_like,
                max_unresolved_fills,
                max_failed_fills,
                max_passive_failed_fills,
                min_fill_rate,
                max_zone_trade_share,
                require_complete_data: !allow_incomplete_data,
            };
            let multi_gate = backtest::experiment::MultiReportPromotionGate {
                min_reports,
                min_profitable_reports,
                min_daily_trades,
                min_daily_pnl,
                max_daily_loss,
            };
            let robust_gate = backtest::experiment::RobustPromotionGate {
                min_neighbor_count,
                min_neighbor_observations,
                min_neighbor_positive_rate,
                max_pbo,
                min_median_oos_percentile,
                min_worst_window_pnl,
                min_robust_score,
                min_profit_factor,
                min_payoff_ratio,
                max_worst_loss_to_avg_win,
                min_causal_bucket_trades,
                min_causal_bucket_pnl,
            };
            let (artifact, diagnostics) =
                match backtest::experiment::PromotionArtifact::from_reports_robust(
                    &reports,
                    gate,
                    multi_gate,
                    robust_gate,
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        eprintln!("{e}");
                        std::process::exit(2);
                    }
                };
            if let Err(e) = backtest::experiment::write_promotion_atomic(&output, &artifact) {
                eprintln!("write promotion artifact failed: {e}");
                std::process::exit(1);
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "output": output,
                    "strategy": artifact.selected_strategy,
                    "trades": artifact.trades,
                    "win_rate": artifact.win_rate,
                    "total_pnl": artifact.total_pnl,
                    "sharpe_like": artifact.sharpe_like,
                    "dominant_zone": artifact.dominant_zone,
                    "dominant_zone_trade_share": artifact.dominant_zone_trade_share,
                    "data_manifest_hash": artifact.data_manifest_hash,
                    "source_report_hash": artifact.source_report_hash,
                    "risk_notes": artifact.risk_notes,
                    "robust": diagnostics,
                }))
                .expect("serialize robust promotion summary")
            );
        }
    }
}

async fn cmd_telegram(settings: &config::Settings, command: TelegramCommand) {
    if let TelegramCommand::Status {
        soak_report,
        send: false,
    } = &command
    {
        let text = telegram_status_text(settings, soak_report.as_deref())
            .unwrap_or_else(|e| format!("PolyMomentum status unavailable\nreason={e:#}"));
        println!("{text}");
        return;
    }

    let Some(client) = monitoring::telegram::TelegramClient::from_env() else {
        eprintln!(
            "Telegram is not configured: set TELEGRAM_BOT_TOKEN and numeric TELEGRAM_CHAT_ID"
        );
        std::process::exit(2);
    };

    match command {
        TelegramCommand::Probe {
            set_commands,
            send_status,
        } => {
            let me = match client.get_me().await {
                Ok(me) => me,
                Err(e) => {
                    eprintln!("telegram probe failed: {e:#}");
                    std::process::exit(1);
                }
            };
            if set_commands {
                if let Err(e) = client.set_operator_commands().await {
                    eprintln!("telegram set commands failed: {e:#}");
                    std::process::exit(1);
                }
            }
            if send_status {
                let text = telegram_status_text(settings, None)
                    .unwrap_or_else(|e| format!("PolyMomentum status unavailable\nreason={e:#}"));
                if let Err(e) = client
                    .send_message(&text, Some(monitoring::telegram::operator_keyboard()))
                    .await
                {
                    eprintln!("telegram send status failed: {e:#}");
                    std::process::exit(1);
                }
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "ok": true,
                    "bot": me,
                    "chat_id": client.chat_id(),
                    "commands_set": set_commands,
                    "status_sent": send_status,
                }))
                .expect("serialize telegram probe")
            );
        }
        TelegramCommand::Status { soak_report, send } => {
            let text = telegram_status_text(settings, soak_report.as_deref())
                .unwrap_or_else(|e| format!("PolyMomentum status unavailable\nreason={e:#}"));
            if send {
                if let Err(e) = client
                    .send_message(&text, Some(monitoring::telegram::operator_keyboard()))
                    .await
                {
                    eprintln!("telegram send status failed: {e:#}");
                    std::process::exit(1);
                }
            } else {
                println!("{text}");
            }
        }
        TelegramCommand::Poll { once, timeout_s } => {
            if let Err(e) = telegram_poll_loop(settings, &client, once, timeout_s).await {
                eprintln!("telegram poll failed: {e:#}");
                std::process::exit(1);
            }
        }
    }
}

async fn telegram_poll_loop(
    settings: &config::Settings,
    client: &monitoring::telegram::TelegramClient,
    once: bool,
    timeout_s: u64,
) -> anyhow::Result<()> {
    let mut offset = None;
    let mut consecutive_errors: u32 = 0;
    loop {
        let updates = match client.get_updates(offset, timeout_s).await {
            Ok(updates) => {
                consecutive_errors = 0;
                updates
            }
            Err(e) if !once => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                let delay_s = 30_u64.min(1_u64 << consecutive_errors.min(5));
                tracing::warn!(
                    error = %e,
                    consecutive_errors,
                    delay_s,
                    "telegram poll request failed; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_secs(delay_s)).await;
                continue;
            }
            Err(e) => return Err(e),
        };
        for update in updates {
            if let Some(id) = update.get("update_id").and_then(|v| v.as_i64()) {
                offset = Some(id + 1);
            }
            if let Err(e) = handle_telegram_update(settings, client, &update).await {
                tracing::warn!(error = %e, "telegram update handler failed");
            }
        }
        if once {
            break;
        }
    }
    Ok(())
}

async fn handle_telegram_update(
    settings: &config::Settings,
    client: &monitoring::telegram::TelegramClient,
    update: &serde_json::Value,
) -> anyhow::Result<()> {
    if let Some(callback) = update.get("callback_query") {
        let callback_id = callback.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let data = callback.get("data").and_then(|v| v.as_str()).unwrap_or("");
        let chat_id = callback
            .get("message")
            .and_then(|m| m.get("chat"))
            .and_then(|c| c.get("id"))
            .and_then(|v| v.as_i64());
        let message_id = callback
            .get("message")
            .and_then(|m| m.get("message_id"))
            .and_then(|v| v.as_i64());
        let Some(chat_id) = chat_id else {
            return Ok(());
        };
        if !client.is_allowed_chat(chat_id) {
            let _ = client
                .answer_callback_query(callback_id, "unauthorized chat")
                .await;
            return Ok(());
        }
        let (text, keyboard, answer) = telegram_callback_response(settings, data).await;
        let _ = client.answer_callback_query(callback_id, answer).await;
        if let Some(message_id) = message_id {
            if let Err(e) = client
                .edit_message_text(chat_id, message_id, &text, Some(keyboard.clone()))
                .await
            {
                tracing::warn!(error = %e, "telegram edit failed; sending new message");
                client.send_message(&text, Some(keyboard)).await?;
            }
        }
        return Ok(());
    }

    let Some(message) = update.get("message") else {
        return Ok(());
    };
    let Some(chat_id) = message
        .get("chat")
        .and_then(|c| c.get("id"))
        .and_then(|v| v.as_i64())
    else {
        return Ok(());
    };
    if !client.is_allowed_chat(chat_id) {
        return Ok(());
    }
    let text = message
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .split_whitespace()
        .next()
        .unwrap_or("");
    let (response, keyboard) = telegram_message_response(settings, text).await;
    client.send_message(&response, Some(keyboard)).await?;
    Ok(())
}

async fn telegram_callback_response(
    settings: &config::Settings,
    action: &str,
) -> (String, serde_json::Value, &'static str) {
    match action {
        "pm:terminate" => (
            telegram_terminate_prompt(settings),
            monitoring::telegram::termination_keyboard(),
            "confirmation required",
        ),
        "pm:terminate_confirm" => (
            telegram_terminate_confirm_text(settings).await,
            monitoring::telegram::operator_keyboard(),
            "stop requested",
        ),
        "pm:terminate_cancel" => (
            "PolyMomentum stop cancelled.".to_string(),
            monitoring::telegram::operator_keyboard(),
            "cancelled",
        ),
        "pm:status" => (
            telegram_status_text(settings, None)
                .unwrap_or_else(|e| format!("PolyMomentum status unavailable\nreason={e:#}")),
            monitoring::telegram::operator_keyboard(),
            "updated",
        ),
        "pm:stale" => (
            telegram_staleness_text(settings)
                .unwrap_or_else(|e| format!("Strategy freshness unavailable\nreason={e:#}")),
            monitoring::telegram::operator_keyboard(),
            "updated",
        ),
        "pm:preflight" => (
            telegram_preflight_text(settings).await,
            monitoring::telegram::operator_keyboard(),
            "updated",
        ),
        "pm:wallet" => (
            telegram_wallet_text(settings).await,
            monitoring::telegram::operator_keyboard(),
            "updated",
        ),
        _ => (
            monitoring::telegram::help_text().to_string(),
            monitoring::telegram::operator_keyboard(),
            "updated",
        ),
    }
}

async fn telegram_message_response(
    settings: &config::Settings,
    text: &str,
) -> (String, serde_json::Value) {
    match text {
        "/status" | "/start" => (
            telegram_status_text(settings, None)
                .unwrap_or_else(|e| format!("PolyMomentum status unavailable\nreason={e:#}")),
            monitoring::telegram::operator_keyboard(),
        ),
        "/stale" => (
            telegram_staleness_text(settings)
                .unwrap_or_else(|e| format!("Strategy freshness unavailable\nreason={e:#}")),
            monitoring::telegram::operator_keyboard(),
        ),
        "/preflight" => (
            telegram_preflight_text(settings).await,
            monitoring::telegram::operator_keyboard(),
        ),
        "/wallet" => (
            telegram_wallet_text(settings).await,
            monitoring::telegram::operator_keyboard(),
        ),
        "/terminate" => (
            telegram_terminate_prompt(settings),
            monitoring::telegram::termination_keyboard(),
        ),
        "/help" => (
            monitoring::telegram::help_text().to_string(),
            monitoring::telegram::operator_keyboard(),
        ),
        _ => (
            monitoring::telegram::help_text().to_string(),
            monitoring::telegram::operator_keyboard(),
        ),
    }
}

fn telegram_terminate_prompt(settings: &config::Settings) -> String {
    format!(
        "Confirm PolyMomentum stop\nThis writes the kill switch at {}.\nOnly polymomentum-engine stops; Telegram monitor stays available. Peer bots are untouched.\nPress Confirm Stop to proceed.",
        settings.kill_switch_path
    )
}

async fn telegram_terminate_confirm_text(settings: &config::Settings) -> String {
    let path = std::path::Path::new(&settings.kill_switch_path);
    if path.as_os_str().is_empty() {
        return "PolyMomentum stop failed\nreason=empty KILL_SWITCH_PATH".to_string();
    }
    if let Some(parent) = path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            return format!(
                "PolyMomentum stop failed\nkill_switch={}\nreason=create parent: {e}",
                path.display()
            );
        }
    }
    let body = format!(
        "requested_at={}\nsource=telegram\nscope=polymomentum-engine\n",
        chrono::Utc::now().to_rfc3339()
    );
    match tokio::fs::write(path, body).await {
        Ok(()) => format!(
            "PolyMomentum stop requested\nkill_switch={}\nEngine will stop on the next cycle. Telegram monitor remains online.",
            path.display()
        ),
        Err(e) => format!(
            "PolyMomentum stop failed\nkill_switch={}\nreason={e}",
            path.display()
        ),
    }
}

fn telegram_status_text(
    settings: &config::Settings,
    report_path: Option<&str>,
) -> anyhow::Result<String> {
    let report_path = match report_path {
        Some(path) => Some(std::path::PathBuf::from(path)),
        None => latest_named_file(
            &std::path::Path::new(&settings.logs_dir).join("soak"),
            "soak_",
            ".json",
        ),
    };
    let Some(path) = report_path else {
        return Ok("PolyMomentum status\nNo soak report found yet.".to_string());
    };
    let report = read_json_value(&path)?;
    let release = report.get("release").unwrap_or(&serde_json::Value::Null);
    let wallet = report.get("wallet").unwrap_or(&serde_json::Value::Null);
    let peers = report.get("peers").unwrap_or(&serde_json::Value::Null);
    let diagnostics = report
        .get("diagnostics")
        .unwrap_or(&serde_json::Value::Null);
    let replay = report.get("replay").unwrap_or(&serde_json::Value::Null);
    let stale_text = if let Some(stale) = report.get("staleness") {
        compact_staleness_line(stale)
    } else {
        report
            .get("latest_session")
            .and_then(|v| v.as_str())
            .and_then(|session| {
                monitoring::staleness::analyze_staleness(
                    session,
                    monitoring::staleness::StalenessConfig::default(),
                )
                .ok()
            })
            .map(|r| {
                format!(
                    "freshness={} outcomes={} recent_wr={}",
                    r.status,
                    r.sample.outcomes,
                    pct_opt(r.sample.recent_win_rate)
                )
            })
            .unwrap_or_else(|| "freshness=unknown".to_string())
    };
    let replay_line = replay
        .get("output")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().replace('\n', " "))
        .unwrap_or_else(|| "replay=unknown".to_string());
    let hash = release
        .get("git_sha")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let strategy_hash = release
        .get("promotion")
        .and_then(|p| p.get("strategy"))
        .and_then(|s| s.get("params_hash"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let wallet_line = format!(
        "wallet_live_ready={} pUSD={} negRiskAllow={} POL={}",
        yes_no(wallet.get("live_ready").and_then(|v| v.as_bool())),
        money_opt(wallet.get("pusd").and_then(|v| v.as_f64())),
        money_opt(
            wallet
                .get("pusd_allowance_neg_risk_exchange")
                .and_then(|v| v.as_f64())
        ),
        num_opt(wallet.get("pol").and_then(|v| v.as_f64()), 4)
    );
    let peers_line = format!(
        "peers adgts={} polyarb={} collector={}",
        str_field(peers, "adgts"),
        str_field(peers, "polyarbitrage"),
        str_field(peers, "polyarbitrage_collector")
    );
    let diag_line = format!(
        "diag_ok={} events={} warnings={}",
        yes_no(diagnostics.get("ok").and_then(|v| v.as_bool())),
        diagnostics
            .get("total_events")
            .and_then(|v| v.as_u64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        diagnostics
            .get("warnings")
            .and_then(|v| v.as_array())
            .map(|v| v.len().to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    Ok(format!(
        "PolyMomentum status\nverdict={} mode={} generated={}\nrelease={} strategy={}\n{}\n{}\n{}\n{}\n{}\nreport={}",
        yes_no(report.get("ok").and_then(|v| v.as_bool())),
        str_field(&report, "mode"),
        str_field(&report, "generated_at"),
        hash,
        short_hash(strategy_hash),
        replay_line,
        stale_text,
        wallet_line,
        diag_line,
        peers_line,
        path.display()
    ))
}

fn telegram_staleness_text(settings: &config::Settings) -> anyhow::Result<String> {
    let latest_session = latest_named_file(
        std::path::Path::new(&settings.session_log_dir),
        "session_",
        ".jsonl",
    )
    .ok_or_else(|| anyhow::anyhow!("no session_*.jsonl found"))?;
    let report = monitoring::staleness::analyze_staleness(
        &latest_session,
        monitoring::staleness::StalenessConfig::default(),
    )?;
    Ok(format!(
        "Strategy freshness\nstatus={} ok={} outcomes={} wr={} recent={} recent_wr={} drift={} drop={} eps={}\nwarnings={}\nrecommendation={}\nsession={}",
        report.status,
        yes_no(Some(report.ok)),
        report.sample.outcomes,
        pct_opt(report.sample.win_rate),
        report.sample.recent_window,
        pct_opt(report.sample.recent_win_rate),
        yes_no(Some(report.drift.significant)),
        num_opt(report.drift.drop, 3),
        num_opt(report.drift.epsilon, 3),
        if report.warnings.is_empty() {
            "none".to_string()
        } else {
            report.warnings.join(", ")
        },
        report.recommendation,
        latest_session.display()
    ))
}

async fn telegram_preflight_text(settings: &config::Settings) -> String {
    let report = run_startup_preflight(settings, RuntimeMode::Paper, false).await;
    format!(
        "Paper preflight\nok={} mode={}\nchecks={} failures={}",
        yes_no(Some(report.ok)),
        report.mode.as_str(),
        report.checks.len(),
        report.failure_summary()
    )
}

async fn telegram_wallet_text(settings: &config::Settings) -> String {
    if settings.private_key.is_empty() {
        return "Wallet\nPRIVATE_KEY not set".to_string();
    }
    let reader =
        match data::wallet::WalletReader::new(&settings.polygon_rpc_url, &settings.private_key) {
            Ok(reader) => reader,
            Err(e) => return format!("Wallet\ninit failed: {e}"),
        };
    match reader.fetch_balances().await {
        Ok(b) => format!(
            "Wallet\naddress={}\nlive_ready={}\npUSD={} exchangeAllow={} negRiskAllow={} POL={:.4}\ndetail={}",
            b.address,
            yes_no(Some(b.live_ready())),
            money_opt(Some(b.pusd)),
            money_opt(Some(b.pusd_allowance_exchange)),
            money_opt(Some(b.pusd_allowance_neg_risk_exchange)),
            b.pol,
            b.live_ready_detail()
        ),
        Err(e) => format!("Wallet\nfetch failed: {e}"),
    }
}

fn latest_named_file(
    dir: &std::path::Path,
    prefix: &str,
    suffix: &str,
) -> Option<std::path::PathBuf> {
    let entries = std::fs::read_dir(dir).ok()?;
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?;
            if !name.starts_with(prefix) || !name.ends_with(suffix) {
                return None;
            }
            let modified = entry.metadata().ok()?.modified().ok()?;
            Some((modified, path))
        })
        .max_by_key(|(modified, _)| *modified)
        .map(|(_, path)| path)
}

fn read_json_value(path: &std::path::Path) -> anyhow::Result<serde_json::Value> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("read JSON {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse JSON {}", path.display()))
}

fn compact_staleness_line(stale: &serde_json::Value) -> String {
    let sample = stale.get("sample").unwrap_or(&serde_json::Value::Null);
    format!(
        "freshness={} outcomes={} recent_wr={} drift={}",
        str_field(stale, "status"),
        sample
            .get("outcomes")
            .and_then(|v| v.as_u64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        pct_opt(sample.get("recent_win_rate").and_then(|v| v.as_f64())),
        yes_no(
            stale
                .get("drift")
                .and_then(|d| d.get("significant"))
                .and_then(|v| v.as_bool())
        )
    )
}

fn str_field<'a>(v: &'a serde_json::Value, field: &str) -> &'a str {
    v.get(field).and_then(|x| x.as_str()).unwrap_or("unknown")
}

fn yes_no(v: Option<bool>) -> &'static str {
    match v {
        Some(true) => "yes",
        Some(false) => "no",
        None => "unknown",
    }
}

fn money_opt(v: Option<f64>) -> String {
    v.map(|x| format!("${x:.2}"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn num_opt(v: Option<f64>, decimals: usize) -> String {
    v.map(|x| format!("{x:.decimals$}"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn pct_opt(v: Option<f64>) -> String {
    v.map(|x| format!("{:.1}%", x * 100.0))
        .unwrap_or_else(|| "unknown".to_string())
}

fn short_hash(v: &str) -> String {
    v.chars().take(10).collect()
}

fn cmd_diagnostics(command: DiagnosticsCommand) {
    match command {
        DiagnosticsCommand::Session { path } => {
            let report = match monitoring::diagnostics::analyze_session(&path) {
                Ok(report) => report,
                Err(e) => {
                    eprintln!("diagnostics failed: {e}");
                    std::process::exit(1);
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serialize diagnostics report")
            );
            if !report.ok {
                std::process::exit(2);
            }
        }
        DiagnosticsCommand::Compare { left, right } => {
            let report = match monitoring::diagnostics::compare_sessions(&left, &right) {
                Ok(report) => report,
                Err(e) => {
                    eprintln!("diagnostics compare failed: {e}");
                    std::process::exit(1);
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serialize diagnostics comparison")
            );
            if !report.ok {
                std::process::exit(2);
            }
        }
        DiagnosticsCommand::Causality {
            path,
            max_clock_skew_s,
            max_post_end_fill_s,
            min_order_timings,
            min_resolution_timings,
        } => {
            let report = match monitoring::causality::audit_session(
                &path,
                monitoring::causality::CausalityAuditConfig {
                    max_clock_skew_s,
                    max_post_end_fill_s,
                    min_order_timings,
                    min_resolution_timings,
                },
            ) {
                Ok(report) => report,
                Err(e) => {
                    eprintln!("causality audit failed: {e}");
                    std::process::exit(1);
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serialize causality report")
            );
            if !report.ok {
                std::process::exit(2);
            }
        }
        DiagnosticsCommand::Staleness {
            path,
            min_outcomes,
            min_recent_window,
            min_recent_win_rate,
            delta,
        } => {
            let report = match monitoring::staleness::analyze_staleness(
                &path,
                monitoring::staleness::StalenessConfig {
                    min_outcomes,
                    min_recent_window,
                    min_recent_win_rate,
                    delta,
                    ..Default::default()
                },
            ) {
                Ok(report) => report,
                Err(e) => {
                    eprintln!("staleness diagnostics failed: {e}");
                    std::process::exit(1);
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serialize staleness report")
            );
            if !report.ok {
                std::process::exit(2);
            }
        }
    }
}

async fn cmd_ctf(s: &config::Settings, condition_id: &str) {
    let r = data::ctf::CtfReader::new(&s.polygon_rpc_url);
    match r.get_resolution(condition_id).await {
        Ok((res, [n0, n1])) => {
            println!("resolution    {}", res.as_str());
            println!("payout_num0   {}", n0);
            println!("payout_num1   {}", n1);
        }
        Err(e) => {
            eprintln!("ctf read failed: {e}");
            std::process::exit(1);
        }
    }
}

async fn cmd_validate_replay(path: &str) {
    use std::io::BufRead;
    let f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("open {path}: {e}");
            std::process::exit(1);
        }
    };
    let reader = std::io::BufReader::new(f);
    let mut total = 0u64;
    let mut mismatches = 0u64;
    let mut validation_cfg = ReplayValidationConfig::default();
    for line in reader.lines().map_while(|l| l.ok()) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        if v.get("cat").and_then(|x| x.as_str()) == Some("system")
            && v.get("type").and_then(|x| x.as_str()) == Some("runtime_strategy")
        {
            validation_cfg.apply_runtime_strategy_event(&v);
            continue;
        }
        if v.get("cat").and_then(|x| x.as_str()) != Some("signal") {
            continue;
        }
        if v.get("type").and_then(|x| x.as_str()) != Some("evaluation") {
            continue;
        }
        total += 1;

        // Build inputs
        let signal = strategy::momentum::MomentumSignal {
            direction: v
                .get("dir")
                .and_then(|x| x.as_str())
                .unwrap_or("up")
                .to_string(),
            confidence: f64opt(&v, "conf").unwrap_or(0.0),
            price_change: f64opt(&v, "chg").unwrap_or(0.0),
            price_change_pct: f64opt(&v, "chg_pct").unwrap_or(0.0),
            consistency: f64opt(&v, "cons").unwrap_or(0.0),
            minutes_elapsed: f64opt(&v, "elapsed_min").unwrap_or(0.0),
            minutes_remaining: f64opt(&v, "remaining_min").unwrap_or(0.0),
            current_price: f64opt(&v, "px").unwrap_or(0.0),
            open_price: f64opt(&v, "open").unwrap_or(0.0),
            z_score: f64opt(&v, "z").unwrap_or(0.0),
            reversion_count: u32opt(&v, "reversion_count").unwrap_or(0),
        };
        let res = strategy::decision::decide_candle_trade(
            &signal,
            signal.minutes_elapsed,
            signal.minutes_remaining,
            signal.minutes_elapsed + signal.minutes_remaining,
            f64opt(&v, "up_price").unwrap_or(0.5),
            f64opt(&v, "down_price").unwrap_or(0.5),
            signal.current_price,
            signal.open_price,
            f64opt(&v, "implied_vol").unwrap_or(0.5),
            validation_cfg.min_confidence,
            validation_cfg.min_edge,
            validation_cfg.skip_dead_zone,
            &validation_cfg.zone_config,
            f64opt(&v, "cross_boost").unwrap_or(0.0),
        );
        let traded = match res {
            strategy::decision::DecisionResult::Trade(decision) => validation_cfg
                .selectivity
                .reject_reason(&decision.regime)
                .is_none(),
            strategy::decision::DecisionResult::Skip(_) => false,
        };
        let expected_logged_decision_trade = traded && validation_cfg.settlement_alignment_ready;
        let logged_decision_trade = v
            .get("decision_trade")
            .and_then(|x| x.as_bool())
            .or_else(|| v.get("traded").and_then(|x| x.as_bool()))
            .unwrap_or(false);
        if expected_logged_decision_trade != logged_decision_trade {
            mismatches += 1;
        }
    }
    let mismatch_pct = if total > 0 {
        100.0 * mismatches as f64 / total as f64
    } else {
        0.0
    };
    println!("validate-replay: total={total} mismatches={mismatches} ({mismatch_pct:.2}%)");
    if mismatches > 0 {
        std::process::exit(1);
    }
}

#[derive(Debug, Clone)]
struct ReplayValidationConfig {
    zone_config: strategy::decision::ZoneConfig,
    min_confidence: f64,
    min_edge: f64,
    skip_dead_zone: bool,
    selectivity: backtest::strategies::SelectivityFilter,
    settlement_alignment_ready: bool,
}

impl Default for ReplayValidationConfig {
    fn default() -> Self {
        Self {
            zone_config: strategy::decision::ZoneConfig::default(),
            min_confidence: strategy::decision::DEFAULT_MIN_CONFIDENCE,
            min_edge: strategy::decision::DEFAULT_MIN_EDGE,
            skip_dead_zone: true,
            selectivity: backtest::strategies::SelectivityFilter::default(),
            settlement_alignment_ready: true,
        }
    }
}

impl ReplayValidationConfig {
    fn apply_runtime_strategy_event(&mut self, v: &serde_json::Value) {
        if let Some(source) = v.get("source").and_then(|x| x.as_str()) {
            if let Some(path) = promotion_path_from_runtime_source(source) {
                if let Some(variant) = load_promotion_variant_for_replay(path) {
                    self.apply_variant(variant);
                }
            }
        }
        if let Some(zone_config) = v.get("zone_config") {
            if let Ok(cfg) =
                serde_json::from_value::<strategy::decision::ZoneConfig>(zone_config.clone())
            {
                self.zone_config = cfg;
            }
        }
        if let Some(vv) = f64opt(v, "settlement_cutoff_minutes") {
            self.zone_config.settlement_cutoff_minutes = vv;
        }
        if let Some(vv) = f64opt(v, "settlement_guard_minutes") {
            self.zone_config.settlement_guard_minutes = vv;
        }
        if let Some(vv) = f64opt(v, "settlement_min_abs_move_usd") {
            self.zone_config.settlement_min_abs_move_usd = vv;
        }
        if let Some(vv) = f64opt(v, "settlement_sigma_buffer") {
            self.zone_config.settlement_sigma_buffer = vv;
        }
        if let Some(vv) = f64opt(v, "min_confidence") {
            self.min_confidence = vv;
        }
        if let Some(vv) = f64opt(v, "min_edge") {
            self.min_edge = vv;
        }
        if let Some(vv) = v.get("skip_dead_zone").and_then(|x| x.as_bool()) {
            self.skip_dead_zone = vv;
        }
        if let Some(selectivity) = v.get("selectivity") {
            if let Ok(filter) = serde_json::from_value::<backtest::strategies::SelectivityFilter>(
                selectivity.clone(),
            ) {
                self.selectivity = filter;
            }
        }
        if let Some(vv) = v
            .get("settlement_alignment_ready")
            .and_then(|x| x.as_bool())
        {
            self.settlement_alignment_ready = vv;
        }
    }

    fn apply_variant(&mut self, variant: backtest::strategies::StrategyVariant) {
        self.zone_config = variant.zone_config;
        self.min_confidence = variant.min_confidence;
        self.min_edge = variant.min_edge;
        self.skip_dead_zone = variant.skip_dead_zone;
        self.selectivity = variant.selectivity;
    }
}

fn promotion_path_from_runtime_source(source: &str) -> Option<&str> {
    let rest = source.strip_prefix("promotion:")?;
    Some(rest.split('+').next().unwrap_or(rest))
}

fn load_promotion_variant_for_replay(path: &str) -> Option<backtest::strategies::StrategyVariant> {
    let text = std::fs::read_to_string(path).ok()?;
    let artifact: backtest::experiment::PromotionArtifact = serde_json::from_str(&text).ok()?;
    serde_json::from_value(artifact.strategy_params).ok()
}

async fn cmd_distill(
    settings: &config::Settings,
    input: &str,
    output: Option<&str>,
    candle_cids_path: Option<&str>,
    hour_override: Option<&str>,
) {
    use chrono::DateTime;
    let in_path = std::path::PathBuf::from(input);
    if !in_path.exists() {
        eprintln!("input parquet not found: {}", in_path.display());
        std::process::exit(1);
    }

    // Derive hour from the filename or --hour override.
    let hour = match hour_override {
        Some(s) => match DateTime::parse_from_rfc3339(s) {
            Ok(d) => d.with_timezone(&chrono::Utc),
            Err(e) => {
                eprintln!("--hour: {e}");
                std::process::exit(2);
            }
        },
        None => {
            let stem = in_path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            // expects polymarket_orderbook_YYYY-MM-DDTHH.parquet
            let h = stem
                .strip_prefix("polymarket_orderbook_")
                .and_then(|s| s.strip_suffix(".parquet"))
                .unwrap_or("");
            match chrono::NaiveDateTime::parse_from_str(&format!("{h}:00:00"), "%Y-%m-%dT%H:%M:%S")
            {
                Ok(naive) => naive.and_utc(),
                Err(_) => {
                    eprintln!("could not derive hour from filename; pass --hour");
                    std::process::exit(2);
                }
            }
        }
    };

    // Build the candle-cid set: explicit file or auto-discover via Gamma.
    let cids: std::collections::HashSet<String> = if let Some(p) = candle_cids_path {
        let text = std::fs::read_to_string(p).unwrap_or_else(|e| {
            eprintln!("read --candle-cids {p}: {e}");
            std::process::exit(1);
        });
        text.split([',', '\n', ' '])
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        tracing::info!("auto-discovering candle cids via Gamma + scanner regex");
        let gamma = data::gamma::GammaClient::new(&settings.poly_gamma_url);
        // Pull a wide window around the hour so we catch markets that
        // closed during it (or are still open).
        let max_hours = 24.0 * 30.0;
        let markets = match gamma.fetch_markets_by_end_date(max_hours, 0.0).await {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Gamma fetch failed: {e}");
                std::process::exit(1);
            }
        };
        let candles = data::scanner::scan_candle_markets_for_backtest(&markets, 0.0);
        candles.into_iter().map(|c| c.market.condition_id).collect()
    };
    tracing::info!(cids = cids.len(), "candle universe loaded for distill");

    let out_path = match output {
        Some(s) => std::path::PathBuf::from(s),
        None => {
            let dir = in_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."));
            backtest::distill::shared_cache_path_for_hour(dir, hour)
        }
    };

    let t0 = std::time::Instant::now();
    match backtest::distill::distill_parquet_to_jsonl(&in_path, &cids, &out_path) {
        Ok(stats) => {
            let elapsed = t0.elapsed();
            println!(
                "distilled {} events ({} book / {} chg / {} trade) -> {} ({} bytes raw JSONL, gzipped on disk) in {:.2}s",
                stats.total(),
                stats.book_events,
                stats.change_events,
                stats.trade_events,
                out_path.display(),
                stats.bytes_written,
                elapsed.as_secs_f64(),
            );
        }
        Err(e) => {
            eprintln!("distill failed: {e}");
            std::process::exit(1);
        }
    }
}

async fn cmd_pmxt_download(start: &str, end: Option<&str>, cache_dir: Option<&str>) {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    let s: DateTime<Utc> = match DateTime::parse_from_rfc3339(start) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--start: {e}");
            std::process::exit(2);
        }
    };
    let e: DateTime<Utc> = match end {
        Some(e) => match DateTime::parse_from_rfc3339(e) {
            Ok(d) => d.with_timezone(&Utc),
            Err(err) => {
                eprintln!("--end: {err}");
                std::process::exit(2);
            }
        },
        None => s,
    };
    let path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(backtest::pmxt::PMXTv2Loader::default_cache_dir);
    let loader = backtest::pmxt::PMXTv2Loader::new(&path);
    let mut cur = s;
    while cur <= e {
        if let Err(err) = loader.download_hour(cur, false).await {
            eprintln!("download {} failed: {err}", cur);
            std::process::exit(1);
        }
        cur += ChronoDuration::hours(1);
    }
    println!("downloaded into {}", path.display());
}

async fn cmd_pmxt_info(hour: &str, cache_dir: Option<&str>, sample: usize) {
    use chrono::{DateTime, Utc};
    let dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(hour) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--hour must be RFC3339: {e}");
            std::process::exit(2);
        }
    };
    let path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(backtest::pmxt::PMXTv2Loader::default_cache_dir);
    let loader = backtest::pmxt::PMXTv2Loader::new(&path);
    if !loader.is_cached(dt) {
        eprintln!("not cached — run `harness` once or `download` first");
        std::process::exit(1);
    }
    match loader.distinct_condition_ids(dt) {
        Ok(s) => {
            println!("hour:                  {hour}");
            println!("distinct condition_ids: {}", s.len());
            for id in s.iter().take(sample) {
                println!("  len={:<3} {}", id.len(), id);
            }
        }
        Err(e) => {
            eprintln!("pmxt-info failed: {e}");
            std::process::exit(1);
        }
    }
}

fn parse_csv_floats(s: &str) -> Vec<f64> {
    s.split(',')
        .filter_map(|p| p.trim().parse::<f64>().ok())
        .collect()
}

fn parse_csv_u64s(s: &str) -> Vec<u64> {
    s.split(',')
        .filter_map(|p| p.trim().parse::<u64>().ok())
        .collect()
}

fn parse_selectivity_filter(
    require: &[String],
    deny: &[String],
) -> anyhow::Result<backtest::strategies::SelectivityFilter> {
    let require_tags = parse_causal_tag_map(require, "--require-causal-tag")?;
    let deny_tags = parse_causal_tag_map(deny, "--deny-causal-tag")?;
    Ok(backtest::strategies::SelectivityFilter {
        require_tags,
        deny_tags,
    })
}

fn parse_causal_tag_map(
    args: &[String],
    flag: &str,
) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
    let mut tags = std::collections::BTreeMap::new();
    for arg in args {
        for raw in arg.split(',') {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            let Some((dimension, value)) = raw.split_once('=') else {
                anyhow::bail!("{flag} value `{raw}` must use dimension=value");
            };
            let dimension = dimension.trim();
            let value = value.trim();
            if dimension.is_empty() || value.is_empty() {
                anyhow::bail!("{flag} value `{raw}` must use non-empty dimension=value");
            }
            match tags.insert(dimension.to_string(), value.to_string()) {
                Some(prev) if prev != value => {
                    anyhow::bail!(
                        "{flag} has conflicting values for `{dimension}`: `{prev}` and `{value}`"
                    );
                }
                _ => {}
            }
        }
    }
    Ok(tags)
}

fn btc_required_range_ms(
    universe: &backtest::harness::CandleUniverse,
    fallback_start_ms: i64,
    fallback_end_ms: i64,
) -> (i64, i64) {
    let mut start_ms = fallback_start_ms;
    let mut end_ms = fallback_end_ms;
    for contract in &universe.contracts {
        let Ok(close) = chrono::DateTime::parse_from_rfc3339(&contract.end_date) else {
            continue;
        };
        let minutes = live::window::estimate_window_minutes(&contract.window_description);
        let minutes = if minutes > 0.0 { minutes } else { 60.0 };
        let close_ms = close.timestamp_millis();
        let open_ms = close_ms - (minutes * 60_000.0).round() as i64;
        start_ms = start_ms.min(open_ms);
        end_ms = end_ms.max(close_ms);
    }
    (start_ms, end_ms)
}

fn ensure_btc_history_covers(
    label: &str,
    btc: &backtest::btc_history::BTCHistory,
    required_start_ms: i64,
    required_end_ms: i64,
) {
    if let Some(message) =
        btc_history_coverage_error(label, btc, required_start_ms, required_end_ms)
    {
        eprintln!("{message}");
        std::process::exit(1);
    }
}

fn btc_history_coverage_error(
    label: &str,
    btc: &backtest::btc_history::BTCHistory,
    required_start_ms: i64,
    required_end_ms: i64,
) -> Option<String> {
    if btc.n_ticks() < 50 {
        return Some(format!(
            "{label}: not enough BTC ticks ({} < 50)",
            btc.n_ticks()
        ));
    }
    let first = btc.first_timestamp_ms();
    let last = btc.last_timestamp_ms();
    if first > required_start_ms + 1_000 || last < required_end_ms {
        return Some(format!(
            "{label}: BTC tape covers {} → {}, but strategy window needs {} → {}",
            fmt_utc_ms(first),
            fmt_utc_ms(last),
            fmt_utc_ms(required_start_ms),
            fmt_utc_ms(required_end_ms),
        ));
    }
    None
}

fn fmt_utc_ms(ts_ms: i64) -> String {
    use chrono::TimeZone;
    chrono::Utc
        .timestamp_millis_opt(ts_ms)
        .single()
        .map(|d| d.to_rfc3339())
        .unwrap_or_else(|| ts_ms.to_string())
}

fn btc_updown_slug_step_seconds(window_minutes: Option<f64>) -> Option<i64> {
    let target = window_minutes?;
    if (target - 5.0).abs() <= 1e-6 {
        Some(5 * 60)
    } else if (target - 15.0).abs() <= 1e-6 {
        Some(15 * 60)
    } else {
        None
    }
}

fn btc_updown_slugs_for_window(
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    step_s: i64,
) -> Vec<String> {
    let start_s = start.timestamp();
    let end_exclusive_s = end.timestamp() + 3_600;
    let mut t = start_s - start_s.rem_euclid(step_s);
    let mut slugs = Vec::new();
    while t < end_exclusive_s {
        if t + step_s > start_s {
            let prefix = if step_s == 300 {
                "btc-updown-5m"
            } else {
                "btc-updown-15m"
            };
            slugs.push(format!("{prefix}-{t}"));
        }
        t += step_s;
    }
    slugs
}

async fn fetch_gamma_historical_markets_for_window(
    gamma: &data::gamma::GammaClient,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    window_minutes: Option<f64>,
    label: &str,
) -> anyhow::Result<Vec<data::models::Market>> {
    if let Some(step_s) = btc_updown_slug_step_seconds(window_minutes) {
        let slugs = btc_updown_slugs_for_window(start, end, step_s);
        eprintln!("{label}: fetching {} BTC candle slug(s)", slugs.len());
        let markets = gamma.fetch_markets_by_slugs(&slugs, true).await?;
        if !markets.is_empty() {
            return Ok(markets);
        }
    }

    let metadata_start = start - chrono::Duration::hours(1);
    let metadata_end = end + chrono::Duration::hours(2);
    eprintln!("{label}: fetching historical markets ending {metadata_start} -> {metadata_end}");
    gamma
        .fetch_markets_by_end_date_range(metadata_start, metadata_end, true)
        .await
}

#[allow(clippy::too_many_arguments)]
async fn cmd_harness_sweep(
    settings: &config::Settings,
    start: &str,
    end: Option<&str>,
    bankroll: f64,
    cache_dir: Option<&str>,
    btc_csv: Option<&str>,
    latency_ms: u64,
    conf: Vec<f64>,
    z: Vec<f64>,
    edge: Vec<f64>,
    ev_buffer: Vec<f64>,
    min_price: Vec<f64>,
    max_price: Vec<f64>,
    settlement_cutoff_minutes: Vec<f64>,
    settlement_min_abs_move_usd: Vec<f64>,
    settlement_guard_minutes: Vec<f64>,
    settlement_sigma_buffer: Vec<f64>,
    max_reversion_count: Vec<u64>,
    min_reversion_count: Vec<u64>,
    micro_max_spread: Vec<f64>,
    micro_min_depth: Vec<f64>,
    micro_min_pressure: Vec<f64>,
    position_pct: f64,
    max_per_market_usd: f64,
    max_total_exposure_usd: f64,
    max_projected_stressed_drawdown_pct: Vec<f64>,
    degraded_after_losses: Vec<u64>,
    degraded_after_drawdown_pct: Vec<f64>,
    degraded_min_z: Vec<f64>,
    degraded_max_price: Vec<f64>,
    degraded_force_taker: bool,
    also_maker: bool,
    maker_only: bool,
    zone_mode: backtest::sweep::ZoneMode,
    taker_only: bool,
    top: usize,
    threads: usize,
    checkpoint: Option<&str>,
    resume: bool,
    report_json: Option<&str>,
    trades_json: Option<&str>,
    trade_features_json: Option<&str>,
    require_causal_tag: Vec<String>,
    deny_causal_tag: Vec<String>,
    window_minutes: Option<f64>,
    adaptive_health_rearm_minutes: f64,
    continuous: bool,
    atomic_parquet: bool,
) {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};

    let start_dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(start) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--start must be RFC3339: {e}");
            std::process::exit(2);
        }
    };
    let end_dt = match end {
        Some(e) => match DateTime::parse_from_rfc3339(e) {
            Ok(d) => d.with_timezone(&Utc),
            Err(err) => {
                eprintln!("--end must be RFC3339: {err}");
                std::process::exit(2);
            }
        },
        None => start_dt,
    };
    let mut hours = Vec::new();
    let mut cur = start_dt;
    while cur <= end_dt {
        hours.push(cur);
        cur += ChronoDuration::hours(1);
    }

    if !(position_pct.is_finite() && position_pct > 0.0) {
        eprintln!("--position-pct must be a positive finite number");
        std::process::exit(2);
    }
    if !(max_per_market_usd.is_finite() && max_per_market_usd > 0.0) {
        eprintln!("--max-per-market-usd must be a positive finite number");
        std::process::exit(2);
    }
    if !(max_total_exposure_usd.is_finite() && max_total_exposure_usd > 0.0) {
        eprintln!("--max-total-exposure-usd must be a positive finite number");
        std::process::exit(2);
    }
    if max_projected_stressed_drawdown_pct.is_empty()
        || max_projected_stressed_drawdown_pct
            .iter()
            .any(|cap| !(cap.is_finite() && (0.0..=1.0).contains(cap)))
    {
        eprintln!("--max-projected-stressed-drawdown-pct must contain finite values in [0, 1]");
        std::process::exit(2);
    }
    if degraded_after_losses.is_empty() {
        eprintln!("--degraded-after-losses must contain at least one integer");
        std::process::exit(2);
    }
    if degraded_after_drawdown_pct.is_empty()
        || degraded_after_drawdown_pct
            .iter()
            .any(|v| !(v.is_finite() && (0.0..=1.0).contains(v)))
    {
        eprintln!("--degraded-after-drawdown-pct must contain finite values in [0, 1]");
        std::process::exit(2);
    }
    if degraded_min_z.is_empty() || degraded_min_z.iter().any(|v| !(v.is_finite() && *v >= 0.0)) {
        eprintln!("--degraded-min-z must contain finite non-negative values");
        std::process::exit(2);
    }
    if degraded_max_price.is_empty()
        || degraded_max_price
            .iter()
            .any(|v| !(v.is_finite() && (0.0..=1.0).contains(v)))
    {
        eprintln!("--degraded-max-price must contain finite values in [0, 1]");
        std::process::exit(2);
    }
    if maker_only && taker_only {
        eprintln!("--maker-only and --taker-only are mutually exclusive");
        std::process::exit(2);
    }
    if settlement_cutoff_minutes.is_empty()
        || settlement_cutoff_minutes
            .iter()
            .any(|v| !(v.is_finite() && *v >= 0.0))
    {
        eprintln!("--settlement-cutoff-minutes must contain finite non-negative values");
        std::process::exit(2);
    }
    if max_reversion_count.is_empty() {
        eprintln!("--max-reversion-count must contain at least one integer");
        std::process::exit(2);
    }
    if min_reversion_count.is_empty() {
        eprintln!("--min-reversion-count must contain at least one integer");
        std::process::exit(2);
    }
    let adaptive_rearm_after_s = match adaptive_health_rearm_minutes {
        m if !m.is_finite() || m < 0.0 => {
            eprintln!("--adaptive-health-rearm-minutes must be a finite non-negative number");
            std::process::exit(2);
        }
        m if m > 0.0 => Some(m * 60.0),
        _ => None,
    };
    let selectivity = match parse_selectivity_filter(&require_causal_tag, &deny_causal_tag) {
        Ok(filter) => filter,
        Err(e) => {
            eprintln!("causal selectivity parse failed: {e:#}");
            std::process::exit(2);
        }
    };

    // Build the variant grid.
    let mut base = backtest::strategies::StrategyVariant::baseline();
    base.position_pct = position_pct;
    base.max_per_market_usd = max_per_market_usd;
    base.max_projected_stressed_drawdown_pct = max_projected_stressed_drawdown_pct[0];
    base.selectivity = selectivity.clone();
    let grid = backtest::sweep::SweepGrid {
        base,
        conf,
        z,
        edge,
        ev_buffer,
        min_price,
        max_price,
        settlement_cutoff_minutes,
        settlement_min_abs_move_usd,
        settlement_guard_minutes,
        settlement_sigma_buffer,
        max_reversion_count,
        min_reversion_count,
        micro_max_spread,
        micro_min_depth,
        micro_min_pressure,
        max_projected_stressed_drawdown_pct,
        degraded_after_losses,
        degraded_after_drawdown_pct,
        degraded_min_z,
        degraded_max_price,
        degraded_force_taker,
        selectivity: vec![selectivity],
        also_maker,
        maker_only,
        taker_only,
        zone_mode,
    };
    let variants = grid.variants();
    if variants.is_empty() {
        eprintln!("empty parameter grid (check --conf/--z/--edge/--ev-buffer)");
        std::process::exit(2);
    }
    tracing::info!(variants = variants.len(), "sweep grid built");

    // Universe + tape (same as cmd_harness)
    let cache_dir_path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(backtest::pmxt::PMXTv2Loader::default_cache_dir);
    let loader = backtest::pmxt::PMXTv2Loader::new(&cache_dir_path);
    if atomic_parquet {
        eprintln!("pmxt: atomic parquet mode enabled; hours download/replay/delete inside harness");
    } else {
        for &h in &hours {
            eprintln!("pmxt: ensuring archive hour {h}");
            if let Err(e) = loader.download_hour(h, false).await {
                eprintln!("download {} failed: {e}", h);
                std::process::exit(1);
            }
        }
    }
    let cache_dir_path_for_meta = cache_dir_path.clone();
    let gamma_cache_path = cache_dir_path_for_meta.join("gamma_market_cache.json");
    let cached_markets: std::collections::BTreeMap<String, data::models::Market> =
        match std::fs::read_to_string(&gamma_cache_path) {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(_) => Default::default(),
        };
    if cached_markets.is_empty() {
        eprintln!(
            "harness-sweep has no cached Gamma metadata at {}; run `harness --allow-gamma-fetch` once to hydrate it",
            gamma_cache_path.display()
        );
        std::process::exit(1);
    }
    eprintln!(
        "harness-sweep: using cached Gamma metadata from {} ({} markets)",
        gamma_cache_path.display(),
        cached_markets.len()
    );
    let markets: Vec<data::models::Market> = cached_markets.values().cloned().collect();

    let mut contracts = data::scanner::scan_candle_markets_for_backtest(&markets, 0.0);
    contracts.retain(|c| c.asset == "BTC");
    filter_contracts_by_window_minutes(&mut contracts, window_minutes, "harness-sweep");
    let start_ts = start_dt.timestamp() as f64;
    let end_ts = end_dt.timestamp() as f64 + 3600.0;
    contracts.retain(|c| {
        let close_t = chrono::DateTime::parse_from_rfc3339(&c.end_date)
            .map(|d| d.timestamp() as f64)
            .unwrap_or(0.0);
        let window_minutes = live::window::estimate_window_minutes(&c.window_description);
        let window_minutes = if window_minutes > 0.0 {
            window_minutes
        } else {
            60.0
        };
        let open_t = close_t - window_minutes * 60.0;
        close_t > start_ts && open_t < end_ts
    });
    let universe = backtest::harness::CandleUniverse { contracts };
    if universe.contracts.is_empty() {
        eprintln!("no candle contracts in archive window");
        std::process::exit(1);
    }
    tracing::info!(
        contracts = universe.contracts.len(),
        "harness universe loaded"
    );
    let (btc_required_start_ms, btc_required_end_ms) = btc_required_range_ms(
        &universe,
        start_dt.timestamp_millis(),
        end_dt.timestamp_millis() + 3_600_000,
    );

    // BTC tape
    let mut btc = backtest::btc_history::BTCHistory::new();
    if let Some(p) = btc_csv {
        if let Err(e) = btc.load_csv(p) {
            eprintln!("BTC CSV load failed: {e}");
            std::process::exit(1);
        }
    } else {
        let pad_ms = 3_600_000;
        let start_ms = btc_required_start_ms - pad_ms;
        let end_ms = btc_required_end_ms + pad_ms;
        match btc
            .load_from_binance(start_ms, end_ms, "BTCUSDT", "1s")
            .await
        {
            Ok(n) if n > 100 => tracing::info!(rows = n, interval = "1s", "BTC klines"),
            _ => {
                btc = backtest::btc_history::BTCHistory::new();
                if let Err(e) = btc
                    .load_from_binance(start_ms, end_ms, "BTCUSDT", "1m")
                    .await
                {
                    eprintln!("Binance fetch failed: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
    ensure_btc_history_covers(
        "harness-sweep",
        &btc,
        btc_required_start_ms,
        btc_required_end_ms,
    );

    let shared_dir = std::env::var("PMXT_DISTILLED_DIR")
        .ok()
        .or_else(|| {
            let p = std::path::PathBuf::from(backtest::distill::SHARED_CACHE_DIR);
            if p.exists() {
                Some(backtest::distill::SHARED_CACHE_DIR.to_string())
            } else {
                None
            }
        })
        .map(std::path::PathBuf::from);
    // Checkpoint setup. If --checkpoint <dir> is set:
    //   - Existing dir + non-empty + no --resume → bail (avoid mixing runs).
    //   - Existing dir + empty OR --resume passed → use it.
    //   - Missing dir → create it.
    // SIGINT handler sets `stop_flag` so the harness exits between hours.
    let checkpoint_dir = if let Some(p) = checkpoint {
        let path = std::path::PathBuf::from(p);
        if path.is_dir() {
            let has_state = std::fs::read_dir(&path)
                .map(|it| {
                    it.flatten()
                        .any(|e| e.file_name().to_string_lossy().ends_with(".json"))
                })
                .unwrap_or(false);
            if has_state && !resume {
                eprintln!(
                    "checkpoint dir {} contains existing state; pass --resume to continue, \
                     or pick a fresh dir to start over.",
                    path.display(),
                );
                std::process::exit(2);
            }
        } else if path.exists() {
            eprintln!(
                "--checkpoint {} exists but isn't a directory",
                path.display()
            );
            std::process::exit(2);
        }
        Some(path)
    } else {
        None
    };
    let stop_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let f = stop_flag.clone();
        tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM");
            let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .expect("install SIGINT");
            tokio::select! {
                _ = term.recv() => tracing::warn!("SIGTERM received — sweep will pause after current hour"),
                _ = int.recv() => tracing::warn!("SIGINT received — sweep will pause after current hour"),
            }
            f.store(true, std::sync::atomic::Ordering::Relaxed);
        });
    }

    let cfg = backtest::harness::HarnessConfig {
        hours,
        universe,
        btc_history: std::sync::Arc::new(btc),
        bankroll_usd: bankroll,
        max_total_exposure_usd,
        min_order_size_shares: settings.live_min_order_size_shares,
        cache_dir: cache_dir_path,
        latency: backtest::l2_replay::StaticLatencyConfig {
            insert_ms: latency_ms,
        },
        breaker_cfg: live::breaker::BreakerConfig::from_settings(settings),
        adaptive_rearm_after_s,
        shared_distilled_dir: shared_dir,
        threads: if threads == 0 { None } else { Some(threads) },
        checkpoint_dir: checkpoint_dir.clone(),
        stop_flag: Some(stop_flag.clone()),
        continuous,
        delete_downloaded_parquet_after_hour: atomic_parquet,
    };

    eprintln!(
        "harness-sweep: replaying {} contract(s), {} variant(s), {} hour(s)",
        cfg.universe.contracts.len(),
        variants.len(),
        cfg.hours.len(),
    );
    println!(
        "\nRunning sweep over {} variants × {} hours…\n",
        variants.len(),
        cfg.hours.len()
    );
    if let Some(d) = &checkpoint_dir {
        println!(
            "Checkpoint: {} (touch {}/PAUSE or send SIGINT to pause cleanly between hours)\n",
            d.display(),
            d.display(),
        );
    }
    match backtest::harness::run_harness(&cfg, &variants).await {
        Ok(runs) => {
            if let Some(path) = report_json {
                let report = backtest::experiment::ExperimentReport::from_harness(
                    "harness_sweep",
                    &cfg,
                    &runs,
                );
                if let Err(e) = backtest::experiment::write_report_atomic(path, &report) {
                    eprintln!("write report {path}: {e}");
                    std::process::exit(1);
                }
                println!("Experiment report: {path}");
            }
            if let Some(path) = trades_json {
                let variants: Vec<_> = runs
                    .iter()
                    .enumerate()
                    .map(|(idx, run)| {
                        serde_json::json!({
                            "variant_index": idx,
                            "strategy_name": &run.variant.name,
                            "risk_profile": run.variant.risk_profile(),
                            "strategy_params": serde_json::to_value(&run.variant)
                                .unwrap_or(serde_json::Value::Null),
                            "summary": {
                                "trades": run.results.n_trades(),
                                "wins": run.results.n_wins(),
                                "losses": run.results.n_losses(),
                                "win_rate": run.results.win_rate(),
                                "total_pnl": run.results.total_pnl(),
                                "avg_pnl": run.results.avg_pnl(),
                                "total_fees": run.results.total_fees(),
                                "fills_success": run.results.fills_success,
                                "fills_failed": run.results.fills_failed,
                                "unresolved_fills": run.results.unresolved_fills.len(),
                            },
                            "trades": &run.results.trades,
                            "unresolved_fills": &run.results.unresolved_fills,
                        })
                    })
                    .collect();
                let report = serde_json::json!({
                    "schema_version": 1,
                    "generated_at": chrono::Utc::now().to_rfc3339(),
                    "mode": "harness_sweep_trades",
                    "start": start_dt.to_rfc3339(),
                    "end": end_dt.to_rfc3339(),
                    "bankroll_usd": cfg.bankroll_usd,
                    "max_total_exposure_usd": cfg.max_total_exposure_usd,
                    "latency_ms": cfg.latency.insert_ms,
                    "window_minutes": window_minutes,
                    "continuous": continuous,
                    "variants": variants,
                });
                if let Err(e) = write_json_atomic(path, &report, true) {
                    eprintln!("write trade report {path}: {e}");
                    std::process::exit(1);
                }
                println!("Trade report: {path}");
            }
            if let Some(path) = trade_features_json {
                let mut rows = Vec::new();
                for (variant_index, run) in runs.iter().enumerate() {
                    let params_hash = strategy::spec::stable_json_hash(&run.variant);
                    for (trade_index, trade) in run.results.trades.iter().enumerate() {
                        let regime = &trade.decision.regime;
                        let causal_tags: std::collections::BTreeMap<_, _> =
                            regime.causal_tags().into_iter().collect();
                        rows.push(serde_json::json!({
                            "variant_index": variant_index,
                            "trade_index": trade_index,
                            "strategy_name": &run.variant.name,
                            "params_hash": &params_hash,
                            "risk_profile": run.variant.risk_profile(),
                            "decision_timestamp_s": trade.fill.order.timestamp_s,
                            "fill_timestamp_s": trade.fill.fill_timestamp_s,
                            "intent_id": &trade.fill.order.intent_id,
                            "condition_id": &trade.fill.order.condition_id,
                            "token_id": &trade.fill.order.token_id,
                            "side": &trade.fill.order.side,
                            "order_type": &trade.fill.order.order_type,
                            "limit_price": trade.fill.order.limit_price,
                            "fill_price": trade.fill.fill_price,
                            "filled_size": trade.fill.filled_size,
                            "fee": trade.fill.fee,
                            "slippage": trade.fill.slippage,
                            "book_age_ms": trade.fill.book_age_ms,
                            "fill_reason": &trade.fill.reason,
                            "open_btc": trade.open_btc,
                            "close_btc": trade.close_btc,
                            "local_direction": &trade.local_direction,
                            "actual_direction": &trade.actual_direction,
                            "resolution_source": &trade.resolution_source,
                            "resolution_disagreed": trade.resolution_disagreed,
                            "won": trade.won,
                            "pnl": trade.pnl,
                            "pnl_after_fee": trade.pnl_after_fee,
                            "decision": {
                                "direction": &trade.decision.direction,
                                "confidence": trade.decision.confidence,
                                "z_score": trade.decision.z_score,
                                "zone": &trade.decision.zone,
                                "fair_value": trade.decision.fair_value,
                                "market_price": trade.decision.market_price,
                                "edge": trade.decision.edge,
                                "minutes_remaining": trade.decision.minutes_remaining,
                                "yes_no_vig": trade.decision.yes_no_vig,
                            },
                            "regime": {
                                "zone": &regime.zone,
                                "direction": &regime.direction,
                                "price_bucket": &regime.price_bucket,
                                "edge_bucket": &regime.edge_bucket,
                                "z_bucket": &regime.z_bucket,
                                "confidence_bucket": &regime.confidence_bucket,
                                "volatility_bucket": &regime.volatility_bucket,
                                "reversion_bucket": &regime.reversion_bucket,
                                "reversion_count": regime.reversion_count,
                                "minutes_remaining_bucket": &regime.minutes_remaining_bucket,
                                "key": regime.key(),
                            },
                            "causal_tags": causal_tags,
                        }));
                    }
                }
                let report = serde_json::json!({
                    "schema_version": 1,
                    "generated_at": chrono::Utc::now().to_rfc3339(),
                    "mode": "harness_sweep_trade_features",
                    "start": start_dt.to_rfc3339(),
                    "end": end_dt.to_rfc3339(),
                    "bankroll_usd": cfg.bankroll_usd,
                    "max_total_exposure_usd": cfg.max_total_exposure_usd,
                    "latency_ms": cfg.latency.insert_ms,
                    "window_minutes": window_minutes,
                    "continuous": continuous,
                    "row_count": rows.len(),
                    "notes": [
                        "Each row is a resolved trade emitted after the simulated fill and before scoring against BTC close.",
                        "Feature fields come from CandleDecision and DecisionRegime computed at decision time.",
                        "Use fold boundaries outside this file for train/OOS splits; do not shuffle timestamps for validation."
                    ],
                    "rows": rows,
                });
                if let Err(e) = write_json_atomic(path, &report, true) {
                    eprintln!("write trade feature report {path}: {e}");
                    std::process::exit(1);
                }
                println!("Trade feature report: {path}");
            }
            // Sort by PnL descending; trim to top N.
            let mut sorted = runs;
            sorted.sort_by(|a, b| {
                b.results
                    .total_pnl()
                    .partial_cmp(&a.results.total_pnl())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            // Filter out variants with zero trades (no signal under those gates)
            // and report the top N positive variants.
            let positive: Vec<_> = sorted
                .iter()
                .filter(|r| r.results.n_trades() > 0)
                .cloned()
                .collect();
            let limit = top.min(positive.len());
            println!("Top {} variants by PnL (variants with ≥1 trade):\n", limit);
            println!("{}", backtest::harness::render_table(&positive[..limit]));
            let zero_count = sorted.iter().filter(|r| r.results.n_trades() == 0).count();
            println!(
                "\n{} of {} variants produced 0 trades (gates too strict for the universe).",
                zero_count,
                sorted.len(),
            );
        }
        Err(e) => {
            eprintln!("sweep failed: {e}");
            std::process::exit(1);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn cmd_harness(
    settings: &config::Settings,
    start: &str,
    end: Option<&str>,
    bankroll: f64,
    max_total_exposure_usd: f64,
    cache_dir: Option<&str>,
    btc_csv: Option<&str>,
    latency_ms: u64,
    threads: usize,
    checkpoint: Option<&str>,
    resume: bool,
    max_contracts: Option<usize>,
    window_minutes: Option<f64>,
    allow_gamma_fetch: bool,
    metadata_only: bool,
    report_json: Option<&str>,
    adaptive_health_rearm_minutes: f64,
    continuous: bool,
    atomic_parquet: bool,
) {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};

    let start_dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(start) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--start must be RFC3339 (e.g. 2026-04-26T10:00:00Z): {e}");
            std::process::exit(2);
        }
    };
    let end_dt = match end {
        Some(e) => match DateTime::parse_from_rfc3339(e) {
            Ok(d) => d.with_timezone(&Utc),
            Err(err) => {
                eprintln!("--end must be RFC3339: {err}");
                std::process::exit(2);
            }
        },
        None => start_dt,
    };
    if end_dt < start_dt {
        eprintln!("--end must be ≥ --start");
        std::process::exit(2);
    }
    if !(max_total_exposure_usd.is_finite() && max_total_exposure_usd > 0.0) {
        eprintln!("--max-total-exposure-usd must be a positive finite number");
        std::process::exit(2);
    }
    let adaptive_rearm_after_s = match adaptive_health_rearm_minutes {
        m if !m.is_finite() || m < 0.0 => {
            eprintln!("--adaptive-health-rearm-minutes must be a finite non-negative number");
            std::process::exit(2);
        }
        m if m > 0.0 => Some(m * 60.0),
        _ => None,
    };

    // Build the hour list (inclusive).
    let mut hours = Vec::new();
    let mut cur = start_dt;
    let one_hour = ChronoDuration::hours(1);
    while cur <= end_dt {
        hours.push(cur);
        cur += one_hour;
    }

    // 1. Download/cache PMXT hours, then hydrate historical Gamma metadata
    //    by end-date range. Gamma's active feed only reflects the present.
    let cache_dir_path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(backtest::pmxt::PMXTv2Loader::default_cache_dir);
    let loader = backtest::pmxt::PMXTv2Loader::new(&cache_dir_path);
    if metadata_only {
        eprintln!("pmxt: metadata-only mode enabled; skipping archive downloads and replay");
    } else if atomic_parquet {
        eprintln!("pmxt: atomic parquet mode enabled; hours download/replay/delete inside harness");
    } else {
        for &h in &hours {
            eprintln!("pmxt: ensuring archive hour {h}");
            if let Err(e) = loader.download_hour(h, false).await {
                eprintln!("download {} failed: {e}", h);
                std::process::exit(1);
            }
        }
    }

    // Cache parsed Markets to disk keyed by condition_id so subsequent
    // harness runs are near-instant.
    let cache_dir_path_for_meta = cache_dir_path.clone();
    let gamma_cache_path = cache_dir_path_for_meta.join("gamma_market_cache.json");
    let mut cached_markets: std::collections::BTreeMap<String, data::models::Market> =
        match std::fs::read_to_string(&gamma_cache_path) {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(_) => Default::default(),
        };
    if allow_gamma_fetch {
        let gamma = data::gamma::GammaClient::new(&settings.poly_gamma_url);
        let new_markets = match fetch_gamma_historical_markets_for_window(
            &gamma,
            start_dt,
            end_dt,
            window_minutes,
            "gamma",
        )
        .await
        {
            Ok(markets) => markets,
            Err(e) => {
                eprintln!("Gamma historical metadata lookup failed: {e}");
                std::process::exit(1);
            }
        };
        let fetched = new_markets.len();
        let candle_markets = data::scanner::scan_candle_markets_for_backtest(&new_markets, 0.0);
        let mut merged = 0usize;
        for contract in candle_markets {
            if contract.asset != "BTC" {
                continue;
            }
            if !window_minutes
                .map(|target| {
                    (live::window::estimate_window_minutes(&contract.window_description) - target)
                        .abs()
                        <= 1e-6
                })
                .unwrap_or(true)
            {
                continue;
            }
            if cached_markets
                .get(&contract.market.condition_id)
                .map(gamma_market_needs_refresh)
                .unwrap_or(true)
            {
                merged += 1;
            }
            cached_markets.insert(
                contract.market.condition_id.clone(),
                contract.market.clone(),
            );
        }
        eprintln!(
            "gamma: fetched {fetched} historical market(s), merged {merged} BTC candle market(s)"
        );
        if merged > 0 {
            if let Err(e) = write_json_atomic(&gamma_cache_path, &cached_markets, false) {
                eprintln!(
                    "write Gamma cache {} failed: {e}",
                    gamma_cache_path.display()
                );
                std::process::exit(1);
            }
        }
    } else {
        eprintln!(
            "harness: using cached Gamma metadata from {}",
            gamma_cache_path.display()
        );
    }
    if cached_markets.is_empty() {
        eprintln!(
            "harness has no cached Gamma metadata at {}; pass --allow-gamma-fetch to build it",
            gamma_cache_path.display()
        );
        std::process::exit(1);
    }
    if metadata_only {
        let summary = serde_json::json!({
            "mode": "metadata_only",
            "gamma_cache_path": gamma_cache_path.display().to_string(),
            "markets": cached_markets.len(),
            "start": start_dt.to_rfc3339(),
            "end": end_dt.to_rfc3339(),
            "window_minutes": window_minutes,
        });
        if let Some(path) = report_json {
            if let Err(e) = write_json_atomic(path, &summary, true) {
                eprintln!("write metadata-only report {path}: {e}");
                std::process::exit(1);
            }
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&summary).expect("serialize metadata-only summary")
        );
        return;
    }
    let markets: Vec<data::models::Market> = cached_markets.values().cloned().collect();
    tracing::info!(markets = markets.len(), "Gamma metadata loaded");

    // 2. Filter to candle markets via the existing scanner regex. For the
    //    first iteration of the harness we restrict to BTC underliers only —
    //    the BTC tape is the only history we load (alts would need their own
    //    feed pulled separately). Plenty of room to widen later.
    let mut contracts = data::scanner::scan_candle_markets_for_backtest(&markets, 0.0);
    contracts.retain(|c| c.asset == "BTC");
    filter_contracts_by_window_minutes(&mut contracts, window_minutes, "harness");
    // Keep candles whose [open_time, close_time] OVERLAPS the harness hours.
    let start_ts = start_dt.timestamp() as f64;
    let end_ts = end_dt.timestamp() as f64 + 3600.0;
    let pre_filter_count = contracts.len();
    contracts.retain(|c| {
        let close_t = chrono::DateTime::parse_from_rfc3339(&c.end_date)
            .map(|d| d.timestamp() as f64)
            .unwrap_or(0.0);
        let window_minutes = live::window::estimate_window_minutes(&c.window_description);
        let window_minutes = if window_minutes > 0.0 {
            window_minutes
        } else {
            60.0
        };
        let open_t = close_t - window_minutes * 60.0;
        close_t > start_ts && open_t < end_ts
    });
    tracing::info!(
        pre = pre_filter_count,
        kept = contracts.len(),
        "candle window filter",
    );
    contracts.sort_by(|a, b| {
        a.end_date
            .cmp(&b.end_date)
            .then_with(|| a.market.condition_id.cmp(&b.market.condition_id))
    });
    if matches!(max_contracts, Some(0)) {
        eprintln!("harness --max-contracts must be greater than zero");
        std::process::exit(2);
    }
    if let Some(limit) = max_contracts {
        contracts.truncate(limit);
    }
    let universe = backtest::harness::CandleUniverse { contracts };
    if universe.contracts.is_empty() {
        eprintln!(
            "no candle contracts in archive window — checked {} markets, found 0 candles in [{start}, {end}]",
            markets.len(),
            start = start,
            end = end.unwrap_or(start),
        );
        std::process::exit(1);
    }
    tracing::info!(
        contracts = universe.contracts.len(),
        "harness universe loaded"
    );
    let (btc_required_start_ms, btc_required_end_ms) = btc_required_range_ms(
        &universe,
        start_dt.timestamp_millis(),
        end_dt.timestamp_millis() + 3_600_000,
    );

    // 2. BTC tape.
    let mut btc = backtest::btc_history::BTCHistory::new();
    if let Some(p) = btc_csv {
        match btc.load_csv(p) {
            Ok(n) => tracing::info!(rows = n, "BTC CSV loaded"),
            Err(e) => {
                eprintln!("BTC CSV load failed: {e}");
                std::process::exit(1);
            }
        }
    } else {
        // Pad ±1 hour around the harness window so the resolver has open/close
        // prices on the boundary. Use 1-second klines for intra-window
        // momentum detection; falls back to 1m if Binance rate-limits.
        let pad_ms = 3_600_000;
        let start_ms = btc_required_start_ms - pad_ms;
        let end_ms = btc_required_end_ms + pad_ms;
        match btc
            .load_from_binance(start_ms, end_ms, "BTCUSDT", "1s")
            .await
        {
            Ok(n) if n > 100 => tracing::info!(rows = n, interval = "1s", "BTC klines pulled"),
            Ok(_) | Err(_) => {
                tracing::warn!("1s klines unavailable; falling back to 1m");
                btc = backtest::btc_history::BTCHistory::new();
                match btc
                    .load_from_binance(start_ms, end_ms, "BTCUSDT", "1m")
                    .await
                {
                    Ok(n) => tracing::info!(rows = n, interval = "1m", "BTC klines pulled"),
                    Err(e) => {
                        eprintln!("Binance kline fetch failed: {e}");
                        std::process::exit(1);
                    }
                }
            }
        }
    }
    ensure_btc_history_covers("harness", &btc, btc_required_start_ms, btc_required_end_ms);

    let shared_dir = std::env::var("PMXT_DISTILLED_DIR")
        .ok()
        .or_else(|| {
            let p = std::path::PathBuf::from(backtest::distill::SHARED_CACHE_DIR);
            if p.exists() {
                Some(backtest::distill::SHARED_CACHE_DIR.to_string())
            } else {
                None
            }
        })
        .map(std::path::PathBuf::from);
    let checkpoint_dir = if let Some(p) = checkpoint {
        let path = std::path::PathBuf::from(p);
        if path.is_dir() {
            let has_state = std::fs::read_dir(&path)
                .map(|it| {
                    it.flatten()
                        .any(|e| e.file_name().to_string_lossy().ends_with(".json"))
                })
                .unwrap_or(false);
            if has_state && !resume {
                eprintln!(
                    "checkpoint dir {} contains existing state; pass --resume to continue, \
                     or pick a fresh dir to start over.",
                    path.display(),
                );
                std::process::exit(2);
            }
        } else if path.exists() {
            eprintln!(
                "--checkpoint {} exists but isn't a directory",
                path.display()
            );
            std::process::exit(2);
        }
        Some(path)
    } else {
        None
    };
    let stop_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let f = stop_flag.clone();
        tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM");
            let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .expect("install SIGINT");
            tokio::select! {
                _ = term.recv() => tracing::warn!("SIGTERM received — harness will pause after current hour"),
                _ = int.recv() => tracing::warn!("SIGINT received — harness will pause after current hour"),
            }
            f.store(true, std::sync::atomic::Ordering::Relaxed);
        });
    }
    let cfg = backtest::harness::HarnessConfig {
        hours,
        universe,
        btc_history: std::sync::Arc::new(btc),
        bankroll_usd: bankroll,
        max_total_exposure_usd,
        min_order_size_shares: settings.live_min_order_size_shares,
        cache_dir: cache_dir_path,
        latency: backtest::l2_replay::StaticLatencyConfig {
            insert_ms: latency_ms,
        },
        breaker_cfg: live::breaker::BreakerConfig::from_settings(settings),
        adaptive_rearm_after_s,
        shared_distilled_dir: shared_dir,
        threads: if threads == 0 { None } else { Some(threads) },
        checkpoint_dir: checkpoint_dir.clone(),
        stop_flag: Some(stop_flag),
        continuous,
        delete_downloaded_parquet_after_hour: atomic_parquet,
    };

    let variants = backtest::strategies::default_variants();
    eprintln!(
        "harness: replaying {} contract(s), {} variant(s), {} hour(s)",
        cfg.universe.contracts.len(),
        variants.len(),
        cfg.hours.len(),
    );
    if let Some(d) = &checkpoint_dir {
        eprintln!("harness: checkpoint dir {}", d.display());
    }
    match backtest::harness::run_harness(&cfg, &variants).await {
        Ok(runs) => {
            if let Some(path) = report_json {
                let report =
                    backtest::experiment::ExperimentReport::from_harness("harness", &cfg, &runs);
                if let Err(e) = backtest::experiment::write_report_atomic(path, &report) {
                    eprintln!("write report {path}: {e}");
                    std::process::exit(1);
                }
                println!("Experiment report: {path}");
            }
            println!(
                "\nHarness — {start} → {end} bankroll=${bankroll:.0} latency={latency_ms}ms variants={}\n",
                runs.len(),
                start = start,
                end = end.unwrap_or(start),
            );
            println!("{}", backtest::harness::render_table(&runs));
            println!("{}", backtest::harness::render_zone_breakdown(&runs));
        }
        Err(e) => {
            eprintln!("harness failed: {e}");
            std::process::exit(1);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn cmd_sweep(
    sessions: &[String],
    bankroll: f64,
    position_pct: f64,
    max_per_market_usd: f64,
    min_trades: u64,
    show_zones: bool,
    grid: Option<sweep::strategy::GridConfig>,
    top: usize,
    report_json: Option<&str>,
) {
    if sessions.is_empty() {
        eprintln!("--session is required (repeat for multiple files)");
        std::process::exit(2);
    }
    let paths: Vec<std::path::PathBuf> = sessions.iter().map(std::path::PathBuf::from).collect();
    let strats = match grid {
        Some(grid) => sweep::strategy::grid_strategies(&grid),
        None => sweep::strategy::default_strategies(),
    };
    if strats.is_empty() {
        eprintln!("empty sweep strategy set (check --conf/--z/--edge/--ev-buffer)");
        std::process::exit(2);
    }
    let runs = match sweep::run_sweep(
        &paths,
        &strats,
        bankroll,
        position_pct,
        max_per_market_usd,
        min_trades,
    ) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("sweep failed: {e}");
            std::process::exit(1);
        }
    };

    // Sort by P&L descending so the strongest variants are at the top.
    let mut sorted = runs.clone();
    sorted.sort_by(|a, b| {
        b.realized_pnl
            .partial_cmp(&a.realized_pnl)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if let Some(path) = report_json {
        if let Err(e) = write_json_atomic(path, &sorted, true) {
            eprintln!("write sweep report {path}: {e}");
            std::process::exit(1);
        }
        println!("Sweep report: {path}");
    }
    let shown: Vec<_> = if top == 0 {
        sorted.iter().collect()
    } else {
        sorted.iter().take(top).collect()
    };
    let shown_runs: Vec<_> = shown.into_iter().cloned().collect();

    println!(
        "\nSweep over {} session file(s) — bankroll=${bankroll:.0}, position_pct={position_pct:.4}, max_per_market=${max_per_market_usd:.2}, min_trades={min_trades}, variants={}\n",
        paths.len(),
        strats.len()
    );
    println!("{}", sweep::render_table(&shown_runs));
    if show_zones {
        println!("{}", sweep::render_zone_breakdown(&shown_runs));
    }

    // Surface data-gap warnings.
    let total_resolved_each: Vec<u64> = runs.iter().map(|r| r.trades).collect();
    let max_resolved = *total_resolved_each.iter().max().unwrap_or(&0);
    if max_resolved < min_trades {
        println!(
            "\n⚠  insufficient sample: best variant has only {max_resolved} resolved trade(s); \
             collect ≥{min_trades} before drawing conclusions."
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn cmd_eval_cache(
    settings: &config::Settings,
    start: &str,
    end: Option<&str>,
    cache_dir: Option<&str>,
    btc_csv: Option<&str>,
    output: &str,
    window_minutes: Option<f64>,
    allow_gamma_fetch: bool,
) {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};

    let start_dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(start) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--start must be RFC3339: {e}");
            std::process::exit(2);
        }
    };
    let end_dt = match end {
        Some(e) => match DateTime::parse_from_rfc3339(e) {
            Ok(d) => d.with_timezone(&Utc),
            Err(err) => {
                eprintln!("--end must be RFC3339: {err}");
                std::process::exit(2);
            }
        },
        None => start_dt,
    };
    if end_dt < start_dt {
        eprintln!("--end must be >= --start");
        std::process::exit(2);
    }

    let mut hours = Vec::new();
    let mut cur = start_dt;
    while cur <= end_dt {
        hours.push(cur);
        cur += ChronoDuration::hours(1);
    }

    let cache_dir_path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(backtest::pmxt::PMXTv2Loader::default_cache_dir);
    let loader = backtest::pmxt::PMXTv2Loader::new(&cache_dir_path);
    for &h in &hours {
        eprintln!("pmxt: ensuring archive hour {h}");
        if let Err(e) = loader.download_hour(h, false).await {
            eprintln!("download {h} failed: {e}");
            std::process::exit(1);
        }
    }

    let gamma_cache_path = cache_dir_path.join("gamma_market_cache.json");
    let mut cached_markets: std::collections::BTreeMap<String, data::models::Market> =
        match std::fs::read_to_string(&gamma_cache_path) {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(_) => Default::default(),
        };
    if allow_gamma_fetch {
        let gamma = data::gamma::GammaClient::new(&settings.poly_gamma_url);
        let new_markets = match fetch_gamma_historical_markets_for_window(
            &gamma,
            start_dt,
            end_dt,
            window_minutes,
            "gamma",
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Gamma historical metadata lookup failed: {e}");
                std::process::exit(1);
            }
        };
        let fetched = new_markets.len();
        let candle_markets = data::scanner::scan_candle_markets_for_backtest(&new_markets, 0.0);
        let mut merged = 0usize;
        for contract in candle_markets {
            if contract.asset != "BTC" {
                continue;
            }
            if !window_minutes
                .map(|target| {
                    (live::window::estimate_window_minutes(&contract.window_description) - target)
                        .abs()
                        <= 1e-6
                })
                .unwrap_or(true)
            {
                continue;
            }
            cached_markets.insert(
                contract.market.condition_id.clone(),
                contract.market.clone(),
            );
            merged += 1;
        }
        eprintln!(
            "gamma: fetched {fetched} historical market(s), merged {merged} BTC candle market(s)"
        );
        if merged > 0 {
            if let Err(e) = write_json_atomic(&gamma_cache_path, &cached_markets, false) {
                eprintln!(
                    "write Gamma cache {} failed: {e}",
                    gamma_cache_path.display()
                );
                std::process::exit(1);
            }
        }
    } else {
        eprintln!(
            "eval-cache: using cached Gamma metadata from {}",
            gamma_cache_path.display()
        );
    }
    if cached_markets.is_empty() {
        eprintln!(
            "eval-cache has no cached Gamma metadata at {}; pass --allow-gamma-fetch to build it",
            gamma_cache_path.display()
        );
        std::process::exit(1);
    }

    let markets: Vec<data::models::Market> = cached_markets.values().cloned().collect();
    let mut contracts = data::scanner::scan_candle_markets_for_backtest(&markets, 0.0);
    contracts.retain(|c| c.asset == "BTC");
    filter_contracts_by_window_minutes(&mut contracts, window_minutes, "eval-cache");
    let start_ts = start_dt.timestamp() as f64;
    let end_ts = end_dt.timestamp() as f64 + 3600.0;
    contracts.retain(|c| {
        let close_t = chrono::DateTime::parse_from_rfc3339(&c.end_date)
            .map(|d| d.timestamp() as f64)
            .unwrap_or(0.0);
        let minutes = live::window::estimate_window_minutes(&c.window_description);
        let minutes = if minutes > 0.0 { minutes } else { 60.0 };
        let open_t = close_t - minutes * 60.0;
        close_t > start_ts && open_t < end_ts
    });
    contracts.sort_by(|a, b| {
        a.end_date
            .cmp(&b.end_date)
            .then_with(|| a.market.condition_id.cmp(&b.market.condition_id))
    });
    let universe = backtest::harness::CandleUniverse { contracts };
    if universe.contracts.is_empty() {
        eprintln!("no candle contracts in archive window");
        std::process::exit(1);
    }
    let (btc_required_start_ms, btc_required_end_ms) = btc_required_range_ms(
        &universe,
        start_dt.timestamp_millis(),
        end_dt.timestamp_millis() + 3_600_000,
    );

    let mut btc = backtest::btc_history::BTCHistory::new();
    if let Some(path) = btc_csv {
        match btc.load_csv(path) {
            Ok(n) => tracing::info!(rows = n, "BTC CSV loaded"),
            Err(e) => {
                eprintln!("BTC CSV load failed: {e}");
                std::process::exit(1);
            }
        }
    } else {
        let pad_ms = 3_600_000;
        let start_ms = btc_required_start_ms - pad_ms;
        let end_ms = btc_required_end_ms + pad_ms;
        match btc
            .load_from_binance(start_ms, end_ms, "BTCUSDT", "1s")
            .await
        {
            Ok(n) if n > 100 => tracing::info!(rows = n, interval = "1s", "BTC klines pulled"),
            _ => {
                btc = backtest::btc_history::BTCHistory::new();
                if let Err(e) = btc
                    .load_from_binance(start_ms, end_ms, "BTCUSDT", "1m")
                    .await
                {
                    eprintln!("Binance kline fetch failed: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
    ensure_btc_history_covers(
        "eval-cache",
        &btc,
        btc_required_start_ms,
        btc_required_end_ms,
    );

    let shared_distilled_dir = std::env::var("PMXT_DISTILLED_DIR")
        .ok()
        .or_else(|| {
            let p = std::path::PathBuf::from(backtest::distill::SHARED_CACHE_DIR);
            if p.exists() {
                Some(backtest::distill::SHARED_CACHE_DIR.to_string())
            } else {
                None
            }
        })
        .map(std::path::PathBuf::from);
    let cfg = backtest::eval_cache::EvalCacheConfig {
        hours,
        universe,
        btc_history: std::sync::Arc::new(btc),
        cache_dir: cache_dir_path,
        shared_distilled_dir,
        output: std::path::PathBuf::from(output),
    };
    match backtest::eval_cache::write_eval_cache(cfg) {
        Ok(summary) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&summary).expect("serialize eval-cache summary")
            );
        }
        Err(e) => {
            eprintln!("eval-cache failed: {e:#}");
            std::process::exit(1);
        }
    }
}

fn f64opt(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}

fn u32opt(v: &serde_json::Value, key: &str) -> Option<u32> {
    v.get(key)
        .and_then(|x| x.as_u64())
        .and_then(|x| u32::try_from(x).ok())
}

#[cfg(test)]
mod replay_validation_tests {
    use super::*;

    #[test]
    fn runtime_event_updates_validation_config_from_inline_strategy() {
        let zone = strategy::decision::ZoneConfig {
            min_ev_buffer: 0.12,
            settlement_min_abs_move_usd: 25.0,
            ..strategy::decision::ZoneConfig::default()
        };
        let event = serde_json::json!({
            "cat": "system",
            "type": "runtime_strategy",
            "zone_config": zone,
            "min_confidence": 0.42,
            "min_edge": 0.03,
            "skip_dead_zone": false,
            "settlement_alignment_ready": false
        });
        let mut cfg = ReplayValidationConfig::default();

        cfg.apply_runtime_strategy_event(&event);

        assert_eq!(cfg.zone_config.min_ev_buffer, 0.12);
        assert_eq!(cfg.zone_config.settlement_min_abs_move_usd, 25.0);
        assert_eq!(cfg.min_confidence, 0.42);
        assert_eq!(cfg.min_edge, 0.03);
        assert!(!cfg.skip_dead_zone);
        assert!(!cfg.settlement_alignment_ready);
    }

    #[test]
    fn promotion_source_path_ignores_suffix_flags() {
        assert_eq!(
            promotion_path_from_runtime_source("promotion:/tmp/promotion.json+settlement_floor"),
            Some("/tmp/promotion.json")
        );
        assert_eq!(promotion_path_from_runtime_source("settings"), None);
    }

    #[test]
    fn replay_validation_reads_logged_reversion_count_as_u32() {
        let event = serde_json::json!({ "reversion_count": 3 });
        assert_eq!(u32opt(&event, "reversion_count"), Some(3));

        let too_large = serde_json::json!({ "reversion_count": u64::from(u32::MAX) + 1 });
        assert_eq!(u32opt(&too_large, "reversion_count"), None);

        let missing = serde_json::json!({});
        assert_eq!(u32opt(&missing, "reversion_count"), None);
    }

    fn wallet_balances(
        pusd: f64,
        exchange_allowance: f64,
        neg_risk_allowance: f64,
    ) -> data::wallet::WalletBalances {
        data::wallet::WalletBalances {
            address: "0x0000000000000000000000000000000000000000".to_string(),
            pusd,
            pusd_allowance_exchange: exchange_allowance,
            pusd_allowance_neg_risk_exchange: neg_risk_allowance,
            pol: 1.0,
            ..Default::default()
        }
    }

    #[test]
    fn live_required_wallet_usd_uses_worst_case_first_order() {
        let mut settings = config::Settings::from_env();
        settings.bankroll_usd = 100.0;
        settings.candle_position_pct = 0.10;
        settings.candle_vol_high_multiplier = 1.5;
        settings.candle_vol_extreme_multiplier = 2.0;
        settings.max_position_per_market_usd = 20.0;
        settings.candle_max_price = 0.90;
        settings.live_min_order_size_shares = 5.0;

        let balances = wallet_balances(100.0, 100.0, 100.0);

        assert_eq!(live_required_wallet_usd(&settings, &balances), 22.0);
    }

    #[test]
    fn live_required_wallet_usd_rejects_sub_minimum_canary_budget() {
        let mut settings = config::Settings::from_env();
        settings.bankroll_usd = 1.0;
        settings.candle_position_pct = 0.10;
        settings.candle_vol_high_multiplier = 1.0;
        settings.candle_vol_extreme_multiplier = 1.0;
        settings.max_position_per_market_usd = 1.0;
        settings.candle_max_price = 0.90;
        settings.live_min_order_size_shares = 5.0;

        let balances = wallet_balances(1.0, 1.0, 1.0);
        let configured = live_configured_order_budget_usd(&settings, &balances);
        let floor = live_min_order_budget_usd(&settings);

        assert_eq!(configured, 1.0);
        assert_eq!(floor, 4.5);
        assert_eq!(live_required_wallet_usd(&settings, &balances), 4.95);
        assert!(!live_wallet_covers_budget(
            &balances,
            live_required_wallet_usd(&settings, &balances)
        ));
    }

    #[test]
    fn live_wallet_budget_blocks_underfunded_configured_bankroll() {
        let mut settings = config::Settings::from_env();
        settings.bankroll_usd = 100.0;
        settings.candle_position_pct = 0.10;
        settings.candle_vol_high_multiplier = 1.5;
        settings.candle_vol_extreme_multiplier = 2.0;
        settings.max_position_per_market_usd = 20.0;
        settings.candle_max_price = 0.90;
        settings.live_min_order_size_shares = 5.0;

        let balances = wallet_balances(1.0, 1.0, 1.0);
        let required = live_required_wallet_usd(&settings, &balances);

        assert_eq!(required, 22.0);
        assert!(!live_wallet_covers_budget(&balances, required));
    }

    #[tokio::test]
    async fn rolling_history_dry_run_builds_fold_manifest() {
        let summary = run_rolling_history(RollingHistoryInput {
            start: "2026-05-24T00:00:00Z".to_string(),
            end: "2026-05-24T03:00:00Z".to_string(),
            out_dir: std::path::PathBuf::from("/tmp/poly_rolling_test"),
            cache_root: None,
            btc_csv: Some("/tmp/btc.csv".to_string()),
            bankroll: 100.0,
            latency_ms: 50,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 2,
            max_folds: None,
            profile: "a_plus5m".to_string(),
            require_causal_tag: vec!["direction=down".to_string()],
            deny_causal_tag: Vec::new(),
            zone_mode: "early".to_string(),
            promotion_output: None,
            execute: false,
            delete_after_process: true,
            atomic_parquet: true,
            preflight_pmxt_hours: false,
            stop_at_first_missing_hour: false,
            require_full_folds: false,
            min_fold_trades: 20,
            min_fold_target_events: 1,
            min_fold_top_trades: None,
            min_promotion_trades: None,
            min_promotion_daily_trades: None,
            min_promotion_profitable_reports: None,
            min_promotion_losses: None,
            max_cache_gb: 1.0,
            min_neighbor_observations: None,
            min_neighbor_positive_rate: 0.60,
            max_pbo: 0.50,
            min_median_oos_percentile: 0.80,
        })
        .await
        .unwrap();

        assert_eq!(summary["mode"], "dry_run");
        assert_eq!(summary["atomic_parquet"], true);
        assert_eq!(summary["folds"].as_array().unwrap().len(), 2);
        assert!(summary["folds"][0]["hydrate_args"]
            .as_array()
            .unwrap()
            .iter()
            .any(|arg| arg.as_str() == Some("--atomic-parquet")));
        assert!(summary["folds"][0]["sweep_args"]
            .as_array()
            .unwrap()
            .iter()
            .any(|arg| arg.as_str() == Some("--atomic-parquet")));
        assert!(summary["promotion_args"]
            .as_array()
            .unwrap()
            .iter()
            .any(|arg| arg.as_str() == Some("--max-pbo")));
        assert!(summary["promotion_args"]
            .as_array()
            .unwrap()
            .windows(2)
            .any(
                |pair| pair[0].as_str() == Some("--min-median-oos-percentile")
                    && pair[1].as_str() == Some("0.8")
            ));
        assert_eq!(summary["coverage_policy"]["min_fold_target_events"], 1);
        assert_eq!(summary["coverage_policy"]["min_fold_top_trades"], 20);
        assert!(summary["folds"][0]["coverage"].is_null());
        assert!(summary["folds"][0]["sweep_args"]
            .as_array()
            .unwrap()
            .iter()
            .any(|arg| arg.as_str() == Some("0.50,0.70,0.90,1.10")));
        assert!(summary["folds"][0]["sweep_args"]
            .as_array()
            .unwrap()
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--require-causal-tag")
                && pair[1].as_str() == Some("direction=down")));
    }

    #[tokio::test]
    async fn rolling_history_down_neighbor_profile_is_taker_only() {
        let summary = run_rolling_history(RollingHistoryInput {
            start: "2026-05-24T00:00:00Z".to_string(),
            end: "2026-05-24T01:00:00Z".to_string(),
            out_dir: std::path::PathBuf::from("/tmp/poly_rolling_down_neighbor_test"),
            cache_root: None,
            btc_csv: Some("/tmp/btc.csv".to_string()),
            bankroll: 100.0,
            latency_ms: 50,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 2,
            max_folds: None,
            profile: "a_plus5m_down_reversion_guard_neighbors".to_string(),
            require_causal_tag: vec!["direction=down".to_string()],
            deny_causal_tag: Vec::new(),
            zone_mode: "all".to_string(),
            promotion_output: None,
            execute: false,
            delete_after_process: true,
            atomic_parquet: true,
            preflight_pmxt_hours: false,
            stop_at_first_missing_hour: false,
            require_full_folds: false,
            min_fold_trades: 20,
            min_fold_target_events: 1,
            min_fold_top_trades: None,
            min_promotion_trades: Some(7),
            min_promotion_daily_trades: Some(0),
            min_promotion_profitable_reports: Some(1),
            min_promotion_losses: Some(0),
            max_cache_gb: 1.0,
            min_neighbor_observations: Some(2),
            min_neighbor_positive_rate: 0.60,
            max_pbo: 0.50,
            min_median_oos_percentile: 0.80,
        })
        .await
        .unwrap();
        let args = summary["folds"][0]["sweep_args"].as_array().unwrap();

        assert!(args.iter().any(|arg| arg.as_str() == Some("--taker-only")));
        assert!(!args.iter().any(|arg| arg.as_str() == Some("--also-maker")));
        assert!(args.iter().any(|arg| arg.as_str() == Some("0.60")));
        assert!(args.iter().any(|arg| arg.as_str() == Some("0.70,0.80")));
        assert!(args
            .iter()
            .any(|arg| arg.as_str() == Some("0.05,0.07,0.09")));
        assert!(args
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--require-causal-tag")
                && pair[1].as_str() == Some("direction=down")));
        let promotion_args = summary["promotion_args"].as_array().unwrap();
        assert!(promotion_args
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--min-trades") && pair[1].as_str() == Some("7")));
        assert!(promotion_args
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--min-daily-trades")
                && pair[1].as_str() == Some("0")));
        assert!(promotion_args
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--min-profitable-reports")
                && pair[1].as_str() == Some("1")));
        assert!(promotion_args
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--min-losses") && pair[1].as_str() == Some("0")));
        assert!(promotion_args.windows(2).any(|pair| pair[0].as_str()
            == Some("--min-neighbor-observations")
            && pair[1].as_str() == Some("2")));
        assert_eq!(summary["promotion_policy"]["min_trades"], 7);
        assert_eq!(summary["promotion_policy"]["min_daily_trades"], 0);
        assert_eq!(summary["promotion_policy"]["min_profitable_reports"], 1);
        assert_eq!(summary["promotion_policy"]["min_losses"], 0);
        assert_eq!(summary["promotion_policy"]["min_neighbor_observations"], 2);
    }

    #[test]
    fn rolling_history_confidence_profile_tightens_recent_loss_regime() {
        let profile = rolling_history_profile("a_plus5m_down_reversion_guard_confidence").unwrap();

        assert_eq!(profile.conf, "0.60,0.70");
        assert_eq!(profile.z, "0.70,0.80");
        assert_eq!(profile.edge, "0.07,0.09");
        assert!(profile.taker_only);
        assert!(profile.degraded_force_taker);
    }

    #[tokio::test]
    async fn rolling_history_require_full_folds_drops_partial_tail() {
        let summary = run_rolling_history(RollingHistoryInput {
            start: "2026-05-24T00:00:00Z".to_string(),
            end: "2026-05-24T04:00:00Z".to_string(),
            out_dir: std::path::PathBuf::from("/tmp/poly_rolling_full_fold_test"),
            cache_root: None,
            btc_csv: None,
            bankroll: 100.0,
            latency_ms: 50,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 2,
            max_folds: None,
            profile: "a_plus5m".to_string(),
            require_causal_tag: Vec::new(),
            deny_causal_tag: Vec::new(),
            zone_mode: "early".to_string(),
            promotion_output: None,
            execute: false,
            delete_after_process: true,
            atomic_parquet: false,
            preflight_pmxt_hours: false,
            stop_at_first_missing_hour: false,
            require_full_folds: true,
            min_fold_trades: 20,
            min_fold_target_events: 1,
            min_fold_top_trades: None,
            min_promotion_trades: None,
            min_promotion_daily_trades: None,
            min_promotion_profitable_reports: None,
            min_promotion_losses: None,
            max_cache_gb: 1.0,
            min_neighbor_observations: None,
            min_neighbor_positive_rate: 0.60,
            max_pbo: 0.50,
            min_median_oos_percentile: 0.80,
        })
        .await
        .unwrap();

        assert_eq!(summary["effective_end"], "2026-05-24T03:00:00+00:00");
        assert_eq!(summary["partial_final_fold_dropped"], true);
        assert_eq!(summary["folds"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn rolling_history_coverage_marks_sparse_fold() {
        let report = coverage_test_report(vec![
            coverage_test_variant("low_trades", 4, 6_631_020, 1.0),
            coverage_test_variant("fewer_trades", 2, 6_631_020, 2.0),
        ]);

        let coverage = sweep_report_coverage_from_report(&report, 8, 10_000_000, 15);

        assert_eq!(coverage.status, "coverage_limited");
        assert_eq!(coverage.target_events, 6_631_020);
        assert_eq!(coverage.top_trades, 4);
        assert_eq!(coverage.top_variant.as_deref(), Some("low_trades"));
        let reason = coverage.reason.unwrap();
        assert!(reason.contains("target_events 6631020 below minimum 10000000"));
        assert!(reason.contains("top variant trades 4 below minimum 15"));
    }

    #[test]
    fn rolling_history_coverage_accepts_dense_fold() {
        let report = coverage_test_report(vec![
            coverage_test_variant("selected", 25, 27_000_000, 12.5),
            coverage_test_variant("neighbor", 18, 27_000_000, 8.0),
        ]);

        let coverage = sweep_report_coverage_from_report(&report, 8, 10_000_000, 15);

        assert_eq!(coverage.status, "ok");
        assert!(coverage.reason.is_none());
        assert_eq!(coverage.top_trades, 25);
        assert_eq!(coverage.top_variant.as_deref(), Some("selected"));
        assert_eq!(coverage.target_events_per_hour, 3_375_000.0);
    }

    #[test]
    fn rolling_history_delete_fold_cache_is_scoped() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tmp.path().join("cache");
        let fold_cache = cache_root.join("fold_001_test");
        std::fs::create_dir_all(&fold_cache).unwrap();
        std::fs::write(fold_cache.join("sample.parquet"), b"owned by test").unwrap();

        delete_fold_cache(&cache_root, &fold_cache).unwrap();
        assert!(!fold_cache.exists());

        let not_fold = cache_root.join("not_a_fold");
        std::fs::create_dir_all(&not_fold).unwrap();
        assert!(delete_fold_cache(&cache_root, &not_fold).is_err());
        assert!(not_fold.exists());
    }

    fn coverage_test_report(
        variants: Vec<backtest::experiment::VariantReport>,
    ) -> backtest::experiment::ExperimentReport {
        let mut src = data::manifest::DataSourceManifest::new("pmxt_v2_archive", "order_book_l2");
        src.complete = true;
        src.row_count = Some(8);
        backtest::experiment::ExperimentReport {
            schema_version: 1,
            generated_at: "2026-05-31T00:00:00Z".to_string(),
            label: "coverage-test".to_string(),
            mode: "harness-sweep".to_string(),
            start: "2026-05-22T16:00:00Z".to_string(),
            end: "2026-05-22T23:00:00Z".to_string(),
            bankroll_usd: 100.0,
            latency_ms: 50,
            market_catalog: data::catalog::MarketCatalog::default(),
            data_manifest: data::manifest::DataManifest::new(vec![src], Vec::new()),
            variants,
        }
    }

    fn coverage_test_variant(
        name: &str,
        trades: usize,
        events_seen: u64,
        total_pnl: f64,
    ) -> backtest::experiment::VariantReport {
        backtest::experiment::VariantReport {
            strategy: strategy::spec::StrategySpec::new(
                "candle_momentum",
                "1",
                name,
                "position_pct=0.05",
            ),
            strategy_params: serde_json::json!({ "name": name }),
            trades,
            wins: trades,
            losses: 0,
            unresolved_fills: 0,
            execution_attempts: trades,
            fills_success: trades,
            fills_failed: 0,
            fill_rate: 1.0,
            reject_reasons: std::collections::BTreeMap::new(),
            breaker_tripped: false,
            breaker_reason: None,
            breaker_tripped_at_s: None,
            breaker_realized_drawdown_pct: 0.0,
            breaker_stressed_drawdown_pct: 0.0,
            diagnostics: backtest::resolver::BacktestDiagnostics {
                events_seen,
                ..backtest::resolver::BacktestDiagnostics::default()
            },
            win_rate: if trades > 0 { 1.0 } else { 0.0 },
            total_pnl,
            avg_pnl: if trades > 0 {
                total_pnl / trades as f64
            } else {
                0.0
            },
            total_fees: 0.0,
            sharpe_like: 0.0,
            by_zone: std::collections::BTreeMap::new(),
        }
    }
}
