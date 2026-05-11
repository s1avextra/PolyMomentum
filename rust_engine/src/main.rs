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
mod sweep;

use clap::{Parser, Subcommand};
use config::RuntimeMode;

#[derive(Parser, Debug)]
#[command(name = "polymomentum-engine", version, about = "PolyMomentum Rust trading engine")]
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
        /// BTC tick/kline CSV used as the virtual exchange price feed.
        #[arg(long)]
        btc_csv: String,
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
        /// Include both maker and taker fill model variants per cell.
        #[arg(long, default_value_t = true)]
        also_maker: bool,
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
        /// Restrict the candle universe to one window length, e.g. 5 for 5-minute candles.
        #[arg(long)]
        window_minutes: Option<f64>,
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
        /// PMXT v2 cache directory (otherwise pulled from PMXT_V2_CACHE_DIR).
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
        /// Write a reproducible JSON experiment report to this path.
        #[arg(long)]
        report_json: Option<String>,
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
        /// Minimum trades for a variant before its numbers are considered
        /// statistically meaningful.
        #[arg(long, default_value_t = 30)]
        min_trades: u64,
        /// Show per-zone breakdown for each strategy.
        #[arg(long, default_value_t = false)]
        zones: bool,
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
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_tracing(&cli.log);
    let settings = config::Settings::from_env();

    match cli.command {
        Command::Live { mode, i_understand_live, promotion_artifact } => {
            let mut settings = settings.clone();
            apply_promotion_override(&mut settings, promotion_artifact);
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
            allow_gamma_fetch,
            max_contracts,
            window_minutes,
            promotion_artifact,
        } => {
            let mut settings = settings.clone();
            apply_promotion_override(&mut settings, promotion_artifact);
            cmd_live_replay(
                &settings,
                &start,
                end.as_deref(),
                cache_dir.as_deref(),
                &btc_csv,
                bankroll,
                latency_ms,
                session_log_dir.as_deref(),
                allow_download,
                allow_gamma_fetch,
                max_contracts,
                window_minutes,
            )
            .await;
        }
        Command::Preflight { mode, i_understand_live, promotion_artifact } => {
            let mut settings = settings.clone();
            apply_promotion_override(&mut settings, promotion_artifact);
            let report = run_startup_preflight(&settings, mode, i_understand_live).await;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serialize preflight report")
            );
            if !report.ok {
                std::process::exit(2);
            }
        }
        Command::ReleaseManifest { mode, promotion_artifact } => {
            let mut settings = settings.clone();
            apply_promotion_override(&mut settings, promotion_artifact);
            let manifest = release::ReleaseManifest::capture(&settings, mode);
            println!(
                "{}",
                serde_json::to_string_pretty(&manifest).expect("serialize release manifest")
            );
        }
        Command::Scan { max_hours, min_liquidity } => {
            cmd_scan(&settings, max_hours, min_liquidity).await;
        }
        Command::Wallet { json } => cmd_wallet(&settings, json).await,
        Command::Clob { command } => cmd_clob(&settings, command).await,
        Command::Experiment { command } => cmd_experiment(command),
        Command::Diagnostics { command } => cmd_diagnostics(command),
        Command::Ctf { condition_id } => cmd_ctf(&settings, &condition_id).await,
        Command::ValidateReplay { path } => cmd_validate_replay(&path).await,
        Command::Sweep { session, bankroll, min_trades, zones } => {
            cmd_sweep(&session, bankroll, min_trades, zones);
        }
        Command::PmxtInfo { hour, cache_dir, sample } => {
            cmd_pmxt_info(&hour, cache_dir.as_deref(), sample).await;
        }
        Command::PmxtDownload { start, end, cache_dir } => {
            cmd_pmxt_download(&start, end.as_deref(), cache_dir.as_deref()).await;
        }
        Command::Distill { input, output, candle_cids, hour } => {
            cmd_distill(&settings, &input, output.as_deref(), candle_cids.as_deref(), hour.as_deref()).await;
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
            also_maker,
            top,
            threads,
            checkpoint,
            resume,
            report_json,
            window_minutes,
        } => {
            let conf = parse_csv_floats(&conf);
            let zs = parse_csv_floats(&z);
            let edges = parse_csv_floats(&edge);
            let evs = parse_csv_floats(&ev_buffer);
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
                also_maker,
                top,
                threads,
                checkpoint.as_deref(),
                resume,
                report_json.as_deref(),
                window_minutes,
            ).await;
        }
        Command::Harness {
            start,
            end,
            bankroll,
            cache_dir,
            btc_csv,
            latency_ms,
            threads,
            checkpoint,
            resume,
            max_contracts,
            window_minutes,
            allow_gamma_fetch,
            report_json,
        } => {
            cmd_harness(&settings, &start, end.as_deref(), bankroll, cache_dir.as_deref(), btc_csv.as_deref(), latency_ms, threads, checkpoint.as_deref(), resume, max_contracts, window_minutes, allow_gamma_fetch, report_json.as_deref()).await;
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
            Ok(balances) if balances.live_ready() => release::PreflightCheck {
                name: "live_wallet",
                status: release::CheckStatus::Ok,
                detail: balances.live_ready_detail(),
            },
            Ok(balances) => release::PreflightCheck {
                name: "live_wallet",
                status: release::CheckStatus::Fail,
                detail: balances.live_ready_detail(),
            },
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
    btc_csv: &str,
    bankroll: f64,
    latency_ms: u64,
    session_log_dir: Option<&str>,
    allow_download: bool,
    allow_gamma_fetch: bool,
    max_contracts: Option<usize>,
    window_minutes: Option<f64>,
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
        cur = cur + ChronoDuration::hours(1);
    }

    let cache_dir_path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(
                std::env::var("PMXT_V2_CACHE_DIR")
                    .unwrap_or_else(|_| backtest::pmxt::DEFAULT_CACHE_DIR.to_string()),
            )
        });
    let loader = backtest::pmxt::PMXTv2Loader::new(&cache_dir_path);
    for &h in &hours {
        if allow_download {
            eprintln!("live-replay: ensuring PMXT archive hour {h}");
            if let Err(e) = loader.download_hour(h, false).await {
                eprintln!("download {h} failed: {e}");
                std::process::exit(1);
            }
        } else if !loader.is_cached(h) {
            eprintln!(
                "PMXT hour {h} is not cached in {}; pass --allow-download to fetch it",
                cache_dir_path.display()
            );
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
        let mut all_cids = std::collections::HashSet::new();
        for &h in &hours {
            eprintln!("live-replay: scanning condition_ids for {h}");
            match loader.distinct_condition_ids(h) {
                Ok(s) => all_cids.extend(s),
                Err(e) => {
                    eprintln!("read distinct cids for {h}: {e}");
                    std::process::exit(1);
                }
            }
        }
        let missing_cids: Vec<String> = all_cids
            .iter()
            .filter(|c| !cached_markets.contains_key(*c))
            .cloned()
            .collect();
        if !missing_cids.is_empty() {
            eprintln!(
                "live-replay: fetching Gamma metadata for {} missing condition_ids",
                missing_cids.len()
            );
            let gamma = data::gamma::GammaClient::new(&settings.poly_gamma_url);
            match gamma.fetch_markets_by_condition_ids(&missing_cids).await {
                Ok(markets) => {
                    for m in markets {
                        cached_markets.insert(m.condition_id.clone(), m);
                    }
                    if let Ok(s) = serde_json::to_string(&cached_markets) {
                        let _ = std::fs::write(&gamma_cache_path, s);
                    }
                }
                Err(e) => {
                    eprintln!("Gamma lookup failed: {e}");
                    std::process::exit(1);
                }
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
        std::process::exit(1);
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
        let window_minutes = if window_minutes > 0.0 { window_minutes } else { 60.0 };
        let open_t = close_t - window_minutes * 60.0;
        close_t > start_ts && open_t < end_ts
    });
    if contracts.is_empty() {
        eprintln!("live-replay found no BTC candle contracts in [{start}, {}]", end.unwrap_or(start));
        std::process::exit(1);
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
        std::process::exit(2);
    }
    eprintln!("live-replay: BTC candle contracts={}", contracts.len());

    let mut btc = backtest::btc_history::BTCHistory::new();
    if let Err(e) = btc.load_csv(btc_csv) {
        eprintln!("BTC CSV load failed: {e}");
        std::process::exit(1);
    }
    if btc.n_ticks() < 50 {
        eprintln!("not enough BTC ticks in {btc_csv} ({} < 50)", btc.n_ticks());
        std::process::exit(1);
    }
    let replay_start_ms = start_dt.timestamp_millis();
    let replay_end_ms = (end_dt + ChronoDuration::hours(1)).timestamp_millis();
    let btc_start_ms = btc.first_timestamp_ms();
    let btc_end_ms = btc.last_timestamp_ms();
    if btc_end_ms < replay_start_ms || btc_start_ms > replay_end_ms {
        eprintln!(
            "BTC CSV does not overlap replay window: btc_ms=[{btc_start_ms},{btc_end_ms}] replay_ms=[{replay_start_ms},{replay_end_ms}]"
        );
        std::process::exit(1);
    }

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
        universe: backtest::harness::CandleUniverse { contracts },
        btc_history: std::sync::Arc::new(btc),
        bankroll_usd: bankroll,
        cache_dir: cache_dir_path,
        session_log_dir,
        latency: backtest::l2_replay::StaticLatencyConfig { insert_ms: latency_ms },
        shared_distilled_dir: shared_dir,
        strategy: match live::replay::ReplayStrategy::load(settings) {
            Ok(strategy) => strategy,
            Err(e) => {
                eprintln!("live-replay strategy load failed: {e:#}");
                std::process::exit(2);
            }
        },
    };
    match live::replay::run_live_replay(cfg, settings).await {
        Ok(report) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serialize live replay report")
            );
        }
        Err(e) => {
            eprintln!("live-replay failed: {e:?}");
            std::process::exit(1);
        }
    }
}

async fn cmd_scan(s: &config::Settings, max_hours: f64, min_liquidity: f64) {
    let client = data::gamma::GammaClient::new(&s.poly_gamma_url);
    match client.fetch_markets_by_end_date(max_hours, min_liquidity).await {
        Ok(markets) => {
            let contracts =
                data::scanner::scan_candle_markets(&markets, max_hours, min_liquidity);
            println!("markets={} candle_contracts={}", markets.len(), contracts.len());
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
                println!("pusd_allow   ${:.2} CTF Exchange V2", b.pusd_allowance_exchange);
                println!(
                    "pusd_allow   ${:.2} Neg Risk CTF Exchange V2",
                    b.pusd_allowance_neg_risk_exchange
                );
                println!("usdc_e_allow ${:.2} Collateral Onramp", b.usdc_e_allowance_onramp);
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
            client.get_price(&token_id, &side.to_ascii_uppercase()).await
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
        Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap_or_else(|_| v.to_string())),
        Err(e) => {
            eprintln!("clob diagnostic failed: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_experiment(command: ExperimentCommand) {
    match command {
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
                max_zone_trade_share,
                require_complete_data: !allow_incomplete_data,
            };
            let artifact = match backtest::experiment::PromotionArtifact::from_report(&report_doc, gate) {
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
            let artifact =
                match backtest::experiment::PromotionArtifact::from_reports(&reports, gate, multi_gate)
                {
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
    }
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
    for line in reader.lines().map_while(|l| l.ok()) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
        if v.get("cat").and_then(|x| x.as_str()) != Some("signal") {
            continue;
        }
        if v.get("type").and_then(|x| x.as_str()) != Some("evaluation") {
            continue;
        }
        total += 1;

        // Build inputs
        let signal = strategy::momentum::MomentumSignal {
            direction: v.get("dir").and_then(|x| x.as_str()).unwrap_or("up").to_string(),
            confidence: f64opt(&v, "conf").unwrap_or(0.0),
            price_change: f64opt(&v, "chg").unwrap_or(0.0),
            price_change_pct: f64opt(&v, "chg_pct").unwrap_or(0.0),
            consistency: f64opt(&v, "cons").unwrap_or(0.0),
            minutes_elapsed: f64opt(&v, "elapsed_min").unwrap_or(0.0),
            minutes_remaining: f64opt(&v, "remaining_min").unwrap_or(0.0),
            current_price: f64opt(&v, "px").unwrap_or(0.0),
            open_price: f64opt(&v, "open").unwrap_or(0.0),
            z_score: f64opt(&v, "z").unwrap_or(0.0),
            reversion_count: 0,
        };
        let cfg = strategy::decision::ZoneConfig::default();
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
            strategy::decision::DEFAULT_MIN_CONFIDENCE,
            strategy::decision::DEFAULT_MIN_EDGE,
            true,
            &cfg,
            f64opt(&v, "cross_boost").unwrap_or(0.0),
        );
        let traded = matches!(res, strategy::decision::DecisionResult::Trade(_));
        let logged_decision_trade = v
            .get("decision_trade")
            .and_then(|x| x.as_bool())
            .or_else(|| v.get("traded").and_then(|x| x.as_bool()))
            .unwrap_or(false);
        if traded != logged_decision_trade {
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
            let stem = in_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            // expects polymarket_orderbook_YYYY-MM-DDTHH.parquet
            let h = stem
                .strip_prefix("polymarket_orderbook_")
                .and_then(|s| s.strip_suffix(".parquet"))
                .unwrap_or("");
            match chrono::NaiveDateTime::parse_from_str(
                &format!("{h}:00:00"),
                "%Y-%m-%dT%H:%M:%S",
            ) {
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
        text.split(|c| c == ',' || c == '\n' || c == ' ')
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
        candles
            .into_iter()
            .map(|c| c.market.condition_id)
            .collect()
    };
    tracing::info!(cids = cids.len(), "candle universe loaded for distill");

    let out_path = match output {
        Some(s) => std::path::PathBuf::from(s),
        None => {
            let dir = in_path.parent().unwrap_or_else(|| std::path::Path::new("."));
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
        .unwrap_or_else(|| std::path::PathBuf::from(backtest::pmxt::DEFAULT_CACHE_DIR));
    let loader = backtest::pmxt::PMXTv2Loader::new(&path);
    let mut cur = s;
    while cur <= e {
        if let Err(err) = loader.download_hour(cur, false).await {
            eprintln!("download {} failed: {err}", cur);
            std::process::exit(1);
        }
        cur = cur + ChronoDuration::hours(1);
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
        .unwrap_or_else(|| {
            std::path::PathBuf::from(
                std::env::var("PMXT_V2_CACHE_DIR")
                    .unwrap_or_else(|_| backtest::pmxt::DEFAULT_CACHE_DIR.to_string()),
            )
        });
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

#[allow(clippy::too_many_arguments)]
async fn cmd_harness_sweep(
    _settings: &config::Settings,
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
    also_maker: bool,
    top: usize,
    threads: usize,
    checkpoint: Option<&str>,
    resume: bool,
    report_json: Option<&str>,
    window_minutes: Option<f64>,
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
        cur = cur + ChronoDuration::hours(1);
    }

    // Build the variant grid.
    let grid = backtest::sweep::SweepGrid {
        base: backtest::strategies::StrategyVariant::baseline(),
        conf,
        z,
        edge,
        ev_buffer,
        also_maker,
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
        .unwrap_or_else(|| {
            std::path::PathBuf::from(
                std::env::var("PMXT_V2_CACHE_DIR")
                    .unwrap_or_else(|_| backtest::pmxt::DEFAULT_CACHE_DIR.to_string()),
            )
        });
    let loader = backtest::pmxt::PMXTv2Loader::new(&cache_dir_path);
    for &h in &hours {
        eprintln!("pmxt: ensuring archive hour {h}");
        if let Err(e) = loader.download_hour(h, false).await {
            eprintln!("download {} failed: {e}", h);
            std::process::exit(1);
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
        let window_minutes = if window_minutes > 0.0 { window_minutes } else { 60.0 };
        let open_t = close_t - window_minutes * 60.0;
        close_t > start_ts && open_t < end_ts
    });
    let universe = backtest::harness::CandleUniverse { contracts };
    if universe.contracts.is_empty() {
        eprintln!("no candle contracts in archive window");
        std::process::exit(1);
    }
    tracing::info!(contracts = universe.contracts.len(), "harness universe loaded");

    // BTC tape
    let mut btc = backtest::btc_history::BTCHistory::new();
    if let Some(p) = btc_csv {
        btc.load_csv(p).ok();
    } else {
        let pad_ms = 3_600_000;
        let start_ms = start_dt.timestamp_millis() - pad_ms;
        let end_ms = end_dt.timestamp_millis() + pad_ms;
        match btc.load_from_binance(start_ms, end_ms, "BTCUSDT", "1s").await {
            Ok(n) if n > 100 => tracing::info!(rows = n, interval = "1s", "BTC klines"),
            _ => {
                btc = backtest::btc_history::BTCHistory::new();
                if let Err(e) = btc.load_from_binance(start_ms, end_ms, "BTCUSDT", "1m").await {
                    eprintln!("Binance fetch failed: {e}");
                    std::process::exit(1);
                }
            }
        }
    }

    let shared_dir = std::env::var("PMXT_DISTILLED_DIR")
        .ok()
        .or_else(|| {
            let p = std::path::PathBuf::from(backtest::distill::SHARED_CACHE_DIR);
            if p.exists() { Some(backtest::distill::SHARED_CACHE_DIR.to_string()) } else { None }
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
                    it.flatten().any(|e| {
                        e.file_name()
                            .to_string_lossy()
                            .ends_with(".json")
                    })
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
            eprintln!("--checkpoint {} exists but isn't a directory", path.display());
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
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
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
        cache_dir: cache_dir_path,
        latency: backtest::l2_replay::StaticLatencyConfig { insert_ms: latency_ms },
        shared_distilled_dir: shared_dir,
        threads: if threads == 0 { None } else { Some(threads) },
        checkpoint_dir: checkpoint_dir.clone(),
        stop_flag: Some(stop_flag.clone()),
    };

    eprintln!(
        "harness-sweep: replaying {} contract(s), {} variant(s), {} hour(s)",
        cfg.universe.contracts.len(),
        variants.len(),
        cfg.hours.len(),
    );
    println!("\nRunning sweep over {} variants × {} hours…\n", variants.len(), cfg.hours.len());
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
            let positive: Vec<_> = sorted.iter().filter(|r| r.results.n_trades() > 0).cloned().collect();
            let limit = top.min(positive.len());
            println!("Top {} variants by PnL (variants with ≥1 trade):\n", limit);
            println!("{}", backtest::harness::render_table(&positive[..limit]));
            let zero_count = sorted.iter().filter(|r| r.results.n_trades() == 0).count();
            println!(
                "\n{} of {} variants produced 0 trades (gates too strict for the universe).",
                zero_count, sorted.len(),
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
    cache_dir: Option<&str>,
    btc_csv: Option<&str>,
    latency_ms: u64,
    threads: usize,
    checkpoint: Option<&str>,
    resume: bool,
    max_contracts: Option<usize>,
    window_minutes: Option<f64>,
    allow_gamma_fetch: bool,
    report_json: Option<&str>,
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

    // Build the hour list (inclusive).
    let mut hours = Vec::new();
    let mut cur = start_dt;
    let one_hour = ChronoDuration::hours(1);
    while cur <= end_dt {
        hours.push(cur);
        cur = cur + one_hour;
    }

    // 1. Discover candle universe directly from the parquet's distinct
    //    condition_ids. This is the only reliable way for HISTORICAL hours —
    //    Gamma's "active" feed only reflects the present.
    let cache_dir_path = cache_dir
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(
                std::env::var("PMXT_V2_CACHE_DIR")
                    .unwrap_or_else(|_| backtest::pmxt::DEFAULT_CACHE_DIR.to_string()),
            )
        });
    let loader = backtest::pmxt::PMXTv2Loader::new(&cache_dir_path);
    for &h in &hours {
        eprintln!("pmxt: ensuring archive hour {h}");
        if let Err(e) = loader.download_hour(h, false).await {
            eprintln!("download {} failed: {e}", h);
            std::process::exit(1);
        }
    }

    // Gamma lookup is the bottleneck (~50 cids/RTT). Cache the parsed Markets
    // to disk keyed by condition_id so subsequent harness runs are near-instant.
    let cache_dir_path_for_meta = cache_dir_path.clone();
    let gamma_cache_path = cache_dir_path_for_meta.join("gamma_market_cache.json");
    let mut cached_markets: std::collections::BTreeMap<String, data::models::Market> = match std::fs::read_to_string(&gamma_cache_path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
        Err(_) => Default::default(),
    };
    if allow_gamma_fetch {
        let mut all_cids: std::collections::HashSet<String> = std::collections::HashSet::new();
        for &h in &hours {
            eprintln!("pmxt: scanning condition_ids for {h}");
            match loader.distinct_condition_ids(h) {
                Ok(s) => all_cids.extend(s),
                Err(e) => {
                    eprintln!("read distinct cids for {}: {e}", h);
                    std::process::exit(1);
                }
            }
        }
        tracing::info!(cids = all_cids.len(), "distinct condition_ids in archive");
        let cid_vec: Vec<String> = all_cids
            .iter()
            .filter(|c| !cached_markets.contains_key(*c))
            .cloned()
            .collect();
        if !cid_vec.is_empty() {
            eprintln!("gamma: fetching metadata for {} condition_ids", cid_vec.len());
            tracing::info!(missing = cid_vec.len(), cached = cached_markets.len(), "Gamma cache miss; fetching");
            let gamma = data::gamma::GammaClient::new(&settings.poly_gamma_url);
            let new_markets = match gamma.fetch_markets_by_condition_ids(&cid_vec).await {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Gamma lookup failed: {e}");
                    std::process::exit(1);
                }
            };
            for m in new_markets {
                cached_markets.insert(m.condition_id.clone(), m);
            }
            if let Ok(s) = serde_json::to_string(&cached_markets) {
                let _ = std::fs::write(&gamma_cache_path, s);
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
        let window_minutes = if window_minutes > 0.0 { window_minutes } else { 60.0 };
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
    tracing::info!(contracts = universe.contracts.len(), "harness universe loaded");

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
        let start_ms = start_dt.timestamp_millis() - pad_ms;
        let end_ms = end_dt.timestamp_millis() + pad_ms;
        match btc.load_from_binance(start_ms, end_ms, "BTCUSDT", "1s").await {
            Ok(n) if n > 100 => tracing::info!(rows = n, interval = "1s", "BTC klines pulled"),
            Ok(_) | Err(_) => {
                tracing::warn!("1s klines unavailable; falling back to 1m");
                btc = backtest::btc_history::BTCHistory::new();
                match btc.load_from_binance(start_ms, end_ms, "BTCUSDT", "1m").await {
                    Ok(n) => tracing::info!(rows = n, interval = "1m", "BTC klines pulled"),
                    Err(e) => {
                        eprintln!("Binance kline fetch failed: {e}");
                        std::process::exit(1);
                    }
                }
            }
        }
    }
    if btc.n_ticks() < 50 {
        eprintln!("not enough BTC ticks ({} < 50)", btc.n_ticks());
        std::process::exit(1);
    }

    let shared_dir = std::env::var("PMXT_DISTILLED_DIR")
        .ok()
        .or_else(|| {
            let p = std::path::PathBuf::from(backtest::distill::SHARED_CACHE_DIR);
            if p.exists() { Some(backtest::distill::SHARED_CACHE_DIR.to_string()) } else { None }
        })
        .map(std::path::PathBuf::from);
    let checkpoint_dir = if let Some(p) = checkpoint {
        let path = std::path::PathBuf::from(p);
        if path.is_dir() {
            let has_state = std::fs::read_dir(&path)
                .map(|it| {
                    it.flatten().any(|e| {
                        e.file_name()
                            .to_string_lossy()
                            .ends_with(".json")
                    })
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
            eprintln!("--checkpoint {} exists but isn't a directory", path.display());
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
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
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
        cache_dir: cache_dir_path,
        latency: backtest::l2_replay::StaticLatencyConfig { insert_ms: latency_ms },
        shared_distilled_dir: shared_dir,
        threads: if threads == 0 { None } else { Some(threads) },
        checkpoint_dir: checkpoint_dir.clone(),
        stop_flag: Some(stop_flag),
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
                let report = backtest::experiment::ExperimentReport::from_harness(
                    "harness",
                    &cfg,
                    &runs,
                );
                if let Err(e) = backtest::experiment::write_report_atomic(path, &report) {
                    eprintln!("write report {path}: {e}");
                    std::process::exit(1);
                }
                println!("Experiment report: {path}");
            }
            println!(
                "\nHarness — {start}{} → {end} bankroll=${bankroll:.0} latency={latency_ms}ms variants={}\n",
                if end.is_some() { "" } else { "" },
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

fn cmd_sweep(sessions: &[String], bankroll: f64, min_trades: u64, show_zones: bool) {
    if sessions.is_empty() {
        eprintln!("--session is required (repeat for multiple files)");
        std::process::exit(2);
    }
    let paths: Vec<std::path::PathBuf> = sessions.iter().map(std::path::PathBuf::from).collect();
    let strats = sweep::strategy::default_strategies();
    let runs = match sweep::run_sweep(&paths, &strats, bankroll, min_trades) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("sweep failed: {e}");
            std::process::exit(1);
        }
    };

    // Sort by P&L descending so the strongest variants are at the top.
    let mut sorted = runs.clone();
    sorted.sort_by(|a, b| {
        b.realized_pnl.partial_cmp(&a.realized_pnl).unwrap_or(std::cmp::Ordering::Equal)
    });

    println!("\nSweep over {} session file(s) — bankroll=${bankroll:.0}, min_trades={min_trades}\n", paths.len());
    println!("{}", sweep::render_table(&sorted));
    if show_zones {
        println!("{}", sweep::render_zone_breakdown(&sorted));
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

fn f64opt(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}
