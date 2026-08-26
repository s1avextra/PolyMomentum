//! polymomentum-engine: unified Rust binary.
//!
//! The binary owns CLI parsing and command orchestration. Run with `--help` for
//! the authoritative command list; trading, replay, research, diagnostics, and
//! data-capture implementations live in the library modules.
//!
//! Environment-driven configuration. See `src/config.rs` for the full list of
//! variables; the runtime reads `.env` from the working directory if present.

#![forbid(unsafe_code)]

use anyhow::Context;
use clap::{Parser, Subcommand};
use polymomentum_engine::config::RuntimeMode;
use polymomentum_engine::{
    artifact, backtest, clob, config, data, live, monitoring, release, strategy, strategy_builder,
    sweep,
};
use sha2::{Digest, Sha256};

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
#[allow(clippy::large_enum_variant)]
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
    /// Generate a promotion artifact for the frozen band family from its
    /// fresh-gate and fill-replay evidence (hash-bound to the policy params).
    BandPromotionArtifact {
        /// Frozen policy JSON (BandPolicyParams shape).
        #[arg(long)]
        params: String,
        /// Fresh-gate verdict JSON (fresh_gate_public_v1 artifact).
        #[arg(long)]
        gate_artifact: String,
        /// Capture fill-replay evidence JSON.
        #[arg(long)]
        fill_artifact: String,
        /// Output promotion artifact path.
        #[arg(long)]
        output: String,
    },
    /// Derive (or create) CLOB L2 API credentials from the PRIVATE_KEY in an
    /// env file and append POLY_API_* lines to that same file atomically.
    /// Secret values never reach stdout or logs.
    DeriveApiCreds {
        /// Env file holding PRIVATE_KEY=...; POLY_API_* lines are written here.
        #[arg(long)]
        env_file: String,
        /// CLOB base URL.
        #[arg(long, default_value = "https://clob.polymarket.com")]
        base_url: String,
    },
    /// Smoke-test scanner: fetch candle markets, print summary.
    Scan {
        #[arg(long, default_value_t = 2.0)]
        max_hours: f64,
        #[arg(long, default_value_t = 100.0)]
        min_liquidity: f64,
    },
    /// Bounded read-only recorder for current BTC candle L2 market data.
    RecordBtcBooks {
        /// Optional UTC anchor for the first slug. Defaults to now, floored to the window.
        #[arg(long)]
        start: Option<String>,
        /// Candle window length. Currently supports 5 or 15 minutes.
        #[arg(long, default_value_t = 5.0)]
        window_minutes: f64,
        /// Number of consecutive candle windows to subscribe to.
        #[arg(long, default_value_t = 3)]
        windows: usize,
        /// Capture duration in seconds.
        #[arg(long, default_value_t = 60)]
        duration_seconds: u64,
        /// Output directory for gamma metadata, raw JSONL frames, and summary.
        #[arg(long)]
        out_dir: String,
    },
    /// Convert a record-btc-books capture into distilled replay-cache files.
    ConvertRecordedBtcBooks {
        /// Directory produced by record-btc-books.
        #[arg(long)]
        input_dir: String,
        /// Output directory for <hour>.v1.candles.jsonl.gz plus manifest.json.
        #[arg(long)]
        output_dir: String,
        /// Optional repeated condition allowlist. Raw capture validation still
        /// covers every market; only selected markets are retained for replay.
        #[arg(long = "condition-id")]
        condition_ids: Vec<String>,
    },
    /// Audit CLOB websocket delay and token coverage in a record-btc-books capture.
    ForwardLatencyAudit {
        /// Directory produced by record-btc-books.
        #[arg(long)]
        input_dir: String,
        /// Output JSON path. Defaults to <input-dir>/forward_latency_audit.json.
        #[arg(long)]
        output: Option<String>,
        /// Maximum acceptable p99 CLOB message delay for promotion evidence.
        #[arg(long, default_value_t = 500.0)]
        max_p99_delay_ms: f64,
        /// Maximum acceptable whole-stream receive gap for capture-health gating.
        /// Per-token gaps are diagnostic because Polymarket book updates are event-driven.
        #[arg(long, default_value_t = 2_000.0)]
        max_token_gap_ms: f64,
        /// Minimum observed events before a token participates in the update-gap gate.
        #[arg(long, default_value_t = 100)]
        min_gap_gate_events: u64,
        /// Maximum acceptable share of book/change events without CLOB event timestamps.
        #[arg(long, default_value_t = 0.0)]
        max_missing_timestamp_rate: f64,
    },
    /// Probe Chainlink Data Streams REST reports as an official settlement shadow feed.
    ChainlinkDataStreamsProbe {
        /// Data Streams REST endpoint.
        #[arg(
            long,
            env = "CHAINLINK_DATA_STREAMS_REST_URL",
            default_value = data::chainlink::DEFAULT_DATA_STREAMS_REST_URL
        )]
        endpoint: String,
        /// Data Streams feed ID. Repeat or pass comma-separated CHAINLINK_DATA_STREAMS_FEED_IDS.
        #[arg(
            long = "feed-id",
            env = "CHAINLINK_DATA_STREAMS_FEED_IDS",
            value_delimiter = ','
        )]
        feed_ids: Vec<String>,
        /// Data Streams REST/WebSocket username from the Chainlink credentials screen.
        #[arg(
            long = "username",
            env = "CHAINLINK_DATA_STREAMS_REST_WEBSOCKET_USERNAME",
            hide_env_values = true
        )]
        rest_websocket_username: Option<String>,
        /// Data Streams API key from the Chainlink credentials screen.
        #[arg(
            long = "api-key",
            env = "CHAINLINK_DATA_STREAMS_API_KEY",
            hide_env_values = true
        )]
        api_key: Option<String>,
        /// Data Streams HMAC/shared secret.
        #[arg(
            long = "hmac-secret",
            visible_alias = "api-secret",
            env = "CHAINLINK_DATA_STREAMS_HMAC_SECRET",
            hide_env_values = true
        )]
        hmac_secret: Option<String>,
        /// Output JSON path.
        #[arg(long)]
        output: Option<String>,
    },
    /// Backfill an official Chainlink Data Streams settlement tape CSV
    /// (`timestamp_ms,price`) over a historical range via /reports/page.
    /// Requires credentials whose plan includes historical access.
    ChainlinkBackfill {
        /// Data Streams REST endpoint.
        #[arg(
            long,
            env = "CHAINLINK_DATA_STREAMS_REST_URL",
            default_value = data::chainlink::DEFAULT_DATA_STREAMS_REST_URL
        )]
        endpoint: String,
        /// Single Data Streams feed ID (e.g. the btc-usd-twap-60s stream
        /// Polymarket settles on).
        #[arg(long = "feed-id")]
        feed_id: String,
        #[arg(
            long = "api-key",
            env = "CHAINLINK_DATA_STREAMS_API_KEY",
            hide_env_values = true
        )]
        api_key: String,
        #[arg(
            long = "hmac-secret",
            visible_alias = "api-secret",
            env = "CHAINLINK_DATA_STREAMS_HMAC_SECRET",
            hide_env_values = true
        )]
        hmac_secret: String,
        /// Inclusive UTC range start (RFC3339).
        #[arg(long)]
        start: String,
        /// Exclusive UTC range end (RFC3339).
        #[arg(long)]
        end: String,
        /// Implied decimals of the stream's integer price.
        #[arg(long, default_value_t = 18)]
        price_decimals: u32,
        /// Reports per page request.
        #[arg(long, default_value_t = 1000)]
        page_limit: u32,
        /// Output settlement tape CSV (`timestamp_ms,price`), atomic write.
        #[arg(long)]
        output: String,
    },
    /// Refresh converted forward BTC captures with terminal Gamma outcomes.
    FinalizeRecordedBtcBooks {
        /// Directory produced by convert-recorded-btc-books.
        #[arg(long)]
        input_dir: String,
        /// Restrict finalization to captured condition IDs. Repeat or comma-separate.
        /// Unknown IDs fail closed. Omitting the flag preserves all captured markets.
        #[arg(long = "condition-id", value_delimiter = ',')]
        condition_ids: Vec<String>,
        /// BTC tick/kline CSV used for settlement-alignment checks. If omitted, pull Binance klines.
        #[arg(long)]
        btc_csv: Option<String>,
        /// Declared source kind for --btc-csv; use chainlink_btc_usd_data_stream for official BTC markets.
        #[arg(long, default_value = "auto")]
        settlement_source_kind: String,
        /// Output JSON path. Defaults to <input-dir>/resolution_manifest.json.
        #[arg(long)]
        output: Option<String>,
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
        /// Hash-pin the exact PMXT sidecar, distilled, or parquet artifacts
        /// available to this replay. Required for settlement-anchor scoring.
        #[arg(long, default_value_t = false)]
        pin_input_artifacts: bool,
        /// Require every replay hour to come from PMXT_DISTILLED_DIR; never fall back
        /// to a sidecar, parquet, or network download.
        #[arg(long, default_value_t = false)]
        require_shared_distilled: bool,
        /// Replay serialized StrategyVariant JSON instead of expanding the parameter grid.
        /// The file may contain one variant or an array of variants.
        #[arg(long)]
        variant_json: Option<String>,
        /// Restrict the replay universe to exact condition IDs. Repeat or comma-separate.
        #[arg(long = "condition-id", value_delimiter = ',')]
        condition_ids: Vec<String>,
        #[arg(long)]
        btc_csv: Option<String>,
        /// Optional official settlement tape. When set, --btc-csv remains the causal
        /// signal/volatility feed and this tape alone resolves market outcomes.
        #[arg(long)]
        settlement_btc_csv: Option<String>,
        /// Optional research-only fair-value tape. When set, --btc-csv remains
        /// the causal momentum/volatility feed while fair spot/open use this
        /// tape with fixed 10-second/current and 2-second/open freshness bounds.
        #[arg(long)]
        fair_value_btc_csv: Option<String>,
        /// Passing outcome-blind allocation lock whose exact 750 condition IDs
        /// must match --condition-id. Required with --fair-value-btc-csv.
        #[arg(long)]
        settlement_anchor_allocation_lock: Option<String>,
        /// Passing label-free Chainlink coverage and public-strike audit for
        /// the same condition set. Required with --fair-value-btc-csv.
        #[arg(long)]
        settlement_anchor_source_audit: Option<String>,
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
        /// Write one labeled pre-edge calibration opportunity per condition-second.
        /// Requires --continuous and a capture variant that produces zero trades.
        #[arg(long)]
        calibration_opportunities_json: Option<String>,
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
        /// Optional output JSON path for the zone audit.
        #[arg(long)]
        output: Option<String>,
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
#[allow(clippy::large_enum_variant)]
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
    /// Export one bounded, outcome-free causal opportunity table from cached PMXT data.
    OpportunityTable {
        /// UTC hour start (RFC3339). Only this cached PMXT hour is read.
        #[arg(long)]
        hour: String,
        /// Strict causal signal JSONL. Unknown fields, including outcomes, are rejected.
        #[arg(long)]
        signals: String,
        /// Read books from this v1 distilled candles file instead of a PMXT
        /// parquet — unlocks forward-capture hours the archive never had.
        #[arg(long)]
        distilled_input: Option<String>,
        /// PMXT v2 cache directory. Defaults to PMXT_V2_CACHE_DIR/shared/local detection.
        #[arg(long)]
        cache_dir: Option<String>,
        /// Output Parquet path.
        #[arg(long)]
        output: String,
        /// Output provenance and content-hash manifest JSON path.
        #[arg(long)]
        manifest: String,
        /// Measurement stake used for visible-depth and fee calculations.
        #[arg(long, default_value_t = 5.0)]
        stake_usd: f64,
        /// Polymarket taker fee rate used for measurement-only economics.
        #[arg(long, default_value_t = 0.07)]
        fee_rate: f64,
        /// Hard row bound for one export.
        #[arg(long, default_value_t = 10_000)]
        max_rows: usize,
    },
    /// Fetch and sanitize Gamma identity metadata for bounded opportunity hours.
    OpportunityMarketCatalog {
        /// UTC hour start. Repeat for each preregistered hour.
        #[arg(long, required = true, num_args = 1..)]
        hour: Vec<String>,
        /// Optional immutable catalog to merge before neutralizing all prices.
        #[arg(long)]
        base_catalog: Option<String>,
        /// Gamma API base URL.
        #[arg(long, default_value = "https://gamma-api.polymarket.com")]
        gamma_url: String,
        /// Output outcome-sanitized market identity catalog JSON.
        #[arg(long)]
        output: String,
        /// Output provenance manifest JSON.
        #[arg(long)]
        manifest: String,
        /// Candle market family: btc-5m, eth-5m, sol-5m, xrp-5m, btc-15m, eth-15m.
        #[arg(long, default_value = "btc-5m")]
        family: String,
    },
    /// Compile strict outcome-free signal JSONL for one opportunity-table hour.
    OpportunitySignals {
        /// UTC observation hour start (RFC3339).
        #[arg(long)]
        hour: String,
        /// Strict causal window JSONL or JSONL.GZ with no terminal fields.
        #[arg(long)]
        causal_windows: String,
        /// Cached Gamma market catalog used only for condition/token identity.
        #[arg(long)]
        market_catalog: String,
        /// Output strict causal signal JSONL path.
        #[arg(long)]
        output: String,
        /// Output provenance and content-hash manifest JSON path.
        #[arg(long)]
        manifest: String,
        /// Hard row bound for one compiled hour.
        #[arg(long, default_value_t = 1_000)]
        max_rows: usize,
        /// Candle market family: btc-5m, eth-5m, sol-5m, xrp-5m, btc-15m, eth-15m.
        #[arg(long, default_value = "btc-5m")]
        family: String,
    },
    /// Build an outcome-free cache of both complementary books per opportunity.
    OpportunityPairFeatures {
        /// Sealed causal opportunity dataset; only coordinates are reused.
        #[arg(long)]
        dataset_seal: String,
        /// Outcome-price-neutralized Gamma condition/token identity catalog.
        #[arg(long)]
        market_catalog: String,
        /// Existing PMXT v2 cache containing every sealed source hour.
        #[arg(long)]
        cache_dir: String,
        /// Output strict paired-book feature JSONL.
        #[arg(long)]
        output: String,
        /// Output provenance and content-hash manifest JSON.
        #[arg(long)]
        manifest: String,
    },
    /// Build the generic outcome-free store with the PMXT trade-flow plugin.
    OpportunityFlowFeatures {
        /// Sealed causal opportunity dataset; only coordinates are reused.
        #[arg(long)]
        dataset_seal: String,
        /// Outcome-price-neutralized Gamma condition/token identity catalog.
        #[arg(long)]
        market_catalog: String,
        /// Existing PMXT v2 cache containing every sealed source hour.
        #[arg(long)]
        cache_dir: String,
        /// Output strict feature-store JSONL.
        #[arg(long)]
        output: String,
        /// Output outcome-safety, plugin, provenance, and content manifest.
        #[arg(long)]
        manifest: String,
    },
    /// Join closed Binance spot/perpetual seconds to outcome-free paired books.
    OpportunityCrossVenueFeatures {
        /// Sealed causal opportunity dataset; only coordinates are reused.
        #[arg(long)]
        dataset_seal: String,
        /// Existing outcome-free paired-book feature manifest.
        #[arg(long)]
        paired_features_manifest: String,
        /// Checksum-verified Binance cross-venue tape manifest.
        #[arg(long)]
        source_tape_manifest: String,
        /// Output strict cross-venue feature-store JSONL.
        #[arg(long)]
        output: String,
        /// Output outcome-safety, plugin, provenance, and content manifest.
        #[arg(long)]
        manifest: String,
    },
    /// Seal the fixed cross-venue policy grid before opening discovery labels.
    OpportunityCrossVenuePreregister {
        /// Sealed causal opportunity dataset.
        #[arg(long)]
        dataset_seal: String,
        /// Discovery-label manifest; the label table is not read at this stage.
        #[arg(long)]
        labels_manifest: String,
        /// Immutable outcome-free cross-venue feature-store manifest.
        #[arg(long)]
        feature_store_manifest: String,
        /// Atomic preregistration artifact.
        #[arg(long)]
        output: String,
        #[arg(long, default_value_t = 12)]
        minimum_calibration_support: usize,
        #[arg(long, default_value_t = 20)]
        minimum_policy_support: usize,
        #[arg(long, default_value_t = 0.02)]
        safety_margin: f64,
        #[arg(long, default_value_t = 128)]
        latency_ms: u64,
        #[arg(long, default_value_t = 2)]
        maximum_exact_replays: usize,
    },
    /// Score the immutable cross-venue grid once on discovery-only labels.
    OpportunityCrossVenueSearch {
        /// Immutable cross-venue preregistration.
        #[arg(long)]
        preregistration: String,
        /// Atomic cheap-screen report and bounded exact-replay plan.
        #[arg(long)]
        output: String,
    },
    /// Resolve the cross-venue family to a terminal bounded decision.
    OpportunityCrossVenueDecision {
        #[arg(long)]
        preregistration: String,
        #[arg(long)]
        search_report: String,
        /// Omit only when the cheap screen emitted no exact-replay plan.
        #[arg(long)]
        exact_replay_report: Option<String>,
        #[arg(long)]
        output: String,
    },
    /// Search the fixed trade-tape directional-flow policy grid.
    OpportunityFlowSearch {
        /// Sealed causal opportunity dataset.
        #[arg(long)]
        dataset_seal: String,
        /// Discovery-label manifest; fresh-holdout labels must be absent.
        #[arg(long)]
        labels_manifest: String,
        /// Immutable outcome-free feature-store manifest.
        #[arg(long)]
        feature_store_manifest: String,
        /// Atomic JSON report and bounded exact-replay plan.
        #[arg(long)]
        output: String,
        /// Minimum selected observations on the older partition.
        #[arg(long, default_value_t = 12)]
        minimum_calibration_support: usize,
        /// Minimum selected observations on recent discovery.
        #[arg(long, default_value_t = 20)]
        minimum_policy_support: usize,
        /// Required recent realized edge above fee-aware break-even.
        #[arg(long, default_value_t = 0.02)]
        safety_margin: f64,
        /// Execution latency pinned into decision-trace identity.
        #[arg(long, default_value_t = 128)]
        latency_ms: u64,
        /// Maximum research-only execution traces emitted for exact replay.
        #[arg(long, default_value_t = 2)]
        maximum_exact_replays: usize,
    },
    /// Resolve the trade-tape directional-flow family to a terminal decision.
    OpportunityFlowDecision {
        /// Immutable preregistration sealed before trade-flow scoring.
        #[arg(long)]
        preregistration: String,
        /// Trade-flow cheap-screen report.
        #[arg(long)]
        flow_search_report: String,
        /// Bounded exact replay; omit only when cheap screen emitted no plan.
        #[arg(long)]
        exact_replay_report: Option<String>,
        /// Atomic terminal decision report.
        #[arg(long)]
        output: String,
    },
    /// Search the fixed pure cross-token liquidity-dislocation grid.
    OpportunityLiquiditySearch {
        /// Sealed causal opportunity dataset.
        #[arg(long)]
        dataset_seal: String,
        /// Discovery-label manifest; fresh-holdout labels must be absent.
        #[arg(long)]
        labels_manifest: String,
        /// Immutable paired-book feature manifest.
        #[arg(long)]
        paired_features_manifest: String,
        /// Atomic JSON report and bounded exact-replay plan.
        #[arg(long)]
        output: String,
        /// Minimum selected observations on the older partition.
        #[arg(long, default_value_t = 20)]
        minimum_calibration_support: usize,
        /// Minimum selected observations on recent discovery.
        #[arg(long, default_value_t = 20)]
        minimum_policy_support: usize,
        /// Required recent realized edge above fee-aware break-even.
        #[arg(long, default_value_t = 0.02)]
        safety_margin: f64,
        /// Execution latency pinned into decision-trace identity.
        #[arg(long, default_value_t = 128)]
        latency_ms: u64,
        /// Maximum research-only execution traces emitted for exact replay.
        #[arg(long, default_value_t = 2)]
        maximum_exact_replays: usize,
    },
    /// Resolve the pure cross-token liquidity family to a terminal decision.
    OpportunityLiquidityDecision {
        /// Immutable preregistration sealed before liquidity-family scoring.
        #[arg(long)]
        preregistration: String,
        /// Liquidity-family cheap-screen report.
        #[arg(long)]
        liquidity_search_report: String,
        /// Bounded exact replay; omit only when the cheap screen emitted no plan.
        #[arg(long)]
        exact_replay_report: Option<String>,
        /// Atomic terminal decision report.
        #[arg(long)]
        output: String,
    },
    /// Seal one or more immutable causal opportunity tables into a dataset index.
    OpportunityDatasetSeal {
        /// Opportunity-table manifest path. Repeat for each hour.
        #[arg(long, required = true, num_args = 1..)]
        opportunity_manifest: Vec<String>,
        /// Output causal dataset seal JSON.
        #[arg(long)]
        output: String,
    },
    /// Join a sealed causal dataset to a physically separate terminal-label source.
    OpportunityLabels {
        /// Sealed causal opportunity dataset JSON.
        #[arg(long)]
        dataset_seal: String,
        /// close_vs_open: strict label-only JSONL(.GZ). twap_vs_open: a
        /// settlement tape CSV the window TWAP is computed from.
        #[arg(long)]
        label_source: String,
        /// Output label Parquet keyed only by opportunity_id.
        #[arg(long)]
        output: String,
        /// Output label provenance manifest JSON.
        #[arg(long)]
        manifest: String,
        /// close_vs_open (pre-2026-08-08 markets) or twap_vs_open
        /// (post-change markets). Each mode refuses the other era's windows.
        #[arg(long, default_value = "close_vs_open")]
        resolution_rule: String,
    },
    /// Locked ONE-SHOT fresh-holdout gate: consumes a sealed dataset's fresh
    /// rows exactly once for one frozen policy. The consumed marker is
    /// written before any outcome is computed; fresh labels never touch disk.
    OpportunityFreshGate {
        /// Sealed causal opportunity dataset JSON.
        #[arg(long)]
        dataset_seal: String,
        /// Settlement tape CSV resolving the fresh windows (TWAP rule).
        #[arg(long)]
        settlement_tape: String,
        /// Frozen policy JSON: {family, decision_seconds, lock_strength_min,
        /// ask_cap, min_lock_fraction, advancement_margin}.
        #[arg(long)]
        policy: String,
        /// The family's preregistration document (hash-pinned into the verdict).
        #[arg(long)]
        preregistration: String,
        /// Directory holding one-shot consumed markers.
        #[arg(long, default_value = "logs/strategy-research/consumed")]
        consumed_dir: String,
        /// Terminal verdict JSON output path.
        #[arg(long)]
        output: String,
    },
    /// Search a bounded policy grid in one pass and collapse exact-replay traces.
    OpportunityPolicySearch {
        /// Sealed causal opportunity dataset JSON.
        #[arg(long)]
        dataset_seal: String,
        /// Discovery-label manifest; fresh-holdout labels must be absent.
        #[arg(long)]
        labels_manifest: String,
        /// Atomic JSON report and exact-replay plan.
        #[arg(long)]
        output: String,
        /// Minimum observations in each chronological calibration cell.
        #[arg(long, default_value_t = 20)]
        minimum_calibration_support: usize,
        /// Minimum calibrated discovery opportunities for a policy.
        #[arg(long, default_value_t = 20)]
        minimum_policy_support: usize,
        /// Required probability margin above fee-aware break-even.
        #[arg(long, default_value_t = 0.02)]
        safety_margin: f64,
        /// Execution latency pinned into decision-trace identity.
        #[arg(long, default_value_t = 0)]
        latency_ms: u64,
        /// Maximum research-only execution traces emitted for exact replay.
        #[arg(long, default_value_t = 2)]
        maximum_exact_replays: usize,
    },
    /// Search the preregistered causal-probability-versus-price family.
    OpportunityProbabilitySearch {
        /// Sealed causal opportunity dataset JSON.
        #[arg(long)]
        dataset_seal: String,
        /// Discovery-label manifest; fresh-holdout labels must be absent.
        #[arg(long)]
        labels_manifest: String,
        /// Atomic JSON report and bounded exact-replay plan.
        #[arg(long)]
        output: String,
        /// Minimum selected observations on the older calibration partition.
        #[arg(long, default_value_t = 20)]
        minimum_calibration_support: usize,
        /// Maximum older-partition Brier score for selected opportunities.
        #[arg(long, default_value_t = 0.25)]
        maximum_calibration_brier_score: f64,
        /// Minimum selected recent-discovery opportunities.
        #[arg(long, default_value_t = 20)]
        minimum_policy_support: usize,
        /// Required realized probability margin above fee-aware break-even.
        #[arg(long, default_value_t = 0.02)]
        safety_margin: f64,
        /// Execution latency pinned into decision-trace identity.
        #[arg(long, default_value_t = 128)]
        latency_ms: u64,
        /// Maximum research-only execution traces emitted for exact replay.
        #[arg(long, default_value_t = 2)]
        maximum_exact_replays: usize,
    },
    /// Resolve the probability family to a terminal bounded decision.
    OpportunityProbabilityDecision {
        /// Immutable preregistration sealed before probability-family scoring.
        #[arg(long)]
        preregistration: String,
        /// Probability-family cheap-screen report.
        #[arg(long)]
        probability_search_report: String,
        /// Bounded exact replay report for that search report.
        #[arg(long)]
        exact_replay_report: String,
        /// Atomic terminal decision report.
        #[arg(long)]
        output: String,
    },
    /// Replay the bounded policy shortlist against latency-adjusted PMXT L2 books.
    OpportunityExactReplay {
        /// Sealed causal opportunity dataset JSON.
        #[arg(long)]
        dataset_seal: String,
        /// Discovery-label manifest; fresh-holdout labels must be absent.
        #[arg(long)]
        labels_manifest: String,
        /// Immutable opportunity-policy-search v2 report with a bounded replay plan.
        #[arg(long)]
        policy_search_report: String,
        /// PMXT v2 parquet cache containing every discovery hour in the plan.
        #[arg(long)]
        cache_dir: String,
        /// Read hour books from `<dir>/<hour>.v1.candles.jsonl.gz` (forward
        /// capture) instead of PMXT parquets.
        #[arg(long)]
        distilled_dir: Option<String>,
        /// Atomic JSON exact-replay report.
        #[arg(long)]
        output: String,
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
    /// Score the frozen binary-complement gate on one post-registration forward block.
    BinaryComplementScreen {
        /// Opportunity JSON emitted by harness-sweep --calibration-opportunities-json.
        #[arg(long, required = true, num_args = 1..)]
        opportunity: Vec<String>,
        /// Terminal official-source resolution manifest. Repeat for segmented captures.
        #[arg(long = "resolution-manifest", required = true, num_args = 1..)]
        resolution_manifest: Vec<String>,
        /// Stable identifier for this forward block.
        #[arg(long)]
        block_id: String,
        /// Write the immutable JSON screen artifact to this path.
        #[arg(long)]
        output: Option<String>,
    },
    /// Verify two passing binary-complement screens are chronological and disjoint.
    BinaryComplementRepeatAudit {
        /// Block screen JSON in chronological order. Provide exactly two.
        #[arg(long, required = true, num_args = 1..)]
        screen: Vec<String>,
        /// Write the immutable two-block audit artifact to this path.
        #[arg(long)]
        output: Option<String>,
    },
    /// Lock one future settlement-anchor block to an exact, disjoint condition set.
    SettlementAnchorAllocationLock {
        /// Frozen settlement-anchor preregistration JSON.
        #[arg(long)]
        preregistration: String,
        /// Frozen one-variant JSON used by the evaluator.
        #[arg(long = "variant-json")]
        variant_json: String,
        /// Outcome-free sealed candidate condition-set manifest.
        #[arg(long = "candidate-condition-set")]
        candidate_condition_set: String,
        /// Every prior binary-complement and settlement-anchor condition set.
        #[arg(long = "prior-condition-set", required = true, num_args = 1..)]
        prior_condition_set: Vec<String>,
        /// Immutable evaluator experiment-report path reserved by this lock.
        #[arg(long = "report-output")]
        report_output: String,
        /// Immutable evaluator trade-report path reserved by this lock.
        #[arg(long = "trades-output")]
        trades_output: String,
        /// Immutable paired-score artifact path reserved by this lock.
        #[arg(long = "pair-audit-output")]
        pair_audit_output: String,
        /// Write the immutable allocation lock to this path.
        #[arg(long)]
        output: Option<String>,
    },
    /// Audit causal Chainlink coverage and public price-to-beat reproduction
    /// for one locked settlement-anchor condition set without loading outcomes.
    SettlementAnchorSourceAudit {
        /// Outcome-free settlement-anchor condition-set manifest.
        #[arg(long = "condition-set")]
        condition_set: String,
        /// Official Chainlink BTC/USD collector CSV.
        #[arg(long = "fair-value-btc-csv")]
        fair_value_btc_csv: String,
        /// Write the immutable label-free source audit to this path.
        #[arg(long)]
        output: Option<String>,
    },
    /// Score the frozen baseline/official pair once after the locked evaluator
    /// outputs exist. Recomputes all absolute gates and exact non-fair parity.
    SettlementAnchorPairAudit {
        /// Passing outcome-blind settlement-anchor allocation lock.
        #[arg(long = "allocation-lock")]
        allocation_lock: String,
        /// Passing label-free official-anchor source audit.
        #[arg(long = "source-audit")]
        source_audit: String,
        /// Exact official Chainlink BTC/USD collector CSV pinned by the audit.
        #[arg(long = "fair-value-btc-csv")]
        fair_value_btc_csv: String,
        /// Baseline experiment report without the official-anchor source.
        #[arg(long = "baseline-report")]
        baseline_report: String,
        /// Baseline trade report without the official-anchor source.
        #[arg(long = "baseline-trades")]
        baseline_trades: String,
        /// Official-anchor experiment report reserved by the allocation lock.
        #[arg(long = "official-report")]
        official_report: String,
        /// Official-anchor trade report reserved by the allocation lock.
        #[arg(long = "official-trades")]
        official_trades: String,
        /// New immutable paired-score artifact path; existing files are refused.
        #[arg(long)]
        output: String,
    },
    /// Search causal/regime bucket selectivity rules with feed-forward OOS scoring.
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
    /// Search causal require-policy conjunctions with prior-only deny learning.
    CausalPolicySearch {
        /// Chronological input JSON generated by harness or harness-sweep --report-json.
        #[arg(long, required = true, num_args = 1..)]
        report: Vec<String>,
        /// Write the JSON search artifact to this path.
        #[arg(long)]
        output: Option<String>,
        /// Minimum prior reports required before a policy can score the next fold.
        #[arg(long, default_value_t = 2)]
        min_train_reports: usize,
        /// Minimum prior trades required after applying the learned policy.
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
        /// Minimum OOS reports with selected trades required to pass; 0 disables.
        #[arg(long, default_value_t = 0)]
        min_oos_eligible_reports: usize,
        /// Minimum worst OOS report PnL.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        min_worst_oos_pnl: f64,
        /// Maximum number of causal require tags in a policy.
        #[arg(long, default_value_t = 3)]
        max_require_terms: usize,
        /// Maximum number of prior-toxic deny rules added inside a policy.
        #[arg(long, default_value_t = 1)]
        max_deny_rules: usize,
        /// Maximum number of causal tags in each deny rule. Keep 1 for direct CLI reruns.
        #[arg(long, default_value_t = 1)]
        max_deny_terms: usize,
        /// Minimum prior trades inside a deny candidate before it can be selected.
        #[arg(long, default_value_t = 5)]
        min_deny_trades: u64,
        /// Minimum prior loss magnitude inside a deny candidate before it can be selected.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        min_deny_loss_pnl: f64,
        /// Minimum prior reports with trades inside a deny candidate before it can be selected.
        #[arg(long, default_value_t = 2)]
        min_deny_loss_reports: usize,
        /// Left-tail fraction for OOS fold CVaR diagnostics.
        #[arg(long, default_value_t = 0.20)]
        tail_alpha: f64,
        /// Minimum OOS fold CVaR PnL. Very negative default makes this diagnostic-only.
        #[arg(long, default_value_t = -1.0e9, allow_hyphen_values = true)]
        min_oos_cvar_pnl: f64,
        /// Rolling OOS report lookback for clustered-loss diagnostics. Zero disables the gate.
        #[arg(long, default_value_t = 0)]
        loss_burst_lookback: usize,
        /// Maximum losing reports inside --loss-burst-lookback. Zero disables the gate.
        #[arg(long, default_value_t = 0)]
        max_loss_burst_reports: usize,
        /// Rank by burst, worst fold, CVaR, and payoff asymmetry before aggregate PnL.
        #[arg(long, default_value_t = false)]
        tail_first_ranking: bool,
        /// Minimum aggregate average-win / average-loss payoff ratio; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        min_oos_payoff_ratio: f64,
        /// Maximum worst-loss / average-win ratio; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        max_oos_worst_loss_to_avg_win: f64,
        /// Prior window for the loss-cluster sentinel; 0 reuses --loss-burst-lookback.
        #[arg(long, default_value_t = 0)]
        prior_loss_cluster_lookback: usize,
        /// Flatten when prior selected folds already show this many losses in the configured burst lookback.
        #[arg(long, default_value_t = 0)]
        max_prior_loss_burst_reports: usize,
        /// Minimum prior average-win / average-loss payoff ratio before scoring the next fold; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        min_prior_payoff_ratio: f64,
        /// Maximum prior worst-loss / average-win ratio before scoring the next fold; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        max_prior_worst_loss_to_avg_win: f64,
        /// Minimum prior exact-regime observations needed before the meta-label risk gate can act; 0 disables.
        #[arg(long, default_value_t = 0)]
        meta_label_min_support: usize,
        /// Left-tail quantile used by the exact-regime meta-label risk gate.
        #[arg(long, default_value_t = 0.20)]
        meta_label_alpha: f64,
        /// Minimum prior meta-label left-tail quantile PnL before scoring the next fold.
        #[arg(long, default_value_t = -1.0e9, allow_hyphen_values = true)]
        meta_label_min_quantile_pnl: f64,
        /// Maximum prior meta-label loss rate before scoring the next fold.
        #[arg(long, default_value_t = 1.0)]
        meta_label_max_loss_rate: f64,
        /// Flatten when the current context does not have enough prior exact or generalized support.
        #[arg(long, default_value_t = false)]
        meta_label_require_supported: bool,
        /// When exact-regime support is thin, test broader causal tag combinations up to this term count; 0 disables.
        #[arg(long, default_value_t = 0)]
        meta_label_max_generalization_terms: usize,
        /// Show top N candidates.
        #[arg(long, default_value_t = 25)]
        top: usize,
    },
    /// Run deterministic offline evolution over report-native causal selectivity candidates.
    EvolveSearch {
        /// Chronological input JSON generated by harness or harness-sweep --report-json.
        #[arg(long, required = true, num_args = 1..)]
        report: Vec<String>,
        /// Optional causal-policy-search or evolve-search JSON artifacts used to seed historical genomes.
        #[arg(long = "historical-search", num_args = 1..)]
        historical_search: Vec<String>,
        /// Output directory for evolution_summary.json, generations, candidates, and ledger.
        #[arg(long)]
        out_dir: String,
        /// Deterministic RNG seed.
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Population size per generation.
        #[arg(long, default_value_t = 64)]
        population: usize,
        /// Number of generations to evaluate.
        #[arg(long, default_value_t = 8)]
        generations: usize,
        /// Number of elite survivors carried into the next generation.
        #[arg(long, default_value_t = 8)]
        elite_count: usize,
        /// Minimum prior reports required before a genome can score the next fold.
        #[arg(long, default_value_t = 2)]
        min_train_reports: usize,
        /// Minimum prior trades required after applying the selected policy.
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
        /// Minimum OOS reports with selected trades required to pass; 0 disables.
        #[arg(long, default_value_t = 0)]
        min_oos_eligible_reports: usize,
        /// Minimum worst OOS report PnL.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        min_worst_oos_pnl: f64,
        /// Maximum number of causal require tags in a genome.
        #[arg(long, default_value_t = 3)]
        max_require_terms: usize,
        /// Maximum number of learned or fixed single-tag deny rules.
        #[arg(long, default_value_t = 1)]
        max_deny_rules: usize,
        /// Maximum number of causal tags in each deny rule. Evolve-search supports 0 or 1.
        #[arg(long, default_value_t = 1)]
        max_deny_terms: usize,
        /// Minimum prior trades inside a deny candidate before it can be selected.
        #[arg(long, default_value_t = 5)]
        min_deny_trades: u64,
        /// Minimum prior loss magnitude inside a deny candidate before it can be selected.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        min_deny_loss_pnl: f64,
        /// Minimum prior reports with trades inside a deny candidate before it can be selected.
        #[arg(long, default_value_t = 2)]
        min_deny_loss_reports: usize,
        /// Left-tail fraction for OOS fold CVaR diagnostics.
        #[arg(long, default_value_t = 0.20)]
        tail_alpha: f64,
        /// Minimum OOS fold CVaR PnL.
        #[arg(long, default_value_t = -1.0e9, allow_hyphen_values = true)]
        min_oos_cvar_pnl: f64,
        /// Rolling OOS report lookback for clustered-loss diagnostics. Zero disables the gate.
        #[arg(long, default_value_t = 0)]
        loss_burst_lookback: usize,
        /// Maximum losing reports inside --loss-burst-lookback. Zero disables the gate.
        #[arg(long, default_value_t = 0)]
        max_loss_burst_reports: usize,
        /// Minimum aggregate average-win / average-loss payoff ratio; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        min_oos_payoff_ratio: f64,
        /// Maximum worst-loss / average-win ratio; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        max_oos_worst_loss_to_avg_win: f64,
        /// Prior window for the loss-cluster sentinel; 0 reuses --loss-burst-lookback.
        #[arg(long, default_value_t = 0)]
        prior_loss_cluster_lookback: usize,
        /// Flatten when prior selected folds already show this many losses in the configured burst lookback.
        #[arg(long, default_value_t = 0)]
        max_prior_loss_burst_reports: usize,
        /// Minimum prior average-win / average-loss payoff ratio before scoring the next fold; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        min_prior_payoff_ratio: f64,
        /// Maximum prior worst-loss / average-win ratio before scoring the next fold; 0 disables.
        #[arg(long, default_value_t = 0.0)]
        max_prior_worst_loss_to_avg_win: f64,
        /// Minimum prior exact-regime observations needed before the meta-label risk gate can act; 0 disables.
        #[arg(long, default_value_t = 0)]
        meta_label_min_support: usize,
        /// Left-tail quantile used by the exact-regime meta-label risk gate.
        #[arg(long, default_value_t = 0.20)]
        meta_label_alpha: f64,
        /// Minimum prior meta-label left-tail quantile PnL before scoring the next fold.
        #[arg(long, default_value_t = -1.0e9, allow_hyphen_values = true)]
        meta_label_min_quantile_pnl: f64,
        /// Maximum prior meta-label loss rate before scoring the next fold.
        #[arg(long, default_value_t = 1.0)]
        meta_label_max_loss_rate: f64,
        /// Flatten when the current context does not have enough prior exact or generalized support.
        #[arg(long, default_value_t = false)]
        meta_label_require_supported: bool,
        /// When exact-regime support is thin, test broader causal tag combinations up to this term count; 0 disables.
        #[arg(long, default_value_t = 0)]
        meta_label_max_generalization_terms: usize,
        /// Keep top N evolved candidates in the summary.
        #[arg(long, default_value_t = 25)]
        top: usize,
        /// Optional inclusive UTC replay start for dry-run rolling-history manifests.
        #[arg(long)]
        replay_start: Option<String>,
        /// Optional inclusive UTC replay end for dry-run rolling-history manifests.
        #[arg(long)]
        replay_end: Option<String>,
        /// Rolling-history profile used in dry-run replay manifests.
        #[arg(long, default_value = "a_plus5m")]
        replay_profile: String,
        /// Rolling-history zone mode used in dry-run replay manifests.
        #[arg(long, default_value = "early")]
        replay_zone_mode: String,
        /// Simulated insert latency in milliseconds for replay manifests.
        #[arg(long, default_value_t = 128)]
        latency_ms: u64,
        /// Forward latency audit JSON; replay will override latency upward when executed later.
        #[arg(long)]
        latency_audit_json: Option<String>,
        /// BTC tick/kline CSV used as the virtual exchange price feed in replay manifests.
        #[arg(long)]
        btc_csv: Option<String>,
        /// Replay fold length in inclusive UTC hours.
        #[arg(long, default_value_t = 8)]
        fold_hours: i64,
        /// Variant-fan-out thread count for replay manifests.
        #[arg(long, default_value_t = 0)]
        threads: usize,
        /// Candle frame length to isolate in replay manifests.
        #[arg(long, default_value_t = 5.0)]
        window_minutes: f64,
        /// Within each replay fold, keep at most one downloaded raw PMXT parquet at a time.
        #[arg(long, default_value_t = false)]
        atomic_parquet: bool,
    },
    /// Materialize one historical policy-search candidate as an exact StrategyVariant JSON.
    MaterializePolicyVariant {
        /// Causal-policy-search or evolve-search JSON artifact with candidates.
        #[arg(long)]
        search: String,
        /// Harness-sweep report containing executable strategy_params for the candidate variant.
        #[arg(long = "source-report", required = true, num_args = 1..)]
        source_report: Vec<String>,
        /// Candidate rank to materialize.
        #[arg(long, default_value_t = 1)]
        rank: usize,
        /// Output StrategyVariant JSON path.
        #[arg(long)]
        output: String,
    },
    /// Materialize one ranked harness-sweep row as an exact StrategyVariant JSON.
    MaterializeSweepVariant {
        /// Harness-sweep report containing executable strategy_params.
        #[arg(long)]
        report: String,
        /// Ranked row to materialize from the report.
        #[arg(long, default_value_t = 1)]
        rank: usize,
        /// Output StrategyVariant JSON path.
        #[arg(long)]
        output: String,
        /// Causal tag required at runtime, formatted as dimension=value.
        #[arg(long = "require-causal-tag")]
        require_causal_tag: Vec<String>,
        /// Causal tag denied at runtime, formatted as dimension=value.
        #[arg(long = "deny-causal-tag")]
        deny_causal_tag: Vec<String>,
    },
    /// Mine trade-feature reports for replay-safe causal filters and exact StrategyVariant artifacts.
    FeatureFilterSearch {
        /// Harness-sweep trade feature JSON emitted by --trade-features-json.
        #[arg(long = "feature", required = true, num_args = 1..)]
        feature: Vec<String>,
        /// Base serialized StrategyVariant JSON to merge candidate filters into.
        #[arg(long = "base-variant")]
        base_variant: String,
        /// Output directory for feature_filter_summary.json and candidate variants.
        #[arg(long)]
        out_dir: String,
        /// Keep top N candidates.
        #[arg(long, default_value_t = 25)]
        top: usize,
        /// Maximum number of extra causal require tags.
        #[arg(long, default_value_t = 2)]
        max_require_terms: usize,
        /// Maximum number of extra single-tag deny filters.
        #[arg(long, default_value_t = 3)]
        max_deny_terms: usize,
        /// Minimum trade support for an atom to be considered.
        #[arg(long, default_value_t = 1)]
        min_atom_trades: u64,
        /// Maximum supported atoms retained before draft generation.
        #[arg(long, default_value_t = 80)]
        max_atoms: usize,
        /// Minimum aggregate selected trades for a passing candidate.
        #[arg(long, default_value_t = 10)]
        min_total_trades: u64,
        /// Minimum feature reports with selected trades for a passing candidate.
        #[arg(long, default_value_t = 2)]
        min_eligible_reports: usize,
        /// Minimum aggregate selected PnL for a passing candidate.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        min_total_pnl: f64,
        /// Minimum selected PnL in the worst eligible feature report.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        min_worst_report_pnl: f64,
    },
    /// Convert causal-policy-search candidates into rolling-history replay verification manifests.
    CausalPolicyReplayPlan {
        /// Causal-policy-search JSON artifact.
        #[arg(long)]
        search: String,
        /// Inclusive UTC start hour (RFC3339) for replay verification.
        #[arg(long)]
        start: String,
        /// Inclusive UTC end hour (RFC3339) for replay verification.
        #[arg(long)]
        end: String,
        /// Output directory for per-candidate rolling-history manifests.
        #[arg(long)]
        out_dir: String,
        /// Optional summary JSON output. Defaults to <out-dir>/causal_policy_replay_plan.json.
        #[arg(long)]
        output: Option<String>,
        /// Inspect the top N search candidates before filtering.
        #[arg(long, default_value_t = 1)]
        top: usize,
        /// Also generate replay manifests for failed static/search candidates.
        #[arg(long, default_value_t = false)]
        include_failed: bool,
        /// Root for per-candidate temporary PMXT caches. Defaults inside each candidate out-dir.
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
        /// Forward latency audit JSON; overrides --latency-ms upward to the measured p99 recommendation.
        #[arg(long)]
        latency_audit_json: Option<String>,
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
        /// Rolling lab profile, e.g. a_plus5m_tail_low_exposure.
        #[arg(long, default_value = "a_plus5m")]
        profile: String,
        /// Restrict sweeps to one timing zone: all, early, primary, late, terminal.
        #[arg(long, default_value = "early")]
        zone_mode: String,
        /// Execute generated rolling-history runs. Without this flag, writes dry-run manifests.
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
        /// Minimum top-variant trades required in each fold before treating it as strategy evidence.
        #[arg(long)]
        min_fold_top_trades: Option<usize>,
        /// Minimum aggregate trades required by robust-promote.
        #[arg(long)]
        min_promotion_trades: Option<usize>,
        /// Minimum trades per report required by robust-promote.
        #[arg(long)]
        min_promotion_daily_trades: Option<usize>,
        /// Minimum profitable reports required by robust-promote.
        #[arg(long)]
        min_promotion_profitable_reports: Option<usize>,
        /// Minimum aggregate losses required by robust-promote.
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
    /// Mark a strategy version as candidate, questionable, dead_end, promoted, or rejected.
    RegistryMark {
        /// Strategy registry JSON path.
        #[arg(long, default_value = "docs/strategy_registry.json")]
        registry: String,
        /// Stable strategy/version label, e.g. reversion_tail_guard_v1.
        #[arg(long)]
        strategy_id: String,
        /// Optional parent strategy id.
        #[arg(long)]
        parent_id: Option<String>,
        /// candidate, active, questionable, dead_end, promoted, or rejected.
        #[arg(long)]
        status: String,
        /// Short reason for this mark.
        #[arg(long)]
        reason: String,
        /// Main search or promotion artifact path.
        #[arg(long)]
        artifact: Option<String>,
        /// Optional metrics artifact path.
        #[arg(long)]
        metrics: Option<String>,
        /// Evidence file path. Repeatable.
        #[arg(long)]
        evidence: Vec<String>,
        /// Additional note. Repeatable.
        #[arg(long)]
        note: Vec<String>,
    },
    /// Audit strategy registry evidence durability and live promotion status.
    RegistryAudit {
        /// Strategy registry JSON path.
        #[arg(long, default_value = "docs/strategy_registry.json")]
        registry: String,
        /// Required durable archive prefix for registry evidence paths.
        #[arg(long, default_value = "deploy/promotions/evidence/strategy_registry")]
        durable_prefix: String,
        /// Write the JSON audit artifact to this path.
        #[arg(long)]
        output: Option<String>,
    },
    /// Copy registry evidence artifacts into a durable archive and optionally rewrite registry paths.
    EvidenceExport {
        /// Strategy registry JSON path.
        #[arg(long, default_value = "docs/strategy_registry.json")]
        registry: String,
        /// Output directory for archived evidence copies.
        #[arg(long, default_value = "deploy/promotions/evidence/strategy_registry")]
        out_dir: String,
        /// Optional path for a JSON manifest describing copied and missing evidence.
        #[arg(long)]
        manifest: Option<String>,
        /// Rewrite artifact/metrics/evidence paths in the registry to archived copies.
        #[arg(long, default_value_t = false)]
        rewrite_registry: bool,
    },
    /// Compose multiple prior-losing full-regime deny rules with feed-forward OOS scoring.
    MultiGuardSearch {
        /// Chronological input JSON generated by harness or harness-sweep --report-json.
        #[arg(long, required = true, num_args = 1..)]
        report: Vec<String>,
        /// Write the JSON search artifact to this path.
        #[arg(long)]
        output: Option<String>,
        /// Minimum prior reports required before a guard can score the next fold.
        #[arg(long, default_value_t = 2)]
        min_train_reports: usize,
        /// Minimum prior trades required after applying the learned guard.
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
        /// Maximum number of full-regime deny rules selected per fold.
        #[arg(long, default_value_t = 4)]
        max_rules: usize,
        /// Minimum prior trades inside a regime before it can be denied.
        #[arg(long, default_value_t = 5)]
        min_guard_trades: u64,
        /// Minimum prior loss magnitude inside a regime before it can be denied.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        min_guard_loss_pnl: f64,
        /// Minimum prior reports with trades inside a regime before it can be denied.
        #[arg(long, default_value_t = 2)]
        min_guard_loss_reports: usize,
        /// Number of prior reports used for recent loss-burst diagnostics.
        #[arg(long, default_value_t = 6)]
        recent_report_lookback: usize,
        /// Also test broader tag-pattern guards learned from losing full-regime buckets.
        #[arg(long, default_value_t = false)]
        pattern_guards: bool,
        /// Left-tail fraction for OOS fold CVaR diagnostics.
        #[arg(long, default_value_t = 0.20)]
        tail_alpha: f64,
        /// Minimum OOS fold CVaR PnL. Very negative default makes this diagnostic-only.
        #[arg(long, default_value_t = -1.0e9, allow_hyphen_values = true)]
        min_oos_cvar_pnl: f64,
        /// Rolling OOS report lookback for clustered-loss diagnostics. Zero disables the gate.
        #[arg(long, default_value_t = 0)]
        loss_burst_lookback: usize,
        /// Maximum losing reports inside --loss-burst-lookback. Zero disables the gate.
        #[arg(long, default_value_t = 0)]
        max_loss_burst_reports: usize,
        /// Show top N candidates.
        #[arg(long, default_value_t = 25)]
        top: usize,
    },
    /// Select up/down/flat per OOS fold from prior direction-bucket evidence only.
    AdaptiveDirectionSearch {
        /// Chronological input JSON generated by harness or harness-sweep --report-json.
        #[arg(long, required = true, num_args = 1..)]
        report: Vec<String>,
        /// Write the JSON search artifact to this path.
        #[arg(long)]
        output: Option<String>,
        /// Minimum prior reports required before a side can score the next fold.
        #[arg(long, default_value_t = 2)]
        min_train_reports: usize,
        /// Minimum prior trades required for a side before it can score the next fold.
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
        /// Left-tail fraction for OOS fold CVaR diagnostics.
        #[arg(long, default_value_t = 0.20)]
        tail_alpha: f64,
        /// Minimum OOS fold CVaR PnL. Very negative default makes this diagnostic-only.
        #[arg(long, default_value_t = -1.0e9, allow_hyphen_values = true)]
        min_oos_cvar_pnl: f64,
        /// Rolling OOS report lookback for clustered-loss diagnostics. Zero disables the gate.
        #[arg(long, default_value_t = 0)]
        loss_burst_lookback: usize,
        /// Maximum losing reports inside --loss-burst-lookback. Zero disables the gate.
        #[arg(long, default_value_t = 0)]
        max_loss_burst_reports: usize,
        /// Show top N candidates.
        #[arg(long, default_value_t = 25)]
        top: usize,
    },
    /// Select flat, direction, or guarded mode per OOS fold from prior evidence only.
    AdaptiveModeSearch {
        /// Chronological input JSON generated by harness or harness-sweep --report-json.
        #[arg(long, required = true, num_args = 1..)]
        report: Vec<String>,
        /// Write the JSON search artifact to this path.
        #[arg(long)]
        output: Option<String>,
        /// Minimum prior reports required before a mode can score the next fold.
        #[arg(long, default_value_t = 2)]
        min_train_reports: usize,
        /// Minimum prior trades required before an active mode can score the next fold.
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
        /// Maximum number of full-regime deny rules selected by guarded mode.
        #[arg(long, default_value_t = 4)]
        max_guard_rules: usize,
        /// Minimum prior trades inside a regime before guarded mode can deny it.
        #[arg(long, default_value_t = 5)]
        min_guard_trades: u64,
        /// Minimum prior loss magnitude inside a regime before guarded mode can deny it.
        #[arg(long, default_value_t = 0.0, allow_hyphen_values = true)]
        min_guard_loss_pnl: f64,
        /// Minimum prior reports with trades inside a regime before guarded mode can deny it.
        #[arg(long, default_value_t = 2)]
        min_guard_loss_reports: usize,
        /// Number of prior reports used for recent loss-burst diagnostics.
        #[arg(long, default_value_t = 6)]
        recent_report_lookback: usize,
        /// Also test broader tag-pattern guards learned from losing full-regime buckets.
        #[arg(long, default_value_t = false)]
        pattern_guards: bool,
        /// Choose flat if the best active mode's prior worst-fold PnL is below this value.
        #[arg(long, default_value_t = -1.0e9, allow_hyphen_values = true)]
        flat_if_worst_train_below: f64,
        /// Left-tail fraction for OOS fold CVaR diagnostics.
        #[arg(long, default_value_t = 0.20)]
        tail_alpha: f64,
        /// Minimum OOS fold CVaR PnL. Very negative default makes this diagnostic-only.
        #[arg(long, default_value_t = -1.0e9, allow_hyphen_values = true)]
        min_oos_cvar_pnl: f64,
        /// Rolling OOS report lookback for clustered-loss diagnostics. Zero disables the gate.
        #[arg(long, default_value_t = 0)]
        loss_burst_lookback: usize,
        /// Maximum losing reports inside --loss-burst-lookback. Zero disables the gate.
        #[arg(long, default_value_t = 0)]
        max_loss_burst_reports: usize,
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
        /// Official settlement-only BTC tape forwarded to every harness fold.
        #[arg(long)]
        settlement_btc_csv: Option<String>,
        /// Replay/backtest bankroll used for sizing.
        #[arg(long, default_value_t = 100.0)]
        bankroll: f64,
        /// Simulated insert latency in milliseconds.
        #[arg(long, default_value_t = 50)]
        latency_ms: u64,
        /// Forward latency audit JSON; overrides --latency-ms upward to the measured p99 recommendation.
        #[arg(long)]
        latency_audit_json: Option<String>,
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
        /// Replay exactly one serialized StrategyVariant instead of expanding the profile grid.
        #[arg(long)]
        variant_json: Option<String>,
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
    // Clap reads environment-backed args while parsing, so load local `.env`
    // before `Cli::parse()`. Settings::from_env repeats this as a harmless
    // fallback for non-CLI runtime config.
    let _ = config::load_dotenv_best_effort(".env");
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
        Command::DeriveApiCreds { env_file, base_url } => {
            if let Err(e) = cmd_derive_api_creds(&env_file, &base_url).await {
                eprintln!("derive-api-creds failed: {e}");
                std::process::exit(1);
            }
        }
        Command::BandPromotionArtifact {
            params,
            gate_artifact,
            fill_artifact,
            output,
        } => {
            if let Err(e) = cmd_band_promotion_artifact(&params, &gate_artifact, &fill_artifact, &output) {
                eprintln!("band promotion artifact failed: {e}");
                std::process::exit(1);
            }
        }
        Command::Scan {
            max_hours,
            min_liquidity,
        } => {
            cmd_scan(&settings, max_hours, min_liquidity).await;
        }
        Command::RecordBtcBooks {
            start,
            window_minutes,
            windows,
            duration_seconds,
            out_dir,
        } => {
            if let Err(e) = cmd_record_btc_books(
                &settings,
                start.as_deref(),
                window_minutes,
                windows,
                duration_seconds,
                &out_dir,
            )
            .await
            {
                eprintln!("record-btc-books failed: {e:#}");
                std::process::exit(1);
            }
        }
        Command::ConvertRecordedBtcBooks {
            input_dir,
            output_dir,
            condition_ids,
        } => {
            if let Err(e) = cmd_convert_recorded_btc_books(&input_dir, &output_dir, &condition_ids)
            {
                eprintln!("convert-recorded-btc-books failed: {e:#}");
                std::process::exit(1);
            }
        }
        Command::ForwardLatencyAudit {
            input_dir,
            output,
            max_p99_delay_ms,
            max_token_gap_ms,
            min_gap_gate_events,
            max_missing_timestamp_rate,
        } => {
            if let Err(e) = cmd_forward_latency_audit(
                &input_dir,
                output.as_deref(),
                max_p99_delay_ms,
                max_token_gap_ms,
                min_gap_gate_events,
                max_missing_timestamp_rate,
            ) {
                eprintln!("forward-latency-audit failed: {e:#}");
                std::process::exit(1);
            }
        }
        Command::ChainlinkDataStreamsProbe {
            endpoint,
            feed_ids,
            rest_websocket_username,
            api_key,
            hmac_secret,
            output,
        } => {
            if let Err(e) = cmd_chainlink_data_streams_probe(
                &endpoint,
                &feed_ids,
                rest_websocket_username.as_deref(),
                api_key.as_deref(),
                hmac_secret.as_deref(),
                output.as_deref(),
            )
            .await
            {
                eprintln!("chainlink-data-streams-probe failed: {e:#}");
                std::process::exit(1);
            }
        }
        Command::ChainlinkBackfill {
            endpoint,
            feed_id,
            api_key,
            hmac_secret,
            start,
            end,
            price_decimals,
            page_limit,
            output,
        } => {
            if let Err(e) = cmd_chainlink_backfill(
                &endpoint,
                &feed_id,
                &api_key,
                &hmac_secret,
                &start,
                &end,
                price_decimals,
                page_limit,
                &output,
            )
            .await
            {
                eprintln!("chainlink-backfill failed: {e:#}");
                std::process::exit(1);
            }
        }
        Command::FinalizeRecordedBtcBooks {
            input_dir,
            condition_ids,
            btc_csv,
            settlement_source_kind,
            output,
        } => {
            if let Err(e) = cmd_finalize_recorded_btc_books(
                &settings,
                &input_dir,
                &condition_ids,
                btc_csv.as_deref(),
                &settlement_source_kind,
                output.as_deref(),
            )
            .await
            {
                eprintln!("finalize-recorded-btc-books failed: {e:#}");
                std::process::exit(1);
            }
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
            pin_input_artifacts,
            require_shared_distilled,
            variant_json,
            condition_ids,
            btc_csv,
            settlement_btc_csv,
            fair_value_btc_csv,
            settlement_anchor_allocation_lock,
            settlement_anchor_source_audit,
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
            calibration_opportunities_json,
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
                pin_input_artifacts,
                require_shared_distilled,
                variant_json.as_deref(),
                condition_ids,
                btc_csv.as_deref(),
                settlement_btc_csv.as_deref(),
                fair_value_btc_csv.as_deref(),
                settlement_anchor_allocation_lock.as_deref(),
                settlement_anchor_source_audit.as_deref(),
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
                calibration_opportunities_json.as_deref(),
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

fn strategy_builder_json<T: serde::Serialize>(
    command: &str,
    output: Option<&str>,
    value: &T,
) -> String {
    let json = match serde_json::to_string_pretty(value) {
        Ok(json) => json,
        Err(error) => {
            eprintln!("strategy-builder {command} serialization failed: {error:#}");
            std::process::exit(2);
        }
    };
    if let Some(path) = output {
        if let Err(error) = artifact::write_json_artifact_atomic(path, value) {
            eprintln!("strategy-builder {command} output write failed: {error:#}");
            std::process::exit(2);
        }
    }
    json
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
        StrategyBuilderCommand::OpportunityTable {
            hour,
            signals,
            distilled_input,
            cache_dir,
            output,
            manifest,
            stake_usd,
            fee_rate,
            max_rows,
        } => {
            let result = strategy_builder::opportunity_table::create(
                strategy_builder::opportunity_table::OpportunityTableInput {
                    hour,
                    signals_path: std::path::PathBuf::from(signals),
                    cache_dir: cache_dir
                        .map(std::path::PathBuf::from)
                        .unwrap_or_else(backtest::pmxt::PMXTv2Loader::default_cache_dir),
                    output_path: std::path::PathBuf::from(output),
                    manifest_path: std::path::PathBuf::from(manifest),
                    stake_usd,
                    fee_rate,
                    max_rows,
                    distilled_input: distilled_input.map(std::path::PathBuf::from),
                },
            );
            match result {
                Ok(manifest) => println!(
                    "{}",
                    serde_json::to_string_pretty(&manifest)
                        .expect("serialize opportunity-table manifest")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-table failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityMarketCatalog {
            hour,
            base_catalog,
            gamma_url,
            output,
            manifest,
            family,
        } => {
            let family = match strategy_builder::opportunity_signals::MarketFamily::from_key(&family)
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("--family: {e:#}");
                    std::process::exit(2);
                }
            };
            let result = strategy_builder::opportunity_signals::fetch_market_catalog(
                strategy_builder::opportunity_signals::OpportunityMarketCatalogInput {
                    hours: hour,
                    base_catalog_path: base_catalog.map(std::path::PathBuf::from),
                    gamma_url,
                    output_path: std::path::PathBuf::from(output),
                    manifest_path: std::path::PathBuf::from(manifest),
                    family,
                },
            )
            .await;
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity market catalog")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-market-catalog failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunitySignals {
            hour,
            causal_windows,
            market_catalog,
            output,
            manifest,
            max_rows,
            family,
        } => {
            let family = match strategy_builder::opportunity_signals::MarketFamily::from_key(&family)
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("--family: {e:#}");
                    std::process::exit(2);
                }
            };
            let result = strategy_builder::opportunity_signals::create(
                strategy_builder::opportunity_signals::OpportunitySignalInput {
                    hour,
                    causal_windows_path: std::path::PathBuf::from(causal_windows),
                    market_catalog_path: std::path::PathBuf::from(market_catalog),
                    output_path: std::path::PathBuf::from(output),
                    manifest_path: std::path::PathBuf::from(manifest),
                    max_rows,
                    family,
                },
            );
            match result {
                Ok(manifest) => println!(
                    "{}",
                    serde_json::to_string_pretty(&manifest)
                        .expect("serialize opportunity-signals manifest")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-signals failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityPairFeatures {
            dataset_seal,
            market_catalog,
            cache_dir,
            output,
            manifest,
        } => {
            let result = strategy_builder::opportunity_liquidity::create_pair_features(
                strategy_builder::opportunity_liquidity::OpportunityPairFeatureInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    market_catalog_path: std::path::PathBuf::from(market_catalog),
                    cache_dir: std::path::PathBuf::from(cache_dir),
                    output_path: std::path::PathBuf::from(output),
                    manifest_path: std::path::PathBuf::from(manifest),
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity paired features")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-pair-features failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityFlowFeatures {
            dataset_seal,
            market_catalog,
            cache_dir,
            output,
            manifest,
        } => {
            let result = strategy_builder::opportunity_flow::create_feature_store(
                strategy_builder::opportunity_flow::OrderFlowFeatureInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    market_catalog_path: std::path::PathBuf::from(market_catalog),
                    cache_dir: std::path::PathBuf::from(cache_dir),
                    output_path: std::path::PathBuf::from(output),
                    manifest_path: std::path::PathBuf::from(manifest),
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity flow feature store")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-flow-features failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityCrossVenueFeatures {
            dataset_seal,
            paired_features_manifest,
            source_tape_manifest,
            output,
            manifest,
        } => {
            let result = strategy_builder::opportunity_cross_venue::create_feature_store(
                strategy_builder::opportunity_cross_venue::CrossVenueFeatureInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    paired_features_manifest_path: std::path::PathBuf::from(
                        paired_features_manifest,
                    ),
                    source_tape_manifest_path: std::path::PathBuf::from(source_tape_manifest),
                    output_path: std::path::PathBuf::from(output),
                    manifest_path: std::path::PathBuf::from(manifest),
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity cross-venue feature store")
                ),
                Err(error) => {
                    eprintln!(
                        "strategy-builder opportunity-cross-venue-features failed: {error:#}"
                    );
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityCrossVenuePreregister {
            dataset_seal,
            labels_manifest,
            feature_store_manifest,
            output,
            minimum_calibration_support,
            minimum_policy_support,
            safety_margin,
            latency_ms,
            maximum_exact_replays,
        } => {
            let result = strategy_builder::opportunity_cross_venue::preregister(
                strategy_builder::opportunity_cross_venue::CrossVenuePreregistrationInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    labels_manifest_path: std::path::PathBuf::from(labels_manifest),
                    feature_store_manifest_path: std::path::PathBuf::from(feature_store_manifest),
                    output_path: std::path::PathBuf::from(output),
                    minimum_calibration_support,
                    minimum_policy_support,
                    safety_margin,
                    latency_ms,
                    maximum_exact_replays,
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize cross-venue preregistration")
                ),
                Err(error) => {
                    eprintln!(
                        "strategy-builder opportunity-cross-venue-preregister failed: {error:#}"
                    );
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityCrossVenueSearch {
            preregistration,
            output,
        } => {
            let result = strategy_builder::opportunity_cross_venue::search(
                strategy_builder::opportunity_cross_venue::CrossVenueSearchInput {
                    preregistration_path: std::path::PathBuf::from(preregistration),
                    output_path: std::path::PathBuf::from(output),
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report).expect("serialize cross-venue search")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-cross-venue-search failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityCrossVenueDecision {
            preregistration,
            search_report,
            exact_replay_report,
            output,
        } => {
            let result = strategy_builder::opportunity_cross_venue::decide(
                strategy_builder::opportunity_cross_venue::CrossVenueDecisionInput {
                    preregistration_path: std::path::PathBuf::from(preregistration),
                    search_report_path: std::path::PathBuf::from(search_report),
                    exact_replay_report_path: exact_replay_report.map(std::path::PathBuf::from),
                    output_path: std::path::PathBuf::from(output),
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report).expect("serialize cross-venue decision")
                ),
                Err(error) => {
                    eprintln!(
                        "strategy-builder opportunity-cross-venue-decision failed: {error:#}"
                    );
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityFlowSearch {
            dataset_seal,
            labels_manifest,
            feature_store_manifest,
            output,
            minimum_calibration_support,
            minimum_policy_support,
            safety_margin,
            latency_ms,
            maximum_exact_replays,
        } => {
            let result = strategy_builder::opportunity_flow::search(
                strategy_builder::opportunity_flow::OrderFlowSearchInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    labels_manifest_path: std::path::PathBuf::from(labels_manifest),
                    feature_store_manifest_path: std::path::PathBuf::from(feature_store_manifest),
                    output_path: std::path::PathBuf::from(output),
                    minimum_calibration_support,
                    minimum_policy_support,
                    safety_margin,
                    latency_ms,
                    maximum_exact_replays,
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity flow search")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-flow-search failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityFlowDecision {
            preregistration,
            flow_search_report,
            exact_replay_report,
            output,
        } => {
            let result = strategy_builder::opportunity_flow::decide(
                strategy_builder::opportunity_flow::OrderFlowDecisionInput {
                    preregistration_path: std::path::PathBuf::from(preregistration),
                    flow_search_report_path: std::path::PathBuf::from(flow_search_report),
                    exact_replay_report_path: exact_replay_report.map(std::path::PathBuf::from),
                    output_path: std::path::PathBuf::from(output),
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity flow decision")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-flow-decision failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityLiquiditySearch {
            dataset_seal,
            labels_manifest,
            paired_features_manifest,
            output,
            minimum_calibration_support,
            minimum_policy_support,
            safety_margin,
            latency_ms,
            maximum_exact_replays,
        } => {
            let result = strategy_builder::opportunity_liquidity::search_liquidity(
                strategy_builder::opportunity_liquidity::OpportunityLiquiditySearchInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    labels_manifest_path: std::path::PathBuf::from(labels_manifest),
                    pair_features_manifest_path: std::path::PathBuf::from(paired_features_manifest),
                    output_path: std::path::PathBuf::from(output),
                    minimum_calibration_support,
                    minimum_policy_support,
                    safety_margin,
                    latency_ms,
                    maximum_exact_replays,
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity liquidity search")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-liquidity-search failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityLiquidityDecision {
            preregistration,
            liquidity_search_report,
            exact_replay_report,
            output,
        } => {
            let result = strategy_builder::opportunity_liquidity::decide_liquidity(
                strategy_builder::opportunity_liquidity::OpportunityLiquidityDecisionInput {
                    preregistration_path: std::path::PathBuf::from(preregistration),
                    liquidity_search_report_path: std::path::PathBuf::from(liquidity_search_report),
                    exact_replay_report_path: exact_replay_report.map(std::path::PathBuf::from),
                    output_path: std::path::PathBuf::from(output),
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity liquidity decision")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-liquidity-decision failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityDatasetSeal {
            opportunity_manifest,
            output,
        } => {
            let result = strategy_builder::opportunity_dataset::seal_dataset(
                strategy_builder::opportunity_dataset::OpportunityDatasetSealInput {
                    opportunity_manifest_paths: opportunity_manifest
                        .into_iter()
                        .map(std::path::PathBuf::from)
                        .collect(),
                    output_path: std::path::PathBuf::from(output),
                },
            );
            match result {
                Ok(seal) => println!(
                    "{}",
                    serde_json::to_string_pretty(&seal)
                        .expect("serialize opportunity dataset seal")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-dataset-seal failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityLabels {
            dataset_seal,
            label_source,
            output,
            manifest,
            resolution_rule,
        } => {
            let result = strategy_builder::opportunity_dataset::create_labels(
                strategy_builder::opportunity_dataset::OpportunityLabelsInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    label_source_path: std::path::PathBuf::from(label_source),
                    output_path: std::path::PathBuf::from(output),
                    manifest_path: std::path::PathBuf::from(manifest),
                    resolution_rule,
                },
            );
            match result {
                Ok(manifest) => println!(
                    "{}",
                    serde_json::to_string_pretty(&manifest)
                        .expect("serialize opportunity labels manifest")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-labels failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityFreshGate {
            dataset_seal,
            settlement_tape,
            policy,
            preregistration,
            consumed_dir,
            output,
        } => {
            let result = strategy_builder::opportunity_fresh_gate::run_fresh_gate(
                strategy_builder::opportunity_fresh_gate::FreshGateInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    settlement_tape_path: std::path::PathBuf::from(settlement_tape),
                    policy_path: std::path::PathBuf::from(policy),
                    preregistration_path: std::path::PathBuf::from(preregistration),
                    consumed_dir: std::path::PathBuf::from(consumed_dir),
                    output_path: std::path::PathBuf::from(output),
                },
            );
            match result {
                Ok(verdict) => println!(
                    "{}",
                    serde_json::to_string_pretty(&verdict)
                        .expect("serialize fresh-gate verdict")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-fresh-gate failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityPolicySearch {
            dataset_seal,
            labels_manifest,
            output,
            minimum_calibration_support,
            minimum_policy_support,
            safety_margin,
            latency_ms,
            maximum_exact_replays,
        } => {
            let result = strategy_builder::opportunity_policy::search(
                strategy_builder::opportunity_policy::OpportunityPolicySearchInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    labels_manifest_path: std::path::PathBuf::from(labels_manifest),
                    output_path: std::path::PathBuf::from(output),
                    minimum_calibration_support,
                    minimum_policy_support,
                    safety_margin,
                    latency_ms,
                    maximum_exact_replays,
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity policy search")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-policy-search failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityProbabilitySearch {
            dataset_seal,
            labels_manifest,
            output,
            minimum_calibration_support,
            maximum_calibration_brier_score,
            minimum_policy_support,
            safety_margin,
            latency_ms,
            maximum_exact_replays,
        } => {
            let result = strategy_builder::opportunity_probability::search(
                strategy_builder::opportunity_probability::OpportunityProbabilitySearchInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    labels_manifest_path: std::path::PathBuf::from(labels_manifest),
                    output_path: std::path::PathBuf::from(output),
                    minimum_calibration_support,
                    maximum_calibration_brier_score,
                    minimum_policy_support,
                    safety_margin,
                    latency_ms,
                    maximum_exact_replays,
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity probability search")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-probability-search failed: {error:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityProbabilityDecision {
            preregistration,
            probability_search_report,
            exact_replay_report,
            output,
        } => {
            let result = strategy_builder::opportunity_probability::decide(
                strategy_builder::opportunity_probability::OpportunityProbabilityDecisionInput {
                    preregistration_path: std::path::PathBuf::from(preregistration),
                    probability_search_report_path: std::path::PathBuf::from(
                        probability_search_report,
                    ),
                    exact_replay_report_path: std::path::PathBuf::from(exact_replay_report),
                    output_path: std::path::PathBuf::from(output),
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity probability decision")
                ),
                Err(error) => {
                    eprintln!(
                        "strategy-builder opportunity-probability-decision failed: {error:#}"
                    );
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::OpportunityExactReplay {
            dataset_seal,
            labels_manifest,
            policy_search_report,
            cache_dir,
            distilled_dir,
            output,
        } => {
            let result = strategy_builder::opportunity_replay::replay(
                strategy_builder::opportunity_replay::OpportunityExactReplayInput {
                    dataset_seal_path: std::path::PathBuf::from(dataset_seal),
                    labels_manifest_path: std::path::PathBuf::from(labels_manifest),
                    policy_search_report_path: std::path::PathBuf::from(policy_search_report),
                    cache_dir: std::path::PathBuf::from(cache_dir),
                    output_path: std::path::PathBuf::from(output),
                    distilled_dir: distilled_dir.map(std::path::PathBuf::from),
                },
            );
            match result {
                Ok(report) => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .expect("serialize opportunity exact replay")
                ),
                Err(error) => {
                    eprintln!("strategy-builder opportunity-exact-replay failed: {error:#}");
                    std::process::exit(2);
                }
            }
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
        StrategyBuilderCommand::BinaryComplementScreen {
            opportunity,
            resolution_manifest,
            block_id,
            output,
        } => {
            let screen = match strategy_builder::binary_complement_screen(
                strategy_builder::BinaryComplementScreenInput {
                    opportunity_paths: opportunity,
                    resolution_manifest_paths: resolution_manifest,
                    block_id,
                },
            ) {
                Ok(screen) => screen,
                Err(e) => {
                    eprintln!("strategy-builder binary-complement-screen failed: {e:#}");
                    std::process::exit(2);
                }
            };
            let ok = screen.ok;
            let json =
                strategy_builder_json("binary-complement-screen", output.as_deref(), &screen);
            println!("{json}");
            if !ok {
                std::process::exit(1);
            }
        }
        StrategyBuilderCommand::BinaryComplementRepeatAudit { screen, output } => {
            let audit = match strategy_builder::binary_complement_repeat_audit(
                strategy_builder::BinaryComplementRepeatAuditInput {
                    screen_paths: screen,
                },
            ) {
                Ok(audit) => audit,
                Err(e) => {
                    eprintln!("strategy-builder binary-complement-repeat-audit failed: {e:#}");
                    std::process::exit(2);
                }
            };
            let ok = audit.ok;
            let json =
                strategy_builder_json("binary-complement-repeat-audit", output.as_deref(), &audit);
            println!("{json}");
            if !ok {
                std::process::exit(1);
            }
        }
        StrategyBuilderCommand::SettlementAnchorAllocationLock {
            preregistration,
            variant_json,
            candidate_condition_set,
            prior_condition_set,
            report_output,
            trades_output,
            pair_audit_output,
            output,
        } => {
            if output.as_deref() == Some(report_output.as_str())
                || output.as_deref() == Some(trades_output.as_str())
                || output.as_deref() == Some(pair_audit_output.as_str())
            {
                eprintln!(
                    "strategy-builder settlement-anchor-allocation-lock output must differ from reserved score outputs"
                );
                std::process::exit(2);
            }
            let lock = match backtest::allocation_lock::build_settlement_anchor_allocation_lock(
                backtest::allocation_lock::SettlementAnchorAllocationLockInput {
                    preregistration_path: preregistration,
                    variant_path: variant_json,
                    candidate_condition_set_path: candidate_condition_set,
                    prior_condition_set_paths: prior_condition_set,
                    report_output_path: report_output,
                    trades_output_path: trades_output,
                    pair_audit_output_path: pair_audit_output,
                },
            ) {
                Ok(lock) => lock,
                Err(error) => {
                    eprintln!(
                        "strategy-builder settlement-anchor-allocation-lock failed: {error:#}"
                    );
                    std::process::exit(2);
                }
            };
            let ok = lock.ok;
            let json = strategy_builder_json(
                "settlement-anchor-allocation-lock",
                output.as_deref(),
                &lock,
            );
            println!("{json}");
            if !ok {
                std::process::exit(1);
            }
        }
        StrategyBuilderCommand::SettlementAnchorSourceAudit {
            condition_set,
            fair_value_btc_csv,
            output,
        } => {
            let audit = match backtest::settlement_anchor::settlement_anchor_source_audit(
                backtest::settlement_anchor::SettlementAnchorSourceAuditInput {
                    condition_set_path: condition_set,
                    fair_value_btc_csv_path: fair_value_btc_csv,
                },
            ) {
                Ok(audit) => audit,
                Err(error) => {
                    eprintln!("strategy-builder settlement-anchor-source-audit failed: {error:#}");
                    std::process::exit(2);
                }
            };
            let ok = audit.ok;
            let json =
                strategy_builder_json("settlement-anchor-source-audit", output.as_deref(), &audit);
            println!("{json}");
            if !ok {
                std::process::exit(1);
            }
        }
        StrategyBuilderCommand::SettlementAnchorPairAudit {
            allocation_lock,
            source_audit,
            fair_value_btc_csv,
            baseline_report,
            baseline_trades,
            official_report,
            official_trades,
            output,
        } => {
            if std::path::Path::new(&output).exists() {
                eprintln!(
                    "strategy-builder settlement-anchor-pair-audit refuses existing output: {output}"
                );
                std::process::exit(2);
            }
            let audit = match backtest::settlement_anchor::settlement_anchor_pair_audit(
                backtest::settlement_anchor::SettlementAnchorPairAuditInput {
                    allocation_lock_path: allocation_lock,
                    source_audit_path: source_audit,
                    fair_value_btc_csv_path: fair_value_btc_csv,
                    baseline_report_path: baseline_report,
                    baseline_trades_path: baseline_trades,
                    official_report_path: official_report,
                    official_trades_path: official_trades,
                    output_path: output.clone(),
                },
            ) {
                Ok(audit) => audit,
                Err(error) => {
                    eprintln!("strategy-builder settlement-anchor-pair-audit failed: {error:#}");
                    std::process::exit(2);
                }
            };
            let ok = audit.ok;
            let json = strategy_builder_json("settlement-anchor-pair-audit", Some(&output), &audit);
            println!("{json}");
            if !ok {
                std::process::exit(1);
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
            let json = strategy_builder_json("selectivity-search", output.as_deref(), &search);
            println!("{json}");
        }
        StrategyBuilderCommand::CausalPolicySearch {
            report,
            output,
            min_train_reports,
            min_train_trades,
            min_oos_trades,
            min_oos_wilson_win_rate_lower,
            min_oos_total_pnl,
            min_oos_profitable_reports,
            min_oos_eligible_reports,
            min_worst_oos_pnl,
            max_require_terms,
            max_deny_rules,
            max_deny_terms,
            min_deny_trades,
            min_deny_loss_pnl,
            min_deny_loss_reports,
            tail_alpha,
            min_oos_cvar_pnl,
            loss_burst_lookback,
            max_loss_burst_reports,
            tail_first_ranking,
            min_oos_payoff_ratio,
            max_oos_worst_loss_to_avg_win,
            prior_loss_cluster_lookback,
            max_prior_loss_burst_reports,
            min_prior_payoff_ratio,
            max_prior_worst_loss_to_avg_win,
            meta_label_min_support,
            meta_label_alpha,
            meta_label_min_quantile_pnl,
            meta_label_max_loss_rate,
            meta_label_require_supported,
            meta_label_max_generalization_terms,
            top,
        } => {
            let search = match strategy_builder::causal_policy_search(
                strategy_builder::StrategyBuilderCausalPolicySearchInput {
                    report_paths: report,
                    min_train_reports,
                    min_train_trades,
                    min_oos_trades,
                    min_oos_wilson_win_rate_lower,
                    min_oos_total_pnl,
                    min_oos_profitable_reports,
                    min_oos_eligible_reports,
                    min_worst_oos_pnl,
                    max_require_terms,
                    max_deny_rules,
                    max_deny_terms,
                    min_deny_trades,
                    min_deny_loss_pnl,
                    min_deny_loss_reports,
                    tail_alpha,
                    min_oos_cvar_pnl,
                    loss_burst_lookback,
                    max_loss_burst_reports,
                    tail_first_ranking,
                    min_oos_payoff_ratio,
                    max_oos_worst_loss_to_avg_win,
                    prior_loss_cluster_lookback,
                    max_prior_loss_burst_reports,
                    min_prior_payoff_ratio,
                    max_prior_worst_loss_to_avg_win,
                    meta_label_min_support,
                    meta_label_alpha,
                    meta_label_min_quantile_pnl,
                    meta_label_max_loss_rate,
                    meta_label_require_supported,
                    meta_label_max_generalization_terms,
                    top,
                },
            ) {
                Ok(search) => search,
                Err(e) => {
                    eprintln!("strategy-builder causal-policy-search failed: {e:#}");
                    std::process::exit(2);
                }
            };
            let json = strategy_builder_json("causal-policy-search", output.as_deref(), &search);
            println!("{json}");
        }
        StrategyBuilderCommand::EvolveSearch {
            report,
            historical_search,
            out_dir,
            seed,
            population,
            generations,
            elite_count,
            min_train_reports,
            min_train_trades,
            min_oos_trades,
            min_oos_wilson_win_rate_lower,
            min_oos_total_pnl,
            min_oos_profitable_reports,
            min_oos_eligible_reports,
            min_worst_oos_pnl,
            max_require_terms,
            max_deny_rules,
            max_deny_terms,
            min_deny_trades,
            min_deny_loss_pnl,
            min_deny_loss_reports,
            tail_alpha,
            min_oos_cvar_pnl,
            loss_burst_lookback,
            max_loss_burst_reports,
            min_oos_payoff_ratio,
            max_oos_worst_loss_to_avg_win,
            prior_loss_cluster_lookback,
            max_prior_loss_burst_reports,
            min_prior_payoff_ratio,
            max_prior_worst_loss_to_avg_win,
            meta_label_min_support,
            meta_label_alpha,
            meta_label_min_quantile_pnl,
            meta_label_max_loss_rate,
            meta_label_require_supported,
            meta_label_max_generalization_terms,
            top,
            replay_start,
            replay_end,
            replay_profile,
            replay_zone_mode,
            latency_ms,
            latency_audit_json,
            btc_csv,
            fold_hours,
            threads,
            window_minutes,
            atomic_parquet,
        } => {
            let search = match strategy_builder::evolve_search(
                strategy_builder::StrategyBuilderEvolveSearchInput {
                    report_paths: report,
                    historical_search_paths: historical_search,
                    out_dir: std::path::PathBuf::from(out_dir),
                    seed,
                    population,
                    generations,
                    elite_count,
                    min_train_reports,
                    min_train_trades,
                    min_oos_trades,
                    min_oos_wilson_win_rate_lower,
                    min_oos_total_pnl,
                    min_oos_profitable_reports,
                    min_oos_eligible_reports,
                    min_worst_oos_pnl,
                    max_require_terms,
                    max_deny_rules,
                    max_deny_terms,
                    min_deny_trades,
                    min_deny_loss_pnl,
                    min_deny_loss_reports,
                    tail_alpha,
                    min_oos_cvar_pnl,
                    loss_burst_lookback,
                    max_loss_burst_reports,
                    min_oos_payoff_ratio,
                    max_oos_worst_loss_to_avg_win,
                    prior_loss_cluster_lookback,
                    max_prior_loss_burst_reports,
                    min_prior_payoff_ratio,
                    max_prior_worst_loss_to_avg_win,
                    meta_label_min_support,
                    meta_label_alpha,
                    meta_label_min_quantile_pnl,
                    meta_label_max_loss_rate,
                    meta_label_require_supported,
                    meta_label_max_generalization_terms,
                    top,
                    replay_start,
                    replay_end,
                    replay_profile,
                    replay_zone_mode,
                    latency_ms,
                    latency_audit_json,
                    btc_csv,
                    fold_hours,
                    threads,
                    window_minutes,
                    atomic_parquet,
                },
            ) {
                Ok(search) => search,
                Err(e) => {
                    eprintln!("strategy-builder evolve-search failed: {e:#}");
                    std::process::exit(2);
                }
            };
            let json = strategy_builder_json("evolve-search", None, &search);
            println!("{json}");
        }
        StrategyBuilderCommand::MaterializePolicyVariant {
            search,
            source_report,
            rank,
            output,
        } => {
            let materialized = match strategy_builder::materialize_policy_variant(
                strategy_builder::StrategyBuilderMaterializePolicyVariantInput {
                    search_path: std::path::PathBuf::from(search),
                    source_report_paths: source_report,
                    rank,
                    output_path: std::path::PathBuf::from(output),
                },
            ) {
                Ok(materialized) => materialized,
                Err(e) => {
                    eprintln!("strategy-builder materialize-policy-variant failed: {e:#}");
                    std::process::exit(2);
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&materialized)
                    .expect("serialize strategy-builder materialized policy variant")
            );
        }
        StrategyBuilderCommand::MaterializeSweepVariant {
            report,
            rank,
            output,
            require_causal_tag,
            deny_causal_tag,
        } => {
            let materialized = match strategy_builder::materialize_sweep_variant(
                strategy_builder::StrategyBuilderMaterializeSweepVariantInput {
                    report_path: std::path::PathBuf::from(report),
                    rank,
                    output_path: std::path::PathBuf::from(output),
                    require_causal_tag,
                    deny_causal_tag,
                },
            ) {
                Ok(materialized) => materialized,
                Err(e) => {
                    eprintln!("strategy-builder materialize-sweep-variant failed: {e:#}");
                    std::process::exit(2);
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&materialized)
                    .expect("serialize strategy-builder materialized sweep variant")
            );
        }
        StrategyBuilderCommand::FeatureFilterSearch {
            feature,
            base_variant,
            out_dir,
            top,
            max_require_terms,
            max_deny_terms,
            min_atom_trades,
            max_atoms,
            min_total_trades,
            min_eligible_reports,
            min_total_pnl,
            min_worst_report_pnl,
        } => {
            let search = match strategy_builder::feature_filter_search(
                strategy_builder::StrategyBuilderFeatureFilterSearchInput {
                    feature_paths: feature,
                    base_variant_path: std::path::PathBuf::from(base_variant),
                    out_dir: std::path::PathBuf::from(out_dir),
                    top,
                    max_require_terms,
                    max_deny_terms,
                    min_atom_trades,
                    max_atoms,
                    min_total_trades,
                    min_eligible_reports,
                    min_total_pnl,
                    min_worst_report_pnl,
                },
            ) {
                Ok(search) => search,
                Err(e) => {
                    eprintln!("strategy-builder feature-filter-search failed: {e:#}");
                    std::process::exit(2);
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&search)
                    .expect("serialize strategy-builder feature filter search")
            );
        }
        StrategyBuilderCommand::CausalPolicyReplayPlan {
            search,
            start,
            end,
            out_dir,
            output,
            top,
            include_failed,
            cache_root,
            btc_csv,
            bankroll,
            latency_ms,
            latency_audit_json,
            threads,
            window_minutes,
            fold_hours,
            max_folds,
            profile,
            zone_mode,
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
            let input = CausalPolicyReplayPlanInput {
                search_path: std::path::PathBuf::from(search),
                start,
                end,
                out_dir: std::path::PathBuf::from(out_dir),
                output: output.map(std::path::PathBuf::from),
                top,
                include_failed,
                cache_root: cache_root.map(std::path::PathBuf::from),
                btc_csv,
                bankroll,
                latency_ms,
                latency_audit_json: latency_audit_json.map(std::path::PathBuf::from),
                threads,
                window_minutes,
                fold_hours,
                max_folds,
                profile,
                zone_mode,
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
            match run_causal_policy_replay_plan(input).await {
                Ok(summary) => println!(
                    "{}",
                    serde_json::to_string_pretty(&summary)
                        .expect("serialize causal policy replay plan")
                ),
                Err(e) => {
                    eprintln!("strategy-builder causal-policy-replay-plan failed: {e:#}");
                    std::process::exit(2);
                }
            }
        }
        StrategyBuilderCommand::RegistryMark {
            registry,
            strategy_id,
            parent_id,
            status,
            reason,
            artifact,
            metrics,
            evidence,
            note,
        } => {
            let status = match strategy_builder::StrategyRegistryStatus::parse(&status) {
                Ok(status) => status,
                Err(e) => {
                    eprintln!("strategy-builder registry-mark failed: {e:#}");
                    std::process::exit(2);
                }
            };
            let registry = match strategy_builder::mark_strategy_version(
                strategy_builder::StrategyRegistryMarkInput {
                    registry_path: std::path::PathBuf::from(registry),
                    strategy_id,
                    parent_id,
                    status,
                    reason,
                    artifact_path: artifact,
                    metrics_path: metrics,
                    evidence_paths: evidence,
                    notes: note,
                },
            ) {
                Ok(registry) => registry,
                Err(e) => {
                    eprintln!("strategy-builder registry-mark failed: {e:#}");
                    std::process::exit(2);
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&registry)
                    .expect("serialize strategy-builder registry")
            );
        }
        StrategyBuilderCommand::RegistryAudit {
            registry,
            durable_prefix,
            output,
        } => {
            let audit = match strategy_builder::audit_strategy_registry(
                strategy_builder::StrategyRegistryAuditInput {
                    registry_path: std::path::PathBuf::from(registry),
                    durable_prefix,
                },
            ) {
                Ok(audit) => audit,
                Err(e) => {
                    eprintln!("strategy-builder registry-audit failed: {e:#}");
                    std::process::exit(2);
                }
            };
            if let Some(path) = output {
                if let Err(e) = write_json_atomic(std::path::Path::new(&path), &audit, true) {
                    eprintln!("strategy-builder registry-audit output write failed: {e:#}");
                    std::process::exit(2);
                }
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&audit)
                    .expect("serialize strategy-builder registry audit")
            );
            if !audit.ok {
                std::process::exit(1);
            }
        }
        StrategyBuilderCommand::EvidenceExport {
            registry,
            out_dir,
            manifest,
            rewrite_registry,
        } => {
            let export = match strategy_builder::export_strategy_evidence(
                strategy_builder::StrategyBuilderEvidenceExportInput {
                    registry_path: std::path::PathBuf::from(registry),
                    out_dir: std::path::PathBuf::from(out_dir),
                    rewrite_registry,
                },
            ) {
                Ok(export) => export,
                Err(e) => {
                    eprintln!("strategy-builder evidence-export failed: {e:#}");
                    std::process::exit(2);
                }
            };
            if let Some(path) = manifest {
                if let Err(e) = write_json_atomic(std::path::Path::new(&path), &export, true) {
                    eprintln!("strategy-builder evidence-export manifest write failed: {e:#}");
                    std::process::exit(2);
                }
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&export)
                    .expect("serialize strategy-builder evidence export")
            );
        }
        StrategyBuilderCommand::MultiGuardSearch {
            report,
            output,
            min_train_reports,
            min_train_trades,
            min_oos_trades,
            min_oos_wilson_win_rate_lower,
            min_oos_total_pnl,
            min_oos_profitable_reports,
            min_worst_oos_pnl,
            max_rules,
            min_guard_trades,
            min_guard_loss_pnl,
            min_guard_loss_reports,
            recent_report_lookback,
            pattern_guards,
            tail_alpha,
            min_oos_cvar_pnl,
            loss_burst_lookback,
            max_loss_burst_reports,
            top,
        } => {
            let search = match strategy_builder::multi_guard_search(
                strategy_builder::StrategyBuilderMultiGuardSearchInput {
                    report_paths: report,
                    min_train_reports,
                    min_train_trades,
                    min_oos_trades,
                    min_oos_wilson_win_rate_lower,
                    min_oos_total_pnl,
                    min_oos_profitable_reports,
                    min_worst_oos_pnl,
                    max_rules,
                    min_guard_trades,
                    min_guard_loss_pnl,
                    min_guard_loss_reports,
                    recent_report_lookback,
                    pattern_guards,
                    tail_alpha,
                    min_oos_cvar_pnl,
                    loss_burst_lookback,
                    max_loss_burst_reports,
                    top,
                },
            ) {
                Ok(search) => search,
                Err(e) => {
                    eprintln!("strategy-builder multi-guard-search failed: {e:#}");
                    std::process::exit(2);
                }
            };
            let json = strategy_builder_json("multi-guard-search", output.as_deref(), &search);
            println!("{json}");
        }
        StrategyBuilderCommand::AdaptiveDirectionSearch {
            report,
            output,
            min_train_reports,
            min_train_trades,
            min_oos_trades,
            min_oos_wilson_win_rate_lower,
            min_oos_total_pnl,
            min_oos_profitable_reports,
            min_worst_oos_pnl,
            tail_alpha,
            min_oos_cvar_pnl,
            loss_burst_lookback,
            max_loss_burst_reports,
            top,
        } => {
            let search = match strategy_builder::adaptive_direction_search(
                strategy_builder::StrategyBuilderAdaptiveDirectionInput {
                    report_paths: report,
                    min_train_reports,
                    min_train_trades,
                    min_oos_trades,
                    min_oos_wilson_win_rate_lower,
                    min_oos_total_pnl,
                    min_oos_profitable_reports,
                    min_worst_oos_pnl,
                    tail_alpha,
                    min_oos_cvar_pnl,
                    loss_burst_lookback,
                    max_loss_burst_reports,
                    top,
                },
            ) {
                Ok(search) => search,
                Err(e) => {
                    eprintln!("strategy-builder adaptive-direction-search failed: {e:#}");
                    std::process::exit(2);
                }
            };
            let json =
                strategy_builder_json("adaptive-direction-search", output.as_deref(), &search);
            println!("{json}");
        }
        StrategyBuilderCommand::AdaptiveModeSearch {
            report,
            output,
            min_train_reports,
            min_train_trades,
            min_oos_trades,
            min_oos_wilson_win_rate_lower,
            min_oos_total_pnl,
            min_oos_profitable_reports,
            min_worst_oos_pnl,
            max_guard_rules,
            min_guard_trades,
            min_guard_loss_pnl,
            min_guard_loss_reports,
            recent_report_lookback,
            pattern_guards,
            flat_if_worst_train_below,
            tail_alpha,
            min_oos_cvar_pnl,
            loss_burst_lookback,
            max_loss_burst_reports,
            top,
        } => {
            let search = match strategy_builder::adaptive_mode_search(
                strategy_builder::StrategyBuilderAdaptiveModeInput {
                    report_paths: report,
                    min_train_reports,
                    min_train_trades,
                    min_oos_trades,
                    min_oos_wilson_win_rate_lower,
                    min_oos_total_pnl,
                    min_oos_profitable_reports,
                    min_worst_oos_pnl,
                    max_guard_rules,
                    min_guard_trades,
                    min_guard_loss_pnl,
                    min_guard_loss_reports,
                    recent_report_lookback,
                    pattern_guards,
                    flat_if_worst_train_below,
                    tail_alpha,
                    min_oos_cvar_pnl,
                    loss_burst_lookback,
                    max_loss_burst_reports,
                    top,
                },
            ) {
                Ok(search) => search,
                Err(e) => {
                    eprintln!("strategy-builder adaptive-mode-search failed: {e:#}");
                    std::process::exit(2);
                }
            };
            let json = strategy_builder_json("adaptive-mode-search", output.as_deref(), &search);
            println!("{json}");
        }
        StrategyBuilderCommand::RollingHistory {
            start,
            end,
            out_dir,
            cache_root,
            btc_csv,
            settlement_btc_csv,
            bankroll,
            latency_ms,
            threads,
            window_minutes,
            fold_hours,
            max_folds,
            profile,
            variant_json,
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
            latency_audit_json,
        } => {
            let input = RollingHistoryInput {
                start,
                end,
                out_dir: std::path::PathBuf::from(out_dir),
                cache_root: cache_root.map(std::path::PathBuf::from),
                btc_csv,
                settlement_btc_csv,
                bankroll,
                latency_ms,
                latency_audit_json: latency_audit_json.map(std::path::PathBuf::from),
                threads,
                window_minutes,
                fold_hours,
                max_folds,
                profile,
                variant_json: variant_json.map(std::path::PathBuf::from),
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
    settlement_btc_csv: Option<String>,
    bankroll: f64,
    latency_ms: u64,
    latency_audit_json: Option<std::path::PathBuf>,
    threads: usize,
    window_minutes: f64,
    fold_hours: i64,
    max_folds: Option<usize>,
    profile: String,
    variant_json: Option<std::path::PathBuf>,
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

#[derive(Debug, Clone)]
struct CausalPolicyReplayPlanInput {
    search_path: std::path::PathBuf,
    start: String,
    end: String,
    out_dir: std::path::PathBuf,
    output: Option<std::path::PathBuf>,
    top: usize,
    include_failed: bool,
    cache_root: Option<std::path::PathBuf>,
    btc_csv: Option<String>,
    bankroll: f64,
    latency_ms: u64,
    latency_audit_json: Option<std::path::PathBuf>,
    threads: usize,
    window_minutes: f64,
    fold_hours: i64,
    max_folds: Option<usize>,
    profile: String,
    zone_mode: String,
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

fn rolling_history_latency_policy(
    requested_latency_ms: u64,
    latency_audit_json: Option<&std::path::Path>,
) -> anyhow::Result<(u64, serde_json::Value)> {
    use anyhow::{bail, Context};

    let Some(path) = latency_audit_json else {
        return Ok((
            requested_latency_ms,
            serde_json::json!({
                "source": "cli",
                "requested_latency_ms": requested_latency_ms,
                "effective_latency_ms": requested_latency_ms,
                "audit_applied": false,
                "override_applied": false,
            }),
        ));
    };
    let audit = read_json_value(path)?;
    let gate = audit
        .get("a_plus_latency_gate")
        .context("latency audit missing a_plus_latency_gate")?;
    let stream_latency_ready = gate
        .get("stream_latency_ready")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !stream_latency_ready {
        bail!(
            "latency audit {} is not stream-ready; verdict={}",
            path.display(),
            gate.get("verdict")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
        );
    }
    let recommended_latency_ms = gate
        .get("recommended_retest_latency_ms")
        .and_then(|v| v.as_u64())
        .context("latency audit missing recommended_retest_latency_ms")?;
    let effective_latency_ms = requested_latency_ms.max(recommended_latency_ms);
    Ok((
        effective_latency_ms,
        serde_json::json!({
            "source": "forward_latency_audit",
            "audit_path": path.display().to_string(),
            "requested_latency_ms": requested_latency_ms,
            "recommended_retest_latency_ms": recommended_latency_ms,
            "effective_latency_ms": effective_latency_ms,
            "audit_applied": true,
            "override_applied": effective_latency_ms != requested_latency_ms,
            "audit_verdict": gate.get("verdict").cloned().unwrap_or(serde_json::Value::Null),
            "audit_p99_delay_ms": audit
                .get("delay_ms")
                .and_then(|v| v.get("p99"))
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        }),
    ))
}

async fn run_causal_policy_replay_plan(
    input: CausalPolicyReplayPlanInput,
) -> anyhow::Result<serde_json::Value> {
    use anyhow::{bail, Context};

    if input.top == 0 {
        bail!("--top must be > 0");
    }
    let raw = std::fs::read_to_string(&input.search_path)
        .with_context(|| format!("read {}", input.search_path.display()))?;
    let search: serde_json::Value = serde_json::from_str(&raw)
        .with_context(|| format!("parse {}", input.search_path.display()))?;
    let candidates = search
        .get("candidates")
        .and_then(|value| value.as_array())
        .context("causal-policy-search artifact has no candidates array")?;

    let mut selected = Vec::new();
    for candidate in candidates.iter().take(input.top) {
        let rank = candidate
            .get("rank")
            .and_then(|value| value.as_u64())
            .context("candidate missing numeric rank")?;
        let passed = candidate
            .get("passed")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        if !input.include_failed && !passed {
            continue;
        }

        let require_causal_tag =
            causal_policy_arg_array(candidate, &["final_policy", "harness_require_args"])?;
        let deny_causal_tag =
            causal_policy_arg_array(candidate, &["final_policy", "harness_deny_args"])?;
        let variant_json = candidate_variant_json_path(candidate, &input.search_path);
        if variant_json.is_none() && require_causal_tag.is_empty() && deny_causal_tag.is_empty() {
            bail!("candidate rank {rank} has no runtime-supported harness policy args");
        }

        let candidate_id = format!("candidate_rank_{rank:03}");
        let candidate_out_dir = input.out_dir.join(&candidate_id);
        let candidate_cache_root = input
            .cache_root
            .as_ref()
            .map(|root| root.join(&candidate_id));
        let rolling = run_rolling_history(RollingHistoryInput {
            start: input.start.clone(),
            end: input.end.clone(),
            out_dir: candidate_out_dir.clone(),
            cache_root: candidate_cache_root,
            btc_csv: input.btc_csv.clone(),
            settlement_btc_csv: None,
            bankroll: input.bankroll,
            latency_ms: input.latency_ms,
            latency_audit_json: input.latency_audit_json.clone(),
            threads: input.threads,
            window_minutes: input.window_minutes,
            fold_hours: input.fold_hours,
            max_folds: input.max_folds,
            profile: input.profile.clone(),
            variant_json: variant_json.clone(),
            require_causal_tag: if variant_json.is_some() {
                Vec::new()
            } else {
                require_causal_tag.clone()
            },
            deny_causal_tag: if variant_json.is_some() {
                Vec::new()
            } else {
                deny_causal_tag.clone()
            },
            zone_mode: input.zone_mode.clone(),
            promotion_output: None,
            execute: input.execute,
            delete_after_process: input.delete_after_process,
            atomic_parquet: input.atomic_parquet,
            preflight_pmxt_hours: input.preflight_pmxt_hours,
            stop_at_first_missing_hour: input.stop_at_first_missing_hour,
            require_full_folds: input.require_full_folds,
            min_fold_trades: input.min_fold_trades,
            min_fold_target_events: input.min_fold_target_events,
            min_fold_top_trades: input.min_fold_top_trades,
            min_promotion_trades: input.min_promotion_trades,
            min_promotion_daily_trades: input.min_promotion_daily_trades,
            min_promotion_profitable_reports: input.min_promotion_profitable_reports,
            min_promotion_losses: input.min_promotion_losses,
            max_cache_gb: input.max_cache_gb,
            min_neighbor_observations: input.min_neighbor_observations,
            min_neighbor_positive_rate: input.min_neighbor_positive_rate,
            max_pbo: input.max_pbo,
            min_median_oos_percentile: input.min_median_oos_percentile,
        })
        .await
        .with_context(|| format!("build replay plan for candidate rank {rank}"))?;

        selected.push(serde_json::json!({
            "rank": rank,
            "passed_static_search": passed,
            "base_require": candidate.get("base_require").cloned().unwrap_or(serde_json::Value::Null),
            "harness_require_args": require_causal_tag,
            "harness_deny_args": deny_causal_tag,
            "variant_json": variant_json.as_ref().map(|path| path.display().to_string()),
            "search_fold_forward": candidate.get("fold_forward").cloned().unwrap_or(serde_json::Value::Null),
            "search_notes": candidate.get("notes").cloned().unwrap_or(serde_json::Value::Null),
            "replay_out_dir": candidate_out_dir.display().to_string(),
            "replay_manifest": candidate_out_dir.join("rolling_history_manifest.json").display().to_string(),
            "rolling_history": rolling,
        }));
    }

    let output_path = input
        .output
        .clone()
        .unwrap_or_else(|| input.out_dir.join("causal_policy_replay_plan.json"));
    let summary = serde_json::json!({
        "schema_version": 1,
        "mode": if input.execute { "executed" } else { "dry_run" },
        "source_search": input.search_path.display().to_string(),
        "search_ok": search.get("ok").cloned().unwrap_or(serde_json::Value::Null),
        "search_report_count": search.get("report_count").cloned().unwrap_or(serde_json::Value::Null),
        "candidate_filter": {
            "top": input.top,
            "include_failed": input.include_failed,
        },
        "selected_count": selected.len(),
        "replay_window": {
            "start": input.start,
            "end": input.end,
            "fold_hours": input.fold_hours,
            "window_minutes": input.window_minutes,
        },
        "profile": input.profile,
        "zone_mode": input.zone_mode,
        "latency_ms": input.latency_ms,
        "latency_audit_json": input.latency_audit_json.map(|path| path.display().to_string()),
        "methodology": [
            "Read causal-policy-search candidates and use only runtime-supported harness require/deny args.",
            "Generate one rolling-history replay manifest per selected candidate.",
            "Static search stats are carried as context only; replay manifests are the required next evidence before promotion credit."
        ],
        "output": output_path.display().to_string(),
        "candidates": selected,
    });
    write_json_atomic(&output_path, &summary, true)
        .with_context(|| format!("write {}", output_path.display()))?;
    Ok(summary)
}

fn causal_policy_arg_array(
    candidate: &serde_json::Value,
    path: &[&str],
) -> anyhow::Result<Vec<String>> {
    use anyhow::{bail, Context};

    let mut value = candidate;
    for key in path {
        value = value
            .get(*key)
            .with_context(|| format!("candidate missing {}", path.join(".")))?;
    }
    let array = value
        .as_array()
        .with_context(|| format!("{} must be an array", path.join(".")))?;
    let mut out = Vec::with_capacity(array.len());
    for item in array {
        let Some(s) = item.as_str() else {
            bail!("{} contains a non-string value", path.join("."));
        };
        out.push(s.to_string());
    }
    Ok(out)
}

fn candidate_variant_json_path(
    candidate: &serde_json::Value,
    search_path: &std::path::Path,
) -> Option<std::path::PathBuf> {
    let raw = candidate
        .get("variant_path")
        .and_then(|value| value.as_str())?;
    if raw.trim().is_empty() {
        return None;
    }
    let path = std::path::PathBuf::from(raw);
    if path.is_absolute() {
        Some(path)
    } else if let Some(parent) = search_path.parent() {
        Some(parent.join(&path))
    } else {
        Some(path)
    }
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
    if input.variant_json.is_some()
        && (!input.require_causal_tag.is_empty() || !input.deny_causal_tag.is_empty())
    {
        bail!("--variant-json cannot be combined with rolling-history causal tag filters");
    }
    let (effective_latency_ms, latency_policy) =
        rolling_history_latency_policy(input.latency_ms, input.latency_audit_json.as_deref())?;

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
    let zone_audit_output = zone_audit_output_for_promotion(&promotion_output);

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
                         promote_args: &[String],
                         zone_audit_args: &[String]| {
        serde_json::json!({
            "schema_version": 1,
            "mode": if input.execute { "executed" } else { "dry_run" },
            "promotion_status": promotion_status,
            "promotion_error": promotion_error,
            "profile": profile,
            "variant_json": input.variant_json.as_ref().map(|path| path.display().to_string()),
            "fold_hours": input.fold_hours,
            "latency_policy": latency_policy.clone(),
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
                "min_neighbor_count": if input.variant_json.is_some() { 0 } else { 2 },
                "min_neighbor_observations": input.min_neighbor_observations.unwrap_or(0),
                "min_neighbor_positive_rate": if input.variant_json.is_some() {
                    0.0
                } else {
                    input.min_neighbor_positive_rate
                },
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
            "zone_audit_output": zone_audit_output.display().to_string(),
            "zone_audit_args": zone_audit_args,
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
            effective_latency_ms.to_string(),
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
        if let Some(settlement_btc_csv) = &input.settlement_btc_csv {
            hydrate_args.extend([
                "--settlement-btc-csv".to_string(),
                settlement_btc_csv.clone(),
            ]);
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
            "--latency-ms".to_string(),
            effective_latency_ms.to_string(),
            "--window-minutes".to_string(),
            cli_float_arg(input.window_minutes),
            "--continuous".to_string(),
            "--report-json".to_string(),
            sweep_report.display().to_string(),
        ];
        if let Some(variant_json) = &input.variant_json {
            sweep_args = vec![
                "harness-sweep".to_string(),
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
                "--variant-json".to_string(),
                variant_json.display().to_string(),
                "--top".to_string(),
                "20".to_string(),
                "--threads".to_string(),
                input.threads.to_string(),
                "--latency-ms".to_string(),
                effective_latency_ms.to_string(),
                "--window-minutes".to_string(),
                cli_float_arg(input.window_minutes),
                "--continuous".to_string(),
                "--report-json".to_string(),
                sweep_report.display().to_string(),
            ];
        }
        for tag in &input.require_causal_tag {
            sweep_args.push("--require-causal-tag".to_string());
            sweep_args.push(tag.clone());
        }
        for tag in &input.deny_causal_tag {
            sweep_args.push("--deny-causal-tag".to_string());
            sweep_args.push(tag.clone());
        }
        if input.variant_json.is_none() {
            if !profile.taker_only {
                sweep_args.push("--also-maker".to_string());
            }
            if profile.degraded_force_taker {
                sweep_args.push("--degraded-force-taker".to_string());
            }
            if profile.taker_only {
                sweep_args.push("--taker-only".to_string());
            }
        }
        if input.atomic_parquet {
            sweep_args.push("--atomic-parquet".to_string());
        }
        if let Some(btc_csv) = &input.btc_csv {
            sweep_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
        }
        if let Some(settlement_btc_csv) = &input.settlement_btc_csv {
            sweep_args.extend([
                "--settlement-btc-csv".to_string(),
                settlement_btc_csv.clone(),
            ]);
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
    let min_promotion_neighbor_count = if input.variant_json.is_some() { 0 } else { 2 };
    let min_promotion_neighbor_positive_rate = if input.variant_json.is_some() {
        0.0
    } else {
        input.min_neighbor_positive_rate
    };
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
        min_promotion_neighbor_count.to_string(),
        "--min-neighbor-observations".to_string(),
        min_neighbor_observations.to_string(),
        "--min-neighbor-positive-rate".to_string(),
        min_promotion_neighbor_positive_rate.to_string(),
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
    let max_zone_trade_share = if zone_mode == backtest::sweep::ZoneMode::All {
        "0.70"
    } else {
        "1.0"
    };
    let mut zone_audit_args = vec![
        "experiment".to_string(),
        "zone-audit".to_string(),
        "--output".to_string(),
        zone_audit_output.display().to_string(),
        "--max-zone-trade-share".to_string(),
        max_zone_trade_share.to_string(),
        "--min-zone-pnl".to_string(),
        "0".to_string(),
    ];
    for report in &sweep_reports {
        zone_audit_args.extend(["--report".to_string(), report.display().to_string()]);
    }

    if input.execute {
        std::fs::create_dir_all(&out_dir)
            .with_context(|| format!("create {}", out_dir.display()))?;
        let manifest = out_dir.join("rolling_history_manifest.json");
        let zone_audit_pending_summary = build_summary(
            "zone_audit_pending",
            None,
            &fold_summaries,
            &promote_args,
            &zone_audit_args,
        );
        write_json_atomic(&manifest, &zone_audit_pending_summary, false)
            .with_context(|| format!("write {}", manifest.display()))?;
        if let Err(err) = run_child(&exe, &zone_audit_args) {
            let failed_summary = build_summary(
                "zone_audit_failed",
                Some(err.to_string()),
                &fold_summaries,
                &promote_args,
                &zone_audit_args,
            );
            write_json_atomic(&manifest, &failed_summary, false)
                .with_context(|| format!("write {}", manifest.display()))?;
            return Err(err);
        }
        let pending_summary = build_summary(
            "promotion_pending",
            None,
            &fold_summaries,
            &promote_args,
            &zone_audit_args,
        );
        write_json_atomic(&manifest, &pending_summary, false)
            .with_context(|| format!("write {}", manifest.display()))?;
        if let Err(err) = run_child(&exe, &promote_args) {
            let failed_summary = build_summary(
                "promotion_failed",
                Some(err.to_string()),
                &fold_summaries,
                &promote_args,
                &zone_audit_args,
            );
            write_json_atomic(&manifest, &failed_summary, false)
                .with_context(|| format!("write {}", manifest.display()))?;
            return Err(err);
        }
        let summary = build_summary(
            "promotion_passed",
            None,
            &fold_summaries,
            &promote_args,
            &zone_audit_args,
        );
        write_json_atomic(&manifest, &summary, false)
            .with_context(|| format!("write {}", manifest.display()))?;
        return Ok(summary);
    }

    let summary = build_summary(
        "dry_run",
        None,
        &fold_summaries,
        &promote_args,
        &zone_audit_args,
    );
    std::fs::create_dir_all(&out_dir).with_context(|| format!("create {}", out_dir.display()))?;
    let manifest = out_dir.join("rolling_history_manifest.json");
    write_json_atomic(&manifest, &summary, false)
        .with_context(|| format!("write {}", manifest.display()))?;
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
        "a_plus5m_tail_guard" => {
            let mut profile = rolling_history_profile("a_plus5m_causal_guard_selected")?;
            profile.name = name.to_string();
            profile.z = "0.90,1.10".to_string();
            profile.position_pct = "0.025".to_string();
            profile.max_per_market_usd = "10".to_string();
            profile.max_total_exposure_usd = "8".to_string();
            profile.max_projected_stressed_drawdown_pct = "0.12".to_string();
            profile.degraded_after_losses = "1".to_string();
            profile.degraded_min_z = "1.10".to_string();
            profile.degraded_max_price = "0.75".to_string();
            profile.degraded_force_taker = true;
            profile.taker_only = true;
            profile
        }
        "a_plus5m_tail_primary" => {
            let mut profile = rolling_history_profile("a_plus5m_tail_guard")?;
            profile.name = name.to_string();
            profile.conf = "0.40,0.50".to_string();
            profile.z = "0.70,0.90".to_string();
            profile.max_price = "0.85,0.90".to_string();
            profile.min_reversion_count = "1".to_string();
            profile.max_reversion_count = "2".to_string();
            profile.position_pct = "0.05".to_string();
            profile
        }
        "a_plus5m_tail_early_reentry" => {
            let mut profile = rolling_history_profile("a_plus5m_tail_guard")?;
            profile.name = name.to_string();
            profile.conf = "0.60,0.70".to_string();
            profile.z = "1.10,1.30".to_string();
            profile.edge = "0.10,0.15".to_string();
            profile.max_price = "0.75".to_string();
            profile.min_reversion_count = "1".to_string();
            profile.max_reversion_count = "2".to_string();
            profile.position_pct = "0.05".to_string();
            profile.max_per_market_usd = "5".to_string();
            profile.max_total_exposure_usd = "5".to_string();
            profile.max_projected_stressed_drawdown_pct = "0.08".to_string();
            profile.degraded_min_z = "1.30".to_string();
            profile.degraded_max_price = "0.65".to_string();
            profile
        }
        "a_plus5m_tail_low_exposure" => {
            let mut profile = rolling_history_profile("a_plus5m_tail_guard")?;
            profile.name = name.to_string();
            profile.conf = "0.50".to_string();
            profile.z = "0.70,0.90".to_string();
            profile.max_price = "0.85,0.90".to_string();
            profile.min_reversion_count = "1".to_string();
            profile.max_reversion_count = "2".to_string();
            profile.position_pct = "0.05".to_string();
            profile.max_per_market_usd = "5".to_string();
            profile.max_total_exposure_usd = "5".to_string();
            profile.max_projected_stressed_drawdown_pct = "0.08".to_string();
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

fn zone_audit_output_for_promotion(path: &std::path::Path) -> std::path::PathBuf {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("promotion");
    path.with_file_name(format!("{stem}.zone_audit.json"))
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

fn cached_gamma_covers_window(
    cached_markets: &std::collections::BTreeMap<String, data::models::Market>,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    window_minutes: Option<f64>,
) -> bool {
    let Some(window_minutes) = window_minutes.filter(|minutes| *minutes > 0.0) else {
        return false;
    };
    let start_ts = start.timestamp() as f64;
    let end_ts = end.timestamp() as f64 + 3600.0;
    let expected = ((end_ts - start_ts) / (window_minutes * 60.0)).round() as usize;
    if expected == 0 {
        return false;
    }

    let markets = cached_markets.values().cloned().collect::<Vec<_>>();
    data::scanner::scan_candle_markets_for_backtest(&markets, 0.0)
        .into_iter()
        .filter(|contract| contract.asset == "BTC")
        .filter(|contract| {
            (live::window::estimate_window_minutes(&contract.window_description) - window_minutes)
                .abs()
                <= 1e-6
        })
        .filter(|contract| {
            let close_t = chrono::DateTime::parse_from_rfc3339(&contract.end_date)
                .map(|date| date.timestamp() as f64)
                .unwrap_or(0.0);
            let open_t = close_t - window_minutes * 60.0;
            close_t > start_ts && open_t < end_ts
        })
        .take(expected)
        .count()
        == expected
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
    artifact::write_json_atomic(path, value, pretty).map_err(std::io::Error::other)
}

/// Derive/create CLOB L2 API creds from PRIVATE_KEY in `env_file` and append
/// POLY_API_KEY/SECRET/PASSPHRASE to the same file atomically (mode and
/// ownership preserved). Secrets are never printed.
async fn cmd_derive_api_creds(env_file: &str, base_url: &str) -> anyhow::Result<()> {
    use polymomentum_engine::signing;

    let path = std::path::Path::new(env_file);
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read {env_file}"))?;
    let key_hex = text
        .lines()
        .filter_map(|l| l.trim().strip_prefix("PRIVATE_KEY="))
        .next_back()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .context("no PRIVATE_KEY=... line in the env file")?;
    let key = signing::parse_private_key(key_hex).context("PRIVATE_KEY does not parse")?;
    let address = format!("0x{}", hex::encode(signing::address_from_key(&key)));

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let signature = format!(
        "0x{}",
        signing::sign_clob_auth(&key, 137, ts, 0).map_err(anyhow::Error::msg)?
    );
    let client = reqwest::Client::new();
    let l1 = |req: reqwest::RequestBuilder| {
        req.header("POLY_ADDRESS", &address)
            .header("POLY_SIGNATURE", &signature)
            .header("POLY_TIMESTAMP", ts.to_string())
            .header("POLY_NONCE", "0")
    };
    let mut resp = l1(client.post(format!("{base_url}/auth/api-key")))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await
        .unwrap_or(serde_json::Value::Null);
    if resp.get("apiKey").and_then(|v| v.as_str()).is_none() {
        resp = l1(client.get(format!("{base_url}/auth/derive-api-key")))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;
    }
    let get = |k: &str| -> anyhow::Result<String> {
        resp.get(k)
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned)
            .with_context(|| format!("CLOB auth response missing {k}: {resp}"))
    };
    let api_key = get("apiKey")?;
    let secret = get("secret")?;
    let passphrase = get("passphrase")?;

    // Rewrite the env file: drop stale POLY_API_* lines, append fresh ones.
    let mut out: String = text
        .lines()
        .filter(|l| {
            let t = l.trim();
            !(t.starts_with("POLY_API_KEY=")
                || t.starts_with("POLY_API_SECRET=")
                || t.starts_with("POLY_API_PASSPHRASE="))
        })
        .collect::<Vec<_>>()
        .join("\n");
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(&format!(
        "POLY_API_KEY={api_key}\nPOLY_API_SECRET={secret}\nPOLY_API_PASSPHRASE={passphrase}\n"
    ));
    let meta = std::fs::metadata(path)?;
    let tmp = path.with_extension("tmp-creds");
    std::fs::write(&tmp, out)?;
    std::fs::set_permissions(&tmp, meta.permissions())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = std::os::unix::fs::chown(&tmp, Some(meta.uid()), Some(meta.gid()));
    }
    std::fs::rename(&tmp, path)?;
    println!(
        "credentials written to {env_file} for {address} (api key {}…)",
        &api_key[..8.min(api_key.len())]
    );
    Ok(())
}

/// Build the band-family promotion artifact from the fresh-gate verdict and
/// the capture fill-replay evidence. PnL fields are recomputed from the gate
/// rows at the frozen stake so the artifact carries no hand-entered numbers.
fn cmd_band_promotion_artifact(
    params_path: &str,
    gate_path: &str,
    fill_path: &str,
    output: &str,
) -> anyhow::Result<()> {
    use polymomentum_engine::backtest::experiment::{
        PromotionArtifact, PromotionGate, CURRENT_INVENTORY_MODEL_VERSION,
    };
    use polymomentum_engine::live::pipeline::{BandPolicyParams, BAND_FAMILY};
    use polymomentum_engine::strategy::spec::{stable_json_hash, StrategySpec};

    let params: BandPolicyParams = serde_json::from_slice(
        &std::fs::read(params_path).with_context(|| format!("read {params_path}"))?,
    )
    .context("parse band policy params")?;
    params.validate().map_err(anyhow::Error::msg)?;
    let gate: serde_json::Value = serde_json::from_slice(
        &std::fs::read(gate_path).with_context(|| format!("read {gate_path}"))?,
    )
    .context("parse fresh-gate artifact")?;
    anyhow::ensure!(
        gate.get("verdict").and_then(|v| v.as_str()) == Some("PASS"),
        "fresh-gate artifact verdict is not PASS"
    );
    anyhow::ensure!(
        gate.get("candidate").and_then(|v| v.as_str()) == Some(BAND_FAMILY),
        "fresh-gate artifact candidate mismatch"
    );
    let rows = gate
        .get("rows")
        .and_then(|v| v.as_array())
        .context("gate artifact rows missing")?;
    anyhow::ensure!(!rows.is_empty(), "gate artifact has no rows");
    let mut wins = 0usize;
    let mut total_pnl = 0.0f64;
    let mut total_fees = 0.0f64;
    for row in rows {
        let entry = row
            .get("signal_entry")
            .and_then(|v| v.as_f64())
            .context("gate row missing signal_entry")?;
        let won = row
            .get("won")
            .and_then(|v| v.as_bool())
            .context("gate row missing won")?;
        anyhow::ensure!(0.0 < entry && entry < 1.0, "gate row entry {entry} out of (0,1)");
        let shares = params.stake_usd / entry;
        let fee = 0.072 * entry * (1.0 - entry) * shares;
        total_fees += fee;
        if won {
            wins += 1;
            total_pnl += shares * (1.0 - entry) - fee;
        } else {
            total_pnl += -params.stake_usd - fee;
        }
    }
    let trades = rows.len();
    let win_rate = wins as f64 / trades as f64;
    let source_window = gate
        .get("fresh_range")
        .and_then(|v| v.as_array())
        .and_then(|r| {
            let a = r.first()?.as_i64()?;
            let b = r.get(1)?.as_i64()?;
            Some(format!(
                "{}..{}",
                chrono::DateTime::from_timestamp(a, 0)?.to_rfc3339(),
                chrono::DateTime::from_timestamp(b, 0)?.to_rfc3339()
            ))
        })
        .context("gate artifact fresh_range missing")?;
    let artifact = PromotionArtifact {
        schema_version: 1,
        inventory_model_version: CURRENT_INVENTORY_MODEL_VERSION,
        created_at: chrono::Utc::now().to_rfc3339(),
        source_report_hash: sha256_file(std::path::Path::new(gate_path))?,
        source_label: gate
            .get("registration")
            .and_then(|v| v.as_str())
            .unwrap_or("fresh_gate_public_v1")
            .to_string(),
        source_window,
        selected_strategy: StrategySpec::new(
            BAND_FAMILY,
            "1",
            stable_json_hash(&params),
            format!(
                "stake_usd={:.2};taker_only;hold_to_expiry",
                params.stake_usd
            ),
        ),
        strategy_params: serde_json::to_value(&params)?,
        data_manifest_hash: sha256_file(std::path::Path::new(fill_path))?,
        market_count: trades,
        trades,
        win_rate,
        total_pnl,
        avg_pnl: total_pnl / trades as f64,
        total_fees,
        sharpe_like: 0.0,
        dominant_zone: Some("band".to_string()),
        dominant_zone_trade_share: Some(1.0),
        risk_notes: vec![
            "Wallet-bounded live canary: band runtime sets the stressed-drawdown cap to 1.0 by design; the operative brakes are the session-loss floor and the consecutive-losses breaker.".to_string(),
            "Signal source is the exchange mid (Binance basis per preregistration); outcomes settle on official resolutions, so the candle settlement-alignment attestation is not consulted by the band branch.".to_string(),
            "Fill realism evidence: 93/93 band-priced captured books filled the stake instantly; the FOK worst-price cap enforces the band bound at execution.".to_string(),
        ],
        promotion_gate: PromotionGate::default(),
        robust_diagnostics: None,
    };
    polymomentum_engine::backtest::experiment::write_promotion_atomic(output, &artifact)?;
    println!(
        "{}",
        serde_json::json!({
            "output": output,
            "params_hash": artifact.selected_strategy.params_hash,
            "trades": trades,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "total_fees": total_fees,
        })
    );
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
    match data::wallet::WalletReader::for_funder(
        &settings.polygon_rpc_url,
        &settings.private_key,
        &settings.poly_funder,
    ) {
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

    let mut position = bankroll * settings.candle_position_pct.max(0.0);
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
        // A deposit wallet never pays gas itself (settlement is relayed),
        // so the POL floor applies only to an EOA maker.
        && (balances.is_deposit_wallet || balances.pol >= 0.01)
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

#[derive(Debug, Default, serde::Serialize)]
struct MarketWsRecordStats {
    connect_attempts: u64,
    connected_sessions: u64,
    subscriptions_sent: u64,
    reconnects: u64,
    websocket_connect_errors: u64,
    websocket_subscription_errors: u64,
    websocket_read_errors: u64,
    websocket_closes: u64,
    idle_timeouts: u64,
    frames: u64,
    json_messages: u64,
    book_messages: u64,
    price_change_messages: u64,
    other_messages: u64,
    bytes: u64,
}

#[derive(Debug, Default, serde::Serialize)]
struct RtdsPriceSourceStats {
    ticks: u64,
    first_observation_ms: Option<i64>,
    last_observation_ms: Option<i64>,
    last_received_ms: Option<i64>,
    max_observation_gap_ms: i64,
    max_receive_lag_ms: i64,
    negative_receive_lag_count: u64,
}

impl RtdsPriceSourceStats {
    fn observe(&mut self, observation_ms: i64, received_ms: i64) {
        if let Some(previous) = self.last_observation_ms {
            if observation_ms > previous {
                self.max_observation_gap_ms =
                    self.max_observation_gap_ms.max(observation_ms - previous);
            }
        }
        self.first_observation_ms = self.first_observation_ms.or(Some(observation_ms));
        self.last_observation_ms = Some(
            self.last_observation_ms
                .map_or(observation_ms, |previous| previous.max(observation_ms)),
        );
        self.last_received_ms = Some(received_ms);
        let receive_lag = received_ms - observation_ms;
        if receive_lag < 0 {
            self.negative_receive_lag_count += 1;
        } else {
            self.max_receive_lag_ms = self.max_receive_lag_ms.max(receive_lag);
        }
        self.ticks += 1;
    }
}

fn rtds_source_is_fresh(
    source: &RtdsPriceSourceStats,
    completed_ms: i64,
    stale_after_ms: i64,
) -> bool {
    source.ticks > 0
        && source.last_received_ms.is_some_and(|received_ms| {
            completed_ms >= received_ms && completed_ms - received_ms <= stale_after_ms
        })
}

#[derive(Debug, Default, serde::Serialize)]
struct RtdsBtcTapeStats {
    connect_attempts: u64,
    connected_sessions: u64,
    subscriptions_sent: u64,
    reconnects: u64,
    websocket_errors: u64,
    websocket_closes: u64,
    idle_timeouts: u64,
    chainlink_idle_timeouts: u64,
    binance_idle_timeouts: u64,
    pings_sent: u64,
    frames: u64,
    malformed_frames: u64,
    ignored_frames: u64,
    chainlink: RtdsPriceSourceStats,
    binance: RtdsPriceSourceStats,
}

#[derive(Debug, serde::Serialize)]
struct RtdsBtcTapeSummary {
    schema_version: u32,
    endpoint: &'static str,
    chainlink_csv: String,
    binance_csv: String,
    source_provenance_ready: bool,
    official_chainlink_ready: bool,
    binance_proxy_ready: bool,
    stats: RtdsBtcTapeStats,
}

#[derive(Debug, Default)]
struct RtdsBtcSourceTapeStats {
    connect_attempts: u64,
    connected_sessions: u64,
    subscriptions_sent: u64,
    reconnects: u64,
    websocket_errors: u64,
    websocket_closes: u64,
    idle_timeouts: u64,
    pings_sent: u64,
    frames: u64,
    malformed_frames: u64,
    ignored_frames: u64,
    prices: RtdsPriceSourceStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RtdsBtcSource {
    Chainlink,
    Binance,
}

impl RtdsBtcSource {
    fn label(self) -> &'static str {
        match self {
            Self::Chainlink => "chainlink",
            Self::Binance => "binance",
        }
    }

    fn parsed_source(self) -> &'static str {
        match self {
            Self::Chainlink => "chainlink_btc_usd_data_stream",
            Self::Binance => "binance_btcusdt_rtds",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RtdsBtcPrice {
    source: &'static str,
    timestamp_ms: i64,
    price: f64,
}

fn json_value_i64(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str()?.parse::<i64>().ok())
}

fn json_value_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
}

#[derive(Debug, serde::Serialize)]
struct LatencyMeasurementHostMetadata {
    label: Option<String>,
    hostname: Option<String>,
    uname: Option<String>,
    os: &'static str,
    arch: &'static str,
    pid: u32,
}

fn latency_measurement_host_metadata() -> LatencyMeasurementHostMetadata {
    LatencyMeasurementHostMetadata {
        label: std::env::var("POLYMOMENTUM_LATENCY_HOST_LABEL")
            .ok()
            .filter(|value| !value.trim().is_empty()),
        hostname: command_stdout_first_line("hostname", &[]),
        uname: command_stdout_first_line("uname", &["-a"]),
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        pid: std::process::id(),
    }
}

fn command_stdout_first_line(command: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(command)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let line = stdout.lines().next()?.trim();
    if line.is_empty() {
        None
    } else {
        Some(line.to_string())
    }
}

fn parse_rtds_btc_prices(text: &str) -> Option<Vec<RtdsBtcPrice>> {
    fn collect(value: &serde_json::Value, rows: &mut Vec<RtdsBtcPrice>) {
        if let Some(values) = value.as_array() {
            for value in values {
                collect(value, rows);
            }
            return;
        }
        let topic = value.get("topic").and_then(|v| v.as_str()).unwrap_or("");
        let payload = value.get("payload").unwrap_or(value);
        let symbol = payload
            .get("symbol")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let source = match (topic, symbol.as_str()) {
            ("crypto_prices_chainlink", "btc/usd") => "chainlink_btc_usd_data_stream",
            ("crypto_prices", "btcusdt") => "binance_btcusdt_rtds",
            _ => return,
        };
        let Some(timestamp_ms) = payload.get("timestamp").and_then(json_value_i64) else {
            return;
        };
        let Some(price) = payload.get("value").and_then(json_value_f64) else {
            return;
        };
        if timestamp_ms > 0 && price.is_finite() && price > 0.0 {
            rows.push(RtdsBtcPrice {
                source,
                timestamp_ms,
                price,
            });
        }
    }

    let value = serde_json::from_str::<serde_json::Value>(text).ok()?;
    let mut rows = Vec::new();
    collect(&value, &mut rows);
    Some(rows)
}

const RTDS_SOURCE_RECONNECT_AFTER_MS: u64 = 3_000;
const RTDS_WATCHDOG_INTERVAL_MS: u64 = 250;
const RTDS_INITIAL_SOURCE_GRACE_MS: u64 = 10_000;
const RTDS_RECONNECTED_SOURCE_GRACE_MS: u64 = 1_500;

fn rtds_source_should_reconnect(
    last_tick_elapsed: Option<std::time::Duration>,
    connection_elapsed: std::time::Duration,
    source_observed_before_connection: bool,
) -> bool {
    match last_tick_elapsed {
        Some(elapsed) => {
            elapsed >= std::time::Duration::from_millis(RTDS_SOURCE_RECONNECT_AFTER_MS)
                && connection_elapsed
                    >= std::time::Duration::from_millis(RTDS_RECONNECTED_SOURCE_GRACE_MS)
        }
        None => {
            let grace_ms = if source_observed_before_connection {
                RTDS_RECONNECTED_SOURCE_GRACE_MS
            } else {
                RTDS_INITIAL_SOURCE_GRACE_MS
            };
            connection_elapsed >= std::time::Duration::from_millis(grace_ms)
        }
    }
}

fn rtds_btc_subscription(source: RtdsBtcSource) -> serde_json::Value {
    let subscription = match source {
        RtdsBtcSource::Chainlink => serde_json::json!({
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": "{\"symbol\":\"btc/usd\"}"
        }),
        RtdsBtcSource::Binance => serde_json::json!({
            "topic": "crypto_prices",
            "type": "update"
        }),
    };
    serde_json::json!({
        "action": "subscribe",
        "subscriptions": [subscription]
    })
}

async fn record_rtds_btc_source_tape(
    path: std::path::PathBuf,
    tmp_path: std::path::PathBuf,
    duration_seconds: u64,
    source: RtdsBtcSource,
) -> anyhow::Result<RtdsBtcSourceTapeStats> {
    use anyhow::Context;
    use futures_util::{SinkExt, StreamExt};
    use std::io::Write;
    use tokio::time::{timeout, Duration, Instant, MissedTickBehavior};
    use tokio_tungstenite::tungstenite::Message;

    const ENDPOINT: &str = "wss://ws-live-data.polymarket.com";
    let mut writer = std::io::BufWriter::new(
        std::fs::File::create(&tmp_path)
            .with_context(|| format!("create {}", tmp_path.display()))?,
    );
    writeln!(writer, "timestamp_ms,source,price,received_at_ms")?;

    let subscription = rtds_btc_subscription(source);
    let capture_for = Duration::from_secs(duration_seconds.max(1));
    let started = Instant::now();
    let mut reconnect_backoff = Duration::from_millis(250);
    let mut stats = RtdsBtcSourceTapeStats::default();

    while started.elapsed() < capture_for {
        let remaining = capture_for.saturating_sub(started.elapsed());
        stats.connect_attempts += 1;
        let (ws, _) = match timeout(
            remaining.min(Duration::from_secs(10)),
            tokio_tungstenite::connect_async(ENDPOINT),
        )
        .await
        {
            Ok(Ok(pair)) => pair,
            Ok(Err(_)) | Err(_) => {
                stats.websocket_errors += 1;
                tokio::time::sleep(reconnect_backoff.min(remaining)).await;
                reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(5));
                continue;
            }
        };
        stats.connected_sessions += 1;
        if stats.connected_sessions > 1 {
            stats.reconnects += 1;
        }
        let (mut write, mut read) = ws.split();
        if write
            .send(Message::Text(subscription.to_string().into()))
            .await
            .is_err()
        {
            stats.websocket_errors += 1;
            continue;
        }
        stats.subscriptions_sent += 1;
        reconnect_backoff = Duration::from_millis(250);
        let mut ping = tokio::time::interval(Duration::from_secs(5));
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut watchdog = tokio::time::interval(Duration::from_millis(RTDS_WATCHDOG_INTERVAL_MS));
        watchdog.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let connected_at = Instant::now();
        let source_observed_before_connection = stats.prices.ticks > 0;
        let mut last_tick = None;

        loop {
            let remaining = capture_for.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(remaining) => break,
                _ = watchdog.tick() => {
                    let connection_elapsed = connected_at.elapsed();
                    let source_stale = rtds_source_should_reconnect(
                        last_tick.map(|tick: Instant| tick.elapsed()),
                        connection_elapsed,
                        source_observed_before_connection,
                    );
                    if source_stale {
                        stats.idle_timeouts += 1;
                        eprintln!(
                            "record-btc-books RTDS {} source stale; reconnecting",
                            source.label(),
                        );
                        break;
                    }
                }
                _ = ping.tick() => {
                    if write.send(Message::Text("PING".into())).await.is_err() {
                        stats.websocket_errors += 1;
                        break;
                    }
                    writer.flush()?;
                    stats.pings_sent += 1;
                }
                message = read.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        stats.frames += 1;
                        let received_ms = chrono::Utc::now().timestamp_millis();
                        let Some(rows) = parse_rtds_btc_prices(&text) else {
                            stats.malformed_frames += 1;
                            continue;
                        };
                        if rows.is_empty() {
                            stats.ignored_frames += 1;
                        }
                        for row in rows {
                            if row.source != source.parsed_source() {
                                stats.ignored_frames += 1;
                                continue;
                            }
                            last_tick = Some(Instant::now());
                            stats.prices.observe(row.timestamp_ms, received_ms);
                            writeln!(
                                &mut writer,
                                "{},{},{:.12},{}",
                                row.timestamp_ms,
                                row.source,
                                row.price,
                                received_ms
                            )?;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = write.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        stats.websocket_closes += 1;
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) => {
                        stats.websocket_errors += 1;
                        break;
                    }
                }
            }
        }
    }

    writer.flush()?;
    drop(writer);
    std::fs::rename(&tmp_path, &path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    Ok(stats)
}

fn merge_rtds_btc_source_stats(
    stats: &mut RtdsBtcTapeStats,
    source: RtdsBtcSource,
    source_stats: RtdsBtcSourceTapeStats,
) {
    stats.connect_attempts += source_stats.connect_attempts;
    stats.connected_sessions += source_stats.connected_sessions;
    stats.subscriptions_sent += source_stats.subscriptions_sent;
    stats.reconnects += source_stats.reconnects;
    stats.websocket_errors += source_stats.websocket_errors;
    stats.websocket_closes += source_stats.websocket_closes;
    stats.idle_timeouts += source_stats.idle_timeouts;
    stats.pings_sent += source_stats.pings_sent;
    stats.frames += source_stats.frames;
    stats.malformed_frames += source_stats.malformed_frames;
    stats.ignored_frames += source_stats.ignored_frames;
    match source {
        RtdsBtcSource::Chainlink => {
            stats.chainlink_idle_timeouts = source_stats.idle_timeouts;
            stats.chainlink = source_stats.prices;
        }
        RtdsBtcSource::Binance => {
            stats.binance_idle_timeouts = source_stats.idle_timeouts;
            stats.binance = source_stats.prices;
        }
    }
}

async fn record_rtds_btc_tapes(
    out_dir: std::path::PathBuf,
    duration_seconds: u64,
) -> anyhow::Result<RtdsBtcTapeSummary> {
    const ENDPOINT: &str = "wss://ws-live-data.polymarket.com";
    let chainlink_path = out_dir.join("chainlink_btcusd.csv");
    let binance_path = out_dir.join("binance_btcusdt_rtds.csv");
    let chainlink_tmp = out_dir.join(format!("chainlink_btcusd.csv.tmp.{}", std::process::id()));
    let binance_tmp = out_dir.join(format!(
        "binance_btcusdt_rtds.csv.tmp.{}",
        std::process::id()
    ));
    let (chainlink_stats, binance_stats) = tokio::try_join!(
        record_rtds_btc_source_tape(
            chainlink_path.clone(),
            chainlink_tmp,
            duration_seconds,
            RtdsBtcSource::Chainlink,
        ),
        record_rtds_btc_source_tape(
            binance_path.clone(),
            binance_tmp,
            duration_seconds,
            RtdsBtcSource::Binance,
        ),
    )?;
    let mut stats = RtdsBtcTapeStats::default();
    merge_rtds_btc_source_stats(&mut stats, RtdsBtcSource::Chainlink, chainlink_stats);
    merge_rtds_btc_source_stats(&mut stats, RtdsBtcSource::Binance, binance_stats);

    let completed_ms = chrono::Utc::now().timestamp_millis();
    let official_chainlink_ready = rtds_source_is_fresh(&stats.chainlink, completed_ms, 20_000);
    let binance_proxy_ready = rtds_source_is_fresh(&stats.binance, completed_ms, 20_000);
    Ok(RtdsBtcTapeSummary {
        schema_version: 2,
        endpoint: ENDPOINT,
        chainlink_csv: chainlink_path.display().to_string(),
        binance_csv: binance_path.display().to_string(),
        source_provenance_ready: official_chainlink_ready && binance_proxy_ready,
        official_chainlink_ready,
        binance_proxy_ready,
        stats,
    })
}

async fn cmd_record_btc_books(
    settings: &config::Settings,
    start: Option<&str>,
    window_minutes: f64,
    windows: usize,
    duration_seconds: u64,
    out_dir: &str,
) -> anyhow::Result<()> {
    use anyhow::{bail, Context};
    use futures_util::{SinkExt, StreamExt};
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::Write;
    use tokio::time::{timeout, Duration, Instant};
    use tokio_tungstenite::tungstenite::Message;

    if windows == 0 {
        bail!("--windows must be greater than zero");
    }
    let step_s = live::window::btc_updown_slug_step_seconds(window_minutes)
        .context("--window-minutes must be 5 or 15 for BTC slug recording")?;
    let anchor = if let Some(raw) = start {
        chrono::DateTime::parse_from_rfc3339(raw)
            .with_context(|| format!("parse --start {raw}"))?
            .with_timezone(&chrono::Utc)
    } else {
        chrono::Utc::now()
    };
    let base_s = anchor.timestamp() - anchor.timestamp().rem_euclid(step_s);
    let slugs: Vec<String> = (0..windows)
        .map(|i| {
            let t = base_s + (i as i64 * step_s);
            if step_s == 300 {
                format!("btc-updown-5m-{t}")
            } else {
                format!("btc-updown-15m-{t}")
            }
        })
        .collect();

    let gamma = data::gamma::GammaClient::new(&settings.poly_gamma_url);
    let markets = gamma
        .fetch_markets_by_slugs(&slugs, false)
        .await
        .context("fetch active BTC candle Gamma metadata")?;
    if markets.is_empty() {
        bail!(
            "Gamma returned no active markets for slugs {}",
            slugs.join(",")
        );
    }

    let mut gamma_by_condition = BTreeMap::new();
    let mut token_ids = BTreeSet::new();
    for market in markets {
        for outcome in &market.outcomes {
            if !outcome.token_id.is_empty() {
                token_ids.insert(outcome.token_id.clone());
            }
        }
        gamma_by_condition.insert(market.condition_id.clone(), market);
    }
    if token_ids.is_empty() {
        bail!(
            "Gamma metadata had no CLOB token IDs for slugs {}",
            slugs.join(",")
        );
    }

    let out_dir = std::path::PathBuf::from(out_dir);
    std::fs::create_dir_all(&out_dir).with_context(|| format!("create {}", out_dir.display()))?;
    let gamma_path = out_dir.join("gamma_market_cache.json");
    write_json_atomic(&gamma_path, &gamma_by_condition, true)
        .with_context(|| format!("write {}", gamma_path.display()))?;

    let frames_path = out_dir.join("market_ws_frames.jsonl");
    let summary_path = out_dir.join("summary.json");
    let mut writer = std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&frames_path)
            .with_context(|| format!("open {}", frames_path.display()))?,
    );

    let endpoint = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    let token_vec: Vec<String> = token_ids.iter().cloned().collect();
    let sub = serde_json::json!({
        "type": "market",
        "assets_ids": token_vec,
        "custom_feature_enabled": true,
    });
    let capture_for = Duration::from_secs(duration_seconds.max(1));
    let reference_tape_task = tokio::spawn(record_rtds_btc_tapes(
        out_dir.clone(),
        duration_seconds.max(1),
    ));
    let started = Instant::now();
    let mut stats = MarketWsRecordStats::default();
    let mut seen_tokens = BTreeSet::new();
    let mut reconnect_backoff = Duration::from_millis(250);

    while started.elapsed() < capture_for {
        let remaining = capture_for
            .checked_sub(started.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));
        if remaining.is_zero() {
            break;
        }

        stats.connect_attempts += 1;
        let connect_wait = remaining.min(Duration::from_secs(10));
        let (ws, _) = match timeout(connect_wait, tokio_tungstenite::connect_async(endpoint)).await
        {
            Ok(Ok(pair)) => pair,
            Ok(Err(e)) => {
                stats.websocket_connect_errors += 1;
                eprintln!("record-btc-books websocket connect failed: {e}");
                let sleep_for = reconnect_backoff.min(remaining);
                tokio::time::sleep(sleep_for).await;
                reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(5));
                continue;
            }
            Err(_) => {
                stats.websocket_connect_errors += 1;
                eprintln!("record-btc-books websocket connect timed out");
                let sleep_for = reconnect_backoff.min(remaining);
                tokio::time::sleep(sleep_for).await;
                reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(5));
                continue;
            }
        };
        stats.connected_sessions += 1;
        if stats.connected_sessions > 1 {
            stats.reconnects += 1;
        }
        let (mut write, mut read) = ws.split();
        if let Err(e) = write.send(Message::Text(sub.to_string().into())).await {
            stats.websocket_subscription_errors += 1;
            eprintln!("record-btc-books websocket subscription failed: {e}");
            let sleep_for = reconnect_backoff.min(remaining);
            tokio::time::sleep(sleep_for).await;
            reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(5));
            continue;
        }
        stats.subscriptions_sent += 1;

        loop {
            let remaining = capture_for
                .checked_sub(started.elapsed())
                .unwrap_or_else(|| Duration::from_secs(0));
            if remaining.is_zero() {
                break;
            }
            let wait_for = remaining.min(Duration::from_secs(10));
            match timeout(wait_for, read.next()).await {
                Ok(Some(Ok(Message::Text(text)))) => {
                    reconnect_backoff = Duration::from_millis(250);
                    let ts_received_ms = chrono::Utc::now().timestamp_millis();
                    let text = text.to_string();
                    stats.frames += 1;
                    stats.bytes += text.len() as u64;
                    record_market_ws_text(&text, &mut stats, &mut seen_tokens);
                    let ts_recorded_ms = chrono::Utc::now().timestamp_millis();
                    let row = serde_json::json!({
                        "ts_received_ms": ts_received_ms,
                        "ts_recorded_ms": ts_recorded_ms,
                        "raw": text,
                    });
                    serde_json::to_writer(&mut writer, &row).context("serialize ws frame")?;
                    writer.write_all(b"\n").context("write ws frame newline")?;
                }
                Ok(Some(Ok(Message::Binary(bytes)))) => {
                    reconnect_backoff = Duration::from_millis(250);
                    let ts_received_ms = chrono::Utc::now().timestamp_millis();
                    stats.frames += 1;
                    stats.bytes += bytes.len() as u64;
                    let ts_recorded_ms = chrono::Utc::now().timestamp_millis();
                    let row = serde_json::json!({
                        "ts_received_ms": ts_received_ms,
                        "ts_recorded_ms": ts_recorded_ms,
                        "raw_binary_len": bytes.len(),
                    });
                    serde_json::to_writer(&mut writer, &row)
                        .context("serialize binary ws frame")?;
                    writer
                        .write_all(b"\n")
                        .context("write binary ws frame newline")?;
                }
                Ok(Some(Ok(Message::Ping(payload)))) => {
                    let _ = write.send(Message::Pong(payload)).await;
                }
                Ok(Some(Ok(Message::Close(_)))) | Ok(None) => {
                    stats.websocket_closes += 1;
                    break;
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => {
                    stats.websocket_read_errors += 1;
                    eprintln!("record-btc-books websocket read failed; reconnecting: {e}");
                    break;
                }
                Err(_) => {
                    if started.elapsed() < capture_for {
                        stats.idle_timeouts += 1;
                    }
                }
            }
        }

        let remaining = capture_for
            .checked_sub(started.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));
        if !remaining.is_zero() {
            let sleep_for = reconnect_backoff.min(remaining);
            tokio::time::sleep(sleep_for).await;
            reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(5));
        }
    }
    writer.flush().context("flush market ws frames")?;

    let reference_tapes = match reference_tape_task.await {
        Ok(Ok(summary)) => serde_json::to_value(summary).context("serialize RTDS summary")?,
        Ok(Err(error)) => serde_json::json!({
            "schema_version": 1,
            "source_provenance_ready": false,
            "error": format!("{error:#}")
        }),
        Err(error) => serde_json::json!({
            "schema_version": 1,
            "source_provenance_ready": false,
            "error": format!("RTDS recorder task failed: {error}")
        }),
    };

    if stats.frames == 0 {
        bail!("websocket capture received zero frames");
    }

    let summary = serde_json::json!({
        "schema_version": 2,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "slugs": slugs,
        "condition_ids": gamma_by_condition.keys().cloned().collect::<Vec<_>>(),
        "token_ids": token_ids.iter().cloned().collect::<Vec<_>>(),
        "seen_token_ids": seen_tokens.iter().cloned().collect::<Vec<_>>(),
        "duration_seconds": duration_seconds.max(1),
        "gamma_market_cache": gamma_path.display().to_string(),
        "frames_jsonl": frames_path.display().to_string(),
        "websocket_endpoint": endpoint,
        "reference_tapes": reference_tapes,
        "measurement_host": latency_measurement_host_metadata(),
        "stats": stats,
    });
    write_json_atomic(&summary_path, &summary, true)
        .with_context(|| format!("write {}", summary_path.display()))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).expect("serialize record summary")
    );
    Ok(())
}

fn record_market_ws_text(
    text: &str,
    stats: &mut MarketWsRecordStats,
    seen_tokens: &mut std::collections::BTreeSet<String>,
) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        stats.other_messages += 1;
        return;
    };
    record_market_ws_value(&value, stats, seen_tokens);
}

fn record_market_ws_value(
    value: &serde_json::Value,
    stats: &mut MarketWsRecordStats,
    seen_tokens: &mut std::collections::BTreeSet<String>,
) {
    if let Some(arr) = value.as_array() {
        for item in arr {
            record_market_ws_value(item, stats, seen_tokens);
        }
        return;
    }
    stats.json_messages += 1;
    let msg_type = value
        .get("event_type")
        .or_else(|| value.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let data = value.get("data").unwrap_or(value);
    match msg_type {
        "book" => {
            stats.book_messages += 1;
            if let Some(token) = data.get("asset_id").and_then(|v| v.as_str()) {
                seen_tokens.insert(token.to_string());
            }
        }
        "price_change" => {
            stats.price_change_messages += 1;
            if let Some(token) = data.get("asset_id").and_then(|v| v.as_str()) {
                seen_tokens.insert(token.to_string());
            }
            if let Some(changes) = data
                .get("price_changes")
                .or_else(|| data.get("changes"))
                .and_then(|v| v.as_array())
            {
                for ch in changes {
                    if let Some(token) = ch.get("asset_id").and_then(|v| v.as_str()) {
                        seen_tokens.insert(token.to_string());
                    }
                }
            }
        }
        _ => stats.other_messages += 1,
    }
}

#[derive(Debug, Clone, Copy)]
struct ForwardLatencyAuditThresholds {
    max_p99_delay_ms: f64,
    max_token_gap_ms: f64,
    min_gap_gate_events: u64,
    max_missing_timestamp_rate: f64,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
struct ForwardLatencyAuditStats {
    raw_frames: u64,
    binary_frames: u64,
    json_messages: u64,
    book_events: u64,
    change_events: u64,
    other_messages: u64,
    malformed_lines: u64,
    malformed_raw: u64,
    missing_received_timestamp: u64,
    missing_event_timestamp: u64,
    timestamped_events: u64,
    delay_samples: u64,
    negative_delay_samples: u64,
    record_overhead_samples: u64,
    negative_record_overhead_samples: u64,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
struct ForwardLatencyTokenStats {
    events: u64,
    book_events: u64,
    change_events: u64,
    first_received_ms: Option<i64>,
    last_received_ms: Option<i64>,
    max_gap_ms: i64,
}

impl ForwardLatencyTokenStats {
    fn observe(&mut self, msg_type: &str, row_ts_ms: Option<i64>) {
        self.events += 1;
        match msg_type {
            "book" => self.book_events += 1,
            "price_change" => self.change_events += 1,
            _ => {}
        }
        let Some(ts) = row_ts_ms else {
            return;
        };
        if self.first_received_ms.is_none() {
            self.first_received_ms = Some(ts);
        }
        if let Some(prev) = self.last_received_ms {
            self.max_gap_ms = self.max_gap_ms.max(ts.saturating_sub(prev));
        }
        self.last_received_ms = Some(ts);
    }
}

#[derive(Debug, Default)]
struct ForwardLatencyAuditAccumulator {
    stats: ForwardLatencyAuditStats,
    delay_ms: Vec<f64>,
    delay_sum_ms: f64,
    record_overhead_ms: Vec<f64>,
    record_overhead_sum_ms: f64,
    token_stats: std::collections::BTreeMap<String, ForwardLatencyTokenStats>,
    first_frame_received_ms: Option<i64>,
    last_frame_received_ms: Option<i64>,
    max_stream_receive_gap_ms: i64,
    active_stream_window_start_ms: Option<i64>,
    active_stream_window_end_ms: Option<i64>,
    first_active_frame_received_ms: Option<i64>,
    last_active_frame_received_ms: Option<i64>,
    max_active_stream_receive_gap_ms: i64,
    max_active_stream_receive_gap_start_ms: Option<i64>,
    max_active_stream_receive_gap_end_ms: Option<i64>,
    active_stream_gap_record_threshold_ms: i64,
    active_stream_receive_gaps_over_limit: Vec<ForwardLatencyReceiveGap>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ForwardLatencyReceiveGap {
    start_ms: i64,
    end_ms: i64,
    gap_ms: i64,
}

fn cmd_forward_latency_audit(
    input_dir: &str,
    output: Option<&str>,
    max_p99_delay_ms: f64,
    max_token_gap_ms: f64,
    min_gap_gate_events: u64,
    max_missing_timestamp_rate: f64,
) -> anyhow::Result<()> {
    use anyhow::Context;
    use serde_json::Value;
    use std::io::BufRead;

    let input_dir = std::path::PathBuf::from(input_dir);
    let frames_path = input_dir.join("market_ws_frames.jsonl");
    let output_path = output
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| input_dir.join("forward_latency_audit.json"));
    let capture_summary = forward_latency_capture_summary(&input_dir);
    let expected_token_ids = forward_latency_expected_tokens(&input_dir)?;
    let token_outcomes = forward_latency_token_outcomes(&input_dir)?;
    let active_stream_range_ms = forward_latency_active_stream_range_ms(&token_outcomes);
    let thresholds = ForwardLatencyAuditThresholds {
        max_p99_delay_ms,
        max_token_gap_ms,
        min_gap_gate_events,
        max_missing_timestamp_rate,
    };

    let frames_file = std::fs::File::open(&frames_path)
        .with_context(|| format!("open {}", frames_path.display()))?;
    let reader = std::io::BufReader::new(frames_file);
    let mut acc = ForwardLatencyAuditAccumulator {
        active_stream_gap_record_threshold_ms: max_token_gap_ms.ceil() as i64,
        ..Default::default()
    };

    for line_res in reader.lines() {
        let line = match line_res {
            Ok(line) => line,
            Err(_) => {
                acc.stats.malformed_lines += 1;
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let row: Value = match serde_json::from_str(&line) {
            Ok(row) => row,
            Err(_) => {
                acc.stats.malformed_lines += 1;
                continue;
            }
        };
        if row.get("raw_binary_len").is_some() {
            acc.stats.binary_frames += 1;
            continue;
        }
        let Some(raw) = row.get("raw").and_then(|v| v.as_str()) else {
            acc.stats.malformed_lines += 1;
            continue;
        };
        acc.stats.raw_frames += 1;
        let row_ts_ms = row.get("ts_received_ms").and_then(recorded_json_i64);
        let row_recorded_ms = row.get("ts_recorded_ms").and_then(recorded_json_i64);
        forward_latency_observe_frame_received(&mut acc, row_ts_ms, active_stream_range_ms);
        forward_latency_observe_record_overhead(&mut acc, row_ts_ms, row_recorded_ms);
        let raw_value: Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(_) => {
                acc.stats.malformed_raw += 1;
                continue;
            }
        };
        forward_latency_audit_ws_value(&raw_value, row_ts_ms, &mut acc);
    }

    let mut report = forward_latency_audit_report(
        &input_dir,
        &frames_path,
        &output_path,
        capture_summary,
        acc,
        &expected_token_ids,
        &token_outcomes,
        thresholds,
    );
    report["window_admissibility"] =
        forward_latency_window_admissibility(&input_dir, &report["window_continuity"])?;
    write_json_atomic(&output_path, &report, true)
        .with_context(|| format!("write {}", output_path.display()))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize latency audit")
    );
    Ok(())
}

fn forward_latency_expected_tokens(
    input_dir: &std::path::Path,
) -> anyhow::Result<std::collections::BTreeSet<String>> {
    use anyhow::Context;

    let summary_path = input_dir.join("summary.json");
    if !summary_path.exists() {
        return Ok(std::collections::BTreeSet::new());
    }
    let file = std::fs::File::open(&summary_path)
        .with_context(|| format!("open {}", summary_path.display()))?;
    let value: serde_json::Value = serde_json::from_reader(file)
        .with_context(|| format!("decode {}", summary_path.display()))?;
    let mut tokens = std::collections::BTreeSet::new();
    if let Some(arr) = value.get("token_ids").and_then(|v| v.as_array()) {
        for token in arr.iter().filter_map(|v| v.as_str()) {
            tokens.insert(token.to_string());
        }
    }
    Ok(tokens)
}

fn forward_latency_token_outcomes(
    input_dir: &std::path::Path,
) -> anyhow::Result<std::collections::BTreeMap<String, serde_json::Value>> {
    use anyhow::Context;

    let gamma_path = input_dir.join("gamma_market_cache.json");
    if !gamma_path.exists() {
        return Ok(std::collections::BTreeMap::new());
    }
    let file = std::fs::File::open(&gamma_path)
        .with_context(|| format!("open {}", gamma_path.display()))?;
    let gamma_by_condition: std::collections::BTreeMap<String, data::models::Market> =
        serde_json::from_reader(file)
            .with_context(|| format!("decode {}", gamma_path.display()))?;
    let mut out = std::collections::BTreeMap::new();
    for (condition_id, market) in gamma_by_condition {
        for outcome in market.outcomes {
            if outcome.token_id.is_empty() {
                continue;
            }
            out.insert(
                outcome.token_id,
                serde_json::json!({
                    "condition_id": condition_id,
                    "slug": market.slug,
                    "end_date": market.end_date,
                    "outcome": outcome.name,
                }),
            );
        }
    }
    Ok(out)
}

fn forward_latency_capture_summary(input_dir: &std::path::Path) -> Option<serde_json::Value> {
    let summary_path = input_dir.join("summary.json");
    let file = std::fs::File::open(summary_path).ok()?;
    serde_json::from_reader(file).ok()
}

fn forward_latency_active_stream_range_ms(
    token_outcomes: &std::collections::BTreeMap<String, serde_json::Value>,
) -> Option<(i64, i64)> {
    let mut start_ms = i64::MAX;
    let mut end_ms = i64::MIN;
    let mut found = false;
    for outcome in token_outcomes.values() {
        let Some(slug) = outcome.get("slug").and_then(|value| value.as_str()) else {
            continue;
        };
        let Some((open_s, close_s, _)) = recorded_btc_slug_window(slug) else {
            continue;
        };
        start_ms = start_ms.min(open_s.saturating_mul(1000));
        end_ms = end_ms.max(close_s.saturating_mul(1000));
        found = true;
    }
    found.then_some((start_ms, end_ms))
}

fn forward_latency_observe_frame_received(
    acc: &mut ForwardLatencyAuditAccumulator,
    row_ts_ms: Option<i64>,
    active_stream_range_ms: Option<(i64, i64)>,
) {
    let Some(ts) = row_ts_ms else {
        return;
    };
    if acc.first_frame_received_ms.is_none() {
        acc.first_frame_received_ms = Some(ts);
    }
    if let Some(prev) = acc.last_frame_received_ms {
        acc.max_stream_receive_gap_ms = acc.max_stream_receive_gap_ms.max(ts.saturating_sub(prev));
    }
    acc.last_frame_received_ms = Some(ts);

    let Some((active_start_ms, active_end_ms)) = active_stream_range_ms else {
        return;
    };
    acc.active_stream_window_start_ms = Some(active_start_ms);
    acc.active_stream_window_end_ms = Some(active_end_ms);
    if ts < active_start_ms || ts > active_end_ms {
        return;
    }
    if acc.first_active_frame_received_ms.is_none() {
        acc.first_active_frame_received_ms = Some(ts);
    }
    if let Some(previous_ms) = acc.last_active_frame_received_ms {
        let gap_ms = ts.saturating_sub(previous_ms);
        if gap_ms > acc.active_stream_gap_record_threshold_ms {
            acc.active_stream_receive_gaps_over_limit
                .push(ForwardLatencyReceiveGap {
                    start_ms: previous_ms,
                    end_ms: ts,
                    gap_ms,
                });
        }
        if gap_ms > acc.max_active_stream_receive_gap_ms {
            acc.max_active_stream_receive_gap_ms = gap_ms;
            acc.max_active_stream_receive_gap_start_ms = Some(previous_ms);
            acc.max_active_stream_receive_gap_end_ms = Some(ts);
        }
    }
    acc.last_active_frame_received_ms = Some(ts);
}

fn forward_latency_observe_record_overhead(
    acc: &mut ForwardLatencyAuditAccumulator,
    received_ms: Option<i64>,
    recorded_ms: Option<i64>,
) {
    let (Some(received), Some(recorded)) = (received_ms, recorded_ms) else {
        return;
    };
    let overhead_ms = recorded as f64 - received as f64;
    acc.stats.record_overhead_samples += 1;
    if overhead_ms < 0.0 {
        acc.stats.negative_record_overhead_samples += 1;
    }
    acc.record_overhead_sum_ms += overhead_ms;
    acc.record_overhead_ms.push(overhead_ms);
}

fn forward_latency_audit_ws_value(
    value: &serde_json::Value,
    row_ts_ms: Option<i64>,
    acc: &mut ForwardLatencyAuditAccumulator,
) {
    if let Some(arr) = value.as_array() {
        for item in arr {
            forward_latency_audit_ws_value(item, row_ts_ms, acc);
        }
        return;
    }

    acc.stats.json_messages += 1;
    let msg_type = value
        .get("event_type")
        .or_else(|| value.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let data = value.get("data").unwrap_or(value);
    match msg_type {
        "book" => {
            acc.stats.book_events += 1;
            forward_latency_observe_delay(acc, row_ts_ms, forward_latency_event_timestamp_ms(data));
            if let Some(token) = data.get("asset_id").and_then(|v| v.as_str()) {
                forward_latency_observe_token(acc, token, msg_type, row_ts_ms);
            }
        }
        "price_change" => {
            acc.stats.change_events += 1;
            forward_latency_observe_delay(acc, row_ts_ms, forward_latency_event_timestamp_ms(data));
            let mut observed_token = false;
            if let Some(changes) = data
                .get("price_changes")
                .or_else(|| data.get("changes"))
                .and_then(|v| v.as_array())
            {
                for change in changes {
                    if let Some(token) = change
                        .get("asset_id")
                        .and_then(|v| v.as_str())
                        .or_else(|| data.get("asset_id").and_then(|v| v.as_str()))
                    {
                        forward_latency_observe_token(acc, token, msg_type, row_ts_ms);
                        observed_token = true;
                    }
                }
            }
            if !observed_token {
                if let Some(token) = data.get("asset_id").and_then(|v| v.as_str()) {
                    forward_latency_observe_token(acc, token, msg_type, row_ts_ms);
                }
            }
        }
        _ => acc.stats.other_messages += 1,
    }
}

fn forward_latency_observe_delay(
    acc: &mut ForwardLatencyAuditAccumulator,
    row_ts_ms: Option<i64>,
    event_ts_ms: Option<i64>,
) {
    match (row_ts_ms, event_ts_ms) {
        (Some(received_ms), Some(event_ms)) => {
            let delay_ms = received_ms as f64 - event_ms as f64;
            if received_ms < event_ms {
                acc.stats.negative_delay_samples += 1;
            }
            acc.stats.timestamped_events += 1;
            acc.stats.delay_samples += 1;
            acc.delay_sum_ms += delay_ms;
            acc.delay_ms.push(delay_ms);
        }
        (None, Some(_)) => acc.stats.missing_received_timestamp += 1,
        (_, None) => acc.stats.missing_event_timestamp += 1,
    }
}

fn forward_latency_observe_token(
    acc: &mut ForwardLatencyAuditAccumulator,
    token: &str,
    msg_type: &str,
    row_ts_ms: Option<i64>,
) {
    acc.token_stats
        .entry(token.to_string())
        .or_default()
        .observe(msg_type, row_ts_ms);
}

fn forward_latency_event_timestamp_ms(data: &serde_json::Value) -> Option<i64> {
    data.get("timestamp").and_then(recorded_json_f64).map(|ts| {
        if ts > 10_000_000_000.0 {
            ts.round() as i64
        } else {
            (ts * 1000.0).round() as i64
        }
    })
}

fn forward_latency_active_expected_tokens(
    expected_token_ids: &std::collections::BTreeSet<String>,
    token_outcomes: &std::collections::BTreeMap<String, serde_json::Value>,
    first_frame_received_ms: Option<i64>,
    last_frame_received_ms: Option<i64>,
) -> std::collections::BTreeSet<String> {
    let (Some(start_ms), Some(end_ms)) = (first_frame_received_ms, last_frame_received_ms) else {
        return expected_token_ids.clone();
    };
    let capture_start_ms = start_ms.min(end_ms);
    let capture_end_ms = start_ms.max(end_ms);
    expected_token_ids
        .iter()
        .filter(|token| {
            forward_latency_token_overlaps_capture(
                token,
                token_outcomes,
                capture_start_ms,
                capture_end_ms,
            )
        })
        .cloned()
        .collect()
}

fn forward_latency_token_overlaps_capture(
    token: &str,
    token_outcomes: &std::collections::BTreeMap<String, serde_json::Value>,
    capture_start_ms: i64,
    capture_end_ms: i64,
) -> bool {
    let Some(outcome) = token_outcomes.get(token) else {
        return true;
    };
    let Some(slug) = outcome.get("slug").and_then(|v| v.as_str()) else {
        return true;
    };
    let Some((open_s, close_s, _)) = recorded_btc_slug_window(slug) else {
        return true;
    };
    let open_ms = open_s.saturating_mul(1000);
    let close_ms = close_s.saturating_mul(1000);
    close_ms > capture_start_ms && open_ms < capture_end_ms
}

#[allow(clippy::too_many_arguments)]
fn forward_latency_audit_report(
    input_dir: &std::path::Path,
    frames_path: &std::path::Path,
    output_path: &std::path::Path,
    capture_summary: Option<serde_json::Value>,
    acc: ForwardLatencyAuditAccumulator,
    expected_token_ids: &std::collections::BTreeSet<String>,
    token_outcomes: &std::collections::BTreeMap<String, serde_json::Value>,
    thresholds: ForwardLatencyAuditThresholds,
) -> serde_json::Value {
    let mut delays = acc.delay_ms.clone();
    delays.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50_delay_ms = forward_latency_percentile(&delays, 0.50);
    let p90_delay_ms = forward_latency_percentile(&delays, 0.90);
    let p95_delay_ms = forward_latency_percentile(&delays, 0.95);
    let p99_delay_ms = forward_latency_percentile(&delays, 0.99);
    let p99_5_delay_ms = forward_latency_percentile(&delays, 0.995);
    let delay_counts_above_ms = forward_latency_counts_above_ms(&delays);
    let min_delay_ms = delays.first().copied();
    let max_delay_ms = delays.last().copied();
    let avg_delay_ms = if acc.stats.delay_samples > 0 {
        Some(acc.delay_sum_ms / acc.stats.delay_samples as f64)
    } else {
        None
    };
    let mut record_overheads = acc.record_overhead_ms.clone();
    record_overheads.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let record_overhead_ms = forward_latency_distribution_report(
        &record_overheads,
        acc.record_overhead_sum_ms,
        acc.stats.record_overhead_samples,
    );
    let clob_events = acc.stats.book_events + acc.stats.change_events;
    let missing_timestamp_rate = if clob_events > 0 {
        acc.stats.missing_event_timestamp as f64 / clob_events as f64
    } else {
        1.0
    };
    let observed_token_ids = acc.token_stats.keys().cloned().collect::<Vec<_>>();
    let missing_token_ids = expected_token_ids
        .iter()
        .filter(|token| !acc.token_stats.contains_key(*token))
        .cloned()
        .collect::<Vec<_>>();
    let active_expected_token_ids = forward_latency_active_expected_tokens(
        expected_token_ids,
        token_outcomes,
        acc.first_frame_received_ms,
        acc.last_frame_received_ms,
    );
    let missing_active_token_ids = active_expected_token_ids
        .iter()
        .filter(|token| !acc.token_stats.contains_key(*token))
        .cloned()
        .collect::<Vec<_>>();
    let max_observed_token_gap_ms = acc
        .token_stats
        .values()
        .map(|stats| stats.max_gap_ms)
        .max()
        .unwrap_or(0) as f64;
    let gap_gate_token_ids = acc
        .token_stats
        .iter()
        .filter(|(token, stats)| {
            active_expected_token_ids.contains(*token)
                && stats.events >= thresholds.min_gap_gate_events
        })
        .map(|(token, _)| token.clone())
        .collect::<Vec<_>>();
    let gap_skipped_token_ids = expected_token_ids
        .iter()
        .filter(|token| !gap_gate_token_ids.contains(token))
        .cloned()
        .collect::<Vec<_>>();
    let max_gate_token_gap_ms = acc
        .token_stats
        .iter()
        .filter(|(token, stats)| {
            active_expected_token_ids.contains(*token)
                && stats.events >= thresholds.min_gap_gate_events
        })
        .map(|(_, stats)| stats.max_gap_ms)
        .max()
        .unwrap_or(0) as f64;
    let active_stream_gap = acc
        .active_stream_window_start_ms
        .zip(acc.active_stream_window_end_ms)
        .zip(acc.first_active_frame_received_ms)
        .zip(acc.last_active_frame_received_ms)
        .map(|(((window_start_ms, window_end_ms), first_ms), last_ms)| {
            let mut max_gap_ms = acc.max_active_stream_receive_gap_ms;
            let mut gap_start_ms = acc.max_active_stream_receive_gap_start_ms;
            let mut gap_end_ms = acc.max_active_stream_receive_gap_end_ms;
            let start_boundary_gap_ms = first_ms.saturating_sub(window_start_ms);
            if start_boundary_gap_ms > max_gap_ms {
                max_gap_ms = start_boundary_gap_ms;
                gap_start_ms = Some(window_start_ms);
                gap_end_ms = Some(first_ms);
            }
            let end_boundary_gap_ms = window_end_ms.saturating_sub(last_ms);
            if end_boundary_gap_ms > max_gap_ms {
                max_gap_ms = end_boundary_gap_ms;
                gap_start_ms = Some(last_ms);
                gap_end_ms = Some(window_end_ms);
            }
            (max_gap_ms, gap_start_ms, gap_end_ms)
        });
    let mut active_stream_receive_gaps_over_limit =
        acc.active_stream_receive_gaps_over_limit.clone();
    if let Some(((window_start_ms, window_end_ms), (first_ms, last_ms))) = acc
        .active_stream_window_start_ms
        .zip(acc.active_stream_window_end_ms)
        .zip(
            acc.first_active_frame_received_ms
                .zip(acc.last_active_frame_received_ms),
        )
    {
        let start_gap_ms = first_ms.saturating_sub(window_start_ms);
        if start_gap_ms > acc.active_stream_gap_record_threshold_ms {
            active_stream_receive_gaps_over_limit.push(ForwardLatencyReceiveGap {
                start_ms: window_start_ms,
                end_ms: first_ms,
                gap_ms: start_gap_ms,
            });
        }
        let end_gap_ms = window_end_ms.saturating_sub(last_ms);
        if end_gap_ms > acc.active_stream_gap_record_threshold_ms {
            active_stream_receive_gaps_over_limit.push(ForwardLatencyReceiveGap {
                start_ms: last_ms,
                end_ms: window_end_ms,
                gap_ms: end_gap_ms,
            });
        }
    }
    active_stream_receive_gaps_over_limit.sort_by_key(|gap| (gap.start_ms, gap.end_ms));
    active_stream_receive_gaps_over_limit.dedup_by_key(|gap| (gap.start_ms, gap.end_ms));
    let window_continuity = forward_latency_window_continuity(
        token_outcomes,
        &acc.token_stats,
        &active_stream_receive_gaps_over_limit,
        thresholds.max_token_gap_ms,
    );
    let per_token_gap_threshold_exceeded_token_ids = acc
        .token_stats
        .iter()
        .filter(|(token, stats)| {
            gap_gate_token_ids.contains(token)
                && stats.max_gap_ms as f64 > thresholds.max_token_gap_ms
        })
        .map(|(token, _)| token.clone())
        .collect::<Vec<_>>();
    let clock_skew_negative_delays = acc.stats.negative_delay_samples > 0;
    let stream_latency_ready = acc.stats.delay_samples > 0
        && !clock_skew_negative_delays
        && p99_delay_ms
            .map(|p99| p99 <= thresholds.max_p99_delay_ms)
            .unwrap_or(false);
    let timestamp_ready =
        clob_events > 0 && missing_timestamp_rate <= thresholds.max_missing_timestamp_rate;
    let coverage_ready = if active_expected_token_ids.is_empty() {
        expected_token_ids.is_empty() || missing_token_ids.is_empty()
    } else {
        missing_active_token_ids.is_empty()
    };
    let evaluated_stream_gap_ms = active_stream_gap
        .map(|(max_gap_ms, _, _)| max_gap_ms)
        .unwrap_or(acc.max_stream_receive_gap_ms);
    let stream_gap_ready = evaluated_stream_gap_ms as f64 <= thresholds.max_token_gap_ms;
    let gap_ready =
        !gap_gate_token_ids.is_empty() && missing_active_token_ids.is_empty() && stream_gap_ready;
    let assumed_backtest_latency_ms = 50.0;
    let backtest_latency_assumption_ready = !clock_skew_negative_delays
        && p99_delay_ms
            .map(|p99| p99 <= assumed_backtest_latency_ms)
            .unwrap_or(false);
    let recommended_retest_latency_ms = if clock_skew_negative_delays {
        None
    } else {
        p99_delay_ms.map(|p99| p99.ceil().max(50.0) as u64)
    };
    let ready = stream_latency_ready
        && timestamp_ready
        && coverage_ready
        && gap_ready
        && backtest_latency_assumption_ready;
    let verdict = if clob_events == 0 {
        "NO_CLOB_EVENTS"
    } else if !coverage_ready {
        "TOKEN_COVERAGE_MISSING"
    } else if acc.stats.delay_samples == 0 {
        "NO_TIMESTAMPED_CLOB_EVENTS"
    } else if clock_skew_negative_delays {
        "CLOCK_SKEW_NEGATIVE_DELAYS"
    } else if !timestamp_ready {
        "MISSING_CLOB_TIMESTAMPS"
    } else if !stream_latency_ready {
        "CLOB_P99_DELAY_TOO_HIGH"
    } else if !stream_gap_ready {
        "STREAM_RECEIVE_GAP_TOO_HIGH"
    } else if gap_gate_token_ids.is_empty() {
        "ACTIVE_TOKEN_GAP_SAMPLES_TOO_SPARSE"
    } else if !backtest_latency_assumption_ready {
        "MEASURED_LATENCY_RETEST_REQUIRED"
    } else {
        "LATENCY_READY"
    };

    serde_json::json!({
        "schema_version": 1,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "source": {
            "input_dir": input_dir.display().to_string(),
            "frames_jsonl": frames_path.display().to_string(),
            "capture_summary": capture_summary,
        },
        "output": output_path.display().to_string(),
        "thresholds": {
            "max_p99_delay_ms": thresholds.max_p99_delay_ms,
            "max_token_gap_ms": thresholds.max_token_gap_ms,
            "min_gap_gate_events": thresholds.min_gap_gate_events,
            "max_missing_timestamp_rate": thresholds.max_missing_timestamp_rate,
            "assumed_backtest_latency_ms": assumed_backtest_latency_ms,
        },
        "stats": {
            "raw_frames": acc.stats.raw_frames,
            "binary_frames": acc.stats.binary_frames,
            "json_messages": acc.stats.json_messages,
            "book_events": acc.stats.book_events,
            "change_events": acc.stats.change_events,
            "clob_events": clob_events,
            "other_messages": acc.stats.other_messages,
            "malformed_lines": acc.stats.malformed_lines,
            "malformed_raw": acc.stats.malformed_raw,
            "missing_received_timestamp": acc.stats.missing_received_timestamp,
            "missing_event_timestamp": acc.stats.missing_event_timestamp,
            "missing_event_timestamp_rate": missing_timestamp_rate,
            "timestamped_events": acc.stats.timestamped_events,
            "delay_samples": acc.stats.delay_samples,
            "negative_delay_samples": acc.stats.negative_delay_samples,
            "negative_delay_rate": if acc.stats.delay_samples > 0 {
                acc.stats.negative_delay_samples as f64 / acc.stats.delay_samples as f64
            } else {
                0.0
            },
            "record_overhead_samples": acc.stats.record_overhead_samples,
            "negative_record_overhead_samples": acc.stats.negative_record_overhead_samples,
            "negative_record_overhead_rate": if acc.stats.record_overhead_samples > 0 {
                acc.stats.negative_record_overhead_samples as f64
                    / acc.stats.record_overhead_samples as f64
            } else {
                0.0
            },
            "first_frame_received_ms": acc.first_frame_received_ms,
            "last_frame_received_ms": acc.last_frame_received_ms,
            "max_stream_receive_gap_ms": acc.max_stream_receive_gap_ms,
            "active_stream_window_start_ms": acc.active_stream_window_start_ms,
            "active_stream_window_end_ms": acc.active_stream_window_end_ms,
            "first_active_frame_received_ms": acc.first_active_frame_received_ms,
            "last_active_frame_received_ms": acc.last_active_frame_received_ms,
            "max_active_stream_receive_gap_ms": active_stream_gap.map(|(gap, _, _)| gap),
            "max_active_stream_receive_gap_start_ms": active_stream_gap.and_then(|(_, start, _)| start),
            "max_active_stream_receive_gap_end_ms": active_stream_gap.and_then(|(_, _, end)| end),
            "evaluated_stream_receive_gap_ms": evaluated_stream_gap_ms,
        },
        "delay_ms": {
            "min": min_delay_ms,
            "avg": avg_delay_ms,
            "p50": p50_delay_ms,
            "p90": p90_delay_ms,
            "p95": p95_delay_ms,
            "p99": p99_delay_ms,
            "p99_5": p99_5_delay_ms,
            "max": max_delay_ms,
        },
        "delay_counts_above_ms": delay_counts_above_ms,
        "record_overhead_ms": record_overhead_ms,
        "token_coverage": {
            "expected_token_ids": expected_token_ids.iter().cloned().collect::<Vec<_>>(),
            "active_expected_token_ids": active_expected_token_ids.iter().cloned().collect::<Vec<_>>(),
            "observed_token_ids": observed_token_ids,
            "missing_token_ids": missing_token_ids,
            "missing_active_token_ids": missing_active_token_ids,
            "expected_count": expected_token_ids.len(),
            "active_expected_count": active_expected_token_ids.len(),
            "observed_count": acc.token_stats.len(),
            "token_outcomes": token_outcomes,
            "per_token": acc.token_stats,
            "max_observed_gap_ms": max_observed_token_gap_ms,
            "gap_gate_token_ids": gap_gate_token_ids,
            "gap_skipped_token_ids": gap_skipped_token_ids,
            "max_gap_gate_ms": max_gate_token_gap_ms,
            "stream_gap_ready": stream_gap_ready,
            "per_token_gap_threshold_exceeded_token_ids": per_token_gap_threshold_exceeded_token_ids,
            "gap_gate_mode": "active_market_window_stream_continuity_min_events",
        },
        "window_continuity": window_continuity,
        "active_stream_receive_gaps_over_limit": active_stream_receive_gaps_over_limit,
        "a_plus_latency_gate": {
            "ready": ready,
            "stream_latency_ready": stream_latency_ready,
            "timestamp_ready": timestamp_ready,
            "coverage_ready": coverage_ready,
            "token_gap_ready": gap_ready,
            "backtest_latency_assumption_ready": backtest_latency_assumption_ready,
            "strategy_retest_required": !backtest_latency_assumption_ready,
            "recommended_retest_latency_ms": recommended_retest_latency_ms,
            "verdict": verdict,
        }
    })
}

fn forward_latency_window_continuity(
    token_outcomes: &std::collections::BTreeMap<String, serde_json::Value>,
    token_stats: &std::collections::BTreeMap<String, ForwardLatencyTokenStats>,
    gaps: &[ForwardLatencyReceiveGap],
    max_gap_ms: f64,
) -> serde_json::Value {
    let mut markets = std::collections::BTreeMap::<String, (String, i64, i64)>::new();
    for outcome in token_outcomes.values() {
        let Some(condition_id) = outcome.get("condition_id").and_then(|value| value.as_str())
        else {
            continue;
        };
        let Some(slug) = outcome.get("slug").and_then(|value| value.as_str()) else {
            continue;
        };
        let Some((open_s, close_s, _)) = recorded_btc_slug_window(slug) else {
            continue;
        };
        markets.entry(condition_id.to_string()).or_insert_with(|| {
            (
                slug.to_string(),
                open_s.saturating_mul(1000),
                close_s.saturating_mul(1000),
            )
        });
    }

    let mut retained_conditions = 0_usize;
    let mut excluded_conditions = 0_usize;
    let mut per_condition = Vec::with_capacity(markets.len());
    for (condition_id, (slug, open_ms, close_ms)) in markets {
        let expected_token_ids = token_outcomes
            .iter()
            .filter(|(_, outcome)| {
                outcome
                    .get("condition_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(condition_id.as_str())
            })
            .map(|(token_id, _)| token_id.clone())
            .collect::<Vec<_>>();
        let missing_token_ids = expected_token_ids
            .iter()
            .filter(|token_id| !token_stats.contains_key(*token_id))
            .cloned()
            .collect::<Vec<_>>();
        let token_coverage_ready = expected_token_ids.len() >= 2 && missing_token_ids.is_empty();
        let intersecting_gaps = gaps
            .iter()
            .filter(|gap| {
                gap.gap_ms as f64 > max_gap_ms && gap.start_ms < close_ms && gap.end_ms > open_ms
            })
            .map(|gap| {
                serde_json::json!({
                    "start_ms": gap.start_ms,
                    "end_ms": gap.end_ms,
                    "gap_ms": gap.gap_ms,
                })
            })
            .collect::<Vec<_>>();
        let stream_continuity_ready = intersecting_gaps.is_empty();
        let ready = stream_continuity_ready && token_coverage_ready;
        if ready {
            retained_conditions += 1;
        } else {
            excluded_conditions += 1;
        }
        per_condition.push(serde_json::json!({
            "condition_id": condition_id,
            "slug": slug,
            "open_ms": open_ms,
            "close_ms": close_ms,
            "expected_token_ids": expected_token_ids,
            "missing_token_ids": missing_token_ids,
            "token_coverage_ready": token_coverage_ready,
            "stream_continuity_ready": stream_continuity_ready,
            "condition_ready": ready,
            "intersecting_gaps": intersecting_gaps,
        }));
    }
    per_condition.sort_by_key(|market| {
        market
            .get("open_ms")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(i64::MAX)
    });

    serde_json::json!({
        "gap_limit_ms": max_gap_ms,
        "conditions": per_condition.len(),
        "retained_conditions": retained_conditions,
        "excluded_conditions": excluded_conditions,
        "per_condition": per_condition,
    })
}

fn forward_latency_window_admissibility(
    input_dir: &std::path::Path,
    window_continuity: &serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    use anyhow::{anyhow, Context};

    let binance_path = input_dir.join("binance_btcusdt_rtds.csv");
    let chainlink_path = input_dir.join("chainlink_btcusd.csv");
    let mut binance = backtest::btc_history::BTCHistory::new();
    let mut chainlink = backtest::btc_history::BTCHistory::new();
    binance
        .load_csv(binance_path.to_string_lossy().as_ref())
        .with_context(|| format!("load Binance RTDS tape {}", binance_path.display()))?;
    chainlink
        .load_csv(chainlink_path.to_string_lossy().as_ref())
        .with_context(|| format!("load Chainlink RTDS tape {}", chainlink_path.display()))?;

    let conditions = window_continuity
        .get("per_condition")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow!("latency audit window continuity is missing per_condition rows"))?;
    let mut rows = Vec::with_capacity(conditions.len());
    for condition in conditions {
        let condition_id = condition
            .get("condition_id")
            .and_then(serde_json::Value::as_str)
            .context("window continuity row missing condition_id")?;
        let slug = condition
            .get("slug")
            .and_then(serde_json::Value::as_str)
            .context("window continuity row missing slug")?;
        let open_ms = condition
            .get("open_ms")
            .and_then(serde_json::Value::as_i64)
            .context("window continuity row missing open_ms")?;
        let close_ms = condition
            .get("close_ms")
            .and_then(serde_json::Value::as_i64)
            .context("window continuity row missing close_ms")?;
        let clob_ready = condition
            .get("condition_ready")
            .or_else(|| condition.get("stream_continuity_ready"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let binance_signal = forward_latency_tape_range_report(
            "Binance causal signal",
            &binance,
            open_ms.saturating_sub(3_600_000),
            close_ms,
        );
        let chainlink_settlement = forward_latency_tape_range_report(
            "Chainlink settlement",
            &chainlink,
            open_ms,
            close_ms,
        );
        let binance_ready = binance_signal["ready"].as_bool().unwrap_or(false);
        let chainlink_ready = chainlink_settlement["ready"].as_bool().unwrap_or(false);
        let admissible = clob_ready && binance_ready && chainlink_ready;
        let mut exclusion_reasons = Vec::new();
        if !clob_ready {
            exclusion_reasons.push("clob_stream_continuity".to_string());
        }
        if !binance_ready {
            exclusion_reasons.push("binance_causal_signal".to_string());
        }
        if !chainlink_ready {
            exclusion_reasons.push("chainlink_settlement".to_string());
        }
        rows.push(serde_json::json!({
            "condition_id": condition_id,
            "slug": slug,
            "open_ms": open_ms,
            "close_ms": close_ms,
            "clob_stream_continuity_ready": clob_ready,
            "binance_signal": binance_signal,
            "chainlink_settlement": chainlink_settlement,
            "admissible": admissible,
            "exclusion_reasons": exclusion_reasons,
        }));
    }
    rows.sort_by_key(|row| {
        row.get("open_ms")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(i64::MAX)
    });

    let mut groups = Vec::new();
    let mut current_group = Vec::new();
    let mut previous_close_ms = None;
    for row in rows
        .iter()
        .filter(|row| row["admissible"].as_bool() == Some(true))
    {
        let open_ms = row["open_ms"].as_i64().unwrap_or_default();
        if previous_close_ms.is_some_and(|close_ms| close_ms != open_ms)
            && !current_group.is_empty()
        {
            groups.push(forward_latency_admissible_group(
                groups.len() + 1,
                &current_group,
            ));
            current_group.clear();
        }
        current_group.push(row.clone());
        previous_close_ms = row["close_ms"].as_i64();
    }
    if !current_group.is_empty() {
        groups.push(forward_latency_admissible_group(
            groups.len() + 1,
            &current_group,
        ));
    }

    let admissible_conditions = rows
        .iter()
        .filter(|row| row["admissible"].as_bool() == Some(true))
        .count();
    Ok(serde_json::json!({
        "schema_version": 1,
        "signal_preroll_seconds": 3600,
        "conditions": rows.len(),
        "admissible_conditions": admissible_conditions,
        "excluded_conditions": rows.len().saturating_sub(admissible_conditions),
        "all_conditions_admissible": admissible_conditions == rows.len() && !rows.is_empty(),
        "has_admissible_conditions": admissible_conditions > 0,
        "groups": groups,
        "per_condition": rows,
    }))
}

fn forward_latency_tape_range_report(
    label: &str,
    history: &backtest::btc_history::BTCHistory,
    required_start_ms: i64,
    required_end_ms: i64,
) -> serde_json::Value {
    let median_interval_ms = history.median_interval_ms(required_start_ms, required_end_ms);
    let max_internal_gap_ms = history.max_gap_ms(required_start_ms, required_end_ms);
    let max_allowed_gap_ms = median_interval_ms.map(|median| median.saturating_mul(3).max(5_000));
    let boundary_coverage_ready = history.first_timestamp_ms() <= required_start_ms + 1_000
        && history.last_timestamp_ms() >= required_end_ms;
    let internal_gap_ready = max_internal_gap_ms
        .zip(max_allowed_gap_ms)
        .is_some_and(|(observed, allowed)| observed <= allowed);
    let failure = btc_history_coverage_error(label, history, required_start_ms, required_end_ms);
    serde_json::json!({
        "required_start_ms": required_start_ms,
        "required_end_ms": required_end_ms,
        "tape_first_ms": history.first_timestamp_ms(),
        "tape_last_ms": history.last_timestamp_ms(),
        "tape_ticks": history.n_ticks(),
        "median_interval_ms": median_interval_ms,
        "max_internal_gap_ms": max_internal_gap_ms,
        "max_allowed_gap_ms": max_allowed_gap_ms,
        "boundary_coverage_ready": boundary_coverage_ready,
        "internal_gap_ready": internal_gap_ready,
        "ready": failure.is_none(),
        "failure": failure,
    })
}

fn forward_latency_admissible_group(index: usize, rows: &[serde_json::Value]) -> serde_json::Value {
    serde_json::json!({
        "group": format!("group_{index:03}"),
        "conditions": rows.len(),
        "first_open_ms": rows.first().and_then(|row| row["open_ms"].as_i64()),
        "last_close_ms": rows.last().and_then(|row| row["close_ms"].as_i64()),
        "condition_ids": rows.iter().filter_map(|row| row["condition_id"].as_str()).collect::<Vec<_>>(),
        "slugs": rows.iter().filter_map(|row| row["slug"].as_str()).collect::<Vec<_>>(),
    })
}

fn forward_latency_percentile(sorted: &[f64], quantile: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let q = quantile.clamp(0.0, 1.0);
    let rank = (sorted.len() as f64 * q).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    Some(sorted[idx])
}

fn forward_latency_distribution_report(
    sorted: &[f64],
    sum_ms: f64,
    samples: u64,
) -> serde_json::Value {
    serde_json::json!({
        "samples": samples,
        "min": sorted.first().copied(),
        "avg": if samples > 0 { Some(sum_ms / samples as f64) } else { None },
        "p50": forward_latency_percentile(sorted, 0.50),
        "p90": forward_latency_percentile(sorted, 0.90),
        "p95": forward_latency_percentile(sorted, 0.95),
        "p99": forward_latency_percentile(sorted, 0.99),
        "p99_5": forward_latency_percentile(sorted, 0.995),
        "max": sorted.last().copied(),
    })
}

fn forward_latency_counts_above_ms(sorted: &[f64]) -> serde_json::Value {
    let thresholds = [75_u64, 100, 150, 200, 250, 300, 400, 500, 750];
    let mut counts = serde_json::Map::new();
    let samples = sorted.len() as f64;
    for threshold in thresholds {
        let count = sorted
            .iter()
            .filter(|value| **value > threshold as f64)
            .count() as u64;
        counts.insert(
            threshold.to_string(),
            serde_json::json!({
                "count": count,
                "rate": if samples > 0.0 { count as f64 / samples } else { 0.0 },
            }),
        );
    }
    serde_json::Value::Object(counts)
}

#[allow(clippy::too_many_arguments)]
async fn cmd_chainlink_backfill(
    endpoint: &str,
    feed_id: &str,
    api_key: &str,
    hmac_secret: &str,
    start: &str,
    end: &str,
    price_decimals: u32,
    page_limit: u32,
    output: &str,
) -> anyhow::Result<()> {
    use anyhow::{bail, Context};
    let start_s = chrono::DateTime::parse_from_rfc3339(start)
        .context("--start must be RFC3339")?
        .timestamp();
    let end_s = chrono::DateTime::parse_from_rfc3339(end)
        .context("--end must be RFC3339")?
        .timestamp();
    if end_s <= start_s {
        bail!("--end must be after --start");
    }
    if api_key.trim().is_empty() || hmac_secret.trim().is_empty() {
        bail!("Chainlink credentials are empty; historical access needs a valid subscription key");
    }
    let client =
        data::chainlink::ChainlinkDataStreamsClient::new(endpoint, api_key, hmac_secret);

    let mut rows: Vec<(i64, f64)> = Vec::new();
    let mut cursor = start_s;
    let mut pages = 0u32;
    loop {
        let (status, reports, error) = client
            .reports_page(feed_id, cursor, page_limit)
            .await?;
        if status == 401 || status == 403 {
            bail!(
                "Data Streams rejected historical access (HTTP {status}): {} — credential/plan \
                 problem, not a range problem",
                error.unwrap_or_default(),
            );
        }
        if status != 200 {
            bail!("reports page HTTP {status}: {}", error.unwrap_or_default());
        }
        if reports.is_empty() {
            break;
        }
        pages += 1;
        let mut advanced = false;
        for report in &reports {
            let Some(ts) = report.observations_timestamp else {
                continue;
            };
            if ts >= end_s {
                advanced = true;
                cursor = end_s;
                break;
            }
            if ts < cursor {
                continue;
            }
            let Some(price) = report
                .decoded_price
                .as_deref()
                .and_then(|raw| data::chainlink::decode_stream_price(raw, price_decimals))
            else {
                bail!("report at {ts} carries an undecodable price; refusing a gappy tape");
            };
            rows.push((ts * 1000, price));
            cursor = ts + 1;
            advanced = true;
        }
        if cursor >= end_s {
            break;
        }
        if !advanced {
            bail!("reports page did not advance the cursor; aborting to avoid an infinite loop");
        }
    }
    if rows.is_empty() {
        bail!("no reports in range — plan may exclude this history or the feed id is wrong");
    }
    rows.sort_by_key(|(ts, _)| *ts);
    rows.dedup_by_key(|(ts, _)| *ts);

    let out_path = std::path::Path::new(output);
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let tmp = out_path.with_extension(format!("tmp.{}", std::process::id()));
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)
            .with_context(|| format!("create {}", tmp.display()))?;
        writeln!(f, "timestamp_ms,price")?;
        for (ts_ms, price) in &rows {
            writeln!(f, "{ts_ms},{price}")?;
        }
        f.sync_all().ok();
    }
    std::fs::rename(&tmp, out_path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), out_path.display()))?;
    let span_s = rows.last().unwrap().0 / 1000 - rows.first().unwrap().0 / 1000;
    println!(
        "chainlink-backfill: {} reports over {}s ({} pages) -> {}",
        rows.len(),
        span_s,
        pages,
        out_path.display(),
    );
    Ok(())
}

async fn cmd_chainlink_data_streams_probe(
    endpoint: &str,
    feed_ids: &[String],
    rest_websocket_username: Option<&str>,
    api_key: Option<&str>,
    hmac_secret: Option<&str>,
    output: Option<&str>,
) -> anyhow::Result<()> {
    use anyhow::Context;

    let feed_ids = feed_ids
        .iter()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
        .collect::<Vec<_>>();
    let rest_websocket_username = chainlink_credential_value(
        rest_websocket_username,
        &[
            "CHAINLINK_DATA_STREAMS_REST_WEBSOCKET_USERNAME",
            "CHAINLINK_DATA_STREAMS_USERNAME",
            "CHAINLINK_DATA_STREAMS_AUTH_ID",
        ],
    );
    let api_key = chainlink_credential_value(api_key, &["CHAINLINK_DATA_STREAMS_API_KEY"]);
    let hmac_secret = chainlink_credential_value(
        hmac_secret,
        &[
            "CHAINLINK_DATA_STREAMS_HMAC_SECRET",
            "CHAINLINK_DATA_STREAMS_API_SECRET",
        ],
    );
    let authorization_value = api_key
        .as_deref()
        .or(rest_websocket_username.as_deref())
        .map(str::to_string);
    let credentials_ready = authorization_value.is_some() && hmac_secret.is_some();
    let malformed_feed_id_count = feed_ids
        .iter()
        .filter(|id| !looks_like_chainlink_feed_id(id))
        .count() as u64;
    let feed_id_shape_ready = malformed_feed_id_count == 0;
    let output_path = output.map(std::path::PathBuf::from);
    let mut probes = Vec::new();
    let mut request_errors = Vec::new();

    if credentials_ready && !feed_ids.is_empty() && feed_id_shape_ready {
        let client = data::chainlink::ChainlinkDataStreamsClient::new(
            endpoint,
            authorization_value.as_deref().unwrap_or_default(),
            hmac_secret.as_deref().unwrap_or_default(),
        );
        for feed_id in &feed_ids {
            match client.latest_report(feed_id).await {
                Ok(probe) => probes.push(probe),
                Err(e) => request_errors.push(serde_json::json!({
                    "feed_id": feed_id,
                    "error": format!("{e:#}"),
                })),
            }
        }
    }

    let requested = feed_ids.len() as u64;
    let successful_http = probes
        .iter()
        .filter(|probe| (200..300).contains(&probe.http_status))
        .count() as u64;
    let reports_with_metadata = probes.iter().filter(|probe| probe.report.is_some()).count() as u64;
    let reports_with_observation_ts = probes
        .iter()
        .filter(|probe| {
            probe
                .report
                .as_ref()
                .and_then(|report| report.observations_timestamp)
                .is_some()
        })
        .count() as u64;
    let reports_with_decoded_price = probes
        .iter()
        .filter(|probe| {
            probe
                .report
                .as_ref()
                .and_then(|report| report.decoded_price.as_ref())
                .is_some()
        })
        .count() as u64;
    let max_latency_ms = probes
        .iter()
        .map(|probe| probe.latency_ms)
        .max()
        .unwrap_or(0);
    let max_observation_lag_ms = probes
        .iter()
        .filter_map(|probe| probe.observation_lag_ms)
        .max();
    let transport_ready = credentials_ready
        && requested > 0
        && feed_id_shape_ready
        && request_errors.is_empty()
        && successful_http == requested;
    let report_metadata_ready = transport_ready
        && reports_with_metadata == requested
        && reports_with_observation_ts == requested;
    let decoded_price_ready = report_metadata_ready && reports_with_decoded_price == requested;
    let settlement_alignment_ready = false;
    let verdict = if feed_ids.is_empty() && !credentials_ready {
        "CHAINLINK_FEED_ID_AND_CREDENTIALS_REQUIRED"
    } else if feed_ids.is_empty() {
        "CHAINLINK_FEED_ID_REQUIRED"
    } else if !credentials_ready {
        "CHAINLINK_CREDENTIALS_REQUIRED"
    } else if !feed_id_shape_ready {
        "CHAINLINK_FEED_ID_LOOKS_INVALID"
    } else if !request_errors.is_empty() {
        "CHAINLINK_TRANSPORT_REQUEST_FAILED"
    } else if !transport_ready {
        "CHAINLINK_TRANSPORT_NOT_READY"
    } else if !report_metadata_ready {
        "CHAINLINK_REPORT_METADATA_MISSING"
    } else if !decoded_price_ready {
        "CHAINLINK_TRANSPORT_READY_DECODER_REQUIRED"
    } else {
        "CHAINLINK_DECODED_TAPE_READY_NEEDS_WINDOW_ALIGNMENT"
    };

    let report = serde_json::json!({
        "schema_version": 1,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "source_kind": "chainlink_btc_usd_data_stream",
        "endpoint": endpoint,
        "feed_ids": feed_ids,
        "auth": {
            "rest_websocket_username_present": rest_websocket_username.is_some(),
            "api_key_present": api_key.is_some(),
            "hmac_secret_present": hmac_secret.is_some(),
            "credentials_ready": credentials_ready,
        },
        "stats": {
            "requested": requested,
            "feed_id_shape_ready": feed_id_shape_ready,
            "malformed_feed_id_count": malformed_feed_id_count,
            "successful_http": successful_http,
            "reports_with_metadata": reports_with_metadata,
            "reports_with_observation_timestamp": reports_with_observation_ts,
            "reports_with_decoded_price": reports_with_decoded_price,
            "request_errors": request_errors.len(),
            "max_latency_ms": max_latency_ms,
            "max_observation_lag_ms": max_observation_lag_ms,
        },
        "chainlink_shadow_gate": {
            "ready": decoded_price_ready && settlement_alignment_ready,
            "transport_ready": transport_ready,
            "report_metadata_ready": report_metadata_ready,
            "decoded_price_ready": decoded_price_ready,
            "settlement_alignment_ready": settlement_alignment_ready,
            "verdict": verdict,
        },
        "request_errors": request_errors,
        "reports": probes,
    });

    if let Some(path) = output_path {
        write_json_atomic(&path, &report, true)
            .with_context(|| format!("write {}", path.display()))?;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize Chainlink probe report")
    );
    Ok(())
}

fn chainlink_credential_value(cli_value: Option<&str>, env_keys: &[&str]) -> Option<String> {
    cli_value
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            env_keys.iter().find_map(|key| {
                std::env::var(key)
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
}

fn looks_like_chainlink_feed_id(feed_id: &str) -> bool {
    let Some(hex) = feed_id.trim().strip_prefix("0x") else {
        return false;
    };
    hex.len() >= 64 && hex.len() % 2 == 0 && hex.chars().all(|c| c.is_ascii_hexdigit())
}

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
struct RecordedBooksConvertStats {
    raw_frames: u64,
    json_messages: u64,
    book_events: u64,
    change_events: u64,
    bytes_written: u64,
    skipped_binary_frames: u64,
    skipped_malformed_lines: u64,
    skipped_malformed_raw: u64,
    skipped_other_messages: u64,
    skipped_unknown_market: u64,
    skipped_unknown_token: u64,
    skipped_missing_fields: u64,
    filtered_out_events: u64,
}

#[derive(Debug, Default)]
struct RecordedTickMarketAudit {
    raw_tick_size_change_rows: u64,
    token_ids: std::collections::BTreeSet<String>,
    old_tick_sizes: std::collections::BTreeSet<String>,
    new_tick_sizes: std::collections::BTreeSet<String>,
    first_tick_event_ts_ms: Option<i64>,
    last_tick_event_ts_ms: Option<i64>,
    first_preserved_threshold_crossing_ts_ms: Option<i64>,
    first_preserved_threshold_crossing_at_or_after_tick_ts_ms: Option<i64>,
}

#[derive(Debug, Default)]
struct RecordedTickIntegrityAccumulator {
    malformed_selected_tick_size_change_rows: u64,
    markets: std::collections::BTreeMap<String, RecordedTickMarketAudit>,
}

impl RecordedTickIntegrityAccumulator {
    fn observe_raw_value(
        &mut self,
        value: &serde_json::Value,
        row_ts_ms: Option<i64>,
        selected_market_ids: &std::collections::BTreeSet<String>,
    ) {
        if let Some(items) = value.as_array() {
            for item in items {
                self.observe_raw_value(item, row_ts_ms, selected_market_ids);
            }
            return;
        }
        let event_type = value
            .get("event_type")
            .or_else(|| value.get("type"))
            .and_then(|field| field.as_str())
            .unwrap_or("");
        if event_type != "tick_size_change" {
            return;
        }
        let data = value.get("data").unwrap_or(value);
        let Some(condition_id) = data.get("market").and_then(|field| field.as_str()) else {
            self.malformed_selected_tick_size_change_rows += 1;
            return;
        };
        if !selected_market_ids.contains(condition_id) {
            return;
        }
        let token_id = data
            .get("asset_id")
            .and_then(|field| field.as_str())
            .filter(|token| !token.is_empty());
        let old_tick_size = recorded_positive_number_string(data.get("old_tick_size"));
        let new_tick_size = recorded_positive_number_string(data.get("new_tick_size"));
        let timestamp_ms = forward_latency_event_timestamp_ms(data).or(row_ts_ms);
        if token_id.is_none()
            || old_tick_size.is_none()
            || new_tick_size.is_none()
            || timestamp_ms.is_none_or(|timestamp| timestamp <= 0)
        {
            self.malformed_selected_tick_size_change_rows += 1;
            return;
        }
        let timestamp_ms = timestamp_ms.unwrap();
        let market = self.markets.entry(condition_id.to_string()).or_default();
        market.raw_tick_size_change_rows += 1;
        market.token_ids.insert(token_id.unwrap().to_string());
        market.old_tick_sizes.insert(old_tick_size.unwrap());
        market.new_tick_sizes.insert(new_tick_size.unwrap());
        market.first_tick_event_ts_ms = Some(
            market
                .first_tick_event_ts_ms
                .map_or(timestamp_ms, |current| current.min(timestamp_ms)),
        );
        market.last_tick_event_ts_ms = Some(
            market
                .last_tick_event_ts_ms
                .map_or(timestamp_ms, |current| current.max(timestamp_ms)),
        );
        if market
            .first_preserved_threshold_crossing_ts_ms
            .is_some_and(|crossing| crossing >= timestamp_ms)
        {
            market.first_preserved_threshold_crossing_at_or_after_tick_ts_ms =
                market.first_preserved_threshold_crossing_ts_ms;
        }
    }

    fn observe_distilled_event(&mut self, event: &backtest::distill::DistilledEvent) {
        let (condition_id, timestamp_s, best_bid, best_ask) = match event {
            backtest::distill::DistilledEvent::Book {
                ts, mkt, bb, ba, ..
            }
            | backtest::distill::DistilledEvent::Change {
                ts, mkt, bb, ba, ..
            } => (mkt, *ts, *bb, *ba),
            backtest::distill::DistilledEvent::Trade { .. } => return,
        };
        if !strategy::microstructure::top_crosses_dynamic_tick_threshold(best_bid, best_ask) {
            return;
        }
        let timestamp_ms = (timestamp_s * 1000.0).round();
        if !timestamp_ms.is_finite() || timestamp_ms <= 0.0 || timestamp_ms > i64::MAX as f64 {
            return;
        }
        let timestamp_ms = timestamp_ms as i64;
        let market = self.markets.entry(condition_id.to_string()).or_default();
        market.first_preserved_threshold_crossing_ts_ms = Some(
            market
                .first_preserved_threshold_crossing_ts_ms
                .map_or(timestamp_ms, |current| current.min(timestamp_ms)),
        );
        if market
            .first_tick_event_ts_ms
            .is_some_and(|tick_event| timestamp_ms >= tick_event)
        {
            market.first_preserved_threshold_crossing_at_or_after_tick_ts_ms = Some(
                market
                    .first_preserved_threshold_crossing_at_or_after_tick_ts_ms
                    .map_or(timestamp_ms, |current| current.min(timestamp_ms)),
            );
        }
    }

    fn finish(&self) -> serde_json::Value {
        let mut tick_size_change_rows = 0_u64;
        let mut markets_with_tick_size_change = 0_u64;
        let mut inferred_without_raw_tick_event = 0_u64;
        let mut transitions_match_documented_contract = true;
        let mut all_observed_transitions_reconstructable = true;
        let mut markets = Vec::new();
        for (condition_id, market) in &self.markets {
            if market.raw_tick_size_change_rows == 0 {
                if market.first_preserved_threshold_crossing_ts_ms.is_some() {
                    inferred_without_raw_tick_event += 1;
                }
                continue;
            }
            tick_size_change_rows += market.raw_tick_size_change_rows;
            markets_with_tick_size_change += 1;
            let documented_transition = market.old_tick_sizes.len() == 1
                && market.old_tick_sizes.contains("0.01")
                && market.new_tick_sizes.len() == 1
                && market.new_tick_sizes.contains("0.001");
            let reconstruction_ready = market
                .first_preserved_threshold_crossing_at_or_after_tick_ts_ms
                .is_some();
            transitions_match_documented_contract &= documented_transition;
            all_observed_transitions_reconstructable &= reconstruction_ready;
            let reconstruction_delay_ms = market
                .first_preserved_threshold_crossing_at_or_after_tick_ts_ms
                .zip(market.first_tick_event_ts_ms)
                .map(|(crossing, tick)| crossing.saturating_sub(tick));
            markets.push(serde_json::json!({
                "condition_id": condition_id,
                "raw_tick_size_change_rows": market.raw_tick_size_change_rows,
                "token_ids": market.token_ids,
                "old_tick_sizes": market.old_tick_sizes,
                "new_tick_sizes": market.new_tick_sizes,
                "first_tick_event_ts_ms": market.first_tick_event_ts_ms,
                "last_tick_event_ts_ms": market.last_tick_event_ts_ms,
                "first_preserved_threshold_crossing_ts_ms": market.first_preserved_threshold_crossing_ts_ms,
                "first_preserved_threshold_crossing_at_or_after_tick_ts_ms": market.first_preserved_threshold_crossing_at_or_after_tick_ts_ms,
                "reconstruction_delay_ms": reconstruction_delay_ms,
                "documented_transition": documented_transition,
                "reconstruction_ready": reconstruction_ready,
            }));
        }
        serde_json::json!({
            "schema_version": 1,
            "inference_rule": "initialize from valid Gamma minimum_tick_size or 0.01; persist 0.001 after a replayed positive top bid or ask crosses below 0.04 or above 0.96",
            "raw_tick_size_change_rows": tick_size_change_rows,
            "markets_with_tick_size_change": markets_with_tick_size_change,
            "markets_with_inferred_transition_without_raw_tick_event": inferred_without_raw_tick_event,
            "malformed_selected_tick_size_change_rows": self.malformed_selected_tick_size_change_rows,
            "transitions_match_documented_contract": transitions_match_documented_contract,
            "all_observed_transitions_reconstructable": all_observed_transitions_reconstructable,
            "distilled_schema_changed": false,
            "tick_size_change_events_preserved_in_distilled_stream": false,
            "markets": markets,
        })
    }
}

fn recorded_positive_number_string(value: Option<&serde_json::Value>) -> Option<String> {
    let value = value?;
    let text = match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Number(number) => number.to_string(),
        _ => return None,
    };
    text.parse::<f64>()
        .ok()
        .filter(|number| number.is_finite() && *number > 0.0)
        .map(|number| number.to_string())
}

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
struct RecordedBooksHourStats {
    book_events: u64,
    change_events: u64,
    bytes_written: u64,
}

struct RecordedDistilledWriter {
    hour: chrono::DateTime<chrono::Utc>,
    path: std::path::PathBuf,
    tmp_path: std::path::PathBuf,
    gz: flate2::write::GzEncoder<std::io::BufWriter<std::fs::File>>,
    stats: RecordedBooksHourStats,
}

impl RecordedDistilledWriter {
    fn new(
        output_dir: &std::path::Path,
        hour: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<Self> {
        use anyhow::Context;

        std::fs::create_dir_all(output_dir)
            .with_context(|| format!("create {}", output_dir.display()))?;
        let path = backtest::distill::shared_cache_path_for_hour(output_dir, hour);
        let tmp_path = path.with_extension(format!("jsonl.gz.tmp.{}", std::process::id()));
        let file = std::fs::File::create(&tmp_path)
            .with_context(|| format!("create tmp {}", tmp_path.display()))?;
        let writer = std::io::BufWriter::new(file);
        let gz = flate2::write::GzEncoder::new(writer, flate2::Compression::fast());
        Ok(Self {
            hour,
            path,
            tmp_path,
            gz,
            stats: RecordedBooksHourStats::default(),
        })
    }

    fn write_event(&mut self, event: &backtest::distill::DistilledEvent) -> anyhow::Result<u64> {
        use anyhow::Context;
        use std::io::Write;

        let line = serde_json::to_string(event).context("serialize distilled event")?;
        self.gz
            .write_all(line.as_bytes())
            .context("write distilled event")?;
        self.gz
            .write_all(b"\n")
            .context("write distilled newline")?;
        let bytes = line.len() as u64 + 1;
        match event {
            backtest::distill::DistilledEvent::Book { .. } => self.stats.book_events += 1,
            backtest::distill::DistilledEvent::Change { .. } => self.stats.change_events += 1,
            backtest::distill::DistilledEvent::Trade { .. } => {}
        }
        self.stats.bytes_written += bytes;
        Ok(bytes)
    }

    fn finish(self) -> anyhow::Result<serde_json::Value> {
        use anyhow::Context;

        let mut gz = self.gz;
        gz.try_finish().context("finish gzip stream")?;
        let inner = gz.finish().context("flush gzip stream")?;
        inner
            .into_inner()
            .context("flush distilled writer")?
            .sync_all()
            .ok();
        std::fs::rename(&self.tmp_path, &self.path).with_context(|| {
            format!(
                "rename {} -> {}",
                self.tmp_path.display(),
                self.path.display()
            )
        })?;
        Ok(serde_json::json!({
            "hour": self.hour.to_rfc3339(),
            "path": self.path.display().to_string(),
            "stats": self.stats,
        }))
    }
}

fn cmd_convert_recorded_btc_books(
    input_dir: &str,
    output_dir: &str,
    requested_condition_ids: &[String],
) -> anyhow::Result<()> {
    use anyhow::{bail, Context};
    use chrono::TimeZone;
    use serde_json::Value;
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::BufRead;

    let input_dir = std::path::PathBuf::from(input_dir);
    let output_dir = std::path::PathBuf::from(output_dir);
    let frames_path = input_dir.join("market_ws_frames.jsonl");
    let gamma_path = input_dir.join("gamma_market_cache.json");
    let manifest_path = output_dir.join("manifest.json");

    let gamma_file = std::fs::File::open(&gamma_path)
        .with_context(|| format!("open {}", gamma_path.display()))?;
    let all_gamma_by_condition: BTreeMap<String, data::models::Market> =
        serde_json::from_reader(gamma_file)
            .with_context(|| format!("decode {}", gamma_path.display()))?;
    if all_gamma_by_condition.is_empty() {
        bail!("{} had no markets", gamma_path.display());
    }

    let mut market_ids = BTreeSet::new();
    let mut token_to_market = BTreeMap::new();
    for (cid, market) in &all_gamma_by_condition {
        market_ids.insert(cid.clone());
        for outcome in &market.outcomes {
            if !outcome.token_id.is_empty() {
                token_to_market.insert(outcome.token_id.clone(), cid.clone());
            }
        }
    }
    if token_to_market.is_empty() {
        bail!("{} had no outcome token IDs", gamma_path.display());
    }

    let selected_market_ids = if requested_condition_ids.is_empty() {
        market_ids.clone()
    } else {
        let mut selected = BTreeSet::new();
        for requested in requested_condition_ids {
            let condition_id = requested.trim();
            if condition_id.is_empty() {
                bail!("--condition-id must not be empty");
            }
            if !market_ids.contains(condition_id) {
                bail!("requested condition {condition_id} is absent from gamma market cache");
            }
            selected.insert(condition_id.to_string());
        }
        selected
    };
    let gamma_by_condition: BTreeMap<_, _> = all_gamma_by_condition
        .iter()
        .filter(|(condition_id, _)| selected_market_ids.contains(*condition_id))
        .map(|(condition_id, market)| (condition_id.clone(), market.clone()))
        .collect();
    let mut token_outcomes = BTreeMap::new();
    for (condition_id, market) in &gamma_by_condition {
        for outcome in &market.outcomes {
            if !outcome.token_id.is_empty() {
                token_outcomes.insert(
                    outcome.token_id.clone(),
                    serde_json::json!({
                        "condition_id": condition_id,
                        "slug": market.slug,
                        "outcome": outcome.name,
                    }),
                );
            }
        }
    }

    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("create {}", output_dir.display()))?;
    let frames_file = std::fs::File::open(&frames_path)
        .with_context(|| format!("open {}", frames_path.display()))?;
    let reader = std::io::BufReader::new(frames_file);
    let mut stats = RecordedBooksConvertStats::default();
    let mut tick_integrity = RecordedTickIntegrityAccumulator::default();
    let mut writers: BTreeMap<i64, RecordedDistilledWriter> = BTreeMap::new();

    for line_res in reader.lines() {
        let line = match line_res {
            Ok(line) => line,
            Err(_) => {
                stats.skipped_malformed_lines += 1;
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let row: Value = match serde_json::from_str(&line) {
            Ok(row) => row,
            Err(_) => {
                stats.skipped_malformed_lines += 1;
                continue;
            }
        };
        if row.get("raw_binary_len").is_some() {
            stats.skipped_binary_frames += 1;
            continue;
        }
        let Some(raw) = row.get("raw").and_then(|v| v.as_str()) else {
            stats.skipped_malformed_lines += 1;
            continue;
        };
        stats.raw_frames += 1;
        let raw_value: Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(_) => {
                stats.skipped_malformed_raw += 1;
                continue;
            }
        };
        let row_ts_ms = row.get("ts_received_ms").and_then(recorded_json_i64);
        tick_integrity.observe_raw_value(&raw_value, row_ts_ms, &selected_market_ids);
        let mut events = Vec::new();
        recorded_ws_value_to_distilled_events(
            &raw_value,
            row_ts_ms,
            &market_ids,
            &token_to_market,
            &mut events,
            &mut stats,
        );
        for event in events {
            if !selected_market_ids.contains(recorded_distilled_event_market(&event)) {
                stats.filtered_out_events += 1;
                continue;
            }
            tick_integrity.observe_distilled_event(&event);
            let ts = recorded_distilled_event_ts(&event);
            let hour_s = (ts.floor() as i64).div_euclid(3600) * 3600;
            let hour = chrono::Utc
                .timestamp_opt(hour_s, 0)
                .single()
                .with_context(|| format!("build hour from timestamp {ts}"))?;
            let writer = match writers.entry(hour_s) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(RecordedDistilledWriter::new(&output_dir, hour)?)
                }
            };
            let bytes = writer.write_event(&event)?;
            stats.bytes_written += bytes;
            match event {
                backtest::distill::DistilledEvent::Book { .. } => stats.book_events += 1,
                backtest::distill::DistilledEvent::Change { .. } => stats.change_events += 1,
                backtest::distill::DistilledEvent::Trade { .. } => {}
            }
        }
    }

    if stats.book_events + stats.change_events == 0 {
        bail!(
            "no distilled book/change events emitted from {}",
            frames_path.display()
        );
    }

    let mut hours = Vec::new();
    for (_, writer) in writers {
        hours.push(writer.finish()?);
    }
    let tick_integrity = tick_integrity.finish();
    if tick_integrity["malformed_selected_tick_size_change_rows"]
        .as_u64()
        .unwrap_or(0)
        > 0
    {
        bail!("selected tick_size_change rows were malformed");
    }
    if tick_integrity["transitions_match_documented_contract"] != true {
        bail!("selected tick_size_change rows violate the documented 0.01-to-0.001 contract");
    }
    if tick_integrity["all_observed_transitions_reconstructable"] != true {
        bail!("selected tick_size_change rows cannot be reconstructed from preserved book events");
    }
    let manifest = serde_json::json!({
        "schema_version": 1,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "source": {
            "input_dir": input_dir.display().to_string(),
            "frames_jsonl": frames_path.display().to_string(),
            "gamma_market_cache": gamma_path.display().to_string(),
        },
        "selection": {
            "filtered_to_condition_ids": !requested_condition_ids.is_empty(),
            "source_market_count": all_gamma_by_condition.len(),
            "selected_market_count": gamma_by_condition.len(),
            "selected_condition_ids": selected_market_ids,
        },
        "output": {
            "output_dir": output_dir.display().to_string(),
            "manifest": manifest_path.display().to_string(),
            "distilled_schema": backtest::distill::SCHEMA_VERSION,
            "harness_env": {
                "PMXT_DISTILLED_DIR": output_dir.display().to_string(),
            },
            "exact_replay_flag": "--require-shared-distilled",
        },
        "stats": stats,
        "tick_integrity": tick_integrity,
        "hours": hours,
        "markets": gamma_by_condition,
        "token_outcomes": token_outcomes,
    });
    write_json_atomic(&manifest_path, &manifest, true)
        .with_context(|| format!("write {}", manifest_path.display()))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&manifest).expect("serialize converter manifest")
    );
    Ok(())
}

fn recorded_distilled_event_market(event: &backtest::distill::DistilledEvent) -> &str {
    match event {
        backtest::distill::DistilledEvent::Book { mkt, .. }
        | backtest::distill::DistilledEvent::Change { mkt, .. }
        | backtest::distill::DistilledEvent::Trade { mkt, .. } => mkt,
    }
}

fn recorded_ws_value_to_distilled_events(
    value: &serde_json::Value,
    row_ts_ms: Option<i64>,
    market_ids: &std::collections::BTreeSet<String>,
    token_to_market: &std::collections::BTreeMap<String, String>,
    out: &mut Vec<backtest::distill::DistilledEvent>,
    stats: &mut RecordedBooksConvertStats,
) {
    if let Some(arr) = value.as_array() {
        for item in arr {
            recorded_ws_value_to_distilled_events(
                item,
                row_ts_ms,
                market_ids,
                token_to_market,
                out,
                stats,
            );
        }
        return;
    }

    stats.json_messages += 1;
    let msg_type = value
        .get("event_type")
        .or_else(|| value.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let data = value.get("data").unwrap_or(value);
    match msg_type {
        "book" => {
            recorded_book_to_distilled(data, row_ts_ms, market_ids, token_to_market, out, stats)
        }
        "price_change" => {
            recorded_change_to_distilled(data, row_ts_ms, market_ids, token_to_market, out, stats)
        }
        _ => stats.skipped_other_messages += 1,
    }
}

fn recorded_book_to_distilled(
    data: &serde_json::Value,
    row_ts_ms: Option<i64>,
    market_ids: &std::collections::BTreeSet<String>,
    token_to_market: &std::collections::BTreeMap<String, String>,
    out: &mut Vec<backtest::distill::DistilledEvent>,
    stats: &mut RecordedBooksConvertStats,
) {
    let Some(mkt) = data.get("market").and_then(|v| v.as_str()) else {
        stats.skipped_missing_fields += 1;
        return;
    };
    let Some(tok) = data.get("asset_id").and_then(|v| v.as_str()) else {
        stats.skipped_missing_fields += 1;
        return;
    };
    if !recorded_market_token_is_known(mkt, tok, market_ids, token_to_market, stats) {
        return;
    }
    let ts = recorded_message_ts_s(data, row_ts_ms);
    let bids = recorded_levels(data.get("bids"));
    let asks = recorded_levels(data.get("asks"));
    let bb = bids
        .iter()
        .filter_map(|[p, _]| p.parse::<f64>().ok())
        .fold(0.0, f64::max);
    let ba = asks
        .iter()
        .filter_map(|[p, _]| p.parse::<f64>().ok())
        .filter(|p| *p > 0.0)
        .fold(f64::INFINITY, f64::min);
    out.push(backtest::distill::DistilledEvent::Book {
        ts,
        mkt: mkt.to_string(),
        tok: tok.to_string(),
        bb,
        ba: if ba.is_finite() { ba } else { 0.0 },
        bids,
        asks,
    });
}

fn recorded_change_to_distilled(
    data: &serde_json::Value,
    row_ts_ms: Option<i64>,
    market_ids: &std::collections::BTreeSet<String>,
    token_to_market: &std::collections::BTreeMap<String, String>,
    out: &mut Vec<backtest::distill::DistilledEvent>,
    stats: &mut RecordedBooksConvertStats,
) {
    let Some(mkt) = data.get("market").and_then(|v| v.as_str()) else {
        stats.skipped_missing_fields += 1;
        return;
    };
    if !market_ids.contains(mkt) {
        stats.skipped_unknown_market += 1;
        return;
    }
    let Some(changes) = data
        .get("price_changes")
        .or_else(|| data.get("changes"))
        .and_then(|v| v.as_array())
    else {
        stats.skipped_missing_fields += 1;
        return;
    };
    let ts = recorded_message_ts_s(data, row_ts_ms);
    for ch in changes {
        let token = ch
            .get("asset_id")
            .and_then(|v| v.as_str())
            .or_else(|| data.get("asset_id").and_then(|v| v.as_str()));
        let Some(tok) = token else {
            stats.skipped_missing_fields += 1;
            continue;
        };
        if !recorded_market_token_is_known(mkt, tok, market_ids, token_to_market, stats) {
            continue;
        }
        let s = ch
            .get("side")
            .map(recorded_json_string)
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        let p = ch
            .get("price")
            .map(recorded_json_string)
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        let sz = ch
            .get("size")
            .map(recorded_json_string)
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        if s.is_empty() || p.is_empty() || sz.is_empty() {
            stats.skipped_missing_fields += 1;
            continue;
        }
        let bb = ch
            .get("best_bid")
            .and_then(recorded_json_f64)
            .unwrap_or(0.0);
        let ba = ch
            .get("best_ask")
            .and_then(recorded_json_f64)
            .unwrap_or(0.0);
        out.push(backtest::distill::DistilledEvent::Change {
            ts,
            mkt: mkt.to_string(),
            tok: tok.to_string(),
            s,
            bb,
            ba,
            p,
            sz,
        });
    }
}

fn recorded_market_token_is_known(
    market_id: &str,
    token_id: &str,
    market_ids: &std::collections::BTreeSet<String>,
    token_to_market: &std::collections::BTreeMap<String, String>,
    stats: &mut RecordedBooksConvertStats,
) -> bool {
    if !market_ids.contains(market_id) {
        stats.skipped_unknown_market += 1;
        return false;
    }
    if token_to_market
        .get(token_id)
        .map(|m| m == market_id)
        .unwrap_or(false)
    {
        return true;
    }
    stats.skipped_unknown_token += 1;
    false
}

fn recorded_distilled_event_ts(event: &backtest::distill::DistilledEvent) -> f64 {
    match event {
        backtest::distill::DistilledEvent::Book { ts, .. }
        | backtest::distill::DistilledEvent::Change { ts, .. }
        | backtest::distill::DistilledEvent::Trade { ts, .. } => *ts,
    }
}

fn recorded_message_ts_s(data: &serde_json::Value, row_ts_ms: Option<i64>) -> f64 {
    data.get("timestamp")
        .and_then(recorded_json_f64)
        .map(|ts| {
            if ts > 10_000_000_000.0 {
                ts / 1000.0
            } else {
                ts
            }
        })
        .or_else(|| row_ts_ms.map(|ms| ms as f64 / 1000.0))
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as f64 / 1000.0)
}

fn recorded_levels(value: Option<&serde_json::Value>) -> Vec<[String; 2]> {
    let Some(serde_json::Value::Array(arr)) = value else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        match entry {
            serde_json::Value::Array(pair) if pair.len() >= 2 => {
                let p = recorded_json_string(&pair[0]);
                let s = recorded_json_string(&pair[1]);
                if !p.is_empty() && !s.is_empty() {
                    out.push([p, s]);
                }
            }
            serde_json::Value::Object(obj) => {
                let p = obj
                    .get("price")
                    .map(recorded_json_string)
                    .unwrap_or_default();
                let s = obj
                    .get("size")
                    .map(recorded_json_string)
                    .unwrap_or_default();
                if !p.is_empty() && !s.is_empty() {
                    out.push([p, s]);
                }
            }
            _ => {}
        }
    }
    out
}

fn recorded_json_f64(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn recorded_json_i64(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn recorded_json_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => String::new(),
    }
}

async fn cmd_finalize_recorded_btc_books(
    settings: &config::Settings,
    input_dir: &str,
    condition_ids: &[String],
    btc_csv: Option<&str>,
    settlement_source_kind: &str,
    output: Option<&str>,
) -> anyhow::Result<()> {
    use anyhow::{bail, Context};
    use std::collections::{BTreeMap, BTreeSet};

    let input_dir = std::path::PathBuf::from(input_dir);
    let manifest_path = input_dir.join("manifest.json");
    let output_path = output
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| input_dir.join("resolution_manifest.json"));

    let manifest_file = std::fs::File::open(&manifest_path)
        .with_context(|| format!("open {}", manifest_path.display()))?;
    let manifest_value: serde_json::Value = serde_json::from_reader(manifest_file)
        .with_context(|| format!("decode {}", manifest_path.display()))?;
    let markets_value = manifest_value
        .get("markets")
        .cloned()
        .context("manifest missing `markets`")?;
    let captured_markets: BTreeMap<String, data::models::Market> =
        serde_json::from_value(markets_value).context("decode manifest markets")?;
    let requested_condition_ids = condition_ids.iter().cloned().collect::<BTreeSet<_>>();
    let captured_condition_ids = captured_markets
        .values()
        .map(|market| market.condition_id.clone())
        .collect::<BTreeSet<_>>();
    let unknown_condition_ids = requested_condition_ids
        .difference(&captured_condition_ids)
        .cloned()
        .collect::<Vec<_>>();
    if !unknown_condition_ids.is_empty() {
        bail!(
            "requested condition IDs are absent from {}: {}",
            manifest_path.display(),
            unknown_condition_ids.join(",")
        );
    }
    let original_markets = if requested_condition_ids.is_empty() {
        captured_markets
    } else {
        captured_markets
            .into_iter()
            .filter(|(_, market)| requested_condition_ids.contains(&market.condition_id))
            .collect()
    };
    if original_markets.is_empty() {
        bail!("{} had no markets", manifest_path.display());
    }

    let slugs: Vec<String> = original_markets
        .values()
        .map(|m| m.slug.clone())
        .filter(|s| !s.is_empty())
        .collect();
    if slugs.is_empty() {
        bail!("{} had no market slugs", manifest_path.display());
    }
    let slug_windows: BTreeMap<String, (i64, i64, i64)> = slugs
        .iter()
        .filter_map(|slug| recorded_btc_slug_window(slug).map(|w| (slug.clone(), w)))
        .collect();
    let min_open_s = slug_windows.values().map(|(open, _, _)| *open).min();
    let max_close_s = slug_windows.values().map(|(_, close, _)| *close).max();

    let gamma = data::gamma::GammaClient::new(&settings.poly_gamma_url);
    let refreshed_raw = gamma
        .fetch_raw_markets_by_slugs(&slugs, true)
        .await
        .context("fetch raw closed Gamma metadata for recorded BTC slugs")?;
    let refreshed: Vec<data::models::Market> = refreshed_raw
        .iter()
        .filter_map(data::gamma::parse_gamma_market)
        .collect();
    let refreshed_by_slug: BTreeMap<String, data::models::Market> = refreshed
        .into_iter()
        .map(|market| (market.slug.clone(), market))
        .collect();
    let resolution_source_by_slug: BTreeMap<String, serde_json::Value> = refreshed_raw
        .iter()
        .filter_map(recorded_gamma_resolution_source)
        .collect();

    let mut btc = backtest::btc_history::BTCHistory::new();
    let (btc_source, btc_rows) = if let Some(path) = btc_csv {
        let provenance = inspect_recorded_btc_csv_provenance(path)?;
        let rows = btc
            .load_csv(path)
            .with_context(|| format!("load BTC CSV {path}"))?;
        (
            serde_json::json!({
                "kind": "csv",
                "path": path,
                "provenance": provenance
            }),
            rows,
        )
    } else if let (Some(start_s), Some(end_s)) = (min_open_s, max_close_s) {
        let start_ms = (start_s * 1000).saturating_sub(5_000);
        let end_ms = (end_s * 1000).saturating_add(5_000);
        let rows_1s = btc
            .load_from_binance(start_ms, end_ms, "BTCUSDT", "1s")
            .await
            .unwrap_or(0);
        if rows_1s > 0 {
            (
                serde_json::json!({
                    "kind": "binance_public_klines",
                    "symbol": "BTCUSDT",
                    "interval": "1s",
                    "start_ms": start_ms,
                    "end_ms": end_ms
                }),
                rows_1s,
            )
        } else {
            let rows_1m = btc
                .load_from_binance(start_ms, end_ms, "BTCUSDT", "1m")
                .await
                .context("load Binance BTCUSDT klines")?;
            (
                serde_json::json!({
                    "kind": "binance_public_klines",
                    "symbol": "BTCUSDT",
                    "interval": "1m",
                    "start_ms": start_ms,
                    "end_ms": end_ms
                }),
                rows_1m,
            )
        }
    } else {
        (
            serde_json::json!({"kind": "none", "reason": "no parseable btc-updown slug windows"}),
            0,
        )
    };
    let btc_settlement_source_kind =
        recorded_btc_settlement_source_kind(&btc_source, settlement_source_kind);
    let coverage_start_ms = min_open_s.map(|value| value.saturating_mul(1000));
    let coverage_end_ms = max_close_s.map(|value| value.saturating_mul(1000));
    let btc_median_interval_ms = coverage_start_ms
        .zip(coverage_end_ms)
        .and_then(|(start, end)| btc.median_interval_ms(start, end));
    let btc_max_gap_ms = coverage_start_ms
        .zip(coverage_end_ms)
        .and_then(|(start, end)| btc.max_gap_ms(start, end));
    let btc_allowed_gap_ms = btc_median_interval_ms.map(|median| (median * 3).max(5_000));
    let btc_boundary_coverage_ready =
        coverage_start_ms
            .zip(coverage_end_ms)
            .is_some_and(|(start, end)| {
                let first = btc.first_timestamp_ms();
                let last = btc.last_timestamp_ms();
                first > 0 && first <= start && last >= end
            });
    let btc_internal_gap_ready = btc_max_gap_ms
        .zip(btc_allowed_gap_ms)
        .is_some_and(|(max_gap, allowed_gap)| max_gap <= allowed_gap);

    let mut rows = Vec::new();
    let mut refreshed_count = 0_u64;
    let mut closed_count = 0_u64;
    let mut terminal_count = 0_u64;
    let mut btc_tape_count = 0_u64;
    let mut oracle_checks = 0_u64;
    let mut oracle_disagreements = 0_u64;
    let mut oracle_ties = 0_u64;
    let mut official_source_known = 0_u64;
    let mut official_source_mismatches = 0_u64;
    let mut official_chainlink_sources = 0_u64;
    let mut official_source_kinds = BTreeSet::new();
    for original in original_markets.values() {
        let market = refreshed_by_slug.get(&original.slug).unwrap_or(original);
        if refreshed_by_slug.contains_key(&original.slug) {
            refreshed_count += 1;
        }
        let official_resolution_source = resolution_source_by_slug
            .get(&market.slug)
            .cloned()
            .unwrap_or_else(|| {
                serde_json::json!({
                    "kind": "unknown",
                    "resolution_source": null,
                    "description": null
                })
            });
        let official_source_kind = official_resolution_source
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        if official_source_kind != "unknown" {
            official_source_known += 1;
        }
        if official_source_kind == "chainlink_btc_usd_data_stream" {
            official_chainlink_sources += 1;
        }
        official_source_kinds.insert(official_source_kind.clone());
        let official_source_matches_btc_tape =
            recorded_settlement_source_matches(&official_source_kind, &btc_settlement_source_kind);
        if !official_source_matches_btc_tape {
            official_source_mismatches += 1;
        }
        if market.closed {
            closed_count += 1;
        }
        let terminal_direction = terminal_direction_from_market(market);
        if terminal_direction.is_some() {
            terminal_count += 1;
        }
        let (open_ts_s, close_ts_s, window_seconds) =
            slug_windows.get(&market.slug).copied().unwrap_or((0, 0, 0));
        let open_btc = if open_ts_s > 0 {
            btc.price_at_seconds(open_ts_s as f64)
        } else {
            0.0
        };
        let close_btc = if close_ts_s > 0 {
            btc.price_at_seconds(close_ts_s as f64)
        } else {
            0.0
        };
        let btc_direction = recorded_btc_direction(open_btc, close_btc);
        if open_btc > 0.0 && close_btc > 0.0 {
            btc_tape_count += 1;
        }
        let settlement_aligned = terminal_direction
            .as_deref()
            .zip(btc_direction.as_deref())
            .and_then(|(terminal, local)| {
                if local == "tie" {
                    Some(false)
                } else if terminal == "up" || terminal == "down" {
                    Some(terminal == local)
                } else {
                    None
                }
            });
        if let Some(aligned) = settlement_aligned {
            oracle_checks += 1;
            if btc_direction.as_deref() == Some("tie") {
                oracle_ties += 1;
            } else if !aligned {
                oracle_disagreements += 1;
            }
        }
        rows.push(serde_json::json!({
            "condition_id": market.condition_id,
            "slug": market.slug,
            "question": market.question,
            "event_title": market.event_title,
            "end_date": market.end_date,
            "open_ts_s": open_ts_s,
            "close_ts_s": close_ts_s,
            "window_seconds": window_seconds,
            "active": market.active,
            "closed": market.closed,
            "terminal_direction": terminal_direction,
            "btc_open": open_btc,
            "btc_close": close_btc,
            "btc_move": close_btc - open_btc,
            "btc_direction": btc_direction,
            "btc_settlement_source_kind": btc_settlement_source_kind,
            "official_resolution_source": official_resolution_source,
            "official_source_matches_btc_tape": official_source_matches_btc_tape,
            "settlement_aligned": settlement_aligned,
            "proxy_settlement_aligned": settlement_aligned,
            "outcomes": market.outcomes,
        }));
    }

    let total = rows.len() as u64;
    let resolution_ready = terminal_count == total;
    let btc_tape_ready =
        btc_tape_count == total && btc_boundary_coverage_ready && btc_internal_gap_ready;
    let official_source_ready = official_source_known == total && official_source_mismatches == 0;
    let proxy_btc_alignment_ready =
        resolution_ready && btc_tape_ready && oracle_disagreements == 0 && oracle_ties == 0;
    let settlement_alignment_ready = proxy_btc_alignment_ready && official_source_ready;
    let official_source_unknown = total.saturating_sub(official_source_known);
    let official_source_kinds: Vec<String> = official_source_kinds.into_iter().collect();
    let verdict = if !resolution_ready {
        "WAIT_FOR_TERMINAL_MARKETS"
    } else if !btc_boundary_coverage_ready {
        "BTC_TAPE_BOUNDARY_GAP"
    } else if !btc_internal_gap_ready {
        "BTC_TAPE_INTERNAL_GAP"
    } else if !btc_tape_ready {
        "BTC_TAPE_MISSING"
    } else if official_source_unknown > 0 {
        "OFFICIAL_SETTLEMENT_SOURCE_UNKNOWN"
    } else if official_source_mismatches > 0
        && official_chainlink_sources > 0
        && btc_settlement_source_kind != "chainlink_btc_usd_data_stream"
    {
        "OFFICIAL_CHAINLINK_TAPE_REQUIRED"
    } else if official_source_mismatches > 0 {
        "OFFICIAL_SETTLEMENT_SOURCE_MISMATCH"
    } else if oracle_ties > 0 {
        "BTC_TIE_NEEDS_REVIEW"
    } else if oracle_disagreements > 0 {
        "SETTLEMENT_DISAGREEMENT"
    } else {
        "FORWARD_GROUND_TRUTH_READY_NEEDS_SAMPLE_SIZE"
    };
    let summary = serde_json::json!({
        "schema_version": 1,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "source_manifest": manifest_path.display().to_string(),
        "selection": {
            "condition_ids": original_markets.values().map(|market| market.condition_id.clone()).collect::<Vec<_>>(),
            "captured_conditions": captured_condition_ids.len(),
            "selected_conditions": original_markets.len(),
        },
        "output": output_path.display().to_string(),
        "btc_tape": {
            "source": btc_source,
            "settlement_source_kind": btc_settlement_source_kind,
            "settlement_source_kind_input": settlement_source_kind,
            "rows": btc_rows,
            "first_timestamp_ms": btc.first_timestamp_ms(),
            "last_timestamp_ms": btc.last_timestamp_ms(),
            "required_start_ms": coverage_start_ms,
            "required_end_ms": coverage_end_ms,
            "median_interval_ms": btc_median_interval_ms,
            "max_internal_gap_ms": btc_max_gap_ms,
            "max_allowed_gap_ms": btc_allowed_gap_ms,
            "boundary_coverage_ready": btc_boundary_coverage_ready,
            "internal_gap_ready": btc_internal_gap_ready
        },
        "official_settlement_sources": {
            "kinds": official_source_kinds,
            "known": official_source_known,
            "unknown": official_source_unknown,
            "matched_to_btc_tape": total.saturating_sub(official_source_mismatches),
            "mismatched_to_btc_tape": official_source_mismatches,
            "chainlink_btc_usd_data_stream": official_chainlink_sources
        },
        "stats": {
            "markets": total,
            "gamma_refreshed": refreshed_count,
            "closed": closed_count,
            "terminal": terminal_count,
            "pending": total.saturating_sub(terminal_count),
            "btc_tape_covered": btc_tape_count,
            "btc_tape_missing": total.saturating_sub(btc_tape_count),
            "oracle_checks": oracle_checks,
            "oracle_disagreements": oracle_disagreements,
            "oracle_ties": oracle_ties,
            "proxy_oracle_checks": oracle_checks,
            "proxy_oracle_disagreements": oracle_disagreements,
            "proxy_oracle_ties": oracle_ties
        },
        "a_plus_gate": {
            "terminal_ground_truth_ready": resolution_ready,
            "resolution_manifest_ready": resolution_ready,
            "btc_tape_ready": btc_tape_ready,
            "official_settlement_source_ready": official_source_ready,
            "proxy_btc_alignment_ready": proxy_btc_alignment_ready,
            "settlement_alignment_ready": settlement_alignment_ready,
            "verdict": verdict
        },
        "markets": rows,
    });
    write_json_atomic(&output_path, &summary, true)
        .with_context(|| format!("write {}", output_path.display()))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).expect("serialize resolution manifest")
    );
    Ok(())
}

fn recorded_btc_slug_window(slug: &str) -> Option<(i64, i64, i64)> {
    let (prefix, window_s) = if let Some(prefix) = slug.strip_prefix("btc-updown-5m-") {
        (prefix, 300)
    } else if let Some(prefix) = slug.strip_prefix("btc-updown-15m-") {
        (prefix, 900)
    } else {
        return None;
    };
    let open_s = prefix.parse::<i64>().ok()?;
    Some((open_s, open_s + window_s, window_s))
}

fn recorded_btc_direction(open_btc: f64, close_btc: f64) -> Option<String> {
    if open_btc <= 0.0 || close_btc <= 0.0 {
        return None;
    }
    if (close_btc - open_btc).abs() <= f64::EPSILON {
        Some("tie".to_string())
    } else if close_btc > open_btc {
        Some("up".to_string())
    } else {
        Some("down".to_string())
    }
}

fn recorded_gamma_resolution_source(
    raw: &serde_json::Value,
) -> Option<(String, serde_json::Value)> {
    let slug = raw.get("slug").and_then(|v| v.as_str())?.to_string();
    let resolution_source = raw
        .get("resolutionSource")
        .or_else(|| raw.get("resolution_source"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let description = raw
        .get("description")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let kind = recorded_resolution_source_kind(
        resolution_source.as_deref().unwrap_or(""),
        description.as_deref().unwrap_or(""),
    );
    Some((
        slug,
        serde_json::json!({
            "kind": kind,
            "resolution_source": resolution_source,
            "description": description
        }),
    ))
}

fn recorded_resolution_source_kind(resolution_source: &str, description: &str) -> String {
    let text = format!("{resolution_source}\n{description}").to_ascii_lowercase();
    if text.contains("chain.link") || text.contains("chainlink") {
        if text.contains("btc") || text.contains("bitcoin") {
            return "chainlink_btc_usd_data_stream".to_string();
        }
        return "chainlink_data_stream".to_string();
    }
    if text.contains("binance") || text.contains("btcusdt") {
        return "binance_btcusdt_klines".to_string();
    }
    if text.trim().is_empty() {
        "unknown".to_string()
    } else {
        "other".to_string()
    }
}

fn inspect_recorded_btc_csv_provenance(path: &str) -> anyhow::Result<serde_json::Value> {
    use anyhow::Context;
    use std::collections::BTreeSet;

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("open BTC provenance CSV {path}"))?;
    let headers = reader.headers()?.clone();
    let source_index = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case("source"));
    let mut source_values = BTreeSet::new();
    let mut rows = 0_u64;
    let mut malformed_rows = 0_u64;
    for record in reader.records() {
        let record = match record {
            Ok(record) => record,
            Err(_) => {
                malformed_rows += 1;
                continue;
            }
        };
        rows += 1;
        if let Some(index) = source_index {
            if let Some(source) = record.get(index) {
                let source = source.trim().to_ascii_lowercase();
                if !source.is_empty() {
                    source_values.insert(source);
                }
            }
        }
    }
    let detected_settlement_source_kind =
        backtest::btc_history::classify_btc_source_values(&source_values);

    Ok(serde_json::json!({
        "source_column_present": source_index.is_some(),
        "source_values": source_values,
        "rows": rows,
        "malformed_rows": malformed_rows,
        "detected_settlement_source_kind": detected_settlement_source_kind,
        "official_chainlink_provenance_ready": detected_settlement_source_kind == "chainlink_btc_usd_data_stream"
            && malformed_rows == 0
            && rows > 0
    }))
}

fn recorded_btc_settlement_source_kind(
    btc_source: &serde_json::Value,
    settlement_source_kind: &str,
) -> String {
    let declared = settlement_source_kind.trim().to_ascii_lowercase();
    let inferred = match btc_source.get("kind").and_then(|v| v.as_str()) {
        Some("binance_public_klines") => "binance_btcusdt_klines".to_string(),
        Some("csv") => btc_source
            .pointer("/provenance/detected_settlement_source_kind")
            .and_then(|value| value.as_str())
            .unwrap_or("csv_unclassified")
            .to_string(),
        Some("none") => "none".to_string(),
        Some(other) if !other.trim().is_empty() => other.trim().to_ascii_lowercase(),
        _ => "unknown".to_string(),
    };
    if declared.is_empty() || declared == "auto" {
        return inferred;
    }
    if btc_source.get("kind").and_then(|value| value.as_str()) == Some("csv")
        && declared != inferred
    {
        return "csv_source_claim_mismatch".to_string();
    }
    declared
}

fn recorded_settlement_source_matches(official_kind: &str, btc_tape_kind: &str) -> bool {
    let official = official_kind.trim();
    let tape = btc_tape_kind.trim();
    !official.is_empty() && official != "unknown" && official == tape
}

fn terminal_direction_from_market(market: &data::models::Market) -> Option<String> {
    if !market.closed {
        return None;
    }
    let mut winners = market
        .outcomes
        .iter()
        .filter(|outcome| outcome.price >= 0.999)
        .collect::<Vec<_>>();
    if winners.len() != 1 {
        return None;
    }
    let winner = winners.pop()?;
    let name = winner.name.trim().to_ascii_lowercase();
    match name.as_str() {
        "up" | "yes" => Some("up".to_string()),
        "down" | "no" => Some("down".to_string()),
        _ => Some(name),
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
    let mut client = match clob::ClobClient::new(
        &s.poly_base_url,
        &s.poly_api_key,
        &s.poly_api_secret,
        &s.poly_api_passphrase,
    ) {
        Ok(client) => client,
        Err(error) => {
            eprintln!("CLOB client initialization failed: {error}");
            std::process::exit(2);
        }
    };
    if !s.private_key.is_empty() {
        if let Err(error) = client.set_signing_key(&s.private_key) {
            eprintln!("CLOB signing key configuration failed: {error}");
            std::process::exit(2);
        }
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
            output,
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
            if let Some(output) = output {
                if let Err(e) =
                    backtest::experiment::write_zone_concentration_audit_atomic(&output, &audit)
                {
                    eprintln!("write zone concentration audit failed: {e}");
                    std::process::exit(1);
                }
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "output": output,
                        "pass": audit.pass,
                        "variant_name": audit.variant_name,
                        "params_hash": audit.params_hash,
                        "dominant_zone": audit.total.dominant_zone,
                        "dominant_zone_trade_share": audit.total.dominant_zone_trade_share,
                        "rejections": audit.rejections,
                    }))
                    .expect("serialize zone concentration audit summary")
                );
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&audit)
                        .expect("serialize zone concentration audit")
                );
            }
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
    match monitoring::replay_validation::validate_replay(path) {
        Ok(summary) => {
            println!(
                "validate-replay: total={} mismatches={} ({:.2}%)",
                summary.total, summary.mismatches, summary.mismatch_pct
            );
            if summary.mismatches > 0 {
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("validate-replay failed: {error:#}");
            std::process::exit(1);
        }
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
    match try_parse_csv_floats(s) {
        Ok(values) => values,
        Err(error) => {
            eprintln!("invalid floating-point list `{s}`: {error}");
            std::process::exit(2);
        }
    }
}

fn parse_csv_u64s(s: &str) -> Vec<u64> {
    match try_parse_csv_u64s(s) {
        Ok(values) => values,
        Err(error) => {
            eprintln!("invalid unsigned-integer list `{s}`: {error}");
            std::process::exit(2);
        }
    }
}

fn try_parse_csv_floats(s: &str) -> anyhow::Result<Vec<f64>> {
    s.split(',')
        .map(|part| {
            let raw = part.trim();
            if raw.is_empty() {
                anyhow::bail!("empty value");
            }
            let value = raw
                .parse::<f64>()
                .with_context(|| format!("`{raw}` is not a number"))?;
            if !value.is_finite() {
                anyhow::bail!("`{raw}` must be finite");
            }
            Ok(value)
        })
        .collect()
}

fn try_parse_csv_u64s(s: &str) -> anyhow::Result<Vec<u64>> {
    s.split(',')
        .map(|part| {
            let raw = part.trim();
            if raw.is_empty() {
                anyhow::bail!("empty value");
            }
            raw.parse::<u64>()
                .with_context(|| format!("`{raw}` is not an unsigned integer"))
        })
        .collect()
}

fn parse_selectivity_filter(
    require: &[String],
    deny: &[String],
) -> anyhow::Result<backtest::strategies::SelectivityFilter> {
    let (require_tags, require_tag_values) =
        parse_causal_tag_maps(require, "--require-causal-tag")?;
    let (deny_tags, deny_tag_values) = parse_causal_tag_maps(deny, "--deny-causal-tag")?;
    Ok(backtest::strategies::SelectivityFilter {
        require_tags,
        deny_tags,
        require_tag_values,
        deny_tag_values,
    })
}

type CausalTagMaps = (
    std::collections::BTreeMap<String, String>,
    std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
);

const SETTLEMENT_ANCHOR_BASELINE_NAME: &str = "primary_v6_volfloor_300";
const SETTLEMENT_ANCHOR_BASELINE_HASH: &str =
    "a5d67641653ae85a853aab531060a240eade257e32fd5bf0e46392c7934302d5";

#[allow(clippy::too_many_arguments)]
fn settlement_anchor_preflight_error(
    enabled: bool,
    variant_json_present: bool,
    condition_ids_present: bool,
    allocation_lock_present: bool,
    source_audit_present: bool,
    continuous: bool,
    adaptive_rearm_enabled: bool,
    report_json_present: bool,
    trades_json_present: bool,
) -> Option<&'static str> {
    if !enabled {
        return None;
    }
    if !variant_json_present {
        return Some(
            "--fair-value-btc-csv requires the frozen --variant-json; grid search is forbidden",
        );
    }
    if !condition_ids_present {
        return Some(
            "--fair-value-btc-csv requires an explicit non-empty --condition-id allowlist",
        );
    }
    if !allocation_lock_present {
        return Some("--fair-value-btc-csv requires --settlement-anchor-allocation-lock");
    }
    if !source_audit_present {
        return Some("--fair-value-btc-csv requires --settlement-anchor-source-audit");
    }
    if !continuous {
        return Some("--fair-value-btc-csv requires --continuous stateful replay");
    }
    if adaptive_rearm_enabled {
        return Some("--fair-value-btc-csv forbids adaptive health rearm");
    }
    if !report_json_present || !trades_json_present {
        return Some(
            "--fair-value-btc-csv requires both --report-json and --trades-json evidence outputs",
        );
    }
    None
}

fn settlement_anchor_variant_error(
    variants: &[backtest::strategies::StrategyVariant],
) -> Option<String> {
    if variants.len() != 1 {
        return Some(format!(
            "--fair-value-btc-csv requires exactly one frozen variant; found {}",
            variants.len()
        ));
    }
    let variant = &variants[0];
    let hash = strategy::spec::stable_json_hash(variant);
    if variant.name != SETTLEMENT_ANCHOR_BASELINE_NAME || hash != SETTLEMENT_ANCHOR_BASELINE_HASH {
        return Some(format!(
            "--fair-value-btc-csv requires {SETTLEMENT_ANCHOR_BASELINE_NAME} hash {SETTLEMENT_ANCHOR_BASELINE_HASH}; found {} hash {hash}",
            variant.name
        ));
    }
    None
}

fn attach_settlement_anchor_allocation_evidence(
    report: &mut backtest::experiment::ExperimentReport,
    evidence: &backtest::allocation_lock::SettlementAnchorAllocationEvidence,
) {
    let mut source = data::manifest::DataSourceManifest::new(
        "settlement_anchor_allocation_lock",
        "forward_condition_allocation",
    );
    source.path = Some(evidence.path.clone());
    source.row_count = Some(evidence.condition_count as u64);
    source.checksum_sha256 = Some(evidence.sha256.clone());
    source.complete = true;
    source.metadata.insert(
        "mechanism_id".to_string(),
        "settlement_source_anchor_v1".to_string(),
    );
    source
        .metadata
        .insert("block_id".to_string(), evidence.block_id.clone());
    source.metadata.insert(
        "block_sequence".to_string(),
        evidence.block_sequence.to_string(),
    );
    source.metadata.insert(
        "allocation_boundary".to_string(),
        evidence.allocation_boundary.as_str().to_string(),
    );
    source.metadata.insert(
        "condition_ids_hash".to_string(),
        evidence.condition_ids_hash.clone(),
    );
    source.metadata.insert(
        "report_count".to_string(),
        evidence.report_count.to_string(),
    );
    source.metadata.insert(
        "report_partition_hash".to_string(),
        evidence.report_partition_hash.clone(),
    );
    source.metadata.insert(
        "candidate_condition_set_sha256".to_string(),
        evidence.candidate_condition_set_sha256.clone(),
    );
    source.metadata.insert(
        "prior_condition_set_sha256".to_string(),
        evidence.prior_condition_set_sha256.join(","),
    );
    source.metadata.insert(
        "preregistration_sha256".to_string(),
        evidence.preregistration_sha256.clone(),
    );
    source.metadata.insert(
        "frozen_variant_sha256".to_string(),
        evidence.frozen_variant_sha256.clone(),
    );
    source.metadata.insert(
        "frozen_variant_params_hash".to_string(),
        evidence.frozen_variant_params_hash.clone(),
    );
    source.metadata.insert(
        "report_output".to_string(),
        evidence.score_outputs.report_json.clone(),
    );
    source.metadata.insert(
        "trades_output".to_string(),
        evidence.score_outputs.trades_json.clone(),
    );
    source.metadata.insert(
        "pair_audit_output".to_string(),
        evidence.score_outputs.pair_audit_json.clone(),
    );
    report.data_manifest.sources.push(source);
    report.data_manifest.complete = report
        .data_manifest
        .sources
        .iter()
        .all(|source| source.complete);
    report.data_manifest.manifest_hash = report.data_manifest.compute_hash();
}

fn attach_settlement_anchor_source_evidence(
    report: &mut backtest::experiment::ExperimentReport,
    evidence: &backtest::settlement_anchor::SettlementAnchorSourceEvidence,
) {
    let mut source = data::manifest::DataSourceManifest::new(
        "settlement_anchor_source_audit",
        "official_anchor_coverage",
    );
    source.path = Some(evidence.path.clone());
    source.row_count = Some(evidence.condition_count as u64);
    source.checksum_sha256 = Some(evidence.sha256.clone());
    source.complete = true;
    source.metadata.insert(
        "condition_set_sha256".to_string(),
        evidence.condition_set_sha256.clone(),
    );
    source.metadata.insert(
        "fair_value_btc_csv_sha256".to_string(),
        evidence.fair_value_btc_csv_sha256.clone(),
    );
    source.metadata.insert(
        "report_count".to_string(),
        evidence.report_count.to_string(),
    );
    source.metadata.insert(
        "source_covered_conditions".to_string(),
        evidence.source_covered_conditions.to_string(),
    );
    source.metadata.insert(
        "official_anchor_coverage".to_string(),
        format!("{:.12}", evidence.official_anchor_coverage),
    );
    source.metadata.insert(
        "maximum_published_price_difference_usd".to_string(),
        evidence
            .maximum_published_price_difference_usd
            .map(|value| format!("{value:.12}"))
            .unwrap_or_else(|| "none".to_string()),
    );
    report.data_manifest.sources.push(source);
    report.data_manifest.complete = report
        .data_manifest
        .sources
        .iter()
        .all(|source| source.complete);
    report.data_manifest.manifest_hash = report.data_manifest.compute_hash();
}

fn attach_input_file_evidence(
    report: &mut backtest::experiment::ExperimentReport,
    source_name: &str,
    path: &str,
) -> anyhow::Result<()> {
    let checksum = sha256_file(std::path::Path::new(path))?;
    let source = report
        .data_manifest
        .sources
        .iter_mut()
        .find(|source| source.name == source_name)
        .with_context(|| format!("report is missing {source_name} source manifest"))?;
    source.path = Some(path.to_string());
    source.checksum_sha256 = Some(checksum);
    report.data_manifest.manifest_hash = report.data_manifest.compute_hash();
    Ok(())
}

fn sha256_file(path: &std::path::Path) -> anyhow::Result<String> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)
        .with_context(|| format!("open replay input {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("hash replay input {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn attach_pmxt_input_evidence(
    report: &mut backtest::experiment::ExperimentReport,
    cfg: &backtest::harness::HarnessConfig,
) -> anyhow::Result<()> {
    let loader = backtest::pmxt::PMXTv2Loader::new(&cfg.cache_dir);
    let mut pinned = std::collections::BTreeMap::new();
    for hour in &cfg.hours {
        let condition_ids = cfg.universe.condition_id_set_for_hour(*hour);
        let sidecar = loader.sidecar_path_for_conditions(*hour, &condition_ids);
        let shared = cfg
            .shared_distilled_dir
            .as_ref()
            .map(|dir| backtest::distill::shared_cache_path_for_hour(dir, *hour));
        let mut found_compact_input = false;
        for path in shared.iter().chain(std::iter::once(&sidecar)) {
            if path.is_file() {
                pinned.insert(path.display().to_string(), sha256_file(path)?);
                found_compact_input = true;
            }
        }
        if !found_compact_input {
            let parquet = loader.cache_path_for_hour(*hour);
            if !parquet.is_file() {
                anyhow::bail!(
                    "no PMXT replay artifact remained for hour {} after replay",
                    hour
                );
            }
            pinned.insert(parquet.display().to_string(), sha256_file(&parquet)?);
        }
    }
    let artifacts: Vec<_> = pinned
        .into_iter()
        .map(|(path, sha256)| backtest::allocation_lock::HashedArtifact { path, sha256 })
        .collect();
    if artifacts.is_empty() {
        anyhow::bail!("PMXT replay input evidence is empty");
    }
    let aggregate_hash = strategy::spec::stable_json_hash(&artifacts);
    let source = report
        .data_manifest
        .sources
        .iter_mut()
        .find(|source| source.name == "pmxt_v2_archive")
        .context("report is missing pmxt_v2_archive source manifest")?;
    source.checksum_sha256 = Some(aggregate_hash.clone());
    source.metadata.insert(
        "input_artifact_count".to_string(),
        artifacts.len().to_string(),
    );
    source
        .metadata
        .insert("input_artifacts_hash".to_string(), aggregate_hash);
    source.metadata.insert(
        "input_artifacts_json".to_string(),
        serde_json::to_string(&artifacts).context("serialize PMXT input evidence")?,
    );
    report.data_manifest.manifest_hash = report.data_manifest.compute_hash();
    Ok(())
}

fn parse_causal_tag_maps(args: &[String], flag: &str) -> anyhow::Result<CausalTagMaps> {
    let mut values: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        std::collections::BTreeMap::new();
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
            values
                .entry(dimension.to_string())
                .or_default()
                .insert(value.to_string());
        }
    }
    let mut singles = std::collections::BTreeMap::new();
    let mut multi = std::collections::BTreeMap::new();
    for (dimension, set) in values {
        if set.len() == 1 {
            let value = set.into_iter().next().expect("single value exists");
            singles.insert(dimension, value);
        } else {
            multi.insert(dimension, set);
        }
    }
    Ok((singles, multi))
}

fn btc_required_range_ms(
    universe: &backtest::harness::CandleUniverse,
    fallback_start_ms: i64,
    fallback_end_ms: i64,
) -> (i64, i64) {
    let mut range: Option<(i64, i64)> = None;
    for contract in &universe.contracts {
        let Ok(close) = chrono::DateTime::parse_from_rfc3339(&contract.end_date) else {
            continue;
        };
        let minutes = live::window::estimate_window_minutes(&contract.window_description);
        let minutes = if minutes > 0.0 { minutes } else { 60.0 };
        let close_ms = close.timestamp_millis();
        let open_ms = close_ms - (minutes * 60_000.0).round() as i64;
        range = Some(match range {
            Some((start_ms, end_ms)) => (start_ms.min(open_ms), end_ms.max(close_ms)),
            None => (open_ms, close_ms),
        });
    }
    range.unwrap_or((fallback_start_ms, fallback_end_ms))
}

fn ensure_settlement_btc_history_covers_universe(
    label: &str,
    btc: &backtest::btc_history::BTCHistory,
    universe: &backtest::harness::CandleUniverse,
) {
    for contract in &universe.contracts {
        let Ok(close) = chrono::DateTime::parse_from_rfc3339(&contract.end_date) else {
            continue;
        };
        let minutes = live::window::estimate_window_minutes(&contract.window_description);
        let minutes = if minutes > 0.0 { minutes } else { 60.0 };
        let close_ms = close.timestamp_millis();
        let open_ms = close_ms - (minutes * 60_000.0).round() as i64;
        if let Some(message) = btc_history_coverage_error(label, btc, open_ms, close_ms) {
            eprintln!("{message}");
            std::process::exit(1);
        }
    }
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
    if let Some(message) = btc_history_bounds_error(label, btc, required_start_ms, required_end_ms)
    {
        return Some(message);
    }
    if let (Some(median_gap_ms), Some(max_gap_ms)) = (
        btc.median_interval_ms(required_start_ms, required_end_ms),
        btc.max_gap_ms(required_start_ms, required_end_ms),
    ) {
        let allowed_gap_ms = 5_000_i64.max(median_gap_ms.saturating_mul(3));
        if max_gap_ms > allowed_gap_ms {
            return Some(format!(
                "{label}: BTC tape has an internal gap of {max_gap_ms} ms; median cadence is {median_gap_ms} ms and the fail-closed limit is {allowed_gap_ms} ms"
            ));
        }
    }
    None
}

fn btc_history_bounds_error(
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

async fn fetch_gamma_historical_markets_for_window(
    gamma: &data::gamma::GammaClient,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    window_minutes: Option<f64>,
    label: &str,
) -> anyhow::Result<Vec<data::models::Market>> {
    if let Some(step_s) = window_minutes.and_then(live::window::btc_updown_slug_step_seconds) {
        let slugs = live::window::btc_updown_slugs_for_range(start, end, step_s);
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
fn inclusive_replay_hours(
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
) -> Vec<chrono::DateTime<chrono::Utc>> {
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};

    if end < start {
        return Vec::new();
    }
    let start_hour_s = start.timestamp().div_euclid(3600) * 3600;
    let end_hour_s = end.timestamp().div_euclid(3600) * 3600;
    let Some(mut current) = Utc.timestamp_opt(start_hour_s, 0).single() else {
        return Vec::new();
    };
    let Some(last) = Utc.timestamp_opt(end_hour_s, 0).single() else {
        return Vec::new();
    };
    let mut hours = Vec::new();
    while current <= last {
        hours.push(current);
        current += ChronoDuration::hours(1);
    }
    hours
}

#[allow(clippy::too_many_arguments)]
async fn cmd_harness_sweep(
    settings: &config::Settings,
    start: &str,
    end: Option<&str>,
    bankroll: f64,
    cache_dir: Option<&str>,
    pin_input_artifacts: bool,
    require_shared_distilled: bool,
    variant_json: Option<&str>,
    condition_ids: Vec<String>,
    btc_csv: Option<&str>,
    settlement_btc_csv: Option<&str>,
    fair_value_btc_csv: Option<&str>,
    settlement_anchor_allocation_lock: Option<&str>,
    settlement_anchor_source_audit: Option<&str>,
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
    calibration_opportunities_json: Option<&str>,
    require_causal_tag: Vec<String>,
    deny_causal_tag: Vec<String>,
    window_minutes: Option<f64>,
    adaptive_health_rearm_minutes: f64,
    continuous: bool,
    atomic_parquet: bool,
) {
    use chrono::{DateTime, Utc};

    let start_dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(start) {
        Ok(d) => d.with_timezone(&Utc),
        Err(e) => {
            eprintln!("--start must be RFC3339: {e}");
            std::process::exit(2);
        }
    };
    if calibration_opportunities_json.is_some() && !continuous {
        eprintln!("--calibration-opportunities-json requires --continuous");
        std::process::exit(2);
    }
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
    let hours = inclusive_replay_hours(start_dt, end_dt);

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
    if let Some(error) = settlement_anchor_preflight_error(
        fair_value_btc_csv.is_some(),
        variant_json.is_some(),
        !condition_ids.is_empty(),
        settlement_anchor_allocation_lock.is_some(),
        settlement_anchor_source_audit.is_some(),
        continuous,
        adaptive_rearm_after_s.is_some(),
        report_json.is_some(),
        trades_json.is_some(),
    ) {
        eprintln!("{error}");
        std::process::exit(2);
    }
    if fair_value_btc_csv.is_none() && settlement_anchor_allocation_lock.is_some() {
        eprintln!("--settlement-anchor-allocation-lock requires --fair-value-btc-csv");
        std::process::exit(2);
    }
    if fair_value_btc_csv.is_none() && settlement_anchor_source_audit.is_some() {
        eprintln!("--settlement-anchor-source-audit requires --fair-value-btc-csv");
        std::process::exit(2);
    }
    if fair_value_btc_csv.is_some() && (btc_csv.is_none() || settlement_btc_csv.is_none()) {
        eprintln!(
            "--fair-value-btc-csv requires explicit --btc-csv and --settlement-btc-csv so every paired input is hash-pinned"
        );
        std::process::exit(2);
    }
    if fair_value_btc_csv.is_some() && !pin_input_artifacts {
        eprintln!("--fair-value-btc-csv requires --pin-input-artifacts");
        std::process::exit(2);
    }
    let selectivity = match parse_selectivity_filter(&require_causal_tag, &deny_causal_tag) {
        Ok(filter) => filter,
        Err(e) => {
            eprintln!("causal selectivity parse failed: {e:#}");
            std::process::exit(2);
        }
    };

    let variants = if let Some(path) = variant_json {
        if !require_causal_tag.is_empty() || !deny_causal_tag.is_empty() {
            eprintln!(
                "--variant-json cannot be combined with --require-causal-tag or --deny-causal-tag"
            );
            std::process::exit(2);
        }
        match backtest::variant_io::read_variants(path) {
            Ok(variants) => variants,
            Err(e) => {
                eprintln!("load --variant-json {path}: {e:#}");
                std::process::exit(2);
            }
        }
    } else {
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
        grid.variants()
    };
    if variants.is_empty() {
        eprintln!("empty parameter grid (check --conf/--z/--edge/--ev-buffer)");
        std::process::exit(2);
    }
    if fair_value_btc_csv.is_some() {
        if let Some(error) = settlement_anchor_variant_error(&variants) {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
    let settlement_anchor_allocation = if fair_value_btc_csv.is_some() {
        let lock_path = settlement_anchor_allocation_lock
            .expect("settlement-anchor preflight requires allocation lock");
        let report_path = report_json.expect("settlement-anchor preflight requires report output");
        let trades_path = trades_json.expect("settlement-anchor preflight requires trade output");
        if let Some(error) = backtest::allocation_lock::settlement_anchor_output_paths_error(
            report_path,
            trades_path,
        ) {
            eprintln!("{error}");
            std::process::exit(2);
        }
        match backtest::allocation_lock::validate_settlement_anchor_allocation_lock(
            lock_path,
            &condition_ids,
            report_path,
            trades_path,
        ) {
            Ok(evidence) => Some(evidence),
            Err(error) => {
                eprintln!("settlement-anchor allocation lock rejected: {error:#}");
                std::process::exit(2);
            }
        }
    } else {
        None
    };
    let settlement_anchor_source = if let (Some(fair_value_path), Some(allocation)) =
        (fair_value_btc_csv, settlement_anchor_allocation.as_ref())
    {
        let audit_path = settlement_anchor_source_audit
            .expect("settlement-anchor preflight requires source audit");
        match backtest::settlement_anchor::validate_settlement_anchor_source_audit(
            audit_path,
            &allocation.candidate_condition_set_sha256,
            fair_value_path,
        ) {
            Ok(evidence) => Some(evidence),
            Err(error) => {
                eprintln!("settlement-anchor source audit rejected: {error:#}");
                std::process::exit(2);
            }
        }
    } else {
        None
    };
    tracing::info!(
        variants = variants.len(),
        source = if variant_json.is_some() {
            "variant_json"
        } else {
            "grid"
        },
        "sweep variants built"
    );

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
    if !condition_ids.is_empty() {
        let requested = condition_ids
            .iter()
            .map(|condition_id| condition_id.trim())
            .filter(|condition_id| !condition_id.is_empty())
            .map(str::to_string)
            .collect::<std::collections::BTreeSet<_>>();
        if requested.is_empty() {
            eprintln!("--condition-id did not contain any non-empty IDs");
            std::process::exit(2);
        }
        let available = contracts
            .iter()
            .map(|contract| contract.market.condition_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let unknown = requested
            .difference(&available)
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            eprintln!(
                "--condition-id values are absent from the selected replay window: {}",
                unknown.join(",")
            );
            std::process::exit(2);
        }
        contracts.retain(|contract| requested.contains(&contract.market.condition_id));
    }
    let universe = backtest::harness::CandleUniverse { contracts };
    if universe.contracts.is_empty() {
        eprintln!("no candle contracts in archive window");
        std::process::exit(1);
    }
    tracing::info!(
        contracts = universe.contracts.len(),
        "harness universe loaded"
    );
    let (settlement_required_start_ms, settlement_required_end_ms) = btc_required_range_ms(
        &universe,
        start_dt.timestamp_millis(),
        end_dt.timestamp_millis() + 3_600_000,
    );
    let signal_required_start_ms = settlement_required_start_ms - 3_600_000;
    let signal_required_end_ms = settlement_required_end_ms;

    // The signal tape needs a full causal hour before the first selected candle
    // so realized volatility cannot silently fall back to its sparse-data default.
    let mut btc = backtest::btc_history::BTCHistory::new();
    if let Some(p) = btc_csv {
        if let Err(e) = btc.load_csv(p) {
            eprintln!("BTC CSV load failed: {e}");
            std::process::exit(1);
        }
    } else {
        let start_ms = signal_required_start_ms;
        let end_ms = signal_required_end_ms;
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
        "harness-sweep signal",
        &btc,
        signal_required_start_ms,
        signal_required_end_ms,
    );

    let mut settlement_btc = btc.clone();
    if let Some(path) = settlement_btc_csv {
        settlement_btc = backtest::btc_history::BTCHistory::new();
        if let Err(e) = settlement_btc.load_csv(path) {
            eprintln!("settlement BTC CSV load failed: {e}");
            std::process::exit(1);
        }
    }
    ensure_settlement_btc_history_covers_universe(
        "harness-sweep settlement",
        &settlement_btc,
        &universe,
    );
    let btc = std::sync::Arc::new(btc);
    let settlement_btc = std::sync::Arc::new(settlement_btc);
    let fair_value_btc = if let Some(path) = fair_value_btc_csv {
        let fair_value_btc = if settlement_btc_csv == Some(path) {
            std::sync::Arc::clone(&settlement_btc)
        } else {
            let mut history = backtest::btc_history::BTCHistory::new();
            if let Err(e) = history.load_csv(path) {
                eprintln!("fair-value BTC CSV load failed: {e}");
                std::process::exit(1);
            }
            std::sync::Arc::new(history)
        };
        if fair_value_btc.source_kind() != "chainlink_btc_usd_data_stream" {
            eprintln!(
                "--fair-value-btc-csv must contain only official Chainlink BTC/USD rows; detected {}",
                fair_value_btc.source_kind()
            );
            std::process::exit(2);
        }
        if let Some(message) = btc_history_bounds_error(
            "harness-sweep fair value",
            &fair_value_btc,
            settlement_required_start_ms,
            settlement_required_end_ms,
        ) {
            eprintln!("{message}");
            std::process::exit(1);
        }
        Some(fair_value_btc)
    } else {
        None
    };

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
        btc_history: btc,
        fair_value_btc_history: fair_value_btc,
        settlement_btc_history: settlement_btc,
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
        require_shared_distilled,
        threads: if threads == 0 { None } else { Some(threads) },
        checkpoint_dir: checkpoint_dir.clone(),
        stop_flag: Some(stop_flag.clone()),
        continuous,
        capture_calibration_opportunities: calibration_opportunities_json.is_some(),
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
                let mut report = backtest::experiment::ExperimentReport::from_harness(
                    "harness_sweep",
                    &cfg,
                    &runs,
                );
                if pin_input_artifacts {
                    if let Err(error) = attach_pmxt_input_evidence(&mut report, &cfg) {
                        eprintln!("attach PMXT input evidence: {error:#}");
                        std::process::exit(1);
                    }
                }
                for (source_name, source_path) in [
                    ("btc_price_tape", btc_csv),
                    ("btc_settlement_price_tape", settlement_btc_csv),
                    ("btc_fair_value_price_tape", fair_value_btc_csv),
                ] {
                    if let Some(source_path) = source_path {
                        if let Err(error) =
                            attach_input_file_evidence(&mut report, source_name, source_path)
                        {
                            eprintln!("attach {source_name} evidence: {error:#}");
                            std::process::exit(1);
                        }
                    }
                }
                if let Some(evidence) = settlement_anchor_allocation.as_ref() {
                    attach_settlement_anchor_allocation_evidence(&mut report, evidence);
                }
                if let Some(evidence) = settlement_anchor_source.as_ref() {
                    attach_settlement_anchor_source_evidence(&mut report, evidence);
                }
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
                    "settlement_anchor_allocation": settlement_anchor_allocation.as_ref(),
                    "settlement_anchor_source": settlement_anchor_source.as_ref(),
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
            if let Some(path) = calibration_opportunities_json {
                let mut rows = Vec::new();
                let mut condition_ids = std::collections::BTreeSet::new();
                for (variant_index, run) in runs.iter().enumerate() {
                    let params_hash = strategy::spec::stable_json_hash(&run.variant);
                    for (opportunity_index, opportunity) in
                        run.calibration_opportunities.iter().enumerate()
                    {
                        condition_ids.insert(opportunity.condition_id.clone());
                        rows.push(serde_json::json!({
                            "variant_index": variant_index,
                            "opportunity_index": opportunity_index,
                            "strategy_name": &run.variant.name,
                            "params_hash": &params_hash,
                            "risk_profile": run.variant.risk_profile(),
                            "opportunity": opportunity,
                        }));
                    }
                }
                if rows.is_empty() {
                    eprintln!(
                        "write calibration opportunity report {path}: no pre-edge opportunities were captured"
                    );
                    std::process::exit(1);
                }
                let experiment = backtest::experiment::ExperimentReport::from_harness(
                    "harness_sweep_calibration_opportunities",
                    &cfg,
                    &runs,
                );
                let report = serde_json::json!({
                    "schema_version": 1,
                    "generated_at": chrono::Utc::now().to_rfc3339(),
                    "mode": "harness_sweep_calibration_opportunities",
                    "start": start_dt.to_rfc3339(),
                    "end": end_dt.to_rfc3339(),
                    "bankroll_usd": cfg.bankroll_usd,
                    "max_total_exposure_usd": cfg.max_total_exposure_usd,
                    "latency_ms": cfg.latency.insert_ms,
                    "window_minutes": window_minutes,
                    "continuous": continuous,
                    "sampling": "first_pre_edge_candidate_per_condition_utc_second",
                    "row_count": rows.len(),
                    "condition_count": condition_ids.len(),
                    "variant_count": runs.len(),
                    "data_manifest": experiment.data_manifest,
                    "notes": [
                        "Rows are captured only after all non-edge strategy gates pass and before the final EV, stale-edge, and minimum-edge checks.",
                        "The capture run fails if its variant submits any trade, preventing first-trade state from truncating later counterfactual opportunities.",
                        "Repeated seconds from one condition share one terminal label and must be condition-weighted during fitting and scoring.",
                        "Chosen-token logit changes require a midpoint no more than two seconds old and at least 80 percent causal coverage of the requested 5, 30, or 60 second horizon; invalid probabilities are not clamped.",
                        "BTC returns are direction-aligned log returns in basis points over the same fixed horizons; missing path features must fail closed and must not be imputed.",
                        "Binary-complement residuals use the simultaneous causal state of both outcome books: chosen midpoint plus opposite midpoint minus one, and the corresponding three-level depth-weighted microprice sum minus one. Missing or invalid paired books are null and must fail closed.",
                        "Fold boundaries are chronological; never shuffle rows or fit on the scored fold."
                    ],
                    "rows": rows,
                });
                if let Err(e) = write_json_atomic(path, &report, false) {
                    eprintln!("write calibration opportunity report {path}: {e}");
                    std::process::exit(1);
                }
                println!("Calibration opportunity report: {path}");
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
    let cache_covers_window =
        cached_gamma_covers_window(&cached_markets, start_dt, end_dt, window_minutes);
    if allow_gamma_fetch && !cache_covers_window {
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
            "harness: using cached Gamma metadata from {} (requested window covered={cache_covers_window})",
            gamma_cache_path.display(),
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
        settlement_btc_history: std::sync::Arc::new(btc.clone()),
        fair_value_btc_history: None,
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
        require_shared_distilled: false,
        threads: if threads == 0 { None } else { Some(threads) },
        checkpoint_dir: checkpoint_dir.clone(),
        stop_flag: Some(stop_flag),
        continuous,
        capture_calibration_opportunities: false,
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

#[cfg(test)]
mod replay_validation_tests {
    use super::*;

    #[test]
    fn harness_sweep_parses_exact_condition_allowlist() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let cli = Cli::try_parse_from([
                    "polymomentum-engine",
                    "harness-sweep",
                    "--start",
                    "2026-07-18T08:10:00Z",
                    "--condition-id",
                    "first,second",
                    "--condition-id",
                    "third",
                ])
                .unwrap();

                let Command::HarnessSweep { condition_ids, .. } = cli.command else {
                    panic!("unexpected command");
                };
                assert_eq!(condition_ids, ["first", "second", "third"]);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn harness_sweep_parses_separate_signal_fair_and_settlement_tapes() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let cli = Cli::try_parse_from([
                    "polymomentum-engine",
                    "harness-sweep",
                    "--start",
                    "2026-07-18T08:10:00Z",
                    "--btc-csv",
                    "/tmp/binance.csv",
                    "--fair-value-btc-csv",
                    "/tmp/chainlink.csv",
                    "--pin-input-artifacts",
                    "--settlement-anchor-allocation-lock",
                    "/tmp/allocation-lock.json",
                    "--settlement-anchor-source-audit",
                    "/tmp/source-audit.json",
                    "--settlement-btc-csv",
                    "/tmp/chainlink.csv",
                ])
                .unwrap();

                let Command::HarnessSweep {
                    btc_csv,
                    fair_value_btc_csv,
                    pin_input_artifacts,
                    settlement_anchor_allocation_lock,
                    settlement_anchor_source_audit,
                    settlement_btc_csv,
                    ..
                } = cli.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(btc_csv.as_deref(), Some("/tmp/binance.csv"));
                assert_eq!(fair_value_btc_csv.as_deref(), Some("/tmp/chainlink.csv"));
                assert!(pin_input_artifacts);
                assert_eq!(
                    settlement_anchor_allocation_lock.as_deref(),
                    Some("/tmp/allocation-lock.json")
                );
                assert_eq!(
                    settlement_anchor_source_audit.as_deref(),
                    Some("/tmp/source-audit.json")
                );
                assert_eq!(settlement_btc_csv.as_deref(), Some("/tmp/chainlink.csv"));
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn settlement_anchor_preflight_forbids_grid_unbounded_and_adaptive_runs() {
        assert!(settlement_anchor_preflight_error(
            false, false, false, false, false, false, true, false, false
        )
        .is_none());
        assert!(settlement_anchor_preflight_error(
            true, false, true, true, true, true, false, true, true
        )
        .unwrap()
        .contains("grid search is forbidden"));
        assert!(settlement_anchor_preflight_error(
            true, true, false, true, true, true, false, true, true
        )
        .unwrap()
        .contains("condition-id"));
        assert!(settlement_anchor_preflight_error(
            true, true, true, false, true, true, false, true, true
        )
        .unwrap()
        .contains("allocation-lock"));
        assert!(settlement_anchor_preflight_error(
            true, true, true, true, false, true, false, true, true
        )
        .unwrap()
        .contains("source-audit"));
        assert!(settlement_anchor_preflight_error(
            true, true, true, true, true, false, false, true, true
        )
        .unwrap()
        .contains("continuous"));
        assert!(settlement_anchor_preflight_error(
            true, true, true, true, true, true, true, true, true
        )
        .unwrap()
        .contains("adaptive"));
        assert!(settlement_anchor_preflight_error(
            true, true, true, true, true, true, false, false, true
        )
        .unwrap()
        .contains("report-json"));
        assert!(settlement_anchor_preflight_error(
            true, true, true, true, true, true, false, true, true
        )
        .is_none());
    }

    #[test]
    fn settlement_anchor_allocation_lock_command_parses_pinned_inputs_and_outputs() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let cli = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "settlement-anchor-allocation-lock",
                    "--preregistration",
                    "/tmp/prereg.json",
                    "--variant-json",
                    "/tmp/variant.json",
                    "--candidate-condition-set",
                    "/tmp/candidate.json",
                    "--prior-condition-set",
                    "/tmp/prior-1.json",
                    "/tmp/prior-2.json",
                    "--report-output",
                    "/tmp/report.json",
                    "--trades-output",
                    "/tmp/trades.json",
                    "--pair-audit-output",
                    "/tmp/pair-audit.json",
                    "--output",
                    "/tmp/lock.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::SettlementAnchorAllocationLock {
                            preregistration,
                            variant_json,
                            candidate_condition_set,
                            prior_condition_set,
                            report_output,
                            trades_output,
                            pair_audit_output,
                            output,
                        },
                } = cli.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(preregistration, "/tmp/prereg.json");
                assert_eq!(variant_json, "/tmp/variant.json");
                assert_eq!(candidate_condition_set, "/tmp/candidate.json");
                assert_eq!(
                    prior_condition_set,
                    ["/tmp/prior-1.json", "/tmp/prior-2.json"]
                );
                assert_eq!(report_output, "/tmp/report.json");
                assert_eq!(trades_output, "/tmp/trades.json");
                assert_eq!(pair_audit_output, "/tmp/pair-audit.json");
                assert_eq!(output.as_deref(), Some("/tmp/lock.json"));
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn opportunity_table_command_parses_bounded_measurement_inputs() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let cli = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-table",
                    "--hour",
                    "2026-08-01T12:00:00Z",
                    "--signals",
                    "/tmp/signals.jsonl",
                    "--cache-dir",
                    "/tmp/pmxt",
                    "--output",
                    "/tmp/opportunities.parquet",
                    "--manifest",
                    "/tmp/opportunities.manifest.json",
                    "--stake-usd",
                    "7",
                    "--max-rows",
                    "99",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityTable {
                            hour,
                            signals,
                            cache_dir,
                            distilled_input,
                            output,
                            manifest,
                            stake_usd,
                            fee_rate,
                            max_rows,
                        },
                } = cli.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(distilled_input, None);
                assert_eq!(hour, "2026-08-01T12:00:00Z");
                assert_eq!(signals, "/tmp/signals.jsonl");
                assert_eq!(cache_dir.as_deref(), Some("/tmp/pmxt"));
                assert_eq!(output, "/tmp/opportunities.parquet");
                assert_eq!(manifest, "/tmp/opportunities.manifest.json");
                assert_eq!(stake_usd, 7.0);
                assert_eq!(fee_rate, 0.07);
                assert_eq!(max_rows, 99);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn opportunity_market_catalog_command_parses_bounded_hours() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let cli = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-market-catalog",
                    "--hour",
                    "2026-07-17T02:00:00Z",
                    "--hour",
                    "2026-07-18T05:00:00Z",
                    "--base-catalog",
                    "/tmp/base.json",
                    "--output",
                    "/tmp/catalog.json",
                    "--manifest",
                    "/tmp/catalog.manifest.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityMarketCatalog {
                            hour,
                            base_catalog,
                            gamma_url,
                            family,
                            output,
                            manifest,
                        },
                } = cli.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(family, "btc-5m");
                assert_eq!(hour, ["2026-07-17T02:00:00Z", "2026-07-18T05:00:00Z"]);
                assert_eq!(base_catalog.as_deref(), Some("/tmp/base.json"));
                assert_eq!(gamma_url, "https://gamma-api.polymarket.com");
                assert_eq!(output, "/tmp/catalog.json");
                assert_eq!(manifest, "/tmp/catalog.manifest.json");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn opportunity_signals_command_parses_strict_sources_and_bound() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let cli = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-signals",
                    "--hour",
                    "2026-08-01T12:00:00Z",
                    "--causal-windows",
                    "/tmp/causal.jsonl.gz",
                    "--market-catalog",
                    "/tmp/gamma.json",
                    "--output",
                    "/tmp/signals.jsonl",
                    "--manifest",
                    "/tmp/signals.manifest.json",
                    "--max-rows",
                    "77",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunitySignals {
                            hour,
                            causal_windows,
                            market_catalog,
                            family,
                            output,
                            manifest,
                            max_rows,
                        },
                } = cli.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(family, "btc-5m");
                assert_eq!(hour, "2026-08-01T12:00:00Z");
                assert_eq!(causal_windows, "/tmp/causal.jsonl.gz");
                assert_eq!(market_catalog, "/tmp/gamma.json");
                assert_eq!(output, "/tmp/signals.jsonl");
                assert_eq!(manifest, "/tmp/signals.manifest.json");
                assert_eq!(max_rows, 77);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn opportunity_liquidity_commands_parse_sealed_inputs_and_budget() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let features = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-pair-features",
                    "--dataset-seal",
                    "/tmp/dataset.json",
                    "--market-catalog",
                    "/tmp/catalog.json",
                    "--cache-dir",
                    "/tmp/cache",
                    "--output",
                    "/tmp/pairs.jsonl",
                    "--manifest",
                    "/tmp/pairs.manifest.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command: StrategyBuilderCommand::OpportunityPairFeatures { output, .. },
                } = features.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(output, "/tmp/pairs.jsonl");

                let search = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-liquidity-search",
                    "--dataset-seal",
                    "/tmp/dataset.json",
                    "--labels-manifest",
                    "/tmp/labels.json",
                    "--paired-features-manifest",
                    "/tmp/pairs.manifest.json",
                    "--output",
                    "/tmp/search.json",
                    "--maximum-exact-replays",
                    "2",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityLiquiditySearch {
                            maximum_exact_replays,
                            ..
                        },
                } = search.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(maximum_exact_replays, 2);

                let decision = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-liquidity-decision",
                    "--preregistration",
                    "/tmp/prereg.json",
                    "--liquidity-search-report",
                    "/tmp/search.json",
                    "--output",
                    "/tmp/decision.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityLiquidityDecision {
                            exact_replay_report,
                            ..
                        },
                } = decision.command
                else {
                    panic!("unexpected command");
                };
                assert!(exact_replay_report.is_none());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn opportunity_flow_commands_parse_feature_store_and_budget() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let features = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-flow-features",
                    "--dataset-seal",
                    "/tmp/dataset.json",
                    "--market-catalog",
                    "/tmp/catalog.json",
                    "--cache-dir",
                    "/tmp/cache",
                    "--output",
                    "/tmp/flow.jsonl",
                    "--manifest",
                    "/tmp/flow.manifest.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command: StrategyBuilderCommand::OpportunityFlowFeatures { output, .. },
                } = features.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(output, "/tmp/flow.jsonl");

                let search = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-flow-search",
                    "--dataset-seal",
                    "/tmp/dataset.json",
                    "--labels-manifest",
                    "/tmp/labels.json",
                    "--feature-store-manifest",
                    "/tmp/flow.manifest.json",
                    "--output",
                    "/tmp/search.json",
                    "--maximum-exact-replays",
                    "2",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityFlowSearch {
                            maximum_exact_replays,
                            ..
                        },
                } = search.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(maximum_exact_replays, 2);

                let decision = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-flow-decision",
                    "--preregistration",
                    "/tmp/prereg.json",
                    "--flow-search-report",
                    "/tmp/search.json",
                    "--output",
                    "/tmp/decision.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityFlowDecision {
                            exact_replay_report,
                            ..
                        },
                } = decision.command
                else {
                    panic!("unexpected command");
                };
                assert!(exact_replay_report.is_none());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn opportunity_cross_venue_commands_parse_staged_contract() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let features = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-cross-venue-features",
                    "--dataset-seal",
                    "/tmp/dataset.json",
                    "--paired-features-manifest",
                    "/tmp/pairs.json",
                    "--source-tape-manifest",
                    "/tmp/tape.json",
                    "--output",
                    "/tmp/features.jsonl",
                    "--manifest",
                    "/tmp/features.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command: StrategyBuilderCommand::OpportunityCrossVenueFeatures { output, .. },
                } = features.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(output, "/tmp/features.jsonl");

                let preregister = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-cross-venue-preregister",
                    "--dataset-seal",
                    "/tmp/dataset.json",
                    "--labels-manifest",
                    "/tmp/labels.json",
                    "--feature-store-manifest",
                    "/tmp/features.json",
                    "--output",
                    "/tmp/prereg.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityCrossVenuePreregister {
                            maximum_exact_replays,
                            ..
                        },
                } = preregister.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(maximum_exact_replays, 2);

                let search = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-cross-venue-search",
                    "--preregistration",
                    "/tmp/prereg.json",
                    "--output",
                    "/tmp/search.json",
                ])
                .unwrap();
                assert!(matches!(
                    search.command,
                    Command::StrategyBuilder {
                        command: StrategyBuilderCommand::OpportunityCrossVenueSearch { .. }
                    }
                ));

                let decision = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-cross-venue-decision",
                    "--preregistration",
                    "/tmp/prereg.json",
                    "--search-report",
                    "/tmp/search.json",
                    "--output",
                    "/tmp/decision.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityCrossVenueDecision {
                            exact_replay_report,
                            ..
                        },
                } = decision.command
                else {
                    panic!("unexpected command");
                };
                assert!(exact_replay_report.is_none());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn opportunity_dataset_and_policy_commands_parse_immutable_inputs() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let seal = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-dataset-seal",
                    "--opportunity-manifest",
                    "/tmp/hour-1.json",
                    "/tmp/hour-2.json",
                    "--output",
                    "/tmp/dataset.seal.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityDatasetSeal {
                            opportunity_manifest,
                            output,
                        },
                } = seal.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(
                    opportunity_manifest,
                    ["/tmp/hour-1.json", "/tmp/hour-2.json"]
                );
                assert_eq!(output, "/tmp/dataset.seal.json");

                let labels = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-labels",
                    "--dataset-seal",
                    "/tmp/dataset.seal.json",
                    "--label-source",
                    "/tmp/labels.jsonl.gz",
                    "--output",
                    "/tmp/labels.parquet",
                    "--manifest",
                    "/tmp/labels.manifest.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityLabels {
                            dataset_seal,
                            label_source,
                            resolution_rule,
                            output,
                            manifest,
                        },
                } = labels.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(resolution_rule, "close_vs_open");
                assert_eq!(dataset_seal, "/tmp/dataset.seal.json");
                assert_eq!(label_source, "/tmp/labels.jsonl.gz");
                assert_eq!(output, "/tmp/labels.parquet");
                assert_eq!(manifest, "/tmp/labels.manifest.json");

                let search = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-policy-search",
                    "--dataset-seal",
                    "/tmp/dataset.seal.json",
                    "--labels-manifest",
                    "/tmp/labels.manifest.json",
                    "--output",
                    "/tmp/search.json",
                    "--minimum-calibration-support",
                    "30",
                    "--minimum-policy-support",
                    "25",
                    "--safety-margin",
                    "0.03",
                    "--latency-ms",
                    "50",
                    "--maximum-exact-replays",
                    "2",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityPolicySearch {
                            dataset_seal,
                            labels_manifest,
                            output,
                            minimum_calibration_support,
                            minimum_policy_support,
                            safety_margin,
                            latency_ms,
                            maximum_exact_replays,
                        },
                } = search.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(dataset_seal, "/tmp/dataset.seal.json");
                assert_eq!(labels_manifest, "/tmp/labels.manifest.json");
                assert_eq!(output, "/tmp/search.json");
                assert_eq!(minimum_calibration_support, 30);
                assert_eq!(minimum_policy_support, 25);
                assert_eq!(safety_margin, 0.03);
                assert_eq!(latency_ms, 50);
                assert_eq!(maximum_exact_replays, 2);

                let probability_search = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-probability-search",
                    "--dataset-seal",
                    "/tmp/dataset.seal.json",
                    "--labels-manifest",
                    "/tmp/labels.manifest.json",
                    "--output",
                    "/tmp/probability-search.json",
                    "--minimum-calibration-support",
                    "24",
                    "--maximum-calibration-brier-score",
                    "0.2",
                    "--minimum-policy-support",
                    "22",
                    "--safety-margin",
                    "0.04",
                    "--latency-ms",
                    "128",
                    "--maximum-exact-replays",
                    "2",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityProbabilitySearch {
                            dataset_seal,
                            labels_manifest,
                            output,
                            minimum_calibration_support,
                            maximum_calibration_brier_score,
                            minimum_policy_support,
                            safety_margin,
                            latency_ms,
                            maximum_exact_replays,
                        },
                } = probability_search.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(dataset_seal, "/tmp/dataset.seal.json");
                assert_eq!(labels_manifest, "/tmp/labels.manifest.json");
                assert_eq!(output, "/tmp/probability-search.json");
                assert_eq!(minimum_calibration_support, 24);
                assert_eq!(maximum_calibration_brier_score, 0.2);
                assert_eq!(minimum_policy_support, 22);
                assert_eq!(safety_margin, 0.04);
                assert_eq!(latency_ms, 128);
                assert_eq!(maximum_exact_replays, 2);

                let probability_decision = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-probability-decision",
                    "--preregistration",
                    "/tmp/preregistration.json",
                    "--probability-search-report",
                    "/tmp/probability-search.json",
                    "--exact-replay-report",
                    "/tmp/probability-replay.json",
                    "--output",
                    "/tmp/probability-decision.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityProbabilityDecision {
                            preregistration,
                            probability_search_report,
                            exact_replay_report,
                            output,
                        },
                } = probability_decision.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(preregistration, "/tmp/preregistration.json");
                assert_eq!(probability_search_report, "/tmp/probability-search.json");
                assert_eq!(exact_replay_report, "/tmp/probability-replay.json");
                assert_eq!(output, "/tmp/probability-decision.json");

                let replay = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "opportunity-exact-replay",
                    "--dataset-seal",
                    "/tmp/dataset.seal.json",
                    "--labels-manifest",
                    "/tmp/labels.manifest.json",
                    "--policy-search-report",
                    "/tmp/search.json",
                    "--cache-dir",
                    "/tmp/pmxt",
                    "--output",
                    "/tmp/replay.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::OpportunityExactReplay {
                            dataset_seal,
                            labels_manifest,
                            policy_search_report,
                            cache_dir,
                            distilled_dir,
                            output,
                        },
                } = replay.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(distilled_dir, None);
                assert_eq!(dataset_seal, "/tmp/dataset.seal.json");
                assert_eq!(labels_manifest, "/tmp/labels.manifest.json");
                assert_eq!(policy_search_report, "/tmp/search.json");
                assert_eq!(cache_dir, "/tmp/pmxt");
                assert_eq!(output, "/tmp/replay.json");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn settlement_anchor_pair_audit_command_parses_all_immutable_artifacts() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let cli = Cli::try_parse_from([
                    "polymomentum-engine",
                    "strategy-builder",
                    "settlement-anchor-pair-audit",
                    "--allocation-lock",
                    "/tmp/allocation.json",
                    "--source-audit",
                    "/tmp/source.json",
                    "--fair-value-btc-csv",
                    "/tmp/fair.csv",
                    "--baseline-report",
                    "/tmp/baseline-report.json",
                    "--baseline-trades",
                    "/tmp/baseline-trades.json",
                    "--official-report",
                    "/tmp/official-report.json",
                    "--official-trades",
                    "/tmp/official-trades.json",
                    "--output",
                    "/tmp/pair-audit.json",
                ])
                .unwrap();
                let Command::StrategyBuilder {
                    command:
                        StrategyBuilderCommand::SettlementAnchorPairAudit {
                            allocation_lock,
                            source_audit,
                            fair_value_btc_csv,
                            baseline_report,
                            baseline_trades,
                            official_report,
                            official_trades,
                            output,
                        },
                } = cli.command
                else {
                    panic!("unexpected command");
                };
                assert_eq!(allocation_lock, "/tmp/allocation.json");
                assert_eq!(source_audit, "/tmp/source.json");
                assert_eq!(fair_value_btc_csv, "/tmp/fair.csv");
                assert_eq!(baseline_report, "/tmp/baseline-report.json");
                assert_eq!(baseline_trades, "/tmp/baseline-trades.json");
                assert_eq!(official_report, "/tmp/official-report.json");
                assert_eq!(official_trades, "/tmp/official-trades.json");
                assert_eq!(output, "/tmp/pair-audit.json");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn settlement_anchor_evaluator_accepts_only_the_frozen_baseline_variant() {
        let pair_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_pair.json",
        );
        let pair = backtest::variant_io::read_variants(pair_path).unwrap();

        assert!(settlement_anchor_variant_error(&pair[..1]).is_none());
        assert!(settlement_anchor_variant_error(&pair)
            .unwrap()
            .contains("exactly one"));
        let mut tampered = pair[0].clone();
        tampered.min_edge += 0.01;
        assert!(settlement_anchor_variant_error(&[tampered])
            .unwrap()
            .contains(SETTLEMENT_ANCHOR_BASELINE_HASH));
    }

    #[test]
    fn finalize_recorded_books_parses_repeated_and_comma_separated_condition_ids() {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let cli = Cli::try_parse_from([
                    "polymomentum-engine",
                    "finalize-recorded-btc-books",
                    "--input-dir",
                    "/tmp/converted",
                    "--condition-id",
                    "first,second",
                    "--condition-id",
                    "third",
                ])
                .unwrap();

                let Command::FinalizeRecordedBtcBooks { condition_ids, .. } = cli.command else {
                    panic!("unexpected command");
                };
                assert_eq!(condition_ids, ["first", "second", "third"]);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn numeric_list_parsers_reject_invalid_and_non_finite_values() {
        assert_eq!(try_parse_csv_floats("0.1, 0.2").unwrap(), vec![0.1, 0.2]);
        assert_eq!(try_parse_csv_u64s("1, 2").unwrap(), vec![1, 2]);
        assert!(try_parse_csv_floats("0.1,,0.2").is_err());
        assert!(try_parse_csv_floats("NaN").is_err());
        assert!(try_parse_csv_floats("inf").is_err());
        assert!(try_parse_csv_u64s("-1").is_err());
        assert!(try_parse_csv_u64s("2.5").is_err());
    }

    #[test]
    fn btc_required_range_uses_selected_contracts_not_whole_clock_hours() {
        let close = chrono::DateTime::parse_from_rfc3339("2026-07-14T16:30:00Z")
            .unwrap()
            .timestamp_millis();
        let universe = backtest::harness::CandleUniverse {
            contracts: vec![data::scanner::CandleContract {
                market: data::models::Market::default(),
                up_token_id: "up".to_string(),
                down_token_id: "down".to_string(),
                up_price: 0.5,
                down_price: 0.5,
                end_date: "2026-07-14T16:30:00Z".to_string(),
                hours_left: 0.0,
                volume: 0.0,
                liquidity: 0.0,
                window_description: "July 14, 12:25PM-12:30PM ET".to_string(),
                asset: "BTC".to_string(),
            }],
        };

        let range = btc_required_range_ms(&universe, close - 9_000_000, close + 9_000_000);

        assert_eq!(range, (close - 300_000, close));
    }

    #[test]
    fn replay_hours_cover_every_clock_hour_touched_by_exact_window_bounds() {
        let start = chrono::DateTime::parse_from_rfc3339("2026-07-15T05:10:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let end = chrono::DateTime::parse_from_rfc3339("2026-07-15T06:15:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let hours = inclusive_replay_hours(start, end);

        assert_eq!(
            hours
                .iter()
                .map(chrono::DateTime::to_rfc3339)
                .collect::<Vec<_>>(),
            vec!["2026-07-15T05:00:00+00:00", "2026-07-15T06:00:00+00:00"]
        );
    }

    #[test]
    fn rtds_btc_parser_separates_official_chainlink_and_binance_proxy() {
        let chainlink = parse_rtds_btc_prices(
            r#"{"topic":"crypto_prices_chainlink","type":"update","payload":{"symbol":"btc/usd","timestamp":1780000000123,"value":67500.25}}"#,
        )
        .unwrap();
        let binance = parse_rtds_btc_prices(
            r#"{"topic":"crypto_prices","type":"update","payload":{"symbol":"btcusdt","timestamp":1780000000456,"value":67499.75}}"#,
        )
        .unwrap();

        assert_eq!(chainlink.len(), 1);
        assert_eq!(chainlink[0].source, "chainlink_btc_usd_data_stream");
        assert_eq!(chainlink[0].timestamp_ms, 1_780_000_000_123);
        assert_eq!(binance.len(), 1);
        assert_eq!(binance[0].source, "binance_btcusdt_rtds");
    }

    #[test]
    fn rtds_btc_subscription_uses_proven_vps_binance_shape() {
        let chainlink = rtds_btc_subscription(RtdsBtcSource::Chainlink);
        let binance = rtds_btc_subscription(RtdsBtcSource::Binance);

        assert_eq!(
            chainlink["subscriptions"][0]["filters"],
            "{\"symbol\":\"btc/usd\"}"
        );
        assert_eq!(binance["subscriptions"][0]["topic"], "crypto_prices");
        assert_eq!(binance["subscriptions"][0]["type"], "update");
        assert!(binance["subscriptions"][0].get("filters").is_none());
        assert_eq!(chainlink["subscriptions"].as_array().unwrap().len(), 1);
        assert_eq!(binance["subscriptions"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn rtds_watchdog_reconnects_before_reference_gap_limit() {
        let reconnected_long_enough =
            std::time::Duration::from_millis(RTDS_RECONNECTED_SOURCE_GRACE_MS);
        assert!(!rtds_source_should_reconnect(
            Some(std::time::Duration::from_millis(
                RTDS_SOURCE_RECONNECT_AFTER_MS - 1
            )),
            reconnected_long_enough,
            true,
        ));
        assert!(rtds_source_should_reconnect(
            Some(std::time::Duration::from_millis(
                RTDS_SOURCE_RECONNECT_AFTER_MS
            )),
            reconnected_long_enough,
            true,
        ));
        assert!(!rtds_source_should_reconnect(
            Some(std::time::Duration::from_millis(
                RTDS_SOURCE_RECONNECT_AFTER_MS
            )),
            std::time::Duration::from_millis(RTDS_RECONNECTED_SOURCE_GRACE_MS - 1),
            true,
        ));
        assert!(!rtds_source_should_reconnect(
            None,
            std::time::Duration::from_millis(RTDS_INITIAL_SOURCE_GRACE_MS - 1),
            false,
        ));
        assert!(rtds_source_should_reconnect(
            None,
            std::time::Duration::from_millis(RTDS_INITIAL_SOURCE_GRACE_MS),
            false,
        ));
        assert!(!rtds_source_should_reconnect(
            None,
            std::time::Duration::from_millis(RTDS_RECONNECTED_SOURCE_GRACE_MS - 1),
            true,
        ));
        assert!(rtds_source_should_reconnect(
            None,
            std::time::Duration::from_millis(RTDS_RECONNECTED_SOURCE_GRACE_MS),
            true,
        ));
        const { assert!(RTDS_SOURCE_RECONNECT_AFTER_MS < 5_000) };
    }

    #[test]
    fn rtds_source_stats_merge_keeps_source_failures_isolated() {
        let mut chainlink = RtdsBtcSourceTapeStats {
            connect_attempts: 2,
            connected_sessions: 2,
            subscriptions_sent: 2,
            reconnects: 1,
            idle_timeouts: 1,
            frames: 20,
            ..Default::default()
        };
        chainlink.prices.observe(1_000, 1_010);
        let mut binance = RtdsBtcSourceTapeStats {
            connect_attempts: 1,
            connected_sessions: 1,
            subscriptions_sent: 1,
            frames: 30,
            ..Default::default()
        };
        binance.prices.observe(2_000, 2_010);

        let mut aggregate = RtdsBtcTapeStats::default();
        merge_rtds_btc_source_stats(&mut aggregate, RtdsBtcSource::Chainlink, chainlink);
        merge_rtds_btc_source_stats(&mut aggregate, RtdsBtcSource::Binance, binance);

        assert_eq!(aggregate.connect_attempts, 3);
        assert_eq!(aggregate.connected_sessions, 3);
        assert_eq!(aggregate.reconnects, 1);
        assert_eq!(aggregate.frames, 50);
        assert_eq!(aggregate.idle_timeouts, 1);
        assert_eq!(aggregate.chainlink_idle_timeouts, 1);
        assert_eq!(aggregate.binance_idle_timeouts, 0);
        assert_eq!(aggregate.chainlink.ticks, 1);
        assert_eq!(aggregate.binance.ticks, 1);
    }

    #[test]
    fn rtds_readiness_rejects_open_but_stale_price_stream() {
        let mut stats = RtdsPriceSourceStats::default();
        stats.observe(1_000, 1_100);
        assert!(rtds_source_is_fresh(&stats, 20_000, 20_000));
        assert!(!rtds_source_is_fresh(&stats, 22_000, 20_000));
        assert!(!rtds_source_is_fresh(&stats, 1_000, 20_000));
    }

    #[test]
    fn record_market_ws_stats_tracks_books_and_price_changes() {
        let payload = serde_json::json!([
            {
                "event_type": "book",
                "asset_id": "up-token",
                "bids": [],
                "asks": []
            },
            {
                "event_type": "price_change",
                "price_changes": [
                    {
                        "asset_id": "down-token",
                        "price": "0.42",
                        "side": "BUY",
                        "size": "12"
                    }
                ]
            }
        ]);
        let mut stats = MarketWsRecordStats::default();
        let mut seen = std::collections::BTreeSet::new();

        record_market_ws_text(&payload.to_string(), &mut stats, &mut seen);

        assert_eq!(stats.json_messages, 2);
        assert_eq!(stats.book_messages, 1);
        assert_eq!(stats.price_change_messages, 1);
        assert!(seen.contains("up-token"));
        assert!(seen.contains("down-token"));
    }

    #[test]
    fn forward_latency_audit_requires_retest_when_p99_exceeds_backtest_assumption() {
        let mut acc = ForwardLatencyAuditAccumulator::default();
        forward_latency_audit_ws_value(
            &serde_json::json!({
                "event_type": "book",
                "market": "0xabc",
                "asset_id": "up-token",
                "timestamp": "1782908899760",
                "bids": [],
                "asks": []
            }),
            Some(1_782_908_900_000),
            &mut acc,
        );
        forward_latency_audit_ws_value(
            &serde_json::json!({
                "event_type": "price_change",
                "market": "0xabc",
                "timestamp": "1782908901200",
                "price_changes": [
                    {"asset_id": "up-token", "price": "0.52", "side": "BUY", "size": "10"},
                    {"asset_id": "down-token", "price": "0.48", "side": "SELL", "size": "10"}
                ]
            }),
            Some(1_782_908_901_400),
            &mut acc,
        );
        acc.stats.record_overhead_samples = 2;
        acc.record_overhead_sum_ms = 2.0;
        acc.record_overhead_ms = vec![1.0, 1.0];
        let expected = ["down-token".to_string(), "up-token".to_string()]
            .into_iter()
            .collect();
        let report = forward_latency_audit_report(
            std::path::Path::new("/tmp/in"),
            std::path::Path::new("/tmp/in/market_ws_frames.jsonl"),
            std::path::Path::new("/tmp/out.json"),
            None,
            acc,
            &expected,
            &std::collections::BTreeMap::new(),
            ForwardLatencyAuditThresholds {
                max_p99_delay_ms: 500.0,
                max_token_gap_ms: 2_000.0,
                min_gap_gate_events: 1,
                max_missing_timestamp_rate: 0.0,
            },
        );

        assert_eq!(report["delay_ms"]["p99"].as_f64(), Some(240.0));
        assert_eq!(report["delay_ms"]["p99_5"].as_f64(), Some(240.0));
        assert_eq!(
            report["delay_counts_above_ms"]["150"]["count"].as_u64(),
            Some(2)
        );
        assert_eq!(
            report["delay_counts_above_ms"]["200"]["count"].as_u64(),
            Some(1)
        );
        assert_eq!(report["record_overhead_ms"]["p99"].as_f64(), Some(1.0));
        assert_eq!(
            report["a_plus_latency_gate"]["stream_latency_ready"].as_bool(),
            Some(true)
        );
        assert_eq!(
            report["a_plus_latency_gate"]["backtest_latency_assumption_ready"].as_bool(),
            Some(false)
        );
        assert_eq!(
            report["a_plus_latency_gate"]["verdict"].as_str(),
            Some("MEASURED_LATENCY_RETEST_REQUIRED")
        );
        assert_eq!(
            report["a_plus_latency_gate"]["recommended_retest_latency_ms"].as_u64(),
            Some(240)
        );
    }

    #[test]
    fn forward_latency_audit_fails_closed_without_clob_timestamps() {
        let mut acc = ForwardLatencyAuditAccumulator::default();
        forward_latency_audit_ws_value(
            &serde_json::json!({
                "event_type": "book",
                "market": "0xabc",
                "asset_id": "up-token",
                "bids": [],
                "asks": []
            }),
            Some(1_000),
            &mut acc,
        );
        let expected = ["up-token".to_string()].into_iter().collect();
        let report = forward_latency_audit_report(
            std::path::Path::new("/tmp/in"),
            std::path::Path::new("/tmp/in/market_ws_frames.jsonl"),
            std::path::Path::new("/tmp/out.json"),
            None,
            acc,
            &expected,
            &std::collections::BTreeMap::new(),
            ForwardLatencyAuditThresholds {
                max_p99_delay_ms: 500.0,
                max_token_gap_ms: 2_000.0,
                min_gap_gate_events: 1,
                max_missing_timestamp_rate: 0.0,
            },
        );

        assert_eq!(
            report["a_plus_latency_gate"]["ready"].as_bool(),
            Some(false)
        );
        assert_eq!(
            report["a_plus_latency_gate"]["verdict"].as_str(),
            Some("NO_TIMESTAMPED_CLOB_EVENTS")
        );
        assert_eq!(report["stats"]["missing_event_timestamp"].as_u64(), Some(1));
    }

    #[test]
    fn forward_latency_audit_does_not_recommend_latency_when_clock_skew_negative() {
        let mut acc = ForwardLatencyAuditAccumulator::default();
        forward_latency_audit_ws_value(
            &serde_json::json!({
                "event_type": "price_change",
                "market": "0xabc",
                "timestamp": "1782908900100",
                "price_changes": [
                    {"asset_id": "up-token", "price": "0.52", "side": "BUY", "size": "10"}
                ]
            }),
            Some(1_782_908_900_000),
            &mut acc,
        );
        let expected = ["up-token".to_string()].into_iter().collect();
        let report = forward_latency_audit_report(
            std::path::Path::new("/tmp/in"),
            std::path::Path::new("/tmp/in/market_ws_frames.jsonl"),
            std::path::Path::new("/tmp/out.json"),
            None,
            acc,
            &expected,
            &std::collections::BTreeMap::new(),
            ForwardLatencyAuditThresholds {
                max_p99_delay_ms: 500.0,
                max_token_gap_ms: 2_000.0,
                min_gap_gate_events: 1,
                max_missing_timestamp_rate: 0.0,
            },
        );

        assert_eq!(
            report["a_plus_latency_gate"]["stream_latency_ready"].as_bool(),
            Some(false)
        );
        assert_eq!(
            report["a_plus_latency_gate"]["backtest_latency_assumption_ready"].as_bool(),
            Some(false)
        );
        assert_eq!(
            report["a_plus_latency_gate"]["strategy_retest_required"].as_bool(),
            Some(true)
        );
        assert_eq!(
            report["a_plus_latency_gate"]["recommended_retest_latency_ms"],
            serde_json::Value::Null
        );
        assert_eq!(
            report["a_plus_latency_gate"]["verdict"].as_str(),
            Some("CLOCK_SKEW_NEGATIVE_DELAYS")
        );
    }

    #[test]
    fn forward_latency_gap_gate_ignores_sparse_future_tokens() {
        let mut acc = ForwardLatencyAuditAccumulator::default();
        for row_ts in [1_782_908_900_000_i64, 1_782_908_900_100, 1_782_908_900_200] {
            forward_latency_audit_ws_value(
                &serde_json::json!({
                    "event_type": "price_change",
                    "market": "0xactive",
                    "asset_id": "active-token",
                    "timestamp": (row_ts - 20).to_string(),
                    "price_changes": []
                }),
                Some(row_ts),
                &mut acc,
            );
        }
        for row_ts in [1_782_908_900_000_i64, 1_782_908_905_000] {
            forward_latency_audit_ws_value(
                &serde_json::json!({
                    "event_type": "price_change",
                    "market": "0xfuture",
                    "asset_id": "future-token",
                    "timestamp": (row_ts - 20).to_string(),
                    "price_changes": []
                }),
                Some(row_ts),
                &mut acc,
            );
        }
        let expected = ["active-token".to_string(), "future-token".to_string()]
            .into_iter()
            .collect();
        let report = forward_latency_audit_report(
            std::path::Path::new("/tmp/in"),
            std::path::Path::new("/tmp/in/market_ws_frames.jsonl"),
            std::path::Path::new("/tmp/out.json"),
            None,
            acc,
            &expected,
            &std::collections::BTreeMap::new(),
            ForwardLatencyAuditThresholds {
                max_p99_delay_ms: 500.0,
                max_token_gap_ms: 1_000.0,
                min_gap_gate_events: 3,
                max_missing_timestamp_rate: 0.0,
            },
        );

        assert_eq!(
            report["token_coverage"]["max_observed_gap_ms"].as_f64(),
            Some(5_000.0)
        );
        assert_eq!(
            report["token_coverage"]["max_gap_gate_ms"].as_f64(),
            Some(100.0)
        );
        assert_eq!(
            report["a_plus_latency_gate"]["token_gap_ready"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn forward_latency_gap_gate_uses_active_slug_windows() {
        let mut acc = ForwardLatencyAuditAccumulator::default();
        for row_ts in [1_783_164_000_000_i64, 1_783_164_000_100, 1_783_164_000_200] {
            forward_latency_observe_frame_received(&mut acc, Some(row_ts), None);
            forward_latency_audit_ws_value(
                &serde_json::json!({
                    "event_type": "price_change",
                    "market": "0xactive",
                    "asset_id": "active-token",
                    "timestamp": (row_ts - 20).to_string(),
                    "price_changes": []
                }),
                Some(row_ts),
                &mut acc,
            );
        }
        for row_ts in [1_783_164_000_300_i64, 1_783_164_090_000, 1_783_164_180_000] {
            if row_ts == 1_783_164_090_000 {
                for keepalive_ts in (1_783_164_001_000_i64..row_ts).step_by(1_000) {
                    forward_latency_observe_frame_received(&mut acc, Some(keepalive_ts), None);
                }
            } else if row_ts == 1_783_164_180_000 {
                for keepalive_ts in (1_783_164_091_000_i64..row_ts).step_by(1_000) {
                    forward_latency_observe_frame_received(&mut acc, Some(keepalive_ts), None);
                }
            }
            forward_latency_observe_frame_received(&mut acc, Some(row_ts), None);
            forward_latency_audit_ws_value(
                &serde_json::json!({
                    "event_type": "price_change",
                    "market": "0xfuture",
                    "asset_id": "future-token",
                    "timestamp": (row_ts - 20).to_string(),
                    "price_changes": []
                }),
                Some(row_ts),
                &mut acc,
            );
        }
        let expected = ["active-token".to_string(), "future-token".to_string()]
            .into_iter()
            .collect();
        let token_outcomes = std::collections::BTreeMap::from([
            (
                "active-token".to_string(),
                serde_json::json!({"slug": "btc-updown-5m-1783164000"}),
            ),
            (
                "future-token".to_string(),
                serde_json::json!({"slug": "btc-updown-5m-1783164600"}),
            ),
        ]);
        let report = forward_latency_audit_report(
            std::path::Path::new("/tmp/in"),
            std::path::Path::new("/tmp/in/market_ws_frames.jsonl"),
            std::path::Path::new("/tmp/out.json"),
            None,
            acc,
            &expected,
            &token_outcomes,
            ForwardLatencyAuditThresholds {
                max_p99_delay_ms: 500.0,
                max_token_gap_ms: 1_000.0,
                min_gap_gate_events: 3,
                max_missing_timestamp_rate: 0.0,
            },
        );

        assert_eq!(
            report["token_coverage"]["active_expected_token_ids"],
            serde_json::json!(["active-token"])
        );
        assert_eq!(
            report["token_coverage"]["gap_gate_token_ids"],
            serde_json::json!(["active-token"])
        );
        assert_eq!(
            report["token_coverage"]["gap_skipped_token_ids"],
            serde_json::json!(["future-token"])
        );
        assert_eq!(
            report["a_plus_latency_gate"]["token_gap_ready"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn forward_latency_gap_gate_fails_on_stream_receive_gap() {
        let mut acc = ForwardLatencyAuditAccumulator::default();
        for row_ts in [1_783_164_000_000_i64, 1_783_164_005_000, 1_783_164_005_100] {
            forward_latency_observe_frame_received(&mut acc, Some(row_ts), None);
            forward_latency_audit_ws_value(
                &serde_json::json!({
                    "event_type": "price_change",
                    "market": "0xactive",
                    "asset_id": "active-token",
                    "timestamp": (row_ts - 20).to_string(),
                    "price_changes": []
                }),
                Some(row_ts),
                &mut acc,
            );
        }
        let expected = ["active-token".to_string()].into_iter().collect();
        let token_outcomes = std::collections::BTreeMap::from([(
            "active-token".to_string(),
            serde_json::json!({"slug": "btc-updown-5m-1783164000"}),
        )]);
        let report = forward_latency_audit_report(
            std::path::Path::new("/tmp/in"),
            std::path::Path::new("/tmp/in/market_ws_frames.jsonl"),
            std::path::Path::new("/tmp/out.json"),
            None,
            acc,
            &expected,
            &token_outcomes,
            ForwardLatencyAuditThresholds {
                max_p99_delay_ms: 500.0,
                max_token_gap_ms: 1_000.0,
                min_gap_gate_events: 3,
                max_missing_timestamp_rate: 0.0,
            },
        );

        assert_eq!(
            report["token_coverage"]["stream_gap_ready"].as_bool(),
            Some(false)
        );
        assert_eq!(
            report["a_plus_latency_gate"]["token_gap_ready"].as_bool(),
            Some(false)
        );
        assert_eq!(
            report["a_plus_latency_gate"]["verdict"].as_str(),
            Some("STREAM_RECEIVE_GAP_TOO_HIGH")
        );
    }

    #[test]
    fn forward_latency_window_continuity_excludes_only_intersecting_market() {
        let first_open_ms = 1_783_164_000_000_i64;
        let second_open_ms = first_open_ms + 300_000;
        let token_outcomes = std::collections::BTreeMap::from([
            (
                "first-token".to_string(),
                serde_json::json!({
                    "condition_id": "first-condition",
                    "slug": "btc-updown-5m-1783164000"
                }),
            ),
            (
                "first-token-2".to_string(),
                serde_json::json!({
                    "condition_id": "first-condition",
                    "slug": "btc-updown-5m-1783164000"
                }),
            ),
            (
                "second-token".to_string(),
                serde_json::json!({
                    "condition_id": "second-condition",
                    "slug": "btc-updown-5m-1783164300"
                }),
            ),
            (
                "second-token-2".to_string(),
                serde_json::json!({
                    "condition_id": "second-condition",
                    "slug": "btc-updown-5m-1783164300"
                }),
            ),
        ]);
        let token_stats = token_outcomes
            .keys()
            .map(|token_id| (token_id.clone(), ForwardLatencyTokenStats::default()))
            .collect();
        let gaps = vec![ForwardLatencyReceiveGap {
            start_ms: first_open_ms + 100_000,
            end_ms: first_open_ms + 102_500,
            gap_ms: 2_500,
        }];

        let report =
            forward_latency_window_continuity(&token_outcomes, &token_stats, &gaps, 2_000.0);

        assert_eq!(report["conditions"].as_u64(), Some(2));
        assert_eq!(report["retained_conditions"].as_u64(), Some(1));
        assert_eq!(report["excluded_conditions"].as_u64(), Some(1));
        assert_eq!(
            report["per_condition"][0]["condition_id"].as_str(),
            Some("first-condition")
        );
        assert_eq!(
            report["per_condition"][0]["stream_continuity_ready"].as_bool(),
            Some(false)
        );
        assert_eq!(
            report["per_condition"][1]["open_ms"].as_i64(),
            Some(second_open_ms)
        );
        assert_eq!(
            report["per_condition"][1]["stream_continuity_ready"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn forward_latency_window_admissibility_isolates_reference_gap_by_condition() {
        let tmp = tempfile::TempDir::new().unwrap();
        let first_open_ms = 10_000_000_i64;
        let last_close_ms = first_open_ms + 900_000;
        let mut binance_csv = String::from("timestamp_ms,source,price,received_at_ms\n");
        for timestamp_ms in (first_open_ms - 3_600_000..=last_close_ms).step_by(1_000) {
            binance_csv.push_str(&format!(
                "{timestamp_ms},binance_btcusdt_rtds,65000,{timestamp_ms}\n"
            ));
        }
        std::fs::write(tmp.path().join("binance_btcusdt_rtds.csv"), binance_csv).unwrap();

        let gap_start_ms = first_open_ms + 400_000;
        let mut chainlink_csv = String::from("timestamp_ms,source,price,received_at_ms\n");
        for timestamp_ms in (first_open_ms..=last_close_ms).step_by(1_000) {
            if timestamp_ms > gap_start_ms && timestamp_ms < gap_start_ms + 8_000 {
                continue;
            }
            chainlink_csv.push_str(&format!(
                "{timestamp_ms},chainlink_btc_usd_data_stream,65000,{timestamp_ms}\n"
            ));
        }
        std::fs::write(tmp.path().join("chainlink_btcusd.csv"), chainlink_csv).unwrap();

        let window_continuity = serde_json::json!({
            "per_condition": [
                {
                    "condition_id": "first",
                    "slug": "btc-updown-5m-10000",
                    "open_ms": first_open_ms,
                    "close_ms": first_open_ms + 300_000,
                    "stream_continuity_ready": false
                },
                {
                    "condition_id": "second",
                    "slug": "btc-updown-5m-10300",
                    "open_ms": first_open_ms + 300_000,
                    "close_ms": first_open_ms + 600_000,
                    "stream_continuity_ready": true
                },
                {
                    "condition_id": "third",
                    "slug": "btc-updown-5m-10600",
                    "open_ms": first_open_ms + 600_000,
                    "close_ms": last_close_ms,
                    "stream_continuity_ready": true
                }
            ]
        });

        let report = forward_latency_window_admissibility(tmp.path(), &window_continuity).unwrap();

        assert_eq!(report["conditions"].as_u64(), Some(3));
        assert_eq!(report["admissible_conditions"].as_u64(), Some(1));
        assert_eq!(report["groups"].as_array().unwrap().len(), 1);
        assert_eq!(report["groups"][0]["condition_ids"][0], "third");
        assert_eq!(
            report["per_condition"][0]["exclusion_reasons"][0],
            "clob_stream_continuity"
        );
        assert_eq!(
            report["per_condition"][1]["exclusion_reasons"][0],
            "chainlink_settlement"
        );
        assert_eq!(report["per_condition"][2]["binance_signal"]["ready"], true);
        assert_eq!(
            report["per_condition"][2]["chainlink_settlement"]["ready"],
            true
        );
    }

    #[test]
    fn forward_latency_gap_gate_ignores_post_window_capture_padding() {
        let open_ms = 1_783_164_000_000_i64;
        let close_ms = open_ms + 300_000;
        let active_range = Some((open_ms, close_ms));
        let mut acc = ForwardLatencyAuditAccumulator::default();
        for row_ts in (open_ms..=close_ms).step_by(1_000) {
            forward_latency_observe_frame_received(&mut acc, Some(row_ts), active_range);
        }
        forward_latency_observe_frame_received(&mut acc, Some(close_ms + 3_082), active_range);
        for row_ts in [open_ms, open_ms + 100_000, close_ms] {
            forward_latency_audit_ws_value(
                &serde_json::json!({
                    "event_type": "price_change",
                    "market": "0xactive",
                    "asset_id": "active-token",
                    "timestamp": (row_ts - 20).to_string(),
                    "price_changes": []
                }),
                Some(row_ts),
                &mut acc,
            );
        }
        let expected = ["active-token".to_string()].into_iter().collect();
        let token_outcomes = std::collections::BTreeMap::from([(
            "active-token".to_string(),
            serde_json::json!({"slug": "btc-updown-5m-1783164000"}),
        )]);

        let report = forward_latency_audit_report(
            std::path::Path::new("/tmp/in"),
            std::path::Path::new("/tmp/in/market_ws_frames.jsonl"),
            std::path::Path::new("/tmp/out.json"),
            None,
            acc,
            &expected,
            &token_outcomes,
            ForwardLatencyAuditThresholds {
                max_p99_delay_ms: 500.0,
                max_token_gap_ms: 2_000.0,
                min_gap_gate_events: 3,
                max_missing_timestamp_rate: 0.0,
            },
        );

        assert_eq!(
            report["stats"]["max_stream_receive_gap_ms"].as_i64(),
            Some(3_082)
        );
        assert_eq!(
            report["stats"]["evaluated_stream_receive_gap_ms"].as_i64(),
            Some(1_000)
        );
        assert_eq!(
            report["a_plus_latency_gate"]["token_gap_ready"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn forward_latency_percentile_uses_nearest_rank() {
        let values = [10.0, 20.0, 30.0, 40.0];
        assert_eq!(forward_latency_percentile(&values, 0.50), Some(20.0));
        assert_eq!(forward_latency_percentile(&values, 0.95), Some(40.0));
        assert_eq!(forward_latency_percentile(&values, 0.995), Some(40.0));
        assert_eq!(forward_latency_percentile(&[], 0.50), None);
    }

    #[test]
    fn rolling_history_latency_policy_applies_forward_audit_recommendation() {
        let tmp = tempfile::tempdir().unwrap();
        let audit_path = tmp.path().join("latency_audit.json");
        write_json_atomic(
            &audit_path,
            &serde_json::json!({
                "a_plus_latency_gate": {
                    "stream_latency_ready": true,
                    "recommended_retest_latency_ms": 366,
                    "verdict": "MEASURED_LATENCY_RETEST_REQUIRED"
                },
                "delay_ms": {"p99": 366.0}
            }),
            true,
        )
        .unwrap();

        let (effective, policy) = rolling_history_latency_policy(50, Some(&audit_path)).unwrap();

        assert_eq!(effective, 366);
        assert_eq!(policy["override_applied"].as_bool(), Some(true));
        assert_eq!(
            policy["audit_verdict"].as_str(),
            Some("MEASURED_LATENCY_RETEST_REQUIRED")
        );
    }

    #[test]
    fn rolling_history_latency_policy_rejects_non_ready_audit() {
        let tmp = tempfile::tempdir().unwrap();
        let audit_path = tmp.path().join("latency_audit.json");
        write_json_atomic(
            &audit_path,
            &serde_json::json!({
                "a_plus_latency_gate": {
                    "stream_latency_ready": false,
                    "recommended_retest_latency_ms": 500,
                    "verdict": "CLOB_P99_DELAY_TOO_HIGH"
                }
            }),
            true,
        )
        .unwrap();

        let err = rolling_history_latency_policy(50, Some(&audit_path)).unwrap_err();

        assert!(err.to_string().contains("not stream-ready"));
    }

    #[test]
    fn recorded_book_converter_emits_distilled_events() {
        let payload = serde_json::json!([
            {
                "event_type": "book",
                "market": "0xabc",
                "asset_id": "up-token",
                "timestamp": "1782898923000",
                "bids": [{"price": "0.41", "size": "10"}, {"price": "0.42", "size": "9"}],
                "asks": [{"price": "0.43", "size": "8"}]
            },
            {
                "event_type": "price_change",
                "market": "0xabc",
                "timestamp": "1782898923500",
                "price_changes": [{
                    "asset_id": "up-token",
                    "price": "0.42",
                    "side": "BUY",
                    "size": "12",
                    "best_bid": "0.42",
                    "best_ask": "0.43"
                }]
            }
        ]);
        let mut market_ids = std::collections::BTreeSet::new();
        market_ids.insert("0xabc".to_string());
        let mut token_to_market = std::collections::BTreeMap::new();
        token_to_market.insert("up-token".to_string(), "0xabc".to_string());
        let mut out = Vec::new();
        let mut stats = RecordedBooksConvertStats::default();

        recorded_ws_value_to_distilled_events(
            &payload,
            None,
            &market_ids,
            &token_to_market,
            &mut out,
            &mut stats,
        );

        assert_eq!(stats.json_messages, 2);
        assert_eq!(stats.skipped_unknown_token, 0);
        assert_eq!(out.len(), 2);
        let backtest::distill::DistilledEvent::Book { bb, ba, bids, .. } = &out[0] else {
            panic!("first event should be a book snapshot")
        };
        assert!((*bb - 0.42).abs() < 1e-9);
        assert!((*ba - 0.43).abs() < 1e-9);
        assert_eq!(bids.len(), 2);
        let backtest::distill::DistilledEvent::Change { p, sz, s, .. } = &out[1] else {
            panic!("second event should be a price change")
        };
        assert_eq!(p, "0.42");
        assert_eq!(sz, "12");
        assert_eq!(s, "BUY");
    }

    #[test]
    fn recorded_book_converter_manifest_uses_exact_replay_contract() {
        let tmp = tempfile::TempDir::new().unwrap();
        let raw_dir = tmp.path().join("raw");
        let output_dir = tmp.path().join("converted");
        std::fs::create_dir_all(&raw_dir).unwrap();
        let market = data::models::Market {
            condition_id: "0xabc".to_string(),
            slug: "btc-updown-5m-test".to_string(),
            outcomes: vec![
                data::models::Outcome {
                    token_id: "up-token".to_string(),
                    name: "Up".to_string(),
                    price: 0.5,
                },
                data::models::Outcome {
                    token_id: "down-token".to_string(),
                    name: "Down".to_string(),
                    price: 0.5,
                },
            ],
            ..Default::default()
        };
        write_json_atomic(
            raw_dir.join("gamma_market_cache.json"),
            &std::collections::BTreeMap::from([("0xabc".to_string(), market)]),
            true,
        )
        .unwrap();
        let raw_message = serde_json::json!({
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "up-token",
            "timestamp": "1782898923000",
            "bids": [{"price": "0.41", "size": "10"}],
            "asks": [{"price": "0.43", "size": "8"}],
        });
        let frame = serde_json::json!({
            "ts_received_ms": 1782898923001_i64,
            "raw": serde_json::to_string(&raw_message).unwrap(),
        });
        let tick_message = serde_json::json!({
            "event_type": "tick_size_change",
            "market": "0xabc",
            "asset_id": "up-token",
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
            "timestamp": "1782898924000",
        });
        let tick_frame = serde_json::json!({
            "ts_received_ms": 1782898924001_i64,
            "raw": serde_json::to_string(&tick_message).unwrap(),
        });
        let crossing_message = serde_json::json!({
            "event_type": "price_change",
            "market": "0xabc",
            "timestamp": "1782898924003",
            "price_changes": [{
                "asset_id": "up-token",
                "side": "BUY",
                "price": "0.999",
                "size": "10",
                "best_bid": "0.999",
                "best_ask": "1"
            }],
        });
        let crossing_frame = serde_json::json!({
            "ts_received_ms": 1782898924004_i64,
            "raw": serde_json::to_string(&crossing_message).unwrap(),
        });
        std::fs::write(
            raw_dir.join("market_ws_frames.jsonl"),
            format!("{frame}\n{tick_frame}\n{crossing_frame}\n"),
        )
        .unwrap();

        cmd_convert_recorded_btc_books(
            raw_dir.to_str().unwrap(),
            output_dir.to_str().unwrap(),
            &[],
        )
        .unwrap();

        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output_dir.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(
            manifest["output"]["harness_env"]["PMXT_DISTILLED_DIR"].as_str(),
            output_dir.to_str()
        );
        assert_eq!(
            manifest["output"]["exact_replay_flag"].as_str(),
            Some("--require-shared-distilled")
        );
        assert!(manifest["output"].get("harness_flag").is_none());
        assert_eq!(manifest["tick_integrity"]["raw_tick_size_change_rows"], 1);
        assert_eq!(
            manifest["tick_integrity"]["markets_with_tick_size_change"],
            1
        );
        assert_eq!(
            manifest["tick_integrity"]["transitions_match_documented_contract"],
            true
        );
        assert_eq!(
            manifest["tick_integrity"]["all_observed_transitions_reconstructable"],
            true
        );
        assert_eq!(
            manifest["tick_integrity"]["markets"][0]["reconstruction_delay_ms"],
            3
        );
    }

    #[test]
    fn recorded_book_converter_rejects_unreconstructable_tick_transition() {
        let tmp = tempfile::TempDir::new().unwrap();
        let raw_dir = tmp.path().join("raw");
        let output_dir = tmp.path().join("converted");
        std::fs::create_dir_all(&raw_dir).unwrap();
        let market = data::models::Market {
            condition_id: "0xabc".to_string(),
            outcomes: vec![data::models::Outcome {
                token_id: "up-token".to_string(),
                name: "Up".to_string(),
                price: 0.5,
            }],
            ..Default::default()
        };
        write_json_atomic(
            raw_dir.join("gamma_market_cache.json"),
            &std::collections::BTreeMap::from([("0xabc".to_string(), market)]),
            true,
        )
        .unwrap();
        let frame = |message: serde_json::Value, received_ms: i64| {
            serde_json::json!({
                "ts_received_ms": received_ms,
                "raw": serde_json::to_string(&message).unwrap(),
            })
        };
        let normal_book = frame(
            serde_json::json!({
                "event_type": "book",
                "market": "0xabc",
                "asset_id": "up-token",
                "timestamp": "1782898923000",
                "bids": [{"price": "0.41", "size": "10"}],
                "asks": [{"price": "0.43", "size": "8"}],
            }),
            1_782_898_923_001,
        );
        let tick = frame(
            serde_json::json!({
                "event_type": "tick_size_change",
                "market": "0xabc",
                "asset_id": "up-token",
                "old_tick_size": "0.01",
                "new_tick_size": "0.001",
                "timestamp": "1782898924000",
            }),
            1_782_898_924_001,
        );
        std::fs::write(
            raw_dir.join("market_ws_frames.jsonl"),
            format!("{normal_book}\n{tick}\n"),
        )
        .unwrap();

        let error = cmd_convert_recorded_btc_books(
            raw_dir.to_str().unwrap(),
            output_dir.to_str().unwrap(),
            &[],
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("cannot be reconstructed from preserved book events"));
    }

    #[test]
    fn recorded_book_converter_filters_to_requested_conditions() {
        let tmp = tempfile::TempDir::new().unwrap();
        let raw_dir = tmp.path().join("raw");
        let output_dir = tmp.path().join("converted");
        std::fs::create_dir_all(&raw_dir).unwrap();
        let market = |condition_id: &str, token_id: &str| data::models::Market {
            condition_id: condition_id.to_string(),
            slug: format!("btc-updown-5m-{condition_id}"),
            outcomes: vec![data::models::Outcome {
                token_id: token_id.to_string(),
                name: "Up".to_string(),
                price: 0.5,
            }],
            ..Default::default()
        };
        write_json_atomic(
            raw_dir.join("gamma_market_cache.json"),
            &std::collections::BTreeMap::from([
                ("0xkeep".to_string(), market("0xkeep", "keep-token")),
                ("0xdrop".to_string(), market("0xdrop", "drop-token")),
            ]),
            true,
        )
        .unwrap();
        let frame = |condition_id: &str, token_id: &str, timestamp: i64| {
            let message = serde_json::json!({
                "event_type": "book",
                "market": condition_id,
                "asset_id": token_id,
                "timestamp": timestamp.to_string(),
                "bids": [{"price": "0.41", "size": "10"}],
                "asks": [{"price": "0.43", "size": "8"}],
            });
            serde_json::json!({
                "ts_received_ms": timestamp + 1,
                "raw": serde_json::to_string(&message).unwrap(),
            })
        };
        std::fs::write(
            raw_dir.join("market_ws_frames.jsonl"),
            format!(
                "{}\n{}\n",
                frame("0xkeep", "keep-token", 1_782_898_923_000),
                frame("0xdrop", "drop-token", 1_782_898_923_500),
            ),
        )
        .unwrap();

        cmd_convert_recorded_btc_books(
            raw_dir.to_str().unwrap(),
            output_dir.to_str().unwrap(),
            &["0xkeep".to_string()],
        )
        .unwrap();

        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output_dir.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest["selection"]["filtered_to_condition_ids"], true);
        assert_eq!(manifest["selection"]["source_market_count"], 2);
        assert_eq!(manifest["selection"]["selected_market_count"], 1);
        assert_eq!(manifest["selection"]["selected_condition_ids"][0], "0xkeep");
        assert!(manifest["markets"].get("0xkeep").is_some());
        assert!(manifest["markets"].get("0xdrop").is_none());
        assert_eq!(manifest["stats"]["book_events"], 1);
        assert_eq!(manifest["stats"]["filtered_out_events"], 1);
        assert_eq!(manifest["stats"]["skipped_unknown_market"], 0);
        assert_eq!(manifest["stats"]["skipped_unknown_token"], 0);

        let err = cmd_convert_recorded_btc_books(
            raw_dir.to_str().unwrap(),
            tmp.path().join("unknown").to_str().unwrap(),
            &["0xmissing".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("absent from gamma market cache"));
    }

    #[test]
    fn terminal_direction_requires_closed_terminal_market() {
        let mut market = data::models::Market {
            condition_id: "0xabc".into(),
            question: "Bitcoin Up or Down".into(),
            slug: "btc-updown-5m-1".into(),
            closed: true,
            outcomes: vec![
                data::models::Outcome {
                    token_id: "up".into(),
                    name: "Up".into(),
                    price: 1.0,
                },
                data::models::Outcome {
                    token_id: "down".into(),
                    name: "Down".into(),
                    price: 0.0,
                },
            ],
            ..Default::default()
        };

        assert_eq!(terminal_direction_from_market(&market), Some("up".into()));
        market.closed = false;
        assert_eq!(terminal_direction_from_market(&market), None);
        market.closed = true;
        market.outcomes[0].price = 0.5;
        market.outcomes[1].price = 0.5;
        assert_eq!(terminal_direction_from_market(&market), None);
    }

    #[test]
    fn recorded_btc_slug_window_parses_supported_windows() {
        assert_eq!(
            recorded_btc_slug_window("btc-updown-5m-1782904500"),
            Some((1782904500, 1782904800, 300))
        );
        assert_eq!(
            recorded_btc_slug_window("btc-updown-15m-1782904500"),
            Some((1782904500, 1782905400, 900))
        );
        assert_eq!(recorded_btc_slug_window("eth-updown-5m-1782904500"), None);
    }

    #[test]
    fn recorded_btc_direction_is_fail_closed_for_missing_prices() {
        assert_eq!(recorded_btc_direction(100.0, 101.0), Some("up".into()));
        assert_eq!(recorded_btc_direction(100.0, 99.0), Some("down".into()));
        assert_eq!(recorded_btc_direction(100.0, 100.0), Some("tie".into()));
        assert_eq!(recorded_btc_direction(0.0, 100.0), None);
        assert_eq!(recorded_btc_direction(100.0, 0.0), None);
    }

    #[test]
    fn recorded_resolution_source_kind_detects_chainlink_btc_streams() {
        assert_eq!(
            recorded_resolution_source_kind(
                "https://data.chain.link/streams/btc-usd",
                "Resolution uses Chainlink BTC/USD data stream"
            ),
            "chainlink_btc_usd_data_stream"
        );
        assert_eq!(recorded_resolution_source_kind("", ""), "unknown");
    }

    #[test]
    fn chainlink_feed_id_shape_requires_hex_stream_id() {
        assert!(looks_like_chainlink_feed_id(
            "0x000359843a543ee2fe414dc14c7e7920ef10f4372990b79d6361cdc0dd1ba782"
        ));
        assert!(!looks_like_chainlink_feed_id(
            "11111111-2222-3333-4444-555555555555"
        ));
        assert!(!looks_like_chainlink_feed_id("0xnot-hex"));
    }

    #[test]
    fn recorded_btc_settlement_source_kind_keeps_binance_proxy_fail_closed() {
        let binance = serde_json::json!({"kind": "binance_public_klines"});
        assert_eq!(
            recorded_btc_settlement_source_kind(&binance, "auto"),
            "binance_btcusdt_klines"
        );
        let csv = serde_json::json!({"kind": "csv"});
        assert_eq!(
            recorded_btc_settlement_source_kind(&csv, "auto"),
            "csv_unclassified"
        );
        assert_eq!(
            recorded_btc_settlement_source_kind(&csv, "chainlink_btc_usd_data_stream"),
            "csv_source_claim_mismatch"
        );
        let verified_chainlink = serde_json::json!({
            "kind": "csv",
            "provenance": {
                "detected_settlement_source_kind": "chainlink_btc_usd_data_stream"
            }
        });
        assert_eq!(
            recorded_btc_settlement_source_kind(&verified_chainlink, "auto"),
            "chainlink_btc_usd_data_stream"
        );
        assert_eq!(
            recorded_btc_settlement_source_kind(
                &verified_chainlink,
                "chainlink_btc_usd_data_stream"
            ),
            "chainlink_btc_usd_data_stream"
        );
    }

    #[test]
    fn recorded_btc_csv_provenance_scans_the_source_column() {
        use std::io::Write;

        let mut csv = tempfile::NamedTempFile::new().unwrap();
        writeln!(csv, "timestamp_ms,source,price,received_at_ms").unwrap();
        writeln!(
            csv,
            "1780000000000,chainlink_btc_usd_data_stream,67500.25,1780000000010"
        )
        .unwrap();

        let provenance = inspect_recorded_btc_csv_provenance(csv.path().to_str().unwrap())
            .expect("inspect provenance");
        assert_eq!(
            provenance["detected_settlement_source_kind"].as_str(),
            Some("chainlink_btc_usd_data_stream")
        );
        assert_eq!(
            provenance["official_chainlink_provenance_ready"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn recorded_settlement_source_matching_requires_explicit_official_kind() {
        assert!(recorded_settlement_source_matches(
            "chainlink_btc_usd_data_stream",
            "chainlink_btc_usd_data_stream"
        ));
        assert!(!recorded_settlement_source_matches(
            "chainlink_btc_usd_data_stream",
            "binance_btcusdt_klines"
        ));
        assert!(!recorded_settlement_source_matches(
            "unknown",
            "chainlink_btc_usd_data_stream"
        ));
    }

    #[test]
    fn selectivity_parser_allows_multi_value_dimensions() {
        let filter = parse_selectivity_filter(
            &["direction=up".to_string()],
            &[
                "regime=zone=early|dir=up".to_string(),
                "regime=zone=primary|dir=up".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(
            filter.require_tags.get("direction").map(String::as_str),
            Some("up")
        );
        let regimes = filter.deny_tag_values.get("regime").unwrap();
        assert!(regimes.contains("zone=early|dir=up"));
        assert!(regimes.contains("zone=primary|dir=up"));
        assert!(!filter.deny_tags.contains_key("regime"));
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
        settings.max_position_per_market_usd = 20.0;
        settings.candle_max_price = 0.90;
        settings.live_min_order_size_shares = 5.0;

        let balances = wallet_balances(100.0, 100.0, 100.0);

        assert_eq!(live_required_wallet_usd(&settings, &balances), 11.0);
    }

    #[test]
    fn live_required_wallet_usd_rejects_sub_minimum_canary_budget() {
        let mut settings = config::Settings::from_env();
        settings.bankroll_usd = 1.0;
        settings.candle_position_pct = 0.10;
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
        settings.max_position_per_market_usd = 20.0;
        settings.candle_max_price = 0.90;
        settings.live_min_order_size_shares = 5.0;

        let balances = wallet_balances(1.0, 1.0, 1.0);
        let required = live_required_wallet_usd(&settings, &balances);

        assert_eq!(required, 11.0);
        assert!(!live_wallet_covers_budget(&balances, required));
    }

    #[tokio::test]
    async fn causal_policy_replay_plan_generates_rolling_manifest_from_runtime_tags() {
        let tmp = tempfile::tempdir().unwrap();
        let search_path = tmp.path().join("policy_search.json");
        let out_dir = tmp.path().join("replay_plan");
        let output = tmp.path().join("summary.json");
        let search = serde_json::json!({
            "ok": true,
            "report_count": 3,
            "candidates": [
                {
                    "rank": 1,
                    "passed": true,
                    "base_require": {"book_age": "lte_100ms"},
                    "final_policy": {
                        "harness_require_args": ["book_age=lte_100ms"],
                        "harness_deny_args": ["book_imbalance=strong_positive"]
                    },
                    "fold_forward": {
                        "eligible_reports": 1,
                        "stats": {"total_pnl": 6.0}
                    },
                    "notes": ["static stats need replay"]
                }
            ]
        });
        write_json_atomic(&search_path, &search, true).unwrap();

        let summary = run_causal_policy_replay_plan(CausalPolicyReplayPlanInput {
            search_path,
            start: "2026-05-31T08:00:00Z".to_string(),
            end: "2026-05-31T15:00:00Z".to_string(),
            out_dir: out_dir.clone(),
            output: Some(output.clone()),
            top: 1,
            include_failed: false,
            cache_root: None,
            btc_csv: Some("/tmp/btc.csv".to_string()),
            bankroll: 100.0,
            latency_ms: 128,
            latency_audit_json: None,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 8,
            max_folds: None,
            profile: "a_plus5m_tail_low_exposure".to_string(),
            zone_mode: "all".to_string(),
            execute: false,
            delete_after_process: true,
            atomic_parquet: true,
            preflight_pmxt_hours: false,
            stop_at_first_missing_hour: false,
            require_full_folds: true,
            min_fold_trades: 1,
            min_fold_target_events: 1,
            min_fold_top_trades: Some(1),
            min_promotion_trades: Some(1),
            min_promotion_daily_trades: Some(1),
            min_promotion_profitable_reports: Some(1),
            min_promotion_losses: Some(0),
            max_cache_gb: 0.0,
            min_neighbor_observations: None,
            min_neighbor_positive_rate: 0.60,
            max_pbo: 0.50,
            min_median_oos_percentile: 0.80,
        })
        .await
        .unwrap();

        assert_eq!(summary["mode"], "dry_run");
        assert_eq!(summary["selected_count"], 1);
        assert!(output.exists());
        let candidate = &summary["candidates"][0];
        assert_eq!(candidate["rank"], 1);
        assert_eq!(
            candidate["harness_require_args"].as_array().unwrap()[0],
            "book_age=lte_100ms"
        );
        assert_eq!(
            candidate["harness_deny_args"].as_array().unwrap()[0],
            "book_imbalance=strong_positive"
        );
        assert!(out_dir
            .join("candidate_rank_001")
            .join("rolling_history_manifest.json")
            .exists());
        let sweep_args = candidate["rolling_history"]["folds"][0]["sweep_args"]
            .as_array()
            .unwrap();
        assert!(sweep_args
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--require-causal-tag")
                && pair[1].as_str() == Some("book_age=lte_100ms")));
        assert!(sweep_args
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--deny-causal-tag")
                && pair[1].as_str() == Some("book_imbalance=strong_positive")));
        assert!(sweep_args
            .iter()
            .any(|arg| arg.as_str() == Some("--atomic-parquet")));
    }

    #[tokio::test]
    async fn causal_policy_replay_plan_uses_evolved_variant_json_exact_path() {
        let tmp = tempfile::tempdir().unwrap();
        let search_path = tmp.path().join("evolution_summary.json");
        let out_dir = tmp.path().join("replay_plan");
        let output = tmp.path().join("summary.json");
        let variant_path = tmp.path().join("candidate_variant.json");
        let variant_path_string = variant_path.display().to_string();
        write_json_atomic(
            &variant_path,
            &backtest::strategies::StrategyVariant::baseline(),
            true,
        )
        .unwrap();
        let search = serde_json::json!({
            "ok": true,
            "report_count": 3,
            "candidates": [
                {
                    "rank": 1,
                    "passed": true,
                    "variant_path": variant_path_string.clone(),
                    "base_require": {"book_age": "lte_100ms"},
                    "final_policy": {
                        "harness_require_args": ["book_age=lte_100ms"],
                        "harness_deny_args": ["book_imbalance=strong_positive"]
                    },
                    "fold_forward": {
                        "eligible_reports": 1,
                        "stats": {"total_pnl": 6.0}
                    },
                    "notes": ["static stats need replay"]
                }
            ]
        });
        write_json_atomic(&search_path, &search, true).unwrap();

        let summary = run_causal_policy_replay_plan(CausalPolicyReplayPlanInput {
            search_path,
            start: "2026-05-31T08:00:00Z".to_string(),
            end: "2026-05-31T15:00:00Z".to_string(),
            out_dir: out_dir.clone(),
            output: Some(output),
            top: 1,
            include_failed: false,
            cache_root: None,
            btc_csv: Some("/tmp/btc.csv".to_string()),
            bankroll: 100.0,
            latency_ms: 128,
            latency_audit_json: None,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 8,
            max_folds: None,
            profile: "a_plus5m_tail_low_exposure".to_string(),
            zone_mode: "all".to_string(),
            execute: false,
            delete_after_process: true,
            atomic_parquet: true,
            preflight_pmxt_hours: false,
            stop_at_first_missing_hour: false,
            require_full_folds: true,
            min_fold_trades: 1,
            min_fold_target_events: 1,
            min_fold_top_trades: Some(1),
            min_promotion_trades: Some(1),
            min_promotion_daily_trades: Some(1),
            min_promotion_profitable_reports: Some(1),
            min_promotion_losses: Some(0),
            max_cache_gb: 0.0,
            min_neighbor_observations: None,
            min_neighbor_positive_rate: 0.60,
            max_pbo: 0.50,
            min_median_oos_percentile: 0.80,
        })
        .await
        .unwrap();

        let candidate = &summary["candidates"][0];
        assert_eq!(
            candidate["variant_json"].as_str(),
            Some(variant_path_string.as_str())
        );
        let sweep_args = candidate["rolling_history"]["folds"][0]["sweep_args"]
            .as_array()
            .unwrap();
        assert!(sweep_args
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--variant-json")
                && pair[1].as_str() == Some(variant_path_string.as_str())));
        assert!(!sweep_args
            .iter()
            .any(|arg| arg.as_str() == Some("--require-causal-tag")));
        assert!(!sweep_args
            .iter()
            .any(|arg| arg.as_str() == Some("--deny-causal-tag")));
        assert!(!sweep_args.iter().any(|arg| arg.as_str() == Some("--conf")));
        assert!(out_dir
            .join("candidate_rank_001")
            .join("rolling_history_manifest.json")
            .exists());
    }

    #[tokio::test]
    async fn rolling_history_variant_json_dry_run_builds_exact_candidate_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let variant_path = tmp.path().join("variant.json");
        let variant_path_string = variant_path.display().to_string();
        let summary = run_rolling_history(RollingHistoryInput {
            start: "2026-05-24T00:00:00Z".to_string(),
            end: "2026-05-24T03:00:00Z".to_string(),
            out_dir: tmp.path().join("rolling_exact"),
            cache_root: None,
            btc_csv: Some("/tmp/btc.csv".to_string()),
            settlement_btc_csv: Some("/tmp/chainlink.csv".to_string()),
            bankroll: 100.0,
            latency_ms: 128,
            latency_audit_json: None,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 2,
            max_folds: None,
            profile: "a_plus5m_tail_low_exposure".to_string(),
            variant_json: Some(variant_path),
            require_causal_tag: Vec::new(),
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
        assert_eq!(
            summary["variant_json"].as_str(),
            Some(variant_path_string.as_str())
        );
        let sweep_args = summary["folds"][0]["sweep_args"].as_array().unwrap();
        assert!(sweep_args
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--variant-json")
                && pair[1].as_str() == Some(variant_path_string.as_str())));
        assert!(!sweep_args.iter().any(|arg| arg.as_str() == Some("--conf")));
        assert!(!sweep_args
            .iter()
            .any(|arg| arg.as_str() == Some("--also-maker")));
        assert!(!sweep_args
            .iter()
            .any(|arg| arg.as_str() == Some("--require-causal-tag")));
        assert!(sweep_args.windows(2).any(
            |pair| pair[0].as_str() == Some("--latency-ms") && pair[1].as_str() == Some("128")
        ));
        assert!(sweep_args.windows(2).any(|pair| {
            pair[0].as_str() == Some("--settlement-btc-csv")
                && pair[1].as_str() == Some("/tmp/chainlink.csv")
        }));
        assert_eq!(summary["promotion_policy"]["min_neighbor_count"], 0);
        assert_eq!(
            summary["promotion_policy"]["min_neighbor_positive_rate"],
            0.0
        );
        let promote_args = summary["promotion_args"].as_array().unwrap();
        assert!(promote_args.windows(2).any(|pair| {
            pair[0].as_str() == Some("--min-neighbor-count") && pair[1].as_str() == Some("0")
        }));
        assert!(promote_args.windows(2).any(|pair| {
            pair[0].as_str() == Some("--min-neighbor-positive-rate")
                && pair[1].as_str() == Some("0")
        }));
    }

    #[tokio::test]
    async fn rolling_history_dry_run_builds_fold_manifest() {
        let summary = run_rolling_history(RollingHistoryInput {
            start: "2026-05-24T00:00:00Z".to_string(),
            end: "2026-05-24T03:00:00Z".to_string(),
            out_dir: std::path::PathBuf::from("/tmp/poly_rolling_test"),
            cache_root: None,
            btc_csv: Some("/tmp/btc.csv".to_string()),
            settlement_btc_csv: None,
            bankroll: 100.0,
            latency_ms: 50,
            latency_audit_json: None,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 2,
            max_folds: None,
            profile: "a_plus5m".to_string(),
            variant_json: None,
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
        assert_eq!(summary["promotion_policy"]["min_neighbor_count"], 2);
        assert_eq!(
            summary["promotion_policy"]["min_neighbor_positive_rate"],
            0.60
        );
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
        assert!(summary["zone_audit_output"]
            .as_str()
            .unwrap()
            .ends_with(".zone_audit.json"));
        assert!(summary["zone_audit_args"]
            .as_array()
            .unwrap()
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--max-zone-trade-share")
                && pair[1].as_str() == Some("1.0")));
        assert!(summary["zone_audit_args"]
            .as_array()
            .unwrap()
            .windows(2)
            .any(
                |pair| pair[0].as_str() == Some("--min-zone-pnl") && pair[1].as_str() == Some("0")
            ));
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
        assert_eq!(summary["latency_policy"]["effective_latency_ms"], 50);
        assert!(
            summary["folds"][0]["hydrate_args"]
                .as_array()
                .unwrap()
                .windows(2)
                .any(|pair| pair[0].as_str() == Some("--latency-ms")
                    && pair[1].as_str() == Some("50"))
        );
        assert!(
            summary["folds"][0]["sweep_args"]
                .as_array()
                .unwrap()
                .windows(2)
                .any(|pair| pair[0].as_str() == Some("--latency-ms")
                    && pair[1].as_str() == Some("50"))
        );
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
            settlement_btc_csv: None,
            bankroll: 100.0,
            latency_ms: 50,
            latency_audit_json: None,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 2,
            max_folds: None,
            profile: "a_plus5m_down_reversion_guard_neighbors".to_string(),
            variant_json: None,
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
        assert!(summary["zone_audit_args"]
            .as_array()
            .unwrap()
            .windows(2)
            .any(|pair| pair[0].as_str() == Some("--max-zone-trade-share")
                && pair[1].as_str() == Some("0.70")));
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

    #[test]
    fn rolling_history_tail_guard_profile_has_non_inert_degraded_mode() {
        let profile = rolling_history_profile("a_plus5m_tail_guard").unwrap();

        assert_eq!(profile.position_pct, "0.025");
        assert_eq!(profile.max_total_exposure_usd, "8");
        assert_eq!(profile.degraded_after_losses, "1");
        assert_eq!(profile.degraded_min_z, "1.10");
        assert_eq!(profile.degraded_max_price, "0.75");
        assert_ne!(profile.degraded_min_z, "0.90");
        assert!(profile.taker_only);
        assert!(profile.degraded_force_taker);
    }

    #[test]
    fn rolling_history_tail_challenger_profiles_match_targeted_shapes() {
        let primary = rolling_history_profile("a_plus5m_tail_primary").unwrap();
        assert_eq!(primary.conf, "0.40,0.50");
        assert_eq!(primary.z, "0.70,0.90");
        assert_eq!(primary.max_price, "0.85,0.90");
        assert_eq!(primary.min_reversion_count, "1");
        assert_eq!(primary.max_reversion_count, "2");
        assert_eq!(primary.position_pct, "0.05");
        assert_eq!(primary.max_total_exposure_usd, "8");
        assert!(primary.taker_only);

        let early = rolling_history_profile("a_plus5m_tail_early_reentry").unwrap();
        assert_eq!(early.conf, "0.60,0.70");
        assert_eq!(early.z, "1.10,1.30");
        assert_eq!(early.edge, "0.10,0.15");
        assert_eq!(early.max_price, "0.75");
        assert_eq!(early.position_pct, "0.05");
        assert_eq!(early.max_per_market_usd, "5");
        assert_eq!(early.max_total_exposure_usd, "5");
        assert_eq!(early.degraded_min_z, "1.30");
        assert_eq!(early.degraded_max_price, "0.65");
        assert!(early.taker_only);

        let low_exposure = rolling_history_profile("a_plus5m_tail_low_exposure").unwrap();
        assert_eq!(low_exposure.conf, "0.50");
        assert_eq!(low_exposure.z, "0.70,0.90");
        assert_eq!(low_exposure.max_price, "0.85,0.90");
        assert_eq!(low_exposure.position_pct, "0.05");
        assert_eq!(low_exposure.max_per_market_usd, "5");
        assert_eq!(low_exposure.max_total_exposure_usd, "5");
        assert!(low_exposure.taker_only);
    }

    #[tokio::test]
    async fn rolling_history_require_full_folds_drops_partial_tail() {
        let summary = run_rolling_history(RollingHistoryInput {
            start: "2026-05-24T00:00:00Z".to_string(),
            end: "2026-05-24T04:00:00Z".to_string(),
            out_dir: std::path::PathBuf::from("/tmp/poly_rolling_full_fold_test"),
            cache_root: None,
            btc_csv: None,
            settlement_btc_csv: None,
            bankroll: 100.0,
            latency_ms: 50,
            latency_audit_json: None,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 2,
            max_folds: None,
            profile: "a_plus5m".to_string(),
            variant_json: None,
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
