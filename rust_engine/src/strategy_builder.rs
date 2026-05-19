//! Strategy-builder orchestration and audit helpers.
//!
//! This module does not invent a new research engine. It makes the existing
//! stages explicit and reproducible: cached PMXT harness sweep, aggregate
//! promotion, cached live-replay parity, and session diagnostics.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, Utc};
use serde::Serialize;

use crate::backtest::experiment::{self, PromotionArtifact};
use crate::backtest::strategies::StrategyVariant;
use crate::monitoring::diagnostics;
use crate::strategy::spec::stable_json_hash;

#[derive(Debug, Clone)]
pub struct StrategyBuilderPlanInput {
    pub start: String,
    pub end: Option<String>,
    pub out_dir: PathBuf,
    pub cache_dir: Option<String>,
    pub btc_csv: Option<String>,
    pub bankroll: f64,
    pub latency_ms: u64,
    pub threads: usize,
    pub window_minutes: f64,
    pub profile: String,
    pub promotion_output: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderAuditInput {
    pub report_paths: Vec<String>,
    pub promotion_artifact: Option<String>,
    pub replay_sessions: Vec<String>,
    pub min_trades: usize,
    pub min_win_rate: f64,
    pub min_wilson_win_rate_lower: f64,
    pub min_total_pnl: f64,
    pub min_shadow_resolutions: u64,
    pub min_research_reports: usize,
    pub min_replay_sessions: usize,
    pub a_plus_min_shadow_resolutions: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderPlan {
    pub schema_version: u32,
    pub profile: String,
    pub start: String,
    pub end: String,
    pub out_dir: String,
    pub window_minutes: f64,
    pub stages: Vec<StrategyBuilderStage>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderStage {
    pub name: String,
    pub purpose: String,
    pub command: String,
    pub outputs: Vec<String>,
    pub verify: Vec<String>,
    pub resource_policy: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderAudit {
    pub schema_version: u32,
    pub ok: bool,
    pub a_plus_ready: bool,
    pub grade: String,
    pub checks: Vec<StrategyBuilderCheck>,
    pub next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyBuilderCheck {
    pub name: String,
    pub status: StrategyBuilderCheckStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyBuilderCheckStatus {
    Ok,
    Warn,
    Fail,
}

pub fn build_plan(input: StrategyBuilderPlanInput) -> Result<StrategyBuilderPlan> {
    let start = parse_rfc3339(&input.start, "--start")?;
    let end = match &input.end {
        Some(end) => parse_rfc3339(end, "--end")?,
        None => start,
    };
    if end < start {
        bail!("--end must be >= --start");
    }
    if input.window_minutes <= 0.0 {
        bail!("--window-minutes must be > 0");
    }

    let out_dir = input.out_dir;
    let reports_dir = out_dir.join("reports");
    let checkpoint_dir = out_dir.join("checkpoints");
    let replay_dir = out_dir.join("live_replay_sessions");
    let replay_report = out_dir.join("live_replay_report.json");
    let promotion_output = input
        .promotion_output
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            out_dir.join(format!(
                "promotion_{}_{}.json",
                compact_stamp(start),
                compact_stamp(end)
            ))
        });

    let profile = StrategyBuilderProfile::from_name(&input.profile)?;
    let windows = daily_windows(start, end)?;
    let mut stages = Vec::new();
    let mut report_paths = Vec::new();

    for (idx, (day_start, day_end)) in windows.iter().enumerate() {
        let date = date_stamp(day_start.date_naive());
        let report_path = reports_dir.join(format!("harness_sweep_{date}.json"));
        let checkpoint = checkpoint_dir.join(date);
        report_paths.push(report_path.clone());

        let mut args = vec![
            "polymomentum-engine".to_string(),
            "harness-sweep".to_string(),
            "--start".to_string(),
            day_start.to_rfc3339(),
            "--end".to_string(),
            day_end.to_rfc3339(),
            "--bankroll".to_string(),
            money_arg(input.bankroll),
            "--latency-ms".to_string(),
            input.latency_ms.to_string(),
            "--window-minutes".to_string(),
            float_arg(input.window_minutes),
            "--conf".to_string(),
            profile.conf.to_string(),
            "--z".to_string(),
            profile.z.to_string(),
            "--edge".to_string(),
            profile.edge.to_string(),
            format!("--ev-buffer={}", profile.ev_buffer),
            format!("--min-price={}", profile.min_price),
            format!("--max-price={}", profile.max_price),
            format!("--settlement-floor={}", profile.settlement_floor),
            format!(
                "--settlement-guard-minutes={}",
                profile.settlement_guard_minutes
            ),
            format!(
                "--settlement-sigma-buffer={}",
                profile.settlement_sigma_buffer
            ),
            format!("--micro-max-spread={}", profile.micro_max_spread),
            format!("--micro-min-depth={}", profile.micro_min_depth),
            format!("--micro-min-pressure={}", profile.micro_min_pressure),
            "--also-maker".to_string(),
            profile.also_maker.to_string(),
            "--threads".to_string(),
            input.threads.to_string(),
            "--checkpoint".to_string(),
            checkpoint.display().to_string(),
            "--report-json".to_string(),
            report_path.display().to_string(),
        ];
        if let Some(cache_dir) = &input.cache_dir {
            args.extend(["--cache-dir".to_string(), cache_dir.clone()]);
        }
        if let Some(btc_csv) = &input.btc_csv {
            args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
        }
        stages.push(StrategyBuilderStage {
            name: format!("backtest_sweep_{}", idx + 1),
            purpose: "Find parameter candidates on one out-of-sample daily slice.".to_string(),
            command: shell_command(&args),
            outputs: vec![report_path.display().to_string()],
            verify: vec![
                "report JSON exists and data_manifest.complete=true".to_string(),
                "selected candidates have nonzero trades and no unresolved fills".to_string(),
            ],
            resource_policy:
                "Run on a dev box; on the 2-core VPS keep --threads 1 and avoid concurrent heavy scans."
                    .to_string(),
        });
    }

    let mut promote_args = vec![
        "polymomentum-engine".to_string(),
        "experiment".to_string(),
        "aggregate-promote".to_string(),
    ];
    for report in &report_paths {
        promote_args.extend(["--report".to_string(), report.display().to_string()]);
    }
    promote_args.extend([
        "--output".to_string(),
        promotion_output.display().to_string(),
        "--min-trades".to_string(),
        "750".to_string(),
        "--min-losses".to_string(),
        "50".to_string(),
        "--min-zone-count".to_string(),
        "2".to_string(),
        "--min-win-rate".to_string(),
        "0.63".to_string(),
        "--min-wilson-win-rate-lower".to_string(),
        "0.60".to_string(),
        "--min-total-pnl".to_string(),
        "250".to_string(),
        "--min-sharpe-like".to_string(),
        "0.02".to_string(),
        "--max-zone-trade-share".to_string(),
        "0.85".to_string(),
        "--min-reports".to_string(),
        windows.len().to_string(),
        "--min-profitable-reports".to_string(),
        windows.len().to_string(),
        "--min-daily-trades".to_string(),
        "50".to_string(),
        "--min-daily-pnl".to_string(),
        "50".to_string(),
    ]);
    stages.push(StrategyBuilderStage {
        name: "aggregate_promote".to_string(),
        purpose:
            "Promote only a candidate that survives every daily slice and strong robustness gates."
                .to_string(),
        command: shell_command(&promote_args),
        outputs: vec![promotion_output.display().to_string()],
        verify: vec![
            "promotion artifact params hash matches strategy_params".to_string(),
            "promotion gate and risk_notes are acceptable for paper shadow".to_string(),
        ],
        resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
    });

    let mut replay_args = vec![
        "polymomentum-engine".to_string(),
        "live-replay".to_string(),
        "--start".to_string(),
        start.to_rfc3339(),
        "--end".to_string(),
        end.to_rfc3339(),
        "--bankroll".to_string(),
        money_arg(input.bankroll),
        "--latency-ms".to_string(),
        input.latency_ms.to_string(),
        "--window-minutes".to_string(),
        float_arg(input.window_minutes),
        "--promotion-artifact".to_string(),
        promotion_output.display().to_string(),
        "--session-log-dir".to_string(),
        replay_dir.display().to_string(),
        "--allow-gamma-fetch".to_string(),
        "--report-json".to_string(),
        replay_report.display().to_string(),
    ];
    if let Some(cache_dir) = &input.cache_dir {
        replay_args.extend(["--cache-dir".to_string(), cache_dir.clone()]);
    }
    if let Some(btc_csv) = &input.btc_csv {
        replay_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
    } else {
        replay_args.extend([
            "--btc-csv".to_string(),
            "<required-btc-tick-csv>".to_string(),
        ]);
    }
    stages.push(StrategyBuilderStage {
        name: "cached_live_replay_shadow".to_string(),
        purpose:
            "Replay the promoted strategy through the live decision path and the settlement-shadow resolver."
                .to_string(),
        command: shell_command(&replay_args),
        outputs: vec![
            replay_report.display().to_string(),
            replay_dir.display().to_string(),
        ],
        verify: vec![
            "live-replay report has shadow_resolutions > 0".to_string(),
            "session diagnostics have oracle.checks >= shadow.resolved and zero disagreements".to_string(),
        ],
        resource_policy:
            "Can be short on the VPS, but full sweeps/replays should still run on a dev box first."
                .to_string(),
    });

    let diagnostic_session = format!(
        "$(jq -r .session_path {})",
        shell_quote_path(&replay_report)
    );
    stages.push(StrategyBuilderStage {
        name: "diagnostics_gate".to_string(),
        purpose: "Turn the replay session into a machine-readable promotion gate.".to_string(),
        command: shell_command(&[
            "polymomentum-engine".to_string(),
            "diagnostics".to_string(),
            "session".to_string(),
            diagnostic_session,
        ]),
        outputs: Vec::new(),
        verify: vec![
            "diagnostics ok=true".to_string(),
            "warnings are explainable; settlement-shadow warning is expected until live gate flips"
                .to_string(),
        ],
        resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
    });

    stages.push(StrategyBuilderStage {
        name: "paper_preflight".to_string(),
        purpose: "Verify the promoted artifact and runtime configuration before paper deployment."
            .to_string(),
        command: shell_command(&[
            "polymomentum-engine".to_string(),
            "preflight".to_string(),
            "--mode".to_string(),
            "paper".to_string(),
            "--promotion-artifact".to_string(),
            promotion_output.display().to_string(),
        ]),
        outputs: Vec::new(),
        verify: vec![
            "preflight ok=true".to_string(),
            "release manifest records the promoted strategy hash".to_string(),
        ],
        resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
    });

    Ok(StrategyBuilderPlan {
        schema_version: 1,
        profile: profile.name.to_string(),
        start: start.to_rfc3339(),
        end: end.to_rfc3339(),
        out_dir: out_dir.display().to_string(),
        window_minutes: input.window_minutes,
        stages,
        notes: vec![
            "The builder uses cached PMXT + BTC data to find candidates, then live-replay to mirror paper shadow before any live gate changes.".to_string(),
            "Do not run CPU-heavy sweeps on the multi-tenant VPS; run them on the dev box and copy artifacts over.".to_string(),
            "Only flip CANDLE_SETTLEMENT_ALIGNMENT_READY after replay and paper sessions both show oracle agreement on resolved shadow candidates.".to_string(),
        ],
    })
}

pub fn audit(input: StrategyBuilderAuditInput) -> StrategyBuilderAudit {
    let mut checks = Vec::new();
    let report_count = input.report_paths.len().max(1);
    let per_report_min_trades = (input.min_trades / report_count).max(1);
    let per_report_min_pnl = input.min_total_pnl / report_count as f64;

    for report_path in &input.report_paths {
        match experiment::read_report(report_path) {
            Ok(report) => {
                checks.push(check(
                    "report.load",
                    StrategyBuilderCheckStatus::Ok,
                    format!(
                        "{} variants={} complete={}",
                        report_path,
                        report.variants.len(),
                        report.data_manifest.complete
                    ),
                ));
                if report.data_manifest.complete {
                    checks.push(check(
                        "report.data_manifest",
                        StrategyBuilderCheckStatus::Ok,
                        format!("{report_path} complete data manifest"),
                    ));
                } else {
                    checks.push(check(
                        "report.data_manifest",
                        StrategyBuilderCheckStatus::Fail,
                        format!("{report_path} incomplete data manifest"),
                    ));
                }
                if let Some(best) = report.variants.first() {
                    let wilson = wilson_lower(best.wins, best.trades);
                    let status = if best.trades >= per_report_min_trades
                        && best.win_rate >= input.min_win_rate
                        && wilson >= input.min_wilson_win_rate_lower
                        && best.total_pnl >= per_report_min_pnl
                        && best.unresolved_fills == 0
                    {
                        StrategyBuilderCheckStatus::Ok
                    } else {
                        StrategyBuilderCheckStatus::Warn
                    };
                    checks.push(check(
                        "report.best_variant",
                        status,
                        format!(
                            "{} trades={} win_rate={:.3} wilson95={:.3} pnl={:.2} unresolved={} per_report_gates[min_trades={}, min_pnl={:.2}]",
                            report_path,
                            best.trades,
                            best.win_rate,
                            wilson,
                            best.total_pnl,
                            best.unresolved_fills,
                            per_report_min_trades,
                            per_report_min_pnl,
                        ),
                    ));
                } else {
                    checks.push(check(
                        "report.best_variant",
                        StrategyBuilderCheckStatus::Fail,
                        format!("{report_path} has no variants"),
                    ));
                }
            }
            Err(e) => checks.push(check(
                "report.load",
                StrategyBuilderCheckStatus::Fail,
                format!("{report_path}: {e:#}"),
            )),
        }
    }
    checks.push(check(
        "a_plus.research_reports",
        if input.report_paths.len() >= input.min_research_reports {
            StrategyBuilderCheckStatus::Ok
        } else {
            StrategyBuilderCheckStatus::Warn
        },
        format!(
            "reports={} min_research_reports={}",
            input.report_paths.len(),
            input.min_research_reports
        ),
    ));

    if let Some(path) = &input.promotion_artifact {
        audit_promotion(path, &input, &mut checks);
    } else {
        checks.push(check(
            "promotion.load",
            StrategyBuilderCheckStatus::Warn,
            "no promotion artifact supplied".to_string(),
        ));
    }

    checks.push(check(
        "a_plus.replay_sessions",
        if input.replay_sessions.len() >= input.min_replay_sessions {
            StrategyBuilderCheckStatus::Ok
        } else {
            StrategyBuilderCheckStatus::Warn
        },
        format!(
            "replay_sessions={} min_replay_sessions={}",
            input.replay_sessions.len(),
            input.min_replay_sessions
        ),
    ));

    for session in &input.replay_sessions {
        match diagnostics::analyze_session(session) {
            Ok(diag) => {
                let shadow = *diag.event_counts.get("shadow.resolved").unwrap_or(&0);
                checks.push(check(
                    "replay.session",
                    if diag.ok {
                        StrategyBuilderCheckStatus::Ok
                    } else {
                        StrategyBuilderCheckStatus::Fail
                    },
                    format!(
                        "{} ok={} events={} shadow={} oracle={} disagreements={} actionable_disagreements={} below_floor_disagreements={} errors={}",
                        session,
                        diag.ok,
                        diag.total_events,
                        shadow,
                        diag.oracle.checks,
                        diag.oracle.disagreements,
                        diag.oracle.actionable_disagreements,
                        diag.oracle.below_floor_disagreements,
                        diag.system.errors
                    ),
                ));
                let status = if shadow >= input.min_shadow_resolutions
                    && diag.oracle.checks >= shadow
                    && diag.oracle.actionable_disagreements == 0
                    && diag.oracle.ties == 0
                    && diag.system.errors == 0
                {
                    StrategyBuilderCheckStatus::Ok
                } else {
                    StrategyBuilderCheckStatus::Fail
                };
                checks.push(check(
                    "replay.shadow_oracle",
                    status,
                    format!(
                        "{} shadow={} min_shadow={} oracle={} ties={} disagreements={} actionable_disagreements={} below_floor_disagreements={} errors={}",
                        session,
                        shadow,
                        input.min_shadow_resolutions,
                        diag.oracle.checks,
                        diag.oracle.ties,
                        diag.oracle.disagreements,
                        diag.oracle.actionable_disagreements,
                        diag.oracle.below_floor_disagreements,
                        diag.system.errors
                    ),
                ));
                checks.push(check(
                    "replay.below_floor_oracle",
                    StrategyBuilderCheckStatus::Ok,
                    format!(
                        "{} below_floor_disagreements={} excluded_from_executable_gate=true",
                        session, diag.oracle.below_floor_disagreements
                    ),
                ));
                checks.push(check(
                    "replay.a_plus_sample",
                    if shadow >= input.a_plus_min_shadow_resolutions {
                        StrategyBuilderCheckStatus::Ok
                    } else {
                        StrategyBuilderCheckStatus::Warn
                    },
                    format!(
                        "{} shadow={} a_plus_min_shadow_resolutions={}",
                        session, shadow, input.a_plus_min_shadow_resolutions
                    ),
                ));
            }
            Err(e) => checks.push(check(
                "replay.session",
                StrategyBuilderCheckStatus::Fail,
                format!("{session}: {e:#}"),
            )),
        }
    }

    if input.replay_sessions.is_empty() {
        checks.push(check(
            "replay.session",
            StrategyBuilderCheckStatus::Warn,
            "no live-replay/paper session supplied".to_string(),
        ));
    }

    let fail_count = checks
        .iter()
        .filter(|c| c.status == StrategyBuilderCheckStatus::Fail)
        .count();
    let warn_count = checks
        .iter()
        .filter(|c| c.status == StrategyBuilderCheckStatus::Warn)
        .count();
    let ok = fail_count == 0;
    let a_plus_ready = ok && warn_count == 0;
    let grade = match (fail_count, warn_count) {
        (0, 0) => "A+",
        (0, 1..=2) => "A-",
        (0, _) => "B",
        (1..=2, _) => "C",
        _ => "D",
    }
    .to_string();
    let next_steps = next_steps(ok, warn_count, a_plus_ready);

    StrategyBuilderAudit {
        schema_version: 1,
        ok,
        a_plus_ready,
        grade,
        checks,
        next_steps,
    }
}

fn audit_promotion(
    path: &str,
    input: &StrategyBuilderAuditInput,
    checks: &mut Vec<StrategyBuilderCheck>,
) {
    match experiment::read_promotion(path) {
        Ok(artifact) => {
            checks.push(check(
                "promotion.load",
                StrategyBuilderCheckStatus::Ok,
                format!(
                    "{} trades={} win_rate={:.3} pnl={:.2}",
                    path, artifact.trades, artifact.win_rate, artifact.total_pnl
                ),
            ));
            let hash_status = promotion_hash_status(&artifact);
            checks.push(check("promotion.params_hash", hash_status.0, hash_status.1));
            let wilson = wilson_lower(
                (artifact.win_rate * artifact.trades as f64).round() as usize,
                artifact.trades,
            );
            let status = if artifact.trades >= input.min_trades
                && artifact.win_rate >= input.min_win_rate
                && wilson >= input.min_wilson_win_rate_lower
                && artifact.total_pnl >= input.min_total_pnl
            {
                StrategyBuilderCheckStatus::Ok
            } else {
                StrategyBuilderCheckStatus::Fail
            };
            checks.push(check(
                "promotion.robustness",
                status,
                format!(
                    "trades={} win_rate={:.3} wilson95~={:.3} pnl={:.2} gates[min_trades={}, min_wr={:.3}, min_wilson={:.3}, min_pnl={:.2}]",
                    artifact.trades,
                    artifact.win_rate,
                    wilson,
                    artifact.total_pnl,
                    input.min_trades,
                    input.min_win_rate,
                    input.min_wilson_win_rate_lower,
                    input.min_total_pnl,
                ),
            ));
        }
        Err(e) => checks.push(check(
            "promotion.load",
            StrategyBuilderCheckStatus::Fail,
            format!("{path}: {e:#}"),
        )),
    }
}

fn promotion_hash_status(artifact: &PromotionArtifact) -> (StrategyBuilderCheckStatus, String) {
    match serde_json::from_value::<StrategyVariant>(artifact.strategy_params.clone()) {
        Ok(variant) => {
            let actual = stable_json_hash(&variant);
            if actual == artifact.selected_strategy.params_hash {
                (
                    StrategyBuilderCheckStatus::Ok,
                    format!("params hash verified: {actual}"),
                )
            } else {
                (
                    StrategyBuilderCheckStatus::Fail,
                    format!(
                        "params hash mismatch: strategy_params {} != selected_strategy {}",
                        actual, artifact.selected_strategy.params_hash
                    ),
                )
            }
        }
        Err(e) => (
            StrategyBuilderCheckStatus::Fail,
            format!("strategy_params do not parse as StrategyVariant: {e}"),
        ),
    }
}

fn next_steps(ok: bool, warn_count: usize, a_plus_ready: bool) -> Vec<String> {
    if !ok {
        return vec![
            "Fix failed checks before promoting or changing paper/live gates.".to_string(),
            "Re-run strategy-builder audit with the new report, promotion, and replay sessions."
                .to_string(),
        ];
    }
    if warn_count > 0 {
        return vec![
            "Review warnings and document why they are acceptable for the current gate.".to_string(),
            "Run a fresh paper-shadow window and compare diagnostics before enabling settlement alignment."
                .to_string(),
        ];
    }
    if a_plus_ready {
        return vec![
            "Begin or continue paper soak on the A+ artifact with diagnostics collection."
                .to_string(),
            "Keep live trading gated until paper soak remains A+ across a fresh resolved sample."
                .to_string(),
        ];
    }
    vec![
        "Run the promoted artifact in paper-shadow mode until multiple resolved oracle checks agree."
            .to_string(),
        "Only then consider setting CANDLE_SETTLEMENT_ALIGNMENT_READY=true for paper execution."
            .to_string(),
    ]
}

fn check(
    name: impl Into<String>,
    status: StrategyBuilderCheckStatus,
    detail: impl Into<String>,
) -> StrategyBuilderCheck {
    StrategyBuilderCheck {
        name: name.into(),
        status,
        detail: detail.into(),
    }
}

fn parse_rfc3339(value: &str, label: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|d| d.with_timezone(&Utc))
        .with_context(|| format!("{label} must be RFC3339"))
}

fn daily_windows(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
    let mut out = Vec::new();
    let mut day = start.date_naive();
    let last_day = end.date_naive();
    loop {
        let day_start = day
            .and_hms_opt(0, 0, 0)
            .context("build day start")?
            .and_utc();
        let day_end = day
            .and_hms_opt(23, 0, 0)
            .context("build day end")?
            .and_utc();
        out.push((start.max(day_start), end.min(day_end)));
        if day >= last_day {
            break;
        }
        day = day
            .checked_add_signed(ChronoDuration::days(1))
            .context("advance day")?;
    }
    Ok(out)
}

#[derive(Debug)]
struct StrategyBuilderProfile {
    name: &'static str,
    conf: &'static str,
    z: &'static str,
    edge: &'static str,
    ev_buffer: &'static str,
    min_price: &'static str,
    max_price: &'static str,
    settlement_floor: &'static str,
    settlement_guard_minutes: &'static str,
    settlement_sigma_buffer: &'static str,
    micro_max_spread: &'static str,
    micro_min_depth: &'static str,
    micro_min_pressure: &'static str,
    also_maker: bool,
}

impl StrategyBuilderProfile {
    fn from_name(name: &str) -> Result<Self> {
        match name {
            "swift5m" => Ok(Self {
                name: "swift5m",
                conf: "0.15,0.25",
                z: "0.10,0.30",
                edge: "0.00,0.02",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.90",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                also_maker: true,
            }),
            "guarded5m" => Ok(Self {
                name: "guarded5m",
                conf: "0.35,0.45,0.55",
                z: "0.50,1.00,1.25",
                edge: "0.02,0.05,0.07",
                ev_buffer: "-1.0,0.05",
                min_price: "0.10",
                max_price: "0.75",
                settlement_floor: "25.0,35.0",
                settlement_guard_minutes: "5.0",
                settlement_sigma_buffer: "0.20",
                micro_max_spread: "0.02",
                micro_min_depth: "20.0",
                micro_min_pressure: "0.0,0.10",
                also_maker: true,
            }),
            "a_plus5m" => Ok(Self {
                name: "a_plus5m",
                conf: "0.35,0.45,0.55",
                z: "0.50,1.00,1.25",
                edge: "0.02,0.05,0.07",
                ev_buffer: "-1.0,0.05",
                min_price: "0.10",
                max_price: "0.75",
                settlement_floor: "25.0,35.0",
                settlement_guard_minutes: "5.0",
                settlement_sigma_buffer: "0.20",
                micro_max_spread: "0.02",
                micro_min_depth: "20.0",
                micro_min_pressure: "0.0,0.10",
                also_maker: true,
            }),
            _ => bail!(
                "unknown strategy-builder profile `{name}`; supported profiles: guarded5m, a_plus5m, swift5m"
            ),
        }
    }
}

fn shell_command(args: &[String]) -> String {
    args.iter()
        .map(|arg| {
            if arg.starts_with("$(") {
                arg.clone()
            } else {
                shell_quote(arg)
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote_path(path: &Path) -> String {
    shell_quote(&path.display().to_string())
}

fn shell_quote(arg: &str) -> String {
    if !arg.is_empty()
        && arg
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_./:=,+".contains(&b))
    {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', "'\\''"))
    }
}

fn compact_stamp(dt: DateTime<Utc>) -> String {
    dt.format("%Y%m%dT%H%M%SZ").to_string()
}

fn date_stamp(day: NaiveDate) -> String {
    format!("{:04}{:02}{:02}", day.year(), day.month(), day.day())
}

fn money_arg(value: f64) -> String {
    format!("{value:.2}")
}

fn float_arg(value: f64) -> String {
    if (value - value.round()).abs() < 1e-9 {
        format!("{value:.0}")
    } else {
        format!("{value}")
    }
}

fn wilson_lower(wins: usize, trades: usize) -> f64 {
    if trades == 0 {
        return 0.0;
    }
    let z = 1.96_f64;
    let n = trades as f64;
    let p = wins as f64 / n;
    let denom = 1.0 + z * z / n;
    let center = p + z * z / (2.0 * n);
    let margin = z * ((p * (1.0 - p) + z * z / (4.0 * n)) / n).sqrt();
    ((center - margin) / denom).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_includes_replay_and_diagnostics_stages() {
        let plan = build_plan(StrategyBuilderPlanInput {
            start: "2026-04-23T00:00:00Z".to_string(),
            end: Some("2026-04-25T23:00:00Z".to_string()),
            out_dir: PathBuf::from("logs/strategy_builder/test"),
            cache_dir: Some("data/pmxt_cache".to_string()),
            btc_csv: Some("data/btc.csv".to_string()),
            bankroll: 100.0,
            latency_ms: 50,
            threads: 4,
            window_minutes: 5.0,
            profile: "guarded5m".to_string(),
            promotion_output: None,
        })
        .unwrap();

        assert_eq!(plan.stages.len(), 7);
        assert!(plan
            .stages
            .iter()
            .any(|s| s.command.contains("live-replay") && s.command.contains("--report-json")));
        assert!(plan
            .stages
            .iter()
            .any(|s| s.command.contains("aggregate-promote")));
    }

    #[test]
    fn wilson_lower_is_conservative() {
        let lower = wilson_lower(560, 813);
        assert!(lower > 0.65 && lower < 0.66);
    }

    #[test]
    fn unknown_profile_is_rejected() {
        let err = StrategyBuilderProfile::from_name("mystery").unwrap_err();
        assert!(err.to_string().contains("unknown strategy-builder profile"));
    }

    #[test]
    fn audit_allows_below_floor_oracle_disagreement() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("session.jsonl");
        let lines = [
            serde_json::json!({
                "cat": "system",
                "type": "release_manifest",
                "mode": "paper",
                "promotion": {
                    "status": "ok",
                    "source_report_hash": "report",
                    "data_manifest_hash": "data",
                    "strategy": {"params_hash": "strategy"}
                }
            }),
            serde_json::json!({
                "cat": "system",
                "type": "runtime_strategy",
                "settlement_alignment_ready": false,
                "settlement_min_abs_move_usd": 25.0,
                "strategy": {"params_hash": "strategy"}
            }),
            serde_json::json!({
                "cat": "shadow",
                "type": "resolved"
            }),
            serde_json::json!({
                "cat": "oracle",
                "type": "resolution",
                "cid": "0xabc",
                "our_actual": "up",
                "polymarket_actual": "down",
                "our_open_btc": 100000.0,
                "our_close_btc": 100011.61,
                "agreed": false
            }),
            serde_json::json!({
                "cat": "signal",
                "type": "evaluation",
                "decision_trade": false,
                "execution_attempted": false,
                "traded": false
            }),
        ];
        let payload = lines
            .into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, payload).unwrap();

        let audit = audit(StrategyBuilderAuditInput {
            report_paths: Vec::new(),
            promotion_artifact: None,
            replay_sessions: vec![path.display().to_string()],
            min_trades: 1,
            min_win_rate: 0.0,
            min_wilson_win_rate_lower: 0.0,
            min_total_pnl: 0.0,
            min_shadow_resolutions: 1,
            min_research_reports: 0,
            min_replay_sessions: 1,
            a_plus_min_shadow_resolutions: 1,
        });

        assert!(audit.ok);
        assert!(audit.checks.iter().any(|c| {
            c.name == "replay.shadow_oracle" && c.status == StrategyBuilderCheckStatus::Ok
        }));
        assert!(audit.checks.iter().any(|c| {
            c.name == "replay.below_floor_oracle" && c.status == StrategyBuilderCheckStatus::Ok
        }));
    }
}
