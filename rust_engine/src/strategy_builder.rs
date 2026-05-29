//! Strategy-builder orchestration and audit helpers.
//!
//! This module does not invent a new research engine. It makes the existing
//! stages explicit and reproducible: one-pass PMXT eval-cache scouting, cached
//! PMXT harness sweep, aggregate promotion, cached live-replay parity, and
//! session diagnostics.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;

use crate::backtest::experiment::{self, PromotionArtifact};
use crate::backtest::strategies::StrategyVariant;
use crate::monitoring::{causality, diagnostics};
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
    pub fold_hours: i64,
    pub profile: String,
    pub zone_mode: String,
    pub promotion_output: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StrategyBuilderAuditInput {
    pub report_paths: Vec<String>,
    pub adaptive_report_paths: Vec<String>,
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
    pub fold_hours: i64,
    pub zone_mode: String,
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

#[derive(Debug, Clone)]
struct CalibrationReportPaths {
    static_report: PathBuf,
    adaptive_report: PathBuf,
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
    if input.fold_hours <= 0 {
        bail!("--fold-hours must be > 0");
    }
    let zone_mode = parse_zone_mode(&input.zone_mode)?;

    let out_dir = input.out_dir;
    let eval_cache_dir = out_dir.join("eval_cache");
    let scout_reports_dir = out_dir.join("scout_reports");
    let reports_dir = out_dir.join("reports");
    let checkpoint_dir = out_dir.join("checkpoints");
    let holdout_replay_dir = out_dir.join("holdout_live_replay_sessions");
    let holdout_replay_reports_dir = out_dir.join("holdout_live_replay_reports");
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
    let windows = feed_forward_windows(start, end, input.fold_hours)?;
    if windows.len() < 2 {
        bail!(
            "feed-forward plan requires at least two folds; got {} window(s). Use a wider --start/--end range or a smaller --fold-hours.",
            windows.len()
        );
    }
    let mut stages = Vec::new();
    let mut calibration_reports = Vec::new();

    for holdout_idx in 1..windows.len() {
        let calibration_idx = holdout_idx - 1;
        let (calibration_start, calibration_end) = windows[calibration_idx];
        let calibration_report = push_calibration_window_stages(
            &mut stages,
            calibration_idx + 1,
            calibration_start,
            calibration_end,
            &eval_cache_dir,
            &scout_reports_dir,
            &reports_dir,
            &checkpoint_dir,
            &profile,
            zone_mode,
            input.cache_dir.as_ref(),
            input.btc_csv.as_ref(),
            input.bankroll,
            input.latency_ms,
            input.threads,
            input.window_minutes,
        );
        calibration_reports.push(calibration_report);

        let (holdout_start, holdout_end) = windows[holdout_idx];
        let fold_promotion = out_dir.join(format!(
            "promotion_ff_fold_{:02}_train_{}_test_{}.json",
            holdout_idx,
            window_stamp(start, calibration_end),
            window_stamp(holdout_start, holdout_end)
        ));
        stages.push(StrategyBuilderStage {
            name: format!("feed_forward_promote_{}", holdout_idx),
            purpose:
                "Select a fixed artifact using only calibration windows that end before the holdout starts."
                    .to_string(),
            command: promotion_command(
                &static_calibration_reports(&calibration_reports),
                &fold_promotion,
                zone_mode,
            ),
            outputs: vec![fold_promotion.display().to_string()],
            verify: vec![
                format!("train_end={} < holdout_start={}", calibration_end.to_rfc3339(), holdout_start.to_rfc3339()),
                "promotion artifact params hash matches strategy_params".to_string(),
            ],
            resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
        });

        let holdout_stamp = window_stamp(holdout_start, holdout_end);
        let replay_report =
            holdout_replay_reports_dir.join(format!("fold_{holdout_idx:02}_{holdout_stamp}.json"));
        let replay_session_dir =
            holdout_replay_dir.join(format!("fold_{holdout_idx:02}_{holdout_stamp}"));
        let mut replay_args = vec![
            "polymomentum-engine".to_string(),
            "live-replay".to_string(),
            "--start".to_string(),
            holdout_start.to_rfc3339(),
            "--end".to_string(),
            holdout_end.to_rfc3339(),
            "--bankroll".to_string(),
            money_arg(input.bankroll),
            "--latency-ms".to_string(),
            input.latency_ms.to_string(),
            "--window-minutes".to_string(),
            float_arg(input.window_minutes),
            "--promotion-artifact".to_string(),
            fold_promotion.display().to_string(),
            "--settlement-alignment-ready".to_string(),
            "--session-log-dir".to_string(),
            replay_session_dir.display().to_string(),
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
            name: format!("feed_forward_holdout_replay_{}", holdout_idx),
            purpose:
                "Test the already-promoted artifact on the next unseen holdout window through the live replay path."
                    .to_string(),
            command: shell_command(&replay_args),
            outputs: vec![
                replay_report.display().to_string(),
                replay_session_dir.display().to_string(),
            ],
            verify: vec![
                "holdout replay uses a promotion artifact trained only on prior windows".to_string(),
                "live-replay runs with --settlement-alignment-ready so executable order mechanics are validated offline".to_string(),
                "live-replay report has resolved fills or shadow resolutions".to_string(),
                "session diagnostics have oracle.checks >= resolved/shadow samples and zero actionable disagreements".to_string(),
                "causality diagnostics prove signal_source <= decision <= order <= fill < market_end".to_string(),
            ],
            resource_policy:
                "Can be short on the VPS, but full feed-forward replays should run on a dev box first."
                    .to_string(),
        });

        let diagnostic_session = format!(
            "$(jq -r .session_path {})",
            shell_quote_path(&replay_report)
        );
        stages.push(StrategyBuilderStage {
            name: format!("feed_forward_diagnostics_{}", holdout_idx),
            purpose: "Turn the holdout replay session into a machine-readable gate.".to_string(),
            command: shell_command(&[
                "polymomentum-engine".to_string(),
                "diagnostics".to_string(),
                "session".to_string(),
                diagnostic_session.clone(),
            ]),
            outputs: Vec::new(),
            verify: vec![
                "diagnostics ok=true".to_string(),
                "warnings are explainable; no oracle disagreement on executable candidates"
                    .to_string(),
            ],
            resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
        });
        stages.push(StrategyBuilderStage {
            name: format!("feed_forward_causality_{}", holdout_idx),
            purpose:
                "Falsify timestamp leakage by auditing order/fill/resolution chronology."
                    .to_string(),
            command: shell_command(&[
                "polymomentum-engine".to_string(),
                "diagnostics".to_string(),
                "causality".to_string(),
                diagnostic_session.clone(),
            ]),
            outputs: Vec::new(),
            verify: vec![
                "causality ok=true".to_string(),
                "no future_signal_source, order_before_decision, fill_after_market_end, or resolution_before_market_end violations".to_string(),
            ],
            resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
        });

        let mut fold_audit_args = vec![
            "polymomentum-engine".to_string(),
            "strategy-builder".to_string(),
            "audit".to_string(),
        ];
        for report in &calibration_reports {
            fold_audit_args.extend([
                "--report".to_string(),
                report.static_report.display().to_string(),
            ]);
            fold_audit_args.extend([
                "--adaptive-report".to_string(),
                report.adaptive_report.display().to_string(),
            ]);
        }
        fold_audit_args.extend([
            "--promotion-artifact".to_string(),
            fold_promotion.display().to_string(),
            "--replay-session".to_string(),
            diagnostic_session,
            "--min-trades".to_string(),
            "750".to_string(),
            "--min-win-rate".to_string(),
            "0.63".to_string(),
            "--min-wilson-win-rate-lower".to_string(),
            "0.60".to_string(),
            "--min-total-pnl".to_string(),
            "250".to_string(),
            "--min-shadow-resolutions".to_string(),
            "50".to_string(),
            "--min-research-reports".to_string(),
            calibration_reports.len().to_string(),
        ]);
        stages.push(StrategyBuilderStage {
            name: format!("feed_forward_audit_{}", holdout_idx),
            purpose:
                "Audit calibration robustness and the future holdout session without selecting on holdout PnL."
                    .to_string(),
            command: shell_command(&fold_audit_args),
            outputs: Vec::new(),
            verify: vec![
                "audit ok=true before treating the fold as validation evidence".to_string(),
                "adaptive.drift is based on the holdout replay session only".to_string(),
                "replay.causality is ok for every replay session".to_string(),
            ],
            resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
        });
    }

    let final_idx = windows.len() - 1;
    let (final_calibration_start, final_calibration_end) = windows[final_idx];
    let final_report = push_calibration_window_stages(
        &mut stages,
        final_idx + 1,
        final_calibration_start,
        final_calibration_end,
        &eval_cache_dir,
        &scout_reports_dir,
        &reports_dir,
        &checkpoint_dir,
        &profile,
        zone_mode,
        input.cache_dir.as_ref(),
        input.btc_csv.as_ref(),
        input.bankroll,
        input.latency_ms,
        input.threads,
        input.window_minutes,
    );
    calibration_reports.push(final_report);

    stages.push(StrategyBuilderStage {
        name: "final_feed_forward_promote".to_string(),
        purpose:
            "Train the deployable artifact on all now-historical windows after feed-forward holdouts pass."
                .to_string(),
        command: promotion_command(
            &static_calibration_reports(&calibration_reports),
            &promotion_output,
            zone_mode,
        ),
        outputs: vec![promotion_output.display().to_string()],
        verify: vec![
            "this artifact is for future integration/live only, not for scoring the historical holdouts"
                .to_string(),
            "promotion artifact params hash matches strategy_params".to_string(),
        ],
        resource_policy: "Lightweight; safe on dev box or VPS.".to_string(),
    });

    stages.push(StrategyBuilderStage {
        name: "runtime_preflight".to_string(),
        purpose:
            "Verify the promoted artifact and non-live runtime configuration before deployment."
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

    let mut audit_args = vec![
        "polymomentum-engine".to_string(),
        "strategy-builder".to_string(),
        "audit".to_string(),
    ];
    for report in &calibration_reports {
        audit_args.extend([
            "--report".to_string(),
            report.static_report.display().to_string(),
        ]);
        audit_args.extend([
            "--adaptive-report".to_string(),
            report.adaptive_report.display().to_string(),
        ]);
    }
    audit_args.extend([
        "--promotion-artifact".to_string(),
        promotion_output.display().to_string(),
        "--replay-session".to_string(),
        "<live-replay-or-venue-integration-session.jsonl>".to_string(),
        "--min-trades".to_string(),
        "750".to_string(),
        "--min-win-rate".to_string(),
        "0.63".to_string(),
        "--min-wilson-win-rate-lower".to_string(),
        "0.60".to_string(),
        "--min-total-pnl".to_string(),
        "250".to_string(),
        "--min-shadow-resolutions".to_string(),
        "50".to_string(),
        "--min-research-reports".to_string(),
        calibration_reports.len().to_string(),
    ]);
    stages.push(StrategyBuilderStage {
        name: "adaptive_health_audit".to_string(),
        purpose:
            "Continuously compare forward replay/integration outcomes against the promoted backtest baseline and flag strategy decay."
                .to_string(),
        command: shell_command(&audit_args),
        outputs: Vec::new(),
        verify: vec![
            "adaptive.drift checks are ok before increasing size".to_string(),
            "any warning starts a rolling re-scout; any failure freezes live promotion".to_string(),
            "replay.causality is ok; otherwise treat the strategy as possibly timestamp-leaked"
                .to_string(),
        ],
        resource_policy: "Lightweight; safe on the VPS after each replay or bounded integration session.".to_string(),
    });

    let rolling_out_dir = out_dir.join("rolling_rescout");
    let rolling_start = format!("<latest-{}m-window-start>", float_arg(input.window_minutes));
    let rolling_end = format!("<latest-{}m-window-end>", float_arg(input.window_minutes));
    let mut rescout_args = vec![
        "polymomentum-engine".to_string(),
        "strategy-builder".to_string(),
        "plan".to_string(),
        "--start".to_string(),
        rolling_start,
        "--end".to_string(),
        rolling_end,
        "--out-dir".to_string(),
        rolling_out_dir.display().to_string(),
        "--bankroll".to_string(),
        money_arg(input.bankroll),
        "--latency-ms".to_string(),
        input.latency_ms.to_string(),
        "--threads".to_string(),
        input.threads.to_string(),
        "--window-minutes".to_string(),
        float_arg(input.window_minutes),
        "--fold-hours".to_string(),
        input.fold_hours.to_string(),
        "--profile".to_string(),
        profile.name.to_string(),
        "--zone-mode".to_string(),
        zone_mode.to_string(),
    ];
    if let Some(cache_dir) = &input.cache_dir {
        rescout_args.extend(["--cache-dir".to_string(), cache_dir.clone()]);
    }
    if let Some(btc_csv) = &input.btc_csv {
        rescout_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
    }
    stages.push(StrategyBuilderStage {
        name: "adaptive_rescout_trigger".to_string(),
        purpose:
            "When drift warnings appear, re-run the same walk-forward loop on the freshest resolved window before touching runtime params."
                .to_string(),
        command: shell_command(&rescout_args),
        outputs: vec![rolling_out_dir.display().to_string()],
        verify: vec![
            "fresh candidate must pass aggregate promotion before replacing the current artifact"
                .to_string(),
            "old artifact remains active until a new artifact passes replay and diagnostics gates"
                .to_string(),
        ],
        resource_policy:
            "Plan is lightweight; execute the generated heavy sweeps on the dev box, not the VPS."
                .to_string(),
    });

    Ok(StrategyBuilderPlan {
        schema_version: 1,
        profile: profile.name.to_string(),
        start: start.to_rfc3339(),
        end: end.to_rfc3339(),
        out_dir: out_dir.display().to_string(),
        window_minutes: input.window_minutes,
        fold_hours: input.fold_hours,
        zone_mode: zone_mode.to_string(),
        stages,
        notes: vec![
            "The builder is feed-forward: every holdout replay uses a promotion artifact selected only from strictly earlier calibration windows.".to_string(),
            "Historical holdout performance is measured by fixed-artifact live-replay, not by selecting the best grid cell on the holdout window.".to_string(),
            "The final promotion artifact is for future integration/live only; it is not reused to score the historical windows that trained it.".to_string(),
            "Zone-specific sweeps keep timing regimes isolated; aggregate promotion must prove the same parameter hash survives the calibration windows available at that point in time.".to_string(),
            "Adaptive operation is reactive, not self-mutating: drift warnings trigger a fresh scout, while runtime keeps the last promoted artifact until a new one passes all gates.".to_string(),
            "Do not run CPU-heavy sweeps on the multi-tenant VPS; run them on the dev box and copy artifacts over.".to_string(),
            "BTC tape coverage is now a hard gate; stale CSVs are rejected instead of producing flat fake resolutions.".to_string(),
            "Use backtest/live-replay validation first; paper mode is only for live venue plumbing that cannot be reproduced offline.".to_string(),
            "Promotion uses robust-promote: hard gates first, then worst-window expectancy, neighbor stability, Wilson lower bound, maker fill reliability, and PBO diagnostics.".to_string(),
            "For broad PMXT history, scan gradually in time-boxed local caches: hydrate one rolling window, emit reports/artifacts, then delete only the parquets downloaded by that session.".to_string(),
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
                    let passive_failures_only =
                        only_passive_execution_failures(&best.reject_reasons);
                    let status = if best.trades >= per_report_min_trades
                        && best.win_rate >= input.min_win_rate
                        && wilson >= input.min_wilson_win_rate_lower
                        && best.total_pnl >= per_report_min_pnl
                        && best.unresolved_fills == 0
                        && (best.fills_failed == 0 || passive_failures_only)
                    {
                        StrategyBuilderCheckStatus::Ok
                    } else {
                        StrategyBuilderCheckStatus::Warn
                    };
                    checks.push(check(
                        "report.best_variant",
                        status,
                        format!(
                            "{} trades={} attempts={} fill_rate={:.3} failed={} passive_failures_only={} win_rate={:.3} wilson95={:.3} pnl={:.2} unresolved={} per_report_gates[min_trades={}, min_pnl={:.2}]",
                            report_path,
                            best.trades,
                            best.execution_attempts,
                            best.fill_rate,
                            best.fills_failed,
                            passive_failures_only,
                            best.win_rate,
                            wilson,
                            best.total_pnl,
                            best.unresolved_fills,
                            per_report_min_trades,
                            per_report_min_pnl,
                        ),
                    ));
                    checks.push(check(
                        "report.best_variant_health",
                        if best.breaker_tripped || best.diagnostics.adaptive_rearms > 0 {
                            StrategyBuilderCheckStatus::Fail
                        } else {
                            StrategyBuilderCheckStatus::Ok
                        },
                        format!(
                            "{} breaker_tripped={} breaker_reason={} adaptive_rearms={} breaker_paused_events={}",
                            report_path,
                            best.breaker_tripped,
                            best.breaker_reason.as_deref().unwrap_or("none"),
                            best.diagnostics.adaptive_rearms,
                            best.diagnostics.breaker_paused_events,
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
    audit_adaptive_probe_reports(&input, &mut checks);

    if let Some(path) = &input.promotion_artifact {
        audit_promotion(path, &input, &mut checks);
        audit_adaptive_drift(path, &input, &mut checks);
    } else {
        checks.push(check(
            "promotion.load",
            StrategyBuilderCheckStatus::Warn,
            "no promotion artifact supplied".to_string(),
        ));
        checks.push(check(
            "adaptive.baseline",
            StrategyBuilderCheckStatus::Warn,
            "no promotion artifact supplied, so forward decay cannot be compared to a locked baseline"
                .to_string(),
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
                let resolved = diag.resolutions.resolved;
                let oracle_samples = shadow.max(resolved);
                checks.push(check(
                    "replay.session",
                    if diag.ok {
                        StrategyBuilderCheckStatus::Ok
                    } else {
                        StrategyBuilderCheckStatus::Fail
                    },
                    format!(
                        "{} ok={} events={} resolved={} shadow={} oracle={} disagreements={} actionable_disagreements={} below_floor_disagreements={} errors={}",
                        session,
                        diag.ok,
                        diag.total_events,
                        resolved,
                        shadow,
                        diag.oracle.checks,
                        diag.oracle.disagreements,
                        diag.oracle.actionable_disagreements,
                        diag.oracle.below_floor_disagreements,
                        diag.system.errors
                    ),
                ));
                let status = if oracle_samples >= input.min_shadow_resolutions
                    && diag.oracle.checks >= oracle_samples
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
                        "{} resolved={} shadow={} samples={} min_samples={} oracle={} ties={} disagreements={} actionable_disagreements={} below_floor_disagreements={} errors={}",
                        session,
                        resolved,
                        shadow,
                        oracle_samples,
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
                    "replay.settlement_alignment",
                    match diag.system.settlement_alignment_ready {
                        Some(true) => StrategyBuilderCheckStatus::Ok,
                        Some(false) | None => StrategyBuilderCheckStatus::Warn,
                    },
                    format!(
                        "{} settlement_alignment_ready={:?}; executable order-path parity requires true",
                        session, diag.system.settlement_alignment_ready
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
                match causality::audit_session(session, causality::CausalityAuditConfig::default())
                {
                    Ok(audit) => checks.push(check(
                        "replay.causality",
                        if audit.ok {
                            StrategyBuilderCheckStatus::Ok
                        } else {
                            StrategyBuilderCheckStatus::Fail
                        },
                        format!(
                            "{} ok={} order_timings={} placed={} filled={} resolution_timings={} violations={}",
                            session,
                            audit.ok,
                            audit.order_timings,
                            audit.order_placed,
                            audit.order_filled,
                            audit.resolution_timings,
                            audit.violations.len()
                        ),
                    )),
                    Err(e) => checks.push(check(
                        "replay.causality",
                        StrategyBuilderCheckStatus::Fail,
                        format!("{session}: {e:#}"),
                    )),
                }
                checks.push(check(
                    "replay.a_plus_sample",
                    if oracle_samples >= input.a_plus_min_shadow_resolutions {
                        StrategyBuilderCheckStatus::Ok
                    } else {
                        StrategyBuilderCheckStatus::Warn
                    },
                    format!(
                        "{} samples={} resolved={} shadow={} a_plus_min_samples={}",
                        session,
                        oracle_samples,
                        resolved,
                        shadow,
                        input.a_plus_min_shadow_resolutions
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
            "no live-replay or bounded integration session supplied".to_string(),
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

fn only_passive_execution_failures(reject_reasons: &BTreeMap<String, usize>) -> bool {
    !reject_reasons.is_empty()
        && reject_reasons
            .keys()
            .all(|reason| matches!(reason.as_str(), "maker_unfilled" | "post_only_cross"))
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

fn audit_adaptive_probe_reports(
    input: &StrategyBuilderAuditInput,
    checks: &mut Vec<StrategyBuilderCheck>,
) {
    if input.adaptive_report_paths.is_empty() {
        checks.push(check(
            "adaptive_probe.reports",
            StrategyBuilderCheckStatus::Warn,
            "no adaptive breaker probe reports supplied; A+ requires checking whether rearm-only runs change the picture"
                .to_string(),
        ));
        return;
    }

    checks.push(check(
        "adaptive_probe.reports",
        StrategyBuilderCheckStatus::Ok,
        format!(
            "adaptive_reports={} static_reports={}",
            input.adaptive_report_paths.len(),
            input.report_paths.len()
        ),
    ));
    for report_path in &input.adaptive_report_paths {
        match experiment::read_report(report_path) {
            Ok(report) => {
                let best = report.variants.first();
                let best_rearms = best.map(|v| v.diagnostics.adaptive_rearms).unwrap_or(0);
                let best_paused = best
                    .map(|v| v.diagnostics.breaker_paused_events)
                    .unwrap_or(0);
                let best_breaker = best.map(|v| v.breaker_tripped).unwrap_or(false);
                let max_rearms = report
                    .variants
                    .iter()
                    .map(|v| v.diagnostics.adaptive_rearms)
                    .max()
                    .unwrap_or(0);
                let max_paused = report
                    .variants
                    .iter()
                    .map(|v| v.diagnostics.breaker_paused_events)
                    .max()
                    .unwrap_or(0);
                let variants_with_rearms = report
                    .variants
                    .iter()
                    .filter(|v| v.diagnostics.adaptive_rearms > 0)
                    .count();
                let status = if best.is_none() || best_breaker || best_rearms > 0 {
                    StrategyBuilderCheckStatus::Fail
                } else if variants_with_rearms > 0 {
                    StrategyBuilderCheckStatus::Warn
                } else {
                    StrategyBuilderCheckStatus::Ok
                };
                checks.push(check(
                    "adaptive_probe.health",
                    status,
                    format!(
                        "{} variants={} best_breaker={} best_adaptive_rearms={} best_paused_events={} variants_with_rearms={} max_adaptive_rearms={} max_paused_events={}",
                        report_path,
                        report.variants.len(),
                        best_breaker,
                        best_rearms,
                        best_paused,
                        variants_with_rearms,
                        max_rearms,
                        max_paused,
                    ),
                ));
            }
            Err(e) => checks.push(check(
                "adaptive_probe.load",
                StrategyBuilderCheckStatus::Fail,
                format!("{report_path}: {e:#}"),
            )),
        }
    }
}

fn audit_adaptive_drift(
    promotion_path: &str,
    input: &StrategyBuilderAuditInput,
    checks: &mut Vec<StrategyBuilderCheck>,
) {
    let artifact = match experiment::read_promotion(promotion_path) {
        Ok(artifact) => artifact,
        Err(_) => return,
    };
    if input.replay_sessions.is_empty() {
        checks.push(check(
            "adaptive.drift",
            StrategyBuilderCheckStatus::Warn,
            "no replay or bounded integration session supplied; adaptive stale checks need resolved forward outcomes"
                .to_string(),
        ));
        return;
    }

    let baseline_avg_pnl = if artifact.trades == 0 {
        0.0
    } else {
        artifact.total_pnl / artifact.trades as f64
    };
    for session in &input.replay_sessions {
        match diagnostics::analyze_session(session) {
            Ok(diag) => {
                let resolved = diag.resolutions.wins + diag.resolutions.losses;
                let (status, reason) = classify_adaptive_drift(
                    artifact.win_rate,
                    baseline_avg_pnl,
                    diag.resolutions.wins,
                    diag.resolutions.losses,
                    diag.resolutions.total_pnl,
                    input.min_shadow_resolutions,
                    input.min_win_rate,
                    diag.risk.breaker_tripped,
                    diag.system.errors,
                );
                checks.push(check(
                    "adaptive.drift",
                    status,
                    format!(
                        "{} baseline_wr={:.3} baseline_avg_pnl={:.4} resolved={} wins={} losses={} pnl={:.2}; {}",
                        session,
                        artifact.win_rate,
                        baseline_avg_pnl,
                        resolved,
                        diag.resolutions.wins,
                        diag.resolutions.losses,
                        diag.resolutions.total_pnl,
                        reason
                    ),
                ));
            }
            Err(e) => checks.push(check(
                "adaptive.drift",
                StrategyBuilderCheckStatus::Fail,
                format!("{session}: {e:#}"),
            )),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn classify_adaptive_drift(
    baseline_win_rate: f64,
    baseline_avg_pnl: f64,
    session_wins: u64,
    session_losses: u64,
    session_pnl: f64,
    min_samples: u64,
    min_win_rate: f64,
    breaker_tripped: bool,
    system_errors: u64,
) -> (StrategyBuilderCheckStatus, String) {
    let resolved = session_wins + session_losses;
    if resolved < min_samples {
        return (
            StrategyBuilderCheckStatus::Warn,
            format!("sample below drift threshold: resolved={resolved} min_samples={min_samples}"),
        );
    }
    if breaker_tripped {
        return (
            StrategyBuilderCheckStatus::Fail,
            "breaker tripped during forward session".to_string(),
        );
    }
    if system_errors > 0 {
        return (
            StrategyBuilderCheckStatus::Fail,
            format!("system errors during forward session: {system_errors}"),
        );
    }

    let session_win_rate = if resolved == 0 {
        0.0
    } else {
        session_wins as f64 / resolved as f64
    };
    let session_avg_pnl = if resolved == 0 {
        0.0
    } else {
        session_pnl / resolved as f64
    };
    let hard_win_floor = min_win_rate.max(baseline_win_rate - 0.20);
    if session_pnl < 0.0 {
        return (
            StrategyBuilderCheckStatus::Fail,
            format!(
                "negative forward pnl: win_rate={session_win_rate:.3} avg_pnl={session_avg_pnl:.4}"
            ),
        );
    }
    if session_win_rate < hard_win_floor {
        return (
            StrategyBuilderCheckStatus::Fail,
            format!(
                "win-rate decay beyond hard floor: win_rate={session_win_rate:.3} floor={hard_win_floor:.3}"
            ),
        );
    }
    if baseline_avg_pnl > 0.0 && session_avg_pnl < baseline_avg_pnl * 0.25 {
        return (
            StrategyBuilderCheckStatus::Fail,
            format!(
                "expectancy decay beyond hard floor: avg_pnl={session_avg_pnl:.4} floor={:.4}",
                baseline_avg_pnl * 0.25
            ),
        );
    }

    let warn_win_floor = baseline_win_rate - 0.12;
    if session_win_rate < warn_win_floor {
        return (
            StrategyBuilderCheckStatus::Warn,
            format!(
                "win-rate decay warning: win_rate={session_win_rate:.3} warn_floor={warn_win_floor:.3}"
            ),
        );
    }
    if baseline_avg_pnl > 0.0 && session_avg_pnl < baseline_avg_pnl * 0.50 {
        return (
            StrategyBuilderCheckStatus::Warn,
            format!(
                "expectancy decay warning: avg_pnl={session_avg_pnl:.4} warn_floor={:.4}",
                baseline_avg_pnl * 0.50
            ),
        );
    }

    (
        StrategyBuilderCheckStatus::Ok,
        format!(
            "forward performance inside adaptive band: win_rate={session_win_rate:.3} avg_pnl={session_avg_pnl:.4}"
        ),
    )
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
            "Fix failed checks before promoting or changing runtime gates.".to_string(),
            "Re-run strategy-builder audit with the new report, promotion, and replay sessions."
                .to_string(),
        ];
    }
    if warn_count > 0 {
        return vec![
            "Review warnings and document why they are acceptable for the current gate.".to_string(),
            "Run fresh feed-forward live-replay/backtest diagnostics before using paper mode for irreducible venue plumbing."
                .to_string(),
        ];
    }
    if a_plus_ready {
        return vec![
            "Begin or continue bounded venue-integration checks on the A+ artifact with diagnostics collection."
                .to_string(),
            "Keep live trading gated until offline replay remains A+ across a fresh resolved sample and venue plumbing is clean."
                .to_string(),
        ];
    }
    vec![
        "Run the promoted artifact through feed-forward live-replay until multiple resolved oracle checks agree."
            .to_string(),
        "Only use paper mode after offline validation, and only for checks that need the real venue."
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

fn parse_zone_mode(value: &str) -> Result<&str> {
    match value {
        "all" | "early" | "primary" | "late" | "terminal" => Ok(value),
        _ => bail!("--zone-mode must be one of: all, early, primary, late, terminal"),
    }
}

fn feed_forward_windows(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    fold_hours: i64,
) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
    let mut out = Vec::new();
    let mut cur = start;
    while cur <= end {
        let inclusive_end = cur + ChronoDuration::hours(fold_hours - 1);
        let window_end = inclusive_end.min(end);
        out.push((cur, window_end));
        cur = window_end + ChronoDuration::hours(1);
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn push_calibration_window_stages(
    stages: &mut Vec<StrategyBuilderStage>,
    idx: usize,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    eval_cache_dir: &Path,
    scout_reports_dir: &Path,
    reports_dir: &Path,
    checkpoint_dir: &Path,
    profile: &StrategyBuilderProfile,
    zone_mode: &str,
    cache_dir: Option<&String>,
    btc_csv: Option<&String>,
    bankroll: f64,
    latency_ms: u64,
    threads: usize,
    window_minutes: f64,
) -> CalibrationReportPaths {
    let stamp = window_stamp(window_start, window_end);
    let eval_cache_path = eval_cache_dir.join(format!("eval_cache_{stamp}.jsonl"));
    let scout_report_path = scout_reports_dir.join(format!("eval_sweep_{stamp}.json"));
    let report_path = reports_dir.join(format!("harness_sweep_{stamp}.json"));
    let checkpoint = checkpoint_dir.join(&stamp);

    let mut eval_args = vec![
        "polymomentum-engine".to_string(),
        "eval-cache".to_string(),
        "--start".to_string(),
        window_start.to_rfc3339(),
        "--end".to_string(),
        window_end.to_rfc3339(),
        "--window-minutes".to_string(),
        float_arg(window_minutes),
        "--output".to_string(),
        eval_cache_path.display().to_string(),
        "--allow-gamma-fetch".to_string(),
    ];
    if let Some(cache_dir) = cache_dir {
        eval_args.extend(["--cache-dir".to_string(), cache_dir.clone()]);
    }
    if let Some(btc_csv) = btc_csv {
        eval_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
    }
    stages.push(StrategyBuilderStage {
        name: format!("calibration_eval_cache_{idx}"),
        purpose:
            "Replay a historical calibration window into live-like signal/resolution rows for candidate scouting."
                .to_string(),
        command: shell_command(&eval_args),
        outputs: vec![eval_cache_path.display().to_string()],
        verify: vec![
            "summary evaluations > 0 and resolutions > 0".to_string(),
            "BTC tape coverage check passes for the full calibration window".to_string(),
        ],
        resource_policy:
            "Run on a dev box for multi-hour windows; safe on VPS only for short diagnostics."
                .to_string(),
    });

    let mut scout_args = vec![
        "polymomentum-engine".to_string(),
        "sweep".to_string(),
        "--session".to_string(),
        eval_cache_path.display().to_string(),
        "--bankroll".to_string(),
        money_arg(bankroll),
        "--position-pct".to_string(),
        profile.position_pct.to_string(),
        "--max-per-market-usd".to_string(),
        profile.max_per_market_usd.to_string(),
        "--min-trades".to_string(),
        "30".to_string(),
        "--grid".to_string(),
        "--zone-mode".to_string(),
        zone_mode.to_string(),
        "--top".to_string(),
        "25".to_string(),
        "--report-json".to_string(),
        scout_report_path.display().to_string(),
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
    ];
    if profile.also_maker {
        scout_args.push("--also-maker".to_string());
    }
    stages.push(StrategyBuilderStage {
        name: format!("calibration_eval_sweep_{idx}"),
        purpose:
            "Search a broad strategy grid over the cached calibration stream before spending full L2 harness time."
                .to_string(),
        command: shell_command(&scout_args),
        outputs: vec![scout_report_path.display().to_string()],
        verify: vec![
            "top variants have enough resolved calibration trades before being trusted".to_string(),
            "candidate parameters are re-run through harness-sweep before promotion".to_string(),
        ],
        resource_policy:
            "CPU-light compared with raw replay; still keep broad multi-window searches on the dev box."
                .to_string(),
    });

    let mut harness_base_args = vec![
        "polymomentum-engine".to_string(),
        "harness-sweep".to_string(),
        "--start".to_string(),
        window_start.to_rfc3339(),
        "--end".to_string(),
        window_end.to_rfc3339(),
        "--bankroll".to_string(),
        money_arg(bankroll),
        "--latency-ms".to_string(),
        latency_ms.to_string(),
        "--window-minutes".to_string(),
        float_arg(window_minutes),
        "--position-pct".to_string(),
        profile.position_pct.to_string(),
        "--max-per-market-usd".to_string(),
        profile.max_per_market_usd.to_string(),
        "--max-total-exposure-usd".to_string(),
        profile.max_total_exposure_usd.to_string(),
        "--max-projected-stressed-drawdown-pct".to_string(),
        profile.max_projected_stressed_drawdown_pct.to_string(),
        "--degraded-after-losses".to_string(),
        profile.degraded_after_losses.to_string(),
        "--degraded-after-drawdown-pct".to_string(),
        profile.degraded_after_drawdown_pct.to_string(),
        "--degraded-min-z".to_string(),
        profile.degraded_min_z.to_string(),
        "--degraded-max-price".to_string(),
        profile.degraded_max_price.to_string(),
        "--zone-mode".to_string(),
        zone_mode.to_string(),
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
        "--threads".to_string(),
        threads.to_string(),
    ];
    if profile.also_maker {
        harness_base_args.push("--also-maker".to_string());
    }
    if profile.degraded_force_taker {
        harness_base_args.push("--degraded-force-taker".to_string());
    }
    if let Some(cache_dir) = cache_dir {
        harness_base_args.extend(["--cache-dir".to_string(), cache_dir.clone()]);
    }
    if let Some(btc_csv) = btc_csv {
        harness_base_args.extend(["--btc-csv".to_string(), btc_csv.clone()]);
    }
    let mut args = harness_base_args.clone();
    args.extend([
        "--checkpoint".to_string(),
        checkpoint.display().to_string(),
        "--report-json".to_string(),
        report_path.display().to_string(),
    ]);
    stages.push(StrategyBuilderStage {
        name: format!("calibration_harness_sweep_{idx}"),
        purpose:
            "Fit candidate parameters on a calibration window that is strictly before any later holdout using it."
                .to_string(),
        command: shell_command(&args),
        outputs: vec![report_path.display().to_string()],
        verify: vec![
            "report JSON exists and data_manifest.complete=true".to_string(),
            "best variant has breaker_tripped=false and diagnostics.adaptive_rearms=0".to_string(),
            "this report is used only for later-window promotion, not as holdout evidence".to_string(),
        ],
        resource_policy:
            "Run on a dev box; on the 2-core VPS keep --threads 1 and avoid concurrent heavy scans."
                .to_string(),
    });

    let adaptive_report_path =
        reports_dir.join(format!("harness_sweep_adaptive_rearm_{stamp}.json"));
    let adaptive_checkpoint = checkpoint_dir.join(format!("{stamp}_adaptive_rearm"));
    let mut adaptive_args = harness_base_args;
    adaptive_args.extend([
        "--checkpoint".to_string(),
        adaptive_checkpoint.display().to_string(),
        "--report-json".to_string(),
        adaptive_report_path.display().to_string(),
        "--adaptive-health-rearm-minutes".to_string(),
        "15".to_string(),
    ]);
    stages.push(StrategyBuilderStage {
        name: format!("calibration_adaptive_breaker_probe_{idx}"),
        purpose:
            "Stress-test whether a calibration window only survives by pausing and rearming the low-win-rate breaker."
                .to_string(),
        command: shell_command(&adaptive_args),
        outputs: vec![adaptive_report_path.display().to_string()],
        verify: vec![
            "diagnostic report is never passed to robust-promote".to_string(),
            "compare diagnostics.adaptive_rearms and breaker_paused_events against the static harness report".to_string(),
            "adaptive_rearms > 0 is research evidence for regime instability, not promotion evidence".to_string(),
        ],
        resource_policy:
            "Heavy diagnostic; run on the dev box unless the window is tiny and --threads 1 on the VPS."
                .to_string(),
    });

    CalibrationReportPaths {
        static_report: report_path,
        adaptive_report: adaptive_report_path,
    }
}

fn static_calibration_reports(reports: &[CalibrationReportPaths]) -> Vec<PathBuf> {
    reports
        .iter()
        .map(|report| report.static_report.clone())
        .collect()
}

fn promotion_command(reports: &[PathBuf], output: &Path, zone_mode: &str) -> String {
    let mut args = vec![
        "polymomentum-engine".to_string(),
        "experiment".to_string(),
        "robust-promote".to_string(),
    ];
    for report in reports {
        args.extend(["--report".to_string(), report.display().to_string()]);
    }
    let min_zone_count = if zone_mode == "all" { "2" } else { "1" };
    let max_zone_trade_share = if zone_mode == "all" { "0.85" } else { "1.0" };
    args.extend([
        "--output".to_string(),
        output.display().to_string(),
        "--min-trades".to_string(),
        "750".to_string(),
        "--min-losses".to_string(),
        "50".to_string(),
        "--min-zone-count".to_string(),
        min_zone_count.to_string(),
        "--min-win-rate".to_string(),
        "0.63".to_string(),
        "--min-wilson-win-rate-lower".to_string(),
        "0.60".to_string(),
        "--min-total-pnl".to_string(),
        "250".to_string(),
        "--min-sharpe-like".to_string(),
        "0.02".to_string(),
        "--max-zone-trade-share".to_string(),
        max_zone_trade_share.to_string(),
        "--min-reports".to_string(),
        reports.len().to_string(),
        "--min-profitable-reports".to_string(),
        reports.len().to_string(),
        "--min-daily-trades".to_string(),
        "50".to_string(),
        "--min-daily-pnl".to_string(),
        "50".to_string(),
        "--min-neighbor-count".to_string(),
        "2".to_string(),
        "--min-neighbor-positive-rate".to_string(),
        "0.60".to_string(),
        "--max-pbo".to_string(),
        "0.50".to_string(),
        "--min-worst-window-pnl".to_string(),
        "0".to_string(),
    ]);
    shell_command(&args)
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
    position_pct: &'static str,
    max_per_market_usd: &'static str,
    max_total_exposure_usd: &'static str,
    max_projected_stressed_drawdown_pct: &'static str,
    degraded_after_losses: &'static str,
    degraded_after_drawdown_pct: &'static str,
    degraded_min_z: &'static str,
    degraded_max_price: &'static str,
    degraded_force_taker: bool,
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
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "0",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.0",
                degraded_max_price: "0.0",
                degraded_force_taker: false,
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
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "0",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.0",
                degraded_max_price: "0.0",
                degraded_force_taker: false,
                also_maker: true,
            }),
            "a_plus5m" => Ok(Self {
                name: "a_plus5m",
                conf: "0.30,0.35,0.40",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.03",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.75,0.90",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "0",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.0",
                degraded_max_price: "0.0",
                degraded_force_taker: false,
                also_maker: true,
            }),
            "a_plus5m_regime" => Ok(Self {
                name: "a_plus5m_regime",
                conf: "0.30,0.35,0.40",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.03",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.75,0.90",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.12,0.16,0.24",
                degraded_after_losses: "0",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.0",
                degraded_max_price: "0.0",
                degraded_force_taker: false,
                also_maker: true,
            }),
            "a_plus5m_adaptive" => Ok(Self {
                name: "a_plus5m_adaptive",
                conf: "0.30,0.35,0.40",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.03",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.75,0.90",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "1,2",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.90",
                degraded_max_price: "0.0",
                degraded_force_taker: true,
                also_maker: true,
            }),
            "a_plus5m_adaptive_price" => Ok(Self {
                name: "a_plus5m_adaptive_price",
                conf: "0.30,0.35,0.40",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.03",
                ev_buffer: "-1.0",
                min_price: "0.10",
                max_price: "0.75,0.90",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "1,2",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.90",
                degraded_max_price: "0.75,0.90",
                degraded_force_taker: true,
                also_maker: true,
            }),
            "a_plus5m_ev_guard" => Ok(Self {
                name: "a_plus5m_ev_guard",
                conf: "0.40",
                z: "0.50,0.70,0.90,1.10",
                edge: "0.03,0.07",
                ev_buffer: "0.05",
                min_price: "0.10",
                max_price: "0.90",
                settlement_floor: "10.0",
                settlement_guard_minutes: "1.0",
                settlement_sigma_buffer: "0.0",
                micro_max_spread: "1.0",
                micro_min_depth: "0.0",
                micro_min_pressure: "-1.0",
                position_pct: "0.05",
                max_per_market_usd: "20",
                max_total_exposure_usd: "15",
                max_projected_stressed_drawdown_pct: "0.24",
                degraded_after_losses: "2",
                degraded_after_drawdown_pct: "0.0",
                degraded_min_z: "0.90",
                degraded_max_price: "0.0",
                degraded_force_taker: true,
                also_maker: true,
            }),
            _ => bail!(
                "unknown strategy-builder profile `{name}`; supported profiles: guarded5m, a_plus5m, a_plus5m_regime, a_plus5m_adaptive, a_plus5m_adaptive_price, a_plus5m_ev_guard, swift5m"
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

fn window_stamp(start: DateTime<Utc>, end: DateTime<Utc>) -> String {
    format!("{}_{}", compact_stamp(start), compact_stamp(end))
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
            fold_hours: 24,
            profile: "guarded5m".to_string(),
            zone_mode: "primary".to_string(),
            promotion_output: None,
        })
        .unwrap();

        assert_eq!(plan.stages.len(), 26);
        assert_eq!(plan.zone_mode, "primary");
        assert_eq!(plan.fold_hours, 24);
        assert!(plan
            .stages
            .iter()
            .any(|s| s.command.contains("eval-cache") && s.command.contains("--window-minutes 5")));
        assert!(plan
            .stages
            .iter()
            .any(|s| s.command.contains("sweep") && s.command.contains("--grid")));
        assert!(plan
            .stages
            .iter()
            .any(|s| s.command.contains("live-replay") && s.command.contains("--report-json")));
        assert!(plan
            .stages
            .iter()
            .any(|s| s.command.contains("robust-promote") && s.command.contains("--max-pbo")));
        assert!(plan.stages.iter().any(|s| {
            s.name.starts_with("calibration_adaptive_breaker_probe_")
                && s.command.contains("--adaptive-health-rearm-minutes 15")
                && s.command.contains("harness_sweep_adaptive_rearm_")
        }));
        assert!(plan
            .stages
            .iter()
            .filter(|s| s.command.contains("harness-sweep"))
            .all(|s| {
                s.command.contains("--also-maker")
                    && s.command.contains("--zone-mode primary")
                    && s.command.contains("--position-pct 0.05")
                    && s.command
                        .contains("--max-projected-stressed-drawdown-pct 0.24")
                    && !s.command.contains("--also-maker true")
            }));
        assert!(plan
            .stages
            .iter()
            .any(|s| s.name == "adaptive_health_audit"));
        assert!(plan.stages.iter().any(|s| {
            s.name.starts_with("feed_forward_causality_")
                && s.command.contains("diagnostics causality")
        }));
        assert!(!plan
            .stages
            .iter()
            .any(|s| s.name == "cached_live_replay_shadow"));

        let fold1_promote = plan
            .stages
            .iter()
            .find(|s| s.name == "feed_forward_promote_1")
            .unwrap();
        assert!(fold1_promote
            .command
            .contains("harness_sweep_20260423T000000Z_20260423T230000Z.json"));
        assert!(!fold1_promote.command.contains("adaptive_rearm"));
        assert!(!fold1_promote
            .command
            .contains("harness_sweep_20260424T000000Z_20260424T230000Z.json"));

        let fold1_replay = plan
            .stages
            .iter()
            .find(|s| s.name == "feed_forward_holdout_replay_1")
            .unwrap();
        assert!(fold1_replay
            .command
            .contains("--start 2026-04-24T00:00:00+00:00"));
        assert!(fold1_replay.command.contains("promotion_ff_fold_01"));
        assert!(fold1_replay
            .command
            .contains("--settlement-alignment-ready"));

        let fold1_audit = plan
            .stages
            .iter()
            .find(|s| s.name == "feed_forward_audit_1")
            .unwrap();
        assert!(fold1_audit.command.contains("--adaptive-report"));
        assert!(fold1_audit
            .command
            .contains("harness_sweep_adaptive_rearm_20260423T000000Z_20260423T230000Z.json"));
    }

    #[test]
    fn plan_rejects_single_fold_to_prevent_lookahead() {
        let err = build_plan(StrategyBuilderPlanInput {
            start: "2026-04-25T10:00:00Z".to_string(),
            end: Some("2026-04-25T17:00:00Z".to_string()),
            out_dir: PathBuf::from("logs/strategy_builder/test"),
            cache_dir: None,
            btc_csv: None,
            bankroll: 100.0,
            latency_ms: 50,
            threads: 1,
            window_minutes: 5.0,
            fold_hours: 24,
            profile: "swift5m".to_string(),
            zone_mode: "primary".to_string(),
            promotion_output: None,
        })
        .unwrap_err();
        assert!(err.to_string().contains("at least two folds"));
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
            adaptive_report_paths: Vec::new(),
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

    #[test]
    fn audit_accepts_executable_resolved_replay_samples() {
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
                "settlement_alignment_ready": true,
                "settlement_min_abs_move_usd": 25.0,
                "strategy": {"params_hash": "strategy"}
            }),
            serde_json::json!({
                "cat": "signal",
                "type": "evaluation",
                "decision_trade": true,
                "execution_attempted": true,
                "traded": false,
                "book_spread": 0.01
            }),
            serde_json::json!({
                "cat": "causality",
                "type": "order_timing",
                "intent_id": "intent_1",
                "condition_id": "0xabc",
                "token_id": "token",
                "signal_source_ts_s": 100.0,
                "decision_ts_s": 100.0,
                "order_ts_s": 100.05,
                "market_start_ts_s": 0.0,
                "market_end_ts_s": 300.0
            }),
            serde_json::json!({
                "cat": "order",
                "type": "placed",
                "intent_id": "intent_1"
            }),
            serde_json::json!({
                "cat": "order",
                "type": "filled",
                "intent_id": "intent_1",
                "fill_time_s": 0.05
            }),
            serde_json::json!({
                "cat": "resolution",
                "type": "resolved",
                "won": true,
                "pnl": 2.0,
                "btc_move": 44.0
            }),
            serde_json::json!({
                "cat": "causality",
                "type": "resolution_timing",
                "condition_id": "0xabc",
                "market_end_ts_s": 300.0,
                "resolution_ts_s": 301.0
            }),
            serde_json::json!({
                "cat": "oracle",
                "type": "resolution",
                "agreed": true,
                "our_open_btc": 100000.0,
                "our_close_btc": 100044.0
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
            adaptive_report_paths: Vec::new(),
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

        assert!(audit.checks.iter().any(|c| {
            c.name == "replay.shadow_oracle" && c.status == StrategyBuilderCheckStatus::Ok
        }));
        assert!(audit.checks.iter().any(|c| {
            c.name == "replay.settlement_alignment" && c.status == StrategyBuilderCheckStatus::Ok
        }));
        assert!(audit.checks.iter().any(|c| {
            c.name == "replay.causality" && c.status == StrategyBuilderCheckStatus::Ok
        }));
    }

    #[test]
    fn audit_fails_best_report_variant_that_needed_adaptive_rearm() {
        use std::collections::BTreeMap;

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("report.json");
        let mut src = crate::data::manifest::DataSourceManifest::new("pmxt", "order_book_l2");
        src.complete = true;
        let diagnostics = crate::backtest::resolver::BacktestDiagnostics {
            adaptive_rearms: 1,
            breaker_paused_events: 42,
            ..Default::default()
        };
        let report = crate::backtest::experiment::ExperimentReport {
            schema_version: 1,
            generated_at: "2026-05-22T00:00:00Z".to_string(),
            label: "test".to_string(),
            mode: "backtest".to_string(),
            start: "2026-05-21T00:00:00Z".to_string(),
            end: "2026-05-21T23:00:00Z".to_string(),
            bankroll_usd: 100.0,
            latency_ms: 50,
            market_catalog: crate::data::catalog::MarketCatalog::default(),
            data_manifest: crate::data::manifest::DataManifest::new(vec![src], Vec::new()),
            variants: vec![crate::backtest::experiment::VariantReport {
                strategy: crate::strategy::spec::StrategySpec::new("s", "1", "hash", "risk"),
                strategy_params: serde_json::json!({"name": "test"}),
                trades: 100,
                wins: 70,
                losses: 30,
                unresolved_fills: 0,
                execution_attempts: 100,
                fills_success: 100,
                fills_failed: 0,
                fill_rate: 1.0,
                reject_reasons: BTreeMap::new(),
                breaker_tripped: false,
                breaker_reason: None,
                breaker_tripped_at_s: None,
                breaker_realized_drawdown_pct: 0.0,
                breaker_stressed_drawdown_pct: 0.0,
                diagnostics,
                win_rate: 0.70,
                total_pnl: 100.0,
                avg_pnl: 1.0,
                total_fees: 0.0,
                sharpe_like: 1.0,
                by_zone: BTreeMap::new(),
            }],
        };
        std::fs::write(&path, serde_json::to_vec(&report).unwrap()).unwrap();

        let audit = audit(StrategyBuilderAuditInput {
            report_paths: vec![path.display().to_string()],
            adaptive_report_paths: Vec::new(),
            promotion_artifact: None,
            replay_sessions: Vec::new(),
            min_trades: 50,
            min_win_rate: 0.60,
            min_wilson_win_rate_lower: 0.50,
            min_total_pnl: 10.0,
            min_shadow_resolutions: 1,
            min_research_reports: 1,
            min_replay_sessions: 0,
            a_plus_min_shadow_resolutions: 1,
        });

        assert!(audit.checks.iter().any(|c| {
            c.name == "report.best_variant_health"
                && c.status == StrategyBuilderCheckStatus::Fail
                && c.detail.contains("adaptive_rearms=1")
        }));
    }

    #[test]
    fn audit_fails_adaptive_probe_when_best_variant_rearms() {
        use std::collections::BTreeMap;

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("adaptive_report.json");
        let mut src = crate::data::manifest::DataSourceManifest::new("pmxt", "order_book_l2");
        src.complete = true;
        let diagnostics = crate::backtest::resolver::BacktestDiagnostics {
            adaptive_rearms: 2,
            breaker_paused_events: 100,
            ..Default::default()
        };
        let report = crate::backtest::experiment::ExperimentReport {
            schema_version: 1,
            generated_at: "2026-05-22T00:00:00Z".to_string(),
            label: "adaptive".to_string(),
            mode: "backtest".to_string(),
            start: "2026-05-21T00:00:00Z".to_string(),
            end: "2026-05-21T23:00:00Z".to_string(),
            bankroll_usd: 100.0,
            latency_ms: 50,
            market_catalog: crate::data::catalog::MarketCatalog::default(),
            data_manifest: crate::data::manifest::DataManifest::new(vec![src], Vec::new()),
            variants: vec![crate::backtest::experiment::VariantReport {
                strategy: crate::strategy::spec::StrategySpec::new("s", "1", "hash", "risk"),
                strategy_params: serde_json::json!({"name": "test"}),
                trades: 100,
                wins: 70,
                losses: 30,
                unresolved_fills: 0,
                execution_attempts: 100,
                fills_success: 100,
                fills_failed: 0,
                fill_rate: 1.0,
                reject_reasons: BTreeMap::new(),
                breaker_tripped: true,
                breaker_reason: Some("win_rate_low".to_string()),
                breaker_tripped_at_s: Some(1_700_000_000.0),
                breaker_realized_drawdown_pct: 0.0,
                breaker_stressed_drawdown_pct: 0.0,
                diagnostics,
                win_rate: 0.70,
                total_pnl: 100.0,
                avg_pnl: 1.0,
                total_fees: 0.0,
                sharpe_like: 1.0,
                by_zone: BTreeMap::new(),
            }],
        };
        std::fs::write(&path, serde_json::to_vec(&report).unwrap()).unwrap();

        let audit = audit(StrategyBuilderAuditInput {
            report_paths: Vec::new(),
            adaptive_report_paths: vec![path.display().to_string()],
            promotion_artifact: None,
            replay_sessions: Vec::new(),
            min_trades: 50,
            min_win_rate: 0.60,
            min_wilson_win_rate_lower: 0.50,
            min_total_pnl: 10.0,
            min_shadow_resolutions: 1,
            min_research_reports: 0,
            min_replay_sessions: 0,
            a_plus_min_shadow_resolutions: 1,
        });

        assert!(audit.checks.iter().any(|c| {
            c.name == "adaptive_probe.health"
                && c.status == StrategyBuilderCheckStatus::Fail
                && c.detail.contains("best_adaptive_rearms=2")
        }));
    }

    #[test]
    fn adaptive_drift_flags_forward_decay() {
        let (status, detail) = classify_adaptive_drift(0.80, 1.0, 5, 5, -1.0, 10, 0.60, false, 0);
        assert_eq!(status, StrategyBuilderCheckStatus::Fail);
        assert!(detail.contains("negative forward pnl"));

        let (status, detail) = classify_adaptive_drift(0.80, 1.0, 8, 2, 7.0, 10, 0.60, false, 0);
        assert_eq!(status, StrategyBuilderCheckStatus::Ok);
        assert!(detail.contains("inside adaptive band"));
    }
}
