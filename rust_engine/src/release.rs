//! Release identity and fail-closed startup preflight.

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::backtest::experiment::PromotionArtifact;
use crate::backtest::strategies::StrategyVariant;
use crate::config::{RuntimeMode, Settings, VenueMode};
use crate::strategy::spec::{stable_json_hash, StrategySpec};

#[derive(Debug, Clone, Serialize)]
pub struct ReleaseManifest {
    pub schema_version: u32,
    pub package: &'static str,
    pub version: &'static str,
    pub git_sha: &'static str,
    pub build_timestamp: &'static str,
    pub binary_path: String,
    pub mode: RuntimeMode,
    pub venue: VenueMode,
    pub config_hash: String,
    pub promotion: PromotionReleaseManifest,
}

#[derive(Debug, Clone, Serialize)]
pub struct PromotionReleaseManifest {
    pub status: &'static str,
    pub path: Option<String>,
    pub detail: String,
    pub source_report_hash: Option<String>,
    pub data_manifest_hash: Option<String>,
    pub strategy: Option<StrategySpec>,
    pub trades: Option<usize>,
    pub win_rate: Option<f64>,
    pub total_pnl: Option<f64>,
}

impl ReleaseManifest {
    pub fn capture(settings: &Settings, mode: RuntimeMode) -> Self {
        Self {
            schema_version: 1,
            package: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            git_sha: option_env!("POLYMOMENTUM_GIT_SHA")
                .or(option_env!("GIT_SHA"))
                .unwrap_or("unknown"),
            build_timestamp: option_env!("POLYMOMENTUM_BUILD_TIMESTAMP")
                .or(option_env!("BUILD_TIMESTAMP"))
                .unwrap_or("unknown"),
            binary_path: std::env::current_exe()
                .ok()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            mode,
            venue: settings.venue,
            config_hash: redacted_config_hash(settings),
            promotion: capture_promotion_manifest(settings),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Ok,
    Warn,
    Fail,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreflightCheck {
    pub name: &'static str,
    pub status: CheckStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreflightReport {
    pub ok: bool,
    pub mode: RuntimeMode,
    pub venue: VenueMode,
    pub release_manifest: ReleaseManifest,
    pub checks: Vec<PreflightCheck>,
}

impl PreflightReport {
    pub fn failure_summary(&self) -> String {
        let failures: Vec<&str> = self
            .checks
            .iter()
            .filter(|c| c.status == CheckStatus::Fail)
            .map(|c| c.detail.as_str())
            .collect();
        if failures.is_empty() {
            "preflight passed".to_string()
        } else {
            failures.join("; ")
        }
    }
}

pub fn run_preflight(
    settings: &Settings,
    mode: RuntimeMode,
    i_understand_live: bool,
) -> PreflightReport {
    let mut checks = Vec::new();

    push(
        &mut checks,
        "runtime_mode",
        CheckStatus::Ok,
        format!("mode={}", mode.as_str()),
    );

    if let Some(e) = &settings.venue_parse_error {
        push(&mut checks, "venue_config", CheckStatus::Fail, e.clone());
    } else {
        push(
            &mut checks,
            "venue_config",
            CheckStatus::Ok,
            format!("venue={}", settings.venue.as_str()),
        );
    }

    check_peer_private_paths(settings, &mut checks);
    check_runtime_paths(settings, mode, &mut checks);
    check_kill_switch(settings, &mut checks);
    check_promotion_artifact(settings, &mut checks);

    if mode.is_live() {
        check_live_confirmation(i_understand_live, &mut checks);
        check_live_venue(settings, &mut checks);
        check_clob_v2_ready(settings, &mut checks);
        check_live_reconciliation(settings, &mut checks);
        check_live_credentials(settings, &mut checks);
        check_live_alerts(settings, &mut checks);
    } else {
        check_paper_bankroll(settings, &mut checks);
        push(
            &mut checks,
            "live_safeguard",
            CheckStatus::Ok,
            "paper mode does not initialize live CLOB order placement".to_string(),
        );
    }

    let ok = !checks.iter().any(|c| c.status == CheckStatus::Fail);
    PreflightReport {
        ok,
        mode,
        venue: settings.venue,
        release_manifest: ReleaseManifest::capture(settings, mode),
        checks,
    }
}

fn redacted_config_hash(settings: &Settings) -> String {
    let material = json!({
        "poly_base_url": settings.poly_base_url,
        "poly_gamma_url": settings.poly_gamma_url,
        "venue": settings.venue.as_str(),
        "operator_country_present": !settings.operator_country.trim().is_empty(),
        "venue_compliance_ok": settings.venue_compliance_ok,
        "polymarket_us_api_enabled": settings.polymarket_us_api_enabled,
        "clob_v2_ready": settings.clob_v2_ready,
        "live_reconciliation_ready": settings.live_reconciliation_ready,
        "private_key_present": !settings.private_key.is_empty(),
        "poly_api_key_present": !settings.poly_api_key.is_empty(),
        "poly_api_secret_present": !settings.poly_api_secret.is_empty(),
        "poly_api_passphrase_present": !settings.poly_api_passphrase.is_empty(),
        "bankroll_usd": settings.bankroll_usd,
        "max_total_exposure_usd": settings.max_total_exposure_usd,
        "max_position_per_market_usd": settings.max_position_per_market_usd,
        "candle_position_pct": settings.candle_position_pct,
        "candle_prefer_maker": settings.candle_prefer_maker,
        "candle_cross_asset_enabled": settings.candle_cross_asset_enabled,
        "alert_required": settings.alert_required,
        "promotion_artifact_present": !settings.promotion_artifact_path.trim().is_empty(),
        "promotion_required": settings.promotion_required,
        "data_dir": settings.data_dir,
        "logs_dir": settings.logs_dir,
        "state_db_path": settings.state_db_path,
        "session_log_dir": settings.session_log_dir,
        "kill_switch_path": settings.kill_switch_path,
    });
    let bytes = serde_json::to_vec(&material).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

fn capture_promotion_manifest(settings: &Settings) -> PromotionReleaseManifest {
    let path = settings.promotion_artifact_path.trim();
    if path.is_empty() {
        return PromotionReleaseManifest {
            status: "absent",
            path: None,
            detail: "no promotion artifact configured".to_string(),
            source_report_hash: None,
            data_manifest_hash: None,
            strategy: None,
            trades: None,
            win_rate: None,
            total_pnl: None,
        };
    }

    match crate::backtest::experiment::read_promotion(path) {
        Ok(artifact) => {
            if let Some(detail) = promotion_validation_error(&artifact) {
                PromotionReleaseManifest {
                    status: "invalid",
                    path: Some(path.to_string()),
                    detail,
                    source_report_hash: Some(artifact.source_report_hash),
                    data_manifest_hash: Some(artifact.data_manifest_hash),
                    strategy: Some(artifact.selected_strategy),
                    trades: Some(artifact.trades),
                    win_rate: Some(artifact.win_rate),
                    total_pnl: Some(artifact.total_pnl),
                }
            } else {
                promotion_manifest_from_artifact(path, &artifact)
            }
        }
        Err(e) => PromotionReleaseManifest {
            status: "invalid",
            path: Some(path.to_string()),
            detail: e.to_string(),
            source_report_hash: None,
            data_manifest_hash: None,
            strategy: None,
            trades: None,
            win_rate: None,
            total_pnl: None,
        },
    }
}

fn promotion_validation_error(artifact: &PromotionArtifact) -> Option<String> {
    if artifact.schema_version != 1 {
        return Some(format!(
            "unsupported promotion schema {}",
            artifact.schema_version
        ));
    }
    if artifact.selected_strategy.name != "candle_momentum" {
        return Some(format!(
            "unsupported promoted strategy {}",
            artifact.selected_strategy.name
        ));
    }
    if artifact.strategy_params.is_null() {
        return Some("promotion artifact has no strategy_params".to_string());
    }
    let variant: StrategyVariant = match serde_json::from_value(artifact.strategy_params.clone()) {
        Ok(variant) => variant,
        Err(e) => return Some(format!("strategy_params do not parse as StrategyVariant: {e}")),
    };
    let params_hash = stable_json_hash(&variant);
    if params_hash != artifact.selected_strategy.params_hash {
        return Some(format!(
            "strategy_params hash {} does not match selected_strategy hash {}",
            params_hash, artifact.selected_strategy.params_hash
        ));
    }
    None
}

fn promotion_manifest_from_artifact(
    path: &str,
    artifact: &PromotionArtifact,
) -> PromotionReleaseManifest {
    PromotionReleaseManifest {
        status: "ok",
        path: Some(path.to_string()),
        detail: format!(
            "promoted {} trades from {}",
            artifact.trades, artifact.source_label
        ),
        source_report_hash: Some(artifact.source_report_hash.clone()),
        data_manifest_hash: Some(artifact.data_manifest_hash.clone()),
        strategy: Some(artifact.selected_strategy.clone()),
        trades: Some(artifact.trades),
        win_rate: Some(artifact.win_rate),
        total_pnl: Some(artifact.total_pnl),
    }
}

fn check_live_confirmation(i_understand_live: bool, checks: &mut Vec<PreflightCheck>) {
    if i_understand_live {
        push(
            checks,
            "live_confirmation",
            CheckStatus::Ok,
            "--i-understand-live supplied".to_string(),
        );
    } else {
        push(
            checks,
            "live_confirmation",
            CheckStatus::Fail,
            "live mode requires --i-understand-live".to_string(),
        );
    }
}

fn check_live_venue(settings: &Settings, checks: &mut Vec<PreflightCheck>) {
    match settings.venue {
        VenueMode::PaperOnly => push(
            checks,
            "live_venue",
            CheckStatus::Fail,
            "VENUE=paper_only refuses real-money live mode".to_string(),
        ),
        VenueMode::PolymarketUs => {
            if settings.polymarket_us_api_enabled && settings.venue_compliance_ok {
                push(
                    checks,
                    "live_venue",
                    CheckStatus::Ok,
                    "Polymarket US venue explicitly enabled by configuration".to_string(),
                );
            } else {
                push(
                    checks,
                    "live_venue",
                    CheckStatus::Fail,
                    "VENUE=polymarket_us requires POLYMARKET_US_API_ENABLED=1 and POLYMOMENTUM_VENUE_COMPLIANCE_OK=1".to_string(),
                );
            }
        }
        VenueMode::PolymarketInternational => {
            let country = settings.operator_country.trim().to_ascii_uppercase();
            let country_blocked = matches!(
                country.as_str(),
                "US" | "USA" | "UNITED_STATES" | "UNITED STATES"
            );
            if country_blocked {
                push(
                    checks,
                    "live_venue",
                    CheckStatus::Fail,
                    "VENUE=polymarket_international is blocked for OPERATOR_COUNTRY=US".to_string(),
                );
            } else if settings.venue_compliance_ok && !country.is_empty() {
                push(
                    checks,
                    "live_venue",
                    CheckStatus::Ok,
                    "international venue compliance acknowledged with non-US operator country"
                        .to_string(),
                );
            } else {
                push(
                    checks,
                    "live_venue",
                    CheckStatus::Fail,
                    "VENUE=polymarket_international requires non-US OPERATOR_COUNTRY and POLYMOMENTUM_VENUE_COMPLIANCE_OK=1".to_string(),
                );
            }
        }
    }
}

fn check_live_credentials(settings: &Settings, checks: &mut Vec<PreflightCheck>) {
    let missing: Vec<&str> = [
        ("PRIVATE_KEY", settings.private_key.as_str()),
        ("POLY_API_KEY", settings.poly_api_key.as_str()),
        ("POLY_API_SECRET", settings.poly_api_secret.as_str()),
        ("POLY_API_PASSPHRASE", settings.poly_api_passphrase.as_str()),
    ]
    .into_iter()
    .filter_map(|(name, value)| if value.is_empty() { Some(name) } else { None })
    .collect();

    if !settings.private_key.is_empty()
        && crate::signing::parse_private_key(&settings.private_key).is_none()
    {
        push(
            checks,
            "live_credentials",
            CheckStatus::Fail,
            "PRIVATE_KEY is present but is not a valid secp256k1 hex key".to_string(),
        );
    } else if missing.is_empty() {
        push(
            checks,
            "live_credentials",
            CheckStatus::Ok,
            "required CLOB credentials are present".to_string(),
        );
    } else {
        push(
            checks,
            "live_credentials",
            CheckStatus::Fail,
            format!("missing live credential(s): {}", missing.join(", ")),
        );
    }
}

fn check_clob_v2_ready(settings: &Settings, checks: &mut Vec<PreflightCheck>) {
    if crate::signing::CLOB_ORDER_SIGNING_VERSION != 2 {
        push(
            checks,
            "clob_v2_ready",
            CheckStatus::Fail,
            format!(
                "compiled CLOB order signer is V{}; live mode requires CLOB V2 signing",
                crate::signing::CLOB_ORDER_SIGNING_VERSION
            ),
        );
    } else if settings.clob_v2_ready {
        push(
            checks,
            "clob_v2_ready",
            CheckStatus::Ok,
            "CLOB_V2_READY=1 acknowledges the live order path has been migrated and verified"
                .to_string(),
        );
    } else {
        push(
            checks,
            "clob_v2_ready",
            CheckStatus::Fail,
            "live mode requires CLOB_V2_READY=1 until the V2 order-signing path is verified"
                .to_string(),
        );
    }
}

fn check_live_reconciliation(settings: &Settings, checks: &mut Vec<PreflightCheck>) {
    if settings.live_reconciliation_ready {
        push(
            checks,
            "live_reconciliation",
            CheckStatus::Ok,
            "authenticated user-channel/REST reconciliation is explicitly enabled".to_string(),
        );
    } else {
        push(
            checks,
            "live_reconciliation",
            CheckStatus::Fail,
            "live mode requires POLYMOMENTUM_LIVE_RECONCILIATION_READY=1 so accepted orders are reconciled from CLOB user-channel/REST evidence".to_string(),
        );
    }
}

fn check_live_alerts(settings: &Settings, checks: &mut Vec<PreflightCheck>) {
    let webhook_present = std::env::var("SLACK_WEBHOOK_URL")
        .or_else(|_| std::env::var("ALERT_WEBHOOK_URL"))
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);
    if settings.alert_required && webhook_present {
        push(
            checks,
            "live_alerting",
            CheckStatus::Ok,
            "alerting is required and a webhook is configured".to_string(),
        );
    } else if settings.alert_required {
        push(
            checks,
            "live_alerting",
            CheckStatus::Fail,
            "ALERT_REQUIRED=1 but no SLACK_WEBHOOK_URL or ALERT_WEBHOOK_URL is configured"
                .to_string(),
        );
    } else {
        push(
            checks,
            "live_alerting",
            CheckStatus::Fail,
            "live mode requires ALERT_REQUIRED=1".to_string(),
        );
    }
}

fn check_runtime_paths(settings: &Settings, mode: RuntimeMode, checks: &mut Vec<PreflightCheck>) {
    check_dir("data_dir", &settings.data_dir, mode, checks);
    check_dir("logs_dir", &settings.logs_dir, mode, checks);
    check_dir("session_log_dir", &settings.session_log_dir, mode, checks);

    let state_parent = Path::new(&settings.state_db_path)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    check_dir(
        "state_db_parent",
        &state_parent.display().to_string(),
        mode,
        checks,
    );
}

fn check_dir(name: &'static str, path: &str, mode: RuntimeMode, checks: &mut Vec<PreflightCheck>) {
    let p = Path::new(path);
    match std::fs::metadata(p) {
        Ok(m) if m.is_dir() => push(checks, name, CheckStatus::Ok, format!("{path} exists")),
        Ok(_) => push(
            checks,
            name,
            CheckStatus::Fail,
            format!("{path} exists but is not a directory"),
        ),
        Err(_) if mode.is_live() => push(
            checks,
            name,
            CheckStatus::Fail,
            format!("{path} is missing"),
        ),
        Err(_) => push(
            checks,
            name,
            CheckStatus::Warn,
            format!("{path} is missing; runtime will attempt to create it"),
        ),
    }
}

fn check_paper_bankroll(settings: &Settings, checks: &mut Vec<PreflightCheck>) {
    if settings.bankroll_usd > 0.0 {
        push(
            checks,
            "paper_bankroll",
            CheckStatus::Ok,
            format!("BANKROLL_USD={:.2}", settings.bankroll_usd),
        );
    } else {
        push(
            checks,
            "paper_bankroll",
            CheckStatus::Fail,
            "paper mode requires BANKROLL_USD > 0 so fills and risk limits are exercised"
                .to_string(),
        );
    }
}

fn check_kill_switch(settings: &Settings, checks: &mut Vec<PreflightCheck>) {
    if Path::new(&settings.kill_switch_path).exists() {
        push(
            checks,
            "kill_switch",
            CheckStatus::Fail,
            format!("kill switch is active at {}", settings.kill_switch_path),
        );
    } else {
        push(
            checks,
            "kill_switch",
            CheckStatus::Ok,
            format!("kill switch absent at {}", settings.kill_switch_path),
        );
    }
}

fn check_promotion_artifact(settings: &Settings, checks: &mut Vec<PreflightCheck>) {
    let path = settings.promotion_artifact_path.trim();
    if path.is_empty() {
        let status = if settings.promotion_required {
            CheckStatus::Fail
        } else {
            CheckStatus::Warn
        };
        push(
            checks,
            "promotion_artifact",
            status,
            "no POLYMOMENTUM_PROMOTION_ARTIFACT configured".to_string(),
        );
        return;
    }

    match crate::backtest::experiment::read_promotion(path) {
        Ok(artifact) => {
            if let Some(detail) = promotion_validation_error(&artifact) {
                push(
                    checks,
                    "promotion_artifact",
                    CheckStatus::Fail,
                    detail,
                );
            } else {
                push(
                    checks,
                    "promotion_artifact",
                    CheckStatus::Ok,
                    format!(
                        "loaded promoted strategy hash={} trades={}",
                        artifact.selected_strategy.params_hash, artifact.trades
                    ),
                );
            }
        }
        Err(e) => push(
            checks,
            "promotion_artifact",
            CheckStatus::Fail,
            format!("failed to load promotion artifact {path}: {e}"),
        ),
    }
}

fn check_peer_private_paths(settings: &Settings, checks: &mut Vec<PreflightCheck>) {
    let mut paths = vec![
        ("data_dir", settings.data_dir.as_str()),
        ("logs_dir", settings.logs_dir.as_str()),
        ("state_db_path", settings.state_db_path.as_str()),
        ("session_log_dir", settings.session_log_dir.as_str()),
        ("kill_switch_path", settings.kill_switch_path.as_str()),
    ];
    if !settings.promotion_artifact_path.trim().is_empty() {
        paths.push((
            "promotion_artifact_path",
            settings.promotion_artifact_path.as_str(),
        ));
    }
    let bad: Vec<String> = paths
        .iter()
        .filter_map(|(name, path)| {
            let normalized = path.trim_end_matches('/');
            if normalized.starts_with("/opt/polyarbitrage")
                || normalized.starts_with("/etc/polyarbitrage")
                || normalized.starts_with("/opt/adgts")
                || normalized.starts_with("/etc/adgts")
            {
                Some(format!("{name}={path}"))
            } else {
                None
            }
        })
        .collect();

    if bad.is_empty() {
        push(
            checks,
            "peer_private_paths",
            CheckStatus::Ok,
            "runtime paths stay out of peer bot private directories".to_string(),
        );
    } else {
        push(
            checks,
            "peer_private_paths",
            CheckStatus::Fail,
            format!(
                "runtime path(s) point into peer private directories: {}",
                bad.join(", ")
            ),
        );
    }
}

fn push(checks: &mut Vec<PreflightCheck>, name: &'static str, status: CheckStatus, detail: String) {
    checks.push(PreflightCheck {
        name,
        status,
        detail,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_settings(tmp: &TempDir) -> Settings {
        let mut s = Settings::from_env();
        let root = tmp.path();
        s.data_dir = root.join("data").display().to_string();
        s.logs_dir = root.join("logs").display().to_string();
        s.session_log_dir = root.join("logs/sessions").display().to_string();
        s.state_db_path = root.join("logs/candle/state.db").display().to_string();
        s.kill_switch_path = root.join("KILL").display().to_string();
        std::fs::create_dir_all(&s.data_dir).unwrap();
        std::fs::create_dir_all(&s.logs_dir).unwrap();
        std::fs::create_dir_all(&s.session_log_dir).unwrap();
        std::fs::create_dir_all(root.join("logs/candle")).unwrap();
        s.venue = VenueMode::PaperOnly;
        s.venue_raw = "paper_only".to_string();
        s.venue_parse_error = None;
        s.alert_required = false;
        s.bankroll_usd = 100.0;
        s.promotion_artifact_path.clear();
        s.promotion_required = false;
        s.clob_v2_ready = false;
        s.live_reconciliation_ready = false;
        s.private_key.clear();
        s.poly_api_key.clear();
        s.poly_api_secret.clear();
        s.poly_api_passphrase.clear();
        s
    }

    #[test]
    fn paper_preflight_passes_with_local_runtime_dirs() {
        let tmp = TempDir::new().unwrap();
        let s = test_settings(&tmp);
        let report = run_preflight(&s, RuntimeMode::Paper, false);
        assert!(report.ok, "{}", report.failure_summary());
    }

    #[test]
    fn paper_preflight_rejects_zero_bankroll() {
        let tmp = TempDir::new().unwrap();
        let mut s = test_settings(&tmp);
        s.bankroll_usd = 0.0;
        let report = run_preflight(&s, RuntimeMode::Paper, false);
        assert!(!report.ok);
        assert!(report.failure_summary().contains("BANKROLL_USD > 0"));
    }

    #[test]
    fn live_preflight_fails_closed_by_default() {
        let tmp = TempDir::new().unwrap();
        let s = test_settings(&tmp);
        let report = run_preflight(&s, RuntimeMode::Live, false);
        assert!(!report.ok);
        assert!(report
            .failure_summary()
            .contains("VENUE=paper_only refuses real-money live mode"));
    }

    #[test]
    fn live_preflight_requires_clob_v2_ready_flag() {
        let tmp = TempDir::new().unwrap();
        let mut s = test_settings(&tmp);
        s.venue = VenueMode::PolymarketInternational;
        s.venue_raw = "polymarket_international".to_string();
        s.operator_country = "IE".to_string();
        s.venue_compliance_ok = true;
        s.clob_v2_ready = false;
        s.private_key = "0xabc".to_string();
        s.poly_api_key = "key".to_string();
        s.poly_api_secret = "secret".to_string();
        s.poly_api_passphrase = "pass".to_string();

        let report = run_preflight(&s, RuntimeMode::Live, true);
        assert!(!report.ok);
        assert!(report
            .failure_summary()
            .contains("live mode requires CLOB_V2_READY=1"));
    }

    #[test]
    fn live_preflight_requires_reconciliation_ready_flag() {
        let tmp = TempDir::new().unwrap();
        let mut s = test_settings(&tmp);
        s.venue = VenueMode::PolymarketInternational;
        s.venue_raw = "polymarket_international".to_string();
        s.operator_country = "IE".to_string();
        s.venue_compliance_ok = true;
        s.clob_v2_ready = true;
        s.live_reconciliation_ready = false;
        s.alert_required = true;
        s.private_key = "0xabc".to_string();
        s.poly_api_key = "key".to_string();
        s.poly_api_secret = "secret".to_string();
        s.poly_api_passphrase = "pass".to_string();

        let report = run_preflight(&s, RuntimeMode::Live, true);
        assert!(!report.ok);
        assert!(report
            .failure_summary()
            .contains("POLYMOMENTUM_LIVE_RECONCILIATION_READY=1"));
    }

    #[test]
    fn preflight_rejects_peer_private_paths() {
        let tmp = TempDir::new().unwrap();
        let mut s = test_settings(&tmp);
        s.logs_dir = "/opt/polyarbitrage/logs".to_string();
        let report = run_preflight(&s, RuntimeMode::Paper, false);
        assert!(!report.ok);
        assert!(report.failure_summary().contains("peer private"));
    }

    #[test]
    fn preflight_can_require_promotion_artifact() {
        let tmp = TempDir::new().unwrap();
        let mut s = test_settings(&tmp);
        s.promotion_required = true;
        let report = run_preflight(&s, RuntimeMode::Paper, false);
        assert!(!report.ok);
        assert!(report
            .failure_summary()
            .contains("POLYMOMENTUM_PROMOTION_ARTIFACT"));
    }

    #[test]
    fn preflight_rejects_invalid_promotion_artifact() {
        let tmp = TempDir::new().unwrap();
        let mut s = test_settings(&tmp);
        let artifact = tmp.path().join("promotion.json");
        std::fs::write(&artifact, "{bad json").unwrap();
        s.promotion_artifact_path = artifact.display().to_string();
        let report = run_preflight(&s, RuntimeMode::Paper, false);
        assert!(!report.ok);
        assert!(report.failure_summary().contains("failed to load promotion"));
    }
}
