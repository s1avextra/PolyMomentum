//! Release identity and fail-closed startup preflight.

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::config::{RuntimeMode, Settings, VenueMode};

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

    if mode.is_live() {
        check_live_confirmation(i_understand_live, &mut checks);
        check_live_venue(settings, &mut checks);
        check_live_credentials(settings, &mut checks);
        check_live_alerts(settings, &mut checks);
    } else {
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

    if missing.is_empty() {
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

fn check_peer_private_paths(settings: &Settings, checks: &mut Vec<PreflightCheck>) {
    let paths = [
        ("data_dir", settings.data_dir.as_str()),
        ("logs_dir", settings.logs_dir.as_str()),
        ("state_db_path", settings.state_db_path.as_str()),
        ("session_log_dir", settings.session_log_dir.as_str()),
        ("kill_switch_path", settings.kill_switch_path.as_str()),
    ];
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
    fn preflight_rejects_peer_private_paths() {
        let tmp = TempDir::new().unwrap();
        let mut s = test_settings(&tmp);
        s.logs_dir = "/opt/polyarbitrage/logs".to_string();
        let report = run_preflight(&s, RuntimeMode::Paper, false);
        assert!(!report.ok);
        assert!(report.failure_summary().contains("peer private"));
    }
}
