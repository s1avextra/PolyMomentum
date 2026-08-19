//! Compile strict, outcome-free signal rows for one opportunity-table hour.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, SecondsFormat, Timelike, Utc};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::artifact::{write_json_artifact_atomic, write_jsonl_atomic};
use crate::data::gamma::GammaClient;
use crate::data::models::Market;
use crate::strategy::momentum::annualized_realized_vol;
use crate::strategy::spec::stable_json_hash;

use super::opportunity_table::{CausalSignal, HashedSource, SignalDirection};

pub const OPPORTUNITY_SIGNAL_SCHEMA_VERSION: &str = "opportunity_signals_v1";
pub const OPPORTUNITY_MARKET_CATALOG_SCHEMA_VERSION: &str =
    "opportunity_market_identity_catalog_v1";
const VOLATILITY_LOOKBACK_SECONDS: i64 = 4 * 60 * 60;
const VOLATILITY_MIN_RETURNS: usize = 20;

/// A Polymarket candle market family the funnel can target. Every family
/// confirmed live in Gamma on 2026-08-17; the slug pattern is
/// `<prefix><window_start_epoch>` and windows tile each hour exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketFamily {
    pub key: &'static str,
    pub slug_prefix: &'static str,
    pub window_seconds: i64,
}

pub const MARKET_FAMILIES: &[MarketFamily] = &[
    MarketFamily { key: "btc-5m", slug_prefix: "btc-updown-5m-", window_seconds: 300 },
    MarketFamily { key: "eth-5m", slug_prefix: "eth-updown-5m-", window_seconds: 300 },
    MarketFamily { key: "sol-5m", slug_prefix: "sol-updown-5m-", window_seconds: 300 },
    MarketFamily { key: "xrp-5m", slug_prefix: "xrp-updown-5m-", window_seconds: 300 },
    MarketFamily { key: "btc-15m", slug_prefix: "btc-updown-15m-", window_seconds: 900 },
    MarketFamily { key: "eth-15m", slug_prefix: "eth-updown-15m-", window_seconds: 900 },
];

impl MarketFamily {
    pub fn from_key(key: &str) -> Result<Self> {
        MARKET_FAMILIES
            .iter()
            .copied()
            .find(|f| f.key == key)
            .with_context(|| {
                let known = MARKET_FAMILIES
                    .iter()
                    .map(|f| f.key)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("unknown market family {key:?}; known: {known}")
            })
    }

    pub fn windows_per_hour(&self) -> i64 {
        3600 / self.window_seconds
    }
}

impl Default for MarketFamily {
    /// btc-5m — the historical target; every pre-2026-08-17 artifact was
    /// produced under it.
    fn default() -> Self {
        MARKET_FAMILIES[0]
    }
}

#[derive(Debug, Clone)]
pub struct OpportunitySignalInput {
    pub hour: String,
    pub causal_windows_path: PathBuf,
    pub market_catalog_path: PathBuf,
    pub output_path: PathBuf,
    pub manifest_path: PathBuf,
    pub max_rows: usize,
    pub family: MarketFamily,
}

#[derive(Debug, Clone)]
pub struct OpportunityMarketCatalogInput {
    pub hours: Vec<String>,
    pub base_catalog_path: Option<PathBuf>,
    pub gamma_url: String,
    pub output_path: PathBuf,
    pub manifest_path: PathBuf,
    pub family: MarketFamily,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunityMarketCatalogManifest {
    pub schema_version: String,
    pub generated_at: String,
    /// Which candle family this catalog targets ("btc-5m", "eth-15m", ...).
    /// Pre-2026-08-17 manifests lack the field and default to btc-5m.
    #[serde(default = "default_market_family_key")]
    pub market_family: String,
    pub requested_hours: Vec<String>,
    pub requested_slugs: usize,
    pub fetched_markets: usize,
    pub total_catalog_markets: usize,
    pub base_catalog: Option<HashedSource>,
    pub output: HashedSource,
    pub gamma_outcome_prices_retained: bool,
    pub identity_semantics: String,
}

fn default_market_family_key() -> String {
    MarketFamily::default().key.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpportunitySignalManifest {
    pub schema_version: String,
    pub generated_at: String,
    pub hour: String,
    pub causal_windows: HashedSource,
    pub market_catalog: HashedSource,
    pub sanitized_market_identity_sha256: String,
    pub output: HashedSource,
    pub output_rows: usize,
    pub matched_markets: usize,
    pub rows_by_decision_offset_seconds: BTreeMap<i64, usize>,
    pub terminal_fields_read_from_causal_windows: bool,
    pub gamma_outcome_prices_influence_output: bool,
    pub volatility_semantics: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CausalWindow {
    window_start: i64,
    utc_day: String,
    utc_hour: u32,
    chronological_window: String,
    p0: f64,
    p60: f64,
    p120: f64,
    p180: f64,
    p240: f64,
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    condition_id: String,
    slug: String,
    end_date: String,
    outcomes: Vec<GammaOutcome>,
}

#[derive(Debug, Deserialize)]
struct GammaOutcome {
    token_id: String,
    name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct MarketIdentity {
    condition_id: String,
    slug: String,
    window_start: i64,
    market_close: String,
    up_token_id: String,
    down_token_id: String,
}

pub async fn fetch_market_catalog(
    input: OpportunityMarketCatalogInput,
) -> Result<OpportunityMarketCatalogManifest> {
    if input.hours.is_empty() {
        bail!("at least one --hour is required");
    }
    if input.output_path == input.manifest_path
        || input
            .base_catalog_path
            .as_ref()
            .is_some_and(|path| path == &input.output_path || path == &input.manifest_path)
    {
        bail!("market catalog outputs must not replace an input or each other");
    }

    let mut parsed_hours = input
        .hours
        .iter()
        .map(|hour| parse_hour(hour))
        .collect::<Result<Vec<_>>>()?;
    parsed_hours.sort();
    parsed_hours.dedup();
    let requested_hours = parsed_hours
        .iter()
        .map(|hour| hour.to_rfc3339_opts(SecondsFormat::Secs, true))
        .collect::<Vec<_>>();
    let family = input.family;
    let slugs = parsed_hours
        .iter()
        .flat_map(|hour| {
            (0..family.windows_per_hour()).map(move |index| {
                format!(
                    "{}{}",
                    family.slug_prefix,
                    hour.timestamp() + index * family.window_seconds
                )
            })
        })
        .collect::<Vec<_>>();

    let base_catalog = input
        .base_catalog_path
        .as_ref()
        .map(|path| -> Result<_> {
            let catalog = serde_json::from_reader::<_, BTreeMap<String, Market>>(
                File::open(path)
                    .with_context(|| format!("open base catalog {}", path.display()))?,
            )
            .with_context(|| format!("parse base catalog {}", path.display()))?;
            Ok((
                catalog,
                HashedSource {
                    path: path.display().to_string(),
                    sha256: sha256_file(path)?,
                },
            ))
        })
        .transpose()?;
    let (mut catalog, base_source) = match base_catalog {
        Some((catalog, source)) => (catalog, Some(source)),
        None => (BTreeMap::new(), None),
    };

    let gamma = GammaClient::new(&input.gamma_url);
    let fetched = gamma.fetch_markets_by_slugs(&slugs, true).await?;
    let requested = slugs
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let fetched_slugs = fetched
        .iter()
        .map(|market| market.slug.as_str())
        .collect::<std::collections::HashSet<_>>();
    let missing = requested
        .difference(&fetched_slugs)
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "Gamma identity lookup missed {} requested market(s)",
            missing.len()
        );
    }
    let fetched_markets = fetched.len();
    for market in fetched {
        catalog.insert(market.condition_id.clone(), market);
    }
    neutralize_outcome_prices(&mut catalog);
    write_json_artifact_atomic(&input.output_path, &catalog)?;
    let output = HashedSource {
        path: input.output_path.display().to_string(),
        sha256: sha256_file(&input.output_path)?,
    };
    let manifest = OpportunityMarketCatalogManifest {
        schema_version: OPPORTUNITY_MARKET_CATALOG_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        market_family: family.key.to_string(),
        requested_hours,
        requested_slugs: slugs.len(),
        fetched_markets,
        total_catalog_markets: catalog.len(),
        base_catalog: base_source,
        output,
        gamma_outcome_prices_retained: false,
        identity_semantics: "condition/token/slug/end-time identity only; every outcome price is overwritten with neutral 0.5 before the catalog is written".to_string(),
    };
    write_json_artifact_atomic(&input.manifest_path, &manifest)?;
    Ok(manifest)
}

fn neutralize_outcome_prices(catalog: &mut BTreeMap<String, Market>) {
    for market in catalog.values_mut() {
        for outcome in &mut market.outcomes {
            outcome.price = 0.5;
        }
    }
}

pub fn create(input: OpportunitySignalInput) -> Result<OpportunitySignalManifest> {
    validate_input(&input)?;
    let hour = parse_hour(&input.hour)?;
    let causal_windows_sha256 = sha256_file(&input.causal_windows_path)?;
    let market_catalog_sha256 = sha256_file(&input.market_catalog_path)?;
    let windows = load_causal_windows(&input.causal_windows_path)?;
    let markets = load_market_identities(&input.market_catalog_path, input.family)?;
    let sanitized_market_identity_sha256 = stable_json_hash(&markets);
    let signals = compile_signals(hour, &windows, &markets, input.max_rows)?;
    write_jsonl_atomic(&input.output_path, &signals)?;
    let output_sha256 = sha256_file(&input.output_path)?;
    let matched_markets = signals
        .iter()
        .map(|signal| signal.condition_id.as_str())
        .collect::<std::collections::HashSet<_>>()
        .len();
    let mut rows_by_decision_offset_seconds = BTreeMap::new();
    for signal in &signals {
        let start = parse_timestamp(&signal.window_start)?;
        let observed = parse_timestamp(&signal.observed_at)?;
        *rows_by_decision_offset_seconds
            .entry((observed - start).num_seconds())
            .or_insert(0) += 1;
    }
    let manifest = OpportunitySignalManifest {
        schema_version: OPPORTUNITY_SIGNAL_SCHEMA_VERSION.to_string(),
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        hour: hour.to_rfc3339_opts(SecondsFormat::Secs, true),
        causal_windows: HashedSource {
            path: input.causal_windows_path.display().to_string(),
            sha256: causal_windows_sha256,
        },
        market_catalog: HashedSource {
            path: input.market_catalog_path.display().to_string(),
            sha256: market_catalog_sha256,
        },
        sanitized_market_identity_sha256,
        output: HashedSource {
            path: input.output_path.display().to_string(),
            sha256: output_sha256,
        },
        output_rows: signals.len(),
        matched_markets,
        rows_by_decision_offset_seconds,
        terminal_fields_read_from_causal_windows: false,
        gamma_outcome_prices_influence_output: false,
        volatility_semantics: format!(
            "annualized realized log-return volatility; causal {}s lookback; minimum {} returns",
            VOLATILITY_LOOKBACK_SECONDS, VOLATILITY_MIN_RETURNS
        ),
    };
    write_json_artifact_atomic(&input.manifest_path, &manifest)?;
    Ok(manifest)
}

fn validate_input(input: &OpportunitySignalInput) -> Result<()> {
    if input.max_rows == 0 {
        bail!("max_rows must be positive");
    }
    if input.output_path == input.manifest_path
        || input.output_path == input.causal_windows_path
        || input.output_path == input.market_catalog_path
        || input.manifest_path == input.causal_windows_path
        || input.manifest_path == input.market_catalog_path
    {
        bail!("outputs must never replace inputs or each other");
    }
    if input
        .output_path
        .extension()
        .and_then(|value| value.to_str())
        != Some("jsonl")
    {
        bail!("signal output path must use the .jsonl extension");
    }
    Ok(())
}

fn parse_hour(raw: &str) -> Result<DateTime<Utc>> {
    let parsed = parse_timestamp(raw)?;
    if parsed.minute() != 0 || parsed.second() != 0 || parsed.nanosecond() != 0 {
        bail!("--hour must identify the start of one UTC hour");
    }
    Ok(parsed)
}

fn parse_timestamp(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("parse timestamp {raw}"))
        .map(|value| value.with_timezone(&Utc))
}

fn open_lines(path: &Path) -> Result<Box<dyn BufRead>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    if path.extension().and_then(|value| value.to_str()) == Some("gz") {
        Ok(Box::new(BufReader::new(GzDecoder::new(file))))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

fn load_causal_windows(path: &Path) -> Result<Vec<CausalWindow>> {
    let mut windows = Vec::new();
    let mut starts = std::collections::HashSet::new();
    for (index, line) in open_lines(path)?.lines().enumerate() {
        let line = line.with_context(|| format!("read causal window line {}", index + 1))?;
        if line.trim().is_empty() {
            bail!("causal window line {} is blank", index + 1);
        }
        let row: CausalWindow = serde_json::from_str(&line)
            .with_context(|| format!("parse strict causal window line {}", index + 1))?;
        validate_window(&row)
            .with_context(|| format!("validate causal window line {}", index + 1))?;
        if !starts.insert(row.window_start) {
            bail!("duplicate causal window_start {}", row.window_start);
        }
        windows.push(row);
    }
    if windows.is_empty() {
        bail!("causal windows source contains no rows");
    }
    windows.sort_by_key(|row| row.window_start);
    Ok(windows)
}

fn validate_window(row: &CausalWindow) -> Result<()> {
    let start = DateTime::<Utc>::from_timestamp(row.window_start, 0)
        .context("window_start is outside chrono range")?;
    if row.utc_day != start.date_naive().to_string() || row.utc_hour != start.hour() {
        bail!("utc_day/utc_hour do not match window_start");
    }
    if !matches!(
        row.chronological_window.as_str(),
        "older" | "recent_discovery" | "fresh_holdout"
    ) {
        bail!("chronological_window is not allowlisted");
    }
    for (name, value) in [
        ("p0", row.p0),
        ("p60", row.p60),
        ("p120", row.p120),
        ("p180", row.p180),
        ("p240", row.p240),
    ] {
        if !value.is_finite() || value <= 0.0 {
            bail!("{name} must be finite and positive");
        }
    }
    Ok(())
}

fn load_market_identities(path: &Path, family: MarketFamily) -> Result<Vec<MarketIdentity>> {
    let file = File::open(path).with_context(|| format!("open catalog {}", path.display()))?;
    let catalog: BTreeMap<String, GammaMarket> =
        serde_json::from_reader(file).context("parse Gamma market cache")?;
    let mut identities = Vec::new();
    for (key, market) in catalog {
        if key != market.condition_id {
            bail!("Gamma catalog key does not match condition_id");
        }
        let Some(raw_start) = market.slug.strip_prefix(family.slug_prefix) else {
            continue;
        };
        let window_start = raw_start
            .parse::<i64>()
            .with_context(|| format!("parse market epoch from {}", market.slug))?;
        let close = parse_timestamp(&market.end_date)?;
        if close.timestamp() != window_start + family.window_seconds {
            bail!(
                "market {} is not an exact {}-second window",
                market.condition_id,
                family.window_seconds,
            );
        }
        let mut up_token_id = None;
        let mut down_token_id = None;
        for outcome in market.outcomes {
            match outcome.name.to_ascii_lowercase().as_str() {
                "up" => up_token_id = Some(outcome.token_id),
                "down" => down_token_id = Some(outcome.token_id),
                _ => {}
            }
        }
        identities.push(MarketIdentity {
            condition_id: market.condition_id,
            slug: market.slug,
            window_start,
            market_close: close.to_rfc3339_opts(SecondsFormat::Secs, true),
            up_token_id: up_token_id.context("BTC market missing Up token")?,
            down_token_id: down_token_id.context("BTC market missing Down token")?,
        });
    }
    if identities.is_empty() {
        bail!("Gamma catalog contains no supported BTC five-minute markets");
    }
    identities.sort_by(|left, right| {
        left.window_start
            .cmp(&right.window_start)
            .then_with(|| left.condition_id.cmp(&right.condition_id))
    });
    Ok(identities)
}

fn compile_signals(
    hour: DateTime<Utc>,
    windows: &[CausalWindow],
    markets: &[MarketIdentity],
    max_rows: usize,
) -> Result<Vec<CausalSignal>> {
    let windows_by_start = windows
        .iter()
        .map(|window| (window.window_start, window))
        .collect::<HashMap<_, _>>();
    let mut prices = BTreeMap::<i64, f64>::new();
    for window in windows {
        for (offset, price) in [
            (0, window.p0),
            (60, window.p60),
            (120, window.p120),
            (180, window.p180),
            (240, window.p240),
        ] {
            let timestamp = window.window_start + offset;
            if let Some(existing) = prices.insert(timestamp, price) {
                if (existing - price).abs() > 1e-9 {
                    bail!("conflicting causal BTC prices at timestamp {timestamp}");
                }
            }
        }
    }

    let hour_end = hour + Duration::hours(1);
    let mut signals = Vec::new();
    for market in markets {
        let Some(window) = windows_by_start.get(&market.window_start) else {
            continue;
        };
        for offset in [120_i64, 180, 240] {
            let observed_at = DateTime::<Utc>::from_timestamp(market.window_start + offset, 0)
                .context("observed_at is outside chrono range")?;
            if observed_at < hour || observed_at >= hour_end {
                continue;
            }
            let observed_price = match offset {
                120 => window.p120,
                180 => window.p180,
                240 => window.p240,
                _ => unreachable!(),
            };
            let direction = if observed_price > window.p0 {
                SignalDirection::Up
            } else if observed_price < window.p0 {
                SignalDirection::Down
            } else {
                continue;
            };
            let volatility =
                causal_volatility(&prices, observed_at.timestamp()).with_context(|| {
                    format!("insufficient causal volatility history at {observed_at}")
                })?;
            let token_id = match direction {
                SignalDirection::Up => market.up_token_id.clone(),
                SignalDirection::Down => market.down_token_id.clone(),
            };
            signals.push(CausalSignal {
                condition_id: market.condition_id.clone(),
                token_id,
                chronological_window: window.chronological_window.clone(),
                window_start: DateTime::<Utc>::from_timestamp(market.window_start, 0)
                    .context("window_start outside chrono range")?
                    .to_rfc3339_opts(SecondsFormat::Secs, true),
                market_close: market.market_close.clone(),
                observed_at: observed_at.to_rfc3339_opts(SecondsFormat::Secs, true),
                signal_direction: direction,
                strike_price: window.p0,
                btc_open: window.p0,
                btc_60s: Some(window.p60),
                btc_120s: Some(window.p120),
                btc_180s: (offset >= 180).then_some(window.p180),
                btc_240s: (offset >= 240).then_some(window.p240),
                btc_observed: observed_price,
                causal_volatility: volatility,
            });
            if signals.len() > max_rows {
                bail!("compiled signals exceed --max-rows {max_rows}");
            }
        }
    }
    if signals.is_empty() {
        bail!("no catalog-backed causal signals observed inside requested hour");
    }
    signals.sort_by(|left, right| {
        left.observed_at
            .cmp(&right.observed_at)
            .then_with(|| left.condition_id.cmp(&right.condition_id))
            .then_with(|| left.token_id.cmp(&right.token_id))
    });
    Ok(signals)
}

fn causal_volatility(prices: &BTreeMap<i64, f64>, observed_at: i64) -> Option<f64> {
    let start = observed_at - VOLATILITY_LOOKBACK_SECONDS;
    annualized_realized_vol(
        prices
            .range(start..=observed_at)
            .map(|(timestamp, price)| (*timestamp as f64, *price)),
        VOLATILITY_MIN_RETURNS,
    )
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("open {} for hashing", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::models::Outcome;

    fn window(start: i64, partition: &str, base: f64) -> CausalWindow {
        let timestamp = DateTime::<Utc>::from_timestamp(start, 0).unwrap();
        CausalWindow {
            window_start: start,
            utc_day: timestamp.date_naive().to_string(),
            utc_hour: timestamp.hour(),
            chronological_window: partition.to_string(),
            p0: base,
            p60: base + 10.0,
            p120: base + 20.0,
            p180: base + 30.0,
            p240: base + 40.0,
        }
    }

    #[test]
    fn strict_causal_window_rejects_terminal_fields() {
        let value = serde_json::json!({
            "window_start": 1785585600,
            "utc_day": "2026-08-01",
            "utc_hour": 12,
            "chronological_window": "recent_discovery",
            "p0": 100.0, "p60": 101.0, "p120": 102.0,
            "p180": 103.0, "p240": 104.0,
            "terminal": 105.0
        });
        assert!(serde_json::from_value::<CausalWindow>(value).is_err());
    }

    #[test]
    fn gamma_terminal_prices_do_not_change_sanitized_identity() {
        let temp = tempfile::TempDir::new().unwrap();
        let first = temp.path().join("first.json");
        let second = temp.path().join("second.json");
        let market = |up: f64, down: f64| {
            serde_json::json!({
                "condition": {
                    "condition_id": "condition",
                    "slug": "btc-updown-5m-1785585600",
                    "end_date": "2026-08-01T12:05:00Z",
                    "outcomes": [
                        {"token_id":"up-token", "name":"Up", "price":up},
                        {"token_id":"down-token", "name":"Down", "price":down}
                    ]
                }
            })
        };
        std::fs::write(&first, serde_json::to_vec(&market(1.0, 0.0)).unwrap()).unwrap();
        std::fs::write(&second, serde_json::to_vec(&market(0.0, 1.0)).unwrap()).unwrap();
        assert_eq!(
            load_market_identities(&first, MarketFamily::default()).unwrap(),
            load_market_identities(&second, MarketFamily::default()).unwrap()
        );
    }

    #[test]
    fn family_registry_parses_known_keys_and_rejects_unknown() {
        for f in MARKET_FAMILIES {
            let parsed = MarketFamily::from_key(f.key).unwrap();
            assert_eq!(parsed, *f);
            assert_eq!(3600 % f.window_seconds, 0, "windows must tile the hour");
        }
        assert!(MarketFamily::from_key("doge-2m").is_err());
        assert_eq!(MarketFamily::default().key, "btc-5m");
    }

    #[test]
    fn load_market_identities_filters_by_family_prefix_and_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.json");
        // A btc-5m market and an eth-15m market in one catalog: each family
        // must see only its own, with the correct window-length check.
        let catalog = serde_json::json!({
            "0xbtc": {
                "condition_id": "0xbtc",
                "slug": "btc-updown-5m-1785585600",
                "end_date": "2026-08-01T12:05:00Z",
                "outcomes": [
                    {"token_id":"u1", "name":"Up", "price":0.5},
                    {"token_id":"d1", "name":"Down", "price":0.5}
                ]
            },
            "0xeth": {
                "condition_id": "0xeth",
                "slug": "eth-updown-15m-1785585600",
                "end_date": "2026-08-01T12:15:00Z",
                "outcomes": [
                    {"token_id":"u2", "name":"Up", "price":0.5},
                    {"token_id":"d2", "name":"Down", "price":0.5}
                ]
            }
        });
        std::fs::write(&path, serde_json::to_vec(&catalog).unwrap()).unwrap();
        let btc = load_market_identities(&path, MarketFamily::from_key("btc-5m").unwrap()).unwrap();
        assert_eq!(btc.len(), 1);
        assert_eq!(btc[0].condition_id, "0xbtc");
        let eth =
            load_market_identities(&path, MarketFamily::from_key("eth-15m").unwrap()).unwrap();
        assert_eq!(eth.len(), 1);
        assert_eq!(eth[0].condition_id, "0xeth");
        assert_eq!(eth[0].window_start + 900, parse_timestamp("2026-08-01T12:15:00Z").unwrap().timestamp());
    }

    #[test]
    fn fetched_catalog_neutralizes_every_outcome_price() {
        let mut catalog = BTreeMap::from([(
            "condition".to_string(),
            Market {
                condition_id: "condition".to_string(),
                outcomes: vec![
                    Outcome {
                        token_id: "up".to_string(),
                        name: "Up".to_string(),
                        price: 1.0,
                    },
                    Outcome {
                        token_id: "down".to_string(),
                        name: "Down".to_string(),
                        price: 0.0,
                    },
                ],
                ..Default::default()
            },
        )]);
        neutralize_outcome_prices(&mut catalog);
        assert!(catalog["condition"]
            .outcomes
            .iter()
            .all(|outcome| outcome.price == 0.5));
    }

    #[test]
    fn compiler_emits_three_causal_offsets_for_catalog_market() {
        let selected_start = 1_785_585_600_i64;
        let mut windows = Vec::new();
        for index in 0..60 {
            windows.push(window(
                selected_start - (60 - index) * 300,
                "older",
                100_000.0 + index as f64 * 50.0,
            ));
        }
        windows.push(window(selected_start, "recent_discovery", 103_000.0));
        let market = MarketIdentity {
            condition_id: "condition".to_string(),
            slug: format!("btc-updown-5m-{selected_start}"),
            window_start: selected_start,
            market_close: "2026-08-01T12:05:00Z".to_string(),
            up_token_id: "up-token".to_string(),
            down_token_id: "down-token".to_string(),
        };
        let hour = DateTime::<Utc>::from_timestamp(selected_start, 0).unwrap();
        let signals = compile_signals(hour, &windows, &[market], 10).unwrap();
        assert_eq!(signals.len(), 3);
        assert_eq!(signals[0].observed_at, "2026-08-01T12:02:00Z");
        assert_eq!(signals[0].btc_180s, None);
        assert_eq!(signals[1].btc_180s, Some(103_030.0));
        assert_eq!(signals[2].btc_240s, Some(103_040.0));
        assert!(signals.iter().all(|signal| signal.causal_volatility > 0.0));
        assert!(signals.iter().all(|signal| signal.token_id == "up-token"));
    }
}
