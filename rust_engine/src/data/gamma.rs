//! Polymarket Gamma REST client (market discovery).
//!
//! The CLOB REST endpoints (`/book`, `/midpoint`) used to live here, but the
//! pipeline now reads books off the WebSocket feed in `polymarket_ws.rs`,
//! so REST book/midpoint queries were removed during the cleanup audit.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures_util::{stream, StreamExt};
use reqwest::Client;
use serde_json::Value;

use crate::data::models::{Market, Outcome};

const GAMMA_MARKETS_KEYSET: &str = "/markets/keyset";
const MAX_SLUG_FETCH_CONCURRENCY: usize = 8;

#[derive(Clone)]
pub struct GammaClient {
    gamma_url: String,
    http: Client,
    max_retries: u32,
}

impl GammaClient {
    pub fn new(gamma_url: impl Into<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(15))
            .http1_only()
            .user_agent("polymomentum-engine/0.2")
            .build()
            .expect("reqwest client builds");
        Self {
            gamma_url: gamma_url.into().trim_end_matches('/').to_string(),
            http,
            max_retries: 3,
        }
    }

    async fn get_with_retry(&self, path: &str, params: &[(&str, String)]) -> Result<Value> {
        let url = format!("{}{path}", self.gamma_url);
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 0..self.max_retries {
            let resp = self.http.get(&url).query(params).send().await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    return r.json::<Value>().await.context("decode gamma json");
                }
                Ok(r) if r.status().as_u16() == 429 => {
                    let wait = Duration::from_secs(1u64 << attempt);
                    tracing::warn!(attempt, ?wait, "Gamma rate limited");
                    tokio::time::sleep(wait).await;
                }
                Ok(r) => {
                    last_err = Some(anyhow!("HTTP {} from {}", r.status(), url));
                    tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) as u64)).await;
                }
                Err(e) => {
                    last_err = Some(anyhow::Error::new(e));
                    tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) as u64)).await;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("Gamma request failed without specific error")))
    }

    /// Fetch markets sorted by endDate ascending — the fast path for candle
    /// discovery. Stops paginating once the last page's endDate exceeds
    /// `now + max_hours`. Filters out markets with degenerate prices /
    /// missing tokens / liquidity below `min_liquidity`.
    pub async fn fetch_markets_by_end_date(
        &self,
        max_hours: f64,
        min_liquidity: f64,
    ) -> Result<Vec<Market>> {
        let now = chrono::Utc::now().timestamp() as f64;
        let cutoff_ts = now + max_hours * 3600.0;
        let mut all: Vec<Market> = Vec::new();
        let page_size = 100u32;
        let mut next_cursor: Option<String> = None;

        loop {
            let mut params = vec![
                ("limit", page_size.to_string()),
                ("active", "true".to_string()),
                ("closed", "false".to_string()),
                ("order", "endDate".to_string()),
                ("ascending", "true".to_string()),
            ];
            if let Some(cursor) = &next_cursor {
                params.push(("next_cursor", cursor.clone()));
            }
            let v = self.get_with_retry(GAMMA_MARKETS_KEYSET, &params).await?;
            let (items, cursor) = unwrap_market_page(v);
            if items.is_empty() {
                break;
            }

            for raw in &items {
                let Some(m) = parse_gamma_market(raw) else {
                    continue;
                };
                if m.outcomes.is_empty() || m.outcomes.iter().all(|o| o.price == 0.0) {
                    continue;
                }
                if m.outcomes.iter().any(|o| o.token_id.is_empty()) {
                    continue;
                }
                if m.liquidity < min_liquidity {
                    continue;
                }
                all.push(m);
            }

            if let Some(last) = items.last() {
                let end_str = last
                    .get("endDate")
                    .or_else(|| last.get("end_date"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if let Some(end_ts) = parse_iso8601(end_str) {
                    if end_ts > cutoff_ts {
                        break;
                    }
                }
            }

            next_cursor = cursor;
            if items.len() < page_size as usize || next_cursor.is_none() {
                break;
            }
        }

        tracing::info!(
            count = all.len(),
            max_hours,
            "Gamma markets-by-endDate fetched"
        );
        Ok(all)
    }

    /// Fetch exact market slugs. This is the fast historical path for
    /// recurring crypto candle markets such as `btc-updown-5m-<start_ts>`.
    pub async fn fetch_markets_by_slugs(
        &self,
        slugs: &[String],
        closed: bool,
    ) -> Result<Vec<Market>> {
        let mut all = Vec::new();
        let total = slugs.len();
        let concurrency = total.clamp(1, MAX_SLUG_FETCH_CONCURRENCY);
        let mut responses = stream::iter(slugs.iter().cloned())
            .map(|slug| async move {
                let params = vec![
                    ("limit", "1".to_string()),
                    ("slug", slug.clone()),
                    ("closed", closed.to_string()),
                ];
                let v = self
                    .get_with_retry(GAMMA_MARKETS_KEYSET, &params)
                    .await
                    .with_context(|| format!("fetch gamma slug {slug}"))?;
                let (items, _) = unwrap_market_page(v);
                let mut markets = Vec::new();
                for raw in &items {
                    if let Some(m) = parse_gamma_market(raw) {
                        markets.push(m);
                    }
                }
                Ok::<_, anyhow::Error>(markets)
            })
            .buffer_unordered(concurrency);

        let mut completed = 0usize;
        while let Some(markets) = responses.next().await {
            let mut markets = markets?;
            all.append(&mut markets);
            completed += 1;
            if completed % 100 == 0 || completed == total {
                eprintln!("gamma: fetched {completed}/{total} slug metadata response(s)");
            }
        }
        all.sort_by(|a, b| a.condition_id.cmp(&b.condition_id));
        all.dedup_by(|a, b| a.condition_id == b.condition_id);
        Ok(all)
    }

    /// Fetch historical markets whose endDate falls inside an explicit UTC
    /// range. This is the targeted metadata path for backtests: fetch by time
    /// first, then let the candle scanner keep only supported candle markets.
    pub async fn fetch_markets_by_end_date_range(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        closed: bool,
    ) -> Result<Vec<Market>> {
        let mut all: Vec<Market> = Vec::new();
        let page_size = 100u32;
        let mut next_cursor: Option<String> = None;
        let start_ts = start.timestamp() as f64;
        let end_ts = end.timestamp() as f64;

        loop {
            let mut params = vec![
                ("limit", page_size.to_string()),
                ("closed", closed.to_string()),
                ("order", "endDate".to_string()),
                ("ascending", "true".to_string()),
                ("end_date_min", start.to_rfc3339()),
                ("end_date_max", end.to_rfc3339()),
            ];
            if let Some(cursor) = &next_cursor {
                params.push(("next_cursor", cursor.clone()));
            }
            let v = self.get_with_retry(GAMMA_MARKETS_KEYSET, &params).await?;
            let (items, cursor) = unwrap_market_page(v);
            if items.is_empty() {
                break;
            }
            for raw in &items {
                let Some(item_end_ts) = market_end_ts(raw) else {
                    continue;
                };
                if item_end_ts < start_ts || item_end_ts > end_ts {
                    continue;
                }
                let Some(m) = parse_gamma_market(raw) else {
                    continue;
                };
                all.push(m);
            }
            if items
                .last()
                .and_then(market_end_ts)
                .map(|item_end_ts| item_end_ts > end_ts)
                .unwrap_or(false)
            {
                break;
            }
            next_cursor = cursor;
            if items.len() < page_size as usize || next_cursor.is_none() {
                break;
            }
        }

        all.sort_by(|a, b| a.condition_id.cmp(&b.condition_id));
        all.dedup_by(|a, b| a.condition_id == b.condition_id);
        tracing::info!(
            count = all.len(),
            start = %start,
            end = %end,
            closed,
            "Gamma historical markets fetched"
        );
        Ok(all)
    }
}

fn unwrap_market_page(v: Value) -> (Vec<Value>, Option<String>) {
    let next_cursor = v
        .get("next_cursor")
        .and_then(|x| x.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty() && *s != "LTE=")
        .map(str::to_string);

    if let Value::Array(arr) = v {
        return (arr, next_cursor);
    }
    if let Some(arr) = v.get("data").and_then(|x| x.as_array()) {
        return (arr.clone(), next_cursor);
    }
    if let Some(arr) = v.get("markets").and_then(|x| x.as_array()) {
        return (arr.clone(), next_cursor);
    }
    (Vec::new(), next_cursor)
}

fn parse_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::Number(n) => n.as_u64().map(|x| x != 0),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn parse_fee_rate_decimal(v: &Value) -> Option<f64> {
    let rate = parse_f64(v)?;
    if rate.is_finite() && (0.0..=1.0).contains(&rate) {
        Some(rate)
    } else {
        None
    }
}

fn parse_fee_rate_bps(v: &Value) -> Option<f64> {
    let bps = parse_f64(v)?;
    if bps.is_finite() && bps >= 0.0 {
        Some(bps / 10_000.0)
    } else {
        None
    }
}

fn parse_taker_fee_rate(raw: &Value) -> Option<f64> {
    raw.get("fd")
        .and_then(|fd| fd.get("r"))
        .and_then(parse_fee_rate_decimal)
        .or_else(|| raw.get("feeRate").and_then(parse_fee_rate_decimal))
        .or_else(|| raw.get("fee_rate").and_then(parse_fee_rate_decimal))
        .or_else(|| raw.get("takerFeeRate").and_then(parse_fee_rate_decimal))
        .or_else(|| raw.get("taker_fee_rate").and_then(parse_fee_rate_decimal))
        .or_else(|| raw.get("feeRateBps").and_then(parse_fee_rate_bps))
        .or_else(|| raw.get("fee_rate_bps").and_then(parse_fee_rate_bps))
        .or_else(|| raw.get("takerFeeRateBps").and_then(parse_fee_rate_bps))
        .or_else(|| raw.get("taker_fee_rate_bps").and_then(parse_fee_rate_bps))
        .or_else(|| raw.get("tbf").and_then(parse_fee_rate_bps))
        .or_else(|| raw.get("base_fee").and_then(parse_fee_rate_bps))
}

fn parse_maker_fee_rate(raw: &Value) -> Option<f64> {
    raw.get("makerFeeRate")
        .and_then(parse_fee_rate_decimal)
        .or_else(|| raw.get("maker_fee_rate").and_then(parse_fee_rate_decimal))
        .or_else(|| raw.get("makerFeeRateBps").and_then(parse_fee_rate_bps))
        .or_else(|| raw.get("maker_fee_rate_bps").and_then(parse_fee_rate_bps))
        .or_else(|| raw.get("mbf").and_then(parse_fee_rate_bps))
}

fn parse_fees_enabled(raw: &Value) -> Option<bool> {
    raw.get("feesEnabled")
        .or_else(|| raw.get("fees_enabled"))
        .and_then(parse_bool)
}

fn parse_json_or_csv(v: Option<&Value>) -> Vec<String> {
    let Some(v) = v else { return Vec::new() };
    if let Value::Array(arr) = v {
        return arr
            .iter()
            .map(|x| match x {
                Value::String(s) => s.clone(),
                _ => x.to_string(),
            })
            .collect();
    }
    let Some(s) = v.as_str() else {
        return Vec::new();
    };
    let s = s.trim();
    if s.is_empty() {
        return Vec::new();
    }
    if s.starts_with('[') {
        if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(s) {
            return arr
                .into_iter()
                .map(|x| match x {
                    Value::String(s) => s,
                    other => other.to_string(),
                })
                .collect();
        }
    }
    s.split(',')
        .map(|p| p.trim().trim_matches('"').to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

fn market_end_ts(raw: &Value) -> Option<f64> {
    raw.get("endDate")
        .or_else(|| raw.get("end_date"))
        .and_then(|v| v.as_str())
        .and_then(parse_iso8601)
}

pub fn parse_gamma_market(raw: &Value) -> Option<Market> {
    let condition_id = raw
        .get("conditionId")
        .or_else(|| raw.get("condition_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let question = raw
        .get("question")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if condition_id.is_empty() || question.is_empty() {
        return None;
    }

    let outcome_names = parse_json_or_csv(raw.get("outcomes"));
    let outcome_prices_raw = parse_json_or_csv(
        raw.get("outcomePrices")
            .or_else(|| raw.get("outcome_prices")),
    );
    let outcome_prices: Vec<f64> = outcome_prices_raw
        .iter()
        .map(|s| s.parse::<f64>().unwrap_or(0.0))
        .collect();
    let token_ids = parse_json_or_csv(
        raw.get("clobTokenIds")
            .or_else(|| raw.get("clob_token_ids")),
    );

    let outcomes: Vec<Outcome> = outcome_names
        .iter()
        .enumerate()
        .map(|(i, name)| Outcome {
            token_id: token_ids.get(i).cloned().unwrap_or_default(),
            name: name.clone(),
            price: outcome_prices.get(i).copied().unwrap_or(0.0),
        })
        .collect();

    let tags_raw = raw.get("tags");
    let tags: Vec<String> = match tags_raw {
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect(),
        Some(Value::String(s)) => s
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect(),
        _ => Vec::new(),
    };

    let mut event_slug = String::new();
    let mut event_id = String::new();
    let mut event_title = String::new();
    let mut neg_risk_augmented = false;
    if let Some(Value::Array(events)) = raw.get("events") {
        if let Some(ev) = events.first() {
            event_slug = ev
                .get("slug")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            event_id = ev
                .get("id")
                .map(|v| match v {
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default();
            event_title = ev
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            neg_risk_augmented = ev
                .get("negRiskAugmented")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
        }
    }

    Some(Market {
        condition_id,
        question,
        slug: raw
            .get("slug")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        outcomes,
        tags,
        category: raw
            .get("category")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        active: raw.get("active").and_then(|v| v.as_bool()).unwrap_or(true),
        closed: raw.get("closed").and_then(|v| v.as_bool()).unwrap_or(false),
        volume: raw.get("volume").and_then(parse_f64).unwrap_or(0.0),
        liquidity: raw.get("liquidity").and_then(parse_f64).unwrap_or(0.0),
        end_date: raw
            .get("endDate")
            .or_else(|| raw.get("end_date"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        event_slug,
        event_id,
        event_title,
        group_slug: raw
            .get("groupSlug")
            .or_else(|| raw.get("group_slug"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        neg_risk: raw
            .get("negRisk")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        neg_risk_augmented,
        minimum_tick_size: raw
            .get("minimum_tick_size")
            .or_else(|| raw.get("minimumTickSize"))
            .and_then(parse_f64),
        fees_enabled: parse_fees_enabled(raw),
        taker_fee_rate: parse_taker_fee_rate(raw),
        maker_fee_rate: parse_maker_fee_rate(raw),
    })
}

fn parse_iso8601(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp() as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_csv_and_json() {
        let v = serde_json::json!(["Yes", "No"]);
        assert_eq!(parse_json_or_csv(Some(&v)), vec!["Yes", "No"]);

        let v = serde_json::json!("[\"Yes\",\"No\"]");
        assert_eq!(parse_json_or_csv(Some(&v)), vec!["Yes", "No"]);

        let v = serde_json::json!("Yes,No");
        assert_eq!(parse_json_or_csv(Some(&v)), vec!["Yes", "No"]);

        assert!(parse_json_or_csv(None).is_empty());
    }

    #[test]
    fn unwraps_keyset_market_page() {
        let raw = serde_json::json!({
            "markets": [{"conditionId": "0xabc", "question": "q"}],
            "next_cursor": "cursor-1"
        });
        let (items, cursor) = unwrap_market_page(raw);
        assert_eq!(items.len(), 1);
        assert_eq!(cursor.as_deref(), Some("cursor-1"));

        let raw = serde_json::json!({
            "markets": [],
            "next_cursor": "LTE="
        });
        let (items, cursor) = unwrap_market_page(raw);
        assert!(items.is_empty());
        assert!(cursor.is_none());
    }

    #[test]
    fn reads_market_end_timestamp() {
        let raw = serde_json::json!({"endDate": "2026-05-20T14:00:00Z"});
        assert_eq!(market_end_ts(&raw), Some(1_779_285_600.0));
    }

    #[test]
    fn parses_gamma_market_skeleton() {
        let raw = serde_json::json!({
            "conditionId": "0xabc",
            "question": "Bitcoin Up or Down - April 4, 3AM ET?",
            "outcomes": "[\"Up\",\"Down\"]",
            "outcomePrices": "[\"0.5\",\"0.5\"]",
            "clobTokenIds": "[\"t1\",\"t2\"]",
            "active": true,
            "closed": false,
            "endDate": "2026-04-04T07:00:00Z",
            "minimum_tick_size": "0.001",
            "feesEnabled": true,
            "fd": {"r": 0.07, "e": 2, "to": true},
            "mbf": 0,
        });
        let m = parse_gamma_market(&raw).unwrap();
        assert_eq!(m.condition_id, "0xabc");
        assert_eq!(m.outcomes.len(), 2);
        assert_eq!(m.outcomes[0].token_id, "t1");
        assert_eq!(m.outcomes[0].name, "Up");
        assert!((m.outcomes[0].price - 0.5).abs() < 1e-9);
        assert_eq!(m.minimum_tick_size, Some(0.001));
        assert_eq!(m.fees_enabled, Some(true));
        assert_eq!(m.taker_fee_rate, Some(0.07));
        assert_eq!(m.maker_fee_rate, Some(0.0));
    }

    #[test]
    fn parses_basis_point_fee_fields() {
        let raw = serde_json::json!({
            "conditionId": "0xabc",
            "question": "Bitcoin Up or Down - April 4, 3AM ET?",
            "tbf": 700,
            "makerFeeRateBps": "0",
        });
        let m = parse_gamma_market(&raw).unwrap();
        assert_eq!(m.taker_fee_rate, Some(0.07));
        assert_eq!(m.maker_fee_rate, Some(0.0));
    }
}
