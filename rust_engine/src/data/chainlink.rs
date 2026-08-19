use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const DEFAULT_DATA_STREAMS_REST_URL: &str = "https://api.dataengine.chain.link";

#[derive(Debug, Clone, Serialize)]
pub struct ChainlinkAuthHeaders {
    pub authorization: String,
    pub timestamp_ms: String,
    pub signature_sha256: String,
    pub body_hash: String,
    pub string_to_sign: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChainlinkReportSummary {
    pub feed_id: Option<String>,
    pub valid_from_timestamp: Option<i64>,
    pub observations_timestamp: Option<i64>,
    pub full_report_bytes: Option<usize>,
    pub decoded_price: Option<String>,
    pub decoded_bid: Option<String>,
    pub decoded_ask: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChainlinkRestProbe {
    pub feed_id: String,
    pub full_path: String,
    pub request_started_ms: i64,
    pub response_received_ms: i64,
    pub http_status: u16,
    pub latency_ms: u128,
    pub observation_lag_ms: Option<i64>,
    pub report: Option<ChainlinkReportSummary>,
    pub error: Option<String>,
    pub raw_response: Option<Value>,
}

#[derive(Clone)]
pub struct ChainlinkDataStreamsClient {
    base_url: String,
    api_key: String,
    api_secret: String,
    http: Client,
}

impl ChainlinkDataStreamsClient {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("polymomentum-engine/0.2")
            .build()
            .expect("reqwest client builds");
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            http,
        }
    }

    pub async fn latest_report(&self, feed_id: &str) -> Result<ChainlinkRestProbe> {
        let full_path = format!("/api/v1/reports/latest?feedID={feed_id}");
        let timestamp_ms = chrono::Utc::now().timestamp_millis().to_string();
        let auth = chainlink_auth_headers(
            &self.api_key,
            &self.api_secret,
            "GET",
            &full_path,
            "",
            &timestamp_ms,
        );
        let url = format!("{}{}", self.base_url, full_path);
        let started_at = chrono::Utc::now().timestamp_millis();
        let started = Instant::now();
        let resp = self
            .http
            .get(&url)
            .header("Authorization", auth.authorization)
            .header("X-Authorization-Timestamp", auth.timestamp_ms)
            .header("X-Authorization-Signature-SHA256", auth.signature_sha256)
            .send()
            .await
            .with_context(|| format!("request Chainlink Data Streams latest report {feed_id}"))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .context("read Chainlink Data Streams response body")?;
        let received_at = chrono::Utc::now().timestamp_millis();
        let raw = serde_json::from_str::<Value>(&text).ok();
        let report = raw.as_ref().and_then(parse_chainlink_report_summary);
        let observation_lag_ms = report
            .as_ref()
            .and_then(|r| r.observations_timestamp)
            .map(|ts| received_at.saturating_sub(ts.saturating_mul(1000)));
        Ok(ChainlinkRestProbe {
            feed_id: feed_id.to_string(),
            full_path,
            request_started_ms: started_at,
            response_received_ms: received_at,
            http_status: status.as_u16(),
            latency_ms: started.elapsed().as_millis(),
            observation_lag_ms,
            report,
            error: if status.is_success() {
                None
            } else {
                Some(text)
            },
            raw_response: raw,
        })
    }
}

pub fn chainlink_auth_headers(
    api_key: &str,
    api_secret: &str,
    method: &str,
    full_path: &str,
    body: &str,
    timestamp_ms: &str,
) -> ChainlinkAuthHeaders {
    let body_hash = sha256_hex(body.as_bytes());
    let method = method.to_ascii_uppercase();
    let string_to_sign =
        chainlink_string_to_sign(&method, full_path, &body_hash, api_key, timestamp_ms);
    let signature_sha256 = hmac_sha256_hex(api_secret.as_bytes(), string_to_sign.as_bytes());
    ChainlinkAuthHeaders {
        authorization: api_key.to_string(),
        timestamp_ms: timestamp_ms.to_string(),
        signature_sha256,
        body_hash,
        string_to_sign,
    }
}

pub fn chainlink_string_to_sign(
    method: &str,
    full_path: &str,
    body_hash: &str,
    api_key: &str,
    timestamp_ms: &str,
) -> String {
    format!("{method} {full_path} {body_hash} {api_key} {timestamp_ms}")
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn hmac_sha256_hex(key: &[u8], message: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC key length error");
    mac.update(message);
    hex::encode(mac.finalize().into_bytes())
}

pub fn parse_chainlink_report_summary(value: &Value) -> Option<ChainlinkReportSummary> {
    let report = value
        .get("report")
        .or_else(|| value.get("reports").and_then(|v| v.as_array()?.first()))?;
    Some(ChainlinkReportSummary {
        feed_id: report
            .get("feedID")
            .or_else(|| report.get("feedId"))
            .and_then(|v| v.as_str())
            .map(str::to_string),
        valid_from_timestamp: report
            .get("validFromTimestamp")
            .or_else(|| report.get("valid_from_timestamp"))
            .and_then(json_i64),
        observations_timestamp: report
            .get("observationsTimestamp")
            .or_else(|| report.get("observations_timestamp"))
            .and_then(json_i64),
        full_report_bytes: report
            .get("fullReport")
            .or_else(|| report.get("full_report"))
            .and_then(|v| v.as_str())
            .map(hex_blob_len),
        decoded_price: report
            .get("price")
            .and_then(json_string)
            .filter(|s| !s.is_empty()),
        decoded_bid: report
            .get("bid")
            .and_then(json_string)
            .filter(|s| !s.is_empty()),
        decoded_ask: report
            .get("ask")
            .and_then(json_string)
            .filter(|s| !s.is_empty()),
    })
}

fn json_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn json_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn hex_blob_len(raw: &str) -> usize {
    let trimmed = raw.strip_prefix("0x").unwrap_or(raw);
    trimmed.len() / 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chainlink_empty_body_hash_matches_sha256() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn chainlink_hmac_sha256_hex_matches_known_vector() {
        assert_eq!(
            hmac_sha256_hex(b"key", b"The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn chainlink_auth_headers_use_documented_string_to_sign() {
        let auth = chainlink_auth_headers(
            "api-key",
            "secret",
            "get",
            "/api/v1/reports/latest?feedID=0xabc",
            "",
            "1716211845123",
        );
        assert_eq!(
            auth.string_to_sign,
            "GET /api/v1/reports/latest?feedID=0xabc e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 api-key 1716211845123"
        );
        assert_eq!(auth.authorization, "api-key");
        assert_eq!(auth.signature_sha256.len(), 64);
    }

    #[test]
    fn chainlink_report_summary_parses_rest_latest_shape() {
        let value = serde_json::json!({
            "report": {
                "feedID": "0xfeed",
                "validFromTimestamp": "1782904500",
                "observationsTimestamp": "1782904501",
                "fullReport": "0x010203",
                "price": "6500012345678",
                "bid": "6500010000000",
                "ask": "6500015000000"
            }
        });
        let summary = parse_chainlink_report_summary(&value).unwrap();
        assert_eq!(summary.feed_id.as_deref(), Some("0xfeed"));
        assert_eq!(summary.observations_timestamp, Some(1_782_904_501));
        assert_eq!(summary.full_report_bytes, Some(3));
        assert_eq!(summary.decoded_price.as_deref(), Some("6500012345678"));
    }
}
