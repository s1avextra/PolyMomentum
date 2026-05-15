//! Polymarket Conditional Token Framework (CTF) reader.
//!
//! Reads on-chain market resolution from Polygon.
//!
//! For binary markets:
//!   payoutDenominator(cid) == 0 → not resolved
//!   payoutNumerators(cid, 0) > num[1] → outcome 0 (Up) won
//!   num0 == num1 > 0, and num0 + num1 == denominator → tie

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde_json::json;

pub const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const DEFAULT_POLYGON_RPC_URL: &str = "https://polygon-bor-rpc.publicnode.com";
const FALLBACK_POLYGON_RPC_URLS: &[&str] = &[
    DEFAULT_POLYGON_RPC_URL,
    "https://polygon.drpc.org",
    "https://polygon.api.onfinality.io/public",
];
const PAYOUT_DENOMINATOR: &str = "0xdd34de67";
const PAYOUT_NUMERATORS: &str = "0x0504c814";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    Up,
    Down,
    Tie,
    NotResolved,
}

impl Resolution {
    pub fn as_str(&self) -> &'static str {
        match self {
            Resolution::Up => "up",
            Resolution::Down => "down",
            Resolution::Tie => "tie",
            Resolution::NotResolved => "pending",
        }
    }
}

pub struct CtfReader {
    rpc_urls: Vec<String>,
    http: Client,
}

impl CtfReader {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("client");
        let rpc_urls = rpc_urls(rpc_url.into());
        Self { rpc_urls, http }
    }

    async fn eth_call(&self, data: &str) -> Result<u128> {
        let mut last_err = None;
        for rpc_url in &self.rpc_urls {
            match self.eth_call_one(rpc_url, data).await {
                Ok(value) => return Ok(value),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("no Polygon RPC URLs configured")))
    }

    async fn eth_call_one(&self, rpc_url: &str, data: &str) -> Result<u128> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": CTF_ADDRESS, "data": data}, "latest"],
            "id": 1,
        });
        let resp = self.http.post(rpc_url).json(&body).send().await?;
        let json: serde_json::Value = resp.json().await.context("decode json-rpc")?;
        if let Some(err) = json.get("error") {
            return Err(anyhow!("CTF eth_call error from {rpc_url}: {err}"));
        }
        let result = json.get("result").and_then(|v| v.as_str()).unwrap_or("0x0");
        let trimmed = result.trim_start_matches("0x");
        let trimmed = if trimmed.is_empty() { "0" } else { trimmed };
        // The result fits in u128 for any sensible payout numerator/denominator,
        // but we parse as u128 from hex.
        u128::from_str_radix(trimmed, 16).context("parse hex")
    }

    pub async fn get_resolution(&self, condition_id: &str) -> Result<(Resolution, [u128; 2])> {
        let cid = condition_id.trim_start_matches("0x");
        if cid.len() != 64 {
            return Err(anyhow!(
                "condition_id must be 32 bytes (64 hex chars), got {}",
                cid.len() / 2
            ));
        }

        let denom_call = format!("{PAYOUT_DENOMINATOR}{cid}");
        let denom = self.eth_call(&denom_call).await?;
        if denom == 0 {
            return Ok((Resolution::NotResolved, [0, 0]));
        }

        let num0_call = format!(
            "{PAYOUT_NUMERATORS}{cid}{}",
            "0".repeat(64),
        );
        let num1_call = format!(
            "{PAYOUT_NUMERATORS}{cid}{}",
            format!("{:0>64}", "1"),
        );
        let num0 = self.eth_call(&num0_call).await?;
        let num1 = self.eth_call(&num1_call).await?;
        let res = classify_binary_payout(denom, num0, num1);
        Ok((res, [num0, num1]))
    }
}

fn rpc_urls(raw: String) -> Vec<String> {
    let mut out: Vec<String> = raw
        .split([',', ';', ' ', '\n', '\t'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    for fallback in FALLBACK_POLYGON_RPC_URLS {
        if !out.iter().any(|url| url == fallback) {
            out.push((*fallback).to_string());
        }
    }
    out
}

fn classify_binary_payout(denom: u128, num0: u128, num1: u128) -> Resolution {
    if denom == 0 {
        return Resolution::NotResolved;
    }
    let Some(total) = num0.checked_add(num1) else {
        return Resolution::NotResolved;
    };
    if total != denom {
        return Resolution::NotResolved;
    }
    if num0 == num1 {
        Resolution::Tie
    } else if num0 > num1 {
        Resolution::Up
    } else {
        Resolution::Down
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_zero_payout_is_not_resolved_even_with_nonzero_denominator() {
        assert_eq!(classify_binary_payout(1, 0, 0), Resolution::NotResolved);
    }

    #[test]
    fn payout_vector_must_sum_to_denominator() {
        assert_eq!(classify_binary_payout(2, 1, 0), Resolution::NotResolved);
    }

    #[test]
    fn equal_nonzero_payouts_are_tie_when_complete() {
        assert_eq!(classify_binary_payout(2, 1, 1), Resolution::Tie);
    }

    #[test]
    fn binary_payouts_pick_winner() {
        assert_eq!(classify_binary_payout(1, 1, 0), Resolution::Up);
        assert_eq!(classify_binary_payout(1, 0, 1), Resolution::Down);
    }

    #[test]
    fn ctf_reader_adds_public_read_only_fallback() {
        let reader = CtfReader::new("https://example.invalid");
        assert_eq!(
            reader.rpc_urls,
            vec![
                "https://example.invalid".to_string(),
                "https://polygon-bor-rpc.publicnode.com".to_string(),
                "https://polygon.drpc.org".to_string(),
                "https://polygon.api.onfinality.io/public".to_string(),
            ]
        );
    }
}
