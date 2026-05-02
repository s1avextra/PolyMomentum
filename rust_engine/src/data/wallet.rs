//! On-chain wallet balance reader (pUSD + USDC diagnostics + POL).

use anyhow::{Context, Result};
use k256::ecdsa::SigningKey;
use reqwest::Client;
use serde::Serialize;
use serde_json::json;
use sha3::{Digest, Keccak256};

pub const PUSD: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
pub const USDC_E: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
pub const USDC_NATIVE: &str = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359";
pub const CTF_EXCHANGE_V2: &str = "0xE111180000d2663C0091e4f400237545B87B996B";
pub const NEG_RISK_CTF_EXCHANGE_V2: &str = "0xe2222d279d744050d28e00520010520000310F59";
pub const COLLATERAL_ONRAMP: &str = "0x93070a847efEf7F70739046A929D47a521F5B8ee";
const BALANCE_OF: &str = "0x70a08231";
const ALLOWANCE: &str = "0xdd62ed3e";

#[derive(Debug, Clone, Default, Serialize)]
pub struct WalletBalances {
    pub address: String,
    pub pusd: f64,
    pub usdc_e: f64,
    pub usdc_native: f64,
    pub total_stable_diagnostics: f64,
    pub pusd_allowance_exchange: f64,
    pub pusd_allowance_neg_risk_exchange: f64,
    pub usdc_e_allowance_onramp: f64,
    pub pol: f64,
}

impl WalletBalances {
    pub fn live_ready(&self) -> bool {
        self.pusd >= 1.0
            && self.pusd_allowance_exchange >= 1.0
            && self.pusd_allowance_neg_risk_exchange >= 1.0
            && self.pol >= 0.01
    }

    pub fn live_ready_detail(&self) -> String {
        if self.live_ready() {
            format!(
                "wallet live_ready yes: address={} pUSD=${:.2} CTF_V2_allow=${:.2} NegRisk_allow=${:.2} POL={:.4}",
                self.address,
                self.pusd,
                self.pusd_allowance_exchange,
                self.pusd_allowance_neg_risk_exchange,
                self.pol
            )
        } else {
            format!(
                "wallet live_ready no: needs pUSD>=1.00, both CTF Exchange V2 pUSD allowances>=1.00, and POL>=0.01; observed address={} pUSD=${:.2} CTF_V2_allow=${:.2} NegRisk_allow=${:.2} POL={:.4}",
                self.address,
                self.pusd,
                self.pusd_allowance_exchange,
                self.pusd_allowance_neg_risk_exchange,
                self.pol
            )
        }
    }
}

pub fn address_from_private_key(pk_hex: &str) -> Result<String> {
    let pk_clean = pk_hex.trim_start_matches("0x");
    let bytes = hex::decode(pk_clean).context("decode private key")?;
    if bytes.len() != 32 {
        return Err(anyhow::anyhow!("private key must be 32 bytes"));
    }
    let signing_key = SigningKey::from_slice(&bytes).context("invalid private key")?;
    let verifying_key = signing_key.verifying_key();
    let public = verifying_key.to_encoded_point(false);
    let public_bytes = &public.as_bytes()[1..]; // strip 0x04 prefix
    let mut hasher = Keccak256::new();
    hasher.update(public_bytes);
    let hash = hasher.finalize();
    let address = &hash[12..]; // last 20 bytes
    Ok(format!("0x{}", hex::encode(address)))
}

pub struct WalletReader {
    rpc_url: String,
    http: Client,
    address: String,
}

impl WalletReader {
    pub fn new(rpc_url: impl Into<String>, private_key: &str) -> Result<Self> {
        let address = address_from_private_key(private_key)?;
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("client");
        Ok(Self { rpc_url: rpc_url.into(), http, address })
    }

    async fn balance_of(&self, token: &str) -> Result<u128> {
        let padded = padded_address(&self.address);
        let data = format!("{BALANCE_OF}{padded}");
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": token, "data": data}, "latest"],
            "id": 1,
        });
        let resp = self.http.post(&self.rpc_url).json(&body).send().await?;
        let v: serde_json::Value = resp.json().await?;
        let raw = v.get("result").and_then(|x| x.as_str()).unwrap_or("0x0");
        let trimmed = raw.trim_start_matches("0x");
        let trimmed = if trimmed.is_empty() { "0" } else { trimmed };
        Ok(u128::from_str_radix(trimmed, 16).unwrap_or(0))
    }

    async fn allowance_of(&self, token: &str, spender: &str) -> Result<f64> {
        let owner = padded_address(&self.address);
        let spender = padded_address(spender);
        let data = format!("{ALLOWANCE}{owner}{spender}");
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": token, "data": data}, "latest"],
            "id": 3,
        });
        let resp = self.http.post(&self.rpc_url).json(&body).send().await?;
        let v: serde_json::Value = resp.json().await?;
        let raw = v.get("result").and_then(|x| x.as_str()).unwrap_or("0x0");
        Ok(hex_units(raw, 6))
    }

    pub async fn fetch_balances(&self) -> Result<WalletBalances> {
        let pusd = self
            .balance_of(PUSD)
            .await
            .context("fetch pUSD balance")? as f64
            / 1e6;
        let usdc_e = self
            .balance_of(USDC_E)
            .await
            .context("fetch USDC.e balance")? as f64
            / 1e6;
        let usdc_native = self
            .balance_of(USDC_NATIVE)
            .await
            .context("fetch native USDC balance")? as f64
            / 1e6;
        let pusd_allowance_exchange = self
            .allowance_of(PUSD, CTF_EXCHANGE_V2)
            .await
            .context("fetch pUSD CTF Exchange V2 allowance")?;
        let pusd_allowance_neg_risk_exchange = self
            .allowance_of(PUSD, NEG_RISK_CTF_EXCHANGE_V2)
            .await
            .context("fetch pUSD Neg Risk CTF Exchange V2 allowance")?;
        let usdc_e_allowance_onramp = self
            .allowance_of(USDC_E, COLLATERAL_ONRAMP)
            .await
            .context("fetch USDC.e Collateral Onramp allowance")?;
        let pol = self.fetch_pol_balance().await.context("fetch POL balance")?;
        Ok(WalletBalances {
            address: self.address.clone(),
            pusd,
            usdc_e,
            usdc_native,
            total_stable_diagnostics: pusd + usdc_e + usdc_native,
            pusd_allowance_exchange,
            pusd_allowance_neg_risk_exchange,
            usdc_e_allowance_onramp,
            pol,
        })
    }

    async fn fetch_pol_balance(&self) -> Result<f64> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": [self.address, "latest"],
            "id": 2,
        });
        let resp = self.http.post(&self.rpc_url).json(&body).send().await?;
        let v: serde_json::Value = resp.json().await?;
        let raw = v.get("result").and_then(|x| x.as_str()).unwrap_or("0x0");
        let trimmed = raw.trim_start_matches("0x");
        let trimmed = if trimmed.is_empty() { "0" } else { trimmed };
        let wei = u128::from_str_radix(trimmed, 16).unwrap_or(0);
        Ok(wei as f64 / 1e18)
    }
}

fn padded_address(address: &str) -> String {
    format!("{:0>64}", address.trim_start_matches("0x").to_lowercase())
}

fn hex_units(raw: &str, decimals: u32) -> f64 {
    let mut value = 0.0;
    let trimmed = raw.trim_start_matches("0x");
    for c in trimmed.chars() {
        let Some(digit) = c.to_digit(16) else {
            return 0.0;
        };
        value = value * 16.0 + digit as f64;
        if !value.is_finite() {
            return f64::INFINITY;
        }
    }
    value / 10_f64.powi(decimals as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_known_address() {
        // Test vector from EIP-55: well-known test private key.
        let pk = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let addr = address_from_private_key(pk).unwrap();
        assert_eq!(addr.to_lowercase(), "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266");
    }

    #[test]
    fn parses_hex_units_with_decimals() {
        assert_eq!(hex_units("0x0", 6), 0.0);
        assert_eq!(hex_units("0xf4240", 6), 1.0);
        assert_eq!(hex_units("0x1e8480", 6), 2.0);
    }

    #[test]
    fn live_ready_requires_pusd_allowances_and_pol() {
        let mut b = WalletBalances {
            address: "0xabc".to_string(),
            pusd: 1.0,
            pusd_allowance_exchange: 1.0,
            pusd_allowance_neg_risk_exchange: 1.0,
            pol: 0.01,
            ..Default::default()
        };
        assert!(b.live_ready());
        b.pol = 0.009;
        assert!(!b.live_ready());
        assert!(b.live_ready_detail().contains("live_ready no"));
    }
}
