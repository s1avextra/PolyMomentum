//! Direct CLOB order placement — bypasses Python for the hot path.
//!
//! When the Rust engine detects an edge, it places orders directly
//! via the Polymarket CLOB API instead of signaling Python.
//!
//! Latency path: signal detection (~1µs) → order build + sign (~50µs) →
//!               HTTP POST (~1-5ms from Dublin) = ~5ms total
//!
//! The Python orchestrator still handles:
//!   - Market scanning / contract discovery
//!   - Risk management
//!   - Position tracking / state persistence
//!   - Monitoring / alerting

use k256::ecdsa::SigningKey;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

use crate::signing;

/// CLOB order placement client with connection pre-warming and EIP-712 signing.
pub struct ClobClient {
    client: Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    api_passphrase: String,
    signing_key: Option<SigningKey>,
    maker_address: String,
    /// Track order latencies for monitoring
    pub latencies: Vec<u64>,
    /// Pre-warmed: have we sent a test request to prime the connection?
    warmed: bool,
}

/// Signed order body for the CLOB /order endpoint.
#[derive(Debug, Serialize)]
struct SignedOrderRequest {
    order: OrderPayload,
    owner: String,        // API key owner in the CLOB V2 wire body
    #[serde(rename = "orderType")]
    order_type: String,   // "GTC" or "FOK"
    #[serde(rename = "deferExec")]
    defer_exec: bool,
}

#[derive(Debug, Serialize)]
struct OrderPayload {
    salt: String,
    maker: String,
    signer: String,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "makerAmount")]
    maker_amount: String,
    #[serde(rename = "takerAmount")]
    taker_amount: String,
    expiration: String,
    side: String,
    #[serde(rename = "signatureType")]
    signature_type: u8,
    timestamp: String,
    metadata: String,
    builder: String,
    signature: String,
}

#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    #[serde(rename = "orderID")]
    pub order_id: Option<String>,
    pub id: Option<String>,
    pub error: Option<String>,
}

impl ClobClient {
    pub fn new(
        base_url: &str,
        api_key: &str,
        api_secret: &str,
        api_passphrase: &str,
    ) -> Self {
        // Build client with connection pooling and HTTP/2
        let client = Client::builder()
            .pool_max_idle_per_host(5)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            api_passphrase: api_passphrase.to_string(),
            signing_key: None,
            maker_address: String::new(),
            latencies: Vec::with_capacity(1000),
            warmed: false,
        }
    }

    /// Set the private key for EIP-712 order signing.
    pub fn set_signing_key(&mut self, hex_key: &str) {
        if let Some(key) = signing::parse_private_key(hex_key) {
            let addr = signing::address_from_key(&key);
            self.maker_address = format!("0x{}", hex::encode(addr));
            self.signing_key = Some(key);
            eprintln!("CLOB signing key set: {}", self.maker_address);
        } else {
            eprintln!("CLOB signing key parse failed");
        }
    }

    /// Pre-warm the connection pool by sending a lightweight request.
    /// First request is ~70% slower due to TLS handshake + TCP setup.
    pub async fn warm_connection(&mut self) {
        if self.warmed {
            return;
        }
        let url = format!("{}/time", self.base_url);
        match self.client.get(&url).send().await {
            Ok(_) => {
                self.warmed = true;
                eprintln!("CLOB connection pre-warmed");
            }
            Err(e) => eprintln!("CLOB warm failed: {}", e),
        }
    }

    /// Build HMAC-SHA256 authenticated headers for a request.
    fn auth_headers(&self, method: &str, path: &str, body: &str) -> Vec<(String, String)> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();

        let signature =
            signing::hmac_sign_request(&self.api_secret, &timestamp, method, path, body);

        vec![
            ("POLY_ADDRESS".into(), self.maker_address.clone()),
            ("POLY_SIGNATURE".into(), signature),
            ("POLY_TIMESTAMP".into(), timestamp),
            ("POLY_API_KEY".into(), self.api_key.clone()),
            ("POLY_PASSPHRASE".into(), self.api_passphrase.clone()),
        ]
    }

    fn require_l2_auth(&self) -> Result<(), String> {
        let missing: Vec<&str> = [
            ("POLY_ADDRESS/PRIVATE_KEY", self.maker_address.as_str()),
            ("POLY_API_KEY", self.api_key.as_str()),
            ("POLY_API_SECRET", self.api_secret.as_str()),
            ("POLY_PASSPHRASE", self.api_passphrase.as_str()),
        ]
        .into_iter()
        .filter_map(|(name, value)| {
            if value.trim().is_empty() {
                Some(name)
            } else {
                None
            }
        })
        .collect();

        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!("missing L2 auth material: {}", missing.join(", ")))
        }
    }

    async fn get_public_json(&self, path: &str, params: &[(&str, &str)]) -> Result<Value, String> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .get(&url)
            .query(params)
            .send()
            .await
            .map_err(|e| format!("Request failed: {e}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status, &body[..100.min(body.len())]));
        }
        serde_json::from_str(&body).map_err(|e| format!("Parse error: {e}: {body}"))
    }

    async fn get_private_json(&self, path: &str, params: &[(&str, &str)]) -> Result<Value, String> {
        self.require_l2_auth()?;
        let path_with_query = path_with_query(path, params);
        let url = format!("{}{}", self.base_url, path_with_query);
        let headers = self.auth_headers("GET", &path_with_query, "");
        let mut req = self.client.get(&url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req.send().await.map_err(|e| format!("Request failed: {e}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status, &body[..100.min(body.len())]));
        }
        serde_json::from_str(&body).map_err(|e| format!("Parse error: {e}: {body}"))
    }

    async fn post_private_json(&self, path: &str, body: &str) -> Result<Value, String> {
        self.require_l2_auth()?;
        let url = format!("{}{}", self.base_url, path);
        let headers = self.auth_headers("POST", path, body);
        let mut req = self.client.post(&url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if !body.is_empty() {
            req = req.header("Content-Type", "application/json").body(body.to_string());
        }
        let resp = req.send().await.map_err(|e| format!("Request failed: {e}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status, &body[..100.min(body.len())]));
        }
        serde_json::from_str(&body).map_err(|e| format!("Parse error: {e}: {body}"))
    }

    /// Public CLOB health check. Does not require wallet/API credentials.
    pub async fn get_ok(&self) -> Result<Value, String> {
        self.get_public_json("/ok", &[]).await
    }

    /// Public CLOB server time. Does not require wallet/API credentials.
    pub async fn get_server_time(&self) -> Result<Value, String> {
        self.get_public_json("/time", &[]).await
    }

    /// Public order book by outcome token ID.
    pub async fn get_book(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/book", &[("token_id", token_id)]).await
    }

    pub async fn get_price(&self, token_id: &str, side: &str) -> Result<Value, String> {
        self.get_public_json("/price", &[("token_id", token_id), ("side", side)])
            .await
    }

    pub async fn get_midpoint(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/midpoint", &[("token_id", token_id)]).await
    }

    pub async fn get_spread(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/spread", &[("token_id", token_id)]).await
    }

    pub async fn get_tick_size(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/tick-size", &[("token_id", token_id)]).await
    }

    pub async fn get_fee_rate_bps(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/fee-rate", &[("token_id", token_id)]).await
    }

    pub async fn get_neg_risk(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/neg-risk", &[("token_id", token_id)]).await
    }

    pub async fn get_market(&self, condition_id: &str) -> Result<Value, String> {
        self.get_public_json("/market", &[("condition_id", condition_id)])
            .await
    }

    /// Authenticated open orders for reconciliation. Does not place orders.
    pub async fn get_user_orders(&self, params: &[(&str, &str)]) -> Result<Value, String> {
        self.get_private_json("/data/orders", params).await
    }

    /// Authenticated single-order status for reconciliation fallback.
    pub async fn get_order(&self, order_id: &str) -> Result<Value, String> {
        self.get_private_json(&format!("/order/{order_id}"), &[]).await
    }

    /// Authenticated user trades for reconciliation. Does not place orders.
    pub async fn get_trades(&self, params: &[(&str, &str)]) -> Result<Value, String> {
        self.get_private_json("/trades", params).await
    }

    /// Authenticated heartbeat for automated order safety.
    pub async fn post_heartbeat(&self) -> Result<Value, String> {
        self.post_private_json("/heartbeats", "").await
    }

    /// Place a GTC maker limit order (0% fee) with EIP-712 signing.
    pub async fn place_maker_order(
        &mut self,
        token_id: &str,
        price: f64,
        size: f64,
        side: &str,
        neg_risk: bool,
        tick_size: f64,
    ) -> Result<String, String> {
        self.place_order_internal(token_id, price, size, side, "GTC", neg_risk, tick_size)
            .await
    }

    /// Place a FOK taker order (crosses the spread immediately).
    pub async fn place_taker_order(
        &mut self,
        token_id: &str,
        price: f64,
        size: f64,
        side: &str,
        neg_risk: bool,
        tick_size: f64,
    ) -> Result<String, String> {
        self.place_order_internal(token_id, price, size, side, "FOK", neg_risk, tick_size)
            .await
    }

    /// Internal: build, sign, and submit an order.
    #[allow(clippy::too_many_arguments)]
    async fn place_order_internal(
        &mut self,
        token_id: &str,
        price: f64,
        size: f64,
        side: &str,
        order_type: &str, // "GTC" or "FOK"
        neg_risk: bool,
        tick_size: f64,
    ) -> Result<String, String> {
        let key = self
            .signing_key
            .as_ref()
            .ok_or_else(|| "No signing key set".to_string())?;

        let t0 = Instant::now();

        // Build and sign the CLOB V2 order. Fees are protocol/operator-set at
        // match time in V2 and are not part of the signed EIP-712 struct.
        let order = signing::build_order(key, token_id, price, size, side, neg_risk, tick_size);
        let signed = signing::sign_order(&order, key, neg_risk);

        let sign_us = t0.elapsed().as_micros();

        // Serialize to CLOB API format
        let payload = SignedOrderRequest {
            order: OrderPayload {
                salt: signed.order.salt.to_string(),
                maker: format!("0x{}", hex::encode(signed.order.maker)),
                signer: format!("0x{}", hex::encode(signed.order.signer)),
                token_id: signed.order.token_id.clone(),
                maker_amount: signed.order.maker_amount.to_string(),
                taker_amount: signed.order.taker_amount.to_string(),
                expiration: "0".to_string(),
                side: side.to_string(),
                signature_type: signed.order.signature_type,
                timestamp: signed.order.timestamp_ms.to_string(),
                metadata: format!("0x{}", hex::encode(signed.order.metadata)),
                builder: format!("0x{}", hex::encode(signed.order.builder)),
                signature: format!("0x{}", signed.signature),
            },
            owner: self.api_key.clone(),
            order_type: order_type.to_string(),
            defer_exec: false,
        };

        let body = serde_json::to_string(&payload).map_err(|e| format!("Serialize: {}", e))?;

        // Build auth headers
        let headers = self.auth_headers("POST", "/order", &body);

        let url = format!("{}/order", self.base_url);
        let mut req = self.client.post(&url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", "application/json");
        req = req.body(body);

        let result = req.send().await;
        let latency_us = t0.elapsed().as_micros() as u64;
        self.latencies.push(latency_us);

        match result {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();

                if !status.is_success() {
                    return Err(format!("HTTP {}: {}", status, &body[..100.min(body.len())]));
                }

                match serde_json::from_str::<OrderResponse>(&body) {
                    Ok(order_resp) => {
                        if let Some(err) = order_resp.error {
                            return Err(err);
                        }
                        let oid = order_resp
                            .order_id
                            .or(order_resp.id)
                            .unwrap_or_default();
                        eprintln!(
                            "Order {} placed in {}µs (sign: {}µs): {} {} {:.1}@{:.4} id={}",
                            order_type,
                            latency_us,
                            sign_us,
                            side,
                            token_id.get(..16).unwrap_or(token_id),
                            size,
                            price,
                            oid.get(..16).unwrap_or(&oid)
                        );
                        Ok(oid)
                    }
                    Err(e) => Err(format!("Parse error: {}", e)),
                }
            }
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
}

fn path_with_query(path: &str, params: &[(&str, &str)]) -> String {
    if params.is_empty() {
        return path.to_string();
    }
    let query = params
        .iter()
        .filter(|(_, value)| !value.trim().is_empty())
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    }
}

/// Shared CLOB client wrapped for async access
pub type SharedClobClient = Arc<RwLock<ClobClient>>;

pub fn create_shared_client(
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    api_passphrase: &str,
) -> SharedClobClient {
    Arc::new(RwLock::new(ClobClient::new(
        base_url,
        api_key,
        api_secret,
        api_passphrase,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_auth_uses_current_poly_header_names() {
        let mut client = ClobClient::new("https://clob.polymarket.com", "key", "secret", "pass");
        client.maker_address = "0x0000000000000000000000000000000000000001".to_string();
        let headers = client.auth_headers("GET", "/data/orders", "");
        let names: Vec<_> = headers.into_iter().map(|(name, _)| name).collect();

        assert_eq!(
            names,
            vec![
                "POLY_ADDRESS",
                "POLY_SIGNATURE",
                "POLY_TIMESTAMP",
                "POLY_API_KEY",
                "POLY_PASSPHRASE",
            ]
        );
    }

    #[test]
    fn path_query_omits_empty_values() {
        assert_eq!(
            path_with_query("/trades", &[("market", ""), ("after", "123")]),
            "/trades?after=123"
        );
    }
}
