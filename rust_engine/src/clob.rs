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
use std::fmt;
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
    owner: String, // API key owner in the CLOB V2 wire body
    #[serde(rename = "orderType")]
    order_type: String, // "GTC" or "FOK"
    #[serde(rename = "postOnly", skip_serializing_if = "Option::is_none")]
    post_only: Option<bool>,
    #[serde(rename = "deferExec")]
    defer_exec: bool,
}

#[derive(Debug, Serialize)]
struct OrderPayload {
    /// JSON integer on the wire: the venue's strictly-typed decoder rejects a
    /// string salt with "Invalid order payload" (reference client sends
    /// `int(order.salt)`). Salt is generated < 2^64 so u64 is lossless.
    salt: u64,
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
    #[serde(rename = "errorMsg")]
    pub error_msg: Option<String>,
    pub success: Option<bool>,
    pub status: Option<String>,
}

#[derive(Debug)]
pub struct PreparedOrder {
    body: String,
    expected_order_id: String,
    order_type: String,
    side: String,
    token_id: String,
    price: f64,
    size: f64,
    sign_us: u128,
    started_at: Instant,
}

impl PreparedOrder {
    pub fn expected_order_id(&self) -> &str {
        &self.expected_order_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionReceipt {
    pub order_id: String,
    pub expected_order_id: String,
}

impl SubmissionReceipt {
    pub fn id_matches_expected(&self) -> bool {
        self.order_id.eq_ignore_ascii_case(&self.expected_order_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitFailureKind {
    DefinitiveReject,
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitOrderError {
    pub kind: SubmitFailureKind,
    pub message: String,
}

impl SubmitOrderError {
    fn definitive(message: impl Into<String>) -> Self {
        Self {
            kind: SubmitFailureKind::DefinitiveReject,
            message: message.into(),
        }
    }

    fn ambiguous(message: impl Into<String>) -> Self {
        Self {
            kind: SubmitFailureKind::Ambiguous,
            message: message.into(),
        }
    }
}

impl fmt::Display for SubmitOrderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SubmitOrderError {}

impl ClobClient {
    pub fn new(
        base_url: &str,
        api_key: &str,
        api_secret: &str,
        api_passphrase: &str,
    ) -> Result<Self, String> {
        // Build client with connection pooling and HTTP/2
        let client = Client::builder()
            .pool_max_idle_per_host(5)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .build()
            .map_err(|error| format!("build CLOB HTTP client: {error}"))?;

        Ok(Self {
            client,
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            api_passphrase: api_passphrase.to_string(),
            signing_key: None,
            maker_address: String::new(),
            latencies: Vec::with_capacity(1000),
            warmed: false,
        })
    }

    /// Set the private key for EIP-712 order signing.
    pub fn set_signing_key(&mut self, hex_key: &str) -> Result<(), String> {
        let Some(key) = signing::parse_private_key(hex_key) else {
            self.signing_key = None;
            self.maker_address.clear();
            return Err("PRIVATE_KEY must be a valid secp256k1 private key".to_string());
        };
        let addr = signing::address_from_key(&key);
        self.maker_address = format!("0x{}", hex::encode(addr));
        self.signing_key = Some(key);
        eprintln!("CLOB signing key set: {}", self.maker_address);
        Ok(())
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
    fn auth_headers(
        &self,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<Vec<(String, String)>, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
            .as_secs()
            .to_string();

        let signature =
            signing::hmac_sign_request(&self.api_secret, &timestamp, method, path, body)?;

        Ok(vec![
            ("POLY_ADDRESS".into(), self.maker_address.clone()),
            ("POLY_SIGNATURE".into(), signature),
            ("POLY_TIMESTAMP".into(), timestamp),
            ("POLY_API_KEY".into(), self.api_key.clone()),
            ("POLY_PASSPHRASE".into(), self.api_passphrase.clone()),
        ])
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

        if !missing.is_empty() {
            Err(format!("missing L2 auth material: {}", missing.join(", ")))
        } else if !signing::api_secret_is_valid(&self.api_secret) {
            Err("POLY_API_SECRET is not valid URL-safe base64".to_string())
        } else {
            Ok(())
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
        let headers = self.auth_headers("GET", &path_with_query, "")?;
        let mut req = self.client.get(&url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req
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

    async fn post_private_json(&self, path: &str, body: &str) -> Result<Value, String> {
        self.require_l2_auth()?;
        let url = format!("{}{}", self.base_url, path);
        let headers = self.auth_headers("POST", path, body)?;
        let mut req = self.client.post(&url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if !body.is_empty() {
            req = req
                .header("Content-Type", "application/json")
                .body(body.to_string());
        }
        let resp = req
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
        self.get_public_json("/book", &[("token_id", token_id)])
            .await
    }

    pub async fn get_price(&self, token_id: &str, side: &str) -> Result<Value, String> {
        self.get_public_json("/price", &[("token_id", token_id), ("side", side)])
            .await
    }

    pub async fn get_midpoint(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/midpoint", &[("token_id", token_id)])
            .await
    }

    pub async fn get_spread(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/spread", &[("token_id", token_id)])
            .await
    }

    pub async fn get_tick_size(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/tick-size", &[("token_id", token_id)])
            .await
    }

    pub async fn get_fee_rate_bps(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/fee-rate", &[("token_id", token_id)])
            .await
    }

    pub async fn get_neg_risk(&self, token_id: &str) -> Result<Value, String> {
        self.get_public_json("/neg-risk", &[("token_id", token_id)])
            .await
    }

    pub async fn get_market(&self, condition_id: &str) -> Result<Value, String> {
        self.get_public_json(&format!("/clob-markets/{condition_id}"), &[])
            .await
    }

    /// Authenticated open orders for reconciliation. Does not place orders.
    pub async fn get_user_orders(&self, params: &[(&str, &str)]) -> Result<Value, String> {
        self.get_private_json("/data/orders", params).await
    }

    /// Authenticated single-order status for reconciliation fallback.
    pub async fn get_order(&self, order_id: &str) -> Result<Value, String> {
        self.get_private_json(&format!("/order/{order_id}"), &[])
            .await
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
        let prepared =
            self.prepare_maker_order(token_id, price, size, side, neg_risk, tick_size)?;
        self.submit_prepared_order(prepared)
            .await
            .map(|receipt| receipt.order_id)
            .map_err(|error| error.to_string())
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
        let prepared =
            self.prepare_taker_order(token_id, price, size, side, neg_risk, tick_size)?;
        self.submit_prepared_order(prepared)
            .await
            .map(|receipt| receipt.order_id)
            .map_err(|error| error.to_string())
    }

    pub fn prepare_maker_order(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        side: &str,
        neg_risk: bool,
        tick_size: f64,
    ) -> Result<PreparedOrder, String> {
        self.prepare_order_internal(
            token_id, price, size, side, "GTC", true, neg_risk, tick_size,
        )
    }

    pub fn prepare_taker_order(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        side: &str,
        neg_risk: bool,
        tick_size: f64,
    ) -> Result<PreparedOrder, String> {
        self.prepare_order_internal(
            token_id, price, size, side, "FOK", false, neg_risk, tick_size,
        )
    }

    /// Build and sign an order without crossing the network.
    #[allow(clippy::too_many_arguments)]
    fn prepare_order_internal(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        side: &str,
        order_type: &str, // "GTC" or "FOK"
        post_only: bool,
        neg_risk: bool,
        tick_size: f64,
    ) -> Result<PreparedOrder, String> {
        self.require_l2_auth()?;
        let key = self
            .signing_key
            .as_ref()
            .ok_or_else(|| "No signing key set".to_string())?;

        let t0 = Instant::now();

        // Build and sign the CLOB V2 order. Fees are protocol/operator-set at
        // match time in V2 and are not part of the signed EIP-712 struct.
        let market_order = matches!(order_type, "FOK" | "FAK");
        let order = signing::build_order(key, token_id, price, size, side, tick_size, market_order)
            .map_err(|error| format!("Build order: {error}"))?;
        let signed = signing::sign_order(&order, key, neg_risk)
            .map_err(|error| format!("Sign order: {error}"))?;
        let expected_order_id = signing::order_hash(&signed.order, neg_risk)
            .map_err(|error| format!("Hash order: {error}"))?;

        let sign_us = t0.elapsed().as_micros();

        // Serialize to CLOB API format
        let payload = SignedOrderRequest {
            order: OrderPayload {
                salt: signed.order.salt as u64,
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
            post_only: Some(post_only),
            defer_exec: false,
        };

        let body = serde_json::to_string(&payload).map_err(|e| format!("Serialize: {}", e))?;

        Ok(PreparedOrder {
            body,
            expected_order_id,
            order_type: order_type.to_string(),
            side: side.to_string(),
            token_id: token_id.to_string(),
            price,
            size,
            sign_us,
            started_at: t0,
        })
    }

    /// Submit a previously journaled order. Transport/server uncertainty is
    /// reported separately from a definitive venue rejection so callers keep
    /// the expected order hash locked for REST recovery.
    pub async fn submit_prepared_order(
        &mut self,
        prepared: PreparedOrder,
    ) -> Result<SubmissionReceipt, SubmitOrderError> {
        let headers = self
            .auth_headers("POST", "/order", &prepared.body)
            .map_err(SubmitOrderError::ambiguous)?;

        // Build auth headers
        let url = format!("{}/order", self.base_url);
        let mut req = self.client.post(&url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", "application/json");
        req = req.body(prepared.body.clone());

        let result = req.send().await;
        let latency_us = prepared.started_at.elapsed().as_micros() as u64;
        self.latencies.push(latency_us);

        match result {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();

                if !status.is_success() {
                    let message = format!("HTTP {}: {}", status, &body[..100.min(body.len())]);
                    return if status.is_client_error()
                        && status != reqwest::StatusCode::REQUEST_TIMEOUT
                        && status != reqwest::StatusCode::TOO_MANY_REQUESTS
                    {
                        Err(SubmitOrderError::definitive(message))
                    } else {
                        Err(SubmitOrderError::ambiguous(message))
                    };
                }

                match serde_json::from_str::<OrderResponse>(&body) {
                    Ok(order_resp) => {
                        if let Some(err) = order_resp.error.filter(|e| !e.trim().is_empty()) {
                            return Err(SubmitOrderError::definitive(err));
                        }
                        if let Some(err) = order_resp.error_msg.filter(|e| !e.trim().is_empty()) {
                            return Err(SubmitOrderError::definitive(err));
                        }
                        if matches!(order_resp.success, Some(false)) {
                            return Err(SubmitOrderError::definitive(format!(
                                "CLOB rejected order: status={}",
                                order_resp.status.unwrap_or_else(|| "unknown".to_string())
                            )));
                        }
                        let oid = order_resp.order_id.or(order_resp.id).unwrap_or_default();
                        if oid.is_empty() {
                            return Err(SubmitOrderError::ambiguous(format!(
                                "CLOB response missing order id: status={}",
                                order_resp.status.unwrap_or_else(|| "unknown".to_string())
                            )));
                        }
                        eprintln!(
                            "Order {} placed in {}µs (sign: {}µs): {} {} {:.1}@{:.4} id={}",
                            prepared.order_type,
                            latency_us,
                            prepared.sign_us,
                            prepared.side,
                            prepared.token_id.get(..16).unwrap_or(&prepared.token_id),
                            prepared.size,
                            prepared.price,
                            oid.get(..16).unwrap_or(&oid)
                        );
                        Ok(SubmissionReceipt {
                            order_id: oid,
                            expected_order_id: prepared.expected_order_id,
                        })
                    }
                    Err(e) => Err(SubmitOrderError::ambiguous(format!("Parse error: {e}"))),
                }
            }
            Err(e) => Err(SubmitOrderError::ambiguous(format!("Request failed: {e}"))),
        }
    }

    /// Cancel one resting order (`DELETE /order`). The transport/venue
    /// uncertainty split mirrors submission: a definitive venue "not
    /// canceled" reason is distinguishable from an ambiguous outcome where
    /// the order may or may not still rest — callers must treat ambiguous
    /// results as "possibly still live" and reconcile via the user channel
    /// or REST lookup, never as a completed cancel.
    ///
    /// This is the primitive required before `LIVE_ALLOW_MAKER_ORDERS` can
    /// ever be enabled; the resting-order timeout policy that drives it is
    /// separate work in the pipeline.
    pub async fn cancel_order(&mut self, order_id: &str) -> Result<CancelReceipt, SubmitOrderError> {
        if order_id.trim().is_empty() {
            return Err(SubmitOrderError::definitive("empty order id".to_string()));
        }
        let body = serde_json::to_string(&CancelPayload {
            order_id: order_id.to_string(),
        })
        .map_err(|e| SubmitOrderError::definitive(format!("Serialize cancel: {e}")))?;
        let headers = self
            .auth_headers("DELETE", "/order", &body)
            .map_err(SubmitOrderError::ambiguous)?;
        let url = format!("{}/order", self.base_url);
        let mut req = self.client.delete(&url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", "application/json").body(body);

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                if !status.is_success() {
                    let message = format!("HTTP {}: {}", status, &body[..100.min(body.len())]);
                    return if status.is_client_error()
                        && status != reqwest::StatusCode::REQUEST_TIMEOUT
                        && status != reqwest::StatusCode::TOO_MANY_REQUESTS
                    {
                        Err(SubmitOrderError::definitive(message))
                    } else {
                        Err(SubmitOrderError::ambiguous(message))
                    };
                }
                match serde_json::from_str::<CancelResponse>(&body) {
                    Ok(cancel_resp) => {
                        if cancel_resp.canceled.iter().any(|id| id == order_id) {
                            return Ok(CancelReceipt {
                                order_id: order_id.to_string(),
                            });
                        }
                        if let Some(reason) = cancel_resp.not_canceled.get(order_id) {
                            return Err(SubmitOrderError::definitive(format!(
                                "venue refused cancel: {reason}"
                            )));
                        }
                        Err(SubmitOrderError::ambiguous(
                            "cancel response listed neither canceled nor a refusal reason"
                                .to_string(),
                        ))
                    }
                    Err(e) => Err(SubmitOrderError::ambiguous(format!("Parse error: {e}"))),
                }
            }
            Err(e) => Err(SubmitOrderError::ambiguous(format!("Request failed: {e}"))),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct CancelPayload {
    #[serde(rename = "orderID")]
    order_id: String,
}

/// `DELETE /order` response: ids actually canceled plus per-id refusal
/// reasons for the rest.
#[derive(Debug, Clone, Default, serde::Deserialize)]
struct CancelResponse {
    #[serde(default)]
    canceled: Vec<String>,
    #[serde(default)]
    not_canceled: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CancelReceipt {
    pub order_id: String,
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
) -> Result<SharedClobClient, String> {
    Ok(Arc::new(RwLock::new(ClobClient::new(
        base_url,
        api_key,
        api_secret,
        api_passphrase,
    )?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_auth_uses_current_poly_header_names() {
        let mut client =
            ClobClient::new("https://clob.polymarket.com", "key", "c2VjcmV0", "pass").unwrap();
        client.maker_address = "0x0000000000000000000000000000000000000001".to_string();
        let headers = client.auth_headers("GET", "/data/orders", "").unwrap();
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

    #[test]
    fn l2_auth_rejects_malformed_api_secret() {
        let mut client =
            ClobClient::new("https://clob.polymarket.com", "key", "not base64!", "pass").unwrap();
        client.maker_address = "0x0000000000000000000000000000000000000001".to_string();

        let err = client.require_l2_auth().unwrap_err();
        assert!(err.contains("POLY_API_SECRET"));
    }

    #[test]
    fn invalid_private_key_clears_stale_signing_identity() {
        let mut client =
            ClobClient::new("https://clob.polymarket.com", "key", "c2VjcmV0", "pass").unwrap();
        client
            .set_signing_key("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
            .unwrap();
        assert!(client.signing_key.is_some());

        assert!(client.set_signing_key("invalid").is_err());
        assert!(client.signing_key.is_none());
        assert!(client.maker_address.is_empty());
    }

    #[test]
    fn signed_order_request_serializes_post_only_flag() {
        let req = SignedOrderRequest {
            order: OrderPayload {
                salt: 1,
                maker: "0x0000000000000000000000000000000000000001".to_string(),
                signer: "0x0000000000000000000000000000000000000001".to_string(),
                token_id: "123".to_string(),
                maker_amount: "1000000".to_string(),
                taker_amount: "2000000".to_string(),
                expiration: "0".to_string(),
                side: "BUY".to_string(),
                signature_type: 0,
                timestamp: "1770000000000".to_string(),
                metadata: "0x".to_string(),
                builder: "0x".to_string(),
                signature: "0xabc".to_string(),
            },
            owner: "owner".to_string(),
            order_type: "GTC".to_string(),
            post_only: Some(true),
            defer_exec: false,
        };

        let body = serde_json::to_string(&req).unwrap();

        assert!(body.contains("\"orderType\":\"GTC\""));
        assert!(body.contains("\"postOnly\":true"));
        assert!(body.contains("\"deferExec\":false"));
        // The venue's decoder requires an integer salt; a quoted salt is
        // rejected as "Invalid order payload" (observed live 2026-08-25).
        assert!(body.contains("\"salt\":1,"));
    }

    #[test]
    fn prepares_order_and_lookup_hash_before_network_submission() {
        let mut client =
            ClobClient::new("https://clob.polymarket.com", "key", "c2VjcmV0", "pass").unwrap();
        client
            .set_signing_key("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
            .unwrap();

        let prepared = client
            .prepare_maker_order("123", 0.5, 10.0, "BUY", false, 0.01)
            .unwrap();

        assert_eq!(prepared.expected_order_id().len(), 66);
        assert!(prepared.expected_order_id().starts_with("0x"));
        assert!(prepared.body.contains("\"orderType\":\"GTC\""));
        assert!(prepared.body.contains("\"postOnly\":true"));
    }

    #[test]
    fn submission_receipt_compares_hashes_case_insensitively() {
        let receipt = SubmissionReceipt {
            order_id: "0xABCD".to_string(),
            expected_order_id: "0xabcd".to_string(),
        };
        assert!(receipt.id_matches_expected());
    }

    #[test]
    fn cancel_payload_uses_venue_field_name() {
        let body = serde_json::to_string(&CancelPayload {
            order_id: "0xabc".to_string(),
        })
        .unwrap();
        assert_eq!(body, r#"{"orderID":"0xabc"}"#);
    }

    #[test]
    fn cancel_response_distinguishes_refusal_from_silence() {
        let ok: CancelResponse =
            serde_json::from_str(r#"{"canceled":["0xabc"],"not_canceled":{}}"#).unwrap();
        assert!(ok.canceled.iter().any(|id| id == "0xabc"));

        let refused: CancelResponse =
            serde_json::from_str(r#"{"canceled":[],"not_canceled":{"0xabc":"order not found"}}"#)
                .unwrap();
        assert_eq!(refused.not_canceled.get("0xabc").unwrap(), "order not found");

        // Neither listed -> the caller must treat the order as possibly live.
        let silent: CancelResponse = serde_json::from_str(r#"{}"#).unwrap();
        assert!(silent.canceled.is_empty() && silent.not_canceled.is_empty());
    }
}
