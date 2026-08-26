//! EIP-712 order signing for Polymarket CLOB.
//!
//! Implements the exact signing protocol used by py_clob_client's OrderBuilder:
//!   1. Encode order struct per EIP-712 type hashing
//!   2. Build domain separator
//!   3. Compute \x19\x01 || domainSeparator || structHash
//!   4. ECDSA sign with k256 (secp256k1)
//!
//! Target: <500µs per signature (actual: ~50µs on modern hardware).

use k256::ecdsa::{RecoveryId, Signature, SigningKey};
use sha3::{Digest, Keccak256};

/// CLOB V2 standard CTF Exchange (non neg-risk)
pub const EXCHANGE_ADDRESS: &str = "E111180000d2663C0091e4f400237545B87B996B";
/// CLOB V2 neg-risk CTF Exchange
pub const NEG_RISK_EXCHANGE_ADDRESS: &str = "e2222d279d744050d28e00520010520000310F59";
/// Polygon chain ID
pub const CHAIN_ID: u64 = 137;
/// Current compiled order signer generation. CLOB V2 live mode must require 2.
pub const CLOB_ORDER_SIGNING_VERSION: u8 = 2;

/// EIP-712 order struct for the Polymarket CTF Exchange.
#[derive(Debug, Clone)]
pub struct Order {
    pub salt: u128,
    pub maker: [u8; 20],
    pub signer: [u8; 20],
    pub token_id: String,   // uint256 as decimal string
    pub maker_amount: u128, // pUSD amount (6 decimals) for BUY
    pub taker_amount: u128, // conditional token amount (6 decimals)
    pub side: u8,           // 0 = BUY, 1 = SELL
    pub signature_type: u8, // 0 = EOA
    pub timestamp_ms: u128,
    pub metadata: [u8; 32],
    pub builder: [u8; 32],
}

/// Signed order ready for CLOB submission.
#[derive(Debug, Clone)]
pub struct SignedOrder {
    pub order: Order,
    pub signature: String, // hex-encoded 65-byte signature (r+s+v)
}

/// Build an order from trade parameters.
/// This mirrors py_clob_client's order_builder logic.
pub fn build_order(
    signing_key: &SigningKey,
    token_id: &str,
    price: f64,
    size: f64,
    side: &str,     // "BUY" or "SELL"
    tick_size: f64, // price grid step (0.01 or 0.001)
    // Market (FOK/FAK) orders quantize amounts differently from resting
    // limit orders: the venue requires whole-cent maker amounts on market
    // buys and floors (not rounds) the price.
    market_order: bool,
    // Deposit wallet (POLY_1271 flow). When set, it becomes BOTH maker and
    // signer and the order carries signature type 3; the EOA key only
    // produces the inner ECDSA signature. None keeps the plain EOA flow.
    funder: Option<&str>,
) -> Result<Order, String> {
    if token_id.is_empty() || !token_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("token_id must be a non-empty decimal uint256".to_string());
    }
    if decimal_to_u256(token_id)?.iter().all(|byte| *byte == 0) {
        return Err("token_id must be greater than zero".to_string());
    }
    if !price.is_finite() || !(0.0..=1.0).contains(&price) {
        return Err("price must be finite and within [0, 1]".to_string());
    }
    if !size.is_finite() || size <= 0.0 {
        return Err("size must be finite and greater than zero".to_string());
    }
    if !tick_size.is_finite() || tick_size <= 0.0 || tick_size >= 1.0 {
        return Err("tick_size must be finite and within (0, 1)".to_string());
    }
    if !matches!(side, "BUY" | "SELL") {
        return Err("side must be BUY or SELL".to_string());
    }

    let (maker, signer, signature_type) = match funder {
        Some(wallet) => {
            let addr = hex_to_address(wallet)?;
            (addr, addr, 3u8)
        }
        None => {
            let addr = address_from_key(signing_key);
            (addr, addr, 0u8)
        }
    };

    // Amount quantization mirrors the reference client's ROUNDING_CONFIG:
    // price decimals follow the tick (0.01→2, 0.001→3, ...), sizes carry at
    // most 2 decimals, derived amounts at most price_dp+2 decimals. The
    // venue enforces these ("invalid amounts ... max accuracy" rejects) and
    // market BUY maker amounts must be whole cents.
    let price_dp: u32 = if tick_size >= 0.1 {
        1
    } else if tick_size >= 0.01 {
        2
    } else if tick_size >= 0.001 {
        3
    } else {
        4
    };
    let amount_dp = price_dp + 2;
    let pow = |dp: u32| 10f64.powi(dp as i32);
    let floor_dp = |x: f64, dp: u32| (x * pow(dp)).floor() / pow(dp);
    let round_dp = |x: f64, dp: u32| (x * pow(dp)).round() / pow(dp);
    // Reference dust-guard: round UP at dp+4 first so float dust just below
    // a representable value does not floor away a whole increment.
    let dust_floor_dp = |x: f64, dp: u32| floor_dp((x * pow(dp + 4)).ceil() / pow(dp + 4), dp);

    let rounded_price = if market_order {
        floor_dp(price, price_dp)
    } else {
        round_dp(price, price_dp)
    };
    let rounded_price = rounded_price.max(tick_size).min(1.0 - tick_size);

    let to_micro = |x: f64| -> Result<u128, String> {
        let units = (x * 1_000_000.0).round();
        if !units.is_finite() || units < 1.0 || units >= u128::MAX as f64 {
            return Err("order amount does not fit positive uint128 base units".to_string());
        }
        Ok(units as u128)
    };

    let (maker_amount, taker_amount) = if side == "BUY" {
        if market_order {
            // Market (FOK) BUY: maker is the USDC spend, whole cents only;
            // taker (shares) derives from maker at the rounded price.
            let maker_raw = floor_dp(price * size, 2);
            if maker_raw <= 0.0 || rounded_price <= 0.0 {
                return Err("size is not representable at CLOB precision".to_string());
            }
            let taker_raw = dust_floor_dp(maker_raw / rounded_price, amount_dp);
            (to_micro(maker_raw)?, to_micro(taker_raw)?)
        } else {
            // Limit BUY: taker is the share size (2 decimals); maker derives
            // from taker at the rounded price.
            let taker_raw = floor_dp(size, 2);
            if taker_raw <= 0.0 {
                return Err("size is not representable at CLOB precision".to_string());
            }
            let maker_raw = dust_floor_dp(taker_raw * rounded_price, amount_dp);
            (to_micro(maker_raw)?, to_micro(taker_raw)?)
        }
    } else {
        // SELL: maker is the share size (2 decimals); taker is the USDC
        // proceeds derived at the rounded price.
        let maker_raw = floor_dp(size, 2);
        if maker_raw <= 0.0 {
            return Err("size is not representable at CLOB precision".to_string());
        }
        let taker_raw = dust_floor_dp(maker_raw * rounded_price, amount_dp);
        (to_micro(maker_raw)?, to_micro(taker_raw)?)
    };

    let side_num = if side == "BUY" { 0u8 } else { 1u8 };

    // Salt: timestamp_seconds * random(0..1)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
        .as_millis();
    let salt = now.wrapping_mul(rand::random::<u64>() as u128) % (1u128 << 64);

    Ok(Order {
        salt,
        maker,
        signer,
        token_id: token_id.to_string(),
        maker_amount,
        taker_amount,
        side: side_num,
        signature_type,
        timestamp_ms: now,
        metadata: [0u8; 32],
        builder: [0u8; 32],
    })
}

/// Sign an order with EIP-712.
pub fn sign_order(order: &Order, key: &SigningKey, neg_risk: bool) -> Result<SignedOrder, String> {
    let digest = order_digest(order, neg_risk)?;
    let signature = ecdsa_sign(&digest, key)?;

    Ok(SignedOrder {
        order: order.clone(),
        signature,
    })
}

/// ERC-1271 order signature for a Polymarket deposit wallet (signature type
/// 3). Mirrors py-clob-client-v2's `_build_poly_1271_order_signature`: the
/// order struct is wrapped in Solady's `TypedDataSign` envelope bound to the
/// deposit wallet's own domain, the EOA signs that digest, and the wire
/// signature carries the envelope material the wallet needs to verify.
///
/// Caller contract: `order.maker` and `order.signer` are BOTH the deposit
/// wallet address; `key` is the owning EOA's key.
pub fn sign_order_1271(
    order: &Order,
    key: &SigningKey,
    neg_risk: bool,
) -> Result<SignedOrder, String> {
    if order.signature_type != 3 {
        return Err("sign_order_1271 requires signature type 3".to_string());
    }
    if order.maker != order.signer {
        return Err("POLY_1271 orders must have maker == signer (the deposit wallet)".to_string());
    }
    let exchange = if neg_risk {
        NEG_RISK_EXCHANGE_ADDRESS
    } else {
        EXCHANGE_ADDRESS
    };
    let app_domain_sep = eip712_domain_separator(exchange)?;
    let contents_hash = order_struct_hash(order)?;

    let mut solady_type = Vec::new();
    solady_type.extend_from_slice(SOLADY_TYPE_PREFIX);
    solady_type.extend_from_slice(ORDER_TYPE_STRING);
    let solady_type_hash = keccak256(&solady_type);

    let mut chain_id_bytes = [0u8; 32];
    chain_id_bytes[24..32].copy_from_slice(&CHAIN_ID.to_be_bytes());

    let mut encoded = Vec::with_capacity(224);
    encoded.extend_from_slice(&solady_type_hash);
    encoded.extend_from_slice(&contents_hash);
    encoded.extend_from_slice(&keccak256(b"DepositWallet"));
    encoded.extend_from_slice(&keccak256(b"1"));
    encoded.extend_from_slice(&chain_id_bytes);
    encoded.extend_from_slice(&address_padded(&order.signer));
    encoded.extend_from_slice(&[0u8; 32]); // deposit wallet domain salt
    let typed_data_sign_hash = keccak256(&encoded);

    let digest = eip712_digest(&app_domain_sep, &typed_data_sign_hash);
    let inner = ecdsa_sign(&digest, key)?;

    // No 0x prefix here: callers add it when serializing, exactly as for
    // the plain EOA path (`ecdsa_sign` also returns bare hex). Emitting it
    // here produced "0x0x…" on the wire and the venue rejected the order
    // with "invalid TypedDataSign signature: signature too short".
    let signature = format!(
        "{inner}{}{}{}{:04x}",
        hex::encode(app_domain_sep),
        hex::encode(contents_hash),
        hex::encode(ORDER_TYPE_STRING),
        ORDER_TYPE_STRING.len()
    );
    Ok(SignedOrder {
        order: order.clone(),
        signature,
    })
}

/// Deterministic CLOB V2 order identifier (the EIP-712 typed-data digest).
///
/// Computing this before the HTTP request lets the live journal retain the
/// venue lookup key across an ambiguous response or process restart.
pub fn order_hash(order: &Order, neg_risk: bool) -> Result<String, String> {
    Ok(format!("0x{}", hex::encode(order_digest(order, neg_risk)?)))
}

fn order_digest(order: &Order, neg_risk: bool) -> Result<[u8; 32], String> {
    let exchange = if neg_risk {
        NEG_RISK_EXCHANGE_ADDRESS
    } else {
        EXCHANGE_ADDRESS
    };

    let domain_sep = eip712_domain_separator(exchange)?;
    let struct_hash = order_struct_hash(order)?;
    Ok(eip712_digest(&domain_sep, &struct_hash))
}

// ── EIP-712 internals ──────────────────────────────────────────────

fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// EIP-712 domain separator for Polymarket CTF Exchange.
fn eip712_domain_separator(verifying_contract: &str) -> Result<[u8; 32], String> {
    // EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)
    let type_hash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );

    let name_hash = keccak256(b"Polymarket CTF Exchange");
    let version_hash = keccak256(b"2");

    let mut chain_id_bytes = [0u8; 32];
    chain_id_bytes[24..32].copy_from_slice(&CHAIN_ID.to_be_bytes());

    let contract_bytes = hex_to_address(verifying_contract)?;
    let mut contract_padded = [0u8; 32];
    contract_padded[12..32].copy_from_slice(&contract_bytes);

    let mut encoded = Vec::with_capacity(128);
    encoded.extend_from_slice(&type_hash);
    encoded.extend_from_slice(&name_hash);
    encoded.extend_from_slice(&version_hash);
    encoded.extend_from_slice(&chain_id_bytes);
    encoded.extend_from_slice(&contract_padded);

    Ok(keccak256(&encoded))
}

/// Canonical EIP-712 type strings. The Order string is also embedded
/// verbatim in ERC-1271 wire signatures, so it must stay byte-identical.
const ORDER_TYPE_STRING: &[u8] = b"Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";
const SOLADY_TYPE_PREFIX: &[u8] = b"TypedDataSign(Order contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)";

/// EIP-712 type hash for the Order struct.
fn order_type_hash() -> [u8; 32] {
    keccak256(ORDER_TYPE_STRING)
}

/// Hash the order struct fields per EIP-712 encoding rules.
fn order_struct_hash(order: &Order) -> Result<[u8; 32], String> {
    if order.maker == [0; 20] || order.signer == [0; 20] {
        return Err("maker and signer addresses must be non-zero".to_string());
    }
    if order.maker_amount == 0 || order.taker_amount == 0 {
        return Err("maker and taker amounts must be greater than zero".to_string());
    }
    if order.side > 1 {
        return Err("signed order side must be 0 (BUY) or 1 (SELL)".to_string());
    }
    if !matches!(order.signature_type, 0 | 3) {
        return Err("signature type must be 0 (EOA) or 3 (POLY_1271)".to_string());
    }

    let type_hash = order_type_hash();

    let mut encoded = Vec::with_capacity(384);
    encoded.extend_from_slice(&type_hash);
    encoded.extend_from_slice(&u256_bytes(order.salt));
    encoded.extend_from_slice(&address_padded(&order.maker));
    encoded.extend_from_slice(&address_padded(&order.signer));

    // tokenId is a uint256 — Polymarket IDs are typically 256-bit,
    // far exceeding u128. Parse decimal string to 32-byte big-endian.
    let token_id = decimal_to_u256(&order.token_id)?;
    if token_id.iter().all(|byte| *byte == 0) {
        return Err("token_id must be greater than zero".to_string());
    }
    encoded.extend_from_slice(&token_id);

    encoded.extend_from_slice(&u256_bytes(order.maker_amount));
    encoded.extend_from_slice(&u256_bytes(order.taker_amount));
    encoded.extend_from_slice(&u256_bytes(order.side as u128));
    encoded.extend_from_slice(&u256_bytes(order.signature_type as u128));
    encoded.extend_from_slice(&u256_bytes(order.timestamp_ms));
    encoded.extend_from_slice(&order.metadata);
    encoded.extend_from_slice(&order.builder);

    Ok(keccak256(&encoded))
}

/// Final EIP-712 digest: \x19\x01 || domainSeparator || structHash
fn eip712_digest(domain_sep: &[u8; 32], struct_hash: &[u8; 32]) -> [u8; 32] {
    let mut data = Vec::with_capacity(66);
    data.push(0x19);
    data.push(0x01);
    data.extend_from_slice(domain_sep);
    data.extend_from_slice(struct_hash);
    keccak256(&data)
}

/// ECDSA sign digest with secp256k1, return hex-encoded 65-byte (r+s+v) signature.
fn ecdsa_sign(digest: &[u8; 32], key: &SigningKey) -> Result<String, String> {
    use k256::ecdsa::signature::hazmat::PrehashSigner;
    let (sig, recid): (Signature, RecoveryId) = key
        .sign_prehash(digest)
        .map_err(|error| format!("ECDSA signing failed: {error}"))?;
    let mut sig_bytes = [0u8; 65];
    sig_bytes[..64].copy_from_slice(&sig.to_bytes());
    sig_bytes[64] = recid.to_byte() + 27; // Ethereum convention: v = recid + 27
    Ok(hex::encode(sig_bytes))
}

/// EIP-712 signature for the CLOB L1 auth handshake (reference:
/// py-clob-client-v2 signing/eip712.py). Domain is `ClobAuthDomain` v1 with
/// chainId only (no verifyingContract); the struct is
/// `ClobAuth(address address,string timestamp,uint256 nonce,string message)`
/// with the fixed attestation message. Returns the 65-byte r||s||v signature
/// hex WITHOUT the 0x prefix (callers prepend it for the header).
pub fn sign_clob_auth(
    key: &SigningKey,
    chain_id: u64,
    timestamp_s: u64,
    nonce: u64,
) -> Result<String, String> {
    let domain_typehash = keccak256(b"EIP712Domain(string name,string version,uint256 chainId)");
    let mut enc = Vec::with_capacity(32 * 4);
    enc.extend_from_slice(&domain_typehash);
    enc.extend_from_slice(&keccak256(b"ClobAuthDomain"));
    enc.extend_from_slice(&keccak256(b"1"));
    let mut cid = [0u8; 32];
    cid[24..].copy_from_slice(&chain_id.to_be_bytes());
    enc.extend_from_slice(&cid);
    let domain_sep = keccak256(&enc);

    let struct_typehash =
        keccak256(b"ClobAuth(address address,string timestamp,uint256 nonce,string message)");
    let addr = address_from_key(key);
    let mut sh = Vec::with_capacity(32 * 5);
    sh.extend_from_slice(&struct_typehash);
    let mut a32 = [0u8; 32];
    a32[12..].copy_from_slice(&addr);
    sh.extend_from_slice(&a32);
    sh.extend_from_slice(&keccak256(timestamp_s.to_string().as_bytes()));
    let mut n32 = [0u8; 32];
    n32[24..].copy_from_slice(&nonce.to_be_bytes());
    sh.extend_from_slice(&n32);
    sh.extend_from_slice(&keccak256(
        b"This message attests that I control the given wallet",
    ));
    let struct_hash = keccak256(&sh);
    let digest = eip712_digest(&domain_sep, &struct_hash);
    ecdsa_sign(&digest, key)
}

// ── Utilities ──────────────────────────────────────────────────────

/// Convert a decimal string to a 32-byte big-endian uint256.
///
/// Polymarket token IDs are 256-bit integers that exceed u128.
/// This performs base-10 long multiplication into a byte array.
fn decimal_to_u256(s: &str) -> Result<[u8; 32], String> {
    if s.is_empty() || !s.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("uint256 value must contain decimal digits only".to_string());
    }
    let mut result = [0u8; 32];
    for ch in s.bytes() {
        let digit = (ch - b'0') as u16;
        // result = result * 10 + digit (big-endian byte array math)
        let mut carry = digit;
        for byte in result.iter_mut().rev() {
            let v = (*byte as u16) * 10 + carry;
            *byte = (v & 0xFF) as u8;
            carry = v >> 8;
        }
        if carry != 0 {
            return Err("decimal value exceeds uint256".to_string());
        }
    }
    Ok(result)
}

/// Derive the Ethereum address from a signing key.
pub fn address_from_key(key: &SigningKey) -> [u8; 20] {
    let verifying = key.verifying_key();
    let pubkey_bytes = verifying.to_encoded_point(false);
    // Skip the 0x04 prefix byte, hash the 64-byte uncompressed public key
    let hash = keccak256(&pubkey_bytes.as_bytes()[1..]);
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&hash[12..32]);
    addr
}

/// Parse a hex address string (without 0x prefix) to 20 bytes.
fn hex_to_address(hex_str: &str) -> Result<[u8; 20], String> {
    let cleaned = hex_str
        .strip_prefix("0x")
        .or_else(|| hex_str.strip_prefix("0X"))
        .unwrap_or(hex_str);
    if cleaned.len() != 40 {
        return Err("Ethereum address must contain exactly 40 hex characters".to_string());
    }
    let bytes =
        hex::decode(cleaned).map_err(|error| format!("invalid Ethereum address: {error}"))?;
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&bytes);
    Ok(addr)
}

/// Pack a u128 into a big-endian 32-byte word (EVM uint256).
fn u256_bytes(val: u128) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[16..32].copy_from_slice(&val.to_be_bytes());
    buf
}

/// Left-pad a 20-byte address to 32 bytes.
fn address_padded(addr: &[u8; 20]) -> [u8; 32] {
    let mut padded = [0u8; 32];
    padded[12..32].copy_from_slice(addr);
    padded
}

/// Parse a hex private key (with or without 0x prefix) into a SigningKey.
pub fn parse_private_key(hex_key: &str) -> Option<SigningKey> {
    let cleaned = hex_key.strip_prefix("0x").unwrap_or(hex_key);
    let bytes = hex::decode(cleaned).ok()?;
    SigningKey::from_bytes(bytes.as_slice().into()).ok()
}

pub fn decode_api_secret(api_secret: &str) -> Option<Vec<u8>> {
    let trimmed = api_secret.trim();
    if trimmed.is_empty() {
        return None;
    }
    base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE, trimmed)
        .or_else(|_| {
            base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, trimmed)
        })
        .ok()
        .filter(|bytes| !bytes.is_empty())
}

pub fn api_secret_is_valid(api_secret: &str) -> bool {
    decode_api_secret(api_secret).is_some()
}

/// Build HMAC-SHA256 request authentication headers.
///
/// Returns (timestamp, signature) for the POLY-TIMESTAMP and POLY-SIGNATURE headers.
pub fn hmac_sign_request(
    api_secret: &str,
    timestamp: &str,
    method: &str,
    request_path: &str,
    body: &str,
) -> Result<String, String> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    // Decode the base64-encoded API secret
    let secret_bytes = decode_api_secret(api_secret)
        .ok_or_else(|| "API secret must be non-empty URL-safe base64".to_string())?;

    // Build the message: timestamp + method + path [+ body]
    let mut message = format!("{}{}{}", timestamp, method, request_path);
    if !body.is_empty() {
        message.push_str(body);
    }

    let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)
        .map_err(|error| format!("invalid HMAC key: {error}"))?;
    mac.update(message.as_bytes());
    let result = mac.finalize().into_bytes();

    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE,
        result,
    ))
}

#[cfg(test)]
mod tests {
    /// Cross-checked against the reference implementation
    /// (py-clob-client-v2 `_build_poly_1271_order_signature`) via an
    /// independent Python computation of the same fixture, 2026-08-26.
    #[test]
    fn poly_1271_envelope_matches_reference_vector() {
        use super::*;
        let wallet = hex_to_address("1a581Bf1995AB04Cc116E4FFDb3B385F8a1D4bDf").unwrap();
        let order = Order {
            salt: 12345,
            maker: wallet,
            signer: wallet,
            token_id: "777".to_string(),
            maker_amount: 5_000_000,
            taker_amount: 5_434_782,
            side: 0,
            signature_type: 3,
            timestamp_ms: 1_787_000_000_000,
            metadata: [0u8; 32],
            builder: [0u8; 32],
        };
        assert_eq!(
            hex::encode(order_struct_hash(&order).unwrap()),
            "7a8cabce1de326d9764015f976e8bfcd0418e03c913aeff45af4e5a24e958b20"
        );
        assert_eq!(
            hex::encode(eip712_domain_separator(EXCHANGE_ADDRESS).unwrap()),
            "3264e159346253e26a64e00b69032db0e7d32f94628de3e6eecb50304d7af3d2"
        );
        assert_eq!(ORDER_TYPE_STRING.len(), 186);

        let key = parse_private_key(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let signed = sign_order_1271(&order, &key, false).unwrap();
        let sig = signed.signature.as_str();
        assert!(!sig.starts_with("0x"), "callers add the 0x prefix");
        // inner sig (65B) + domain sep (32B) + contents hash (32B) +
        // type string (186B) + uint16 length = 317 bytes.
        assert_eq!(sig.len(), 2 * (65 + 32 + 32 + 186 + 2));
        assert!(sig[130..].starts_with(
            "3264e159346253e26a64e00b69032db0e7d32f94628de3e6eecb50304d7af3d2"
        ));
        assert!(sig.ends_with("00ba"));
    }

    use super::*;

    #[test]
    fn test_keccak256_empty() {
        let hash = keccak256(b"");
        // Well-known: keccak256("") = 0xc5d2...
        assert_eq!(
            hex::encode(hash),
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
    }

    #[test]
    fn test_u256_bytes_packing() {
        let bytes = u256_bytes(1);
        assert_eq!(bytes[31], 1);
        assert_eq!(bytes[0], 0);
    }

    #[test]
    fn validates_url_safe_api_secret() {
        assert!(api_secret_is_valid("c2VjcmV0"));
        assert!(!api_secret_is_valid("not base64!"));
        assert!(!api_secret_is_valid(""));
    }

    #[test]
    fn test_address_derivation_deterministic() {
        // Use a known test key
        let key =
            parse_private_key("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
                .unwrap();
        let addr = address_from_key(&key);
        // Hardhat account #0
        assert_eq!(
            hex::encode(addr),
            "f39fd6e51aad88f6f4ce6ab8827279cfffb92266"
        );
    }

    #[test]
    fn test_domain_separator_deterministic() {
        let ds1 = eip712_domain_separator(EXCHANGE_ADDRESS).unwrap();
        let ds2 = eip712_domain_separator(EXCHANGE_ADDRESS).unwrap();
        assert_eq!(ds1, ds2);
    }

    #[test]
    fn test_sign_order_produces_65_byte_hex() {
        let key =
            parse_private_key("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
                .unwrap();
        let order = Order {
            salt: 12345,
            maker: address_from_key(&key),
            signer: address_from_key(&key),
            token_id:
                "71321045679252212594626385532706912750332728571942532289631379312455583992563"
                    .to_string(),
            maker_amount: 5_000_000,
            taker_amount: 10_000_000,
            side: 0,
            signature_type: 0,
            timestamp_ms: 1_713_398_400_000,
            metadata: [0u8; 32],
            builder: [0u8; 32],
        };
        let signed = sign_order(&order, &key, false).unwrap();
        // 65 bytes = 130 hex chars
        assert_eq!(signed.signature.len(), 130);
        let hash = order_hash(&order, false).unwrap();
        assert_eq!(
            hash,
            "0x4151f4e426332af0e4e24ba2e3a924a7dab7c5bf94e34fce7d535f60a901d27a"
        );
    }

    #[test]
    fn test_decimal_to_u256_small() {
        let result = decimal_to_u256("256").unwrap();
        assert_eq!(result[31], 0x00);
        assert_eq!(result[30], 0x01);
        // 256 = 0x100
        let val = u16::from_be_bytes([result[30], result[31]]);
        assert_eq!(val, 256);
    }

    #[test]
    fn test_decimal_to_u256_real_polymarket_token() {
        // Real Polymarket token ID (256-bit)
        let tid = "71321045679252212594626385532706912750332728571942532289631379312455583992563";
        let bytes = decimal_to_u256(tid).unwrap();
        // Must not be all zeros (would indicate overflow/truncation)
        assert!(
            bytes.iter().any(|&b| b != 0),
            "token_id encoded as all zeros!"
        );
        // Re-encode back to decimal and verify round-trip
        let mut val = [0u8; 32];
        val.copy_from_slice(&bytes);
        // Simple verification: the last byte should be the low digit
        // 71321...3 → last digit 3, but packed as binary not BCD
        // Just ensure non-zero encoding
        assert_ne!(bytes, [0u8; 32]);
    }

    #[test]
    fn test_decimal_to_u256_matches_known_hex() {
        // 255 = 0xFF
        let bytes = decimal_to_u256("255").unwrap();
        assert_eq!(bytes[31], 0xFF);
        assert_eq!(bytes[30], 0x00);

        // 65536 = 0x10000
        let bytes = decimal_to_u256("65536").unwrap();
        assert_eq!(bytes[31], 0x00);
        assert_eq!(bytes[30], 0x00);
        assert_eq!(bytes[29], 0x01);
    }

    #[test]
    fn signing_inputs_fail_closed() {
        let key =
            parse_private_key("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
                .unwrap();
        let uint256_overflow =
            "115792089237316195423570985008687907853269984665640564039457584007913129639936";

        assert!(decimal_to_u256("12bad34").is_err());
        assert!(decimal_to_u256(uint256_overflow).is_err());
        assert!(hex_to_address("not-an-address").is_err());
        assert!(hmac_sign_request("not base64!", "1", "GET", "/", "").is_err());
        assert!(build_order(&key, "123", 0.5, 1.0, "buy", 0.01, true, None).is_err());
        assert!(build_order(&key, "0", 0.5, 1.0, "BUY", 0.01, true, None).is_err());
        assert!(build_order(&key, "123", f64::NAN, 1.0, "BUY", 0.01, true, None).is_err());
        assert!(build_order(&key, "123", 0.5, 0.001, "BUY", 0.01, true, None).is_err());
        assert!(build_order(&key, "123", 0.5, f64::MAX, "BUY", 0.01, true, None).is_err());
        assert!(build_order(&key, uint256_overflow, 0.5, 1.0, "BUY", 0.01, true, None).is_err());

        let valid = build_order(&key, "123", 0.5, 2.0, "BUY", 0.01, true, None).unwrap();
        assert_eq!(valid.side, 0);
        assert_eq!(valid.maker_amount, 1_000_000);
        assert_eq!(valid.taker_amount, 2_000_000);

        // Market BUY maker amounts must be whole cents even when
        // price*size lands off-cent (the venue's live reject 2026-08-25:
        // "maker amount supports a max accuracy of 2 decimals").
        let fok = build_order(&key, "123", 0.71, 7.04, "BUY", 0.01, true, None).unwrap();
        assert_eq!(fok.maker_amount % 10_000, 0, "maker must be whole cents");
        assert_eq!(fok.maker_amount, 4_990_000); // floor(0.71*7.04=4.9984, 2dp)
        assert_eq!(fok.taker_amount % 100, 0, "shares max 4 decimals at tick 0.01");
        assert_eq!(fok.taker_amount, 7_028_100); // floor(4.99/0.71, 4dp)

        // Limit BUY keeps 2dp shares and derives maker at amount precision.
        let gtc = build_order(&key, "123", 0.71, 7.046, "BUY", 0.01, false, None).unwrap();
        assert_eq!(gtc.taker_amount, 7_040_000); // floor(7.046, 2dp)
        assert_eq!(gtc.maker_amount, 4_998_400); // 7.04*0.71 at 4dp

        let mut invalid_order = valid;
        invalid_order.side = 2;
        assert!(sign_order(&invalid_order, &key, false).is_err());
    }
}
