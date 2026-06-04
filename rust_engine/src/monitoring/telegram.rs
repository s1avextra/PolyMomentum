//! Minimal Telegram Bot API client for operator monitoring.

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde_json::{json, Value};

#[derive(Clone)]
pub struct TelegramClient {
    token: String,
    chat_id: String,
    allowed_chat_ids: Vec<i64>,
    api_base: String,
    http: Client,
}

impl TelegramClient {
    pub fn from_env() -> Option<Self> {
        let token = std::env::var("TELEGRAM_BOT_TOKEN")
            .or_else(|_| std::env::var("POLYMOMENTUM_TELEGRAM_BOT_TOKEN"))
            .ok()?;
        let chat_id = std::env::var("TELEGRAM_CHAT_ID")
            .or_else(|_| std::env::var("POLYMOMENTUM_TELEGRAM_CHAT_ID"))
            .ok()?;
        Self::new(
            token,
            chat_id,
            std::env::var("TELEGRAM_ALLOWED_CHAT_IDS").ok(),
            std::env::var("TELEGRAM_API_BASE").ok(),
        )
        .ok()
    }

    pub fn new(
        token: String,
        chat_id: String,
        allowed_chat_ids: Option<String>,
        api_base: Option<String>,
    ) -> Result<Self> {
        let token = token.trim().to_string();
        let chat_id = chat_id.trim().to_string();
        if token.is_empty() || chat_id.is_empty() {
            bail!("Telegram token and chat id must both be set");
        }
        let mut allowed = parse_chat_ids(allowed_chat_ids.as_deref());
        if let Ok(id) = chat_id.parse::<i64>() {
            allowed.push(id);
        }
        allowed.sort_unstable();
        allowed.dedup();
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("build Telegram HTTP client")?;
        Ok(Self {
            token,
            chat_id,
            allowed_chat_ids: allowed,
            api_base: api_base.unwrap_or_else(|| "https://api.telegram.org".to_string()),
            http,
        })
    }

    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    pub fn is_allowed_chat(&self, chat_id: i64) -> bool {
        self.allowed_chat_ids.binary_search(&chat_id).is_ok()
    }

    pub async fn get_me(&self) -> Result<Value> {
        self.post_json("getMe", json!({})).await
    }

    pub async fn set_operator_commands(&self) -> Result<()> {
        self.post_json(
            "setMyCommands",
            json!({
                "commands": [
                    {"command": "status", "description": "Current paper/live health"},
                    {"command": "stale", "description": "Strategy freshness verdict"},
                    {"command": "preflight", "description": "Read-only paper preflight"},
                    {"command": "wallet", "description": "Wallet live-readiness snapshot"},
                    {"command": "help", "description": "Show safe commands"}
                ]
            }),
        )
        .await?;
        Ok(())
    }

    pub async fn send_message(&self, text: &str, reply_markup: Option<Value>) -> Result<()> {
        let mut payload = json!({
            "chat_id": self.chat_id,
            "text": truncate(text, 3900),
            "disable_web_page_preview": true,
        });
        if let Some(markup) = reply_markup {
            payload["reply_markup"] = markup;
        }
        self.post_json("sendMessage", payload).await?;
        Ok(())
    }

    pub async fn edit_message_text(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        reply_markup: Option<Value>,
    ) -> Result<()> {
        let mut payload = json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "text": truncate(text, 3900),
            "disable_web_page_preview": true,
        });
        if let Some(markup) = reply_markup {
            payload["reply_markup"] = markup;
        }
        self.post_json("editMessageText", payload).await?;
        Ok(())
    }

    pub async fn answer_callback_query(&self, callback_query_id: &str, text: &str) -> Result<()> {
        self.post_json(
            "answerCallbackQuery",
            json!({
                "callback_query_id": callback_query_id,
                "text": truncate(text, 200),
                "show_alert": false,
            }),
        )
        .await?;
        Ok(())
    }

    pub async fn get_updates(&self, offset: Option<i64>, timeout_s: u64) -> Result<Vec<Value>> {
        let mut payload = json!({
            "timeout": timeout_s.min(50),
            "allowed_updates": ["message", "callback_query"],
        });
        if let Some(offset) = offset {
            payload["offset"] = json!(offset);
        }
        let result = self.post_json("getUpdates", payload).await?;
        Ok(result.as_array().cloned().unwrap_or_default())
    }

    async fn post_json(&self, method: &str, payload: Value) -> Result<Value> {
        let url = format!(
            "{}/bot{}/{}",
            self.api_base.trim_end_matches('/'),
            self.token,
            method
        );
        let resp = self
            .http
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|err| {
                anyhow!(
                    "Telegram {method} request failed: {}",
                    redact_secret(&err.to_string(), &self.token)
                )
            })?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "Telegram {method} returned HTTP {status}: {}",
                redact_secret(&body, &self.token)
            );
        }
        let parsed: Value = serde_json::from_str(&body)
            .with_context(|| format!("parse Telegram {method} response"))?;
        if !parsed.get("ok").and_then(|x| x.as_bool()).unwrap_or(false) {
            return Err(anyhow!(
                "Telegram {method} returned not-ok: {}",
                redact_secret(&parsed.to_string(), &self.token)
            ));
        }
        Ok(parsed.get("result").cloned().unwrap_or(Value::Null))
    }
}

pub fn operator_keyboard() -> Value {
    json!({
        "inline_keyboard": [
            [
                {"text": "Status", "callback_data": "pm:status"},
                {"text": "Freshness", "callback_data": "pm:stale"}
            ],
            [
                {"text": "Preflight", "callback_data": "pm:preflight"},
                {"text": "Wallet", "callback_data": "pm:wallet"}
            ]
        ]
    })
}

pub fn help_text() -> &'static str {
    "PolyMomentum monitor commands\n/status - current service, replay, wallet, peers\n/stale - deployed strategy freshness verdict\n/preflight - read-only paper preflight\n/wallet - wallet readiness snapshot\n\nAll Telegram actions are read-only; live mode and orders cannot be changed from chat."
}

fn parse_chat_ids(raw: Option<&str>) -> Vec<i64> {
    raw.unwrap_or("")
        .split(',')
        .filter_map(|part| part.trim().parse::<i64>().ok())
        .collect()
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max.saturating_sub(16)).collect();
    out.push_str("\n[truncated]");
    out
}

fn redact_secret(text: &str, secret: &str) -> String {
    let secret = secret.trim();
    if secret.is_empty() || !text.contains(secret) {
        return text.to_string();
    }
    text.replace(secret, "<redacted>")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_allowed_chat_ids() {
        assert_eq!(parse_chat_ids(Some("1, 2, nope, -3")), vec![1, 2, -3]);
    }

    #[test]
    fn operator_keyboard_uses_short_callback_data() {
        let keyboard = operator_keyboard();
        let rows = keyboard
            .get("inline_keyboard")
            .and_then(|v| v.as_array())
            .unwrap();
        for row in rows {
            for button in row.as_array().unwrap() {
                let data = button.get("callback_data").unwrap().as_str().unwrap();
                assert!(data.len() <= 64);
            }
        }
    }

    #[test]
    fn redacts_bot_token_from_error_text() {
        let token = "123:secret-token";
        let text = "request failed for https://api.telegram.org/bot123:secret-token/getUpdates";
        let redacted = redact_secret(text, token);
        assert!(!redacted.contains(token));
        assert!(redacted.contains("bot<redacted>/getUpdates"));
    }
}
