//! Operator alerter.
//!
//! Posts a single message per alert; non-blocking on failure (we never want
//! an alerting outage to halt the bot).

use std::time::Duration;

use anyhow::Result;
use reqwest::Client;
use serde_json::json;

use crate::monitoring::telegram::{operator_keyboard, TelegramClient};

#[derive(Clone)]
pub struct Alerter {
    webhook: Option<String>,
    telegram: Option<TelegramClient>,
    http: Client,
}

impl Alerter {
    pub fn new(webhook: Option<String>) -> Self {
        Self::new_with_telegram(webhook, TelegramClient::from_env())
    }

    fn new_with_telegram(webhook: Option<String>, telegram: Option<TelegramClient>) -> Self {
        let webhook = webhook.and_then(|url| {
            let trimmed = url.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });
        let http = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("client");
        Self {
            webhook,
            telegram,
            http,
        }
    }

    pub fn from_env() -> Self {
        let webhook =
            std::env::var("SLACK_WEBHOOK_URL").or_else(|_| std::env::var("ALERT_WEBHOOK_URL"));
        Self::new(webhook.ok())
    }

    pub fn enabled(&self) -> bool {
        self.webhook.is_some() || self.telegram.is_some()
    }

    pub async fn send(&self, severity: &str, title: &str, body: &str) -> Result<()> {
        if let Some(url) = self.webhook.clone() {
            self.send_webhook(&url, severity, title, body).await?;
        }
        if let Some(telegram) = &self.telegram {
            let prefix = match severity {
                "info" => "[info]",
                "warning" => "[warning]",
                "critical" => "[critical]",
                _ => "[alert]",
            };
            let text = format!("{prefix} {title}\n{body}");
            if let Err(e) = telegram
                .send_message(&text, Some(operator_keyboard()))
                .await
            {
                tracing::warn!(error = %e, "telegram alert failed");
            }
        }
        Ok(())
    }

    async fn send_webhook(&self, url: &str, severity: &str, title: &str, body: &str) -> Result<()> {
        let icon = match severity {
            "info" => ":information_source:",
            "warning" => ":warning:",
            "critical" => ":rotating_light:",
            _ => ":speech_balloon:",
        };
        let text = format!("{icon} *{title}*\n{body}");
        let resp = self
            .http
            .post(url)
            .json(&json!({"text": text}))
            .send()
            .await;
        match resp {
            Ok(r) if r.status().is_success() => Ok(()),
            Ok(r) => {
                tracing::warn!(status = %r.status(), "alerter non-2xx");
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "alerter post failed");
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Alerter;

    #[test]
    fn disabled_without_webhook() {
        assert!(!Alerter::new_with_telegram(None, None).enabled());
        assert!(!Alerter::new_with_telegram(Some("   ".to_string()), None).enabled());
    }

    #[test]
    fn enabled_with_trimmed_webhook() {
        assert!(
            Alerter::new_with_telegram(Some(" https://example.com/hook ".to_string()), None)
                .enabled()
        );
    }
}
