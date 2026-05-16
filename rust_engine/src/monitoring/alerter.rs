//! Slack webhook alerter.
//!
//! Posts a single message per alert; non-blocking on failure (we never want
//! a webhook outage to halt the bot).

use std::time::Duration;

use anyhow::Result;
use reqwest::Client;
use serde_json::json;

#[derive(Clone)]
pub struct Alerter {
    webhook: Option<String>,
    http: Client,
}

impl Alerter {
    pub fn new(webhook: Option<String>) -> Self {
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
        Self { webhook, http }
    }

    pub fn from_env() -> Self {
        let webhook =
            std::env::var("SLACK_WEBHOOK_URL").or_else(|_| std::env::var("ALERT_WEBHOOK_URL"));
        Self::new(webhook.ok())
    }

    pub fn enabled(&self) -> bool {
        self.webhook.is_some()
    }

    pub async fn send(&self, severity: &str, title: &str, body: &str) -> Result<()> {
        let Some(url) = self.webhook.clone() else {
            return Ok(());
        };
        let icon = match severity {
            "info" => ":information_source:",
            "warning" => ":warning:",
            "critical" => ":rotating_light:",
            _ => ":speech_balloon:",
        };
        let text = format!("{icon} *{title}*\n{body}");
        let resp = self
            .http
            .post(&url)
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
        assert!(!Alerter::new(None).enabled());
        assert!(!Alerter::new(Some("   ".to_string())).enabled());
    }

    #[test]
    fn enabled_with_trimmed_webhook() {
        assert!(Alerter::new(Some(" https://example.com/hook ".to_string())).enabled());
    }
}
