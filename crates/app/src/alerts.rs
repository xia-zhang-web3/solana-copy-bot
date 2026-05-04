use anyhow::{anyhow, Context, Result};
use copybot_storage::{RiskEventRow, SqliteStore};
use reqwest::{Client, Url};
use serde_json::{json, Value};
use std::env;
use std::time::Duration;

pub const APP_ALERT_WEBHOOK_URL_ENV: &str = "COPYBOT_APP_ALERT_WEBHOOK_URL";
pub const APP_ALERT_TIMEOUT_MS_ENV: &str = "COPYBOT_APP_ALERT_TIMEOUT_MS";
pub const APP_ALERT_TEST_ON_STARTUP_ENV: &str = "COPYBOT_APP_ALERT_TEST_ON_STARTUP";
const DEFAULT_ALERT_TIMEOUT_MS: u64 = 3_000;
const ALERT_DELIVERY_CHANNEL: &str = "webhook";
const ALERT_DELIVERY_BATCH_LIMIT: u32 = 50;

#[derive(Debug, Clone)]
pub(crate) struct AlertDispatcher {
    client: Client,
    webhook_url: Url,
    test_on_startup: bool,
}

impl AlertDispatcher {
    pub(crate) fn from_env() -> Result<Option<Self>> {
        let Some(raw_webhook_url) = env::var_os(APP_ALERT_WEBHOOK_URL_ENV) else {
            return Ok(None);
        };
        let raw_webhook_url = raw_webhook_url
            .into_string()
            .map_err(|_| anyhow!("{APP_ALERT_WEBHOOK_URL_ENV} must be valid UTF-8"))?;
        let webhook_url = raw_webhook_url.trim();
        if webhook_url.is_empty() {
            return Err(anyhow!(
                "{APP_ALERT_WEBHOOK_URL_ENV} must not be empty when set"
            ));
        }
        let timeout_ms = match env::var(APP_ALERT_TIMEOUT_MS_ENV) {
            Ok(raw) => raw.trim().parse::<u64>().map_err(|error| {
                anyhow!(
                    "{APP_ALERT_TIMEOUT_MS_ENV} must be a valid u64, got {:?}: {}",
                    raw.trim(),
                    error
                )
            })?,
            Err(env::VarError::NotPresent) => DEFAULT_ALERT_TIMEOUT_MS,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(anyhow!("{APP_ALERT_TIMEOUT_MS_ENV} must be valid UTF-8"));
            }
        };
        let test_on_startup = match env::var(APP_ALERT_TEST_ON_STARTUP_ENV) {
            Ok(raw) => parse_bool(raw.trim()).ok_or_else(|| {
                anyhow!(
                    "{APP_ALERT_TEST_ON_STARTUP_ENV} must be a valid bool (1/0/true/false/yes/no/on/off), got {:?}",
                    raw.trim()
                )
            })?,
            Err(env::VarError::NotPresent) => false,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(anyhow!(
                    "{APP_ALERT_TEST_ON_STARTUP_ENV} must be valid UTF-8"
                ));
            }
        };
        let webhook_url = Url::parse(webhook_url).with_context(|| {
            format!(
                "{APP_ALERT_WEBHOOK_URL_ENV} must be a valid absolute URL, got {:?}",
                webhook_url
            )
        })?;
        match webhook_url.scheme() {
            "https" => {}
            "http" if endpoint_host_is_local(&webhook_url) => {}
            other => {
                return Err(anyhow!(
                    "{APP_ALERT_WEBHOOK_URL_ENV} must use https or loopback http, got scheme={other:?}"
                ));
            }
        }
        let client = Client::builder()
            .timeout(Duration::from_millis(timeout_ms.max(1)))
            .build()
            .context("failed building alert webhook client")?;
        Ok(Some(Self {
            client,
            webhook_url,
            test_on_startup,
        }))
    }

    pub(crate) fn test_on_startup(&self) -> bool {
        self.test_on_startup
    }

    pub(crate) async fn send_startup_test(&self, env_name: &str) -> Result<()> {
        let payload = json!({
            "kind": "alert_delivery_test",
            "env": env_name,
            "ts": chrono::Utc::now().to_rfc3339(),
        });
        self.post_payload(&payload).await
    }

    pub(crate) async fn deliver_pending(&self, store: &SqliteStore) -> Result<usize> {
        let cursor = store
            .load_alert_delivery_cursor(ALERT_DELIVERY_CHANNEL)
            .context("failed reading alert delivery cursor")?;
        let events = store
            .list_risk_events_after_cursor(cursor, ALERT_DELIVERY_BATCH_LIMIT)
            .context("failed reading pending risk events for alert delivery")?;
        let mut delivered = 0usize;
        for event in events {
            self.post_payload(&risk_event_payload(&event))
                .await
                .with_context(|| {
                    format!(
                        "failed delivering alert webhook for event_type={} event_id={}",
                        event.event_type, event.event_id
                    )
                })?;
            store
                .upsert_alert_delivery_cursor(ALERT_DELIVERY_CHANNEL, event.rowid)
                .context("failed to advance alert delivery cursor")?;
            delivered += 1;
        }
        Ok(delivered)
    }

    async fn post_payload(&self, payload: &Value) -> Result<()> {
        self.client
            .post(self.webhook_url.clone())
            .json(payload)
            .send()
            .await
            .context("alert webhook request failed")?
            .error_for_status()
            .context("alert webhook responded with error status")?;
        Ok(())
    }
}

fn risk_event_payload(event: &RiskEventRow) -> Value {
    let details = event
        .details_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .unwrap_or_else(|| match &event.details_json {
            Some(raw) => Value::String(raw.clone()),
            None => Value::Null,
        });
    json!({
        "kind": "risk_event",
        "event_id": event.event_id,
        "event_type": event.event_type,
        "severity": event.severity,
        "ts": event.ts,
        "details": details,
    })
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn endpoint_host_is_local(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host
        .trim()
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase();
    if host == "localhost" || host.ends_with(".localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    include!("alerts_tests.rs");
}
