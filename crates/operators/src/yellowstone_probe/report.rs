use serde::Serialize;
use std::time::Instant;
use url::Url;

use super::{REASON_FIRST_MESSAGE_RECEIVED, REASON_OK};

#[derive(Debug, Serialize)]
pub(crate) struct ProbeReport {
    pub(crate) probe_mode: String,
    pub(crate) config_loaded: bool,
    pub(crate) endpoint_host_redacted: Option<String>,
    pub(crate) connect_started: bool,
    pub(crate) connect_completed: bool,
    pub(crate) subscribe_started: bool,
    pub(crate) subscribe_completed: bool,
    pub(crate) subscribe_send_completed: bool,
    pub(crate) initial_request_sent_during_subscribe_open: bool,
    pub(crate) first_message_received: bool,
    pub(crate) elapsed_ms: u64,
    pub(crate) reason_class: String,
    pub(crate) error_redacted: Option<String>,
    pub(crate) production_green: bool,
}

impl ProbeReport {
    pub(crate) fn failed(
        reason_class: &str,
        error_redacted: Option<String>,
        elapsed_ms: u64,
    ) -> Self {
        Self {
            probe_mode: "unknown".to_string(),
            config_loaded: false,
            endpoint_host_redacted: None,
            connect_started: false,
            connect_completed: false,
            subscribe_started: false,
            subscribe_completed: false,
            subscribe_send_completed: false,
            initial_request_sent_during_subscribe_open: false,
            first_message_received: false,
            elapsed_ms,
            reason_class: reason_class.to_string(),
            error_redacted,
            production_green: false,
        }
    }

    pub(crate) fn exit_code(&self) -> i32 {
        match self.reason_class.as_str() {
            REASON_OK | REASON_FIRST_MESSAGE_RECEIVED => 0,
            _ => 1,
        }
    }
}

pub(crate) fn redacted_endpoint_host(raw_url: &str) -> String {
    match Url::parse(raw_url.trim()) {
        Ok(parsed) => {
            let port = parsed
                .port()
                .map(|port| format!(":{port}"))
                .unwrap_or_default();
            format!("{}://<redacted-host>{port}", parsed.scheme())
        }
        Err(_) => "<invalid-endpoint-redacted>".to_string(),
    }
}

pub(crate) fn redact_error(message: &str, endpoint_url: &str, token: &str) -> String {
    let mut redacted = message.to_string();
    let token = token.trim();
    if !token.is_empty() && token != "REPLACE_ME" {
        redacted = redacted.replace(token, "<redacted-token>");
    }
    if let Ok(parsed) = Url::parse(endpoint_url.trim()) {
        if let Some(host) = parsed.host_str() {
            redacted = redacted.replace(host, "<redacted-host>");
        }
        if !parsed.username().is_empty() {
            redacted = redacted.replace(parsed.username(), "<redacted-user>");
        }
        if let Some(password) = parsed.password() {
            redacted = redacted.replace(password, "<redacted-password>");
        }
        if let Some(query) = parsed.query() {
            redacted = redacted.replace(query, "<redacted-query>");
        }
    }
    redacted
}

pub(crate) fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}
