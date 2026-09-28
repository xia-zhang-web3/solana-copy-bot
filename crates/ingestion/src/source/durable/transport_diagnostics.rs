//! Sanitized transport facts. Raw Status metadata, endpoints and payloads are never logged.
use super::telemetry::{DurableIngressTelemetry, TransportClass, TransportStage};
use std::{error::Error, time::Instant};

const MESSAGE_CHARS: usize = 240;
const CAUSE_CHARS: usize = 160;
const MAX_CAUSES: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ErrorDetails {
    pub class: TransportClass,
    pub code: Option<tonic::Code>,
    pub message: String,
    pub causes: Vec<String>,
}

/// Suppress known credentials before bounded text is produced. Unknown URLs and
/// credential/header-shaped lines are discarded rather than partially exposed.
fn sanitized(value: &str, secrets: &[&str], limit: usize) -> String {
    let mut clean = value.to_owned();
    for secret in secrets.iter().filter(|v| !v.is_empty()) {
        clean = clean.replace(secret, "[redacted]");
    }
    let mut out = String::new();
    for line in clean.lines() {
        // Remove URI tokens before scanning sensitivity markers so a private URL
        // does not erase the useful protocol error text preceding it.
        let line = line
            .split_whitespace()
            .map(|word| {
                if word.contains("://") || word.len() > 64 {
                    "[redacted]"
                } else {
                    word
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        let lower = line.to_ascii_lowercase();
        let sensitive = [
            "authorization",
            "x-token",
            "x_token",
            "bearer",
            "api-key",
            "api_key",
            "headers",
            "metadata",
            "cookie",
            "password",
            "secret",
            "token=",
            "token:",
        ]
        .iter()
        .any(|marker| lower.contains(marker));
        if sensitive {
            out.push_str("[redacted sensitive detail] ");
        } else {
            for word in line.split_whitespace() {
                if word.contains("://") || word.len() > 64 {
                    out.push_str("[redacted] ");
                } else {
                    out.extend(word.chars().filter(|c| !c.is_control()));
                    out.push(' ');
                }
            }
        }
        if out.chars().count() >= limit {
            break;
        }
    }
    out.chars()
        .take(limit)
        .collect::<String>()
        .trim()
        .to_owned()
}

impl ErrorDetails {
    pub(crate) fn status(status: &tonic::Status, secrets: &[&str]) -> Self {
        Self {
            class: TransportClass::status(status),
            code: Some(status.code()),
            message: sanitized(status.message(), secrets, MESSAGE_CHARS),
            causes: causes(status.source(), secrets),
        }
    }
    pub(crate) fn error(error: &anyhow::Error, secrets: &[&str]) -> Self {
        if let Some(status) = error
            .chain()
            .find_map(|v| v.downcast_ref::<tonic::Status>())
        {
            return Self::status(status, secrets);
        }
        Self {
            class: TransportClass::error(error),
            code: None,
            message: sanitized(&error.to_string(), secrets, MESSAGE_CHARS),
            causes: causes(error.source(), secrets),
        }
    }
    pub(crate) fn subscribe(
        error: &yellowstone_grpc_client::GeyserGrpcClientError,
        secrets: &[&str],
    ) -> Self {
        match error {
            yellowstone_grpc_client::GeyserGrpcClientError::TonicStatus(status) => {
                Self::status(status, secrets)
            }
            _ => Self {
                class: TransportClass::subscribe(error),
                code: None,
                message: sanitized(&error.to_string(), secrets, MESSAGE_CHARS),
                causes: causes(error.source(), secrets),
            },
        }
    }
    pub(crate) fn eof() -> Self {
        Self {
            class: TransportClass::End,
            code: None,
            message: "stream EOF".into(),
            causes: vec![],
        }
    }
}
fn causes(mut cause: Option<&(dyn Error + 'static)>, secrets: &[&str]) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(error) = cause {
        if out.len() == MAX_CAUSES {
            break;
        }
        out.push(sanitized(&error.to_string(), secrets, CAUSE_CHARS));
        cause = error.source();
    }
    out
}

pub(crate) fn report(
    telemetry: &DurableIngressTelemetry,
    stage: TransportStage,
    started: Instant,
    details: ErrorDetails,
) {
    let s = telemetry.snapshot();
    telemetry.reconnect(stage, details.class);
    tracing::warn!(
        ?stage, class = ?details.class, grpc_code = ?details.code,
        error_message = %details.message, causes = ?details.causes,
        connection_age_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        last_received_transaction_slot = s.last_transaction_slot,
        last_received_block_slot = s.last_received_block_slot,
        last_emitted_parent_slot = s.last_parent_slot,
        last_durably_stored_parent_slot = s.last_durably_stored_parent_slot,
        "durable ingress transport boundary"
    );
}
