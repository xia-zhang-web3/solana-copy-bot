//! Validate successful envelopes without allocating the complete block result.
use super::response;
use anyhow::{ensure, Result};
use serde::{de::IgnoredAny, Deserialize, Deserializer};

#[derive(Deserialize)]
struct Envelope {
    #[serde(default)]
    jsonrpc: Option<String>,
    #[serde(default)]
    id: Option<u64>,
    #[serde(default, deserialize_with = "present")]
    result: bool,
    #[serde(default, deserialize_with = "nonnull")]
    error: bool,
    #[serde(default, deserialize_with = "present")]
    broker_error: bool,
}
fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    IgnoredAny::deserialize(deserializer).map(|_| true)
}
fn nonnull<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    Option::<IgnoredAny>::deserialize(deserializer).map(|value| value.is_some())
}

pub(super) fn validate(status: u16, id: u64, method: &str, raw: &[u8]) -> Result<()> {
    match serde_json::from_slice::<Envelope>(raw) {
        Ok(envelope)
            if (200..300).contains(&status) && !envelope.error && !envelope.broker_error =>
        {
            ensure!(
                envelope.jsonrpc.as_deref() == Some("2.0") && envelope.id == Some(id),
                "http_recovery_response_identity http_status={status}"
            );
            // An explicit null result is valid RPC evidence; the block decoder
            // owns its existing unavailable/unconfirmed refusal.
            ensure!(envelope.result, "http_recovery_missing_result");
            Ok(())
        }
        // Preserve the accepted interpreter's exact refusal facts, including
        // duplicate/non-scalar envelope fields and broker retry classification.
        _ => response::interpret(status, id, method, raw).map(|_| ()),
    }
}
