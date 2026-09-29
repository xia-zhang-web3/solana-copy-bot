//! Closed error facts from the budget broker. Untrusted bodies are never logged.
use anyhow::{bail, ensure, Context, Result};
use serde_json::Value;

const STAGES: &[&str] = &[
    "request",
    "route",
    "gate",
    "reservation",
    "outbound",
    "tls",
    "http_response",
    "archive",
    "transport",
    "connect",
];
const CAUSES: &[&str] = &[
    "SSLCertVerificationError",
    "SSLError",
    "gaierror",
    "TimeoutError",
    "ConnectionRefusedError",
    "ConnectionResetError",
    "RemoteDisconnected",
    "IncompleteRead",
    "BadStatusLine",
    "HTTPException",
    "OSError",
    "EOFError",
    "ValueError",
    "Refused",
    "HTTPStatus",
    "PermissionError",
    "FileNotFoundError",
    "BrokenPipeError",
    "ConnectionAbortedError",
    "UnknownFailure",
    "Error",
    "KeyError",
    "TypeError",
    "OperationalError",
    "IntegrityError",
];
const REASONS: &[&str] = &[
    "http_status",
    "response_too_large",
    "invalid_json",
    "invalid_rpc_envelope",
    "rpc_error",
    "tls_certificate",
    "tls_hostname",
    "ca_binding_missing",
    "ca_binding_invalid",
    "tls_protocol",
    "dns",
    "timeout",
    "connection_refused",
    "connection_reset",
    "http_protocol",
    "io_error",
    "upstream_unavailable",
    "upstream_response_too_large",
    "stop_present",
    "lease_or_clock_invalid",
    "clock_unavailable",
    "invalid_or_unavailable",
    "transport_error",
    "unclassified_failure",
    "read_only_stop_present",
    "read_only_clock_or_lease_invalid",
    "permanent_financial_stop_required",
    "http_or_rpc_cap_exhausted",
    "read_only_recovery_method_required",
    "route_denied",
    "bad_or_unpriced_rpc",
    "bounded_confirmed_range_required",
    "confirmed_full_version1_required",
    "absolute_or_fragment_url_denied",
    "request_invalid",
    "request_too_large",
    "frame_too_large",
    "method_denied",
    "connect_denied",
    "unknown_rpc_method_or_price",
    "upstream_unbound",
    "upstream_host_denied",
    "rpc_path_unbound",
    "upstream_query_denied",
    "provider_key_path_unbound",
    "provider_key_missing",
    "provider_key_permissions",
    "provider_key_identity_changed",
    "provider_key_format",
    "test_upstream_forbidden",
    "quote_path_unbound",
    "quote_key_unbound",
    "unknown_quote_price",
    "unknown_route",
    "ledger_binding_changed",
    "ledger_lost_after_first_open",
    "ledger_marker_binding_changed",
];

fn token<'a>(v: &'a Value, allowed: &[&str], fallback: &'a str) -> &'a str {
    v.as_str()
        .filter(|s| allowed.contains(s))
        .unwrap_or(fallback)
}

fn optional_number(v: &Value, min: i64, max: i64) -> Result<String> {
    if v.is_null() {
        return Ok("unknown".to_owned());
    }
    let number = v.as_i64().context("http_recovery_invalid_broker_number")?;
    ensure!(
        (min..=max).contains(&number),
        "http_recovery_invalid_broker_number"
    );
    Ok(number.to_string())
}

fn broker_error(status: u16, method: &str, error: &Value) -> Result<Value> {
    ensure!(
        error["schema"] == "http_recovery_broker_v1",
        "http_recovery_invalid_broker_schema"
    );
    let kind = token(&error["kind"], &["failed", "refused"], "invalid");
    ensure!(kind != "invalid", "http_recovery_invalid_broker_kind");
    ensure!(
        error["method"].is_null() || error["method"].as_str() == Some(method),
        "http_recovery_broker_method_identity"
    );
    let stage = token(&error["stage"], STAGES, "unclassified");
    let reason = token(&error["reason"], REASONS, "unclassified_failure");
    let cause = token(&error["cause_type"], CAUSES, "UnknownFailure");
    let reservation = optional_number(&error["reservation_id"], 1, i64::MAX)?;
    let upstream_status = optional_number(&error["http_status"], 100, 599)?;
    let verify = optional_number(&error["verify_code"], 0, i64::from(i32::MAX))?;
    bail!("http_recovery_broker_error method={method} kind={kind} stage={stage} reason={reason} cause_type={cause} broker_http_status={status} upstream_http_status={upstream_status} reservation_id={reservation} verify_code={verify}")
}

pub(super) fn interpret(status: u16, id: u64, method: &str, raw: &[u8]) -> Result<Value> {
    let parsed = serde_json::from_slice::<Value>(raw);
    // Local broker failures are HTTP responses, not JSON-RPC success envelopes.
    if let Ok(envelope) = &parsed {
        if let Some(error) = envelope.get("broker_error") {
            return broker_error(status, method, error);
        }
    }
    if !(200..300).contains(&status) {
        if let Ok(envelope) = &parsed {
            if envelope.get("jsonrpc").is_some() || envelope.get("id").is_some() {
                ensure!(
                    envelope["jsonrpc"] == "2.0" && envelope["id"].as_u64() == Some(id),
                    "http_recovery_response_identity http_status={status}"
                );
                if let Some(code) = envelope["error"]["code"].as_i64() {
                    bail!("http_recovery_rpc_error http_status={status} code={code}");
                }
            }
        }
        bail!("http_recovery_http_status {status}");
    }
    let envelope =
        parsed.with_context(|| format!("http_recovery_invalid_json http_status={status}"))?;
    ensure!(
        envelope["jsonrpc"] == "2.0" && envelope["id"].as_u64() == Some(id),
        "http_recovery_response_identity http_status={status}"
    );
    if let Some(error) = envelope.get("error").filter(|v| !v.is_null()) {
        let code = error["code"]
            .as_i64()
            .context("http_recovery_invalid_rpc_error")?;
        // Numeric RPC cause is stable; the original provider reply belongs to broker
        // evidence, and free message/data can contain credentials or private URLs.
        bail!("http_recovery_rpc_error http_status={status} code={code}");
    }
    Ok(envelope
        .get("result")
        .context("http_recovery_missing_result")?
        .clone())
}
