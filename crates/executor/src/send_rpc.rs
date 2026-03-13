use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use bincode::Options;
use serde_json::{json, Value};
use solana_transaction::versioned::VersionedTransaction;
use tracing::warn;

use crate::{
    http_utils::{
        classify_request_error, read_response_body_limited, redacted_endpoint_label,
        truncate_detail_chars, MAX_HTTP_ERROR_BODY_DETAIL_CHARS, MAX_HTTP_ERROR_BODY_READ_BYTES,
        MAX_HTTP_JSON_BODY_READ_BYTES,
    },
    key_validation::validate_signature_like,
    route_backend::SendRpcEndpointChainError,
    submit_deadline::SubmitDeadline,
    AppState, Reject,
};

fn validate_send_rpc_deadline_context(
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<(), Reject> {
    if submit_deadline.is_none() {
        return Err(Reject::terminal(
            "invalid_request_body",
            "submit send RPC missing deadline at send-rpc boundary",
        ));
    }
    Ok(())
}

pub(crate) async fn send_signed_transaction_via_rpc(
    state: &AppState,
    route: &str,
    signed_tx_base64: &str,
    submit_deadline: Option<&SubmitDeadline>,
) -> std::result::Result<String, Reject> {
    validate_send_rpc_deadline_context(submit_deadline)?;
    let backend = state.config.route_backends.get(route).ok_or_else(|| {
        Reject::terminal(
            "route_backend_not_configured",
            format!("route={} not configured", route),
        )
    })?;

    let signed_tx_base64 = signed_tx_base64.trim();
    if signed_tx_base64.is_empty() {
        return Err(Reject::retryable(
            "submit_adapter_invalid_response",
            "signed_tx_base64 must be non-empty when present",
        ));
    }
    let signed_tx_bytes = BASE64_STANDARD.decode(signed_tx_base64).map_err(|error| {
        Reject::retryable(
            "submit_adapter_invalid_response",
            format!("signed_tx_base64 is not valid base64: {}", error),
        )
    })?;
    let expected_signature = extract_expected_signature_from_signed_tx_bytes(
        signed_tx_bytes.as_slice(),
        state.config.signer_pubkey.as_str(),
    )
    .map_err(|error| {
        Reject::retryable(
            "submit_adapter_invalid_response",
            format!(
                "signed_tx_base64 does not contain valid transaction signature bytes: {}",
                error
            ),
        )
    })?;

    // Defense-in-depth: keep this invariant local to send path even though
    // config parsing and endpoint_chain_checked also enforce it.
    if backend.send_rpc_url.is_none() && backend.send_rpc_fallback_url.is_some() {
        return Err(reject_send_rpc_fallback_without_primary(route));
    }

    let endpoints = backend
        .send_rpc_endpoint_chain_checked()
        .map_err(|error| match error {
            SendRpcEndpointChainError::FallbackWithoutPrimary => {
                reject_send_rpc_fallback_without_primary(route)
            }
        })?;
    if endpoints.is_empty() {
        return Err(Reject::terminal(
            "adapter_send_rpc_not_configured",
            format!(
                "route={} missing send RPC URL (set COPYBOT_EXECUTOR_ROUTE_{}_SEND_RPC_URL or COPYBOT_EXECUTOR_SEND_RPC_URL)",
                route,
                route.to_ascii_uppercase()
            ),
        ));
    }

    let mut last_retryable: Option<Reject> = None;
    for (attempt_idx, (url, auth_token)) in endpoints.iter().enumerate() {
        let endpoint_label = redacted_endpoint_label(url);
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                signed_tx_base64,
                {
                    "encoding": "base64",
                    "skipPreflight": false,
                    "maxRetries": 0
                }
            ]
        });
        let mut request = state.http.post(*url).json(&payload);
        if let Some(deadline) = submit_deadline {
            let remaining = deadline.remaining_timeout("send_rpc")?;
            request = request.timeout(remaining);
        }
        if let Some(token) = *auth_token {
            request = request.bearer_auth(token);
        }
        let response = match request.send().await {
            Ok(value) => value,
            Err(error) => {
                let reject = Reject::retryable(
                    "send_rpc_unavailable",
                    format!(
                        "send RPC request failed endpoint={} class={}",
                        endpoint_label,
                        classify_request_error(&error)
                    ),
                );
                if attempt_idx + 1 < endpoints.len() {
                    warn!(
                        route = %route,
                        endpoint = %endpoint_label,
                        attempt = attempt_idx + 1,
                        total = endpoints.len(),
                        "retryable send RPC failure, trying fallback endpoint"
                    );
                    last_retryable = Some(reject);
                    continue;
                }
                return Err(reject);
            }
        };
        let status = response.status();
        if !status.is_success() {
            let body = read_response_body_limited(response, MAX_HTTP_ERROR_BODY_READ_BYTES).await;
            let body_detail =
                truncate_detail_chars(body.text.as_str(), MAX_HTTP_ERROR_BODY_DETAIL_CHARS);
            let reject = if status.as_u16() == 429 || status.is_server_error() {
                Reject::retryable(
                    "send_rpc_http_unavailable",
                    format!(
                        "send RPC status={} endpoint={} body={}",
                        status, endpoint_label, body_detail
                    ),
                )
            } else {
                Reject::terminal(
                    "send_rpc_http_rejected",
                    format!(
                        "send RPC status={} endpoint={} body={}",
                        status, endpoint_label, body_detail
                    ),
                )
            };
            if reject.retryable && attempt_idx + 1 < endpoints.len() {
                warn!(
                    route = %route,
                    endpoint = %endpoint_label,
                    status = %status,
                    attempt = attempt_idx + 1,
                    total = endpoints.len(),
                    "retryable send RPC HTTP status, trying fallback endpoint"
                );
                last_retryable = Some(reject);
                continue;
            }
            return Err(reject);
        }
        if let Some(content_length) = response.content_length() {
            if content_length > MAX_HTTP_JSON_BODY_READ_BYTES as u64 {
                let reject = Reject::retryable(
                    "send_rpc_response_too_large",
                    format!(
                        "send RPC response declared content-length={} exceeds max_bytes={} endpoint={}",
                        content_length,
                        MAX_HTTP_JSON_BODY_READ_BYTES,
                        endpoint_label
                    ),
                );
                if attempt_idx + 1 < endpoints.len() {
                    warn!(
                        route = %route,
                        endpoint = %endpoint_label,
                        attempt = attempt_idx + 1,
                        total = endpoints.len(),
                        "retryable send RPC declared-oversized response, trying fallback endpoint"
                    );
                    last_retryable = Some(reject);
                    continue;
                }
                return Err(reject);
            }
        }
        let body_read = read_response_body_limited(response, MAX_HTTP_JSON_BODY_READ_BYTES).await;
        if let Some(read_error_class) = body_read.read_error_class {
            let reject = Reject::retryable(
                "send_rpc_unavailable",
                format!(
                    "send RPC response read failed endpoint={} class={}",
                    endpoint_label, read_error_class
                ),
            );
            if attempt_idx + 1 < endpoints.len() {
                warn!(
                    route = %route,
                    endpoint = %endpoint_label,
                    attempt = attempt_idx + 1,
                    total = endpoints.len(),
                    "retryable send RPC response-read failure, trying fallback endpoint"
                );
                last_retryable = Some(reject);
                continue;
            }
            return Err(reject);
        }
        if body_read.was_truncated {
            let reject = Reject::retryable(
                "send_rpc_response_too_large",
                format!(
                    "send RPC response exceeded max bytes endpoint={} max_bytes={}",
                    endpoint_label, MAX_HTTP_JSON_BODY_READ_BYTES
                ),
            );
            if attempt_idx + 1 < endpoints.len() {
                warn!(
                    route = %route,
                    endpoint = %endpoint_label,
                    attempt = attempt_idx + 1,
                    total = endpoints.len(),
                    "retryable send RPC truncated response body, trying fallback endpoint"
                );
                last_retryable = Some(reject);
                continue;
            }
            return Err(reject);
        }
        let body: Value = match serde_json::from_slice(body_read.bytes.as_slice()) {
            Ok(body) => body,
            Err(error) => {
                return Err(Reject::terminal(
                    "send_rpc_invalid_json",
                    format!(
                        "send RPC response invalid JSON endpoint={} err={}",
                        endpoint_label, error
                    ),
                ));
            }
        };
        if let Some(error_payload) = body.get("error") {
            if !error_payload.is_null() {
                let payload_detail = truncate_detail_chars(
                    error_payload.to_string().as_str(),
                    MAX_HTTP_ERROR_BODY_DETAIL_CHARS,
                );
                match classify_send_rpc_error_payload(error_payload) {
                    SendRpcErrorPayloadDisposition::AlreadyProcessed => {
                        warn!(
                            route = %route,
                            endpoint = %endpoint_label,
                            signature = %expected_signature,
                            "send RPC returned already-processed error payload; accepting expected signature"
                        );
                        return Ok(expected_signature.clone());
                    }
                    SendRpcErrorPayloadDisposition::Retryable => {
                        let reject = Reject::retryable(
                            "send_rpc_error_payload_retryable",
                            format!(
                                "send RPC returned retryable error endpoint={} payload={}",
                                endpoint_label, payload_detail
                            ),
                        );
                        if attempt_idx + 1 < endpoints.len() {
                            warn!(
                                route = %route,
                                endpoint = %endpoint_label,
                                attempt = attempt_idx + 1,
                                total = endpoints.len(),
                                "send RPC retryable error payload, trying fallback endpoint"
                            );
                            last_retryable = Some(reject);
                            continue;
                        }
                        return Err(reject);
                    }
                    SendRpcErrorPayloadDisposition::BlockhashExpired => {
                        return Err(Reject::terminal(
                            "executor_blockhash_expired",
                            format!(
                                "send RPC returned blockhash-expired error endpoint={} payload={}",
                                endpoint_label, payload_detail
                            ),
                        ));
                    }
                    SendRpcErrorPayloadDisposition::Terminal => {
                        return Err(Reject::terminal(
                            "send_rpc_error_payload_terminal",
                            format!(
                                "send RPC returned terminal error endpoint={} payload={}",
                                endpoint_label, payload_detail
                            ),
                        ));
                    }
                }
            }
        }
        let signature = body
            .get("result")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Reject::terminal(
                    "send_rpc_invalid_response",
                    format!(
                        "send RPC response missing result signature endpoint={}",
                        endpoint_label
                    ),
                )
            })?;
        validate_signature_like(signature).map_err(|error| {
            Reject::terminal(
                "send_rpc_invalid_response",
                format!("send RPC result signature is invalid: {}", error),
            )
        })?;
        if signature != expected_signature {
            return Err(Reject::terminal(
                "send_rpc_signature_mismatch",
                format!(
                    "send RPC returned signature mismatch endpoint={} expected={} actual={}",
                    endpoint_label, expected_signature, signature
                ),
            ));
        }
        return Ok(signature.to_string());
    }

    Err(last_retryable.unwrap_or_else(|| {
        Reject::retryable(
            "send_rpc_unavailable",
            format!(
                "send RPC failed for all configured endpoints route={}",
                route
            ),
        )
    }))
}

fn reject_send_rpc_fallback_without_primary(route: &str) -> Reject {
    Reject::terminal(
        "adapter_send_rpc_not_configured",
        format!(
            "route={} has send RPC fallback URL but missing primary send RPC URL (set COPYBOT_EXECUTOR_ROUTE_{}_SEND_RPC_URL or COPYBOT_EXECUTOR_SEND_RPC_URL)",
            route,
            route.to_ascii_uppercase()
        ),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SendRpcErrorPayloadDisposition {
    AlreadyProcessed,
    BlockhashExpired,
    Retryable,
    Terminal,
}

fn extract_send_rpc_error_text(error_payload: &Value) -> String {
    let mut fragments: Vec<&str> = Vec::new();

    if let Some(message) = error_payload.get("message").and_then(Value::as_str) {
        let trimmed = message.trim();
        if !trimmed.is_empty() {
            fragments.push(trimmed);
        }
    }

    if let Some(data) = error_payload.get("data") {
        if let Some(data_text) = data.as_str() {
            let trimmed = data_text.trim();
            if !trimmed.is_empty() {
                fragments.push(trimmed);
            }
        }

        for field_name in ["message", "details", "err"] {
            if let Some(value) = data.get(field_name).and_then(Value::as_str) {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    fragments.push(trimmed);
                }
            }
        }
    }

    fragments.join(" ").to_ascii_lowercase()
}

fn classify_send_rpc_error_payload(error_payload: &Value) -> SendRpcErrorPayloadDisposition {
    let code = error_payload.get("code").and_then(Value::as_i64);
    let payload_lower = extract_send_rpc_error_text(error_payload);

    if code == Some(-32002)
        && (payload_lower.contains("already processed")
            || payload_lower.contains("already confirmed")
            || payload_lower.contains("already finalized"))
    {
        return SendRpcErrorPayloadDisposition::AlreadyProcessed;
    }

    if payload_lower.contains("blockhash not found")
        || payload_lower.contains("block height exceeded")
        || payload_lower.contains("transaction is too old")
    {
        return SendRpcErrorPayloadDisposition::BlockhashExpired;
    }

    if code == Some(-32005)
        || payload_lower.contains("node is unhealthy")
        || payload_lower.contains("temporarily unavailable")
        || payload_lower.contains("rate limit")
        || payload_lower.contains("try again")
        || payload_lower.contains("timed out")
        || payload_lower.contains("timeout")
    {
        return SendRpcErrorPayloadDisposition::Retryable;
    }

    SendRpcErrorPayloadDisposition::Terminal
}

fn extract_expected_signature_from_signed_tx_bytes(
    bytes: &[u8],
    executor_signer_pubkey: &str,
) -> Result<String> {
    let transaction: VersionedTransaction = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(bytes)
        .context("transaction bincode decode failed")?;
    transaction
        .sanitize()
        .map_err(|error| anyhow!("transaction failed sanitize checks: {}", error))?;

    let required_signatures = usize::from(transaction.message.header().num_required_signatures);
    let expected_signature = transaction
        .signatures
        .first()
        .ok_or_else(|| anyhow!("transaction contains zero signatures"))?
        .to_string();

    let executor_signer_pubkey = decode_executor_signer_pubkey_bytes(executor_signer_pubkey)?;
    let signer_required = transaction
        .message
        .static_account_keys()
        .iter()
        .take(required_signatures)
        .any(|pubkey| pubkey.as_ref() == executor_signer_pubkey.as_slice());
    if !signer_required {
        return Err(anyhow!(
            "transaction does not require configured executor signer pubkey"
        ));
    }

    if !transaction
        .verify_with_results()
        .into_iter()
        .all(|verified| verified)
    {
        return Err(anyhow!(
            "transaction contains invalid required signature bytes"
        ));
    }

    Ok(expected_signature)
}

fn decode_executor_signer_pubkey_bytes(signer_pubkey: &str) -> Result<[u8; 32]> {
    let decoded = bs58::decode(signer_pubkey.trim())
        .into_vec()
        .context("configured executor signer pubkey must be valid base58")?;
    let signer_bytes: [u8; 32] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("configured executor signer pubkey must decode to 32 bytes"))?;
    Ok(signer_bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        classify_send_rpc_error_payload, decode_executor_signer_pubkey_bytes,
        extract_expected_signature_from_signed_tx_bytes, extract_send_rpc_error_text,
        validate_send_rpc_deadline_context, SendRpcErrorPayloadDisposition,
    };
    use crate::submit_deadline::SubmitDeadline;
    use serde_json::json;
    use solana_keypair::{Keypair, Signature, Signer};
    use solana_message::{
        compiled_instruction::CompiledInstruction, legacy::Message as LegacyMessage, Address, Hash,
        MessageHeader, VersionedMessage,
    };
    use solana_transaction::versioned::VersionedTransaction;

    const TEST_EXECUTOR_SIGNER_SECRET: [u8; 32] = [11u8; 32];

    fn test_signed_tx_bytes_with_signature_and_signer(
        signature_seed: [u8; 64],
        signer_secret: [u8; 32],
    ) -> (Vec<u8>, String) {
        let signer = Keypair::new_from_array(signer_secret);
        let message = LegacyMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![signer.pubkey(), Address::from([9u8; 32])],
            recent_blockhash: Hash::new_from_array(
                signature_seed[32..64]
                    .try_into()
                    .expect("recent blockhash seed slice"),
            ),
            instructions: vec![CompiledInstruction::new_from_raw_parts(
                1,
                signature_seed[0..8].to_vec(),
                vec![0],
            )],
        };
        let versioned_message = VersionedMessage::Legacy(message);
        let signature: Signature = signer.sign_message(&versioned_message.serialize());
        let tx_bytes = bincode::serialize(&VersionedTransaction {
            signatures: vec![signature],
            message: versioned_message,
        })
        .expect("serialize test transaction");
        (tx_bytes, signature.to_string())
    }

    #[test]
    fn send_rpc_deadline_context_rejects_missing_deadline() {
        let reject = validate_send_rpc_deadline_context(None)
            .expect_err("send RPC without deadline must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[test]
    fn send_rpc_deadline_context_accepts_present_deadline() {
        let submit_deadline = SubmitDeadline::new(1_000);
        validate_send_rpc_deadline_context(Some(&submit_deadline))
            .expect("send RPC with deadline should pass");
    }

    #[test]
    fn classify_send_rpc_error_payload_treats_generic_recent_blockhash_text_as_terminal() {
        let payload = json!({
            "code": -32002,
            "message": "recent blockhash cache warming up"
        });
        assert_eq!(
            classify_send_rpc_error_payload(&payload),
            SendRpcErrorPayloadDisposition::Terminal
        );
    }

    #[test]
    fn classify_send_rpc_error_payload_keeps_blockhash_not_found_as_expired() {
        let payload = json!({
            "code": -32002,
            "message": "Blockhash not found"
        });
        assert_eq!(
            classify_send_rpc_error_payload(&payload),
            SendRpcErrorPayloadDisposition::BlockhashExpired
        );
    }

    #[test]
    fn classify_send_rpc_error_payload_ignores_timeout_marker_in_unstructured_data() {
        let payload = json!({
            "code": -32002,
            "message": "mystery failure class",
            "data": {
                "raw_payload": "this string says timeout but is not classifier text"
            }
        });
        assert_eq!(
            classify_send_rpc_error_payload(&payload),
            SendRpcErrorPayloadDisposition::Terminal
        );
    }

    #[test]
    fn classify_send_rpc_error_payload_uses_structured_data_message_for_retryable() {
        let payload = json!({
            "code": -32002,
            "message": "mystery failure class",
            "data": {
                "message": "node is unhealthy"
            }
        });
        assert_eq!(
            classify_send_rpc_error_payload(&payload),
            SendRpcErrorPayloadDisposition::Retryable
        );
    }

    #[test]
    fn classify_send_rpc_error_payload_uses_string_data_for_retryable() {
        let payload = json!({
            "code": -32002,
            "message": "mystery failure class",
            "data": "temporarily unavailable"
        });
        assert_eq!(
            classify_send_rpc_error_payload(&payload),
            SendRpcErrorPayloadDisposition::Retryable
        );
    }

    #[test]
    fn extract_send_rpc_error_text_reads_message_and_structured_data_fields() {
        let payload = json!({
            "message": " primary ",
            "data": {
                "message": " secondary ",
                "details": " tertiary ",
                "err": " quaternary ",
                "raw_payload": "timeout"
            }
        });
        assert_eq!(
            extract_send_rpc_error_text(&payload),
            "primary secondary tertiary quaternary"
        );
    }

    #[test]
    fn extract_send_rpc_error_text_reads_string_data_field() {
        let payload = json!({
            "message": "primary",
            "data": " secondary "
        });
        assert_eq!(extract_send_rpc_error_text(&payload), "primary secondary");
    }

    #[test]
    fn extract_expected_signature_from_signed_tx_bytes_reads_canonical_versioned_transaction() {
        let (bytes, expected_signature) =
            test_signed_tx_bytes_with_signature_and_signer([5u8; 64], TEST_EXECUTOR_SIGNER_SECRET);
        let signature = extract_expected_signature_from_signed_tx_bytes(
            bytes.as_slice(),
            Keypair::new_from_array(TEST_EXECUTOR_SIGNER_SECRET)
                .pubkey()
                .to_string()
                .as_str(),
        )
        .expect("canonical transaction should decode");
        assert_eq!(signature, expected_signature);
    }

    #[test]
    fn extract_expected_signature_from_signed_tx_bytes_rejects_when_executor_signer_missing() {
        let (bytes, _expected_signature) =
            test_signed_tx_bytes_with_signature_and_signer([6u8; 64], [8u8; 32]);
        let error = extract_expected_signature_from_signed_tx_bytes(
            bytes.as_slice(),
            Keypair::new_from_array(TEST_EXECUTOR_SIGNER_SECRET)
                .pubkey()
                .to_string()
                .as_str(),
        )
        .expect_err("mismatched signer should reject");
        assert!(
            error
                .to_string()
                .contains("does not require configured executor signer"),
            "error={}",
            error
        );
    }

    #[test]
    fn extract_expected_signature_from_signed_tx_bytes_rejects_trailing_garbage() {
        let (mut bytes, _expected_signature) =
            test_signed_tx_bytes_with_signature_and_signer([7u8; 64], TEST_EXECUTOR_SIGNER_SECRET);
        bytes.extend_from_slice(b"TRAILING_GARBAGE");
        let error = extract_expected_signature_from_signed_tx_bytes(
            bytes.as_slice(),
            Keypair::new_from_array(TEST_EXECUTOR_SIGNER_SECRET)
                .pubkey()
                .to_string()
                .as_str(),
        )
        .expect_err("trailing garbage must reject");
        assert!(
            error
                .to_string()
                .contains("transaction bincode decode failed"),
            "error={}",
            error
        );
    }

    #[test]
    fn extract_expected_signature_from_signed_tx_bytes_rejects_invalid_signature_bytes() {
        let (bytes, _expected_signature) =
            test_signed_tx_bytes_with_signature_and_signer([8u8; 64], TEST_EXECUTOR_SIGNER_SECRET);
        let mut transaction: VersionedTransaction =
            bincode::deserialize(bytes.as_slice()).expect("decode valid test transaction");
        transaction.signatures[0] = Signature::from([99u8; 64]);
        let tampered_bytes =
            bincode::serialize(&transaction).expect("serialize tampered test transaction");
        let error = extract_expected_signature_from_signed_tx_bytes(
            tampered_bytes.as_slice(),
            Keypair::new_from_array(TEST_EXECUTOR_SIGNER_SECRET)
                .pubkey()
                .to_string()
                .as_str(),
        )
        .expect_err("invalid signature bytes must reject");
        assert!(
            error
                .to_string()
                .contains("contains invalid required signature bytes"),
            "error={}",
            error
        );
    }

    #[test]
    fn decode_executor_signer_pubkey_bytes_rejects_wrong_length_pubkey() {
        let error = decode_executor_signer_pubkey_bytes("111111111111111111111111111111111")
            .expect_err("wrong-length base58 must reject");
        assert!(error.to_string().contains("32 bytes"), "error={}", error);
    }
}
