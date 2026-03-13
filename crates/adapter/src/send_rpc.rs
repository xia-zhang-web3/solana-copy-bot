use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use bincode::Options;
use serde_json::{json, Value};
use solana_transaction::versioned::VersionedTransaction;
use tracing::warn;

use crate::{
    http_utils::{
        classify_request_error, read_response_body_limited, redacted_endpoint_label,
        MAX_HTTP_ERROR_BODY_READ_BYTES, MAX_HTTP_JSON_BODY_READ_BYTES,
    },
    validate_signature_like, AppState, Reject,
};

pub(crate) async fn send_signed_transaction_via_rpc(
    state: &AppState,
    route: &str,
    signed_tx_base64: &str,
) -> std::result::Result<String, Reject> {
    let backend = state.config.route_backends.get(route).ok_or_else(|| {
        Reject::terminal(
            "route_not_allowed",
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

    let Some(primary_url) = backend.send_rpc_url.as_deref() else {
        return Err(Reject::terminal(
            "adapter_send_rpc_not_configured",
            format!(
                "route={} missing send RPC URL (set COPYBOT_ADAPTER_ROUTE_{}_SEND_RPC_URL or COPYBOT_ADAPTER_SEND_RPC_URL)",
                route,
                route.to_ascii_uppercase()
            ),
        ));
    };
    let mut endpoints = Vec::with_capacity(2);
    endpoints.push((primary_url, backend.send_rpc_primary_auth_token.as_deref()));
    if let Some(url) = backend.send_rpc_fallback_url.as_deref() {
        endpoints.push((url, backend.send_rpc_fallback_auth_token.as_deref()));
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
        if let Some(token) = auth_token {
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
            let body_text = read_response_body_limited(response, MAX_HTTP_ERROR_BODY_READ_BYTES)
                .await
                .text;
            let reject = if status.as_u16() == 429 || status.is_server_error() {
                Reject::retryable(
                    "send_rpc_http_unavailable",
                    format!(
                        "send RPC status={} endpoint={} body={}",
                        status, endpoint_label, body_text
                    ),
                )
            } else {
                Reject::terminal(
                    "send_rpc_http_rejected",
                    format!(
                        "send RPC status={} endpoint={} body={}",
                        status, endpoint_label, body_text
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
        let body_read = read_response_body_limited(response, MAX_HTTP_JSON_BODY_READ_BYTES).await;
        if let Some(read_error_class) = body_read.read_error_class {
            return Err(Reject::terminal(
                "send_rpc_body_read_failed",
                format!(
                    "send RPC body read failed endpoint={} class={}",
                    endpoint_label, read_error_class
                ),
            ));
        }
        if body_read.was_truncated {
            return Err(Reject::terminal(
                "send_rpc_response_too_large",
                format!(
                    "send RPC response exceeded {} bytes endpoint={}",
                    MAX_HTTP_JSON_BODY_READ_BYTES, endpoint_label
                ),
            ));
        }
        let body: Value = serde_json::from_slice(body_read.bytes.as_slice()).map_err(|error| {
            Reject::terminal(
                "send_rpc_invalid_json",
                format!(
                    "send RPC response invalid JSON endpoint={} err={}",
                    endpoint_label, error
                ),
            )
        })?;
        if let Some(error_payload) = body.get("error") {
            if !error_payload.is_null() {
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
                                endpoint_label, error_payload
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
                    SendRpcErrorPayloadDisposition::Terminal => {
                        return Err(Reject::terminal(
                            "send_rpc_error_payload_terminal",
                            format!(
                                "send RPC returned terminal error endpoint={} payload={}",
                                endpoint_label, error_payload
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SendRpcErrorPayloadDisposition {
    AlreadyProcessed,
    Retryable,
    Terminal,
}

fn classify_send_rpc_error_payload(error_payload: &Value) -> SendRpcErrorPayloadDisposition {
    let code = error_payload.get("code").and_then(Value::as_i64);
    let payload_lower = error_payload.to_string().to_ascii_lowercase();

    if code == Some(-32002)
        && (payload_lower.contains("already processed")
            || payload_lower.contains("already confirmed")
            || payload_lower.contains("already finalized"))
    {
        return SendRpcErrorPayloadDisposition::AlreadyProcessed;
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
    adapter_signer_pubkey: &str,
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

    let adapter_signer_pubkey = decode_adapter_signer_pubkey_bytes(adapter_signer_pubkey)?;
    let signer_required = transaction
        .message
        .static_account_keys()
        .iter()
        .take(required_signatures)
        .any(|pubkey| pubkey.as_ref() == adapter_signer_pubkey.as_slice());
    if !signer_required {
        return Err(anyhow!(
            "transaction does not require configured adapter signer pubkey"
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

fn decode_adapter_signer_pubkey_bytes(signer_pubkey: &str) -> Result<[u8; 32]> {
    let decoded = bs58::decode(signer_pubkey.trim())
        .into_vec()
        .context("configured adapter signer pubkey must be valid base58")?;
    let signer_bytes: [u8; 32] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("configured adapter signer pubkey must decode to 32 bytes"))?;
    Ok(signer_bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        classify_send_rpc_error_payload, decode_adapter_signer_pubkey_bytes,
        extract_expected_signature_from_signed_tx_bytes, SendRpcErrorPayloadDisposition,
    };
    use serde_json::json;
    use solana_keypair::{Keypair, Signature, Signer};
    use solana_message::{
        compiled_instruction::CompiledInstruction, legacy::Message as LegacyMessage, Address, Hash,
        MessageHeader, VersionedMessage,
    };
    use solana_transaction::versioned::VersionedTransaction;

    const TEST_ADAPTER_SIGNER_SECRET: [u8; 32] = [11u8; 32];

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
    fn classify_send_rpc_error_payload_keeps_blockhash_not_found_terminal() {
        let payload = json!({
            "code": -32002,
            "message": "Blockhash not found"
        });
        assert_eq!(
            classify_send_rpc_error_payload(&payload),
            SendRpcErrorPayloadDisposition::Terminal
        );
    }

    #[test]
    fn extract_expected_signature_from_signed_tx_bytes_reads_canonical_versioned_transaction() {
        let (bytes, expected_signature) =
            test_signed_tx_bytes_with_signature_and_signer([5u8; 64], TEST_ADAPTER_SIGNER_SECRET);
        let signature = extract_expected_signature_from_signed_tx_bytes(
            bytes.as_slice(),
            Keypair::new_from_array(TEST_ADAPTER_SIGNER_SECRET)
                .pubkey()
                .to_string()
                .as_str(),
        )
        .expect("canonical transaction should decode");
        assert_eq!(signature, expected_signature);
    }

    #[test]
    fn extract_expected_signature_from_signed_tx_bytes_rejects_when_adapter_signer_missing() {
        let (bytes, _expected_signature) =
            test_signed_tx_bytes_with_signature_and_signer([6u8; 64], [8u8; 32]);
        let error = extract_expected_signature_from_signed_tx_bytes(
            bytes.as_slice(),
            Keypair::new_from_array(TEST_ADAPTER_SIGNER_SECRET)
                .pubkey()
                .to_string()
                .as_str(),
        )
        .expect_err("mismatched signer should reject");
        assert!(
            error
                .to_string()
                .contains("does not require configured adapter signer"),
            "error={}",
            error
        );
    }

    #[test]
    fn extract_expected_signature_from_signed_tx_bytes_rejects_trailing_garbage() {
        let (mut bytes, _expected_signature) =
            test_signed_tx_bytes_with_signature_and_signer([7u8; 64], TEST_ADAPTER_SIGNER_SECRET);
        bytes.extend_from_slice(b"TRAILING_GARBAGE");
        let error = extract_expected_signature_from_signed_tx_bytes(
            bytes.as_slice(),
            Keypair::new_from_array(TEST_ADAPTER_SIGNER_SECRET)
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
            test_signed_tx_bytes_with_signature_and_signer([8u8; 64], TEST_ADAPTER_SIGNER_SECRET);
        let mut transaction: VersionedTransaction =
            bincode::deserialize(bytes.as_slice()).expect("decode valid test transaction");
        transaction.signatures[0] = Signature::from([99u8; 64]);
        let tampered_bytes =
            bincode::serialize(&transaction).expect("serialize tampered test transaction");
        let error = extract_expected_signature_from_signed_tx_bytes(
            tampered_bytes.as_slice(),
            Keypair::new_from_array(TEST_ADAPTER_SIGNER_SECRET)
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
    fn decode_adapter_signer_pubkey_bytes_rejects_wrong_length_pubkey() {
        let error = decode_adapter_signer_pubkey_bytes("111111111111111111111111111111111")
            .expect_err("wrong-length base58 must reject");
        assert!(error.to_string().contains("32 bytes"), "error={}", error);
    }
}
