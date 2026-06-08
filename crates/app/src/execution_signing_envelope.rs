use crate::execution_submit_adapter::{
    execution_submit_idempotency_key, ExecutionSubmitRequest, ExecutionTransactionPlan,
};
use anyhow::Result;

pub(crate) const EXECUTION_SIGNING_ENVELOPE_MODE_DRY_RUN: &str = "dry_run_unsigned";
pub(crate) const EXECUTION_SIGNING_ENVELOPE_MODE_SERIALIZED_TRANSACTION_DRY_RUN: &str =
    "serialized_transaction_dry_run";
pub(crate) const EXECUTION_SIGNING_ENVELOPE_MODE_SIGNED_TRANSACTION_DRY_RUN: &str =
    "signed_transaction_dry_run";
const EXECUTION_SIGNING_PAYLOAD_KIND_V1: &str = "execution_transaction_plan_v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionSigningEnvelope {
    pub(crate) envelope_id: String,
    pub(crate) idempotency_key: String,
    pub(crate) mode: String,
    pub(crate) payload_kind: String,
    pub(crate) payload_fingerprint: String,
    pub(crate) submit_enabled: bool,
    pub(crate) serialized_transaction_base64: Option<String>,
    pub(crate) signed_transaction_base64: Option<String>,
    pub(crate) tx_signature_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionSerializedTransactionPayload {
    pub(crate) source: String,
    pub(crate) serialized_transaction_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionSignedTransactionPayload {
    pub(crate) signed_transaction_base64: String,
    pub(crate) tx_signature_hint: Option<String>,
}

pub(crate) fn build_dry_run_execution_signing_envelope(
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
) -> Result<ExecutionSigningEnvelope> {
    validate_request_plan_alignment(request, plan)?;
    let payload = canonical_signing_payload(request, plan);
    let envelope = ExecutionSigningEnvelope {
        envelope_id: format!(
            "copybot:sign:{}:attempt:{}",
            request.client_order_id, request.attempt
        ),
        idempotency_key: execution_submit_idempotency_key(request),
        mode: EXECUTION_SIGNING_ENVELOPE_MODE_DRY_RUN.to_string(),
        payload_kind: EXECUTION_SIGNING_PAYLOAD_KIND_V1.to_string(),
        payload_fingerprint: stable_payload_fingerprint(payload.as_bytes()),
        submit_enabled: false,
        serialized_transaction_base64: None,
        signed_transaction_base64: None,
        tx_signature_hint: None,
    };
    validate_execution_signing_envelope(&envelope, request, plan)?;
    Ok(envelope)
}

pub(crate) fn build_serialized_transaction_execution_envelope(
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
    payload: ExecutionSerializedTransactionPayload,
) -> Result<ExecutionSigningEnvelope> {
    validate_request_plan_alignment(request, plan)?;
    validate_serialized_transaction_payload(&payload)?;
    let canonical_payload = canonical_serialized_transaction_payload(request, plan, &payload);
    let envelope = ExecutionSigningEnvelope {
        envelope_id: format!(
            "copybot:sign:{}:attempt:{}",
            request.client_order_id, request.attempt
        ),
        idempotency_key: execution_submit_idempotency_key(request),
        mode: EXECUTION_SIGNING_ENVELOPE_MODE_SERIALIZED_TRANSACTION_DRY_RUN.to_string(),
        payload_kind: EXECUTION_SIGNING_PAYLOAD_KIND_V1.to_string(),
        payload_fingerprint: stable_payload_fingerprint(canonical_payload.as_bytes()),
        submit_enabled: false,
        serialized_transaction_base64: Some(payload.serialized_transaction_base64),
        signed_transaction_base64: None,
        tx_signature_hint: None,
    };
    validate_execution_signing_envelope(&envelope, request, plan)?;
    Ok(envelope)
}

pub(crate) fn build_signed_transaction_execution_envelope(
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
    payload: ExecutionSignedTransactionPayload,
) -> Result<ExecutionSigningEnvelope> {
    validate_request_plan_alignment(request, plan)?;
    validate_signed_transaction_payload(&payload)?;
    let canonical_payload = canonical_signed_transaction_payload(request, plan, &payload);
    let envelope = ExecutionSigningEnvelope {
        envelope_id: format!(
            "copybot:sign:{}:attempt:{}",
            request.client_order_id, request.attempt
        ),
        idempotency_key: execution_submit_idempotency_key(request),
        mode: EXECUTION_SIGNING_ENVELOPE_MODE_SIGNED_TRANSACTION_DRY_RUN.to_string(),
        payload_kind: EXECUTION_SIGNING_PAYLOAD_KIND_V1.to_string(),
        payload_fingerprint: stable_payload_fingerprint(canonical_payload.as_bytes()),
        submit_enabled: false,
        serialized_transaction_base64: None,
        signed_transaction_base64: Some(payload.signed_transaction_base64),
        tx_signature_hint: payload.tx_signature_hint,
    };
    validate_execution_signing_envelope(&envelope, request, plan)?;
    Ok(envelope)
}

pub(crate) fn validate_execution_signing_envelope(
    envelope: &ExecutionSigningEnvelope,
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
) -> Result<()> {
    validate_request_plan_alignment(request, plan)?;
    if envelope.envelope_id.trim().is_empty() {
        anyhow::bail!("signing envelope id must be non-empty");
    }
    let expected_key = execution_submit_idempotency_key(request);
    if envelope.idempotency_key != expected_key {
        anyhow::bail!(
            "signing envelope idempotency key mismatch: actual={} expected={}",
            envelope.idempotency_key,
            expected_key
        );
    }
    if envelope.payload_kind != EXECUTION_SIGNING_PAYLOAD_KIND_V1 {
        anyhow::bail!("signing envelope payload kind mismatch");
    }
    if envelope.payload_fingerprint.trim().is_empty() {
        anyhow::bail!("signing envelope payload fingerprint must be non-empty");
    }
    let expected_payload_fingerprint = match envelope.mode.as_str() {
        EXECUTION_SIGNING_ENVELOPE_MODE_DRY_RUN => {
            if envelope.submit_enabled {
                anyhow::bail!("dry-run signing envelope must not enable submit");
            }
            if envelope.serialized_transaction_base64.is_some()
                || envelope.signed_transaction_base64.is_some()
                || envelope.tx_signature_hint.is_some()
            {
                anyhow::bail!("dry-run signing envelope must not carry transaction data");
            }
            stable_payload_fingerprint(canonical_signing_payload(request, plan).as_bytes())
        }
        EXECUTION_SIGNING_ENVELOPE_MODE_SERIALIZED_TRANSACTION_DRY_RUN => {
            if envelope.submit_enabled {
                anyhow::bail!("serialized transaction dry-run envelope must not enable submit");
            }
            if envelope.signed_transaction_base64.is_some() || envelope.tx_signature_hint.is_some()
            {
                anyhow::bail!("serialized transaction dry-run envelope must not carry signed data");
            }
            let Some(serialized_transaction_base64) =
                envelope.serialized_transaction_base64.clone()
            else {
                anyhow::bail!(
                    "serialized transaction dry-run envelope requires serialized transaction"
                );
            };
            let payload = ExecutionSerializedTransactionPayload {
                source: "envelope".to_string(),
                serialized_transaction_base64,
            };
            validate_serialized_transaction_payload(&payload)?;
            stable_payload_fingerprint(
                canonical_serialized_transaction_payload(request, plan, &payload).as_bytes(),
            )
        }
        EXECUTION_SIGNING_ENVELOPE_MODE_SIGNED_TRANSACTION_DRY_RUN => {
            if envelope.submit_enabled {
                anyhow::bail!("signed transaction dry-run envelope must not enable submit");
            }
            if envelope.serialized_transaction_base64.is_some() {
                anyhow::bail!(
                    "signed transaction dry-run envelope must not carry unsigned serialized data"
                );
            }
            let Some(signed_transaction_base64) = envelope.signed_transaction_base64.clone() else {
                anyhow::bail!("signed transaction dry-run envelope requires signed transaction");
            };
            let payload = ExecutionSignedTransactionPayload {
                signed_transaction_base64,
                tx_signature_hint: envelope.tx_signature_hint.clone(),
            };
            validate_signed_transaction_payload(&payload)?;
            stable_payload_fingerprint(
                canonical_signed_transaction_payload(request, plan, &payload).as_bytes(),
            )
        }
        _ => anyhow::bail!("unknown signing envelope mode: {}", envelope.mode),
    };
    if envelope.payload_fingerprint != expected_payload_fingerprint {
        anyhow::bail!("signing envelope payload fingerprint mismatch");
    }
    Ok(())
}

pub(crate) fn validate_serialized_transaction_base64(raw: &str) -> Result<()> {
    validate_transaction_base64(raw, "serialized transaction base64")
}

pub(crate) fn validate_signed_transaction_base64(raw: &str) -> Result<()> {
    validate_transaction_base64(raw, "signed transaction base64")
}

fn validate_transaction_base64(raw: &str, label: &str) -> Result<()> {
    let value = raw.trim();
    if value.is_empty() {
        anyhow::bail!("{label} must be non-empty");
    }
    if value.len() % 4 != 0 {
        anyhow::bail!("{label} length must be padded to a multiple of 4");
    }

    let mut data_chars = 0usize;
    let mut padding_chars = 0usize;
    let mut padding_started = false;
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' if !padding_started => {
                data_chars += 1;
            }
            b'=' => {
                padding_started = true;
                padding_chars += 1;
                if padding_chars > 2 {
                    anyhow::bail!("{label} padding is invalid");
                }
            }
            _ => anyhow::bail!("{label} contains invalid characters"),
        }
    }
    if data_chars == 0 {
        anyhow::bail!("{label} must contain data");
    }
    Ok(())
}

fn validate_serialized_transaction_payload(
    payload: &ExecutionSerializedTransactionPayload,
) -> Result<()> {
    if payload.source.trim().is_empty() {
        anyhow::bail!("serialized transaction source must be non-empty");
    }
    validate_serialized_transaction_base64(&payload.serialized_transaction_base64)?;
    Ok(())
}

fn validate_signed_transaction_payload(payload: &ExecutionSignedTransactionPayload) -> Result<()> {
    validate_signed_transaction_base64(&payload.signed_transaction_base64)?;
    if let Some(tx_signature_hint) = payload.tx_signature_hint.as_deref() {
        validate_tx_signature_hint(tx_signature_hint)?;
    }
    Ok(())
}

fn validate_tx_signature_hint(raw: &str) -> Result<()> {
    let value = raw.trim();
    if value.is_empty() {
        anyhow::bail!("transaction signature hint must be non-empty");
    }
    if value.len() > 128 {
        anyhow::bail!("transaction signature hint is too long");
    }
    Ok(())
}

fn validate_request_plan_alignment(
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
) -> Result<()> {
    if request.order_id != plan.order_id
        || request.signal_id != plan.signal_id
        || request.client_order_id != plan.client_order_id
        || request.attempt != plan.attempt
        || request.route != plan.route
        || request.token != plan.token
        || request.side != plan.side
        || request.wallet_pubkey != plan.wallet_pubkey
    {
        anyhow::bail!("execution signing envelope request/plan mismatch");
    }
    Ok(())
}

fn canonical_signing_payload(
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
) -> String {
    [
        EXECUTION_SIGNING_PAYLOAD_KIND_V1,
        request.order_id.as_str(),
        request.signal_id.as_str(),
        request.client_order_id.as_str(),
        &request.attempt.to_string(),
        request.route.as_str(),
        request.token.as_str(),
        request.side.as_str(),
        request.wallet_pubkey.as_str(),
        plan.plan_id.as_str(),
    ]
    .join("|")
}

fn canonical_serialized_transaction_payload(
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
    payload: &ExecutionSerializedTransactionPayload,
) -> String {
    [
        canonical_signing_payload(request, plan),
        EXECUTION_SIGNING_ENVELOPE_MODE_SERIALIZED_TRANSACTION_DRY_RUN.to_string(),
        "envelope".to_string(),
        payload.serialized_transaction_base64.clone(),
    ]
    .join("|")
}

fn canonical_signed_transaction_payload(
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
    payload: &ExecutionSignedTransactionPayload,
) -> String {
    [
        canonical_signing_payload(request, plan),
        EXECUTION_SIGNING_ENVELOPE_MODE_SIGNED_TRANSACTION_DRY_RUN.to_string(),
        payload.signed_transaction_base64.clone(),
        payload.tx_signature_hint.clone().unwrap_or_default(),
    ]
    .join("|")
}

fn stable_payload_fingerprint(payload: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
}
