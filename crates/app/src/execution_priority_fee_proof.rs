use crate::execution_priority_fee::metadata_fee;
use crate::execution_priority_fee_wire::decode_priority_fee;
use crate::execution_signing_envelope::*;
use crate::execution_submit_adapter::{
    ExecutionSubmitAdapter, ExecutionSubmitIntent, ExecutionSubmitRequest, ExecutionTransactionPlan,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::SqliteStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PriorityFeeProof {
    version: u8,
    idempotency_key: String,
    message_sha256: String,
    transaction_sha256: String,
    requested_compute_unit_limit: u32,
    micro_lamports_per_compute_unit: u64,
    total_priority_fee_lamports: u64,
}

pub(crate) fn prove(
    request: &ExecutionSubmitRequest,
    payload: &str,
    cap: u64,
) -> Result<PriorityFeeProof> {
    metadata_fee(&request.metadata)?;
    let encoded = decode_priority_fee(payload)?;
    // cap=0 retains its existing meaning: no numeric cap, but a complete proof is mandatory.
    ensure!(
        cap == 0 || encoded.total <= cap,
        "priority_fee_cap_exceeded: encoded={} cap={}",
        encoded.total,
        cap
    );
    Ok(PriorityFeeProof {
        version: 1,
        idempotency_key: crate::execution_submit_adapter::execution_submit_idempotency_key(request),
        message_sha256: encoded.message_sha256,
        transaction_sha256: encoded.transaction_sha256,
        requested_compute_unit_limit: encoded.limit.get(),
        micro_lamports_per_compute_unit: encoded.price,
        total_priority_fee_lamports: encoded.total,
    })
}

pub(crate) fn build_envelope<A: ExecutionSubmitAdapter + ?Sized>(
    adapter: &A,
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
) -> Result<ExecutionSigningEnvelope> {
    if let Some(slot) = &plan.serialized_transaction_payload_slot {
        if let Some(payload) = slot.load()? {
            let before = prove(
                request,
                &payload.serialized_transaction_base64,
                adapter.priority_fee_cap(),
            )?;
            let floor = if request.side.eq_ignore_ascii_case("buy")
                || plan.side.eq_ignore_ascii_case("buy")
            {
                crate::execution_native_floor_policy::verify_signing_payload(
                    adapter.native_floor_config()?,
                    request,
                    plan,
                    &payload.serialized_transaction_base64,
                )?
            } else {
                None
            };
            let mut envelope = if let Some(signed) =
                adapter.sign_serialized_transaction(request, plan, &payload)?
            {
                build_signed_transaction_execution_envelope(request, plan, signed)?
            } else {
                build_serialized_transaction_execution_envelope(request, plan, payload)?
            };
            crate::execution_native_floor_policy::verify_after_signing(
                floor.as_ref(),
                envelope_payload(&envelope)?,
            )?;
            let after = prove(
                request,
                envelope_payload(&envelope)?,
                adapter.priority_fee_cap(),
            )?;
            ensure!(
                before.message_sha256 == after.message_sha256,
                "priority_fee_message_changed_after_proof"
            );
            envelope.priority_fee_proof = Some(after);
            return Ok(envelope);
        }
    }
    build_dry_run_execution_signing_envelope(request, plan)
}

pub(crate) fn persist(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
    envelope: &ExecutionSigningEnvelope,
    cap: u64,
    now: DateTime<Utc>,
) -> Result<()> {
    if envelope.signed_transaction_base64.is_none()
        && envelope.serialized_transaction_base64.is_none()
    {
        return Ok(());
    }
    let proof = prove(request, envelope_payload(envelope)?, cap)?;
    ensure!(
        envelope.priority_fee_proof.as_ref() == Some(&proof),
        "priority_fee_envelope_proof_missing_or_changed"
    );
    let mut persisted = plan.clone();
    let mut json: Value = serde_json::from_str(
        plan.metadata
            .priority_fee_json
            .as_deref()
            .context("priority_fee_sample_missing")?,
    )?;
    json["fee_proof"] = serde_json::to_value(proof)?;
    persisted.metadata.priority_fee_json = Some(json.to_string());
    crate::execution_build_plan_metadata::record_execution_build_plan_metadata(
        store, &persisted, now,
    )?;
    Ok(())
}

pub(crate) fn validate_submit(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    envelope: &ExecutionSigningEnvelope,
    intent: &ExecutionSubmitIntent,
    cap: u64,
) -> Result<()> {
    ensure!(
        envelope.idempotency_key == intent.idempotency_key,
        "priority_fee_envelope_identity_mismatch"
    );
    ensure!(
        envelope.signed_transaction_base64.as_deref()
            == Some(intent.signed_transaction_base64.as_str()),
        "priority_fee_submit_payload_changed"
    );
    let proof = prove(request, &intent.signed_transaction_base64, cap)?;
    ensure!(
        envelope.priority_fee_proof.as_ref() == Some(&proof),
        "priority_fee_envelope_proof_missing_or_changed"
    );
    let persisted = store
        .load_execution_canary_build_plan_metadata(&request.order_id)?
        .context("priority_fee_durable_proof_missing")?;
    let json: Value = serde_json::from_str(
        persisted
            .priority_fee_json
            .as_deref()
            .context("priority_fee_durable_proof_missing")?,
    )?;
    let durable: PriorityFeeProof = serde_json::from_value(json["fee_proof"].clone())
        .context("priority_fee_durable_proof_missing")?;
    ensure!(durable == proof, "priority_fee_durable_proof_mismatch");
    Ok(())
}

fn envelope_payload(envelope: &ExecutionSigningEnvelope) -> Result<&str> {
    envelope
        .signed_transaction_base64
        .as_deref()
        .or(envelope.serialized_transaction_base64.as_deref())
        .context("priority_fee_envelope_payload_missing")
}
