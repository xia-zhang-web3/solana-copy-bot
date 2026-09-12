//! Explicit synthetic fee metadata, never inferred from an old recommended field.
use crate::execution_solana_tx::{serialize_unsigned_legacy_transaction, SolanaInstruction};
use base64::{engine::general_purpose::STANDARD, Engine};

pub(super) fn total_json(value: u64) -> String {
    serde_json::json!({"version":1,"source":"synthetic_known_total",
        "unit":"total_priority_fee_lamports","value":value})
    .to_string()
}

pub(super) fn transaction(payer: [u8; 32], limit: u32, price: u64) -> String {
    STANDARD.encode(
        serialize_unsigned_legacy_transaction(payer, [9; 32], &budget(limit, price)).unwrap(),
    )
}

pub(super) fn budget(limit: u32, price: u64) -> Vec<SolanaInstruction> {
    let program_id = bs58::decode("ComputeBudget111111111111111111111111111111")
        .into_vec()
        .unwrap()
        .try_into()
        .unwrap();
    vec![
        SolanaInstruction {
            program_id,
            accounts: vec![],
            data: [vec![2], limit.to_le_bytes().to_vec()].concat(),
        },
        SolanaInstruction {
            program_id,
            accounts: vec![],
            data: [vec![3], price.to_le_bytes().to_vec()].concat(),
        },
    ]
}

pub(super) fn metadata() -> crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
    crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
        http_request_started_ts: None,
        quote_response_available_ts: None,
        priority_fee_status: Some("ok".into()),
        priority_fee_lamports: Some(2_000),
        priority_fee_json: Some(total_json(2_000)),
        ..Default::default()
    }
}

// Receipt/transport tests use unsigned placeholder signatures. Only the route tests sign.
// A complete local fee proof is still required before their loopback transport call.
pub(super) fn envelope(
    store: &copybot_storage_core::SqliteStore,
    request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<crate::execution_signing_envelope::ExecutionSigningEnvelope> {
    use crate::execution_submit_adapter::ExecutionSubmitAdapter;
    let plan = crate::execution_submit_adapter::NoSubmitExecutionAdapter
        .build_transaction_plan(request)?;
    let tx = if request.side.eq_ignore_ascii_case("buy") {
        let payer =
            crate::execution_pumpswap_accounts::parse_pubkey(&request.wallet_pubkey, "test payer")?;
        guarded_transaction(payer, 200_000, 10_000)
    } else {
        transaction([7; 32], 200_000, 10_000)
    };
    let mut envelope =
        crate::execution_signing_envelope::build_signed_transaction_execution_envelope(
            request,
            &plan,
            crate::execution_signing_envelope::ExecutionSignedTransactionPayload {
                signed_transaction_base64: tx.clone(),
                tx_signature_hint: Some("tx-hint-from-envelope".into()),
            },
        )?;
    envelope.priority_fee_proof =
        Some(crate::execution_priority_fee_proof::prove(request, &tx, 0)?);
    crate::execution_priority_fee_proof::persist(store, request, &plan, &envelope, 0, now)?;
    Ok(envelope)
}

pub(super) fn proven_envelope(
    request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
    payload: crate::execution_signing_envelope::ExecutionSignedTransactionPayload,
) -> anyhow::Result<crate::execution_signing_envelope::ExecutionSigningEnvelope> {
    let proof =
        crate::execution_priority_fee_proof::prove(request, &payload.signed_transaction_base64, 0)?;
    let mut envelope =
        crate::execution_signing_envelope::build_signed_transaction_execution_envelope(
            request, plan, payload,
        )?;
    envelope.priority_fee_proof = Some(proof);
    Ok(envelope)
}

// Explicit dry-run policy for adapters testing the pre-existing non-tiny signing contract.
pub(super) fn dry_run_config() -> &'static copybot_config::ExecutionConfig {
    static CONFIG: std::sync::OnceLock<copybot_config::ExecutionConfig> =
        std::sync::OnceLock::new();
    CONFIG.get_or_init(copybot_config::ExecutionConfig::default)
}

pub(super) fn guarded_transaction(payer: [u8; 32], limit: u32, price: u64) -> String {
    crate::execution_native_floor::prepare_final_native_floor(
        payer,
        [9; 32],
        &budget(limit, price),
        50_000_001,
    )
    .unwrap()
    .payload()
    .to_owned()
}
