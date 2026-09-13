//! Complete legacy bundle for tiny SELL, after existing direct route selection.
use crate::execution_guarded_generic_buy::GenericBuyOutcome;
use crate::execution_instruction_bundle::{pubkey, MAX_RESPONSE_BYTES};
use crate::execution_instruction_bundle_binding::{BoundInstructionBundle, BundleRequest};
use crate::execution_submit_adapter::{
    soft_swap_instructions_failure_proof, ExecutionTransactionPlan,
};
use crate::execution_swap_instructions_http::fetch_instructions_response;
use crate::execution_swap_transaction_http::SwapTransactionDryRunResult;
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use copybot_config::ExecutionConfig;
use std::time::Duration;

pub(crate) fn applies(config: &ExecutionConfig, plan: &ExecutionTransactionPlan) -> bool {
    config.canary_tiny_submit_enabled
        && config.swap_instructions_dry_run_enabled
        && config.swap_transaction_dry_run_enabled
        && plan.side.eq_ignore_ascii_case("sell")
}

pub(crate) async fn prepare(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<GenericBuyOutcome> {
    ensure!(
        applies(config, plan),
        "instruction_bundle_sell_mode_binding"
    );
    let binding = BundleRequest::capture(plan)?;
    let response =
        match fetch_instructions_response(http, config, plan, Some(MAX_RESPONSE_BYTES)).await {
            Ok(response) => response,
            Err(error) => {
                return match soft_swap_instructions_failure_proof(&error) {
                    Some(proof) => Ok(GenericBuyOutcome::ExistingPath(Some(proof))),
                    None => Err(error),
                }
            }
        };
    ensure!(
        binding.body() == &response.body,
        "instruction_bundle_request_binding"
    );
    // Only the existing provider soft errors above may reach /swap. Every refusal
    // after binding/assembly is terminal, including oversize and client simulation.
    let bundle = binding.bind(&response.value)?;
    let mut transaction = assemble(config, plan, &bundle)?;
    let simulation =
        crate::execution_transaction_rpc_simulation::verify_serialized_transaction_rpc_simulation(
            http,
            config,
            &transaction.serialized_transaction_base64,
            &transaction.source,
            Duration::from_millis(config.quote_canary_timeout_ms.max(1)),
        )
        .await?;
    ensure!(
        matches!(
            simulation,
            crate::execution_transaction_rpc_simulation::RpcSimulationOutcome::Passed { .. }
        ),
        "instruction_bundle_simulation_required"
    );
    transaction.summary = simulation.with_summary(&transaction.summary);
    Ok(GenericBuyOutcome::Built(transaction))
}

pub(crate) fn assemble(
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
    bundle: &BoundInstructionBundle,
) -> Result<SwapTransactionDryRunResult> {
    ensure!(
        applies(config, plan),
        "instruction_bundle_sell_mode_binding"
    );
    assemble_unsigned(config, plan, bundle)
}
fn assemble_unsigned(
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
    bundle: &BoundInstructionBundle,
) -> Result<SwapTransactionDryRunResult> {
    let (binding, instructions) = bundle.verified_parts(plan)?;
    ensure!(
        !config.execution_signer_pubkey.is_empty()
            && config.execution_signer_pubkey == config.canary_wallet_pubkey
            && plan.wallet_pubkey == config.execution_signer_pubkey,
        "instruction_bundle_wallet_identity_mismatch"
    );
    // Explicit account operands are supplied by the bundle; lookup metadata is
    // never chain proof. The existing serializer preserves every instruction and
    // creates exactly one payer signer with a zero signature. No SELL native floor.
    let bytes = crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
        pubkey(&plan.wallet_pubkey)?,
        instructions.blockhash(),
        instructions.instructions(),
    )?;
    ensure!(
        bytes.len() <= 1232,
        "instruction_bundle_legacy_packet_too_large"
    );
    let payload = STANDARD.encode(bytes);
    crate::execution_priority_fee_proof::prove(
        binding.request(),
        &payload,
        config.pretrade_max_priority_fee_lamports,
    )?;
    crate::execution_signing_envelope::build_serialized_transaction_execution_envelope(
        binding.request(),
        plan,
        crate::execution_signing_envelope::ExecutionSerializedTransactionPayload {
            source: "metis_instruction_bundle_legacy".into(),
            serialized_transaction_base64: payload.clone(),
        },
    )?;
    Ok(SwapTransactionDryRunResult {
        summary: format!(
            "metis_instruction_bundle_legacy instructions={} final_native_floor=false",
            instructions.instructions().len()
        ),
        serialized_transaction_base64: payload,
        source: "metis_instruction_bundle_legacy".into(),
    })
}

/// Only the closed finalized authority can enter with trading disabled. Legacy
/// prepare/assemble retain their existing mode predicates and RPC semantics.
pub(crate) async fn prepare_owned(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
    authority: &crate::execution_owned_sell_rpc::Authority,
    snapshot: &copybot_storage_core::rpc_owned_sell_snapshot::OwnedSellSnapshot,
    check: &mut impl FnMut() -> Result<()>,
) -> Result<SwapTransactionDryRunResult> {
    ensure!(
        copybot_config::owned_sell_flags(config)
            && config.owned_sell_preparation.is_some()
            && plan.side == "sell"
            && !plan.submit_enabled,
        "owned_sell_unsigned_only_flags"
    );
    authority.binding(config, snapshot)?;
    ensure!(
        plan.token == snapshot.quote.mint
            && plan.wallet_pubkey == config.canary_wallet_pubkey
            && plan.metadata.quote_in_amount_raw.as_deref()
                == Some(snapshot.quote.raw.to_string().as_str()),
        "owned_sell_plan_binding"
    );
    check()?;
    let binding = BundleRequest::capture(plan)?;
    let response =
        fetch_instructions_response(http, config, plan, Some(MAX_RESPONSE_BYTES)).await?;
    check()?;
    ensure!(
        binding.body() == &response.body,
        "instruction_bundle_request_binding"
    );
    let transaction = assemble_unsigned(config, plan, &binding.bind(&response.value)?)?;
    let rpc=crate::execution_owned_sell_rpc::exchange(http,config,&crate::execution_owned_sell_rpc::endpoint(config)?,serde_json::json!({"jsonrpc":"2.0","id":"execution-swap-transaction-simulate","method":"simulateTransaction","params":[transaction.serialized_transaction_base64,{"encoding":"base64","sigVerify":false,"replaceRecentBlockhash":!copybot_config::owned_sell_dispatch(config),"commitment":"finalized"}]}),check).await?;
    crate::execution_transaction_rpc_simulation::parse_rpc_simulation_response(
        rpc.value(),
        &transaction.source,
    )?;
    check()?;
    Ok(transaction)
}
