//! Guarded generic BUY seam, after existing direct selection. No fallback after assembly.
use crate::execution_instruction_bundle::{pubkey, MAX_RESPONSE_BYTES};
use crate::execution_instruction_bundle_binding::{BoundInstructionBundle, BundleRequest};
use crate::execution_submit_adapter::{
    soft_swap_instructions_failure_proof, ExecutionTransactionPlan,
};
use crate::execution_swap_instructions_http::{
    fetch_instructions_response, fetch_swap_instructions_dry_run,
};
use crate::execution_swap_transaction_http::SwapTransactionDryRunResult;
use anyhow::{ensure, Result};
use copybot_config::ExecutionConfig;
use std::time::Duration;

pub(crate) enum GenericBuyOutcome {
    Built(SwapTransactionDryRunResult),
    ExistingPath(Option<String>),
}

pub(crate) async fn prepare(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<GenericBuyOutcome> {
    // Preserve instructions-only and flag-disabled behavior, including its current
    // allowed soft errors. No transaction build/simulation when the tx flag is off.
    let reserve =
        if config.swap_instructions_dry_run_enabled && config.swap_transaction_dry_run_enabled {
            crate::execution_native_floor_policy::required_for_plan(config, plan)?
        } else {
            None
        };
    let Some(reserve) = reserve else {
        let proof = match fetch_swap_instructions_dry_run(http, config, plan).await {
            Ok(proof) => proof,
            Err(error) => Some(soft_swap_instructions_failure_proof(&error).ok_or(error)?),
        };
        return Ok(GenericBuyOutcome::ExistingPath(proof));
    };
    let binding = BundleRequest::capture(plan)?;
    let response =
        match fetch_instructions_response(http, config, plan, Some(MAX_RESPONSE_BYTES)).await {
            Ok(response) => response,
            Err(error) => {
                return match soft_swap_instructions_failure_proof(&error) {
                    Some(proof) => Ok(GenericBuyOutcome::ExistingPath(Some(proof))),
                    None => Err(error),
                };
            }
        };
    ensure!(
        binding.body() == &response.body,
        "instruction_bundle_request_binding"
    );
    // Only provider errors above may take the existing soft fallback. Structural,
    // binding, fee, floor and simulation refusals below are terminal.
    let bundle = binding.bind(&response.value)?;
    let mut transaction = assemble(config, plan, &bundle, reserve)?;
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
    reserve: u64,
) -> Result<SwapTransactionDryRunResult> {
    ensure!(
        crate::execution_native_floor_policy::required_for_plan(config, plan)? == Some(reserve),
        "instruction_bundle_floor_policy_binding"
    );
    let (binding, instructions) = bundle.verified_parts(plan)?;
    let mut executable = instructions.instructions().to_vec();
    // Jupiter omits CU-price for an explicitly requested zero total fee. Encode
    // that same zero for owner BUY only; the final wire fee parser stays strict.
    let budget = crate::execution_pumpswap_accounts::compute_budget_program_id();
    if plan.signal_id.starts_with("owner-buy:") && plan.side == "buy"
        && crate::execution_priority_fee::metadata_fee(&binding.request().metadata)?
            == crate::execution_priority_fee::PriorityFee::TotalPriorityFeeLamports(0)
        && !executable.iter().any(|ix| ix.program_id == budget && ix.data.first() == Some(&3))
    {
        executable.insert(0, crate::execution_solana_tx::SolanaInstruction {
            program_id: budget,
            accounts: Vec::new(),
            data: [vec![3], 0_u64.to_le_bytes().to_vec()].concat(),
        });
    }
    let prepared = crate::execution_native_floor::prepare_final_native_floor(
        pubkey(&plan.wallet_pubkey)?,
        instructions.blockhash(),
        &executable,
        reserve,
    )?;
    let payload = prepared.payload();
    crate::execution_priority_fee_proof::prove(
        binding.request(),
        payload,
        config.pretrade_max_priority_fee_lamports,
    )?;
    crate::execution_native_floor_policy::verify_signing_payload(
        config,
        binding.request(),
        plan,
        payload,
    )?;
    Ok(SwapTransactionDryRunResult {
        summary: format!(
            "metis_instruction_bundle_legacy instructions={} final_native_floor=true",
            instructions.instructions().len()
        ),
        serialized_transaction_base64: payload.to_owned(),
        source: "metis_instruction_bundle_legacy".into(),
    })
}
