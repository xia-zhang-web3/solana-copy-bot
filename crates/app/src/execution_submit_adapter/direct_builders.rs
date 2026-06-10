use super::{ExecutionSimulationResult, ExecutionTransactionPlan};
use crate::execution_pump_fun_swap_instructions_http::fetch_pump_fun_swap_instructions_dry_run;
use crate::execution_pump_fun_swap_transaction_http::fetch_pump_fun_swap_transaction_dry_run;
use crate::execution_pumpswap_direct_builder::fetch_pumpswap_direct_transaction_dry_run;
use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_signing_envelope::ExecutionSerializedTransactionPayload;
use crate::execution_simulation_proof::combined_simulation_proof;
use anyhow::Result;
use copybot_config::ExecutionConfig;
use copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED;

pub(super) async fn pump_fun_simulation_result(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<ExecutionSimulationResult> {
    let instructions_proof =
        match fetch_pump_fun_swap_instructions_dry_run(http, config, plan).await {
            Ok(proof) => proof,
            Err(error) => Some(soft_pump_fun_swap_instructions_failure_proof(&error)),
        };
    let transaction_dry_run = fetch_pump_fun_swap_transaction_dry_run(http, config, plan).await?;
    if let Some(transaction) = transaction_dry_run.as_ref() {
        if let Some(slot) = plan.serialized_transaction_payload_slot.as_ref() {
            slot.store(ExecutionSerializedTransactionPayload {
                source: transaction.source.clone(),
                serialized_transaction_base64: transaction.serialized_transaction_base64.clone(),
            })?;
        }
    }
    let transaction_proof = transaction_dry_run.map(|transaction| transaction.summary);
    Ok(ExecutionSimulationResult {
        status: EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
        error: combined_simulation_proof(instructions_proof, transaction_proof),
    })
}

pub(super) async fn pumpswap_direct_simulation_result(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<ExecutionSimulationResult>> {
    let transaction = fetch_pumpswap_direct_transaction_dry_run(http, config, plan).await?;
    if let Some(transaction) = transaction.as_ref() {
        if let Some(slot) = plan.serialized_transaction_payload_slot.as_ref() {
            slot.store(ExecutionSerializedTransactionPayload {
                source: transaction.source.clone(),
                serialized_transaction_base64: transaction.serialized_transaction_base64.clone(),
            })?;
        }
    }
    Ok(transaction.map(|transaction| ExecutionSimulationResult {
        status: EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
        error: Some(transaction.summary),
    }))
}

pub(super) fn result_with_soft_pump_fun_error(
    mut result: ExecutionSimulationResult,
    error: Option<&anyhow::Error>,
) -> ExecutionSimulationResult {
    if let Some(error) = error {
        result.error = combined_simulation_proof(
            Some(format!(
                "pump_fun_direct_swap_soft_failed error={}",
                truncate_for_log(&crate::telemetry::format_error_chain(error), 180)
            )),
            result.error,
        );
    }
    result
}

fn soft_pump_fun_swap_instructions_failure_proof(error: &anyhow::Error) -> String {
    format!(
        "pump_fun_swap_instructions_soft_failed error={}",
        truncate_for_log(&error.to_string(), 180)
    )
}
