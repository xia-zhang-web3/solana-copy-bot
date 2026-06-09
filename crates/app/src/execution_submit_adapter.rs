use crate::execution_pump_fun_swap_instructions_http::fetch_pump_fun_swap_instructions_dry_run;
use crate::execution_pump_fun_swap_transaction_http::fetch_pump_fun_swap_transaction_dry_run;
use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_quote_provider_selection::QUOTE_SOURCE_PUMP_FUN_PAID;
use crate::execution_serialized_transaction_slot::ExecutionSerializedTransactionPayloadSlot;
use crate::execution_signing_envelope::{
    build_dry_run_execution_signing_envelope, build_serialized_transaction_execution_envelope,
    build_signed_transaction_execution_envelope, ExecutionSerializedTransactionPayload,
    ExecutionSignedTransactionPayload, ExecutionSigningEnvelope,
};
use crate::execution_simulation_proof::combined_simulation_proof;
use crate::execution_swap_blueprint::{
    build_execution_swap_blueprint, validate_execution_swap_blueprint_for_simulation,
    ExecutionSwapBlueprint,
};
use crate::execution_swap_http_request::is_missing_account_error_text;
use crate::execution_swap_instructions_http::fetch_swap_instructions_dry_run;
use crate::execution_swap_transaction_http::fetch_swap_transaction_dry_run;
use anyhow::Result;
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    EXECUTION_SIMULATION_STATUS_PASSED, EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT,
};
use std::future::Future;
use std::pin::Pin;

mod confirmation;
mod confirmation_boundary;
mod confirmed_fill;
mod file_signer;
mod rpc_confirmation;
mod rpc_confirmed_fill;
mod rpc_submit;
mod submit_plan;
mod tiny_runner;
mod transport;
mod transport_record;

pub(crate) use self::confirmation::{
    build_confirmation_request_from_order, record_confirmation_tracker_outcome,
    ExecutionConfirmationProof, ExecutionConfirmationRequest, ExecutionConfirmationTrackerOutcome,
};
#[cfg(test)]
pub(crate) use self::confirmation::{
    ExecutionConfirmationTracker, ExecutionConfirmationTrackerRecordOutcome,
    MockConfirmedExecutionConfirmationTracker, NoSendExecutionConfirmationTracker,
};
pub(crate) use self::confirmation_boundary::{
    record_execution_rpc_confirmation_boundary, ExecutionConfirmationBoundaryOutcome,
};
pub(crate) use self::confirmed_fill::{
    record_confirmed_fill_accounting, ExecutionConfirmedBuyFill, ExecutionConfirmedFill,
    ExecutionConfirmedSellFill,
};
pub(crate) use self::file_signer::sign_serialized_transaction_from_config;
pub(crate) use self::rpc_confirmation::fetch_rpc_signature_confirmation;
#[cfg(test)]
pub(crate) use self::rpc_confirmed_fill::confirmed_fill_from_transaction_json;
pub(crate) use self::rpc_submit::RpcExecutionSubmitTransport;
pub(crate) use self::submit_plan::{
    execution_submit_idempotency_key, execution_submit_intent_from_signed_envelope,
    ExecutionSubmitIntent, ExecutionSubmitPlan,
};
#[cfg(test)]
pub(crate) use self::tiny_runner::build_execution_confirmed_fill_from_request;
pub(crate) use self::tiny_runner::{
    build_tiny_submit_reconciliation_request, reconcile_execution_tiny_submit_confirmation,
    record_execution_tiny_submit_confirm_path, ExecutionTinySubmitConfirmPathOutcome,
};
pub(crate) use self::transport::{
    build_submit_transport_attempt, dry_run_no_send_submit_intent, ExecutionSubmitTransportAttempt,
    ExecutionSubmitTransportOutcome,
};
#[cfg(test)]
pub(crate) use self::transport::{ExecutionSubmitTransport, NoSendExecutionSubmitTransport};
pub(crate) use self::transport_record::record_submit_transport_outcome;
#[cfg(test)]
pub(crate) use self::transport_record::ExecutionSubmitTransportRecordOutcome;

const EXECUTION_SUBMIT_ROUTE_RPC_DRY_RUN: &str = "rpc_dry_run";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionBuildPlanMetadata {
    pub(crate) quote_source: Option<String>,
    pub(crate) quote_event_id: Option<String>,
    pub(crate) quote_status: Option<String>,
    pub(crate) quote_in_amount_raw: Option<String>,
    pub(crate) quote_out_amount_raw: Option<String>,
    pub(crate) quote_response_json: Option<String>,
    pub(crate) quote_price_sol: Option<f64>,
    pub(crate) price_impact_pct: Option<f64>,
    pub(crate) route_plan_json: Option<String>,
    pub(crate) priority_fee_source: Option<String>,
    pub(crate) priority_fee_status: Option<String>,
    pub(crate) priority_fee_lamports: Option<u64>,
    pub(crate) priority_fee_json: Option<String>,
    pub(crate) slippage_bps: Option<f64>,
    pub(crate) decision_status: Option<String>,
    pub(crate) decision_reason: Option<String>,
}

impl Default for ExecutionBuildPlanMetadata {
    fn default() -> Self {
        Self {
            quote_source: Some("not_available".to_string()),
            quote_event_id: None,
            quote_status: None,
            quote_in_amount_raw: None,
            quote_out_amount_raw: None,
            quote_response_json: None,
            quote_price_sol: None,
            price_impact_pct: None,
            route_plan_json: None,
            priority_fee_source: Some("not_available".to_string()),
            priority_fee_status: None,
            priority_fee_lamports: None,
            priority_fee_json: None,
            slippage_bps: None,
            decision_status: None,
            decision_reason: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionSubmitRequest {
    pub(crate) order_id: String,
    pub(crate) signal_id: String,
    pub(crate) client_order_id: String,
    pub(crate) attempt: u32,
    pub(crate) route: String,
    pub(crate) wallet_id: String,
    pub(crate) token: String,
    pub(crate) side: String,
    pub(crate) buy_size_sol: f64,
    pub(crate) slippage_tolerance_bps: u64,
    pub(crate) wallet_pubkey: String,
    pub(crate) metadata: ExecutionBuildPlanMetadata,
}

pub(crate) fn cap_execution_priority_fee_lamports(
    config: &ExecutionConfig,
    mut metadata: ExecutionBuildPlanMetadata,
) -> ExecutionBuildPlanMetadata {
    let cap = config.pretrade_max_priority_fee_lamports;
    if cap == 0 {
        return metadata;
    }
    if metadata.priority_fee_lamports.is_some_and(|fee| fee > cap) {
        metadata.priority_fee_lamports = Some(cap);
    }
    metadata
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionTransactionPlan {
    pub(crate) plan_id: String,
    pub(crate) order_id: String,
    pub(crate) signal_id: String,
    pub(crate) client_order_id: String,
    pub(crate) attempt: u32,
    pub(crate) route: String,
    pub(crate) token: String,
    pub(crate) side: String,
    pub(crate) buy_size_sol: f64,
    pub(crate) slippage_tolerance_bps: u64,
    pub(crate) wallet_pubkey: String,
    pub(crate) metadata: ExecutionBuildPlanMetadata,
    pub(crate) swap_blueprint: Option<ExecutionSwapBlueprint>,
    pub(crate) serialized_transaction_payload_slot:
        Option<ExecutionSerializedTransactionPayloadSlot>,
    pub(crate) submit_enabled: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionSimulationResult {
    pub(crate) status: String,
    pub(crate) error: Option<String>,
}

pub(crate) type ExecutionSimulationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ExecutionSimulationResult>> + Send + 'a>>;

pub(crate) trait ExecutionSubmitAdapter {
    fn build_transaction_plan(
        &self,
        request: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan>;

    fn simulate_transaction_plan<'a>(
        &'a self,
        plan: &'a ExecutionTransactionPlan,
    ) -> ExecutionSimulationFuture<'a>;

    fn build_signing_envelope(
        &self,
        request: &ExecutionSubmitRequest,
        plan: &ExecutionTransactionPlan,
    ) -> Result<ExecutionSigningEnvelope> {
        if let Some(slot) = plan.serialized_transaction_payload_slot.as_ref() {
            if let Some(payload) = slot.load()? {
                if let Some(signed_payload) =
                    self.sign_serialized_transaction(request, plan, &payload)?
                {
                    return build_signed_transaction_execution_envelope(
                        request,
                        plan,
                        signed_payload,
                    );
                }
                return build_serialized_transaction_execution_envelope(request, plan, payload);
            }
        }
        build_dry_run_execution_signing_envelope(request, plan)
    }

    fn sign_serialized_transaction(
        &self,
        _request: &ExecutionSubmitRequest,
        _plan: &ExecutionTransactionPlan,
        _payload: &ExecutionSerializedTransactionPayload,
    ) -> Result<Option<ExecutionSignedTransactionPayload>> {
        Ok(None)
    }

    fn plan_submit(&self, request: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan>;

    fn plan_submit_with_envelope(
        &self,
        request: &ExecutionSubmitRequest,
        _envelope: &ExecutionSigningEnvelope,
    ) -> Result<ExecutionSubmitPlan> {
        self.plan_submit(request)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NoSubmitExecutionAdapter;

impl ExecutionSubmitAdapter for NoSubmitExecutionAdapter {
    fn build_transaction_plan(
        &self,
        request: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan> {
        Ok(ExecutionTransactionPlan {
            plan_id: format!("canary-plan:{}", request.order_id),
            order_id: request.order_id.clone(),
            signal_id: request.signal_id.clone(),
            client_order_id: request.client_order_id.clone(),
            attempt: request.attempt,
            route: request.route.clone(),
            token: request.token.clone(),
            side: request.side.clone(),
            buy_size_sol: request.buy_size_sol,
            slippage_tolerance_bps: request.slippage_tolerance_bps,
            wallet_pubkey: request.wallet_pubkey.clone(),
            metadata: request.metadata.clone(),
            swap_blueprint: None,
            serialized_transaction_payload_slot: None,
            submit_enabled: false,
        })
    }

    fn simulate_transaction_plan<'a>(
        &'a self,
        _plan: &'a ExecutionTransactionPlan,
    ) -> ExecutionSimulationFuture<'a> {
        Box::pin(async {
            Ok(ExecutionSimulationResult {
                status: EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT.to_string(),
                error: Some("no_submit_adapter_simulation_skipped".to_string()),
            })
        })
    }

    fn plan_submit(&self, request: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        Ok(ExecutionSubmitPlan::submit_disabled(
            request,
            format!(
                "no_submit_adapter:{}:{}:{}",
                request.route, request.side, request.token
            ),
        ))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JupiterMetisDryRunExecutionAdapter {
    config: ExecutionConfig,
    http: reqwest::Client,
}

impl JupiterMetisDryRunExecutionAdapter {
    pub(crate) fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }
}

impl ExecutionSubmitAdapter for JupiterMetisDryRunExecutionAdapter {
    fn build_transaction_plan(
        &self,
        request: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan> {
        let swap_blueprint = build_execution_swap_blueprint(request)?;
        Ok(ExecutionTransactionPlan {
            plan_id: format!("canary-swap-blueprint:{}", request.order_id),
            order_id: request.order_id.clone(),
            signal_id: request.signal_id.clone(),
            client_order_id: request.client_order_id.clone(),
            attempt: request.attempt,
            route: request.route.clone(),
            token: request.token.clone(),
            side: request.side.clone(),
            buy_size_sol: request.buy_size_sol,
            slippage_tolerance_bps: request.slippage_tolerance_bps,
            wallet_pubkey: request.wallet_pubkey.clone(),
            metadata: request.metadata.clone(),
            swap_blueprint: Some(swap_blueprint),
            serialized_transaction_payload_slot: Some(
                ExecutionSerializedTransactionPayloadSlot::new(),
            ),
            submit_enabled: false,
        })
    }

    fn simulate_transaction_plan<'a>(
        &'a self,
        plan: &'a ExecutionTransactionPlan,
    ) -> ExecutionSimulationFuture<'a> {
        Box::pin(async move {
            let Some(blueprint) = plan.swap_blueprint.as_ref() else {
                anyhow::bail!("missing swap blueprint for dry-run simulation");
            };
            validate_execution_swap_blueprint_for_simulation(blueprint)?;
            if plan.metadata.quote_source.as_deref() == Some(QUOTE_SOURCE_PUMP_FUN_PAID) {
                let instructions_proof =
                    match fetch_pump_fun_swap_instructions_dry_run(&self.http, &self.config, plan)
                        .await
                    {
                        Ok(proof) => proof,
                        Err(error) => Some(soft_pump_fun_swap_instructions_failure_proof(&error)),
                    };
                let transaction_dry_run =
                    fetch_pump_fun_swap_transaction_dry_run(&self.http, &self.config, plan).await?;
                if let Some(transaction) = transaction_dry_run.as_ref() {
                    if let Some(slot) = plan.serialized_transaction_payload_slot.as_ref() {
                        slot.store(ExecutionSerializedTransactionPayload {
                            source: transaction.source.clone(),
                            serialized_transaction_base64: transaction
                                .serialized_transaction_base64
                                .clone(),
                        })?;
                    }
                }
                let transaction_proof = transaction_dry_run.map(|transaction| transaction.summary);
                return Ok(ExecutionSimulationResult {
                    status: EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                    error: combined_simulation_proof(instructions_proof, transaction_proof),
                });
            }
            let instructions_proof =
                match fetch_swap_instructions_dry_run(&self.http, &self.config, plan).await {
                    Ok(proof) => proof,
                    Err(error) => {
                        let Some(proof) = soft_swap_instructions_failure_proof(&error) else {
                            return Err(error);
                        };
                        Some(proof)
                    }
                };
            let transaction_dry_run =
                fetch_swap_transaction_dry_run(&self.http, &self.config, plan).await?;
            if let Some(transaction) = transaction_dry_run.as_ref() {
                if let Some(slot) = plan.serialized_transaction_payload_slot.as_ref() {
                    slot.store(ExecutionSerializedTransactionPayload {
                        source: transaction.source.clone(),
                        serialized_transaction_base64: transaction
                            .serialized_transaction_base64
                            .clone(),
                    })?;
                }
            }
            let transaction_proof = transaction_dry_run.map(|transaction| transaction.summary);
            Ok(ExecutionSimulationResult {
                status: EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                error: combined_simulation_proof(instructions_proof, transaction_proof),
            })
        })
    }

    fn sign_serialized_transaction(
        &self,
        request: &ExecutionSubmitRequest,
        plan: &ExecutionTransactionPlan,
        payload: &ExecutionSerializedTransactionPayload,
    ) -> Result<Option<ExecutionSignedTransactionPayload>> {
        sign_serialized_transaction_from_config(&self.config, request, plan, payload)
    }

    fn plan_submit(&self, request: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        Ok(ExecutionSubmitPlan::submit_disabled(
            request,
            format!(
                "jupiter_metis_dry_run:no_submit:{}:{}:{}",
                request.route, request.side, request.token
            ),
        ))
    }

    fn plan_submit_with_envelope(
        &self,
        request: &ExecutionSubmitRequest,
        envelope: &ExecutionSigningEnvelope,
    ) -> Result<ExecutionSubmitPlan> {
        if envelope.signed_transaction_base64.is_some() {
            let intent = execution_submit_intent_from_signed_envelope(
                request,
                envelope,
                EXECUTION_SUBMIT_ROUTE_RPC_DRY_RUN.to_string(),
            )?;
            return Ok(ExecutionSubmitPlan::SubmitReady(intent));
        }
        self.plan_submit(request)
    }
}

fn soft_swap_instructions_failure_proof(error: &anyhow::Error) -> Option<String> {
    let message = error.to_string();
    if !is_missing_account_error_text(&message) {
        return None;
    }
    Some(format!(
        "metis_swap_instructions_missing_account_soft_failed error={}",
        truncate_for_log(&message, 180)
    ))
}

fn soft_pump_fun_swap_instructions_failure_proof(error: &anyhow::Error) -> String {
    format!(
        "pump_fun_swap_instructions_soft_failed error={}",
        truncate_for_log(&error.to_string(), 180)
    )
}
