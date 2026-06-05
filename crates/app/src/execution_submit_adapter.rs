use crate::execution_swap_blueprint::{
    build_execution_swap_blueprint, validate_execution_swap_blueprint_for_simulation,
    ExecutionSwapBlueprint,
};
use crate::execution_swap_instructions_http::fetch_swap_instructions_dry_run;
use anyhow::Result;
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    EXECUTION_SIMULATION_STATUS_PASSED, EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT,
};
use std::future::Future;
use std::pin::Pin;

pub(crate) const CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN: &str =
    "metis-swap-instructions-dry-run";

pub(crate) fn uses_jupiter_metis_dry_run_adapter(route: &str) -> bool {
    route
        .trim()
        .eq_ignore_ascii_case(CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionBuildPlanMetadata {
    pub(crate) quote_source: Option<String>,
    pub(crate) quote_event_id: Option<String>,
    pub(crate) quote_status: Option<String>,
    pub(crate) quote_in_amount_raw: Option<String>,
    pub(crate) quote_out_amount_raw: Option<String>,
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
    pub(crate) route: String,
    pub(crate) wallet_id: String,
    pub(crate) token: String,
    pub(crate) side: String,
    pub(crate) buy_size_sol: f64,
    pub(crate) slippage_tolerance_bps: u64,
    pub(crate) wallet_pubkey: String,
    pub(crate) metadata: ExecutionBuildPlanMetadata,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionTransactionPlan {
    pub(crate) plan_id: String,
    pub(crate) order_id: String,
    pub(crate) signal_id: String,
    pub(crate) client_order_id: String,
    pub(crate) route: String,
    pub(crate) token: String,
    pub(crate) side: String,
    pub(crate) buy_size_sol: f64,
    pub(crate) slippage_tolerance_bps: u64,
    pub(crate) wallet_pubkey: String,
    pub(crate) metadata: ExecutionBuildPlanMetadata,
    pub(crate) swap_blueprint: Option<ExecutionSwapBlueprint>,
    pub(crate) submit_enabled: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionSimulationResult {
    pub(crate) status: String,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ExecutionSubmitPlan {
    SubmitDisabled { reason: String },
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

    fn plan_submit(&self, request: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan>;
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
            route: request.route.clone(),
            token: request.token.clone(),
            side: request.side.clone(),
            buy_size_sol: request.buy_size_sol,
            slippage_tolerance_bps: request.slippage_tolerance_bps,
            wallet_pubkey: request.wallet_pubkey.clone(),
            metadata: request.metadata.clone(),
            swap_blueprint: None,
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
        Ok(ExecutionSubmitPlan::SubmitDisabled {
            reason: format!(
                "no_submit_adapter:{}:{}:{}",
                request.route, request.side, request.token
            ),
        })
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
            route: request.route.clone(),
            token: request.token.clone(),
            side: request.side.clone(),
            buy_size_sol: request.buy_size_sol,
            slippage_tolerance_bps: request.slippage_tolerance_bps,
            wallet_pubkey: request.wallet_pubkey.clone(),
            metadata: request.metadata.clone(),
            swap_blueprint: Some(swap_blueprint),
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
            let proof = fetch_swap_instructions_dry_run(&self.http, &self.config, plan).await?;
            Ok(ExecutionSimulationResult {
                status: EXECUTION_SIMULATION_STATUS_PASSED.to_string(),
                error: proof,
            })
        })
    }

    fn plan_submit(&self, request: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        Ok(ExecutionSubmitPlan::SubmitDisabled {
            reason: format!(
                "jupiter_metis_dry_run:no_submit:{}:{}:{}",
                request.route, request.side, request.token
            ),
        })
    }
}
