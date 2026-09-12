use super::{
    cap_execution_priority_fee_lamports, record_execution_rpc_confirmation_boundary,
    ExecutionConfirmationBoundaryOutcome, ExecutionSubmitAdapter, ExecutionSubmitRequest,
    RpcExecutionSubmitTransport,
};
use crate::execution_canary_submit_contract::{
    record_execution_tiny_submit_plan, ExecutionSubmitPlanOutcome, ExecutionTinySubmitGate,
};
use crate::execution_signing_envelope::ExecutionSigningEnvelope;
use crate::execution_tiny_entry_route::load_entry_route_plan_json_for_sell;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionCanaryBuildPlanMetadata, SqliteStore, EXECUTION_STATUS_CANARY_CONFIRMED,
    EXECUTION_STATUS_CANARY_SUBMITTED,
};

const SUBMITTED_WITHOUT_TX_SIGNATURE_REASON: &str = "submitted_without_tx_signature";
const RECONCILE_ALREADY_CONFIRMED_REASON: &str = "submitted_reconcile_already_confirmed";
const RECONCILE_NOT_SUBMITTED_REASON: &str = "submitted_reconcile_not_submitted";

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionTinySubmitConfirmPathOutcome {
    pub(crate) pre_submit_refusal: Option<Box<crate::execution_submit_refusal::PreSubmitRefusal>>,
    pub(crate) submit_failed: usize,
    pub(crate) confirmation_failed: usize,
    pub(crate) submitted: usize,
    pub(crate) submit_disabled: usize,
    pub(crate) submit_ready_rejected: usize,
    pub(crate) confirmation_confirmed: usize,
    pub(crate) confirmation_pending: usize,
    pub(crate) buy_opened: usize,
    pub(crate) buy_existing: usize,
    pub(crate) sell_closed: usize,
    pub(crate) sell_partial: usize,
    pub(crate) sell_dust_closed: usize,
    pub(crate) sell_no_position: usize,
    pub(crate) cash_settlement: Option<copybot_storage_core::ExecutionCanaryCashSettlement>,
    pub(crate) tx_signature: Option<String>,
    pub(crate) reason: Option<String>,
    pub(crate) error: Option<String>,
}

pub(crate) async fn record_execution_tiny_submit_confirm_path<A: ExecutionSubmitAdapter>(
    store: &SqliteStore,
    adapter: &A,
    request: &ExecutionSubmitRequest,
    envelope: &ExecutionSigningEnvelope,
    gate: &ExecutionTinySubmitGate,
    transport: &RpcExecutionSubmitTransport,
    confirmation_http: &reqwest::Client,
    confirmation_rpc_url: &str,
    now: DateTime<Utc>,
    confirmation_timeout_ms: u64,
) -> Result<ExecutionTinySubmitConfirmPathOutcome> {
    let submit =
        record_execution_tiny_submit_plan(store, adapter, request, envelope, gate, transport, now)
            .await?;
    let mut outcome = outcome_from_submit(&submit);
    if submit.submitted == 0 {
        return Ok(outcome);
    }
    if submit.tx_signature.is_none() {
        outcome.confirmation_pending = 1;
        outcome.reason = Some(SUBMITTED_WITHOUT_TX_SIGNATURE_REASON.to_string());
        return Ok(outcome);
    }
    let confirmation = record_execution_rpc_confirmation_boundary(
        store,
        confirmation_http,
        confirmation_rpc_url,
        &request.order_id,
        &request.wallet_pubkey,
        now,
        confirmation_timeout_ms,
    )
    .await?;
    apply_confirmation(&mut outcome, confirmation);
    Ok(outcome)
}

pub(crate) async fn reconcile_execution_tiny_submit_confirmation(
    store: &SqliteStore,
    config: &ExecutionConfig,
    order_id: &str,
    confirmation_http: &reqwest::Client,
    confirmation_rpc_url: &str,
    now: DateTime<Utc>,
    confirmation_timeout_ms: u64,
) -> Result<ExecutionTinySubmitConfirmPathOutcome> {
    store.visit_execution_canary_reconciliation(order_id, &config.canary_wallet_pubkey, now)?;
    let Some(order) = store.load_execution_canary_order(order_id)? else {
        anyhow::bail!("missing execution canary order {order_id}");
    };
    if order.status == EXECUTION_STATUS_CANARY_CONFIRMED {
        if store.execution_canary_fill_exists(order_id)? {
            store.validate_execution_canary_cash_settlement_replay(
                order_id,
                &config.canary_wallet_pubkey,
            )?;
            return Ok(ExecutionTinySubmitConfirmPathOutcome {
                confirmation_confirmed: 1,
                cash_settlement: store.load_execution_canary_cash_settlement(order_id)?,
                tx_signature: order.tx_signature,
                reason: Some(RECONCILE_ALREADY_CONFIRMED_REASON.into()),
                ..Default::default()
            });
        }
    }
    if !matches!(
        order.status.as_str(),
        EXECUTION_STATUS_CANARY_SUBMITTED
            | EXECUTION_STATUS_CANARY_CONFIRMED
            | copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
    ) {
        return Ok(ExecutionTinySubmitConfirmPathOutcome {
            reason: Some(RECONCILE_NOT_SUBMITTED_REASON.to_string()),
            ..ExecutionTinySubmitConfirmPathOutcome::default()
        });
    }
    if order
        .tx_signature
        .as_deref()
        .is_none_or(|signature| signature.trim().is_empty())
    {
        return Ok(ExecutionTinySubmitConfirmPathOutcome {
            submitted: 1,
            confirmation_pending: 1,
            reason: Some(SUBMITTED_WITHOUT_TX_SIGNATURE_REASON.to_string()),
            ..ExecutionTinySubmitConfirmPathOutcome::default()
        });
    }

    let confirmation = record_execution_rpc_confirmation_boundary(
        store,
        confirmation_http,
        confirmation_rpc_url,
        order_id,
        &config.canary_wallet_pubkey,
        now,
        confirmation_timeout_ms,
    )
    .await?;
    let mut outcome = ExecutionTinySubmitConfirmPathOutcome {
        submitted: 1,
        tx_signature: order.tx_signature,
        ..ExecutionTinySubmitConfirmPathOutcome::default()
    };
    apply_confirmation(&mut outcome, confirmation);
    Ok(outcome)
}

pub(crate) fn build_tiny_submit_reconciliation_request(
    store: &SqliteStore,
    config: &ExecutionConfig,
    order: &copybot_storage_core::ExecutionCanaryOrder,
) -> Result<ExecutionSubmitRequest> {
    let signal = store
        .load_copy_signal_by_signal_id(&order.signal_id)?
        .ok_or_else(|| anyhow::anyhow!("missing copy signal for {}", order.order_id))?;
    let metadata = store
        .load_execution_canary_build_plan_metadata(&order.order_id)?
        .ok_or_else(|| anyhow::anyhow!("missing build metadata for {}", order.order_id))?;
    let mut metadata =
        cap_execution_priority_fee_lamports(config, build_plan_metadata_from_storage(metadata));
    // This constructor is used by unsigned retry. Known signatures reconcile above without it.
    if signal.side.eq_ignore_ascii_case("sell") {
        metadata.owned_sell_amount =
            crate::execution_source_sell_guard::amount::decode(store, &order.order_id)?;
    }
    let entry_route_plan_json =
        load_entry_route_plan_json_for_sell(store, &signal.token, signal.side.as_str())?;
    Ok(ExecutionSubmitRequest {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        attempt: order.attempt,
        route: order.route.clone(),
        wallet_id: signal.wallet_id,
        token: signal.token,
        side: signal.side.clone(),
        buy_size_sol: config.canary_buy_size_sol,
        slippage_tolerance_bps:
            crate::execution_quote_canary_helpers::quote_canary_slippage_limit_bps(
                config,
                &signal.side,
            ),
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        entry_route_plan_json,
        metadata,
    })
}

fn build_plan_metadata_from_storage(
    metadata: ExecutionCanaryBuildPlanMetadata,
) -> super::ExecutionBuildPlanMetadata {
    super::ExecutionBuildPlanMetadata {
        owned_sell_amount: None,
        protected_capital: None,
        http_request_started_ts: metadata.http_request_started_ts,
        quote_response_available_ts: metadata.quote_response_available_ts,
        quote_source: metadata.quote_source,
        quote_event_id: metadata.quote_event_id,
        quote_request_ts: metadata.quote_request_ts,
        quote_status: metadata.quote_status,
        quote_in_amount_raw: metadata.quote_in_amount_raw,
        quote_out_amount_raw: metadata.quote_out_amount_raw,
        quote_response_json: metadata.quote_response_json,
        quote_price_sol: metadata.quote_price_sol,
        price_impact_pct: metadata.price_impact_pct,
        route_plan_json: metadata.route_plan_json,
        priority_fee_source: metadata.priority_fee_source,
        priority_fee_status: metadata.priority_fee_status,
        priority_fee_lamports: metadata.priority_fee_lamports,
        priority_fee_json: metadata.priority_fee_json,
        slippage_bps: metadata.slippage_bps,
        decision_status: metadata.decision_status,
        decision_reason: metadata.decision_reason,
    }
}

fn outcome_from_submit(
    submit: &ExecutionSubmitPlanOutcome,
) -> ExecutionTinySubmitConfirmPathOutcome {
    ExecutionTinySubmitConfirmPathOutcome {
        pre_submit_refusal: submit.pre_submit_refusal.clone(),
        submit_failed: submit.failed,
        submitted: submit.submitted,
        submit_disabled: submit.submit_disabled,
        submit_ready_rejected: submit.submit_ready_rejected,
        tx_signature: submit.tx_signature.clone(),
        reason: submit.reason.clone(),
        error: submit.error.clone(),
        ..ExecutionTinySubmitConfirmPathOutcome::default()
    }
}

fn apply_confirmation(
    outcome: &mut ExecutionTinySubmitConfirmPathOutcome,
    confirmation: ExecutionConfirmationBoundaryOutcome,
) {
    outcome.confirmation_failed = confirmation.failed;
    outcome.confirmation_confirmed = confirmation.confirmed;
    outcome.confirmation_pending = confirmation.pending;
    outcome.buy_opened = confirmation.buy_opened;
    outcome.buy_existing = confirmation.buy_existing;
    outcome.sell_closed = confirmation.sell_closed;
    outcome.sell_partial = confirmation.sell_partial;
    outcome.sell_dust_closed = confirmation.sell_dust_closed;
    outcome.sell_no_position = confirmation.sell_no_position;
    outcome.cash_settlement = confirmation.cash_settlement;
    outcome.reason = confirmation.reason;
    outcome.error = confirmation.error;
}
