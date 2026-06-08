use super::{
    cap_execution_priority_fee_lamports, record_execution_rpc_confirmation_boundary,
    ExecutionConfirmationBoundaryOutcome, ExecutionConfirmedBuyFill, ExecutionConfirmedFill,
    ExecutionConfirmedSellFill, ExecutionSubmitAdapter, ExecutionSubmitRequest,
    RpcExecutionSubmitTransport,
};
use crate::execution_canary_submit_contract::{
    record_execution_tiny_submit_plan, ExecutionSubmitPlanOutcome, ExecutionTinySubmitGate,
};
use crate::execution_signing_envelope::ExecutionSigningEnvelope;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::TokenQuantity;
use copybot_storage_core::{
    ExecutionCanaryBuildPlanMetadata, SqliteStore, EXECUTION_STATUS_CANARY_CONFIRMED,
    EXECUTION_STATUS_CANARY_SUBMITTED,
};
use serde_json::Value;

const DEFAULT_TINY_SELL_DUST_QTY_EPSILON: f64 = 1e-9;
const SUBMITTED_WITHOUT_TX_SIGNATURE_REASON: &str = "submitted_without_tx_signature";
const RECONCILE_ALREADY_CONFIRMED_REASON: &str = "submitted_reconcile_already_confirmed";
const RECONCILE_NOT_SUBMITTED_REASON: &str = "submitted_reconcile_not_submitted";

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionTinySubmitConfirmPathOutcome {
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
    let fill = build_execution_confirmed_fill_from_request(store, request, now)?;
    let confirmation = record_execution_rpc_confirmation_boundary(
        store,
        confirmation_http,
        confirmation_rpc_url,
        &request.order_id,
        fill,
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
    let Some(order) = store.load_execution_canary_order(order_id)? else {
        anyhow::bail!("missing execution canary order {order_id}");
    };
    if order.status == EXECUTION_STATUS_CANARY_CONFIRMED {
        return Ok(ExecutionTinySubmitConfirmPathOutcome {
            confirmation_confirmed: 1,
            reason: Some(RECONCILE_ALREADY_CONFIRMED_REASON.to_string()),
            ..ExecutionTinySubmitConfirmPathOutcome::default()
        });
    }
    if order.status != EXECUTION_STATUS_CANARY_SUBMITTED {
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

    let request = build_tiny_submit_reconciliation_request(store, config, &order)?;
    let fill = build_execution_confirmed_fill_from_request(store, &request, now)?;
    let confirmation = record_execution_rpc_confirmation_boundary(
        store,
        confirmation_http,
        confirmation_rpc_url,
        order_id,
        fill,
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

pub(crate) fn build_execution_confirmed_fill_from_request(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    fill_ts: DateTime<Utc>,
) -> Result<ExecutionConfirmedFill> {
    if request.side.eq_ignore_ascii_case("buy") {
        return build_buy_fill_from_request(request, fill_ts);
    }
    if request.side.eq_ignore_ascii_case("sell") {
        return build_sell_fill_from_request(store, request, fill_ts);
    }
    anyhow::bail!("unsupported tiny execution fill side: {}", request.side)
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
    let metadata =
        cap_execution_priority_fee_lamports(config, build_plan_metadata_from_storage(metadata));
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
        metadata,
    })
}

fn build_plan_metadata_from_storage(
    metadata: ExecutionCanaryBuildPlanMetadata,
) -> super::ExecutionBuildPlanMetadata {
    super::ExecutionBuildPlanMetadata {
        quote_source: metadata.quote_source,
        quote_event_id: metadata.quote_event_id,
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

fn build_buy_fill_from_request(
    request: &ExecutionSubmitRequest,
    fill_ts: DateTime<Utc>,
) -> Result<ExecutionConfirmedFill> {
    let price_sol = positive_quote_price(request)?;
    let qty_exact = quote_exact_output_quantity(request)?;
    let qty = qty_exact
        .map(TokenQuantity::as_f64)
        .or_else(|| quote_output_ui_amount(request))
        .unwrap_or(request.buy_size_sol / price_sol);
    validate_positive(qty, "buy fill qty")?;
    validate_positive(request.buy_size_sol, "buy fill cost_sol")?;
    Ok(ExecutionConfirmedFill::Buy(ExecutionConfirmedBuyFill {
        order_id: request.order_id.clone(),
        token: request.token.clone(),
        qty,
        qty_exact,
        cost_sol: request.buy_size_sol,
        fill_ts,
    }))
}

fn build_sell_fill_from_request(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    fill_ts: DateTime<Utc>,
) -> Result<ExecutionConfirmedFill> {
    let position = store
        .load_execution_canary_open_position(&request.token)?
        .ok_or_else(|| anyhow::anyhow!("missing confirmed open position for {}", request.token))?;
    let exit_price_sol = positive_quote_price(request)?;
    validate_positive(position.qty, "sell fill target_qty")?;
    Ok(ExecutionConfirmedFill::Sell(ExecutionConfirmedSellFill {
        order_id: request.order_id.clone(),
        token: request.token.clone(),
        target_qty: position.qty,
        target_qty_exact: position.qty_exact,
        exit_price_sol,
        dust_qty_epsilon: DEFAULT_TINY_SELL_DUST_QTY_EPSILON,
        fill_ts,
    }))
}

fn outcome_from_submit(
    submit: &ExecutionSubmitPlanOutcome,
) -> ExecutionTinySubmitConfirmPathOutcome {
    ExecutionTinySubmitConfirmPathOutcome {
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
    outcome.reason = confirmation.reason;
    outcome.error = confirmation.error;
}

fn positive_quote_price(request: &ExecutionSubmitRequest) -> Result<f64> {
    let price = request
        .metadata
        .quote_price_sol
        .ok_or_else(|| anyhow::anyhow!("missing quote_price_sol for {}", request.order_id))?;
    validate_positive(price, "quote_price_sol")?;
    Ok(price)
}

fn quote_exact_output_quantity(request: &ExecutionSubmitRequest) -> Result<Option<TokenQuantity>> {
    let Some(raw) = request.metadata.quote_out_amount_raw.as_deref() else {
        return Ok(None);
    };
    let Some(decimals) = quote_output_decimals(request) else {
        return Ok(None);
    };
    let raw = raw
        .parse::<u64>()
        .with_context(|| format!("invalid quote_out_amount_raw for {}", request.order_id))?;
    Ok(Some(TokenQuantity::new(raw, decimals)))
}

fn quote_output_decimals(request: &ExecutionSubmitRequest) -> Option<u8> {
    let json = quote_response_json(request)?;
    decimal_pointer(&json, "/quote/meta/outDecimals")
        .or_else(|| decimal_pointer(&json, "/meta/outDecimals"))
        .or_else(|| decimal_pointer(&json, "/quote/outDecimals"))
        .or_else(|| decimal_pointer(&json, "/outDecimals"))
}

fn quote_output_ui_amount(request: &ExecutionSubmitRequest) -> Option<f64> {
    let json = quote_response_json(request)?;
    number_pointer(&json, "/quote/outAmountUi").or_else(|| number_pointer(&json, "/outAmountUi"))
}

fn quote_response_json(request: &ExecutionSubmitRequest) -> Option<Value> {
    serde_json::from_str(request.metadata.quote_response_json.as_deref()?).ok()
}

fn decimal_pointer(value: &Value, path: &str) -> Option<u8> {
    value
        .pointer(path)
        .and_then(Value::as_u64)
        .and_then(|raw| u8::try_from(raw).ok())
}

fn number_pointer(value: &Value, path: &str) -> Option<f64> {
    value
        .pointer(path)
        .and_then(Value::as_f64)
        .filter(|amount| amount.is_finite() && *amount > 0.0)
}

fn validate_positive(value: f64, label: &str) -> Result<()> {
    if !value.is_finite() || value <= 0.0 {
        anyhow::bail!("{label} must be positive, got {value}");
    }
    Ok(())
}
