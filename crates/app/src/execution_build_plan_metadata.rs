use crate::execution_submit_adapter::{ExecutionBuildPlanMetadata, ExecutionTransactionPlan};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    ExecutionCanaryBuildPlanMetadata, ExecutionCanaryBuildPlanMetadataRecordOutcome,
};
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};

pub(crate) fn load_execution_build_plan_metadata(
    store: &SqliteStore,
    signal_id: &str,
) -> Result<ExecutionBuildPlanMetadata> {
    let Some(event) = store.load_latest_execution_quote_canary_entry_event(signal_id)? else {
        return Ok(ExecutionBuildPlanMetadata::default());
    };
    Ok(metadata_from_quote_event(event))
}

pub(crate) fn record_execution_build_plan_metadata(
    store: &SqliteStore,
    plan: &ExecutionTransactionPlan,
    now: DateTime<Utc>,
) -> Result<ExecutionCanaryBuildPlanMetadataRecordOutcome> {
    let metadata = &plan.metadata;
    store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
        order_id: plan.order_id.clone(),
        signal_id: plan.signal_id.clone(),
        client_order_id: plan.client_order_id.clone(),
        recorded_ts: now,
        quote_source: metadata.quote_source.clone(),
        quote_event_id: metadata.quote_event_id.clone(),
        quote_status: metadata.quote_status.clone(),
        quote_in_amount_raw: metadata.quote_in_amount_raw.clone(),
        quote_out_amount_raw: metadata.quote_out_amount_raw.clone(),
        quote_response_json: metadata.quote_response_json.clone(),
        quote_price_sol: metadata.quote_price_sol,
        price_impact_pct: metadata.price_impact_pct,
        route_plan_json: metadata.route_plan_json.clone(),
        priority_fee_source: metadata.priority_fee_source.clone(),
        priority_fee_status: metadata.priority_fee_status.clone(),
        priority_fee_lamports: metadata.priority_fee_lamports,
        priority_fee_json: metadata.priority_fee_json.clone(),
        slippage_bps: metadata.slippage_bps,
        decision_status: metadata.decision_status.clone(),
        decision_reason: metadata.decision_reason.clone(),
    })
}

fn metadata_from_quote_event(event: ExecutionQuoteCanaryEventInsert) -> ExecutionBuildPlanMetadata {
    let priority_fee_source = if event.priority_fee_status.is_some()
        || event.priority_fee_lamports.is_some()
        || event.priority_fee_json.is_some()
    {
        Some("execution_quote_canary_event".to_string())
    } else {
        None
    };
    ExecutionBuildPlanMetadata {
        quote_source: Some("execution_quote_canary_event".to_string()),
        quote_event_id: Some(event.event_id),
        quote_status: Some(event.quote_status),
        quote_in_amount_raw: event.quote_in_amount_raw,
        quote_out_amount_raw: event.quote_out_amount_raw,
        quote_response_json: event.quote_response_json,
        quote_price_sol: event.quote_price_sol,
        price_impact_pct: event.price_impact_pct,
        route_plan_json: event.route_plan_json,
        priority_fee_source,
        priority_fee_status: event.priority_fee_status,
        priority_fee_lamports: event.priority_fee_lamports,
        priority_fee_json: event.priority_fee_json,
        slippage_bps: event.slippage_bps,
        decision_status: event.decision_status,
        decision_reason: event.decision_reason,
    }
}
