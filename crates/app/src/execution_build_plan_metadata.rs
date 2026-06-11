use crate::execution_quote_provider_selection::selected_execution_build_plan_metadata;
use crate::execution_submit_adapter::{ExecutionBuildPlanMetadata, ExecutionTransactionPlan};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::SqliteStore;
use copybot_storage_core::{
    ExecutionCanaryBuildPlanMetadata, ExecutionCanaryBuildPlanMetadataRecordOutcome,
};

pub(crate) fn load_execution_build_plan_metadata(
    store: &SqliteStore,
    signal_id: &str,
) -> Result<ExecutionBuildPlanMetadata> {
    let Some(event) = store.load_latest_execution_quote_canary_entry_event(signal_id)? else {
        return Ok(ExecutionBuildPlanMetadata::default());
    };
    selected_execution_build_plan_metadata(store, event)
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
        recorded_ts: metadata_recorded_ts(now, metadata.quote_request_ts),
        quote_source: metadata.quote_source.clone(),
        quote_event_id: metadata.quote_event_id.clone(),
        quote_request_ts: metadata.quote_request_ts,
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

fn metadata_recorded_ts(
    fallback: DateTime<Utc>,
    quote_request_ts: Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    quote_request_ts
        .filter(|quote_request_ts| *quote_request_ts > fallback)
        .unwrap_or(fallback)
}
