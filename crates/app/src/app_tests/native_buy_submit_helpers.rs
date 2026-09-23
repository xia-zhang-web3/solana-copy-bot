//! Test-only adapter and recovery controls; actual route and accounting remain shared.
use crate::execution_canary_route::{
    test_process_buy, test_reconcile_existing_tiny_submit_order_inner, NativeBuyGuard,
    NativeBuyMockIo,
};
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_submit_adapter::{
    test_record_execution_rpc_confirmation_boundary_inner, ExecutionBuildPlanMetadata,
    ExecutionConfirmationBoundaryOutcome, ExecutionSubmitAdapter,
    ExecutionTinySubmitConfirmPathOutcome,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{ExecutionCanaryOrder, SqliteStore};

pub(crate) async fn process_native_buy_with_mock_quote_and_adapter<A: ExecutionSubmitAdapter>(
    config: &ExecutionConfig,
    store: &SqliteStore,
    signal: &CopySignalRow,
    now: DateTime<Utc>,
    guard: &NativeBuyGuard,
    adapter: &A,
    refreshed_quote: ExecutionBuildPlanMetadata,
) -> Result<ExecutionCanaryStateMachineSummary> {
    test_process_buy(
        config,
        store,
        signal,
        now,
        Some(guard),
        adapter,
        Some(refreshed_quote),
    )
    .await
}

pub(crate) async fn process_native_buy_receipt_recovery_for_route_with_mock(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    mock: &NativeBuyMockIo,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut summary = ExecutionCanaryStateMachineSummary::default();
    let orders = store.list_native_buy_receipt_obligations(config.canary_batch_limit.max(1))?;
    for order in orders {
        summary.existing += 1;
        summary.last_order_id = Some(order.order_id.clone());
        reconcile_existing_tiny_submit_order_with_mock(
            config,
            store,
            &order,
            now,
            &mut summary,
            mock,
        )
        .await?;
    }
    Ok(summary)
}

async fn reconcile_existing_tiny_submit_order_with_mock(
    config: &ExecutionConfig,
    store: &SqliteStore,
    order: &ExecutionCanaryOrder,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
    mock: &NativeBuyMockIo,
) -> Result<()> {
    test_reconcile_existing_tiny_submit_order_inner(config, store, order, now, summary, Some(mock))
        .await
}

pub(crate) async fn record_execution_rpc_confirmation_boundary_mock(
    store: &SqliteStore,
    http: &reqwest::Client,
    rpc_url: &str,
    order_id: &str,
    wallet_pubkey: &str,
    now: DateTime<Utc>,
    timeout_ms: u64,
    mock: &crate::execution_canary_route::NativeBuyMockIo,
) -> Result<ExecutionConfirmationBoundaryOutcome> {
    test_record_execution_rpc_confirmation_boundary_inner(
        store,
        http,
        rpc_url,
        order_id,
        wallet_pubkey,
        now,
        timeout_ms,
        Some(mock),
    )
    .await
}

pub(crate) async fn reconcile_execution_tiny_submit_confirmation_mock(
    store: &SqliteStore,
    config: &ExecutionConfig,
    order_id: &str,
    confirmation_http: &reqwest::Client,
    confirmation_rpc_url: &str,
    now: DateTime<Utc>,
    confirmation_timeout_ms: u64,
    mock: &crate::execution_canary_route::NativeBuyMockIo,
) -> Result<ExecutionTinySubmitConfirmPathOutcome> {
    crate::execution_submit_adapter::tiny_runner::reconcile_execution_tiny_submit_confirmation_inner(
        store, config, order_id, confirmation_http, confirmation_rpc_url,
        now, confirmation_timeout_ms, Some(mock),
    ).await
}
