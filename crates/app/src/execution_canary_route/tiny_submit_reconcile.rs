//! Receipt-only native BUY recovery and existing tiny order reconciliation.
#[cfg(test)]
pub(crate) use crate::app_tests::native_buy_submit_helpers::process_native_buy_receipt_recovery_for_route_with_mock;
use super::tiny_submit::apply_tiny_submit_confirm_path_outcome;
use super::tiny_submit_retry::{is_tiny_submit_retry_ready, retry_existing_simulated_tiny_submit_order};
use super::tiny_submit_timeout::process_tiny_submit_timeout;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{ExecutionCanaryOrder, SqliteStore,
    EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_SUBMITTED};

/// Strict durable mode resumes only previously dispatched native BUY obligations.
/// This path never selects a candidate for quote, reserve, retry or send.
pub(crate) async fn process_native_buy_receipt_recovery_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut summary = ExecutionCanaryStateMachineSummary::default();
    let orders = store.list_native_buy_receipt_obligations(
        config.canary_batch_limit.max(1),
    )?;
    for order in orders {
        summary.existing += 1;
        summary.last_order_id = Some(order.order_id.clone());
        reconcile_existing_tiny_submit_order(config, store, &order, now, &mut summary).await?;
    }
    Ok(summary)
}


pub(super) async fn reconcile_existing_tiny_submit_order(
    config: &ExecutionConfig,
    store: &SqliteStore,
    order: &ExecutionCanaryOrder,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    reconcile_existing_tiny_submit_order_inner(
        config, store, order, now, summary, #[cfg(test)] None,
    ).await
}


pub(crate) async fn reconcile_existing_tiny_submit_order_inner(
    config: &ExecutionConfig, store: &SqliteStore, order: &ExecutionCanaryOrder,
    now: DateTime<Utc>, summary: &mut ExecutionCanaryStateMachineSummary,
    #[cfg(test)] mock: Option<&super::NativeBuyMockIo>,
) -> Result<()> {
    // Native source decisions are one use. Reconcile an already dispatched
    // obligation, but never rearm a candidate, simulation failure or unknown send.
    if order.signal_id.starts_with("native-buy-v1:")
        || store
            .load_copy_signal_by_signal_id(&order.signal_id)?
            .is_some_and(|signal| signal.status == "native_buy_fenced_v1")
    {
        if let Some(dispatch) = store.load_execution_canary_dispatch(&order.order_id)? {
            let signal = store.load_copy_signal_by_signal_id(&order.signal_id)?;
            anyhow::ensure!(dispatch.order_id == order.order_id
                && dispatch.signal_id == order.signal_id
                && dispatch.route == order.route
                && dispatch.side == "buy"
                && order.tx_signature.as_deref().is_none_or(|s| s == dispatch.tx_signature)
                && !dispatch.wallet.trim().is_empty()
                && signal.as_ref().is_some_and(|s| s.token == dispatch.token && s.side == "buy"),
                "native_buy_dispatch_provenance_mismatch");
            let mut recovery_config = config.clone();
            recovery_config.canary_wallet_pubkey = dispatch.wallet;
            let confirmation_timeout_ms = config.max_confirm_seconds.saturating_mul(1_000).max(1);
            #[cfg(test)]
            let outcome = if let Some(mock) = mock {
                crate::execution_submit_adapter::tiny_runner::reconcile_execution_tiny_submit_confirmation_mock(
                    store, &recovery_config, &order.order_id, &reqwest::Client::new(),
                    &config.submit_adapter_http_url, now, confirmation_timeout_ms, mock,
                ).await?
            } else {
                reconcile_execution_tiny_submit_confirmation(store, &recovery_config,
                    &order.order_id, &reqwest::Client::new(), &config.submit_adapter_http_url,
                    now, confirmation_timeout_ms).await?
            };
            #[cfg(not(test))]
            let outcome = reconcile_execution_tiny_submit_confirmation(
                store,
                &recovery_config,
                &order.order_id,
                &reqwest::Client::new(),
                &config.submit_adapter_http_url,
                now,
                confirmation_timeout_ms,
            )
            .await?;
            apply_tiny_submit_confirm_path_outcome(summary, outcome);
        }
        return Ok(());
    }
    let restored;
    if matches!(
        order.status.as_str(),
        copybot_storage_core::EXECUTION_STATUS_CANARY_EXPIRED
            | copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    ) || (order.status == copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED
        && order
            .simulation_error
            .as_deref()
            .is_some_and(|s| s.starts_with("retry_after_unknown_submit_timeout")))
    {
        store.visit_execution_canary_reconciliation(
            &order.order_id,
            &config.canary_wallet_pubkey,
            now,
        )?;
        restored = store
            .load_execution_canary_order(&order.order_id)?
            .expect("visited order");
    } else {
        restored = order.clone();
    }
    let order = &restored;
    if !matches!(
        order.status.as_str(),
        EXECUTION_STATUS_CANARY_SUBMITTED
            | EXECUTION_STATUS_CANARY_CONFIRMED
            | copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
    ) {
        if is_tiny_submit_retry_ready(order) {
            retry_existing_simulated_tiny_submit_order(config, store, order, now, summary).await?;
        }
        return Ok(());
    }
    store.visit_execution_canary_reconciliation(
        &order.order_id,
        &config.canary_wallet_pubkey,
        now,
    )?;
    let confirmation_timeout_ms = config.max_confirm_seconds.saturating_mul(1_000).max(1);
    if order
        .tx_signature
        .as_deref()
        .is_none_or(|signature| signature.trim().is_empty())
    {
        process_tiny_submit_timeout(
            store,
            &order.order_id,
            confirmation_timeout_ms,
            config.max_submit_attempts,
            now,
            summary,
        )?;
        return Ok(());
    }
    let outcome = reconcile_execution_tiny_submit_confirmation(
        store,
        config,
        &order.order_id,
        &reqwest::Client::new(),
        &config.submit_adapter_http_url,
        now,
        confirmation_timeout_ms,
    )
    .await?;
    let confirmation_pending = outcome.confirmation_pending;
    apply_tiny_submit_confirm_path_outcome(summary, outcome);
    if confirmation_pending > 0 {
        process_tiny_submit_timeout(
            store,
            &order.order_id,
            confirmation_timeout_ms,
            config.max_submit_attempts,
            now,
            summary,
        )?;
    }
    Ok(())
}
