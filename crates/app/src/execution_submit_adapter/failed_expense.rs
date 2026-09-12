use super::{confirmation_boundary::ExecutionConfirmationBoundaryOutcome, rpc_failed_expense};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{FailedExpenseTask, SqliteStore};
use serde_json::Value;

pub(super) async fn detected(
    store: &SqliteStore,
    http: &reqwest::Client,
    url: &str,
    id: &str,
    wallet: &str,
    source: &str,
    commitment: &str,
    slot: Option<u64>,
    error: &Value,
    now: DateTime<Utc>,
    timeout_ms: u64,
    receipt: Option<&Value>,
) -> Result<ExecutionConfirmationBoundaryOutcome> {
    let task = store.detect_failed_expense(id, wallet, source, commitment, slot, error, now)?;
    recover(store, http, url, wallet, task, now, timeout_ms, receipt).await?;
    let task = store
        .load_failed_expense_task(id)?
        .expect("durable failed task");
    let terminal = store
        .load_execution_canary_order(id)?
        .is_some_and(|o| o.status == copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED);
    Ok(ExecutionConfirmationBoundaryOutcome {
        failed: usize::from(terminal),
        pending: usize::from(!terminal),
        reason: Some(if terminal && source == "signature_status" {
            copybot_storage_core::EXECUTION_ERROR_CONFIRMATION_FAILED.into()
        } else if terminal {
            "receipt_transaction_failed".into()
        } else {
            task.reason
        }),
        error: (terminal && source == "signature_status")
            .then(|| "confirmation RPC transaction_error".into()),
        ..Default::default()
    })
}
async fn recover(
    store: &SqliteStore,
    http: &reqwest::Client,
    url: &str,
    wallet: &str,
    task: FailedExpenseTask,
    now: DateTime<Utc>,
    timeout_ms: u64,
    receipt: Option<&Value>,
) -> Result<()> {
    if task.status == "conflict" {
        return Ok(());
    }
    if task.wallet != wallet {
        return store
            .reject_failed_expense(&task.order_id, "failed_expense_configured_wallet_conflict");
    }
    if task.status == "complete" {
        return Ok(());
    }
    let fetched;
    let result = if let Some(value) = receipt {
        Ok(value)
    } else {
        fetched = rpc_failed_expense::fetch(http, url, &task.tx_signature, timeout_ms).await;
        fetched.as_ref().map_err(|e| anyhow::anyhow!(e.to_string()))
    }
    .and_then(|value| rpc_failed_expense::parse(&task, value));
    let facts = match result {
        Ok(facts) => facts,
        Err(error) => {
            let reason = error.to_string();
            if reason.ends_with("_conflict") {
                store.reject_failed_expense(&task.order_id, &reason)?;
            } else {
                store.defer_failed_expense(&task.order_id, &reason)?;
            }
            return Ok(());
        }
    };
    if let Err(error) = store.apply_failed_expense(&task.order_id, &facts, now) {
        if copybot_storage_core::is_fatal_sqlite_anyhow_error(&error)
            || !matches!(error.downcast_ref::<rusqlite::Error>(),Some(rusqlite::Error::SqliteFailure(code,_)) if code.code==rusqlite::ErrorCode::ConstraintViolation)
        {
            return Err(error);
        }
        // Cursor already advanced; successful new transaction + readback preserves retry.
        store.defer_failed_expense(&task.order_id, "failed_expense_write_rejected")?;
        crate::telemetry::record_failed_expense_write_failure(&task.order_id);
    }
    Ok(())
}
pub(crate) async fn sweep(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<()> {
    let tasks = store.take_failed_expense_tasks(
        &config.canary_route,
        config.canary_batch_limit.max(1),
        now,
    )?;
    let http = reqwest::Client::new();
    for task in tasks {
        recover(
            store,
            &http,
            &config.submit_adapter_http_url,
            &config.canary_wallet_pubkey,
            task,
            now,
            config.max_confirm_seconds.saturating_mul(1000).max(1),
            None,
        )
        .await?;
    }
    Ok(())
}
