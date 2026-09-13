//! Continues the actual strict runner into the existing signer/dispatch/receipt engines.
#[path = "execution_owned_sell_guard.rs"]
pub(crate) mod guard;
#[path = "execution_owned_sell_recovery.rs"]
pub(crate) mod recovery;
use crate::execution_submit_adapter::*;
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::{rpc_owned_sell_handoff::dispatch::Prepared, SqliteStore};

pub(crate) async fn run(
    store: &SqliteStore,
    c: &ExecutionConfig,
    mut r: ExecutionSubmitRequest,
    p: Prepared,
    authority: &crate::execution_owned_sell_rpc::Authority,
    live: guard::Live,
) -> Result<()> {
    guard::config(c, &p)?;
    authority.binding(c, &p.handoff.snapshot)?;
    r.order_id = p.order_id();
    r.metadata.rpc_owned_live = Some(live);
    r.metadata.rpc_owned_sell = Some(Box::new(p.clone()));
    let adapter = JupiterMetisDryRunExecutionAdapter::new(c.clone());
    let plan = adapter.build_transaction_plan(&r)?;
    plan.serialized_transaction_payload_slot
        .as_ref()
        .context("owned_sell_payload_slot")?
        .store(
            crate::execution_signing_envelope::ExecutionSerializedTransactionPayload {
                source: "rpc_owned_sell_handoff".into(),
                serialized_transaction_base64: p.payload.clone(),
            },
        )?;
    guard::blockhash(c, store, &r, &p.payload, p.handoff.snapshot.sell.facts.slot).await?;
    // No further await before the existing key loader and signing boundary.
    guard::config(c, &p)?;
    ensure!(
        guard::request(store, &r)? == p,
        "owned_sell_prepared_changed"
    );
    authority.binding(c, &p.handoff.snapshot)?;
    let envelope = adapter.build_signing_envelope(&r, &plan)?;
    crate::execution_signing_envelope::validate_execution_signing_envelope(&envelope, &r, &plan)?;
    guard::config(c, &p)?;
    ensure!(
        guard::request(store, &r)? == p,
        "owned_sell_prepared_changed"
    );
    let signed = envelope
        .signed_transaction_base64
        .as_deref()
        .context("owned_sell_signed_payload_missing")?;
    guard::payload(&p, signed)?;
    let url = crate::execution_owned_sell_rpc::endpoint(c)?.to_string();
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    let transport =
        RpcExecutionSubmitTransport::with_client(http.clone(), url.clone(), Default::default());
    let result = record_execution_tiny_submit_confirm_path(
        store,
        &adapter,
        &r,
        &envelope,
        &crate::execution_canary_submit_contract::ExecutionTinySubmitGate::from_config(c),
        &transport,
        &http,
        &url,
        Utc::now(),
        c.max_confirm_seconds.saturating_mul(1000).clamp(1, 30_000),
    )
    .await?;
    ensure!(
        result.submitted == 1,
        "{}",
        result
            .reason
            .or(result.error)
            .unwrap_or_else(|| "owned_sell_submit_refused".into())
    );
    Ok(())
}
/// Obligations are processed independently of new-trade flags, kill/deadline and policy removal.
pub(super) async fn recover(
    store: &SqliteStore,
    c: &ExecutionConfig,
) -> Result<recovery::Completed> {
    let mut completed = recovery::Completed::default();
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    for id in store.owned_sell_dispatch_ids(c.canary_batch_limit.max(1))? {
        completed.checked += 1;
        let d = store
            .load_execution_canary_dispatch(&id)?
            .context("owned_sell_dispatch_missing")?;
        let timeout = c.max_confirm_seconds.saturating_mul(1000).clamp(1, 30_000);
        if store.load_failed_expense_task(&id)?.is_some() {
            recover_failed_expense_order(
                store,
                &http,
                &c.submit_adapter_http_url,
                &id,
                &d.wallet,
                Utc::now(),
                timeout,
            )
            .await?;
            let task = store
                .load_failed_expense_task(&id)?
                .context("owned_sell_failed_task_missing")?;
            if task.status == "complete" {
                completed.reconciled += 1;
            } else {
                completed.pending_reason = Some(task.reason);
            }
        } else {
            let outcome = record_execution_rpc_confirmation_boundary(
                store,
                &http,
                &c.submit_adapter_http_url,
                &id,
                &d.wallet,
                Utc::now(),
                timeout,
            )
            .await?;
            // A failed order can be terminal while its Unknown expense remains
            // held. Report completion from the durable expense task, not status.
            if let Some(task) = store.load_failed_expense_task(&id)? {
                if task.status == "complete" {
                    completed.reconciled += 1;
                } else {
                    completed.pending_reason = Some(task.reason);
                }
            } else if outcome.pending > 0 {
                completed.pending_reason = outcome.reason;
            } else {
                completed.reconciled += outcome.confirmed;
            }
        }
    }
    Ok(completed)
}
