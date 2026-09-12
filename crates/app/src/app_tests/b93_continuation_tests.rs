use super::{b93_attempt_fixture::Attempt, b93_fixture as f};
use crate::execution_source_sell_guard as guard;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use copybot_storage_core::*;
use serde_json::json;

#[tokio::test]
async fn b93_amount_refusal_does_not_spend_healthy_unsigned_retry_budget() -> Result<()> {
    let mut a = Attempt::new("b93-continuation").await?;
    a.partial("continuation-partial")?;
    let mut m = a.m.clone();
    m["sell"]["signature"] = json!("synthetic-fresh-source");
    let signal = f::legacy(&a.db, &m)?;
    // Existing 300s admission cutoff is unchanged; both orders have no known signature.
    let now = f::at() + chrono::Duration::seconds(600);
    let quote = super::source_write_off_fixture::quote(&signal, now);
    a.db.store.record_execution_quote_canary_event(&quote)?;
    let reservation =
        a.db.store
            .reserve_execution_canary_sell_order_unless_token_in_flight(
                &signal.signal_id,
                &a.config.canary_route,
                now,
            )?;
    assert!(!reservation.blocked_by_in_flight_sell);
    let order = reservation.order;
    a.rpc.state.lock().unwrap().wallet_raw = 4000;
    let snapshot = guard::order(
        &a.db.store,
        &order.order_id,
        &[EXECUTION_STATUS_CANARY_CANDIDATE],
    )?;
    let metadata =
        crate::execution_quote_provider_selection::selected_execution_build_plan_metadata(
            &a.db.store,
            quote,
        )?;
    let metadata = crate::execution_canary_route::guarded_owned_position_sell_metadata(
        &a.config,
        &a.db.store,
        &signal.token,
        metadata,
        snapshot.as_ref(),
    )
    .await?;
    let request = ExecutionSubmitRequest {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        attempt: order.attempt,
        metadata,
        ..a.request.clone()
    };
    let adapter = JupiterMetisDryRunExecutionAdapter::new(a.config.clone());
    let plan = adapter.build_transaction_plan(&request)?;
    crate::execution_build_plan_metadata::record_execution_build_plan_metadata(
        &a.db.store,
        &plan,
        now,
    )?;
    a.db.store
        .mark_execution_canary_built(&order.order_id, now)?;
    let sim = adapter.simulate_transaction_plan(&plan).await?;
    assert_eq!(sim.status, EXECUTION_SIMULATION_STATUS_PASSED);
    a.db.store.mark_execution_canary_simulated(
        &order.order_id,
        now,
        &sim.status,
        Some(crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON),
    )?;
    a.reopen()?;
    a.config.canary_batch_limit = 1;
    let out = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &a.config,
        &a.db.store,
        now,
    )
    .await?
    .unwrap();
    a.rpc.finish().await?;
    assert_eq!(out.source_sell_refusals.count(), 1, "{out:?}");
    assert_eq!(out.existing, 2, "{out:?}");
    assert_eq!(out.last_order_id.as_deref(), Some(order.order_id.as_str()));
    assert!(
        out.last_error
            .as_deref()
            .unwrap()
            .contains("signer keypair"),
        "{out:?}"
    );
    assert_eq!(
        a.db.store
            .load_execution_canary_order(&a.request.order_id)?
            .unwrap()
            .status,
        EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(out.signing_envelope_built, 0);
    assert_eq!(a.rpc.count("sendTransaction"), 0);
    Ok(())
}
