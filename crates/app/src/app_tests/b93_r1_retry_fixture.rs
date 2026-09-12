use super::{b93_attempt_fixture::Attempt, b93_fixture as f};
use anyhow::Result;
use copybot_storage_core::*;

pub async fn fresh_after_refusal(a: &mut Attempt) -> Result<()> {
    let old =
        a.db.store
            .load_execution_canary_order(&a.request.order_id)?
            .unwrap();
    assert_eq!(old.status, EXECUTION_STATUS_CANARY_FAILED);
    assert_eq!(old.err_code.as_deref(), Some(EXECUTION_ERROR_BUILD_FAILED));
    assert_eq!(
        old.simulation_error.as_deref(),
        Some("source_sell_amount_stale")
    );
    assert_eq!(old.attempt, a.request.attempt);
    // Refusal retains the exact old payload/proof. Only the next attempt replaces it.
    assert_eq!(
        a.db.store
            .load_execution_canary_build_plan_metadata(&old.order_id)?
            .unwrap()
            .quote_in_amount_raw
            .as_deref(),
        Some("7000")
    );
    assert_eq!(
        a.db.store
            .load_execution_canary_sell_amount_proof(&old.order_id)?
            .unwrap(),
        serde_json::to_string(a.request.metadata.owned_sell_amount.as_ref().unwrap())?
    );
    a.config.max_submit_attempts = 3;
    a.rpc.state.lock().unwrap().wallet_raw = 4000;
    a.reopen()?;
    let swept = a.retry().await?;
    let fresh = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &a.config,
        &a.db.store,
        &a.event_id,
        f::at(),
    )
    .await?
    .unwrap();
    let next =
        a.db.store
            .load_execution_canary_order(&old.order_id)?
            .unwrap();
    assert_eq!(next.attempt, old.attempt + 1);
    assert_eq!(fresh.built + swept.built, 1);
    assert!(fresh
        .last_error
        .as_deref()
        .into_iter()
        .chain(swept.last_error.as_deref())
        .any(|s| s.contains("signer keypair")));
    let request = crate::execution_submit_adapter::build_tiny_submit_reconciliation_request(
        &a.db.store,
        &a.config,
        &next,
    )?;
    assert_eq!(
        request.metadata.quote_in_amount_raw.as_deref(),
        Some("4000")
    );
    assert_ne!(
        request.metadata.owned_sell_amount,
        a.request.metadata.owned_sell_amount
    );
    request
        .metadata
        .owned_sell_amount
        .as_ref()
        .unwrap()
        .validate(&request)?;
    assert_eq!(a.rpc.count("sendTransaction"), 0);
    assert_eq!(f::raw(&a.db, &a.m)?, 4000);
    Ok(())
}
