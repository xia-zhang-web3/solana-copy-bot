use super::{
    association_sell_fixture as financial, b93_attempt_fixture::Attempt, b93_fixture as f,
};
use crate::execution_source_sell_guard as guard;
use anyhow::Result;
use copybot_storage_core::*;

#[tokio::test]
async fn b93_r1_refusal_transition_preserves_concurrent_authority() -> Result<()> {
    for arm in ["signature", "pending", "attempt", "source", "dispatch"] {
        let mut a = Attempt::new(&format!("b93-r1-race-{arm}")).await?;
        a.partial("race-partial")?;
        let error = guard::request(
            &a.db.store,
            &a.request,
            &[EXECUTION_STATUS_CANARY_SIMULATED],
        )
        .err()
        .unwrap();
        assert_eq!(
            error.downcast_ref::<guard::Refusal>().unwrap().reason,
            "source_sell_amount_stale"
        );
        let other = f::open(&a.db.path)?;
        match arm {
            "signature" => {
                other.store.mark_execution_canary_submitted(
                    &a.request.order_id,
                    f::at(),
                    "synthetic-concurrent-signature",
                )?;
            }
            "pending" => {
                f::receipt(&other, &a.m, "race-pending", 1000)?;
            }
            "attempt" => {
                other
                    .store
                    .mark_execution_canary_retry_after_submit_not_sent(
                        &a.request.order_id,
                        f::at(),
                        "retry_after_rpc_not_sent",
                    )?;
            }
            "source" => {
                f::additional_buy(
                    &other,
                    &a.m,
                    "race-buy",
                    a.m["source"]["signer"].as_str().unwrap(),
                    "2026-09-09T00:00:20Z",
                )?;
            }
            "dispatch" => {
                let order = other
                    .store
                    .load_execution_canary_order(&a.request.order_id)?
                    .unwrap();
                let dispatch = ExecutionCanaryDispatch {
                    order_id: order.order_id.clone(),
                    signal_id: order.signal_id.clone(),
                    client_order_id: order.client_order_id.clone(),
                    route: order.route.clone(),
                    attempt: order.attempt,
                    wallet: a.config.canary_wallet_pubkey.clone(),
                    token: a.signal.token.clone(),
                    side: a.signal.side.clone(),
                    tx_signature: "synthetic-concurrent-dispatch".into(),
                    transaction_sha256: "a".repeat(64),
                    message_sha256: "b".repeat(64),
                };
                assert_eq!(
                    other.store.claim_execution_canary_dispatch(
                        &order,
                        &a.signal,
                        &dispatch,
                        f::at()
                    )?,
                    ExecutionDispatchClaim::New
                );
            }
            _ => unreachable!(),
        }
        let before = financial::snapshot(&other)?;
        let mut out = Default::default();
        assert!(
            guard::retry::state_result::<()>(&a.db.store, f::at(), Err(error), &mut out)?.is_none()
        );
        assert_eq!(out.skipped_reason, Some("source_sell_amount_stale"));
        assert_eq!(financial::snapshot(&other)?, before, "{arm}");
        assert_eq!(a.rpc.count("sendTransaction"), 0);
        a.rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn b93_r1_amount_refusal_at_budget_never_retries_or_writes_off() -> Result<()> {
    let mut a = Attempt::new("b93-r1-budget").await?;
    a.partial("budget-partial")?;
    a.config.max_submit_attempts = a.request.attempt;
    let financial_before = financial::snapshot(&a.db)?
        .into_iter()
        .filter(|s| !s.starts_with("orders:"))
        .collect::<Vec<_>>();
    let calls_before = a.rpc.state.lock().unwrap().calls.len();
    let out = a.retry().await?;
    assert_eq!(out.skipped_reason, Some("source_sell_amount_stale"));
    let retired =
        a.db.store
            .load_execution_canary_order(&a.request.order_id)?
            .unwrap();
    assert_eq!(
        retired.err_code.as_deref(),
        Some(EXECUTION_ERROR_BUILD_FAILED)
    );
    a.reopen()?;
    for seconds in [0, 600, 1200] {
        let now = f::at() + chrono::Duration::seconds(seconds);
        crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
            &a.config,
            &a.db.store,
            now,
        )
        .await?;
        crate::execution_canary_route::process_failed_sell_simulation_sweep(
            &a.config,
            &a.db.store,
            now,
        )
        .await?;
        let out = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
            &a.config,
            &a.db.store,
            &a.event_id,
            now,
        )
        .await?
        .unwrap();
        assert_eq!(
            out.built + out.sell_closed + out.sell_dust_closed + out.signing_envelope_built,
            0
        );
        assert_eq!(
            a.db.store.load_execution_canary_order(&retired.order_id)?,
            Some(retired.clone())
        );
    }
    assert_eq!(a.rpc.state.lock().unwrap().calls.len(), calls_before);
    assert_eq!(
        financial::snapshot(&a.db)?
            .into_iter()
            .filter(|s| !s.starts_with("orders:"))
            .collect::<Vec<_>>(),
        financial_before
    );
    assert_eq!(f::raw(&a.db, &a.m)?, 4000);
    a.rpc.finish().await?;
    Ok(())
}
