use super::{
    b53_fixture::Fixture,
    priority_fee_route_fixture::{Fixture as Signed, Route},
};
use anyhow::Result;
use copybot_storage_core::*;

#[tokio::test]
async fn b53_atomic_order_write_failure_is_zero_dispatch_and_rollback() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("audit-a", 0)?;
    f.conn()?.execute_batch("CREATE TRIGGER reject_dispatch BEFORE UPDATE OF tx_signature ON orders
        WHEN NEW.tx_signature IS NOT NULL BEGIN SELECT RAISE(ABORT,'synthetic dispatch write failure'); END;")?;
    assert!(f.tick(0).await.is_err());
    assert_eq!(f.sends().len(), 0, "write failure must precede HTTP");
    assert_eq!(
        f.order("audit-a")?.status,
        EXECUTION_STATUS_CANARY_SIMULATED
    );
    assert!(f
        .store
        .load_execution_canary_dispatch(&f.order("audit-a")?.order_id)?
        .is_none());
    f.finish().await
}

#[tokio::test]
async fn b53_outcome_write_failure_keeps_original_signature_and_blocks_b() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("audit-a", 0)?;
    f.conn()?.execute_batch(
        "CREATE TRIGGER reject_note BEFORE UPDATE ON execution_canary_dispatch
        BEGIN SELECT RAISE(ABORT,'synthetic outcome write failure'); END;",
    )?;
    assert!(f.tick(0).await.is_err());
    assert_eq!(f.sends().len(), 1);
    let order = f.order("audit-a")?;
    assert_eq!(
        order.tx_signature.as_deref(),
        f.sends()[0]["signature"].as_str()
    );
    f.conn()?.execute_batch("DROP TRIGGER reject_note")?;
    f.reopen()?;
    f.seed("audit-b", 2)?;
    f.tick(2).await?;
    assert_eq!(f.sends().len(), 1);
    assert_eq!(f.order("audit-a")?, order);
    f.finish().await
}

#[tokio::test]
async fn b53_hint_mismatch_is_no_send_and_cannot_claim() -> Result<()> {
    let mut f = Signed::new(Route::Direct, 200_000, 1_400_000).await?;
    let mut envelope = f.build().await?.envelope.unwrap();
    envelope.tx_signature_hint = Some("forged-hint".into());
    let result = f.submit(&envelope).await;
    assert!(result.is_err() || result.as_ref().is_ok_and(|r| r.submitted == 0));
    assert_eq!(f.sends(), 0);
    assert!(f
        .store
        .load_execution_canary_dispatch(&f.request.order_id)?
        .is_none());
    f.finish().await
}

#[tokio::test]
async fn b53_claim_before_transport_crash_reopen_is_not_a_second_permission() -> Result<()> {
    let mut f = Signed::new(Route::Direct, 200_000, 1_400_000).await?;
    let envelope = f.build().await?.envelope.unwrap();
    let intent = crate::execution_submit_adapter::execution_submit_intent_from_signed_envelope(
        &f.request,
        &envelope,
        "rpc".into(),
    )?;
    let identity = crate::execution_tiny_submit_state::dispatch::identity(&f.request, &intent)?;
    let order = f
        .store
        .load_execution_canary_order(&f.request.order_id)?
        .unwrap();
    let signal = f
        .store
        .load_copy_signal_by_signal_id(&f.request.signal_id)?
        .unwrap();
    assert_eq!(
        f.store
            .claim_execution_canary_dispatch(&order, &signal, &identity, f.now)?,
        ExecutionDispatchClaim::New
    );
    let db = f.conn()?.path().unwrap().to_owned();
    drop(std::mem::replace(
        &mut f.store,
        SqliteStore::open(":memory:")?,
    ));
    f.store = SqliteStore::open(db)?;
    assert_eq!(
        f.store
            .claim_execution_canary_dispatch(&order, &signal, &identity, f.now)?,
        ExecutionDispatchClaim::Existing
    );
    assert_eq!(f.submit(&envelope).await?.submitted, 0);
    assert_eq!(f.sends(), 0);
    assert!(f.store.execution_canary_unresolved_buy()?);
    assert_eq!(
        f.store.load_execution_canary_dispatch(&order.order_id)?,
        Some(identity)
    );
    f.finish().await
}

#[tokio::test]
async fn b53_signed_legacy_expired_resumes_but_unsigned_submit_never_retries() -> Result<()> {
    for legacy in ["submitted_unsigned", "simulated_unknown", "expired_signed"] {
        let signed = legacy == "expired_signed";
        let mut f = Fixture::new().await?;
        f.seed("audit-a", 0)?;
        // Genuine historical order, created before tiny activation/reservation.
        let id =
            super::execution_state_machine_tiny_submit_timeout_route::mark_tiny_timeout_simulated(
                &f.store,
                &f.store.load_copy_signal_by_signal_id("audit-a")?.unwrap(),
                f.now,
            )?;
        super::execution_state_machine_tiny_submit_timeout_route::record_tiny_timeout_build_metadata(
            &f.store, &id, &f.store.load_copy_signal_by_signal_id("audit-a")?.unwrap(), f.now)?;
        if signed {
            f.store
                .mark_execution_canary_submitted(&id, f.now, "legacy-signed")?;
        } else {
            f.store
                .mark_execution_canary_submitted_unknown(&id, f.now, "legacy_unknown")?;
        }
        let order = f.order("audit-a")?;
        assert!(f.store.load_tiny_experiment(f.now)?.is_none());
        assert!(f.store.load_execution_canary_dispatch(&id)?.is_none());
        if signed {
            f.store
                .mark_execution_canary_expired(&order.order_id, f.now, "old_local_expiry")?;
            f.rpc.state.lock().unwrap().chain.insert(
                order.tx_signature.clone().unwrap(),
                super::b53_http::Chain::Settled,
            );
        } else {
            f.conn()?.execute(
                "UPDATE orders SET tx_signature=NULL WHERE order_id=?1",
                [&order.order_id],
            )?;
        }
        if legacy == "simulated_unknown" {
            f.conn()?.execute("UPDATE orders SET status='execution_canary_simulated', simulation_error='retry_after_unknown_submit_timeout' WHERE order_id=?1",[&order.order_id])?;
        }
        f.reopen()?;
        f.tick(35).await?;
        assert_eq!(f.sends().len(), 0, "legacy recovery never sends");
        assert_eq!(
            f.store.execution_canary_fill_exists(&order.order_id)?,
            signed
        );
        assert_eq!(f.store.execution_canary_unresolved_buy()?, !signed);
        if !signed {
            assert_eq!(f.order("audit-a")?.attempt, 1);
        }
        f.finish().await?;
    }
    Ok(())
}
