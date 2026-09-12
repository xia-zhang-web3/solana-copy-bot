use super::{
    b53_fixture::Fixture,
    priority_fee_route_fixture::{Fixture as Signed, Route},
};
use anyhow::Result;
use copybot_storage_core::*;

#[tokio::test]
async fn b53_unresolved_buy_blocks_hot_route_and_unsigned_retry_after_reopen() -> Result<()> {
    for retry in [false, true] {
        let mut f = Fixture::new().await?;
        f.seed("audit-a", 0)?;
        f.tick(0).await?;
        f.seed("audit-b", 2)?;
        let signal = f.store.load_copy_signal_by_signal_id("audit-b")?.unwrap();
        let mut before = None;
        if retry {
            let o = f
                .store
                .reserve_execution_canary_order("audit-b", &f.config.canary_route, f.now)?
                .order;
            f.store.mark_execution_canary_built(&o.order_id, f.now)?;
            f.store.mark_execution_canary_simulated(
                &o.order_id,
                f.now,
                EXECUTION_SIMULATION_STATUS_PASSED,
                None,
            )?;
            // API-seeded proved pre-dispatch retry; B has never reached a transport.
            before=Some(f.store.mark_execution_canary_retry_after_submit_not_sent(&o.order_id,f.now,
                crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON)?);
        }
        f.reopen()?;
        if retry {
            f.tick(3).await?;
        } else {
            let out = super::entry_risk_clock_fixture::at(
                f.now + chrono::Duration::seconds(4),
                crate::execution_canary_route::process_canary_state_machine_for_route(
                    &f.config,
                    &f.store,
                    &signal,
                    f.now + chrono::Duration::seconds(3),
                ),
            )
            .await?;
            assert_eq!(out.safety_blocked, 1);
            assert_eq!(out.skipped_reason, Some(EXECUTION_UNRESOLVED_BUY_REASON));
        }
        assert_eq!(f.sends().len(), 1);
        assert_eq!(
            f.store.load_execution_canary_order_by_signal("audit-b")?,
            before
        );
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn b53_other_pending_appears_during_funding_no_dispatch_and_no_order_mutation() -> Result<()>
{
    let mut f = Signed::new(Route::Direct, 200_000, 1_400_000).await?;
    let envelope = f.build().await?.envelope.unwrap();
    let before = f
        .store
        .load_execution_canary_order(&f.request.order_id)?
        .unwrap();
    let id = super::receipt_reconciliation_fixture::add_order(
        &f.store,
        "racing-a",
        "buy",
        "OtherMint",
        f.now,
        false,
    )?;
    let db = f.conn()?.path().unwrap().to_owned();
    let other = std::sync::Mutex::new(SqliteStore::open(db)?);
    let at = f.now;
    let rpc = super::native_rpc_fixture::Fixture::start_with_in_flight(3, move |request| {
        if request["method"] == "getFeeForMessage" {
            other
                .lock()
                .unwrap()
                .mark_execution_canary_submitted(&id, at, "already-sent-elsewhere")
                .unwrap();
        }
        super::native_rpc_fixture::Reply::json(
            super::initial_sol_rpc_fixture::FundingRpc::default().reply(request),
        )
    })
    .await?;
    f.config.submit_adapter_http_url = rpc.endpoint.clone();
    let out = f.submit(&envelope).await?;
    assert_eq!(out.submitted, 0);
    assert_eq!(out.submit_ready_rejected, 1);
    assert_eq!(
        f.store.load_execution_canary_order(&f.request.order_id)?,
        Some(before)
    );
    let trace = rpc.finish().await?;
    assert_eq!(trace.len(), 3);
    assert!(!trace
        .iter()
        .any(|r| r.request["method"] == "sendTransaction"));
    assert!(f
        .store
        .load_execution_canary_dispatch(&f.request.order_id)?
        .is_none());
    f.finish().await
}
