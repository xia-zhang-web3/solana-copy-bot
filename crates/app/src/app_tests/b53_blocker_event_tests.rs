use super::{b53_fixture::Fixture, b53_http::Chain, b53_root_event_review_tests::capture_tick};
use anyhow::Result;
use copybot_storage_core::EXECUTION_UNRESOLVED_BUY_REASON;

#[tokio::test]
async fn b53_r1_accounting_clears_a_or_replaces_it_with_new_c_in_same_tick() -> Result<()> {
    for new_c in [false, true] {
        let mut f = Fixture::new().await?;
        f.seed("audit-a", 0)?;
        f.tick(0).await?;
        let a = f.order("audit-a")?;
        f.reopen()?;
        let pending = capture_tick(&f.tick(1).await?);
        assert_eq!(
            pending["buy_blocker_reason"],
            EXECUTION_UNRESOLVED_BUY_REASON
        );
        assert_eq!(pending["buy_blocker_order_id"], a.order_id);
        f.rpc
            .state
            .lock()
            .unwrap()
            .chain
            .insert(a.tx_signature.clone().unwrap(), Chain::Settled);
        if new_c {
            f.backend.wire.lock().unwrap().blockhash = 17;
            f.seed("audit-c", 1)?;
        }
        f.reopen()?;
        let settled = capture_tick(&f.tick(2).await?);
        assert!(f.store.execution_canary_fill_exists(&a.order_id)?);
        let expected = if new_c {
            f.order("audit-c")?.order_id
        } else {
            "none".into()
        };
        assert_eq!(settled["buy_blocker_order_id"], expected);
        assert_eq!(
            settled["buy_blocker_reason"],
            if new_c {
                EXECUTION_UNRESOLVED_BUY_REASON
            } else {
                "none"
            }
        );
        assert_eq!(f.sends().len(), if new_c { 2 } else { 1 });
        f.reopen()?;
        let next = capture_tick(&f.tick(3).await?);
        assert_eq!(next["buy_blocker_order_id"], expected);
        f.finish().await?;
        println!(
            "B53_R1_EVENT {}",
            serde_json::json!({"new_c":new_c,"pending":pending,"settled":settled,"next":next})
        );
    }
    Ok(())
}

#[tokio::test]
async fn b53_r1_read_failure_has_no_stale_witness_and_does_not_release_buy() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("audit-a", 0)?;
    f.tick(0).await?;
    let mut tick = f.tick(1).await?;
    assert_eq!(tick.buy_blocker.order_id(), f.order("audit-a")?.order_id);
    f.conn()?
        .execute_batch("ALTER TABLE execution_canary_dispatch RENAME TO unavailable_dispatch")?;
    tick.buy_blocker.refresh(&f.store);
    let event = capture_tick(&tick);
    assert_eq!(event["buy_blocker_order_id"], "none");
    assert_eq!(
        event["buy_blocker_reason"],
        "unresolved_buy_state_unavailable"
    );
    assert!(f.store.execution_canary_unresolved_buy().is_err());
    assert!(
        crate::execution_canary_safety::pre_submit_safety_snapshot(&f.config, &f.store, f.now)
            .is_err()
    );
    assert_eq!(f.sends().len(), 1);
    f.finish().await
}

#[tokio::test]
async fn b53_r1_legacy_multiple_buy_witness_is_deterministic_and_pair_stays_together() -> Result<()>
{
    let mut f = Fixture::new().await?;
    let mut ids = Vec::new();
    for (id, signed) in [("legacy-z", false), ("legacy-a", true)] {
        let order = super::receipt_reconciliation_fixture::add_order(
            &f.store, id, "buy", id, f.now, signed,
        )?;
        if !signed {
            f.store
                .mark_execution_canary_submitted_unknown(&order, f.now, "legacy_unknown")?;
        }
        ids.push(order);
    }
    ids.sort();
    f.reopen()?;
    let summary = f.tick(1).await?;
    let event = capture_tick(&summary);
    assert_eq!(event["buy_blocker_order_id"], ids[0]);
    assert_eq!(event["buy_blocker_reason"], EXECUTION_UNRESOLVED_BUY_REASON);
    assert!(f.store.execution_canary_unresolved_buy()?);
    assert_eq!(f.sends().len(), 0);
    for id in ids {
        let order = f.store.load_execution_canary_order(&id)?.unwrap();
        assert_eq!(
            order.status,
            copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
        );
        assert!(!f.store.execution_canary_fill_exists(&id)?);
    }
    f.finish().await
}
