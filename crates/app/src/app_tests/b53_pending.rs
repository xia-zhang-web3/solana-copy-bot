use super::b53_fixture::Fixture;
use anyhow::Result;

#[tokio::test]
async fn b53_actual_tick_pending_buy_blocks_b_with_unreflected_balance() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.backend.funding.lock().unwrap().balance = 80_000_000;
    f.seed("audit-a", 0)?;
    let first = f.tick(0).await?;
    println!("B53_TICK_FIRST {first:?}");
    assert_eq!(f.sends().len(), 1, "{first:?}");
    assert_eq!(
        f.order("audit-a")?.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    f.reopen()?;
    let a = f.emit("pending_reopen", "audit-a")?;
    assert_eq!(a["accounting_pending"], false);
    assert_eq!(a["selected"], true);
    f.backend.wire.lock().unwrap().blockhash = 17;
    f.seed("audit-b", 2)?;
    let second = f.tick(2).await?;
    println!("B53_TICK_SECOND {second:?}");
    assert_eq!(f.sends().len(), 1, "{second:?}");
    f.emit("second_buy_blocked", "audit-a")?;
    f.finish().await
}

#[tokio::test]
async fn b53_lower_rpc_balance_blocks_b_without_inventing_a_second_debit() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.backend.funding.lock().unwrap().balance = 70_000_000;
    f.seed("audit-a", 0)?;
    f.tick(0).await?;
    assert_eq!(f.sends().len(), 1);
    f.reopen()?;
    // Separate observed-balance scenario. Attribution to A is a scenario assumption,
    // not proven by this pending status or a guessed reservation subtraction.
    f.backend.funding.lock().unwrap().balance = 59_993_000;
    f.backend.wire.lock().unwrap().blockhash = 17;
    f.seed("audit-b", 2)?;
    f.tick(2).await?;
    let a = f.emit("lower_snapshot_no_double_subtraction", "audit-a")?;
    assert_eq!(f.sends().len(), 1);
    assert_eq!(a["buy_blocker"], "unresolved_buy_dispatch");
    assert_eq!(
        f.order("audit-a")?.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    f.finish().await
}

#[tokio::test]
async fn b53_known_signature_local_timeout_keeps_selection_and_late_receipt() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("audit-a", 0)?;
    f.tick(0).await?;
    assert_eq!(f.sends().len(), 1);
    let signature = f.order("audit-a")?.tx_signature.unwrap();
    f.reopen()?;
    f.tick(31).await?;
    let a = f.emit("known_signature_local_expiry", "audit-a")?;
    assert_eq!(
        a["status"],
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert_eq!(a["selected"], true);
    assert_eq!(a["signature"], signature);
    assert_eq!(a["accounting_pending"], false);
    assert_eq!(a["failed_expenses"], 0);
    let queries = f
        .rpc
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|r| r["body"]["method"] == "getSignatureStatuses")
        .count();
    f.rpc
        .state
        .lock()
        .unwrap()
        .chain
        .insert(signature, super::b53_http::Chain::Settled);
    f.reopen()?;
    f.tick(32).await?;
    assert_eq!(f.sends().len(), 1, "expired A is not retried");
    assert!(
        f.rpc
            .state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r["body"]["method"] == "getSignatureStatuses")
            .count()
            > queries
    );
    f.backend.wire.lock().unwrap().blockhash = 17;
    f.seed("audit-b", 33)?;
    f.tick(33).await?;
    assert_eq!(f.sends().len(), 2);
    assert!(f
        .store
        .execution_canary_fill_exists(&f.order("audit-a")?.order_id)?);
    f.emit("expired_a_late_receipt_not_reconciled_b_sent", "audit-a")?;
    f.finish().await
}
