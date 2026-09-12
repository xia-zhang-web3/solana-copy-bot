use super::{b53_fixture::Fixture, b53_http::Chain};
use anyhow::Result;
use copybot_storage_core::*;

#[tokio::test]
async fn b53_actual_confirmed_missing_receipt_blocks_b_until_real_accounting() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("audit-a", 0)?;
    f.tick(0).await?;
    let signature = f.order("audit-a")?.tx_signature.unwrap();
    f.rpc
        .state
        .lock()
        .unwrap()
        .chain
        .insert(signature.clone(), Chain::MissingReceipt);
    f.reopen()?;
    f.tick(2).await?;
    let a = f.emit("confirmed_missing_receipt", "audit-a")?;
    assert_eq!(a["status"], EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED);
    assert_eq!(a["buy_blocker"], EXECUTION_ACCOUNTING_PENDING_REASON);
    f.seed("audit-b", 3)?;
    f.tick(3).await?;
    assert_eq!(f.sends().len(), 1, "B blocked before another send");
    f.rpc
        .state
        .lock()
        .unwrap()
        .chain
        .insert(signature, Chain::Settled);
    f.backend.wire.lock().unwrap().blockhash = 17;
    f.backend.funding.lock().unwrap().balance = 69_993_000;
    f.reopen()?;
    f.tick(4).await?;
    let a = f.emit("settled_release_then_b", "audit-a")?;
    assert_eq!(a["status"], EXECUTION_STATUS_CANARY_CONFIRMED);
    assert!(f
        .store
        .execution_canary_fill_exists(&f.order("audit-a")?.order_id)?);
    assert_eq!(f.sends().len(), 1); // Variant A never restores the consumed BUY slot.
    assert_eq!(a["accounting_pending"], false);
    f.finish().await
}

#[tokio::test]
async fn b53_failed_receipt_records_actual_fee_once_and_keeps_buy_budget_consumed() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("audit-a", 0)?;
    f.tick(0).await?;
    let signature = f.order("audit-a")?.tx_signature.unwrap();
    f.rpc
        .state
        .lock()
        .unwrap()
        .chain
        .insert(signature, Chain::Failed);
    f.reopen()?;
    f.tick(2).await?;
    let a = f.emit("proven_failed_receipt", "audit-a")?;
    assert_eq!(a["status"], EXECUTION_STATUS_CANARY_FAILED);
    assert_eq!(a["failed_expenses"], 1);
    assert!(!f
        .store
        .execution_canary_fill_exists(&f.order("audit-a")?.order_id)?);
    let loss = crate::execution_canary_safety::pre_submit_safety_snapshot(
        &f.config,
        &f.store,
        f.now + chrono::Duration::seconds(4),
    )?;
    println!("B53_FAILED_FEE {loss:?}");
    assert_eq!(loss.daily_loss_sol, 0.000007);
    assert_eq!(loss.blocked_reason, None);
    f.backend.wire.lock().unwrap().blockhash = 17;
    f.seed("audit-b", 3)?;
    f.reopen()?;
    f.tick(3).await?;
    assert_eq!(f.sends().len(), 1); // Variant A never restores the consumed BUY slot.
    assert_eq!(
        f.emit("failed_fee_retained_after_b", "audit-a")?["failed_expenses"],
        1
    );
    f.finish().await
}
