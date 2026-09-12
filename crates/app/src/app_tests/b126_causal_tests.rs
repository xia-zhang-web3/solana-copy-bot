use super::{b53_fixture::Fixture, b53_http::Chain};
use anyhow::Result;
use copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED;

#[tokio::test]
async fn b126_new_order_cannot_reset_buy_budget_after_failed_receipt() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("budget-a", 0)?;
    f.tick(0).await?;
    assert_eq!(f.sends().len(), 1, "first BUY must really reach transport");
    let a = f.order("budget-a")?;
    f.rpc
        .state
        .lock()
        .unwrap()
        .chain
        .insert(a.tx_signature.clone().unwrap(), Chain::Failed);
    f.reopen()?;
    f.tick(2).await?;
    assert_eq!(f.order("budget-a")?.status, EXECUTION_STATUS_CANARY_FAILED);
    let fee: u64 = f.conn()?.query_row(
        "SELECT SUM(CAST(wallet_fee_lamports AS INTEGER)) FROM execution_failed_expense_ledger",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(fee, 7000);
    assert!(!f.store.execution_canary_unresolved_buy()?);
    f.backend.wire.lock().unwrap().blockhash = 17;
    f.seed("budget-b", 3)?;
    f.reopen()?;
    f.tick(3).await?;
    let b = f.order("budget-b")?;
    assert_ne!(a.order_id, b.order_id);
    assert_eq!(b.attempt, 1);
    let count = f.sends().len();
    println!("B126_CAUSAL actual=ExecutionCanaryRunner.process_tick order_a={} order_b={} attempts_b={} failed_fee={} sends={count}",a.order_id,b.order_id,b.attempt,fee);
    f.finish().await?;
    assert_eq!(
        count, 1,
        "a new order must not reset the experiment BUY slot"
    );
    Ok(())
}
