use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn already_confirmed_tiny_buy_requires_receipt_without_quote() -> Result<()> {
    let f = Fixture::new("buy")?;
    f.store
        .mark_execution_canary_confirmed(&f.order_id, f.now)?;
    f.conn()?
        .execute("DELETE FROM execution_canary_build_plan_metadata", [])?;
    let rpc = Rpc::new(json!({"result":null})).await?;
    rpc.context(format!(
        "already_confirmed_tiny_buy_requires_receipt_without_quote"
    ));
    let missing = f.reconcile(&rpc, 10).await?;
    assert_eq!(missing.confirmation_pending, 1);
    assert_eq!(f.fills()?, 0);
    assert!(f.store.execution_canary_accounting_pending()?);
    rpc.set(receipt("buy", -900_000_000));
    assert_eq!(f.reconcile(&rpc, 400).await?.buy_opened, 1);
    let before: (String, String) = f.conn()?.query_row(
        "SELECT status, confirm_ts FROM orders WHERE order_id = ?1",
        [&f.order_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    rpc.set(json!({"error":"should never read again"}));
    assert_eq!(f.reconcile(&rpc, 800).await?.buy_opened, 0);
    let after: (String, String) = f.conn()?.query_row(
        "SELECT status, confirm_ts FROM orders WHERE order_id = ?1",
        [&f.order_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(before, after);
    assert_eq!(
        *rpc.calls.lock().unwrap(),
        vec!["getTransaction", "getTransaction"]
    );
    assert_eq!(f.fills()?, 1);
    rpc.finish().await?;
    Ok(())
}
