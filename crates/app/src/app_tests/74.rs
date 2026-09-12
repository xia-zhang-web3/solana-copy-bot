use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn confirmed_sell_accounting_preserves_actual_partial_amount_and_one_raw_dust() -> Result<()>
{
    let f = Fixture::new("sell")?;
    let mut transaction = receipt("sell", 480_000_000);
    transaction["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("1");
    let rpc = Rpc::new(transaction).await?;
    rpc.context(format!(
        "confirmed_sell_accounting_preserves_actual_partial_amount_and_one_raw_dust"
    ));
    let outcome = f.reconcile(&rpc, 10).await?;
    assert_eq!(outcome.sell_partial, 1);
    assert_eq!(outcome.sell_dust_closed, 0);
    let remaining = f.store.load_execution_canary_open_position(TOKEN)?.unwrap();
    assert_eq!(
        remaining.qty_exact,
        Some(copybot_core_types::TokenQuantity::new(1, 3))
    );
    let raw: String = f.conn()?.query_row(
        "SELECT qty_raw FROM fills WHERE order_id = ?1",
        [&f.order_id],
        |r| r.get(0),
    )?;
    assert_eq!(raw, "9999");
    rpc.finish().await?;
    Ok(())
}
