use super::receipt_reconciliation_fixture::*;
use anyhow::Result;

#[tokio::test]
async fn confirmed_transaction_meta_replaces_quote_buy_fill() -> Result<()> {
    let f = Fixture::new("buy")?;
    let rpc = Rpc::new(receipt("buy", -900_000_000)).await?;
    rpc.context(format!(
        "confirmed_transaction_meta_replaces_quote_buy_fill"
    ));
    let outcome = f.reconcile(&rpc, 10).await?;
    assert_eq!(outcome.buy_opened, 1);
    let p = f.store.load_execution_canary_open_position(TOKEN)?.unwrap();
    assert_eq!(
        p.qty_exact,
        Some(copybot_core_types::TokenQuantity::new(7_000, 3))
    );
    assert!((p.cost_sol - 0.9).abs() < 1e-12);
    assert_eq!(f.fills()?, 1);
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn confirmed_transaction_meta_replaces_quote_sell_fill() -> Result<()> {
    let f = Fixture::new("sell")?;
    let rpc = Rpc::new(receipt("sell", 1_200_000_000)).await?;
    rpc.context(format!(
        "confirmed_transaction_meta_replaces_quote_sell_fill"
    ));
    let outcome = f.reconcile(&rpc, 10).await?;
    assert_eq!(outcome.sell_partial, 1);
    let (raw, native_delta): (String, i64) = f.conn()?.query_row(
        "SELECT qty_raw, wallet_native_delta_lamports FROM fills WHERE order_id = ?1",
        [&f.order_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(raw, "7000");
    assert_eq!(native_delta, 1_200_000_000);
    assert_eq!(
        f.store
            .load_execution_canary_open_position(TOKEN)?
            .unwrap()
            .qty_exact,
        Some(copybot_core_types::TokenQuantity::new(3_000, 3))
    );
    rpc.finish().await?;
    Ok(())
}
