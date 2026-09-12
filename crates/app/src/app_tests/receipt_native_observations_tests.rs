use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn receipt_native_observations_actual_boundary_retains_accounts_and_instructions(
) -> Result<()> {
    let mut f = Fixture::new("buy")?;
    let mut value = receipt("buy", -800_000_000);
    value["result"]["meta"]["preBalances"][1] = json!(9007199254740993_u64);
    value["result"]["meta"]["postBalances"][1] = json!(9007199254741010_u64);
    value["result"]["meta"]["innerInstructions"] = json!([]);
    value["result"]["transaction"]["message"]["instructions"] = json!([{
        "programId":"11111111111111111111111111111111",
        "parsed":{"type":"transfer","info":{"source":WALLET,"destination":"token-account","lamports":17}}
    }]);
    let rpc = Rpc::new(value).await?;
    rpc.context(format!(
        "receipt_native_observations_actual_boundary_retains_accounts_and_instructions"
    ));
    assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_confirmed, 1);
    f.reopen()?;
    let exists: bool = f.conn()?.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='execution_receipt_native_observations')", [], |r|r.get(0))?;
    assert!(
        exists,
        "successful boundary lost quantitative account/instruction observations"
    );
    let stored: String = f.conn()?.query_row(
        "SELECT observations_json FROM execution_receipt_native_observations WHERE order_id=?1",
        [&f.order_id],
        |r| r.get(0),
    )?;
    let data: serde_json::Value = serde_json::from_str(&stored)?;
    assert_eq!(
        data["accounts"][0]["native_pre"]["value"],
        "9007199254740993"
    );
    assert_eq!(
        data["accounts"][0]["native_post"]["value"],
        "9007199254741010"
    );
    assert_eq!(data["accounts"][0]["native_delta"]["value"], "17");
    assert_eq!(data["instructions"][0]["fields"]["lamports"]["value"], "17");
    assert_eq!(f.fills()?, 1);
    rpc.finish().await?;
    Ok(())
}
