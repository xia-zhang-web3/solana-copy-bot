use super::{
    b126_runtime_fixture::fixture, priority_fee_route_fixture::Fixture, receipt_rpc_fixture::Rpc,
};
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::*;
use serde_json::{json, Value};

pub(super) async fn dispatched() -> Result<Fixture> {
    let f = fixture().await?;
    f.funding.lock().unwrap().fee = Some(100000);
    let env = f.build().await?.envelope.unwrap();
    let out =
        super::entry_risk_clock_fixture::at(f.now + Duration::seconds(1), f.submit(&env)).await?;
    assert_eq!(out.submitted, 1);
    assert_eq!(f.sends(), 1);
    Ok(f)
}
pub(super) fn receipt(f: &Fixture, fee: Option<Value>, payer_known: bool) -> Value {
    let wallet = &f.config.canary_wallet_pubkey;
    let row = |raw| {
        json!({"accountIndex":1,"owner":wallet,"mint":f.request.token,
        "uiTokenAmount":{"amount":raw,"decimals":0}})
    };
    let signature = f
        .store
        .load_execution_canary_order(&f.request.order_id)
        .unwrap()
        .unwrap()
        .tx_signature
        .unwrap();
    let mut value = json!({"result":{"slot":42,"blockTime":f.now.timestamp(),
        "transaction":{"signatures":[signature],"message":{"accountKeys":[
            {"pubkey":wallet,"signer":true,"writable":true},
            {"pubkey":"token-account","signer":false,"writable":true}]}},
        "meta":{"err":null,"preBalances":[100000000,2039280],"postBalances":[89995000,2039280],
            "preTokenBalances":[row("0")],"postTokenBalances":[row("123456")]}}});
    if let Some(fee) = fee {
        value["result"]["meta"]["fee"] = fee;
    }
    if !payer_known {
        // Keep wallet signer/native/token proof. The first account has no payer
        // metadata and no balance movement; no foreign-wallet fee is invented.
        value["result"]["transaction"]["message"]["accountKeys"]
            .as_array_mut()
            .unwrap()
            .insert(0, json!({"pubkey":"unproven-payer","writable":true}));
        for name in ["preBalances", "postBalances"] {
            value["result"]["meta"][name]
                .as_array_mut()
                .unwrap()
                .insert(0, json!(0));
        }
        for name in ["preTokenBalances", "postTokenBalances"] {
            value["result"]["meta"][name][0]["accountIndex"] = json!(2);
        }
    }
    value
}
pub(super) async fn reconcile(
    f: &Fixture,
    rpc: &Rpc,
    buy: &str,
) -> Result<crate::execution_submit_adapter::ExecutionConfirmationBoundaryOutcome> {
    crate::execution_submit_adapter::record_execution_rpc_confirmation_boundary(
        &f.store,
        &reqwest::Client::new(),
        &rpc.url,
        buy,
        &f.config.canary_wallet_pubkey,
        f.now + Duration::seconds(2),
        2000,
    )
    .await
}
pub(super) fn reopen(f: &mut Fixture) -> Result<()> {
    let path = f.conn()?.path().unwrap().to_owned();
    f.store = SqliteStore::open(path)?;
    Ok(())
}
pub(super) fn accounting(f: &Fixture) -> Result<Value> {
    let conn = f.conn()?;
    let mut result = serde_json::Map::new();
    for table in [
        "fills",
        "positions",
        "execution_canary_receipt_facts",
        "execution_tiny_reservations",
        "execution_failed_expense_ledger",
    ] {
        let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
        let columns = stmt.column_count();
        let rows = stmt
            .query_map([], |r| {
                (0..columns)
                    .map(|i| {
                        r.get::<_, rusqlite::types::Value>(i)
                            .map(|v| format!("{v:?}"))
                    })
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        result.insert(table.into(), json!(rows));
    }
    Ok(Value::Object(result))
}
pub(super) fn readback(f: &Fixture, case: &str, stage: &str) -> Result<()> {
    let conn = f.conn()?;
    let (buys,sells,reserved,actual):(u64,u64,u64,u64)=conn.query_row("SELECT COUNT(CASE WHEN side='buy' THEN 1 END),COUNT(CASE WHEN side='sell' THEN 1 END),COALESCE(SUM(CASE WHEN actual_fee IS NULL THEN fee_bound ELSE 0 END),0),COALESCE(SUM(actual_fee),0) FROM execution_tiny_reservations",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    println!(
        "B126_R1_READBACK {}",
        json!({"case":case,"stage":stage,"buys":buys,"sells":sells,"reserved":reserved,"actual":actual,"sends":f.sends(),"accounting":accounting(f)?})
    );
    // Optional local evidence export; ordinary tests have no required environment.
    if let Ok(dir) = std::env::var("B126_R1_EVIDENCE_DIR") {
        let path = std::path::Path::new(&dir).join(format!("{case}-{stage}.db"));
        conn.execute("VACUUM INTO ?1", [path.to_str().unwrap()])?;
    }
    Ok(())
}
