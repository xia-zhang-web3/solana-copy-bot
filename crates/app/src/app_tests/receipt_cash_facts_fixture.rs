use super::receipt_reconciliation_fixture::{Fixture, Rpc, TOKEN};
use anyhow::{anyhow, Result};
use copybot_storage_core::{
    ExecutionCanaryReceiptFacts, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
};
use serde_json::{json, Value};

pub(super) fn facts(f: &Fixture) -> Result<ExecutionCanaryReceiptFacts> {
    f.store
        .load_execution_canary_receipt_facts(&f.order_id)?
        .ok_or_else(|| anyhow!("verified receipt facts missing after boundary"))
}

pub(super) fn assert_pending(f: &Fixture) -> Result<()> {
    assert_eq!(f.fills()?, 0);
    assert_eq!(
        f.store
            .load_execution_canary_order(&f.order_id)?
            .unwrap()
            .status,
        EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
    );
    assert!(f.store.execution_canary_accounting_pending()?);
    assert!(f.store.execution_canary_token_accounting_pending(TOKEN)?);
    assert!(f
        .store
        .execution_canary_receipt_submit_block_reason("another-buy", "other", "buy")?
        .is_some());
    assert!(f
        .store
        .execution_canary_receipt_submit_block_reason("another-sell", TOKEN, "sell")?
        .is_some());
    assert!(f
        .store
        .execution_canary_receipt_submit_block_reason("other-sell", "other", "sell")?
        .is_none());
    Ok(())
}

pub(super) fn money_snapshot(f: &Fixture) -> Result<Vec<Vec<String>>> {
    let conn = f.conn()?;
    let mut rows = Vec::new();
    for table in ["positions", "fills"] {
        let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
        let columns = stmt.column_count();
        rows.extend(
            stmt.query_map([], |r| {
                (0..columns)
                    .map(|c| {
                        r.get::<_, rusqlite::types::Value>(c)
                            .map(|v| format!("{v:?}"))
                    })
                    .collect()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?,
        );
    }
    Ok(rows)
}

pub(super) fn transaction_calls(rpc: &Rpc) -> usize {
    rpc.calls
        .lock()
        .unwrap()
        .iter()
        .filter(|m| *m == "getTransaction")
        .count()
}

pub(super) fn sponsored(mut value: Value) -> Value {
    value["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap()
        .insert(0, json!({"pubkey":"Sponsor","signer":true,"writable":true}));
    for name in ["preBalances", "postBalances"] {
        value["result"]["meta"][name]
            .as_array_mut()
            .unwrap()
            .insert(
                0,
                json!(if name == "preBalances" {
                    1_000_000
                } else {
                    995_000
                }),
            );
    }
    for name in ["preTokenBalances", "postTokenBalances"] {
        for row in value["result"]["meta"][name].as_array_mut().unwrap() {
            row["accountIndex"] = json!(row["accountIndex"].as_u64().unwrap() + 1);
        }
    }
    value
}

pub(super) fn stop_accounting(f: &Fixture) -> Result<()> {
    f.conn()?.execute_batch("CREATE TRIGGER stop_accounting BEFORE UPDATE ON execution_canary_receipt_proofs
        WHEN NEW.reason = 'accounting_complete' BEGIN SELECT RAISE(ABORT, 'synthetic_crash_before_accounting'); END;")?;
    Ok(())
}
