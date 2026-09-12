use super::fixture::Db;
use anyhow::{ensure, Result};
use copybot_storage_core::ExecutionCanaryOwnedPositionRecordResult;
use rusqlite::params;

/// Historical corruption fixture only. Account a distinct valid receipt through
/// the public writer, then explicitly restore the conflicting historical key.
/// This is not the duplicate-writer probe and must never bypass production code.
pub fn buy(db: &Db, id: &str, token: &str) -> Result<ExecutionCanaryOwnedPositionRecordResult> {
    ensure!(!db.store.execution_canary_fill_exists(id)?);
    let original = db
        .store
        .load_execution_canary_receipt_proof(id)?
        .unwrap()
        .tx_signature;
    set_signature(db, id, &format!("historical-fixture:{id}"))?;
    let result = db.buy_claim(id, token);
    set_signature(db, id, &original)?;
    result
}

fn set_signature(db: &Db, id: &str, signature: &str) -> Result<()> {
    let mut conn = db.conn()?;
    let tx = conn.transaction()?;
    for table in [
        "orders",
        "execution_canary_receipt_proofs",
        "execution_canary_receipt_facts",
    ] {
        let rows = tx.execute(
            &format!("UPDATE {table} SET tx_signature=?2 WHERE order_id=?1"),
            params![id, signature],
        )?;
        ensure!(
            rows == 1,
            "historical fixture requires complete public receipt setup"
        );
    }
    tx.commit()?;
    Ok(())
}
