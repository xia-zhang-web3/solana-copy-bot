//! Historical reader fixtures only: account distinct receipts, then restore an
//! old conflicting identity. Never disable the production guard or edit money.
use crate::fixture::{prepare_identity, Db};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{ExecutionCanaryCashSettlementResult, ExecutionCanaryReceiptFacts};

pub fn settle(
    db: &Db,
    mut facts: ExecutionCanaryReceiptFacts,
    route: &str,
    at: DateTime<Utc>,
) -> Result<ExecutionCanaryCashSettlementResult> {
    let historical_signature = facts.tx_signature.clone();
    facts.tx_signature = format!("historical-independent:{}", facts.order_id);
    let facts = prepare_identity(db, facts, route)?;
    let result = db
        .store
        .apply_execution_canary_sell_settlement(&facts, at)?;
    // Explicit historical corruption AFTER successful independent accounting.
    let mut conn = db.conn()?;
    let tx = conn.transaction()?;
    for table in [
        "orders",
        "execution_canary_receipt_proofs",
        "execution_canary_receipt_facts",
    ] {
        assert_eq!(
            tx.execute(
                &format!("UPDATE {table} SET tx_signature=?1 WHERE order_id=?2"),
                [&historical_signature, &facts.order_id],
            )?,
            1
        );
    }
    tx.commit()?;
    Ok(result)
}
