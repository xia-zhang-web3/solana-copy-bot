use crate::SellSettlementUnsupported as Unsupported;
use anyhow::{ensure, Context, Result};
use rusqlite::Connection;

/// Signature-indexed probes include each receipt and its linked order's separate
/// claim. Keep this local to native SELL; BUY's legacy/schema boundary is unchanged.
pub(crate) fn claim_sql(table: &str) -> String {
    format!(
        "SELECT r.order_id,r.wallet_pubkey,r.tx_signature,o.tx_signature
         FROM {table} r LEFT JOIN orders o ON o.order_id=r.order_id
         WHERE r.tx_signature=?1
         UNION ALL
         SELECT r.order_id,r.wallet_pubkey,r.tx_signature,o.tx_signature
         FROM orders o JOIN {table} r ON r.order_id=o.order_id
         WHERE o.tx_signature=?1"
    )
}

fn key(value: &str) -> Result<()> {
    // Preserve exact TEXT key semantics; malformed well-formed rows are local
    // typed refusals. SQL type decoding, schema and I/O errors propagate separately.
    ensure!(
        !value.trim().is_empty(),
        Unsupported::UnprovenReceiptOwnership
    );
    Ok(())
}

/// Fresh identity and recorded-fill replay have already been checked on this conn.
/// Read durable keys again; never use caller wallet/signature as ownership proof.
pub(crate) fn validate(conn: &Connection, order_id: &str) -> Result<()> {
    let (wallet, signature): (String, String) = conn.query_row(
        "SELECT wallet_pubkey,tx_signature FROM execution_canary_receipt_facts WHERE order_id=?1",
        [order_id], |r| Ok((r.get(0)?,r.get(1)?)),
    ).context("load current SELL receipt ownership")?;
    key(&wallet)?;
    key(&signature)?;
    for table in [
        "execution_canary_receipt_proofs",
        "execution_canary_receipt_facts",
    ] {
        let mut stmt = conn
            .prepare(&claim_sql(table))
            .context("prepare SELL receipt ownership")?;
        let mut rows = stmt
            .query([&signature])
            .context("lookup SELL receipt ownership")?;
        while let Some(row) = rows.next()? {
            let owner: String = row.get(0)?;
            let claim_wallet: String = row.get(1)?;
            let claim_signature: String = row.get(2)?;
            let order_signature: Option<String> = row.get(3)?;
            key(&owner)?;
            key(&claim_wallet)?;
            key(&claim_signature)?;
            if let Some(value) = order_signature {
                key(&value)?;
            }
            // Proof+facts of one order are one owner. Orphan/pending claims remain
            // owners; mint, leader, day, route, side and position are not filters.
            ensure!(
                owner == order_id || claim_wallet != wallet,
                Unsupported::ReceiptAlreadyClaimed
            );
        }
    }
    Ok(())
}
