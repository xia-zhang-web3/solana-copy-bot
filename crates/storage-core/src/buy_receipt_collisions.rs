use anyhow::{ensure, Context, Result};
use rusqlite::Connection;
use std::collections::BTreeSet;

/// One ledger-wide grouped read in the caller's existing snapshot. A receipt key
/// belongs to an execution wallet, not a leader/mint/position. Keep even orphaned,
/// pending or contradictory claims: incomplete joins must not make them vanish.
/// Proof and facts of one order count once, including repeated identity assertions.
pub(crate) fn load(conn: &Connection) -> Result<BTreeSet<(String, String)>> {
    let mut statement = conn
        .prepare(
            "WITH receipt_claims AS (
            SELECT order_id,wallet_pubkey,tx_signature FROM execution_canary_receipt_proofs
            UNION ALL
            SELECT order_id,wallet_pubkey,tx_signature FROM execution_canary_receipt_facts
         ), claims AS (
            SELECT order_id,wallet_pubkey,tx_signature FROM receipt_claims
            UNION ALL
            SELECT r.order_id,r.wallet_pubkey,o.tx_signature FROM receipt_claims r
            JOIN orders o ON o.order_id=r.order_id WHERE o.tx_signature IS NOT NULL
         )
         SELECT wallet_pubkey,tx_signature,COUNT(DISTINCT order_id),
            MIN(typeof(order_id)='text' AND length(trim(order_id))>0)
         FROM claims GROUP BY wallet_pubkey,tx_signature",
        )
        .context("prepare BUY receipt collision scan")?;
    let mut rows = statement.query([]).context("read BUY receipt collisions")?;
    let mut collisions = BTreeSet::new();
    while let Some(row) = rows.next()? {
        // Decode every group, not just collisions: malformed identity domains
        // remain errors, never a silently dropped counterpart or empty success.
        let wallet: String = row.get(0)?;
        let signature: String = row.get(1)?;
        let orders: i64 = row.get(2)?;
        let valid_order_ids: bool = row.get(3)?;
        ensure!(
            !wallet.trim().is_empty()
                && !signature.trim().is_empty()
                && valid_order_ids
                && orders > 0,
            "invalid BUY receipt claim identity"
        );
        if orders > 1 {
            collisions.insert((wallet, signature));
        }
    }
    Ok(collisions)
}
