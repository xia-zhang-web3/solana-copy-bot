use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};

const TABLES: [(&str, &str); 2] = [
    (
        "execution_canary_receipt_proofs",
        "0051_execution_canary_receipt_proofs.sql",
    ),
    (
        "execution_canary_receipt_facts",
        "0054_execution_canary_receipt_facts.sql",
    ),
];

/// Two indexed signature probes, including the linked order's independent claim.
/// Keep orphan receipts and pending/contradictory claims. Repeated rows from the
/// same order are harmless: ownership is by order ID, not a row count.
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
    // Match existing receipt validators: exact TEXT keys, nonempty after trim.
    // No base58 requirement, case folding, or normalization of historical keys.
    ensure!(
        !value.trim().is_empty(),
        "invalid BUY receipt claim identity"
    );
    Ok(())
}

/// Called only before a new fill, inside the writer's existing IMMEDIATE tx.
/// No receipt is the supported unproven legacy/import path, not a fabricated key.
pub(crate) fn validate(conn: &Connection, order_id: &str) -> Result<()> {
    let mut tables = Vec::new();
    let mut identity = None;
    for (table, migration) in TABLES {
        let kind: Option<String> = conn
            .query_row(
                "SELECT type FROM sqlite_master WHERE name=?1",
                [table],
                |r| r.get(0),
            )
            .optional()?;
        let Some(kind) = kind else {
            // Only this exact migration establishes whether absence is legacy.
            // A later version cannot substitute for it in supported old schemas.
            let recorded: Option<String> = conn
                .query_row(
                    "SELECT version FROM schema_migrations WHERE version=?1",
                    [migration],
                    |r| r.get(0),
                )
                .optional()
                .context("load BUY receipt table migration")?;
            ensure!(
                recorded.is_none(),
                "BUY receipt claim table {table} missing after migration {migration}"
            );
            continue;
        };
        ensure!(kind == "table", "BUY receipt claim table unavailable");
        tables.push(table);
        let receipt: Option<(String, String, String, String)> = conn
            .query_row(
                &format!(
                    "SELECT wallet_pubkey,tx_signature,token,side FROM {table} WHERE order_id=?1"
                ),
                [order_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()
            .context("load current BUY receipt claim")?;
        let Some((wallet, signature, token, side)) = receipt else {
            continue;
        };
        key(&wallet)?;
        key(&signature)?;
        let binding: Option<(String, String, String)> = if order_id.starts_with("exec-canary:owner-buy:") {
            let signature: Option<String> = conn.query_row(
                "SELECT tx_signature FROM orders WHERE order_id=?1", [order_id], |r| r.get(0),
            ).optional()?.flatten();
            signature.map(|sig| {
                crate::rpc_owned_sell_handoff::dispatch::identity::token_side(conn, order_id)
                    .map(|(mint, side)| (sig, mint, side))
            }).transpose()?
        } else { conn
            .query_row(
                "SELECT o.tx_signature,s.token,s.side FROM orders o
             JOIN copy_signals s ON s.signal_id=o.signal_id WHERE o.order_id=?1",
                [order_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .context("load durable BUY receipt binding")? };
        ensure!(
            binding.is_some_and(|(sig, mint, direction)| sig == signature
                && mint == token
                && direction.eq_ignore_ascii_case("buy")
                && side.eq_ignore_ascii_case("buy")),
            "unproven current BUY receipt identity"
        );
        let claim = (wallet, signature);
        ensure!(
            identity.as_ref().is_none_or(|old| old == &claim),
            "conflicting current BUY receipt identity"
        );
        identity = Some(claim);
    }
    let Some((wallet, signature)) = identity else {
        return Ok(());
    };
    for table in tables {
        let mut stmt = conn
            .prepare(&claim_sql(table))
            .context("prepare BUY receipt ownership lookup")?;
        let mut rows = stmt
            .query([&signature])
            .context("lookup BUY receipt ownership")?;
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
            // Other execution wallets with this signature remain independent.
            // Undecodable/empty keys in this signature domain fail above.
            ensure!(
                owner == order_id || claim_wallet != wallet,
                "BUY receipt already claimed by another order"
            );
        }
    }
    Ok(())
}
