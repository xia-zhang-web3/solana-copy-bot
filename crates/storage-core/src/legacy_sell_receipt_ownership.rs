use crate::{sell_receipt_ownership::claim_sql, SellSettlementUnsupported as Unsupported};
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

fn key(value: &str) -> Result<()> {
    // Validate without normalizing historical TEXT keys.
    ensure!(
        !value.trim().is_empty(),
        Unsupported::UnprovenReceiptOwnership
    );
    Ok(())
}

/// Fresh legacy SELL, after fill replay and before any writes on the same conn.
/// No receipt remains unproven legacy/import accounting, not an inferred owner.
pub(crate) fn validate(conn: &Connection, order_id: &str, current_token: &str) -> Result<()> {
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
            // Only this exact migration proves absence is a modern schema error.
            let recorded: Option<String> = conn
                .query_row(
                    "SELECT version FROM schema_migrations WHERE version=?1",
                    [migration],
                    |r| r.get(0),
                )
                .optional()
                .context("load legacy SELL receipt table migration")?;
            ensure!(
                recorded.is_none(),
                "SELL receipt claim table {table} missing after migration {migration}"
            );
            continue;
        };
        ensure!(
            kind == "table",
            "SELL receipt claim table {table} unavailable"
        );
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
            .context("load current legacy SELL receipt claim")?;
        let Some((wallet, signature, token, side)) = receipt else {
            continue;
        };
        key(&wallet)?;
        key(&signature)?;
        let binding: Option<(String, String, String)> = conn
            .query_row(
                "SELECT o.tx_signature,s.token,s.side FROM orders o
             JOIN copy_signals s ON s.signal_id=o.signal_id WHERE o.order_id=?1",
                [order_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .context("load durable legacy SELL receipt binding")?;
        ensure!(
            binding.is_some_and(|(sig, mint, direction)| sig == signature
                && mint == token
                && token == current_token
                && direction.eq_ignore_ascii_case("sell")
                && side.eq_ignore_ascii_case("sell")),
            Unsupported::UnprovenReceiptOwnership
        );
        let claim = (wallet, signature);
        ensure!(
            identity.as_ref().is_none_or(|old| old == &claim),
            Unsupported::UnprovenReceiptOwnership
        );
        identity = Some(claim);
    }
    let Some((wallet, signature)) = identity else {
        return Ok(());
    };
    for table in tables {
        // Reuse the unchanged native SELL query and 0061 signature indexes.
        let mut stmt = conn
            .prepare(&claim_sql(table))
            .context("prepare legacy SELL receipt ownership")?;
        let mut rows = stmt
            .query([&signature])
            .context("lookup legacy SELL receipt ownership")?;
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
            // Pending/orphan claims and the linked order's independent signature
            // remain claims regardless of mint, leader, status, side or generation.
            ensure!(
                owner == order_id || claim_wallet != wallet,
                Unsupported::ReceiptAlreadyClaimed
            );
        }
    }
    Ok(())
}
