use crate::{
    buy_fill_identity, execution_canary_position_open::load_open_position_by_token,
    receipt_facts_identity::validate_identity, receipt_facts_rows,
    BuyAttributionCoverage as Coverage, BuyAttributionIssue as Issue,
    ExecutionCanaryBuyAttribution as Attribution, OpenPositionBuyAttribution, ProvenBuyContributor,
    ReceiptFactsIdentityRejection, SqliteDiscoveryStore, UnprovenBuyLink,
    EXECUTION_STATUS_CANARY_CONFIRMED,
};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection};

impl SqliteDiscoveryStore {
    /// Snapshot of durable BUY contributors to the current open generation.
    /// Read-only; unknown/unlinked history is never assigned by mint or chronology.
    pub fn load_execution_canary_buy_attribution(&self, token: &str) -> Result<Attribution> {
        let tx = self
            .conn
            .unchecked_transaction()
            .context("begin BUY attribution snapshot")?;
        let result = read_on_conn(&tx, token)?;
        tx.commit().context("finish BUY attribution snapshot")?;
        Ok(result)
    }
}

pub(crate) fn read_on_conn(conn: &Connection, token: &str) -> Result<Attribution> {
    let Some(position) = load_open_position_by_token(conn, token)? else {
        return Ok(Attribution::NoOpenPosition);
    };
    let mut result = OpenPositionBuyAttribution {
        position_id: position.position_id.clone(),
        token: position.token.clone(),
        proven_contributors: Vec::new(),
        coverage: Coverage::Unknown,
        unproven_links: Vec::new(),
    };
    if !buy_fill_identity::has_destination(conn)? {
        result.unproven_links.push(UnprovenBuyLink {
            order_id: None,
            reason: Issue::LegacySchema,
        });
        return Ok(Attribution::Open(result));
    }
    let collisions = crate::buy_receipt_collisions::load(conn)?;
    // Include unassigned/dangling same-token rows only as diagnostics. Rows linked
    // to another existing generation never become contributors to this position.
    // Cash SELLs are not BUY attribution candidates, regardless of position_id.
    let mut stmt = conn.prepare(
        "SELECT f.id,f.order_id,f.position_id,f.token,f.qty_raw,f.qty_decimals,f.notional_lamports,
            p.position_id,f.accounting_basis
         FROM fills f LEFT JOIN positions p ON p.position_id=f.position_id
         WHERE (f.position_id=?1 OR (f.token=?2 AND (f.position_id IS NULL OR p.position_id IS NULL)))
           AND f.accounting_basis!='receipt_native_cash'
         ORDER BY f.id")?;
    let mut rows = stmt.query(params![position.position_id, token])?;
    while let Some(row) = rows.next()? {
        let id: String = row.get(1)?;
        let destination: Option<String> = row.get(2)?;
        let exists: Option<String> = row.get(7)?;
        let candidate = (|| {
            let destination = destination.ok_or(Issue::MissingDestination)?;
            ensure!(exists.is_some(), Issue::DanglingDestination);
            ensure!(destination == position.position_id, Issue::PositionConflict);
            let fill_token: String = row.get(3)?;
            let basis: String = row.get(8)?;
            ensure!(
                fill_token == token && basis == "legacy_unclassified",
                Issue::IdentityConflict
            );
            let source = buy_fill_identity::source(conn, &id, token)?.ok_or_else(|| {
                if id.starts_with("exec-canary:owner-buy:") {
                    Issue::OwnerTechnicalBuyHasNoSourceWallet
                } else {
                    Issue::MissingOrder
                }
            })?;
            ensure!(
                source.status == EXECUTION_STATUS_CANARY_CONFIRMED,
                Issue::NotConfirmed
            );
            let facts = receipt_facts_rows::load(conn, &id)?.ok_or(Issue::MissingReceiptFacts)?;
            validate_identity(conn, &facts)?;
            ensure!(
                !collisions.contains(&(facts.wallet_pubkey.clone(), facts.tx_signature.clone())),
                Issue::AmbiguousReceipt
            );
            let raw: Option<String> = row.get(4)?;
            let decimals: Option<u8> = row.get(5)?;
            let cost: Option<i64> = row.get(6)?;
            ensure!(
                facts.token == token && facts.side == "buy",
                Issue::ReceiptIdentityConflict
            );
            ensure!(
                facts.token_delta.is_some_and(|d| d.raw > 0
                    && raw.as_deref() == Some(d.raw.to_string().as_str())
                    && decimals == Some(d.decimals))
                    && cost.is_some_and(
                        |c| c > 0 && facts.wallet_native_delta.as_i128() == -i128::from(c)
                    ),
                Issue::ReceiptOperandsConflict
            );
            Ok(ProvenBuyContributor {
                fill_id: row.get(0)?,
                order_id: id.clone(),
                signal_id: source.signal_id,
                source_wallet: source.wallet_id,
                tx_signature: facts.tx_signature,
            })
        })();
        match candidate {
            Ok(value) => result.proven_contributors.push(value),
            Err(error) => {
                let reason = if let Some(reason) = error.downcast_ref::<Issue>() {
                    *reason
                } else if error.is::<ReceiptFactsIdentityRejection>() {
                    Issue::ReceiptIdentityConflict
                } else {
                    return Err(error);
                }; // SQL/corruption errors never become an empty success.
                result.unproven_links.push(UnprovenBuyLink {
                    order_id: Some(id),
                    reason,
                });
            }
        }
    }
    if result.proven_contributors.is_empty() {
        result.unproven_links.push(UnprovenBuyLink {
            order_id: None,
            reason: Issue::NoProvenContributors,
        });
    } else {
        result.coverage = Coverage::ProvenSubset;
    }
    Ok(Attribution::Open(result))
}
