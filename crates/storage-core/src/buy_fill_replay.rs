use crate::{
    buy_fill_identity, execution_canary_position_open::load_position_by_id,
    BuyAttributionIssue as Issue, ExecutionCanaryOwnedPosition,
};
use anyhow::{ensure, Result};
use rusqlite::Connection;

/// Existing fill identity is authoritative even after its position has closed.
/// NULL legacy links deliberately fail instead of guessing another generation.
pub(crate) fn load(
    conn: &Connection,
    order_id: &str,
    token: &str,
) -> Result<ExecutionCanaryOwnedPosition> {
    ensure!(
        buy_fill_identity::has_destination(conn)?,
        Issue::LegacySchema
    );
    let (destination, fill_token, basis): (Option<String>, String, String) = conn.query_row(
        "SELECT position_id,token,accounting_basis FROM fills WHERE order_id=?1",
        [order_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    ensure!(
        fill_token == token && basis == "legacy_unclassified",
        Issue::IdentityConflict
    );
    let destination = destination.ok_or(Issue::MissingDestination)?;
    let position = load_position_by_id(conn, &destination)?.ok_or(Issue::DanglingDestination)?;
    buy_fill_identity::position(&position, token)?;
    buy_fill_identity::source(conn, order_id, token)?.ok_or(Issue::MissingOrder)?;
    // Validate any persisted receipt identity too, without requiring facts from
    // compatibility callers that never had them. Such callers remain unproven.
    buy_fill_identity::validate_receipt_if_present(conn, order_id)?;
    // No writes, no quantity/cost additions, no by-mint or timestamp fallback.
    Ok(position)
}
