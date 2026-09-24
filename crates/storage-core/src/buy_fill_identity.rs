use crate::{
    BuyAttributionIssue as Issue, ExecutionCanaryOwnedPosition,
    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
};
use anyhow::{ensure, Result};
use rusqlite::{Connection, OptionalExtension};

pub(crate) struct Source {
    pub signal_id: String,
    pub wallet_id: String,
    pub status: String,
}

/// Missing orders remain supported by the legacy orphan/import writer. They are
/// never evidence of a source. Present but contradictory chains must fail atomically.
pub(crate) fn source(conn: &Connection, order_id: &str, token: &str) -> Result<Option<Source>> {
    let order: Option<(String, String)> = conn
        .query_row(
            "SELECT signal_id,status FROM orders WHERE order_id=?1",
            [order_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((signal_id, status)) = order else {
        return Ok(None);
    };
    if order_id.starts_with("exec-canary:owner-buy:") {
        let (saved_token, side)=crate::rpc_owned_sell_handoff::dispatch::identity::token_side(conn,order_id)?;
        ensure!(saved_token==token && side=="buy"
            && signal_id.starts_with("owner-buy:"),Issue::IdentityConflict);
        // An owner BUY has no leader/source wallet. The fill remains linked to its
        // explicit order origin; source-triggered SELL attribution stays unavailable.
        return Ok(None);
    }
    let (wallet_id, signal_token, side): (String, String, String) = conn
        .query_row(
            "SELECT wallet_id,token,side FROM copy_signals WHERE signal_id=?1",
            [&signal_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?
        .ok_or(Issue::MissingSignal)?;
    ensure!(
        signal_token == token && side.eq_ignore_ascii_case("buy"),
        Issue::IdentityConflict
    );
    ensure!(!wallet_id.trim().is_empty(), Issue::MissingSourceWallet);
    Ok(Some(Source {
        signal_id,
        wallet_id,
        status,
    }))
}

pub(crate) fn position(position: &ExecutionCanaryOwnedPosition, token: &str) -> Result<()> {
    ensure!(
        position.token == token
            && position.accounting_bucket == EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET
            && matches!(position.state.as_str(), "open" | "closed"),
        Issue::PositionConflict
    );
    Ok(())
}

/// Compatibility with fixtures/importers executing against pre-0055 schemas.
/// This never creates or backfills a column; migrated production always has it.
pub(crate) fn has_destination(conn: &Connection) -> Result<bool> {
    Ok(conn
        .prepare("SELECT 1 FROM pragma_table_info('fills') WHERE name='position_id'")?
        .exists([])?)
}

/// Check available durable receipts without manufacturing facts for legacy imports.
pub(crate) fn validate_receipt_if_present(conn: &Connection, order_id: &str) -> Result<()> {
    if conn.prepare("SELECT 1 FROM sqlite_master WHERE name='execution_canary_receipt_facts' AND type='table'")?.exists([])? {
        if let Some(facts) = crate::receipt_facts_rows::load(conn, order_id)? {
            crate::receipt_facts_identity::validate_identity(conn, &facts)?;
        }
    }
    Ok(())
}
