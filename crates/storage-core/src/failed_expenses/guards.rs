use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

/// Conflicting execution evidence cannot release existing BUY/same-mint guards.
/// Ordinary failed-fee retrieval does not block an unrelated permitted SELL.
pub(crate) fn conflicting_order(
    conn: &Connection,
    token: Option<&str>,
    except: Option<&str>,
) -> Result<Option<String>> {
    let available: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='execution_failed_expense_tasks')",
        [],
        |r| r.get(0),
    )?;
    if !available {
        return Ok(None);
    }
    Ok(conn
        .query_row(
            "SELECT order_id FROM execution_failed_expense_tasks
        WHERE status='conflict' AND (?1 IS NULL OR token=?1) AND (?2 IS NULL OR order_id!=?2)
        ORDER BY operation_at,order_id LIMIT 1",
            params![token, except],
            |r| r.get(0),
        )
        .optional()?)
}
