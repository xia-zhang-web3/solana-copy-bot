use crate::failed_expenses::report::consume_rows;
use crate::FailedExpenseReport;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::Connection;

pub(super) fn read(
    conn: &Connection,
    since: DateTime<Utc>,
    as_of: DateTime<Utc>,
) -> Result<FailedExpenseReport> {
    let has_schema: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='execution_failed_expense_tasks')",
        [],
        |r| r.get(0),
    )?;
    // Runtime cohort is canary-only even when a task exists. Public legacy report keeps its cohort.
    let sql = if has_schema {
        "SELECT o.order_id,o.submit_ts FROM orders o
         LEFT JOIN execution_failed_expense_tasks t ON t.order_id=o.order_id
         WHERE o.order_id LIKE 'exec-canary:%' AND (t.order_id IS NOT NULL OR
         (o.status='execution_canary_failed' AND LENGTH(TRIM(o.tx_signature))>0))
         ORDER BY o.submit_ts,o.order_id"
    } else {
        "SELECT order_id,submit_ts FROM orders WHERE order_id LIKE 'exec-canary:%'
         AND status='execution_canary_failed' AND LENGTH(TRIM(tx_signature))>0
         ORDER BY submit_ts,order_id"
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    let mut selected = Vec::new();
    for row in rows {
        let (id, raw) = row?;
        let submitted = DateTime::parse_from_rfc3339(&raw)?.with_timezone(&Utc);
        ensure!(since <= as_of, "invalid entry cost window");
        if submitted >= since && submitted < as_of {
            selected.push(Ok((id, raw)));
        }
    }
    let mut report = consume_rows(conn, since, as_of, 0, has_schema, selected)?;
    report.window_basis = "original_order_submit_ts_parsed_UTC_[since,as_of)_task_timestamp_validated_after_selection".into();
    Ok(report)
}
