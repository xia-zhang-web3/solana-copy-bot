use super::FailedExpenseTask;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

/// Called inside the detection/sweep transaction. All receipt attempts share
/// this route sequence, including initial work and pending facts enrichment.
/// Reservation records intended work before I/O, not proof that an RPC occurred.
pub(super) fn reserve(
    conn: &Connection,
    route: &str,
    tasks: &[FailedExpenseTask],
    now: DateTime<Utc>,
) -> Result<()> {
    if tasks.is_empty() {
        return Ok(());
    }
    let mut sequence: i64 = conn
        .query_row(
            "SELECT sequence FROM execution_failed_expense_cursor WHERE route=?1",
            [route],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);
    let reserved_at = now.to_rfc3339();
    for task in tasks {
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("failed expense cursor overflow"))?;
        ensure!(conn.execute(
            "UPDATE execution_failed_expense_tasks SET attempt_seq=?2,last_attempt_at=?3 WHERE order_id=?1 AND route=?4 AND status='pending'",
            params![task.order_id, sequence, reserved_at, route],
        )? == 1, "failed expense cursor task missing");
        let stored: (i64, String) = conn.query_row(
            "SELECT attempt_seq,last_attempt_at FROM execution_failed_expense_tasks WHERE order_id=?1",
            [&task.order_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            stored == (sequence, reserved_at.clone()),
            "failed expense cursor task readback mismatch"
        );
    }
    ensure!(conn.execute(
        "INSERT INTO execution_failed_expense_cursor(route,sequence) VALUES(?1,?2) ON CONFLICT(route) DO UPDATE SET sequence=excluded.sequence",
        params![route, sequence],
    )? == 1, "failed expense cursor write missing");
    let stored: Option<i64> = conn
        .query_row(
            "SELECT sequence FROM execution_failed_expense_cursor WHERE route=?1",
            [route],
            |r| r.get(0),
        )
        .optional()?;
    ensure!(
        stored == Some(sequence),
        "failed expense cursor readback mismatch"
    );
    Ok(())
}
