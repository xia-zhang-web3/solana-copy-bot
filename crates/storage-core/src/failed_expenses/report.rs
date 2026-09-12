use super::FailedExpenseReport;
use crate::SqliteDiscoveryStore;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

impl SqliteDiscoveryStore {
    pub fn execution_failed_expense_report(
        &self,
        since: DateTime<Utc>,
        as_of: DateTime<Utc>,
        limit: u32,
    ) -> Result<FailedExpenseReport> {
        let tx = self.conn.unchecked_transaction()?;
        let report = on_conn(&tx, since, as_of, limit)?;
        tx.commit()?;
        Ok(report)
    }
}
pub(crate) fn on_conn(
    conn: &Connection,
    since: DateTime<Utc>,
    as_of: DateTime<Utc>,
    limit: u32,
) -> Result<FailedExpenseReport> {
    ensure!(since <= as_of, "failed expense report invalid window");
    let report = FailedExpenseReport {
        since: since.to_rfc3339(),
        as_of: as_of.to_rfc3339(),
        ..Default::default()
    };
    let has_schema: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='execution_failed_expense_tasks')",
        [],
        |r| r.get(0),
    )?;
    let query = if has_schema {
        "SELECT o.order_id,COALESCE(t.operation_at,o.submit_ts) FROM orders o
         LEFT JOIN execution_failed_expense_tasks t ON t.order_id=o.order_id
         WHERE (t.order_id IS NOT NULL OR (o.order_id LIKE 'exec-canary:%' AND o.status='execution_canary_failed' AND LENGTH(TRIM(o.tx_signature))>0))
         AND julianday(COALESCE(t.operation_at,o.submit_ts))>=julianday(?1)
         AND julianday(COALESCE(t.operation_at,o.submit_ts))<julianday(?2)
         ORDER BY COALESCE(t.operation_at,o.submit_ts),o.order_id"
    } else {
        "SELECT order_id,submit_ts FROM orders WHERE order_id LIKE 'exec-canary:%'
         AND status='execution_canary_failed' AND LENGTH(TRIM(tx_signature))>0
         AND julianday(submit_ts)>=julianday(?1) AND julianday(submit_ts)<julianday(?2)
         ORDER BY submit_ts,order_id"
    };
    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map(params![report.since, report.as_of], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    consume_rows(conn, since, as_of, limit, has_schema, rows)
}

pub(crate) fn consume_rows(
    conn: &Connection,
    since: DateTime<Utc>,
    as_of: DateTime<Utc>,
    limit: u32,
    has_schema: bool,
    rows: impl IntoIterator<Item = rusqlite::Result<(String, String)>>,
) -> Result<FailedExpenseReport> {
    let mut report = FailedExpenseReport {
        since: since.to_rfc3339(),
        as_of: as_of.to_rfc3339(),
        ..Default::default()
    };
    let (mut fee_sum, mut native_sum, mut residual_sum) = (0_u128, 0_i128, 0_i128);
    let (mut native_count, mut residual_count) = (0_u64, 0_u64);
    for row in rows {
        let (id, operation_at) = row?;
        let sample = super::report_row::validated_row(conn, &id, operation_at, has_schema)?;
        report.total_orders += 1;
        report.legacy_uncovered_orders += u64::from(sample.task.is_none());
        if let Some(fee) = &sample.wallet_fee_lamports {
            fee_sum = fee_sum
                .checked_add(fee.parse::<u128>()?)
                .context("failed expense fee sum overflow")?;
            report.known_orders += 1;
        }
        if let Some(value) = &sample.native_delta_lamports {
            native_sum = native_sum
                .checked_add(value.parse::<i128>()?)
                .context("failed expense native sum overflow")?;
            native_count += 1;
        }
        if let Some(value) = &sample.unexplained_delta_lamports {
            residual_sum = residual_sum
                .checked_add(value.parse::<i128>()?)
                .context("failed expense residual sum overflow")?;
            residual_count += 1;
        }
        if sample.wallet_fee_lamports.is_none()
            || sample.unexplained_delta_lamports.as_deref() != Some("0")
        {
            report.unresolved_orders += 1;
        }
        if report.rows.len() < limit as usize {
            report.rows.push(sample);
        }
    }
    report.unknown_orders = report.total_orders - report.known_orders;
    if report.known_orders > 0 {
        report.known_wallet_fee_lamports = Some(fee_sum.to_string());
    }
    if native_count > 0 {
        report.known_native_delta_lamports = Some(native_sum.to_string());
    }
    if residual_count > 0 {
        report.known_unexplained_delta_lamports = Some(residual_sum.to_string());
    }
    report.coverage = if !has_schema {
        "schema_unavailable"
    } else if report.total_orders == 0 {
        "empty_unknown"
    } else if report.unresolved_orders > 0 {
        "partial_unresolved"
    } else {
        "complete_selected_cohort"
    }
    .into();
    if report.coverage == "complete_selected_cohort" {
        report.cohort_wallet_fee_lamports = report.known_wallet_fee_lamports.clone();
    }
    report.rows_truncated = report.total_orders > report.rows.len() as u64;
    Ok(report)
}
