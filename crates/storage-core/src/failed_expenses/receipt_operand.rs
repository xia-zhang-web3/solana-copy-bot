use super::FailedExpenseReportRow;
use crate::SqliteDiscoveryStore;
use anyhow::{ensure, Context, Result};
use rusqlite::OptionalExtension;

impl SqliteDiscoveryStore {
    /// One selected order's validated fee and ledger knowledge time, from the same
    /// read transaction. This does not establish network truth or history coverage.
    /// Missing/corrupt schema or order is unavailable; an uncovered row has no fee.
    pub fn failed_expense_receipt_operand(
        &self,
        order_id: &str,
    ) -> Result<(FailedExpenseReportRow, Option<String>)> {
        let tx = self.conn.unchecked_transaction()?;
        let operation: String = tx
            .query_row(
                "SELECT submit_ts FROM orders WHERE order_id=?1",
                [order_id],
                |r| r.get(0),
            )
            .optional()?
            .context("failed expense order unavailable")?;
        // Reuse all task/facts/fee/success/conflict checks used by existing readers.
        let row = super::report_row::validated_row(&tx, order_id, operation, true)?;
        let recorded_at: Option<String> = tx
            .query_row(
                "SELECT recorded_at FROM execution_failed_expense_ledger WHERE order_id=?1",
                [order_id],
                |r| r.get(0),
            )
            .optional()?;
        if recorded_at.is_some() {
            // Do not rely solely on schema uniqueness if a damaged DB was supplied.
            let unique: bool = tx.query_row(
                "SELECT COUNT(*)=1 AND NOT EXISTS(
                    SELECT 1 FROM execution_failed_expense_ledger b
                    JOIN execution_failed_expense_ledger a ON a.tx_signature=b.tx_signature
                    WHERE a.order_id=?1 AND b.order_id<>?1)
                 FROM execution_failed_expense_ledger WHERE order_id=?1",
                [order_id],
                |r| r.get(0),
            )?;
            ensure!(unique, "failed expense ledger payment not unique");
        }
        tx.commit()?;
        Ok((row, recorded_at))
    }
}
