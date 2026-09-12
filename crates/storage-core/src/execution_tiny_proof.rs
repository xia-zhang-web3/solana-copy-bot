use crate::{
    execution_tiny_proof_aggregate::build_report,
    execution_tiny_proof_rows::{
        execution_tiny_open_positions, execution_tiny_proof_rows, execution_tiny_recent_orders,
    },
    ExecutionTinyProofReport, SqliteDiscoveryStore,
};
use anyhow::Result;
use chrono::{DateTime, Utc};

impl SqliteDiscoveryStore {
    pub fn execution_tiny_proof_report(
        &self,
        as_of: DateTime<Utc>,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<ExecutionTinyProofReport> {
        let tx = self.conn.unchecked_transaction()?;
        let rows = execution_tiny_proof_rows(self, since, limit)?;
        let recent_orders = execution_tiny_recent_orders(self, since, limit)?;
        let open_positions = execution_tiny_open_positions(self, limit)?;
        let entry_funnel = self.execution_tiny_entry_funnel(since, limit)?;
        let mut report = build_report(
            as_of,
            since,
            limit,
            entry_funnel,
            rows,
            recent_orders,
            open_positions,
        );
        report.cash_settlements =
            crate::execution_cash_settlement_report::on_conn(&tx, since, as_of, limit)?;
        report.failed_expenses = crate::failed_expenses::report::on_conn(&tx, since, as_of, limit)?;
        report.native_observations =
            crate::native_observations::report::on_conn(&tx, since, as_of, limit)?;
        tx.commit()?;
        Ok(report)
    }
}
