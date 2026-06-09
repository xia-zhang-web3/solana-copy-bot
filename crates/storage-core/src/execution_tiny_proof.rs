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
        let rows = execution_tiny_proof_rows(self, since, limit)?;
        let recent_orders = execution_tiny_recent_orders(self, since, limit)?;
        let open_positions = execution_tiny_open_positions(self, limit)?;
        let entry_funnel = self.execution_tiny_entry_funnel(since, limit)?;
        Ok(build_report(
            as_of,
            since,
            limit,
            entry_funnel,
            rows,
            recent_orders,
            open_positions,
        ))
    }
}
