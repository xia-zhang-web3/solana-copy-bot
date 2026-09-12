use crate::{source_sell_promotion_guard as guard, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use copybot_core_types::CopySignalRow;

/// Classification only; a read snapshot cannot grant authority across an await.
#[derive(Debug)]
pub enum ExecutionSourceSellGuard {
    NotPromoted(CopySignalRow),
    Allowed(CopySignalRow),
    Refused(&'static str),
}

impl SqliteDiscoveryStore {
    /// Read actual durable identity and both association directions in one snapshot.
    /// The canonical generation/witness implementation is shared with final submit.
    pub fn check_execution_source_sell(&self, signal_id: &str) -> Result<ExecutionSourceSellGuard> {
        let tx = self
            .conn
            .unchecked_transaction()
            .context("begin source SELL classification")?;
        let result = match self.load_copy_signal_by_signal_id(signal_id)? {
            None => ExecutionSourceSellGuard::Refused("source_sell_signal_missing"),
            Some(saved) => match guard::binding_for_signal(self, &tx, signal_id)? {
                Some(binding) => match guard::block_reason(self, &tx, &saved, &binding)? {
                    Some(reason) => ExecutionSourceSellGuard::Refused(reason),
                    None => ExecutionSourceSellGuard::Allowed(saved),
                },
                None => ExecutionSourceSellGuard::NotPromoted(saved),
            },
        };
        tx.commit().context("finish source SELL classification")?;
        Ok(result)
    }
}
