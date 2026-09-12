use crate::SqliteDiscoveryStore;
use anyhow::{ensure, Context, Result};
use rusqlite::{types::ValueRef, OptionalExtension};

const FIRST: &str = "SELECT rowid FROM execution_source_sell_intents ORDER BY rowid DESC LIMIT 1";
const NEXT: &str =
    "SELECT rowid FROM execution_source_sell_intents WHERE rowid<?1 ORDER BY rowid DESC LIMIT 1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionSourceSellStagingVisit {
    /// The checkpoint is committed even if the key/payload cannot be decoded or was deleted.
    Row {
        rowid: i64,
        intent_id: Option<String>,
    },
    /// End of this pass; the next call starts at the current head, including repaired rows.
    Wrapped,
}

impl SqliteDiscoveryStore {
    /// Exactly one raw rowid seek. New arrivals above the checkpoint wait until wrap.
    /// This singleton belongs to the periodic producer, not an execution reservation.
    pub fn advance_execution_source_sell_staging(&self) -> Result<ExecutionSourceSellStagingVisit> {
        let next = self
            .with_immediate_transaction_retry("advance source SELL staging", |conn| {
                let previous: Option<Option<i64>> = conn.query_row(
                "SELECT last_rowid FROM execution_source_sell_staging_cursor WHERE singleton=1",
                [], |r| r.get(0)).optional()?;
                let next: Option<i64> = match previous.flatten() {
                    None => conn.query_row(FIRST, [], |r| r.get(0)).optional()?,
                    Some(id) => conn.query_row(NEXT, [id], |r| r.get(0)).optional()?,
                };
                let changed = conn.execute(
                "INSERT INTO execution_source_sell_staging_cursor(singleton,last_rowid) VALUES(1,?1)
                 ON CONFLICT(singleton) DO UPDATE SET last_rowid=excluded.last_rowid", [next])?;
                ensure!(changed == 1, "staging cursor updated {changed} rows");
                let saved: Option<i64> = conn.query_row(
                    "SELECT last_rowid FROM execution_source_sell_staging_cursor WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )?;
                ensure!(saved == next, "staging cursor changed after write");
                Ok(next)
            })
            .context("checkpoint source SELL staging")?;
        let Some(rowid) = next else {
            return Ok(ExecutionSourceSellStagingVisit::Wrapped);
        };
        // Deliberately outside the checkpoint transaction. SQL/schema/I/O errors propagate;
        // a missing or non-text key is local data, identified by its exact checkpoint rowid.
        let key: Option<Option<String>> = self
            .conn
            .query_row(
                "SELECT intent_id FROM execution_source_sell_intents WHERE rowid=?1",
                [rowid],
                |r| {
                    Ok(match r.get_ref(0)? {
                        ValueRef::Text(bytes) => std::str::from_utf8(bytes).ok().map(str::to_owned),
                        _ => None,
                    })
                },
            )
            .optional()
            .context("read checkpointed staged SELL key")?;
        Ok(ExecutionSourceSellStagingVisit::Row {
            rowid,
            intent_id: key.flatten(),
        })
    }
}
