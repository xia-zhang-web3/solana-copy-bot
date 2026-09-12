use crate::{source_sell_handoff_rows as rows, SourceSellHandoff, SqliteDiscoveryStore};
use anyhow::{ensure, Context, Result};
use rusqlite::OptionalExtension;
const FIRST: &str = "SELECT sequence FROM source_sell_handoffs WHERE disposition='pending' ORDER BY sequence DESC LIMIT 1";
const NEXT: &str = "SELECT sequence FROM source_sell_handoffs WHERE disposition='pending' AND sequence<?1 ORDER BY sequence DESC LIMIT 1";

impl SqliteDiscoveryStore {
    /// One indexed pending visit; checkpoint commit precedes payload/proof work.
    /// None is a committed wrap, never proof that no job can arrive later.
    pub fn advance_source_sell_handoff(&self) -> Result<Option<SourceSellHandoff>> {
        let sequence =
            self.with_immediate_transaction_retry("advance source SELL handoff", |conn| {
                crate::source_sell_handoff_schema::required(conn)?;
                let previous: Option<Option<i64>> = conn
                    .query_row(
                        "SELECT last_sequence FROM source_sell_handoff_cursor WHERE singleton=1",
                        [],
                        |r| r.get(0),
                    )
                    .optional()?;
                let next: Option<i64> = match previous.flatten() {
                    Some(id) => conn.query_row(NEXT, [id], |r| r.get(0)).optional()?,
                    None => conn.query_row(FIRST, [], |r| r.get(0)).optional()?,
                };
                let n = conn.execute(
                    "INSERT INTO source_sell_handoff_cursor(singleton,last_sequence) VALUES(1,?1)
                ON CONFLICT(singleton) DO UPDATE SET last_sequence=excluded.last_sequence",
                    [next],
                )?;
                ensure!(n == 1, "handoff cursor did not update");
                let saved: Option<i64> = conn.query_row(
                    "SELECT last_sequence FROM source_sell_handoff_cursor WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )?;
                ensure!(saved == next, "handoff cursor changed after write");
                Ok(next)
            })?;
        let Some(sequence) = sequence else {
            return Ok(None);
        };
        let signature: String = self
            .conn
            .query_row(
                "SELECT signature FROM source_sell_handoffs WHERE sequence=?1",
                [sequence],
                |r| r.get(0),
            )
            .context("read checkpointed source SELL handoff")?;
        rows::load(&self.conn, &signature)?
            .map(Some)
            .context("checkpointed handoff disappeared")
    }
}
