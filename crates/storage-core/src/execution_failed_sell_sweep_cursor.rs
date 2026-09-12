use crate::{SqliteDiscoveryStore, EXECUTION_STATUS_CANARY_FAILED};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, OptionalExtension};

// The existing (status,submit_ts) index includes rowid as its final stable row key.
// Visit raw FAILED rows before route/eligibility filters: even an ineligible prefix
// consumes a bounded number of seeks and can never hide an unbounded scan in LIMIT.
const FIRST_SQL: &str = "SELECT rowid,submit_ts,order_id,route FROM orders
    INDEXED BY idx_orders_status_submit_ts WHERE status=?1
    ORDER BY submit_ts DESC,rowid DESC LIMIT 1";
const SAME_TIME_SQL: &str = "SELECT rowid,submit_ts,order_id,route FROM orders
    INDEXED BY idx_orders_status_submit_ts WHERE status=?1 AND submit_ts=?2 AND rowid<?3
    ORDER BY rowid DESC LIMIT 1";
const OLDER_SQL: &str = "SELECT rowid,submit_ts,order_id,route FROM orders
    INDEXED BY idx_orders_status_submit_ts WHERE status=?1 AND submit_ts<?2
    ORDER BY submit_ts DESC,rowid DESC LIMIT 1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionFailedSellSweepVisit {
    Order(String),
    SkippedOtherRoute,
    /// End of this pass. The next call starts a new pass, including repaired rows.
    Wrapped,
}

impl SqliteDiscoveryStore {
    /// Advance exactly one raw row, durably, before returning it for fresh validation.
    /// A vanished row needs no lookup to resume; this cursor grants no execution proof.
    pub fn advance_execution_failed_sell_sweep(
        &self,
        route: &str,
    ) -> Result<ExecutionFailedSellSweepVisit> {
        ensure!(!route.trim().is_empty(), "failed SELL sweep route missing");
        self.with_immediate_transaction_retry("advance failed SELL sweep", |conn| {
            let previous: Option<(Option<String>, Option<i64>)> = conn.query_row(
                "SELECT last_submit_ts,last_rowid FROM execution_failed_sell_sweep_cursors WHERE route=?1",
                [route], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;
            let previous = previous.unwrap_or_default();
            let map = |row: &rusqlite::Row<'_>| -> rusqlite::Result<(i64,String,String,String)> {
                Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))
            };
            let raw = match previous {
                (None,None) => conn.query_row(FIRST_SQL, [EXECUTION_STATUS_CANARY_FAILED], map).optional()?,
                (Some(ts),Some(id)) => {
                    // Split the keyset seek: the bundled SQLite does not optimize the
                    // tuple comparison's implicit-rowid boundary within large time ties.
                    let same = conn.query_row(SAME_TIME_SQL, params![EXECUTION_STATUS_CANARY_FAILED,ts,id], map).optional()?;
                    match same {
                        Some(row) => Some(row),
                        None => conn.query_row(OLDER_SQL, params![EXECUTION_STATUS_CANARY_FAILED,ts], map).optional()?,
                    }
                },
                _ => anyhow::bail!("inconsistent failed SELL sweep cursor"),
            };
            let point = raw.as_ref().map(|(id,ts,_,_)| (Some(ts.clone()),Some(*id))).unwrap_or_default();
            let updated = conn.execute("INSERT INTO execution_failed_sell_sweep_cursors(route,last_submit_ts,last_rowid)
                VALUES(?1,?2,?3) ON CONFLICT(route) DO UPDATE SET last_submit_ts=excluded.last_submit_ts,last_rowid=excluded.last_rowid",
                params![route,point.0,point.1])?;
            ensure!(updated==1, "failed SELL sweep cursor updated {updated} rows");
            let saved = conn.query_row("SELECT last_submit_ts,last_rowid FROM execution_failed_sell_sweep_cursors WHERE route=?1",
                [route], |row| Ok((row.get::<_,Option<String>>(0)?, row.get::<_,Option<i64>>(1)?)))?;
            ensure!(saved==point, "failed SELL sweep cursor changed after write");
            Ok(match raw {
                None => ExecutionFailedSellSweepVisit::Wrapped,
                Some((_,_,id,actual_route)) if actual_route==route => ExecutionFailedSellSweepVisit::Order(id),
                Some(_) => ExecutionFailedSellSweepVisit::SkippedOtherRoute,
            })
        }).context("failed advancing durable SELL sweep")
    }
}
