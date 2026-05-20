use crate::observed_budget::{interrupted_after_deadline, sqlite_progress_deadline};
use crate::observed_row::parse_sqlite_slot;
use crate::observed_timestamp::parse_rfc3339_utc;
use crate::{
    DiscoveryRuntimeCursor, ObservedSolLegSwap, ObservedSwapCursorPage, SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};
use std::time::Instant;

const PROJECTION_INDEX: &str = "idx_observed_sol_leg_swaps_ts_slot_signature";

pub(crate) fn ensure_observed_sol_leg_projection_schema(
    store: &SqliteDiscoveryStore,
) -> Result<()> {
    store
        .conn
        .execute_batch(OBSERVED_SOL_LEG_PROJECTION_SCHEMA)
        .context("failed ensuring observed SOL-leg projection schema")
}

impl SqliteDiscoveryStore {
    pub fn rebuild_observed_sol_leg_projection_window(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<usize> {
        ensure_observed_sol_leg_projection_schema(self)?;
        crate::observed_timestamp::ensure_observed_swaps_timestamps_canonical_utc_read_only(
            &self.conn,
        )?;
        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();
        let tx = self
            .conn
            .unchecked_transaction()
            .context("failed opening observed SOL-leg projection rebuild transaction")?;
        tx.execute(
            "DELETE FROM observed_sol_leg_swaps
             WHERE ts >= ?1 AND ts <= ?2",
            params![&since_raw, &until_raw],
        )
        .context("failed clearing observed SOL-leg projection window")?;
        let inserted = tx
            .execute(
                "INSERT OR REPLACE INTO observed_sol_leg_swaps(
                    signature, wallet_id, is_buy, token_mint, token_qty, sol_notional, slot, ts
                 )
                 SELECT
                    signature,
                    wallet_id,
                    CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN 1 ELSE 0 END,
                    CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN token_out ELSE token_in END,
                    CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN qty_out ELSE qty_in END,
                    CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN qty_in ELSE qty_out END,
                    slot,
                    ts
                 FROM observed_swaps INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature
                 WHERE ts >= ?1
                   AND ts <= ?2
                   AND (
                       token_in = 'So11111111111111111111111111111111111111112'
                       OR token_out = 'So11111111111111111111111111111111111111112'
                   )",
                params![&since_raw, &until_raw],
            )
            .context("failed rebuilding observed SOL-leg projection window")?;
        tx.execute(
            "INSERT INTO observed_sol_leg_coverage(
                id, covered_from_ts, covered_through_ts, updated_at
             ) VALUES (
                1, ?1, ?2, strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
             )
             ON CONFLICT(id) DO UPDATE SET
                covered_from_ts = excluded.covered_from_ts,
                covered_through_ts = excluded.covered_through_ts,
                updated_at = excluded.updated_at",
            params![&since_raw, &until_raw],
        )
        .context("failed updating observed SOL-leg projection coverage")?;
        tx.commit()
            .context("failed committing observed SOL-leg projection rebuild")?;
        Ok(inserted)
    }

    pub(crate) fn observed_sol_leg_projection_covers_window(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<bool> {
        if !self.sqlite_table_exists("observed_sol_leg_swaps")?
            || !self.sqlite_table_exists("observed_sol_leg_coverage")?
            || !observed_sol_leg_projection_index_is_valid(self)?
        {
            return Ok(false);
        }

        let coverage = self
            .conn
            .query_row(
                "SELECT covered_from_ts, covered_through_ts
                 FROM observed_sol_leg_coverage
                 WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .optional()
            .context("failed loading observed SOL-leg projection coverage")?;
        let Some((Some(from_raw), Some(through_raw))) = coverage else {
            return Ok(false);
        };
        let covered_from =
            parse_rfc3339_utc(&from_raw, "observed_sol_leg_coverage.covered_from_ts")?;
        let covered_through =
            parse_rfc3339_utc(&through_raw, "observed_sol_leg_coverage.covered_through_ts")?;
        if covered_from > since {
            return Ok(false);
        }

        let Some(tail) = self.observed_swaps_tail_cursor_at_or_before_read_only(until)? else {
            return Ok(false);
        };
        Ok(tail.ts_utc < since || covered_through >= tail.ts_utc)
    }

    pub(crate) fn for_each_observed_sol_leg_projection_in_window_after_cursor_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(ObservedSolLegSwap) -> Result<()>,
    {
        if limit == 0 || Instant::now() >= deadline {
            return Ok(ObservedSwapCursorPage {
                rows_seen: 0,
                time_budget_exhausted: Instant::now() >= deadline,
            });
        }
        let limit = limit.min(i64::MAX as usize) as i64;
        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();
        let cursor_ts = cursor.map(|value| value.ts_utc.to_rfc3339());
        let cursor_slot = cursor.map(|value| value.slot as i64);
        let cursor_signature = cursor.map(|value| value.signature.as_str());
        let query = if cursor.is_some() {
            "SELECT signature, wallet_id, is_buy, token_mint, token_qty, sol_notional, slot, ts
             FROM observed_sol_leg_swaps INDEXED BY idx_observed_sol_leg_swaps_ts_slot_signature
             WHERE ts >= ?1
               AND ts <= ?2
               AND (ts, slot, signature) > (?3, ?4, ?5)
             ORDER BY ts ASC, slot ASC, signature ASC
             LIMIT ?6"
        } else {
            "SELECT signature, wallet_id, is_buy, token_mint, token_qty, sol_notional, slot, ts
             FROM observed_sol_leg_swaps INDEXED BY idx_observed_sol_leg_swaps_ts_slot_signature
             WHERE ts >= ?1 AND ts <= ?2
             ORDER BY ts ASC, slot ASC, signature ASC
             LIMIT ?3"
        };

        let _deadline_guard = sqlite_progress_deadline(&self.conn, deadline);
        let mut stmt = match self.conn.prepare(query) {
            Ok(stmt) => stmt,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: 0,
                    time_budget_exhausted: true,
                })
            }
            Err(error) => {
                return Err(error).context("failed preparing observed SOL-leg projection query")
            }
        };
        let mut rows = if let (Some(ts), Some(slot), Some(signature)) =
            (cursor_ts.as_deref(), cursor_slot, cursor_signature)
        {
            match stmt.query(params![since_raw, until_raw, ts, slot, signature, limit]) {
                Ok(rows) => rows,
                Err(error) if interrupted_after_deadline(&error, deadline) => {
                    return Ok(ObservedSwapCursorPage {
                        rows_seen: 0,
                        time_budget_exhausted: true,
                    })
                }
                Err(error) => {
                    return Err(error).context("failed querying observed SOL-leg projection")
                }
            }
        } else {
            match stmt.query(params![since_raw, until_raw, limit]) {
                Ok(rows) => rows,
                Err(error) if interrupted_after_deadline(&error, deadline) => {
                    return Ok(ObservedSwapCursorPage {
                        rows_seen: 0,
                        time_budget_exhausted: true,
                    })
                }
                Err(error) => {
                    return Err(error).context("failed querying observed SOL-leg projection")
                }
            }
        };

        let mut seen = 0usize;
        loop {
            if Instant::now() >= deadline {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: seen,
                    time_budget_exhausted: true,
                });
            }
            let Some(row) = (match rows.next() {
                Ok(row) => row,
                Err(error) if interrupted_after_deadline(&error, deadline) => {
                    return Ok(ObservedSwapCursorPage {
                        rows_seen: seen,
                        time_budget_exhausted: true,
                    });
                }
                Err(error) => {
                    return Err(error).context("failed reading observed SOL-leg projection row")
                }
            }) else {
                break;
            };
            on_swap(projection_row_to_sol_leg_swap(row)?)?;
            seen = seen.saturating_add(1);
        }
        Ok(ObservedSwapCursorPage {
            rows_seen: seen,
            time_budget_exhausted: false,
        })
    }
}

fn projection_row_to_sol_leg_swap(row: &rusqlite::Row<'_>) -> Result<ObservedSolLegSwap> {
    let is_buy: i64 = row.get(2)?;
    let is_buy = match is_buy {
        1 => true,
        0 => false,
        _ => anyhow::bail!("observed_sol_leg_swaps.is_buy is invalid: {is_buy}"),
    };
    let slot_raw: i64 = row.get(6)?;
    let ts_raw: String = row.get(7)?;
    Ok(ObservedSolLegSwap {
        signature: row.get(0)?,
        wallet_id: row.get(1)?,
        is_buy,
        token_mint: row.get(3)?,
        token_qty: row.get(4)?,
        sol_notional: row.get(5)?,
        slot: parse_sqlite_slot(slot_raw, "observed_sol_leg_swaps.slot")?,
        ts_utc: parse_rfc3339_utc(&ts_raw, "observed_sol_leg_swaps.ts")?,
    })
}

fn observed_sol_leg_projection_index_is_valid(store: &SqliteDiscoveryStore) -> Result<bool> {
    let index_flags = store
        .conn
        .query_row(
            "SELECT [unique], partial
             FROM pragma_index_list('observed_sol_leg_swaps')
             WHERE name = ?1",
            [PROJECTION_INDEX],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .context("failed checking observed SOL-leg projection index flags")?;
    let Some((unique, partial)) = index_flags else {
        return Ok(false);
    };
    if unique != 0 || partial != 0 {
        return Ok(false);
    }

    let mut stmt = store
        .conn
        .prepare(&format!(
            "SELECT name, [desc], coll, key
             FROM pragma_index_xinfo('{PROJECTION_INDEX}')
             ORDER BY seqno"
        ))
        .context("failed preparing observed SOL-leg projection index introspection")?;
    let key_columns = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .context("failed querying observed SOL-leg projection index columns")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed collecting observed SOL-leg projection index columns")?
        .into_iter()
        .filter(|(_, _, _, key)| *key != 0)
        .collect::<Vec<_>>();
    let expected = ["ts", "slot", "signature"];
    if key_columns.len() != expected.len() {
        return Ok(false);
    }
    for ((name, desc, coll, _), expected_column) in key_columns.iter().zip(expected) {
        if name.as_deref() != Some(expected_column) {
            return Ok(false);
        }
        if *desc != 0 || coll.as_deref() != Some("BINARY") {
            return Ok(false);
        }
    }
    Ok(true)
}

const OBSERVED_SOL_LEG_PROJECTION_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS observed_sol_leg_swaps (
    signature TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    is_buy INTEGER NOT NULL CHECK(is_buy IN (0, 1)),
    token_mint TEXT NOT NULL,
    token_qty REAL NOT NULL,
    sol_notional REAL NOT NULL,
    slot INTEGER NOT NULL,
    ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observed_sol_leg_swaps_ts_slot_signature
    ON observed_sol_leg_swaps(ts, slot, signature);

CREATE TABLE IF NOT EXISTS observed_sol_leg_coverage (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    covered_from_ts TEXT,
    covered_through_ts TEXT,
    updated_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS observed_swaps_sol_leg_projection_insert
AFTER INSERT ON observed_swaps
WHEN NEW.token_in = 'So11111111111111111111111111111111111111112'
  OR NEW.token_out = 'So11111111111111111111111111111111111111112'
BEGIN
    INSERT OR REPLACE INTO observed_sol_leg_swaps(
        signature, wallet_id, is_buy, token_mint, token_qty, sol_notional, slot, ts
    ) VALUES (
        NEW.signature,
        NEW.wallet_id,
        CASE WHEN NEW.token_in = 'So11111111111111111111111111111111111111112' THEN 1 ELSE 0 END,
        CASE WHEN NEW.token_in = 'So11111111111111111111111111111111111111112' THEN NEW.token_out ELSE NEW.token_in END,
        CASE WHEN NEW.token_in = 'So11111111111111111111111111111111111111112' THEN NEW.qty_out ELSE NEW.qty_in END,
        CASE WHEN NEW.token_in = 'So11111111111111111111111111111111111111112' THEN NEW.qty_in ELSE NEW.qty_out END,
        NEW.slot,
        NEW.ts
    );
END;

CREATE TRIGGER IF NOT EXISTS observed_swaps_sol_leg_projection_delete
AFTER DELETE ON observed_swaps
BEGIN
    DELETE FROM observed_sol_leg_swaps WHERE signature = OLD.signature;
END;

CREATE TRIGGER IF NOT EXISTS observed_swaps_sol_leg_coverage_insert
AFTER INSERT ON observed_swaps
BEGIN
    UPDATE observed_sol_leg_coverage
       SET covered_through_ts = CASE
               WHEN covered_through_ts IS NULL OR NEW.ts > covered_through_ts THEN NEW.ts
               ELSE covered_through_ts
           END,
           updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
     WHERE id = 1;
END;
";
