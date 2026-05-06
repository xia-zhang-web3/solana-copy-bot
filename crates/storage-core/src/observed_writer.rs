use crate::{ObservedSwapBatchWriteMetrics, SqliteBatchedDeleteSummary, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::time::Instant;

struct WalletActivityDayRow {
    wallet_id: String,
    activity_day: NaiveDate,
    last_seen: DateTime<Utc>,
}

impl SqliteDiscoveryStore {
    pub fn ensure_observed_swap_writer_tables(&self) -> Result<()> {
        ensure_observed_swap_writer_tables_on_conn(&self.conn)
    }

    pub fn insert_observed_swaps_batch_with_activity_days(
        &self,
        swaps: &[SwapEvent],
    ) -> Result<Vec<bool>> {
        Ok(self
            .insert_observed_swaps_batch_with_activity_days_measured(swaps)?
            .inserted)
    }

    pub fn insert_observed_swaps_batch_with_activity_days_measured(
        &self,
        swaps: &[SwapEvent],
    ) -> Result<ObservedSwapBatchWriteMetrics> {
        if swaps.is_empty() {
            return Ok(ObservedSwapBatchWriteMetrics {
                inserted: Vec::new(),
                observed_swaps_insert_ms: 0,
                wallet_activity_days_upsert_ms: 0,
            });
        }

        self.ensure_observed_swap_writer_tables()?;
        self.with_immediate_transaction_retry("observed swap batch write", |conn| {
            let observed_swaps_insert_started = Instant::now();
            let mut stmt = conn
                .prepare_cached(
                    "INSERT OR IGNORE INTO observed_swaps(
                        signature,
                        wallet_id,
                        dex,
                        token_in,
                        token_out,
                        qty_in,
                        qty_out,
                        qty_in_raw,
                        qty_in_decimals,
                        qty_out_raw,
                        qty_out_decimals,
                        slot,
                        ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                )
                .context("failed to prepare observed swap batch insert statement")?;

            let mut inserted = Vec::with_capacity(swaps.len());
            let mut activity_dedup = HashMap::<(String, NaiveDate), DateTime<Utc>>::new();
            for swap in swaps {
                let changed = stmt
                    .execute(params![
                        &swap.signature,
                        &swap.wallet,
                        &swap.dex,
                        &swap.token_in,
                        &swap.token_out,
                        swap.amount_in,
                        swap.amount_out,
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| value.amount_in_raw.as_str()),
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| i64::from(value.amount_in_decimals)),
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| value.amount_out_raw.as_str()),
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| i64::from(value.amount_out_decimals)),
                        swap.slot as i64,
                        swap.ts_utc.to_rfc3339(),
                    ])
                    .context("failed to insert observed swap in batch write")?;
                let was_inserted = changed > 0;
                inserted.push(was_inserted);
                if was_inserted {
                    activity_dedup
                        .entry((swap.wallet.clone(), swap.ts_utc.date_naive()))
                        .and_modify(|current| {
                            if swap.ts_utc > *current {
                                *current = swap.ts_utc;
                            }
                        })
                        .or_insert(swap.ts_utc);
                }
            }
            let observed_swaps_insert_ms = elapsed_ms(observed_swaps_insert_started);

            let activity_rows = activity_dedup
                .into_iter()
                .map(
                    |((wallet_id, activity_day), last_seen)| WalletActivityDayRow {
                        wallet_id,
                        activity_day,
                        last_seen,
                    },
                )
                .collect::<Vec<_>>();
            let wallet_activity_days_started = Instant::now();
            upsert_wallet_activity_days_on_conn(conn, &activity_rows)?;
            let wallet_activity_days_upsert_ms = elapsed_ms(wallet_activity_days_started);

            Ok(ObservedSwapBatchWriteMetrics {
                inserted,
                observed_swaps_insert_ms,
                wallet_activity_days_upsert_ms,
            })
        })
        .context("failed to insert observed swap batch with activity days")
    }

    pub fn delete_observed_swaps_before_batch(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<usize> {
        let cutoff_ts = cutoff.to_rfc3339();
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        self.execute_with_retry(|conn| {
            conn.execute(
                "DELETE FROM observed_swaps
                 WHERE rowid IN (
                     SELECT rowid
                     FROM observed_swaps
                     WHERE ts < ?1
                     ORDER BY ts ASC, slot ASC, signature ASC
                     LIMIT ?2
                 )",
                params![&cutoff_ts, batch_limit],
            )
        })
        .context("failed to delete observed swap retention slice")
    }

    pub fn delete_observed_swaps_before_batched(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<SqliteBatchedDeleteSummary> {
        let mut summary = SqliteBatchedDeleteSummary::default();
        loop {
            let deleted = self.delete_observed_swaps_before_batch(cutoff, batch_size)?;
            if deleted == 0 {
                break;
            }
            summary.deleted_rows = summary.deleted_rows.saturating_add(deleted);
            summary.batches = summary.batches.saturating_add(1);
        }
        Ok(summary)
    }

    pub fn checkpoint_wal_truncate(&self) -> Result<(i64, i64, i64)> {
        self.execute_with_retry_result(|conn| {
            let mut stmt = conn.prepare("PRAGMA wal_checkpoint(TRUNCATE)")?;
            stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        })
        .context("failed to checkpoint sqlite wal")
    }

    pub fn checkpoint_wal_passive(&self) -> Result<(i64, i64, i64)> {
        self.execute_with_retry_result(|conn| {
            let mut stmt = conn.prepare("PRAGMA wal_checkpoint(PASSIVE)")?;
            stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        })
        .context("failed to checkpoint sqlite wal in passive mode")
    }
}

fn ensure_observed_swap_writer_tables_on_conn(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS observed_swaps (
            signature TEXT PRIMARY KEY,
            wallet_id TEXT NOT NULL,
            dex TEXT NOT NULL,
            token_in TEXT NOT NULL,
            token_out TEXT NOT NULL,
            qty_in REAL NOT NULL,
            qty_out REAL NOT NULL,
            qty_in_raw TEXT,
            qty_in_decimals INTEGER,
            qty_out_raw TEXT,
            qty_out_decimals INTEGER,
            slot INTEGER NOT NULL,
            ts TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_observed_swaps_ts_slot_signature
            ON observed_swaps(ts, slot, signature);
        CREATE TABLE IF NOT EXISTS wallet_activity_days (
            wallet_id TEXT NOT NULL,
            activity_day TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            PRIMARY KEY (wallet_id, activity_day)
        );
        CREATE INDEX IF NOT EXISTS idx_wallet_activity_days_day_wallet
            ON wallet_activity_days(activity_day, wallet_id);",
    )
    .context("failed ensuring observed swap writer tables exist")
}

fn upsert_wallet_activity_days_on_conn(
    conn: &Connection,
    rows: &[WalletActivityDayRow],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO wallet_activity_days(wallet_id, activity_day, last_seen)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
                last_seen = CASE
                    WHEN excluded.last_seen > wallet_activity_days.last_seen
                        THEN excluded.last_seen
                    ELSE wallet_activity_days.last_seen
                END",
        )
        .context("failed to prepare wallet_activity_days upsert statement")?;
    for row in rows {
        stmt.execute(params![
            &row.wallet_id,
            row.activity_day.format("%Y-%m-%d").to_string(),
            row.last_seen.to_rfc3339(),
        ])
        .context("failed to upsert wallet_activity_days row")?;
    }
    Ok(())
}

fn elapsed_ms(started: Instant) -> u64 {
    let micros = started.elapsed().as_micros();
    if micros == 0 {
        0
    } else {
        micros.div_ceil(1000).min(u128::from(u64::MAX)) as u64
    }
}
