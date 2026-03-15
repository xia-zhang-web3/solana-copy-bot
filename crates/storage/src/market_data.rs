use crate::{
    discovery::upsert_wallet_activity_days_on_conn, DiscoveryRuntimeCursor,
    ObservedSwapBatchWriteMetrics, SqliteBatchedDeleteSummary, SqliteStore, TokenMarketStats,
    TokenQualityCacheRow, TokenQualityRpcRow, WalletActivityDayRow,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use reqwest::blocking::Client;
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::time::{Duration as StdDuration, Instant};

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const OBSERVED_SWAP_CURSOR_PROGRESS_OPS: i32 = 2_000;

#[derive(Debug, Clone, Copy, Default)]
pub struct ObservedSwapCursorPage {
    pub rows_seen: usize,
    pub time_budget_exhausted: bool,
}

struct ProgressHandlerGuard<'a> {
    conn: &'a Connection,
}

impl<'a> ProgressHandlerGuard<'a> {
    fn install(conn: &'a Connection, deadline: Instant) -> Self {
        conn.progress_handler(
            OBSERVED_SWAP_CURSOR_PROGRESS_OPS,
            Some(move || Instant::now() >= deadline),
        );
        Self { conn }
    }
}

impl Drop for ProgressHandlerGuard<'_> {
    fn drop(&mut self) {
        self.conn.progress_handler(0, None::<fn() -> bool>);
    }
}

impl SqliteStore {
    pub fn insert_observed_swap(&self, swap: &SwapEvent) -> Result<bool> {
        let written = self
            .execute_with_retry(|conn| {
                conn.execute(
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
                    params![
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
                    ],
                )
            })
            .context("failed to insert observed swap")?;
        Ok(written > 0)
    }

    pub fn insert_observed_swaps_batch(&self, swaps: &[SwapEvent]) -> Result<Vec<bool>> {
        if swaps.is_empty() {
            return Ok(Vec::new());
        }

        self.with_immediate_transaction_retry("observed swap batch write", |conn| {
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
                inserted.push(changed > 0);
            }
            Ok(inserted)
        })
        .context("failed to insert observed swap batch")
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
            let mut activity_rows = Vec::new();
            let mut activity_dedup = std::collections::HashMap::<
                (String, chrono::NaiveDate),
                chrono::DateTime<Utc>,
            >::new();
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
                    let key = (swap.wallet.clone(), swap.ts_utc.date_naive());
                    activity_dedup
                        .entry(key)
                        .and_modify(|current| {
                            if swap.ts_utc > *current {
                                *current = swap.ts_utc;
                            }
                        })
                        .or_insert(swap.ts_utc);
                }
            }
            let observed_swaps_insert_ms =
                duration_ms_ceil(observed_swaps_insert_started.elapsed());

            activity_rows.extend(activity_dedup.into_iter().map(
                |((wallet_id, activity_day), last_seen)| WalletActivityDayRow {
                    wallet_id,
                    activity_day,
                    last_seen,
                },
            ));
            let wallet_activity_days_started = Instant::now();
            upsert_wallet_activity_days_on_conn(conn, &activity_rows)?;
            let wallet_activity_days_upsert_ms =
                duration_ms_ceil(wallet_activity_days_started.elapsed());
            Ok(ObservedSwapBatchWriteMetrics {
                inserted,
                observed_swaps_insert_ms,
                wallet_activity_days_upsert_ms,
            })
        })
        .context("failed to insert observed swap batch with activity days")
    }

    pub fn delete_observed_swaps_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.delete_observed_swaps_before_batched(cutoff, usize::MAX)
            .map(|summary| summary.deleted_rows)
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
            summary.deleted_rows += deleted;
            summary.batches += 1;
        }
        Ok(summary)
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

    pub fn load_observed_swaps_since(&self, since: DateTime<Utc>) -> Result<Vec<SwapEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1
                 ORDER BY ts ASC, slot ASC",
            )
            .context("failed to prepare observed_swaps load query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339()])
            .context("failed to query observed_swaps")?;

        let mut swaps = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating observed_swaps rows")?
        {
            swaps.push(Self::row_to_swap_event(row)?);
        }

        Ok(swaps)
    }

    pub fn oldest_observed_swap_timestamp(&self) -> Result<Option<DateTime<Utc>>> {
        let raw: Option<String> = self
            .conn
            .query_row("SELECT MIN(ts) FROM observed_swaps", [], |row| row.get(0))
            .optional()
            .context("failed querying oldest observed_swaps timestamp")?
            .flatten();
        raw.map(|raw| {
            DateTime::parse_from_rfc3339(&raw)
                .map(|ts| ts.with_timezone(&Utc))
                .with_context(|| format!("invalid observed_swaps.ts rfc3339 value: {raw}"))
        })
        .transpose()
    }

    pub fn for_each_observed_swap_since<F>(
        &self,
        since: DateTime<Utc>,
        mut on_swap: F,
    ) -> Result<usize>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1
                 ORDER BY ts ASC, slot ASC",
            )
            .context("failed to prepare observed_swaps streaming query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339()])
            .context("failed to stream observed_swaps rows")?;

        let mut seen = 0usize;
        while let Some(row) = rows
            .next()
            .context("failed iterating observed_swaps stream")?
        {
            let swap = Self::row_to_swap_event(row)?;
            on_swap(swap)?;
            seen = seen.saturating_add(1);
        }
        Ok(seen)
    }

    pub fn for_each_observed_swap_after_cursor<F>(
        &self,
        cursor_ts: DateTime<Utc>,
        cursor_slot: u64,
        cursor_signature: &str,
        limit: usize,
        mut on_swap: F,
    ) -> Result<usize>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        Ok(self
            .for_each_observed_swap_after_cursor_with_budget(
                cursor_ts,
                cursor_slot,
                cursor_signature,
                limit,
                Instant::now() + StdDuration::from_secs(24 * 60 * 60),
                |swap| on_swap(swap),
            )?
            .rows_seen)
    }

    pub fn for_each_observed_swap_after_cursor_with_budget<F>(
        &self,
        cursor_ts: DateTime<Utc>,
        cursor_slot: u64,
        cursor_signature: &str,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if limit == 0 {
            return Ok(ObservedSwapCursorPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedSwapCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
            });
        }
        let limit = (limit.min(i64::MAX as usize)) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE (ts, slot, signature) > (?1, ?2, ?3)
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?4",
            )
            .context("failed to prepare observed_swaps cursor query")?;
        let mut rows = stmt
            .query(params![
                cursor_ts.to_rfc3339(),
                cursor_slot as i64,
                cursor_signature,
                limit,
            ])
            .context("failed to query observed_swaps by cursor")?;

        let mut seen = 0usize;
        let mut time_budget_exhausted = false;
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        time_budget_exhausted = true;
                        break;
                    }
                    return Err(error).context("failed iterating observed_swaps cursor rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let swap = Self::row_to_swap_event(row)?;
            on_swap(swap)?;
            seen = seen.saturating_add(1);
        }
        Ok(ObservedSwapCursorPage {
            rows_seen: seen,
            time_budget_exhausted,
        })
    }

    pub fn load_recent_observed_swaps_since(
        &self,
        since: DateTime<Utc>,
        limit: usize,
    ) -> Result<(Vec<SwapEvent>, bool)> {
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let retained_limit = limit.min(i64::MAX as usize);
        let query_limit = retained_limit.saturating_add(1).min(i64::MAX as usize) as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1
                 ORDER BY ts DESC, slot DESC, signature DESC
                 LIMIT ?2",
            )
            .context("failed to prepare recent observed_swaps query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339(), query_limit])
            .context("failed to query recent observed_swaps")?;

        let mut swaps = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating recent observed_swaps rows")?
        {
            swaps.push(Self::row_to_swap_event(row)?);
        }
        let truncated_by_limit = swaps.len() > retained_limit;
        if truncated_by_limit {
            swaps.truncate(retained_limit);
        }
        swaps.reverse();
        Ok((swaps, truncated_by_limit))
    }

    pub fn load_discovery_runtime_cursor(&self) -> Result<Option<DiscoveryRuntimeCursor>> {
        self.ensure_discovery_runtime_state_table()?;
        let row: Option<(String, i64, String)> = self
            .conn
            .query_row(
                "SELECT cursor_ts, cursor_slot, cursor_signature
                 FROM discovery_runtime_state
                 WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .context("failed reading discovery runtime cursor")?;
        let Some((cursor_ts_raw, cursor_slot_raw, cursor_signature)) = row else {
            return Ok(None);
        };
        let cursor_ts = DateTime::parse_from_rfc3339(cursor_ts_raw.as_str())
            .map(|dt| dt.with_timezone(&Utc))
            .with_context(|| format!("invalid discovery cursor timestamp: {cursor_ts_raw}"))?;
        Ok(Some(DiscoveryRuntimeCursor {
            ts_utc: cursor_ts,
            slot: cursor_slot_raw.max(0) as u64,
            signature: cursor_signature,
        }))
    }

    pub fn upsert_discovery_runtime_cursor(&self, cursor: &DiscoveryRuntimeCursor) -> Result<()> {
        self.ensure_discovery_runtime_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_runtime_state(
                    id, cursor_ts, cursor_slot, cursor_signature, updated_at
                 ) VALUES (1, ?1, ?2, ?3, datetime('now'))
                 ON CONFLICT(id) DO UPDATE SET
                    cursor_ts = excluded.cursor_ts,
                    cursor_slot = excluded.cursor_slot,
                    cursor_signature = excluded.cursor_signature,
                    updated_at = excluded.updated_at",
                params![
                    cursor.ts_utc.to_rfc3339(),
                    cursor.slot as i64,
                    cursor.signature.as_str(),
                ],
            )
        })
        .context("failed updating discovery runtime cursor")?;
        Ok(())
    }

    pub fn list_unique_sol_buy_mints_since(&self, since: DateTime<Utc>) -> Result<HashSet<String>> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT token_out
                 FROM observed_swaps
                 WHERE ts >= ?1
                   AND token_in = ?2
                   AND token_out <> ?2",
            )
            .context("failed to prepare unique sol-buy mints query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339(), SOL_MINT])
            .context("failed to query unique sol-buy mints")?;

        let mut out = HashSet::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating unique sol-buy mints rows")?
        {
            let mint: String = row
                .get(0)
                .context("failed reading observed_swaps.token_out")?;
            out.insert(mint);
        }
        Ok(out)
    }

    fn ensure_discovery_runtime_state_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_runtime_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    cursor_ts TEXT NOT NULL,
                    cursor_slot INTEGER NOT NULL,
                    cursor_signature TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )",
            )
            .context("failed to ensure discovery_runtime_state table exists")?;
        Ok(())
    }

    fn row_to_swap_event(row: &rusqlite::Row<'_>) -> Result<SwapEvent> {
        let ts_raw: String = row.get(8).context("failed reading observed_swaps.ts")?;
        let ts_utc = DateTime::parse_from_rfc3339(&ts_raw)
            .map(|dt| dt.with_timezone(&Utc))
            .with_context(|| format!("invalid observed_swaps.ts rfc3339 value: {ts_raw}"))?;
        let slot_raw: i64 = row.get(7).context("failed reading observed_swaps.slot")?;
        let slot = if slot_raw < 0 { 0 } else { slot_raw as u64 };
        let exact_amounts = Self::read_exact_swap_amounts(row)?;

        Ok(SwapEvent {
            signature: row
                .get(0)
                .context("failed reading observed_swaps.signature")?,
            wallet: row
                .get(1)
                .context("failed reading observed_swaps.wallet_id")?,
            dex: row.get(2).context("failed reading observed_swaps.dex")?,
            token_in: row
                .get(3)
                .context("failed reading observed_swaps.token_in")?,
            token_out: row
                .get(4)
                .context("failed reading observed_swaps.token_out")?,
            amount_in: row.get(5).context("failed reading observed_swaps.qty_in")?,
            amount_out: row
                .get(6)
                .context("failed reading observed_swaps.qty_out")?,
            slot,
            ts_utc,
            exact_amounts,
        })
    }

    fn read_exact_swap_amounts(row: &rusqlite::Row<'_>) -> Result<Option<ExactSwapAmounts>> {
        let amount_in_raw: Option<String> = row
            .get(9)
            .context("failed reading observed_swaps.qty_in_raw")?;
        let amount_in_decimals_raw: Option<i64> = row
            .get(10)
            .context("failed reading observed_swaps.qty_in_decimals")?;
        let amount_out_raw: Option<String> = row
            .get(11)
            .context("failed reading observed_swaps.qty_out_raw")?;
        let amount_out_decimals_raw: Option<i64> = row
            .get(12)
            .context("failed reading observed_swaps.qty_out_decimals")?;

        match (
            amount_in_raw,
            amount_in_decimals_raw,
            amount_out_raw,
            amount_out_decimals_raw,
        ) {
            (
                Some(amount_in_raw),
                Some(amount_in_decimals_raw),
                Some(amount_out_raw),
                Some(amount_out_decimals_raw),
            ) => {
                let amount_in_decimals =
                    u8::try_from(amount_in_decimals_raw).with_context(|| {
                        format!(
                            "invalid observed_swaps.qty_in_decimals value: {amount_in_decimals_raw}"
                        )
                    })?;
                let amount_out_decimals =
                    u8::try_from(amount_out_decimals_raw).with_context(|| {
                        format!(
                            "invalid observed_swaps.qty_out_decimals value: {amount_out_decimals_raw}"
                        )
                    })?;
                Ok(Some(ExactSwapAmounts {
                    amount_in_raw,
                    amount_in_decimals,
                    amount_out_raw,
                    amount_out_decimals,
                }))
            }
            (None, None, None, None) => Ok(None),
            _ => Err(anyhow!(
                "observed_swaps exact amount columns must be fully populated or fully NULL"
            )),
        }
    }

    pub fn token_market_stats(
        &self,
        token: &str,
        as_of: DateTime<Utc>,
    ) -> Result<TokenMarketStats> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let as_of_raw = as_of.to_rfc3339();

        let first_seen_raw: Option<String> = self
            .conn
            .query_row(
                "SELECT MIN(ts)
                 FROM (
                    SELECT ts FROM observed_swaps WHERE token_in = ?1 AND ts <= ?2
                    UNION ALL
                    SELECT ts FROM observed_swaps WHERE token_out = ?1 AND ts <= ?2
                 )",
                params![token, &as_of_raw],
                |row| row.get(0),
            )
            .context("failed querying token first_seen")?;

        let first_seen = first_seen_raw
            .as_deref()
            .map(|raw| {
                DateTime::parse_from_rfc3339(raw)
                    .map(|dt| dt.with_timezone(&Utc))
                    .with_context(|| format!("invalid observed_swaps.ts rfc3339 value: {raw}"))
            })
            .transpose()?;

        let holders_proxy_raw: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM (
                    SELECT DISTINCT wallet_id
                    FROM observed_swaps
                    WHERE token_in = ?1
                      AND ts <= ?2
                    UNION
                    SELECT DISTINCT wallet_id
                    FROM observed_swaps
                    WHERE token_out = ?1
                      AND ts <= ?2
                 )",
                params![token, &as_of_raw],
                |row| row.get(0),
            )
            .context("failed querying token holders proxy")?;

        let window_start = (as_of - Duration::minutes(5)).to_rfc3339();
        let window_end = as_of.to_rfc3339();
        let (volume_5m_sol, liquidity_sol_proxy, unique_traders_5m_raw): (f64, f64, i64) = self
            .conn
            .query_row(
                "SELECT
                    COALESCE(SUM(sol_notional), 0.0) AS volume_5m_sol,
                    COALESCE(MAX(sol_notional), 0.0) AS liquidity_sol_proxy,
                    COUNT(DISTINCT wallet_id) AS unique_traders_5m
                 FROM (
                    SELECT wallet_id, qty_out AS sol_notional
                    FROM observed_swaps
                    WHERE token_in = ?1
                      AND token_out = ?2
                      AND ts >= ?3
                      AND ts <= ?4
                    UNION ALL
                    SELECT wallet_id, qty_in AS sol_notional
                    FROM observed_swaps
                    WHERE token_out = ?1
                      AND token_in = ?2
                      AND ts >= ?3
                      AND ts <= ?4
                 )",
                params![token, SOL_MINT, window_start, window_end],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .context("failed querying token 5m market stats")?;

        Ok(TokenMarketStats {
            first_seen,
            holders_proxy: holders_proxy_raw.max(0) as u64,
            liquidity_sol_proxy,
            volume_5m_sol,
            unique_traders_5m: unique_traders_5m_raw.max(0) as u64,
        })
    }

    pub fn get_token_quality_cache(&self, mint: &str) -> Result<Option<TokenQualityCacheRow>> {
        let row: Option<(String, Option<i64>, Option<f64>, Option<i64>, String)> = self
            .conn
            .query_row(
                "SELECT mint, holders, liquidity_sol, token_age_seconds, fetched_at
                 FROM token_quality_cache
                 WHERE mint = ?1",
                params![mint],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .context("failed querying token_quality_cache row")?;

        row.map(
            |(mint, holders, liquidity_sol, token_age_seconds, fetched_at_raw)| {
                let fetched_at = DateTime::parse_from_rfc3339(&fetched_at_raw)
                    .map(|dt| dt.with_timezone(&Utc))
                    .with_context(|| {
                        format!(
                            "invalid token_quality_cache.fetched_at rfc3339 value: {fetched_at_raw}"
                        )
                    })?;
                Ok(TokenQualityCacheRow {
                    mint,
                    holders: holders.map(|value| value.max(0) as u64),
                    liquidity_sol,
                    token_age_seconds: token_age_seconds.map(|value| value.max(0) as u64),
                    fetched_at,
                })
            },
        )
        .transpose()
    }

    pub fn upsert_token_quality_cache(
        &self,
        mint: &str,
        holders: Option<u64>,
        liquidity_sol: Option<f64>,
        token_age_seconds: Option<u64>,
        fetched_at: DateTime<Utc>,
    ) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO token_quality_cache(
                    mint,
                    holders,
                    liquidity_sol,
                    token_age_seconds,
                    fetched_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(mint) DO UPDATE SET
                    holders = excluded.holders,
                    liquidity_sol = excluded.liquidity_sol,
                    token_age_seconds = excluded.token_age_seconds,
                    fetched_at = excluded.fetched_at",
                params![
                    mint,
                    holders.map(|value| value as i64),
                    liquidity_sol,
                    token_age_seconds.map(|value| value as i64),
                    fetched_at.to_rfc3339(),
                ],
            )
            .context("failed upserting token_quality_cache row")?;
        Ok(())
    }

    pub fn fetch_token_quality_from_helius(
        helius_http_url: &str,
        mint: &str,
        timeout_ms: u64,
        max_signature_pages: u32,
        min_age_hint_seconds: Option<u64>,
    ) -> Result<TokenQualityRpcRow> {
        let client = Client::builder()
            .timeout(StdDuration::from_millis(timeout_ms.max(100)))
            .build()
            .context("failed building reqwest blocking client for token quality fetch")?;

        let holders = fetch_token_holders(&client, helius_http_url, mint).ok();
        let token_age_seconds = fetch_token_age_seconds(
            &client,
            helius_http_url,
            mint,
            max_signature_pages.max(1),
            min_age_hint_seconds,
        )
        .ok()
        .flatten();
        if holders.is_none() && token_age_seconds.is_none() {
            return Err(anyhow!(
                "failed to fetch token quality fields for mint {} via helius",
                mint
            ));
        }

        Ok(TokenQualityRpcRow {
            holders,
            liquidity_sol: None,
            token_age_seconds,
        })
    }
}

fn duration_ms_ceil(duration: StdDuration) -> u64 {
    let micros = duration.as_micros();
    if micros == 0 {
        0
    } else {
        micros.div_ceil(1000).min(u128::from(u64::MAX)) as u64
    }
}

fn rpc_result(payload: &Value) -> &Value {
    payload.get("result").unwrap_or(payload)
}

fn post_helius_json(client: &Client, helius_http_url: &str, payload: &Value) -> Result<Value> {
    let response = client
        .post(helius_http_url)
        .json(payload)
        .send()
        .with_context(|| format!("failed helius rpc request to {helius_http_url}"))?;
    let status = response.status();
    let body = response
        .json::<Value>()
        .context("failed parsing helius rpc response json")?;
    if !status.is_success() {
        return Err(anyhow!("helius rpc returned http status {status}: {body}"));
    }
    Ok(body)
}

fn fetch_token_holders(client: &Client, helius_http_url: &str, mint: &str) -> Result<u64> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [
            TOKEN_PROGRAM_ID,
            {
                "encoding": "jsonParsed"
                ,
                "filters": [
                    { "dataSize": 165 },
                    { "memcmp": { "offset": 0, "bytes": mint } }
                ]
            },
        ],
    });
    let response = post_helius_json(client, helius_http_url, &payload)?;
    parse_token_holders_from_program_accounts_response(&response)
}

fn parse_token_holders_from_program_accounts_response(response: &Value) -> Result<u64> {
    let rpc_result = rpc_result(response);
    let accounts = rpc_result
        .as_array()
        .or_else(|| rpc_result.get("value").and_then(Value::as_array))
        .ok_or_else(|| anyhow!("missing token accounts array in rpc response"))?;
    let mut unique_owners = HashSet::new();
    for (index, account) in accounts.iter().enumerate() {
        let info = account
            .get("account")
            .and_then(|value| value.get("data"))
            .and_then(|value| value.get("parsed"))
            .and_then(|value| value.get("info"))
            .ok_or_else(|| anyhow!("missing parsed token account info at index={index}"))?;
        let owner = info
            .get("owner")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing token account owner at index={index}"))?;
        let amount_raw = info
            .get("tokenAmount")
            .and_then(|value| value.get("amount"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing token amount at index={index}"))?;
        let amount = amount_raw
            .parse::<u64>()
            .with_context(|| format!("invalid token amount at index={index}: {amount_raw}"))?;
        if amount > 0 {
            unique_owners.insert(owner.to_string());
        }
    }
    Ok(unique_owners.len() as u64)
}

fn fetch_token_age_seconds(
    client: &Client,
    helius_http_url: &str,
    mint: &str,
    max_pages: u32,
    min_age_hint_seconds: Option<u64>,
) -> Result<Option<u64>> {
    let now_ts = Utc::now().timestamp();
    let min_block_time = min_age_hint_seconds
        .and_then(|hint| now_ts.checked_sub(hint as i64))
        .unwrap_or(i64::MIN);

    let mut oldest_seen: Option<i64> = None;
    let mut before_sig: Option<String> = None;

    for _page in 0..max_pages {
        let mut options = json!({ "limit": 1000 });
        if let Some(before) = before_sig.as_deref() {
            options["before"] = Value::String(before.to_string());
        }
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [mint, options],
        });

        let response = post_helius_json(client, helius_http_url, &payload)?;
        let entries = rpc_result(&response)
            .as_array()
            .ok_or_else(|| anyhow!("missing signatures array in helius response"))?;
        if entries.is_empty() {
            break;
        }

        for entry in entries {
            if let Some(value) = entry.get("blockTime").and_then(Value::as_i64) {
                oldest_seen = Some(oldest_seen.map_or(value, |current| current.min(value)));
            }
            if let Some(signature) = entry.get("signature").and_then(Value::as_str) {
                before_sig = Some(signature.to_string());
            }
        }

        if oldest_seen.is_some_and(|value| value <= min_block_time) {
            break;
        }
    }

    let Some(oldest) = oldest_seen else {
        return Ok(None);
    };

    if oldest > now_ts {
        return Ok(None);
    }

    Ok(Some((now_ts - oldest) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_token_holders_from_program_accounts_response_counts_unique_nonzero_owners(
    ) -> Result<()> {
        let response = json!({
            "jsonrpc": "2.0",
            "result": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "owner": "OwnerA",
                                    "tokenAmount": { "amount": "10" }
                                }
                            }
                        }
                    }
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "owner": "OwnerA",
                                    "tokenAmount": { "amount": "5" }
                                }
                            }
                        }
                    }
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "owner": "OwnerB",
                                    "tokenAmount": { "amount": "0" }
                                }
                            }
                        }
                    }
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "owner": "OwnerC",
                                    "tokenAmount": { "amount": "42" }
                                }
                            }
                        }
                    }
                }
            ]
        });
        let holders = parse_token_holders_from_program_accounts_response(&response)?;
        assert_eq!(holders, 2);
        Ok(())
    }

    #[test]
    fn parse_token_holders_from_program_accounts_response_accepts_wrapped_value_array() -> Result<()>
    {
        let response = json!({
            "jsonrpc": "2.0",
            "result": {
                "value": [
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {
                                        "owner": "OwnerX",
                                        "tokenAmount": { "amount": "1" }
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        });
        let holders = parse_token_holders_from_program_accounts_response(&response)?;
        assert_eq!(holders, 1);
        Ok(())
    }

    #[test]
    fn parse_token_holders_from_program_accounts_response_rejects_invalid_shape() {
        let response = json!({
            "jsonrpc": "2.0",
            "result": {
                "value": {
                    "owner": "not-an-array"
                }
            }
        });
        let error = parse_token_holders_from_program_accounts_response(&response)
            .expect_err("invalid response shape must fail");
        assert!(
            error.to_string().contains("missing token accounts array"),
            "unexpected error: {error}"
        );
    }
}
