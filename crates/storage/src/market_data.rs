use crate::{
    discovery::upsert_wallet_activity_days_on_conn, DiscoveryPersistedRebuildPhase,
    DiscoveryPersistedRebuildStateRow, DiscoveryRuntimeCursor, ObservedSwapBatchWriteMetrics,
    SqliteBatchedDeleteSummary, SqliteStore, TokenMarketStats, TokenQualityCacheRow,
    TokenQualityRpcRow, WalletActivityDayRow,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use reqwest::blocking::Client;
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::time::{Duration as StdDuration, Instant};

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const OBSERVED_SWAP_CURSOR_PROGRESS_OPS: i32 = 2_000;

#[derive(Debug, Clone, Copy, Default)]
pub struct ObservedSwapCursorPage {
    pub rows_seen: usize,
    pub time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedSolLegCursorAccessPath {
    TsCursorFallback,
    SolLegPartialIndex,
}

impl ObservedSolLegCursorAccessPath {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TsCursorFallback => "ts_cursor_fallback",
            Self::SolLegPartialIndex => "sol_leg_partial_index",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedSolLegCursorPage {
    pub rows_seen: usize,
    pub time_budget_exhausted: bool,
    pub access_path: ObservedSolLegCursorAccessPath,
}

#[derive(Debug, Clone, Default)]
pub struct ObservedBuyMintPage {
    pub mints: Vec<String>,
    pub time_budget_exhausted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedWalletActivityRow {
    pub wallet_id: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub trades: usize,
    pub active_day_count: u32,
    pub suspicious: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedWalletActivityDayCountSource {
    WalletActivityDays,
    ObservedSwapsFallback,
}

impl ObservedWalletActivityDayCountSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WalletActivityDays => "wallet_activity_days",
            Self::ObservedSwapsFallback => "observed_swaps_fallback",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ObservedWalletActivityPage {
    pub rows: Vec<ObservedWalletActivityRow>,
    pub rows_seen: usize,
    pub time_budget_exhausted: bool,
    pub active_day_count_source: Option<ObservedWalletActivityDayCountSource>,
}

#[derive(Debug, Clone, Default)]
struct ObservedWalletActiveDayCountPage {
    counts: HashMap<String, u32>,
    time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct ObservedWalletActivityDaySummaryRow {
    inclusive_day_count: u32,
    has_start_day: bool,
    has_end_day: bool,
}

#[derive(Debug, Clone, Default)]
struct ObservedWalletActivityDaySummaryPage {
    rows: HashMap<String, ObservedWalletActivityDaySummaryRow>,
    time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObservedBuyMintCountRow {
    pub mint: String,
    pub buy_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ObservedBuyMintCountPage {
    pub rows: Vec<ObservedBuyMintCountRow>,
    pub time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ObservedBuyMintCount {
    pub count: usize,
    pub time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ObservedBuyMintOccurrenceCount {
    pub buy_count: usize,
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

fn parse_rfc3339_utc(raw: &str, field_name: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid {field_name} timestamp value: {raw}"))
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

    pub fn load_observed_buy_mints_in_window(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT token_out
                 FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                 WHERE ts >= ?1
                   AND ts <= ?2
                   AND token_in = ?3
                   AND token_out != ?3
                 ORDER BY token_out ASC",
            )
            .context("failed to prepare observed_swaps distinct buy mint query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339(), until.to_rfc3339(), SOL_MINT,])
            .context("failed to query observed_swaps distinct buy mints")?;

        let mut mints = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating observed_swaps distinct buy mints")?
        {
            mints.push(
                row.get::<_, String>(0)
                    .context("failed reading observed_swaps distinct buy mint")?,
            );
        }

        Ok(mints)
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

    pub fn for_each_observed_swap_in_window<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
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
                   AND ts <= ?2
                 ORDER BY ts ASC, slot ASC, signature ASC",
            )
            .context("failed to prepare observed_swaps window streaming query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339(), until.to_rfc3339()])
            .context("failed to stream observed_swaps window")?;

        let mut seen = 0usize;
        while let Some(row) = rows
            .next()
            .context("failed iterating observed_swaps window stream")?
        {
            let swap = Self::row_to_swap_event(row)?;
            on_swap(swap)?;
            seen = seen.saturating_add(1);
        }
        Ok(seen)
    }

    pub fn for_each_observed_swap_in_window_after_cursor_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
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
        let (query, params): (&str, Vec<rusqlite::types::Value>) = match cursor {
            Some(cursor) => (
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1
                   AND ts <= ?2
                   AND (ts, slot, signature) > (?3, ?4, ?5)
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?6",
                vec![
                    since.to_rfc3339().into(),
                    until.to_rfc3339().into(),
                    cursor.ts_utc.to_rfc3339().into(),
                    (cursor.slot as i64).into(),
                    cursor.signature.clone().into(),
                    limit.into(),
                ],
            ),
            None => (
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1
                   AND ts <= ?2
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?3",
                vec![
                    since.to_rfc3339().into(),
                    until.to_rfc3339().into(),
                    limit.into(),
                ],
            ),
        };
        let mut stmt = self
            .conn
            .prepare(query)
            .context("failed to prepare observed_swaps window cursor query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed to query observed_swaps by window cursor")?;

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
                    return Err(error)
                        .context("failed iterating observed_swaps window cursor rows");
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

    pub fn observed_wallet_activity_page_in_window_with_budget(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        wallet_cursor: Option<&str>,
        wallet_limit: usize,
        max_tx_per_minute: u32,
        deadline: Instant,
    ) -> Result<ObservedWalletActivityPage> {
        if wallet_limit == 0 {
            return Ok(ObservedWalletActivityPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedWalletActivityPage {
                rows: Vec::new(),
                rows_seen: 0,
                time_budget_exhausted: true,
                active_day_count_source: None,
            });
        }

        let wallet_limit = wallet_limit.min(900).max(1) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();

        let (wallet_ids_query, wallet_ids_params): (&str, Vec<rusqlite::types::Value>) =
            match wallet_cursor {
                Some(wallet_cursor) => (
                    "SELECT wallet_id
                     FROM (
                        SELECT wallet_id
                        FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
                        WHERE ts >= ?1
                          AND ts <= ?2
                          AND wallet_id > ?3
                        GROUP BY wallet_id
                        ORDER BY wallet_id ASC
                        LIMIT ?4
                     )
                     ORDER BY wallet_id ASC",
                    vec![
                        since_raw.clone().into(),
                        until_raw.clone().into(),
                        wallet_cursor.to_string().into(),
                        wallet_limit.into(),
                    ],
                ),
                None => (
                    "SELECT wallet_id
                     FROM (
                        SELECT wallet_id
                        FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
                        WHERE ts >= ?1
                          AND ts <= ?2
                        GROUP BY wallet_id
                        ORDER BY wallet_id ASC
                        LIMIT ?3
                     )
                     ORDER BY wallet_id ASC",
                    vec![
                        since_raw.clone().into(),
                        until_raw.clone().into(),
                        wallet_limit.into(),
                    ],
                ),
            };

        let mut wallet_ids_stmt = self
            .conn
            .prepare(wallet_ids_query)
            .context("failed to prepare observed wallet activity wallet-id page query")?;
        let mut wallet_ids_rows = wallet_ids_stmt
            .query(rusqlite::params_from_iter(wallet_ids_params))
            .context("failed querying observed wallet activity wallet-id page")?;
        let mut wallet_ids = Vec::new();
        loop {
            let next_row = match wallet_ids_rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        return Ok(ObservedWalletActivityPage {
                            rows: Vec::new(),
                            rows_seen: 0,
                            time_budget_exhausted: true,
                            active_day_count_source: None,
                        });
                    }
                    return Err(error)
                        .context("failed iterating observed wallet activity wallet-id rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            wallet_ids.push(
                row.get::<_, String>(0)
                    .context("failed reading observed wallet activity wallet_id")?,
            );
        }

        if wallet_ids.is_empty() {
            return Ok(ObservedWalletActivityPage::default());
        }

        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");

        let mut summaries: HashMap<String, ObservedWalletActivityRow> = HashMap::new();
        let summary_query = format!(
            "SELECT wallet_id, MIN(ts), MAX(ts), COUNT(*)
             FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
             WHERE ts >= ?1
               AND ts <= ?2
               AND wallet_id IN ({placeholders})
             GROUP BY wallet_id
             ORDER BY wallet_id ASC"
        );
        let mut summary_params = vec![since_raw.clone().into(), until_raw.clone().into()];
        summary_params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut summary_stmt = self
            .conn
            .prepare(&summary_query)
            .context("failed to prepare observed wallet activity summary query")?;
        let mut summary_rows = summary_stmt
            .query(rusqlite::params_from_iter(summary_params))
            .context("failed querying observed wallet activity summary rows")?;
        let mut rows_seen = 0usize;
        loop {
            let next_row = match summary_rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        return Ok(ObservedWalletActivityPage {
                            rows: Vec::new(),
                            rows_seen: 0,
                            time_budget_exhausted: true,
                            active_day_count_source: None,
                        });
                    }
                    return Err(error)
                        .context("failed iterating observed wallet activity summary rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let wallet_id: String = row
                .get(0)
                .context("failed reading observed wallet activity summary wallet_id")?;
            let first_seen_raw: String = row
                .get(1)
                .context("failed reading observed wallet activity summary first_seen")?;
            let last_seen_raw: String = row
                .get(2)
                .context("failed reading observed wallet activity summary last_seen")?;
            let trades_raw: i64 = row
                .get(3)
                .context("failed reading observed wallet activity summary trades")?;
            let trades = trades_raw.max(0) as usize;
            rows_seen = rows_seen.saturating_add(trades);
            summaries.insert(
                wallet_id.clone(),
                ObservedWalletActivityRow {
                    wallet_id,
                    first_seen: parse_rfc3339_utc(
                        &first_seen_raw,
                        "observed wallet activity summary first_seen",
                    )?,
                    last_seen: parse_rfc3339_utc(
                        &last_seen_raw,
                        "observed wallet activity summary last_seen",
                    )?,
                    trades,
                    active_day_count: 0,
                    suspicious: false,
                },
            );
        }

        // wallet_activity_days is maintained atomically with observed_swaps inserts, so it can
        // provide exact day counts without paying COUNT(DISTINCT day) on the raw swap window.
        // If the auxiliary table looks incomplete for this page, fall back to the exact raw scan.
        let active_day_summaries = self
            .observed_wallet_activity_day_summaries_in_window_with_budget(
                &wallet_ids,
                since,
                until,
                deadline,
            )?;
        let mut active_day_count_source =
            Some(ObservedWalletActivityDayCountSource::WalletActivityDays);
        if active_day_summaries.time_budget_exhausted {
            return Ok(ObservedWalletActivityPage {
                rows: Vec::new(),
                rows_seen: 0,
                time_budget_exhausted: true,
                active_day_count_source: None,
            });
        }
        let since_day = since.date_naive();
        let until_day = until.date_naive();
        let same_day_window = since_day == until_day;
        let mut active_day_counts = HashMap::new();
        if active_day_summaries.rows.len() != wallet_ids.len() {
            let fallback_active_day_counts = self
                .observed_wallet_active_day_counts_from_swaps_in_window_with_budget(
                    &wallet_ids,
                    since,
                    until,
                    deadline,
                )?;
            if fallback_active_day_counts.time_budget_exhausted {
                return Ok(ObservedWalletActivityPage {
                    rows: Vec::new(),
                    rows_seen: 0,
                    time_budget_exhausted: true,
                    active_day_count_source: None,
                });
            }
            active_day_counts = fallback_active_day_counts.counts;
            active_day_count_source =
                Some(ObservedWalletActivityDayCountSource::ObservedSwapsFallback);
        } else {
            for wallet_id in &wallet_ids {
                let summary = summaries.get(wallet_id).ok_or_else(|| {
                    anyhow!(
                        "missing observed wallet activity summary for wallet {} while loading exact day counts",
                        wallet_id
                    )
                })?;
                let activity_day_summary =
                    active_day_summaries.rows.get(wallet_id).ok_or_else(|| {
                        anyhow!(
                            "missing wallet_activity_days summary for wallet {} in observed wallet activity page",
                            wallet_id
                        )
                    })?;
                let active_day_count = if same_day_window {
                    1
                } else {
                    let mut count = activity_day_summary.inclusive_day_count;
                    if activity_day_summary.has_start_day
                        && summary.first_seen.date_naive() > since_day
                    {
                        count = count.saturating_sub(1);
                    }
                    if activity_day_summary.has_end_day
                        && summary.last_seen.date_naive() < until_day
                    {
                        count = count.saturating_sub(1);
                    }
                    if count == 0 {
                        return Err(anyhow!(
                            "wallet_activity_days summary resolved to zero in-window days for wallet {} despite observed_swaps summary rows",
                            wallet_id
                        ));
                    }
                    count
                };
                active_day_counts.insert(wallet_id.clone(), active_day_count);
            }
        }
        for wallet_id in &wallet_ids {
            let active_day_count = active_day_counts.get(wallet_id).copied().ok_or_else(|| {
                anyhow!(
                    "failed loading exact wallet activity day count for wallet {} in observed wallet activity page",
                    wallet_id
                )
            })?;
            if let Some(summary) = summaries.get_mut(wallet_id) {
                summary.active_day_count = active_day_count;
            }
        }

        let max_tx_query = format!(
            "SELECT wallet_id, MAX(tx_count)
             FROM (
                SELECT wallet_id,
                       CAST(strftime('%s', ts) AS INTEGER) / 60 AS minute_bucket,
                       COUNT(*) AS tx_count
                FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
                WHERE ts >= ?1
                  AND ts <= ?2
                  AND wallet_id IN ({placeholders})
                GROUP BY wallet_id, minute_bucket
             )
             GROUP BY wallet_id"
        );
        let mut max_tx_params = vec![since_raw.into(), until_raw.into()];
        max_tx_params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut max_tx_stmt = self
            .conn
            .prepare(&max_tx_query)
            .context("failed to prepare observed wallet activity max-tx query")?;
        let mut max_tx_rows = max_tx_stmt
            .query(rusqlite::params_from_iter(max_tx_params))
            .context("failed querying observed wallet activity max-tx rows")?;
        loop {
            let next_row = match max_tx_rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        return Ok(ObservedWalletActivityPage {
                            rows: Vec::new(),
                            rows_seen: 0,
                            time_budget_exhausted: true,
                            active_day_count_source: None,
                        });
                    }
                    return Err(error)
                        .context("failed iterating observed wallet activity max-tx rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let wallet_id: String = row
                .get(0)
                .context("failed reading observed wallet activity max-tx wallet_id")?;
            let max_tx_raw: i64 = row
                .get(1)
                .context("failed reading observed wallet activity max(tx_count)")?;
            if let Some(summary) = summaries.get_mut(&wallet_id) {
                summary.suspicious = (max_tx_raw.max(0) as u32) > max_tx_per_minute.max(1);
            }
        }

        let rows = wallet_ids
            .into_iter()
            .filter_map(|wallet_id| summaries.remove(&wallet_id))
            .collect();
        Ok(ObservedWalletActivityPage {
            rows,
            rows_seen,
            time_budget_exhausted: false,
            active_day_count_source,
        })
    }

    fn observed_wallet_activity_day_summaries_in_window_with_budget(
        &self,
        wallet_ids: &[String],
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        deadline: Instant,
    ) -> Result<ObservedWalletActivityDaySummaryPage> {
        if wallet_ids.is_empty() {
            return Ok(ObservedWalletActivityDaySummaryPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedWalletActivityDaySummaryPage {
                rows: HashMap::new(),
                time_budget_exhausted: true,
            });
        }

        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let day_start = since.date_naive().format("%Y-%m-%d").to_string();
        let day_end = until.date_naive().format("%Y-%m-%d").to_string();
        let sql = format!(
            "SELECT wallet_id,
                    COUNT(*),
                    MAX(CASE WHEN activity_day = ?1 THEN 1 ELSE 0 END),
                    MAX(CASE WHEN activity_day = ?2 THEN 1 ELSE 0 END)
             FROM wallet_activity_days INDEXED BY idx_wallet_activity_days_day_wallet
             WHERE activity_day >= ?1
               AND activity_day <= ?2
               AND wallet_id IN ({placeholders})
             GROUP BY wallet_id"
        );
        let mut params = vec![
            rusqlite::types::Value::from(day_start),
            rusqlite::types::Value::from(day_end),
        ];
        params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare wallet_activity_days summary query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed querying wallet_activity_days summaries")?;
        let mut activity_day_summaries = HashMap::new();
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        return Ok(ObservedWalletActivityDaySummaryPage {
                            rows: HashMap::new(),
                            time_budget_exhausted: true,
                        });
                    }
                    return Err(error).context("failed iterating wallet_activity_days summaries");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let wallet_id: String = row
                .get(0)
                .context("failed reading wallet_activity_days summary wallet_id")?;
            let inclusive_day_count: i64 = row
                .get(1)
                .context("failed reading wallet_activity_days inclusive day count")?;
            let has_start_day: i64 = row
                .get(2)
                .context("failed reading wallet_activity_days start-day presence")?;
            let has_end_day: i64 = row
                .get(3)
                .context("failed reading wallet_activity_days end-day presence")?;
            activity_day_summaries.insert(
                wallet_id,
                ObservedWalletActivityDaySummaryRow {
                    inclusive_day_count: inclusive_day_count.max(0) as u32,
                    has_start_day: has_start_day > 0,
                    has_end_day: has_end_day > 0,
                },
            );
        }

        Ok(ObservedWalletActivityDaySummaryPage {
            rows: activity_day_summaries,
            time_budget_exhausted: false,
        })
    }

    fn observed_wallet_active_day_counts_from_swaps_in_window_with_budget(
        &self,
        wallet_ids: &[String],
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        deadline: Instant,
    ) -> Result<ObservedWalletActiveDayCountPage> {
        if wallet_ids.is_empty() {
            return Ok(ObservedWalletActiveDayCountPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedWalletActiveDayCountPage {
                counts: HashMap::new(),
                time_budget_exhausted: true,
            });
        }

        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT wallet_id, COUNT(DISTINCT substr(ts, 1, 10))
             FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
             WHERE ts >= ?1
               AND ts <= ?2
               AND wallet_id IN ({placeholders})
             GROUP BY wallet_id"
        );
        let mut params = vec![
            rusqlite::types::Value::from(since.to_rfc3339()),
            rusqlite::types::Value::from(until.to_rfc3339()),
        ];
        params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare observed_swaps fallback day-count query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed querying observed_swaps fallback day counts")?;
        let mut counts = HashMap::new();
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        return Ok(ObservedWalletActiveDayCountPage {
                            counts: HashMap::new(),
                            time_budget_exhausted: true,
                        });
                    }
                    return Err(error)
                        .context("failed iterating observed_swaps fallback day counts");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let wallet_id: String = row
                .get(0)
                .context("failed reading observed_swaps fallback day-count wallet_id")?;
            let count: i64 = row
                .get(1)
                .context("failed reading observed_swaps fallback day count")?;
            counts.insert(wallet_id, count.max(0) as u32);
        }

        Ok(ObservedWalletActiveDayCountPage {
            counts,
            time_budget_exhausted: false,
        })
    }

    fn sqlite_index_exists(&self, index_name: &str) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1
                 FROM sqlite_master
                 WHERE type = 'index'
                   AND name = ?1
                 LIMIT 1",
                params![index_name],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .with_context(|| format!("failed checking sqlite index presence for {index_name}"))?
            .is_some())
    }

    pub fn for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSolLegCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if limit == 0 {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: false,
                access_path: ObservedSolLegCursorAccessPath::TsCursorFallback,
            });
        }
        let access_path =
            if self.sqlite_index_exists("idx_observed_swaps_sol_leg_ts_slot_signature")? {
                ObservedSolLegCursorAccessPath::SolLegPartialIndex
            } else {
                ObservedSolLegCursorAccessPath::TsCursorFallback
            };
        let index_hint = match access_path {
            ObservedSolLegCursorAccessPath::SolLegPartialIndex => {
                "INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature"
            }
            ObservedSolLegCursorAccessPath::TsCursorFallback => {
                "INDEXED BY idx_observed_swaps_ts_slot_signature"
            }
        };
        if Instant::now() >= deadline {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
                access_path,
            });
        }

        let limit = (limit.min(i64::MAX as usize)) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let (query, params): (String, Vec<rusqlite::types::Value>) = match cursor {
            Some(cursor) => (
                format!(
                    "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                            qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                     FROM observed_swaps {index_hint}
                     WHERE ts >= ?1
                       AND ts <= ?2
                       AND (token_in = '{SOL_MINT}' OR token_out = '{SOL_MINT}')
                       AND (ts, slot, signature) > (?3, ?4, ?5)
                     ORDER BY ts ASC, slot ASC, signature ASC
                     LIMIT ?6"
                ),
                vec![
                    since.to_rfc3339().into(),
                    until.to_rfc3339().into(),
                    cursor.ts_utc.to_rfc3339().into(),
                    (cursor.slot as i64).into(),
                    cursor.signature.clone().into(),
                    limit.into(),
                ],
            ),
            None => (
                format!(
                    "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                            qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                     FROM observed_swaps {index_hint}
                     WHERE ts >= ?1
                       AND ts <= ?2
                       AND (token_in = '{SOL_MINT}' OR token_out = '{SOL_MINT}')
                     ORDER BY ts ASC, slot ASC, signature ASC
                     LIMIT ?3"
                ),
                vec![
                    since.to_rfc3339().into(),
                    until.to_rfc3339().into(),
                    limit.into(),
                ],
            ),
        };
        let mut stmt = self
            .conn
            .prepare(&query)
            .context("failed to prepare observed_swaps sol-leg window cursor query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed to query observed_swaps sol-leg window cursor")?;

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
                    return Err(error)
                        .context("failed iterating observed_swaps sol-leg window cursor rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let swap = Self::row_to_swap_event(row)?;
            on_swap(swap)?;
            seen = seen.saturating_add(1);
        }
        Ok(ObservedSolLegCursorPage {
            rows_seen: seen,
            time_budget_exhausted,
            access_path,
        })
    }

    pub fn load_observed_buy_mints_in_window_after_token_with_budget(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        token_out_after: Option<&str>,
        limit: usize,
        deadline: Instant,
    ) -> Result<ObservedBuyMintPage> {
        self.load_observed_buy_mints_in_time_bounds_after_token_with_budget(
            since,
            true,
            until,
            true,
            token_out_after,
            None,
            limit,
            deadline,
        )
    }

    pub fn load_observed_buy_mints_in_time_bounds_after_token_with_budget(
        &self,
        since: DateTime<Utc>,
        since_inclusive: bool,
        until: DateTime<Utc>,
        until_inclusive: bool,
        token_out_after: Option<&str>,
        token_out_at_most: Option<&str>,
        limit: usize,
        deadline: Instant,
    ) -> Result<ObservedBuyMintPage> {
        if limit == 0 {
            return Ok(ObservedBuyMintPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedBuyMintPage {
                mints: Vec::new(),
                time_budget_exhausted: true,
            });
        }

        let limit = (limit.min(i64::MAX as usize)) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let since_op = if since_inclusive { ">=" } else { ">" };
        let until_op = if until_inclusive { "<=" } else { "<" };
        let mut params: Vec<rusqlite::types::Value> = vec![
            SOL_MINT.to_string().into(),
            since.to_rfc3339().into(),
            until.to_rfc3339().into(),
        ];
        // Candidate discovery is used by stale expired-head/new-tail reconciliation.
        // Those paths operate on narrow time windows, so a time-first access path
        // avoids crawling token-order space just to find the next touched mints.
        let mut query = format!(
            "SELECT DISTINCT token_out
             FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_ts
             WHERE token_in = ?1
               AND token_out <> ?1
               AND ts {since_op} ?2
               AND ts {until_op} ?3"
        );
        let mut next_param = 4usize;
        if let Some(token_out_after) = token_out_after {
            query.push_str(&format!(" AND token_out > ?{next_param}"));
            params.push(token_out_after.to_string().into());
            next_param = next_param.saturating_add(1);
        }
        if let Some(token_out_at_most) = token_out_at_most {
            query.push_str(&format!(" AND token_out <= ?{next_param}"));
            params.push(token_out_at_most.to_string().into());
            next_param = next_param.saturating_add(1);
        }
        query.push_str(" ORDER BY token_out ASC");
        query.push_str(&format!(" LIMIT ?{next_param}"));
        params.push(limit.into());
        let mut stmt = self
            .conn
            .prepare(&query)
            .context("failed to prepare observed_swaps distinct buy mint page query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed to query observed_swaps distinct buy mint page")?;

        let mut mints = Vec::new();
        let mut time_budget_exhausted = false;
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        time_budget_exhausted = true;
                        break;
                    }
                    return Err(error)
                        .context("failed iterating observed_swaps distinct buy mint page rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            mints.push(
                row.get::<_, String>(0)
                    .context("failed reading observed_swaps distinct buy mint page row")?,
            );
        }

        Ok(ObservedBuyMintPage {
            mints,
            time_budget_exhausted,
        })
    }

    pub fn load_observed_buy_mint_counts_in_window_after_token_with_budget(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        token_out_after: Option<&str>,
        token_out_at_most: Option<&str>,
        limit: usize,
        deadline: Instant,
    ) -> Result<ObservedBuyMintCountPage> {
        self.load_observed_buy_mint_counts_in_time_bounds_after_token_with_budget(
            since,
            true,
            until,
            true,
            token_out_after,
            token_out_at_most,
            limit,
            deadline,
        )
    }

    pub fn load_observed_buy_mint_counts_in_time_bounds_after_token_with_budget(
        &self,
        since: DateTime<Utc>,
        since_inclusive: bool,
        until: DateTime<Utc>,
        until_inclusive: bool,
        token_out_after: Option<&str>,
        token_out_at_most: Option<&str>,
        limit: usize,
        deadline: Instant,
    ) -> Result<ObservedBuyMintCountPage> {
        if limit == 0 {
            return Ok(ObservedBuyMintCountPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedBuyMintCountPage {
                rows: Vec::new(),
                time_budget_exhausted: true,
            });
        }

        let limit = (limit.min(i64::MAX as usize)) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let since_op = if since_inclusive { ">=" } else { ">" };
        let until_op = if until_inclusive { "<=" } else { "<" };
        let mut params: Vec<rusqlite::types::Value> = vec![
            SOL_MINT.to_string().into(),
            since.to_rfc3339().into(),
            until.to_rfc3339().into(),
        ];
        let mut query = format!(
            "SELECT token_out, COUNT(*)
             FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
             WHERE token_in = ?1
               AND token_out <> ?1
               AND ts {since_op} ?2
               AND ts {until_op} ?3"
        );
        let mut next_param = 4usize;
        if let Some(token_out_after) = token_out_after {
            query.push_str(&format!(" AND token_out > ?{next_param}"));
            params.push(token_out_after.to_string().into());
            next_param = next_param.saturating_add(1);
        }
        if let Some(token_out_at_most) = token_out_at_most {
            query.push_str(&format!(" AND token_out <= ?{next_param}"));
            params.push(token_out_at_most.to_string().into());
            next_param = next_param.saturating_add(1);
        }
        query.push_str(" GROUP BY token_out ORDER BY token_out ASC");
        query.push_str(&format!(" LIMIT ?{next_param}"));
        params.push(limit.into());

        let mut stmt = self
            .conn
            .prepare(&query)
            .context("failed to prepare observed_swaps grouped buy mint count page query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed to query observed_swaps grouped buy mint count page")?;

        let mut mint_rows = Vec::new();
        let mut time_budget_exhausted = false;
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        time_budget_exhausted = true;
                        break;
                    }
                    return Err(error)
                        .context("failed iterating observed_swaps grouped buy mint count rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            mint_rows.push(ObservedBuyMintCountRow {
                mint: row
                    .get::<_, String>(0)
                    .context("failed reading observed_swaps grouped buy mint token")?,
                buy_count: row
                    .get::<_, i64>(1)
                    .context("failed reading observed_swaps grouped buy mint count")?
                    .max(0) as usize,
            });
        }

        Ok(ObservedBuyMintCountPage {
            rows: mint_rows,
            time_budget_exhausted,
        })
    }

    pub fn count_observed_buy_mints_in_window_up_to_token_with_budget(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        token_out_inclusive: &str,
        deadline: Instant,
    ) -> Result<ObservedBuyMintCount> {
        if Instant::now() >= deadline {
            return Ok(ObservedBuyMintCount {
                count: 0,
                time_budget_exhausted: true,
            });
        }

        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        match self.conn.query_row(
            "SELECT COUNT(DISTINCT token_out)
             FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
             WHERE token_in = ?1
               AND token_out <> ?1
               AND ts >= ?2
               AND ts <= ?3
               AND token_out <= ?4",
            params![
                SOL_MINT,
                since.to_rfc3339(),
                until.to_rfc3339(),
                token_out_inclusive,
            ],
            |row| row.get::<_, i64>(0),
        ) {
            Ok(count) => Ok(ObservedBuyMintCount {
                count: count.max(0) as usize,
                time_budget_exhausted: false,
            }),
            Err(error) if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) => {
                Ok(ObservedBuyMintCount {
                    count: 0,
                    time_budget_exhausted: true,
                })
            }
            Err(error) => {
                Err(error).context("failed counting observed_swaps distinct buy mints up to token")
            }
        }
    }

    pub fn count_observed_buy_mint_occurrences_in_time_bounds_with_budget(
        &self,
        since: DateTime<Utc>,
        since_inclusive: bool,
        until: DateTime<Utc>,
        until_inclusive: bool,
        token_out: &str,
        deadline: Instant,
    ) -> Result<ObservedBuyMintOccurrenceCount> {
        if Instant::now() >= deadline {
            return Ok(ObservedBuyMintOccurrenceCount {
                buy_count: 0,
                time_budget_exhausted: true,
            });
        }

        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let since_op = if since_inclusive { ">=" } else { ">" };
        let until_op = if until_inclusive { "<=" } else { "<" };
        let query = format!(
            "SELECT COUNT(*)
             FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
             WHERE token_in = ?1
               AND token_out = ?2
               AND ts {since_op} ?3
               AND ts {until_op} ?4"
        );
        match self.conn.query_row(
            &query,
            params![SOL_MINT, token_out, since.to_rfc3339(), until.to_rfc3339(),],
            |row| row.get::<_, i64>(0),
        ) {
            Ok(count) => Ok(ObservedBuyMintOccurrenceCount {
                buy_count: count.max(0) as usize,
                time_budget_exhausted: false,
            }),
            Err(error) if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) => {
                Ok(ObservedBuyMintOccurrenceCount {
                    buy_count: 0,
                    time_budget_exhausted: true,
                })
            }
            Err(error) => Err(error)
                .context("failed counting observed_swaps buy mint occurrences for exact token"),
        }
    }

    pub fn load_observed_buy_mint_counts_for_exact_tokens_in_time_bounds_with_budget(
        &self,
        since: DateTime<Utc>,
        since_inclusive: bool,
        until: DateTime<Utc>,
        until_inclusive: bool,
        token_outs: &[String],
        deadline: Instant,
    ) -> Result<ObservedBuyMintCountPage> {
        if token_outs.is_empty() {
            return Ok(ObservedBuyMintCountPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedBuyMintCountPage {
                rows: Vec::new(),
                time_budget_exhausted: true,
            });
        }

        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let since_op = if since_inclusive { ">=" } else { ">" };
        let until_op = if until_inclusive { "<=" } else { "<" };
        let mut params: Vec<rusqlite::types::Value> = vec![
            SOL_MINT.to_string().into(),
            since.to_rfc3339().into(),
            until.to_rfc3339().into(),
        ];
        let mut token_placeholders = Vec::with_capacity(token_outs.len());
        for token_out in token_outs {
            let placeholder = format!("?{}", params.len() + 1);
            token_placeholders.push(placeholder);
            params.push(token_out.clone().into());
        }
        let query = format!(
            "SELECT token_out, COUNT(*)
             FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
             WHERE token_in = ?1
               AND token_out <> ?1
               AND ts {since_op} ?2
               AND ts {until_op} ?3
               AND token_out IN ({})
             GROUP BY token_out
             ORDER BY token_out ASC",
            token_placeholders.join(", ")
        );
        let mut stmt = self
            .conn
            .prepare(&query)
            .context("failed to prepare exact observed_swaps buy mint batch count query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed to query exact observed_swaps buy mint batch count page")?;

        let mut mint_rows = Vec::new();
        let mut time_budget_exhausted = false;
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        time_budget_exhausted = true;
                        break;
                    }
                    return Err(error).context(
                        "failed iterating exact observed_swaps buy mint batch count rows",
                    );
                }
            };
            let Some(row) = next_row else {
                break;
            };
            mint_rows.push(ObservedBuyMintCountRow {
                mint: row
                    .get::<_, String>(0)
                    .context("failed reading exact observed_swaps buy mint batch token")?,
                buy_count: row
                    .get::<_, i64>(1)
                    .context("failed reading exact observed_swaps buy mint batch count")?
                    .max(0) as usize,
            });
        }

        Ok(ObservedBuyMintCountPage {
            rows: mint_rows,
            time_budget_exhausted,
        })
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

    pub fn load_discovery_persisted_rebuild_state(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateRow>> {
        self.ensure_discovery_persisted_rebuild_state_table()?;
        let raw = self
            .conn
            .query_row(
                "SELECT
                    phase,
                    window_start,
                    horizon_end,
                    metrics_window_start,
                    phase_cursor_ts,
                    phase_cursor_slot,
                    phase_cursor_signature,
                    prepass_rows_processed,
                    prepass_pages_processed,
                    replay_rows_processed,
                    replay_pages_processed,
                    chunks_completed,
                    state_json,
                    started_at,
                    updated_at
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, String>(12)?,
                        row.get::<_, String>(13)?,
                        row.get::<_, String>(14)?,
                    ))
                },
            )
            .optional()
            .context("failed reading discovery persisted rebuild state")?;

        raw.map(
            |(
                phase_raw,
                window_start_raw,
                horizon_end_raw,
                metrics_window_start_raw,
                cursor_ts_raw,
                cursor_slot_raw,
                cursor_signature,
                prepass_rows_processed,
                prepass_pages_processed,
                replay_rows_processed,
                replay_pages_processed,
                chunks_completed,
                state_json,
                started_at_raw,
                updated_at_raw,
            )| {
                let phase_cursor = match (cursor_ts_raw, cursor_slot_raw, cursor_signature) {
                    (None, None, None) => None,
                    (Some(ts_raw), Some(slot_raw), Some(signature)) => {
                        Some(DiscoveryRuntimeCursor {
                            ts_utc: parse_rfc3339_utc(
                                &ts_raw,
                                "discovery_persisted_rebuild_state.phase_cursor_ts",
                            )?,
                            slot: slot_raw.max(0) as u64,
                            signature,
                        })
                    }
                    _ => {
                        return Err(anyhow!(
                            "discovery_persisted_rebuild_state contains partial phase cursor state"
                        ));
                    }
                };
                Ok(DiscoveryPersistedRebuildStateRow {
                    phase: DiscoveryPersistedRebuildPhase::parse(&phase_raw)?,
                    window_start: parse_rfc3339_utc(
                        &window_start_raw,
                        "discovery_persisted_rebuild_state.window_start",
                    )?,
                    horizon_end: parse_rfc3339_utc(
                        &horizon_end_raw,
                        "discovery_persisted_rebuild_state.horizon_end",
                    )?,
                    metrics_window_start: parse_rfc3339_utc(
                        &metrics_window_start_raw,
                        "discovery_persisted_rebuild_state.metrics_window_start",
                    )?,
                    phase_cursor,
                    prepass_rows_processed: prepass_rows_processed.max(0) as usize,
                    prepass_pages_processed: prepass_pages_processed.max(0) as usize,
                    replay_rows_processed: replay_rows_processed.max(0) as usize,
                    replay_pages_processed: replay_pages_processed.max(0) as usize,
                    chunks_completed: chunks_completed.max(0) as usize,
                    state_json,
                    started_at: parse_rfc3339_utc(
                        &started_at_raw,
                        "discovery_persisted_rebuild_state.started_at",
                    )?,
                    updated_at: parse_rfc3339_utc(
                        &updated_at_raw,
                        "discovery_persisted_rebuild_state.updated_at",
                    )?,
                })
            },
        )
        .transpose()
    }

    pub fn upsert_discovery_persisted_rebuild_state(
        &self,
        state: &DiscoveryPersistedRebuildStateRow,
    ) -> Result<()> {
        self.ensure_discovery_persisted_rebuild_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_persisted_rebuild_state(
                    id,
                    phase,
                    window_start,
                    horizon_end,
                    metrics_window_start,
                    phase_cursor_ts,
                    phase_cursor_slot,
                    phase_cursor_signature,
                    prepass_rows_processed,
                    prepass_pages_processed,
                    replay_rows_processed,
                    replay_pages_processed,
                    chunks_completed,
                    state_json,
                    started_at,
                    updated_at
                 ) VALUES (
                    1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15
                 )
                 ON CONFLICT(id) DO UPDATE SET
                    phase = excluded.phase,
                    window_start = excluded.window_start,
                    horizon_end = excluded.horizon_end,
                    metrics_window_start = excluded.metrics_window_start,
                    phase_cursor_ts = excluded.phase_cursor_ts,
                    phase_cursor_slot = excluded.phase_cursor_slot,
                    phase_cursor_signature = excluded.phase_cursor_signature,
                    prepass_rows_processed = excluded.prepass_rows_processed,
                    prepass_pages_processed = excluded.prepass_pages_processed,
                    replay_rows_processed = excluded.replay_rows_processed,
                    replay_pages_processed = excluded.replay_pages_processed,
                    chunks_completed = excluded.chunks_completed,
                    state_json = excluded.state_json,
                    started_at = excluded.started_at,
                    updated_at = excluded.updated_at",
                params![
                    state.phase.as_str(),
                    state.window_start.to_rfc3339(),
                    state.horizon_end.to_rfc3339(),
                    state.metrics_window_start.to_rfc3339(),
                    state
                        .phase_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    state.phase_cursor.as_ref().map(|cursor| cursor.slot as i64),
                    state
                        .phase_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    state.prepass_rows_processed as i64,
                    state.prepass_pages_processed as i64,
                    state.replay_rows_processed as i64,
                    state.replay_pages_processed as i64,
                    state.chunks_completed as i64,
                    &state.state_json,
                    state.started_at.to_rfc3339(),
                    state.updated_at.to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery persisted rebuild state")?;
        Ok(())
    }

    pub fn clear_discovery_persisted_rebuild_state(&self) -> Result<()> {
        self.ensure_discovery_persisted_rebuild_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "DELETE FROM discovery_persisted_rebuild_state WHERE id = 1",
                [],
            )
        })
        .context("failed clearing discovery persisted rebuild state")?;
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

    fn ensure_discovery_persisted_rebuild_state_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_persisted_rebuild_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    phase TEXT NOT NULL,
                    window_start TEXT NOT NULL,
                    horizon_end TEXT NOT NULL,
                    metrics_window_start TEXT NOT NULL,
                    phase_cursor_ts TEXT,
                    phase_cursor_slot INTEGER,
                    phase_cursor_signature TEXT,
                    prepass_rows_processed INTEGER NOT NULL DEFAULT 0,
                    prepass_pages_processed INTEGER NOT NULL DEFAULT 0,
                    replay_rows_processed INTEGER NOT NULL DEFAULT 0,
                    replay_pages_processed INTEGER NOT NULL DEFAULT 0,
                    chunks_completed INTEGER NOT NULL DEFAULT 0,
                    state_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )",
            )
            .context("failed to ensure discovery_persisted_rebuild_state table exists")?;
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
    use copybot_core_types::SwapEvent;
    use std::path::Path;
    use tempfile::tempdir;

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

    fn swap(
        signature: &str,
        wallet: &str,
        ts_utc: DateTime<Utc>,
        token_in: &str,
        token_out: &str,
        slot: u64,
    ) -> SwapEvent {
        SwapEvent {
            signature: signature.to_string(),
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot,
            ts_utc,
        }
    }

    #[test]
    fn observed_wallet_activity_page_uses_exact_day_counts_with_partial_first_day() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-activity-page-partial-day.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        store.insert_observed_swap(&swap(
            "sig-wallet-a-pre",
            "wallet-a",
            DateTime::parse_from_rfc3339("2026-03-06T08:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenWalletA1111111111111111111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-a-in-window",
            "wallet-a",
            DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenWalletA1111111111111111111111111111111",
            2,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-a-next-day",
            "wallet-a",
            DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenWalletA1111111111111111111111111111111",
            3,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-b-in-window",
            "wallet-b",
            DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenWalletB1111111111111111111111111111111",
            4,
        ))?;
        store.upsert_wallet_activity_days(&[
            WalletActivityDayRow {
                wallet_id: "wallet-a".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-a".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-b".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
        ])?;

        let since = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = DateTime::parse_from_rfc3339("2026-03-07T23:59:59Z")
            .expect("ts")
            .with_timezone(&Utc);
        let page = store.observed_wallet_activity_page_in_window_with_budget(
            since,
            until,
            None,
            10,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert!(!page.time_budget_exhausted);
        assert_eq!(page.rows_seen, 3);
        assert_eq!(
            page.active_day_count_source,
            Some(ObservedWalletActivityDayCountSource::WalletActivityDays)
        );
        let by_wallet: HashMap<String, ObservedWalletActivityRow> = page
            .rows
            .into_iter()
            .map(|row| (row.wallet_id.clone(), row))
            .collect();
        assert_eq!(by_wallet["wallet-a"].trades, 2);
        assert_eq!(by_wallet["wallet-a"].active_day_count, 2);
        assert_eq!(by_wallet["wallet-a"].first_seen, since + Duration::hours(2));
        assert_eq!(
            by_wallet["wallet-a"].last_seen,
            DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
                .expect("ts")
                .with_timezone(&Utc)
        );
        assert_eq!(by_wallet["wallet-b"].trades, 1);
        assert_eq!(by_wallet["wallet-b"].active_day_count, 1);
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_page_falls_back_when_wallet_activity_days_is_incomplete(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-activity-page-fallback.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        store.insert_observed_swap(&swap(
            "sig-wallet-fallback-a-1",
            "wallet-fallback-a",
            DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenFallbackA11111111111111111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-fallback-a-2",
            "wallet-fallback-a",
            DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenFallbackA11111111111111111111111111111",
            2,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-fallback-b-1",
            "wallet-fallback-b",
            DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenFallbackB11111111111111111111111111111",
            3,
        ))?;

        let conn = Connection::open(&db_path)?;
        conn.execute(
            "DELETE FROM wallet_activity_days WHERE wallet_id = 'wallet-fallback-a'",
            [],
        )?;

        let page = store.observed_wallet_activity_page_in_window_with_budget(
            DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2026-03-07T23:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
            None,
            10,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert!(!page.time_budget_exhausted);
        assert_eq!(
            page.active_day_count_source,
            Some(ObservedWalletActivityDayCountSource::ObservedSwapsFallback)
        );
        let by_wallet: HashMap<String, ObservedWalletActivityRow> = page
            .rows
            .into_iter()
            .map(|row| (row.wallet_id.clone(), row))
            .collect();
        assert_eq!(by_wallet["wallet-fallback-a"].active_day_count, 2);
        assert_eq!(by_wallet["wallet-fallback-b"].active_day_count, 1);
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_page_does_not_count_future_until_day_from_fast_path() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-activity-page-future-until-day.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let window_start = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let window_end = DateTime::parse_from_rfc3339("2026-03-07T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);

        store.insert_observed_swap(&swap(
            "sig-wallet-until-future-in-window",
            "wallet-until-future",
            DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenUntilFuture111111111111111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-until-future-after-window",
            "wallet-until-future",
            DateTime::parse_from_rfc3339("2026-03-07T20:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenUntilFuture111111111111111111111111111",
            2,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-until-present-day-one",
            "wallet-until-present",
            DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenUntilPresent11111111111111111111111111",
            3,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-until-present-day-two",
            "wallet-until-present",
            DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenUntilPresent11111111111111111111111111",
            4,
        ))?;
        store.upsert_wallet_activity_days(&[
            WalletActivityDayRow {
                wallet_id: "wallet-until-future".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-until-future".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-07T20:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-07T20:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-until-present".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-until-present".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
        ])?;

        let page = store.observed_wallet_activity_page_in_window_with_budget(
            window_start,
            window_end,
            None,
            10,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert!(!page.time_budget_exhausted);
        assert_eq!(
            page.active_day_count_source,
            Some(ObservedWalletActivityDayCountSource::WalletActivityDays)
        );
        let by_wallet: HashMap<String, ObservedWalletActivityRow> = page
            .rows
            .into_iter()
            .map(|row| (row.wallet_id.clone(), row))
            .collect();
        assert_eq!(
            by_wallet["wallet-until-future"].active_day_count,
            1,
            "future activity after until on the same day must not inflate fast-path active_day_count"
        );
        assert_eq!(by_wallet["wallet-until-present"].active_day_count, 2);
        Ok(())
    }
}
